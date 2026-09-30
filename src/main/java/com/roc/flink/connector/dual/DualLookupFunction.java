package com.roc.flink.connector.dual;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 双源容错的异步 Lookup Function，带攒批。本类是连接器的<b>主备路由核心</b>。
 *
 * <p>核心逻辑：把一条条查询攒成批（攒满 {@code lookup.batch.size} 条、或最多等
 * {@code lookup.batch.max-wait} 毫秒），一次性批量查主源（默认 HBase）；批量超时或异常
 * 则批量降级查备源（Doris）；查不到（空结果）不算异常、不降级。
 *
 * <p><b>数据流全景</b>：
 * <pre>
 *   asyncLookup(key)  ─┐
 *   asyncLookup(key)  ─┼─► batchKeys 缓冲区 ──(攒满 / 超时)──► flush()
 *   asyncLookup(key)  ─┘                                          │
 *                                                    ┌────────────┴────────────┐
 *                                            主源成功 │                         │ 主源失败/超时
 *                                                    ▼                         ▼
 *                                              distribute()              备源查询后再 distribute()
 *                                                    │
 *                                                    ▼
 *                                        各 key 的 CompletableFuture 完成
 * </pre>
 *
 * <p><b>并发模型</b>：{@code asyncLookup} 会被 Flink 的多条输入端线程并发调用，因此
 * 入队必须加锁；但批量 IO 绝不能持锁执行，否则一个慢批次会阻塞所有入队请求、
 * 让攒批退化成串行。故本类的模式是「<b>锁内只做入队，锁外做 IO</b>」。
 *
 * <p>Flink 1.18 说明：异步查找继承 {@link AsyncLookupFunction}，实现 {@code asyncLookup(RowData)}。
 * 基类的 {@code eval(...)} 是 final，不要覆盖。主备两个 Reader 采用惰性建连。
 */
public class DualLookupFunction extends AsyncLookupFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DualLookupFunction.class);

    /** 全局配置（含主备源、超时、攒批参数） */
    private final DualLookupOptions.Config cfg;
    /** 维表名，仅用于日志与统计 */
    private final String tableName;
    /** RowData <-> Java 对象转换器 */
    private final RowDataConverter converter;
    /** 主键列类型，决定从 RowData 里用哪种 getXxx 取值 */
    private final LogicalType[] keyTypes;
    /** 主源读取器（已由 TableSource 按 lookup.primary 排好序） */
    private final LookupReader primary;
    /** 备源读取器 */
    private final LookupReader standby;

    // ---- 攒批状态（transient，运行时在 ensureOpened 里初始化） ----
    // 全部 transient 的原因：它们是运行期状态，不该（也不能）被序列化下发；
    // 真正跨节点传输的是 Reader 与 Config（都是 Serializable）。

    /** 保护 batchKeys / batchFutures / pendingFlush 的锁对象（延迟创建，避免序列化） */
    private transient Object lock;
    /** 攒批中的主键值（ArrayList 顺序与 batchFutures 严格对应） */
    private transient List<Object[]> batchKeys;
    /** 攒批中每个 key 对应的结果容器，与 batchKeys 同下标 */
    private transient List<CompletableFuture<Collection<RowData>>> batchFutures;
    /** 攒批定时器（时间兜底触发） */
    private transient ScheduledExecutorService batchScheduler;
    /**
     * 在途的定时 flush 任务，同时承担两个职责：
     * <ol>
     *   <li><b>标志位</b>：非 null 即表示「已安排定时器」，避免每个 key 都重复安排；</li>
     *   <li><b>取消句柄</b>：主动 flush（攒满触发或关闭）时取消它，防止残留的定时任务
     *       到点后把<b>下一批</b>提前冲掉。</li>
     * </ol>
     *
     * <p>为什么必须能取消？假设 batchMaxWaitMs=5：
     * <pre>
     *   t=0    安排定时任务 T1（5ms 后触发）
     *   t=2    攒满 50 条 -> flush() 摘走整批
     *   t=2.5  新 key 入队 -> 又安排 T2（7.5ms 触发）
     *   t=5    T1 照常触发 -> 看到缓冲非空 -> 把刚攒的新批提前冲掉（只剩 1~2 条）
     * </pre>
     * 后果是实际批大小系统性小于 {@code lookup.batch.size}，SQL 次数高于理论值、
     * FE 解析规划压力被白白放大——而定时的意义正是「兜底」，不该干扰正常攒批。
     */
    private transient ScheduledFuture<?> pendingFlush;
    /** 运行期统计 */
    private transient LookupStats stats;
    /** 降级日志限流闸门（主源长故障时把 WARN 洪峰压平） */
    private transient FailoverLogGate failoverGate;
    /** 是否已完成初始化（保证 ensureOpened 幂等） */
    private transient volatile boolean initialized;

    public DualLookupFunction(DualLookupOptions.Config cfg,
                              String tableName,
                              RowDataConverter converter,
                              LogicalType[] keyTypes,
                              LookupReader primary,
                              LookupReader standby) {
        this.cfg = cfg;
        this.tableName = tableName;
        this.converter = converter;
        this.keyTypes = keyTypes;
        this.primary = primary;
        this.standby = standby;
    }

    /**
     * 算子启动钩子。这里只初始化轻量状态（缓冲区、定时器、统计），
     * <b>不建立数据源连接</b>——建连延迟到首次查询时惰性触发，
     * 这样主源在作业启动瞬间不可用时作业也能正常起来（直接走备源）。
     */
    @Override
    public void open(FunctionContext context) throws Exception {
        ensureOpened();
    }

    /**
     * 幂等地初始化运行期状态。
     *
     * <p>为什么 open 和 asyncLookup 都会调用它？因为异步算子在 open 之前理论上不应被调用，
     * 但为了对「某些运行模式/测试场景下 open 未被显式触发」保持健壮，在查询入口再兜一次。
     * volatile + synchronized 双重保证：既不会被重复初始化，也不会读到半个初始化状态。
     */
    private synchronized void ensureOpened() {
        if (initialized) {
            return;
        }
        lock = new Object();
        batchKeys = new ArrayList<>();
        batchFutures = new ArrayList<>();
        batchScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dual-lookup-batch");
            t.setDaemon(true);
            return t;
        });
        stats = new LookupStats(tableName, cfg.primary, cfg.standby, cfg.statsLogIntervalSec);
        stats.start();
        // 限流间隔从「秒」换算成「毫秒」；<=0 时闸门退化为不限流（visit 恒返回 >=0）
        failoverGate = new FailoverLogGate(cfg.failoverLogIntervalSec * 1000L);
        initialized = true;
    }

    /**
     * 异步查找入口：把请求塞进攒批缓冲并返回一个「将来会完成」的 Future。
     *
     * <p>即 Flink 调用本方法后立刻拿到 Future 就返回了，真正的查询由攒批触发，
     * 这是攒批能在异步算子里成立的前提。
     */
    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        // 双检锁：initialized 是 volatile，正常情况下这里只读一次标志就返回，
        // 不必每条记录都去抢一次 synchronized 方法的锁（那是热路径上纯浪费的固定开销）
        if (!initialized) {
            ensureOpened();
        }

        // 先取出 JVM 友好的主键值：RowData 是 Flink 内部可变结构，
        // 攒批期间可能被复用/改写，必须在入队前就转成独立的对象数组
        Object[] keys = toJavaKeys(extractKeyValues(keyRow));
        CompletableFuture<Collection<RowData>> f = new CompletableFuture<>();

        boolean needFlush;
        synchronized (lock) {
            // 入队：keys 与 future 同下标，保证结果能按位置分发回去
            batchKeys.add(keys);
            batchFutures.add(f);
            if (batchKeys.size() >= cfg.batchSize) {
                // 条件一：攒满即发
                needFlush = true;
            } else {
                needFlush = false;
                if (pendingFlush == null) {
                    // 条件二：时间兜底。只在「尚未安排」时才安排，避免每个 key 都排一个定时任务。
                    // 把返回的 Future 存进 pendingFlush：既是标志位，也是后续 flush() 取消它的句柄
                    pendingFlush = batchScheduler.schedule(
                            this::onFlushTimer, cfg.batchMaxWaitMs, TimeUnit.MILLISECONDS);
                }
            }
        }
        if (needFlush) {
            flush(); // 锁外触发，避免批量 IO 持锁阻塞后续入队
        }
        return f;
    }

    /**
     * 定时兜底触发（由 {@code batchScheduler} 调用）。
     *
     * <p>为什么先作废自身引用、再调用 {@link #flush()}，而不直接在 flush() 里取消自己？
     * 因为定时任务触发时它的 Future 已处于「正在运行」状态，{@code cancel(false)} 对它无效。
     * 若不清空 {@code pendingFlush}，接下来 flush() 里的取消动作就会误伤
     * 「此时刚被新批次安排好的那个定时器」，让新批次失去时间兜底。
     */
    private void onFlushTimer() {
        synchronized (lock) {
            pendingFlush = null; // 本任务已触发，引用作废
        }
        flush();
    }

    /**
     * 把当前攒的整批「原子地摘走」，然后锁外执行批量查询。
     *
     * <p>关键设计：摘取时立刻清空缓冲并取消在途定时任务，
     * 这样在查询进行中累积的新请求可以重新攒批、重新计时，两个批次互不干扰。
     */
    private void flush() {
        List<Object[]> keys;
        List<CompletableFuture<Collection<RowData>>> futures;
        synchronized (lock) {
            // 取消在途的定时任务：不取消的话，它到点后会看到一个非空的新批次并把它提前冲掉
            if (pendingFlush != null) {
                pendingFlush.cancel(false); // false：不打断正在执行的线程，只取消尚未触发的任务
                pendingFlush = null;
            }
            if (batchKeys.isEmpty()) {
                // 已被其他线程抢先 flush，直接返回（定时任务与「攒满」同时触发时会出现）
                return;
            }
            keys = new ArrayList<>(batchKeys);
            futures = new ArrayList<>(batchFutures);
            batchKeys.clear();
            batchFutures.clear();
        }
        doBatchLookup(keys, futures);
    }

    /**
     * 批量查主源，失败/超时批量降级备源，最后按 key 分发。
     *
     * <p>降级粒度是<b>整批</b>而非单条：主源失败时整批改查备源，
     * 好处是实现简单、备源负载可控；代价是主源部分成功时也要整批重查——
     * 但这个取舍换来了「绝不漏查、绝不静默丢数据」的确定性，对风控类场景更重要。
     */
    private void doBatchLookup(List<Object[]> keys, List<CompletableFuture<Collection<RowData>>> futures) {
        long startNanos = System.nanoTime(); // 计时起点的唯一来源：含降级时长的总耗时由此算出

        call(primary, keys).whenComplete((resultMap, ex) -> {
            // 主源这一批的耗时（成功、失败都记）：失败批次的耗时同样是主源健康度的关键指标，
            // 只统计成功批次会把「主源正在变慢、开始不断超时」这段恶化过程完全掩盖掉。
            // 在完成回调里才计时，才能保证「批次数」与「耗时样本数」严格一一对应
            stats.recordPrimaryBatch(keys.size(), System.nanoTime() - startNanos);
            if (ex == null) {
                // 主源正常返回（包含「查不到」的情况，那是正常业务结果，不算失败）
                infoRecovered();
                distribute(futures, resultMap, cfg.primary, startNanos);
                return;
            }
            // ---- 进入降级分支 ----
            Throwable cause = unwrap(ex);
            stats.recordPrimaryFail();
            stats.recordFailover();
            warnFailover(keys.size(), cause);

            // 备源耗时从「真正发起备源查询」的那一刻起算，不含前面等主源超时的那段时间——
            // 否则主源越慢，备源的「平均耗时」就越难看，指标就失去了诊断价值
            long standbyStartNanos = System.nanoTime();
            call(standby, keys).whenComplete((rm2, ex2) -> {
                stats.recordStandbyBatch(System.nanoTime() - standbyStartNanos);
                if (ex2 != null) {
                    // 两源都失败：让整批 Future 异常完成。
                    // 刻意不「补空结果」，因为静默补空等于把故障伪装成「该 key 不存在」，
                    // 会产出错误的关联结果；宁可让作业失败暴露问题
                    stats.recordStandbyFail();
                    for (CompletableFuture<Collection<RowData>> f : futures) {
                        f.completeExceptionally(unwrap(ex2));
                    }
                } else {
                    distribute(futures, rm2, cfg.standby, startNanos);
                }
            });
        });
    }

    /**
     * 输出降级日志（受 {@code lookup.failover.log-interval} 限流）。
     *
     * <p>为什么不能像最初那样「每批一条 WARN」？主源长时间故障时，这个日志的量级由
     * 「批次数」决定而非「记录数」：每分钟 5000 条流量、{@code max-wait=30ms} 下有效批大小
     * 约 2~3，即每秒约 30 批 → 一天近 300 万行。除了占磁盘，更糟的是 TM 日志频繁滚动，
     * 会把故障起始时刻的真实上下文冲掉，让排查反而变难。
     *
     * <p>限流策略是「首条必打 + 按间隔采样 + 合并计数」：
     * <ul>
     *   <li>故障第一批携带完整原因，故障起点不会被漏掉；</li>
     *   <li>后续每条附带「期间共合并了多少批」，数量信息没有丢失；</li>
     *   <li>精确计数另有 {@code stats} 的 {@code failover=} 字段兜底。</li>
     * </ul>
     *
     * @param batchSize 本批的 key 数量（用于判断攒批是否生效）
     * @param cause     主源失败的真实原因（已剥掉 CompletableFuture 包装）
     */
    private void warnFailover(int batchSize, Throwable cause) {
        long merged = failoverGate.visit(System.currentTimeMillis());
        if (merged == 0L) {
            LOG.warn("[dual-lookup] 主源 {} 批量查询失败，降级到备源 {}。批大小={}，原因：{}",
                    cfg.primary, cfg.standby, batchSize, cause.toString());
        } else if (merged > 0L) {
            LOG.warn("[dual-lookup] 主源 {} 仍在降级中，降级到备源 {}。批大小={}，原因：{}"
                            + "（距上次打印又发生 {} 批降级已被合并，需要逐批留痕请将 lookup.failover.log-interval 设为 0）",
                    cfg.primary, cfg.standby, batchSize, cause.toString(), merged);
        }
        // merged < 0：本次被限流，静默丢弃——计数已进 stats，信息不会丢
    }

    /**
     * 主源从故障中恢复时补一条 INFO，与上面的降级 WARN 配对。
     *
     * <p>只在「确实降级过」时才输出（{@code total() > 0}），因此主源一直健康时完全无噪音。
     * 有了它，运维不必再靠「WARN 停了没有」去推断恢复时点，也顺手拿到本次故障的降级总量。
     */
    private void infoRecovered() {
        long totalFailovers = failoverGate.total();
        if (totalFailovers <= 0L) {
            return;
        }
        failoverGate.reset(); // 先清零，再打日志：避免日志线程异常导致状态残留
        LOG.info("[dual-lookup] 主源 {} 已恢复，重新由主源提供数据（本次故障共降级 {} 批）",
                cfg.primary, totalFailovers);
    }

    /**
     * 把批量结果按下标分发到各 future，并附上来源/耗时元信息。
     *
     * <p>未命中的 key 补 {@code emptyList()} 而不是留空——Lookup Join 需要明确知道
     * 「查过了、但没查到」，Future 必须完成，否则算子的缓冲会一直被占用。
     *
     * @param source     本次结果来自哪个源；降级后这里会是备源名
     * @param startNanos 批次开始时间，用于算总耗时（降级场景下含主源超时时间）
     */
    private void distribute(List<CompletableFuture<Collection<RowData>>> futures,
                            Map<Integer, List<Map<String, Object>>> resultMap,
                            String source, long startNanos) {
        long costMs = (System.nanoTime() - startNanos) / 1_000_000L;
        for (int i = 0; i < futures.size(); i++) {
            List<Map<String, Object>> rows = resultMap.get(i);
            if (rows == null || rows.isEmpty()) {
                // 查不到：空结果，不算故障（元字段保持 NULL，由 LEFT JOIN 语义体现）
                futures.get(i).complete(Collections.emptyList());
                continue;
            }
            // 元信息直接写进行 Map，由 RowDataConverter.resolveValue 读取：
            // 这样无需改动 Reader 的返回结构，元字段的实现对两个 Reader 完全透明
            for (Map<String, Object> row : rows) {
                row.put(RowDataConverter.META_SOURCE_KEY, source);
                row.put(RowDataConverter.META_COST_KEY, costMs);
            }
            futures.get(i).complete(toRows(rows));
        }
    }

    /**
     * 惰性建连 + 批量查询。
     *
     * <p>把「建连失败」也包装成异常完成的 Future，而不是直接抛出——
     * 这样调用方（doBatchLookup）无需 try/catch，主源建连失败能自然而然地走降级分支。
     */
    private CompletableFuture<Map<Integer, List<Map<String, Object>>>> call(
            LookupReader reader, List<Object[]> keys) {
        // close 期间，已提交批次的 whenComplete 降级链仍可能走到这里。
        // 此时绝不能重新 open()：新建出来的连接/线程池将没有人负责关闭，造成资源泄漏
        if (!initialized) {
            CompletableFuture<Map<Integer, List<Map<String, Object>>>> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("[dual-lookup] function already closed"));
            return f;
        }
        if (!reader.isOpened()) {
            try {
                reader.open(); // open() 本身 synchronized 且幂等
            } catch (Exception e) {
                // 建连失败视为该源本次查询失败：主源建连失败 → 降级查备源
                CompletableFuture<Map<Integer, List<Map<String, Object>>>> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        }
        return reader.batchLookupAsync(keys);
    }

    /**
     * 算子关闭：清理攒批缓冲、统计线程、两个数据源连接。
     *
     * <p>这里有一处容易被忽略的正确性隐患：异步算子在关闭时，缓冲区里可能还躺着若干未 flush 的请求。
     * 若不处理，这些 Future 永远不会完成，异步算子会一直等待（表现为作业取消失败或卡住）。
     * 因此关闭时必须把它们<b>补空</b>——注意这里是「关闭」场景，补空是安全的，
     * 与运行期「两源失败不补空」的策略不同。
     */
    @Override
    public void close() throws Exception {
        // 先宣告关闭，再做清理：clearing 期间到达的降级链（call()）会看到 initialized=false，
        // 直接走「已关闭」异常分支，而不是在 close 进行途中重新 open() 出一个没人关的连接
        initialized = false;
        // 兜底：把攒批缓冲里尚未 flush 的请求补空，避免异步算子等待永不完成的 future
        if (lock != null) {
            List<CompletableFuture<Collection<RowData>>> pending;
            synchronized (lock) {
                pending = new ArrayList<>(batchFutures);
                batchKeys.clear();
                batchFutures.clear();
                // 缓冲已清空，在途定时任务也必须一并取消，否则它到点后可能触发空 flush 或误伤后续状态
                if (pendingFlush != null) {
                    pendingFlush.cancel(false);
                    pendingFlush = null;
                }
            }
            for (CompletableFuture<Collection<RowData>> f : pending) {
                f.complete(Collections.emptyList());
            }
        }
        if (stats != null) {
            stats.close();
        }
        if (batchScheduler != null) {
            batchScheduler.shutdownNow();
        }
        // 两个 Reader 都关：即便某个从未建连，close() 也应容忍（内部做了 null 判断）
        primary.close();
        standby.close();
    }

    // ---------------- 键值转换 ----------------

    /** 从 Flink 的 keyRow 中按声明类型取出主键值（仍是 Flink 内部表示） */
    private Object[] extractKeyValues(RowData keyRow) {
        Object[] keys = new Object[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            keys[i] = getField(keyRow, i, keyTypes[i]);
        }
        return keys;
    }

    /**
     * 按逻辑类型从 RowData 取值。
     *
     * <p>必须按类型分支调用对应的 {@code getXxx}，不能用统一的 {@code getField}：
     * RowData 的访问器带有「按类型解码」的语义（例如 DECIMAL 需要 precision/scale 才能正确解码），
     * 用错访问器会得到错误的数值。
     */
    private Object getField(RowData row, int pos, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case DECIMAL: {
                // DecimalData 是定长表示，解码必须带上 precision/scale
                DecimalType dt = (DecimalType) type;
                return row.getDecimal(pos, dt.getPrecision(), dt.getScale());
            }
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // 两者内部都是 int（天数 / 毫秒数）
                return row.getInt(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                TimestampType tt = (TimestampType) type;
                return row.getTimestamp(pos, tt.getPrecision());
            }
            case BINARY:
            case VARBINARY:
                return row.getBinary(pos);
            default:
                // 字符串及其他类型统一走 getString（StringData）
                return row.getString(pos);
        }
    }

    /**
     * 把 Flink 内部值批量转成标准 Java 对象。
     *
     * <p>必须在入队（攒批）前完成转换：RowData 的内容可能在 Flink 内部被复用，
     * 攒批期间持有内部值引用会读到脏数据。这里一次性转成独立对象，彻底消除该隐患。
     */
    private Object[] toJavaKeys(Object[] keys) {
        Object[] out = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            out[i] = converter.toJavaValue(keys[i], keyTypes[i]);
        }
        return out;
    }

    /** Java 对象行 -> RowData 行集合，交给 Lookup Join 继续处理 */
    private Collection<RowData> toRows(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<RowData> out = new ArrayList<>(rows.size());
        for (Map<String, Object> r : rows) {
            out.add(converter.toRowData(r));
        }
        return out;
    }

    /** 剥掉 CompletableFuture 包装的 CompletionException，让日志看到真实异常原因 */
    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }

    /**
     * 降级日志限流闸门：把「每批一条 WARN」压缩成「每个故障周期首条必打 + 按间隔采样」。
     *
     * <p>为什么单独抽出来？因为「限流」这件事的判定是纯时间逻辑，与查询流程无关；
     * 抽成独立的、不依赖时钟的纯状态机（时刻由调用方传入）后可以直接被单元测试覆盖——
     * 否则只能靠捕获日志输出来验证，既脆弱又难覆盖边界。
     *
     * <p>线程安全：{@code doBatchLookup} 的完成回调可能跑在多个源各自的 IO 线程上，
     * 并发进入是常态（同时刻可能有多个在途批次），因此所有状态变更都在 {@code synchronized} 内，
     * 保证计数不丢、不重复。
     */
    static final class FailoverLogGate {

        /** 最小输出间隔（毫秒）；&lt;= 0 表示不限流 */
        private final long intervalMs;
        /** 是否已经输出过；用它而不是和时间做差，是为了让「首条必打」不依赖时钟初值 */
        private boolean emitted;
        /** 上一次实际输出的时刻（毫秒） */
        private long lastLogMs;
        /** 距上次输出被抑制（合并）的批数 */
        private long suppressed;
        /** 自上次 {@link #reset()} 以来的降级总批数 */
        private long total;

        FailoverLogGate(long intervalMs) {
            this.intervalMs = intervalMs;
            this.lastLogMs = System.currentTimeMillis();
        }

        /**
         * 记一次降级，并判定本次是否应输出日志。
         *
         * @param nowMs 当前时刻（毫秒）；由调用方传入而不是内部取，便于测试构造时间线
         * @return {@code >= 0}：应输出，值为「距上次输出被合并的批数」；{@code -1}：本次应抑制
         */
        synchronized long visit(long nowMs) {
            total++;
            // 首条（!emitted）无条件放行；之后必须距上次输出满一个间隔
            if (!emitted || nowMs - lastLogMs >= intervalMs) {
                long merged = suppressed;
                suppressed = 0L;
                lastLogMs = nowMs;
                emitted = true;
                return merged;
            }
            suppressed++;
            return -1L;
        }

        /** 自上次 reset 以来的降级总批数：既用于恢复日志汇总，也用于判断当前是否处于故障期 */
        synchronized long total() {
            return total;
        }

        /** 主源恢复后清零，开始统计下一个故障周期 */
        synchronized void reset() {
            suppressed = 0L;
            total = 0L;
            emitted = false;
            lastLogMs = System.currentTimeMillis();
        }
    }
}
