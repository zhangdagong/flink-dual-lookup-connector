package com.roc.flink.connector.dual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 运行期统计：主/备源的批次数、key 数、失败数、命中率、平均与最长读取耗时、降级次数与影响面、
 * 失败原因分类，定时打一行日志。
 *
 * <p>为什么用定时日志而不是 Flink 指标：异步 Lookup Function 确实拿不到 {@code RuntimeContext}，
 * 但它的 {@code open(FunctionContext)} 提供的 {@code FunctionContext.getMetricGroup()} 就是
 * <b>本并行子任务的 metric group</b>，注册 {@code Counter}/{@code Gauge} 是可行的（见 README 第 14 章）。
 * 这里仍选日志，是因为巡检与复盘需要「一行看全 + 可直接 grep + 能带文本字段（失败原因、
 * 本阶段与累计两个视角）」，而 {@code Gauge} 只给单个数值；另外 {@code FunctionContext}
 * 在常量折叠等本地执行路径上拿到的是未注册的 metric group，两条路径行为不一致，日志则始终一致。
 *
 * <p><b>每行日志同时给出两个视角</b>，用 {@code ||} 分隔：
 * <ul>
 *   <li>前半段 {@code window=Ns}：<b>本阶段</b>增量，读到的就是「刚刚过去这段时间发生了什么」，
 *       不必再拿两行日志做减法；</li>
 *   <li>后半段 {@code since-start uptime=Ns}：<b>自启动累计</b>（准确说是本统计实例创建以来，
 *       TaskManager 重启后归零），一眼看到全程总量与长期基线。</li>
 * </ul>
 *
 * <p>实现上内部计数<b>始终是单调递增的累计值</b>（因此中途漏看某一行也不会丢数），
 * 阶段值靠「当前累计 − 上次快照」换算，快照字段见 {@link #lastSample}。
 * 两段共用同一份 {@link Sample}，保证两个视角来自同一时刻，日志内部不会自相矛盾。
 *
 * <p><b>两个刻意不一致的口径</b>（都容易看错，详见 README 第 8 章）：
 * <ol>
 *   <li>{@code max=} 是<b>区间最大值，不能用「累计相减」换算</b>，因此由写侧用
 *       {@code getAndSet(0)} 取走（见 {@link #snapshotLine()}）；漏打一行时峰值只会顺延到
 *       下一个窗口，不会像计数那样「合并到下次」，但也不会丢。</li>
 *   <li>{@code hit=} 的分母是<b>该源成功返回的查询所覆盖的 key 数</b>，不是全部批次——
 *       失败的批次根本没有「命中率」可言，若并入分母，主源一故障命中率就会莫名其妙地掉。</li>
 * </ol>
 *
 * <p>线程安全：所有累计计数都是 {@link AtomicLong} / {@link AtomicLongArray}，因为埋点来自攒批线程、
 * 源侧回调线程等多个并发来源，且读（打点）写（埋点）同时发生。
 * 而 {@link #lastSample}、{@link #histPMax} 等只在打点线程里读写
 * （{@link #logSnapshot} 由单线程调度器串行调用），不需要额外的同步。
 */
public class LookupStats {

    private static final Logger LOG = LoggerFactory.getLogger(LookupStats.class);

    /** 统计日志统一前缀，便于在 TM 日志里直接 grep（降级/恢复日志共用同一前缀） */
    private static final String PREFIX = "[dual-lookup] ";

    /** 毫秒转纳秒的除数，用于把累计耗时换算成「毫秒/批」 */
    private static final double NANOS_PER_MS = 1_000_000.0;

    // ---- 失败原因分类：桶的顺序即历史原因，改动会导致新旧日志口径不可比，只能往后追加 ----
    private static final int K_TIMEOUT = 0;
    private static final int K_CONNECT = 1;
    private static final int K_POOL_FULL = 2;
    private static final int K_DB = 3;
    private static final int K_OTHER = 4;
    /** 分类标签；与上面的索引一一对应 */
    static final String[] FAIL_KINDS = {"timeout", "connect", "poolFull", "db", "other"};

    /** 所属维表名，仅用于日志标识（同一作业可能有多张 dual-lookup 维表） */
    private final String tableName;
    /** 主源名（hbase/doris），既用于日志展示，也用于把命中统计分流到正确的一侧 */
    private final String primary;
    /** 备源名（hbase/doris），仅用于日志展示 */
    private final String standby;
    /** 打点间隔（秒），<=0 表示关闭打点 */
    private final int logIntervalSec;
    /** 配置摘要（只打一次的回执）；为 null 表示不打配置行（单测里常见） */
    private final String configSummary;
    /** 统计实例创建时刻，用于算 since-start 段的 uptime（从而给出「全程平均吞吐」） */
    private final long startMs = System.currentTimeMillis();

    // ==================== 累计计数（埋点侧并发写，只增不减） ====================

    /**
     * 提交给主源的 key 总数。
     *
     * <p>注意口径：降级时同一批 key 不会重复计入——备源查询是同一批数据的重试，
     * 若也算进来，「处理量」会被降级频率虚高。
     */
    private final AtomicLong totalKeys = new AtomicLong();
    /** 降级次数：一次「主源失败 -> 改查备源」记 1 */
    private final AtomicLong failoverCount = new AtomicLong();
    /**
     * 降级波及的 key 数（本阶段/累计）。
     *
     * <p>为什么批次数不够用：33 批 × 50 key 和 33 批 × 1 key 的严重性差 50 倍，
     * 只看 {@code failover=33} 完全分不出来。有了 key 数才能算出「降级率」，
     * 也才能回答「这次故障到底影响了多少条数据」。
     */
    private final AtomicLong failoverKeys = new AtomicLong();

    // ---- 主源 ----
    /** 主源执行的批次数（含失败的批次） */
    private final AtomicLong pBatches = new AtomicLong();
    /** 主源处理的 key 数（用于算平均批大小） */
    private final AtomicLong pKeys = new AtomicLong();
    /** 主源失败的批次数 */
    private final AtomicLong pFails = new AtomicLong();
    /** 主源累计查询耗时（纳秒），除以批次数即为平均读取耗时 */
    private final AtomicLong pNanos = new AtomicLong();
    /** 主源本窗口内最长单批耗时（纳秒）；打点后清零，见 {@link #snapshotLine()} */
    private final AtomicLong pMax = new AtomicLong();
    /** 主源成功返回的查询中命中的 key 数 */
    private final AtomicLong pHitKeys = new AtomicLong();
    /** 主源成功返回的查询覆盖的 key 数（命中率分母） */
    private final AtomicLong pHitTotal = new AtomicLong();
    /** 主源失败原因分类计数，下标见 {@code K_*} 常量 */
    private final AtomicLongArray pFailKinds = new AtomicLongArray(FAIL_KINDS.length);

    // ---- 备源 ----
    /** 备源执行的批次数（正常情况下应为 0） */
    private final AtomicLong sBatches = new AtomicLong();
    /** 备源失败的批次数（>0 说明两源都异常，需要立即介入） */
    private final AtomicLong sFails = new AtomicLong();
    /** 备源累计查询耗时（纳秒） */
    private final AtomicLong sNanos = new AtomicLong();
    /** 备源本窗口内最长单批耗时（纳秒） */
    private final AtomicLong sMax = new AtomicLong();
    /** 备源命中的 key 数 */
    private final AtomicLong sHitKeys = new AtomicLong();
    /** 备源命中率分母 */
    private final AtomicLong sHitTotal = new AtomicLong();

    // ==================== 上一次打点的快照（只有打点线程读写） ====================
    // 阶段值 = 累计值 − 快照值。用「相减」而不是「清零」的原因是：
    // 若中途漏打一行（例如 INFO 级别被调高过滤掉），清零会把这批数据永久丢掉，
    // 而相减只会把两个区间合并到下一次输出里，数据仍然守恒。

    /** 上次打点时的累计计数；{@link Sample#EMPTY} 表示还没打过点 */
    private Sample lastSample = Sample.ofEmpty();
    /** 上次打点时刻（毫秒），用于给出真实的采样窗口长度（调度器可能被负载拖延） */
    private long lastSnapshotMs = 0L;
    /**
     * 历史最长单批耗时（纳秒）。由打点线程维护，而不是另开原子变量：
     * since-start 段的 max = max(历史上每个窗口的 max)，这个"取最大值"的合并操作
     * 放在打点线程里做就够了，热路径（埋点侧）不必再付一次原子写。
     */
    private long histPMax = 0L;
    private long histSMax = 0L;
    /** 打点任务自身出错时只告警一次，避免每 6 分钟重复刷一条同样的 WARN */
    private boolean logFailureReported = false;

    /** 定时打点线程池；transient，因为线程池本身不可序列化，且只在当前 TM 内有效 */
    private transient ScheduledExecutorService scheduler;

    /** 不带配置回执的构造（单测与「不需要配置行」的场景用） */
    public LookupStats(String tableName, String primary, String standby, int logIntervalSec) {
        this(tableName, primary, standby, logIntervalSec, null);
    }

    /**
     * @param configSummary 关键参数摘要，仅在启动时打印一次；传 {@code null} 表示不打印配置行
     */
    public LookupStats(String tableName, String primary, String standby,
                       int logIntervalSec, String configSummary) {
        this.tableName = tableName;
        this.primary = primary;
        this.standby = standby;
        this.logIntervalSec = logIntervalSec;
        this.configSummary = configSummary;
    }

    /** 启动定时打点；幂等（已启动则不再重复创建线程）；interval<=0 时静默跳过 */
    public void start() {
        if (logIntervalSec > 0 && scheduler == null) {
            lastSnapshotMs = System.currentTimeMillis();
            // 配置回执只打一次。它的价值不在当时，而在于事后：拿到一段 TM 日志时能立刻确认
            // 「这段日志是在 batch.size=50、timeout=500ms 下产生的」，否则只能靠翻作业历史猜
            if (configSummary != null && LOG.isInfoEnabled()) {
                LOG.info(configLine());
            }
            // 用守护线程：作业异常退出时不至于因为该线程而卡住 JVM 关闭
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dual-lookup-stats");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(this::logSnapshotSafely, logIntervalSec, logIntervalSec, TimeUnit.SECONDS);
        }
    }

    /**
     * 停止打点；置空引用以便重复调用安全。
     *
     * <p>收尾时补一条累计汇总，但有两个前提，避免日志噪音：
     * ① 统计功能本身是开启的（{@code log-interval > 0}，主动关掉的用户不该再收到统计日志）；
     * ② 最后一次打点之后确实还有新活动——否则每行日志已经带过 {@code since-start} 段，
     * 这条收尾行只会是重复内容。
     */
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (!LOG.isInfoEnabled() || logIntervalSec <= 0) {
            return;
        }
        Sample cur = readCounters();
        if (cur.sameAs(Sample.ofEmpty()) || cur.sameAs(lastSample)) {
            return;
        }
        LOG.info(totalLine());
    }

    // ---------------- 埋点 ----------------

    /**
     * 主源完成一批查询（无论成败都记）。
     *
     * <p>为什么在这里同时记「批大小」「耗时」「最长耗时」？因为它们都只在批次真正结束时才拿得到，
     * 合并成一次调用可以保证「批次数」「耗时样本数」严格一一对应——
     * 平均耗时的分母直接复用批次数（见 {@link #avgMs}），不会出现样本与批次错位。
     *
     * @param keys  本批 key 数
     * @param nanos 本批查询耗时（纳秒）
     */
    public void recordPrimaryBatch(int keys, long nanos) {
        pBatches.incrementAndGet();
        pKeys.addAndGet(keys);
        totalKeys.addAndGet(keys);
        pNanos.addAndGet(nanos);
        updateMax(pMax, nanos);
    }

    /** 备源完成一批查询（含正常降级与主源失败两种情形）；key 数不重复累加 */
    public void recordStandbyBatch(long nanos) {
        sBatches.incrementAndGet();
        sNanos.addAndGet(nanos);
        updateMax(sMax, nanos);
    }

    /**
     * 主源一批失败（超时/异常），并按其<b>原因分类</b>计数。
     *
     * <p>为什么细分而不只记一个总数：三类原因的处置完全不同——超时要去看主源的慢查询、
     * 连接问题要去看进程/网络、队列满要去看备源连接池大小。只有一个 {@code fail=2} 时，
     * 值班的人只能靠翻 WARN 日志逐条猜。
     *
     * @param cause 已剥掉 CompletableFuture 包装的真实异常
     */
    public void recordPrimaryFail(Throwable cause) {
        pFails.incrementAndGet();
        pFailKinds.incrementAndGet(classify(cause));
    }

    /** 备源一批也失败：两源同时异常（不分类：此时作业通常已经暴露，原因看降级 WARN/异常更直接） */
    public void recordStandbyFail() {
        sFails.incrementAndGet();
    }

    /**
     * 成功返回一批时登记命中情况（主源与备源各记各的）。
     *
     * <p>为什么这个指标重要：本连接器的核心语义是「查不到不算异常、不降级」，
     * 于是「维表大面积查不到」这条最危险的故障在日志上<b>完全隐身</b>——
     * 表被误删、rowkey 编码写错、Doris 同步延迟时，{@code fail=0 failover=0} 一片健康，
     * 作业却默默输出一堆 NULL。命中率是唯一能看出来的指标。
     *
     * @param source    本次结果来自哪个源（主源名或备源名）
     * @param hitKeys   命中的 key 数
     * @param totalKeys 本次查询覆盖的 key 数（该源成功返回才有意义）
     */
    public void recordHits(String source, int hitKeys, int totalKeys) {
        // 按源名分流而不是加一个 boolean：调用方本来就在用源名字符串写元字段，
        // 多引一个平行参数只会给「两个参数不一致」留下空间
        if (primary.equals(source)) {
            pHitKeys.addAndGet(hitKeys);
            pHitTotal.addAndGet(totalKeys);
        } else {
            sHitKeys.addAndGet(hitKeys);
            sHitTotal.addAndGet(totalKeys);
        }
    }

    /**
     * 发生一次降级（主源失败后改查备源）。
     *
     * @param keys 本批 key 数，用于量化「降级影响了多少条记录」
     */
    public void recordFailover(int keys) {
        failoverCount.incrementAndGet();
        failoverKeys.addAndGet(keys);
    }

    // ---------------- 输出 ----------------

    /**
     * 打点任务的入口，带异常兜底。
     *
     * <p>{@code scheduleAtFixedRate} 有个不显眼的坑：任务抛出未捕获异常后，
     * <b>后续调度会被静默取消</b>（既不打印也不重试），统计日志就此永久消失且无人察觉。
     * 因此这里兜住 Throwable 并只在首次失败时告警——统计只是可观测性手段，
     * 绝不能因为一个日志格式化问题把定时任务打死。这里 catch Throwable 是有意为之，
     * 连 Error 也要兜，因为兜不住就等于静默停摆。
     */
    private void logSnapshotSafely() {
        try {
            logSnapshot();
        } catch (Throwable t) {
            if (!logFailureReported) {
                logFailureReported = true;
                LOG.warn(PREFIX + "table={} 统计日志打点失败，该实例的周期统计可能就此停止", tableName, t);
            }
        }
    }

    /**
     * 打一行「本阶段 + 自启动累计」聚合快照。
     *
     * <p>{@code LOG.isInfoEnabled()} 提前返回不是可有可无的优化：统计日志被关掉时，
     * 这里每 {@code logIntervalSec} 秒仍会被调度一次，提前返回可以省掉二十几次原子读与字符串拼接。
     */
    private void logSnapshot() {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info(snapshotLine());
    }

    /**
     * 组装一行统计日志，并推进快照游标。
     *
     * <p>包可见而非 private：让单元测试能直接断言日志内容，而不必去捕获日志输出
     * （捕获日志的测试既脆弱又难以覆盖边界）。
     *
     * @return 形如 {@code [dual-lookup] table=dim_account window=360s ... || table=dim_account since-start uptime=5400s ...}
     *         的单行文本
     */
    String snapshotLine() {
        long now = System.currentTimeMillis();
        // 首行：窗口按配置间隔算；之后按真实间隔算（调度器可能被负载拖延，真实值更有诊断价值）
        long windowSec = lastSnapshotMs == 0L
                ? logIntervalSec
                : Math.max(1L, (now - lastSnapshotMs) / 1000L);
        lastSnapshotMs = now;

        // max 取走并清零：区间最大值无法用「累计相减」换算——两个窗口的 max 相减会得到负值或无意义的值。
        // 代价是漏打一行时峰值会顺延到下一个窗口（而不是像计数那样被合并），但它不会丢，这是可接受的
        long windowPMax = pMax.getAndSet(0L);
        long windowSMax = sMax.getAndSet(0L);
        histPMax = Math.max(histPMax, windowPMax);
        histSMax = Math.max(histSMax, windowSMax);

        Sample raw = readCounters();
        Sample stage = raw.minus(lastSample).withMax(windowPMax, windowSMax);
        Sample total = raw.withMax(histPMax, histSMax);
        lastSample = raw;

        long uptimeSec = uptimeSec();
        return PREFIX + format("window=" + windowSec + "s", windowSec, stage)
                + " || " + format("since-start uptime=" + uptimeSec + "s", uptimeSec, total);
    }

    /**
     * 组装一行「自启动以来」的累计快照；仅在 {@link #close()} 收尾时输出。
     *
     * <p>注意副作用：它会把窗口 max 取走并清零（并入历史 max），
     * 因此调用之后的下一次窗口打点其 {@code max=} 会变短。收尾场景无影响，测试里需留意。
     */
    String totalLine() {
        histPMax = Math.max(histPMax, pMax.getAndSet(0L));
        histSMax = Math.max(histSMax, sMax.getAndSet(0L));
        long uptimeSec = uptimeSec();
        return PREFIX + format("since-start uptime=" + uptimeSec + "s", uptimeSec,
                readCounters().withMax(histPMax, histSMax));
    }

    /** 配置回执行（包可见，便于测试断言） */
    String configLine() {
        return PREFIX + "table=" + tableName + " cfg[" + configSummary + "]";
    }

    /** 自启动至今的秒数（至少 1，避免除零） */
    private long uptimeSec() {
        return Math.max(1L, (System.currentTimeMillis() - startMs) / 1000L);
    }

    /** 把 9 个累计计数读成一份快照（同一时刻），保证两个视角的数字对齐；max 字段由调用方补齐 */
    private Sample readCounters() {
        return new Sample(
                totalKeys.get(), failoverCount.get(), failoverKeys.get(),
                new Src(pBatches.get(), pKeys.get(), pFails.get(), pNanos.get(),
                        pHitKeys.get(), pHitTotal.get()),
                new Src(sBatches.get(), 0L, sFails.get(), sNanos.get(),
                        sHitKeys.get(), sHitTotal.get()),
                readFailKinds());
    }

    /** 主源失败分类的当前累计值（5 个桶，顺序同 {@link #FAIL_KINDS}） */
    private long[] readFailKinds() {
        long[] out = new long[FAIL_KINDS.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = pFailKinds.get(i);
        }
        return out;
    }

    /**
     * 把一份计数快照拼成一段文本。
     *
     * <p>平均批大小的分母里有讲究：{@code avgBatch} 刻意只用主源批次数——降级时同一批 key
     * 会被主源和备源各查一次，若把备源也算进分母，totalKeys 只加一次而批次加了两次，
     * avgBatch 会被系统性低估，从而把「降级频繁」误读成「攒批没生效」，导致无效调参。
     * 另给一个 {@code avgBatchWithStandby} 作为对照，两者差距越大说明降级越频繁。
     *
     * @param head      视角标识（{@code window=360s} / {@code since-start uptime=5400s}）
     * @param windowSec 本视角覆盖的秒数，仅用于算吞吐
     */
    private String format(String head, long windowSec, Sample s) {
        long avgBatch = s.primary.batches == 0L ? 0L : s.primary.keys / s.primary.batches;
        long span = s.primary.batches + s.standby.batches;
        long avgBatchWithStandby = span == 0L ? 0L : s.totalKeys / span;

        return String.format(
                "table=%s %s totalKeys=%d rate=%s avgBatch=%d avgBatchWithStandby=%d | "
                        + "%s[batches=%d keys=%d fail=%s avg=%s max=%s hit=%s] "
                        + "%s[batches=%d fail=%d avg=%s max=%s hit=%s] failover=%s",
                tableName, head, s.totalKeys, rate(s.totalKeys, windowSec), avgBatch, avgBatchWithStandby,
                primary, s.primary.batches, s.primary.keys, failText(s.primary.fails, s.failKinds),
                avgMs(s.primary.nanos, s.primary.batches), singleMs(s.primary.maxNanos, s.primary.batches),
                pct(s.primary.hitKeys, s.primary.hitTotal),
                standby, s.standby.batches, s.standby.fails,
                avgMs(s.standby.nanos, s.standby.batches), singleMs(s.standby.maxNanos, s.standby.batches),
                pct(s.standby.hitKeys, s.standby.hitTotal),
                failoverText(s.failover, s.failoverKeys, s.totalKeys));
    }

    /** 平均单批耗时，形如 {@code 12.3ms}；无样本时返回 {@code -} */
    private static String avgMs(long totalNanos, long batches) {
        if (batches <= 0L) {
            return "-";
        }
        return String.format("%.1fms", totalNanos / NANOS_PER_MS / batches);
    }

    /** 单个耗时值，形如 {@code 480.0ms}；无样本时返回 {@code -}（0 会被误读成「秒回」） */
    private static String singleMs(long nanos, long samples) {
        if (samples <= 0L) {
            return "-";
        }
        return String.format("%.1fms", nanos / NANOS_PER_MS);
    }

    /** 占比文本，形如 {@code 98.7%}；分母为 0 时返回 {@code -} */
    private static String pct(long part, long total) {
        if (total <= 0L) {
            return "-";
        }
        return String.format("%.1f%%", 100.0 * part / total);
    }

    /** 吞吐率，形如 {@code 15.0/s} */
    private static String rate(long keys, long windowSec) {
        if (windowSec <= 0L) {
            return "-";
        }
        return String.format("%.1f/s", keys / (double) windowSec);
    }

    /**
     * 失败数文本：无失败就是朴素的 {@code 0}，有失败才展开非零分类，形如 {@code 2(timeout=1,other=1)}。
     *
     * <p>只列非零项是刻意的：分类桶会随着演进变多，若全量平铺，日志会常年挂着一串 {@code connect=0,db=0}，
     * 既占宽度又稀释真正的信息。零值不需要出现——{@code fail=} 总数本身就是守恒的。
     */
    private static String failText(long fails, long[] kinds) {
        if (fails <= 0L) {
            return "0";
        }
        StringBuilder detail = new StringBuilder();
        for (int i = 0; i < kinds.length && i < FAIL_KINDS.length; i++) {
            if (kinds[i] > 0L) {
                if (detail.length() > 0) {
                    detail.append(',');
                }
                detail.append(FAIL_KINDS[i]).append('=').append(kinds[i]);
            }
        }
        return detail.length() == 0 ? String.valueOf(fails) : fails + "(" + detail + ")";
    }

    /** 降级文本：无降级就是 {@code 0}，有降级则带上波及 key 数与占全部 key 的比例 */
    private static String failoverText(long count, long keys, long totalKeys) {
        if (count <= 0L) {
            return "0";
        }
        return String.format("%d(%dkeys,%s)", count, keys, pct(keys, totalKeys));
    }

    /** 无锁地更新最大值（CAS 失败说明别的线程刚写了更大的值，重试即可） */
    private static void updateMax(AtomicLong max, long value) {
        long cur;
        do {
            cur = max.get();
            if (value <= cur) {
                return;
            }
        } while (!max.compareAndSet(cur, value));
    }

    /**
     * 失败原因归类：把异常映射成 5 个桶之一，让日志能为「下一步该查什么」指路。
     *
     * <p>判定顺序有讲究：先具体后笼统。{@code SocketTimeoutException} 既是 IOException 也是超时，
     * 归「超时」才符合运维直觉；{@code NoRouteToHostException} 是 {@code SocketException} 的子类，
     * 先判具体类型才不会漏。
     */
    private static int classify(Throwable t) {
        if (t == null) {
            return K_OTHER;
        }
        if (t instanceof TimeoutException || t instanceof SocketTimeoutException) {
            return K_TIMEOUT;
        }
        if (t instanceof RejectedExecutionException) {
            // Doris 侧有界队列 + AbortPolicy：持续过载时快速拒绝、交给上层降级（见 DorisLookupReader）
            return K_POOL_FULL;
        }
        if (t instanceof ConnectException || t instanceof UnknownHostException
                || t instanceof NoRouteToHostException || t instanceof SocketException) {
            return K_CONNECT;
        }
        if (t instanceof SQLException) {
            // 认证失败、库表不存在、HikariCP 取连接超时等都落在这里
            return K_DB;
        }
        // 兜底：Netty 等第三方异常不在上面任何一族里，按类名再认一次。
        // 按类名判断不优雅，但比引入 netty 依赖或把它一律归到 other 更有价值
        String name = t.getClass().getName();
        if (name.contains("ConnectTimeout")) {
            return K_TIMEOUT;
        }
        if (name.contains("ConnectionRefused")) {
            return K_CONNECT;
        }
        return K_OTHER;
    }

    /**
     * 单源计数快照（不可变）。
     */
    private static final class Src {

        final long batches;
        final long keys;
        final long fails;
        final long nanos;
        final long maxNanos;
        final long hitKeys;
        final long hitTotal;

        Src(long batches, long keys, long fails, long nanos, long hitKeys, long hitTotal) {
            this(batches, keys, fails, nanos, 0L, hitKeys, hitTotal);
        }

        Src(long batches, long keys, long fails, long nanos, long maxNanos, long hitKeys, long hitTotal) {
            this.batches = batches;
            this.keys = keys;
            this.fails = fails;
            this.nanos = nanos;
            this.maxNanos = maxNanos;
            this.hitKeys = hitKeys;
            this.hitTotal = hitTotal;
        }

        /** 阶段值 = 当前累计 − 上次快照；max 刻意留 0，由 {@link #withMax} 单独补齐（区间最大值不能相减） */
        Src minus(Src base) {
            return new Src(batches - base.batches, keys - base.keys, fails - base.fails, nanos - base.nanos,
                    hitKeys - base.hitKeys, hitTotal - base.hitTotal);
        }

        Src withMax(long max) {
            return new Src(batches, keys, fails, nanos, max, hitKeys, hitTotal);
        }

        /** 与另一份快照是否完全一致（max 不参与：它不影响「有没有新活动」的判断） */
        boolean sameAs(Src o) {
            return batches == o.batches && keys == o.keys && fails == o.fails && nanos == o.nanos
                    && hitKeys == o.hitKeys && hitTotal == o.hitTotal;
        }
    }

    /**
     * 某一时刻的计数快照（不可变）。
     *
     * <p>为什么要把所有计数先读成一个对象？因为每行日志要同时给出「本阶段」与「自启动」两个视角，
     * 若两段各自去读原子变量，就可能读到两个不同的瞬间（例如 since-start 比 stage 少算了 3 批），
     * 日志内部就自相矛盾了——看日志的人只会怀疑统计不准，然后去查一个并不存在的 bug。
     * 读成一份快照后，两个视角天然对齐，且 stage + 此前累计 == since-start 恒成立。
     */
    private static final class Sample {

        final long totalKeys;
        final long failover;
        final long failoverKeys;
        final Src primary;
        final Src standby;
        /** 主源失败分类（长度同 {@link #FAIL_KINDS}） */
        final long[] failKinds;

        Sample(long totalKeys, long failover, long failoverKeys, Src primary, Src standby, long[] failKinds) {
            this.totalKeys = totalKeys;
            this.failover = failover;
            this.failoverKeys = failoverKeys;
            this.primary = primary;
            this.standby = standby;
            this.failKinds = failKinds;
        }

        static Sample ofEmpty() {
            Src zero = new Src(0L, 0L, 0L, 0L, 0L, 0L, 0L);
            return new Sample(0L, 0L, 0L, zero, zero, new long[FAIL_KINDS.length]);
        }

        /** 阶段值 = 当前累计 − 上次快照（max 由 {@link #withMax} 补齐） */
        Sample minus(Sample base) {
            return new Sample(totalKeys - base.totalKeys, failover - base.failover,
                    failoverKeys - base.failoverKeys, primary.minus(base.primary), standby.minus(base.standby),
                    diffKinds(failKinds, base.failKinds));
        }

        Sample withMax(long primaryMax, long standbyMax) {
            return new Sample(totalKeys, failover, failoverKeys,
                    primary.withMax(primaryMax), standby.withMax(standbyMax), failKinds);
        }

        /** 是否与另一份快照完全一致（用于 close 时判断「自上次打点后有没有新活动」） */
        boolean sameAs(Sample o) {
            return totalKeys == o.totalKeys && failover == o.failover && failoverKeys == o.failoverKeys
                    && primary.sameAs(o.primary) && standby.sameAs(o.standby);
        }

        private static long[] diffKinds(long[] now, long[] last) {
            long[] out = new long[now.length];
            for (int i = 0; i < out.length; i++) {
                out[i] = now[i] - last[i];
            }
            return out;
        }
    }
}
