package com.roc.flink.connector.dual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 运行期统计：主/备源的查询批次数、key 数、失败数、平均读取耗时、降级次数，定时打一行日志。
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
 *   <li>后半段 {@code since-start}：<b>自启动累计</b>，一眼看到全程总量与全程平均耗时，
 *       不必等作业关闭或去别处翻汇总。</li>
 * </ul>
 *
 * <p>实现上内部计数<b>始终是单调递增的累计值</b>（因此中途漏看某一行也不会丢数），
 * 阶段值靠「当前累计 − 上次快照」换算，快照字段见 {@link #lastSample}。
 * 两段共用同一份 {@link Sample}，保证两个视角来自同一时刻，日志内部不会自相矛盾。
 *
 * <p><b>{@code since-start} 的准确含义是「本统计实例创建以来」</b>：TaskManager 进程重启
 * （作业故障恢复、扩缩容、升级）后计数从 0 重新开始，不等于作业生命周期总量。
 *
 * <p>线程安全：所有累计计数都是 {@link AtomicLong}，因为埋点来自攒批线程、源侧回调线程
 * 等多个并发来源，且读（打点）写（埋点）同时发生。
 * 而 {@link #lastSample} 只在打点线程里读写（{@link #logSnapshot} 由单线程调度器串行调用），
 * 不需要额外的同步。
 */
public class LookupStats {

    private static final Logger LOG = LoggerFactory.getLogger(LookupStats.class);

    /** 统计日志统一前缀，便于在 TM 日志里直接 grep（降级/恢复日志共用同一前缀） */
    private static final String PREFIX = "[dual-lookup] ";

    /** 毫秒转纳秒的除数，用于把累计耗时换算成「毫秒/批」 */
    private static final double NANOS_PER_MS = 1_000_000.0;

    /** 所属维表名，仅用于日志标识（同一作业可能有多张 dual-lookup 维表） */
    private final String tableName;
    /** 主源名（hbase/doris），仅用于日志展示 */
    private final String primary;
    /** 备源名（hbase/doris），仅用于日志展示 */
    private final String standby;
    /** 打点间隔（秒），<=0 表示关闭打点 */
    private final int logIntervalSec;

    // ==================== 累计计数（埋点侧并发写，只增不减） ====================

    /**
     * 提交给主源的 key 总数。
     *
     * <p>注意口径：降级时同一批 key 不会重复计入——备源查询是同一批数据的重试，
     * 若也算进来，「处理量」会被降级频率虚高。
     */
    private final AtomicLong totalKeys = new AtomicLong();
    /** 主源执行的批次数（含失败的批次） */
    private final AtomicLong primaryBatches = new AtomicLong();
    /** 主源处理的 key 数（用于算平均批大小） */
    private final AtomicLong primaryKeys = new AtomicLong();
    /** 主源失败的批次数 */
    private final AtomicLong primaryFails = new AtomicLong();
    /** 主源累计查询耗时（纳秒），除以批次数即为平均读取耗时 */
    private final AtomicLong primaryNanos = new AtomicLong();
    /** 备源执行的批次数（正常情况下应为 0） */
    private final AtomicLong standbyBatches = new AtomicLong();
    /** 备源失败的批次数（>0 说明两源都异常，需要立即介入） */
    private final AtomicLong standbyFails = new AtomicLong();
    /** 备源累计查询耗时（纳秒） */
    private final AtomicLong standbyNanos = new AtomicLong();
    /** 降级次数：一次「主源失败 -> 改查备源」记 1 */
    private final AtomicLong failoverCount = new AtomicLong();

    // ==================== 上一次打点的快照（只有打点线程读写） ====================
    // 阶段值 = 累计值 − 快照值。用「相减」而不是「清零」的原因是：
    // 若中途漏打一行（例如 INFO 级别被调高过滤掉），清零会把这批数据永久丢掉，
    // 而相减只会把两个区间合并到下一次输出里，数据仍然守恒。

    /** 上次打点时的累计计数；{@link Sample#EMPTY} 表示还没打过点 */
    private Sample lastSample = Sample.EMPTY;
    /** 上次打点时刻（毫秒），用于给出真实的采样窗口长度（调度器可能被负载拖延） */
    private long lastSnapshotMs = 0L;
    /** 打点任务自身出错时只告警一次，避免每 6 分钟重复刷一条同样的 WARN */
    private boolean logFailureReported = false;

    /** 定时打点线程池；transient，因为线程池本身不可序列化，且只在当前 TM 内有效 */
    private transient ScheduledExecutorService scheduler;

    public LookupStats(String tableName, String primary, String standby, int logIntervalSec) {
        this.tableName = tableName;
        this.primary = primary;
        this.standby = standby;
        this.logIntervalSec = logIntervalSec;
    }

    /** 启动定时打点；幂等（已启动则不再重复创建线程）；interval<=0 时静默跳过 */
    public void start() {
        if (logIntervalSec > 0 && scheduler == null) {
            lastSnapshotMs = System.currentTimeMillis();
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
     *
     * <p>为什么还需要这条收尾行：作业停止前最后几分钟的统计仍留在计数里，
     * 若不打出来，最后一次打点到关闭之间的查询量就看不到了。
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
        if (cur.sameAs(Sample.EMPTY) || cur.sameAs(lastSample)) {
            return;
        }
        LOG.info(totalLine());
    }

    // ---------------- 埋点 ----------------

    /**
     * 主源完成一批查询（无论成败都记）。
     *
     * <p>为什么在这里同时记「批大小」和「耗时」？因为两者都只在批次真正结束时才拿得到，
     * 合并成一次调用可以保证「批次数」与「耗时样本数」严格一一对应——
     * 平均耗时的分母直接复用批次数（见 {@link #avgMs}），不会出现样本与批次错位。
     *
     * @param keys 本批 key 数
     * @param nanos 本批查询耗时（纳秒）
     */
    public void recordPrimaryBatch(int keys, long nanos) {
        primaryBatches.incrementAndGet();
        primaryKeys.addAndGet(keys);
        totalKeys.addAndGet(keys);
        primaryNanos.addAndGet(nanos);
    }

    /** 主源一批失败（超时/异常），即将触发降级 */
    public void recordPrimaryFail() {
        primaryFails.incrementAndGet();
    }

    /**
     * 备源完成一批查询（含正常降级与主源失败两种情形）。
     *
     * <p>key 数不重复累加：这批 key 在主源尝试时已计入 {@link #totalKeys}。
     *
     * @param nanos 本批查询耗时（纳秒）
     */
    public void recordStandbyBatch(long nanos) {
        standbyBatches.incrementAndGet();
        standbyNanos.addAndGet(nanos);
    }

    /** 备源一批也失败：两源同时异常 */
    public void recordStandbyFail() {
        standbyFails.incrementAndGet();
    }

    /** 发生一次降级（主源失败后改查备源） */
    public void recordFailover() {
        failoverCount.incrementAndGet();
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
     * 这里每 {@code logIntervalSec} 秒仍会被调度一次，提前返回可以省掉十几次原子读与字符串拼接。
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
     * @return 形如
     *         {@code [dual-lookup] table=dim_account window=360s totalKeys=5400 ... failover=2 || table=dim_account since-start totalKeys=... }
     *         的单行文本
     */
    String snapshotLine() {
        long now = System.currentTimeMillis();
        // 首行：窗口按配置间隔算；之后按真实间隔算（调度器可能被负载拖延，真实值更有诊断价值）
        long windowSec = lastSnapshotMs == 0L
                ? logIntervalSec
                : Math.max(1L, (now - lastSnapshotMs) / 1000L);
        lastSnapshotMs = now;

        Sample cur = readCounters();
        Sample stage = cur.minus(lastSample);
        lastSample = cur;

        return PREFIX + format("window=" + windowSec + "s", stage)
                + " || " + format("since-start", cur);
    }

    /**
     * 组装一行「自启动以来」的累计快照；仅在 {@link #close()} 收尾时输出。
     *
     * <p>与 {@link #snapshotLine()} 后半段同格式，便于直接对比。
     *
     * @return 累计视角的单行文本
     */
    String totalLine() {
        return PREFIX + format("since-start", readCounters());
    }

    /** 把 9 个累计计数读成一份快照（同一时刻），保证两个视角的数字对齐 */
    private Sample readCounters() {
        return new Sample(
                totalKeys.get(),
                primaryBatches.get(), primaryKeys.get(), primaryFails.get(), primaryNanos.get(),
                standbyBatches.get(), standbyFails.get(), standbyNanos.get(),
                failoverCount.get());
    }

    /**
     * 把一份计数快照拼成一段文本。
     *
     * <p>平均批大小的分母里有讲究：{@code avgBatch} 刻意只用主源批次数——降级时同一批 key
     * 会被主源和备源各查一次，若把备源也算进分母，totalKeys 只加一次而批次加了两次，
     * avgBatch 会被系统性低估，从而把「降级频繁」误读成「攒批没生效」，导致无效调参。
     * 另给一个 {@code avgBatchWithStandby} 作为对照，两者差距越大说明降级越频繁。
     *
     * @param scope 视角标识：{@code window=360s} 或 {@code since-start}
     * @param s     该视角下的计数
     */
    private String format(String scope, Sample s) {
        long avgBatch = s.pBatches == 0L ? 0L : s.pKeys / s.pBatches;
        long avgBatchWithStandby = (s.pBatches + s.sBatches) == 0L ? 0L : s.totalKeys / (s.pBatches + s.sBatches);

        return String.format(
                "table=%s %s totalKeys=%d avgBatch=%d avgBatchWithStandby=%d | "
                        + "%s[batches=%d keys=%d fail=%d avg=%s] %s[batches=%d fail=%d avg=%s] failover=%d",
                tableName, scope, s.totalKeys, avgBatch, avgBatchWithStandby,
                primary, s.pBatches, s.pKeys, s.pFails, avgMs(s.pNanos, s.pBatches),
                standby, s.sBatches, s.sFails, avgMs(s.sNanos, s.sBatches), s.failover);
    }

    /**
     * 平均单批耗时，形如 {@code 12.3ms}；无样本时返回 {@code -}。
     *
     * <p>分母直接用批次数：{@link #recordPrimaryBatch} / {@link #recordStandbyBatch}
     * 保证「每完成一批就记一次耗时」，因此样本数与批次数恒等，无需额外维护计数器。
     *
     * @param totalNanos 累计耗时（纳秒）
     * @param batches    批次数
     */
    private static String avgMs(long totalNanos, long batches) {
        if (batches <= 0L) {
            return "-";
        }
        return String.format("%.1fms", totalNanos / NANOS_PER_MS / batches);
    }

    /**
     * 某一时刻的计数快照（不可变）。
     *
     * <p>为什么要把 9 个计数先读成一个对象？因为每行日志要同时给出「本阶段」与「自启动」两个视角，
     * 若两段各自去读原子变量，就可能读到两个不同的瞬间（例如 since-start 比 stage 少算了 3 批），
     * 日志内部就自相矛盾了——看日志的人只会怀疑统计不准，然后去查一个并不存在的 bug。
     * 读成一份快照后，两个视角天然对齐，且 stage + 此前累计 == since-start 恒成立。
     *
     * <p>为什么阶段值用「相减」而不是「读完后清零」：清零一旦遇到漏打（INFO 被过滤、打点线程
     * 被打断），那一段数据就永久丢了；相减最多把两个区间合并到下一次输出，总量仍然守恒。
     */
    private static final class Sample {

        static final Sample EMPTY = new Sample(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        final long totalKeys;
        final long pBatches;
        final long pKeys;
        final long pFails;
        final long pNanos;
        final long sBatches;
        final long sFails;
        final long sNanos;
        final long failover;

        Sample(long totalKeys, long pBatches, long pKeys, long pFails, long pNanos,
               long sBatches, long sFails, long sNanos, long failover) {
            this.totalKeys = totalKeys;
            this.pBatches = pBatches;
            this.pKeys = pKeys;
            this.pFails = pFails;
            this.pNanos = pNanos;
            this.sBatches = sBatches;
            this.sFails = sFails;
            this.sNanos = sNanos;
            this.failover = failover;
        }

        /** 本快照相对 {@code base} 的增量（阶段值 = 当前累计 − 上次快照） */
        Sample minus(Sample base) {
            return new Sample(
                    totalKeys - base.totalKeys,
                    pBatches - base.pBatches, pKeys - base.pKeys, pFails - base.pFails, pNanos - base.pNanos,
                    sBatches - base.sBatches, sFails - base.sFails, sNanos - base.sNanos,
                    failover - base.failover);
        }

        /** 是否与另一份快照完全一致（用于 close 时判断「自上次打点后有没有新活动」） */
        boolean sameAs(Sample o) {
            return totalKeys == o.totalKeys
                    && pBatches == o.pBatches && pKeys == o.pKeys && pFails == o.pFails && pNanos == o.pNanos
                    && sBatches == o.sBatches && sFails == o.sFails && sNanos == o.sNanos
                    && failover == o.failover;
        }
    }
}
