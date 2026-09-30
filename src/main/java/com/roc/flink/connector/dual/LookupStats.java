package com.roc.flink.connector.dual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 运行期统计：主/备源的查询批次数、key 数、失败数、降级次数，定时打一行日志。
 *
 * <p>用定时日志而不是 Flink MetricGroup：异步 Lookup Function 不是 RichFunction，
 * 拿不到 RuntimeContext，日志在巡检场景也更直观。
 *
 * <p>线程安全：所有计数都是 {@link AtomicLong}，因为埋点来自攒批线程、超时调度线程、
 * HBase Netty 回调线程等多个并发来源，且读（日志快照）写（埋点）同时发生。
 *
 * <p>状态是单调递增的累计值（非窗口值），两次日志相减即可算出区间指标，
 * 这样即使漏看几行日志也能通过求差还原因果。
 */
public class LookupStats {

    private static final Logger LOG = LoggerFactory.getLogger(LookupStats.class);

    /** 所属维表名，仅用于日志标识（同一作业可能有多张 dual-lookup 维表） */
    private final String tableName;
    /** 主源名（hbase/doris），仅用于日志展示 */
    private final String primary;
    /** 备源名（hbase/doris），仅用于日志展示 */
    private final String standby;
    /** 打点间隔（秒），<=0 表示关闭打点 */
    private final int logIntervalSec;

    /** 累计处理的总 key 数（主源 + 备源） */
    private final AtomicLong totalKeys = new AtomicLong();
    /** 主源执行的批次数 */
    private final AtomicLong primaryBatches = new AtomicLong();
    /** 主源处理的 key 数（用于算平均批大小） */
    private final AtomicLong primaryKeys = new AtomicLong();
    /** 主源失败的批次数 */
    private final AtomicLong primaryFails = new AtomicLong();
    /** 备源执行的批次数（正常情况下应为 0） */
    private final AtomicLong standbyBatches = new AtomicLong();
    /** 备源失败的批次数（>0 说明两源都异常，需要立即介入） */
    private final AtomicLong standbyFails = new AtomicLong();
    /** 降级次数：一次「主源失败 -> 改查备源」记 1 */
    private final AtomicLong failoverCount = new AtomicLong();

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
            // 用守护线程：作业异常退出时不至于因为该线程而卡住 JVM 关闭
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dual-lookup-stats");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(this::logSnapshot, logIntervalSec, logIntervalSec, TimeUnit.SECONDS);
        }
    }

    /** 停止打点；置空引用以便重复调用安全 */
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    // ---------------- 埋点 ----------------

    /** 主源发起一批查询（无论成败都记，含 key 数） */
    public void recordPrimaryBatch(int keys) {
        primaryBatches.incrementAndGet();
        primaryKeys.addAndGet(keys);
        totalKeys.addAndGet(keys);
    }

    /** 主源一批失败（超时/异常），即将触发降级 */
    public void recordPrimaryFail() {
        primaryFails.incrementAndGet();
    }

    /** 备源发起一批查询（含正常降级与主源失败两种情形） */
    public void recordStandbyBatch() {
        standbyBatches.incrementAndGet();
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

    /** 打一行聚合快照；高频打点但未开启 INFO 日志时直接返回，避免无谓的原子读与字符串构造 */
    private void logSnapshot() {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        long pb = primaryBatches.get();
        long sb = standbyBatches.get();
        // 平均批大小：反映「攒批是否生效」，若长期接近 1，说明流量过小或 batch.size 配置过大。
        // 分母刻意只用主源批次数：降级时同一批 key 会被主源和备源各计一次，
        // 若把备源也算进分母，totalKeys 只加一次而批次加了两次，avgBatch 会被系统性低估，
        // 从而把「降级频繁」误读成「攒批没生效」，导致无效调参
        long avgBatch = pb == 0 ? 0 : primaryKeys.get() / pb;
        long avgBatchWithStandby = (pb + sb) == 0 ? 0 : totalKeys.get() / (pb + sb);
        LOG.info("[dual-lookup] table={} totalKeys={} avgBatch={} avgBatchWithStandby={} | "
                        + "{}[batches={} keys={} fail={}] {}[batches={} fail={}] failover={}",
                tableName, totalKeys.get(), avgBatch, avgBatchWithStandby,
                primary, pb, primaryKeys.get(), primaryFails.get(),
                standby, sb, standbyFails.get(), failoverCount.get());
    }
}
