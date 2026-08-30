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
 */
public class LookupStats {

    private static final Logger LOG = LoggerFactory.getLogger(LookupStats.class);

    private final String tableName;
    private final String primary;
    private final String standby;
    private final int logIntervalSec;

    private final AtomicLong totalKeys = new AtomicLong();
    private final AtomicLong primaryBatches = new AtomicLong();
    private final AtomicLong primaryKeys = new AtomicLong();
    private final AtomicLong primaryFails = new AtomicLong();
    private final AtomicLong standbyBatches = new AtomicLong();
    private final AtomicLong standbyFails = new AtomicLong();
    private final AtomicLong failoverCount = new AtomicLong();

    private transient ScheduledExecutorService scheduler;

    public LookupStats(String tableName, String primary, String standby, int logIntervalSec) {
        this.tableName = tableName;
        this.primary = primary;
        this.standby = standby;
        this.logIntervalSec = logIntervalSec;
    }

    public void start() {
        if (logIntervalSec > 0 && scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "dual-lookup-stats");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(this::logSnapshot, logIntervalSec, logIntervalSec, TimeUnit.SECONDS);
        }
    }

    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    // ---------------- 埋点 ----------------

    public void recordPrimaryBatch(int keys) {
        primaryBatches.incrementAndGet();
        primaryKeys.addAndGet(keys);
        totalKeys.addAndGet(keys);
    }

    public void recordPrimaryFail() {
        primaryFails.incrementAndGet();
    }

    public void recordStandbyBatch() {
        standbyBatches.incrementAndGet();
    }

    public void recordStandbyFail() {
        standbyFails.incrementAndGet();
    }

    public void recordFailover() {
        failoverCount.incrementAndGet();
    }

    // ---------------- 输出 ----------------

    private void logSnapshot() {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        long pb = primaryBatches.get();
        long sb = standbyBatches.get();
        long totalBatch = pb + sb;
        long avgBatch = totalBatch == 0 ? 0 : totalKeys.get() / totalBatch;
        LOG.info("[dual-lookup] table={} totalKeys={} avgBatch={} | "
                        + "{}[batches={} keys={} fail={}] {}[batches={} fail={}] failover={}",
                tableName, totalKeys.get(), avgBatch,
                primary, pb, primaryKeys.get(), primaryFails.get(),
                standby, sb, standbyFails.get(), failoverCount.get());
    }
}
