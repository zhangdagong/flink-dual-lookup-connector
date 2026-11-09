package com.roc.flink.connector.dual;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Kafka 告警旁路的 Flink 指标。
 *
 * <p>指标只做观测，任何注册/自增失败都不能影响告警或查询；因此所有字段可空，
 * 注册异常时退化为 no-op。指标在 TaskManager 本地按子任务聚合，跨子任务汇总由
 * Flink MetricReporter 完成。
 */
final class AlertMetrics {

    static final AlertMetrics NOOP = new AlertMetrics(null, null, null, null, null, null, null, null);

    final Counter triggered;
    final Counter sent;
    final Counter failed;
    final Counter dropped;
    final Counter retries;
    final Counter suppressed;
    final Counter recovery;
    final Counter spooled;

    private AlertMetrics(Counter triggered, Counter sent, Counter failed, Counter dropped,
                         Counter retries, Counter suppressed, Counter recovery, Counter spooled) {
        this.triggered = triggered;
        this.sent = sent;
        this.failed = failed;
        this.dropped = dropped;
        this.retries = retries;
        this.suppressed = suppressed;
        this.recovery = recovery;
        this.spooled = spooled;
    }

    static AlertMetrics of(MetricGroup group) {
        if (group == null) {
            return NOOP;
        }
        try {
            return new AlertMetrics(
                    group.counter("dualLookupAlertTriggered"),
                    group.counter("dualLookupAlertSent"),
                    group.counter("dualLookupAlertFailed"),
                    group.counter("dualLookupAlertDropped"),
                    group.counter("dualLookupAlertRetries"),
                    group.counter("dualLookupAlertSuppressed"),
                    group.counter("dualLookupAlertRecovery"),
                    group.counter("dualLookupAlertSpooled"));
        } catch (Throwable ignored) {
            return NOOP;
        }
    }

    void triggered() {
        inc(triggered);
    }

    void sent() {
        inc(sent);
    }

    void failed() {
        inc(failed);
    }

    void dropped() {
        inc(dropped);
    }

    void retry() {
        inc(retries);
    }

    void suppressed() {
        inc(suppressed);
    }

    void recovery() {
        inc(recovery);
    }

    void spooled() {
        inc(spooled);
    }

    private static void inc(Counter counter) {
        if (counter != null) {
            counter.inc();
        }
    }
}
