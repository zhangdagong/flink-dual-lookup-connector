package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * HBase 读取器的测试。不建真实连接（withTimeout 只依赖调度器），聚焦超时任务的清理行为：
 * 查询在超时时间之前正常完成时，挂上的超时任务必须被取消并移出队列，
 * 否则高 QPS 下单线程调度器会被「早已用不到的任务」塞满，到期逐个空转扫描。
 */
public class HBaseLookupReaderTest {

    private static HBaseLookupReader newReader(ScheduledThreadPoolExecutor scheduler) throws Exception {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        HBaseLookupReader reader = new HBaseLookupReader(
                DualLookupOptions.Config.from(c, "dim_account"),
                new String[]{"c1"}, new LogicalType[]{new VarCharType()},
                new LogicalType[]{new VarCharType()});
        // 未调用 open()：直接注入调度器，避免真的去连 HBase
        Field f = HBaseLookupReader.class.getDeclaredField("timeoutScheduler");
        f.setAccessible(true);
        f.set(reader, scheduler);
        return reader;
    }

    @Test
    public void completedQueriesCancelTheirTimeoutTasks() throws Exception {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "test-hbase-timeout");
            t.setDaemon(true);
            return t;
        });
        // 与生产代码 open() 里的设置保持一致：取消即出队
        scheduler.setRemoveOnCancelPolicy(true);
        try {
            HBaseLookupReader reader = newReader(scheduler);
            Method m = HBaseLookupReader.class.getDeclaredMethod(
                    "withTimeout", CompletableFuture.class, long.class);
            m.setAccessible(true);

            // 模拟高 QPS：大量查询在超时时间（60s）之前就已正常完成
            int n = 50;
            for (int i = 0; i < n; i++) {
                CompletableFuture<String> query = new CompletableFuture<>();
                @SuppressWarnings("unchecked")
                CompletableFuture<String> wrapped =
                        (CompletableFuture<String>) m.invoke(reader, query, 60_000L);
                query.complete("ok");
                wrapped.get(1, TimeUnit.SECONDS);
            }
            for (int i = 0; i < 50 && !scheduler.getQueue().isEmpty(); i++) {
                Thread.sleep(20);
            }
            assertTrue("已完成查询的超时任务必须被取消并移出队列（取消即出队），队列应为空",
                    scheduler.getQueue().isEmpty());
        } finally {
            scheduler.shutdownNow();
        }
    }
}
