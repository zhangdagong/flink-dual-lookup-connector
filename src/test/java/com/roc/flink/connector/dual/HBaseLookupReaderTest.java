package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
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

    /**
     * 连接类异常识别：HBase 的报错经常被包在 {@code RetriesExhaustedException} /
     * {@code ExecutionException} 里，必须沿 cause 链判断，否则「连接断开」无法触发重连重试。
     */
    @Test
    public void detectsConnectionClosedInCauseChain() throws Exception {
        Method m = HBaseLookupReader.class.getDeclaredMethod("isConnectionClosed", Throwable.class);
        m.setAccessible(true);

        assertTrue((Boolean) m.invoke(null, new ConnectionClosedException("Connection closed")));
        assertTrue((Boolean) m.invoke(null, new RuntimeException(
                new IOException(new ConnectionClosedException("Connection closed")))));
        // 有些 HBase 版本只把原因拼在 message 里
        assertTrue((Boolean) m.invoke(null, new RuntimeException(
                "Call to bigdatadev07/158.2.100.32:16020 failed on local exception: "
                        + "org.apache.hadoop.hbase.exceptions.ConnectionClosedException: Connection closed")));
        // 普通业务异常不能被误判为连接断开（否则会无谓重连）
        assertFalse((Boolean) m.invoke(null, new RuntimeException("Table not found: dim_account")));
    }
}
