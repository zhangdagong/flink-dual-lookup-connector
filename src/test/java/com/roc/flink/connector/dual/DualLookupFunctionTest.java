package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 攒批与主备路由的测试。用假的 LookupReader 记录「每次批次的大小与触发时刻」，
 * 从而可以断言攒批的时序行为（这是最容易出并发 bug 的地方）。
 */
public class DualLookupFunctionTest {

    private static RowData key(String v) {
        return GenericRowData.of(StringData.fromString(v));
    }

    private static DualLookupFunction newFunction(LookupReader primary, LookupReader standby,
                                                  int batchSize, int maxWaitMs) {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setInteger(DualLookupOptions.BATCH_SIZE.key(), batchSize);
        c.setInteger(DualLookupOptions.BATCH_MAX_WAIT.key(), maxWaitMs);
        // 业务超时必须显著大于攒批等待（交叉校验会拒绝 maxWait >= timeout 的组合）
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), maxWaitMs + 3000);
        c.setInteger(DualLookupOptions.STATS_LOG_INTERVAL.key(), 0); // 关掉统计线程，让测试无额外线程
        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(c, "dim_account");
        return new DualLookupFunction(cfg, "dim_account",
                new RowDataConverter(new String[]{"pk"}, new LogicalType[]{new VarCharType()}),
                new LogicalType[]{new VarCharType()},
                primary, standby);
    }

    @Test
    public void flushWhenBatchReachesSize() throws Exception {
        RecordingReader primary = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, new RecordingReader(), 100, 1000);
        for (int i = 0; i < 100; i++) {
            fn.asyncLookup(key("k" + i));
        }
        assertEquals(1, primary.batchSizes.size());
        assertEquals(Integer.valueOf(100), primary.batchSizes.get(0));
        fn.close();
    }

    @Test
    public void flushByTimerWhenBatchNotFull() throws Exception {
        RecordingReader primary = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, new RecordingReader(), 100, 50);
        for (int i = 0; i < 3; i++) {
            fn.asyncLookup(key("k" + i));
        }
        assertTrue("未攒满时不应立刻发起查询", primary.batchSizes.isEmpty());
        Thread.sleep(400);
        assertEquals("应被时间兜底触发一次", 1, primary.batchSizes.size());
        assertEquals(Integer.valueOf(3), primary.batchSizes.get(0));
        fn.close();
    }

    /**
     * 回归测试：攒满即发之后，残留的定时任务不得把下一批提前冲掉。
     *
     * <p>时序（maxWait=1000ms）：
     * <pre>
     *   t=0    发 1 个 key        -> 安排 T1 ≈ t+1000ms
     *   t=500  连发 100 个 key    -> 攒满即发（批次1），并安排 T2 ≈ t+1500ms
     *   t=700  再发 30 个 key     -> 缓冲 31 条，等各自的定时器
     * </pre>
     * 批次2 只能在 ≈1500ms 被 T2 触发；若在 ≈1000ms 就被冲掉，说明 T1 没被取消。
     * 用「两批次的时间差」判定，可规避 JVM 启动/类加载造成的绝对时间偏移。
     */
    @Test
    public void staleTimerDoesNotFlushNextBatch() throws Exception {
        RecordingReader primary = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, new RecordingReader(), 100, 1000);

        fn.asyncLookup(key("first"));      // 安排 T1 ≈ 1000ms
        Thread.sleep(500);
        for (int i = 0; i < 100; i++) {
            fn.asyncLookup(key("fill" + i)); // 攒满 -> 批次1，并安排 T2 ≈ 1500ms
        }
        Thread.sleep(200);
        for (int i = 0; i < 30; i++) {
            fn.asyncLookup(key("rest" + i)); // 缓冲 31 条
        }
        Thread.sleep(1300);

        assertEquals(2, primary.batchSizes.size());
        assertEquals(Integer.valueOf(100), primary.batchSizes.get(0));
        assertEquals(Integer.valueOf(31), primary.batchSizes.get(1));
        long gap = primary.times.get(1) - primary.times.get(0);
        assertTrue("批次2 距批次1 应约 1000ms（T2 的起算点晚 500ms），实际 " + gap
                + "ms——偏小说明残留的定时任务把新批次提前冲掉了", gap > 800);
        fn.close();
    }

    @Test
    public void failoverToStandbyWhenPrimaryFails() throws Exception {
        RecordingReader primary = new RecordingReader();
        primary.failWith = new RuntimeException("primary down");
        RecordingReader standby = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, standby, 2, 1000);

        CompletableFuture<Collection<RowData>> f1 = fn.asyncLookup(key("a"));
        CompletableFuture<Collection<RowData>> f2 = fn.asyncLookup(key("b")); // 攒满 -> 触发
        f1.get(3, TimeUnit.SECONDS);
        f2.get(3, TimeUnit.SECONDS);

        assertEquals(1, primary.batchSizes.size());
        assertEquals("主源失败后应整批改查备源", 1, standby.batchSizes.size());
        assertEquals(Integer.valueOf(2), standby.batchSizes.get(0));
        fn.close();
    }

    /**
     * 核心语义回归：主源「查询成功但没查到」绝不能触发降级。
     *
     * <p>维表场景下「HBase 里没有这个主体的数据」是大概率事件，若误判为故障，
     * 备源 Doris 会被打满无谓的重复查询，且 lookup_source 元字段也会错乱。
     */
    @Test
    public void emptyResultDoesNotFailover() throws Exception {
        RecordingReader primary = new RecordingReader(); // 默认返回空 Map：查过了、没查到
        RecordingReader standby = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, standby, 2, 1000);

        CompletableFuture<Collection<RowData>> f1 = fn.asyncLookup(key("a"));
        CompletableFuture<Collection<RowData>> f2 = fn.asyncLookup(key("b")); // 攒满 -> 触发

        // 每个 key 的 Future 都必须正常完成（空集合），而不是异常或挂起
        assertTrue("查不到应以空结果正常完成", f1.get(3, TimeUnit.SECONDS).isEmpty());
        assertTrue(f2.get(3, TimeUnit.SECONDS).isEmpty());

        assertEquals(1, primary.batchSizes.size());
        assertEquals("查不到数据是正常业务结果，绝不能触发降级去查备源",
                0, standby.batchSizes.size());
        fn.close();
    }

    @Test
    public void bothSourcesFailedCompleteExceptionally() throws Exception {
        RecordingReader primary = new RecordingReader();
        primary.failWith = new RuntimeException("primary down");
        RecordingReader standby = new RecordingReader();
        standby.failWith = new RuntimeException("standby down");
        DualLookupFunction fn = newFunction(primary, standby, 2, 1000);

        CompletableFuture<Collection<RowData>> f1 = fn.asyncLookup(key("a"));
        CompletableFuture<Collection<RowData>> f2 = fn.asyncLookup(key("b"));

        for (CompletableFuture<Collection<RowData>> f : new CompletableFuture[]{f1, f2}) {
            try {
                f.get(3, TimeUnit.SECONDS);
                fail("两源都失败时必须让 Future 异常完成，绝不能补空结果把故障伪装成「查不到」");
            } catch (ExecutionException e) {
                assertNotNull(e.getCause());
            }
        }
        fn.close();
    }

    @Test
    public void closeCompletesPendingFutures() throws Exception {
        DualLookupFunction fn = newFunction(new RecordingReader(), new RecordingReader(), 100, 1000);
        CompletableFuture<Collection<RowData>> f = fn.asyncLookup(key("k"));
        assertFalse(f.isDone());
        fn.close();
        assertTrue("关闭时必须把缓冲区里的请求补空，否则异步算子会一直等一个永不完成的 Future",
                f.isDone());
        assertTrue(f.get(1, TimeUnit.SECONDS).isEmpty());
    }

    /**
     * 回归测试：close() 之后主源的迟到失败触发降级链时，绝不能重新 open() 备源。
     *
     * <p>场景：批次已提交给主源（Future 挂起，模拟慢查询），函数随后被关闭；
     * 主源的失败结果（如超时、连接中断）在 close 之后才到达，降级链走到 call(standby)。
     * 若此时重新 open() 备源，新建出的连接/线程池将没有任何人负责关闭——资源泄漏。
     */
    @Test
    public void inFlightFailoverAfterCloseDoesNotReconnect() throws Exception {
        PendingReader primary = new PendingReader();
        PendingReader standby = new PendingReader();
        DualLookupFunction fn = newFunction(primary, standby, 1, 1000);

        CompletableFuture<Collection<RowData>> f = fn.asyncLookup(key("a")); // batchSize=1 -> 立即 flush
        // 等主源真正被调用（其 Future 挂起，查询「在途」）
        for (int i = 0; i < 100 && primary.calls.get() == 0; i++) {
            Thread.sleep(10);
        }
        assertEquals(1, primary.calls.get());
        assertEquals(1, primary.openCalls.get()); // 惰性建连已发生

        fn.close(); // 函数关闭，两个 reader 都被 close

        // 主源的失败此刻才姗姗来迟，降级链被触发
        primary.lastFuture.completeExceptionally(new RuntimeException("late failure after close"));

        try {
            f.get(3, TimeUnit.SECONDS);
            fail("close 后的迟到失败应让 Future 异常结束，绝不能静默补空");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
        // 关键断言：降级链不得重新建连、不得把查询发给备源
        assertEquals("close 后降级链重新 open() 备源会造成资源泄漏（新建资源无人关闭）",
                0, standby.openCalls.get());
        assertEquals(0, standby.calls.get());
    }

    /** 假 Reader：batchLookupAsync 返回一个由测试手动控制的 Future，模拟「查询在途」 */
    static class PendingReader implements LookupReader {

        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger openCalls = new AtomicInteger();
        volatile CompletableFuture<Map<Integer, List<Map<String, Object>>>> lastFuture;

        @Override
        public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
            calls.incrementAndGet();
            lastFuture = new CompletableFuture<>();
            return lastFuture;
        }

        @Override
        public void open() {
            openCalls.incrementAndGet();
        }

        @Override
        public boolean isOpened() {
            return openCalls.get() > 0;
        }

        @Override
        public void close() {
        }
    }

    /** 假 Reader：记录每次批次的大小与触发时刻，可配置为「必定失败」 */
    static class RecordingReader implements LookupReader {

        final long t0 = System.currentTimeMillis();
        final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<Integer>());
        final List<Long> times = Collections.synchronizedList(new ArrayList<Long>());
        final AtomicInteger calls = new AtomicInteger();
        volatile Throwable failWith;

        @Override
        public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
            calls.incrementAndGet();
            batchSizes.add(keys.size());
            times.add(System.currentTimeMillis() - t0);
            if (failWith != null) {
                CompletableFuture<Map<Integer, List<Map<String, Object>>>> f = new CompletableFuture<>();
                f.completeExceptionally(failWith);
                return f;
            }
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        @Override
        public void open() {
        }

        @Override
        public boolean isOpened() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
