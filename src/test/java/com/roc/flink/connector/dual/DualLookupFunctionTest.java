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
import java.util.HashMap;
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

    /**
     * 降级日志限流闸门：首条必打，之后按间隔采样，并如实回报「被合并的批数」。
     *
     * <p>时间线由测试直接构造（{@code visit} 的时刻入参），不依赖真实时钟，
     * 因此不会出现「机器慢/类加载导致偶发失败」的脆弱测试。
     */
    @Test
    public void failoverLogGateEmitsFirstThenThrottlesByInterval() {
        DualLookupFunction.FailoverLogGate gate = new DualLookupFunction.FailoverLogGate(10_000L);

        // 第 1 批：故障起点，必须打印，且此刻无被合并的批次
        assertEquals("首条降级日志必须打印（含真实原因），否则故障起点就丢了", 0L, gate.visit(1_000L));
        // 间隔内的第 2、3 批：抑制
        assertEquals(-1L, gate.visit(1_500L));
        assertEquals(-1L, gate.visit(2_000L));
        // 距上次打印满 10s：放行，并如实回报期间被合并了 2 批
        assertEquals("放行时应带上被合并的批数，数量信息不能丢", 2L, gate.visit(11_000L));
        // 第 5 批又落入新的间隔窗口：抑制
        assertEquals(-1L, gate.visit(11_500L));

        assertEquals("无论是否打印，总数都要精确累计（供恢复日志汇总）", 5L, gate.total());
    }

    @Test
    public void failoverLogGateWithZeroIntervalEmitsEveryTime() {
        // interval=0 表示不限流，退化为「每批一条」——即优化前的原始行为，必须仍然可用
        DualLookupFunction.FailoverLogGate gate = new DualLookupFunction.FailoverLogGate(0L);
        assertEquals(0L, gate.visit(1L));
        assertEquals(0L, gate.visit(2L));
        assertEquals(0L, gate.visit(3L));
        assertEquals(3L, gate.total());
    }

    @Test
    public void failoverLogGateResetStartsNewEpisode() {
        DualLookupFunction.FailoverLogGate gate = new DualLookupFunction.FailoverLogGate(10_000L);
        gate.visit(1_000L);
        gate.visit(1_100L); // 被抑制
        gate.reset();

        assertEquals(0L, gate.total());
        // 复位后「首条必打」规则重新生效：下一次降级必须打印，且合并计数从 0 开始
        assertEquals(0L, gate.visit(1_200L));
        assertEquals(1L, gate.total());
    }

    /**
     * 接线测试：确认限流闸门真的被 {@code doBatchLookup} 使用、且主源恢复后会被清零。
     *
     * <p>只测 {@code FailoverLogGate} 本身不足以证明它被调用了——这段代码一旦被误删，
     * 纯状态机测试仍会全绿，而线上又会退化成日志洪峰。这里通过反射读真实 Function 的状态，
     * 把「接线」也纳入回归防线。
     */
    @Test
    public void failoverGateIsWiredIntoRoutingAndClearedOnRecovery() throws Exception {
        RecordingReader primary = new RecordingReader();
        primary.failWith = new RuntimeException("primary down");
        RecordingReader standby = new RecordingReader(); // 备源正常返回空结果
        // batchSize=1 -> 每个 key 独立成批，便于精确控制「降级了多少批」
        DualLookupFunction fn = newFunction(primary, standby, 1, 1000);

        for (int i = 0; i < 5; i++) {
            fn.asyncLookup(key("k" + i)).get(3, TimeUnit.SECONDS);
        }

        java.lang.reflect.Field f = DualLookupFunction.class.getDeclaredField("failoverGate");
        f.setAccessible(true);
        DualLookupFunction.FailoverLogGate gate = (DualLookupFunction.FailoverLogGate) f.get(fn);
        assertNotNull("闸门应在 ensureOpened 里初始化", gate);
        assertEquals("5 批降级都应被计数（限流只影响打印，不影响计数）", 5L, gate.total());
        assertEquals("限流不应影响降级本身：5 批都应真的查了备源", 5, standby.calls.get());

        // 主源恢复：降级计数必须清零，故障周期结束
        primary.failWith = null;
        fn.asyncLookup(key("ok")).get(3, TimeUnit.SECONDS);
        assertEquals("主源恢复后应清零，否则下一次故障的首条日志会被误判为「间隔内」而抑制",
                0L, gate.total());
        fn.close();
    }

    /**
     * 接线测试：确认「各源平均耗时」的埋点真的接到了路由链路上。
     *
     * <p>只测 {@link LookupStats} 本身证明不了 {@code DualLookupFunction} 传了正确的耗时——
     * 埋点一旦被误删、或图省事改成传 0，统计类的测试仍会全绿，而线上再也看不出「哪个源变慢了」。
     * 因此这里跑真实路由，再反射取出统计对象检查它的阶段快照。
     */
    @Test
    public void latencyIsRecordedForEachSource() throws Exception {
        RecordingReader primary = new RecordingReader();
        RecordingReader standby = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, standby, 1, 1000); // batchSize=1 -> 每条立即成批

        fn.asyncLookup(key("a")).get(3, TimeUnit.SECONDS);
        String line1 = statsOf(fn).snapshotLine();
        assertTrue("主源成功的一批必须留下耗时样本: " + line1,
                line1.contains("hbase[batches=1 keys=1 fail=0 avg="));
        assertTrue("未降级时备源不应有任何样本: " + line1,
                line1.contains("doris[batches=0 fail=0 avg=- max=- hit=-]"));

        primary.failWith = new RuntimeException("primary down");
        fn.asyncLookup(key("b")).get(3, TimeUnit.SECONDS); // 主源失败 -> 整批降级
        String line2 = statsOf(fn).snapshotLine();
        assertTrue("失败批次也要计入主源耗时，否则主源恶化过程会被掩盖: " + line2,
                line2.contains("hbase[batches=1 keys=1 fail=1(other=1) avg="));
        assertTrue("降级后备源必须留下耗时样本: " + line2,
                line2.contains("doris[batches=1 fail=0 avg="));
        assertTrue("降级次数应同步: " + line2, line2.contains("failover=1"));
        fn.close();
    }

    /**
     * 接线测试：确认命中率统计真的接在分发链路上。
     *
     * <p>只测 {@link LookupStats} 本身证明不了 {@code distribute()} 报了正确的命中数——
     * 一旦有人删掉那行统计、或把分母写成别的数，统计类的测试仍然全绿，
     * 而「维表大面积查不到」这条最危险的故障（表被误删、rowkey 编码写错、备源同步延迟）
     * 又会重新隐身。因此这里跑真实路由，再反射取出统计对象检查命中率。
     */
    @Test
    public void hitRateIsRecordedFromDistribution() throws Exception {
        RecordingReader primary = new RecordingReader();
        DualLookupFunction fn = newFunction(primary, new RecordingReader(), 3, 1000);

        // 一批 3 个 key，只有第 1 个（下标 0）查到行 -> 命中 1/3
        Map<Integer, List<Map<String, Object>>> oneHit = new HashMap<>();
        oneHit.put(0, Collections.singletonList(newResultRow("a")));
        primary.result = oneHit;

        fn.asyncLookup(key("a"));
        fn.asyncLookup(key("b"));
        fn.asyncLookup(key("c")).get(3, TimeUnit.SECONDS); // 攒满 3 条触发整批查询

        String line = statsOf(fn).snapshotLine();
        assertTrue("命中率必须来自 distribute 的实际命中数（1/3）: " + line, line.contains("hit=33.3%]"));
        fn.close();
    }

    /** 构造一行维表结果（DDL 只声明了主键列 pk，其余无需给出） */
    private static Map<String, Object> newResultRow(String pk) {
        Map<String, Object> row = new HashMap<>();
        row.put("pk", pk);
        return row;
    }

    /** 反射取出 Function 内部的统计对象（它是 transient 运行期状态，没有对外 getter） */
    private static LookupStats statsOf(DualLookupFunction fn) throws Exception {
        java.lang.reflect.Field f = DualLookupFunction.class.getDeclaredField("stats");
        f.setAccessible(true);
        return (LookupStats) f.get(fn);
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
        /** 命中结果：默认空（全部未命中），测试可改写它以驱动命中率统计 */
        volatile Map<Integer, List<Map<String, Object>>> result = Collections.emptyMap();

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
            return CompletableFuture.completedFuture(result);
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
