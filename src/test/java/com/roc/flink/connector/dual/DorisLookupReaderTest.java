package com.roc.flink.connector.dual;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Doris 读取器的测试。用伪造的 HikariDataSource / JDBC 对象替换真实 Doris，不依赖任何外部服务，
 * 因此可以在 mvn test 里直接跑。
 *
 * <p>覆盖曾经出过问题的点：
 * <ol>
 *   <li>批内同一主键出现多次时，每个入参下标都要拿到结果（曾静默丢数据）；</li>
 *   <li>超时后必须打断「还没拿到连接」的任务（曾因 stmtRef 为 null 而漏掉取消）；</li>
 *   <li>线程池队列满时必须把拒绝翻译成异常完成的 Future，而不是抛给调用方（否则作业会失败而不是降级）；</li>
 *   <li>byte[] / BigDecimal 主键必须按「内容」匹配结果行（曾用 toString 拼键，永远查不到）；</li>
 *   <li>已完成查询的超时任务必须被取消并出队（否则高 QPS 下调度器队列只增不减）；</li>
 *   <li>Doris 宕机时 open() 必须立刻返回（HikariCP 建池自检失败会硬睡 1 秒）。</li>
 * </ol>
 */
public class DorisLookupReaderTest {

    private final List<ExecutorService> pools = new ArrayList<>();
    private final ScheduledThreadPoolExecutor timeoutScheduler = newScheduler();

    private static ScheduledThreadPoolExecutor newScheduler() {
        ScheduledThreadPoolExecutor s = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "test-doris-timeout");
            t.setDaemon(true);
            return t;
        });
        // 与生产代码 open() 里的设置保持一致：取消即出队，测试才能断言队列被清空
        s.setRemoveOnCancelPolicy(true);
        return s;
    }

    @After
    public void tearDown() {
        for (ExecutorService p : pools) {
            p.shutdownNow();
        }
        timeoutScheduler.shutdownNow();
    }

    // ---------------- 用例 ----------------

    @Test
    public void duplicateKeysInBatchAllIndexesGetResult() throws Exception {
        FakeBackend backend = new FakeBackend();
        DorisLookupReader reader = newReader(backend, pool(2));

        // A 出现两次、B 一次、末尾一个空 key
        List<Object[]> keys = Arrays.asList(
                new Object[]{"A"}, new Object[]{"B"}, new Object[]{"A"}, new Object[]{null});

        Map<Integer, List<Map<String, Object>>> result =
                reader.batchLookupAsync(keys).get(5, TimeUnit.SECONDS);

        assertEquals("批内同一主键的每个下标都必须拿到结果（曾只保留最后一个下标）",
                new TreeSet<>(Arrays.asList(0, 2)), new TreeSet<>(result.keySet()));
        assertEquals(1, result.get(0).size());
        assertEquals(1, result.get(2).size());
        assertEquals("重复主键应只在 SQL 里出现一次", 2, countPlaceholders(backend.lastSql.get()));
        assertEquals(Arrays.asList("A", "B"), new ArrayList<>(backend.boundParams));
    }

    @Test
    public void queryTimesOutWhenBackendIsSlow() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.blockOnGetConnectionMs = 3000; // 连连接都还没拿到，此时 stmtRef 仍为 null
        DorisLookupReader reader = newReader(backend, pool(1));

        CompletableFuture<?> f = reader.batchLookupAsync(Collections.singletonList(new Object[]{"A"}));
        try {
            f.get(3, TimeUnit.SECONDS);
            fail("慢查询应当以 TimeoutException 结束");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }

        // 关键回归点：Statement 尚未创建时也必须取消任务，
        // 否则它会照常拿到连接、把慢查询发到 Doris 跑完，把连接池与线程池越拖越满
        for (int i = 0; i < 50 && !backend.interruptedOnGetConnection.get(); i++) {
            Thread.sleep(20);
        }
        assertTrue("超时后必须打断仍在等待连接的任务（stmtRef 为 null 的场景）",
                backend.interruptedOnGetConnection.get());
    }

    @Test
    public void rejectedExecutionFailsFutureInsteadOfThrowing() throws Exception {
        FakeBackend backend = new FakeBackend();
        backend.blockOnGetConnectionMs = 1000; // 占住唯一线程，让后续任务排队、再被拒
        ThreadPoolExecutor smallPool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
        pools.add(smallPool);
        DorisLookupReader reader = newReader(backend, smallPool);

        reader.batchLookupAsync(Collections.singletonList(new Object[]{"A"})); // 占住线程
        reader.batchLookupAsync(Collections.singletonList(new Object[]{"B"})); // 进队列

        CompletableFuture<?> rejected;
        try {
            rejected = reader.batchLookupAsync(Collections.singletonList(new Object[]{"C"})); // 队列满
        } catch (RejectedExecutionException e) {
            fail("拒绝必须被翻译成异常完成的 Future，不能抛给调用方——"
                    + "否则整个作业会失败，而不是降级到备源");
            return;
        }
        try {
            rejected.get(3, TimeUnit.SECONDS);
            fail("被拒绝的批次应以 RejectedExecutionException 结束");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RejectedExecutionException);
        }
    }

    @Test
    public void allKeysEmptySkipsBackend() throws Exception {
        FakeBackend backend = new FakeBackend();
        DorisLookupReader reader = newReader(backend, pool(1));

        Map<Integer, List<Map<String, Object>>> result = reader
                .batchLookupAsync(Collections.singletonList(new Object[]{null}))
                .get(2, TimeUnit.SECONDS);

        assertTrue(result.isEmpty());
        assertNull("整批无有效 key 时不应发起任何 SQL", backend.lastSql.get());
    }

    @Test
    public void singleKeyUsesInClause() throws Exception {
        FakeBackend backend = new FakeBackend();
        DorisLookupReader reader = newReader(backend, pool(1));
        reader.batchLookupAsync(Arrays.asList(new Object[]{"A"}, new Object[]{"B"}))
                .get(3, TimeUnit.SECONDS);
        assertEquals("SELECT `pk`, `party_name` FROM `dim`.`dim_account` WHERE `pk` IN (?,?)",
                backend.lastSql.get());
    }

    @Test
    public void binaryKeysMatchByContentNotIdentity() throws Exception {
        FakeBackend backend = new FakeBackend();
        // 结果行返回的是「另一个实例」：内容相同、引用不同。
        // 旧实现用 toString() 拼反查键，byte[] 得到的是 [B@对象哈希，
        // 两个实例哈希不同 -> 结果行匹配不上 -> 静默查不到（不降级、无日志）
        backend.pkValue = new byte[]{1, 2, 3};
        DorisLookupReader reader = newReader(backend, pool(1));

        Map<Integer, List<Map<String, Object>>> result = reader
                .batchLookupAsync(Collections.singletonList(new Object[]{new byte[]{1, 2, 3}}))
                .get(3, TimeUnit.SECONDS);

        assertTrue("byte[] 主键必须按内容匹配（Base64 规范化），而不是按对象哈希",
                result.containsKey(0));
    }

    @Test
    public void decimalKeysMatchIgnoringScale() throws Exception {
        FakeBackend backend = new FakeBackend();
        // Doris 侧 DECIMAL(10,2) 返回 123.00，入参是 123：
        // 旧实现 toString() 拼出 "123" 与 "123.00" 两个键 -> 静默查不到
        backend.pkValue = new BigDecimal("123.00");
        DorisLookupReader reader = newReader(backend, pool(1));

        Map<Integer, List<Map<String, Object>>> result = reader
                .batchLookupAsync(Collections.singletonList(new Object[]{new BigDecimal("123")}))
                .get(3, TimeUnit.SECONDS);

        assertTrue("DECIMAL 主键必须忽略 scale 匹配（123 与 123.00 是同一个键）",
                result.containsKey(0));
    }

    @Test
    public void completedQueriesCancelTheirTimeoutTasks() throws Exception {
        FakeBackend backend = new FakeBackend();
        DorisLookupReader reader = newReader(backend, pool(2));

        // 连续完成多批快速查询：每批都会往调度器放一个 500ms 后到期的超时任务
        int n = 20;
        for (int i = 0; i < n; i++) {
            reader.batchLookupAsync(Collections.singletonList(new Object[]{"A"}))
                    .get(3, TimeUnit.SECONDS);
        }
        // 查询早已完成，对应的超时任务必须已被取消并移出队列；
        // 否则它们要一直滞留到 500ms 到期，高 QPS 下队列只增不减
        for (int i = 0; i < 50 && !timeoutScheduler.getQueue().isEmpty(); i++) {
            Thread.sleep(20);
        }
        assertTrue("已完成的查询必须取消自己的超时任务（且取消即出队），队列应为空",
                timeoutScheduler.getQueue().isEmpty());
    }

    /**
     * 回归测试：Doris 不可达时，open() 必须立刻返回，绝不能卡住调用线程。
     *
     * <p>背景（实测值）：HikariCP 4.0.3 的 {@code HikariPool.checkFailFast()} 在首次建连失败后
     * <b>无条件</b> {@code quietlySleep(SECONDS.toMillis(1))}，而 {@code initializationFailTimeout}
     * 默认值是 1（= 要求建池时先抢一条连接）。于是每次失败的池构造都固定烧掉 1 秒
     * （实测 1002ms，而同样地址的裸 JDBC 只要 2~107ms）。这 1 秒是睡在调用 open() 的线程上的
     * ——也就是 Flink 的异步算子线程/攒批定时线程；Doris 当主源且宕机时每个批次都会重试 open()，
     * 每批固定多付 1 秒，远超 lookup.timeout(500ms)。
     *
     * <p>因此本用例用一个无人监听的端口触发「连接被拒绝」，断言 open() 立刻返回：
     * 建池不做连接尝试（{@code initializationFailTimeout=-1}），失败推迟到首次 getConnection，
     * 由 {@code doris.connect.timeout} 封顶。
     */
    @Test
    public void openDoesNotBlockWhenDorisIsUnreachable() throws Exception {
        Configuration c = new Configuration();
        // 127.0.0.1:19030 无人监听 -> 连接被拒绝。注意本用例不依赖外部服务，只依赖「被拒绝」
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://127.0.0.1:19030/dim?useSSL=false");
        c.setString(DualLookupOptions.DORIS_TABLE_NAME.key(), "dim.dim_account");
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 500);

        DorisLookupReader reader = new DorisLookupReader(
                DualLookupOptions.Config.from(c, "dim_account"), new String[]{"pk"}, new String[]{"pk"});
        long t0 = System.nanoTime();
        try {
            reader.open();
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            assertTrue("Doris 不可达时 open() 不得被建池自检阻塞（HikariCP 失败会硬睡 1 秒），实际 "
                    + ms + "ms", ms < 600);
            assertTrue("建池本身应成功：连接失败要留到首次 getConnection 时由 connectionTimeout 封顶",
                    reader.isOpened());
        } finally {
            reader.close(); // 池里有后台线程（housekeeper / connection adder），必须关掉
        }
    }

    // ---------------- 辅助 ----------------

    private ExecutorService pool(int n) {
        ExecutorService p = Executors.newFixedThreadPool(n);
        pools.add(p);
        return p;
    }

    private DorisLookupReader newReader(FakeBackend backend, ExecutorService pool) throws Exception {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setString(DualLookupOptions.DORIS_TABLE_NAME.key(), "dim.dim_account");
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 500);
        // 交叉校验要求：hbase.rpc.timeout(默认 300) < lookup.timeout，doris.connect.timeout 同理
        c.setInteger(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS.key(), 250);

        DorisLookupReader reader = new DorisLookupReader(
                DualLookupOptions.Config.from(c, "dim_account"),
                new String[]{"pk", "party_name"}, new String[]{"pk"});
        // 未调用 open()：这里直接注入运行期资源，避免真的去建连接池
        set(reader, "dataSource", backend.dataSource());
        set(reader, "queryExecutor", pool);
        set(reader, "timeoutScheduler", timeoutScheduler);
        return reader;
    }

    private static void set(Object target, String field, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static int countPlaceholders(String sql) {
        int n = 0;
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == '?') {
                n++;
            }
        }
        return n;
    }

    /** 伪造的 JDBC 后端：单行结果、可配置「取连接阻塞时长」 */
    static class FakeBackend {

        final AtomicReference<String> lastSql = new AtomicReference<>();
        final List<Object> boundParams = Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean interruptedOnGetConnection = new AtomicBoolean(false);
        volatile long blockOnGetConnectionMs = 0L;
        /** 结果行第一列（主键列）返回的值；Object 类型以便测试 byte[] / BigDecimal 主键 */
        volatile Object pkValue = "A";

        HikariDataSource dataSource() {
            return new HikariDataSource() {
                @Override
                public Connection getConnection() throws SQLException {
                    if (blockOnGetConnectionMs > 0) {
                        try {
                            Thread.sleep(blockOnGetConnectionMs);
                        } catch (InterruptedException e) {
                            interruptedOnGetConnection.set(true);
                            Thread.currentThread().interrupt();
                            throw new SQLException("interrupted while waiting for connection", e);
                        }
                    }
                    return (Connection) proxy(Connection.class, (p, m, a) -> {
                        if ("prepareStatement".equals(m.getName())) {
                            lastSql.set((String) a[0]);
                            return statement();
                        }
                        return null;
                    });
                }
            };
        }

        private PreparedStatement statement() {
            return (PreparedStatement) proxy(PreparedStatement.class, (p, m, a) -> {
                switch (m.getName()) {
                    case "executeQuery":
                        return resultSet();
                    case "setObject":
                        boundParams.add(a[1]);
                        return null;
                    default:
                        return null;
                }
            });
        }

        private ResultSet resultSet() {
            final int[] fetched = {0};
            return (ResultSet) proxy(ResultSet.class, (p, m, a) -> {
                switch (m.getName()) {
                    case "next":
                        return fetched[0]++ < 1; // 只返回一行
                    case "getObject":
                        return ((Integer) a[0]) == 1 ? pkValue : "某公司";
                    case "getMetaData":
                        return metaData();
                    default:
                        return null;
                }
            });
        }

        private ResultSetMetaData metaData() {
            return (ResultSetMetaData) proxy(ResultSetMetaData.class, (p, m, a) -> {
                switch (m.getName()) {
                    case "getColumnCount":
                        return 2;
                    case "getColumnLabel":
                        return ((Integer) a[0]) == 1 ? "pk" : "party_name";
                    default:
                        return null;
                }
            });
        }

        private static Object proxy(Class<?> iface, InvocationHandler handler) {
            return Proxy.newProxyInstance(FakeBackend.class.getClassLoader(),
                    new Class<?>[]{iface}, handler);
        }
    }
}
