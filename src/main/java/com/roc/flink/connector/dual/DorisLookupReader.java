package com.roc.flink.connector.dual;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Doris 维表读取器（MySQL 协议 + HikariCP 连接池），批量查询。
 *
 * <p><b>为什么要攒批？</b>批量把 N 个 key 合成一次 {@code WHERE pk IN (?,?,...)}（复合主键用 OR 拼接），
 * 大幅减少与 Doris 的交互次数，是提升 Doris 查询效率的关键——逐条查的话，
 * 「网络往返 + SQL 解析 + 连接借用」的固定开销会占掉绝大部分耗时。
 *
 * <p><b>为什么不能像 HBase 那样纯异步？</b>JDBC 是阻塞 API，没有 Future 可用。
 * 因此这里用「业务线程池执行阻塞查询 + 独立调度器负责超时」来模拟异步语义，
 * 对外仍返回 CompletableFuture，从而与 HBase 实现保持同一份契约。
 *
 * <p><b>超时要真正中断后端：</b>靠 {@link Statement#cancel()} 让 Doris 侧真正中止 SQL，
 * 否则慢查询会在业务层已降级后继续占着连接，迅速打满连接池与线程池。
 */
public class DorisLookupReader implements LookupReader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisLookupReader.class);

    /** 全局配置（可序列化，随算子下发） */
    private final DualLookupOptions.Config cfg;
    /** 主键列名（Doris 侧拼 WHERE 条件用） */
    private final String[] keyColumnNames;
    /** 主键列名的小写副本：结果集列名统一按小写存放，反查时要用（构造期算好，避免每行重复 toLowerCase） */
    private final String[] keyColumnNamesLower;
    /** SQL 的固定前半段，如 {@code SELECT `c1`, `c2` FROM `dim`.`dim_account` WHERE }，构造时算好以省去重复拼接 */
    private final String selectPrefix;   // "SELECT `c1`,`c2` FROM `t` WHERE "

    // ---- 运行期资源句柄：不可序列化，open() 时重建 ----

    /** 是否已建连；volatile 保证多线程可见性 */
    private transient volatile boolean opened = false;
    /** HikariCP 连接池 */
    private transient HikariDataSource dataSource;
    /** 执行阻塞 JDBC 查询的业务线程池 */
    private transient ExecutorService queryExecutor;
    /** 负责到期强制超时的调度器（单线程，只做轻量标记动作）；
     * 用 ScheduledThreadPoolExecutor 而非 Executors 工厂方法：后者返回的是包装类，
     * 拿不到队列句柄，无法观测「已完成查询的超时任务是否被及时清理」 */
    private transient ScheduledThreadPoolExecutor timeoutScheduler;

    public DorisLookupReader(DualLookupOptions.Config cfg, String[] columnNames, String[] keyColumnNames) {
        this.cfg = cfg;
        this.keyColumnNames = keyColumnNames;
        this.keyColumnNamesLower = new String[keyColumnNames.length];
        for (int i = 0; i < keyColumnNames.length; i++) {
            this.keyColumnNamesLower[i] = keyColumnNames[i].toLowerCase();
        }
        // 提前生成 SELECT 前缀：列名与表名在运行期不变，拼一次即可复用
        this.selectPrefix = buildSelectPrefix(cfg.dorisTableName, columnNames);
    }

    /**
     * 生成 "SELECT `c1`,`c2` FROM `库`.`表` WHERE "（WHERE 条件按批量大小动态拼接）。
     *
     * <p>所有标识符都加反引号，避免列名/表名撞上 MySQL 关键字（如 {@code order}、{@code level}）。
     */
    private static String buildSelectPrefix(String table, String[] columns) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('`').append(columns[i]).append('`');
        }
        sb.append(" FROM ").append(quoteTable(table)).append(" WHERE ");
        return sb.toString();
    }

    /** 表名支持「库名.表名」：dim.dim_account → `dim`.`dim_account` */
    private static String quoteTable(String table) {
        int dot = table.indexOf('.');
        if (dot > 0 && dot < table.length() - 1) {
            return "`" + table.substring(0, dot) + "`.`" + table.substring(dot + 1) + "`";
        }
        return "`" + table + "`";
    }

    // ---------------- 生命周期 ----------------

    /** 建连接池 + 两个线程池。synchronized 保证多线程下只真正执行一次（幂等） */
    @Override
    public synchronized void open() throws Exception {
        if (opened) {
            return;
        }
        dataSource = createDataSource();

        // 线程数与连接池等大，保证拿到连接后不用排队等待
        // （若线程数 < 池大小，池里有空闲连接却没有线程去用，等于浪费；
        //   若线程数 > 池大小，多出的线程会阻塞在 getConnection 上，白占线程）
        //
        // 为什么不用 Executors.newFixedThreadPool？它内部是无界队列，等于没有背压：
        // Doris 变慢时每个任务都要等满 lookup.timeout 才「结束」（Future 早已异常完成，
        // 但任务本身还在跑），上游却仍在按 QPS 提交，队列只增不减。每个排队任务都持有一整批
        // 绑定参数，堆积既吃堆内存，又会在 Doris 恢复后集中涌去抢连接。
        // 这里改成有界队列 + AbortPolicy：容量取「池大小 × 2」，足以吸收瞬时波动，
        // 持续过载时则快速拒绝——拒绝会被翻译成「该源本次查询失败」，交给上层降级。
        queryExecutor = new ThreadPoolExecutor(
                cfg.dorisPoolSize, cfg.dorisPoolSize,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(1, cfg.dorisPoolSize * 2)),
                r -> {
                    Thread t = new Thread(r, "dual-lookup-doris-query");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.AbortPolicy());

        timeoutScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "dual-lookup-doris-timeout");
            t.setDaemon(true);
            return t;
        });
        // 取消即出队：默认策略下被取消的任务会滞留在队列里直到到期时刻，
        // 高 QPS 时「查询早已完成」的超时任务仍会堆积；开启后 cancel(false) 立即释放队列空间
        timeoutScheduler.setRemoveOnCancelPolicy(true);

        // 放最后：任一步骤失败时 opened 仍为 false，下次查询会重新尝试
        opened = true;
        LOG.info("[dual-lookup] Doris reader opened, url={}, table={}, poolSize={}",
                cfg.dorisJdbcUrl, cfg.dorisTableName, cfg.dorisPoolSize);
    }

    /** 把 DDL 参数映射成 HikariCP 配置；连接池参数的意义见 {@link DualLookupOptions} 中的注释 */
    private HikariDataSource createDataSource() {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("dual-lookup-doris");
        hc.setJdbcUrl(cfg.dorisJdbcUrl);
        hc.setUsername(cfg.dorisUsername);
        hc.setPassword(cfg.dorisPassword);
        // 驱动类名允许留空：留空时 HikariCP 会按 JDBC URL 自动推断
        if (cfg.dorisDriver != null && !cfg.dorisDriver.trim().isEmpty()) {
            hc.setDriverClassName(cfg.dorisDriver.trim());
        }
        hc.setMaximumPoolSize(cfg.dorisPoolSize);
        hc.setMinimumIdle(cfg.dorisPoolMinIdle);
        hc.setConnectionTimeout(cfg.dorisConnectTimeoutMs);
        // 关闭「构造池时先抢一条连接」的行为（HikariCP 默认 initializationFailTimeout=1 表示要抢）。
        // 为什么必须关掉？HikariCP 4.0.3 的 HikariPool.checkFailFast() 里在首次建连失败后是
        // <b>无条件</b> quietlySleep(SECONDS.toMillis(1)) 的，因此只要 Doris 不可达，
        // 每构造一次池就固定烧掉 1 秒（实测 1002ms；同样地址裸 JDBC 只要 2~107ms）。
        // 而这 1 秒是睡在「调用 open() 的那个线程」上的——也就是 Flink 的异步算子线程/攒批定时线程。
        // 后果：Doris 当主源且宕机时，每个批次都会重试 open()（失败后 opened 仍为 false），
        // 于是每批固定多付 1 秒，远超 lookup.timeout(默认 500ms)，降级形同虚设、算子吞吐被拖垮。
        //
        // 设为 -1 后：构造池不做任何连接尝试，连接推迟到首次 getConnection() 惰性创建，
        // 失败时的等待由 connectionTimeout（即 doris.connect.timeout，默认 300ms）封顶——
        // 这才与 DualLookupOptions 里对 doris.connect.timeout 的说明一致。
        // 附带好处：opened 会保持 true，不再每批重建池；Doris 恢复后连接池自行恢复，无需重新建连。
        // 代价：URL/账号配错不再在 open() 阶段暴露，而是首次查询时以「该源本次失败」的形式暴露，
        // 这与本连接器「失败即降级」的语义一致，也因此不会影响主备切换的正确性。
        hc.setInitializationFailTimeout(-1L);
        hc.setIdleTimeout(cfg.dorisPoolIdleTimeoutMs);
        hc.setMaxLifetime(cfg.dorisPoolMaxLifetimeMs);
        hc.setValidationTimeout(cfg.dorisPoolValidationTimeoutMs);
        return new HikariDataSource(hc);
    }

    @Override
    public boolean isOpened() {
        return opened;
    }

    /**
     * 释放资源。顺序：调度器 -> 查询线程 -> 连接池。
     * 先停调度器可避免「池已关闭但仍有人尝试调度超时任务」的无谓报错。
     */
    @Override
    public void close() throws Exception {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
            timeoutScheduler = null;
        }
        if (queryExecutor != null) {
            queryExecutor.shutdownNow();
            queryExecutor = null;
        }
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        opened = false;
    }

    // ---------------- 批量查询 ----------------

    /**
     * 批量点查：把 N 个 key 合成一条 SQL，丢到线程池执行，并挂上业务层硬超时。
     *
     * <p>完整时序：
     * <ol>
     *   <li>主线程：过滤空 key、建立「主键值 -> 入参下标列表」反查映射、拼 WHERE 子句（快）；</li>
     *   <li>提交任务到线程池：真正执行 JDBC 阻塞查询（慢，不阻塞异步算子线程）；</li>
     *   <li>调度器在 timeoutMs 后检查：Future 未完成则强制标记超时；</li>
     *   <li>Future 异常完成时回调：{@code Statement.cancel()} 让 Doris 中止 SQL、并打断等待中的线程。</li>
     * </ol>
     */
    @Override
    public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
        CompletableFuture<Map<Integer, List<Map<String, Object>>>> result = new CompletableFuture<>();

        // 过滤空 key（主键为空直接查不到），并建立「主键值 -> 入参下标列表」反查映射
        // 为什么用映射而不是列表下标？因为 SQL 返回的行序与 IN 列表顺序无关（由 Doris 决定），
        // 只能靠主键值反查才能把结果行归到正确的入参下标上。
        //
        // 为什么 value 是 List<Integer> 而不是单个 Integer？同一批里同一个主键完全可能出现多次
        // （按账户关联交易流水时极常见：一个账户在 50 条流水里出现 3 次）。
        // 若只存一个下标，后写会覆盖先写，先出现的那些下标在 groupByKey 里查不到、
        // 被上层 distribute 当成「查不到」而静默输出 NULL —— 这正是本连接器最忌讳的静默丢数据。
        List<Object[]> validKeys = new ArrayList<>(keys.size());
        Map<String, List<Integer>> keyIndex = new HashMap<>(keys.size() * 2);
        Set<String> distinctKeys = new HashSet<>(keys.size() * 2);
        for (int i = 0; i < keys.size(); i++) {
            Object[] k = keys.get(i);
            if (k == null || k.length == 0 || k[0] == null) {
                continue;
            }
            String jk = joinKey(k);
            // 记录该主键对应的「所有」入参下标，一个都不能漏
            keyIndex.computeIfAbsent(jk, x -> new ArrayList<>(1)).add(i);
            // 同一主键只往 SQL 里发一次：IN 列表（或 OR 子句）去重后能少绑参数、少传字节，
            // 结果行再按上面的下标列表分发回每个入参，语义完全不变
            if (distinctKeys.add(jk)) {
                validKeys.add(k);
            }
        }
        if (validKeys.isEmpty()) {
            result.complete(Collections.emptyMap());
            return result;
        }

        // SQL 是「前缀 + N 组占位符」拼出来的；值全部走 PreparedStatement 参数绑定，无注入风险
        String sql = selectPrefix + buildBatchWhere(validKeys.size());

        // AtomicReference 让「任务内创建的 Statement」能被「任务外的超时回调」拿到并 cancel
        AtomicReference<Statement> stmtRef = new AtomicReference<>();
        Future<?> task;
        try {
            task = queryExecutor.submit(() -> {
                // 上游可能已因超时结束（队列有积压时最常见）：没必要再占一个连接去做没人要的查询
                if (result.isDone()) {
                    return;
                }
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {
                    // 先暴露 Statement 引用，再执行查询：保证超时回调一定拿得到它
                    stmtRef.set(ps);
                    // JDBC queryTimeout 必须 <= lookup.timeout，否则 Doris 慢查询会在业务层超时降级后
                    // 仍占用查询线程与连接，直到 JDBC 层才中止，从而把线程池和连接池打满。
                    // 注意：无论用户配多大，都强制不超过 ceil(lookup.timeout/1000) 秒，这是防打满的硬约束。
                    int jdbcTimeoutSec = Math.max(1, Math.min(cfg.dorisQueryTimeoutSec,
                            (int) Math.ceil(cfg.timeoutMs / 1000.0)));
                    ps.setQueryTimeout(jdbcTimeoutSec);
                    // 顺序绑定参数：与 buildBatchWhere 生成的 ? 顺序严格一致（外层 key -> 内层列）
                    int idx = 1;
                    for (Object[] k : validKeys) {
                        for (Object v : k) {
                            ps.setObject(idx++, v);
                        }
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        result.complete(groupByKey(rs, keyIndex));
                    }
                } catch (Throwable t) {
                    // 包含「连接池取连接超时」「SQL 语法错误」「表不存在」「Doris 不可达」等，统一交上层降级
                    result.completeExceptionally(t);
                }
            });
        } catch (RejectedExecutionException ree) {
            // 队列已满（Doris 持续变慢导致任务积压）：等价于「该源本次查询失败」，
            // 返回一个异常完成的 Future 让上层自然走降级路径。
            // 绝不能让它冒泡出 asyncLookup——那会让整个作业失败，而不是降级到备源。
            result.completeExceptionally(ree);
            return result;
        }

        // 业务层硬超时：与 HBase 实现保持完全一致的语义（未来实现方换掉 HBase，降级逻辑无需改）
        if (cfg.timeoutMs > 0) {
            ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
                if (!result.isDone()) {
                    result.completeExceptionally(
                            new TimeoutException("Doris lookup timeout after " + cfg.timeoutMs + "ms"));
                }
            }, cfg.timeoutMs, TimeUnit.MILLISECONDS);
            // 查询提前结束时立即取消超时任务：否则它会一直留在调度器队列里直到到期。
            // 高 QPS + 较大 timeout 时，单线程调度器里会堆积大量「结果早已返回」的任务，
            // 到期逐个空转扫描，白白占用调度线程与内存
            result.whenComplete((rows, ex) -> timeoutTask.cancel(false));
        }

        // 无论因超时还是异常而失败，都要主动收拾后端：
        // - st.cancel() 让 Doris 真正终止这条 SQL（否则慢查询一直占着连接）
        // - task.cancel(true) 打断阻塞在 JDBC 上的线程、或取消尚未开始执行的任务
        result.whenComplete((rows, ex) -> {
            if (ex == null) {
                return; // 正常完成，后端无事可做
            }
            Statement st = stmtRef.getAndSet(null); // getAndSet 保证只会 cancel 一次
            if (st != null) {
                // 让 Doris 侧终止这条 SQL，否则慢查询会一直占着连接
                try {
                    st.cancel();
                } catch (Exception ignore) {
                    // cancel 失败无所谓，连接最终会被关闭并归还
                }
            }
            // 关键：无论 Statement 是否已创建，都要取消任务。
            // stmtRef 只在「任务真正拿到连接并 prepare 之后」才被赋值，而最容易超时的恰恰是
            // 「任务还排在队列里」或「阻塞在 getConnection（连接池打满时，connect.timeout 往往
            // 远大于 lookup.timeout）」这两种情况——此时 stmtRef 仍是 null。
            // 若把 cancel 一起关在 st != null 里，任务会在超时后照常拿到连接、把慢查询发到 Doris 跑完，
            // 于是「超时保护」在最需要它的场景里失效，连接池与线程池越拖越满。
            task.cancel(true);
        });

        return result;
    }

    /**
     * 拼 WHERE 子句：单主键用 IN，复合主键用 OR 拼接。
     *
     * <p>为什么复合主键不用 {@code (a,b) IN ((?,?),(?,?))}？因为该行构造函数语法在部分 Doris/MySQL
     * 版本上兼容性不佳，而 {@code (a=? AND b=?) OR (a=? AND b=?)} 是各版本通吃的写法。
     */
    private String buildBatchWhere(int count) {
        StringBuilder sb = new StringBuilder(64);
        if (keyColumnNames.length == 1) {
            // 单主键：WHERE `pk` IN (?,?,...)
            sb.append('`').append(keyColumnNames[0]).append("` IN (");
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('?');
            }
            sb.append(')');
        } else {
            // 复合主键：WHERE (`k1` = ? AND `k2` = ?) OR (`k1` = ? AND `k2` = ?) ...
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append('(');
                for (int j = 0; j < keyColumnNames.length; j++) {
                    if (j > 0) {
                        sb.append(" AND ");
                    }
                    sb.append('`').append(keyColumnNames[j]).append("` = ?");
                }
                sb.append(')');
            }
        }
        return sb.toString();
    }

    /**
     * 遍历结果集，按主键值反查原始下标，把行归组到对应 key。
     *
     * <p>用 {@code computeIfAbsent(...).add(row)} 而不是直接 put：同一主键在 Doris 侧可能有多行
     * （维度表未做唯一约束时），接口用 {@code List} 承载一对多，由上层决定取哪一行。
     *
     * <p><b>一对多分发</b>：一个结果行要发给该主键对应的<b>每一个</b>入参下标（批内同一主键出现多次时
     * 有多个下标）。多个下标共享同一个 row 实例是安全的——上层 {@code distribute} 只会往 row 里
     * 写入相同的元字段（来源、耗时），且每个下标各自转换成独立的 RowData。
     *
     * <p>注意这里返回的行 Map 已额外塞进了元字段键（由上层 {@code distribute} 处理），
     * 因此不能直接拿来当业务数据用。
     */
    private Map<Integer, List<Map<String, Object>>> groupByKey(ResultSet rs, Map<String, List<Integer>> keyIndex)
            throws Exception {
        Map<Integer, List<Map<String, Object>>> result = new HashMap<>();
        ResultSetMetaData md = rs.getMetaData();
        int columnCount = md.getColumnCount();
        // 列名标签在整批里是恒定的，提取到循环外做一次即可：
        // 放在内层就会变成「每一行 × 每一列」都调一次 getColumnLabel + toLowerCase（都新建字符串对象）
        String[] labels = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            // 用 ColumnLabel（而非 ColumnName）以兼容 SQL 中的别名；
            // 统一转小写，与 HBase 侧保持一致，使 RowDataConverter 的取值逻辑对两源通用
            labels[i] = md.getColumnLabel(i + 1).toLowerCase();
        }
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columnCount * 2);
            for (int i = 0; i < columnCount; i++) {
                // JDBC 列下标从 1 开始
                row.put(labels[i], rs.getObject(i + 1));
            }
            List<Integer> idxs = keyIndex.get(joinKeyFromRow(row));
            if (idxs == null) {
                continue; // 理论上不会发生（SQL 已限定 IN 列表）；防御性跳过，避免脏数据污染结果
            }
            // 分发给该主键的全部入参下标，任何一个都不能落下
            for (Integer idx : idxs) {
                result.computeIfAbsent(idx, x -> new ArrayList<>()).add(row);
            }
        }
        return result;
    }

    /**
     * 把入参主键数组拼成一个字符串键，用于结果反查。
     *
     * <p>用 {@code \u0001}（SOH，不可见控制字符）作分隔符而不是逗号：业务主键值里几乎不可能出现该字符，
     * 可最大限度避免「a,b + c」与「a + b,c」拼成同一个字符串导致串键的经典问题。
     */
    private String joinKey(Object[] key) {
        StringBuilder sb = new StringBuilder();
        for (Object v : key) {
            sb.append(normKeyPart(v)).append('\u0001');
        }
        return sb.toString();
    }

    /** 从结果行中按主键列名取出值，拼成与 {@link #joinKey} 完全一致的字符串键，才能对得上 */
    private String joinKeyFromRow(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        for (String col : keyColumnNamesLower) { // 已预转小写，无需每行重复 toLowerCase
            sb.append(normKeyPart(row.get(col))).append('\u0001');
        }
        return sb.toString();
    }

    /**
     * 把单个主键值规范化成「内容相等 ⇔ 字符串相等」的形式。
     *
     * <p>两类值必须特殊处理，否则对应类型的主键会<b>静默查不到数据</b>（不触发降级、不打异常日志）：
     * <ul>
     *   <li><b>byte[]（BINARY/VARBINARY）</b>：{@code toString()} 得到的是对象哈希
     *       （形如 {@code [B@1a2b3c}），两个内容完全相同的数组也会拼出不同字符串，永远查不到。
     *       改用 Base64 编码，保证「内容相等 ⇔ 字符串相等」；</li>
     *   <li><b>BigDecimal（DECIMAL）</b>：{@code toString()} 会带上 scale
     *       （入参 {@code 123}、Doris 返回 {@code 123.00}），二者不匹配。
     *       统一 {@code stripTrailingZeros() + toPlainString()}，
     *       既消除 scale 差异，也避免科学计数法（{@code 1E+2} vs {@code 100}）。</li>
     * </ul>
     */
    private static String normKeyPart(Object v) {
        if (v == null) {
            return "";
        }
        if (v instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) v);
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).stripTrailingZeros().toPlainString();
        }
        return v.toString();
    }
}
