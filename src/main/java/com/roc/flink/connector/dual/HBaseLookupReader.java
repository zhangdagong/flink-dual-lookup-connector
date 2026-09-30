package com.roc.flink.connector.dual;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HBase 维表读取器（基于 HBase 2.x 原生异步客户端 AsyncConnection）。
 *
 * <p>为什么用 AsyncConnection 而不是 HTable？因为 Lookup Join 的异步算子里若混入阻塞调用，
 * 会把异步算子的线程池拖死，退化成同步性能。原生异步客户端全程返回 CompletableFuture，
 * 与 Flink 的 {@code AsyncLookupFunction} 天然契合。
 *
 * <p>超时分两层（职责必须分清，否则会误判降级）：
 * <ol>
 *   <li>客户端层：{@code hbase.rpc.timeout} / {@code hbase.client.operation.timeout}，让 HBase 自己先中断 RPC，
 *       目的是<b>让后端尽早释放资源</b>；</li>
 *   <li>Future 层：{@code lookup.timeout}，由本类调度器强制让 Future 以 TimeoutException 结束，
 *       目的是<b>让业务层立刻降级</b>，不必等 HBase 把重试走完。</li>
 * </ol>
 */
public class HBaseLookupReader implements LookupReader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupReader.class);

    /** 全局配置（可序列化，随算子下发） */
    private final DualLookupOptions.Config cfg;
    /** 要查询的业务列名（已剥离元字段），同时作为 HBase 列限定符（qualifier） */
    private final String[] columnNames;
    /** 业务列对应的 Flink 类型，用于把 HBase 的 byte[] 还原成正确的 Java 类型 */
    private final LogicalType[] columnTypes;
    /**
     * 主键列类型。仅 {@code hbase.rowkey.encoding = typed} 时使用：
     * 类型化编码必须知道每段主键的类型，才能编出与建表侧一致的字节。
     */
    private final LogicalType[] keyTypes;
    /**
     * 列名的小写形式（结果行 Map 的 key 统一小写，与 Doris 侧保持一致）。
     * 构造期算好——放在取值循环里就变成「每一行 × 每一列」都调一次 toLowerCase()（每次都新建字符串对象）。
     */
    private final String[] columnNamesLower;
    /**
     * 列名的 HBase 字节形式（qualifier 字节）。
     * 构造期算好——放在查询路径上就变成「每个 Get × 每一列」和「每一行 × 每一列」都重复编码。
     */
    private final byte[][] columnBytes;

    // ---- 以下均为运行期资源句柄：不能被序列化，故声明为 transient，open() 时重建 ----

    /** 是否已建连；volatile 保证多线程可见性（攒批线程可能来自不同调用栈） */
    private transient volatile boolean opened = false;
    /** 异步连接：内部自带 Netty 线程池，整个 Reader 生命周期内共享一个即可 */
    private transient AsyncConnection connection;
    /** 异步表句柄：轻量对象，复用才能命中客户端侧的 region location 缓存 */
    private transient AsyncTable<AdvancedScanResultConsumer> table;
    /** 列族名，提前转好 byte[]（每次查询都要用，避免重复编码） */
    private transient byte[] familyBytes;
    /** 专门用于「到期强制让 Future 超时」的调度线程池；
     * 用 ScheduledThreadPoolExecutor 而非 Executors 工厂方法：后者返回的是包装类，
     * 拿不到队列句柄，无法观测「已完成查询的超时任务是否被及时清理」 */
    private transient ScheduledThreadPoolExecutor timeoutScheduler;

    public HBaseLookupReader(DualLookupOptions.Config cfg, String[] columnNames, LogicalType[] columnTypes,
                             LogicalType[] keyTypes) {
        this.cfg = cfg;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.keyTypes = keyTypes;
        // 列名的小写副本与字节形式都在这里算一次，运行期只读
        this.columnNamesLower = new String[columnNames.length];
        this.columnBytes = new byte[columnNames.length][];
        for (int i = 0; i < columnNames.length; i++) {
            this.columnNamesLower[i] = columnNames[i].toLowerCase();
            this.columnBytes[i] = Bytes.toBytes(columnNames[i]);
        }
    }

    // ---------------- 生命周期 ----------------

    /**
     * 建连 + 预热。synchronized 保证多线程下只真正执行一次（幂等）。
     *
     * <p>注意这里是<b>同步阻塞</b>的：HBase 不可用时 {@code createAsyncConnection().get()} 会一直等到
     * ZooKeeper/客户端超时。这是「精简优先」的取舍——作业启动期不会因此失败（惰性建连），
     * 但建连那一刻的个别批次会被阻塞住，靠 ZK 超时兜底。
     */
    @Override
    public synchronized void open() throws Exception {
        if (opened) {
            return;
        }
        // HBaseConfiguration.create() 会先读 classpath 下的 hbase-site.xml 作为基线
        Configuration hbaseConf = HBaseConfiguration.create();
        // 显式配置 ZK：留空则沿用 hbase-site.xml，便于容器/云上不改配置文件即可接入
        if (cfg.hbaseZkQuorum != null && !cfg.hbaseZkQuorum.trim().isEmpty()) {
            hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, cfg.hbaseZkQuorum.trim());
            hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, cfg.hbaseZkPort);
            hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, cfg.hbaseZnodeParent);
        }
        // 实时链路：超时收窄 + 少重试，避免一次卡顿拖垮整个算子
        hbaseConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, cfg.hbaseRpcTimeoutMs);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, cfg.hbaseOperationTimeoutMs);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, cfg.hbaseRetries);

        // Kerberos：在建连前完成认证配置，HBase 2.2.0+ 会据此自动登录并续期
        applyKerberos(hbaseConf);

        connection = ConnectionFactory.createAsyncConnection(hbaseConf).get();
        table = connection.getTable(TableName.valueOf(cfg.hbaseTableName));
        familyBytes = Bytes.toBytes(cfg.hbaseColumnFamily);

        // 预热 region location：首条查询若才去定位 region（冷启动），
        // 会因「waiting for region location」耗时数百毫秒而撞上 500ms 超时被降级。
        // 提前在建连时取好 location，后续查询直接命中客户端缓存。
        // 注意：getRegionLocator() 是同步返回 AsyncTableRegionLocator，getAllRegionLocations() 才返回 CompletableFuture。
        table.getRegionLocator()
                .getAllRegionLocations()
                .get(cfg.hbaseOperationTimeoutMs, TimeUnit.MILLISECONDS);

        // 超时调度器：一个 Reader 一个线程足够（只做「到点标记超时」这个轻量动作）
        timeoutScheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "dual-lookup-hbase-timeout");
            t.setDaemon(true);
            return t;
        });
        // 取消即出队：默认策略下被取消的任务会滞留在队列里直到到期时刻，
        // 高 QPS 时「查询早已完成」的超时任务仍会堆积；开启后 cancel(false) 立即释放队列空间
        timeoutScheduler.setRemoveOnCancelPolicy(true);

        // 放在最后：以上任一步骤抛异常，opened 仍为 false，下次查询会重新尝试建连
        opened = true;
        // rowkey 编码模式务必打出来：它决定了「能不能查到数据」，是排查「HBase 全部查不到」时的第一现场
        LOG.info("[dual-lookup] HBase reader opened, table={}, family={}, zk={}, rowkeyEncoding={}",
                cfg.hbaseTableName, cfg.hbaseColumnFamily, cfg.hbaseZkQuorum, cfg.hbaseRowkeyEncoding);
    }

    /**
     * 配置 Kerberos 认证。HBase 2.2.0+ 的 ConnectionFactory 只要发现
     * {@code hbase.client.keytab.file} + {@code hbase.client.kerberos.principal}，
     * 就会自动完成登录和 TGT 续期，应用侧无需再手动调用 UserGroupInformation。
     *
     * <p>这也是为什么本连接器能在长期运行的流作业里安全使用 Kerberos：凭据续期交给客户端，
     * 不会出现「作业跑几天后 TGT 过期导致全部查询失败」的经典问题。
     */
    private void applyKerberos(Configuration conf) {
        if (!"kerberos".equalsIgnoreCase(cfg.hbaseSecurityAuthentication)) {
            return; // 默认 simple 模式，直接返回，零开销
        }
        // krb5.conf 是 JVM 级系统属性，必须在建连（发起认证）之前设置
        if (!cfg.hbaseKrb5Conf.trim().isEmpty()) {
            System.setProperty("java.security.krb5.conf", cfg.hbaseKrb5Conf.trim());
        }
        conf.set("hbase.security.authentication", "kerberos");
        if (!cfg.hbaseClientKeytabFile.trim().isEmpty()) {
            conf.set("hbase.client.keytab.file", cfg.hbaseClientKeytabFile.trim());
        }
        if (!cfg.hbaseClientKerberosPrincipal.trim().isEmpty()) {
            conf.set("hbase.client.kerberos.principal", cfg.hbaseClientKerberosPrincipal.trim());
        }
        if (!cfg.hbaseRegionserverKerberosPrincipal.trim().isEmpty()) {
            conf.set("hbase.regionserver.kerberos.principal", cfg.hbaseRegionserverKerberosPrincipal.trim());
        }
        if (!cfg.hbaseMasterKerberosPrincipal.trim().isEmpty()) {
            conf.set("hbase.master.kerberos.principal", cfg.hbaseMasterKerberosPrincipal.trim());
        }
        LOG.info("[dual-lookup] HBase Kerberos enabled, principal={}, keytab={}",
                cfg.hbaseClientKerberosPrincipal, cfg.hbaseClientKeytabFile);
    }

    @Override
    public boolean isOpened() {
        return opened;
    }

    /**
     * 释放资源。依次关闭调度器 -> 连接；
     * 顺序不能反：调度器若在连接关闭后仍触发，会在已销毁的连接上做无意义操作。
     */
    @Override
    public void close() throws Exception {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
            timeoutScheduler = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
        opened = false;
    }

    // ---------------- 查询 ----------------

    /**
     * 批量点查：N 个 key 合成一次 batch get，再统一挂上业务层硬超时。
     *
     * <p>返回的 Future 语义（对上层 {@link DualLookupFunction} 的承诺）：
     * 正常完成 = 查询成功（可能全都没命中，属正常业务结果）；
     * 异常完成 = 超时或 RPC 失败 → 触发降级。
     */
    @Override
    public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
        CompletableFuture<Map<Integer, List<Map<String, Object>>>> result = new CompletableFuture<>();

        // 过滤空 key，构建批量 Get；indexMap 记录每个 Get 对应的原始下标
        // 下标映射不可省：过滤掉空 key 后，Get 的下标与入参 keys 的下标就不再一一对应了
        List<Get> gets = new ArrayList<>();
        List<Integer> indexMap = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            Object[] k = keys.get(i);
            if (k == null || k.length == 0 || k[0] == null) {
                continue; // 主键为空的记录直接跳过：结果里不含该下标，上层按「查不到」处理
            }
            Get get = new Get(buildRowKey(k));
            // 只取需要的列（列投影），减少网络与反序列化开销。
            // 用构造期预编码好的 columnBytes，避免每个 Get、每一列都重复 Bytes.toBytes
            for (int c = 0; c < columnBytes.length; c++) {
                get.addColumn(familyBytes, columnBytes[c]);
            }
            gets.add(get);
            indexMap.add(i);
        }
        if (gets.isEmpty()) {
            // 整批都无有效 key：直接返回空 Map，连 IO 都不发起
            result.complete(Collections.emptyMap());
            return result;
        }

        try {
            // HBase 2.x 的 table.get(List<Get>) 返回 List<CompletableFuture<Result>>，
            // 下标与 gets 一一对应，需用 allOf 组合等待全部完成
            List<CompletableFuture<Result>> futures = table.get(gets);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            // 任一个 Get 失败（含超时），整批按失败处理，交由上层降级。
                            // 这是「批量语义」的必然取舍：部分成功的结果不完整，用不完整的维表数据
                            // 比走备源重查更危险（会静默产出错误的关联结果）
                            result.completeExceptionally(unwrap(ex));
                            return;
                        }
                        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
                        for (int j = 0; j < futures.size(); j++) {
                            Result r = futures.get(j).join(); // allOf 已完成，join 立即返回
                            if (r != null && !r.isEmpty()) {
                                // 未命中的 key 不写入 map：与「空结果不降级」的约定保持一致
                                map.put(indexMap.get(j), toRows(r));
                            }
                        }
                        result.complete(map);
                    });
        } catch (Throwable t) {
            // table.get() 本身同步抛错（如表不存在、连接已关闭）
            result.completeExceptionally(t);
        }

        // 关键：无条件挂上业务层硬超时，保证 Future 一定会终结
        return withTimeout(result, cfg.timeoutMs);
    }

    /**
     * 业务层硬超时：到点若 Future 仍未完成，用 TimeoutException 强制终结它。
     *
     * <p>为什么要「强制终结」而不是取消底层请求？因为 Flink 的异步算子只认 Future 的状态，
     * 不认底层 RPC 是否还在跑。让 Future 立刻异常完成，就能马上降级到备源，
     * 底层那次慢 RPC 则交给客户端超时自行回收。
     */
    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> f, long timeoutMs) {
        if (timeoutMs <= 0) {
            return f; // 关闭超时（仅测试用；生产配置强校验必须 > 0）
        }
        ScheduledFuture<?> timeoutTask = timeoutScheduler.schedule(() -> {
            if (!f.isDone()) { // 已完成则什么都不做，避免覆盖真实结果
                f.completeExceptionally(
                        new TimeoutException("HBase lookup timeout after " + timeoutMs + "ms"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        // 查询提前结束时立即取消超时任务，不让它在单线程调度器队列里空待到到期
        // （高 QPS 下队列里会堆积大量「结果早已返回」的任务，到期逐个空转扫描）
        f.whenComplete((v, ex) -> timeoutTask.cancel(false));
        return f;
    }

    // ---------------- 转换 ----------------

    /**
     * 由 join key 拼 HBase rowkey。
     *
     * <p><b>编码模式由 {@code hbase.rowkey.encoding} 决定，必须与被查表写入端严格一致</b>，
     * 否则表现为「一条也查不到」（空结果不触发降级，全部关联结果为空，很难从日志定位）：
     * <ul>
     *   <li>{@code string}（默认，兼容既有数据）：主键值一律转成字符串再取 UTF-8 字节。
     *       单主键直接编码；复合主键用 {@code hbase.rowkey.delimiter} 拼接；</li>
     *   <li>{@code typed}：按主键的 Flink 类型编码（BIGINT → {@code Bytes.toBytes(long)}、
     *       INTEGER → {@code Bytes.toBytes(int)} 等），与 {@link #decode} 的解码规则严格对称。
     *       写入端按 HBase 惯例写类型化 rowkey 时必须用这个模式。</li>
     * </ul>
     */
    private byte[] buildRowKey(Object[] keyValues) {
        if (!"typed".equalsIgnoreCase(cfg.hbaseRowkeyEncoding)) {
            // ---- 字符串编码（默认）----
            if (keyValues.length == 1) {
                return stringBytes(keyValues[0]);
            }
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0; i < keyValues.length; i++) {
                if (i > 0) {
                    sb.append(cfg.hbaseRowkeyDelimiter);
                }
                sb.append(keyValues[i] == null ? "" : keyValues[i].toString());
            }
            return Bytes.toBytes(sb.toString());
        }

        // ---- 类型化编码 ----
        if (keyValues.length == 1) {
            return typedBytes(keyValues[0], keyTypes[0]);
        }
        // 复合主键：各段类型化编码后用「分隔符的字节」连接。
        // 不能像单段那样首尾直接相接：字符串段是变长的，直接拼接会产生经典的前缀歧义
        // （("a","bc") 与 ("ab","c") 会拼出同一串字节）
        byte[] delim = Bytes.toBytes(cfg.hbaseRowkeyDelimiter);
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        for (int i = 0; i < keyValues.length; i++) {
            if (i > 0) {
                out.write(delim, 0, delim.length);
            }
            byte[] seg = typedBytes(keyValues[i], i < keyTypes.length ? keyTypes[i] : null);
            out.write(seg, 0, seg.length);
        }
        return out.toByteArray();
    }

    /**
     * 字符串编码（{@code rowkey.encoding=string}）：byte[] 直接用；BigDecimal 用 HBase 专用编码
     * 以保证可比较性；其余一律 {@code toString()} 后取 UTF-8 字节。
     */
    private byte[] stringBytes(Object v) {
        if (v == null) {
            return new byte[0];
        }
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        if (v instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal) v);
        }
        return Bytes.toBytes(v.toString());
    }

    /**
     * 类型化编码（{@code rowkey.encoding=typed}）：按主键声明的 Flink 类型编码，
     * 与 {@link #decode} 逐类型严格对称——这是「写入端与读取端用同一套编码」的保证。
     *
     * <p>注意 TINYINT/SMALLINT 也编成 4 字节 int：HBase 只有一种 4 字节整型，
     * 与 {@code decode} 里统一按 {@code Bytes.toInt} 解码的做法保持一致。
     *
     * <p>DATE/TIME/TIMESTAMP 的值是经 {@code RowDataConverter.toJavaValue} 转换后的
     * {@code java.sql.Date/Time/Timestamp}，这里反推回 Flink 内部的「天数/毫秒/毫秒」，
     * 与 {@code decode} 的 {@code Bytes.toInt/toLong} 对称。
     */
    private byte[] typedBytes(Object v, LogicalType type) {
        if (v == null) {
            return new byte[0];
        }
        if (type == null) {
            return stringBytes(v); // 防御：类型缺失时退回字符串编码，行为可预期
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBytes((Boolean) v);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Bytes.toBytes(((Number) v).intValue());
            case BIGINT:
                return Bytes.toBytes(((Number) v).longValue());
            case FLOAT:
                return Bytes.toBytes(((Number) v).floatValue());
            case DOUBLE:
                return Bytes.toBytes(((Number) v).doubleValue());
            case DECIMAL:
                return Bytes.toBytes((BigDecimal) v);
            case DATE:
                return Bytes.toBytes((int) ((java.sql.Date) v).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return Bytes.toBytes(
                        (int) (((java.sql.Time) v).toLocalTime().toNanoOfDay() / 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Bytes.toBytes(((java.sql.Timestamp) v).getTime());
            case BINARY:
            case VARBINARY:
                return (v instanceof byte[]) ? (byte[]) v : Bytes.toBytes(v.toString());
            default:
                // CHAR/VARCHAR 及未知类型：UTF-8 字符串，与 decode 的兜底分支对称
                return Bytes.toBytes(v.toString());
        }
    }

    /** 一行 Result -> 单元素列表（接口需要 List 以兼容 Doris 侧一对多的情况） */
    private List<Map<String, Object>> toRows(Result result) {
        if (result == null || result.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> row = new HashMap<>(columnNames.length * 2);
        for (int i = 0; i < columnNames.length; i++) {
            // 列名字节与列名小写形式都是构造期预计算好的，运行期只读
            byte[] v = result.getValue(familyBytes, columnBytes[i]);
            row.put(columnNamesLower[i], decode(v, columnTypes[i]));
        }
        List<Map<String, Object>> out = new ArrayList<>(1);
        out.add(row);
        return out;
    }

    /**
     * HBase 里存的是字节数组，按 DDL 声明的类型还原成 Java 对象。
     *
     * <p>这里隐含一个约束：<b>写入 HBase 时的编码方式必须与 DDL 类型一致</b>。
     * 例如 DDL 声明 BIGINT，就必须用 {@code Bytes.toBytes(long)} 写入，
     * 若误写成字符串形式的字节，解码会得到错误结果。
     */
    private Object decode(byte[] v, LogicalType type) {
        if (v == null || v.length == 0) {
            return null; // 列不存在或为空，交给 RowDataConverter 处理成 NULL
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBoolean(v);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                // HBase 只有 int 一种 4 字节整型，TINYINT/SMALLINT 一律按 int 解码，
                // 由 RowDataConverter 再做窄化（byteValue/shortValue）
                return Bytes.toInt(v);
            case BIGINT:
                return Bytes.toLong(v);
            case FLOAT:
                return Bytes.toFloat(v);
            case DOUBLE:
                return Bytes.toDouble(v);
            case DECIMAL:
                return Bytes.toBigDecimal(v);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // Flink 内部 DATE/TIME 都是 int（天数 / 毫秒数），与 HBase 的 int 编码直接对应
                return Bytes.toInt(v);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Flink TIMESTAMP(3) 内部是 long 毫秒，与 HBase long 编码对应
                return Bytes.toLong(v);
            case BINARY:
            case VARBINARY:
                return v; // 二进制列原样返回
            case CHAR:
            case VARCHAR:
            default:
                // 兜底按 UTF-8 字符串解码，兼容新类型/未知类型
                return Bytes.toString(v);
        }
    }

    /** 剥掉 CompletableFuture 包装的 CompletionException，让上层日志/异常链看到真实原因 */
    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null)
                ? t.getCause()
                : t;
    }
}
