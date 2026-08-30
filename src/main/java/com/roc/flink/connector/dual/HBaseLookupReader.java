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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HBase 维表读取器（基于 HBase 2.x 原生异步客户端 AsyncConnection）。
 *
 * <p>超时分两层：
 * <ol>
 *   <li>客户端层：{@code hbase.rpc.timeout} / {@code hbase.client.operation.timeout}，让 HBase 自己先中断 RPC；</li>
 *   <li>Future 层：{@code lookup.timeout}，由本类调度器强制让 Future 以 TimeoutException 结束，
 *       这样 Router 能立刻降级到 Doris，不必等 HBase 把重试走完。</li>
 * </ol>
 */
public class HBaseLookupReader implements LookupReader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupReader.class);

    private final DualLookupOptions.Config cfg;
    private final String[] columnNames;
    private final LogicalType[] columnTypes;

    private transient volatile boolean opened = false;
    private transient AsyncConnection connection;
    private transient AsyncTable<AdvancedScanResultConsumer> table;
    private transient byte[] familyBytes;
    private transient ScheduledExecutorService timeoutScheduler;

    public HBaseLookupReader(DualLookupOptions.Config cfg, String[] columnNames, LogicalType[] columnTypes) {
        this.cfg = cfg;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    // ---------------- 生命周期 ----------------

    @Override
    public synchronized void open() throws Exception {
        if (opened) {
            return;
        }
        Configuration hbaseConf = HBaseConfiguration.create();
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

        timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dual-lookup-hbase-timeout");
            t.setDaemon(true);
            return t;
        });

        opened = true;
        LOG.info("[dual-lookup] HBase reader opened, table={}, family={}, zk={}",
                cfg.hbaseTableName, cfg.hbaseColumnFamily, cfg.hbaseZkQuorum);
    }

    /**
     * 配置 Kerberos 认证。HBase 2.2.0+ 的 ConnectionFactory 只要发现
     * {@code hbase.client.keytab.file} + {@code hbase.client.kerberos.principal}，
     * 就会自动完成登录和 TGT 续期，应用侧无需再手动调用 UserGroupInformation。
     */
    private void applyKerberos(Configuration conf) {
        if (!"kerberos".equalsIgnoreCase(cfg.hbaseSecurityAuthentication)) {
            return;
        }
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

    @Override
    public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
        CompletableFuture<Map<Integer, List<Map<String, Object>>>> result = new CompletableFuture<>();

        // 过滤空 key，构建批量 Get；indexMap 记录每个 Get 对应的原始下标
        List<Get> gets = new ArrayList<>();
        List<Integer> indexMap = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            Object[] k = keys.get(i);
            if (k == null || k.length == 0 || k[0] == null) {
                continue;
            }
            Get get = new Get(buildRowKey(k));
            for (String col : columnNames) {
                get.addColumn(familyBytes, Bytes.toBytes(col));
            }
            gets.add(get);
            indexMap.add(i);
        }
        if (gets.isEmpty()) {
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
                            // 任一个 Get 失败（含超时），整批按失败处理，交由上层降级
                            result.completeExceptionally(unwrap(ex));
                            return;
                        }
                        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
                        for (int j = 0; j < futures.size(); j++) {
                            Result r = futures.get(j).join(); // allOf 已完成，join 立即返回
                            if (r != null && !r.isEmpty()) {
                                map.put(indexMap.get(j), toRows(r));
                            }
                        }
                        result.complete(map);
                    });
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }

        return withTimeout(result, cfg.timeoutMs);
    }

    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> f, long timeoutMs) {
        if (timeoutMs <= 0) {
            return f;
        }
        timeoutScheduler.schedule(() -> {
            if (!f.isDone()) {
                f.completeExceptionally(
                        new TimeoutException("HBase lookup timeout after " + timeoutMs + "ms"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        return f;
    }

    // ---------------- 转换 ----------------

    private byte[] buildRowKey(Object[] keyValues) {
        if (keyValues.length == 1) {
            return toBytes(keyValues[0]);
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

    private byte[] toBytes(Object v) {
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

    private List<Map<String, Object>> toRows(Result result) {
        if (result == null || result.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> row = new HashMap<>(columnNames.length * 2);
        for (int i = 0; i < columnNames.length; i++) {
            byte[] v = result.getValue(familyBytes, Bytes.toBytes(columnNames[i]));
            row.put(columnNames[i].toLowerCase(), decode(v, columnTypes[i]));
        }
        List<Map<String, Object>> out = new ArrayList<>(1);
        out.add(row);
        return out;
    }

    /** HBase 里存的是字节数组，按 DDL 声明的类型还原成 Java 对象 */
    private Object decode(byte[] v, LogicalType type) {
        if (v == null || v.length == 0) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBoolean(v);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
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
                return Bytes.toInt(v);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Bytes.toLong(v);
            case BINARY:
            case VARBINARY:
                return v;
            case CHAR:
            case VARCHAR:
            default:
                return Bytes.toString(v);
        }
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null)
                ? t.getCause()
                : t;
    }
}
