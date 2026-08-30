package com.roc.flink.connector.dual;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/**
 * dual-lookup connector 的全部配置项 + 解析后的配置值对象。
 *
 * <p>说明：所有时间类参数统一用「毫秒整数」，DDL 里直接写数字，无需带 ms 等单位。
 */
public final class DualLookupOptions {

    private DualLookupOptions() {}

    public static final String CONNECTOR_ID = "dual-lookup";

    public static final String SRC_HBASE = "hbase";
    public static final String SRC_DORIS = "doris";

    // ==================== 核心：主备路由与超时 ====================

    /** 主查询源，取值 hbase 或 doris；另一个自动作为备源 */
    public static final ConfigOption<String> PRIMARY =
            ConfigOptions.key("lookup.primary")
                    .stringType()
                    .defaultValue(SRC_HBASE);

    /** 单次（批量）查询硬超时，单位毫秒，超过即判定该源故障、转查另一个源 */
    public static final ConfigOption<Integer> LOOKUP_TIMEOUT =
            ConfigOptions.key("lookup.timeout")
                    .intType()
                    .defaultValue(500);

    // ==================== 批量（攒批） ====================

    /** 攒批条数阈值：攒满这么多条就批量查一次，显著降低 Doris 的查询次数 */
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("lookup.batch.size")
                    .intType()
                    .defaultValue(50);

    /** 攒批最大等待（毫秒）：攒不满 batch.size 时，最多等这么久就发一次，避免低流量下延迟无界 */
    public static final ConfigOption<Integer> BATCH_MAX_WAIT =
            ConfigOptions.key("lookup.batch.max-wait")
                    .intType()
                    .defaultValue(5);

    // ==================== 可观测性 ====================

    /** 统计日志打印间隔（秒），0 表示关闭 */
    public static final ConfigOption<Integer> STATS_LOG_INTERVAL =
            ConfigOptions.key("lookup.stats.log-interval")
                    .intType()
                    .defaultValue(60);

    // ==================== HBase ====================

    public static final ConfigOption<String> HBASE_TABLE_NAME =
            ConfigOptions.key("hbase.table-name").stringType().noDefaultValue();

    public static final ConfigOption<String> HBASE_ZK_QUORUM =
            ConfigOptions.key("hbase.zookeeper.quorum").stringType().noDefaultValue();

    public static final ConfigOption<String> HBASE_ZK_PORT =
            ConfigOptions.key("hbase.zookeeper.property.clientPort").stringType().defaultValue("2181");

    public static final ConfigOption<String> HBASE_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent").stringType().defaultValue("/hbase");

    public static final ConfigOption<String> HBASE_COLUMN_FAMILY =
            ConfigOptions.key("hbase.column-family").stringType().defaultValue("info");

    public static final ConfigOption<String> HBASE_ROWKEY_DELIMITER =
            ConfigOptions.key("hbase.rowkey.delimiter").stringType().defaultValue("|");

    public static final ConfigOption<Integer> HBASE_RPC_TIMEOUT_MS =
            ConfigOptions.key("hbase.rpc.timeout").intType().defaultValue(300);

    public static final ConfigOption<Integer> HBASE_OPERATION_TIMEOUT_MS =
            ConfigOptions.key("hbase.client.operation.timeout").intType().defaultValue(500);

    public static final ConfigOption<Integer> HBASE_RETRIES =
            ConfigOptions.key("hbase.client.retries.number").intType().defaultValue(1);

    // ---- HBase Kerberos（可选，默认 simple 免认证） ----

    /** 认证方式：simple（默认）或 kerberos */
    public static final ConfigOption<String> HBASE_SECURITY_AUTHENTICATION =
            ConfigOptions.key("hbase.security.authentication")
                    .stringType()
                    .defaultValue("simple");

    /** 客户端 keytab 文件路径（kerberos 下必填） */
    public static final ConfigOption<String> HBASE_CLIENT_KEYTAB_FILE =
            ConfigOptions.key("hbase.client.keytab.file").stringType().noDefaultValue();

    /** 客户端 Kerberos principal，如 hbase/ro@EXAMPLE.COM（kerberos 下必填） */
    public static final ConfigOption<String> HBASE_CLIENT_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.client.kerberos.principal").stringType().noDefaultValue();

    /** RegionServer 的 Kerberos principal，如 hbase/_HOST@EXAMPLE.COM（可选，缺省用 _HOST 规则） */
    public static final ConfigOption<String> HBASE_REGIONSERVER_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.regionserver.kerberos.principal").stringType().noDefaultValue();

    /** Master 的 Kerberos principal（可选） */
    public static final ConfigOption<String> HBASE_MASTER_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.master.kerberos.principal").stringType().noDefaultValue();

    /** krb5.conf 路径（可选，默认用系统 /etc/krb5.conf） */
    public static final ConfigOption<String> HBASE_KRB5_CONF =
            ConfigOptions.key("hbase.kerberos.krb5.conf").stringType().noDefaultValue();

    // ==================== Doris ====================

    public static final ConfigOption<String> DORIS_JDBC_URL =
            ConfigOptions.key("doris.jdbc-url").stringType().noDefaultValue();

    public static final ConfigOption<String> DORIS_TABLE_NAME =
            ConfigOptions.key("doris.table-name").stringType().noDefaultValue();

    public static final ConfigOption<String> DORIS_USERNAME =
            ConfigOptions.key("doris.username").stringType().defaultValue("root");

    public static final ConfigOption<String> DORIS_PASSWORD =
            ConfigOptions.key("doris.password").stringType().defaultValue("");

    public static final ConfigOption<String> DORIS_DRIVER =
            ConfigOptions.key("doris.driver").stringType().defaultValue("com.mysql.cj.jdbc.Driver");

    /** JDBC Statement 的 queryTimeout（秒），服务端侧兜底 */
    public static final ConfigOption<Integer> DORIS_QUERY_TIMEOUT_SEC =
            ConfigOptions.key("doris.query.timeout").intType().defaultValue(1);

    // ---- 连接池（HikariCP）调优项 ----

    /** 连接池最大连接数 */
    public static final ConfigOption<Integer> DORIS_POOL_SIZE =
            ConfigOptions.key("doris.pool.size").intType().defaultValue(8);

    /** 连接池最小空闲连接数 */
    public static final ConfigOption<Integer> DORIS_POOL_MIN_IDLE =
            ConfigOptions.key("doris.pool.min-idle").intType().defaultValue(2);

    /** 获取连接的超时（毫秒） */
    public static final ConfigOption<Integer> DORIS_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("doris.connect.timeout").intType().defaultValue(3000);

    /** 连接空闲多久被回收（毫秒） */
    public static final ConfigOption<Integer> DORIS_POOL_IDLE_TIMEOUT_MS =
            ConfigOptions.key("doris.pool.idle-timeout").intType().defaultValue(600000);

    /** 连接最大存活时间（毫秒），须大于 idle-timeout */
    public static final ConfigOption<Integer> DORIS_POOL_MAX_LIFETIME_MS =
            ConfigOptions.key("doris.pool.max-lifetime").intType().defaultValue(1800000);

    /** 连接有效性校验超时（毫秒） */
    public static final ConfigOption<Integer> DORIS_POOL_VALIDATION_TIMEOUT_MS =
            ConfigOptions.key("doris.pool.validation-timeout").intType().defaultValue(3000);

    // ==================== 配置值对象 ====================

    /** 从 WITH 子句解析出来的只读配置（必须可序列化，会随 Function 下发到 TaskManager） */
    public static class Config implements Serializable {

        private static final long serialVersionUID = 1L;

        public String primary;          // 主源：hbase | doris
        public String standby;          // 备源：由 primary 推导
        public int timeoutMs;           // 单次（批量）查询硬超时（毫秒）

        public int batchSize;           // 攒批条数阈值
        public int batchMaxWaitMs;      // 攒批最大等待（毫秒）
        public int statsLogIntervalSec; // 统计日志间隔（秒），0 关闭

        public String hbaseTableName;
        public String hbaseZkQuorum;
        public String hbaseZkPort;
        public String hbaseZnodeParent;
        public String hbaseColumnFamily;
        public String hbaseRowkeyDelimiter;
        public int hbaseRpcTimeoutMs;
        public int hbaseOperationTimeoutMs;
        public int hbaseRetries;
        public String hbaseSecurityAuthentication;
        public String hbaseClientKeytabFile;
        public String hbaseClientKerberosPrincipal;
        public String hbaseRegionserverKerberosPrincipal;
        public String hbaseMasterKerberosPrincipal;
        public String hbaseKrb5Conf;

        public String dorisJdbcUrl;
        public String dorisTableName;
        public String dorisUsername;
        public String dorisPassword;
        public String dorisDriver;
        public int dorisQueryTimeoutSec;
        public int dorisPoolSize;
        public int dorisPoolMinIdle;
        public int dorisConnectTimeoutMs;
        public int dorisPoolIdleTimeoutMs;
        public int dorisPoolMaxLifetimeMs;
        public int dorisPoolValidationTimeoutMs;

        private Config() {}

        /** ddlTableName：HBase/Doris 表名缺省时回退到 DDL 表名（同名表场景） */
        public static Config from(ReadableConfig c, String ddlTableName) {
            Config cfg = new Config();

            cfg.primary = normalize(c.get(PRIMARY));
            cfg.standby = SRC_HBASE.equals(cfg.primary) ? SRC_DORIS : SRC_HBASE;
            cfg.timeoutMs = c.get(LOOKUP_TIMEOUT);

            cfg.batchSize = c.get(BATCH_SIZE);
            cfg.batchMaxWaitMs = c.get(BATCH_MAX_WAIT);
            cfg.statsLogIntervalSec = c.get(STATS_LOG_INTERVAL);

            cfg.hbaseTableName = c.getOptional(HBASE_TABLE_NAME).orElse(ddlTableName);
            cfg.hbaseZkQuorum = c.getOptional(HBASE_ZK_QUORUM).orElse("");
            cfg.hbaseZkPort = c.get(HBASE_ZK_PORT);
            cfg.hbaseZnodeParent = c.get(HBASE_ZNODE_PARENT);
            cfg.hbaseColumnFamily = c.get(HBASE_COLUMN_FAMILY);
            cfg.hbaseRowkeyDelimiter = c.get(HBASE_ROWKEY_DELIMITER);
            cfg.hbaseRpcTimeoutMs = c.get(HBASE_RPC_TIMEOUT_MS);
            cfg.hbaseOperationTimeoutMs = c.get(HBASE_OPERATION_TIMEOUT_MS);
            cfg.hbaseRetries = c.get(HBASE_RETRIES);
            cfg.hbaseSecurityAuthentication = c.get(HBASE_SECURITY_AUTHENTICATION);
            cfg.hbaseClientKeytabFile = c.getOptional(HBASE_CLIENT_KEYTAB_FILE).orElse("");
            cfg.hbaseClientKerberosPrincipal = c.getOptional(HBASE_CLIENT_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseRegionserverKerberosPrincipal = c.getOptional(HBASE_REGIONSERVER_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseMasterKerberosPrincipal = c.getOptional(HBASE_MASTER_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseKrb5Conf = c.getOptional(HBASE_KRB5_CONF).orElse("");

            cfg.dorisJdbcUrl = c.get(DORIS_JDBC_URL);
            cfg.dorisTableName = c.getOptional(DORIS_TABLE_NAME).orElse(ddlTableName);
            cfg.dorisUsername = c.get(DORIS_USERNAME);
            cfg.dorisPassword = c.get(DORIS_PASSWORD);
            cfg.dorisDriver = c.get(DORIS_DRIVER);
            cfg.dorisQueryTimeoutSec = c.get(DORIS_QUERY_TIMEOUT_SEC);
            cfg.dorisPoolSize = c.get(DORIS_POOL_SIZE);
            cfg.dorisPoolMinIdle = c.get(DORIS_POOL_MIN_IDLE);
            cfg.dorisConnectTimeoutMs = c.get(DORIS_CONNECT_TIMEOUT_MS);
            cfg.dorisPoolIdleTimeoutMs = c.get(DORIS_POOL_IDLE_TIMEOUT_MS);
            cfg.dorisPoolMaxLifetimeMs = c.get(DORIS_POOL_MAX_LIFETIME_MS);
            cfg.dorisPoolValidationTimeoutMs = c.get(DORIS_POOL_VALIDATION_TIMEOUT_MS);

            if (cfg.timeoutMs <= 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.timeout 必须大于 0");
            }
            if (cfg.batchSize <= 0 || cfg.batchMaxWaitMs < 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.batch.size 必须大于 0，batch.max-wait 不能为负");
            }
            if (cfg.dorisPoolMinIdle > cfg.dorisPoolSize) {
                throw new IllegalArgumentException("[dual-lookup] doris.pool.min-idle 不能大于 doris.pool.size");
            }
            if (cfg.dorisJdbcUrl == null || cfg.dorisJdbcUrl.trim().isEmpty()) {
                throw new IllegalArgumentException("[dual-lookup] doris.jdbc-url 必填");
            }
            if ("kerberos".equalsIgnoreCase(cfg.hbaseSecurityAuthentication)
                    && (cfg.hbaseClientKeytabFile.trim().isEmpty()
                    || cfg.hbaseClientKerberosPrincipal.trim().isEmpty())) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.security.authentication=kerberos 时，"
                                + "hbase.client.keytab.file 与 hbase.client.kerberos.principal 必填");
            }
            return cfg;
        }

        private static String normalize(String raw) {
            String v = raw == null ? "" : raw.trim().toLowerCase();
            if (!SRC_HBASE.equals(v) && !SRC_DORIS.equals(v)) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.primary 只能取值 HBase 或 Doris，当前为: " + raw);
            }
            return v;
        }
    }
}
