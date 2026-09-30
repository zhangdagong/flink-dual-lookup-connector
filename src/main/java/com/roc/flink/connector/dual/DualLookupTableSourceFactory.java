package com.roc.flink.connector.dual;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * dual-lookup connector 的 SPI 工厂。
 * 通过 {@code META-INF/services/org.apache.flink.table.factories.Factory} 注册，
 * 放到 $FLINK_HOME/lib 下即可被 SQL Client / 作业自动发现。
 *
 * <p>本类是「DDL -> 连接器对象」的唯一入口，承担三件事：
 * <ol>
 *   <li>声明 connector 标识符，让 Flink 按 {@code 'connector' = 'dual-lookup'} 路由到这里；</li>
 *   <li>声明必填/选填配置项，配合 {@link FactoryUtil.TableFactoryHelper#validate()} 做拼写与合法性校验；</li>
 *   <li>把 DDL 的 schema（列、类型、主键）与配置组装成 {@link DynamicTableSource}。</li>
 * </ol>
 */
public class DualLookupTableSourceFactory implements DynamicTableSourceFactory {

    /** Flink 用该返回值匹配 DDL 中的 'connector' 值 */
    @Override
    public String factoryIdentifier() {
        return DualLookupOptions.CONNECTOR_ID;
    }

    /**
     * 由 DDL 构建 TableSource。本方法在建表阶段（SQL 解析/校验期）被调用，
     * 因此这里抛出的异常会立刻反馈给用户，而不是等到作业运行才暴露。
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // validate() 会做两件事：(1) 检查是否漏了必填项；(2) 检查是否出现未声明的 key（防拼写错误，如 lookup.timeoutt）
        helper.validate();

        // 主键强校验：Lookup Join 的本质是「按 key 点查」，没有主键就无法构成点查语义，
        // 提前拦下比运行期报错更友好
        if (!context.getCatalogTable().getResolvedSchema().getPrimaryKey().isPresent()) {
            throw new IllegalArgumentException(
                    "[dual-lookup] 维表必须声明 PRIMARY KEY，Lookup Join 依赖主键做点查。"
                            + " 请在 DDL 中加上 PRIMARY KEY (...) NOT ENFORCED");
        }

        ReadableConfig config = helper.getOptions();
        // 用 DDL 表名而非完整 identifier：HBase/Doris 表名缺省时回退到它（同名表场景）
        String tableName = context.getObjectIdentifier().getObjectName();

        // 物理列 => 字段名 / 字段类型，与 Lookup Join 传进来的 RowData 对齐
        // 注意用的是 PhysicalRowDataType（只含物理列，不含计算列），
        // 因为运行期拿到的 RowData 只包含物理列
        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        List<String> nameList = rowType.getFieldNames();
        List<LogicalType> typeList = rowType.getChildren();
        String[] fieldNames = nameList.toArray(new String[0]);
        LogicalType[] fieldTypes = typeList.toArray(new LogicalType[0]);

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(config, tableName);
        return new DualLookupTableSource(cfg, tableName, fieldNames, fieldTypes);
    }

    /**
     * 必填项。这里只声明 doris.jdbc-url 一个，因为：
     * 备源（Doris）是本连接器容错能力的下限——它一挂，主源故障就没有退路了；
     * 而 HBase 相关参数允许留空（走 classpath 下 hbase-site.xml），
     * 且表名可由 DDL 表名回退，故都不设为必填。
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new LinkedHashSet<>();
        required.add(DualLookupOptions.DORIS_JDBC_URL);
        return required;
    }

    /**
     * 选填项：必须把每个 {@link DualLookupOptions} 里的 ConfigOption 都登记进来，
     * 否则 Flink 的 validate() 会把 DDL 中写的该项判为「未知配置」直接报错。
     * 这也是升级连接器时最容易漏的一步——新增配置项后务必同步在这里登记。
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optional = new LinkedHashSet<>();
        // 核心：主备路由、超时、攒批、统计
        optional.add(DualLookupOptions.PRIMARY);
        optional.add(DualLookupOptions.LOOKUP_TIMEOUT);
        optional.add(DualLookupOptions.BATCH_SIZE);
        optional.add(DualLookupOptions.BATCH_MAX_WAIT);
        optional.add(DualLookupOptions.STATS_LOG_INTERVAL);
        optional.add(DualLookupOptions.FAILOVER_LOG_INTERVAL);

        // HBase 连接与超时
        optional.add(DualLookupOptions.HBASE_TABLE_NAME);
        optional.add(DualLookupOptions.HBASE_ZK_QUORUM);
        optional.add(DualLookupOptions.HBASE_ZK_PORT);
        optional.add(DualLookupOptions.HBASE_ZNODE_PARENT);
        optional.add(DualLookupOptions.HBASE_COLUMN_FAMILY);
        optional.add(DualLookupOptions.HBASE_ROWKEY_DELIMITER);
        optional.add(DualLookupOptions.HBASE_ROWKEY_ENCODING);
        optional.add(DualLookupOptions.HBASE_RPC_TIMEOUT_MS);
        optional.add(DualLookupOptions.HBASE_OPERATION_TIMEOUT_MS);
        optional.add(DualLookupOptions.HBASE_RETRIES);
        // HBase Kerberos
        optional.add(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION);
        optional.add(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE);
        optional.add(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_MASTER_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_KRB5_CONF);

        // Doris 连接、驱动与超时
        optional.add(DualLookupOptions.DORIS_TABLE_NAME);
        optional.add(DualLookupOptions.DORIS_USERNAME);
        optional.add(DualLookupOptions.DORIS_PASSWORD);
        optional.add(DualLookupOptions.DORIS_DRIVER);
        optional.add(DualLookupOptions.DORIS_QUERY_TIMEOUT_SEC);
        // Doris 连接池（HikariCP）
        optional.add(DualLookupOptions.DORIS_POOL_SIZE);
        optional.add(DualLookupOptions.DORIS_POOL_MIN_IDLE);
        optional.add(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS);
        optional.add(DualLookupOptions.DORIS_POOL_IDLE_TIMEOUT_MS);
        optional.add(DualLookupOptions.DORIS_POOL_MAX_LIFETIME_MS);
        optional.add(DualLookupOptions.DORIS_POOL_VALIDATION_TIMEOUT_MS);
        return optional;
    }
}
