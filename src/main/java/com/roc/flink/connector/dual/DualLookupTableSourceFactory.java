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
 */
public class DualLookupTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return DualLookupOptions.CONNECTOR_ID;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        if (!context.getCatalogTable().getResolvedSchema().getPrimaryKey().isPresent()) {
            throw new IllegalArgumentException(
                    "[dual-lookup] 维表必须声明 PRIMARY KEY，Lookup Join 依赖主键做点查。"
                            + " 请在 DDL 中加上 PRIMARY KEY (...) NOT ENFORCED");
        }

        ReadableConfig config = helper.getOptions();
        String tableName = context.getObjectIdentifier().getObjectName();

        // 物理列 => 字段名 / 字段类型，与 Lookup Join 传进来的 RowData 对齐
        RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();
        List<String> nameList = rowType.getFieldNames();
        List<LogicalType> typeList = rowType.getChildren();
        String[] fieldNames = nameList.toArray(new String[0]);
        LogicalType[] fieldTypes = typeList.toArray(new LogicalType[0]);

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(config, tableName);
        return new DualLookupTableSource(cfg, tableName, fieldNames, fieldTypes);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> required = new LinkedHashSet<>();
        required.add(DualLookupOptions.DORIS_JDBC_URL);
        return required;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optional = new LinkedHashSet<>();
        optional.add(DualLookupOptions.PRIMARY);
        optional.add(DualLookupOptions.LOOKUP_TIMEOUT);
        optional.add(DualLookupOptions.BATCH_SIZE);
        optional.add(DualLookupOptions.BATCH_MAX_WAIT);
        optional.add(DualLookupOptions.STATS_LOG_INTERVAL);

        optional.add(DualLookupOptions.HBASE_TABLE_NAME);
        optional.add(DualLookupOptions.HBASE_ZK_QUORUM);
        optional.add(DualLookupOptions.HBASE_ZK_PORT);
        optional.add(DualLookupOptions.HBASE_ZNODE_PARENT);
        optional.add(DualLookupOptions.HBASE_COLUMN_FAMILY);
        optional.add(DualLookupOptions.HBASE_ROWKEY_DELIMITER);
        optional.add(DualLookupOptions.HBASE_RPC_TIMEOUT_MS);
        optional.add(DualLookupOptions.HBASE_OPERATION_TIMEOUT_MS);
        optional.add(DualLookupOptions.HBASE_RETRIES);
        optional.add(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION);
        optional.add(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE);
        optional.add(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_MASTER_KERBEROS_PRINCIPAL);
        optional.add(DualLookupOptions.HBASE_KRB5_CONF);

        optional.add(DualLookupOptions.DORIS_TABLE_NAME);
        optional.add(DualLookupOptions.DORIS_USERNAME);
        optional.add(DualLookupOptions.DORIS_PASSWORD);
        optional.add(DualLookupOptions.DORIS_DRIVER);
        optional.add(DualLookupOptions.DORIS_QUERY_TIMEOUT_SEC);
        optional.add(DualLookupOptions.DORIS_POOL_SIZE);
        optional.add(DualLookupOptions.DORIS_POOL_MIN_IDLE);
        optional.add(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS);
        optional.add(DualLookupOptions.DORIS_POOL_IDLE_TIMEOUT_MS);
        optional.add(DualLookupOptions.DORIS_POOL_MAX_LIFETIME_MS);
        optional.add(DualLookupOptions.DORIS_POOL_VALIDATION_TIMEOUT_MS);
        return optional;
    }
}
