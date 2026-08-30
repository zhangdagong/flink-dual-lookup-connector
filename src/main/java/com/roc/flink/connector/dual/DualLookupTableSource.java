package com.roc.flink.connector.dual;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

/**
 * 双源容错维表 Source。对 SQL 完全透明：DDL 里写 {@code connector = 'dual-lookup'}，
 * 关联时正常写 {@code FOR SYSTEM_TIME AS OF}，主备切换、超时降级都发生在算子内部。
 */
public class DualLookupTableSource implements LookupTableSource {

    private final DualLookupOptions.Config cfg;
    private final String tableName;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    public DualLookupTableSource(DualLookupOptions.Config cfg,
                                 String tableName,
                                 String[] fieldNames,
                                 LogicalType[] fieldTypes) {
        this.cfg = cfg;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // getKeys() 返回键索引路径 int[][]（支持嵌套结构）；本连接器只支持普通非嵌套主键
        int[][] keyPaths = context.getKeys();

        // 主键列名（Doris 生成 WHERE 条件用）+ 主键类型（键值转换用）
        String[] keyNames = new String[keyPaths.length];
        LogicalType[] keyTypes = new LogicalType[keyPaths.length];
        for (int i = 0; i < keyPaths.length; i++) {
            int[] path = keyPaths[i];
            if (path.length != 1) {
                throw new IllegalArgumentException(
                        "[dual-lookup] 不支持嵌套主键，请使用普通列作为 PRIMARY KEY");
            }
            keyNames[i] = fieldNames[path[0]];
            keyTypes[i] = fieldTypes[path[0]];
        }

        RowDataConverter converter = new RowDataConverter(fieldNames, fieldTypes);

        // 剥离元字段（lookup_source / lookup_cost_ms），它们不查后端，由运行时填充
        List<String> bizNames = new ArrayList<>();
        List<LogicalType> bizTypes = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; i++) {
            if (!RowDataConverter.isMetaField(fieldNames[i])) {
                bizNames.add(fieldNames[i]);
                bizTypes.add(fieldTypes[i]);
            }
        }
        String[] bizNameArr = bizNames.toArray(new String[0]);
        LogicalType[] bizTypeArr = bizTypes.toArray(new LogicalType[0]);

        // 两个 Reader 都构造，但惰性建连：真正查询时才建对应源的连接
        LookupReader hbase = new HBaseLookupReader(cfg, bizNameArr, bizTypeArr);
        LookupReader doris = new DorisLookupReader(cfg, bizNameArr, keyNames);

        boolean hbasePrimary = DualLookupOptions.SRC_HBASE.equals(cfg.primary);
        LookupReader primary = hbasePrimary ? hbase : doris;
        LookupReader standby = hbasePrimary ? doris : hbase;

        return AsyncLookupFunctionProvider.of(
                new DualLookupFunction(cfg, tableName, converter, keyTypes, primary, standby));
    }

    @Override
    public DynamicTableSource copy() {
        return new DualLookupTableSource(cfg, tableName, fieldNames, fieldTypes);
    }

    @Override
    public String asSummaryString() {
        return "dual-lookup(primary=" + cfg.primary + ", standby=" + cfg.standby + ")";
    }
}
