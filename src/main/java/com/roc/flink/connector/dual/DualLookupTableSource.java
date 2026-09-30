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
 *
 * <p>本类只做「装配」：把 DDL 的列信息、主键信息、配置组合成运行期需要的 Reader 与 Function，
 * 不含任何查询/容错逻辑（那些都在 {@link DualLookupFunction} 与两个 Reader 里）。
 * 保持这一层足够薄，是为了让「配置 -> 组件」的对应关系一眼可见。
 */
public class DualLookupTableSource implements LookupTableSource {

    /** 已校验的配置 */
    private final DualLookupOptions.Config cfg;
    /** 维表名（用于日志与 HBase/Doris 表名缺省回退） */
    private final String tableName;
    /** DDL 声明的全部列名（含元字段，顺序即 RowData 字段顺序） */
    private final String[] fieldNames;
    /** DDL 声明的全部列类型，与 fieldNames 一一对应 */
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

    /**
     * 构建运行期 Provider。Flink 在「逻辑计划转物理计划」阶段调用本方法，
     * 因此这里做的事必须在 JobManager 侧可完成，且产出的对象要能序列化下发到 TaskManager。
     *
     * @param context 由 Flink 注入，携带本次 Lookup Join 的关联键信息（{@code context.getKeys()}）
     */
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // getKeys() 返回键索引路径 int[][]（支持嵌套结构）；本连接器只支持普通非嵌套主键
        int[][] keyPaths = context.getKeys();

        // 主键列名（Doris 生成 WHERE 条件用）+ 主键类型（键值转换用）
        String[] keyNames = new String[keyPaths.length];
        LogicalType[] keyTypes = new LogicalType[keyPaths.length];
        for (int i = 0; i < keyPaths.length; i++) {
            int[] path = keyPaths[i];
            // 形如 [0] 表示第 0 列的普通字段；形如 [0,1] 表示「第 0 列的嵌套子字段」——
            // 后者本连接器不支持（HBase rowkey 与 JDBC 参数都无法直接承载嵌套结构）
            if (path.length != 1) {
                throw new IllegalArgumentException(
                        "[dual-lookup] 不支持嵌套主键，请使用普通列作为 PRIMARY KEY");
            }
            keyNames[i] = fieldNames[path[0]];
            keyTypes[i] = fieldTypes[path[0]];
        }

        // 转换器持有完整列信息（含元字段）：输出 RowData 时必须补齐所有 DDL 列，
        // 漏掉任何一列都会让下游字段错位，因此这里传的是完整 fieldNames/fieldTypes
        RowDataConverter converter = new RowDataConverter(fieldNames, fieldTypes);

        // 剥离元字段（lookup_source / lookup_cost_ms），它们不查后端，由运行时填充。
        // 必须剥掉的原因：这两个列在 HBase/Doris 里并不存在，若不剥离，查询会带上不存在的列名，
        // 导致 HBase 查不到列、Doris 直接报 SQL 错误
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

        // 两个 Reader 都构造，但惰性建连：真正查询时才建对应源的连接。
        // 这也是「主源宕机时作业依然能起来」的前提——构造对象不产生任何网络行为
        // 二者构造参数不同是各自实现所需：
        // - HBase 需要「业务列名 + 业务列类型」按类型解码字节，还需要「主键类型」用于
        //   按类型编码 rowkey（hbase.rowkey.encoding=typed 时）
        // - Doris 只需要「列名（拼 SELECT）+ 主键列名（拼 WHERE）」，类型由 JDBC 元数据自行提供
        LookupReader hbase = new HBaseLookupReader(cfg, bizNameArr, bizTypeArr, keyTypes);
        LookupReader doris = new DorisLookupReader(cfg, bizNameArr, keyNames);

        // 按 lookup.primary 装配主备顺序：路由逻辑只认 primary/standby 两个槽位，
        // 不关心具体是哪个实现，因此「主备对调」在这里只是交换两个变量
        boolean hbasePrimary = DualLookupOptions.SRC_HBASE.equals(cfg.primary);
        LookupReader primary = hbasePrimary ? hbase : doris;
        LookupReader standby = hbasePrimary ? doris : hbase;

        return AsyncLookupFunctionProvider.of(
                new DualLookupFunction(cfg, tableName, converter, keyTypes, primary, standby));
    }

    /** 复制自身（Flink 可能对 Source 做多次拷贝/优化）；各字段均为不可变引用，浅拷贝即可 */
    @Override
    public DynamicTableSource copy() {
        return new DualLookupTableSource(cfg, tableName, fieldNames, fieldTypes);
    }

    /** 出现在 EXPLAIN 结果里的描述，便于快速确认某张表实际的主备配置 */
    @Override
    public String asSummaryString() {
        return "dual-lookup(primary=" + cfg.primary + ", standby=" + cfg.standby + ")";
    }
}
