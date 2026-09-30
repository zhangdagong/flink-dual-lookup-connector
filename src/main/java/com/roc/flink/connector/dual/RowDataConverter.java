package com.roc.flink.connector.dual;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;

/**
 * Flink 内部数据结构（RowData）与 Java 通用对象之间的双向转换器。
 *
 * <p>这是"HBase / Doris 两源可互相替换"的关键：两个 Reader 都把结果统一成
 * {@code Map<列名小写, Java对象>}，再由本类按 DDL 声明的类型转成 RowData。
 * 只要两源表结构与 DDL 一致，切换后输出完全等价。
 *
 * <p>共提供三个方向的转换：
 * <ol>
 *   <li><b>入参方向</b>（{@link #toJavaValue}）：Flink 内部值（{@link StringData} /
 *       {@link DecimalData} / {@link TimestampData} ...）→ Java 对象，
 *       供 HBase 拼 rowkey、JDBC 绑定参数使用；</li>
 *   <li><b>出参方向</b>（{@link #toRowData}）：后端返回的 Java 对象 → RowData，
 *       交给 Lookup Join 做后续处理；</li>
 *   <li><b>列名解析</b>（{@link #resolveValue}）：处理元字段与大小写差异。</li>
 * </ol>
 *
 * <p>为什么内部值不能直接当 Java 对象用？因为 Flink 为了性能把字符串、Decimal、Timestamp
 * 都做成了可复用的二进制/可变实现，跨源传递（尤其经过 HBase 与 JDBC 两个外部系统）时必须
 * 落到标准的 Java 类型，否则会出现「引用被复用导致数据串行」的诡异问题。
 */
public class RowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 约定元字段名：用户在 DDL 里声明这两个字段，即可输出数据来源和查询耗时 */
    public static final String SOURCE_FIELD = "lookup_source";
    public static final String COST_FIELD = "lookup_cost_ms";

    /**
     * 元信息在结果 Map 里的内部键（加前后双下划线前缀，避免和业务列名冲突）。
     *
     * <p>为什么需要这层间接？因为 RowData 的字段顺序由 DDL 决定，而元字段可以出现在 DDL 任意位置，
     * 用固定内部键存放，取值时就不必关心它排在第几列。
     */
    public static final String META_SOURCE_KEY = "__dual_source__";
    public static final String META_COST_KEY = "__dual_cost_ms__";

    /** 判断某个 DDL 列名是否为元字段（这些列不查后端，由运行时填充） */
    public static boolean isMetaField(String name) {
        return SOURCE_FIELD.equalsIgnoreCase(name) || COST_FIELD.equalsIgnoreCase(name);
    }

    /** DDL 声明的完整列名（含元字段，顺序即 RowData 的字段顺序） */
    private final String[] fieldNames;
    /** DDL 声明的完整列类型，与 fieldNames 一一对应 */
    private final LogicalType[] fieldTypes;
    /**
     * 列名的小写副本。两个 Reader 写入结果 Map 时都用小写 key，而 DDL 里的列名大小写由用户决定，
     * 所以取值时总要走一次「小写兜底」；构造期算好可避免「每一行 × 每一列」都新建一个字符串。
     */
    private final String[] fieldNamesLower;

    public RowDataConverter(String[] fieldNames, LogicalType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.fieldNamesLower = new String[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            this.fieldNamesLower[i] = fieldNames[i].toLowerCase();
        }
    }

    /**
     * 把外部查询结果转成 Lookup Join 需要的 RowData。
     *
     * <p>逐列取值再统一转成内部表示；缺列/空值会得到 null，
     * LEFT JOIN 场景下由 Flink 补 NULL，无需在此特殊处理。
     */
    public RowData toRowData(Map<String, Object> values) {
        Object[] row = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            row[i] = toInternal(resolveValue(values, fieldNames[i], fieldNamesLower[i]), fieldTypes[i]);
        }
        // 必须显式指定 RowKind.INSERT：Lookup Join 会据此判断这是「查到的行」而非更新/删除
        return GenericRowData.ofKind(org.apache.flink.types.RowKind.INSERT, row);
    }

    /**
     * 元字段从元键取值，业务字段按列名（含小写兼容）取值。
     *
     * <p>大小写兼容的原因：HBase 的列名由用户建表时决定、Doris 的列名由 SQL 返回，
     * 两端大小写习惯可能不同，统一转小写后查一次可省掉大小写对齐的运维负担
     * （两个 Reader 在写入 Map 时也都用小写列名）。
     */
    private Object resolveValue(Map<String, Object> values, String fieldName, String fieldNameLower) {
        if (SOURCE_FIELD.equalsIgnoreCase(fieldName)) {
            return values.get(META_SOURCE_KEY);
        }
        if (COST_FIELD.equalsIgnoreCase(fieldName)) {
            return values.get(META_COST_KEY);
        }
        Object v = values.get(fieldName);
        if (v == null) {
            // 大小写不敏感兜底：DDL 写 ACCOUNT_NO、后端返回 account_no 时也能命中
            v = values.get(fieldNameLower);
        }
        return v;
    }

    /**
     * 把 Flink 内部的 join key 值转成 Java 对象，供 HBase Rowkey 拼接 / JDBC 参数绑定。
     *
     * <p>注意与 {@link #toInternal} 的方向相反，且这里必须产出「标准 Java 类型」：
     * JDBC 的 {@code setObject} 与 HBase 的 rowkey 拼接都只认标准类型。
     */
    public Object toJavaValue(Object internal, LogicalType type) {
        if (internal == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // StringData 是可变实现，toString() 得到不可变的独立字符串副本，跨线程/跨调用安全
                return internal.toString();
            case BOOLEAN:
                return internal;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                // 数值类型内部表示已是 Java 包装类型（Boolean/Byte/Integer/Long/Float/Double），直接透传
                return internal;
            case DECIMAL:
                // 转 BigDecimal，JDBC 才能正确绑定 DECIMAL 类型（否则精度/标度可能丢失）
                return ((DecimalData) internal).toBigDecimal();
            case DATE:
                // Flink 内部 DATE 存的是「距 1970-01-01 的天数（int）」
                return Date.valueOf(LocalDate.ofEpochDay((Integer) internal));
            case TIME_WITHOUT_TIME_ZONE:
                // Flink 内部 TIME 存的是「当天零点起的毫秒数（int）」，需乘 1e6 转纳秒
                return Time.valueOf(LocalTime.ofNanoOfDay(((Integer) internal) * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // 转 java.sql.Timestamp，JDBC 4.2 之后也推荐用 LocalDateTime，但这里保持 JDBC 兼容性更好
                return ((TimestampData) internal).toTimestamp();
            case BINARY:
            case VARBINARY:
                return internal;
            default:
                // 兜底：一律按字符串处理，避免新类型导致运行期崩溃
                return internal.toString();
        }
    }

    /**
     * Java 对象 -> Flink 内部表示。
     *
     * <p>这是「后端异构 → Flink 统一」的收口点：HBase 返回 byte[] 解码出的 Java 对象、
     * Doris 返回 JDBC 类型，都经由这里归一化。
     */
    private Object toInternal(Object v, LogicalType type) {
        if (v == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(v.toString());
            case BOOLEAN:
                return toBoolean(v);
            case TINYINT:
                return ((Number) v).byteValue();
            case SMALLINT:
                return ((Number) v).shortValue();
            case INTEGER:
                return ((Number) v).intValue();
            case BIGINT:
                return ((Number) v).longValue();
            case FLOAT:
                return ((Number) v).floatValue();
            case DOUBLE:
                return ((Number) v).doubleValue();
            case DECIMAL:
                // 必须用 DDL 声明的 precision/scale：DecimalData 是定长表示，
                // 标度不符会导致后续比较/计算出现精度问题
                DecimalType dt = (DecimalType) type;
                return DecimalData.fromBigDecimal(toBigDecimal(v), dt.getPrecision(), dt.getScale());
            case DATE:
                return toDate(v);
            case TIME_WITHOUT_TIME_ZONE:
                return toTime(v);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromTimestamp(toTimestamp(v, type));
            case BINARY:
            case VARBINARY:
                return toBytes(v);
            default:
                return StringData.fromString(v.toString());
        }
    }

    // ---------------- 内部工具 ----------------
    // 下面几个方法都在做同一件事：把「不同源可能给出的不同 Java 表示」收敛成统一形式。
    // HBase 侧给出 byte[] 解码结果，Doris 侧给出 JDBC 结果，二者的具体类型并不一致。

    /** 兼容 Boolean / 数值 / 字符串三种入参形式（Doris 的 TINYINT(1) 可能返回数字） */
    private static boolean toBoolean(Object v) {
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue() != 0;
        }
        return Boolean.parseBoolean(v.toString());
    }

    /** 统一转 BigDecimal（注意：走 double 分支会有精度损失风险，仅作为非精确类型的兜底路径） */
    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Number) {
            return BigDecimal.valueOf(((Number) v).doubleValue());
        }
        return new BigDecimal(v.toString().trim());
    }

    /** -> Flink 内部 DATE（距 epoch 的天数，int） */
    private static int toDate(Object v) {
        if (v instanceof LocalDate) {
            return (int) ((LocalDate) v).toEpochDay();
        }
        if (v instanceof Date) {
            return (int) ((Date) v).toLocalDate().toEpochDay();
        }
        if (v instanceof Number) {
            // HBase 里的 DATE 本身就是按天数的 int 存储，直接使用
            return ((Number) v).intValue();
        }
        return (int) LocalDate.parse(v.toString().trim()).toEpochDay();
    }

    /** -> Flink 内部 TIME（当天零点起的毫秒数，int） */
    private static int toTime(Object v) {
        LocalTime lt;
        if (v instanceof Time) {
            lt = ((Time) v).toLocalTime();
        } else if (v instanceof LocalTime) {
            lt = (LocalTime) v;
        } else if (v instanceof Number) {
            // HBase 里的 TIME 已是毫秒数，直接用
            return ((Number) v).intValue();
        } else {
            lt = LocalTime.parse(v.toString().trim());
        }
        return (int) (lt.toNanoOfDay() / 1_000_000L);
    }

    /**
     * -> java.sql.Timestamp。
     *
     * <p>这里的分支最杂，因为时间戳在不同源的表示差异最大：
     * HBase 存 long（且精度 >3 时是微秒）、Doris 返回 Timestamp 或字符串，都要能兜住。
     */
    private static Timestamp toTimestamp(Object v, LogicalType type) {
        if (v instanceof Timestamp) {
            return (Timestamp) v;
        }
        if (v instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) v);
        }
        if (v instanceof Long) {
            // Long 的语义取决于 DDL 精度：precision<=3 按毫秒，>3 按微秒（HBase 常见存法）
            int precision = (type instanceof TimestampType) ? ((TimestampType) type).getPrecision() : 6;
            if (precision <= 3) {
                return new Timestamp((Long) v);
            }
            long millis = (Long) v / 1000;
            return new Timestamp(millis);
        }
        if (v instanceof Number) {
            return new Timestamp(((Number) v).longValue());
        }
        String s = v.toString().trim();
        // 兼容 "yyyy-MM-dd HH:mm:ss" 与 ISO 的 "yyyy-MM-ddTHH:mm:ss" 两种写法。
        // 不能只判 length == 19：带小数秒的 ISO 串（如 ...T12:30:15.123456，长度 26）
        // 会漏掉替换，截断后仍带着 'T'，Timestamp.valueOf 直接抛异常
        if (s.length() > 10 && s.charAt(10) == 'T') {
            s = s.replace('T', ' ');
        }
        // 截断到秒：Timestamp.valueOf 无法解析小数秒之后的内容，多余部分直接丢弃
        return Timestamp.valueOf(s.length() > 19 ? s.substring(0, 19) : s);
    }

    /** -> byte[]（已是的直接返回，否则按 UTF-8 编码） */
    private static byte[] toBytes(Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        return v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
