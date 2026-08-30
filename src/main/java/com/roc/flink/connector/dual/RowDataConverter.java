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
 */
public class RowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 约定元字段名：用户在 DDL 里声明这两个字段，即可输出数据来源和查询耗时 */
    public static final String SOURCE_FIELD = "lookup_source";
    public static final String COST_FIELD = "lookup_cost_ms";

    /** 元信息在结果 Map 里的内部键（加前缀避免和业务列名冲突） */
    public static final String META_SOURCE_KEY = "__dual_source__";
    public static final String META_COST_KEY = "__dual_cost_ms__";

    /** 判断某个 DDL 列名是否为元字段（这些列不查后端，由运行时填充） */
    public static boolean isMetaField(String name) {
        return SOURCE_FIELD.equalsIgnoreCase(name) || COST_FIELD.equalsIgnoreCase(name);
    }

    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    public RowDataConverter(String[] fieldNames, LogicalType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    /** 把外部查询结果转成 Lookup Join 需要的 RowData */
    public RowData toRowData(Map<String, Object> values) {
        Object[] row = new Object[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            row[i] = toInternal(resolveValue(values, fieldNames[i]), fieldTypes[i]);
        }
        return GenericRowData.ofKind(org.apache.flink.types.RowKind.INSERT, row);
    }

    /** 元字段从元键取值，业务字段按列名（含小写兼容）取值 */
    private Object resolveValue(Map<String, Object> values, String fieldName) {
        if (SOURCE_FIELD.equalsIgnoreCase(fieldName)) {
            return values.get(META_SOURCE_KEY);
        }
        if (COST_FIELD.equalsIgnoreCase(fieldName)) {
            return values.get(META_COST_KEY);
        }
        Object v = values.get(fieldName);
        if (v == null) {
            v = values.get(fieldName.toLowerCase());
        }
        return v;
    }

    /** 把 Flink 内部的 join key 值转成 Java 对象，供 HBase Rowkey 拼接 / JDBC 参数绑定 */
    public Object toJavaValue(Object internal, LogicalType type) {
        if (internal == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return internal.toString();
            case BOOLEAN:
                return internal;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return internal;
            case DECIMAL:
                return ((DecimalData) internal).toBigDecimal();
            case DATE:
                return Date.valueOf(LocalDate.ofEpochDay((Integer) internal));
            case TIME_WITHOUT_TIME_ZONE:
                return Time.valueOf(LocalTime.ofNanoOfDay(((Integer) internal) * 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((TimestampData) internal).toTimestamp();
            case BINARY:
            case VARBINARY:
                return internal;
            default:
                return internal.toString();
        }
    }

    /** Java 对象 -> Flink 内部表示 */
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

    private static boolean toBoolean(Object v) {
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue() != 0;
        }
        return Boolean.parseBoolean(v.toString());
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal) {
            return (BigDecimal) v;
        }
        if (v instanceof Number) {
            return BigDecimal.valueOf(((Number) v).doubleValue());
        }
        return new BigDecimal(v.toString().trim());
    }

    private static int toDate(Object v) {
        if (v instanceof LocalDate) {
            return (int) ((LocalDate) v).toEpochDay();
        }
        if (v instanceof Date) {
            return (int) ((Date) v).toLocalDate().toEpochDay();
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        return (int) LocalDate.parse(v.toString().trim()).toEpochDay();
    }

    private static int toTime(Object v) {
        LocalTime lt;
        if (v instanceof Time) {
            lt = ((Time) v).toLocalTime();
        } else if (v instanceof LocalTime) {
            lt = (LocalTime) v;
        } else if (v instanceof Number) {
            return ((Number) v).intValue();
        } else {
            lt = LocalTime.parse(v.toString().trim());
        }
        return (int) (lt.toNanoOfDay() / 1_000_000L);
    }

    private static Timestamp toTimestamp(Object v, LogicalType type) {
        if (v instanceof Timestamp) {
            return (Timestamp) v;
        }
        if (v instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) v);
        }
        if (v instanceof Long) {
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
        // 兼容 "yyyy-MM-dd HH:mm:ss" 与 "yyyy-MM-ddTHH:mm:ss" 两种写法
        if (s.length() == 19 && s.charAt(10) == 'T') {
            s = s.replace('T', ' ');
        }
        return Timestamp.valueOf(s.length() > 19 ? s.substring(0, 19) : s);
    }

    private static byte[] toBytes(Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        return v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
