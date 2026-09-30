package com.roc.flink.connector.dual;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * RowDataConverter 的测试。
 *
 * <p>这是「HBase / Doris 两源结果可以互相替换」的收口点，转换错一处就是静默的脏数据，
 * 所以把三条转换路径都固定下来：
 * <ol>
 *   <li>入参方向 toJavaValue：Flink 内部值 -> 标准 Java 对象（供 rowkey / JDBC 绑定）；</li>
 *   <li>出参方向 toRowData：后端返回的异构 Java 对象 -> Flink 内部表示；</li>
 *   <li>列名解析 resolveValue：元字段填充与大小写兼容。</li>
 * </ol>
 */
public class RowDataConverterTest {

    private static RowDataConverter conv(String[] names, LogicalType[] types) {
        return new RowDataConverter(names, types);
    }

    // ---------------- 入参方向 ----------------

    @Test
    public void toJavaValueConvertsInternalValuesToStandardJava() {
        RowDataConverter c = conv(new String[]{"x"}, new LogicalType[]{new VarCharType()});

        // StringData 是可变实现，必须转成独立的 String 才能跨源/跨线程传递
        Object s = c.toJavaValue(StringData.fromString("abc"), new VarCharType());
        assertEquals("abc", s);
        assertEquals(String.class, s.getClass());

        // DecimalData -> BigDecimal：JDBC 绑定 DECIMAL 必须用它，否则精度/标度可能丢失
        assertEquals(new BigDecimal("12.34"),
                c.toJavaValue(DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
                        new DecimalType(10, 2)));

        // Flink 内部 DATE = 距 epoch 的天数（int）
        int epochDay = (int) LocalDate.of(2026, 9, 29).toEpochDay();
        assertEquals(Date.valueOf(LocalDate.of(2026, 9, 29)),
                c.toJavaValue(epochDay, new DateType()));

        // Flink 内部 TIME = 当天零点起的毫秒数（int）
        int msOfDay = 3 * 3600_000 + 4 * 60_000 + 5_000;
        assertEquals(Time.valueOf(LocalTime.of(3, 4, 5)),
                c.toJavaValue(msOfDay, new TimeType()));

        // TimestampData -> java.sql.Timestamp
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2026, 9, 29, 12, 30, 15));
        assertEquals(ts, c.toJavaValue(TimestampData.fromTimestamp(ts), new TimestampType(3)));

        // BINARY 原样透传
        byte[] raw = {1, 2, 3};
        assertArrayEquals(raw, (byte[]) c.toJavaValue(raw, new VarBinaryType()));

        assertNull(c.toJavaValue(null, new VarCharType()));
    }

    // ---------------- 列名解析与元字段 ----------------

    @Test
    public void metaFieldDetectionIsCaseInsensitive() {
        assertTrue(RowDataConverter.isMetaField("lookup_source"));
        assertTrue(RowDataConverter.isMetaField("LOOKUP_SOURCE"));
        assertTrue(RowDataConverter.isMetaField("Lookup_Cost_Ms"));
        assertFalse(RowDataConverter.isMetaField("party_name"));
    }

    @Test
    public void toRowDataResolvesCaseInsensitivelyAndFillsMetaFields() {
        RowDataConverter c = conv(
                new String[]{"ACCOUNT_NO", "party_name",
                        RowDataConverter.SOURCE_FIELD, RowDataConverter.COST_FIELD},
                new LogicalType[]{new VarCharType(), new VarCharType(),
                        new VarCharType(), new BigIntType()});

        Map<String, Object> row = new HashMap<>();
        row.put("account_no", "A-1"); // 后端统一小写 key，DDL 却是大写声明
        row.put("party_name", "某公司");
        row.put(RowDataConverter.META_SOURCE_KEY, "hbase");
        row.put(RowDataConverter.META_COST_KEY, 7L);

        RowData out = c.toRowData(row);
        assertEquals("A-1", out.getString(0).toString());
        assertEquals("某公司", out.getString(1).toString());
        assertEquals("hbase", out.getString(2).toString());
        assertEquals(7L, out.getLong(3));
    }

    // ---------------- 出参方向 ----------------

    @Test
    public void toRowDataNormalizesHeterogeneousBackendTypes() {
        RowDataConverter c = conv(
                new String[]{"flag", "cnt", "amount", "ratio", "big"},
                new LogicalType[]{new BooleanType(), new IntType(), new DecimalType(10, 2),
                        new DoubleType(), new BigIntType()});

        Map<String, Object> row = new HashMap<>();
        row.put("flag", 1);                          // Doris 的 TINYINT(1) 以数字形式返回
        row.put("cnt", 42L);                         // Long -> int 窄化
        row.put("amount", new BigDecimal("123.4"));  // 标度不符 -> 按 DDL 的 scale=2 规整
        row.put("ratio", 0.5d);
        row.put("big", 99);                          // Integer -> long  widening

        RowData out = c.toRowData(row);
        assertTrue(out.getBoolean(0));
        assertEquals(42, out.getInt(1));
        assertEquals(new BigDecimal("123.40"), out.getDecimal(2, 10, 2).toBigDecimal());
        assertEquals(0.5d, out.getDouble(3), 0.0);
        assertEquals(99L, out.getLong(4));
    }

    @Test
    public void dateAndTimeAcceptMultipleBackendRepresentations() {
        RowDataConverter c = conv(
                new String[]{"d", "t"},
                new LogicalType[]{new DateType(), new TimeType()});

        int epochDay = (int) LocalDate.of(2026, 9, 29).toEpochDay();
        int nineAmMs = 9 * 3600_000;

        // Doris 形态：java.sql.Date / 毫秒数 int
        Map<String, Object> row = new HashMap<>();
        row.put("d", Date.valueOf("2026-09-29"));
        row.put("t", nineAmMs);
        RowData out = c.toRowData(row);
        assertEquals(epochDay, out.getInt(0));
        assertEquals(nineAmMs, out.getInt(1));

        // HBase 形态：天数 int / 字符串
        Map<String, Object> row2 = new HashMap<>();
        row2.put("d", epochDay);
        row2.put("t", "09:00:00");
        RowData out2 = c.toRowData(row2);
        assertEquals(epochDay, out2.getInt(0));
        assertEquals(nineAmMs, out2.getInt(1));
    }

    @Test
    public void timestampConversionCoversAllBackendRepresentations() {
        RowDataConverter c = conv(
                new String[]{"ts3", "ts6"},
                new LogicalType[]{new TimestampType(3), new TimestampType(6)});

        // 注意：TIMESTAMP_WITHOUT_TIME_ZONE 的 TimestampData 内部按「墙钟时间」存储，
        // getMillisecond() 会把墙钟当 UTC 纪元返回（JVM 时区为 UTC+8 时差 8 小时），
        // 因此断言必须用 toLocalDateTime() 比墙钟，而不是比毫秒
        LocalDateTime expected = LocalDateTime.of(2026, 9, 29, 12, 30, 15);
        long millis = Timestamp.valueOf("2026-09-29 12:30:15").getTime();

        // Long 的语义取决于 DDL 精度：precision<=3 按毫秒、>3 按微秒（HBase 常见存法）
        Map<String, Object> row = new HashMap<>();
        row.put("ts3", millis);
        row.put("ts6", millis * 1000);
        RowData out = c.toRowData(row);
        assertEquals(expected, out.getTimestamp(0, 3).toLocalDateTime());
        assertEquals(expected, out.getTimestamp(1, 6).toLocalDateTime());

        // 字符串形态：ISO 的 'T' 分隔 + 小数秒截断；空格分隔原样解析
        Map<String, Object> row2 = new HashMap<>();
        row2.put("ts3", "2026-09-29T12:30:15.123456"); // 曾在此抛异常：'T' 替换被 length==19 漏掉
        row2.put("ts6", "2026-09-29 12:30:15");
        RowData out2 = c.toRowData(row2);
        assertEquals(expected, out2.getTimestamp(0, 3).toLocalDateTime());
        assertEquals(expected, out2.getTimestamp(1, 6).toLocalDateTime());

        // java.sql.Timestamp 原样透传
        Map<String, Object> row3 = new HashMap<>();
        row3.put("ts3", new Timestamp(millis));
        row3.put("ts6", new Timestamp(millis));
        RowData out3 = c.toRowData(row3);
        assertEquals(expected, out3.getTimestamp(0, 3).toLocalDateTime());
        assertEquals(expected, out3.getTimestamp(1, 6).toLocalDateTime());
    }

    @Test
    public void missingColumnsBecomeNull() {
        RowDataConverter c = conv(
                new String[]{"a", "b"},
                new LogicalType[]{new VarCharType(), new IntType()});
        RowData out = c.toRowData(new HashMap<>());
        assertTrue("缺列应得到 NULL，由 LEFT JOIN 语义兜底", out.isNullAt(0));
        assertTrue(out.isNullAt(1));
    }
}
