package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.lang.reflect.Method;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * HBase rowkey 编码的测试。
 *
 * <p>rowkey 编码选错是本连接器最难排查的故障：它表现为「一条也查不到」，
 * 而空结果不算故障、不触发降级，最终整张维表关联结果全为 NULL，日志里却没有任何异常。
 * 因此这里把两种模式的字节结果固定下来。
 */
public class HBaseRowKeyEncodingTest {

    private static DualLookupOptions.Config cfg(String encoding) {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setString(DualLookupOptions.HBASE_ROWKEY_ENCODING.key(), encoding);
        return DualLookupOptions.Config.from(c, "dim_account");
    }

    private static HBaseLookupReader reader(String encoding, LogicalType[] keyTypes) {
        return new HBaseLookupReader(cfg(encoding),
                new String[]{"c1"}, new LogicalType[]{new VarCharType()}, keyTypes);
    }

    private static byte[] rowKey(String encoding, LogicalType[] keyTypes, Object[] values) throws Exception {
        Method m = HBaseLookupReader.class.getDeclaredMethod("buildRowKey", Object[].class);
        m.setAccessible(true);
        return (byte[]) m.invoke(reader(encoding, keyTypes), (Object) values);
    }

    private static Object decode(String encoding, LogicalType[] keyTypes, byte[] v, LogicalType type)
            throws Exception {
        Method m = HBaseLookupReader.class.getDeclaredMethod("decode", byte[].class, LogicalType.class);
        m.setAccessible(true);
        return m.invoke(reader(encoding, keyTypes), v, type);
    }

    @Test
    public void stringEncodingKeepsHistoricalBehaviour() throws Exception {
        // 默认模式必须与改动前完全一致：数值主键也按字符串写（兼容既有数据）
        assertArrayEquals(Bytes.toBytes("123"),
                rowKey("string", new LogicalType[]{new BigIntType()}, new Object[]{123L}));
        assertArrayEquals(Bytes.toBytes("acc-1|7"),
                rowKey("string", new LogicalType[]{new VarCharType(), new BigIntType()},
                        new Object[]{"acc-1", 7L}));
    }

    @Test
    public void typedEncodingMatchesTypeAwareWrite() throws Exception {
        // typed 模式：与写入端按类型编码的结果逐字节一致
        assertArrayEquals(Bytes.toBytes(123L),
                rowKey("typed", new LogicalType[]{new BigIntType()}, new Object[]{123L}));
        assertArrayEquals(Bytes.toBytes(123),
                rowKey("typed", new LogicalType[]{new IntType()}, new Object[]{123}));
        assertArrayEquals(Bytes.toBytes(12.5d),
                rowKey("typed", new LogicalType[]{new DoubleType()}, new Object[]{12.5d}));
        assertArrayEquals(Bytes.toBytes("account_no"),
                rowKey("typed", new LogicalType[]{new VarCharType()}, new Object[]{"account_no"}));
    }

    @Test
    public void typedEncodingForCompositeKeyJoinsSegmentsWithDelimiter() throws Exception {
        ByteArrayOutputStream expect = new ByteArrayOutputStream();
        expect.write(Bytes.toBytes("acc-1"));
        expect.write(Bytes.toBytes("|"));
        expect.write(Bytes.toBytes(7L));
        assertArrayEquals(expect.toByteArray(),
                rowKey("typed", new LogicalType[]{new VarCharType(), new BigIntType()},
                        new Object[]{"acc-1", 7L}));
    }

    @Test
    public void typedEncodingRoundTripsWithDecode() throws Exception {
        // 编码 → 解码 应还原原值：这是「写入端与读取端同一套规则」的直接证据
        byte[] encoded = rowKey("typed", new LogicalType[]{new BigIntType()}, new Object[]{123456789L});
        assertEquals(123456789L, decode("typed", new LogicalType[]{new BigIntType()}, encoded, new BigIntType()));

        byte[] intEncoded = rowKey("typed", new LogicalType[]{new IntType()}, new Object[]{42});
        assertEquals(42, decode("typed", new LogicalType[]{new IntType()}, intEncoded, new IntType()));

        byte[] strEncoded = rowKey("typed", new LogicalType[]{new VarCharType()}, new Object[]{"abc"});
        assertEquals("abc", decode("typed", new LogicalType[]{new VarCharType()}, strEncoded, new VarCharType()));
    }

    @Test
    public void nullKeyProducesEmptyBytes() throws Exception {
        assertArrayEquals(new byte[0], rowKey("string", new LogicalType[]{new VarCharType()}, new Object[]{null}));
        assertArrayEquals(new byte[0], rowKey("typed", new LogicalType[]{new VarCharType()}, new Object[]{null}));
    }

    /**
     * string 模式的契约是「按字符串写 rowkey」，DECIMAL 也必须一致。
     *
     * <p>回归点：旧实现对单主键的 DECIMAL 用了 HBase 的 {@code Bytes.toBytes(BigDecimal)}
     * 专用编码，而复合主键却用 {@code toString()}——同一类型在两条路径上编出不同字节。
     */
    @Test
    public void stringEncodingUsesPlainStringForDecimalKeys() throws Exception {
        BigDecimal v = new BigDecimal("123.45");
        assertArrayEquals(Bytes.toBytes("123.45"),
                rowKey("string", new LogicalType[]{new DecimalType(10, 2)}, new Object[]{v}));
    }

    /** 复合主键的每一段都必须走同一套 string 编码，不能出现「单键一套、复合键另一套」 */
    @Test
    public void stringEncodingForCompositeKeyUsesSamePerSegmentEncoding() throws Exception {
        ByteArrayOutputStream expect = new ByteArrayOutputStream();
        expect.write(Bytes.toBytes("acc-1"));
        expect.write(Bytes.toBytes("|"));
        expect.write(Bytes.toBytes("123.45"));
        assertArrayEquals(expect.toByteArray(),
                rowKey("string", new LogicalType[]{new VarCharType(), new DecimalType(10, 2)},
                        new Object[]{"acc-1", new BigDecimal("123.45")}));
    }

    /**
     * typed 模式的 TIMESTAMP 必须与「列值解码」使用同一套精度约定：
     * precision ≤ 3 用毫秒，&gt; 3 用微秒。否则 TS(6) 主键必然有一边对不上。
     */
    @Test
    public void typedTimestampEncodingFollowsDdlPrecision() throws Exception {
        long millis = Timestamp.valueOf("2026-09-29 12:30:15").getTime();
        Timestamp ts = new Timestamp(millis);

        assertArrayEquals("TIMESTAMP(3) 应按毫秒编码",
                Bytes.toBytes(millis),
                rowKey("typed", new LogicalType[]{new TimestampType(3)}, new Object[]{ts}));
        assertArrayEquals("TIMESTAMP(6) 应按微秒编码（与 decode/RowDataConverter 对称）",
                Bytes.toBytes(millis * 1000L),
                rowKey("typed", new LogicalType[]{new TimestampType(6)}, new Object[]{ts}));
    }

    /** 编码 -> 解码应还原同一个时间戳；解码出的是微秒 long，由 RowDataConverter 按精度还原 */
    @Test
    public void typedTimestampRoundTripsWithPrecisionAwareDecode() throws Exception {
        Timestamp ts = new Timestamp(Timestamp.valueOf("2026-09-29 12:30:15").getTime());
        byte[] encoded = rowKey("typed", new LogicalType[]{new TimestampType(6)}, new Object[]{ts});

        Object decoded = decode("typed", new LogicalType[]{new TimestampType(6)}, encoded,
                new TimestampType(6));
        assertEquals(ts.getTime() * 1000L, ((Number) decoded).longValue());
    }
}
