package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 配置解析与跨字段校验的测试。
 *
 * <p>重点覆盖两类容易「说了没做」的规则：
 * <ol>
 *   <li>默认值组合必须自洽——否则所有不显式写这些参数的 DDL 都会建表失败；</li>
 *   <li>超时预算的四条交叉校验——单独看每个参数都合法，组合起来才会失效。</li>
 * </ol>
 */
public class DualLookupOptionsTest {

    private static Configuration base() {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        return c;
    }

    private static DualLookupOptions.Config parse(Configuration c) {
        return DualLookupOptions.Config.from(c, "dim_account");
    }

    private static void assertRejected(Configuration c, String expectedFragment) {
        try {
            parse(c);
            fail("应当抛出 IllegalArgumentException，期望信息包含: " + expectedFragment);
        } catch (IllegalArgumentException e) {
            assertTrue("异常信息应包含 " + expectedFragment + "，实际为: " + e.getMessage(),
                    e.getMessage().contains(expectedFragment));
        }
    }

    @Test
    public void defaultValuesAreSelfConsistent() {
        // 这条测试是「默认配置开箱可用」的守门人：
        // 一旦改了某个默认值导致交叉校验不通过，这里会立刻红
        DualLookupOptions.Config cfg = parse(base());
        assertEquals("hbase", cfg.primary);
        assertEquals("doris", cfg.standby);
        assertEquals(500, cfg.timeoutMs);
        assertEquals(5, cfg.batchMaxWaitMs);
        assertEquals(300, cfg.hbaseRpcTimeoutMs);          // < lookup.timeout
        assertEquals(300, cfg.dorisConnectTimeoutMs);      // < lookup.timeout
        assertEquals(250, cfg.dorisPoolValidationTimeoutMs); // <= connect.timeout
        assertEquals("string", cfg.hbaseRowkeyEncoding);
    }

    @Test
    public void batchMaxWaitMustBeLessThanLookupTimeout() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.BATCH_MAX_WAIT.key(), 500);
        assertRejected(c, "lookup.batch.max-wait");
    }

    @Test
    public void hbaseRpcTimeoutMustBeLessThanLookupTimeout() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.HBASE_RPC_TIMEOUT_MS.key(), 600);
        assertRejected(c, "hbase.rpc.timeout");
    }

    @Test
    public void dorisConnectTimeoutMustBeLessThanLookupTimeout() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS.key(), 3000);
        assertRejected(c, "doris.connect.timeout");
    }

    @Test
    public void operatorAsyncLookupTimeoutMustExceedLookupTimeout() {
        Configuration c = base();
        // 算子层兜底超时小于业务超时：降级还没走完 Flink 自己就先判超时了
        c.setString("table.exec.async-lookup.timeout", "100ms");
        assertRejected(c, "table.exec.async-lookup.timeout");
    }

    @Test
    public void validationTimeoutCannotExceedConnectTimeout() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.DORIS_POOL_VALIDATION_TIMEOUT_MS.key(), 500);
        c.setInteger(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS.key(), 300);
        assertRejected(c, "doris.pool.validation-timeout");
    }

    @Test
    public void rowkeyEncodingOnlyAcceptsStringOrTyped() {
        Configuration bad = base();
        bad.setString(DualLookupOptions.HBASE_ROWKEY_ENCODING.key(), "str");
        assertRejected(bad, "hbase.rowkey.encoding");

        // 大小写与首尾空格应被归一化，而不是判为非法
        Configuration ok = base();
        ok.setString(DualLookupOptions.HBASE_ROWKEY_ENCODING.key(), " TYPED ");
        assertEquals("typed", parse(ok).hbaseRowkeyEncoding);
    }

    @Test
    public void primaryOnlyAcceptsHbaseOrDoris() {
        Configuration c = base();
        c.setString(DualLookupOptions.PRIMARY.key(), "mysql");
        assertRejected(c, "lookup.primary");
    }

    @Test
    public void standbyIsDerivedFromPrimary() {
        Configuration c = base();
        c.setString(DualLookupOptions.PRIMARY.key(), "Doris");
        DualLookupOptions.Config cfg = parse(c);
        assertEquals("doris", cfg.primary);
        assertEquals("hbase", cfg.standby);
    }

    @Test
    public void tableNameDefaultsToDdlTableName() {
        DualLookupOptions.Config cfg = parse(base());
        assertEquals("dim_account", cfg.hbaseTableName);
        assertEquals("dim_account", cfg.dorisTableName);
    }
}
