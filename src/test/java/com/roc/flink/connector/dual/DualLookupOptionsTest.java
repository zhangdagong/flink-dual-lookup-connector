package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        assertEquals(500, cfg.hbaseOperationTimeoutMs);    // 默认值，未做联动推导
        assertEquals(300, cfg.dorisConnectTimeoutMs);      // < lookup.timeout
        assertEquals(250, cfg.dorisPoolValidationTimeoutMs); // <= connect.timeout
        assertEquals(4, cfg.dorisPoolSize);                 // 默认 4，Doris 作主源时再调大
        assertEquals(2, cfg.dorisPoolMinIdle);              // <= pool.size
        assertEquals(0, cfg.dorisQueryTimeoutSec);          // 0 = 使用 Doris 集群/会话默认
        assertEquals("string", cfg.hbaseRowkeyEncoding);
        assertEquals(10, cfg.failoverLogIntervalSec);        // 降级日志默认 10s 限流
        assertEquals(360, cfg.statsLogIntervalSec);          // 统计日志默认 6 分钟一条
        assertEquals("simple", cfg.hbaseSecurityAuthentication);
        assertEquals("authentication", cfg.hbaseRpcProtection);
    }

    @Test
    public void statsLogIntervalRejectsNegativeButAcceptsZero() {
        // 负数与 0 在行为上无法区分（都等于关掉打点），与其静默照做不如在建表阶段拦下
        Configuration bad = base();
        bad.setInteger(DualLookupOptions.STATS_LOG_INTERVAL.key(), -1);
        assertRejected(bad, "lookup.stats.log-interval");

        // 0 是合法值：关闭统计打点（DualLookupFunctionTest 也依赖它来保证测试不额外起线程）
        Configuration zero = base();
        zero.setInteger(DualLookupOptions.STATS_LOG_INTERVAL.key(), 0);
        assertEquals(0, parse(zero).statsLogIntervalSec);
    }

    @Test
    public void failoverLogIntervalRejectsNegativeButAcceptsZero() {
        // 负间隔无意义（0 已表达「不限流」），必须拦下来，避免被当成「关闭日志」误用
        Configuration bad = base();
        bad.setInteger(DualLookupOptions.FAILOVER_LOG_INTERVAL.key(), -1);
        assertRejected(bad, "lookup.failover.log-interval");

        // 0 是合法值：表示不限流、每批降级都打印（保留原始行为）
        Configuration zero = base();
        zero.setInteger(DualLookupOptions.FAILOVER_LOG_INTERVAL.key(), 0);
        assertEquals(0, parse(zero).failoverLogIntervalSec);
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
    public void dorisQueryTimeoutRejectsNegativeAndAcceptsZeroOrLarge() {
        Configuration bad = base();
        bad.setInteger(DualLookupOptions.DORIS_QUERY_TIMEOUT_SEC.key(), -1);
        assertRejected(bad, "doris.query.timeout");

        Configuration zero = base();
        zero.setInteger(DualLookupOptions.DORIS_QUERY_TIMEOUT_SEC.key(), 0);
        assertEquals(0, parse(zero).dorisQueryTimeoutSec);

        // 不再被 lookup.timeout 钳制：30s 就是 30s
        Configuration large = base();
        large.setInteger(DualLookupOptions.DORIS_QUERY_TIMEOUT_SEC.key(), 30);
        assertEquals(30, parse(large).dorisQueryTimeoutSec);
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

    // ==================== Kerberos 相关校验 ====================

    @Test
    public void authenticationOnlyAcceptsSimpleOrKerberos() {
        // 拼错（kerberbos）若被静默当成 simple，最终会在 SASL 阶段炸，离根因很远
        Configuration bad = base();
        bad.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberbos");
        assertRejected(bad, "hbase.security.authentication");

        // 大小写/空格应被归一化，而不是判为非法
        Configuration ok = base();
        ok.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "  Kerberos ");
        ok.setString(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE.key(), "/tmp/ro.keytab");
        ok.setString(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL.key(), "ro@EXAMPLE.COM");
        assertEquals("kerberos", parse(ok).hbaseSecurityAuthentication);
    }

    @Test
    public void kerberosWithoutKeytabReusesFlinkLogin() {
        // 与 flink-hbase 对齐：只开 kerberos、不配 keytab 是合法的——复用 Flink 全局登录
        // （security.kerberos.login.keytab/principal）或 kinit 凭据。
        Configuration c = base();
        c.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        DualLookupOptions.Config cfg = parse(c);
        assertEquals("kerberos", cfg.hbaseSecurityAuthentication);
        assertEquals("", cfg.hbaseClientKeytabFile.trim());
        assertEquals("", cfg.hbaseClientKerberosPrincipal.trim());
    }

    @Test
    public void kerberosKeytabAndPrincipalMustBeConfiguredTogether() {
        // 有 keytab 没 principal：无法完成登录，建表期拦下
        Configuration keytabOnly = base();
        keytabOnly.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        keytabOnly.setString(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE.key(), "/tmp/ro.keytab");
        assertRejected(keytabOnly, "要么都配置");

        // 有 principal 没 keytab：同上
        Configuration principalOnly = base();
        principalOnly.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        principalOnly.setString(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL.key(), "ro@EXAMPLE.COM");
        assertRejected(principalOnly, "要么都配置");
    }

    @Test
    public void clientPrincipalRejectsHostPlaceholder() {
        // _HOST 是服务端 principal 的占位符，客户端用它登录必然失败
        Configuration c = base();
        c.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        c.setString(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE.key(), "/tmp/ro.keytab");
        c.setString(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL.key(), "ro/_HOST@EXAMPLE.COM");
        assertRejected(c, "_HOST");
    }

    @Test
    public void rpcProtectionOnlyAcceptsKnownLevels() {
        Configuration bad = base();
        bad.setString(DualLookupOptions.HBASE_RPC_PROTECTION.key(), "encrypted");
        assertRejected(bad, "hbase.rpc.protection");

        Configuration ok = base();
        ok.setString(DualLookupOptions.HBASE_RPC_PROTECTION.key(), "PRIVACY");
        assertEquals("privacy", parse(ok).hbaseRpcProtection);
    }

    /**
     * 与 flink-hbase 对齐：DDL 里的 {@code properties.xxx} 会去掉前缀后原样透传给 HBase Configuration。
     * 这样从 flink-hbase 迁过来的 `properties.hbase.client.keytab.file` 等写法可以直接复用。
     */
    @Test
    public void propertiesArePassedThroughToHBaseConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.client.keytab.file", "/path/to/ro.keytab");
        properties.put("hbase.rpc.timeout", "500");

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(base(), "dim_account", properties);

        assertEquals("/path/to/ro.keytab", cfg.hbaseProperties.get("hbase.client.keytab.file"));
        assertEquals("500", cfg.hbaseProperties.get("hbase.rpc.timeout"));
    }

    /**
     * 从 flink-hbase 迁移时最常见的情况：Kerberos 只写在 properties.* 里，
     * 没有写本连接器的显式参数。此时必须真的进入 Kerberos 模式，
     * 否则 applyKerberos() 会按默认 simple 直接 return，整套认证逻辑被静默跳过。
     */
    @Test
    public void propertiesEnableKerberosWithoutExplicitOptions() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.security.authentication", "kerberos");
        properties.put("hbase.rpc.protection", "privacy");
        properties.put("hbase.master.kerberos.principal", "hbase/_HOST@HADOOP.COM");
        properties.put("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM");
        // 现场 DDL 里带的是 master keytab；它是服务端配置，客户端不能拿它登录
        properties.put("hbase.master.keytab.file", "/home/flink/streampark/kerberos/rtdw.keytab");

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(base(), "dim_account", properties);

        assertEquals("kerberos", cfg.hbaseSecurityAuthentication);
        assertEquals("privacy", cfg.hbaseRpcProtection);
        assertEquals("", cfg.hbaseClientKeytabFile);
        assertEquals("", cfg.hbaseClientKerberosPrincipal);
        assertEquals("/home/flink/streampark/kerberos/rtdw.keytab",
                cfg.hbaseProperties.get("hbase.master.keytab.file"));
    }

    /** 显式参数优先于 properties.*：两者同时出现时不能被 properties 悄悄改掉 */
    @Test
    public void explicitKerberosOptionsWinOverProperties() {
        Configuration c = base();
        c.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "simple");
        c.setString(DualLookupOptions.HBASE_RPC_PROTECTION.key(), "authentication");

        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.security.authentication", "kerberos");
        properties.put("hbase.rpc.protection", "privacy");

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(c, "dim_account", properties);

        assertEquals("simple", cfg.hbaseSecurityAuthentication);
        assertEquals("authentication", cfg.hbaseRpcProtection);
    }

    /** 客户端 keytab/principal 同样可以用 properties.* 提供（显式参数优先） */
    @Test
    public void clientCredentialsCanComeFromProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.security.authentication", "kerberos");
        properties.put("hbase.client.keytab.file", "/path/to/ro.keytab");
        properties.put("hbase.client.kerberos.principal", "ro@EXAMPLE.COM");

        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(base(), "dim_account", properties);

        assertEquals("/path/to/ro.keytab", cfg.hbaseClientKeytabFile);
        assertEquals("ro@EXAMPLE.COM", cfg.hbaseClientKerberosPrincipal);
    }

    /** properties.* 提供的 keytab 也必须遵守「与 principal 成对」的校验 */
    @Test
    public void propertiesClientKeytabStillRequiresPrincipal() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.security.authentication", "kerberos");
        properties.put("hbase.client.keytab.file", "/path/to/ro.keytab");

        try {
            DualLookupOptions.Config.from(base(), "dim_account", properties);
            fail("只有 properties.hbase.client.keytab.file 时应报错");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("要么都配置"));
        }
    }

    /**
     * Session 级 {@code table.exec.async-lookup.timeout} 不在 DDL 的 WITH 选项里，
     * 必须通过第四个参数传入；否则这条校验永远只拿默认 3 分钟比较。
     */
    @Test
    public void operatorAsyncLookupTimeoutIsReadFromGlobalConfig() {
        Configuration tableOptions = base();
        Configuration global = new Configuration();
        global.setString("table.exec.async-lookup.timeout", "100ms");

        try {
            DualLookupOptions.Config.from(tableOptions, "dim_account", new HashMap<>(), global);
            fail("全局 async-lookup.timeout 小于 lookup.timeout 时应报错");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("table.exec.async-lookup.timeout"));
        }
    }

    // ==================== lookup.timeout 联动推导 ====================

    /**
     * 显式调 lookup.timeout 时，未显式配置的依赖超时自动跟随：
     * HBase RPC 80%，HBase operation 不低于 500ms/lookup.timeout，Doris connect 80% 但夹到 [250,300]ms。
     */
    @Test
    public void explicitLookupTimeoutDerivesDependentTimeouts() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 1000);

        DualLookupOptions.Config cfg = parse(c);

        assertEquals(800, cfg.hbaseRpcTimeoutMs);
        assertEquals(1000, cfg.hbaseOperationTimeoutMs);
        assertEquals("80% = 800ms，但 Doris 是最后一跳，上限 300ms", 300, cfg.dorisConnectTimeoutMs);

        Configuration smaller = base();
        smaller.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 300);
        DualLookupOptions.Config smallCfg = parse(smaller);
        assertEquals(240, smallCfg.hbaseRpcTimeoutMs);
        assertEquals(500, smallCfg.hbaseOperationTimeoutMs);
        assertEquals("80% = 240ms，被 HikariCP 下限抬到 250ms", 250, smallCfg.dorisConnectTimeoutMs);
    }

    /** 显式写了的依赖项优先，不能被联动推导覆盖 */
    @Test
    public void explicitDependentTimeoutsWinOverDerivation() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 1000);
        c.setInteger(DualLookupOptions.HBASE_RPC_TIMEOUT_MS.key(), 250);
        c.setInteger(DualLookupOptions.HBASE_OPERATION_TIMEOUT_MS.key(), 1500);
        c.setInteger(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS.key(), 300);

        DualLookupOptions.Config cfg = parse(c);

        assertEquals(250, cfg.hbaseRpcTimeoutMs);
        assertEquals(1500, cfg.hbaseOperationTimeoutMs);
        assertEquals(300, cfg.dorisConnectTimeoutMs);
    }

    /**
     * lookup.timeout 太小且没显式配 doris.connect.timeout 时，
     * 推导值会被 HikariCP 的 250ms 下限顶住，无法满足 < lookup.timeout；建表期给出可操作提示。
     */
    @Test
    public void tooSmallLookupTimeoutForDerivedDorisConnectIsRejected() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 250);

        try {
            parse(c);
            fail("lookup.timeout=250ms 时无法推导出合法的 doris.connect.timeout");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("lookup.timeout"));
            assertTrue(e.getMessage().contains("250"));
        }
    }

    /** 显式把 doris.connect.timeout 配到 HikariCP 下限以下，必须建表期拦下 */
    @Test
    public void dorisConnectBelowHikariMinimumIsRejected() {
        Configuration c = base();
        c.setInteger(DualLookupOptions.LOOKUP_TIMEOUT.key(), 1000);
        c.setInteger(DualLookupOptions.DORIS_CONNECT_TIMEOUT_MS.key(), 249);

        try {
            parse(c);
            fail("doris.connect.timeout < 250ms 应被拒绝");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("HikariCP"));
        }
    }
}
