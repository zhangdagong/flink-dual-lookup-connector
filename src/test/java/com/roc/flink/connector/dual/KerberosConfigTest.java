package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Kerberos 配置映射的测试。
 *
 * <p>覆盖的是「不接 KDC 也能验证」的那部分——也就是最容易出错、又最难在集群上定位的那部分：
 * 配置项到 Hadoop/HBase key 的映射关系。真实登录（kinit/keytab）依赖 KDC，无法在单测里完成，
 * 由 README 第 9 章的手工核验步骤覆盖。
 *
 * <p>核心回归点：HBase 2.2.x 的 {@code AuthUtil} 读取的 principal key 是
 * {@code hbase.client.keytab.principal}，而文档写的是 {@code hbase.client.kerberos.principal}。
 * 只写后者会让 {@code shouldLoginFromKeytab()} 返回 false，keytab 登录被静默跳过——
 * 这正是「Kerberos 配了却不生效」的典型原因。两个 key 都必须写。
 */
public class KerberosConfigTest {

    private static DualLookupOptions.Config kerberosConfig() {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        c.setString(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE.key(), "/etc/security/keytabs/ro.keytab");
        c.setString(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL.key(), "ro@EXAMPLE.COM");
        c.setString(DualLookupOptions.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL.key(), "hbase/_HOST@EXAMPLE.COM");
        return DualLookupOptions.Config.from(c, "dim_account");
    }

    private static HBaseLookupReader reader(DualLookupOptions.Config cfg) {
        LogicalType[] types = new LogicalType[]{new VarCharType()};
        return new HBaseLookupReader(cfg, new String[]{"c1"}, types, types);
    }

    @Test
    public void writesBothPrincipalKeysAndAuthenticationMode() {
        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        HBaseLookupReader r = reader(kerberosConfig());
        r.writeKerberosBaseConfig(hconf);
        r.writeKerberosClientCredentials(hconf, "/k.keytab", "ro@EXAMPLE.COM");

        // HBase 2.2.x 实际读取的 key；漏写它 = keytab 登录被静默跳过
        assertEquals("ro@EXAMPLE.COM", hconf.get("hbase.client.keytab.principal"));
        // 官方文档里写的 key（也是后续版本/其他读取路径使用的），同样要写
        assertEquals("ro@EXAMPLE.COM", hconf.get("hbase.client.kerberos.principal"));
        assertEquals("/k.keytab", hconf.get("hbase.client.keytab.file"));

        // UGI 的认证方式：不写它，UGI.isSecurityEnabled() 永远是 false，
        // AuthUtil.loginClient 会整段跳过登录
        assertEquals("kerberos", hconf.get("hadoop.security.authentication"));
        assertEquals("kerberos", hconf.get("hbase.security.authentication"));
        assertEquals("authentication", hconf.get("hbase.rpc.protection"));

        // 服务端 principal 只在显式配置时才写入
        assertEquals("hbase/_HOST@EXAMPLE.COM", hconf.get("hbase.regionserver.kerberos.principal"));
    }

    /**
     * 只写 HBase 侧配置时（复用 Flink 全局登录的路径），不应把客户端 keytab key 也写进去——
     * 否则 HBase 的 AuthUtil 会认为「用户要求从 keytab 登录」，反而可能覆盖/重置全局 UGI。
     */
    @Test
    public void baseConfigDoesNotBindClientKeytab() {
        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        reader(kerberosConfig()).writeKerberosBaseConfig(hconf);

        assertEquals("kerberos", hconf.get("hbase.security.authentication"));
        assertEquals(null, hconf.get("hbase.client.keytab.file"));
        assertEquals(null, hconf.get("hbase.client.keytab.principal"));
    }

    @Test
    public void rpcProtectionIsPropagated() {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setString(DualLookupOptions.HBASE_SECURITY_AUTHENTICATION.key(), "kerberos");
        c.setString(DualLookupOptions.HBASE_CLIENT_KEYTAB_FILE.key(), "/etc/security/keytabs/ro.keytab");
        c.setString(DualLookupOptions.HBASE_CLIENT_KERBEROS_PRINCIPAL.key(), "ro@EXAMPLE.COM");
        c.setString(DualLookupOptions.HBASE_RPC_PROTECTION.key(), "privacy");

        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        reader(DualLookupOptions.Config.from(c, "dim_account")).writeKerberosBaseConfig(hconf);

        // 服务端要求 privacy 而客户端还是默认 authentication 时，握手阶段才失败，极难定位
        assertEquals("privacy", hconf.get("hbase.rpc.protection"));
    }

    /**
     * 复用已有 UGI 时必须清掉客户端 keytab 配置：否则 HBase 的 AuthUtil 会认为用户要求从 keytab
     * 登录，再次去读那个可能只存在于部分 TM 的文件，复用判断就失效了。
     */
    @Test
    public void reuseModeClearsClientKeytabConfig() {
        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        hconf.set("hbase.client.keytab.file", "/only/on/some/tms/ro.keytab");
        hconf.set("hbase.client.keytab.principal", "ro@EXAMPLE.COM");
        hconf.set("hbase.client.kerberos.principal", "ro@EXAMPLE.COM");

        HBaseLookupReader.clearClientKeytabConf(hconf);

        assertEquals(null, hconf.get("hbase.client.keytab.file"));
        assertEquals(null, hconf.get("hbase.client.keytab.principal"));
        assertEquals(null, hconf.get("hbase.client.kerberos.principal"));
    }

    /**
     * 纯 {@code properties.*} 的迁移写法：不写显式 {@code hbase.security.authentication}，
     * 也必须真正进入 Kerberos 模式，并把 rpc.protection 透传到 HBase Configuration。
     *
     * <p>旧实现只读显式参数，默认 simple → {@code applyKerberos()} 直接 return，
     * 用户表面配了 Kerberos，实际整套认证逻辑被跳过。
     */
    @Test
    public void propertiesOnlyKerberosProducesCorrectBaseConfig() {
        Map<String, String> properties = new HashMap<>();
        properties.put("hbase.security.authentication", "kerberos");
        properties.put("hbase.rpc.protection", "privacy");
        properties.put("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM");

        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(c, "dim_account", properties);

        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        // 模拟 HBaseLookupReader.open() 里的 properties.* 透传
        for (Map.Entry<String, String> property : cfg.hbaseProperties.entrySet()) {
            hconf.set(property.getKey(), property.getValue());
        }
        reader(cfg).writeKerberosBaseConfig(hconf);

        assertEquals("kerberos", hconf.get("hbase.security.authentication"));
        assertEquals("privacy", hconf.get("hbase.rpc.protection"));
        assertEquals("hbase/_HOST@HADOOP.COM", hconf.get("hbase.regionserver.kerberos.principal"));
    }
}
