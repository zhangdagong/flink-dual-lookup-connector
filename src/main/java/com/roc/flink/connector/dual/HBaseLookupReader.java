package com.roc.flink.connector.dual;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * HBase 维表读取器（基于 HBase 2.x 原生异步客户端 AsyncConnection）。
 *
 * <p>为什么用 AsyncConnection 而不是 HTable？因为 Lookup Join 的异步算子里若混入阻塞调用，
 * 会把异步算子的线程池拖死，退化成同步性能。原生异步客户端全程返回 CompletableFuture，
 * 与 Flink 的 {@code AsyncLookupFunction} 天然契合。
 *
 * <p>超时分两层（职责必须分清，否则会误判降级）：
 * <ol>
 *   <li>客户端层：{@code hbase.rpc.timeout} / {@code hbase.client.operation.timeout}，让 HBase 自己先中断 RPC，
 *       目的是<b>让后端尽早释放资源</b>；</li>
 *   <li>Future 层：{@code lookup.timeout}，由本类调度器强制让 Future 以 TimeoutException 结束，
 *       目的是<b>让业务层立刻降级</b>，不必等 HBase 把重试走完。</li>
 * </ol>
 *
 * <p><b>连接自愈</b>：HBase 连接可能因空闲回收 / ZK 会话过期 / RS 重启而被关闭。本类在
 * {@link #isOpened()} 里检查 {@code AsyncConnection.isClosed()}，{@link #open()} 会自动重建；
 * 查询过程中撞上 {@code ConnectionClosedException} 时，还会替换掉"当时那条坏连接"并重试一次。
 * 否则备源长时间空闲后第一次降级就会失败，并持续失败到重启作业。
 */
public class HBaseLookupReader implements LookupReader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupReader.class);

    /** 全局配置（可序列化，随算子下发） */
    private final DualLookupOptions.Config cfg;
    /** 要查询的业务列名（已剥离元字段），同时作为 HBase 列限定符（qualifier） */
    private final String[] columnNames;
    /** 业务列对应的 Flink 类型，用于把 HBase 的 byte[] 还原成正确的 Java 类型 */
    private final LogicalType[] columnTypes;
    /**
     * 主键列类型。仅 {@code hbase.rowkey.encoding = typed} 时使用：
     * 类型化编码必须知道每段主键的类型，才能编出与建表侧一致的字节。
     */
    private final LogicalType[] keyTypes;
    /**
     * 列名的小写形式（结果行 Map 的 key 统一小写，与 Doris 侧保持一致）。
     * 构造期算好——放在取值循环里就变成「每一行 × 每一列」都调一次 toLowerCase()（每次都新建字符串对象）。
     */
    private final String[] columnNamesLower;
    /**
     * 列名的 HBase 字节形式（qualifier 字节）。
     * 构造期算好——放在查询路径上就变成「每个 Get × 每一列」和「每一行 × 每一列」都重复编码。
     */
    private final byte[][] columnBytes;

    // ---- 以下均为运行期资源句柄：不能被序列化，故声明为 transient，open() 时重建 ----

    /** 是否已建连；volatile 保证多线程可见性（攒批线程可能来自不同调用栈） */
    private transient volatile boolean opened = false;
    /** 异步连接：内部自带 Netty 线程池，整个 Reader 生命周期内共享一个即可。
     *  volatile：连接可能被 HBase 侧关闭（空闲回收 / ZK 会话过期），查询线程要能立刻看到最新引用 */
    private transient volatile AsyncConnection connection;
    /** 异步表句柄：轻量对象，复用才能命中客户端侧的 region location 缓存 */
    private transient volatile AsyncTable<AdvancedScanResultConsumer> table;
    /** 列族名，提前转好 byte[]（每次查询都要用，避免重复编码） */
    private transient volatile byte[] familyBytes;
    /** 专门用于「到期强制让 Future 超时」的调度线程池；
     * 用 ScheduledThreadPoolExecutor 而非 Executors 工厂方法：后者返回的是包装类，
     * 拿不到队列句柄，无法观测「已完成查询的超时任务是否被及时清理」 */
    private transient ScheduledThreadPoolExecutor timeoutScheduler;
    /**
     * 专门执行「断连重连」的线程池。
     *
     * <p>不能复用 {@code timeoutScheduler}：重连里的 {@code open()} 是同步阻塞的（ZK/RS 不可用时
     * 可能几百毫秒到数秒），如果它跑在超时调度器上，会把这期间所有批次的超时任务一起拖住，
     * 表现为「查询已经超过 lookup.timeout 却成功返回、没有降级」。
     */
    private transient ExecutorService reconnectExecutor;

    public HBaseLookupReader(DualLookupOptions.Config cfg, String[] columnNames, LogicalType[] columnTypes,
                             LogicalType[] keyTypes) {
        this.cfg = cfg;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.keyTypes = keyTypes;
        // 列名的小写副本与字节形式都在这里算一次，运行期只读
        this.columnNamesLower = new String[columnNames.length];
        this.columnBytes = new byte[columnNames.length][];
        for (int i = 0; i < columnNames.length; i++) {
            this.columnNamesLower[i] = columnNames[i].toLowerCase();
            this.columnBytes[i] = Bytes.toBytes(columnNames[i]);
        }
    }

    // ---------------- 生命周期 ----------------

    /**
     * 建连 + 预热。synchronized 保证多线程下只真正执行一次（幂等）。
     *
     * <p><b>连接被关闭时会自动重建</b>：HBase 的异步连接可能因为空闲回收、ZK 会话过期或
     * RegionServer 重启而被关闭。若只看 {@code opened} 布尔标志，就会出现「第一次降级时
     * 撞上一年前建立的死连接」——错误是 {@code ConnectionClosedException: Connection closed}，
     * 而且之后每一批都会失败。因此健康判断必须落到 {@code AsyncConnection.isClosed()}。
     *
     * <p>注意这里是<b>同步阻塞</b>的：HBase 不可用时 {@code createAsyncConnection().get()} 会一直等到
     * ZooKeeper/客户端超时。这是「精简优先」的取舍——作业启动期不会因此失败（惰性建连），
     * 但建连那一刻的个别批次会被阻塞住，靠 ZK 超时兜底。
     */
    @Override
    public synchronized void open() throws Exception {
        AsyncConnection existing = connection;
        if (opened && existing != null && !existing.isClosed()) {
            return;
        }
        if (existing != null) {
            LOG.warn("[dual-lookup] HBase 连接已被关闭（空闲回收 / ZK 会话过期 / RS 重启），重建连接：table={}",
                    cfg.hbaseTableName);
            resetConnection();
        }
        // HBaseConfiguration.create() 会先读 classpath 下的 hbase-site.xml 作为基线
        Configuration hbaseConf = HBaseConfiguration.create();
        // 显式配置 ZK：留空则沿用 hbase-site.xml，便于容器/云上不改配置文件即可接入
        if (cfg.hbaseZkQuorum != null && !cfg.hbaseZkQuorum.trim().isEmpty()) {
            hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, cfg.hbaseZkQuorum.trim());
            hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, cfg.hbaseZkPort);
            hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, cfg.hbaseZnodeParent);
        }
        // 实时链路：超时收窄 + 少重试，避免一次卡顿拖垮整个算子
        hbaseConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, cfg.hbaseRpcTimeoutMs);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, cfg.hbaseOperationTimeoutMs);
        hbaseConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, cfg.hbaseRetries);

        // properties.* 透传：与 flink-hbase 的 HBaseConnectorOptionsUtil.getHBaseConfiguration 一致，
        // 逐项写入 HBase Configuration（DDL 里的前缀已由 Factory 去掉）。
        // 放在 Kerberos 之前，applyKerberos 才能看到 properties 里配置的 keytab/principal。
        if (cfg.hbaseProperties != null && !cfg.hbaseProperties.isEmpty()) {
            for (Map.Entry<String, String> property : cfg.hbaseProperties.entrySet()) {
                hbaseConf.set(property.getKey(), property.getValue());
            }
        }

        // Kerberos：必须在建连前完成。优先复用 Flink 已登录的 UGI，见 applyKerberos。
        applyKerberos(hbaseConf);

        // 先全部用局部变量构建，成功后再一次性发布：
        // 失败时不会留下「connection 已建、opened 仍为 false」的半初始化对象
        AsyncConnection conn = null;
        try {
            conn = ConnectionFactory.createAsyncConnection(hbaseConf).get();
            TableName tableName = TableName.valueOf(cfg.hbaseTableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = conn.getTable(tableName);

            // 预热 region location：首条查询若才去定位 region（冷启动），
            // 会因「waiting for region location」耗时数百毫秒而撞上 500ms 超时被降级。
            // 提前在建连时取好 location，后续查询直接命中客户端缓存。
            //
            // ⚠️ HBase 2.2.x 的 AsyncTable 没有 getRegionLocator()（那是 2.4+ 才补的 API），
            //    必须从 AsyncConnection 上取；用 2.6.x 的写法换到 2.2.7 会在编译期直接失败。
            conn.getRegionLocator(tableName)
                    .getAllRegionLocations()
                    .get(cfg.hbaseOperationTimeoutMs, TimeUnit.MILLISECONDS);

            // 超时调度器：一个 Reader 一个线程足够（只做「到点标记超时」这个轻量动作）。
            // 重连时复用已有实例——否则每次重建都会漏掉一个线程池。
            ScheduledThreadPoolExecutor scheduler = timeoutScheduler;
            if (scheduler == null || scheduler.isShutdown()) {
                scheduler = new ScheduledThreadPoolExecutor(1, r -> {
                    Thread t = new Thread(r, "dual-lookup-hbase-timeout");
                    t.setDaemon(true);
                    return t;
                });
                // 取消即出队：默认策略下被取消的任务会滞留在队列里直到到期时刻，
                // 高 QPS 时「查询早已完成」的超时任务仍会堆积；开启后 cancel(false) 立即释放队列空间
                scheduler.setRemoveOnCancelPolicy(true);
                timeoutScheduler = scheduler;
            }

            // 重连线程池：与超时调度器分开，避免阻塞的 open() 延迟其他批次的超时判定。
            ExecutorService reconnect = reconnectExecutor;
            if (reconnect == null || reconnect.isShutdown()) {
                reconnect = Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "dual-lookup-hbase-reconnect");
                    t.setDaemon(true);
                    return t;
                });
                reconnectExecutor = reconnect;
            }

            this.connection = conn;
            this.table = asyncTable;
            this.familyBytes = Bytes.toBytes(cfg.hbaseColumnFamily);
            // 放在最后：以上任一步骤抛异常，opened 仍为 false，下次查询会重新尝试建连
            opened = true;
        } catch (Exception e) {
            // 预热点（region location）失败时连接已经建立，必须显式关闭它。
            // 否则下次 open() 会用新连接覆盖字段，旧连接的 Netty 线程池与 ZK 会话将永久泄漏——
            // 这类泄漏在「HBase 刚重启、region 尚未上线」的窗口期会每批次发生一次，很容易把 TM 拖垮。
            closeQuietly(conn);
            throw e;
        }
        // rowkey 编码模式务必打出来：它决定了「能不能查到数据」，是排查「HBase 全部查不到」时的第一现场
        LOG.info("[dual-lookup] HBase reader opened, table={}, family={}, zk={}, rowkeyEncoding={}",
                cfg.hbaseTableName, cfg.hbaseColumnFamily, cfg.hbaseZkQuorum, cfg.hbaseRowkeyEncoding);
    }

    /** 丢弃当前连接（不改动超时调度器），下次 open() 会重建。 */
    private synchronized void resetConnection() {
        opened = false;
        AsyncConnection old = connection;
        connection = null;
        table = null;
        familyBytes = null;
        closeQuietly(old);
    }

    /** 忽略异常地关闭连接，只用于失败清理路径；避免清理动作本身掩盖真正的失败原因 */
    private static void closeQuietly(AsyncConnection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (Exception e) {
            LOG.warn("[dual-lookup] 关闭建连失败的 HBase 连接时出错（忽略）", e);
        }
    }

    /**
     * Kerberos 认证：<b>与 flink-hbase 保持一致，优先复用 Flink 已登录的 UGI</b>。
     *
     * <p>Flink 的 {@code HadoopModule} 在 TaskManager 启动时就会用
     * {@code security.kerberos.login.keytab} / {@code security.kerberos.login.principal}
     * 完成登录（并起一个 {@code security.kerberos.relogin.period} 周期 —— 默认 1 分钟 —— 的
     * TGT 续期任务）。flink-hbase 自己不读任何 keytab，拿到的是这个已登录的 UGI。
     *
     * <p>因此本连接器的规则是：
     * <ol>
     *   <li>未配置 {@code hbase.client.keytab.file}：直接复用当前 UGI（Flink 全局登录 / kinit）；
     *       没有凭据时给出明确的双选一提示，而不是抛一句 GSS 错误；</li>
     *   <li>配置了 keytab 且当前用户就是该 principal：复用，不重复登录；</li>
     *   <li>配置了 keytab 但文件不可读、而当前 UGI 已有凭据：<b>降级为复用</b>（只打 WARN）。
     *       这是本次生产问题的修复点——Flink 全局登录已经成功，不应因为某张表多写了一个
     *       只在部分节点存在的路径而让作业起不来；</li>
     *   <li>既没有可用凭据、keytab 也读不到：才报错，并在消息里同时给出两种配置方式。</li>
     * </ol>
     *
     * <p>为什么「只写 hbase.* 配置项」不够？HBase 2.2.x 的
     * {@code ConnectionFactory.createAsyncConnection(conf)} 会调用 {@code AuthUtil.loginClient(conf)}，
     * 它的第一道门槛是 {@code UGI.isSecurityEnabled()}；UGI 只看自己的静态配置。
     * 在我们需要自己登录时会显式调用 {@link UserGroupInformation#setConfiguration}；
     * 但 Flink 已安装安全上下文时<b>不会</b>再覆盖它，以免破坏 Flink 的 auth_to_local / group mapping 配置。
     *
     * <p>principal 的 key 名也有坑：HBase 2.2.x 的 {@code AuthUtil} 常量
     * {@code HBASE_CLIENT_KERBEROS_PRINCIPAL} 的字符串值是
     * {@code hbase.client.keytab.principal}，而官方文档写的是
     * {@code hbase.client.kerberos.principal}。只写后者会导致登录被静默跳过，
     * 因此显式 keytab 模式下两个 key 都写。
     */
    private void applyKerberos(Configuration conf) throws IOException {
        if (!"kerberos".equals(cfg.hbaseSecurityAuthentication)) {
            return; // 默认 simple 模式，直接返回，零开销
        }
        String keytab = cfg.hbaseClientKeytabFile.trim();
        String principal = cfg.hbaseClientKerberosPrincipal.trim();

        // properties.* / hbase-site.xml 里配置的 keytab 同样参与判断。
        // flink-hbase 正是把 `properties.hbase.client.keytab.file` 原样写进 Configuration，
        // 再由 HBase 的 AuthUtil 完成登录；这里显式读出来，是为了复用同一套判断与报错逻辑。
        if (keytab.isEmpty()) {
            keytab = trimToEmpty(conf.get("hbase.client.keytab.file"));
        }
        if (principal.isEmpty()) {
            principal = trimToEmpty(conf.get("hbase.client.kerberos.principal"));
            if (principal.isEmpty()) {
                // HBase 2.2.x 的 AuthUtil 实际读这个 key
                principal = trimToEmpty(conf.get("hbase.client.keytab.principal"));
            }
        }

        // hbase.master.keytab.file 是 HMaster 服务端用来登录 hbase/_HOST 的配置，
        // 客户端（本连接器）不会读它。flink-hbase 的 DDL 里偶尔会带这个参数，
        // 但它不能替代 hbase.client.keytab.file；这里显式提示，避免用户以为「配了 keytab 却没生效」。
        String masterKeytab = trimToEmpty(conf.get("hbase.master.keytab.file"));
        if (keytab.isEmpty() && !masterKeytab.isEmpty()) {
            LOG.warn("[dual-lookup] 检测到 hbase.master.keytab.file={}，但它是 HBase Master 服务端配置，"
                    + "客户端不会用它登录。将复用 Flink 全局登录（security.kerberos.login.*）/ kinit 凭据；"
                    + "如需单表指定客户端凭据，请配置 hbase.client.keytab.file + "
                    + "hbase.client.kerberos.principal（或对应的 properties.*）",
                    masterKeytab);
        }

        // ---- 0) krb5.conf ----
        // JVM 级系统属性，必须在第一次 GSS/Kerberos 调用之前生效。
        // 不无脑覆盖：启动脚本若已用 -Djava.security.krb5.conf 指定，就沿用启动参数，
        // 避免同一台 TM 上多个作业互相改写这个全局属性。
        if (!cfg.hbaseKrb5Conf.trim().isEmpty()) {
            String existing = System.getProperty("java.security.krb5.conf");
            String wanted = cfg.hbaseKrb5Conf.trim();
            if (existing == null || existing.trim().isEmpty()) {
                System.setProperty("java.security.krb5.conf", wanted);
                LOG.info("[dual-lookup] 已设置 java.security.krb5.conf={}", wanted);
            } else if (!existing.trim().equals(wanted)) {
                LOG.warn("[dual-lookup] java.security.krb5.conf 已被启动参数设为 {}，忽略 "
                        + "hbase.kerberos.krb5.conf={}（该属性全局生效，建议统一在 JVM 启动参数里配置）",
                        existing, wanted);
            }
        }

        // ---- 1) HBase 侧配置（与凭据来源无关的部分） ----
        writeKerberosBaseConfig(conf);

        // ---- 2) 凭据来源：优先复用 Flink 已登录的 UGI ----
        // 与 flink-hbase 对齐：Flink 的 HadoopModule 在 TM 启动时就会用
        // security.kerberos.login.keytab / principal 完成登录，并起一个每分钟的 TGT 续期任务；
        // flink-hbase 自己不读 keytab，直接用这个已登录的 UGI。
        // 因此本连接器也把「自己登录」降级成可选项：只有显式配置了 hbase.client.keytab.* 且
        // 与当前登录用户不一致时才自己登录。
        UserGroupInformation current = currentUserOrNull();
        boolean hasKerberosCreds = current != null && current.hasKerberosCredentials();

        if (keytab.isEmpty()) {
            // 未配置 keytab：完全依赖 Flink 全局登录 / kinit（推荐用法）
            if (!hasKerberosCreds) {
                throw new IOException("[dual-lookup] 已开启 Kerberos 但未检测到可用凭据。请二选一："
                        + "① 推荐（与 flink-hbase 一致）：在 flink-conf.yaml 配置 "
                        + "security.kerberos.login.keytab / security.kerberos.login.principal；"
                        + "② 在本表 WITH 里配置 hbase.client.keytab.file / hbase.client.kerberos.principal"
                        + "（或对应的 properties.*）。"
                        + (masterKeytab.isEmpty()
                                ? ""
                                : "注意：hbase.master.keytab.file 是 Master 服务端配置，不能作为客户端凭据；")
                        + "当前 UGI=" + (current == null ? "null" : current.getUserName()));
            }
            LOG.info("[dual-lookup] HBase Kerberos 复用已登录用户（Flink 全局登录或 kinit）: {}",
                    current.getUserName());
            clearClientKeytabConf(conf);
            return;
        }

        // 只有 keytab、没有 principal：无法自己登录。当前已有凭据则复用，否则明确报错。
        if (principal.isEmpty()) {
            if (hasKerberosCreds) {
                LOG.warn("[dual-lookup] 检测到 hbase.client.keytab.file 但缺少 principal，改为复用当前登录用户（{}）。"
                        + "如需显式登录，请同时配置 hbase.client.kerberos.principal", current.getUserName());
                clearClientKeytabConf(conf);
                return;
            }
            throw new IOException("[dual-lookup] 配置了 hbase.client.keytab.file 但缺少 principal，"
                    + "无法执行 keytab 登录；请同时配置 hbase.client.kerberos.principal"
                    + "（或对应的 properties.hbase.client.kerberos.principal）");
        }

        // 显式配置了 keytab，但当前用户已经是目标 principal：直接复用
        if (hasKerberosCreds && principal.equals(current.getUserName())) {
            LOG.info("[dual-lookup] HBase Kerberos 复用已登录用户（principal 匹配）: {}", principal);
            clearClientKeytabConf(conf);
            return;
        }

        File keytabFile = new File(keytab);
        if (!keytabFile.isFile() || !keytabFile.canRead()) {
            // 关键兜底（本次生产问题的修复点）：keytab 可能只存在于部分节点（本次生产就是这种情况），
            // 而 Flink 已经用全局配置或 delegation token 登录成功时，不应因为「这一张表多写了一个
            // 只在部分 TM 存在的路径」而让作业起不来。
            if (hasKerberosCreds) {
                LOG.warn("[dual-lookup] hbase.client.keytab.file 在本 TaskManager 上不可读（{}），"
                        + "但当前 UGI 已有 Kerberos 凭据（{}），改为复用之（不会再去读该文件）。"
                        + "若确实需要该 keytab，请把它分发到每台 TaskManager 的同一路径并保证运行用户可读",
                        keytab, current.getUserName());
                clearClientKeytabConf(conf);
                return;
            }
            throw new IOException("[dual-lookup] 未检测到可用 Kerberos 凭据，且 hbase.client.keytab.file 不可读: "
                    + keytab + "（该文件必须存在于每个 TaskManager 上，且运行用户可读）");
        }

        // ---- 3) 自己登录（显式覆盖 Flink 的登录用户） ----
        writeKerberosClientCredentials(conf, keytab, principal);
        if (!UserGroupInformation.isSecurityEnabled()) {
            // 只有在我们自己启用安全时才覆盖 UGI 配置；Flink 已安装时保持它的配置不变
            UserGroupInformation.setConfiguration(conf);
        }
        if (!UserGroupInformation.isSecurityEnabled()) {
            throw new IOException("[dual-lookup] 已配置 kerberos 但 UGI 仍判定为 simple 模式，"
                    + "keytab 登录会被静默跳过。请确认没有在 JVM 启动参数里强制 simple 认证");
        }
        try {
            // 注意 UGI.loginUserFromKeytab 的第一行是 `if (!isSecurityEnabled()) return;`：
            // security 未开启时它会静默返回，所以上面必须先断言。
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            throw new IOException("[dual-lookup] keytab 登录失败, principal=" + principal
                    + ", keytab=" + keytab
                    + "。请先用 klist -kt <keytab> 核对 principal 与 keytab 是否匹配，"
                    + "并确认 TM 与 KDC 时间差在 5 分钟内", e);
        }
        LOG.info("[dual-lookup] HBase Kerberos 就绪: principal={}, keytab={}, rpc.protection={}, loginUser={}",
                principal, keytab, cfg.hbaseRpcProtection, currentUserName());
    }

    /**
     * HBase 侧通用 Kerberos 配置（与凭据来源无关）。
     *
     * <p>纯字符串映射、不需要 KDC，因此抽出来给单测直接断言。
     */
    void writeKerberosBaseConfig(Configuration conf) {
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hbase.rpc.protection", cfg.hbaseRpcProtection);
        // 服务端 principal：不配时 HBase 默认按 hbase/_HOST@REALM 规则推导，
        // 只有集群改过 principal 名才需要显式指定
        if (!cfg.hbaseRegionserverKerberosPrincipal.trim().isEmpty()) {
            conf.set("hbase.regionserver.kerberos.principal", cfg.hbaseRegionserverKerberosPrincipal.trim());
        }
        if (!cfg.hbaseMasterKerberosPrincipal.trim().isEmpty()) {
            conf.set("hbase.master.kerberos.principal", cfg.hbaseMasterKerberosPrincipal.trim());
        }
    }

    /**
     * 显式 keytab 模式的客户端凭据配置。
     *
     * <p>两个 principal key 都写：HBase 2.2.x 的 {@code AuthUtil} 实际读
     * {@code hbase.client.keytab.principal}，而官方文档写的是
     * {@code hbase.client.kerberos.principal}；只写一个会出现「配了但登录被跳过」。
     */
    void writeKerberosClientCredentials(Configuration conf, String keytab, String principal) {
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.client.keytab.file", keytab);
        conf.set("hbase.client.keytab.principal", principal);
        conf.set("hbase.client.kerberos.principal", principal);
    }

    /** 当前 UGI；取不到时返回 null（调用方据此判断「没有可用登录用户」） */
    private static UserGroupInformation currentUserOrNull() {
        try {
            return UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            return null;
        }
    }

    /** null 安全的 trim */
    private static String trimToEmpty(String v) {
        return v == null ? "" : v.trim();
    }

    /**
     * 复用已有 UGI 时清掉客户端 keytab 配置。
     *
     * <p>这些 key 可能来自 DDL 的 {@code properties.*} 或 classpath 的 {@code hbase-site.xml}。
     * 如果保留着，HBase 的 {@code AuthUtil.loginClient} 会认为「用户要求从 keytab 登录」，
     * 于是再次去读那个可能只存在于部分节点的文件——我们刚刚的复用判断就白做了。
     */
    static void clearClientKeytabConf(Configuration conf) {
        conf.unset("hbase.client.keytab.file");
        conf.unset("hbase.client.keytab.principal");
        conf.unset("hbase.client.kerberos.principal");
    }

    /** 当前 UGI 用户名，仅用于启动日志；取不到时返回明确的占位符，绝不让日志本身抛异常 */
    private static String currentUserName() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException e) {
            return "<unknown: " + e.getMessage() + ">";
        }
    }

    /**
     * 连接是否可用。
     *
     * <p>不能只返回 {@code opened}：HBase 侧关闭连接后这个标志仍是 true，
     * 上层就再也不会调用 {@link #open()}，故障会一直持续到重启作业。
     */
    @Override
    public boolean isOpened() {
        AsyncConnection c = connection;
        return opened && c != null && !c.isClosed();
    }

    /**
     * 释放资源。依次关闭超时调度器 -> 重连线程池 -> 连接；
     * 顺序不能反：调度器/重连任务若在连接关闭后仍触发，会在已销毁的连接上做无意义操作。
     */
    @Override
    public synchronized void close() throws Exception {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
            timeoutScheduler = null;
        }
        if (reconnectExecutor != null) {
            reconnectExecutor.shutdownNow();
            reconnectExecutor = null;
        }
        if (connection != null) {
            connection.close();
            connection = null;
        }
        table = null;
        familyBytes = null;
        opened = false;
    }

    // ---------------- 查询 ----------------

    /**
     * 批量点查：N 个 key 合成一次 batch get，再统一挂上业务层硬超时。
     *
     * <p>返回的 Future 语义（对上层 {@link DualLookupFunction} 的承诺）：
     * 正常完成 = 查询成功（可能全都没命中，属正常业务结果）；
     * 异常完成 = 超时或 RPC 失败 → 触发降级。
     */
    @Override
    public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
        CompletableFuture<Map<Integer, List<Map<String, Object>>>> result = new CompletableFuture<>();
        execute(keys, result, true);
        // 关键：无条件挂上业务层硬超时，保证 Future 一定会终结
        return withTimeout(result, cfg.timeoutMs);
    }

    /**
     * 执行一次批量查询；遇到「连接已关闭」时重建连接并重试一次。
     *
     * <p>为什么必须重试：主源正常时 HBase（备源）可能几天都不被调用，连接早已被服务端空闲回收。
     * 「很久以来第一次降级」正好撞上死连接时，如果直接把 {@code ConnectionClosedException}
     * 抛给上层，这一批就会失败（两源都失败 → 作业失败），而连接其实重建后立刻可用。
     *
     * <p>只重试一次、且只针对连接类异常：普通查询异常（表不存在、权限不足）重试没有意义，
     * 交给上层降级。
     *
     * @param allowRetry 是否允许重连重试（内部递归时置 false，保证最多一次）
     */
    private void execute(List<Object[]> keys,
                         CompletableFuture<Map<Integer, List<Map<String, Object>>>> result,
                         boolean allowRetry) {
        if (result.isDone()) {
            return;
        }
        // 记录本次查询使用的连接实例：重试时只有「还是这条连接」才关闭重建，
        // 否则可能把别的线程刚刚重建好的新连接误关掉。
        final AsyncConnection usedConnection = connection;
        try {
            AsyncTable<AdvancedScanResultConsumer> currentTable = table;
            AsyncConnection currentConnection = usedConnection;
            byte[] family = familyBytes;
            if (currentTable == null || currentConnection == null
                    || currentConnection.isClosed() || family == null) {
                throw new ConnectionClosedException("HBase connection is not open");
            }

            // 过滤空 key，构建批量 Get；indexMap 记录每个 Get 对应的原始下标
            // 下标映射不可省：过滤掉空 key 后，Get 的下标与入参 keys 的下标就不再一一对应了
            List<Get> gets = new ArrayList<>();
            List<Integer> indexMap = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                Object[] k = keys.get(i);
                if (k == null || k.length == 0 || k[0] == null) {
                    continue; // 主键为空的记录直接跳过：结果里不含该下标，上层按「查不到」处理
                }
                Get get = new Get(buildRowKey(k));
                // 只取需要的列（列投影），减少网络与反序列化开销。
                // 用构造期预编码好的 columnBytes，避免每个 Get、每一列都重复 Bytes.toBytes
                for (int c = 0; c < columnBytes.length; c++) {
                    get.addColumn(family, columnBytes[c]);
                }
                gets.add(get);
                indexMap.add(i);
            }
            if (gets.isEmpty()) {
                // 整批都无有效 key：直接返回空 Map，连 IO 都不发起
                result.complete(Collections.emptyMap());
                return;
            }

            // HBase 2.x 的 table.get(List<Get>) 返回 List<CompletableFuture<Result>>，
            // 下标与 gets 一一对应，需用 allOf 组合等待全部完成
            List<CompletableFuture<Result>> futures = currentTable.get(gets);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            // 任一个 Get 失败（含超时），整批按失败处理，交由上层降级。
                            // 这是「批量语义」的必然取舍：部分成功的结果不完整，用不完整的维表数据
                            // 比走备源重查更危险（会静默产出错误的关联结果）
                            Throwable cause = unwrap(ex);
                            if (allowRetry && isConnectionClosed(cause)) {
                                retryAsync(keys, result, cause, usedConnection);
                                return;
                            }
                            result.completeExceptionally(cause);
                            return;
                        }
                        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
                        for (int j = 0; j < futures.size(); j++) {
                            Result r = futures.get(j).join(); // allOf 已完成，join 立即返回
                            if (r != null && !r.isEmpty()) {
                                // 未命中的 key 不写入 map：与「空结果不降级」的约定保持一致
                                map.put(indexMap.get(j), toRows(r));
                            }
                        }
                        result.complete(map);
                    });
        } catch (Throwable t) {
            // table.get() 本身同步抛错（连接已关闭、表不存在等）
            if (allowRetry && isConnectionClosed(t)) {
                retryAsync(keys, result, t, usedConnection);
                return;
            }
            result.completeExceptionally(t);
        }
    }

    /**
     * 异步重建连接后重试一次。
     *
     * <p>不在 HBase 的回调线程里直接 {@code open()}：那会阻塞 Netty 事件循环。
     * 放到**独立的重连线程池**执行，重连耗时不会计入下一批的 {@code lookup.timeout}，
     * 也不会像以前那样阻塞超时调度器、延迟其他批次的超时判定。
     */
    private void retryAsync(List<Object[]> keys,
                            CompletableFuture<Map<Integer, List<Map<String, Object>>>> result,
                            Throwable cause,
                            AsyncConnection failedConnection) {
        if (result.isDone()) {
            return;
        }
        LOG.warn("[dual-lookup] HBase 连接已断开，重建后重试本批（原因：{}）", cause.toString());
        // 用独立的重连线程池，绝不复用 timeoutScheduler：open() 是同步阻塞的，
        // 跑在超时调度器上会拖住同一时刻所有批次的超时任务。
        ExecutorService reconnect = reconnectExecutor;
        if (reconnect == null || reconnect.isShutdown()) {
            result.completeExceptionally(cause);
            return;
        }
        try {
            reconnect.execute(() -> {
                if (result.isDone()) {
                    return;
                }
                try {
                    synchronized (HBaseLookupReader.this) {
                        // HBase 服务端回收的往往只是底层 RPC 连接，AsyncConnection.isClosed() 仍为 false，
                        // 所以这里按「实例是否是失败的那一条」来判断，强制替换掉它。
                        if (connection == failedConnection) {
                            resetConnection();
                        }
                    }
                    open();
                } catch (Exception e) {
                    result.completeExceptionally(e);
                    return;
                }
                execute(keys, result, false);
            });
        } catch (RejectedExecutionException e) {
            result.completeExceptionally(cause);
        }
    }

    /** 异常链里是否属于「连接已关闭」这类可以通过重连恢复的错误 */
    private static boolean isConnectionClosed(Throwable t) {
        Throwable cur = t;
        for (int depth = 0; cur != null && depth < 16; depth++) {
            if (cur instanceof ConnectionClosedException
                    || cur instanceof java.nio.channels.ClosedChannelException) {
                return true;
            }
            String msg = cur.getMessage();
            if (msg != null && (msg.contains("Connection closed")
                    || msg.contains("Connection reset")
                    || msg.contains("connection is closed"))) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    /**
     * 业务层硬超时：到点若 Future 仍未完成，用 TimeoutException 强制终结它。
     *
     * <p>为什么要「强制终结」而不是取消底层请求？因为 Flink 的异步算子只认 Future 的状态，
     * 不认底层 RPC 是否还在跑。让 Future 立刻异常完成，就能马上降级到备源，
     * 底层那次慢 RPC 则交给客户端超时自行回收。
     */
    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> f, long timeoutMs) {
        if (timeoutMs <= 0) {
            return f; // 关闭超时（仅测试用；生产配置强校验必须 > 0）
        }
        // close() 与在途查询存在竞态：调度器可能已被 shutdown 并置空。
        // 此时不能再 schedule（NPE/RejectedExecutionException），直接让 Future 以「已关闭」结束，
        // 保证调用方一定能拿到结果，不会悬挂。
        ScheduledThreadPoolExecutor scheduler = timeoutScheduler;
        if (scheduler == null || scheduler.isShutdown()) {
            f.completeExceptionally(new IllegalStateException("[dual-lookup] HBaseLookupReader is closed"));
            return f;
        }
        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            if (!f.isDone()) { // 已完成则什么都不做，避免覆盖真实结果
                f.completeExceptionally(
                        new TimeoutException("HBase lookup timeout after " + timeoutMs + "ms"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        // 查询提前结束时立即取消超时任务，不让它在单线程调度器队列里空待到到期
        // （高 QPS 下队列里会堆积大量「结果早已返回」的任务，到期逐个空转扫描）
        f.whenComplete((v, ex) -> timeoutTask.cancel(false));
        return f;
    }

    // ---------------- 转换 ----------------

    /**
     * 由 join key 拼 HBase rowkey。
     *
     * <p><b>编码模式由 {@code hbase.rowkey.encoding} 决定，必须与被查表写入端严格一致</b>，
     * 否则表现为「一条也查不到」（空结果不触发降级，全部关联结果为空，很难从日志定位）：
     * <ul>
     *   <li>{@code string}（默认，兼容既有数据）：主键值一律转成字符串再取 UTF-8 字节。
     *       单主键直接编码；复合主键用 {@code hbase.rowkey.delimiter} 拼接。
     *       注意：DECIMAL 在这里也走 {@code toString()}；如果写入端用的是 HBase 的
     *       {@code Bytes.toBytes(BigDecimal)} 编码，请改用 {@code typed} 模式（两者对 DECIMAL 等价）；</li>
     *   <li>{@code typed}：按主键的 Flink 类型编码（BIGINT → {@code Bytes.toBytes(long)}、
     *       INTEGER → {@code Bytes.toBytes(int)} 等），与 {@link #decode} 的解码规则严格对称。
     *       TIMESTAMP 的精度约定与列值一致：precision ≤ 3 用毫秒，&gt; 3 用微秒。
     *       写入端按 HBase 惯例写类型化 rowkey 时必须用这个模式。</li>
     * </ul>
     */
    private byte[] buildRowKey(Object[] keyValues) {
        if (!"typed".equalsIgnoreCase(cfg.hbaseRowkeyEncoding)) {
            // ---- 字符串编码（默认）----
            if (keyValues.length == 1) {
                return stringBytes(keyValues[0]);
            }
            // 复合主键：逐段用 stringBytes 编码后再用分隔符的字节连接。
            // 不能先拼成字符串再统一转字节：byte[] 段会被 toString() 成对象哈希（[B@...），
            // 两个内容相同的数组会拼出不同的 rowkey。
            byte[] delim = Bytes.toBytes(cfg.hbaseRowkeyDelimiter);
            ByteArrayOutputStream out = new ByteArrayOutputStream(64);
            for (int i = 0; i < keyValues.length; i++) {
                if (i > 0) {
                    out.write(delim, 0, delim.length);
                }
                byte[] seg = stringBytes(keyValues[i]);
                out.write(seg, 0, seg.length);
            }
            return out.toByteArray();
        }

        // ---- 类型化编码 ----
        if (keyValues.length == 1) {
            return typedBytes(keyValues[0], keyTypes[0]);
        }
        // 复合主键：各段类型化编码后用「分隔符的字节」连接。
        // 不能像单段那样首尾直接相接：字符串段是变长的，直接拼接会产生经典的前缀歧义
        // （("a","bc") 与 ("ab","c") 会拼出同一串字节）
        byte[] delim = Bytes.toBytes(cfg.hbaseRowkeyDelimiter);
        ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        for (int i = 0; i < keyValues.length; i++) {
            if (i > 0) {
                out.write(delim, 0, delim.length);
            }
            byte[] seg = typedBytes(keyValues[i], i < keyTypes.length ? keyTypes[i] : null);
            out.write(seg, 0, seg.length);
        }
        return out.toByteArray();
    }

    /**
     * 字符串编码（{@code rowkey.encoding=string}）：byte[] 直接用；其余一律 {@code toString()}
     * 后取 UTF-8 字节。
     *
     * <p>这里刻意不对 BigDecimal 做 HBase 专用编码：{@code string} 的契约是「按字符串写 rowkey」，
     * 而 HBase 的 {@code Bytes.toBytes(BigDecimal)} 是另一种字节格式。此前单主键走 HBase 编码、
     * 复合主键却走 {@code toString()}，同一种类型在两条路径上编出不同字节，属于隐蔽的不一致。
     * DECIMAL 主键若要匹配 HBase BigDecimal 编码，请使用 {@code typed} 模式。
     */
    private byte[] stringBytes(Object v) {
        if (v == null) {
            return new byte[0];
        }
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        return Bytes.toBytes(v.toString());
    }

    /**
     * 类型化编码（{@code rowkey.encoding=typed}）：按主键声明的 Flink 类型编码，
     * 与 {@link #decode} 逐类型严格对称——这是「写入端与读取端用同一套编码」的保证。
     *
     * <p>注意 TINYINT/SMALLINT 也编成 4 字节 int：HBase 只有一种 4 字节整型，
     * 与 {@code decode} 里统一按 {@code Bytes.toInt} 解码的做法保持一致。
     *
     * <p>DATE/TIME/TIMESTAMP 的值是经 {@code RowDataConverter.toJavaValue} 转换后的
     * {@code java.sql.Date/Time/Timestamp}，这里反推回 Flink 内部的「天数/毫秒/毫秒或微秒」，
     * 与 {@code decode} 的 {@code Bytes.toInt/toLong} 对称。
     */
    private byte[] typedBytes(Object v, LogicalType type) {
        if (v == null) {
            return new byte[0];
        }
        if (type == null) {
            return stringBytes(v); // 防御：类型缺失时退回字符串编码，行为可预期
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBytes((Boolean) v);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Bytes.toBytes(((Number) v).intValue());
            case BIGINT:
                return Bytes.toBytes(((Number) v).longValue());
            case FLOAT:
                return Bytes.toBytes(((Number) v).floatValue());
            case DOUBLE:
                return Bytes.toBytes(((Number) v).doubleValue());
            case DECIMAL:
                return Bytes.toBytes((BigDecimal) v);
            case DATE:
                return Bytes.toBytes((int) ((java.sql.Date) v).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return Bytes.toBytes(
                        (int) (((java.sql.Time) v).toLocalTime().toNanoOfDay() / 1_000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                // 与 decode()/RowDataConverter 的约定保持一致：
                // TIMESTAMP(0..3) 存毫秒，TIMESTAMP(4..9) 存微秒。
                // 只按 getTime()（毫秒）写会和 precision>3 的列值解码路径错开 1000 倍，
                // 表现为「rowkey 能查到但时间全错」或「时间对但一条也查不到」。
                long millis = ((java.sql.Timestamp) v).getTime();
                int precision = (type instanceof TimestampType) ? ((TimestampType) type).getPrecision() : 3;
                return Bytes.toBytes(precision <= 3 ? millis : millis * 1000L);
            }
            case BINARY:
            case VARBINARY:
                return (v instanceof byte[]) ? (byte[]) v : Bytes.toBytes(v.toString());
            default:
                // CHAR/VARCHAR 及未知类型：UTF-8 字符串，与 decode 的兜底分支对称
                return Bytes.toBytes(v.toString());
        }
    }

    /** 一行 Result -> 单元素列表（接口需要 List 以兼容 Doris 侧一对多的情况） */
    private List<Map<String, Object>> toRows(Result result) {
        if (result == null || result.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> row = new HashMap<>(columnNames.length * 2);
        for (int i = 0; i < columnNames.length; i++) {
            // 列名字节与列名小写形式都是构造期预计算好的，运行期只读
            byte[] v = result.getValue(familyBytes, columnBytes[i]);
            row.put(columnNamesLower[i], decode(v, columnTypes[i]));
        }
        List<Map<String, Object>> out = new ArrayList<>(1);
        out.add(row);
        return out;
    }

    /**
     * HBase 里存的是字节数组，按 DDL 声明的类型还原成 Java 对象。
     *
     * <p>这里隐含一个约束：<b>写入 HBase 时的编码方式必须与 DDL 类型一致</b>。
     * 例如 DDL 声明 BIGINT，就必须用 {@code Bytes.toBytes(long)} 写入，
     * 若误写成字符串形式的字节，解码会得到错误结果。
     */
    private Object decode(byte[] v, LogicalType type) {
        if (v == null || v.length == 0) {
            return null; // 列不存在或为空，交给 RowDataConverter 处理成 NULL
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Bytes.toBoolean(v);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                // HBase 只有 int 一种 4 字节整型，TINYINT/SMALLINT 一律按 int 解码，
                // 由 RowDataConverter 再做窄化（byteValue/shortValue）
                return Bytes.toInt(v);
            case BIGINT:
                return Bytes.toLong(v);
            case FLOAT:
                return Bytes.toFloat(v);
            case DOUBLE:
                return Bytes.toDouble(v);
            case DECIMAL:
                return Bytes.toBigDecimal(v);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // Flink 内部 DATE/TIME 都是 int（天数 / 毫秒数），与 HBase 的 int 编码直接对应
                return Bytes.toInt(v);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Flink TIMESTAMP(3) 内部是 long 毫秒，与 HBase long 编码对应
                return Bytes.toLong(v);
            case BINARY:
            case VARBINARY:
                return v; // 二进制列原样返回
            case CHAR:
            case VARCHAR:
            default:
                // 兜底按 UTF-8 字符串解码，兼容新类型/未知类型
                return Bytes.toString(v);
        }
    }

    /** 剥掉 CompletableFuture 包装的 CompletionException，让上层日志/异常链看到真实原因 */
    private static Throwable unwrap(Throwable t) {
        return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null)
                ? t.getCause()
                : t;
    }
}
