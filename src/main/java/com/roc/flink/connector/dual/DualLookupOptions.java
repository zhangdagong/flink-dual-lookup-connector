package com.roc.flink.connector.dual;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;

/**
 * dual-lookup connector 的全部配置项 + 解析后的配置值对象。
 *
 * <p>本类承担两件事，务必区分：
 * <ol>
 *   <li><b>配置项声明</b>（{@link ConfigOption} 常量）：只描述「DDL WITH 子句里能写哪些 key、什么类型、默认值是什么」，
 *       由 {@link DualLookupTableSourceFactory#optionalOptions()} 收集后交给 Flink 做合法性校验
 *       （写了未声明的 key 会直接报错，这是防拼写错误的第一道防线）；</li>
 *   <li><b>配置值对象</b>（内部类 {@link Config}）：把 {@code ReadableConfig} 一次性读取、校验、拍平成普通字段，
 *       之后算子运行期只读这个对象，不再碰 Flink 的配置 API。</li>
 * </ol>
 *
 * <p><b>为什么要有 Config 这一层？</b>因为 Flink 会把 Lookup Function 序列化下发到各个 TaskManager
 * （甚至跨 JM/TM 网络传输），而 {@code ReadableConfig} 不可序列化、且体积大。
 * 拍平成基本类型的 {@code Config} 后，既满足 {@link Serializable} 要求，也避免每次查询都走配置查表。
 *
 * <p><b>单位约定</b>：所有时间类参数统一用「毫秒整数」，DDL 里直接写数字，无需带 ms 等单位。
 * 例外只有 {@link #DORIS_QUERY_TIMEOUT_SEC}（JDBC 原生 API 就是秒，跟随 JDBC 语义以免混淆）。
 */
public final class DualLookupOptions {

    /** 工具类：全静态成员，禁止实例化 */
    private DualLookupOptions() {}

    /** connector 标识符，DDL 里写 {@code 'connector' = 'dual-lookup'} 即由本工厂接管 */
    public static final String CONNECTOR_ID = "dual-lookup";

    /**
     * 算子层兜底超时（Flink 全局配置项，不是本连接器的参数）。
     *
     * <p>这里只是把它<b>读出来做交叉校验</b>，不会写回配置。刻意用 key 直接声明、
     * 而不是引用 Flink 的 {@code ExecutionConfigOptions} 常量，是为了避免与具体 Flink 版本的类路径绑定。
     *
     * <p>默认值与 Flink 自身保持一致（3 分钟），这样即使读不到该配置也不会造成误判。
     */
    private static final ConfigOption<Duration> ASYNC_LOOKUP_TIMEOUT =
            ConfigOptions.key("table.exec.async-lookup.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(3));

    /** 数据源枚举值：HBase（点查快、适合实时） */
    public static final String SRC_HBASE = "hbase";
    /** 数据源枚举值：Doris（SQL 能力强、适合兜底/大表） */
    public static final String SRC_DORIS = "doris";

    // ==================== 核心：主备路由与超时 ====================

    /**
     * 主查询源，取值 hbase 或 doris；另一个自动作为备源。
     *
     * <p>默认 hbase，因为实时链路里 HBase 单行点查延迟通常远低于走 SQL 的 Doris。
     * 想对调主备只改这一行，其余配置无需调整（两源表结构要求一致）。
     */
    public static final ConfigOption<String> PRIMARY =
            ConfigOptions.key("lookup.primary")
                    .stringType()
                    .defaultValue(SRC_HBASE);

    /**
     * 单次（批量）查询硬超时，单位毫秒，超过即判定该源故障、转查另一个源。
     *
     * <p>这是本连接器容错能力的核心阈值，是<b>业务层独立控制</b>的降级判据，
     * 与 HBase 客户端自身的 {@code hbase.rpc.timeout} 互不影响（后者只负责让后端尽早中止 RPC）。
     *
     * <p>调优建议：取值应略大于主源 P99 延迟，否则会把正常抖动误判为故障、频繁降级；
     * 同时必须让算子层兜底 {@code table.exec.async-lookup.timeout} 显著大于它（经验值 ×2 + 2s），
     * 否则降级还没走完，Flink 自己就先抛超时异常了。
     */
    public static final ConfigOption<Integer> LOOKUP_TIMEOUT =
            ConfigOptions.key("lookup.timeout")
                    .intType()
                    .defaultValue(500);

    // ==================== 批量（攒批） ====================

    /**
     * 攒批条数阈值：攒满这么多条就批量查一次，显著降低 Doris 的查询次数。
     *
     * <p>为什么必须攒批？Doris 走 JDBC + SQL，逐条查询的「网络往返 + SQL 解析」开销远大于 HBase 的 RPC 点查；
     * 把 N 条合成一次 {@code WHERE pk IN (...)}，交互次数降为 1/N，是 Doris 侧吞吐的关键。
     *
     * <p>注意：HBase 侧同样受益（一次 batch get 替代 N 次 Get），所以攒批对主备两源都生效。
     */
    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("lookup.batch.size")
                    .intType()
                    .defaultValue(50);

    /**
     * 攒批最大等待（毫秒）：攒不满 batch.size 时，最多等这么久就发一次，避免低流量下延迟无界。
     *
     * <p>没有这个兜底时间，流量稀疏时最后几条数据会一直躺在缓冲区里等「攒满」，
     * 导致端到端延迟失控。这两个条件（条数 or 时间）构成典型的「攒批双触发」。
     *
     * <p><b>有效批大小由它决定</b>：实际攒到的条数 ≈ 每子任务 QPS × max-wait（再受
     * {@link #BATCH_SIZE} 封顶）。高 QPS 下默认 5ms 只能攒到个位数——每子任务 500 QPS 时
     * 约 2.5 条/批，攒批形同虚设、SQL 次数远高于理论值；一般建议 30~50ms。
     * 上限受 {@link #LOOKUP_TIMEOUT} 约束（校验会拦下 max-wait &gt;= timeout 的组合），
     * 否则攒批等待本身就吃掉了超时预算。
     */
    public static final ConfigOption<Integer> BATCH_MAX_WAIT =
            ConfigOptions.key("lookup.batch.max-wait")
                    .intType()
                    .defaultValue(5);

    // ==================== 可观测性 ====================

    /**
     * 统计日志打印间隔（秒），默认 360（6 分钟），0 表示关闭。
     *
     * <p>用于巡检：能直接看出主源失败数、降级次数、平均批大小、各源平均读取耗时，
     * 从而判断是主源真的挂了、还是阈值配小了、还是单纯流量太低。
     *
     * <p><b>每行同时给出两个视角</b>，用 {@code ||} 分隔：{@code window=} 段是「本阶段」增量，
     * 只描述刚刚过去这一个间隔内发生了什么，因此可以直接读、不需要拿两行做减法；
     * {@code since-start} 段是「自启动累计」（准确说是本统计实例创建以来，TM 重启后归零），
     * 用来一眼看到全程总量与长期基线。判断「最近是否变慢」只看 {@code window=} 段——
     * 累计均值会被历史稀释。内部计数始终是累计值，漏看某行也不会丢数
     * （下一次输出会把两个区间合并），作业关闭时若还有新活动再补一行累计汇总。
     *
     * <p>默认取 6 分钟而不是更短，是因为该日志的价值在于「阶段趋势」而非「实时告警」——
     * 真正的故障有降级 WARN 即时暴露，统计日志只需足够频繁到能看出趋势即可。
     * 间隔过短（如 10 秒）时低流量任务每行都只有个位数样本、噪声大且刷屏。
     * 排查期间可临时调到 10~30 秒。
     */
    public static final ConfigOption<Integer> STATS_LOG_INTERVAL =
            ConfigOptions.key("lookup.stats.log-interval")
                    .intType()
                    .defaultValue(360);

    /**
     * 降级日志的最小输出间隔（秒），默认 10；<b>0 表示不限流</b>（每批降级都打一条，即最原始的行为）。
     *
     * <p><b>为什么需要它？</b>主源长时间故障时，「每批一条 WARN」会演变成日志洪峰：
     * 每分钟 5000 条流量、{@code max-wait=30ms} 下有效批大小约 2~3，即每秒约 30 批——
     * 一天近 300 万行 WARN。后果不只是占磁盘：TM 日志滚动会把故障发生前后几分钟的
     * 真实上下文冲掉，反而更难定位根因。
     *
     * <p>限流后的行为（三者配合，做到「洪峰被压平、数量不丢失」）：
     * <ul>
     *   <li>故障发生的第一批<b>一定</b>打印，含完整异常原因——这是最关键的诊断信息；</li>
     *   <li>之后每隔该间隔最多打印一条，并附带「期间被合并的批数」；</li>
     *   <li>主源恢复时补打一条 INFO，汇总本次故障一共降级了多少批，做到有始有终。</li>
     * </ul>
     * 精确的逐批次数不会因此丢失：要么在限流日志的合并计数里，要么在
     * {@link #STATS_LOG_INTERVAL} 统计日志的 {@code failover=} 字段里。
     *
     * <p>需要「每一次降级都留痕」（例如做故障复盘、统计精确的失败时序）时把它设为 0。
     */
    public static final ConfigOption<Integer> FAILOVER_LOG_INTERVAL =
            ConfigOptions.key("lookup.failover.log-interval")
                    .intType()
                    .defaultValue(10);

    // ==================== HBase ====================

    /** HBase 表名；缺省时回退为 DDL 里的表名（同名表场景）。支持「命名空间:表名」，如 dim:dim_account */
    public static final ConfigOption<String> HBASE_TABLE_NAME =
            ConfigOptions.key("hbase.table-name").stringType().noDefaultValue();

    /** ZooKeeper 地址（逗号分隔的 host 列表）；留空则使用 classpath 下的 hbase-site.xml */
    public static final ConfigOption<String> HBASE_ZK_QUORUM =
            ConfigOptions.key("hbase.zookeeper.quorum").stringType().noDefaultValue();

    /** ZooKeeper 客户端端口 */
    public static final ConfigOption<String> HBASE_ZK_PORT =
            ConfigOptions.key("hbase.zookeeper.property.clientPort").stringType().defaultValue("2181");

    /** Znodes 在 ZK 中的根路径；只有改过 hbase-site.xml 的 zookeeper.znode.parent 才需要动 */
    public static final ConfigOption<String> HBASE_ZNODE_PARENT =
            ConfigOptions.key("zookeeper.znode.parent").stringType().defaultValue("/hbase");

    /** 列族名：DDL 里所有业务列都从该列族下按「同名列」读取（本连接器不支持多列族） */
    public static final ConfigOption<String> HBASE_COLUMN_FAMILY =
            ConfigOptions.key("hbase.column-family").stringType().defaultValue("info");

    /** 复合主键拼接 rowkey 时的分隔符；单主键时该配置无效 */
    public static final ConfigOption<String> HBASE_ROWKEY_DELIMITER =
            ConfigOptions.key("hbase.rowkey.delimiter").stringType().defaultValue("|");

    /**
     * rowkey 编码方式，取值 {@code string}（默认）或 {@code typed}。
     *
     * <p>它决定「能不能查到数据」，必须与被查表的写入端严格一致：
     * <ul>
     *   <li>{@code string}：主键值一律转成字符串再取 UTF-8 字节（复合主键用
     *       {@code hbase.rowkey.delimiter} 拼接）。兼容「按字符串写 rowkey」的既有数据；</li>
     *   <li>{@code typed}：按主键的 Flink 类型编码（BIGINT → {@code Bytes.toBytes(long)}、
     *       INTEGER → {@code Bytes.toBytes(int)} …），与本连接器解码列值用的是同一套规则。
     *       写入端按 HBase 惯例写类型化 rowkey 时必须用这个模式。</li>
     * </ul>
     *
     * <p><b>选错的症状</b>：一条也查不到。而空结果不算故障、不触发降级，
     * 最终表现为整张维表关联结果全为 NULL——很难从日志定位根因。
     * 建连日志会打印实际生效的模式（{@code rowkeyEncoding=}），可据此核对。
     */
    public static final ConfigOption<String> HBASE_ROWKEY_ENCODING =
            ConfigOptions.key("hbase.rowkey.encoding").stringType().defaultValue("string");

    /** 单次 RPC 超时（毫秒）：让 HBase 客户端尽早中断一次 RPC，快速把控制权交回业务层降级 */
    public static final ConfigOption<Integer> HBASE_RPC_TIMEOUT_MS =
            ConfigOptions.key("hbase.rpc.timeout").intType().defaultValue(300);

    /**
     * 客户端整体操作超时（毫秒），含 region 冷启动定位，建议给足（如 2000）。
     *
     * <p>注意这个值可以大于 {@link #LOOKUP_TIMEOUT}：它的作用是让 HBase 客户端别「太快」放弃，
     * 真正的降级判断由业务层超时负责；两者职责不同，不要用同一个值去理解。
     */
    public static final ConfigOption<Integer> HBASE_OPERATION_TIMEOUT_MS =
            ConfigOptions.key("hbase.client.operation.timeout").intType().defaultValue(500);

    /** 重试次数：实时链路建议 0~1，重试越多越容易把一次卡顿放大成整批超时 */
    public static final ConfigOption<Integer> HBASE_RETRIES =
            ConfigOptions.key("hbase.client.retries.number").intType().defaultValue(1);

    // ---- HBase Kerberos（可选，默认 simple 免认证） ----

    /** 认证方式：simple（默认，免认证）或 kerberos */
    public static final ConfigOption<String> HBASE_SECURITY_AUTHENTICATION =
            ConfigOptions.key("hbase.security.authentication")
                    .stringType()
                    .defaultValue("simple");

    /** 客户端 keytab 文件路径（kerberos 下必填） */
    public static final ConfigOption<String> HBASE_CLIENT_KEYTAB_FILE =
            ConfigOptions.key("hbase.client.keytab.file").stringType().noDefaultValue();

    /** 客户端 Kerberos principal，如 hbase/ro@EXAMPLE.COM（kerberos 下必填） */
    public static final ConfigOption<String> HBASE_CLIENT_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.client.kerberos.principal").stringType().noDefaultValue();

    /** RegionServer 的 Kerberos principal，如 hbase/_HOST@EXAMPLE.COM（可选，缺省用 _HOST 规则） */
    public static final ConfigOption<String> HBASE_REGIONSERVER_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.regionserver.kerberos.principal").stringType().noDefaultValue();

    /** Master 的 Kerberos principal（可选） */
    public static final ConfigOption<String> HBASE_MASTER_KERBEROS_PRINCIPAL =
            ConfigOptions.key("hbase.master.kerberos.principal").stringType().noDefaultValue();

    /** krb5.conf 路径（可选，默认用系统 /etc/krb5.conf） */
    public static final ConfigOption<String> HBASE_KRB5_CONF =
            ConfigOptions.key("hbase.kerberos.krb5.conf").stringType().noDefaultValue();

    // ==================== Doris ====================

    /** Doris JDBC 连接串，如 jdbc:mysql://host:9030/dim；本连接器<b>唯一必填</b>的参数 */
    public static final ConfigOption<String> DORIS_JDBC_URL =
            ConfigOptions.key("doris.jdbc-url").stringType().noDefaultValue();

    /** Doris 表名；缺省时回退为 DDL 表名。支持「库名.表名」，如 dim.dim_account */
    public static final ConfigOption<String> DORIS_TABLE_NAME =
            ConfigOptions.key("doris.table-name").stringType().noDefaultValue();

    /** Doris 用户名 */
    public static final ConfigOption<String> DORIS_USERNAME =
            ConfigOptions.key("doris.username").stringType().defaultValue("root");

    /** Doris 密码，默认空串 */
    public static final ConfigOption<String> DORIS_PASSWORD =
            ConfigOptions.key("doris.password").stringType().defaultValue("");

    /** JDBC 驱动类名；Doris 走 MySQL 协议，因此默认用 MySQL 驱动 */
    public static final ConfigOption<String> DORIS_DRIVER =
            ConfigOptions.key("doris.driver").stringType().defaultValue("com.mysql.cj.jdbc.Driver");

    /**
     * JDBC Statement 的 queryTimeout（秒），服务端侧兜底。
     *
     * <p>运行期会被自动钳制到 {@code <= ceil(lookup.timeout/1000)}，防止「业务层已超时降级、
     * 但 JDBC 还在傻等」导致连接与线程被慢查询长期占用。详见 {@code DorisLookupReader.batchLookupAsync}。
     */
    public static final ConfigOption<Integer> DORIS_QUERY_TIMEOUT_SEC =
            ConfigOptions.key("doris.query.timeout").intType().defaultValue(1);

    // ---- 连接池（HikariCP）调优项 ----

    /**
     * 连接池最大连接数；Doris 作主源时建议调大（如 16）。
     *
     * <p>注意容量要按维表份数规划：同一作业里每张 dual-lookup 维表都有独立的连接池，
     * Doris 侧总连接数 = Σ（每表算子并行度 × 每表 pool.size）。多表 join 时别把每张表
     * 都按单表标准配，码表类小维表降到 2~4 即可。
     */
    public static final ConfigOption<Integer> DORIS_POOL_SIZE =
            ConfigOptions.key("doris.pool.size").intType().defaultValue(8);

    /** 连接池最小空闲连接数；必须 <= {@link #DORIS_POOL_SIZE}，否则启动即报错 */
    public static final ConfigOption<Integer> DORIS_POOL_MIN_IDLE =
            ConfigOptions.key("doris.pool.min-idle").intType().defaultValue(2);

    /**
     * 从连接池获取连接的超时（毫秒）；等待超过该时间会抛异常，进而触发降级。
     *
     * <p>默认 300ms 是有意压得比 {@link #LOOKUP_TIMEOUT} 更小：Doris 是备源、后面没有第三次降级机会，
     * 若连接池被打满时业务层先超时，整批就直接失败了。校验规则会拒绝「大于等于 lookup.timeout」的取值。
     *
     * <p>下限受 HikariCP 约束，不能小于 250ms。
     */
    public static final ConfigOption<Integer> DORIS_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("doris.connect.timeout").intType().defaultValue(300);

    /** 连接空闲多久被回收（毫秒） */
    public static final ConfigOption<Integer> DORIS_POOL_IDLE_TIMEOUT_MS =
            ConfigOptions.key("doris.pool.idle-timeout").intType().defaultValue(600000);

    /** 连接最大存活时间（毫秒），须大于 idle-timeout（HikariCP 的硬性约束） */
    public static final ConfigOption<Integer> DORIS_POOL_MAX_LIFETIME_MS =
            ConfigOptions.key("doris.pool.max-lifetime").intType().defaultValue(1800000);

    /**
     * 借出连接时的有效性校验超时（毫秒），用于剔除已被服务端断开的死连接。
     *
     * <p>默认 250ms（HikariCP 允许的最小值）：HikariCP 要求它不大于
     * {@link #DORIS_CONNECT_TIMEOUT_MS}（配置校验会拒绝违反该约束的组合）。
     * 校验本身只是发一次轻量 ping，250ms 对正常连接绰绰有余。
     */
    public static final ConfigOption<Integer> DORIS_POOL_VALIDATION_TIMEOUT_MS =
            ConfigOptions.key("doris.pool.validation-timeout").intType().defaultValue(250);

    // ==================== 配置值对象 ====================

    /**
     * 从 WITH 子句解析出来的只读配置（必须可序列化，会随 Function 下发到 TaskManager）。
     *
     * <p>设计要点：
     * <ul>
     *   <li>字段全部是基本类型/String，{@link Serializable} 天然成立，避免下发时序列化失败；</li>
     *   <li>只暴露 {@link #from(ReadableConfig, String)} 一个构造入口，保证「读取 + 校验」一次性完成，
     *       校验失败会在建表阶段（而非作业运行期）就抛错，问题暴露得足够早；</li>
     *   <li>字段是 public 的直接访问，省掉 getter 样板代码，本类只在连接器内部使用、不对外发布。</li>
     * </ul>
     */
    public static class Config implements Serializable {

        private static final long serialVersionUID = 1L;

        public String primary;          // 主源：hbase | doris
        public String standby;          // 备源：由 primary 推导
        public int timeoutMs;           // 单次（批量）查询硬超时（毫秒）

        public int batchSize;           // 攒批条数阈值
        public int batchMaxWaitMs;      // 攒批最大等待（毫秒）
        public int statsLogIntervalSec; // 统计日志间隔（秒），0 关闭；日志数值为本阶段增量而非累计
        public int failoverLogIntervalSec; // 降级日志最小间隔（秒），0 不限流

        public String hbaseTableName;
        public String hbaseZkQuorum;
        public String hbaseZkPort;
        public String hbaseZnodeParent;
        public String hbaseColumnFamily;
        public String hbaseRowkeyDelimiter;
        public String hbaseRowkeyEncoding;  // rowkey 编码：string（默认）| typed
        public int hbaseRpcTimeoutMs;
        public int hbaseOperationTimeoutMs;
        public int hbaseRetries;
        public String hbaseSecurityAuthentication;
        public String hbaseClientKeytabFile;
        public String hbaseClientKerberosPrincipal;
        public String hbaseRegionserverKerberosPrincipal;
        public String hbaseMasterKerberosPrincipal;
        public String hbaseKrb5Conf;

        public String dorisJdbcUrl;
        public String dorisTableName;
        public String dorisUsername;
        public String dorisPassword;
        public String dorisDriver;
        public int dorisQueryTimeoutSec;
        public int dorisPoolSize;
        public int dorisPoolMinIdle;
        public int dorisConnectTimeoutMs;
        public int dorisPoolIdleTimeoutMs;
        public int dorisPoolMaxLifetimeMs;
        public int dorisPoolValidationTimeoutMs;

        /** 私有构造：只允许通过 {@link #from} 创建，防止出现「字段没赋值就被使用」的半成品对象 */
        private Config() {}

        /**
         * 读取并校验全部配置。
         *
         * @param c            Flink 已合并好的配置视图（WITH 子句 + 全局配置）
         * @param ddlTableName DDL 中声明的表名；HBase/Doris 表名缺省时回退到它（同名表场景）
         * @return 已完成校验的配置对象
         * @throws IllegalArgumentException 配置不合法时抛出，Flink 会在建表阶段直接失败并打印原因
         */
        public static Config from(ReadableConfig c, String ddlTableName) {
            Config cfg = new Config();

            // ---- 主备路由：只声明主源，备源由代码推导，避免用户配出「主备同源」的无效组合 ----
            cfg.primary = normalize(c.get(PRIMARY));
            cfg.standby = SRC_HBASE.equals(cfg.primary) ? SRC_DORIS : SRC_HBASE;
            cfg.timeoutMs = c.get(LOOKUP_TIMEOUT);

            cfg.batchSize = c.get(BATCH_SIZE);
            cfg.batchMaxWaitMs = c.get(BATCH_MAX_WAIT);
            cfg.statsLogIntervalSec = c.get(STATS_LOG_INTERVAL);
            cfg.failoverLogIntervalSec = c.get(FAILOVER_LOG_INTERVAL);

            // getOptional(...).orElse(ddlTableName)：表名支持「不配就与 DDL 表名同名」的快捷写法
            cfg.hbaseTableName = c.getOptional(HBASE_TABLE_NAME).orElse(ddlTableName);
            cfg.hbaseZkQuorum = c.getOptional(HBASE_ZK_QUORUM).orElse("");
            cfg.hbaseZkPort = c.get(HBASE_ZK_PORT);
            cfg.hbaseZnodeParent = c.get(HBASE_ZNODE_PARENT);
            cfg.hbaseColumnFamily = c.get(HBASE_COLUMN_FAMILY);
            cfg.hbaseRowkeyDelimiter = c.get(HBASE_ROWKEY_DELIMITER);
            cfg.hbaseRowkeyEncoding = normalizeRowkeyEncoding(c.get(HBASE_ROWKEY_ENCODING));
            cfg.hbaseRpcTimeoutMs = c.get(HBASE_RPC_TIMEOUT_MS);
            cfg.hbaseOperationTimeoutMs = c.get(HBASE_OPERATION_TIMEOUT_MS);
            cfg.hbaseRetries = c.get(HBASE_RETRIES);
            cfg.hbaseSecurityAuthentication = c.get(HBASE_SECURITY_AUTHENTICATION);
            cfg.hbaseClientKeytabFile = c.getOptional(HBASE_CLIENT_KEYTAB_FILE).orElse("");
            cfg.hbaseClientKerberosPrincipal = c.getOptional(HBASE_CLIENT_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseRegionserverKerberosPrincipal = c.getOptional(HBASE_REGIONSERVER_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseMasterKerberosPrincipal = c.getOptional(HBASE_MASTER_KERBEROS_PRINCIPAL).orElse("");
            cfg.hbaseKrb5Conf = c.getOptional(HBASE_KRB5_CONF).orElse("");

            cfg.dorisJdbcUrl = c.get(DORIS_JDBC_URL);
            cfg.dorisTableName = c.getOptional(DORIS_TABLE_NAME).orElse(ddlTableName);
            cfg.dorisUsername = c.get(DORIS_USERNAME);
            cfg.dorisPassword = c.get(DORIS_PASSWORD);
            cfg.dorisDriver = c.get(DORIS_DRIVER);
            cfg.dorisQueryTimeoutSec = c.get(DORIS_QUERY_TIMEOUT_SEC);
            cfg.dorisPoolSize = c.get(DORIS_POOL_SIZE);
            cfg.dorisPoolMinIdle = c.get(DORIS_POOL_MIN_IDLE);
            cfg.dorisConnectTimeoutMs = c.get(DORIS_CONNECT_TIMEOUT_MS);
            cfg.dorisPoolIdleTimeoutMs = c.get(DORIS_POOL_IDLE_TIMEOUT_MS);
            cfg.dorisPoolMaxLifetimeMs = c.get(DORIS_POOL_MAX_LIFETIME_MS);
            cfg.dorisPoolValidationTimeoutMs = c.get(DORIS_POOL_VALIDATION_TIMEOUT_MS);

            // ---- 跨字段校验：单看一个 ConfigOption 无法发现的错误组合，统一在这里拦截 ----

            // 超时为 0/负数会让「超时降级」永不触发，等同于容错机制失效，必须拒绝
            if (cfg.timeoutMs <= 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.timeout 必须大于 0");
            }
            // 攒批条数为 0 会导致批永远发不出去（攒不满）；
            // 等待时间为负则等价于「不做时间兜底」，低流量下延迟会无界增长
            if (cfg.batchSize <= 0 || cfg.batchMaxWaitMs < 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.batch.size 必须大于 0，batch.max-wait 不能为负");
            }
            // 负间隔没有意义（0 已经表达了「不限流」这个语义），拦下来避免被当成「关闭日志」误用
            if (cfg.failoverLogIntervalSec < 0) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.failover.log-interval 不能为负，0 表示不限流（每批降级都打印）");
            }
            // 同理：负数与 0 在行为上无法区分（都等于关闭打点），与其静默照做，不如在建表阶段拦下
            if (cfg.statsLogIntervalSec < 0) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.stats.log-interval 不能为负，0 表示关闭统计日志");
            }

            // ---- 超时预算的四条交叉校验 ----
            // 这几个参数单独看都合法，组合起来才会失效，因此必须放在跨字段校验里拦（README 已把它们列为反模式）

            // (1) 攒批等待必须小于业务超时：否则「时间兜底」还没触发，批次就先被判超时降级了
            if (cfg.batchMaxWaitMs >= cfg.timeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.batch.max-wait(" + cfg.batchMaxWaitMs + "ms) 必须小于 "
                                + "lookup.timeout(" + cfg.timeoutMs + "ms)，"
                                + "否则攒批等待会吃掉整个超时预算，批次等不到定时触发就被判超时");
            }
            // (2) HBase RPC 超时必须小于业务超时：它只负责让 HBase 尽早中断并释放资源，
            //     真正的降级判据是业务层超时；反过来会出现「业务层已切源、HBase 还在傻等 RPC」
            if (cfg.hbaseRpcTimeoutMs >= cfg.timeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.rpc.timeout(" + cfg.hbaseRpcTimeoutMs + "ms) 必须小于 "
                                + "lookup.timeout(" + cfg.timeoutMs + "ms)，"
                                + "否则业务层已经降级切源，HBase 侧还在等待那次 RPC");
            }
            // (3) Doris 取连接超时必须小于业务超时：Doris 是备源、后面没有第三次降级机会，
            //     连接池被打满时若业务层先超时，这一批就直接失败了
            if (cfg.dorisConnectTimeoutMs >= cfg.timeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] doris.connect.timeout(" + cfg.dorisConnectTimeoutMs + "ms) 必须小于 "
                                + "lookup.timeout(" + cfg.timeoutMs + "ms)，"
                                + "否则连接池被打满时业务层先超时，而备源之后无处可降、整批直接失败");
            }
            // (4) 算子层兜底超时必须大于业务超时：否则降级还没走完，Flink 的异步算子自己就先判超时了，
            //     容错逻辑形同虚设。该值是 Flink 全局配置，读不到时会取 Flink 自身的默认值
            long asyncLookupTimeoutMs = c.get(ASYNC_LOOKUP_TIMEOUT).toMillis();
            if (asyncLookupTimeoutMs <= cfg.timeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] table.exec.async-lookup.timeout(" + asyncLookupTimeoutMs + "ms) 必须大于 "
                                + "lookup.timeout(" + cfg.timeoutMs + "ms)，"
                                + "否则降级逻辑还没执行完，Flink 侧就先把这次查找判为超时了");
            }
            // HikariCP 的硬性约束：minIdle 不得超过 maximumPoolSize
            if (cfg.dorisPoolMinIdle > cfg.dorisPoolSize) {
                throw new IllegalArgumentException("[dual-lookup] doris.pool.min-idle 不能大于 doris.pool.size");
            }
            // HikariCP 的另一条硬性约束：连接校验超时不能大于取连接超时
            if (cfg.dorisPoolValidationTimeoutMs > cfg.dorisConnectTimeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] doris.pool.validation-timeout(" + cfg.dorisPoolValidationTimeoutMs
                                + "ms) 不能大于 doris.connect.timeout(" + cfg.dorisConnectTimeoutMs
                                + "ms)，这是 HikariCP 的硬性约束");
            }
            // 备源（Doris）不可用时整个容错链路就断了，因此 URL 是唯一必填项
            if (cfg.dorisJdbcUrl == null || cfg.dorisJdbcUrl.trim().isEmpty()) {
                throw new IllegalArgumentException("[dual-lookup] doris.jdbc-url 必填");
            }
            // Kerberos 模式下 keytab 与 principal 必须成对出现，缺一无法完成登录
            if ("kerberos".equalsIgnoreCase(cfg.hbaseSecurityAuthentication)
                    && (cfg.hbaseClientKeytabFile.trim().isEmpty()
                    || cfg.hbaseClientKerberosPrincipal.trim().isEmpty())) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.security.authentication=kerberos 时，"
                                + "hbase.client.keytab.file 与 hbase.client.kerberos.principal 必填");
            }
            return cfg;
        }

        /**
         * 归一化并校验主源取值：忽略大小写/首尾空格，只接受 hbase 或 doris。
         *
         * <p>尽早把「写法五花八门」收敛成枚举字符串，下游路由判断都用 {@code equals} 比较，
         * 不必再考虑大小写问题。
         */
        private static String normalize(String raw) {
            String v = raw == null ? "" : raw.trim().toLowerCase();
            if (!SRC_HBASE.equals(v) && !SRC_DORIS.equals(v)) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.primary 只能取值 HBase 或 Doris，当前为: " + raw);
            }
            return v;
        }

        /**
         * 归一化并校验 rowkey 编码模式：忽略大小写/首尾空格，只接受 string 或 typed。
         *
         * <p>为什么要在建表阶段就拦下拼错的值？因为这个配置写错不会报任何错，
         * 只会「静默查不到数据」，是最难排查的一类故障。
         */
        private static String normalizeRowkeyEncoding(String raw) {
            String v = raw == null ? "" : raw.trim().toLowerCase();
            if (!"string".equals(v) && !"typed".equals(v)) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.rowkey.encoding 只能取值 string 或 typed，当前为: " + raw);
            }
            return v;
        }
    }
}
