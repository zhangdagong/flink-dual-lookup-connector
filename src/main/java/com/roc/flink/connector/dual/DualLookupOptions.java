package com.roc.flink.connector.dual;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
     *
     * <p><b>联动推导</b>：显式设置本项后，未显式配置的依赖项会自动跟随：
     * {@code hbase.rpc.timeout = 80%}、{@code hbase.client.operation.timeout = max(500, 本值)}、
     * {@code doris.connect.timeout = 80% 并夹到 [250,300]ms}。显式写了的参数优先，不会被覆盖。
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

    /**
     * 单次 RPC 超时（毫秒）：让 HBase 客户端尽早中断一次 RPC，快速把控制权交回业务层降级。
     *
     * <p>显式设置 {@code lookup.timeout} 且本项未显式设置时，自动取 80%（留 20% 给降级判定）。
     */
    public static final ConfigOption<Integer> HBASE_RPC_TIMEOUT_MS =
            ConfigOptions.key("hbase.rpc.timeout").intType().defaultValue(300);

    /**
     * 客户端整体操作超时（毫秒），含 region 冷启动定位，建议给足（如 2000）。
     *
     * <p>注意这个值可以大于 {@link #LOOKUP_TIMEOUT}：它的作用是让 HBase 客户端别「太快」放弃，
     * 真正的降级判断由业务层超时负责；两者职责不同，不要用同一个值去理解。
     *
     * <p>显式设置 {@code lookup.timeout} 且本项未显式设置时，自动取 {@code max(500, lookup.timeout)}，
     * 避免 HBase 先于业务层放弃。
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

    /**
     * SASL 保护级别：authentication（默认，只认证）、integrity（校验完整性）、privacy（加密）。
     *
     * <p>对应 HBase 的 {@code hbase.rpc.protection}。多数集群保持默认 authentication 即可；
     * 若服务端配置了 {@code privacy}/更高的级别，客户端必须一致，否则握手阶段就会失败。
     * 这类失败在日志里通常只表现为 "SaslException: ... GSS initiate failed"，很难反推配置项，
     * 因此把它显式暴露成参数，建表时写清楚。
     */
    public static final ConfigOption<String> HBASE_RPC_PROTECTION =
            ConfigOptions.key("hbase.rpc.protection")
                    .stringType()
                    .defaultValue("authentication");

    /**
     * 客户端 keytab 文件路径（<b>可选</b>）。
     *
     * <p>不配置时复用 Flink 全局登录（{@code security.kerberos.login.keytab/principal}）或 kinit 凭据——
     * 这与 flink-hbase 的行为一致；配置时用于「单表覆盖全局登录用户」。与 principal 必须成对出现。
     *
     * <p>也可以用 {@code properties.hbase.client.keytab.file} 提供，两者等价（显式参数优先）。
     * <b>注意 {@code hbase.master.keytab.file} 是 HBase Master 服务端配置，不是客户端凭据</b>，
     * 连接器不会拿它登录；从 flink-hbase 迁移时若只写了它，应改用本参数或直接复用 Flink 全局登录。
     */
    public static final ConfigOption<String> HBASE_CLIENT_KEYTAB_FILE =
            ConfigOptions.key("hbase.client.keytab.file").stringType().noDefaultValue();

    /**
     * 客户端 Kerberos principal，形如 {@code ro@EXAMPLE.COM}（<b>可选</b>，与 keytab 成对）。
     *
     * <p><b>为什么这里叫 kerberos.principal 而运行期要写两个 key？</b>
     * HBase 2.2.x 的 {@code AuthUtil.loginClient()} 实际读取的常量值却是
     * <b>{@code hbase.client.keytab.principal}</b>（常量名 {@code HBASE_CLIENT_KERBEROS_PRINCIPAL}
     * 与它的字符串值不一致，官方 javadoc 又写作 {@code hbase.client.kerberos.principal}）。
     * 只写文档里那个 key 时，{@code shouldLoginFromKeytab()} 会判定"没配 principal"，
     * 于是 keytab 自动登录被静默跳过——这正是"配了 Kerberos 却始终认证不上"的常见原因。
     *
     * <p>因此 {@code HBaseLookupReader} 在显式 keytab 模式下会把同一个值同时写入两个 key。
     *
     * <p><b>注意：客户端 principal 必须是具体账号，不能含 {@code _HOST}</b>。
     * {@code _HOST} 是服务端（RS/Master）principal 的占位符，客户端用它登录只会失败，
     * 且报错信息与根因相距很远，故在建表阶段直接拦下。
     *
     * <p>也可以用 {@code properties.hbase.client.kerberos.principal}（或 HBase 2.2.x 实际读取的
     * {@code properties.hbase.client.keytab.principal}）提供，显式参数优先。
     */
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
     * JDBC Statement 的 queryTimeout（秒）。
     *
     * <p><b>0（默认）</b>：不调用 {@code Statement.setQueryTimeout}，由 Doris 集群/会话的
     * {@code query_timeout} 等默认机制控制；业务层的 {@code lookup.timeout} 仍然是降级判据，
     * 到点会调用 {@code Statement.cancel()} 主动中止 SQL。
     *
     * <p><b>&gt;0</b>：显式设置 JDBC queryTimeout，且<b>不再被 lookup.timeout 钳制</b>。
     * 需要更长的 Doris 服务端超时就直接配大；业务超时仍由 lookup.timeout 独立控制。
     */
    public static final ConfigOption<Integer> DORIS_QUERY_TIMEOUT_SEC =
            ConfigOptions.key("doris.query.timeout").intType().defaultValue(0);

    // ---- 连接池（HikariCP）调优项 ----

    /**
     * 连接池最大连接数（默认 4）；Doris 作主源时建议调大（如 16）。
     *
     * <p>注意容量要按维表份数规划：同一作业里每张 dual-lookup 维表都有独立的连接池，
     * Doris 侧总连接数 = Σ（每表算子并行度 × 每表 pool.size）。多表 join 时别把每张表
     * 都按单表标准配，码表类小维表降到 2~4 即可。
     *
     * <p>默认值取 4 而不是 8：Doris 在多数部署里是备源，长期空闲，没必要为每张表预留
     * 8 个连接；4 仍能满足「并行查询 + 连接自愈」的基本盘，Doris 作主源时再显式调到 8~16。
     */
    public static final ConfigOption<Integer> DORIS_POOL_SIZE =
            ConfigOptions.key("doris.pool.size").intType().defaultValue(4);

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
     *
     * <p>显式设置 {@code lookup.timeout} 且本项未显式设置时，自动取 80% 并夹到 [250, 300]ms：
     * 80% 保证池先于业务层失败，300ms 上限则避免最后一跳把预算全花在等连接上。
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
        public String hbaseSecurityAuthentication;   // simple（默认）| kerberos
        public String hbaseRpcProtection;            // authentication（默认）| integrity | privacy
        public String hbaseClientKeytabFile;
        public String hbaseClientKerberosPrincipal;
        public String hbaseRegionserverKerberosPrincipal;
        public String hbaseMasterKerberosPrincipal;
        public String hbaseKrb5Conf;
        /**
         * DDL 里 {@code properties.*} 透传的 HBase 客户端配置（前缀已去掉）。
         *
         * <p>与 flink-hbase 完全一致：它的工厂用 {@code helper.validateExcept("properties.")} 放行，
         * 再由 {@code HBaseConnectorOptionsUtil.getHBaseConfiguration()} 逐项写入 HBase Configuration。
         * 这样迁移过来的 DDL 可以原样写 {@code 'properties.hbase.client.keytab.file'} 等参数。
         */
        public Map<String, String> hbaseProperties;

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

        /** lookup.timeout 显式设置时，未显式配置的 hbase.rpc.timeout 自动取 80% */
        private static final double HBASE_RPC_TIMEOUT_RATIO = 0.8;
        /** hbase.client.operation.timeout 自动推导时的下限（保留冷启动 region 定位余量） */
        private static final int HBASE_OPERATION_TIMEOUT_MIN_MS = 500;
        /**
         * Doris 取连接超时的自动化比例。
         *
         * <p>虽然 80% 能保证「池先失败、业务再降级」，但 Doris 是最后一跳：
         * 等待连接就花掉 80% 的话，留给 SQL 执行的时间只剩 20%。
         * 因此 80% 只作为上限参考，再夹到 [250, 300]ms——250 是 HikariCP 的下限，
         * 300 是原默认值，避免大 timeout 场景把预算全花在等连接上。
         */
        private static final double DORIS_CONNECT_TIMEOUT_RATIO = 0.8;
        private static final int DORIS_CONNECT_TIMEOUT_MIN_MS = 250;
        private static final int DORIS_CONNECT_TIMEOUT_MAX_MS = 300;

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
            return from(c, ddlTableName, Collections.<String, String>emptyMap(), null);
        }

        /**
         * 读取并校验全部配置，并接收 {@code properties.*} 透传项。
         *
         * @param hbaseProperties DDL 中 {@code properties.xxx} 去掉前缀后的键值对（可为空但不为 null）
         */
        public static Config from(ReadableConfig c, String ddlTableName, Map<String, String> hbaseProperties) {
            return from(c, ddlTableName, hbaseProperties, null);
        }

        /**
         * 读取并校验全部配置。
         *
         * @param globalConfig Session 级配置，即 {@code SET 'table.exec.async-lookup.timeout' = '10s'} 所在的那份配置。
         *                     这个 key 不在 CatalogTable 的 WITH 选项里，必须由 Factory 从
         *                     {@code context.getConfiguration()} 单独传入；否则第 (4) 条交叉校验
         *                     只能拿到 Flink 默认的 3 分钟，用户把算子超时调小也发现不了。
         *                     传 null 时退回 {@code c}（单测与旧调用路径）。
         */
        public static Config from(ReadableConfig c, String ddlTableName,
                                  Map<String, String> hbaseProperties, ReadableConfig globalConfig) {
            Config cfg = new Config();
            cfg.hbaseProperties = new HashMap<>(
                    hbaseProperties == null ? Collections.<String, String>emptyMap() : hbaseProperties);

            // ---- 主备路由：只声明主源，备源由代码推导，避免用户配出「主备同源」的无效组合 ----
            cfg.primary = normalize(c.get(PRIMARY));
            cfg.standby = SRC_HBASE.equals(cfg.primary) ? SRC_DORIS : SRC_HBASE;
            cfg.timeoutMs = c.get(LOOKUP_TIMEOUT);
            // 只有用户显式调过 lookup.timeout，才联动推导其他超时；用默认 500 时保持原有默认值
            boolean lookupTimeoutExplicit = hasExplicit(c, LOOKUP_TIMEOUT);

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
            // 兼容 flink-hbase 的写法：properties.hbase.security.authentication / rpc.protection
            // 只在显式参数没写时生效（显式参数优先级更高）。
            // 这一步很关键：迁移过来的 DDL 往往只写 properties.*，如果这里仍按显式参数的默认值
            // 判成 simple，applyKerberos() 会直接 return，整套 Kerberos 逻辑都被跳过。
            String authentication = c.get(HBASE_SECURITY_AUTHENTICATION);
            if (!hasExplicit(c, HBASE_SECURITY_AUTHENTICATION)) {
                String fromProperties = cfg.hbaseProperties.get("hbase.security.authentication");
                if (fromProperties != null && !fromProperties.trim().isEmpty()) {
                    authentication = fromProperties;
                }
            }
            cfg.hbaseSecurityAuthentication = normalizeAuthentication(authentication);

            String rpcProtection = c.get(HBASE_RPC_PROTECTION);
            if (!hasExplicit(c, HBASE_RPC_PROTECTION)) {
                String fromProperties = cfg.hbaseProperties.get("hbase.rpc.protection");
                if (fromProperties != null && !fromProperties.trim().isEmpty()) {
                    rpcProtection = fromProperties;
                }
            }
            cfg.hbaseRpcProtection = normalizeRpcProtection(rpcProtection);

            // 客户端 keytab/principal 同样允许由 properties.* 提供（与 flink-hbase 对齐）。
            // 注意 hbase.master.keytab.file 是 HBase Master 服务端配置，绝不能当成客户端凭据；
            // 这里只认 hbase.client.*，master 相关的 key 原样透传给 HBase Configuration 即可。
            cfg.hbaseClientKeytabFile = c.getOptional(HBASE_CLIENT_KEYTAB_FILE).orElse("");
            if (cfg.hbaseClientKeytabFile.trim().isEmpty()) {
                cfg.hbaseClientKeytabFile = firstNonEmpty(
                        cfg.hbaseProperties.get("hbase.client.keytab.file"));
            }
            cfg.hbaseClientKerberosPrincipal = c.getOptional(HBASE_CLIENT_KERBEROS_PRINCIPAL).orElse("");
            if (cfg.hbaseClientKerberosPrincipal.trim().isEmpty()) {
                cfg.hbaseClientKerberosPrincipal = firstNonEmpty(
                        cfg.hbaseProperties.get("hbase.client.kerberos.principal"),
                        cfg.hbaseProperties.get("hbase.client.keytab.principal"));
            }
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

            // 显式调小/调大 lookup.timeout 时，未显式配置的依赖项跟着推导，避免「只改一个参数建表就失败」
            applyDerivedTimeouts(c, cfg, lookupTimeoutExplicit);

            // ---- 跨字段校验：单看一个 ConfigOption 无法发现的错误组合，统一在这里拦截 ----

            // 超时为 0/负数会让「超时降级」永不触发，等同于容错机制失效，必须拒绝
            if (cfg.timeoutMs <= 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.timeout 必须大于 0");
            }
            // 未显式配置 doris.connect.timeout 时，自动推导会被 HikariCP 的 250ms 下限顶住，
            // 无法再满足 < lookup.timeout；这里给出比通用校验更可操作的提示。
            if (lookupTimeoutExplicit && !hasExplicit(c, DORIS_CONNECT_TIMEOUT_MS)
                    && cfg.timeoutMs <= DORIS_CONNECT_TIMEOUT_MIN_MS) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.timeout(" + cfg.timeoutMs + "ms) 必须大于 "
                                + DORIS_CONNECT_TIMEOUT_MIN_MS + "ms：未显式配置 doris.connect.timeout 时，"
                                + "连接器会自动取 80% 并夹到 [250, 300]ms；而 250ms 是 HikariCP 的 "
                                + "connectionTimeout 下限，无法同时满足 < lookup.timeout。"
                                + "请调大 lookup.timeout，或显式配置 doris.connect.timeout（仍需 >= 250ms）");
            }
            // 攒批条数为 0 会导致批永远发不出去（攒不满）；
            // 等待时间为负则等价于「不做时间兜底」，低流量下延迟会无界增长
            if (cfg.batchSize <= 0 || cfg.batchMaxWaitMs < 0) {
                throw new IllegalArgumentException("[dual-lookup] lookup.batch.size 必须大于 0，batch.max-wait 不能为负");
            }
            // 0 表示不设置 JDBC queryTimeout，交给 Doris 集群/会话默认；负数没有意义
            if (cfg.dorisQueryTimeoutSec < 0) {
                throw new IllegalArgumentException(
                        "[dual-lookup] doris.query.timeout 不能为负：0 表示使用 Doris 集群/会话默认，"
                                + "大于 0 表示显式 JDBC 超时（秒）");
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
            if (cfg.dorisConnectTimeoutMs < DORIS_CONNECT_TIMEOUT_MIN_MS) {
                throw new IllegalArgumentException(
                        "[dual-lookup] doris.connect.timeout(" + cfg.dorisConnectTimeoutMs + "ms) 不能小于 "
                                + DORIS_CONNECT_TIMEOUT_MIN_MS + "ms，这是 HikariCP 的硬性下限");
            }
            if (cfg.dorisConnectTimeoutMs >= cfg.timeoutMs) {
                throw new IllegalArgumentException(
                        "[dual-lookup] doris.connect.timeout(" + cfg.dorisConnectTimeoutMs + "ms) 必须小于 "
                                + "lookup.timeout(" + cfg.timeoutMs + "ms)，"
                                + "否则连接池被打满时业务层先超时，而备源之后无处可降、整批直接失败");
            }
            // (4) 算子层兜底超时必须大于业务超时：否则降级还没走完，Flink 的异步算子自己就先判超时了，
            //     容错逻辑形同虚设。该值是 Flink 全局配置，读不到时会取 Flink 自身的默认值
            ReadableConfig asyncConfig =
                    (globalConfig != null && hasExplicit(globalConfig, ASYNC_LOOKUP_TIMEOUT)) ? globalConfig : c;
            long asyncLookupTimeoutMs = asyncConfig.get(ASYNC_LOOKUP_TIMEOUT).toMillis();
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
            // Kerberos 模式：keytab 与 principal 允许都不配置。
            // 不配置时复用 Flink 全局登录（security.kerberos.login.keytab/principal）或 kinit 凭据，
            // 这与 flink-hbase 的行为一致：flink-hbase 自己不读 keytab，直接用 Flink 已登录的 UGI。
            // 但只配一半（有 keytab 没 principal，或反之）无法完成登录，必须建表期拦下。
            if ("kerberos".equals(cfg.hbaseSecurityAuthentication)) {
                boolean hasKeytab = !cfg.hbaseClientKeytabFile.trim().isEmpty();
                boolean hasPrincipal = !cfg.hbaseClientKerberosPrincipal.trim().isEmpty();
                if (hasKeytab != hasPrincipal) {
                    throw new IllegalArgumentException(
                            "[dual-lookup] hbase.client.keytab.file 与 hbase.client.kerberos.principal "
                                    + "要么都配置，要么都不配置（都不配时复用 Flink 全局登录 / kinit 凭据）");
                }
                // _HOST 只适用于服务端 principal（hbase/_HOST@REALM）。客户端 principal 写了它，
                // 登录必然失败，且失败信息是 GSS/kinit 层面的，和"写错占位符"毫无关联，极难排查。
                if (hasPrincipal && cfg.hbaseClientKerberosPrincipal.contains("_HOST")) {
                    throw new IllegalArgumentException(
                            "[dual-lookup] hbase.client.kerberos.principal 必须是具体账号"
                                    + "（如 ro@EXAMPLE.COM），不能使用服务端占位符 _HOST，当前为: "
                                    + cfg.hbaseClientKerberosPrincipal);
                }
            }
            return cfg;
        }

        /**
         * 根据显式设置的 {@code lookup.timeout} 推导未显式配置的依赖超时。
         *
         * <p>只改一个 {@code lookup.timeout} 就建表失败，是很常见的运维摩擦：
         * 例如把它调到 1000ms 时，默认的 {@code hbase.rpc.timeout=300} 仍然合法但偏小；
         * 调到 300ms 时，默认的 300ms 反而会让交叉校验失败。这里在用户没显式指定时自动跟随：
         * <ul>
         *   <li>{@code hbase.rpc.timeout} = 80% × lookup.timeout（留 20% 给降级判定）；</li>
         *   <li>{@code hbase.client.operation.timeout} = max(500, lookup.timeout)，避免 HBase 先于业务超时放弃；</li>
         *   <li>{@code doris.connect.timeout} = 80% × lookup.timeout，夹到 [250, 300]ms（HikariCP 下限 + 最后一跳留出 SQL 时间）。</li>
         * </ul>
         *
         * <p>用户显式写了任何一项，就以显式值为准，不再覆盖。
         */
        private static void applyDerivedTimeouts(ReadableConfig c, Config cfg, boolean lookupTimeoutExplicit) {
            if (!lookupTimeoutExplicit) {
                return;
            }
            if (!hasExplicit(c, HBASE_RPC_TIMEOUT_MS)) {
                cfg.hbaseRpcTimeoutMs = (int) Math.max(1L,
                        Math.round(cfg.timeoutMs * HBASE_RPC_TIMEOUT_RATIO));
            }
            if (!hasExplicit(c, HBASE_OPERATION_TIMEOUT_MS)) {
                cfg.hbaseOperationTimeoutMs = Math.max(HBASE_OPERATION_TIMEOUT_MIN_MS, cfg.timeoutMs);
            }
            if (!hasExplicit(c, DORIS_CONNECT_TIMEOUT_MS)) {
                long derived = Math.round(cfg.timeoutMs * DORIS_CONNECT_TIMEOUT_RATIO);
                cfg.dorisConnectTimeoutMs = (int) Math.max(DORIS_CONNECT_TIMEOUT_MIN_MS,
                        Math.min(DORIS_CONNECT_TIMEOUT_MAX_MS, derived));
            }
        }

        /**
         * 判断某个配置项是否真的在 DDL / SET 里写过。
         *
         * <p>不能只看 {@code getOptional(...)}：带默认值的 {@code ConfigOption} 在未配置时
         * 也会返回默认值，无法区分「用户写了 simple」与「用户没写、取到默认 simple」。
         * Flink 的 {@link Configuration} 提供了 {@code containsKey}，用它判断最准确；
         * 其它 {@link ReadableConfig} 实现（例如测试里的自定义实现）退化为「视为未显式配置」。
         */
        private static boolean hasExplicit(ReadableConfig c, ConfigOption<?> option) {
            return c instanceof Configuration && ((Configuration) c).containsKey(option.key());
        }

        /** 返回第一个非空白值（null 安全）；用于「显式参数优先、properties.* 兜底」的取值链 */
        private static String firstNonEmpty(String... values) {
            for (String value : values) {
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
            return "";
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

        /**
         * 归一化认证方式：只接受 simple / kerberos。
         *
         * <p>HBase 内部用 {@code equalsIgnoreCase} 判断，所以大小写本身不会出错；这里归一化是为了
         * 让后续分支判断只写一次，并且把拼错（如 {@code kerberbos}）在建表阶段就变成明确报错，
         * 而不是静默按 simple 处理、最后在 SASL 阶段炸掉。
         */
        private static String normalizeAuthentication(String raw) {
            String v = raw == null ? "" : raw.trim().toLowerCase();
            if (!"simple".equals(v) && !"kerberos".equals(v)) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.security.authentication 只能取值 simple 或 kerberos，当前为: " + raw);
            }
            return v;
        }

        /** 归一化 SASL 保护级别：authentication / integrity / privacy（与 HBase 的取值一致） */
        private static String normalizeRpcProtection(String raw) {
            String v = raw == null ? "" : raw.trim().toLowerCase();
            if (!"authentication".equals(v) && !"integrity".equals(v) && !"privacy".equals(v)) {
                throw new IllegalArgumentException(
                        "[dual-lookup] hbase.rpc.protection 只能取值 authentication、integrity 或 privacy，当前为: " + raw);
            }
            return v;
        }
    }
}
