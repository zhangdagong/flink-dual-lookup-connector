-- =============================================================================
-- dual-lookup connector 示例：Kafka 交易流水 × 双源维表（HBase 主 / Doris 备）
-- 核心逻辑：优先查 HBase，超时（500ms）/异常则改查 Doris。
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. 全局参数（必须调整！）
--    table.exec.async-lookup.timeout 是算子层兜底超时，
--    必须显著大于 WITH 里的 lookup.timeout，否则还没等降级就被算子判超时。
-- -----------------------------------------------------------------------------
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '10s';
SET 'table.exec.async-lookup.output-mode' = 'ALLOW_UNORDERED';
set 'pipeline.operator-chaining' = 'false';
-- -----------------------------------------------------------------------------
-- 1. Kafka 交易流水流表
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS trans_detail;
CREATE TABLE trans_detail (
    trans_jnls_no STRING,
    account_no    STRING,
    trans_time    STRING,
    trans_type    STRING,
    trans_chnl    STRING,
    d_c_flag      STRING,
    amount        STRING,
    party_name    STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'test',
    'scan.startup.mode' = 'latest-offset',   -- latest-offset=从最新位点开始（跳过历史消息）；要从最早开始请改成 'earliest-offset'
    'format' = 'json'
);

-- -----------------------------------------------------------------------------
-- 2. 双源维表（HBase 主 / Doris 备）
--    注意：DDL 声明的业务列必须同时存在于 HBase（info 列族下同名列）和 Doris 表。
--    lookup_source / lookup_cost_ms 是连接器内置元字段，不查后端、由运行时填充。
--    PRIMARY KEY 必填，Lookup Join 靠它做点查。

-- 变体：以 Doris 为主、HBase 为备只改一行，其余不变：'lookup.primary' = 'doris', 建议同时把 Doris 连接池调大：'doris.pool.size' = '16'
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_account;
CREATE TABLE dim_account (
    account_no   string,
    account_name string,
    account_type string,
    balance      string,
    credit_level string,
    open_date    string,
    lookup_source  string, -- 元字段：数据来自哪个源（hbase / doris）
    lookup_cost_ms bigint, -- 元字段：本次查询耗时（毫秒，含降级耗时）
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector' = 'dual-lookup',
    -- ===== 核心：主备路由与超时 =====
    'lookup.primary' = 'hbase', -- 主源：hbase | doris
    'lookup.timeout' = '500', -- 单次(批量)查询硬超时，毫秒，无需带 ms
    -- ===== 攒批（批量查询，降低 Doris 交互次数）=====
    'lookup.batch.size' = '50', -- 攒满 50 条就批量查一次
    'lookup.batch.max-wait' = '30', -- 攒不满时最多等 30ms 就发。注意：有效批大小由它决定
                                    --（每子任务 QPS × max-wait），调太小会让攒批形同虚设；
                                    -- 必须小于 lookup.timeout，否则建表时会被校验拦下
    -- ===== 日志 =====
    'lookup.stats.log-interval' = '360', -- 每 6 分钟打一行统计（一行两个视角：window= 本阶段增量 + since-start 自启动累计；
                                         -- 含吞吐 rate、各源命中率 hit、平均 avg 与最长 max 耗时、失败原因分类、
                                         -- 降级波及的 key 数与占比；排查期间可临时调小到 10~30 秒）
    'lookup.failover.log-interval' = '10', -- 降级日志最小间隔（秒）：主源长故障时把 WARN 限流成
                                        --「首条必打 + 每 10s 一条 + 合并计数」，避免日志洪峰。
                                        -- 设为 0 = 不限流（每批降级都打印，用于故障复盘）
    -- ===== HBase =====
    'hbase.zookeeper.quorum' = '127.0.0.1',
    'hbase.zookeeper.property.clientPort' = '2181',
    'zookeeper.znode.parent' = '/hbase',
    'hbase.table-name' = 'dim:dim_account', -- 与 DDL 同名时可省略
    'hbase.column-family' = 'cf',
    'hbase.rowkey.encoding' = 'string', -- rowkey 编码：string=按字符串写（默认）| typed=按主键类型写。
                                        -- 必须与写入端一致，选错会「一条也查不到」而日志无异常
    'hbase.rpc.timeout' = '300', -- 单次 RPC 超时（ms）。它只负责让 HBase 尽早中断并释放资源，
                                 -- 因此必须小于 lookup.timeout；反过来会出现
                                 --「业务层已降级、HBase 还在等 RPC」
    'hbase.client.operation.timeout' = '2000', -- 客户端操作总超时，含 region location 冷启动定位，要给足
    'hbase.client.retries.number' = '1',
    -- ===== HBase Kerberos（可选，默认 simple 免认证；启用时取消下面注释）=====
    -- 'hbase.security.authentication' = 'kerberos',
    -- 'hbase.client.kerberos.principal' = 'hbase/ro@EXAMPLE.COM',
    -- 'hbase.client.keytab.file' = '/path/to/hbase.keytab',
    -- 'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM',
    -- 'hbase.kerberos.krb5.conf' = '/etc/krb5.conf',
    -- ===== Doris =====
    'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
    'doris.table-name' = 'dim.dim_account',
    'doris.username' = 'root',
    'doris.password' = '',
    'doris.query.timeout' = '1', -- JDBC queryTimeout（秒）。运行期会被自动钳制到 <= ceil(lookup.timeout/1000)
    -- Doris 连接池（HikariCP）调优项
    'doris.pool.size' = '8', -- 最大连接数
    'doris.pool.min-idle'= '2', -- 最小空闲连接
    'doris.connect.timeout' = '300', -- 获取连接超时（ms）。必须小于 lookup.timeout：
                                     -- Doris 是备源、后面没有第三次降级机会，
                                     -- 连接池打满时若业务层先超时，这一批就直接失败了
    'doris.pool.idle-timeout' = '600000', -- 空闲多久回收（ms）
    'doris.pool.max-lifetime' = '1800000', -- 连接最大存活（ms）
    'doris.pool.validation-timeout' = '250' -- 连接校验超时（ms）。HikariCP 要求它 <= connect.timeout
);

-- -----------------------------------------------------------------------------
-- 3. 结果表（示例：写回 Kafka）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS trans_detail_sink;
CREATE TABLE trans_detail_sink (
    trans_jnls_no   string
    ,account_no     string
    ,trans_time     string
    ,trans_type     string
    ,trans_chnl     string
    ,d_c_flag       string
    ,amount         string
    ,party_name     string
    ,account_name   string
    ,account_type   string
    ,balance        string
    ,credit_level   string
    ,open_date      string
    ,lookup_source  string
    ,lookup_cost_ms bigint
) WITH (
    'connector' = 'print'
);


-- -----------------------------------------------------------------------------
-- 4. 关联查询：hint 建议显式写（不是语法必需，维表关联由 FOR SYSTEM_TIME AS OF 决定，
--    不写时算子参数取全局 SET；详见 README 3.3）
--    注意：hint 的 'table' 必须和 FROM 子句里的表引用名一致——
--    这里维表起了别名 m1，所以 'table' 要写别名 'm1'（表有别名时必须用别名），
--    不是表名、也不是物理表名。物理表名（命名空间/库名）写在 hbase.table-name / doris.table-name 里。
--    多张维表 join 时每张各写一条 LOOKUP hint，全部放在 SELECT 后，见 README 3.4。
-- -----------------------------------------------------------------------------
INSERT INTO trans_detail_sink
SELECT /*+ LOOKUP('table' = 'm1', 'async' = 'true', 'output-mode' = 'allow_unordered',  'capacity' = '200', 'timeout' = '10s' ) */
    t1.trans_jnls_no
    ,t1.account_no
    ,t1.trans_time
    ,t1.trans_type
    ,t1.trans_chnl
    ,t1.d_c_flag
    ,t1.amount
    ,t1.party_name
    ,m1.account_name
    ,m1.account_type
    ,m1.balance
    ,m1.credit_level
    ,m1.open_date
    ,m1.lookup_source
    ,m1.lookup_cost_ms
FROM trans_detail AS t1
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t1.proc_time AS m1
    ON t1.account_no = m1.account_no
;

