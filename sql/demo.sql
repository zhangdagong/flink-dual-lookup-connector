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
    'scan.startup.mode' = 'latest-offset',   -- 从最早消息消费; 也可改 'latest-offset'
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
    'lookup.timeout' = '1000', -- 单次(批量)查询硬超时，毫秒，无需带 ms
    -- ===== 攒批（批量查询，降低 Doris 交互次数）=====
    'lookup.batch.size' = '50', -- 攒满 50 条就批量查一次
    'lookup.batch.max-wait' = '5', -- 攒不满时最多等 5 毫秒就发
    -- ===== 统计日志 =====
    'lookup.stats.log-interval' = '60', -- 每 60 秒打一行统计
    -- ===== HBase =====
    'hbase.zookeeper.quorum' = '127.0.0.1',
    'hbase.zookeeper.property.clientPort' = '2181',
    'zookeeper.znode.parent' = '/hbase',
    'hbase.table-name' = 'dim:dim_account', -- 与 DDL 同名时可省略
    'hbase.column-family' = 'cf',
    'hbase.rpc.timeout' = '600', -- 单次 RPC 超时（ms）
    'hbase.client.operation.timeout' = '2000', -- 客户端操作总超时，含 region location 冷启动定位，要给足
    'hbase.client.retries.number' = '1',
    -- ===== Doris =====
    'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
    'doris.table-name' = 'dim.dim_account',
    'doris.username' = 'root',
    'doris.password' = '',
    'doris.query.timeout' = '1', -- JDBC queryTimeout（秒）
    -- Doris 连接池（HikariCP）调优项
    'doris.pool.size' = '8', -- 最大连接数
    'doris.pool.min-idle'= '2', -- 最小空闲连接
    'doris.connect.timeout' = '3000', -- 获取连接超时（ms）
    'doris.pool.idle-timeout' = '600000', -- 空闲多久回收（ms）
    'doris.pool.max-lifetime' = '1800000', -- 连接最大存活（ms）
    'doris.pool.validation-timeout' = '3000' -- 连接校验超时（ms）
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
-- 4. 关联查询：hint 里的 'async'='true' 必须写
--    注意：hint 的 'table' 必须和 FROM 子句里的表引用名一致——
--    这里维表起了别名 m1，所以 'table' 要写别名 'm1'（表有别名时必须用别名），
--    不是表名、也不是物理表名。物理表名（命名空间/库名）写在 hbase.table-name / doris.table-name 里。
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

