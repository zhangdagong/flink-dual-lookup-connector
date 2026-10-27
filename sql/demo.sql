-- =============================================================================
-- dual-lookup 示例：Kafka 交易流水 × 双源维表（HBase 主 / Doris 备）
--
-- 前置条件
--   1) jar 已构建并放到 JM 与所有 TM 的 $FLINK_HOME/lib，集群已重启
--      （打包后用 jar tf | grep 自查 jar 内没有 Hadoop/htrace 类）
--   2) HBase 2.2.7：表 dim:dim_account，列族 cf，业务列按同名列写入
--   3) Doris：表 dim.dim_account，列名与下方 DDL 完全一致
--
-- 运行
--   $FLINK_HOME/bin/sql-client.sh -f sql/demo.sql
--
-- 看什么
--   - 结果里的 lookup_source（hbase/doris）与 lookup_cost_ms（本批耗时）
--   - TM 日志里的 [dual-lookup] 行：建连、配置回执、统计、降级 WARN/Kerberos 就绪
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. 算子层参数（全局默认值，可按需被 LOOKUP hint 覆盖）
--    table.exec.async-lookup.timeout 必须显著大于 WITH 里的 lookup.timeout，
--    否则降级还没走完，Flink 自己就先判这次查找超时了。
--    经验值：lookup.timeout × 2 + 2s（默认 500ms → 10s 已足够）。
-- -----------------------------------------------------------------------------
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '10s';
SET 'table.exec.async-lookup.output-mode' = 'ALLOW_UNORDERED';

-- 排查时取消注释：每个算子独立，便于在 WebUI 里单独观察 lookup 算子
-- SET 'pipeline.operator-chaining' = 'false';

-- -----------------------------------------------------------------------------
-- 1. 源表：Kafka 交易流水
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
    proc_time     AS PROCTIME()          -- Lookup Join 必需的处理时间列
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id'          = 'test',
    -- latest-offset：只消费启动之后的消息；要回放历史改成 earliest-offset
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json'
);

-- -----------------------------------------------------------------------------
-- 2. 双源维表（HBase 主 / Doris 备）
--
--    约束：
--      - 下面的业务列必须同时存在于 HBase（cf 列族、同名列）和 Doris 表中
--      - lookup_source / lookup_cost_ms 是连接器内置元字段，不查后端，无需建列
--      - PRIMARY KEY 必填，Lookup Join 依赖它做点查
--
--    主备对调：只改 'lookup.primary' = 'doris'（建议同时把 doris.pool.size 调到 16）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_account;
CREATE TABLE dim_account (
    account_no     STRING,
    account_name   STRING,
    account_type   STRING,
    balance        STRING,
    credit_level   STRING,
    open_date      STRING,
    lookup_source  STRING,               -- 元字段：数据实际来自 hbase / doris
    lookup_cost_ms BIGINT,               -- 元字段：本批查询耗时（ms，含降级耗时）
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector' = 'dual-lookup',

    -- ===== 主备路由与超时 =====
    'lookup.primary'      = 'hbase',     -- 主源：hbase | doris，另一个自动成为备源
    'lookup.timeout'      = '500',       -- 单批硬超时（ms），超时即降级；唯一的降级判据

    -- ===== 攒批：条数阈值 + 时间兜底 =====
    'lookup.batch.size'     = '50',
    'lookup.batch.max-wait' = '30',      -- 有效批大小 ≈ 每子任务 QPS × max-wait，
                                         -- 默认 5ms 在中等流量下会让攒批失效；必须 < lookup.timeout
    -- ===== 日志 =====
    'lookup.stats.log-interval'    = '360',  -- 每 6 分钟一行统计（排查期可临时调 10~30）
    'lookup.failover.log-interval' = '10',   -- 降级 WARN 最小间隔（秒）；0 = 不限流，逐批打印

    -- ===== HBase =====
    'hbase.zookeeper.quorum'            = '127.0.0.1',
    'hbase.zookeeper.property.clientPort' = '2181',
    'zookeeper.znode.parent'            = '/hbase',
    'hbase.table-name'                  = 'dim:dim_account',  -- 与 DDL 同名时可省略
    'hbase.column-family'               = 'cf',               -- 只支持单列族
    'hbase.rowkey.encoding'             = 'string',           -- string | typed，必须与写入端一致
                                                              -- 选错会「一条也查不到」且日志无异常
    'hbase.rpc.timeout'                 = '300',              -- 必须 < lookup.timeout
    'hbase.client.operation.timeout'    = '2000',             -- 含冷启动 region 定位，给足
    'hbase.client.retries.number'       = '1',

    -- ===== HBase Kerberos（可选，默认 simple；启用时整块取消注释）=====
    -- 连接器负责「写配置 -> 配置 UGI -> keytab 登录」，之后 TGT 续期由客户端负责。
    -- 客户端 principal 必须是具体账号，不能写 _HOST（那是服务端 principal 的占位符）。
    -- 'hbase.security.authentication'         = 'kerberos',
    -- 'hbase.client.kerberos.principal'       = 'ro@EXAMPLE.COM',
    -- 'hbase.client.keytab.file'              = '/path/to/ro.keytab',
    -- 'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM',
    -- 'hbase.rpc.protection'                  = 'authentication',  -- 须与服务端一致
    -- 'hbase.kerberos.krb5.conf'              = '/etc/krb5.conf',

    -- ===== Doris =====
    'doris.jdbc-url'      = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
    'doris.table-name'    = 'dim.dim_account',   -- 与 DDL 同名时可省略
    'doris.username'      = 'root',
    'doris.password'      = '',
    'doris.query.timeout' = '1',                 -- 秒；运行期会被钳制到 ≤ ceil(lookup.timeout/1000)

    -- Doris 连接池（HikariCP）
    'doris.pool.size'                = '8',      -- 总连接数 = 并行度 × 本值，别打爆 Doris
    'doris.pool.min-idle'            = '2',
    'doris.connect.timeout'          = '300',    -- 必须 < lookup.timeout（HikariCP 下限 250）
    'doris.pool.idle-timeout'        = '600000',
    'doris.pool.max-lifetime'        = '1800000',
    'doris.pool.validation-timeout'  = '250'     -- 必须 ≤ connect.timeout
);

-- -----------------------------------------------------------------------------
-- 3. 结果表（示例用 print；生产换成 Kafka/OLAP sink）
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS trans_detail_sink;
CREATE TABLE trans_detail_sink (
    trans_jnls_no   STRING,
    account_no      STRING,
    trans_time      STRING,
    trans_type      STRING,
    trans_chnl      STRING,
    d_c_flag        STRING,
    amount          STRING,
    party_name      STRING,
    account_name    STRING,
    account_type    STRING,
    balance         STRING,
    credit_level    STRING,
    open_date       STRING,
    lookup_source   STRING,
    lookup_cost_ms  BIGINT
) WITH (
    'connector' = 'print'
);

-- -----------------------------------------------------------------------------
-- 4. 关联并写出
--
--    hint 不是语法必需（维表关联由 FOR SYSTEM_TIME AS OF + 主键等值条件决定），
--    但生产建议显式写，评审时一眼能看出这是 async lookup。
--
--    ⚠️ hint 的 'table' 必须写 FROM 子句里的**别名**（这里是 m1），
--       不是表名，也不是物理表名（物理名写在 hbase.table-name / doris.table-name）。
--    多张维表 join 时每张各写一条 hint，全部放在 SELECT 之后。
-- -----------------------------------------------------------------------------
INSERT INTO trans_detail_sink
SELECT /*+ LOOKUP('table' = 'm1', 'async' = 'true',
                   'output-mode' = 'allow_unordered',
                   'capacity' = '200', 'timeout' = '10s') */
    t1.trans_jnls_no,
    t1.account_no,
    t1.trans_time,
    t1.trans_type,
    t1.trans_chnl,
    t1.d_c_flag,
    t1.amount,
    t1.party_name,
    m1.account_name,
    m1.account_type,
    m1.balance,
    m1.credit_level,
    m1.open_date,
    m1.lookup_source,
    m1.lookup_cost_ms
FROM trans_detail AS t1
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t1.proc_time AS m1
    ON t1.account_no = m1.account_no;

-- -----------------------------------------------------------------------------
-- 5. 验证（按需执行）
--
-- 5.1 只看关联结果、不写 sink：在 SQL Client 里执行
-- SELECT t1.account_no, m1.account_name, m1.lookup_source, m1.lookup_cost_ms
-- FROM trans_detail AS t1
-- LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t1.proc_time AS m1
--     ON t1.account_no = m1.account_no;
--
-- 5.2 TM 日志里应能看到：
--   [dual-lookup] HBase reader opened, table=dim:dim_account, ... rowkeyEncoding=string
--   [dual-lookup] table=dim_account cfg[primary=hbase standby=doris batch.size=50 ...]
--   [dual-lookup] table=dim_account window=360s totalKeys=... hbase[... hit=...] || ...
--
-- 5.3 故障演练（预期行为）：
--   停 HBase        -> 作业不失败；lookup_source 变 doris；统计行 failover 增长；WARN 首条带原因
--   停 Doris(备源)  -> 无影响（备源未建连）；但这段时间容错能力为 0
--   两源都停        -> 整批异常、作业失败（有意为之：不静默补空）
-- -----------------------------------------------------------------------------
