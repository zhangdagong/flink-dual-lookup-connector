-- =============================================================================
-- dual-lookup connector 端到端测试 SQL（配合 TEST_PLAN.md 使用，共 25 个案例）
--
-- 用法：在 Flink SQL Client 里「一节一节执行」，不要整个文件一次性贴进去。
--   第 0~4 节 = B 组基础功能（先把这条链路跑通，案例 9~13）
--   第 5 节   = A 组建表校验反例（8 个，逐条取消注释、各自单独执行，全部预期报错）
--   第 6 节   = D 组 max-wait 对比变体（案例 21 用）
--   第 7 节   = E 组 typed rowkey 编码（案例 24）
--   第 8 节   = F 组多表 join（案例 25）
--
-- C 组降级容错（案例 14~19）不需要额外 SQL：作业保持运行，直接对 HBase / Doris
-- 做 stop / start / tc 延迟注入，观察输出与日志即可。
--
-- 数据准备（HBase/Doris 造数、Kafka 发数）见 TEST_PLAN.md「前置准备」。
-- ⚠️ 开始前务必先 `mvn clean package` 重打 jar 并重启集群，旧 jar 不含最新修复。
-- =============================================================================


-- =============================================================================
-- 0. 全局参数（作业级；对所有异步 lookup 算子生效，hint 可按单条 join 覆盖后三项）
--    table.exec.async-lookup.timeout 是算子层兜底超时，必须显著大于 DDL 里的
--    lookup.timeout —— 校验规则会在建表阶段检查，配反了直接报错。
-- =============================================================================
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '10s';
SET 'table.exec.async-lookup.output-mode' = 'ALLOW_UNORDERED';
SET 'pipeline.operator-chaining' = 'false';  -- 解除算子链，便于在 Web UI 单独观察 lookup 算子的反压（D 组案例 22）


-- =============================================================================
-- 1. Kafka 交易流水源表（topic=test，消息体由 gen_test_data.py 生成）
-- =============================================================================
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
    proc_time AS PROCTIME()   -- 处理时间属性，Lookup Join 靠它触发维表查询
) WITH (
    'connector' = 'kafka',
    'topic' = 'test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'test',
    'scan.startup.mode' = 'latest-offset',   -- latest-offset=只读新消息；想回放历史消息改成 'earliest-offset'
    'format' = 'json'
);


-- =============================================================================
-- 2. 主维表 dim_account（HBase 主 / Doris 备）—— B、C、D 组共用
--    DDL 业务列必须同时存在于 HBase（cf 列族下同名列）与 Doris 表 dim.dim_account。
--    lookup_source / lookup_cost_ms 是连接器内置元字段，不查后端、由运行时填充。
-- =============================================================================
DROP TABLE IF EXISTS dim_account;
CREATE TABLE dim_account (
    account_no     STRING,
    account_name   STRING,
    account_type   STRING,
    balance        STRING,
    credit_level   STRING,
    open_date      STRING,
    lookup_source  STRING,   -- 元字段：本行数据来自哪个源（hbase / doris）
    lookup_cost_ms BIGINT,   -- 元字段：本批查询耗时（毫秒，降级场景下含主源超时时间）
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector' = 'dual-lookup',
    -- ---- 主备路由与超时 ----
    'lookup.primary' = 'hbase',
    'lookup.timeout' = '500',
    -- ---- 攒批 ----
    'lookup.batch.size' = '50',
    'lookup.batch.max-wait' = '30',          -- D 组案例 20 的主配置；案例 21 用第 6 节的 max-wait=5 变体对比
    -- ---- 日志 ----
    'lookup.stats.log-interval' = '10',      -- 统计日志每 10 秒一行（D 组要观察 avgBatch，间隔调小便于读取；
                                             -- 每行两个视角：window= 段是这 10 秒的增量，since-start 段是自启动累计；
                                             -- 案例 10 看 hbase[... hit=0.0%]，案例 14/16 看 fail= 分类与 max=）
    'lookup.failover.log-interval' = '10',   -- 降级 WARN 限流：首条必打 + 之后每 10s 一条（0 = 不限流，逐批打印）
    -- ---- HBase ----
    'hbase.zookeeper.quorum' = '127.0.0.1',
    'hbase.zookeeper.property.clientPort' = '2181',
    'zookeeper.znode.parent' = '/hbase',
    'hbase.table-name' = 'dim:dim_account',
    'hbase.column-family' = 'cf',
    'hbase.rowkey.encoding' = 'string',      -- A001~A010 按字符串 rowkey 写入（E 组才用 typed）
    'hbase.rpc.timeout' = '300',
    'hbase.client.operation.timeout' = '2000',
    'hbase.client.retries.number' = '1',
    -- ---- Doris（doris.jdbc-url 是唯一必填项）----
    'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
    'doris.table-name' = 'dim.dim_account',
    'doris.username' = 'root',
    'doris.password' = '',
    'doris.query.timeout' = '1',
    'doris.pool.size' = '8',
    'doris.pool.min-idle' = '2',
    'doris.connect.timeout' = '300',
    'doris.pool.idle-timeout' = '600000',
    'doris.pool.max-lifetime' = '1800000',
    'doris.pool.validation-timeout' = '250'
);


-- =============================================================================
-- 3. 结果表（print connector：写到 TaskManager 的 .out 日志，便于逐条核对）
--    观察点：tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.out
-- =============================================================================
DROP TABLE IF EXISTS trans_detail_sink;
CREATE TABLE trans_detail_sink (
    trans_jnls_no  STRING,
    account_no     STRING,
    trans_time     STRING,
    trans_type     STRING,
    trans_chnl     STRING,
    d_c_flag       STRING,
    amount         STRING,
    party_name     STRING,
    account_name   STRING,
    account_type   STRING,
    balance        STRING,
    credit_level   STRING,
    open_date      STRING,
    lookup_source  STRING,
    lookup_cost_ms BIGINT
) WITH (
    'connector' = 'print'
);


-- =============================================================================
-- 4. B 组基础关联（案例 9 ~ 13）
--
--    hint 不是语法必需（维表关联由 FOR SYSTEM_TIME AS OF 决定，本连接器只提供异步实现），
--    这里显式写是为了「一眼看出异步语义」，并能对单条 join 覆盖算子参数（详见 README 3.3）。
--    'table' 必须与 FROM 子句里的表引用名一致：维表起了别名 m1，所以写 'm1'，
--    不是表名、也不是物理表名；写错会在建表/校验阶段直接报错。
--
--    发数：gen_test_data.py 用 MODE='basic', RATE=5
-- =============================================================================
INSERT INTO trans_detail_sink
SELECT /*+ LOOKUP('table' = 'm1', 'async' = 'true', 'output-mode' = 'allow_unordered', 'capacity' = '200', 'timeout' = '10s') */
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

-- 核对要点：
--   案例 9  A001~A010  -> account_name = Name01~Name10，lookup_source = hbase，cost > 0
--   案例 10 A901~A905  -> 维表列全 NULL、lookup_source 为 NULL，且 TM 日志【无】降级 WARN
--   案例 11 A011~A020  -> 同样全 NULL 且【不降级】。这是核心语义：HBase 查不到属正常业务结果，
--                        不会去 Doris 补查（若业务期望「HBase 没有就查 Doris」，那是另一套语义）
--   案例 12 A001 在一轮里连发 3 次 -> 3 条输出【都】要有 account_name = Name01（回归静默丢数据缺陷）
--   案例 13 同一批输出的 lookup_cost_ms 相同（批次耗时）；C 组降级期间该值 ≈ 500 + Doris 耗时


-- =============================================================================
-- 5. A 组建表校验反例（案例 2 ~ 8）—— 不需要发数据
--
--    下面每条 DDL 【全部预期在建表阶段报错】。逐条取消注释、单独执行、核对报错关键字。
--    若某条「预期报错」却建表成功，说明校验失效，属于 bug。
--    案例 1（对照组）就是第 2 节的 dim_account，已成功建表即可。
-- =============================================================================

-- 案例 2：缺 PRIMARY KEY -> 报错关键字「维表必须声明 PRIMARY KEY」
--         注意：必须带上 doris.jdbc-url。工厂里 Flink 的 validate()（必填项/拼写检查）先执行，
--         主键检查在其后；若不写 jdbc-url，报错会变成「缺少必填项」而掩盖掉主键这一条。
-- CREATE TABLE case2_no_pk (
--     account_no   STRING,
--     account_name STRING
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 3：lookup.primary 非法值 -> 报错关键字「lookup.primary 只能取值 HBase 或 Doris」
-- CREATE TABLE case3_bad_primary (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.primary' = 'clickhouse',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 4：lookup.batch.max-wait >= lookup.timeout -> 报错关键字「必须小于 lookup.timeout」
--         （攒批等待会吃掉整个超时预算，时间兜底还没触发，批次就先被判超时降级了）
-- CREATE TABLE case4_maxwait_ge_timeout (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.timeout' = '500',
--     'lookup.batch.max-wait' = '600',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 5：hbase.rpc.timeout >= lookup.timeout -> 报错关键字「hbase.rpc.timeout(...) 必须小于 lookup.timeout」
--         （否则业务层已降级切源，HBase 侧还在傻等那次 RPC）
-- CREATE TABLE case5_rpc_ge_timeout (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.timeout' = '500',
--     'lookup.batch.max-wait' = '30',
--     'hbase.rpc.timeout' = '600',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 6：doris.connect.timeout >= lookup.timeout -> 报错关键字「doris.connect.timeout(...) 必须小于 lookup.timeout」
--         （Doris 是备源、后面没有第三次降级机会，连接池打满时若业务层先超时，整批直接失败）
-- CREATE TABLE case6_connect_ge_timeout (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.timeout' = '500',
--     'lookup.batch.max-wait' = '30',
--     'hbase.rpc.timeout' = '300',
--     'doris.connect.timeout' = '600',
--     'doris.pool.validation-timeout' = '250',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 7：参数名拼写错误 -> 报错关键字「Unsupported options / 未知配置项」
--         （防拼写错误的第一道防线：写了未在工厂里登记的 key 会被 Flink 直接拦下）
-- CREATE TABLE case7_typo (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.timeoutt' = '500',   -- 故意多一个 t
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );

-- 案例 8：doris.pool.min-idle > doris.pool.size -> 报错关键字「doris.pool.min-idle 不能大于 doris.pool.size」
--         （HikariCP 的硬性约束）
-- CREATE TABLE case8_minidle_gt_size (
--     account_no STRING,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'doris.pool.size' = '8',
--     'doris.pool.min-idle' = '16',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai'
-- );


-- =============================================================================
-- 6. D 组：max-wait 对比变体（案例 21）
--
--    与第 4 节用同一张 HBase/Doris 表，仅把 lookup.batch.max-wait 从 30 改成 5，
--    用来直观对比「有效批大小 ≈ 每子任务 QPS × max-wait」：
--        max-wait=30 -> 2000 QPS 下 avgBatch ≈ 50（被 batch.size 封顶）
--        max-wait=5  -> 2000 QPS 下 avgBatch ≈ 10（攒批几乎失效）
--    ⚠️ 不要与第 4 节的作业同时跑：同 group.id（test）会瓜分同一 topic 的消息，互相干扰。
--       做法：停掉案例 20 的作业，再取消下面注释执行。
-- =============================================================================

-- DROP TABLE IF EXISTS dim_account_mw5;
-- CREATE TABLE dim_account_mw5 (
--     account_no     STRING,
--     account_name   STRING,
--     account_type   STRING,
--     balance        STRING,
--     credit_level   STRING,
--     open_date      STRING,
--     lookup_source  STRING,
--     lookup_cost_ms BIGINT,
--     PRIMARY KEY (account_no) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.primary' = 'hbase',
--     'lookup.timeout' = '500',
--     'lookup.batch.size' = '50',
--     'lookup.batch.max-wait' = '5',           -- 唯一差异：默认值（观察攒批为何近乎失效）
--     'lookup.stats.log-interval' = '10',
--     'hbase.zookeeper.quorum' = '127.0.0.1',
--     'hbase.table-name' = 'dim:dim_account',
--     'hbase.column-family' = 'cf',
--     'hbase.rowkey.encoding' = 'string',
--     'hbase.rpc.timeout' = '300',
--     'hbase.client.operation.timeout' = '2000',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
--     'doris.table-name' = 'dim.dim_account',
--     'doris.username' = 'root',
--     'doris.password' = '',
--     'doris.pool.size' = '8',
--     'doris.pool.min-idle' = '2',
--     'doris.connect.timeout' = '300',
--     'doris.pool.validation-timeout' = '250'
-- );
--
-- INSERT INTO trans_detail_sink
-- SELECT /*+ LOOKUP('table' = 'm1', 'async' = 'true', 'capacity' = '200', 'timeout' = '10s') */
--     t1.trans_jnls_no, t1.account_no, t1.trans_time, t1.trans_type, t1.trans_chnl,
--     t1.d_c_flag, t1.amount, t1.party_name,
--     m1.account_name, m1.account_type, m1.balance, m1.credit_level, m1.open_date,
--     m1.lookup_source, m1.lookup_cost_ms
-- FROM trans_detail AS t1
-- LEFT JOIN dim_account_mw5 FOR SYSTEM_TIME AS OF t1.proc_time AS m1
--     ON t1.account_no = m1.account_no;


-- =============================================================================
-- 7. E 组：typed rowkey 编码（案例 24，BIGINT 主键）
--
--    前置造数见 TEST_PLAN.md E 组：
--      HBase rowkey 必须用 [id].pack('q>') 写 8 字节大端 long，与 Bytes.toBytes(long) 一致；
--      Doris 表 dim.dim_cust_score 同步准备 1001~1003。
--    发数：gen_test_data.py 改成 MODE='cust'
--
--    反向验证（可选）：把下面 hbase.rowkey.encoding 改回 'string' 后重新建表查询，
--    1001~1003 会全部查不到（NULL）——亲身体会「编码选错 = 静默全 NULL」这个最难排查的坑。
-- =============================================================================

-- DROP TABLE IF EXISTS trans_cust;
-- CREATE TABLE trans_cust (
--     cust_id   BIGINT,
--     proc_time AS PROCTIME()
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = 'test',
--     'properties.bootstrap.servers' = 'localhost:9092',
--     'properties.group.id' = 'test-cust',      -- 独立 group.id，避免与 B/D 组作业抢消息
--     'scan.startup.mode' = 'latest-offset',
--     'format' = 'json'
-- );
--
-- DROP TABLE IF EXISTS dim_cust_score;
-- CREATE TABLE dim_cust_score (
--     cust_id        BIGINT,
--     score          STRING,
--     lookup_source  STRING,
--     lookup_cost_ms BIGINT,
--     PRIMARY KEY (cust_id) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.primary' = 'hbase',
--     'lookup.timeout' = '500',
--     'lookup.batch.size' = '50',
--     'lookup.batch.max-wait' = '30',
--     'lookup.stats.log-interval' = '10',
--     'lookup.failover.log-interval' = '10',
--     'hbase.zookeeper.quorum' = '127.0.0.1',
--     'hbase.table-name' = 'dim:dim_cust_score',
--     'hbase.column-family' = 'cf',
--     'hbase.rowkey.encoding' = 'typed',       -- ★ 关键：按 Bytes.toBytes(long) 写/查 rowkey
--     'hbase.rpc.timeout' = '300',
--     'hbase.client.operation.timeout' = '2000',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
--     'doris.table-name' = 'dim.dim_cust_score',
--     'doris.username' = 'root',
--     'doris.password' = '',
--     'doris.pool.size' = '2',
--     'doris.pool.min-idle' = '1',
--     'doris.connect.timeout' = '300',
--     'doris.pool.validation-timeout' = '250'
-- );
--
-- DROP TABLE IF EXISTS cust_sink;
-- CREATE TABLE cust_sink (
--     cust_id        BIGINT,
--     score          STRING,
--     lookup_source  STRING,
--     lookup_cost_ms BIGINT
-- ) WITH (
--     'connector' = 'print'
-- );
--
-- INSERT INTO cust_sink
-- SELECT /*+ LOOKUP('table' = 'c1', 'async' = 'true', 'capacity' = '200', 'timeout' = '10s') */
--     t1.cust_id, c1.score, c1.lookup_source, c1.lookup_cost_ms
-- FROM trans_cust AS t1
-- LEFT JOIN dim_cust_score FOR SYSTEM_TIME AS OF t1.proc_time AS c1
--     ON t1.cust_id = c1.cust_id;
--
-- 核对要点：1001~1003 -> score = S1001~S1003，lookup_source = hbase；
--           9001 -> 全 NULL 且不降级（HBase 里也没有）
-- 建连日志里应有 rowkeyEncoding=typed，这是排查「全部查不到」时的第一现场。


-- =============================================================================
-- 8. F 组：多表 join（案例 25）—— 两张维表各自独立工作、各自降级
--
--    前置造数见 TEST_PLAN.md F 组（HBase + Doris 各建 dim_mcc）。
--    发数：gen_test_data.py 用 MODE='basic', RATE=5
--
--    要点：每张维表各写一条 LOOKUP hint，靠各自的 'table'（别名）区分，互不影响；
--          连接器层参数（lookup.timeout、扫批、主备策略、连接池）本来就按表隔离，
--          每张维表有独立的 DualLookupFunction 实例与各自的 HBase 连接 / Doris 连接池。
-- =============================================================================

-- DROP TABLE IF EXISTS dim_mcc;
-- CREATE TABLE dim_mcc (
--     mcc_code       STRING,
--     mcc_name       STRING,
--     lookup_source  STRING,
--     lookup_cost_ms BIGINT,
--     PRIMARY KEY (mcc_code) NOT ENFORCED
-- ) WITH (
--     'connector' = 'dual-lookup',
--     'lookup.primary' = 'hbase',
--     'lookup.timeout' = '500',
--     'lookup.batch.size' = '20',              -- 码表类小维表：查询量小，参数可给小
--     'lookup.batch.max-wait' = '30',
--     'lookup.stats.log-interval' = '10',
--     'lookup.failover.log-interval' = '10',
--     'hbase.zookeeper.quorum' = '127.0.0.1',
--     'hbase.table-name' = 'dim:dim_mcc',
--     'hbase.column-family' = 'cf',
--     'hbase.rowkey.encoding' = 'string',
--     'hbase.rpc.timeout' = '300',
--     'hbase.client.operation.timeout' = '2000',
--     'doris.jdbc-url' = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
--     'doris.table-name' = 'dim.dim_mcc',
--     'doris.username' = 'root',
--     'doris.password' = '',
--     'doris.pool.size' = '4',                 -- 多表 join 时容量按份数规划：Σ(并行度 × pool.size) 别超 Doris 承载
--     'doris.pool.min-idle' = '1',
--     'doris.connect.timeout' = '300',
--     'doris.pool.validation-timeout' = '250'
-- );
--
-- DROP TABLE IF EXISTS trans_detail_sink2;
-- CREATE TABLE trans_detail_sink2 (
--     trans_jnls_no  STRING,
--     account_no     STRING,
--     trans_type     STRING,
--     account_name   STRING,
--     account_source STRING,
--     mcc_name       STRING,
--     mcc_source     STRING
-- ) WITH (
--     'connector' = 'print'
-- );
--
-- INSERT INTO trans_detail_sink2
-- SELECT
--     /*+ LOOKUP('table' = 'm1', 'async' = 'true', 'capacity' = '200', 'timeout' = '10s')
--         LOOKUP('table' = 'm2', 'async' = 'true', 'capacity' = '100', 'timeout' = '10s') */
--     t1.trans_jnls_no,
--     t1.account_no,
--     t1.trans_type,
--     m1.account_name,
--     m1.lookup_source AS account_source,
--     m2.mcc_name,
--     m2.lookup_source AS mcc_source
-- FROM trans_detail AS t1
-- LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t1.proc_time AS m1
--     ON t1.account_no = m1.account_no
-- LEFT JOIN dim_mcc FOR SYSTEM_TIME AS OF t1.proc_time AS m2
--     ON t1.trans_type = m2.mcc_code;
--
-- 核对要点：每条输出同时带 account_name 与 mcc_name；
--           TM 日志里出现【两行】table 不同的统计行（dim_account / dim_mcc）；
--           停 HBase 后两张表的 lookup_source 各自变为 doris，互不干扰。


-- =============================================================================
-- 收尾（跑完后清理）
-- =============================================================================
-- STOP 当前作业；如需重来，按需 DROP 各表后从第 0 节重新执行。
