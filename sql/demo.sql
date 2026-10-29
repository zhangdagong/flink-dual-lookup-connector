-- =============================================================================
-- dual-lookup 示例：Kafka 交易流水 × 双源维表（HBase 主 / Doris 备）
--
-- 前置条件
--   1) jar 1.2.9+ 已构建并放到 JM 与所有 TM 的 $FLINK_HOME/lib，集群已重启
--      （打包后用 jar tf | grep 自查 jar 内没有 Hadoop/htrace 类）
--      旧版 jar（1.0.x ~ 1.2.8）必须删除，否则可能仍报 properties.* 不可用或没有新行为
--   2) HBase 2.2.7：表 dim:dim_account，列族 cf，业务列按同名列写入
--   3) Doris：表 dim.dim_account，列名与下方 DDL 完全一致
--
-- 运行
--   $FLINK_HOME/bin/sql-client.sh -f sql/demo.sql
--
-- 看什么
--   - 结果里的 lookup_source（hbase/doris）与 lookup_cost_ms（查询阶段耗时，不含建连）
--   - TM 日志里的 [dual-lookup] 行：建连、配置回执、统计、降级 WARN/Kerberos 就绪
--   - Kerberos：出现「HBase Kerberos 复用已登录用户」或「HBase Kerberos 就绪」；
--     若出现「hbase.master.keytab.file ... 服务端配置」WARN，说明该参数不用于客户端登录
--   - PushGateway：1.2.4+ 会自动把 PrometheusPushGatewayReporter 的 logger 设为 OFF，
--     不再打印 failed to push metrics to pushgateway；其他日志不受影响（仅 Log4j2 生效）
--
-- 从 flink-hbase 迁移时的 key 映射：
--   table-name        -> hbase.table-name
--   zookeeper.quorum  -> hbase.zookeeper.quorum（端口拆到 hbase.zookeeper.property.clientPort）
--   properties.*      -> 可原样保留（连接器会参与 Kerberos 决策）
--   另外必须补：hbase.column-family、hbase.rowkey.encoding、doris.jdbc-url、PRIMARY KEY
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. 算子层参数（全局默认值，可按需被 LOOKUP hint 覆盖）
--    table.exec.async-lookup.timeout 必须显著大于 WITH 里的 lookup.timeout，
--    否则降级还没走完，Flink 自己就先判这次查找超时了。
--    经验值：lookup.timeout × 2 + 2s（默认 500ms → 10s 已足够）。
--    这是 Session 级配置，Factory 建表时会读取它做交叉校验，所以 SET 必须在 CREATE TABLE 之前。
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
--    主备对调：只改 'lookup.primary' = 'doris'（建议同时把 doris.pool.size 调到 8~16）
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
    lookup_cost_ms BIGINT,               -- 元字段：查询阶段耗时（ms，不含建连，含降级耗时）
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector' = 'dual-lookup',

    -- ===== 主备路由与超时 =====
    'lookup.primary'      = 'hbase',     -- 主源：hbase | doris，另一个自动成为备源
    'lookup.timeout'      = '500',       -- 单批硬超时（ms），超时即降级；唯一的降级判据
                                         -- 显式调本项后，未显式配置的 hbase.rpc.timeout（80%）、
                                         -- hbase.client.operation.timeout（≥500）、
                                         -- doris.connect.timeout（80% 夹 250~300ms）会自动跟随；
                                         -- 下面显式写了的参数优先，不会被推导覆盖

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
                                                              -- DECIMAL 主键：string 按 toString 编码；
                                                              -- 写入端若用 HBase Bytes.toBytes(BigDecimal)，请改用 typed
    'hbase.rpc.timeout'                 = '300',              -- 必须 < lookup.timeout；不写则自动取 80%
    'hbase.client.operation.timeout'    = '2000',             -- 含冷启动 region 定位，给足；不写则 max(500, lookup.timeout)
    'hbase.client.retries.number'       = '1',

    -- ===== HBase Kerberos（可选，默认 simple；启用时取消注释）=====
    -- 下面的 REALM / principal / keytab 路径请换成你们集群的实际值。
    -- 方案 A（推荐）：keytab 交给 Flink 全局配置，DDL 只打开认证方式。
    --   flink-conf.yaml:
    --     security.kerberos.login.keytab: /home/flink/streampark/kerberos/rtdw.keytab
    --     security.kerberos.login.principal: rtdw@HADOOP.COM
    --   连接器会复用这个已登录的 UGI（与 flink-hbase 行为一致）。
    -- 'hbase.security.authentication'         = 'kerberos',
    -- 'hbase.rpc.protection'                  = 'authentication',  -- 须与服务端一致
    -- 'hbase.master.kerberos.principal'       = 'hbase/_HOST@HADOOP.COM',
    -- 'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@HADOOP.COM',

    -- 方案 B：单表覆盖 Flink 全局登录用户（keytab 与 principal 成对，principal 不能含 _HOST）
    -- 'hbase.client.keytab.file'              = '/home/flink/streampark/kerberos/rtdw.keytab',
    -- 'hbase.client.kerberos.principal'       = 'rtdw@HADOOP.COM',

    -- 方案 C：沿用 flink-hbase 的 properties.* 写法（可原样迁移；连接器会据此进入 Kerberos 模式）
    -- 'properties.hbase.security.authentication'         = 'kerberos',
    -- 'properties.hbase.rpc.protection'                  = 'authentication',
    -- 'properties.hbase.master.kerberos.principal'       = 'hbase/_HOST@HADOOP.COM',
    -- 'properties.hbase.regionserver.kerberos.principal' = 'hbase/_HOST@HADOOP.COM',
    -- 'properties.hbase.client.keytab.file'              = '/home/flink/streampark/kerberos/rtdw.keytab',
    -- 'properties.hbase.client.kerberos.principal'       = 'rtdw@HADOOP.COM',

    -- ⚠️ hbase.master.keytab.file 是 HMaster 服务端配置，客户端不会用它登录；
    --    从旧 DDL 带过来可以保留，但连接器只会透传并打 WARN。
    --    真正客户端凭据用上面的 hbase.client.keytab.file + hbase.client.kerberos.principal。
    -- 'properties.hbase.master.keytab.file' = '/home/flink/streampark/kerberos/rtdw.keytab',

    -- 注意：keytab 只需在真正读它的 TM 上可读；不配客户端 keytab 时完全复用 Flink 全局登录。
    -- 'hbase.kerberos.krb5.conf'              = '/etc/krb5.conf',

    -- ===== Doris =====
    -- 唯一必填项：即使 lookup.primary=hbase，也需要 Doris 作为备源
    'doris.jdbc-url'      = 'jdbc:mysql://192.168.214.128:9030/dim?useSSL=false&serverTimezone=Asia/Shanghai',
    'doris.table-name'    = 'dim.dim_account',   -- 与 DDL 同名时可省略
    'doris.username'      = 'root',
    'doris.password'      = '',
    'doris.query.timeout' = '0',                 -- 秒；0 = 不设置 JDBC queryTimeout，
                                                 -- 跟随 Doris 集群/会话默认；>0 不受 lookup.timeout 钳制

    -- Doris 连接池（HikariCP）
    'doris.pool.size'                = '4',      -- 默认 4（备源够用）；Doris 作主源调到 8~16
                                                 -- 总连接数 = 并行度 × 本值，别打爆 Doris
    'doris.pool.min-idle'            = '2',
    'doris.connect.timeout'          = '300',    -- 必须 < lookup.timeout；不写则 80% 夹 250~300ms
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
--   [dual-lookup] 表=dim_account 配置[primary=hbase standby=doris batch.size=50 ...]
--   [dual-lookup] 表=dim_account 子任务=1/4 | 本窗口=360s 业务数据量=... 总处理key数=... 主源(hbase)[... 命中率=...] 备源(doris)[...] 降级=... || 自启动=...
--   Kerberos 复用模式：[dual-lookup] HBase Kerberos 复用已登录用户（Flink 全局登录或 kinit）: ...
--   Kerberos 显式 keytab：[dual-lookup] HBase Kerberos 就绪: principal=..., loginUser=...
--   旧 DDL 带 master keytab：[dual-lookup] 检测到 hbase.master.keytab.file=...，但它是 HBase Master 服务端配置...
--
-- 5.3 从 flink-hbase 迁移自检：
--   建表不再报 properties.*；行 key 编码与写入端一致；hbase.table-name/column-family 正确；
--   日志里 kerberos 不是 silent simple；必要时用 klist -kt 核对 keytab 里的 principal。
--
-- 5.4 故障演练（预期行为）：
--   停 HBase        -> 作业不失败；lookup_source 变 doris；统计行 failover 增长；WARN 首条带原因
--   停 Doris(备源)  -> 无影响（备源未建连）；但这段时间容错能力为 0
--   两源都停        -> 整批异常、作业失败（有意为之：不静默补空）
--
-- 5.5 HBase 备源空闲过久时，首次降级可能先重建连接：
--   日志出现「HBase 连接已断开，重建后重试本批」属正常自愈，该批会继续执行（只重试一次）。
--
-- 5.6 PushGateway 日志：
--   1.2.4+ 连接器启动时会把 PrometheusPushGatewayReporter 的 logger 设为 OFF，
--   不再打印 failed to push metrics to pushgateway；只影响该 logger，其他日志正常。
--   若仍然出现，确认加载的是 1.2.4+、Flink 使用 Log4j2（logback 下不生效）。
--   注意：这是 JVM 级副作用，同一 TM 上所有作业共享，也会隐藏真实的推送失败。
-- -----------------------------------------------------------------------------
