# dual-lookup connector 端到端测试计划

在真实 Flink 环境中验证连接器的全部关键行为：建表校验、基础关联、查不到语义、
降级容错、攒批性能、rowkey 编码、多表 join。共 **25 个案例**，分 6 组。

配套文件：

| 文件 | 用途 |
|---|---|
| `sql/e2e_test.sql` | Flink SQL Client 里执行的 DDL 与查询（按案例编号分段） |
| `sql/gen_test_data.py` | Kafka 测试数据生成器（basic / burst / cust 三种模式） |
| 本文档「前置准备」 | HBase / Doris 造数命令（复制即用） |

---

## 前置准备（只做一次）

### 0. 重新打包并部署 jar ⚠️

仓库 `target/` 里的旧 jar 不含最新修复，必须先重打：

```bash
mvn clean package -DskipTests
cp target/flink-dual-lookup-connector-1.0.0.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

### 1. HBase 造数（hbase shell 里执行）

```ruby
# 命名空间已存在时忽略报错
create_namespace 'dim'
# 表已存在时先 disable + drop 再建
create 'dim:dim_account', 'cf'

# 写入 A001~A010（与 Doris 侧同值，便于降级后比对结果一致性）
(1..10).each do |i|
  key = format('A%03d', i)
  put 'dim:dim_account', key, 'cf:account_name', format('Name%02d', i)
  put 'dim:dim_account', key, 'cf:account_type', i.even? ? 'VIP' : 'NORMAL'
  put 'dim:dim_account', key, 'cf:balance', (1000 * i).to_s
  put 'dim:dim_account', key, 'cf:credit_level', (i % 5 + 1).to_s
  put 'dim:dim_account', key, 'cf:open_date', '2024-01-15'
end

scan 'dim:dim_account'   # 应看到 10 行
```

### 2. Doris 造数（mysql 客户端连接 192.168.214.128:9030 执行）

```sql
CREATE DATABASE IF NOT EXISTS dim;

-- Unique Key + merge-on-write + 行存：点查走 SHORT-CIRCUIT 短路径（README 9.1 的建议配置）
CREATE TABLE IF NOT EXISTS dim.dim_account (
    account_no   VARCHAR(32),
    account_name VARCHAR(64),
    account_type VARCHAR(16),
    balance      VARCHAR(32),
    credit_level VARCHAR(8),
    open_date    VARCHAR(16)
) UNIQUE KEY(account_no)
DISTRIBUTED BY HASH(account_no) BUCKETS 4
PROPERTIES (
    'replication_num' = '1',
    'enable_unique_key_merge_on_write' = 'true',
    'store_row_column' = 'true',
    'light_schema_change' = 'true'
);

-- A001~A010 与 HBase 同值；A011~A020 只有 Doris 有（验证「查不到不降级」语义）
INSERT INTO dim.dim_account VALUES
('A001','Name01','NORMAL','1000','2','2024-01-15'),
('A002','Name02','VIP','2000','3','2024-01-15'),
('A003','Name03','NORMAL','3000','4','2024-01-15'),
('A004','Name04','VIP','4000','5','2024-01-15'),
('A005','Name05','NORMAL','5000','1','2024-01-15'),
('A006','Name06','VIP','6000','2','2024-01-15'),
('A007','Name07','NORMAL','7000','3','2024-01-15'),
('A008','Name08','VIP','8000','4','2024-01-15'),
('A009','Name09','NORMAL','9000','5','2024-01-15'),
('A010','Name10','VIP','10000','1','2024-01-15'),
('A011','Name11','NORMAL','11000','2','2024-01-15'),
('A012','Name12','VIP','12000','3','2024-01-15'),
('A013','Name13','NORMAL','13000','4','2024-01-15'),
('A014','Name14','VIP','14000','5','2024-01-15'),
('A015','Name15','NORMAL','15000','1','2024-01-15'),
('A016','Name16','VIP','16000','2','2024-01-15'),
('A017','Name17','NORMAL','17000','3','2024-01-15'),
('A018','Name18','VIP','18000','4','2024-01-15'),
('A019','Name19','NORMAL','19000','5','2024-01-15'),
('A020','Name20','VIP','20000','1','2024-01-15');
```

### 3. Kafka 数据生成器

```bash
pip install kafka-python   # 只需一次
# basic 模式（低速率逐条核对）：编辑 gen_test_data.py 顶部 MODE='basic', RATE=5
python sql/gen_test_data.py
# burst 模式（压测）：MODE='burst', RATE=2000
```

### 4. 观察点速查

| 要看什么 | 在哪里 |
|---|---|
| print 结果表输出 | `tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.out` |
| 统计日志（avgBatch / 各源 avg 耗时 / failover） | `tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.log \| grep dual-lookup`。⚠️ 每行有**两个视角**用 `\|\|` 分隔：前半段 `window=` 是本阶段增量，后半段 `since-start` 是自启动累计（TM 重启后归零）。判断「最近是否变慢」只看前半段 |
| 降级 WARN 日志 | 同上，关键字「降级到备源」（**默认按 10s 限流**，见案例 14 注） |
| 建连日志（rowkeyEncoding=） | 同上，关键字「reader opened」 |
| 作业状态 / 异常 | Flink Web UI（默认 8081） |

---

## 案例总览

| # | 组 | 案例 | 预期结论 |
|---|---|---|---|
| 1 | A | 合法配置建表 | 成功 |
| 2 | A | 缺 PRIMARY KEY | 建表报错 |
| 3 | A | lookup.primary 非法值 | 建表报错 |
| 4 | A | max-wait ≥ lookup.timeout | 建表报错 |
| 5 | A | rpc.timeout ≥ lookup.timeout | 建表报错 |
| 6 | A | connect.timeout ≥ lookup.timeout | 建表报错 |
| 7 | A | 参数名拼写错误 | 建表报错 |
| 8 | A | min-idle > pool.size | 建表报错 |
| 9 | B | 命中主源 | 字段正确、source=hbase |
| 10 | B | 两源都没有的 key | NULL、不降级 |
| 11 | B | 仅 Doris 有的 key | NULL、**不降级**（核心语义） |
| 12 | B | 批内重复 key | 每条都有结果 |
| 13 | B | 元字段语义 | cost 批内相同、降级含 500ms |
| 14 | C | 停 HBase | 降级 Doris，作业不失败 |
| 15 | C | 恢复 HBase | 自动回到主源 |
| 16 | C | 网络延迟注入 | 超时降级 |
| 17 | C | 停 Doris（HBase 正常） | 无影响 |
| 18 | C | 两源都停 | 作业失败（不静默） |
| 19 | C | HBase 停机时启动作业 | 惰性建连直接走 Doris |
| 20 | D | burst 2000/s（max-wait=30） | avgBatch 接近 50 |
| 21 | D | burst 2000/s（max-wait=5） | avgBatch 仅个位数 |
| 22 | D | buffer-capacity 压死 | 反压出现 |
| 23 | E | string 编码（B 组已覆盖） | 命中正常 |
| 24 | E | typed 编码（BIGINT 主键） | 命中正常 |
| 25 | F | 多表 join 各自降级 | 两表独立工作 |

---

## A 组：建表校验（不需要发数据）

在 SQL Client 里逐条执行 `sql/e2e_test.sql` **第 5 节**里注释掉的 8 个 DDL（含案例 1 的对照表）。

| # | 步骤 | 预期（报错关键字） |
|---|---|---|
| 1 | 执行第 2 节 `CREATE TABLE dim_account` | 建表成功（对照组） |
| 2 | 取消注释「案例 2」执行 | `维表必须声明 PRIMARY KEY` |
| 3 | 取消注释「案例 3」执行 | `lookup.primary 只能取值 HBase 或 Doris` |
| 4 | 取消注释「案例 4」执行 | `必须小于 lookup.timeout`（攒批等待吃掉超时预算） |
| 5 | 取消注释「案例 5」执行 | `hbase.rpc.timeout(...) 必须小于 lookup.timeout` |
| 6 | 取消注释「案例 6」执行 | `doris.connect.timeout(...) 必须小于 lookup.timeout` |
| 7 | 取消注释「案例 7」执行 | 未声明的配置项 / Unsupported options |
| 8 | 取消注释「案例 8」执行 | `doris.pool.min-idle 不能大于 doris.pool.size` |

**通过标准**：案例 1 成功；案例 2~8 全部在建表阶段报错（不是运行期）。
任何一个「预期报错」却建表成功，说明校验失效，属于 bug。

---

## B 组：基础功能

**步骤**：执行 e2e_test.sql 第 0~3 节 + B 组 INSERT；`gen_test_data.py` 用 `MODE='basic', RATE=5` 发数。

| # | 案例 | 观察点 | 预期 |
|---|---|---|---|
| 9 | 命中主源 | A001~A010 的输出 | `account_name=Name01~Name10`，`lookup_source=hbase`，`lookup_cost_ms>0`；字段值与 HBase 写入一致 |
| 10 | 两源都没有 | A901~A905 的输出 | 维表列全 NULL、`lookup_source` 为 NULL；TM 日志**无**降级 WARN；统计日志 `doris[batches=0]` |
| 11 | 仅 Doris 有 | A011~A020 的输出 | **同样全 NULL、不降级**——这是核心语义：HBase 查不到是正常业务结果，不会去 Doris 补查。若你的业务期望「HBase 没有就查 Doris」，那是另一套语义，本连接器刻意不支持 |
| 12 | 批内重复 key | 连发 3 次的 A001 | 3 条输出**都**有 `account_name=Name01`（曾有的静默丢数据缺陷，回归验证） |
| 13 | 元字段 | 同一批输出的 `lookup_cost_ms` | 同批内相同（批次耗时）；案例 14 降级期间该值 ≈ 500 + Doris 耗时 |

**通过标准**：9~13 全部符合预期；案例 11 的 NULL 是「特性」不是 bug，确认你理解并接受这套语义。

---

## C 组：降级容错（作业保持运行，边发数边操作）

**步骤**：B 组作业不停，`MODE='basic', RATE=20` 持续发数，按下表操作环境。

| # | 操作 | 预期 |
|---|---|---|
| 14 | `stop-hbase.sh` | 约 1 个批次周期后出现 WARN「降级到备源 doris」；输出继续且字段完整，`lookup_source` 变为 `doris`，A011~A020 此时**能查到了**（因为直接查 Doris）；统计 `failover` 持续增长，且 `doris[batches=... avg=..]` 开始出现真实耗时（此前恒为 `avg=-`） |
| 15 | `start-hbase.sh`，等 1~2 分钟 | `lookup_source` 恢复为 `hbase`。⚠️ 若 2 分钟后仍全是 doris（旧连接已失效），重启作业即可——这是已知取舍，见 README 第 14 节 |
| 16 | `tc qdisc add dev eth0 root netem delay 600ms`（600ms > lookup.timeout） | 超时降级，WARN 出现 `HBase lookup timeout after 500ms`；`tc qdisc del dev eth0 root netem` 恢复。此时统计日志里 `hbase[... avg=..]` 会明显抬升到 ≈500ms 以上——**这正是「主源在变慢」的可观测信号**，且失败批次的耗时也计入 |
| 17 | 停 Doris（`docker stop` 或停 FE/BE），HBase 正常 | 输出完全无变化，`lookup_source` 保持 `hbase`；统计日志里 `doris[...]` 恒为 `batches=0 avg=-`（备源压根没被调用） |
| 18 | 在 17 基础上再停 HBase | 两源都失败 → 作业**失败抛异常**（不静默补空，属预期行为）；恢复任一源后重启作业可继续 |
| 19 | 恢复 Doris、保持 HBase 停止，**重启一个全新的 B 组作业** | 作业正常启动不报错（惰性建连），从一开始 `lookup_source=doris` |

**通过标准**：14/16/19 降级链路全部符合预期；17 证明备源故障不影响主路径；18 证明不静默丢数据。

> **案例 14 注：降级 WARN 默认被限流。**`lookup.failover.log-interval` 默认 10 秒，
> 所以 HBase 停掉后你看到的 WARN 是「首条 + 之后每 10 秒一条」，而不是每批一条——
> 这是有意设计（5000 条/分的流量下每批一条约 285 万行/天，会把故障起点冲掉）。
> 每条日志末尾会带「距上次打印又发生 N 批降级已被合并」，N 才是真实降级批数；
> 精确总量看统计日志的 `failover=` 字段。想逐批留痕就把该参数设为 `0`。
> 案例 15 恢复 HBase 后，应看到一条 INFO「主源 hbase 已恢复，重新由主源提供数据（本次故障共降级 N 批）」。

---

## D 组：性能与攒批

**步骤**：`gen_test_data.py` 用 `MODE='burst', RATE=2000`。统计日志已调成 10 秒一行。

> 读数值前先确认行首的 `window=` 确实约为 10——日志里的每个数字都只描述这一个窗口，不是自启动累计。

| # | 案例 | 预期 |
|---|---|---|
| 20 | 主配置（max-wait=30）跑 burst | 统计日志 `avgBatch` 接近 50（2000×0.03=60，被 batch.size=50 封顶）；`avgBatch ≈ avgBatchWithStandby`（无降级） |
| 21 | 停掉 20 的作业，改用 e2e_test.sql 里注释掉的 `dim_account_mw5`（max-wait=5）跑同样 burst | `avgBatch` 只有 ≈10（2000×0.005）——直观看到「默认 5ms 攒批近乎失效」。⚠️ 两个作业不要同时跑：同 group.id 会瓜分同一 topic 的消息，互相干扰 |
| 22 | （可选）把 hint 里 `capacity` 改成 `10`，继续 burst | Web UI 可见反压（busy/backpressured），证明缓冲容量是吞吐上限的第一约束；改回 200 恢复 |

**通过标准**：20 与 21 的 `avgBatch` 差异显著（≈50 vs ≈10），与「有效批大小 ≈ QPS × max-wait」公式吻合。

---

## E 组：rowkey 编码

| # | 案例 | 步骤 | 预期 |
|---|---|---|---|
| 23 | string 编码 | B 组已全部覆盖（A001~A010 按字符串 rowkey 命中） | 已通过即无需重复 |
| 24 | typed 编码（BIGINT 主键） | ① HBase 造数（下）；② Doris 造数（下）；③ 执行 e2e_test.sql E 组全部取消注释的语句；④ `gen_test_data.py` 用 `MODE='cust'` | 1001~1003 输出 `score=S100x`、`lookup_source=hbase`；9001 输出 NULL 不降级 |

HBase 造数（hbase shell，rowkey 必须是 8 字节大端 long）：

```ruby
create 'dim:dim_cust_score', 'cf'
# pack('q>') 生成 8 字节大端有符号 long，与 Bytes.toBytes(long) 一致
[1001, 1002, 1003].each do |id|
  put 'dim:dim_cust_score', [id].pack('q>'), 'cf:score', "S#{id}"
end
# 验证：scan 时 rowkey 显示为不可读字节属正常（typed 模式本就如此）
```

Doris 造数：

```sql
CREATE TABLE IF NOT EXISTS dim.dim_cust_score (
    cust_id BIGINT,
    score   VARCHAR(16)
) UNIQUE KEY(cust_id)
DISTRIBUTED BY HASH(cust_id) BUCKETS 2
PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true');

INSERT INTO dim.dim_cust_score VALUES (1001,'S1001'),(1002,'S1002'),(1003,'S1003');
```

**反向验证（可选）**：把 DDL 里 `hbase.rowkey.encoding` 改回 `string` 重启查询 → 全部查不到（NULL），
亲身体会「编码选错 = 静默全 NULL」这个最难排查的坑。

---

## F 组：多表 join

| # | 案例 | 步骤 | 预期 |
|---|---|---|---|
| 25 | 两张维表各自独立工作 | ① 造数（下）；② 执行 e2e_test.sql F 组取消注释的语句；③ basic 模式发数 | 每条输出同时带 `account_name` 与交易类型名称（可加到 select 里）；TM 日志出现**两行**不同 table 的统计行；停 HBase 后两张表的 `lookup_source` 各自变为 doris，互不干扰 |

dim_mcc 造数（HBase + Doris）：

```ruby
create 'dim:dim_mcc', 'cf'
put 'dim:dim_mcc', 'PAY', 'cf:mcc_name', 'PAY_NAME'
put 'dim:dim_mcc', 'TRANSFER', 'cf:mcc_name', 'TRANSFER_NAME'
put 'dim:dim_mcc', 'WITHDRAW', 'cf:mcc_name', 'WITHDRAW_NAME'
put 'dim:dim_mcc', 'DEPOSIT', 'cf:mcc_name', 'DEPOSIT_NAME'
```

```sql
CREATE TABLE IF NOT EXISTS dim.dim_mcc (
    mcc_code VARCHAR(32),
    mcc_name VARCHAR(64)
) UNIQUE KEY(mcc_code)
DISTRIBUTED BY HASH(mcc_code) BUCKETS 2
PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true');

INSERT INTO dim.dim_mcc VALUES
('PAY','PAY_NAME'),('TRANSFER','TRANSFER_NAME'),('WITHDRAW','WITHDRAW_NAME'),('DEPOSIT','DEPOSIT_NAME');
```

---

## 测试通过标准汇总

- **A 组**：2~8 全部建表报错；
- **B 组**：9~13 输出与预期逐条相符，特别确认案例 11 的「不降级」语义；
- **C 组**：降级/恢复/双停/冷启动 6 个场景全部符合预期；
- **D 组**：avgBatch 在 max-wait=30 与 5 之间差异显著（≈50 vs ≈10）；
- **E 组**：typed 编码正常命中，反向验证（改回 string）全 NULL；
- **F 组**：两张维表统计日志各自出现、各自独立降级。

全部通过后，这套连接器即可按 README 第 9 节的容量建议进入生产参数调优阶段。
