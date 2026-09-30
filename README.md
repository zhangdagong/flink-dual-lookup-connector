# Flink Dual Lookup Connector

HBase / Doris 双源容错的 Flink SQL 维表连接器。

**核心逻辑一句话**：Kafka 流进入后，优先查主源（默认 HBase）；若查询超时（默认 500ms）或服务异常，则整批改查备源（Doris）。全程对 SQL 透明，主备切换、超时降级都发生在算子内部。

---

## 目录

- [1. 为什么需要它](#1-为什么需要它)
- [2. 环境要求](#2-环境要求)
- [3. 快速开始](#3-快速开始)
- [4. 运行机制详解](#4-运行机制详解)
- [5. 参数完整参考](#5-参数完整参考)
- [6. 内置元字段（数据来源与耗时）](#6-内置元字段数据来源与耗时)
- [7. Kerberos 认证（可选）](#7-kerberos-认证可选)
- [8. 可观测性与日志解读](#8-可观测性与日志解读)
- [9. 容量测算与调优](#9-容量测算与调优)
- [10. 故障演练](#10-故障演练)
- [11. 常见问题排查](#11-常见问题排查)
- [12. 代码结构与实现要点](#12-代码结构与实现要点)
- [13. 配置项校验规则](#13-配置项校验规则)
- [14. 已知限制与演进方向](#14-已知限制与演进方向)

---

## 1. 为什么需要它

实时风控/账务场景里，维表关联（Lookup Join）几乎都要求「主键点查 + 低延迟」。但单源方案都有各自的软肋：

| 场景 | 后果 |
|---|---|
| HBase 某个 RegionServer 抖动 / 网络抖动 | 单次点查从 5ms 飙到数秒，算子被拖住，整个作业背压 |
| HBase 集群做运维重启 | 维表关联整体不可用，作业直接失败 |
| Doris 单点慢查询 | JDBC 阻塞 Worker 线程，连接池被打满，雪崩 |

传统做法是在 SQL 层写两套作业或加外部路由，改造成本高且对业务不透明。

本连接器把「主备路由 + 超时降级 + 攒批」收敛进一个 connector：DDL 里把 HBase 和 Doris 的位置一次性配好，SQL 侧照常写 `FOR SYSTEM_TIME AS OF`，容错过程对业务 SQL 完全无感。

```
                        ┌──────────────────────────────────────────┐
                        │           DualLookupFunction             │
                        │        （异步算子 + 攒批 + 主备路由）      │
   Kafka 交易流         │                                          │
        │               │   batchKeys 缓冲区                       │
        ▼               │   ┌────────────────────────┐             │
   asyncLookup(key) ───►│   │ 攒满 50 条 或 等满 5ms  │             │
                        │   └───────────┬────────────┘             │
                        │               │ flush()                  │
                        │               ▼                          │
                        │      ┌────────────────┐                  │
                        │      │ 主源 HBase     │ 批量 Get         │
                        │      └───────┬────────┘                  │
                        │              │                           │
                        │      成功 ◄──┴──► 超时/异常              │
                        │       │               │                  │
                        │       │               ▼                  │
                        │       │        ┌────────────┐            │
                        │       │        │ 备源 Doris │ 批量 SQL   │
                        │       │        └───────┬────┘            │
                        │       │                │                 │
                        │       ▼                ▼                 │
                        │  distribute() 按下标分发给各key的Future  │
                        └──────────────────────────────────────────┘
                                        │
                                        ▼
                                  Lookup Join 输出
```

---

## 2. 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 1.8 | 编译与运行（`maven.compiler.source/target = 1.8`） |
| Flink | 1.18.1 | 用了 `AsyncLookupFunction` 抽象类 API（1.16+ 可用） |
| HBase | 2.6.2-hadoop3 | 依赖 `hbase-shaded-client`，走原生异步客户端。Hadoop 2.x 集群请改用 `2.6.2` |
| Doris | 4.0.8 | 走 MySQL 协议（JDBC + HikariCP）；2.x/3.x 同样适用 |
| 构建 | Maven 3.6+ | 用 `maven-shade-plugin` 打 fat jar |

几个版本约束背后的原因（改版本前务必确认）：

- **HikariCP 锁 4.0.3**：这是兼容 Java 8 的最后一代，5.x 起要求 Java 11+。若你把 JDK 升到 11+，可同步升级 HikariCP。
- **HBase 客户端锁 `-hadoop3`**：与 Hadoop 3.x 匹配；Hadoop 2.x 环境必须换成不带后缀的 `2.6.2`，否则会出现 `NoSuchMethodError`。
- **Kerberos 自动续期依赖 HBase 2.2.0+**：本连接器不做手动 `kinit`，全靠客户端自动登录与 TGT 续期。

---

## 3. 快速开始

### 3.1 构建与部署

```bash
# 跑单元测试（不依赖任何外部服务：用伪造的 JDBC / LookupReader 对象驱动真实代码）
mvn test

# 打 fat jar：已把 HBase 客户端、MySQL 驱动、HikariCP 打进去
mvn clean package

# 产物是单个 jar
ls target/flink-dual-lookup-connector-1.0.0.jar

# 放到 Flink 的 lib 目录（JM 与所有 TM 都要有）
cp target/flink-dual-lookup-connector-1.0.0.jar $FLINK_HOME/lib/

# 重启集群生效（connector 是启动期通过 SPI 扫描发现的，不能热加载）
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

> **关于 fat jar 与 Hadoop 依赖**：
>
> - `org.apache.hadoop:*` 已在 shade 配置里 **exclude**，改由 Flink 集群提供。原因是
>   `hbase-shaded-client` 只 relocate 第三方依赖（protobuf/guava/zookeeper 等），Hadoop 自身的类
>   保持原包名会被整个打进 jar（实测 3 万余个类、约 72MB），与集群 lib 里的 `hadoop-common`
>   冲突时表现为 `NoSuchMethodError` / `ClassCastException`，且只在特定路径偶发。排除后 jar 明显变小。
> - ⚠️ **若你的 Flink 是 hadoop-free 发行版**（`lib/` 下没有 `hadoop-common`），
>   请删掉那一行 `<exclude>org.apache.hadoop:*</exclude>`，否则 HBase 客户端初始化会因缺类而失败。
> - Flink 自身（`org.apache.flink:*`）、SLF4J、Log4j、commons-logging 同样已 **exclude**，保证与集群版本不打架。

### 3.2 最小可用示例

完整可运行示例见 [`sql/demo.sql`](sql/demo.sql)，这里是最小骨架：

```sql
-- 1) 算子层兜底超时。必须显著大于 lookup.timeout，否则降级还没走完算子就判超时了。
--    经验值：table.exec.async-lookup.timeout ≈ lookup.timeout × 2 + 2s
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '10s';
SET 'table.exec.async-lookup.output-mode' = 'ALLOW_UNORDERED';

-- 2) 源表：Kafka 交易流水
CREATE TABLE trans_detail (
    trans_jnls_no STRING,
    account_no    STRING,
    amount        STRING,
    proc_time AS PROCTIME()          -- 处理时间属性列，Lookup Join 必需
) WITH (
    'connector' = 'kafka',
    'topic' = 'test',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'test',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 3) 维表 DDL：HBase 与 Doris 中同名同构的 dim_account
--    要求：这里声明的每个业务列，在 HBase（指定列族下）和 Doris 表里都要存在，且列名一致
CREATE TABLE dim_account (
    account_no   STRING,
    cust_name    STRING,
    risk_level   STRING,
    balance      DECIMAL(18,2),
    PRIMARY KEY (account_no) NOT ENFORCED   -- 必填！Lookup Join 依赖主键做点查
) WITH (
    'connector'              = 'dual-lookup',
    'lookup.primary'         = 'hbase',     -- 主源；改成 'doris' 即对调主备
    'lookup.timeout'         = '500',       -- 主源超时阈值（毫秒，无需带 ms）
    'hbase.zookeeper.quorum' = 'localhost',
    'doris.jdbc-url'         = 'jdbc:mysql://192.168.214.128:9030/risk'
);

-- 4) 关联查询
--    hint 不是语法必需（维表关联由 FOR SYSTEM_TIME AS OF 决定），但建议显式写，见 3.3；
--    hint 的 'table' 必须与 FROM 子句里的表引用名一致：维表有别名就必须写别名
SELECT /*+ LOOKUP('table'='d', 'async'='true', 'capacity'='200') */
    t.trans_jnls_no, t.account_no, t.amount, d.cust_name, d.risk_level
FROM trans_detail t
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t.proc_time d
    ON t.account_no = d.account_no;
```

**三个最容易踩的坑**（都已在 demo.sql 里标注）：

1. hint 的 `'table'` 写错（写成表名而不是 FROM 里的别名）→ SQL 校验期直接报 hint 不匹配，关联失败。
2. 忘了 `SET 'table.exec.async-lookup.timeout'` → Flink 默认 3 分钟，Future 真卡死时暴露太晚；若配得比 `lookup.timeout` 小，降级逻辑永远走不到（这种组合建表时会被校验拦下）。
3. 维表 DDL 漏写 `PRIMARY KEY` → 建表时直接报错（这是有意设计的强校验）。

### 3.3 LOOKUP hint：写不写、每个参数管什么

结论：**hint 不是语法必需的**——维表关联由 `FOR SYSTEM_TIME AS OF` + 主键等值条件决定，优化器自然会生成 Lookup Join；本连接器只提供异步实现（`AsyncLookupFunctionProvider`），异步模式也是自动确定的。不写 hint 时，算子参数取全局 `SET 'table.exec.async-lookup.*'` 的值。但生产上建议显式写：

| 参数 | 作用层 | 作用 | 不写时 |
|---|---|---|---|
| `'table'` | 优化器 | 指定哪张是维表（**必须用别名**；hint 里只要写了其他参数，它就必填） | 整个 hint 都不写则不需要 |
| `'async'` | 优化器 | 强制异步模式；本连接器只有异步实现，写了是显式声明，防止未来换成同步实现后行为漂移 | 自动走异步 |
| `'capacity'` / `'timeout'` / `'output-mode'` | 异步算子 | **只覆盖这一条 join** 的算子参数，优先级高于全局 SET | 用全局 SET 的值 |

真正需要写的两种情况：

1. **一条 SQL join 多张维表且需要差异化调参**（见 3.4）——全局 SET 对所有 join 一刀切，只有 hint 能精确到单条；
2. 想显式锁定异步语义，让评审一眼看出这是 async lookup，不用去翻 connector 实现。

两个易混点：

- hint 的 `'timeout'` 是**算子层**兜底，与 DDL 里 `lookup.timeout`（**连接器层**业务超时）是两回事，约束关系是「算子 timeout > lookup.timeout」，否则降级没走完算子先判超时；
- hint **管不到连接器内部行为**：攒批、主备降级、rowkey 编码全由 DDL 的 `WITH` 参数决定，别试图在 hint 里调。

### 3.4 多表 join：每张维表各写一条 hint

一条 SQL join 多张 dual-lookup 维表时，hint 按表各写一条、全部放在 `SELECT` 后面，靠各自的 `'table'`（别名）区分：

```sql
SELECT /*+ LOOKUP('table' = 'm1', 'async' = 'true', 'capacity' = '500', 'timeout' = '10s')
           LOOKUP('table' = 'm2', 'async' = 'true', 'capacity' = '100', 'timeout' = '5s') */
    t1.*, m1.account_name, m2.mcc_name
FROM trans_detail AS t1
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t1.proc_time AS m1
    ON t1.account_no = m1.account_no
LEFT JOIN dim_mcc FOR SYSTEM_TIME AS OF t1.proc_time AS m2
    ON t1.mcc = m2.mcc_code;
```

每条 hint 只作用于自己 `'table'` 指向的 join，互不影响。若所有维表用同一套算子参数就行，也可以全都不写，统一由全局 SET 生效。

连接器侧的两个多表注意点：

- **连接器层参数本来就是按表隔离的**：每张维表有自己的 DDL、`WITH` 参数和独立的 `DualLookupFunction` 实例（各自的 HBase 连接、Doris 连接池、攒批缓冲）。`lookup.timeout`、攒批、主备策略在各自 DDL 里各配各的，与 hint 无关。
- **容量规划要按维表份数翻倍**：N 张 dual-lookup 维表 = N 套独立连接池，Doris 侧总连接数 = Σ（每表并行度 × 每表 `doris.pool.size`）。多表场景下码表类小维表的 `doris.pool.size` 建议降到 2~4。

---

## 4. 运行机制详解

### 4.1 攒批：条数 + 时间双触发

单条查询的开销大头是「网络往返 + SQL 解析 + 连接借用」这些固定成本。把 N 条合成一次批量查询，交互次数降为 `1/N`——这对走 SQL 的 Doris 尤其关键（HBase 侧同样受益：一次 batch Get 替代 N 次 Get）。

两个触发条件，谁先满足谁触发：

| 触发条件 | 参数 | 默认 | 作用 |
|---|---|---|---|
| 条数阈值 | `lookup.batch.size` | 50 | 高流量下快速攒满，吞吐优先 |
| 时间兜底 | `lookup.batch.max-wait` | 5ms | 低流量下防止延迟无界（否则几条数据会一直躺在缓冲区等「攒满」） |

`lookup.batch.max-wait` 应远小于 `lookup.timeout`（默认 5ms vs 500ms），否则攒批等待本身就吃掉了超时预算。反过来，中等流量下 5ms 又会让攒批近乎失效——**有效批大小 ≈ 每子任务 QPS × max-wait**，由它而非 `batch.size` 决定，调优测算见 9.1。

**并发模型**：`asyncLookup` 会被 Flink 的多条输入端线程并发调用，因此入队加锁，但**批量 IO 绝不在锁内执行**——否则一个慢批次会阻塞所有入队请求，攒批退化成串行。锁内只做「入队 + 判定是否 flush」，摘批后立刻释放锁。

### 4.2 主备路由与降级时序

```
 时间轴 ────────────────────────────────────────────────────────────────►

 t0  收到一批 50 条 key，批量查主源 HBase
     │
     ├─ 情况 A：t0+30ms 返回 ──────────────────────────► 分发结果（来源=hbase，cost=30）
     │
     ├─ 情况 B：t0+30ms 返回，但 key 都没命中 ──────────► 分发空结果（不降级！来源 NULL）
     │
     └─ 情况 C：t0+500ms 仍未完成（lookup.timeout 触发）
              │
              ▼ 记一次 failover，打 WARN 日志
         t0+500ms 起批量查备源 Doris
              │
              ├─ t0+560ms 返回 ──► 分发结果（来源=doris，cost=560，含主源超时的 500ms）
              │
              └─ t0+1060ms 仍未完成 ──► 整批 Future 异常完成 ──► 作业失败
```

关键设计点：

- **降级粒度是整批**，不是单条。主源部分成功时也会整批重查。取舍理由：换来「绝不漏查、绝不静默丢数据」的确定性，对风控/账务场景比省一半查询更重要。
- **HBase 侧任一个 Get 失败，整批按失败处理**。因为「部分成功」的结果不完整，用不完整的维表数据比走备源重查更危险——会静默产出错误的关联结果。
- **两源都失败时不补空结果**，而是让整批 Future 异常完成、作业失败。静默补空等于把故障伪装成「该 key 不存在」，会产出错误关联结果。宁可失败暴露问题。

### 4.3 超时三层模型

| 层 | 参数 | 默认 | 职责 |
|---|---|---|---|
| 客户端层 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | 300 / 500 / 1s | 让**后端尽早中止**、释放资源 |
| 业务层 | `lookup.timeout`（本连接器） | 500 | **独立的硬超时**，超过即判该源故障、触发降级 |
| 算子层 | `table.exec.async-lookup.timeout` | Flink 默认 3min，**建议显式设为 10s** | 最终兜底，防 Future 永不完成 |

> 算子层这一项在 `sql/demo.sql` 里显式设成了 `10s`。Flink 自带的 3 分钟默认值太长：
> 一旦 Future 真的卡死，要等 3 分钟才报错，问题暴露太晚；设成 `lookup.timeout × 2 + 2s` 更合适。

> **关键理解**：`lookup.timeout` 是业务层独立控制的降级阈值，**不受** HBase 客户端超时影响。
> 这两层职责不同，不要用同一个值去理解——比如 `hbase.client.operation.timeout` 可以配得比 `lookup.timeout` 更大，
> 它的作用只是「别让 HBase 客户端太快放弃」，降级判断交给业务层。

**HBase region location 预热**：HBase 客户端冷启动时，首条查询要先定位 region（日志里的 `waiting for region location`），这就要几百毫秒，会直接撞上 500ms 超时被误判降级。本连接器在 `open()` 建连阶段就调用 `getAllRegionLocations()` 把 location 取好，后续查询直接命中客户端缓存，把这部分耗时挪出查询路径。

### 4.4 查不到数据 ≠ 故障

主源查不到某个 key，返回的是空结果——这是**正常业务结果，不会触发切换**。只有超时、异常才算故障。

这条边界很重要：如果空结果也降级，那么「主源里本就存在的合法缺失」会导致每一批都重复查两遍后端，备源压力翻倍且延迟白白翻倍。

### 4.5 惰性建连

两个 Reader 都在 TableSource 构造时创建对象，但**不建立连接**；真正查询时才 `open()`。

好处：主源在作业启动时刻不可用（比如 HBase 整体宕机）时，作业依然能正常启动并直接走备源，无需等主源恢复。这也是为什么 `open()` 必须幂等且线程安全。

**Doris 侧更进一步：建池也不连接。** HikariCP 的 `initializationFailTimeout` 被显式设为 `-1`，所以 `open()` 只创建池对象、不做任何连接尝试，连接推迟到首次 `getConnection()`。这一点很关键——HikariCP 默认值 `1` 会让建池时先抢一条连接，而它 4.0.3 的 `checkFailFast()` 在首次建连失败后**无条件 `sleep(1s)`**（实测每次失败的池构造固定耗时 1002ms，同样地址的裸 JDBC 只要 2~107ms）。那 1 秒睡在调用 `open()` 的线程上，也就是 Flink 的异步算子线程；一旦 Doris 是主源且宕机，每个批次都要重试 `open()`，每批白烧 1 秒。关掉之后，连接失败的代价由 `doris.connect.timeout` 封顶（默认 300ms），且 `open()` 只发生一次、池在 Doris 恢复后自行恢复。

> 注意：`lookup.timeout` **不约束建连阶段**，它只作用在 `batchLookupAsync` 上（超时调度器 + `Statement.cancel()`）。所以「建连失败」这类故障的耗时由客户端/连接池参数决定，不要用 `lookup.timeout` 去推算它。HBase 侧 `open()` 是同步的，同样不受 `lookup.timeout` 约束（见 4.2 与第 14 章）。

### 4.6 防连接/线程打满

Doris 走 JDBC（阻塞 API），若 Doris 出现慢查询而 JDBC 层不先中止，慢查询会在业务层超时降级后**仍占着连接与线程**，很快把池子打满，引发雪崩。为此做了三层保护：

1. **超时真正中断后端**：JDBC 不响应 `thread.interrupt`，所以超时回调里调用 `Statement.cancel()` 让 Doris 真正中止这条 SQL，同时 `task.cancel(true)` 打断等待中的线程。
2. **JDBC queryTimeout 自动收敛**：实际生效的 `doris.query.timeout` 会被钳制到 `≤ ceil(lookup.timeout / 1000)` 秒，保证 JDBC 层最迟在业务层超时前后自行中止。
3. **线程数与连接池等大**：`queryExecutor` 线程数 = `doris.pool.size`。线程少于池大小 → 有空闲连接却没线程用，浪费；线程多于池大小 → 多出的线程阻塞在 `getConnection`，白占线程。

另外，作业取消（`close()`）时会把攒批缓冲里尚未发出的请求**补空**，避免异步算子等待永不完成的 Future 导致作业取消卡住。

> 注意这里「补空」是安全的，与 4.2 中「运行期两源失败不补空」的策略不同——关闭时没有后续重试机会，只能让算子尽快退出。

---

## 5. 参数完整参考

> 所有时间类参数统一用「毫秒整数」，直接写数字，无需带 `ms` 等单位。
> 唯一例外是 `doris.query.timeout`（秒），因为它直接映射 JDBC 原生 API 语义。

### 5.1 核心参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `lookup.primary` | `hbase` | 主源，`hbase` 或 `doris`；另一个自动成为备源（不能配成主备同源） |
| `lookup.timeout` | `500` | 单次（批量）查询硬超时（毫秒），超时即判该源故障、转查备源 |
| `lookup.batch.size` | `50` | 攒批条数阈值：攒满就批量查一次 |
| `lookup.batch.max-wait` | `5` | 攒批最大等待（毫秒），攒不满时最多等这么久就发 |
| `lookup.stats.log-interval` | `360` | 统计日志间隔（秒），默认 6 分钟；0 关闭（连打点线程都不建）。每行同时给出**本阶段**与**自启动累计**两个视角（见第 8 章） |
| `lookup.failover.log-interval` | `10` | 降级日志最小间隔（秒），**0 = 不限流**（每批降级都打印） |

### 5.2 HBase 参数（表名缺省取 DDL 表名）

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.zookeeper.quorum` | — | ZK 地址（逗号分隔）；留空则用 classpath 下 `hbase-site.xml` |
| `hbase.zookeeper.property.clientPort` | `2181` | ZK 端口 |
| `zookeeper.znode.parent` | `/hbase` | ZK 根节点（只有改过 `hbase-site.xml` 才需要动） |
| `hbase.table-name` | DDL 表名 | HBase 表名，支持「命名空间:表名」，如 `dim:dim_account` |
| `hbase.column-family` | `info` | 列族，DDL 所有业务列都从该列族下按列名读（不支持多列族） |
| `hbase.rowkey.delimiter` | `\|` | 复合主键拼接 rowkey 的分隔符；单主键时无效 |
| `hbase.rowkey.encoding` | `string` | rowkey 编码方式：`string`（主键值按字符串写）或 `typed`（按主键类型写）。**必须与写入端严格一致，选错会「一条也查不到」** |
| `hbase.rpc.timeout` | `300` | 单次 RPC 超时（毫秒）。**必须小于 `lookup.timeout`**：它只负责让 HBase 尽早中断 RPC、释放资源，真正的降级判据是业务层超时 |
| `hbase.client.operation.timeout` | `500` | 客户端整体操作超时（毫秒），含冷启动 region 定位，**建议给足 2000** |
| `hbase.client.retries.number` | `1` | 重试次数，实时链路建议 0~1（重试越多越容易把卡顿放大成整批超时） |

**HBase 数据编码约定**（`HBaseLookupReader.decode` 按 DDL 类型解码字节数组，写入端必须一致）：

| DDL 类型 | HBase 存储 | 写入方式 |
|---|---|---|
| `BOOLEAN` | 1 字节 | `Bytes.toBytes(boolean)` |
| `TINYINT` / `SMALLINT` / `INTEGER` | 4 字节 int | `Bytes.toBytes(int)`（读时按 int 解码，再由转换器窄化） |
| `BIGINT` | 8 字节 long | `Bytes.toBytes(long)` |
| `FLOAT` / `DOUBLE` | 4 / 8 字节 | `Bytes.toBytes(float/double)` |
| `DECIMAL` | HBase BigDecimal 编码 | `Bytes.toBytes(BigDecimal)` |
| `DATE` / `TIME` | 4 字节 int（天数 / 毫秒数） | `Bytes.toBytes(int)`，与 Flink 内部表示一致 |
| `TIMESTAMP` | 8 字节 long（毫秒） | `Bytes.toBytes(long)` |
| `CHAR` / `VARCHAR` | UTF-8 字节 | `Bytes.toBytes(String)` |
| `BINARY` / `VARBINARY` | 原始字节 | 直接写 byte[] |

**Rowkey 编码规则**（由 `hbase.rowkey.encoding` 决定，**必须与被查表的写入端严格一致**）：

| 模式 | 单主键编码 | 复合主键编码 | 对应的写入端写法 |
|---|---|---|---|
| `string`（默认） | 主键值转字符串后取 UTF-8 字节：`BIGINT 123` → `Bytes.toBytes("123")` | 按 `hbase.rowkey.delimiter` 拼成字符串再取字节，如 `acct001\|2026-09-29` | Put 时传 `String.valueOf(key).getBytes(UTF_8)` |
| `typed` | 按主键类型编码：`BIGINT` → `Bytes.toBytes(long)`、`INTEGER` → `Bytes.toBytes(int)`、`VARCHAR` → UTF-8 字节、`DECIMAL` → `Bytes.toBytes(BigDecimal)` | 各段类型化编码后用**分隔符字节**连接（定长段也保留分隔符，避免变长段之间的前缀歧义） | 按 HBase 惯例写 `Bytes.toBytes(long)` 等 |

> ⚠️ **这是本连接器最容易踩的坑**：模式选错时一条数据也查不到，而「查不到」在本连接器里属于
> **正常业务结果**（不触发降级、不报错），最终表现为整张维表关联结果全为 NULL，日志里却没有任何异常。
> 排查时先看建连日志里打印的实际模式（`rowkeyEncoding=`），再与被查表的写入方式对齐。
> 编码规则与 `decode()` 的解码规则严格对称，`typed` 模式下「编码 → 解码」可无损还原。
>
> 从 `string` 切到 `typed` 属于**不兼容变更**：它改变的是查询用的 rowkey 字节，不改数据——
> 若历史数据是按字符串写的，切换后会立刻查不到，需要先确认写入端口径再切换。

### 5.3 HBase Kerberos 参数（可选，默认 `simple` 免认证）

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.security.authentication` | `simple` | 认证方式，`simple` 或 `kerberos` |
| `hbase.client.keytab.file` | — | 客户端 keytab 文件路径（kerberos 下必填） |
| `hbase.client.kerberos.principal` | — | 客户端 principal，如 `hbase/ro@EXAMPLE.COM`（kerberos 下必填） |
| `hbase.regionserver.kerberos.principal` | — | RegionServer principal，如 `hbase/_HOST@EXAMPLE.COM` |
| `hbase.master.kerberos.principal` | — | Master principal |
| `hbase.kerberos.krb5.conf` | — | krb5.conf 路径，缺省用系统 `/etc/krb5.conf` |

### 5.4 Doris 参数（`doris.jdbc-url` 必填，表名缺省取 DDL 表名）

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | 如 `jdbc:mysql://host:9030/dim`（库名也可写在 URL 里）。**本连接器唯一必填项** |
| `doris.table-name` | DDL 表名 | Doris 表名，支持「库名.表名」，如 `dim.dim_account` |
| `doris.username` / `doris.password` | `root` / 空 | 账号密码 |
| `doris.driver` | `com.mysql.cj.jdbc.Driver` | JDBC 驱动类名；留空则按 URL 自动推断 |
| `doris.query.timeout` | `1` | JDBC queryTimeout（秒），服务端侧兜底。**运行期会被自动钳制到 ≤ `ceil(lookup.timeout/1000)`** |

### 5.5 Doris 连接池（HikariCP）调优项

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.pool.size` | `8` | 最大连接数（= JDBC 查询线程数），Doris 作主源时建议调大 |
| `doris.pool.min-idle` | `2` | 最小空闲连接数，必须 ≤ `doris.pool.size` |
| `doris.connect.timeout` | `300` | 获取连接超时（毫秒），超时即抛异常触发降级。**必须小于 `lookup.timeout`**：Doris 是备源、无处再降，连接池打满时若业务层先超时会让整批直接失败（HikariCP 下限 250ms）。建池时不做连接尝试（见 4.5），所以 Doris 宕机期间每次取连接的失败代价也由它封顶——即每批最多等这么久就降级 |
| `doris.pool.idle-timeout` | `600000` | 连接空闲多久被回收（毫秒） |
| `doris.pool.max-lifetime` | `1800000` | 连接最大存活时间（毫秒），须大于 `idle-timeout` |
| `doris.pool.validation-timeout` | `250` | 借出连接时的有效性校验超时（毫秒），用于剔除死连接。**不能大于 `doris.connect.timeout`**（HikariCP 硬约束） |

---

## 6. 内置元字段（数据来源与耗时）

在维表 DDL 里声明下面两个**约定名**的字段，即可在查询结果里拿到每次 Lookup 的元信息。这两个字段是连接器内置的，**不需要**在 HBase / Doris 表里存在，也无需任何配置——TableSource 会自动把它们从「待查询列」中剥离。

| 字段名 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | `STRING` | 本次数据实际来自哪个源，取值 `hbase` 或 `doris`（降级后是备源） |
| `lookup_cost_ms` | `BIGINT` | 本次查询耗时（毫秒），从发起到返回，**含降级耗时** |

```sql
CREATE TABLE dim_account (
    account_no     STRING,
    cust_name      STRING,
    lookup_source  STRING,   -- 输出数据来源
    lookup_cost_ms BIGINT,   -- 输出查询耗时
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH ('connector' = 'dual-lookup', /* ... */);

SELECT d.cust_name, d.lookup_source, d.lookup_cost_ms
FROM trans_detail t
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t.proc_time d
    ON t.account_no = d.account_no;
```

**语义细节**（容易被误解，务必看）：

- `lookup_cost_ms` 是**批次耗时**，同一批（最多 `lookup.batch.size` 条）内所有命中的行会拿到**相同的值**。它不是单条查询耗时。
- 主源命中：`lookup_source` = 主源，`lookup_cost_ms` = 主源耗时。
- 降级后命中：`lookup_source` = 备源，`lookup_cost_ms` = **主源超时时间 + 备源查询耗时**的总和（因为计时起点在批次发出时）。
- 主源查不到（空结果）：两者均为 `NULL`（由 LEFT JOIN 补 NULL 体现）。

把这两个字段落库，就能直接用 SQL 分析「降级发生在哪些时段、代价多少」，无需翻日志。

---

## 7. Kerberos 认证（可选）

HBase 2.2.0+ 内置了 keytab 自动登录与 TGT 续期，本连接器只需在 DDL 里配置即可，**无需手动 `kinit`**：

```sql
CREATE TABLE dim_account (...) WITH (
    'connector'                             = 'dual-lookup',
    'hbase.security.authentication'         = 'kerberos',
    'hbase.client.kerberos.principal'       = 'hbase/ro@EXAMPLE.COM',
    'hbase.client.keytab.file'              = '/path/to/hbase.keytab',
    'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM',
    -- 可选：自定义 krb5.conf，缺省用系统 /etc/krb5.conf
    'hbase.kerberos.krb5.conf'              = '/path/to/krb5.conf',
    'doris.jdbc-url'                        = 'jdbc:mysql://...'
);
```

要点：

- `hbase.client.kerberos.principal` 与 `hbase.client.keytab.file` 二者**同时**配置才会生效，缺一会在建表时报错（配置校验已提前拦截）。
- keytab 必须放在**每台 TaskManager** 都能访问的路径（共享存储或随镜像分发），且对 Flink 运行账号可读。
- `hbase.kerberos.krb5.conf` 是 JVM 级系统属性，连接器会在建连（发起认证）之前设置。
- TGT 续期由 HBase 客户端自动完成，长期运行的流作业无需担心凭据过期。

---

## 8. 可观测性与日志解读

连接器会按 `lookup.stats.log-interval`（默认 360 秒 = 6 分钟）在 **TaskManager 日志**里打一行统计。
**一行里给出两个视角**，用 `||` 分隔：前半段 `window=` 是**本阶段**，后半段 `since-start` 是**自启动累计**：

```
[dual-lookup] table=dim_account window=360s totalKeys=5400 avgBatch=45 avgBatchWithStandby=44 | hbase[batches=120 keys=5400 fail=2 avg=16.5ms] doris[batches=2 fail=0 avg=15.1ms] failover=2 || table=dim_account since-start totalKeys=9900 avgBatch=45 avgBatchWithStandby=44 | hbase[batches=220 keys=9900 fail=2 avg=11.8ms] doris[batches=2 fail=0 avg=15.1ms] failover=2
```

（真实运行输出，一行太长这里折成两屏：前 6 分钟跑了 5400 个 key、2 批降级；自启动以来累计 9900 个 key。）

两个视角的字段完全一致，只是取值范围不同：

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `table` | 维表名（一个作业可能有多张 dual-lookup 表） | — |
| `window` | **只有本阶段段有**：本行覆盖的窗口长度（秒），即距上次打点的真实间隔 | 明显大于配置值 → 打点被负载拖延或日志积压 |
| `since-start` | 该段的视角标识，意为「本统计实例创建以来」 | 与 `window=` 段对比，即可看出「总量」与「当前趋势」的差别 |
| `totalKeys` | 该视角下提交给主源的 key 数（降级不重复计） | — |
| `avgBatch` | 平均批大小（**只用主源批次做分母**） | 长期接近 1 → 攒批没生效，检查流量与 `lookup.batch.size` |
| `avgBatchWithStandby` | 含备源批次的平均批大小 | 明显小于 `avgBatch` → 降级期间同一批 key 被主备各查一次，说明降级频繁 |
| `hbase[batches= keys= fail= avg=]` | 主源：批次数 / key 数 / 失败数 / **平均单批耗时** | `fail` 持续增长 → 主源确实有问题或 `lookup.timeout` 配小了；`avg` 持续上升 → 主源正在变慢（此时 `fail` 可能还是 0） |
| `doris[batches= fail= avg=]` | 备源：批次数 / 失败数 / 平均单批耗时 | `batches` > 0 说明发生了降级；`fail` > 0 说明**两源同时异常，需立即介入** |
| `failover` | 该视角下的降级次数 | 持续大于 0 → 主源不可用或阈值不合理 |

五个容易看错的点：

1. **两个视角回答两个不同问题**：`window=` 段看「最近这段时间是否变慢/变差」，`since-start` 段看「全程总量与长期基线」。**判断「现在有没有问题」只看前半段**——累计均值会被历史稀释，主源刚坏 10 分钟时 `since-start` 的 `avg` 几乎不会动。
2. **`since-start` 是「本统计实例创建以来」**：TaskManager 重启（故障恢复、扩缩容、升级）后计数从 0 重新开始，它不等于作业生命周期总量。要跨重启的长期量级，需按 TM 日志分段累加或另接 Flink 指标。
3. **平均耗时的口径**：① 失败批次的耗时也计入，否则「主源正在变慢、开始不断超时」这段恶化过程会被完全掩盖；② 备源耗时从**真正发起备源查询**那一刻起算，不含前面等主源超时的时间——否则主源越慢备源的数字越难看，指标就失去了诊断价值。
4. **无样本时显示 `-`**（如 `doris[batches=0 fail=0 avg=-]`），不是 `0`——`0ms` 会被误读成「秒回」。
5. **为什么默认 6 分钟**：这条日志的价值在于**趋势**而非实时告警——真正的故障有降级 WARN 即时暴露。间隔太短（如 10 秒）时低流量任务每行只有个位数样本，噪声大且刷屏；排查期间可临时调到 10~30 秒。

**为什么两个视角要挤在同一行**，而不是各打一行：① TM 日志是多线程并发写的，两行之间可能插进别的子任务输出，靠时间戳配对容易错位；② 同一行里的两段来自**同一份计数快照**（见 `LookupStats.Sample`），因此 `本行阶段 + 此前累计 = 本行累计` 恒成立，可以直接做守恒校验；若两段各自去读原子变量，就会出现「累计比阶段少 3 批」这类自相矛盾的数字，让人去查一个并不存在的 bug。

另外注意区分两类日志：

- **降级 WARN**：`[dual-lookup] 主源 X 批量查询失败，降级到备源 Y。批大小=N，原因：...`
  含具体异常原因，是排查主源问题的第一手材料。这条日志**按 `lookup.failover.log-interval`（默认 10 秒）限流**（见下方「日志洪峰」）。
- `LOG.info("[dual-lookup] 主源 X 已恢复，重新由主源提供数据（本次故障共降级 N 批）")` —— 主源从故障中恢复时补一条，与上面的 WARN 配对，不必再靠「WARN 停了没有」推断恢复时点。
- `LOG.info("[dual-lookup] HBase reader opened, ...")` / `Doris reader opened, ...` —— 每个并发子任务各打一次，出现次数 = 算子并行度。若某个子任务一直没打，说明它从未成功建连。

### 8.1 日志洪峰：为什么降级日志要限流

主源长时间故障时，降级日志的量级由**批次数**决定，而不是记录数。以每分钟 5000 条流量、`lookup.batch.max-wait=30ms` 为例：

| 指标 | 数值 |
|---|---|
| 流量 | 5000/min ≈ 83 条/秒 |
| 有效批大小（83 × 0.03） | ≈ 2.5 条/批 |
| 降级批次 | ≈ 33 批/秒 |
| 不限流时的 WARN 量 | ≈ 33 行/秒 → **约 285 万行/天** |

后果不只是占磁盘——TM 日志频繁滚动会把**故障起始时刻那几行**冲掉，反而更难定位根因。

限流策略是「**首条必打 + 按间隔采样 + 合并计数 + 恢复补打**」：

```
t=0.00s  主源 hbase 批量查询失败，降级到备源 doris。批大小=3，原因：TimeoutException: HBase lookup timeout after 500ms     ← 首条必打
          （0.03s ~ 10.00s 之间约 330 批降级，静默）
t=10.03s 主源 hbase 仍在降级中，降级到备源 doris。批大小=3，原因：...（距上次打印又发生 330 批降级已被合并）              ← 采样一条
t=25.10s 主源 hbase 已恢复，重新由主源提供数据（本次故障共降级 830 批）                                                ← 恢复信号
```

三点保证「洪峰被压平，但数量不丢」：故障起点必留痕、每条日志自带被合并的批数、精确计数另有统计日志的 `failover=` 字段兜底（`window=` 段给本阶段降级次数，`since-start` 段给累计降级次数，见第 8 章）。

需要每一次降级都留痕（故障复盘、精确排失败时序）时，把 `lookup.failover.log-interval` 设为 `0` 即可回到逐批打印。该值不允许为负——0 已经表达了「不限流」，负数会在建表阶段被拦下。

### 8.2 跑几个月的资源开销：可以忽略，但有三处要留意

统计是「**每 6 分钟打一行**」而不是「每批打一行」，成本量级由此决定：

| 维度 | 量级 | 说明 |
|---|---|---|
| 打点 CPU | 每 6 分钟几十微秒 | 单守护线程被唤醒一次：9 次原子读 + 一次字符串格式化 + 一次日志写入，占整个 6 分钟窗口的比例 **< 0.001%** |
| 打点线程 | 每算子实例 1 个 | 守护线程；`lookup.stats.log-interval=0` 时**连线程都不创建** |
| 埋点 CPU | 每批 4 次原子自增 | 无争用时约 10~20ns/次 → 每批 < 0.1μs。而每批本身至少一次 HBase RPC 或 JDBC 查询（毫秒级），占比 **< 0.01%** |
| 常驻内存 | **固定约 200 字节** | 9 个 `AtomicLong`，无集合、无缓存、无队列——**跑一个月和跑一年的内存占用完全一样**，不存在随时间增长的状态 |
| 日志体积 | 约 240 行/天/子任务 | 默认 360s 一行，每行约 320 字节 → 约 77KB/天；一年约 28MB，8 并发约 224MB/年 |
| 计数器溢出 | **不会**（理论 29 年起） | 见下 |

**计数器不会溢出**：9 个计数器都是 `long`（上限 9.22×10^18）。
① key 数按 5000 条/分钟算，一年约 2.6×10^9，要溢出需约 5000 万年；
② 唯一相对接近上限的是**纳秒级累计耗时**：只有在「1000 批/秒 × 10ms/批」这种极端吞吐下**连续跑 29 年**才会触及 `long` 上限——而单子任务 1000 批/秒的 HBase RPC 吞吐本身就不现实（`lookup.timeout` 默认 500ms 也意味着单批更慢）。结论：跑几个月连零头都用不到。

三处**真正需要留意**的地方：

1. **日志体积与滚动策略**（全文唯一需要运维动作的点）。默认间隔下每子任务约 77KB/天，本身毫无压力；Flink 1.18 的 TM 日志 appender 默认就是 `SizeBasedTriggeringPolicy=100MB` + `DefaultRolloverStrategy max=10`（即最多约 1GB，位于 `$FLINK_HOME/conf/log4j2.properties`），不额外配置也不会失控。若嫌噪音大，把 `lookup.stats.log-interval` 调到 `1800`（半小时一行），体积再降 5 倍。
2. **打点任务异常停摆**（代码里已兜底）。`scheduleAtFixedRate` 有个不显眼的坑：任务一旦抛出未捕获异常，**后续调度会被静默取消**——不打印、不重试，统计日志就此永久消失而没人发现。`LookupStats.logSnapshotSafely()` 用 `try/catch (Throwable)` 兜住，并只在首次失败时告警一次：统计只是可观测性手段，不能因为一次格式化问题把自己打死。
3. **`since-start` 会被历史稀释、且随 TM 重启归零**（解释性，不是故障）。因此「最近是否变慢」永远看 `window=` 段；要跨重启的长期曲线，应另接 Flink 指标系统。

> **关于 `LongAdder` 与 `MetricGroup`**：`LongAdder` 在高争用下更快，但读时要 `sum()`、拿不到精确一致快照，会让「本行阶段 + 此前累计 = 本行累计」的守恒校验变模糊；而本场景的争用极低（埋点来自攒批线程与源侧回调，批次速率受 RPC 吞吐限制），换 `LongAdder` 省下的纳秒级开销换不来任何收益。
>
> Flink 指标**其实是可以接的**：异步 Lookup Function 虽然拿不到 `RuntimeContext`，但它的 `open(FunctionContext)` 拿到的 `FunctionContext.getMetricGroup()` 返回的正是**本并行子任务的 metric group**（已核对 Flink 1.18 `flink-table-common` 源码），注册 `Counter` / `Gauge` 完全可行，并不需要改成 `RichAsyncFunction`。之所以仍用日志：① 巡检与复盘要的是「一行看全 + 可直接 grep + 能带文本字段（失败原因、本阶段/累计两个视角）」，`Gauge` 只给单个数值；② `FunctionContext` 在常量折叠等本地执行路径上拿到的是**未注册**的 metric group（注册不生效且只打一条 WARN），两条路径行为不一致，日志则始终一致。要接指标系统见第 14 章演进方向。

---

## 9. 容量测算与调优

### 9.1 容量测算（以 2000 QPS、5000 万行维表为例）

参数怎么调都围绕这三个公式，先记住它们：

```
并发数       = QPS × 单次延迟                                  # 压到后端的真实压力，不是 QPS 本身
有效批大小   = min(batch.size, 每子任务QPS × batch.max-wait)
算子吞吐上限 ≈ buffer-capacity × 并行度 ÷ 端到端延迟
```

**「5000 万行」几乎不进公式。** 点查是 O(log n) 的索引定位 + 一次 tablet 随机读，5000 万行对 Doris 属中小表。真正决定点查速度的是这四项：

| 因素 | 要求 |
|---|---|
| 表模型 | Unique Key 模型 + `enable_unique_key_merge_on_write = true` |
| 行存 | `store_row_column = true` + `light_schema_change = true`，两者同开才能走 **SHORT-CIRCUIT** 短路径 |
| BUCKETS 数 | 决定并发分散度；bucket 太少，热点 key 会压在同一块 tablet 上 |
| 版本数 / compaction | 维表高频更新时版本堆积，点查从 merge-on-read 退化为 merge-on-many-read，延迟成倍上涨 |

验证方法：对点查 SQL 跑 `EXPLAIN`，看计划里有没有 `SHORT-CIRCUIT` 标记。**有 → 一次 RPC 完成，2000 QPS 很轻松；没有 → 走常规规划路径，FE 的 CPU 就是天花板。**

**2000 QPS 的账**（入流 2000 行/秒、并行度 4）：

| 配置 | 到达后端的 SQL 次数 | 说明 |
|---|---|---|
| `batch.size=1`（不攒批） | 2000 / 秒 | 每条都要 FE 解析 + 规划，按 1~3ms/条算约烧掉 2~6 个 FE 核。**会出问题** |
| `max-wait=5`（默认） | ≈ 800 / 秒 | 每子任务仅 500 行/秒，5ms 只能攒到 2.5 条；`batch.size=50` 要 25ms，永远攒不满 |
| `max-wait=30~50` | 40~67 / 秒 | 攒批真正生效 |

> ⚠️ **默认 `lookup.batch.max-wait=5` 在中等流量下会让攒批基本失效** —— 有效批大小由 `max-wait` 决定，而非 `batch.size`。
> 判定方法：看统计日志里的 `avgBatch`，若长期远小于 `batch.size`，就是这个原因。

**2000 QPS 下三个会先触顶的地方**（按触发顺序）：

1. **异步算子 `buffer-capacity`**（默认 100）。延迟 25ms（5ms 攒批 + 20ms 查询）时上限约 `100 × 4 ÷ 0.025 ≈ 16000/s`，看着够；但 Doris 延迟一旦抖到 250ms 就塌到 1600/s，**直接反压**。高 QPS 场景建议显式设到 300~500。
2. **FE 规划 CPU** —— 仅在攒批失效时才会成为问题。
3. **降级瞬间，备源要独扛全量。** 本连接器是整批降级，主源一挂，2000 QPS 会全部压到备源。因此**备源必须按全量峰值规划容量，而不是按稳态分担比例** —— 这是双源容错最容易被忽略的一点。

**另外两点容易踩的**：

- 连接数通常不是瓶颈（Doris `qe_max_connection` 默认 1024，本连接器实际占用 = 并行度 × `doris.pool.size` = 4 × 8 = 32）。但若连接池被打满，`doris.connect.timeout`（默认 300ms）会先抛「获取连接超时」——**注意这时候 Doris 是备源、没有下一级可降，整批仍然失败**。排查方向是加大 `doris.pool.size`（以及查 Doris 侧为什么慢），而不是调大 `connect.timeout`（后者会被校验拦下，且调大只会让业务层先超时）。
- 想靠固定 `IN` 占位符长度去命中 Doris 的 PreparedStatement 优化是**行不通的**：该特性仅支持单主键等值点查，明确不支持 `IN` 列表与嵌套子查询。维表侧真正有效的仍是 SHORT-CIRCUIT + 行存。

### 9.2 上线前必做

- [ ] `SET 'table.exec.async-lookup.timeout'`，取值 ≥ `lookup.timeout × 2 + 2s`
- [ ] 关联 SQL 写了 hint 时，`'table'` 与 FROM 子句里的别名一致（多表 join 时每表各写一条，见 3.4）
- [ ] 维表 DDL 声明了 `PRIMARY KEY (...) NOT ENFORCED`
- [ ] 维表的每个业务列在两源都存在、列名一致、类型与 DDL 匹配
- [ ] `hbase.client.operation.timeout` 给足（建议 2000），避免冷启动定位 region 误判超时
- [ ] Doris 连接池 `doris.pool.size` 与算子并行度匹配（总连接数 = 并行度 × pool.size，别打爆 Doris）

### 9.3 场景化参数建议

| 场景 | 调整方向 |
|---|---|
| **高吞吐**（每并行度几万 QPS） | `lookup.batch.size` 提到 100~200；`lookup.batch.max-wait` 保持 5~10ms；`doris.pool.size` 适量调大 |
| **中等流量**（每并行度数百 ~ 数千行/秒） | `lookup.batch.max-wait` 提到 30~50ms —— 默认 5ms 会让攒批失效，见 9.1 |
| **低延迟敏感**（P99 要求 < 50ms） | `lookup.batch.max-wait` 降到 1~2ms；`lookup.batch.size` 降到 20~30（别为了吞吐牺牲延迟） |
| **以 Doris 为主源** | `lookup.primary` 改 `doris`；`doris.pool.size` 建议 16 起；HBase 转备源，其 `hbase.rpc.timeout` 仍须小于 `lookup.timeout`，但 `hbase.client.operation.timeout` 可放宽 |
| **主源频繁抖动** | 先查清是网络还是 RegionServer；临时可放宽 `lookup.timeout`，但别超过业务可接受延迟 |
| **两源数据不一致** | 用 `lookup_source` 字段做对比校验，定位差异时段 |

### 9.4 反模式（别这么配）

| 配置 | 问题 |
|---|---|
| `lookup.timeout` 大于 `table.exec.async-lookup.timeout` | 降级逻辑永远不会执行，容错形同虚设（**已由校验拦截**） |
| `lookup.timeout` 约等于主源 P50 延迟 | 正常抖动就被判故障，备源被无谓打爆 |
| `lookup.batch.max-wait` 接近 `lookup.timeout` | 攒批等待吃掉全部超时预算（**已由校验拦截**） |
| `hbase.rpc.timeout` / `doris.connect.timeout` ≥ `lookup.timeout` | 业务层已切源，后端还在等 RPC / 等连接（**已由校验拦截**） |
| `lookup.batch.size` 配成 1 | 攒批失效，Doris 交互次数退化为逐条 |
| `lookup.batch.max-wait` 用默认 5ms 而流量中等 | 有效批大小被卡在个位数，攒批只发挥 2~3 倍效果；用 `batch.size ÷ 每子任务QPS × 1.5` 估算取值 |
| 备源按「稳态分担比例」规划容量 | 主源挂掉时备源要独扛全量，会直接被打爆 |
| 多表 join 时每张维表都按单表标准配 `doris.pool.size` | 总连接数 = Σ（每表并行度 × pool.size），叠加后超出 Doris 承载；码表类小维表降到 2~4 |
| `doris.pool.min-idle > doris.pool.size` | 启动即报错（已做校验拦截） |
| HBase rowkey 编码模式与写入端不一致 | 一条也查不到，且不触发降级、不打异常日志，最终关联结果全为 NULL |

---

## 10. 故障演练

```bash
# 场景 1：主源 HBase 整体宕机
#   预期：作业不失败，数据持续输出，lookup_source 变为 doris，failover 持续增长
stop-hbase.sh

# 场景 2：注入网络延迟制造超时（600ms > lookup.timeout=500ms）
#   预期：降级到 Doris，WARN 日志出现 "HBase lookup timeout after 500ms"
tc qdisc add dev eth0 root netem delay 600ms
#   恢复
tc qdisc del dev eth0 root netem

# 场景 3：两源同时不可用
#   预期：作业失败并抛异常（不静默丢数据，属预期行为）

# 场景 4：备源 Doris 宕机（HBase 正常，默认 lookup.primary=hbase）
#   预期：完全无影响——Doris 的 Reader 从未被 open()，一个连接都不会建，日志里也不会出现 Doris 相关报错
#   唯一代价：这段时间「容错能力归零」，若 HBase 同时出问题会直接走到场景 3
mysql -h 192.168.214.128 -P 9030 -uroot -e "shutdown"   # 或直接停 FE/BE

# 场景 5：Doris 作主源（lookup.primary=doris）且 Doris 宕机
#   预期：每批降级到 HBase、数据连续；统计行 failover 持续增长，WARN 每批一条
#   代价：每批多等最多 doris.connect.timeout(300ms)（不是 lookup.timeout）
#   当前实现没有熔断器，所以会「每批都试一次死掉的主源」——属于预期，注意日志量
```

---

## 11. 常见问题排查

| 现象 | 可能原因 | 排查方向 |
|---|---|---|
| 建表报 `Could not find any factory for identifier 'dual-lookup'` | jar 没放到所有节点，或集群未重启 | 确认 `$FLINK_HOME/lib` 下 jar 存在（JM 与 TM 都要），并重启集群 |
| 建表报「必须声明 PRIMARY KEY」 | DDL 漏写主键 | 加上 `PRIMARY KEY (...) NOT ENFORCED` |
| 建表报「未知配置项」 | 参数拼写错误，或该参数未在 Factory 里登记 | 对照第 5 节核对参数名 |
| SQL 校验报 LOOKUP hint 不匹配 | hint 的 `'table'` 写成了表名而不是 FROM 里的别名 | 把 `'table'` 改成该 join 的表别名（见 3.3） |
| 想确认是否真走了异步 lookup | — | `EXPLAIN` 看计划；或观察 TM 日志里统计行是否按批出现（见第 8 节） |
| 一直降级到 Doris，但 HBase 看起来正常 | `lookup.timeout` 配得小于实际 P99；或冷启动 region 定位未预热 | 看 WARN 日志里的异常原因；确认 `hbase.client.operation.timeout` 给足 |
| Doris 报 `Unknown column 'lookup_source'` | 元字段被误当成真实列下推了 | 这属于 bug，请检查 `RowDataConverter.isMetaField` 的剥离逻辑 |
| HBase 查到的数值/时间全部错乱 | 写入 HBase 时的编码与 DDL 类型不匹配 | 对照 5.2 节的编码约定表逐列核对 |
| HBase 复合主键查不到 | rowkey 拼接规则与建表侧不一致 | 核对 `hbase.rowkey.delimiter` 与建表侧的 rowkey 生成规则 |
| Doris 侧 BINARY/DECIMAL 主键查不到 | 旧版本按 `toString()` 匹配主键（byte[] 得对象哈希、BigDecimal 带 scale） | 已修复：统一按 Base64 / 去尾零规范化后匹配，升级即可 |
| 作业取消失败 / 卡住 | 旧版本可能遗漏了 `close()` 补空 | 确认 `DualLookupFunction.close()` 会补空攒批缓冲 |
| TaskManager 报连接数过多 | 并行度 × `doris.pool.size` 超出 Doris 承载 | 降低 `doris.pool.size` 或算子并行度 |
| 集群 lib 里已有 HBase 但报 `NoSuchMethodError` | 集群 HBase 版本与 `hbase-shaded-client` 不一致 | 调整 pom 里 `hbase.version` 与集群对齐后重新打包 |
| Doris 宕机，但作业毫无反应（备源场景） | 备源只在需要降级时才被建连，**平时不做健康检查** | 属预期行为；但这期间容错能力为 0，建议对 Doris 单独做可用性监控，别等 HBase 出事才发现 |
| Doris 当主源且宕机时，每批都要重试死掉的主源、吞吐下降 | 当前没有熔断器：`open()` 失败或查询超时后会持续重试（降级 WARN 本身已按 `lookup.failover.log-interval` 限流，日志不再是问题） | 属预期；短期可把 `doris.connect.timeout` 压到 250ms（HikariCP 下限）限损，长期见第 14 章熔断器 |

---

## 12. 代码结构与实现要点

```
src/main/java/com/roc/flink/connector/dual/
├── DualLookupOptions.java             # 配置项声明 + 可序列化配置值对象（含跨字段校验）
├── RowDataConverter.java              # Flink 内部类型 ↔ Java 对象双向转换
├── LookupReader.java                  # 单源读取抽象（接口，批量异步 + 自带超时）
├── HBaseLookupReader.java             # HBase 异步查询 + 建连预热 region location + 超时
├── DorisLookupReader.java             # Doris JDBC + 连接池 + 攒批 SQL + 中断后端
├── DualLookupFunction.java            # 异步 Lookup Function（攒批 + 主备路由核心）
├── LookupStats.java                   # 运行期统计（一次打点两个视角：本阶段增量 + 自启动累计 / 各源平均耗时）
├── DualLookupTableSource.java         # LookupTableSource（装配 Reader 与 Function）
├── DualLookupTableSourceFactory.java  # SPI 工厂（配置校验 + schema 提取）
└── resources/META-INF/services/org.apache.flink.table.factories.Factory
                                       # SPI 注册文件，内容为实现类全限定名

src/test/java/com/roc/flink/connector/dual/
├── DualLookupOptionsTest.java         # 配置解析 + 交叉校验 + 日志间隔取值（12 个用例）
├── DorisLookupReaderTest.java         # 伪造 JDBC 对象：重复主键分发 / 超时清理 / 队列满拒绝 /
│                                      #   byte[]·BigDecimal 主键按内容匹配 / 超时任务取消 /
│                                      #   Doris 宕机时 open() 不被建池自检拖住（9 个）
├── DualLookupFunctionTest.java        # 攒批时序（含残留定时器回归）/ 主备降级 / 空结果不降级 /
│                                      #   关闭补空 / close 后降级链不重新建连 / 降级日志限流闸门 /
│                                      #   各源耗时埋点接线（13 个）
├── HBaseRowKeyEncodingTest.java       # rowkey 两种编码模式的字节结果与往返还原（5 个）
├── HBaseLookupReaderTest.java         # 已完成查询取消超时任务、调度器队列清空（1 个）
├── RowDataConverterTest.java          # 全类型双向转换 / 元字段 / 大小写兼容 / 时间多形态（7 个）
└── LookupStatsTest.java               # 阶段值而非累计 / 每行都带累计视角且守恒 / 各源平均耗时 /
                                       #   无样本显示 - / 累计汇总 / interval=0 不建线程（6 个）
```

各层的职责边界（读代码时按这个映射找）：

| 类 | 只做这件事 | 关键实现点 |
|---|---|---|
| `DualLookupOptions` | 声明参数 + 一次性读取校验 | `Config` 必须 `Serializable`，会随 Function 下发 TM；校验在建表阶段就报错 |
| `LookupReader` | 定义契约 | Future 若不正常完成，就一定是「超时或异常」；空结果不算故障 |
| `HBaseLookupReader` | HBase 点查 | 用 `AsyncConnection` 避免阻塞异步算子；`table.get(List<Get>)` + `allOf`；Future 层强制超时 |
| `DorisLookupReader` | Doris SQL 点查 | 攒批拼 `IN`/`OR`；JDBC 阻塞查询丢线程池；超时调 `Statement.cancel()` 真中断 |
| `RowDataConverter` | 类型转换 | 两源结果统一为 `Map<列名小写, Java对象>`，是「两源可互换」的关键 |
| `DualLookupFunction` | 攒批 + 主备路由 | 锁内只入队、锁外做 IO；整批降级；两源都失败不补空 |
| `DualLookupTableSource` | 装配 | 剥离元字段后才交给 Reader；按 `lookup.primary` 交换主备变量 |
| `DualLookupTableSourceFactory` | 入口与校验 | `doris.jdbc-url` 唯一必填；主键强校验；ConfigOption 必须全部登记 |

**「两源可互换」是怎么保证的**：两个 Reader 都把结果统一成 `Map<列名小写, Java对象>`，再由 `RowDataConverter` 按 DDL 类型转成 `RowData`。只要两源表结构与 DDL 一致，切换后输出完全等价。列名统一转小写、元字段走独立内部键，都是为这一点服务的。

**单元测试**：`mvn test` 跑全部 53 个用例，**不依赖任何外部服务**——用伪造的 `HikariDataSource` 子类 + JDBC `Proxy` 对象、以及假的 `LookupReader` 驱动真实代码，因此可以直接放进 CI 作为发布门槛。覆盖的关键行为包括：批内重复主键分发、超时后打断后端、线程池队列满时降级、Doris 宕机时 open() 不被建池自检拖住、残留定时器不冲下一批、**空结果不降级（核心语义）**、两源失败不补空、rowkey 编码往返、byte[]/BigDecimal 主键按内容匹配、已完成查询取消超时任务、close 后降级链不重新建连、降级日志首条必打与限流、统计输出同时给出阶段值与自启动累计（且两者守恒）、各源耗时独立、全类型双向转换。

> 本机没有 mvn 时，可改用仓库根目录的 `run_units.py`（IDEA 自带 JBR 编译 + `JUnitCore` 运行），效果相同。

> 各轮代码评审发现的问题、修复方案与实测对比，见根目录 `CODE_REVIEW.md`（累计 19 项，均已修复并固化为回归用例）。
> 端到端测试方案见 `TEST_PLAN.md` + `sql/e2e_test.sql` + `sql/gen_test_data.py`。

---

## 13. 配置项校验规则

以下校验在建表阶段（`Config.from` / Factory `validate`）执行，配置有问题会**立刻报错**而不是等运行期：

| 校验 | 规则 |
|---|---|
| 主源取值 | 只能是 `hbase` 或 `doris`（忽略大小写与首尾空格） |
| `lookup.timeout` | 必须 > 0（否则降级永不触发） |
| `lookup.batch.size` | 必须 > 0（否则批永远发不出去） |
| `lookup.batch.max-wait` | 不能为负（否则失去时间兜底） |
| `lookup.failover.log-interval` | 不能为负（`0` 已表达「不限流」，负数无意义，避免被当成「关闭日志」误用） |
| `lookup.stats.log-interval` | 不能为负（负数与 `0` 行为上无法区分，与其静默照做不如建表期报错） |
| `doris.pool.min-idle` | 不能大于 `doris.pool.size`（HikariCP 硬约束） |
| `doris.pool.validation-timeout` | 不能大于 `doris.connect.timeout`（HikariCP 硬约束） |
| `doris.connect.timeout` | 必须小于 `lookup.timeout`（备源无处再降，池满时业务层先超时会让整批失败） |
| `hbase.rpc.timeout` | 必须小于 `lookup.timeout`（否则业务层已切源、HBase 还在等那次 RPC） |
| `lookup.batch.max-wait` | 必须小于 `lookup.timeout`（否则攒批等待吃掉整个超时预算，批次等不到定时触发） |
| `hbase.rowkey.encoding` | 只能是 `string` 或 `typed`（写错不会报错、只会静默查不到，因此强制拦截） |
| `table.exec.async-lookup.timeout` | 必须大于 `lookup.timeout`（否则降级还没执行完 Flink 就先判超时） |
| `doris.jdbc-url` | 必填（备源是容错能力的下限） |
| Kerberos 模式 | `hbase.client.keytab.file` 与 `hbase.client.kerberos.principal` 必须成对配置 |
| 主键 | 维表必须声明 `PRIMARY KEY`，且不支持嵌套主键 |
| 参数名 | 出现未在 Factory 中登记的配置项会直接报错（防拼写错误） |

> **给二次开发者**：新增一个配置项需要改三处——① `DualLookupOptions` 加 `ConfigOption` 与 `Config` 字段；
> ② `DualLookupTableSourceFactory.optionalOptions()` 里登记；③ 在 `Config.from` 中赋值。
> **漏掉第 ② 步会导致 DDL 里写该参数直接报「未知配置」，这是最容易遗漏的一步。**
>
> 新增配置项时建议同时补一条 `DualLookupOptionsTest` 用例——上面这些校验规则都有对应测试守着。

> **超时预算的推荐关系**（上面四条交叉校验就是按这个顺序设计的）：
>
> ```
> hbase.rpc.timeout  <  doris.connect.timeout  <  lookup.batch.max-wait  <  lookup.timeout  <  table.exec.async-lookup.timeout
>      让 HBase 释放资源        让池等待快速失败          攒批等待上限                判定降级             算子层兜底
> ```
>
> 各层职责不能混：前两者只负责「尽早释放资源/快速失败」，**降级判据只有一个**——`lookup.timeout`。

> **升级注意**：本次新增了上述 4 条超时交叉校验，并调整了两个默认值
> （`doris.connect.timeout` 3000 → 300、`doris.pool.validation-timeout` 3000 → 250）。
> 如果你现有的 DDL **显式写了** `doris.connect.timeout` 且其值 ≥ `lookup.timeout`，
> 升级后会在建表阶段直接报错——这是有意为之：那种组合在连接池被打满时会导致整批失败。
> 按上表调整取值即可，可参考 `sql/demo.sql`。

---

## 14. 已知限制与演进方向

**当前限制**：

- HBase 侧要求所有列在**同一列族**（DDL 所有业务列都从 `hbase.column-family` 下按列名读取）。
- 不支持主键**嵌套结构**（PRIMARY KEY 必须是普通列）。
- **无熔断保护**：主源长时间宕机时，每批数据仍需等满 `lookup.timeout` 才降级，吞吐受超时限制。这是「精简优先」的取舍。
- HBase 建连失败会**阻塞在 `open()`**（`createAsyncConnection().get()` 等待 ZooKeeper 超时），此时该批请求会被同步阻塞，依赖 ZK/客户端超时兜底。
- 降级粒度是整批，主源部分成功时也要整批重查备源。

**演进方向**（按优先级）：

1. **熔断器**：主源连续失败 N 次后直接跳过主源一段时间，解决「主源整体宕机时吞吐被超时拖死」的问题。
2. **主备结果对账**：利用 `lookup_source` 元字段做双写抽样比对，用于数据迁移期的正确性验证。
3. **更多源**：抽象层已就绪（`LookupReader` 接口），新增 Redis / MySQL 源只需实现该接口 + 在 Factory 里登记。
4. **Flink Metric 上报**：让统计接入 Flink 的指标体系（WebUI 可视化、可接告警）。路径比想象中短——不需要改成 `RichAsyncFunction`，`open(FunctionContext)` 里的 `FunctionContext.getMetricGroup()` 就是本并行子任务的 metric group，直接在 `LookupStats.start()` 时注册 `Counter` / `Gauge` 即可；代价是两套出口要各自保证不重复统计。当前定时日志已覆盖巡检需求，其长期开销评估见第 8.2 节。

---
