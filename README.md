# Flink Dual Lookup Connector

一个 Flink SQL 维表连接器：**HBase 主查、Doris 兜底**，超时/异常自动降级，全程攒批异步。SQL 侧照常写 `FOR SYSTEM_TIME AS OF`，容错对业务透明。

```
asyncLookup(key) ─► 攒批缓冲 ─(满 50 条 / 等 5ms)─► 主源 HBase 批量 Get
                                                   │ 成功        │ 超时/异常
                                                   ▼             ▼
                                              distribute()  备源 Doris 批量 SQL
                                                   └──────┬──────┘
                                                          ▼
                                                  Lookup Join 输出
```

## 1. 解决什么问题

单源 HBase 的 RegionServer 抖动、运维重启、网络分区，都会让单次点查从 5ms 涨到数秒，最终算子背压甚至作业失败；单源 Doris 则可能被慢查询打满线程与连接池。本连接器把「主备路由 + 超时降级 + 攒批异步」收进一个 DDL：配好两个源即可，主备对调只改一行 `lookup.primary`。

## 2. 环境与依赖

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 1.8 | 编译与运行 |
| Flink | 1.18.1 | 使用 `AsyncLookupFunction`（1.16+ 可用） |
| HBase | **2.2.7** | `hbase-shaded-client-**byo-hadoop**`，原生异步客户端 |
| Hadoop | **3.1.3** | 集群提供；`hadoop-common`/`hadoop-auth` 仅 `provided`，**不打进 jar** |
| Doris | 4.0.8 | MySQL 协议（JDBC + HikariCP），2.x/3.x 也适用 |
| Kafka | 3.x（客户端由集群提供） | **仅告警旁路用**，不参与数据读取；`kafka-clients` 为 `provided`，避免与集群版本重复 |
| 构建 | Maven 3.6+ | shade 打 fat jar（约 29MB） |

硬约束：

- **HBase 与 Hadoop 必须成对**：用 `hbase-shaded-client-byo-hadoop`，带 Hadoop 的版本会引发 `NoSuchMethodError: ... FsTracer.get(...)Lorg/apache/htrace/core/Tracer;`（jar 内置 Hadoop 3.4.1，集群 3.1.3，返回类型不同）。`htrace-core4` 同理不打进 jar，hadoop-free 部署需手动补进 `lib/`。
- **HikariCP 4.0.3**：兼容 Java 8 的最后一代（5.x 需 Java 11+）。
- **Kafka 客户端（v1.4+）**：只服务告警旁路（10.1），`lookup.alert.threshold=0`（默认）时不创建任何 Kafka 对象。`kafka-clients` 改为 **`provided`**，由集群的 `flink-connector-kafka` 统一提供，避免 fat jar 与集群出现两份不同版本的客户端。若集群没有 kafka-clients，告警初始化会打 WARN 并禁用告警，但作业继续运行。打包仍排除 `zstd-jni`/`lz4-java`/`snappy-java`，默认 `compression.type=none`。

核对集群 Hadoop 世代：`javap -classpath $FLINK_HOME/lib/hadoop-common-*.jar org.apache.hadoop.fs.FsTracer`。3.1.3 返回 `org.apache.htrace.core.Tracer`，3.4+ 是 `org.apache.hadoop.tracing.Tracer`——两者不能混用。

## 3. 构建与部署

```bash
mvn test                          # 127 个用例；真实 Kafka 集成测试默认跳过
mvn clean package

# 产物体检：有输出就说明 jar 混入了本应由集群提供的类
jar tf target/flink-dual-lookup-connector-1.4.0.jar \
  | grep -E 'org/apache/hadoop/(fs|conf|security)/|org/apache/htrace/|org/apache/kafka/' \
  && echo '✗ jar 被污染' || echo '✓ jar 干净'

rm $FLINK_HOME/lib/flink-dual-lookup-connector-1.*.jar    # 旧包必须删，JM 与所有 TM
cp target/flink-dual-lookup-connector-1.4.0.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

> 只允许 `org/apache/hadoop/hbase/**` 出现在 jar 里。1.4.x 约 **29MB**（Kafka 客户端改为集群提供；更早带 Hadoop 的版本 65MB）。升级前看第 15 章。

## 4. 最小示例

完整示例见 [sql/demo.sql](sql/demo.sql)。

```sql
SET 'table.exec.async-lookup.timeout' = '10s';        -- 必须 > lookup.timeout
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.output-mode' = 'ALLOW_UNORDERED';

CREATE TABLE dim_account (
    account_no     STRING,
    cust_name      STRING,
    balance        DECIMAL(18,2),
    lookup_source  STRING,        -- 元字段：hbase / doris
    lookup_cost_ms BIGINT,        -- 元字段：查询阶段耗时（不含惰性建连）
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector'              = 'dual-lookup',
    'lookup.primary'         = 'hbase',   -- 改 'doris' 即主备对调
    'lookup.timeout'         = '500',
    'lookup.batch.max-wait'  = '30',
    'hbase.zookeeper.quorum' = '127.0.0.1',
    'doris.jdbc-url'         = 'jdbc:mysql://host:9030/dim'
);

SELECT /*+ LOOKUP('table'='d', 'async'='true', 'capacity'='200') */
    t.trans_jnls_no, t.amount, d.cust_name, d.lookup_source
FROM trans_detail t
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t.proc_time d
    ON t.account_no = d.account_no;
```

上线前核对：算子层 `table.exec.async-lookup.timeout` = `lookup.timeout × 2 + 2s`；hint 的 `'table'` 用 **FROM 里的别名**；维表有 `PRIMARY KEY`；每个业务列在两源都存在且同名；`hbase.table-name` / `hbase.column-family` / `hbase.rowkey.encoding` 与写入端一致。

## 5. LOOKUP hint

不是语法必需——`FOR SYSTEM_TIME AS OF` + 主键等值条件已决定 Lookup Join，本连接器也只提供异步实现。生产建议显式写，方便评审。

| 参数 | 作用 | 不写时 |
|---|---|---|
| `'table'` | 指定维表；**必须用别名**，写了其他参数它就必填 | 整个 hint 都可以不写 |
| `'async'` | 显式声明异步语义 | 自动异步 |
| `'capacity'` / `'timeout'` / `'output-mode'` | 只覆盖本条 join 的算子参数，优先级高于全局 SET | 取全局 SET |

hint 的 `'timeout'` 是**算子层**兜底，与 DDL 的 `lookup.timeout`（**连接器层**降级判据）是两回事，必须大于后者；hint 管不到攒批、主备、rowkey 编码。多表 join 时每张维表各写一条（靠 `'table'` 区分）。

## 6. 运行机制

### 6.1 攒批：条数 + 时间双触发

条数 `lookup.batch.size`（默认 50）或时间 `lookup.batch.max-wait`（默认 5ms），谁先到谁触发。

**有效批大小 ≈ min(batch.size, 每子任务 QPS × max-wait)**，由 `max-wait` 而非 `batch.size` 决定。中等流量下 5ms 只攒到个位数，攒批近乎失效，建议 30~50ms。并发上「锁内只入队、锁外做 IO」，否则一个慢批次会堵住所有入队请求。

### 6.2 主备路由与降级

```
t0       整批查主源 HBase
 t0+30ms  成功            → 分发（lookup_source=hbase）
 t0+30ms  成功但没命中    → 分发空结果，不降级
 t0+500ms 仍未完成        → 记 failover、打 WARN，整批改查 Doris
 t0+560ms Doris 成功      → 分发（lookup_source=doris，cost 含主源查询耗时）
 t0+1060ms Doris 也没回   → 整批 Future 异常完成，作业失败
```

**降级粒度是整批**（主源部分成功也整批重查，换来「绝不漏查、不静默丢数据」的确定性）；**HBase 任一个 Get 失败即整批失败**（不完整的结果比走备源更危险）；**两源都失败不补空**（静默补空等于把故障伪装成「key 不存在」，宁可让作业失败）。

### 6.3 超时三层

| 层 | 参数 | 默认 | 职责 |
|---|---|---|---|
| 客户端 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | 300 / 500 / 0（跟随 Doris） | 让后端尽早中止、释放资源 |
| 业务 | `lookup.timeout` | 500 | **唯一的降级判据** |
| 算子 | `table.exec.async-lookup.timeout` | Flink 3min，建议 10s | 最终兜底，防 Future 永不完成 |

建连阶段会预热 region location，避免冷启动定位耗时撞上 500ms 超时被误判降级。交叉校验与推导见第 7 章。

### 6.4 五条边界

- **查不到 ≠ 故障**：空结果不降级，否则合法缺失会让每批都查两遍、备源压力翻倍。
- **Future 一定有终结**：类型转换失败、Reader 同步抛错、Reader 返回 null，都会让对应 Future 异常完成，绝不留「永不完成」的批。
- **惰性建连 + 自愈**：Reader 构造时不建连，首次查询才 `open()`，因此主源启动时不可用也能起作业、直接走备源（Doris 连池同理，`initializationFailTimeout=-1`，避免建池失败固定睡 1 秒）；HBase 连接被服务端回收后，`isOpened()` 检查 `AsyncConnection.isClosed()` 自动重建，撞 `ConnectionClosedException` 会换连接重试一次。
- **防打满**：超时回调 `Statement.cancel()` 真中止 SQL；`doris.query.timeout=0` 不设 JDBC queryTimeout；查询线程数 = `doris.pool.size`，队列有界，过载快速拒绝并降级。

## 7. 参数

时间类统一「毫秒整数」；`doris.query.timeout` 例外（秒，跟随 JDBC）。

### 7.1 核心

| 参数 | 默认 | 说明 |
|---|---|---|
| `lookup.primary` | `hbase` | 主源 `hbase`/`doris`，另一个自动成为备源 |
| `lookup.timeout` | `500` | 单批硬超时，超时即降级 |
| `lookup.batch.size` | `50` | 攒批条数阈值 |
| `lookup.batch.max-wait` | `5` | 攒批最大等待 |
| `lookup.stats.log-interval` | `360` | 统计日志间隔（秒），0 关闭（连线程都不建） |
| `lookup.failover.log-interval` | `10` | 降级日志最小间隔（秒），0 = 不限流 |

跨字段校验（建表期即报错）：

```
lookup.batch.max-wait      <  lookup.timeout
hbase.rpc.timeout          <  lookup.timeout
doris.connect.timeout      <  lookup.timeout
lookup.timeout             <  table.exec.async-lookup.timeout
doris.pool.min-idle        ≤  doris.pool.size
doris.pool.validation-timeout  ≤  doris.connect.timeout
```

`table.exec.async-lookup.timeout` 是 **Session 级**配置（`SET`），不是 WITH 参数，且必须写在 `CREATE TABLE` 之前。

**`lookup.timeout` 联动推导**：仅在显式设置 `lookup.timeout`、且下列参数未显式设置时生效，显式参数优先。

| 依赖项 | 自动推导 | 理由 |
|---|---|---|
| `hbase.rpc.timeout` | `80% × lookup.timeout` | 留 20% 给业务层判定降级 |
| `hbase.client.operation.timeout` | `max(500, lookup.timeout)` | 避免 HBase 客户端先于业务超时放弃 |
| `doris.connect.timeout` | `80% × lookup.timeout`，夹到 `[250, 300]ms` | 250 是 HikariCP 下限，300 防止最后一跳吃满预算 |

`lookup.timeout ≤ 250ms` 且未显式配 `doris.connect.timeout` 时，下限与 `< lookup.timeout` 无法同时满足，建表期直接报错。

### 7.2 HBase

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.zookeeper.quorum` | — | 留空则用 classpath 下 `hbase-site.xml` |
| `hbase.zookeeper.property.clientPort` / `zookeeper.znode.parent` | `2181` / `/hbase` | |
| `hbase.table-name` | DDL 表名 | 支持 `命名空间:表名` |
| `hbase.column-family` | `info` | 只支持单列族，业务列按同名列读取 |
| `hbase.rowkey.delimiter` | `\|` | 复合主键拼接分隔符 |
| `hbase.rowkey.encoding` | `string` | `string` / `typed`，**必须与写入端一致** |
| `hbase.rpc.timeout` | `300` | 必须 < `lookup.timeout`；未显式配置按 80% 推导 |
| `hbase.client.operation.timeout` | `500` | 含冷启动定位，建议 2000；未显式配置取 `max(500, lookup.timeout)` |
| `hbase.client.retries.number` | `1` | 实时链路 0~1 |

⚠️ **rowkey 编码选错是最难查的故障**：一条也查不到，但不报错、不降级，最终整表关联结果全为 NULL。建连日志会打印 `rowkeyEncoding=`，先核对它。

- `string` 模式 DECIMAL 走 `toString()`；写入端若用 `Bytes.toBytes(BigDecimal)` 请改 `typed`。复合主键逐段编码后用分隔符字节拼接，`byte[]` 段用原始字节。列值按 HBase 原生编码解码（BOOLEAN 1 字节，TINYINT/SMALLINT/INTEGER/FLOAT/DATE/TIME 4 字节，BIGINT/DOUBLE 8 字节，CHAR/VARCHAR UTF-8，DECIMAL 用 BigDecimal 编码，BINARY 原始字节）；`TIMESTAMP(p≤3)` 的 long 是**毫秒**、`p>3` 是**微秒**，rowkey 与列值同约定。

### 7.3 HBase Kerberos

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.security.authentication` | `simple` | 只接受 `simple` / `kerberos` |
| `hbase.client.keytab.file` | — | **可选**；不配则复用 Flink 全局登录 / kinit |
| `hbase.client.kerberos.principal` | — | **可选**，与 keytab 成对；不能含 `_HOST` |
| `hbase.rpc.protection` | `authentication` | `authentication` / `integrity` / `privacy`，须与服务端一致 |
| `hbase.regionserver.kerberos.principal` / `hbase.master.kerberos.principal` | — | 如 `hbase/_HOST@REALM` |
| `hbase.kerberos.krb5.conf` | — | JVM 已用 `-Djava.security.krb5.conf` 指定则不覆盖 |
| `properties.*` | — | 原样透传给 HBase `Configuration`（兼容 flink-hbase）；`properties.hbase.security.authentication` 等也参与 Kerberos 决策 |

> `properties.hbase.master.keytab.file` 是 HMaster **服务端**配置，客户端不会用它登录：只透传 + WARN。客户端凭据是 `hbase.client.keytab.file + principal`，或复用 Flink 全局登录。

### 7.4 Doris 与连接池

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | **唯一必填项**（即使主源是 HBase） |
| `doris.table-name` | DDL 表名 | 支持 `库.表` |
| `doris.username` / `doris.password` | `root` / 空 | |
| `doris.driver` | `com.mysql.cj.jdbc.Driver` | 留空按 URL 推断 |
| `doris.query.timeout` | `0` | 秒；`0` = 不设 JDBC queryTimeout；`>0` 时**不受 `lookup.timeout` 钳制** |
| `doris.pool.size` | `4` | 最大连接数 = 查询线程数；Doris 作主源时调到 8~16 |
| `doris.pool.min-idle` | `2` | 必须 ≤ `pool.size` |
| `doris.connect.timeout` | `300` | 必须 < `lookup.timeout`；未显式配置按 80% 推导并夹到 `[250,300]ms` |
| `doris.pool.idle-timeout` / `doris.pool.max-lifetime` | `600000` / `1800000` | 后者须 > 前者 |
| `doris.pool.validation-timeout` | `250` | 必须 ≤ `connect.timeout` |

> `doris.query.timeout` 只管 JDBC/服务端查询能跑多久，业务降级仍由 `lookup.timeout` 控制：到点 Future 失败并 `Statement.cancel()` 中止 SQL。

### 7.5 连续降级告警

| 参数 | 默认 | 说明 |
|---|---|---|
| `lookup.alert.threshold` | `0` | 连续降级批数阈值；`0` = 关闭（不建线程、不建生产者） |
| `lookup.alert.kafka.bootstrap-servers` | — | 阈值 > 0 时**必填** |
| `lookup.alert.kafka.topic` | — | 阈值 > 0 时**必填** |
| `lookup.alert.kafka.message` | 内置 JSON | 降级消息模板，占位符见 10.1；建表期校验 |
| `lookup.alert.recovery.message` | 内置 JSON | 恢复消息模板，支持同一套占位符；建表期校验 |
| `lookup.alert.kafka.acks` | `1` | `0` / `1` / `all`；生产者已关幂等，不与 acks 冲突 |
| `lookup.alert.cooldown` | `3` | 同表同 topic 在 N 分钟内不重复降级告警；0 = 不去重 |
| `lookup.alert.recovery` | `true` | 主源恢复后是否按 `lookup.alert.recovery.message` 推送 |
| `lookup.alert.retry.max` | `2` | 应用层重试次数（不含首次） |
| `lookup.alert.retry.backoff-ms` / `backoff-max-ms` | `500` / `5000` | 指数退避与上限 |
| `lookup.alert.spool.dir` | 空 | 最终失败后的本地 JSONL 死信目录；空 = 不落盘 |
| `lookup.alert.spool.max-bytes` | `10485760` | 单个死信文件上限，超过后丢弃并 WARN |
| `lookup.alert.kafka.client-id` | 自动 | 默认 `dual-lookup-alert-<table>-<subtask>` |
| `lookup.alert.kafka.queue-capacity` | `8` | 有界告警队列 |
| `lookup.alert.kafka.max-block-ms` / `request-timeout-ms` / `delivery-timeout-ms` | `5000` / `5000` / `15000` | Kafka 客户端超时；只在告警线程生效 |
| `lookup.alert.kafka.retries` | `2` | KafkaProducer 内部重试次数 |
| `lookup.alert.kafka.close-timeout-ms` | `5000` | 作业关闭时等待告警冲刷的上限 |
| `lookup.alert.kafka.properties.*` | — | 透传 SASL/GSSAPI/SSL/JAAS 等 Kafka 客户端参数，例如 `security.protocol`、`sasl.mechanism`、`sasl.jaas.config`、`ssl.truststore.location` |

语义与注意事项见 **10.1**。建表期校验：阈值为负、缺少 bootstrap/topic、`acks` 非法、模板非法、重试/队列/超时参数非法都会直接报错。

## 8. 内置元字段

在维表 DDL 里声明这两个约定名即可拿到每次 Lookup 的元信息；它们不查后端，TableSource 会自动从查询列中剥离。

| 字段 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | `STRING` | 数据来源 `hbase` / `doris` |
| `lookup_cost_ms` | `BIGINT` | 查询阶段耗时（毫秒，**不含惰性建连**；降级时含两段查询） |

同一批内所有命中行 `lookup_cost_ms` 相同；主源空结果时两者为 `NULL`（miss 没有维表行可承载元字段）。

## 9. Kerberos 实践

**推荐：keytab 交给 Flink 全局配置，连接器不写 keytab。** 在 `flink-conf.yaml` 配 `security.kerberos.login.keytab` / `security.kerberos.login.principal`（keytab 须存在于每个 TM 的该路径），`HadoopModule` 在 TM 启动时完成登录并按 `security.kerberos.relogin.period` 自动续期；flink-hbase 与本连接器都复用这个 UGI。

```sql
-- 维表侧只打开认证方式与保护级别
'hbase.security.authentication'         = 'kerberos',
'hbase.rpc.protection'                  = 'authentication',
'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM'
```

单表覆盖全局用户：`hbase.client.kerberos.principal` + `hbase.client.keytab.file`；沿用 flink-hbase 写法：`properties.hbase.client.keytab.file` / `properties.hbase.client.kerberos.principal`。

**凭据顺序**：① 未配 keytab → 用当前 UGI，完全无凭据则报错并提示两种配法；② keytab 配了且当前用户就是这个 principal → 复用，不重复登录；③ keytab 不可读但 UGI 已有凭据 → **降级复用 + WARN**（keytab 只需在真正读它的节点可读）；④ 都没有 → 报错。

**从 flink-hbase 迁移**：参数加 `hbase.` 前缀（`table-name`→`hbase.table-name`、`zookeeper.quorum`→`hbase.zookeeper.quorum`），补 `hbase.column-family` / `hbase.rowkey.encoding` / `doris.jdbc-url` / `PRIMARY KEY`，`properties.*` 原样保留。

**核验**：日志出现 `HBase Kerberos 复用已登录用户（Flink 全局登录或 kinit）: ro@EXAMPLE.COM`（显式模式为 `HBase Kerberos 就绪: ... loginUser=...`）→ `klist -kt <keytab>` 有该 principal → TM 与 KDC 时间差 < 5 分钟 → principal 有表 `RW` 权限。

## 10. 可观测性

每 `lookup.stats.log-interval` 秒一行，**本窗口 + 自启动累计**两个视角：

```
[dual-lookup] 表=dim_account 子任务=1/4 | 本窗口=360s 业务数据量=5400 总处理key数=5490 速率=15.3/s 主源平均批大小=45 总体平均批大小=45 | 主源(hbase)[批次=120 key数=5400 失败=2(超时=2) 平均=16.5ms 最长=500.0ms 命中率=97.8%] 备源(doris)[批次=2 key数=90 失败=0 平均=15.1ms 最长=15.1ms 命中率=100.0%] 降级=2次(90个key,1.7%) || 自启动=5400s ...
```

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `子任务=i/N` | 并行子任务编号；并行度 >1 时每个子任务各一行 | 同表多行属正常 |
| `本窗口=` / `自启动=` | 本阶段 / 自启动累计（TM 重启归零） | 判断「现在好不好」只看前者 |
| `业务数据量` / `总处理key数` | 主源 key 数 / 主源+备源实际处理 key 数 | 两者差值 = 备源重复量 |
| `速率` / 平均批大小 | 吞吐与攒批效果 | `主源平均批大小` 长期接近 1 → 攒批没生效 |
| `主源(...)` / `备源(...)` | 批次数、key 数、失败数、平均/最长耗时、命中率 | 备源 `批次>0` 在降级；备源 `失败>0` 两源同时异常 |
| `失败=` | 分类：`超时` / `连接` / `池满` / `数据库` / `其他`，只列非零项 | 分类决定处置动作 |
| `降级=N次(K个key,P%)` | 降级次数 + 波及 key 数与占主源 key 比例 | 占比持续上升 → 主源不可用 |

读法：① 判断当前是否变差只看 `本窗口=`；② **`命中率=` 是唯一能照出「维表大面积查不到」的指标**（表误删、编码写错时 `失败=0 降级=0` 一片健康）；③ `平均` 与 `最长` 一起看，长尾才是隐患；④ `最长` 逐窗口清零，漏打一行会顺延；⑤ 无样本显示 `-` 而不是 `0`。

降级 WARN 默认按 `lookup.failover.log-interval` 限流（首条必打 + 采样 + 合并计数 + 恢复补打）。不限流时，5000 条/分钟、30ms 攒批下约 **285 万行/天**，会把故障起点的上下文冲掉。

> **PushGateway 日志静音（v1.2.4）**：`DualLookupFunction` 初始化与 `open()` 时把 `org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter` 的 logger 设为 `OFF`。这是 **JVM 级副作用**，只对 Log4j2 有效，且会隐藏该 reporter 的全部日志（含真实推送失败）；集群侧能改 `log4j2.properties` 时优先集群侧处理。

### 10.1 连续降级告警（推 Kafka）

日志要人去翻，告警要系统自己喊。`lookup.alert.threshold = n` 开启后：**主源任意一批成功即清零**，连续 n 批都降级到备源时向 Kafka 推一条自定义 JSON。

```sql
'lookup.alert.threshold'               = '10',
'lookup.alert.kafka.bootstrap-servers' = 'localhost:9092',
'lookup.alert.kafka.topic'             = 'dual_lookup_alert',
'lookup.alert.cooldown'                = '3',          -- 同表同 topic 3 分钟内不重复降级告警
'lookup.alert.recovery'                = 'true',       -- 主源恢复后推 recovery
'lookup.alert.retry.max'               = '2',
'lookup.alert.spool.dir'               = '/data/flink/alert-deadletter',
-- 可选，不写就用内置模板；SQL 里用单引号包整个模板，内部一律双引号
'lookup.alert.kafka.message' = '{"level":"P1","table":"${table}","n":${consecutive},"keys":${affectedKeys},"reason":"${reason}"}',
-- 恢复消息也支持自定义 JSON（占位符同降级消息；可用 ${affectedKeys} 等，恢复时数值为 0）
'lookup.alert.recovery.message' = '{"level":"INFO","event":"recovered","table":"${table}","subtask":"${subtask}"}',
-- SASL/GSSAPI/SSL/JAAS 等参数原样透传（键名就是 Kafka 客户端键名）
'lookup.alert.kafka.properties.security.protocol' = 'SASL_PLAINTEXT',
'lookup.alert.kafka.properties.sasl.mechanism'    = 'GSSAPI',
'lookup.alert.kafka.properties.sasl.kerberos.service.name' = 'kafka',
'lookup.alert.kafka.properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true keyTab="/etc/security/keytabs/ro.keytab" principal="ro@EXAMPLE.COM";'
```

默认模板渲染（连续 10 批降级、波及 500 个 key）：

```json
{"alert":"dual-lookup-failover","table":"dim_account","primary":"hbase","standby":"doris","threshold":10,"consecutive":10,"affectedKeys":500,"reason":"TimeoutException: HBase lookup timeout after 500ms","subtask":"1/4","timestamp":1794141799631,"datetime":"2026-11-08T20:43:19.631+08:00"}
```

| 占位符 | 含义 |
|---|---|
| `${table}` / `${primary}` / `${standby}` | 维表名 / 主源 / 备源（`hbase`/`doris`） |
| `${threshold}` / `${consecutive}` | 配置阈值 / 触发时的连续批数 |
| `${affectedKeys}` | 这几批一共波及多少 key（回答「影响多少条数据」） |
| `${reason}` / `${subtask}` | 最近一次失败原因 `异常类名: message` / 子任务号如 `1/4` |
| `${timestamp}` / `${datetime}` | epoch 毫秒 / 带时区 ISO-8601 |

默认恢复模板渲染为：

```json
{"alert":"dual-lookup-recovered","table":"dim_account","primary":"hbase","standby":"doris","subtask":"1/4","timestamp":1794141859631,"datetime":"2026-11-08T20:44:19.631+08:00"}
```

可通过 `lookup.alert.recovery.message` 完全自定义；它和降级消息使用同一套占位符（恢复时 `${consecutive}`/`${affectedKeys}` 为 0，`${reason}` 为 `primary recovered`），建表期同样校验 JSON 外形和占位符白名单。

**七条容易踩的**：

1. **边沿触发 + 冷却窗**。计数「恰好等于」阈值那一批才尝试推送；同一张表、同一 topic 在 `lookup.alert.cooldown` 分钟（默认 3）内只发一条。冷却键是 `表 + topic`，在同一个 JVM 内跨子任务生效；跨 TM 仍可能各发一条，完全去重需要下游按 `table + 时间窗` 聚合。重新武装的条件仍是**主源成功一批**。
2. **恢复通知**。本轮真正触发过降级告警后，主源恢复会推一条固定 JSON：`{"alert":"dual-lookup-recovered",...}`；`lookup.alert.recovery=false` 可关闭。它不受降级冷却窗约束，但同一故障周期只发一次。
3. **并行度会放大条数**。计数是每个子任务独立的；冷却窗只能压掉同 TM 的重复。`n × 单批耗时 ≈ 容忍时长`：`lookup.timeout=500ms` 下 10 批 ≈ 5 秒、60 批 ≈ 30 秒。
4. **旁路保障（v1.4 加固）**：投递在独立守护线程 + 有界队列里做，`send()` 只入队——`KafkaProducer.send()` 在元数据不可用时最长阻塞 60 秒，直接在降级回调里调会把维表查询拖死。Kafka 不可达、队列打满、生产者创建失败、同步/异步发送失败、topic 不存在、Kerberos/SASL 认证失败，都只记录失败原因，**不影响任何一次查询，也不让作业失败**。
5. **有界重试 + 死信**。投递失败按 `lookup.alert.retry.max` 做指数退避重试；最终失败尽量写入 `lookup.alert.spool.dir` 下的 JSONL 死信，单文件受 `spool.max-bytes` 限制。队列打满时只 WARN/计数，不同步写盘，避免磁盘 IO 拖慢降级回调；死信只落盘，不自动重放，需外部补偿。
6. **指标**：`dualLookupAlertTriggered` / `Sent` / `Failed` / `Dropped` / `Retries` / `Suppressed` / `Recovery` / `Spooled` 会注册到算子 MetricGroup（仅告警启用时）。
7. **模板规则**：占位符替换成**已做 JSON 转义、不含引号**的内容，所以字符串字段自己加引号、数字字段裸写；模板必须以 `{}` 或 `[]` 包裹，占位符名须在白名单内——两者都在建表期校验，避免「告警发出去但字段永远缺失」。

> 失败日志关键字：`告警初始化失败` / `告警队列已满` / `告警生产者创建失败` / `告警发送失败` / `告警投递失败` / `告警本地死信写入失败`。
>
> 真实 Kafka 集成测试默认跳过；启用：
> `mvn test -Ddual.lookup.kafka.bootstrap=localhost:9092 -Ddual.lookup.kafka.topic=dual_lookup_alert_it`。

> 关闭方式：`lookup.alert.threshold = 0`（默认），此时不创建生产者、不起任何线程。

## 11. 调优

`并发数 = QPS × 单次延迟`，`算子吞吐上限 ≈ buffer-capacity × 并行度 ÷ 端到端延迟`（有效批大小公式见 6.1）。

2000 QPS、并行度 4：`max-wait=5ms` 时每子任务只攒到 2.5 条/批（约 800 SQL/秒）；调到 30~50ms 后降到 40~67 SQL/秒。`buffer-capacity` 默认 100，高 QPS 建议 300~500。

| 场景 | 调整 |
|---|---|
| 高吞吐 | `batch.size` 100~200，`max-wait` 5~10ms |
| 中等流量 | `max-wait` 30~50ms（默认 5ms 会让攒批失效） |
| 低延迟敏感 | `max-wait` 1~2ms，`batch.size` 20~30 |
| Doris 作主源 | `lookup.primary=doris`，`doris.pool.size` 调到 8~16 |
| 两源数据不一致 | 用 `lookup_source` 做对比校验 |

反模式（`lookup.timeout` 大于算子 timeout、`max-wait` 接近 `lookup.timeout`、客户端超时 ≥ `lookup.timeout`、`batch.size=1`）在建表期就会被拦截，见 7.1。另外备源要按**全量峰值**规划容量；多表 join 总连接数 = Σ（每表并行度 × 每表 `pool.size`），小码表降到 2~4。

## 12. 排查

| 现象 | 原因 / 方向 |
|---|---|
| 建表报错：找不到 `dual-lookup` factory | jar 没放到所有节点，或集群未重启 |
| 建表报错：必须声明 PRIMARY KEY / 未知配置项 / `doris.jdbc-url 必填` | 补 `PRIMARY KEY (...) NOT ENFORCED`；参数拼错（`properties.*` 需 1.2.0+）；备源是容错下限，即使主源 HBase 也必须配 |
| **`NoSuchMethodError: FsTracer.get(...)`** | jar 混入另一世代 Hadoop → `jar tf ... \| grep 'org/apache/hadoop/'` 自查；见第 2 章 |
| 备源空闲后首次降级报 `ConnectionClosedException` 且持续失败 | 已修复：健康检查 + 自动重建 + 重试一次；确认加载的是新 jar |
| 一直降级但 HBase 正常 | `lookup.timeout` 小于实际 P99，或 region 预热点未生效 |
| `命中率=` 偏低但 `失败=0` | rowkey 编码不一致 / 表被误删 / 备源同步延迟 |
| HBase 数值或时间错乱 | 写入编码与 DDL 类型不匹配；TIMESTAMP 注意 p≤3 毫秒、p>3 微秒 |
| Kerberos 报 `GSS initiate failed` | 先确认 Flink 全局 `security.kerberos.login.*` 生效；再查 `klist -kt`、时间差、`hbase.rpc.protection` |
| `keytab 不可读` 但 Flink 登录正常 | 新版降级复用并 WARN；要显式用 keytab 则确认每台 TM 可读 |
| 时间类型主键命中率偏低 | 新版已把 Timestamp/LocalDateTime 归一化并截断到微秒；旧包需升级 |
| 反复出现 `failed to push metrics to pushgateway` | 1.2.4+ 会静音该 logger；仍出现就确认加载新 jar、Flink 用 Log4j2 |
| `告警生产者创建失败` / `告警发送失败` | Kafka 认证、网络、参数或 topic 问题；只影响告警，作业继续 |
| `告警投递失败` | topic 不存在、权限不足或 broker 返回错误；消息未送达，查询不受影响 |
| `告警队列已满` | Kafka 长时间不可达，告警被丢弃；看累计条数与 Kafka 健康 |
| `告警已写入本地死信` | 重试耗尽后落到 `lookup.alert.spool.dir`；需要外部补偿/重放 |
| 冷却窗内没有新告警 | `lookup.alert.cooldown` 默认 3 分钟；看 `Suppressed` 指标确认 |
| TaskManager 连接数过多 | 并行度 × `doris.pool.size` 超出 Doris 承载 |

## 13. 限制与演进

- 单列族；不支持嵌套主键。
- **无熔断器**：主源持续宕机时每批仍要等满 `lookup.timeout`。
- HBase `open()` 同步阻塞，建连失败会阻塞该批；建连耗时不计入 `lookup_cost_ms` / `lookup.timeout`，但计入墙钟时间。
- 降级粒度为整批。**演进方向**：熔断器 → 主备结果对账 → 新数据源（实现 `LookupReader` 即可）→ 统计/告警指标全面接入 Flink Metric（告警指标已接入，统计日志仍以日志为主）。

## 14. 开发与测试

`mvn test` 127 个用例；真实 Kafka 集成测试默认跳过（见 10.1），其余不依赖外部服务，可进 CI。

| 类 | 职责 |
|---|---|
| `DualLookupTableSourceFactory` | SPI 入口、配置校验、schema 提取 |
| `DualLookupTableSource` | 装配 Reader 与 Function、剥离元字段 |
| `DualLookupFunction` | 攒批 + 主备路由 + 降级日志限流 + 告警计数接线 |
| `HBaseLookupReader` / `DorisLookupReader` | 单源批量查询（异步 / JDBC 模拟异步） |
| `RowDataConverter` | Flink 内部值与 Java 对象双向转换 |
| `LookupStats` | 定时统计日志 |
| `FailoverAlertNotifier` | 连续降级计数、阈值边沿触发、冷却窗、恢复通知、JSON 模板渲染（不依赖 Kafka） |
| `KafkaAlertSink` | 告警旁路：懒建生产者、有界队列、重试退避、死信、SASL/SSL 透传、失败不外溢 |
| `AlertMetrics` | 告警触发/成功/失败/丢弃/重试/抑制/恢复/死信指标 |

新增配置项要改三处：`DualLookupOptions` 加 Option 与字段、`Config.from` 赋值、`DualLookupTableSourceFactory.optionalOptions()` 登记。

## 15. 变更与升级要点

**v1.4（1.3.1 → 1.4.0）**：告警旁路增强——`lookup.alert.kafka.properties.*` 透传 SASL/GSSAPI/SSL/JAAS；`kafka-clients` 改 `provided` 消除重复依赖；应用层有界重试 + 指数退避 + 本地 JSONL 死信；Flink 告警指标；`client.id` 带 table/subtask；恢复通知（`lookup.alert.recovery.message` 可自定义 JSON）；关闭幂等；同表同 topic 冷却窗（默认 3 分钟，JVM 内跨子任务）；Kafka 队列/超时/重试可调。另修复 HBase/Doris close 清理、Doris open 失败清理、TIMESTAMP(7..9) 纳秒边界、子任务号线程名兜底。**升级动作**：确认每个节点 `$FLINK_HOME/lib` 已有 kafka-clients（通常随 flink-connector-kafka），否则告警禁用但作业正常；重打 jar 并重启。

**v1.3.1（1.3.0 → 1.3.1）**：加固连续降级告警旁路的失败隔离——告警初始化、入队、生产者创建、同步发送、异步投递、队列满全部只记录原因，不向查询/降级链路抛异常；新增每次丢队列的累计计数。修复 HBase 行转换异常导致 Future 悬挂到超时、关闭后重连重新建连接、关闭后迟到 `asyncLookup` 重新初始化、Doris 超时调度关闭竞态；TIMESTAMP 字符串小数秒不再截断到秒；DECIMAL 整数转 BigDecimal 不再经过 double 丢精度。

**v1.3（1.2.9 → 1.3.0）**：新增 `lookup.alert.*` 五个参数（默认全关，**不配就没有任何行为变化**，可直接替换 jar）；fat jar 内置 kafka-clients 3.6.2，体积 29MB → 约 34MB，与集群已有的会重复但 child-first 不影响功能；排除 zstd/lz4/snappy（生产者固定 `compression.type=none`，要开压缩需同改 pom 与 `KafkaAlertSink`）。升级动作：重打 jar → 各节点 `$FLINK_HOME/lib` 删旧包换新包 → 重启集群；判别生效看统计日志的**配置回显行**里是否出现 `alert.threshold=`（仅启用时出现）。

**v1.2（1.1.x → 1.2.9）**：从 1.1.x 直升需逐项确认——DECIMAL 单主键改 `toString()`（写入端用 `Bytes.toBytes(BigDecimal)` 的须改 `typed`）、`lookup_cost_ms` 不含建连、超时联动推导、`doris.pool.size` 8→4 与 `doris.query.timeout` 1→0、统计日志中文化（解析脚本同步）、`properties.*` 生效、TIMESTAMP 精度统一（p≤3 毫秒 / p>3 微秒）。
