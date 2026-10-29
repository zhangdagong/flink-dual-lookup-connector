# Flink Dual Lookup Connector

一个 Flink SQL 维表连接器：**HBase 主查、Doris 兜底**，超时/异常自动降级，全程攒批异步。
SQL 侧照常写 `FOR SYSTEM_TIME AS OF`，容错对业务完全透明。

```
asyncLookup(key) ─► 攒批缓冲 ─(默认：满 50 条 / 等 5ms)─► 主源 HBase 批量 Get
                                                      │ 成功        │ 超时/异常
                                                      ▼             ▼
                                                 distribute()  备源 Doris 批量 SQL
                                                      └──────┬──────┘
                                                             ▼
                                                     Lookup Join 输出
```

---

## 1. 解决什么问题

| 单源方案的软肋 | 后果 |
|---|---|
| HBase RegionServer / 网络抖动 | 单次点查 5ms → 数秒，算子背压 |
| HBase 运维重启 | 维表关联整体不可用，作业失败 |
| Doris 慢查询 | JDBC 阻塞线程、连接池打满，雪崩 |

本连接器把「主备路由 + 超时降级 + 攒批」收进一个 connector，DDL 里配好两个源即可，主备对调只改一行。

## 2. 环境与依赖

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 1.8 | 编译与运行 |
| Flink | 1.18.1 | 使用 `AsyncLookupFunction`（1.16+ 可用） |
| HBase | **2.2.7** | `hbase-shaded-client-**byo-hadoop**`，原生异步客户端 |
| Hadoop | **3.1.3** | 集群提供；`hadoop-common`/`hadoop-auth` 仅 `provided`，**不打进 jar** |
| Doris | 4.0.8 | MySQL 协议（JDBC + HikariCP），2.x/3.x 也适用 |
| 构建 | Maven 3.6+ | shade 打 fat jar（约 29MB） |

三条硬约束：

- **HBase 与 Hadoop 必须成对**。`hbase-shaded-client`（不带 `byo-hadoop`）的 manifest 写着 *with Hadoop bundled*，会把整套 Hadoop 打进 jar；曾因此出现
  `NoSuchMethodError: org.apache.hadoop.fs.FsTracer.get(...)Lorg/apache/htrace/core/Tracer;`
  （jar 内置 Hadoop 3.4.1，生产是 3.1.3，`FsTracer` 返回类型不同）。
- **htrace 由 Hadoop 提供**：HBase 客户端引用的 `org.apache.htrace.core.Tracer` 必须与 Hadoop 的 `FsTracer` 是同一个类，所以 `htrace-core4` 也不打进 jar。Hadoop 3.1.3 自带；hadoop-free 部署请手动补进 `lib/`。
- **HikariCP 4.0.3**：兼容 Java 8 的最后一代（5.x 需 Java 11+）。

确认集群 Hadoop 世代：

```bash
javap -classpath $FLINK_HOME/lib/hadoop-common-*.jar org.apache.hadoop.fs.FsTracer
# 3.1.3 期望：... org.apache.htrace.core.Tracer get(Configuration)
# 3.4+ 会是：  ... org.apache.hadoop.tracing.Tracer get(Configuration)  ← 不能混用
```

## 3. 构建与部署

```bash
mvn test                          # 95 个用例，不依赖任何外部服务
mvn clean package

# 产物体检：有输出就说明 jar 混入了本应由集群提供的类
jar tf target/flink-dual-lookup-connector-1.2.9.jar \
  | grep -E 'org/apache/hadoop/(fs|conf|security)/|org/apache/htrace/' \
  && echo '✗ jar 被污染' || echo '✓ jar 干净'

rm $FLINK_HOME/lib/flink-dual-lookup-connector-1.*.jar    # 旧包必须删，JM 与所有 TM
cp target/flink-dual-lookup-connector-1.2.9.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

> fat jar 里只允许 `org/apache/hadoop/hbase/**`，产物约 **29MB**（旧版带 Hadoop 时 65MB）。
> 上面那行 `jar tf | grep` 打印 `✓ jar 干净` 才算通过。
>
> **1.2.0 升级提醒**：`hbase.rowkey.encoding=string` 下 DECIMAL 单主键改为真正的 `toString()`。如果写入端用的是 HBase `Bytes.toBytes(BigDecimal)`，请改用 `typed` 模式；升级前先用一条已知数据核对 rowkey（建连日志会打印 `rowkeyEncoding=`）。
>
> **1.2.1 变化**：`lookup_cost_ms` 改为「查询阶段」耗时，不再包含 HBase 惰性建连/region 预热；完成回调增加 deadline 兜底；HBase 重连任务与超时调度器拆成两个线程池。
>
> **1.2.2 变化**：显式设置 `lookup.timeout` 时，未显式配置的 `hbase.rpc.timeout`（80%）、`hbase.client.operation.timeout`（≥500ms）、`doris.connect.timeout`（80% 夹 250~300ms）自动跟随；显式参数优先。
>
> **1.2.3 变化**：`doris.pool.size` 默认值从 8 调整为 4（Doris 多数场景是备源，长期空闲，不必为每张表预留 8 个连接）；Doris 作主源时显式调到 8~16。
>
> **1.2.4 变化**：`DualLookupFunction` 启动时会尝试把 Flink `PrometheusPushGatewayReporter` 的 logger 设为 `OFF`，用于无法修改集群 `log4j2.properties` 的部署；只影响该 logger，详见第 10 章。
>
> **1.2.5 变化**：`doris.query.timeout` 默认改为 `0`，不设置 JDBC queryTimeout，跟随 Doris 集群/会话默认；显式 `>0` 时也不再被 `lookup.timeout` 钳制。业务超时仍由 `lookup.timeout` 控制并触发 `Statement.cancel()`。
>
> **1.2.6 变化**：统计日志改为中文；`总处理key数` 改为主源+备源之和；主/备源都输出批次数与 key 数；首次窗口与自启动重合时只输出一份，表名整行只出现一次。
>
> **1.2.7 变化**：新增 `业务数据量` 指标 = 主源处理的 key 数；`总处理key数` 仍为主源+备源，两者差值即备源重复处理量。
>
> **1.2.8 变化**：仅修复测试断言以匹配 1.2.7 的中文日志格式（`failover=1` → `降级=1次`），运行时行为无变化。
>
> **1.2.9 变化**：统计日志增加 `子任务=i/N`，用于区分同表并行子任务各自输出的统计行。

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
    lookup_cost_ms BIGINT,        -- 元字段：查询阶段耗时（不含惰性建连，含降级）
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

上线前五条：`table.exec.async-lookup.timeout` 已设为 `lookup.timeout × 2 + 2s`；hint 的 `'table'` 用 **FROM 里的别名**；维表有 `PRIMARY KEY`；DDL 每个业务列在两源都存在且列名一致；`hbase.table-name` / `hbase.column-family` / `hbase.rowkey.encoding` 与被查表写入端一致。

## 5. LOOKUP hint

不是语法必需——`FOR SYSTEM_TIME AS OF` + 主键等值条件已决定 Lookup Join，本连接器也只提供异步实现。生产建议显式写，方便评审。

| 参数 | 作用 | 不写时 |
|---|---|---|
| `'table'` | 指定维表；**必须用别名**，写了其他参数它就必填 | 整个 hint 都可以不写 |
| `'async'` | 显式声明异步语义 | 自动异步 |
| `'capacity'` / `'timeout'` / `'output-mode'` | 只覆盖本条 join 的算子参数，优先级高于全局 SET | 取全局 SET |

- hint 的 `'timeout'` 是**算子层**兜底，与 DDL 的 `lookup.timeout`（**连接器层**降级判据）是两回事，必须大于后者。
- hint 管不到连接器行为：攒批、主备、rowkey 编码只由 WITH 参数决定。
- 多表 join 时每张维表各写一条 hint（靠 `'table'` 区分），见 demo.sql。

## 6. 运行机制

### 6.1 攒批：条数 + 时间双触发

| 触发 | 参数 | 默认 |
|---|---|---|
| 攒满条数 | `lookup.batch.size` | 50 |
| 时间兜底 | `lookup.batch.max-wait` | 5ms |

**有效批大小 ≈ min(batch.size, 每子任务 QPS × max-wait)**，由 `max-wait` 而非 `batch.size` 决定。中等流量下 5ms 只能攒到个位数，攒批近乎失效，建议 30~50ms。

并发上「锁内只入队、锁外做 IO」：批量查询绝不持锁，否则一个慢批次会把所有入队请求堵住。

### 6.2 主备路由与降级

```
t0       整批查主源 HBase
 t0+30ms  成功            → 分发（lookup_source=hbase）
 t0+30ms  成功但没命中    → 分发空结果，不降级
 t0+500ms 仍未完成        → 记 failover、打 WARN，整批改查 Doris
 t0+560ms Doris 成功      → 分发（lookup_source=doris，cost 含主源超时）
 t0+1060ms Doris 也没回   → 整批 Future 异常完成，作业失败
```

三个刻意的取舍：

- **降级粒度是整批**，主源部分成功也整批重查——换来「绝不漏查、不静默丢数据」的确定性。
- **HBase 任一个 Get 失败即整批失败**：不完整的结果比走备源更危险。
- **两源都失败不补空**：静默补空等于把故障伪装成「key 不存在」，宁可让作业失败。

### 6.3 超时三层

| 层 | 参数 | 默认 | 职责 |
|---|---|---|---|
| 客户端 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | 300 / 500 / 0（跟随 Doris 默认） | 让后端尽早中止、释放资源；Doris 的 JDBC queryTimeout 不再被 `lookup.timeout` 钳制 |
| 业务 | `lookup.timeout` | 500 | **唯一的降级判据** |
| 算子 | `table.exec.async-lookup.timeout` | Flink 3min，建议 10s | 最终兜底，防 Future 永不完成 |

四者的交叉校验见第 7 章与 `DualLookupOptions`。另外建连阶段会预热 region location，避免冷启动定位耗时撞上 500ms 超时被误判降级。

### 6.4 五条边界

- **查不到 ≠ 故障**：空结果不降级。否则合法缺失会让每批都查两遍，备源压力翻倍。
- **Future 一定有终结**：类型转换失败、Reader 同步抛错、Reader 返回 null 结果，都会让对应 Future 异常完成；绝不留「永不完成」的批把 Lookup Join 挂到算子级超时。
- **惰性建连**：Reader 构造时不建连，首次查询才 `open()`。主源启动时不可用也能起作业、直接走备源。Doris 侧连池都是惰性的（`initializationFailTimeout=-1`），避免 HikariCP 建池失败固定睡 1 秒。
- **连接自愈**：HBase 备源可能长期空闲，连接会被服务端回收。`isOpened()` 同时检查 `AsyncConnection.isClosed()`，断开时自动重建；查询中撞上 `ConnectionClosedException` 还会重建并重试一次（详见第 9 章与第 12 章）。
- **防打满**：超时回调调 `Statement.cancel()` 真中止 SQL；`doris.query.timeout=0`（默认）不设置 JDBC queryTimeout，交给 Doris 集群/会话默认，业务超时仍由 `lookup.timeout` 独立控制；查询线程数 = `doris.pool.size`，队列有界，过载快速拒绝并降级。

## 7. 参数

时间类参数统一「毫秒整数」；`doris.query.timeout` 例外（秒，跟随 JDBC）。

### 7.1 核心

| 参数 | 默认 | 说明 |
|---|---|---|
| `lookup.primary` | `hbase` | 主源 `hbase`/`doris`，另一个自动成为备源 |
| `lookup.timeout` | `500` | 单批硬超时，超时即降级 |
| `lookup.batch.size` | `50` | 攒批条数阈值 |
| `lookup.batch.max-wait` | `5` | 攒批最大等待 |
| `lookup.stats.log-interval` | `360` | 统计日志间隔（秒），0 关闭（连线程都不建） |
| `lookup.failover.log-interval` | `10` | 降级日志最小间隔（秒），0 = 不限流 |

跨字段校验（建表期即报错，不用等运行期）：

```
lookup.batch.max-wait      <  lookup.timeout          # 攒批等待不能吃掉超时预算
hbase.rpc.timeout          <  lookup.timeout          # 后端先释放，业务层再判降级
doris.connect.timeout      <  lookup.timeout          # 备源之后无处可降，必须让池先失败
lookup.timeout             <  table.exec.async-lookup.timeout   # 算子层兜底必须留出降级时间
doris.pool.min-idle        ≤  doris.pool.size
doris.pool.validation-timeout  ≤  doris.connect.timeout
```

`table.exec.async-lookup.timeout` 是 **Session 级**配置（`SET`），不是 WITH 参数。Factory 会从 `context.getConfiguration()` 读取它做校验，所以 `SET` 必须在 `CREATE TABLE` 之前生效；写进 WITH 会被当成未知配置直接报错。

**`lookup.timeout` 联动推导**：只有显式设置 `lookup.timeout`、且下列参数未显式设置时才生效；显式写了的参数优先。

| 依赖项 | 自动推导 | 理由 |
|---|---|---|
| `hbase.rpc.timeout` | `80% × lookup.timeout` | 留 20% 给业务层判定降级 |
| `hbase.client.operation.timeout` | `max(500, lookup.timeout)` | 避免 HBase 客户端先于业务超时放弃 |
| `doris.connect.timeout` | `80% × lookup.timeout`，夹到 `[250, 300]ms` | 250 是 HikariCP 下限；300 上限避免最后一跳把预算全花在等连接上 |

> 80% 对 HBase RPC 合理；对 Doris connect 则偏激进（等连接占满 80% 后 SQL 只剩 20%），所以上限压到 300ms。
>
> `lookup.timeout ≤ 250ms` 且未显式配置 `doris.connect.timeout` 时，推导值会被 250ms 下限顶住、无法满足 `< lookup.timeout`，建表期会直接报错并提示调大 `lookup.timeout`。

### 7.2 HBase

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.zookeeper.quorum` | — | 留空则用 classpath 下 `hbase-site.xml` |
| `hbase.zookeeper.property.clientPort` | `2181` | |
| `zookeeper.znode.parent` | `/hbase` | |
| `hbase.table-name` | DDL 表名 | 支持 `命名空间:表名` |
| `hbase.column-family` | `info` | 只支持单列族，所有业务列按同名列读取 |
| `hbase.rowkey.delimiter` | `\|` | 复合主键拼接分隔符 |
| `hbase.rowkey.encoding` | `string` | `string` / `typed`，**必须与写入端一致**；`string` 下 DECIMAL 也走 `toString()`，若写入端用 HBase `Bytes.toBytes(BigDecimal)` 请改用 `typed` |
| `hbase.rpc.timeout` | `300` | 必须 < `lookup.timeout`；`lookup.timeout` 显式设置且本项未写时自动取 80% |
| `hbase.client.operation.timeout` | `500` | 含冷启动定位，建议给足 2000；本项未写时自动取 `max(500, lookup.timeout)` |
| `hbase.client.retries.number` | `1` | 实时链路 0~1 |

⚠️ **rowkey 编码选错是最难查的故障**：一条也查不到，但「查不到」不报错、不降级，最终整表关联结果全为 NULL。建连日志会打印 `rowkeyEncoding=`，先核对它，再对照写入端。

列值解码约定（写入端必须一致）：`BOOLEAN`→1 字节；`TINYINT/SMALLINT/INTEGER`→4 字节 int；`BIGINT`→8 字节 long；`FLOAT/DOUBLE`→4/8 字节；`DECIMAL`→HBase BigDecimal 编码；`DATE/TIME`→4 字节 int；`TIMESTAMP`→8 字节 long；`CHAR/VARCHAR`→UTF-8；`BINARY`→原始字节。细节见 `HBaseLookupReader.decode`。

> **TIMESTAMP 精度约定**：`TIMESTAMP(p ≤ 3)` 的 8 字节 long 是**毫秒**，`p > 3` 是**微秒**；`typed` rowkey 与列值使用同一约定（否则 TS(6) 主键会出现「rowkey 能查到但时间错 1000 倍」或「时间对但查不到」）。
>
> **`string` 模式的复合主键**：逐段编码后再用分隔符的字节连接；`byte[]` 段直接用原始字节，不再经过 `toString()`。

### 7.3 HBase Kerberos

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.security.authentication` | `simple` | 只接受 `simple` / `kerberos` |
| `hbase.client.keytab.file` | — | **可选**。不配时复用 Flink 全局登录（`security.kerberos.login.keytab`）或 kinit 凭据；也可用 `properties.hbase.client.keytab.file` |
| `hbase.client.kerberos.principal` | — | **可选**，与 keytab 成对；具体账号，**不能含 `_HOST`**；也可用 `properties.hbase.client.kerberos.principal` |
| `hbase.rpc.protection` | `authentication` | `authentication` / `integrity` / `privacy`，须与服务端一致 |
| `hbase.regionserver.kerberos.principal` | — | 如 `hbase/_HOST@REALM`，集群没改名可不配 |
| `hbase.master.kerberos.principal` | — | 同上 |
| `hbase.kerberos.krb5.conf` | — | JVM 已用 `-Djava.security.krb5.conf` 指定则不覆盖 |
| `properties.*` | — | **原样透传给 HBase `Configuration`**（兼容 flink-hbase）；其中 `properties.hbase.security.authentication`、`properties.hbase.rpc.protection`、`properties.hbase.client.keytab.*` 也参与连接器 Kerberos 决策，不只是透传 |

> `properties.hbase.master.keytab.file` 是 HMaster 服务端配置，客户端不会用它登录：连接器只透传并打 WARN。客户端凭据是 `hbase.client.keytab.file` + `hbase.client.kerberos.principal`（或对应的 `properties.*`），或者直接复用 Flink 全局登录。

### 7.4 Doris 与连接池

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | **唯一必填项** |
| `doris.table-name` | DDL 表名 | 支持 `库.表` |
| `doris.username` / `doris.password` | `root` / 空 | |
| `doris.driver` | `com.mysql.cj.jdbc.Driver` | 留空按 URL 推断 |
| `doris.query.timeout` | `0` | 秒；`0` = 不设置 JDBC queryTimeout，跟随 Doris 集群/会话默认；`>0` 显式设置且**不受 `lookup.timeout` 钳制** |
| `doris.pool.size` | `4` | 最大连接数 = 查询线程数；Doris 作主源时建议显式调到 8~16 |
| `doris.pool.min-idle` | `2` | 必须 ≤ `pool.size` |
| `doris.connect.timeout` | `300` | 必须 < `lookup.timeout`（HikariCP 下限 250）；本项未写时自动取 80% 并夹到 `[250,300]ms` |
| `doris.pool.idle-timeout` | `600000` | |
| `doris.pool.max-lifetime` | `1800000` | 须 > idle-timeout |
| `doris.pool.validation-timeout` | `250` | 必须 ≤ `connect.timeout` |

> `doris.query.timeout` 只管 JDBC/服务端查询能跑多久，**不再被 `lookup.timeout` 钳制**。业务层的 `lookup.timeout` 仍然是降级判据：到点 Future 失败，并调用 `Statement.cancel()` 主动中止 SQL。`0` 表示完全交给 Doris 的 `query_timeout` 等默认机制。

## 8. 内置元字段

在维表 DDL 里声明这两个约定名，即可拿到每次 Lookup 的元信息；它们不查后端，TableSource 会自动从查询列中剥离。

| 字段 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | `STRING` | 数据来源 `hbase` / `doris` |
| `lookup_cost_ms` | `BIGINT` | 查询阶段耗时（毫秒，**不含惰性建连**；降级时含主源查询 + 备源查询） |

细节：同一批内所有命中行拿到相同的 `lookup_cost_ms`；降级后它是「主源查询耗时 + 备源查询耗时」之和，不再把 HBase 首次建连/region 预热算进去；主源空结果时两者为 `NULL`（Lookup Join 的 miss 没有维表行可承载元字段）。

## 9. Kerberos（与 flink-hbase 对齐）

**推荐做法：keytab 交给 Flink 全局配置，连接器不写 keytab。**

Flink 的 `HadoopModule` 在 TaskManager 启动时就会用 `security.kerberos.login.keytab` /
`security.kerberos.login.principal` 完成登录，并按 `security.kerberos.relogin.period`（默认 1 分钟）
自动续期；**flink-hbase 自己并不读 keytab**，直接用这个已登录的 UGI。本连接器行为一致：

```yaml
# flink-conf.yaml —— 一次配置，所有 TM 生效（keytab 必须存在于每个 TM 的这个路径）
security.kerberos.login.keytab: /home/flink/streampark/kerberos/trdw.keytab
security.kerberos.login.principal: ro@EXAMPLE.COM
```

```sql
-- 维表 DDL 只打开 kerberos 与保护级别即可
'hbase.security.authentication'         = 'kerberos',
'hbase.rpc.protection'                  = 'authentication',      -- 须与服务端一致
'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM'
```

需要**单张表覆盖** Flink 全局登录用户时，再显式配 keytab：

```sql
'hbase.client.kerberos.principal' = 'ro@EXAMPLE.COM',   -- 与 keytab 成对，不能含 _HOST
'hbase.client.keytab.file'        = '/path/to/ro.keytab'
```

**与 flink-hbase 的兼容写法**：任何 `properties.xxx` 都会去掉前缀后写进 HBase `Configuration`
（与 flink-hbase 的 `HBaseConnectorOptionsUtil.getHBaseConfiguration` 完全一致），
所以从 flink-hbase 迁过来的 DDL 可以原样使用：

```sql
'properties.hbase.client.keytab.file'        = '/path/to/ro.keytab',
'properties.hbase.client.kerberos.principal' = 'ro@EXAMPLE.COM'
```

显式参数（`hbase.client.*`）与 `properties.*` 二选一即可；同时出现时显式参数优先。

### 9.1 从 flink-hbase 迁移的 DDL（含 `properties.*`）

当前版本的工厂用 `helper.validateExcept("properties.")` 放行 `properties.*`，并在
`DualLookupOptions.Config.from` 里把它们当作 Kerberos 决策的输入（不只是原样透传）：

```sql
'connector' = 'dual-lookup',
'hbase.table-name'       = 'db.table_name',   -- 原 'table-name'；命名空间请写 'db:table_name'
'hbase.zookeeper.quorum' = 'bigdatadev01.hzbank.com,bigdatadev02.hzbank.com',
                                              -- 原 'zookeeper.quorum'（去掉端口）
'hbase.zookeeper.property.clientPort' = '2182',  -- 原 quorum 里的 :2182
'zookeeper.znode.parent' = '/hbase',          -- 原样
'hbase.column-family'    = 'info',             -- 按实际列族
'hbase.rowkey.encoding'  = 'string',           -- 必须与写入端一致

-- Kerberos：下面这些 properties.* 都可以原样保留
'properties.hbase.security.authentication'         = 'kerberos',
'properties.hbase.rpc.protection'                  = 'authentication',  -- 须与服务端一致
'properties.hbase.master.kerberos.principal'       = 'hbase/_HOST@HADOOP.COM',
'properties.hbase.regionserver.kerberos.principal' = 'hbase/_HOST@HADOOP.COM',
-- 旧 DDL 里的这一行可以保留，但它是 HMaster 服务端配置，客户端不会用它登录：
-- 'properties.hbase.master.keytab.file' = '/home/flink/streampark/kerberos/rtdw.keytab',
-- 只有需要「单表覆盖 Flink 全局登录用户」时才配客户端凭据（两者成对，principal 不能含 _HOST）：
-- 'properties.hbase.client.keytab.file'        = '/home/flink/streampark/kerberos/rtdw.keytab',
-- 'properties.hbase.client.kerberos.principal' = 'rtdw@HADOOP.COM'
```

> ZK 端口按 HBase 标准拆到 `hbase.zookeeper.property.clientPort`；如果你们集群要求 quorum 自带端口，沿用原写法即可，但先确认 HBase 不会重复拼接。

> **`properties.hbase.master.keytab.file` 不是客户端凭据。** 它是 HMaster 服务端用来登录
> `hbase/_HOST` 的配置；HBase 客户端的 `AuthUtil` 只读 `hbase.client.keytab.file`。
> 正确做法是：keytab 交给 Flink 全局 `security.kerberos.login.*`（推荐），
> 或在本表写 `hbase.client.keytab.file + hbase.client.kerberos.principal`。
> 只写 `properties.hbase.master.keytab.file` 不会报错：连接器会复用 Flink 已登录的 UGI，
> 并在日志里打一条 WARN 说明该参数是服务端配置、客户端不会使用。

迁移检查清单：

1. `'table-name'` → `'hbase.table-name'`；命名空间写成 `ns:table`，不是 `ns.table`。
2. `'zookeeper.quorum'` → `'hbase.zookeeper.quorum'`；如果端口是 2182，建议拆到 `'hbase.zookeeper.property.clientPort' = '2182'`。
3. 补 `'hbase.column-family'`（默认 `info`）与 `'hbase.rowkey.encoding'`（必须与写入端一致）。
4. 补 `'doris.jdbc-url'`（本连接器唯一必填项）与 `PRIMARY KEY (...) NOT ENFORCED`。
5. `properties.*` 可以原样保留；`hbase.master.keytab.file` 只透传，不作为客户端凭据。

**为什么以前会报「不可使用 properties.*」？** 旧包或旧版本可能仍用 `helper.validate()` 校验，
`properties.*` 会被判为未知配置。当前源码已改为 `validateExcept("properties.")`；
如果仍然报错，先确认 JM 与所有 TM 上加载的是新构建的 jar（旧包必须删掉并重启集群）。

凭据选择顺序：

1. **未配 keytab** → 使用当前 UGI（Flink 全局登录 / kinit）；没有任何 Kerberos 凭据时，报错并给出上面两种配置方式；
2. **配了 keytab 且当前用户就是该 principal** → 复用，不重复登录；
3. **配了 keytab 但文件不可读、而当前 UGI 已有凭据** → **降级为复用，只打 WARN**（本次生产问题的修复点：不会因为某张表多写了一个只在部分节点存在的路径而让作业起不来）；
4. 无凭据且 keytab 读不到 → 报错，消息里同时给出两种来源。

三个已处理的坑：

1. **只配 `hbase.*` 不配 UGI，自动登录会被静默跳过**。`AuthUtil.loginClient` 的门槛是 `UGI.isSecurityEnabled()`，UGI 不看 `hbase.security.authentication`；`loginUserFromKeytab` 在 security 关闭时还会**静默 return**。需要自己登录时连接器会设 `hadoop.security.authentication=kerberos` 并 `UGI.setConfiguration`；复用 Flink 登录时**不会**覆盖 Flink 的 UGI 配置（避免破坏它的 auth_to_local / group mapping）。
2. **principal 的 key 名有两套**。HBase 2.2.x 实际读 `hbase.client.keytab.principal`，文档写 `hbase.client.kerberos.principal`；显式 keytab 模式下连接器两个 key 都写（`KerberosConfigTest` 守着）。
3. **认证方式拼错不会报错**，等于按 simple 跑。现在建表期只接受 `simple`/`kerberos`。

上线核验：日志出现 `HBase Kerberos 复用已登录用户（Flink 全局登录或 kinit）: ro@EXAMPLE.COM`
（或显式 keytab 模式下的 `HBase Kerberos 就绪: ... loginUser=...`）→ `klist -kt <keytab>` 里确有该 principal
→ TM 与 KDC 时间差 < 5 分钟 → principal 有表的 `RW` 权限。

## 10. 可观测性

每 `lookup.stats.log-interval` 秒打一行，**本窗口 + 自启动累计**两个视角：

```
[dual-lookup] 表=dim_account 子任务=1/4 | 本窗口=360s 业务数据量=5400 总处理key数=5490 速率=15.3/s 主源平均批大小=45 总体平均批大小=45 | 主源(hbase)[批次=120 key数=5400 失败=2(超时=2) 平均=16.5ms 最长=500.0ms 命中率=97.8%] 备源(doris)[批次=2 key数=90 失败=0 平均=15.1ms 最长=15.1ms 命中率=100.0%] 降级=2次(90个key,1.7%) || 自启动=5400s ...
```

> 表名整行只输出一次；首次打点时「本窗口」和「自启动」覆盖同一段，会合并成 `本窗口=自启动=360s`，避免前后两段完全重复。

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `表=` | 维表名，整行只出现一次 | — |
| `子任务=` | 并行子任务编号 `i/N`；并行度 >1 时每个子任务各有一行统计 | 两行同表但子任务不同，属正常现象 |
| `本窗口=` / `自启动=` | 本阶段 / 自启动累计（TM 重启归零） | 判断「现在好不好」只看前者 |
| `业务数据量` | **主源处理的 key 数**（业务 key，不设备源重复） | 与上游 QPS 对比，判断处理量是否正常 |
| `总处理key数` | **主源 + 备源实际处理的 key 数**；降级批次会被两源各计一次 | 与 `业务数据量` 的差值 = 备源重复处理的量；突增说明降级变多 |
| `速率` | 总处理key数 / 窗口秒数 | 明显低于上游 QPS → 背压/攒批问题 |
| `主源平均批大小` | 主源 key 数 / 主源批次数 | 长期接近 1 → 攒批没生效 |
| `总体平均批大小` | 总处理key数 / 两源总批次数 | 与主源平均差距大 → 降级较频繁 |
| `主源(...)[...]` / `备源(...)[...]` | 批次数、key 数、失败数、平均/最长耗时、命中率 | 备源 `批次>0` 说明在降级；备源 `失败>0` 说明两源同时异常 |
| `失败=` | 失败原因分类：`超时` / `连接` / `池满` / `数据库` / `其他`，只列非零项 | 分类决定处置动作 |
| `降级=N次(K个key,P%)` | 降级次数 + 波及 key 数与占主源 key 的比例 | 占比持续上升 → 主源不可用 |

五个读法：① 判断当前是否变差只看 `本窗口=`（累计会被历史稀释）；② **`命中率=` 是唯一能照出「维表大面积查不到」的指标**（表误删、编码写错时 `失败=0 降级=0` 一片健康）；③ `平均` 与 `最长` 一起看，长尾才是隐患；④ `最长` 逐窗口清零，漏打一行会顺延到下一窗口；⑤ 无样本显示 `-` 而不是 `0`。

降级 WARN 默认按 `lookup.failover.log-interval` 限流（首条必打 + 采样 + 合并计数 + 恢复补打）。不限流时，5000 条/分钟、30ms 攒批下约 **285 万行/天**，会把故障起点的上下文冲掉。需要逐批留痕设为 `0`。

> **PushGateway 日志静音（v1.2.4）**：`DualLookupFunction` 在类初始化与 `open()` 时会尝试把
> `org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter` 这个 logger 设为 `OFF`，
> 避免 `failed to push metrics to pushgateway with jobname ...` 刷屏。
> 它只影响该 logger，不影响其他 Flink / 连接器日志；但这是 **JVM 级副作用**（同一 TM 上所有作业共享），
> 只对 Log4j2 有效，并且会把该 reporter 的其他日志（包括真实推送失败）一起隐藏。
> 它关的是该 logger 的**全部**日志，不是只过滤 `failed to push...` 这一条；想恢复需删掉连接器里的静音代码，
> 只在 `log4j2.properties` 调级别会被 `open()` 重新覆盖。
> 如果集群侧能改日志配置，应优先在集群侧处理。

## 11. 调优

```
并发数       = QPS × 单次延迟
有效批大小   = min(batch.size, 每子任务QPS × batch.max-wait)
算子吞吐上限 ≈ buffer-capacity × 并行度 ÷ 端到端延迟
```

2000 QPS、并行度 4 的账：`max-wait=5ms` 时每子任务只能攒到 2.5 条/批（约 800 SQL/秒）；调到 30~50ms 后降到 40~67 SQL/秒。另注意 `buffer-capacity` 默认 100，高 QPS 建议 300~500。

| 场景 | 调整 |
|---|---|
| 高吞吐 | `batch.size` 100~200，`max-wait` 5~10ms |
| 中等流量 | `max-wait` 30~50ms（默认 5ms 会让攒批失效） |
| 低延迟敏感 | `max-wait` 1~2ms，`batch.size` 20~30 |
| Doris 作主源 | `lookup.primary=doris`，`doris.pool.size` 调到 8~16（默认 4 面向备源） |
| 两源数据不一致 | 用 `lookup_source` 做对比校验 |

反模式：`lookup.timeout` 大于算子 timeout、`max-wait` 接近 `lookup.timeout`、`hbase.rpc.timeout`/`doris.connect.timeout` ≥ `lookup.timeout`、`batch.size=1`——**前四类建表期就会报错**。另外两个易错点：备源要按**全量峰值**规划容量（主源挂掉时它独扛全量）；多表 join 的总连接数 = Σ（每表并行度 × 每表 `pool.size`），小码表降到 2~4。

## 12. 排查

| 现象 | 原因 / 方向 |
|---|---|
| 建表报找不到 `dual-lookup` factory | jar 没放到所有节点，或集群未重启 |
| 建表报必须声明 PRIMARY KEY | DDL 缺 `PRIMARY KEY (...) NOT ENFORCED` |
| 建表报未知配置项 | 参数拼错，或新增项没在 Factory 登记 |
| 建表报 `doris.jdbc-url 必填` | 本连接器必须有 Doris 备源；即使 `lookup.primary=hbase`，也要配 `doris.jdbc-url` |
| **建表报「不可使用 `properties.*`」** | 运行的是旧包（旧版用 `helper.validate()`）。当前源码用 `validateExcept("properties.")`；删掉旧 jar、重新构建并重启 JM/TM |
| hint 不匹配 | `'table'` 写成了表名而不是 FROM 别名 |
| **`NoSuchMethodError: FsTracer.get(...)Lorg/apache/htrace/core/Tracer;`** | jar 混入另一世代 Hadoop → `jar tf target/*.jar \| grep -E 'org/apache/hadoop/(fs\|conf\|security)/'` 自查；修复见第 2 章 |
| 一直降级但 HBase 看起来正常 | `lookup.timeout` 小于实际 P99，或 region 预热点未生效 |
| **备源 HBase 空闲很久后首次降级报 `ConnectionClosedException: Connection closed`，且之后一直失败** | 服务端回收了空闲连接；旧实现只看 `opened` 布尔值不会重建。**已修复**：健康检查落到 `AsyncConnection.isClosed()`，断开自动重建，查询中撞上还会重试一次 |
| `命中率=` 明显偏低但 `失败=0` | rowkey 编码与写入端不一致 / 表被误删 / 备源同步延迟 |
| HBase 数值或时间全错乱 | 写入编码与 DDL 类型不匹配（见 7.2） |
| Kerberos 报 `GSS initiate failed` / `No valid credentials` | 未配 keytab 时先确认 Flink 全局 `security.kerberos.login.*` 是否生效；再 `klist -kt`、时间差、`hbase.rpc.protection` |
| 只写了 `properties.hbase.security.authentication=kerberos`，却仍按 simple 跑 | 旧版只读显式参数，会跳过 `applyKerberos()`；1.2.0+ 已让 `properties.*` 参与解析，确认加载的是新 jar |
| 日志报 `hbase.client.keytab.file 不可读`，但 Flink 全局登录是好的 | 新版会降级复用已有 UGI 并打 WARN；若一定要用该 keytab，确认每台 TM 上路径可读 |
| 配了 `properties.hbase.master.keytab.file` 却认证不上 | 这是 HMaster 服务端配置，客户端不读；改用 Flink 全局登录，或 `hbase.client.keytab.file + hbase.client.kerberos.principal`。日志会有对应 WARN |
| TIMESTAMP(6) 主键「能查到但时间差 1000 倍」或「时间对但查不到」 | 写入端与连接器的精度约定不一致：`p ≤ 3` 用毫秒、`p > 3` 用微秒；`typed` rowkey 与列值必须同一套 |
| 时间类型主键（DATE/TIME/TIMESTAMP）`命中率=` 偏低但 `失败=0` | 旧版用 `toString()` 拼反查键，`java.sql.Timestamp` 与 `LocalDateTime` 对不上；新版已归一化到 ISO 并截断到微秒 |
| TM 日志反复出现 `failed to push metrics to pushgateway with jobname ...` | 1.2.4+ 会在函数初始化时把 `PrometheusPushGatewayReporter` logger 设为 `OFF`；仍出现就确认加载的是 1.2.4+、Flink 用 Log4j2，集群侧也可在 `log4j2.properties` 里设 OFF |
| 作业取消失败 / 卡住 | 确认 `close()` 会补空攒批缓冲（已有回归用例） |
| TaskManager 连接数过多 | 并行度 × `doris.pool.size` 超出 Doris 承载 |

## 13. 限制与演进

- 单列族；不支持嵌套主键。
- **无熔断器**：主源持续宕机时每批仍要等满 `lookup.timeout`，吞吐受超时限制。
- HBase `open()` 是同步的，建连失败会阻塞该批，依赖 ZK/客户端超时兜底；建连耗时不计入 `lookup_cost_ms` 与 `lookup.timeout`，但会计入该批的实际墙钟时间。
- 降级粒度为整批。

演进方向：熔断器 → 主备结果对账 → 新数据源（实现 `LookupReader` 即可）→ 接 Flink Metric（`FunctionContext.getMetricGroup()` 可用，无需改 `RichAsyncFunction`）。

## 14. 开发与测试

```bash
mvn test          # 95 个用例；无外部依赖，可进 CI
```

| 类 | 职责 |
|---|---|
| `DualLookupTableSourceFactory` | SPI 入口、配置校验、schema 提取 |
| `DualLookupTableSource` | 装配 Reader 与 Function、剥离元字段 |
| `DualLookupFunction` | 攒批 + 主备路由 + 降级日志限流 |
| `HBaseLookupReader` / `DorisLookupReader` | 单源批量查询（异步 / JDBC 模拟异步） |
| `RowDataConverter` | Flink 内部值与 Java 对象双向转换 |
| `LookupStats` | 定时统计日志 |

新增配置项要改三处：`DualLookupOptions` 加 Option 与字段、`Config.from` 赋值、`DualLookupTableSourceFactory.optionalOptions()` 登记（漏了第三步 DDL 会报未知配置）。

## 15. 变更记录（2026-10-28）

本轮针对生产反馈做了三组修复；除 Kerberos 参数变为可选外，不改变 DDL 语义。

**① HBase 连接自愈**（`ConnectionClosedException: Connection closed`）

- 现象：`lookup.primary=doris` 时降级到 HBase 报连接已关闭，且此后每批都失败、看起来"没切备源"。
- 原因：HBase 备源长期空闲，连接被服务端回收；旧实现只判断 `opened` 布尔值，不会重建。
- 修复：`isOpened()` 检查 `AsyncConnection.isClosed()`；断开时自动重建；查询过程中撞上断连，会替换"当时那条坏连接"并重试一次（只重试一次，且只针对连接类异常）。

**② Kerberos 与 flink-hbase 对齐**

- flink-hbase 没有 keytab 参数：它用 `properties.*` 把 HBase 客户端配置原样透传，登录交给 HBase `AuthUtil` 复用 Flink 已登录的 UGI。
- 本连接器新增 `properties.*` 透传；`hbase.client.keytab.file` / `principal` 改为**可选**（不配则复用 Flink 全局登录 / kinit）。
- keytab 只存在于部分 TM 时：若当前 UGI 已有凭据，**降级为复用并打 WARN**，同时清掉 `hbase.client.keytab.*`，避免 HBase 再去读那个不存在的文件。
- 只有在"既无可用凭据、keytab 也不可读"时才报错，并提示两种配置来源。

**③ 打包**

- 排除 HBase 父 POM 带入的 `protobuf-java 2.5.0`（HBase 实际用 shaded 副本）与 MySQL 的 protobuf（仅 X DevAPI 需要）。
- 排除各依赖的 `META-INF/MANIFEST.MF` 并加 `ManifestResourceTransformer`，消除 shade 的「overlapping resource」告警。

验证（v1.1.0）：`mvn -o clean package` 通过，**69 个测试全绿**。

## 16. 变更记录（2026-10-29，v1.2.0）

本轮以「异步链路不悬挂 + 兼容 flink-hbase 的 `properties.*` 写法」为目标，新增 17 个回归用例（当前共 86 个）。

**① 异步链路异常安全**

- `distribute` 每个 key 单独 try/catch：某一行类型转换失败（DECIMAL 溢出、时间戳格式错误）只让该 key 异常完成，不再中断循环、让后续 Future 悬挂。
- `call` 包住 `batchLookupAsync`：Reader 同步抛错（关闭竞态、参数异常）翻译成异常完成的 Future，不再冒泡出 `flush()`。
- `doBatchLookup` / `flush` 最外层兜底，`failAll` 保证整批 Future 一定有终结。
- Reader 返回 `null` 结果 Map 时整批失败，不再静默当成「全部查不到」。

**② HBase TIMESTAMP 精度一致**

- `typed` rowkey 的 TIMESTAMP 与列值解码使用同一约定：precision ≤ 3 用毫秒，> 3 用微秒。
- `RowDataConverter.toTimestamp` 对任意 `Number` 都按精度解释，不再只特判 `Long`。

**③ string rowkey 一致性**

- DECIMAL 单主键改为真正的 `toString()`；复合主键逐段使用同一套编码（`byte[]` 段直接用原始字节）。
- 若写入端使用 HBase `Bytes.toBytes(BigDecimal)`，请改用 `typed` 模式（两者对 DECIMAL 等价）。

**④ Kerberos `properties.*` 真正生效**

- `properties.hbase.security.authentication` / `properties.hbase.rpc.protection` 参与 `Config` 解析，不再因显式参数默认 `simple` 而跳过 `applyKerberos()`。
- `properties.hbase.client.keytab.file` / principal 可以作为客户端凭据，并同样受「成对配置」「principal 不能含 `_HOST`」校验。
- `hbase.master.keytab.file` 明确为 HMaster 服务端配置：只透传 + WARN，绝不当客户端凭据。
- 工厂把 `context.getConfiguration()` 传给 `Config`，`table.exec.async-lookup.timeout` 的交叉校验不再只拿 Flink 默认 3 分钟比较。

**⑤ 其它**

- Doris 时间类型主键反查键归一化：`java.sql.Timestamp` 与 `LocalDateTime`、`java.sql.Date` 与 `LocalDate`、`java.sql.Time` 与 `LocalTime` 互认，统一截断到微秒。
- 统计日志使用 `Locale.ROOT`，小数分隔符不受 JVM 默认 locale 影响。
- 吞吐 / 平均批大小 / 命中率分母排除空主键（Reader 本来就不会查询它们）。

## 17. 变更记录（2026-10-29，v1.2.1）

**① 超时判定与耗时口径**

- `lookup_cost_ms` 改为只统计**查询阶段**：从 Reader 真正发起请求时开始，不再包含 HBase 惰性建连/region 预热。这样它和 `lookup.timeout` 的判据口径一致，不会再出现「显示 915ms 但源是 hbase」的误导。
- 完成回调增加 deadline 兜底：即使超时调度器被重连/GC 拖延，只要查询阶段超过 `lookup.timeout`，也按超时处理并降级。
- HBase 的断连重连从超时调度器拆到独立线程池 `dual-lookup-hbase-reconnect`，避免同步的 `open()` 阻塞同一时刻所有批次的超时任务。

**② 测试**

- 新增 1 个用例（当前共 87 个）：主源查询超过 `lookup.timeout` 后即使成功返回，也必须降级。

## 18. 变更记录（2026-10-29，v1.2.2）

**① `lookup.timeout` 联动推导**

- 显式设置 `lookup.timeout` 时，未显式配置的依赖项自动跟随，避免「只改一个参数建表失败」：
  - `hbase.rpc.timeout = 80% × lookup.timeout`；
  - `hbase.client.operation.timeout = max(500, lookup.timeout)`；
  - `doris.connect.timeout = 80% × lookup.timeout`，夹到 `[250, 300]ms`。
- 显式写了的参数优先，不会被推导覆盖。
- 80% 对 HBase RPC 合理（留 20% 给降级判定）；Doris connect 是最后一跳，等待连接不宜占满预算，因此上限压到 300ms。
- `lookup.timeout ≤ 250ms` 且未显式配置 `doris.connect.timeout` 时，建表期明确报错（HikariCP 下限 250ms 与 `< lookup.timeout` 无法同时满足）。

**② 测试**

- 新增 4 个用例（当前共 91 个）：联动推导、显式参数优先、过小 timeout 拒绝、Doris connect 低于 HikariCP 下限拒绝。

## 19. 变更记录（2026-10-29，v1.2.3）

- `doris.pool.size` 默认值从 **8 调整为 4**：Doris 在多数部署里是备源、长期空闲，不必为每张维表预留 8 个连接；4 个连接 + 有界队列仍能满足基本并发。
- `doris.pool.min-idle` 默认仍为 2，满足 `min-idle ≤ pool.size`。
- 显式配置了 `doris.pool.size` 的 DDL 不受影响；Doris 作主源时仍建议显式调到 8~16。

## 20. 变更记录（2026-10-29，v1.2.4）

- `DualLookupFunction` 在类初始化和 `open()` 时通过反射调用 Log4j2 `Configurator.setLevel`，把 `org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter` 设为 `OFF`。
- 目的：在无法修改集群 `flink-conf.yaml` / `log4j2.properties` 的部署里，压掉 `failed to push metrics to pushgateway with jobname ...`。
- 影响范围：只影响该 logger，不影响其他 Flink / connector 日志；但它是 JVM 级配置，同一 TM 上所有作业共享；只对 Log4j2 有效；会把该 reporter 的真实推送失败也一起隐藏。
- 如果集群侧可改日志配置，仍应优先在 `log4j2.properties` 中设置 logger level，而不是依赖连接器做进程级副作用。

## 21. 变更记录（2026-10-29，v1.2.5）

- `doris.query.timeout` 默认值从 `1` 改为 **`0`**：不调用 `Statement.setQueryTimeout`，让 Doris 集群/会话的 `query_timeout` 等默认机制生效。
- 显式配置 `doris.query.timeout > 0` 时，**不再被 `lookup.timeout` 钳制**，按配置的秒数原样传给 JDBC。
- `lookup.timeout` 仍是唯一的业务降级判据：到点 Future 失败，并调用 `Statement.cancel()` 主动中止 SQL。
- 取舍：如果 `Statement.cancel()` 因网络问题失败，查询可能会一直跑到 Doris 的 `query_timeout`（集群默认），期间占用一个查询线程/连接；需要关注 `doris.pool.size` 与 Doris 侧超时设置。
- 新增 3 个用例（当前共 94 个）：默认 0 不设置 queryTimeout、显式值不被钳制、负数被拒绝。

## 22. 变更记录（2026-10-29，v1.2.6）

- 统计日志改为中文标签：`表=`、`本窗口=`、`自启动=`、`总处理key数=`、`速率=`、`主源平均批大小=`、`总体平均批大小=`、`主源(...)`、`备源(...)`、`失败=`、`平均=`、`最长=`、`命中率=`、`降级=`。
- `总处理key数` 改为主源 + 备源实际处理 key 数之和；降级批次会被两源各计一次。
- 主源、备源都输出 `批次`、`key数`、`失败`、`平均`、`最长`、`命中率`。
- 表名整行只出现一次；首次打点时「本窗口」和「自启动」覆盖同一段，合并为 `本窗口=自启动=Ns`，避免前后两段完全重复。
- 失败分类改为中文：`超时 / 连接 / 池满 / 数据库 / 其他`；降级占比仍以主源 key 数为分母。

## 23. 变更记录（2026-10-29，v1.2.7）

- 新增 `业务数据量` 指标：等于主源处理的 key 数，不受降级重复处理影响。
- `总处理key数` 仍为主源 + 备源实际处理 key 数；`总处理key数 - 业务数据量` 即备源重复处理的量。
- 日志示例：`业务数据量=5400 总处理key数=5490`，表示 90 个 key 因降级被备源又处理了一次。

## 24. 变更记录（2026-10-29，v1.2.8）

- 修复 `DualLookupFunctionTest` 中仍断言旧英文格式 `failover=1` 的问题，改为匹配中文 `降级=1次`。
- 仅测试代码变更，运行时行为与 v1.2.7 一致。

## 25. 变更记录（2026-10-29，v1.2.9）

- 统计日志和配置回执增加 `子任务=i/N`，用于区分同一张维表在不同并行子任务上的统计行。
- 解析方式：优先使用 `FunctionContext.getTaskInfo()`；Flink 1.18 没有该方法时反射内部 `RuntimeContext`；失败则省略该字段，不影响统计。
- 并行度 >1 时，每个子任务独立输出一行统计是正常现象；日志里现在可以通过 `子任务=` 直接区分。
