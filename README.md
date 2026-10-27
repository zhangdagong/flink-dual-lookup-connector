# Flink Dual Lookup Connector

一个 Flink SQL 维表连接器：**HBase 主查、Doris 兜底**，超时/异常自动降级，全程攒批异步。
SQL 侧照常写 `FOR SYSTEM_TIME AS OF`，容错对业务完全透明。

```
asyncLookup(key) ─► 攒批缓冲 ─(满 50 条 / 等 5ms)─► 主源 HBase 批量 Get
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
mvn test                          # 64 个用例，不依赖任何外部服务
mvn clean package

# 产物体检：有输出就说明 jar 混入了本应由集群提供的类
jar tf target/flink-dual-lookup-connector-1.1.0.jar \
  | grep -E 'org/apache/hadoop/(fs|conf|security)/|org/apache/htrace/' \
  && echo '✗ jar 被污染' || echo '✓ jar 干净'

rm $FLINK_HOME/lib/flink-dual-lookup-connector-1.0.0.jar   # 旧包必须删，JM 与所有 TM
cp target/flink-dual-lookup-connector-1.1.0.jar $FLINK_HOME/lib/
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

> fat jar 里只允许 `org/apache/hadoop/hbase/**`，产物约 **29MB**（旧版带 Hadoop 时 65MB）。
> 上面那行 `jar tf | grep` 打印 `✓ jar 干净` 才算通过。

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
    lookup_cost_ms BIGINT,        -- 元字段：本批查询耗时（含降级）
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

上线前四条：`table.exec.async-lookup.timeout` 已设为 `lookup.timeout × 2 + 2s`；hint 的 `'table'` 用 **FROM 里的别名**；维表有 `PRIMARY KEY`；DDL 每个业务列在两源都存在且列名一致。

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
| 客户端 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | 300 / 500 / 1s | 让后端尽早中止、释放资源 |
| 业务 | `lookup.timeout` | 500 | **唯一的降级判据** |
| 算子 | `table.exec.async-lookup.timeout` | Flink 3min，建议 10s | 最终兜底，防 Future 永不完成 |

四者的交叉校验见第 7 章与 `DualLookupOptions`。另外建连阶段会预热 region location，避免冷启动定位耗时撞上 500ms 超时被误判降级。

### 6.4 三条边界

- **查不到 ≠ 故障**：空结果不降级。否则合法缺失会让每批都查两遍，备源压力翻倍。
- **惰性建连**：Reader 构造时不建连，首次查询才 `open()`。主源启动时不可用也能起作业、直接走备源。Doris 侧连池都是惰性的（`initializationFailTimeout=-1`），避免 HikariCP 建池失败固定睡 1 秒。
- **防打满**：超时回调调 `Statement.cancel()` 真中止 SQL；JDBC `queryTimeout` 自动钳制到 `≤ ceil(lookup.timeout/1000)`；查询线程数 = `doris.pool.size`，队列有界，过载快速拒绝并降级。

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

### 7.2 HBase

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.zookeeper.quorum` | — | 留空则用 classpath 下 `hbase-site.xml` |
| `hbase.zookeeper.property.clientPort` | `2181` | |
| `zookeeper.znode.parent` | `/hbase` | |
| `hbase.table-name` | DDL 表名 | 支持 `命名空间:表名` |
| `hbase.column-family` | `info` | 只支持单列族，所有业务列按同名列读取 |
| `hbase.rowkey.delimiter` | `\|` | 复合主键拼接分隔符 |
| `hbase.rowkey.encoding` | `string` | `string` / `typed`，**必须与写入端一致** |
| `hbase.rpc.timeout` | `300` | 必须 < `lookup.timeout` |
| `hbase.client.operation.timeout` | `500` | 含冷启动定位，建议给足 2000 |
| `hbase.client.retries.number` | `1` | 实时链路 0~1 |

⚠️ **rowkey 编码选错是最难查的故障**：一条也查不到，但「查不到」不报错、不降级，最终整表关联结果全为 NULL。建连日志会打印 `rowkeyEncoding=`，先核对它，再对照写入端。

列值解码约定（写入端必须一致）：`BOOLEAN`→1 字节；`TINYINT/SMALLINT/INTEGER`→4 字节 int；`BIGINT`→8 字节 long；`FLOAT/DOUBLE`→4/8 字节；`DECIMAL`→HBase BigDecimal 编码；`DATE/TIME`→4 字节 int；`TIMESTAMP`→8 字节 long；`CHAR/VARCHAR`→UTF-8；`BINARY`→原始字节。细节见 `HBaseLookupReader.decode`。

### 7.3 HBase Kerberos

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.security.authentication` | `simple` | 只接受 `simple` / `kerberos` |
| `hbase.client.keytab.file` | — | kerberos 下必填，每台 TM 都要有 |
| `hbase.client.kerberos.principal` | — | 具体账号，**不能含 `_HOST`** |
| `hbase.rpc.protection` | `authentication` | `authentication` / `integrity` / `privacy`，须与服务端一致 |
| `hbase.regionserver.kerberos.principal` | — | 如 `hbase/_HOST@REALM`，集群没改名可不配 |
| `hbase.master.kerberos.principal` | — | 同上 |
| `hbase.kerberos.krb5.conf` | — | JVM 已用 `-Djava.security.krb5.conf` 指定则不覆盖 |

### 7.4 Doris 与连接池

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | **唯一必填项** |
| `doris.table-name` | DDL 表名 | 支持 `库.表` |
| `doris.username` / `doris.password` | `root` / 空 | |
| `doris.driver` | `com.mysql.cj.jdbc.Driver` | 留空按 URL 推断 |
| `doris.query.timeout` | `1` | 秒；运行期钳制到 `≤ ceil(lookup.timeout/1000)` |
| `doris.pool.size` | `8` | 最大连接数 = 查询线程数 |
| `doris.pool.min-idle` | `2` | 必须 ≤ `pool.size` |
| `doris.connect.timeout` | `300` | 必须 < `lookup.timeout`（HikariCP 下限 250） |
| `doris.pool.idle-timeout` | `600000` | |
| `doris.pool.max-lifetime` | `1800000` | 须 > idle-timeout |
| `doris.pool.validation-timeout` | `250` | 必须 ≤ `connect.timeout` |

## 8. 内置元字段

在维表 DDL 里声明这两个约定名，即可拿到每次 Lookup 的元信息；它们不查后端，TableSource 会自动从查询列中剥离。

| 字段 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | `STRING` | 数据来源 `hbase` / `doris` |
| `lookup_cost_ms` | `BIGINT` | **批次**耗时（毫秒），含降级耗时 |

细节：同一批内所有命中行拿到相同的 `lookup_cost_ms`；降级后它是「主源超时 + 备源查询」之和；主源空结果时两者为 `NULL`。落库后可直接用 SQL 分析降级时段与代价。

## 9. Kerberos

连接器在建连前完成三步：**写配置 → 配置 UGI → keytab 登录**。登录后 TGT 续期由 UGI 的续期线程与 HBase 的续期 chore 共同负责，长跑作业无需手动 `kinit`。

```sql
'hbase.security.authentication'         = 'kerberos',
'hbase.client.kerberos.principal'       = 'ro@EXAMPLE.COM',      -- 具体账号，不能写 _HOST
'hbase.client.keytab.file'              = '/path/to/ro.keytab',
'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM',
'hbase.rpc.protection'                  = 'authentication',      -- 须与服务端一致
```

三个已处理的坑（排查时要认得）：

1. **只配 `hbase.*` 不配 UGI，登录会被静默跳过**。`AuthUtil.loginClient` 的门槛是 `UGI.isSecurityEnabled()`，UGI 不看 `hbase.security.authentication`；`loginUserFromKeytab` 在 security 关闭时还会**静默 return**。连接器已设 `hadoop.security.authentication=kerberos` 并调 `UGI.setConfiguration`，且登录前断言安全已开启。
2. **principal 的 key 名有两套**。HBase 2.2.x 实际读 `hbase.client.keytab.principal`，文档写 `hbase.client.kerberos.principal`；只写后者会跳过登录。连接器两个 key 都写（`KerberosConfigTest` 守着）。
3. **认证方式拼错不会报错**，等于按 simple 跑。现在建表期只接受 `simple`/`kerberos`。

上线核验：TM 日志出现 `HBase Kerberos 就绪: ... loginUser=ro@EXAMPLE.COM`（必须等于配置的 principal）→ `klist -kt <keytab>` 里确有该 principal → TM 与 KDC 时间差 < 5 分钟 → principal 有表的 `RW` 权限。

## 10. 可观测性

每 `lookup.stats.log-interval` 秒打一行，**本阶段 + 自启动累计**两个视角：

```
[dual-lookup] table=dim_account window=360s totalKeys=5400 rate=15.0/s avgBatch=45 | hbase[batches=120 keys=5400 fail=2(timeout=2) avg=16.5ms max=500.0ms hit=97.8%] doris[batches=2 fail=0 avg=15.1ms max=15.1ms hit=100.0%] failover=2(90keys,1.7%) || table=dim_account since-start uptime=5400s ...
```

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `window=` / `since-start` | 本阶段 / 自启动累计（TM 重启归零） | 判断「现在好不好」只看前者 |
| `avgBatch` | 平均批大小（仅主源批次做分母） | 长期接近 1 → 攒批没生效 |
| `hbase[...]` / `doris[...]` | 批次数、key 数、失败数、平均/最长耗时、命中率 | 备源 `batches>0` 说明在降级；`doris fail>0` 说明两源同时异常 |
| `fail=` | 失败原因分类：`timeout` / `connect` / `poolFull` / `db` / `other`，只列非零项 | 分类决定处置动作 |
| `failover=N(Kkeys,P%)` | 降级次数 + 波及 key 数与占比 | 占比持续上升 → 主源不可用 |

五个读法：① 判断当前是否变差只看 `window=`（累计会被历史稀释）；② **`hit=` 是唯一能照出「维表大面积查不到」的指标**（表误删、编码写错时 `fail=0 failover=0` 一片健康）；③ `avg` 与 `max` 一起看，长尾才是隐患；④ `max` 逐窗口清零，漏打一行会顺延到下一窗口；⑤ 无样本显示 `-` 而不是 `0`。

降级 WARN 默认按 `lookup.failover.log-interval` 限流（首条必打 + 采样 + 合并计数 + 恢复补打）。不限流时，5000 条/分钟、30ms 攒批下约 **285 万行/天**，会把故障起点的上下文冲掉。需要逐批留痕设为 `0`。

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
| Doris 作主源 | `lookup.primary=doris`，`doris.pool.size` 16 起 |
| 两源数据不一致 | 用 `lookup_source` 做对比校验 |

反模式：`lookup.timeout` 大于算子 timeout、`max-wait` 接近 `lookup.timeout`、`hbase.rpc.timeout`/`doris.connect.timeout` ≥ `lookup.timeout`、`batch.size=1`——**前四类建表期就会报错**。另外两个易错点：备源要按**全量峰值**规划容量（主源挂掉时它独扛全量）；多表 join 的总连接数 = Σ（每表并行度 × 每表 `pool.size`），小码表降到 2~4。

## 12. 排查

| 现象 | 原因 / 方向 |
|---|---|
| 建表报找不到 `dual-lookup` factory | jar 没放到所有节点，或集群未重启 |
| 建表报必须声明 PRIMARY KEY | DDL 缺 `PRIMARY KEY (...) NOT ENFORCED` |
| 建表报未知配置项 | 参数拼错，或新增项没在 Factory 登记 |
| hint 不匹配 | `'table'` 写成了表名而不是 FROM 别名 |
| **`NoSuchMethodError: FsTracer.get(...)Lorg/apache/htrace/core/Tracer;`** | jar 混入另一世代 Hadoop → `jar tf target/*.jar \| grep -E 'org/apache/hadoop/(fs\|conf\|security)/'` 自查；修复见第 2 章 |
| 一直降级但 HBase 看起来正常 | `lookup.timeout` 小于实际 P99，或 region 预热点未生效 |
| `hit=` 明显偏低但 `fail=0` | rowkey 编码与写入端不一致 / 表被误删 / 备源同步延迟 |
| HBase 数值或时间全错乱 | 写入编码与 DDL 类型不匹配（见 7.2） |
| Kerberos 报 `GSS initiate failed` | 看 `loginUser=`、`klist -kt`、时间差、`hbase.rpc.protection` |
| 作业取消失败 / 卡住 | 确认 `close()` 会补空攒批缓冲（已有回归用例） |
| TaskManager 连接数过多 | 并行度 × `doris.pool.size` 超出 Doris 承载 |

## 13. 限制与演进

- 单列族；不支持嵌套主键。
- **无熔断器**：主源持续宕机时每批仍要等满 `lookup.timeout`，吞吐受超时限制。
- HBase `open()` 是同步的，建连失败会阻塞该批，依赖 ZK/客户端超时兜底。
- 降级粒度为整批。

演进方向：熔断器 → 主备结果对账 → 新数据源（实现 `LookupReader` 即可）→ 接 Flink Metric（`FunctionContext.getMetricGroup()` 可用，无需改 `RichAsyncFunction`）。

## 14. 开发与测试

```bash
mvn test          # 64 个用例；无外部依赖，可进 CI
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
