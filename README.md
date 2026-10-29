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

## 1. 解决什么问题

| 单源方案的软肋 | 后果 |
|---|---|
| HBase RegionServer / 网络抖动 | 单次点查 5ms → 数秒，算子背压 |
| HBase 运维重启 | 维表关联整体不可用，作业失败 |
| Doris 慢查询 | JDBC 阻塞线程、连接池打满，雪崩 |

把「主备路由 + 超时降级 + 攒批」收进一个 connector，DDL 里配好两个源即可，主备对调只改一行。

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

- **HBase 与 Hadoop 必须成对**。`hbase-shaded-client`（不带 `byo-hadoop`）的 manifest 是 *with Hadoop bundled*，会把整套 Hadoop 打进 jar；曾因此出现
  `NoSuchMethodError: org.apache.hadoop.fs.FsTracer.get(...)Lorg/apache/htrace/core/Tracer;`（jar 内置 Hadoop 3.4.1，生产 3.1.3，`FsTracer` 返回类型不同）。
- **htrace 由 Hadoop 提供**：HBase 引用的 `org.apache.htrace.core.Tracer` 必须与 Hadoop 的 `FsTracer` 是同一个类，所以 `htrace-core4` 也不打进 jar。hadoop-free 部署需手动补进 `lib/`。
- **HikariCP 4.0.3**：兼容 Java 8 的最后一代（5.x 需 Java 11+）。

```bash
# 确认集群 Hadoop 世代
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

> 只允许 `org/apache/hadoop/hbase/**` 出现在 jar 里，产物约 **29MB**（旧版带 Hadoop 时 65MB）。
> 从 1.1.x 升级时先看第 15 章的「升级要点」。

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

上线前五条：算子层 `table.exec.async-lookup.timeout` 已设为 `lookup.timeout × 2 + 2s`；hint 的 `'table'` 用 **FROM 里的别名**；维表有 `PRIMARY KEY`；DDL 每个业务列在两源都存在且列名一致；`hbase.table-name` / `hbase.column-family` / `hbase.rowkey.encoding` 与写入端一致。

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
 t0+560ms Doris 成功      → 分发（lookup_source=doris，cost 含主源查询耗时）
 t0+1060ms Doris 也没回   → 整批 Future 异常完成，作业失败
```

三个刻意的取舍：

- **降级粒度是整批**，主源部分成功也整批重查——换来「绝不漏查、不静默丢数据」的确定性。
- **HBase 任一个 Get 失败即整批失败**：不完整的结果比走备源更危险。
- **两源都失败不补空**：静默补空等于把故障伪装成「key 不存在」，宁可让作业失败。

### 6.3 超时三层

| 层 | 参数 | 默认 | 职责 |
|---|---|---|---|
| 客户端 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | 300 / 500 / 0（跟随 Doris 默认） | 让后端尽早中止、释放资源 |
| 业务 | `lookup.timeout` | 500 | **唯一的降级判据** |
| 算子 | `table.exec.async-lookup.timeout` | Flink 3min，建议 10s | 最终兜底，防 Future 永不完成 |

建连阶段会预热 region location，避免冷启动定位耗时撞上 500ms 超时被误判降级。交叉校验与自动推导见第 7 章。

### 6.4 五条边界

- **查不到 ≠ 故障**：空结果不降级。否则合法缺失会让每批都查两遍，备源压力翻倍。
- **Future 一定有终结**：类型转换失败、Reader 同步抛错、Reader 返回 null 结果，都会让对应 Future 异常完成；绝不留「永不完成」的批。
- **惰性建连**：Reader 构造时不建连，首次查询才 `open()`。主源启动时不可用也能起作业、直接走备源。Doris 侧连池也是惰性的（`initializationFailTimeout=-1`），避免 HikariCP 建池失败固定睡 1 秒。
- **连接自愈**：HBase 备源空闲时连接会被服务端回收。`isOpened()` 检查 `AsyncConnection.isClosed()`，断开自动重建；查询中撞上 `ConnectionClosedException` 会替换失败连接并重试一次。
- **防打满**：超时回调调 `Statement.cancel()` 真中止 SQL；`doris.query.timeout=0`（默认）不设置 JDBC queryTimeout，业务超时仍由 `lookup.timeout` 独立控制；查询线程数 = `doris.pool.size`，队列有界，过载快速拒绝并降级。

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

跨字段校验（建表期即报错）：

```
lookup.batch.max-wait      <  lookup.timeout
hbase.rpc.timeout          <  lookup.timeout
doris.connect.timeout      <  lookup.timeout
lookup.timeout             <  table.exec.async-lookup.timeout
doris.pool.min-idle        ≤  doris.pool.size
doris.pool.validation-timeout  ≤  doris.connect.timeout
```

`table.exec.async-lookup.timeout` 是 **Session 级**配置（`SET`），不是 WITH 参数；`SET` 必须写在 `CREATE TABLE` 之前。

**`lookup.timeout` 联动推导**：仅在显式设置 `lookup.timeout`、且下列参数未显式设置时生效；显式参数优先。

| 依赖项 | 自动推导 | 理由 |
|---|---|---|
| `hbase.rpc.timeout` | `80% × lookup.timeout` | 留 20% 给业务层判定降级 |
| `hbase.client.operation.timeout` | `max(500, lookup.timeout)` | 避免 HBase 客户端先于业务超时放弃 |
| `doris.connect.timeout` | `80% × lookup.timeout`，夹到 `[250, 300]ms` | 250 是 HikariCP 下限，300 防止最后一跳吃满预算 |

`lookup.timeout ≤ 250ms` 且未显式配 `doris.connect.timeout` 时，下限与 `< lookup.timeout` 无法同时满足，建表期会直接报错。

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
| `hbase.rpc.timeout` | `300` | 必须 < `lookup.timeout`；未显式配置时按 80% 推导 |
| `hbase.client.operation.timeout` | `500` | 含冷启动定位，建议 2000；未显式配置时取 `max(500, lookup.timeout)` |
| `hbase.client.retries.number` | `1` | 实时链路 0~1 |

⚠️ **rowkey 编码选错是最难查的故障**：一条也查不到，但不报错、不降级，最终整表关联结果全为 NULL。建连日志会打印 `rowkeyEncoding=`，先核对它。

- `string` 模式下 DECIMAL 走 `toString()`；若写入端用 HBase `Bytes.toBytes(BigDecimal)`，请改用 `typed`。
- `string` 复合主键：逐段编码后用分隔符字节连接；`byte[]` 段直接用原始字节。
- 列值解码：`BOOLEAN`→1 字节；`TINYINT/SMALLINT/INTEGER`→4 字节 int；`BIGINT`/`DOUBLE`→8 字节；`FLOAT`→4 字节；`DECIMAL`→HBase BigDecimal 编码；`DATE/TIME`→4 字节 int；`TIMESTAMP`→8 字节 long；`CHAR/VARCHAR`→UTF-8；`BINARY`→原始字节。
- `TIMESTAMP(p ≤ 3)` 的 long 是**毫秒**，`p > 3` 是**微秒**；`typed` rowkey 与列值使用同一约定。

### 7.3 HBase Kerberos

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.security.authentication` | `simple` | 只接受 `simple` / `kerberos` |
| `hbase.client.keytab.file` | — | **可选**；不配则复用 Flink 全局登录 / kinit |
| `hbase.client.kerberos.principal` | — | **可选**，与 keytab 成对；不能含 `_HOST` |
| `hbase.rpc.protection` | `authentication` | `authentication` / `integrity` / `privacy`，须与服务端一致 |
| `hbase.regionserver.kerberos.principal` | — | 如 `hbase/_HOST@REALM` |
| `hbase.master.kerberos.principal` | — | 同上 |
| `hbase.kerberos.krb5.conf` | — | JVM 已用 `-Djava.security.krb5.conf` 指定则不覆盖 |
| `properties.*` | — | 原样透传给 HBase `Configuration`（兼容 flink-hbase）；`properties.hbase.security.authentication` 等也参与 Kerberos 决策 |

> `properties.hbase.master.keytab.file` 是 HMaster **服务端**配置，客户端不会用它登录：只透传 + WARN。客户端凭据是 `hbase.client.keytab.file + principal`，或直接复用 Flink 全局登录。

### 7.4 Doris 与连接池

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | **唯一必填项**（即使主源是 HBase） |
| `doris.table-name` | DDL 表名 | 支持 `库.表` |
| `doris.username` / `doris.password` | `root` / 空 | |
| `doris.driver` | `com.mysql.cj.jdbc.Driver` | 留空按 URL 推断 |
| `doris.query.timeout` | `0` | 秒；`0` = 不设 JDBC queryTimeout，跟随 Doris 默认；`>0` 时**不受 `lookup.timeout` 钳制** |
| `doris.pool.size` | `4` | 最大连接数 = 查询线程数；Doris 作主源时调到 8~16 |
| `doris.pool.min-idle` | `2` | 必须 ≤ `pool.size` |
| `doris.connect.timeout` | `300` | 必须 < `lookup.timeout`；未显式配置时按 80% 推导并夹到 `[250,300]ms` |
| `doris.pool.idle-timeout` | `600000` | |
| `doris.pool.max-lifetime` | `1800000` | 须 > idle-timeout |
| `doris.pool.validation-timeout` | `250` | 必须 ≤ `connect.timeout` |

> `doris.query.timeout` 只管 JDBC/服务端查询能跑多久，业务降级仍由 `lookup.timeout` 控制：到点 Future 失败并 `Statement.cancel()` 中止 SQL。

## 8. 内置元字段

在维表 DDL 里声明这两个约定名，即可拿到每次 Lookup 的元信息；它们不查后端，TableSource 会自动从查询列中剥离。

| 字段 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | `STRING` | 数据来源 `hbase` / `doris` |
| `lookup_cost_ms` | `BIGINT` | 查询阶段耗时（毫秒，**不含惰性建连**；降级时含两段查询） |

同一批内所有命中行拿到相同的 `lookup_cost_ms`；主源空结果时两者为 `NULL`（miss 没有维表行可承载元字段）。

## 9. Kerberos 实践

**推荐：keytab 交给 Flink 全局配置，连接器不写 keytab。** Flink 的 `HadoopModule` 在 TM 启动时用
`security.kerberos.login.keytab` / `principal` 完成登录（并按 `security.kerberos.relogin.period` 自动续期），
flink-hbase 与本连接器都直接复用这个 UGI。

```yaml
# flink-conf.yaml —— keytab 必须存在于每个 TM 的这个路径
security.kerberos.login.keytab: /home/flink/streampark/kerberos/trdw.keytab
security.kerberos.login.principal: ro@EXAMPLE.COM
```

```sql
-- 维表只打开认证方式与保护级别
'hbase.security.authentication'         = 'kerberos',
'hbase.rpc.protection'                  = 'authentication',
'hbase.regionserver.kerberos.principal' = 'hbase/_HOST@EXAMPLE.COM'
```

需要**单表覆盖**全局登录用户时，或沿用 flink-hbase 写法时：

```sql
-- A. 本连接器显式参数
'hbase.client.kerberos.principal' = 'ro@EXAMPLE.COM',
'hbase.client.keytab.file'        = '/path/to/ro.keytab',

-- B. flink-hbase 迁移写法（properties.* 原样透传）
'properties.hbase.client.keytab.file'        = '/path/to/ro.keytab',
'properties.hbase.client.kerberos.principal' = 'ro@EXAMPLE.COM'
```

**凭据选择顺序**：

1. 未配 keytab → 用当前 UGI（Flink 全局登录 / kinit）；完全没有凭据时报错并提示两种配置方式；
2. 配了 keytab 且当前用户就是该 principal → 复用，不重复登录；
3. 配了 keytab 但文件不可读、而当前 UGI 已有凭据 → **降级复用 + WARN**（keytab 只需在真正读它的节点可读）；
4. 无凭据且 keytab 读不到 → 报错。

**从 flink-hbase 迁移**：参数名加 `hbase.` 前缀（`table-name`→`hbase.table-name`、`zookeeper.quorum`→`hbase.zookeeper.quorum`），补 `hbase.column-family` / `hbase.rowkey.encoding` / `doris.jdbc-url` / `PRIMARY KEY`；`properties.*` 可原样保留。

**核验**：日志出现 `HBase Kerberos 复用已登录用户（Flink 全局登录或 kinit）: ro@EXAMPLE.COM`（或显式模式下的 `HBase Kerberos 就绪: ... loginUser=...`）→ `klist -kt <keytab>` 确有该 principal → TM 与 KDC 时间差 < 5 分钟 → principal 有表的 `RW` 权限。

## 10. 可观测性

每 `lookup.stats.log-interval` 秒一行，**本窗口 + 自启动累计**两个视角：

```
[dual-lookup] 表=dim_account 子任务=1/4 | 本窗口=360s 业务数据量=5400 总处理key数=5490 速率=15.3/s 主源平均批大小=45 总体平均批大小=45 | 主源(hbase)[批次=120 key数=5400 失败=2(超时=2) 平均=16.5ms 最长=500.0ms 命中率=97.8%] 备源(doris)[批次=2 key数=90 失败=0 平均=15.1ms 最长=15.1ms 命中率=100.0%] 降级=2次(90个key,1.7%) || 自启动=5400s ...
```

| 字段 | 含义 | 异常信号 |
|---|---|---|
| `子任务=i/N` | 并行子任务编号；并行度 >1 时每个子任务各一行 | 同表多行属正常 |
| `本窗口=` / `自启动=` | 本阶段 / 自启动累计（TM 重启归零） | 判断「现在好不好」只看前者 |
| `业务数据量` | 主源处理的 key 数（不设备源重复） | 与上游 QPS 对比 |
| `总处理key数` | 主源 + 备源实际处理 key 数 | 与业务数据量的差值 = 备源重复量 |
| `速率` / 平均批大小 | 吞吐与攒批效果 | `主源平均批大小` 长期接近 1 → 攒批没生效 |
| `主源(...)` / `备源(...)` | 批次数、key 数、失败数、平均/最长耗时、命中率 | 备源 `批次>0` 说明在降级；备源 `失败>0` 说明两源同时异常 |
| `失败=` | 分类：`超时` / `连接` / `池满` / `数据库` / `其他`，只列非零项 | 分类决定处置动作 |
| `降级=N次(K个key,P%)` | 降级次数 + 波及 key 数与占主源 key 比例 | 占比持续上升 → 主源不可用 |

五个读法：① 判断当前是否变差只看 `本窗口=`；② **`命中率=` 是唯一能照出「维表大面积查不到」的指标**（表误删、编码写错时 `失败=0 降级=0` 一片健康）；③ `平均` 与 `最长` 一起看，长尾才是隐患；④ `最长` 逐窗口清零，漏打一行会顺延；⑤ 无样本显示 `-` 而不是 `0`。

降级 WARN 默认按 `lookup.failover.log-interval` 限流（首条必打 + 采样 + 合并计数 + 恢复补打）。不限流时，5000 条/分钟、30ms 攒批下约 **285 万行/天**，会把故障起点的上下文冲掉。

> **PushGateway 日志静音（v1.2.4）**：`DualLookupFunction` 初始化与 `open()` 时会把
> `org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter` 的 logger 设为 `OFF`。
> 这是 **JVM 级副作用**（同 TM 所有作业共享），只对 Log4j2 有效，且会隐藏该 reporter 的全部日志（含真实推送失败）。
> 集群侧能改 `log4j2.properties` 时，应优先在集群侧处理；否则可删掉连接器里的静音代码。

## 11. 调优

```
并发数       = QPS × 单次延迟
有效批大小   = min(batch.size, 每子任务QPS × batch.max-wait)
算子吞吐上限 ≈ buffer-capacity × 并行度 ÷ 端到端延迟
```

2000 QPS、并行度 4：`max-wait=5ms` 时每子任务只能攒到 2.5 条/批（约 800 SQL/秒）；调到 30~50ms 后降到 40~67 SQL/秒。`buffer-capacity` 默认 100，高 QPS 建议 300~500。

| 场景 | 调整 |
|---|---|
| 高吞吐 | `batch.size` 100~200，`max-wait` 5~10ms |
| 中等流量 | `max-wait` 30~50ms（默认 5ms 会让攒批失效） |
| 低延迟敏感 | `max-wait` 1~2ms，`batch.size` 20~30 |
| Doris 作主源 | `lookup.primary=doris`，`doris.pool.size` 调到 8~16 |
| 两源数据不一致 | 用 `lookup_source` 做对比校验 |

反模式：`lookup.timeout` 大于算子 timeout、`max-wait` 接近 `lookup.timeout`、`hbase.rpc.timeout`/`doris.connect.timeout` ≥ `lookup.timeout`、`batch.size=1`——**前四类建表期就会报错**。另外：备源要按**全量峰值**规划容量；多表 join 的总连接数 = Σ（每表并行度 × 每表 `pool.size`），小码表降到 2~4。

## 12. 排查

| 现象 | 原因 / 方向 |
|---|---|
| 建表报找不到 `dual-lookup` factory | jar 没放到所有节点，或集群未重启 |
| 建表报必须声明 PRIMARY KEY | 补 `PRIMARY KEY (...) NOT ENFORCED` |
| 建表报未知配置项 | 参数拼错；`properties.*` 需要 1.2.0+（旧包用 `helper.validate()`） |
| 建表报 `doris.jdbc-url 必填` | 备源是容错下限，即使主源 HBase 也必须配 |
| **`NoSuchMethodError: FsTracer.get(...)`** | jar 混入另一世代 Hadoop → `jar tf ... \| grep -E 'org/apache/hadoop/(fs\|conf\|security)/'` 自查；见第 2 章 |
| 备源空闲后首次降级报 `ConnectionClosedException` 且持续失败 | 已修复：连接健康检查 + 自动重建 + 重试一次；确认加载的是新 jar |
| 一直降级但 HBase 正常 | `lookup.timeout` 小于实际 P99，或 region 预热点未生效 |
| `命中率=` 偏低但 `失败=0` | rowkey 编码不一致 / 表被误删 / 备源同步延迟 |
| HBase 数值或时间错乱 | 写入编码与 DDL 类型不匹配；TIMESTAMP 注意 p≤3 毫秒、p>3 微秒 |
| Kerberos 报 `GSS initiate failed` | 先确认 Flink 全局 `security.kerberos.login.*` 生效；再查 `klist -kt`、时间差、`hbase.rpc.protection` |
| `keytab 不可读` 但 Flink 登录正常 | 新版降级复用并 WARN；要显式用 keytab 则确认每台 TM 可读 |
| 时间类型主键命中率偏低 | 新版已把 Timestamp/LocalDateTime 等归一化并截断到微秒；旧包需升级 |
| 反复出现 `failed to push metrics to pushgateway` | 1.2.4+ 会静音该 logger；仍出现就确认加载的是新 jar、Flink 用 Log4j2 |
| TaskManager 连接数过多 | 并行度 × `doris.pool.size` 超出 Doris 承载 |

## 13. 限制与演进

- 单列族；不支持嵌套主键。
- **无熔断器**：主源持续宕机时每批仍要等满 `lookup.timeout`。
- HBase `open()` 同步阻塞，建连失败会阻塞该批；建连耗时不计入 `lookup_cost_ms` / `lookup.timeout`，但计入墙钟时间。
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

新增配置项要改三处：`DualLookupOptions` 加 Option 与字段、`Config.from` 赋值、`DualLookupTableSourceFactory.optionalOptions()` 登记。

## 15. v1.2 升级要点（1.1.x → 1.2.9）

| 变化 | 影响 / 动作 |
|---|---|
| `string` 模式 DECIMAL 单主键改为 `toString()` | 写入端若用 `Bytes.toBytes(BigDecimal)`，改用 `rowkey.encoding=typed`，否则查不到 |
| `lookup_cost_ms` 只统计查询阶段 | 不再包含 HBase 惰性建连/预热；与 `lookup.timeout` 口径一致 |
| `lookup.timeout` 联动推导 | 未显式配置的 rpc/connect 超时自动跟随；显式参数优先 |
| `doris.pool.size` 默认 8→4 | Doris 作主源时显式调到 8~16 |
| `doris.query.timeout` 默认 1→0 | 不设 JDBC queryTimeout；显式值不再被 `lookup.timeout` 钳制 |
| 统计日志中文化 | 新增 `子任务=i/N`、`业务数据量`；解析脚本需同步 |
| `properties.*` 真正参与解析 | `properties.hbase.security.authentication` 等可生效；需 1.2.0+ |
| 异步链路异常安全 | 单个 key 转换失败不再导致后续 Future 悬挂 |
| TIMESTAMP 精度统一 | p≤3 毫秒、p>3 微秒；rowkey 与列值一致 |
| PushGateway 日志静音 | JVM 级副作用，会隐藏真实推送失败；集群侧能改日志时优先集群侧 |
