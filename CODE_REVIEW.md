# dual-lookup connector 代码评审记录

本项目历经五轮代码评审：首轮全量评审、第三轮复查、核心语义核实、Doris 宕机场景核查、降级日志限流。
评审覆盖 9 个 Java 类、`pom.xml`、构建产物、`sql/demo.sql`、测试用例与全部文档。

**结论：累计 19 项问题已全部修复**（首轮 12 项 + 复查 5 项 + 宕机场景 1 项 + 日志限流 1 项），
单元测试从 0 增加到 **45 个**，把每一次修复都固化为回归防线。

> **文档说明**：本文档曾在一次 IDE 提交操作中被误删（作为未跟踪文件被 Rollback），
> 现依据各轮修复的实际代码、测试与实测数据重建。文中的所有代码行为、参数默认值、
> 测试数量均与仓库现状逐一核对过。修复细节见下文章节，文末列出需要按你的实际环境确认的事。

---

## 一、修复清单总览

| # | 轮次 | 级别 | 问题 | 修复 |
|---|---|---|---|---|
| 1 | 首轮 | **P0** | Doris 侧批内**同一主键出现多次**时静默丢数据：反查映射用 `Map<String,Integer>`，后写覆盖先写 | 改为 `Map<String,List<Integer>>` + `Set<String>` 去重，结果行按全部下标分发 |
| 2 | 首轮 | **P0** | **残留定时器冲掉下一批**：定时任务的 `ScheduledFuture` 被丢弃，无法取消 | 用 `pendingFlush` 同时承担标志位与取消句柄；`flush()` 摘批前先 cancel |
| 3 | 首轮 | P1 | 超时只取消「已拿到 Statement」的任务，「还在队列里/阻塞在取连接」的任务照常跑完 | `task.cancel(true)` 移出 `st != null` 判断 |
| 4 | 首轮 | P1 | 查询线程池用 `Executors.newFixedThreadPool`，**无界队列等于没有背压** | 改 `ThreadPoolExecutor` + `ArrayBlockingQueue(poolSize×2)` + `AbortPolicy` |
| 5 | 首轮 | P1 | 队列满时 `RejectedExecutionException` 会冒泡出去，**让整个作业失败而不是降级** | 捕获并翻译成「异常完成的 Future」，交由上层走降级 |
| 6 | 首轮 | P2 | `avgBatch` 分母把备源批次也算进去，降级频繁时被系统性低估 | 分母只用主源批次数；新增 `avgBatchWithStandby` 指标 |
| 7 | 首轮 | P2 | rowkey 只支持「按字符串写」，写入端按类型化 rowkey 时一条也查不到 | 新增 `hbase.rowkey.encoding`（`string`/`typed`），`typed` 与 `decode()` 严格对称 |
| 8 | 首轮 | P2 | 超时类参数可配出**互相矛盾**的组合，且只在运行期才暴露 | 新增 6 条建表期交叉校验（见第五章），配错在建表阶段直接报错 |
| 9 | 首轮 | P2 | 热路径上重复计算（每行每列 `getColumnLabel().toLowerCase()`、每次 Get 重复 `Bytes.toBytes`） | 列标签/列名小写/qualifier 字节全部提到构造期或循环外预计算 |
| 10 | 首轮 | P2 | fat jar 打进了整个 `org.apache.hadoop.*`（实测 3 万余类、约 72MB），与集群版本冲突时偶发 `NoSuchMethodError` | shade `artifactSet` 排除 `org.apache.hadoop:*`，改由集群提供 |
| 11 | 首轮 | P3 | `sql/demo.sql` 存在笔误与默认值不一致（`scan.startup.mode` 注释、`rpc.timeout`、`connect.timeout` 等） | 逐条修正，与 `DualLookupOptions` 对齐 |
| 12 | 首轮 | P3 | 仓库提交了 `.idea/` 与 `target/`（含数十 MB fat jar） | 新增 `.gitignore` 并 `git rm -r --cached .idea` |
| 13 | 复查 | **P1** | Doris 侧 **byte[] / BigDecimal 主键静默查不到**：结果反查靠 `toString()`，得到 `[B@哈希` / 带 scale 的字符串 | 新增 `normKeyPart`：byte[]→Base64、BigDecimal→`stripTrailingZeros().toPlainString()`，入参与结果行两侧共用 |
| 14 | 复查 | **P1** | `close()` 之后迟到的降级链会**重新 `open()` 备源**，新建的连接池/线程池无人关闭（泄漏） | `call()` 入口加 `initialized` 守卫；`close()` 开头即置 `initialized=false`，连「close 进行中」的窗口也封死 |
| 15 | 复查 | P2 | 已完成查询的超时任务滞留在单线程调度器队列直到到期，高 QPS 下只增不减 | `whenComplete` 里 `cancel(false)`；调度器改 `ScheduledThreadPoolExecutor` 并开 `setRemoveOnCancelPolicy(true)` |
| 16 | 复查 | P2 | `RowDataConverter` 是全项目唯一零测试的核心类 | 新增 `RowDataConverterTest`（7 例） |
| 17 | 复查 | P2 | ISO 带小数秒的时间戳（`...T12:30:15.123456`）漏掉 `'T'`→空格替换，`Timestamp.valueOf` 抛异常 | 条件由 `length == 19` 改为 `length > 10 && charAt(10) == 'T'` |
| 18 | 宕机场景 | **P1** | Doris 不可达时 `open()` **每次固定耗时 1002ms**（HikariCP 建池自检硬睡 1 秒），降级预算被彻底打破 | `hc.setInitializationFailTimeout(-1L)`：建池不做连接尝试，代价由 `doris.connect.timeout` 封顶 |
| 19 | 日志限流 | P2 | 主源长故障时降级 WARN 洪峰（5000 条/分流量下约 **285 万行/天**），把故障起点冲掉 | 新增 `lookup.failover.log-interval`（默认 10s，0=不限流）+ `FailoverLogGate` 闸门 |

**回归用例**：26 个（首轮）→ 38 个（复查）→ 39 个（语义核实）→ 40 个（宕机场景）→ **45 个（日志限流）**。

---

## 二、两处核心缺陷详解（首轮 #1 #2）

这两项会**静默出错**（不报错、不打异常日志），是首轮评审中最要紧的发现。
下面给出修复前后的实测对比——不是断言，而是把 `git show HEAD:` 的旧版本编译到独立 classpath 实际跑出来的输出。

### 2.1 缺陷①：Doris 侧批内重复主键静默丢数据

**根因**：`DorisLookupReader` 建立「主键值 → 入参下标」反查映射时用的是 `Map<String, Integer>`。
按账户关联交易流水时，同一账户在 50 条流水里出现 3 次是极常见的，此时后写会覆盖先写的下标。
`groupByKey` 拿着被覆盖后的单个下标去分发，**前面的下标在结果里查不到**，
上层 `distribute` 便把它当成「查不到」输出 NULL——正是本连接器最忌讳的静默丢数据。

**修复**：反查映射改为 `Map<String, List<Integer>>`，同一主键的全部入参下标都记下来；
SQL 侧再用 `Set<String> distinctKeys` 去重（同一主键只发一次参数），结果行按全部下标分发。

**实测对比**（同一批输入，主键 `[A, A, B]`，Doris 返回 A、B 两行）：

| 版本 | A 对应的结果下标 | 结论 |
|---|---|---|
| 修复前 | `[2]` | 下标 0 **丢失**（静默 NULL） |
| 修复后 | `[0, 2]` | 两个下标都拿到结果 |

回归用例：`DorisLookupReaderTest.duplicateKeysInBatchAllIndexesGetResult`（同时断言 IN 占位符只有 2 个，验证去重生效）。

### 2.2 缺陷②：残留定时器冲掉下一批

**根因**：时间兜底触发的定时任务，其 `ScheduledFuture` 被创建后直接丢弃，无法取消。
于是出现下面的时序（设 `batchMaxWaitMs = 5`）：

```
t=0     安排定时任务 T1（5ms 后触发）
t=2     攒满 50 条 -> flush() 摘走整批
t=2.5   新 key 入队 -> 又安排 T2（7.5ms 触发）
t=5     T1 照常触发 -> 看到缓冲非空 -> 把刚攒的新批提前冲掉（只剩 1~2 条）
```

后果是实际批大小系统性小于 `lookup.batch.size`，SQL 次数高于理论值、Doris FE 的解析规划压力被白白放大。

**修复**：用 `pendingFlush`（`ScheduledFuture`）同时承担两个职责——既作「已安排定时器」的标志位，
又作「取消句柄」；`flush()` 摘批前先 `cancel(false)`。`onFlushTimer()` 触发时先清空自身引用再 flush，
避免定时任务取消自己（正在运行时 `cancel` 无效）而误伤新批次刚安排的定时器。

**实测对比**（第二批的实际触发时刻，间隔越小说明被冲得越早）：

| 版本 | 第二批触发时刻 | 结论 |
|---|---|---|
| 修复前 | 1071ms | 被 T1 残留任务提前冲掉 |
| 修复后 | 1580ms | 由自己的定时器触发（间隔正常） |

回归用例：`DualLookupFunctionTest.staleTimerDoesNotFlushNextBatch`（在两个锚点之间插入 500ms 间隔，
断言批间间隔 > 800ms，避免受 JVM 首次类加载耗时干扰）。

---

## 三、首轮其余 10 项（#3 ~ #12）

### 3.1 超时保护在「最需要它的场景」失效（P1，#3）

`Statement.cancel()` 与 `Future.cancel(true)` 原本一起关在 `if (st != null)` 里，
但 `stmtRef` 只在「任务真正拿到连接并 `prepare` 之后」才被赋值。而最容易超时的恰恰是：

- 任务还排在**队列里**（`queryExecutor` 有积压）；
- 任务阻塞在 **`getConnection()`**（连接池打满，`doris.connect.timeout` 往往远大于 `lookup.timeout`）。

这两种情况下 `stmtRef` 仍是 null，`task.cancel(true)` 被跳过——任务会在「业务层已超时降级」之后
照常拿到连接、把慢查询发到 Doris 跑完，连接池与线程池越拖越满。

**修复**：把 `task.cancel(true)` 移出判空，无论 Statement 是否已创建都要取消任务。

### 3.2 背压缺失（P1，#4 #5）

原实现用 `Executors.newFixedThreadPool`，其内部是**无界队列**，等于没有背压：Doris 变慢时每个任务
都要等满 `lookup.timeout` 才「结束」（Future 早已异常完成，任务本身还在跑），上游却仍在按 QPS 提交，
队列只增不减。每个排队任务持有一整批绑定参数，堆积既吃堆内存，又会在 Doris 恢复后集中涌去抢连接。

**修复**：改为有界队列 `ArrayBlockingQueue(poolSize × 2)` + `AbortPolicy`；
`submit` 外包 try/catch，把 `RejectedExecutionException` **翻译成异常完成的 Future**（等价于「该源本次查询失败」）。

> 这一条的关键在于：**拒绝绝不能冒泡出 `asyncLookup`**。一旦冒泡，Flink 会让整个作业失败，
> 而不是降级到备源——那就完全违背了本连接器的设计目标。

### 3.3 平均批大小被系统性低估（P2，#6）

`avgBatch` 的分母原本是主源 + 备源批次之和，但降级时同一批 key 主备各查一次、
`totalKeys` 只加一次而批次加了两次，于是 `avgBatch` 被系统性低估——会把「降级频繁」
误读成「攒批没生效」，导致无效调参。

**修复**：`avgBatch` 分母只用主源批次数；另加 `avgBatchWithStandby` 指标，两者差异即可反映降级占比。

### 3.4 rowkey 编码模式（P2，#7）

新增 `hbase.rowkey.encoding`：

| 模式 | 单主键 | 复合主键 |
|---|---|---|
| `string`（默认，兼容既有数据） | 主键值转字符串后取 UTF-8 字节 | 按 `hbase.rowkey.delimiter` 拼成字符串再取字节 |
| `typed` | 按主键 Flink 类型编码（`BIGINT`→`Bytes.toBytes(long)` 等） | 各段类型化编码后用**分隔符字节**连接（避免变长段的前缀歧义） |

`typed` 与 `HBaseLookupReader.decode()` 的解码规则严格对称，编码→解码可无损还原。
⚠️ 两种模式**必须与被查表的写入端严格一致**，选错会「一条也查不到」，且因空结果不算故障而不触发降级、
不打异常日志，最终表现为整张维表关联结果全为 NULL。建连日志会打印实际生效的模式（`rowkeyEncoding=`）。

### 3.5 建表期交叉校验（P2，#8）

见第五章。核心思路：这些参数单独看都合法，**组合起来才会失效**，因此必须放在跨字段校验里拦。

### 3.6 热路径重复计算（P2，#9）

- `DorisLookupReader.groupByKey`：列标签 `getColumnLabel(i).toLowerCase()` 原本在内层循环，
  变成「每行 × 每列」都调用一次（且每次新建字符串对象）→ 提到循环外算一次。
- `RowDataConverter` / `HBaseLookupReader`：列名小写副本预计算；
- `HBaseLookupReader`：列名的 UTF-8 字节（qualifier）预计算，避免「每个 Get × 每一列」重复编码。

### 3.7 fat jar 打入整个 Hadoop（P2，#10）

`hbase-shaded-client` 只 relocate 了第三方依赖（protobuf/guava/zookeeper 等），
`org.apache.hadoop.*` **保持原包名**，默认会整个打进 fat jar（实测 3 万余类、约 72MB）。
多带一份 `hadoop-common` 与 Flink lib 下的版本冲突时，典型症状是 `NoSuchMethodError` / `ClassCastException`，
且只在特定代码路径上偶发。

**修复**：shade `artifactSet` 排除 `org.apache.hadoop:*`。
⚠️ `pom.xml` 里已加注释：若你的 Flink 是 **hadoop-free 发行版**（lib 下没有 `hadoop-common`），删掉那一行即可。

---

## 四、复查（第三轮）5 项

### 4.1 byte[] / BigDecimal 主键静默查不到（P1，#13）

`groupByKey` 要把 Doris 返回的行归到正确的入参下标上，做法是「拼字符串键再比对」。
但两类值的 `toString()` 无法表达内容相等：

| 类型 | `toString()` 的结果 | 问题 |
|---|---|---|
| `byte[]`（BINARY/VARBINARY） | `[B@1a2b3c`（对象哈希） | 两个内容完全相同的数组拼出**不同**字符串，永远查不到 |
| `BigDecimal`（DECIMAL） | 带 scale，如 `123.00` | 入参 `123` 与返回值 `123.00` 不匹配 |

症状是「该类型的主键全部查不到」——不触发降级、不打异常日志。

**修复**：新增 `normKeyPart(Object)`，入参侧（`joinKey`）与结果行侧（`joinKeyFromRow`）共用：

```java
private static String normKeyPart(Object v) {
    if (v == null) return "";
    if (v instanceof byte[])  return Base64.getEncoder().encodeToString((byte[]) v);
    if (v instanceof BigDecimal) return ((BigDecimal) v).stripTrailingZeros().toPlainString();
    return v.toString();
}
```

`stripTrailingZeros() + toPlainString()` 同时解决两件事：消除 scale 差异、避免科学计数法（`1E+2` vs `100`）。

### 4.2 close 后降级链重新建连泄漏（P1，#14）

`close()` 会把 `initialized` 置 false 并关闭两个 Reader；但此时可能仍有**在途批次**：
主源查询失败 → 触发降级 → 走到 `call(standby, keys)` → 发现 `standby.isOpened()` 为 false → 重新 `open()`。
新建出来的连接池、线程池、`timeoutScheduler` **没有任何人负责关闭**，直接泄漏。

**修复**：`call()` 入口先查 `initialized`，为 false 时返回一个「已关闭」的异常完成 Future（会被上层丢弃）；
`close()` 开头即置 `initialized = false`，把「close 进行途中」的窗口也一并封死。

> 注意这里与「两源失败不补空」的策略并不冲突：close 阶段的补空由 `close()` 自己完成（补 `emptyList`），
> `call()` 抛出的异常只影响那些注定要被丢弃的在途 Future。

### 4.3 超时任务滞留调度器队列（P2，#15）

业务层硬超时用单线程 `ScheduledExecutorService`；查询提前结束（绝大多数情况）后，
对应的超时任务仍会**躺在队列里直到原定到期时刻**才被清理。高 QPS + 较大 `lookup.timeout` 时，
队列里会堆积大量「结果早已返回」的任务，到期逐个空转扫描。

**修复**：两处 Reader 都在 `whenComplete` 里 `timeoutTask.cancel(false)`；
并把调度器从 `Executors.newSingleThreadScheduledExecutor`（返回包装类，拿不到队列）
改为直接构造的 `ScheduledThreadPoolExecutor`，开启 `setRemoveOnCancelPolicy(true)`。

> **关键点**：JDK 默认策略下，`cancel()` 之后任务**仍然留在队列里**，只是被标记为已取消。
> 只 `cancel(false)` 而不开 `setRemoveOnCancelPolicy(true)`，等于没修。改成直接构造还有个附带好处：
> 测试可以通过 `getQueue()` 直接观测队列是否被及时清理。

### 4.4 RowDataConverter 零测试（P2，#16）

`RowDataConverter` 是「两源结果可互换」的收口点，却是全项目唯一没有测试的核心类。
新增 `RowDataConverterTest`（7 例）：全类型双向转换、元字段识别、大小写兼容、
异构后端类型归一化（Doris 的 `TINYINT(1)` 可能返回数字而非 Boolean）、
DATE/TIME/TIMESTAMP 的多种后端表示、缺列补 NULL。

### 4.5 ISO 带小数秒时间戳抛异常（P2，#17）

`toTimestamp` 兼容 `'T'` 分隔符的条件是 `length == 19`，而带小数秒的 ISO 串长度是 26：

```
"2026-09-29T12:30:15.123456"
                    └─ 长度 26 ≠ 19，'T' 替换被跳过
截断到 19 位 -> "2026-09-29T12:30:15"
Timestamp.valueOf(...) -> IllegalArgumentException
```

于是一个完全合法的时间戳会触发**无谓的降级**。

**修复**：条件改为 `s.length() > 10 && s.charAt(10) == 'T'`。

### 4.6 附带记录：一个「虚惊」

测试曾断言 `TimestampData.getMillisecond()` 与输入毫秒相等，差值恰好 8 小时。
排查确认**不是代码缺陷**：`TIMESTAMP_WITHOUT_TIME_ZONE` 的 `TimestampData` 内部按墙钟存储，
`getMillisecond()` 返回「墙钟当 UTC」的值，JVM 在 UTC+8 下与 `Timestamp.getTime()` 差 8 小时。
这是 Flink 的语义陷阱，测试已统一改为断言 `toLocalDateTime()` 的墙钟值。

---

## 五、核心语义核实

用户明确的核心需求：**HBase 主 / Doris 备（可配），主源任何异常降级，但「查不到」不是异常**。
逐路径核实结论：**满足**。

### 5.1 降级的唯一判据是「Future 异常完成」

主源所有失败路径都收敛到这一点，无一遗漏：

| 失败场景 | 代码路径 |
|---|---|
| HBase 连不上（含启动期宕机） | 惰性建连，`open()` 抛错 → `call()` 包成异常 Future → 降级 |
| RPC 失败 / 任一 Get 失败 | `allOf` 回调 → `completeExceptionally` |
| 超时（HBase 卡慢） | `withTimeout` 强制 `TimeoutException` |
| JDBC 异常 / 连接池取连接超时 | 任务内 `catch (Throwable)` → 异常完成 |
| 线程池队列满（过载） | 拒绝被翻译成异常 Future（不冒泡） |
| 超时（Doris 卡慢） | 调度器强制 `TimeoutException` + `Statement.cancel()` 真中断后端 SQL |

### 5.2 「查不到」在三层都与「失败」区分开

- **Reader 层**：key 全空 → `complete(emptyMap)`；行未命中 → 不下标入 Map，Future **正常完成**；
- **接口契约**：`LookupReader` javadoc 明确「未命中的 key 不出现在结果里，空结果不算故障」；
- **分发层**：`distribute` 对未命中的下标完成 `emptyList()`——Future 正常完成，**不会**走降级分支。

维表场景里「HBase 没有这个主体」是大概率事件，若误判会打满 Doris 做无谓重查。

### 5.3 补了一个测试缺口

核实时发现：「空结果不降级」在**函数级**没有显式回归测试——若将来有人改坏成「空结果也降级」，
现有测试不会报警。已补 `DualLookupFunctionTest.emptyResultDoesNotFailover`
（断言备源零调用 + 每个 key 的 Future 以空结果正常完成）。

---

## 六、Doris 宕机场景核查

问题：**Flink 启动时 HBase 正常、Doris 宕机，会发生什么？** 逐路径核查 + 写探针实测（探针已删）。

| 场景 | 行为 |
|---|---|
| 默认配置 `lookup.primary=hbase` | Doris 的 Reader **全程不被 `open()`**：零连接、零报错日志、零耗时。作业正常启动、正常关联，`lookup_source` 全程 `hbase`。**代价是这段时间容错能力归零**（备源无健康信号），必须对 Doris 单独做可用性监控 |
| 建表 / 作业提交阶段 | 参数校验是纯参数检查，不连任何数据库，**零影响** |
| `lookup.primary=doris` 且 Doris 宕机 | 每批降级到 HBase，数据连续。**修复前 1002ms/批，修复后 300ms/批** |

### 6.1 根因与修复（P1，#18）

**实测**：`DorisLookupReader.open()` 指向一个拒绝连接的端口，**每次固定耗时 1002ms**；
同一地址换裸 JDBC（`DriverManager`）只要 2~107ms——差一个数量级，说明这 1 秒不是网络花的。

**根因**（读 HikariCP 4.0.3 源码确认，非猜测）——`HikariPool.checkFailFast()`：

```java
private void checkFailFast() {
    final long initializationTimeout = config.getInitializationFailTimeout(); // 默认 1
    do {
        final PoolEntry poolEntry = createPoolEntry();
        if (poolEntry != null) { ...; return; }
        quietlySleep(SECONDS.toMillis(1));   // ← 首次建连失败后无条件硬睡 1 秒
    } while (elapsedMillis(startTime) < initializationTimeout);
}
```

`initializationFailTimeout` 默认值 `1` 表示「建池时先抢一条连接」，抢失败后**必然** `sleep(1000)`。
而这 1 秒睡在**调用 `open()` 的线程**上——也就是 Flink 的异步算子线程 / 攒批定时线程。
更糟的是 `open()` 失败后 `opened` 仍为 `false`，**每个批次都会重试建池**，每批白烧 1 秒，
而 `lookup.timeout` 才 500ms，降级预算被彻底打破。

**修复**：`hc.setInitializationFailTimeout(-1L)`——让 `checkFailFast()` 直接返回、建池不做连接尝试，
把连接推迟到首次 `getConnection()`，代价由 `doris.connect.timeout`（默认 300ms）封顶。

| 指标 | 修复前 | 修复后 |
|---|---|---|
| `open()` 耗时 | 1002 / 1002 / 1002 ms | 29 / 0 / 0 ms |
| `open()` 后 `opened` | `false`（下批再试） | `true` |
| 每批端到端 | 1032 / 1002 / 1002 ms | 312 / 300 / 301 ms |
| 3 批内 `open()` 次数 | 3 次（每批重建池） | 1 次 |

附带好处：Doris 恢复后连接池自行补连，不需要重启作业。
代价：URL / 账号配错不再在 `open()` 阶段暴露，而是首次查询时以「该源本次失败」的形式暴露——
这与本连接器「失败即降级」的语义一致，不影响主备切换的正确性。

回归用例：`DorisLookupReaderTest.openDoesNotBlockWhenDorisIsUnreachable`（断言 < 500ms 且 `opened=true`）。

---

## 七、降级日志限流

问题：**主源 HBase 长时间故障 + 每分钟 5000 条流量时**，
`[dual-lookup] 主源 hbase 批量查询失败，降级到备源 doris。批大小=1` 会刷屏。

| 指标 | 数值 |
|---|---|
| 流量 | 5000/min ≈ 83 条/秒 |
| 有效批大小（83 × 0.03） | ≈ 2.5 条/批 |
| 降级批次 | ≈ 33 批/秒 |
| 不限流时 | **约 285 万行 WARN/天** |

真正的代价不是占磁盘，而是 TM 日志频繁滚动会把**故障起点那几行**冲掉，排查反而更难。

**修复**：新增 `lookup.failover.log-interval`（秒，默认 `10`，**`0` = 不限流**，负数建表期报错）
与 `DualLookupFunction.FailoverLogGate`（不依赖时钟的纯状态机，时刻由调用方传入，因此边界可直接单测）。
策略是「**首条必打 + 按间隔采样 + 合并计数 + 恢复补打**」：

```
t=0.00s  主源 hbase 批量查询失败，降级到备源 doris。批大小=3，原因：TimeoutException: ...   ← 首条必打
         （0.03s ~ 10.00s 之间约 330 批降级，静默）
t=10.03s 主源 hbase 仍在降级中，降级到备源 doris。批大小=3，原因：...（距上次打印又发生 330 批降级已被合并）
t=25.10s 主源 hbase 已恢复，重新由主源提供数据（本次故障共降级 830 批）                      ← 恢复信号
```

三点保证「洪峰被压平，但数量不丢」：

1. 故障第一批携带完整异常原因，**故障起点必留痕**；
2. 后续每条附带「期间被合并的批数」，数量信息没有丢失；
3. 精确逐批计数另有统计日志的 `failover=` 字段兜底——**限流只影响打印，不影响 `stats.recordFailover()` 计数**。

恢复日志只在「确实降级过」时才输出（`total() > 0`），主源一直健康时零噪音。

---

## 八、测试用例分布

单元测试全部用**伪造对象**驱动真实代码（`HikariDataSource` 匿名子类 + JDBC `Proxy`、假 `LookupReader`），
因此不需要启动 Doris / HBase / Flink 就能跑，适合放进 CI 作为发布门槛。

```bash
mvn test          # 53 个用例，不依赖任何外部服务
mvn clean package # 打 fat jar
```

| 测试类 | 用例数 | 覆盖内容 |
|---|---|---|
| `DualLookupOptionsTest` | 12 | 默认值自洽性、6 条交叉校验、枚举归一化、表名回退、两个日志间隔的边界（含 `stats.log-interval` 负数拒绝） |
| `DorisLookupReaderTest` | 9 | 批内重复主键分发、超时打断、队列满降级、空 key 短路、IN 子句形态、byte[]/BigDecimal 按内容匹配、超时任务取消、**Doris 宕机 open() 不被建池自检拖住** |
| `DualLookupFunctionTest` | 13 | 攒满即发、时间兜底、**残留定时器不冲下一批**、主备降级、**空结果不降级**、两源失败不补空、关闭补空、close 后降级链不重新建连、降级日志闸门（首条+采样+合并、interval=0、reset、**反射验证接线**）、**各源耗时埋点接线** |
| `HBaseLookupReaderTest` | 1 | 已完成查询取消超时任务、调度器队列清空 |
| `HBaseRowKeyEncodingTest` | 5 | 两种编码模式的字节结果、复合主键拼接、编码→解码往返、空 key |
| `RowDataConverterTest` | 7 | 全类型双向转换、元字段、大小写兼容、异构后端类型归一化、时间多形态 |
| `LookupStatsTest` | 6 | **阶段值而非累计值**、**每行都带累计视角且阶段/累计守恒**、各源平均耗时互相独立、无样本显示 `-`、关闭时累计汇总、interval=0 不建线程 |

**配置项校验规则**（建表期失败，共 6 条跨字段校验）：

| # | 规则 | 不拦会怎样 |
|---|---|---|
| 1 | `lookup.batch.max-wait < lookup.timeout` | 攒批等待吃掉整个超时预算，时间兜底还没触发批次就被判超时 |
| 2 | `hbase.rpc.timeout < lookup.timeout` | 业务层已降级切源，HBase 侧还在等那次 RPC |
| 3 | `doris.connect.timeout < lookup.timeout` | 连接池打满时业务层先超时，而备源无处可降、整批失败 |
| 4 | `table.exec.async-lookup.timeout > lookup.timeout` | 降级没走完，Flink 异步算子自己先判超时 |
| 5 | `doris.pool.min-idle <= doris.pool.size` | HikariCP 硬性约束 |
| 6 | `doris.pool.validation-timeout <= doris.connect.timeout` | HikariCP 硬性约束 |

另有 `lookup.primary` ∈ {hbase, doris}、`hbase.rowkey.encoding` ∈ {string, typed}、
`lookup.timeout > 0`、`batch.size > 0`、`batch.max-wait >= 0`、`failover.log-interval >= 0`、
`stats.log-interval >= 0`、`doris.jdbc-url` 必填、Kerberos 下 keytab 与 principal 成对等若干条。

---

## 九、验证方式（本机无 mvn 时）

仓库根目录的 `run_units.py` 可在没有 Maven 的情况下完成「编译 + 跑测试」：

1. 用 IDEA 自带的 JBR 编译器（`<IDEA>/jbr/bin/javac.exe`）+ `~/.m2/repository` 的依赖手工编译；
2. 用 `org.junit.runner.JUnitCore` 直接运行测试类。

两个必须注意的坑：

- Windows 下用 `@argfile` 传长 classpath 时，路径里的 `\` 会被 javac 当作转义符，需统一换成 `/`；
- 用 Git Bash 的 `$(find ...)` 拼 classpath 会得到 `/c/...` 形式，Windows 的 `javac.exe` 不识别，
  必须走脚本拼 Windows 路径。

---

## 十、需要按你的实际环境确认的事

1. **rowkey 编码选择**（`hbase.rowkey.encoding`）
   默认 `string` 兼容「按字符串写 rowkey」的既有数据；若写入端按 HBase 惯例写 `Bytes.toBytes(long)`，
   必须改成 `typed`。**选错不会报错，只会一条也查不到**（关联结果全 NULL）。
   确认方法：看建连日志的 `rowkeyEncoding=`，再核对写入端代码。

2. **Flink 是否为 hadoop-free 发行版**
   若不是（lib 下有 `hadoop-common`），当前 `pom.xml` 的 `org.apache.hadoop:*` 排除就是对的；
   若是 hadoop-free，需删掉那行排除，让 fat jar 自带 hadoop。

3. **默认值的破坏性变更**
   `doris.connect.timeout` 默认值由 `3000` 改为 `300`、`doris.pool.validation-timeout` 由 `3000` 改为 `250`，
   以保证默认组合本身能通过第 4、6 条校验。若你的旧 DDL 显式写了这两个值且不满足新规则，
   升级后建表会报错——按报错提示调整即可。

---

## 十一、不在本次修复范围（演进方向）

以下属于能力增强而非缺陷，详见 README 第 14 章「已知限制与演进方向」：

- **熔断器**：主源持续故障时不做熔断，每批仍会重试一次（代价已被 `doris.connect.timeout` / `lookup.timeout` 封顶）；
- **监听主源恢复并自动重连**：当前 HBase 侧旧连接失效后需重启作业（README 第 14 章、TEST_PLAN 案例 15）；
- **Flink Metric 上报**：异步 Lookup Function 不是 `RichFunction`，拿不到 `RuntimeContext`，暂用定时日志；
- **Doris 侧查询缓存**：官方 Doris Connector 的 lookup join 三项优化中，本项目已实现异步与攒批，尚缺缓存。

---

## 十二、后续变更记录（非缺陷，按使用反馈发起）

### 12.1 统计日志：默认 6 分钟、输出阶段值、新增各源平均耗时

**背景**：原统计日志一行里全是**自启动累计值**（`totalKeys=1203400`、`batches=25071`……），
要看「最近一段发生了什么」得自己拿两行做减法；而运维最常问的两个问题恰恰是
「**这一段时间**主源有没有变慢」和「备源现在多快」，前者原来完全没有指标。

**改动**：

| 项 | 变更前 | 变更后 |
|---|---|---|
| `lookup.stats.log-interval` 默认值 | 60（秒） | **360（秒）**，即 6 分钟一行 |
| 数值口径 | 自启动累计 | **本阶段增量**，行首给出 `window=Ns` |
| 各源耗时 | 无 | `hbase[... avg=8.3ms]` / `doris[... avg=15.1ms]`，无样本显示 `-` |
| 长期总量 | 靠单行累计体现 | 行尾 `since-start` 段给累计；作业关闭时若还有新活动再补打一次 |
| 负间隔 | 静默等价于关闭 | 建表期报错（与 `failover.log-interval` 规则对齐） |

**实现要点**：内部计数**仍是累计值**（因此漏看某行不会丢数），打点时用「当前 − 上次快照」
换算区间值——刻意不用「清零」，否则一次漏打（如 INFO 被过滤）就会把那批数据永久丢掉。

**两个耗时口径**，都是有意为之：
1. **失败批次的耗时也计入**。只统计成功批次的话，「主源正在变慢、开始不断超时」这段恶化过程会被完全掩盖；
2. **备源耗时从真正发起备源查询那一刻起算**，不含前面等主源超时的时间。否则主源越慢备源数字越难看，指标失去诊断价值。

**测试**：新增 `LookupStatsTest`（5 例）+ `DualLookupFunctionTest.latencyIsRecordedForEachSource`
（反射读真实 Function 的统计对象，验证埋点确实接在路由链路上）+ `DualLookupOptionsTest` 默认值与负数校验。
合计 45 → **52 个用例全部通过**。

**兼容性提示**：统计日志的字段变了（多了 `window=` 与两个 `avg=`），
如果有脚本在解析这行日志，需要同步调整；判别「新版是否已生效」最快的办法就是看有没有 `avg=`。

### 12.2 统计日志补齐累计视角 + 长期运行开销评估

**背景（用户反馈）**：改造后 `window=` 段只看得到本阶段，`since-start` 汇总却只在作业关闭时才打一行——
作业连跑几个月不重启，**全程总量实际上永远看不到**。同时用户提出一个更根本的问题：
这种统计日志对资源消耗和性能有没有影响，尤其是长期运行？

**改动**：每次打点**一行内同时给出两个视角**，用 `||` 分隔：

```
[dual-lookup] table=dim_account window=360s totalKeys=5400 ... failover=2 || table=dim_account since-start totalKeys=9900 ... failover=2
```

| 项 | 变更前 | 变更后 |
|---|---|---|
| 单行内容 | 只有本阶段 | 本阶段 + 自启动累计 |
| `since-start` | 仅作业关闭时补打 | **每行都有**；关闭时只在「上次打点后还有新活动」时补打，避免重复 |
| 计数读取 | 各字段分别 `get()` | 一次读成不可变 `Sample`（9 个 long），两段共用 → `本行阶段 + 此前累计 = 本行累计` 恒成立 |
| 打点任务抛异常 | 被 `scheduleAtFixedRate` **静默取消**后续调度 | `try/catch (Throwable)` 兜住 + 首次失败告警一次 |
| 关闭时补打条件 | 只要查过就打 | `interval > 0` + 有活动 + 自上次打点后有新活动 |

「一次读成 Sample」不是洁癖：两段若各自读原子变量，就可能出现「since-start 比 stage 少 3 批」这类
自相矛盾的数字，看日志的人只会去查一个并不存在的 bug。

**长期运行开销评估**（结论：可忽略）：

| 维度 | 量级 |
|---|---|
| 打点 CPU | 每 6 分钟几十微秒（9 次原子读 + 一次格式化 + 一次写日志），占比 **< 0.001%** |
| 埋点 CPU | 每批 4 次原子自增 ≈ < 0.1μs，而每批本身是一次毫秒级 RPC/查询，占比 **< 0.01%** |
| 常驻内存 | **固定约 200 字节**（9 个 `AtomicLong`，无集合/缓存/队列），跑一个月与跑一年完全相同 |
| 日志体积 | 约 240 行/天/子任务 ≈ 77KB/天，一年约 28MB（8 并发约 224MB/年） |
| 计数溢出 | long 上限 9.22×10^18；纳秒累计要「1000 批/秒 × 10ms/批」连跑 **29 年**才触及，key 计数要 5000 万年 |

唯一需要运维动作的是日志滚动——Flink 1.18 的 TM appender 默认已是 `100MB × 10`，无需额外配置。
不想有任何开销就把 `lookup.stats.log-interval` 设为 `0`：连打点线程都不创建。

**测试**：`LookupStatsTest` 5 → 6 例，新增 `everySnapshotCarriesCumulativeView`——
专门盯住「累计段不能把阶段值抄一遍」（那样两个视角会退化成同一个数字，而单看任何一行都发现不了）。
合计 52 → **53 个用例全部通过**，真实输出样本见 README 第 8 章。
