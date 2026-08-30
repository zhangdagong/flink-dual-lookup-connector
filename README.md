# Flink Dual Lookup Connector

HBase / Doris 双源容错的 Flink SQL 维表连接器。

**核心逻辑只有一句话**：Kafka 流进入后，优先查主源（默认 HBase），若查询超时（默认 500ms）、服务异常、表异常，则改查备源（Doris）。全程对 SQL 透明。

## 1. 环境要求

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 1.8 | 编译与运行 |
| Flink | 1.18.1 | 用了 `AsyncLookupFunction` 抽象类 API |
| HBase | 2.6.2 | 依赖 `hbase-shaded-client 2.6.2-hadoop3`，走原生异步客户端 |
| Doris | 4.0.8 | 走 MySQL 协议（JDBC + HikariCP） |

## 2. 快速开始

### 2.1 编译部署

```bash
mvn clean package
# 产物是单个 fat jar：已把 HBase 客户端、MySQL 驱动、HikariCP 全部打进去
cp target/flink-dual-lookup-connector-1.0.0.jar $FLINK_HOME/lib/
# 重启集群生效
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```

> fat jar 体积较大（几十 MB，主要来自 hbase-shaded-client）。若集群 lib 里已有 HBase 客户端，
> 可能产生类冲突——不过 hbase-shaded-client 本身已做了包名 relocate，一般无碍；真有冲突再改用"瘦 jar + 单独放依赖"。

### 2.2 使用（完整示例见 `sql/demo.sql`）

```sql
-- 1) 维表 DDL：HBase 与 Doris 中同名同构的 dim_account
CREATE TABLE dim_account (
    account_no  STRING,
    cust_name   STRING,
    risk_level  STRING,
    balance     DECIMAL(18,2),
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH (
    'connector'              = 'dual-lookup',
    'lookup.primary'         = 'hbase',     -- 主源；改 'doris' 即对调
    'lookup.timeout'         = '500 ms',    -- 主源超时阈值
    'hbase.zookeeper.quorum' = 'localhost',
    'doris.jdbc-url'         = 'jdbc:mysql://192.168.214.128:9030/risk'
);

-- 2) 算子层兜底超时必须大于 lookup.timeout
SET 'table.exec.async-lookup.timeout' = '10s';

-- 3) 关联：async='true' 必须写
--    hint 的 'table' 要和 FROM 里的表引用名一致：这里维表别名是 d，所以写 'd'
SELECT /*+ LOOKUP('table'='d','async'='true','capacity'='200') */
    t.trans_jnls_no, t.account_no, t.amount, d.cust_name, d.risk_level
FROM kafka_trans t
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t.proc_time d
ON t.account_no = d.account_no;
```

## 3. 参数

> 所有时间类参数统一用「毫秒整数」，直接写数字，无需带 `ms` 等单位。

| 参数 | 默认 | 说明 |
|---|---|---|
| `lookup.primary` | `hbase` | 主源，`hbase` 或 `doris`；另一个自动成为备源 |
| `lookup.timeout` | `500` | 单次（批量）查询硬超时（毫秒），超时即判该源故障、转查备源 |
| `lookup.batch.size` | `50` | 攒批条数阈值：攒满就批量查一次，降低 Doris 交互次数 |
| `lookup.batch.max-wait` | `5` | 攒批最大等待（毫秒），攒不满时最多等这么久就发，避免低流量下延迟无界 |
| `lookup.stats.log-interval` | `60` | 统计日志间隔（秒），0 关闭 |

**HBase 参数**（表名缺省取 DDL 表名）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `hbase.zookeeper.quorum` | — | ZK 地址；留空则用 classpath 下 `hbase-site.xml` |
| `hbase.zookeeper.property.clientPort` | `2181` | ZK 端口 |
| `zookeeper.znode.parent` | `/hbase` | ZK 根节点 |
| `hbase.table-name` | DDL 表名 | HBase 表名，支持「命名空间:表名」，如 `dim:dim_account` |
| `hbase.column-family` | `info` | 列族，DDL 所有列都从该列族下按列名读 |
| `hbase.rowkey.delimiter` | `\|` | 复合主键拼接 rowkey 的分隔符 |
| `hbase.rpc.timeout` | `300` | 单次 RPC 超时（毫秒） |
| `hbase.client.operation.timeout` | `500` | 客户端整体操作超时（毫秒），含 region 冷启动定位，建议给足 |
| `hbase.client.retries.number` | `1` | 重试次数，实时链路建议 0~1 |

**Doris 参数**（`doris.jdbc-url` 必填，表名缺省取 DDL 表名）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.jdbc-url` | — | 如 `jdbc:mysql://host:9030/dim`（库名也可写在 URL 里） |
| `doris.table-name` | DDL 表名 | Doris 表名，支持「库名.表名」，如 `dim.dim_account` |
| `doris.username` / `doris.password` | `root` / 空 | 账号密码 |
| `doris.query.timeout` | `1` | JDBC queryTimeout（秒），服务端侧兜底 |

**Doris 连接池（HikariCP）调优项**：

| 参数 | 默认 | 说明 |
|---|---|---|
| `doris.pool.size` | `8` | 最大连接数 |
| `doris.pool.min-idle` | `2` | 最小空闲连接数 |
| `doris.connect.timeout` | `3000` | 获取连接超时（毫秒） |
| `doris.pool.idle-timeout` | `600000` | 连接空闲多久被回收（毫秒） |
| `doris.pool.max-lifetime` | `1800000` | 连接最大存活时间（毫秒），须大于 idle-timeout |
| `doris.pool.validation-timeout` | `3000` | 连接有效性校验超时（毫秒） |

## 3.5 内置元字段（可选，输出来源与耗时）

在维表 DDL 里声明下面两个**约定名**的字段，即可在查询结果里拿到每次 Lookup 的元信息。这两个字段是连接器内置的，**不需要**在 HBase / Doris 表里存在，也无需配置。

| 字段名 | 类型 | 含义 |
|---|---|---|
| `lookup_source` | STRING | 本次数据实际来自哪个源，取值 `hbase` 或 `doris`（降级后是备源） |
| `lookup_cost_ms` | BIGINT | 本次查询耗时（毫秒），从发起到返回，含降级耗时 |

```sql
CREATE TABLE dim_account (
    account_no     STRING,
    cust_name      STRING,
    lookup_source  STRING,   -- 输出数据来源
    lookup_cost_ms BIGINT,   -- 输出查询耗时
    PRIMARY KEY (account_no) NOT ENFORCED
) WITH ('connector' = 'dual-lookup', ...);

SELECT d.cust_name, d.lookup_source, d.lookup_cost_ms
FROM kafka_trans t
LEFT JOIN dim_account FOR SYSTEM_TIME AS OF t.proc_time d
ON t.account_no = d.account_no;
```

说明：主源命中时 `lookup_source` 是主源、`lookup_cost_ms` 是主源耗时；主源超时降级后，`lookup_source` 变为备源、`lookup_cost_ms` 是主源超时 + 备源查询的总耗时；主源查不到（空结果）时两者均为 NULL（LEFT JOIN 补 NULL）。

## 4. 关键设计

### 4.1 超时机制

| 层 | 位置 | 作用 |
|---|---|---|
| 客户端层 | `hbase.rpc.timeout` / `hbase.client.operation.timeout` / `doris.query.timeout` | HBase 客户端内部超时（单次 RPC / 整体操作），让后端及时中止、释放资源 |
| 业务层 | `lookup.timeout`（本连接器） | **独立的硬超时**，超过即判定该源故障、触发降级，与客户端内部超时无关 |
| 算子层 | `table.exec.async-lookup.timeout` | 最终兜底，经验值 = `lookup.timeout × 2 + 2s` |

> 关键：`lookup.timeout` 是业务层独立控制的降级阈值，不受 HBase 客户端超时影响。
> `hbase.client.operation.timeout` 要给足（建议 2000ms），因为 HBase 客户端**冷启动**时定位 region
> （`waiting for region location`）就要数百毫秒——本连接器已在建连时预热 region location，
> 把这部分耗时挪出查询路径，避免首条查询误判超时降级。

### 4.2 Doris 超时要真正中断后端

JDBC 是阻塞 API，超时不能靠 `thread.interrupt`（JDBC 不响应）。本连接器在超时时调用 `Statement.cancel()`，让 Doris 真正中止这条 SQL，否则慢查询会占满连接池。

### 4.3 查不到数据 ≠ 故障

主源查不到某个 key 返回空列表，是正常业务结果，**不会**触发切换。只有超时、异常才算故障。

### 4.4 惰性建连

主源在作业启动时不可用（如 HBase 整体宕机），作业也能正常起来直接走备源，无需等主源恢复。

## 5. 故障演练

```bash
# 1) 停 HBase：作业不失败，数据持续输出（走 Doris）
stop-hbase.sh

# 2) 注入网络延迟制造超时（600ms > lookup.timeout=500ms）
tc qdisc add dev eth0 root netem delay 600ms
# 恢复
tc qdisc del dev eth0 root netem

# 3) 两源都挂：作业失败并抛异常（不静默丢数据，属预期行为）
```

## 6. 文件清单

```
src/main/java/com/roc/flink/connector/dual/
├── DualLookupOptions.java          # 配置项 + 配置值对象
├── RowDataConverter.java           # Flink 内部类型 ↔ Java 对象
├── LookupReader.java               # 单源读取抽象（接口）
├── HBaseLookupReader.java          # HBase 异步查询 + 超时
├── DorisLookupReader.java          # Doris JDBC + 连接池 + 超时
├── DualLookupFunction.java         # 异步 Lookup Function（主备路由核心）
├── DualLookupTableSource.java      # LookupTableSource
└── DualLookupTableSourceFactory.java  # SPI 工厂
```

## 7. 已知限制

- 每次查询都是单条点查，未做攒批（`WHERE pk IN` / HBase 批量 Get）。
- 不支持 Kerberos 认证的 HBase。
- HBase 侧要求所有列在同一列族。
