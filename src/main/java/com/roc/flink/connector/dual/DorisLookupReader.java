package com.roc.flink.connector.dual;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Doris 维表读取器（MySQL 协议 + HikariCP 连接池），批量查询。
 *
 * <p>批量把 N 个 key 合成一次 {@code WHERE pk IN (?,?,...)}（复合主键用 OR 拼接），
 * 大幅减少与 Doris 的交互次数，是提升 Doris 查询效率的关键。
 *
 * <p>JDBC 是阻塞 API，因此每次批量查询丢到独立线程池执行并做超时控制；
 * 超时靠 {@link Statement#cancel()} 让 Doris 侧真正中止 SQL，避免慢查询占满连接池。
 */
public class DorisLookupReader implements LookupReader {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisLookupReader.class);

    private final DualLookupOptions.Config cfg;
    private final String[] keyColumnNames;
    private final String selectPrefix;   // "SELECT `c1`,`c2` FROM `t` WHERE "

    private transient volatile boolean opened = false;
    private transient HikariDataSource dataSource;
    private transient ExecutorService queryExecutor;
    private transient ScheduledExecutorService timeoutScheduler;

    public DorisLookupReader(DualLookupOptions.Config cfg, String[] columnNames, String[] keyColumnNames) {
        this.cfg = cfg;
        this.keyColumnNames = keyColumnNames;
        this.selectPrefix = buildSelectPrefix(cfg.dorisTableName, columnNames);
    }

    /** 生成 "SELECT `c1`,`c2` FROM `库`.`表` WHERE "（WHERE 条件按批量大小动态拼接） */
    private static String buildSelectPrefix(String table, String[] columns) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append('`').append(columns[i]).append('`');
        }
        sb.append(" FROM ").append(quoteTable(table)).append(" WHERE ");
        return sb.toString();
    }

    /** 表名支持「库名.表名」：dim.dim_account → `dim`.`dim_account` */
    private static String quoteTable(String table) {
        int dot = table.indexOf('.');
        if (dot > 0 && dot < table.length() - 1) {
            return "`" + table.substring(0, dot) + "`.`" + table.substring(dot + 1) + "`";
        }
        return "`" + table + "`";
    }

    // ---------------- 生命周期 ----------------

    @Override
    public synchronized void open() throws Exception {
        if (opened) {
            return;
        }
        dataSource = createDataSource();

        // 线程数与连接池等大，保证拿到连接后不用排队等待
        queryExecutor = Executors.newFixedThreadPool(cfg.dorisPoolSize, r -> {
            Thread t = new Thread(r, "dual-lookup-doris-query");
            t.setDaemon(true);
            return t;
        });

        timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dual-lookup-doris-timeout");
            t.setDaemon(true);
            return t;
        });

        opened = true;
        LOG.info("[dual-lookup] Doris reader opened, url={}, table={}, poolSize={}",
                cfg.dorisJdbcUrl, cfg.dorisTableName, cfg.dorisPoolSize);
    }

    private HikariDataSource createDataSource() {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("dual-lookup-doris");
        hc.setJdbcUrl(cfg.dorisJdbcUrl);
        hc.setUsername(cfg.dorisUsername);
        hc.setPassword(cfg.dorisPassword);
        if (cfg.dorisDriver != null && !cfg.dorisDriver.trim().isEmpty()) {
            hc.setDriverClassName(cfg.dorisDriver.trim());
        }
        hc.setMaximumPoolSize(cfg.dorisPoolSize);
        hc.setMinimumIdle(cfg.dorisPoolMinIdle);
        hc.setConnectionTimeout(cfg.dorisConnectTimeoutMs);
        hc.setIdleTimeout(cfg.dorisPoolIdleTimeoutMs);
        hc.setMaxLifetime(cfg.dorisPoolMaxLifetimeMs);
        hc.setValidationTimeout(cfg.dorisPoolValidationTimeoutMs);
        return new HikariDataSource(hc);
    }

    @Override
    public boolean isOpened() {
        return opened;
    }

    @Override
    public void close() throws Exception {
        if (timeoutScheduler != null) {
            timeoutScheduler.shutdownNow();
            timeoutScheduler = null;
        }
        if (queryExecutor != null) {
            queryExecutor.shutdownNow();
            queryExecutor = null;
        }
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        opened = false;
    }

    // ---------------- 批量查询 ----------------

    @Override
    public CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys) {
        CompletableFuture<Map<Integer, List<Map<String, Object>>>> result = new CompletableFuture<>();

        // 过滤空 key（主键为空直接查不到），并建立「主键值 -> 下标」反查映射
        List<Object[]> validKeys = new ArrayList<>(keys.size());
        Map<String, Integer> keyIndex = new HashMap<>(keys.size() * 2);
        for (int i = 0; i < keys.size(); i++) {
            Object[] k = keys.get(i);
            if (k == null || k.length == 0 || k[0] == null) {
                continue;
            }
            keyIndex.put(joinKey(k), i);
            validKeys.add(k);
        }
        if (validKeys.isEmpty()) {
            result.complete(Collections.emptyMap());
            return result;
        }

        String sql = selectPrefix + buildBatchWhere(validKeys.size());

        AtomicReference<Statement> stmtRef = new AtomicReference<>();
        Future<?> task = queryExecutor.submit(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                stmtRef.set(ps);
                // JDBC queryTimeout 必须 <= lookup.timeout，否则 Doris 慢查询会在业务层超时降级后
                // 仍占用查询线程与连接，直到 JDBC 层才中止，从而把线程池和连接池打满。
                int jdbcTimeoutSec = Math.max(1, Math.min(cfg.dorisQueryTimeoutSec,
                        (int) Math.ceil(cfg.timeoutMs / 1000.0)));
                ps.setQueryTimeout(jdbcTimeoutSec);
                int idx = 1;
                for (Object[] k : validKeys) {
                    for (Object v : k) {
                        ps.setObject(idx++, v);
                    }
                }
                try (ResultSet rs = ps.executeQuery()) {
                    result.complete(groupByKey(rs, keyIndex));
                }
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });

        if (cfg.timeoutMs > 0) {
            timeoutScheduler.schedule(() -> {
                if (!result.isDone()) {
                    result.completeExceptionally(
                            new TimeoutException("Doris lookup timeout after " + cfg.timeoutMs + "ms"));
                }
            }, cfg.timeoutMs, TimeUnit.MILLISECONDS);
        }

        result.whenComplete((rows, ex) -> {
            Statement st = stmtRef.getAndSet(null);
            if (ex != null && st != null) {
                // 关键：让 Doris 侧终止这条 SQL，否则慢查询会一直占着连接
                try {
                    st.cancel();
                } catch (Exception ignore) {
                    // cancel 失败无所谓，连接最终会被关闭并归还
                }
                task.cancel(true);
            }
        });

        return result;
    }

    /** 单主键用 IN，复合主键用 OR 拼接（Doris 通用） */
    private String buildBatchWhere(int count) {
        StringBuilder sb = new StringBuilder(64);
        if (keyColumnNames.length == 1) {
            sb.append('`').append(keyColumnNames[0]).append("` IN (");
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('?');
            }
            sb.append(')');
        } else {
            for (int i = 0; i < count; i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append('(');
                for (int j = 0; j < keyColumnNames.length; j++) {
                    if (j > 0) {
                        sb.append(" AND ");
                    }
                    sb.append('`').append(keyColumnNames[j]).append("` = ?");
                }
                sb.append(')');
            }
        }
        return sb.toString();
    }

    /** 遍历结果集，按主键值反查原始下标，把行归组到对应 key */
    private Map<Integer, List<Map<String, Object>>> groupByKey(ResultSet rs, Map<String, Integer> keyIndex)
            throws Exception {
        Map<Integer, List<Map<String, Object>>> result = new HashMap<>();
        ResultSetMetaData md = rs.getMetaData();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(md.getColumnCount() * 2);
            for (int i = 1; i <= md.getColumnCount(); i++) {
                row.put(md.getColumnLabel(i).toLowerCase(), rs.getObject(i));
            }
            Integer idx = keyIndex.get(joinKeyFromRow(row));
            if (idx == null) {
                continue;
            }
            result.computeIfAbsent(idx, x -> new ArrayList<>()).add(row);
        }
        return result;
    }

    private String joinKey(Object[] key) {
        StringBuilder sb = new StringBuilder();
        for (Object v : key) {
            sb.append(v == null ? "" : v.toString()).append('\u0001');
        }
        return sb.toString();
    }

    private String joinKeyFromRow(Map<String, Object> row) {
        StringBuilder sb = new StringBuilder();
        for (String col : keyColumnNames) {
            Object v = row.get(col.toLowerCase());
            sb.append(v == null ? "" : v.toString()).append('\u0001');
        }
        return sb.toString();
    }
}
