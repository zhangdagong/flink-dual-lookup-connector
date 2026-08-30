package com.roc.flink.connector.dual;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 双源容错的异步 Lookup Function，带攒批。
 *
 * <p>核心逻辑：把一条条查询攒成批（攒满 {@code lookup.batch.size} 条、或最多等
 * {@code lookup.batch.max-wait} 毫秒），一次性批量查主源（默认 HBase）；批量超时或异常
 * 则批量降级查备源（Doris）；查不到（空结果）不算异常、不降级。
 *
 * <p>Flink 1.18 说明：异步查找继承 {@link AsyncLookupFunction}，实现 {@code asyncLookup(RowData)}。
 * 基类的 {@code eval(...)} 是 final，不要覆盖。主备两个 Reader 采用惰性建连。
 */
public class DualLookupFunction extends AsyncLookupFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DualLookupFunction.class);

    private final DualLookupOptions.Config cfg;
    private final String tableName;
    private final RowDataConverter converter;
    private final LogicalType[] keyTypes;
    private final LookupReader primary;
    private final LookupReader standby;

    // ---- 攒批状态（transient，运行时在 ensureOpened 里初始化） ----
    private transient Object lock;
    private transient List<Object[]> batchKeys;
    private transient List<CompletableFuture<Collection<RowData>>> batchFutures;
    private transient ScheduledExecutorService batchScheduler;
    private transient boolean flushScheduled;
    private transient LookupStats stats;
    private transient volatile boolean initialized;

    public DualLookupFunction(DualLookupOptions.Config cfg,
                              String tableName,
                              RowDataConverter converter,
                              LogicalType[] keyTypes,
                              LookupReader primary,
                              LookupReader standby) {
        this.cfg = cfg;
        this.tableName = tableName;
        this.converter = converter;
        this.keyTypes = keyTypes;
        this.primary = primary;
        this.standby = standby;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        ensureOpened();
    }

    private synchronized void ensureOpened() {
        if (initialized) {
            return;
        }
        lock = new Object();
        batchKeys = new ArrayList<>();
        batchFutures = new ArrayList<>();
        batchScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dual-lookup-batch");
            t.setDaemon(true);
            return t;
        });
        stats = new LookupStats(tableName, cfg.primary, cfg.standby, cfg.statsLogIntervalSec);
        stats.start();
        initialized = true;
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        ensureOpened();

        Object[] keys = toJavaKeys(extractKeyValues(keyRow));
        CompletableFuture<Collection<RowData>> f = new CompletableFuture<>();

        boolean needFlush;
        synchronized (lock) {
            batchKeys.add(keys);
            batchFutures.add(f);
            if (batchKeys.size() >= cfg.batchSize) {
                needFlush = true;
            } else {
                needFlush = false;
                if (!flushScheduled) {
                    flushScheduled = true;
                    batchScheduler.schedule(this::flush, cfg.batchMaxWaitMs, TimeUnit.MILLISECONDS);
                }
            }
        }
        if (needFlush) {
            flush(); // 锁外触发，避免批量 IO 持锁阻塞后续入队
        }
        return f;
    }

    /** 取出当前攒的整批，锁外执行批量查询 */
    private void flush() {
        List<Object[]> keys;
        List<CompletableFuture<Collection<RowData>>> futures;
        synchronized (lock) {
            if (batchKeys.isEmpty()) {
                return;
            }
            keys = new ArrayList<>(batchKeys);
            futures = new ArrayList<>(batchFutures);
            batchKeys.clear();
            batchFutures.clear();
            flushScheduled = false;
        }
        doBatchLookup(keys, futures);
    }

    /** 批量查主源，失败/超时批量降级备源，最后按 key 分发 */
    private void doBatchLookup(List<Object[]> keys, List<CompletableFuture<Collection<RowData>>> futures) {
        long startNanos = System.nanoTime();
        stats.recordPrimaryBatch(keys.size());

        call(primary, keys).whenComplete((resultMap, ex) -> {
            if (ex == null) {
                distribute(futures, resultMap, cfg.primary, startNanos);
                return;
            }
            Throwable cause = unwrap(ex);
            stats.recordPrimaryFail();
            stats.recordFailover();
            LOG.warn("[dual-lookup] 主源 {} 批量查询失败，降级到备源 {}。批大小={}，原因：{}",
                    cfg.primary, cfg.standby, keys.size(), cause.toString());

            stats.recordStandbyBatch();
            call(standby, keys).whenComplete((rm2, ex2) -> {
                if (ex2 != null) {
                    stats.recordStandbyFail();
                    for (CompletableFuture<Collection<RowData>> f : futures) {
                        f.completeExceptionally(unwrap(ex2));
                    }
                } else {
                    distribute(futures, rm2, cfg.standby, startNanos);
                }
            });
        });
    }

    /** 把批量结果按下标分发到各 future，并附上来源/耗时元信息 */
    private void distribute(List<CompletableFuture<Collection<RowData>>> futures,
                            Map<Integer, List<Map<String, Object>>> resultMap,
                            String source, long startNanos) {
        long costMs = (System.nanoTime() - startNanos) / 1_000_000L;
        for (int i = 0; i < futures.size(); i++) {
            List<Map<String, Object>> rows = resultMap.get(i);
            if (rows == null || rows.isEmpty()) {
                futures.get(i).complete(Collections.emptyList());
                continue;
            }
            for (Map<String, Object> row : rows) {
                row.put(RowDataConverter.META_SOURCE_KEY, source);
                row.put(RowDataConverter.META_COST_KEY, costMs);
            }
            futures.get(i).complete(toRows(rows));
        }
    }

    /** 惰性建连 + 批量查询 */
    private CompletableFuture<Map<Integer, List<Map<String, Object>>>> call(
            LookupReader reader, List<Object[]> keys) {
        if (!reader.isOpened()) {
            try {
                reader.open(); // open() 本身 synchronized 且幂等
            } catch (Exception e) {
                CompletableFuture<Map<Integer, List<Map<String, Object>>>> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        }
        return reader.batchLookupAsync(keys);
    }

    @Override
    public void close() throws Exception {
        if (stats != null) {
            stats.close();
        }
        if (batchScheduler != null) {
            batchScheduler.shutdownNow();
        }
        primary.close();
        standby.close();
        initialized = false;
    }

    // ---------------- 键值转换 ----------------

    private Object[] extractKeyValues(RowData keyRow) {
        Object[] keys = new Object[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            keys[i] = getField(keyRow, i, keyTypes[i]);
        }
        return keys;
    }

    private Object getField(RowData row, int pos, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case DECIMAL: {
                DecimalType dt = (DecimalType) type;
                return row.getDecimal(pos, dt.getPrecision(), dt.getScale());
            }
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
                TimestampType tt = (TimestampType) type;
                return row.getTimestamp(pos, tt.getPrecision());
            }
            case BINARY:
            case VARBINARY:
                return row.getBinary(pos);
            default:
                return row.getString(pos);
        }
    }

    private Object[] toJavaKeys(Object[] keys) {
        Object[] out = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            out[i] = converter.toJavaValue(keys[i], keyTypes[i]);
        }
        return out;
    }

    private Collection<RowData> toRows(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<RowData> out = new ArrayList<>(rows.size());
        for (Map<String, Object> r : rows) {
            out.add(converter.toRowData(r));
        }
        return out;
    }

    private static Throwable unwrap(Throwable t) {
        return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
    }
}
