package com.roc.flink.connector.dual;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 把告警 JSON 投递到 Kafka 的旁路实现。
 *
 * <p><b>四条不可妥协的约束</b>（都源于「告警不能影响主链路」）：
 * <ol>
 *   <li><b>调用线程绝不做网络 IO</b>。{@code KafkaProducer.send()} 在元数据不可用时会阻塞
 *       {@code max.block.ms}；若在降级回调线程里直接调用，HBase 故障叠加 Kafka 不可达就会把
 *       维表查询整条链路拖死。因此 {@link #send} 只负责入队，真正的发送在独立守护线程上执行。</li>
 *   <li><b>生产者惰性创建</b>。没触发过告警的作业不该为此付出任何代价。</li>
 *   <li><b>队列有界、满了就丢</b>。Kafka 长时间不可用而故障持续时，宁可丢告警也不能让内存
 *       或线程堆积；队列满时只 WARN/计数，最终投递失败才尽量写入本地死信。</li>
 *   <li><b>失败只记录，不冒泡</b>。任何网络/认证/topic/序列化异常都只影响告警。</li>
 * </ol>
 */
final class KafkaAlertSink implements FailoverAlertNotifier.Sink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAlertSink.class);

    private static final String PREFIX = "[dual-lookup] ";
    private static final int DEFAULT_QUEUE_CAPACITY = 8;
    private static final int DEFAULT_MAX_BLOCK_MS = 5_000;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_DELIVERY_TIMEOUT_MS = 15_000;
    private static final int DEFAULT_PRODUCER_RETRIES = 2;
    private static final String DEFAULT_CLIENT_ID = "dual-lookup-alert";

    private final String bootstrapServers;
    private final String topic;
    private final String acks;
    private final String clientId;
    private final Map<String, String> extraProperties;
    private final int maxBlockMs;
    private final int requestTimeoutMs;
    private final int deliveryTimeoutMs;
    private final int producerRetries;
    private final int retryMax;
    private final int retryBackoffMs;
    private final int retryBackoffMaxMs;
    private final String spoolDir;
    private final long spoolMaxBytes;
    private final AlertMetrics metrics;

    private final ExecutorService executor;
    /** 惰性创建的生产者；volatile 供无锁快路径读取 */
    private volatile KafkaProducer<String, String> producer;
    private final AtomicBoolean closed = new AtomicBoolean();
    /** 因队列满/最终失败被丢弃的告警条数 */
    private final AtomicInteger dropped = new AtomicInteger();

    /** 测试与默认路径：所有可调参数取默认值 */
    KafkaAlertSink(String bootstrapServers, String topic, String acks) {
        this(bootstrapServers, topic, acks, DEFAULT_CLIENT_ID, Collections.<String, String>emptyMap(),
                DEFAULT_QUEUE_CAPACITY, DEFAULT_MAX_BLOCK_MS, DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_DELIVERY_TIMEOUT_MS, DEFAULT_PRODUCER_RETRIES, 0, 0, 0,
                "", 0L, AlertMetrics.NOOP);
    }

    /** 生产路径：从已校验的 DDL 配置装配 */
    KafkaAlertSink(DualLookupOptions.Config cfg, String clientId, AlertMetrics metrics) {
        this(cfg.alertKafkaBootstrap, cfg.alertKafkaTopic, cfg.alertKafkaAcks, clientId,
                cfg.alertKafkaProperties, cfg.alertKafkaQueueCapacity, cfg.alertKafkaMaxBlockMs,
                cfg.alertKafkaRequestTimeoutMs, cfg.alertKafkaDeliveryTimeoutMs,
                cfg.alertKafkaRetries, cfg.alertRetryMax, cfg.alertRetryBackoffMs,
                cfg.alertRetryBackoffMaxMs, cfg.alertSpoolDir, cfg.alertSpoolMaxBytes, metrics);
    }

    private KafkaAlertSink(String bootstrapServers, String topic, String acks, String clientId,
                           Map<String, String> extraProperties, int queueCapacity, int maxBlockMs,
                           int requestTimeoutMs, int deliveryTimeoutMs, int producerRetries,
                           int retryMax, int retryBackoffMs, int retryBackoffMaxMs,
                           String spoolDir, long spoolMaxBytes, AlertMetrics metrics) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.acks = acks;
        this.clientId = clientId == null || clientId.trim().isEmpty() ? DEFAULT_CLIENT_ID : clientId;
        this.extraProperties = extraProperties == null
                ? Collections.<String, String>emptyMap() : extraProperties;
        this.maxBlockMs = maxBlockMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.deliveryTimeoutMs = deliveryTimeoutMs;
        this.producerRetries = producerRetries;
        this.retryMax = retryMax;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.spoolDir = spoolDir == null ? "" : spoolDir.trim();
        this.spoolMaxBytes = spoolMaxBytes;
        this.metrics = metrics == null ? AlertMetrics.NOOP : metrics;
        // 单线程 + 有界队列：保证告警按触发顺序发出，且积压量有上限
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(Math.max(1, queueCapacity)),
                r -> {
                    Thread t = new Thread(r, "dual-lookup-alert");
                    t.setDaemon(true);
                    return t;
                },
                (r, pool) -> {
                    // 不做 CallerRuns：那会让入队线程（降级回调）去发 Kafka，正好违反约束 1。
                    // 这里也不再同步写死信：磁盘 IO 同样可能在故障现场拖慢降级回调。
                    int total = dropped.incrementAndGet();
                    KafkaAlertSink.this.metrics.dropped();
                    LOG.warn(PREFIX + "告警队列已满，本次告警被丢弃（累计丢弃 {} 条）；不影响维表查询", total);
                });
    }

    @Override
    public void send(String payload) {
        if (closed.get()) {
            return;
        }
        try {
            executor.execute(() -> deliver(payload));
        } catch (Throwable t) {
            // 线程池已关闭等极端情况：只记录原因，绝不让告警把降级链路带崩
            metrics.failed();
            LOG.warn(PREFIX + "告警入队失败（不影响维表查询）：{}", t.toString());
            spool(payload, "入队失败: " + t);
        }
    }

    private void deliver(String payload) {
        Throwable last = null;
        int attempts = Math.max(0, retryMax) + 1;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (closed.get()) {
                return;
            }
            if (attempt > 0) {
                metrics.retry();
                if (!sleepBackoff(attempt)) {
                    spool(payload, "重试被中断");
                    return;
                }
            }
            KafkaProducer<String, String> p;
            try {
                p = producer();
            } catch (Throwable t) {
                last = t;
                metrics.failed();
                LOG.warn(PREFIX + "告警生产者创建失败（topic={}，第 {}/{} 次，不影响维表查询）：{}",
                        topic, attempt + 1, attempts, t.toString());
                continue;
            }
            try {
                // 在独立告警线程上同步等待 Future：这样才能对「投递失败」做有界重试和死信，
                // 同时不会阻塞降级回调线程。
                p.send(new ProducerRecord<>(topic, payload)).get(deliveryTimeoutMs, TimeUnit.MILLISECONDS);
                metrics.sent();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(PREFIX + "告警已投递 topic={}", topic);
                }
                return;
            } catch (Throwable t) {
                last = t;
                metrics.failed();
                LOG.warn(PREFIX + "告警投递失败（topic={}，第 {}/{} 次，不影响维表查询）：{}",
                        topic, attempt + 1, attempts, t.toString());
            }
        }
        spool(payload, last == null ? "未知错误" : last.toString());
    }

    private boolean sleepBackoff(int attempt) {
        long delay = retryBackoffMs;
        for (int i = 1; i < attempt && delay < retryBackoffMaxMs; i++) {
            delay = Math.min((long) retryBackoffMaxMs, delay * 2L);
        }
        delay = Math.min(delay, retryBackoffMaxMs);
        try {
            Thread.sleep(Math.max(0L, delay));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 最终失败后的本地死信：有界、尽力而为，任何 IO 异常都不能冒泡 */
    synchronized void spool(String payload, String reason) {
        if (spoolDir.isEmpty()) {
            metrics.dropped();
            return;
        }
        try {
            Path dir = Paths.get(spoolDir);
            Files.createDirectories(dir);
            String safe = clientId.replaceAll("[^A-Za-z0-9._-]", "_");
            if (safe.length() > 120) {
                safe = safe.substring(0, 120);
            }
            Path file = dir.resolve(safe + ".jsonl");
            if (Files.exists(file) && Files.size(file) >= spoolMaxBytes) {
                metrics.dropped();
                LOG.warn(PREFIX + "告警死信文件已达到上限 {} 字节，丢弃本条：{}", spoolMaxBytes, file);
                return;
            }
            String line = "{\"timestamp\":" + System.currentTimeMillis()
                    + ",\"clientId\":\"" + FailoverAlertNotifier.escapeJson(clientId)
                    + "\",\"error\":\"" + FailoverAlertNotifier.escapeJson(reason)
                    + "\",\"payload\":" + payload + "}\n";
            Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            metrics.spooled();
            LOG.warn(PREFIX + "告警已写入本地死信 {}（原因：{}）", file, reason);
        } catch (Throwable t) {
            metrics.dropped();
            LOG.warn(PREFIX + "告警本地死信写入失败（不影响维表查询）：{}", t.toString());
        }
    }

    /** 惰性创建生产者；并发调用只在第一次真正同步 */
    private KafkaProducer<String, String> producer() {
        KafkaProducer<String, String> p = producer;
        if (p != null) {
            return p;
        }
        synchronized (this) {
            if (producer == null) {
                producer = new KafkaProducer<>(producerProps(bootstrapServers, acks, clientId,
                        extraProperties, maxBlockMs, requestTimeoutMs, deliveryTimeoutMs, producerRetries));
            }
            return producer;
        }
    }

    /** 测试与默认路径：只给 bootstrap/acks */
    static Properties producerProps(String bootstrapServers, String acks) {
        return producerProps(bootstrapServers, acks, DEFAULT_CLIENT_ID,
                Collections.<String, String>emptyMap(), DEFAULT_MAX_BLOCK_MS, DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_DELIVERY_TIMEOUT_MS, DEFAULT_PRODUCER_RETRIES);
    }

    static Properties producerProps(String bootstrapServers, String acks, String clientId,
                                    Map<String, String> extraProperties, int maxBlockMs,
                                    int requestTimeoutMs, int deliveryTimeoutMs, int producerRetries) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        // 幂等要求 acks=all，与默认的 acks=1 冲突；显式关掉，避免 kafka-clients 3.x 直接拒绝启动
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        // 必须与「打包时排除压缩库」保持一致：改成别的压缩算法会在运行期 NoClassDefFoundError
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(ProducerConfig.RETRIES_CONFIG, producerRetries);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                clientId == null || clientId.trim().isEmpty() ? DEFAULT_CLIENT_ID : clientId);
        // 用户透传项最后写入，允许覆盖默认值（如 security.protocol/sasl.*/ssl.*）
        if (extraProperties != null) {
            for (Map.Entry<String, String> entry : extraProperties.entrySet()) {
                if (entry.getKey() != null && entry.getValue() != null) {
                    props.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return props;
    }

    /**
     * 尽力冲刷并关闭。重复调用安全（作业关闭可能被触发两次）。
     *
     * <p>顺序是「先停队列、再关生产者」；整个过程受 {@code timeoutMs} 约束，
     * 告警不值得让作业取消多等。
     */
    @Override
    public void close(long timeoutMs) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.shutdown();
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        } catch (Throwable t) {
            LOG.warn(PREFIX + "告警队列关闭异常（已忽略，不影响维表查询）：{}", t.toString());
        }
        KafkaProducer<String, String> p = producer;
        if (p == null) {
            return; // 从未触发过告警：不创建生产者，也就没有资源要释放
        }
        try {
            p.close(Duration.ofMillis(Math.max(1L, timeoutMs)));
        } catch (Throwable t) {
            LOG.warn(PREFIX + "告警生产者关闭异常（已忽略，不影响维表查询）：{}", t.toString());
        }
    }

}
