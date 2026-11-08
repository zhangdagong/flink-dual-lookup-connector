package com.roc.flink.connector.dual;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
 * <p><b>三条不可妥协的约束</b>（都源于「告警不能影响主链路」）：
 * <ol>
 *   <li><b>调用线程绝不做网络 IO</b>。{@code KafkaProducer.send()} 在元数据不可用时会阻塞
 *       {@code max.block.ms}（默认 60 秒！）——若在降级回调线程里直接调用，HBase 故障叠加
 *       Kafka 不可达就会把维表查询整条链路拖死。因此 {@link #send} 只负责入队，真正的发送
 *       在独立守护线程上执行。</li>
 *   <li><b>生产者惰性创建</b>。构造 {@code KafkaProducer} 会拉起 sender 线程并做元数据初始化；
 *       没触发过告警的作业不该为此付出任何代价，所以推迟到第一次真正要发消息时再建。</li>
 *   <li><b>队列有界、满了就丢</b>。Kafka 长时间不可用而故障持续时，宁可丢告警也不能让内存
 *       或线程堆积；丢弃会打一条 WARN 并计数，丢了多少是可查的。</li>
 * </ol>
 *
 * <p>其余取舍：
 * <ul>
 *   <li>{@code acks} 默认 1（leader 落盘即返回）：告警不需要金融级的持久性，却要求尽快发出；</li>
 *   <li>显式 {@code enable.idempotence=false}：幂等要求 {@code acks=all}，与 acks=1 冲突会直接
 *       让生产者构造失败（kafka-clients 3.x 默认开启幂等）；</li>
 *   <li>显式 {@code compression.type=none}：因此可以在打包时排除 zstd/lz4/snappy 三个压缩库，
 *       省下约 8MB——这是本连接器唯一能压缩 jar 体积的地方（见 pom 注释）。</li>
 * </ul>
 */
final class KafkaAlertSink implements FailoverAlertNotifier.Sink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAlertSink.class);

    private static final String PREFIX = "[dual-lookup] ";

    /** 待发告警的队列容量：告警是低频事件，能堆 8 条说明 Kafka 已经不可达了 */
    private static final int QUEUE_CAPACITY = 8;
    /** 等元数据的上限；只在告警线程上阻塞，不能设太长以免故障恢复后队列迟迟排不空 */
    private static final int MAX_BLOCK_MS = 5_000;
    /** 单条消息的整体投递上限（必须 >= request.timeout.ms） */
    private static final int DELIVERY_TIMEOUT_MS = 15_000;

    private final String bootstrapServers;
    private final String topic;
    private final String acks;

    private final ExecutorService executor;
    /** 惰性创建的生产者；volatile 供无锁快路径读取 */
    private volatile KafkaProducer<String, String> producer;
    /** 创建失败只报一次，避免每次告警都刷同一条 ERROR */
    private final AtomicBoolean createFailureReported = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    /** 因队列满被丢弃的告警条数（只在首次丢弃时告警） */
    private final AtomicInteger dropped = new AtomicInteger();

    KafkaAlertSink(String bootstrapServers, String topic, String acks) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.acks = acks;
        // 单线程 + 有界队列：保证告警按触发顺序发出，且积压量有上限
        this.executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(QUEUE_CAPACITY),
                r -> {
                    Thread t = new Thread(r, "dual-lookup-alert");
                    t.setDaemon(true);
                    return t;
                },
                (r, pool) -> {
                    // 不做 CallerRuns：那会让入队线程（降级回调）去发 Kafka，正好违反约束 1
                    if (dropped.incrementAndGet() == 1) {
                        LOG.warn(PREFIX + "告警队列已满（Kafka 不可达或极慢），本次告警被丢弃；"
                                + "丢一条告警不影响维表查询，恢复后统计日志里的降级计数仍然准确");
                    }
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
            // 线程池已关闭等极端情况：吞掉，绝不让告警把降级链路带崩
        }
    }

    private void deliver(String payload) {
        try {
            KafkaProducer<String, String> p = producer();
            if (p == null) {
                return;
            }
            p.send(new ProducerRecord<>(topic, payload), (metadata, error) -> {
                if (error != null) {
                    LOG.warn(PREFIX + "告警投递失败（topic={}）：{}", topic, error.toString());
                } else if (LOG.isDebugEnabled()) {
                    // 成功路径放到 DEBUG：告警本身低频，但 Kafka 客户端在 INFO 下已经够吵
                    LOG.debug(PREFIX + "告警已投递 topic={} partition={} offset={}",
                            topic, metadata.partition(), metadata.offset());
                }
            });
        } catch (Throwable t) {
            if (createFailureReported.compareAndSet(false, true)) {
                LOG.warn(PREFIX + "告警生产者不可用，后续告警将静默跳过（不影响维表查询）：{}", t.toString());
            }
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
                producer = new KafkaProducer<>(producerProps(bootstrapServers, acks));
            }
            return producer;
        }
    }

    /**
     * 生产者参数。抽成静态方法是为了让单元测试能拿「真实参数」去构造一个 Producer：
     * 这样配置是否合法、以及「排除压缩库后类还能不能加载」都能在没有 broker 的环境里验证。
     */
    static Properties producerProps(String bootstrapServers, String acks) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        // 幂等要求 acks=all，与默认的 acks=1 冲突；显式关掉，避免 kafka-clients 3.x 直接拒绝启动
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        // 必须与「打包时排除压缩库」保持一致：改成别的压缩算法会在运行期 NoClassDefFoundError
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        // 告警允许少量重试；重试太多会让一条过期告警在故障恢复后才到达，反而误导
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "dual-lookup-alert");
        return props;
    }

    /**
     * 尽力冲刷并关闭。重复调用安全（作业关闭可能被触发两次）。
     *
     * <p>顺序是「先停队列、再关生产者」：{@code shutdown()} 让已入队的消息有机会跑完，
     * 然后由 {@code close(Duration)} 冲刷在途消息——<b>不要额外调用 {@code flush()}</b>：
     * 它没有超时上限（最坏等到 {@code delivery.timeout.ms=15s}），会让「关闭有上限」的承诺失效。
     * 整个过程受 {@code timeoutMs} 约束——告警不值得让作业取消多等。
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
        }
        KafkaProducer<String, String> p = producer;
        if (p == null) {
            return; // 从未触发过告警：不创建生产者，也就没有资源要释放
        }
        try {
            p.close(Duration.ofMillis(timeoutMs));
        } catch (Throwable t) {
            LOG.debug(PREFIX + "告警生产者关闭异常（已忽略）：{}", t.toString());
        }
    }
}
