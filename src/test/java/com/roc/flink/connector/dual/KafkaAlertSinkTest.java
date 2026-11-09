package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Kafka 旁路的「能起得来、起不来也不拖累主链路」测试。
 *
 * <p>这个类刻意不连真 broker，但仍然要构造一个真实的 {@link KafkaProducer}，因为要验证的是
 * 两件只有在真实客户端上才会暴露的事：
 * <ol>
 *   <li><b>参数组合是否被 kafka-clients 接受</b>。最典型的是 {@code acks=1} 与 kafka-clients 3.x
 *       默认开启的幂等冲突——这类错误只会在「第一次要发告警时」才抛，也就是故障现场；</li>
 *   <li><b>排除压缩库后类还能不能加载</b>。pom 里把 zstd/lz4/snappy 都排除了（省约 8MB），
 *       前提是 {@code compression.type=none} 时不会触碰这些类；测试用的 classpath 同样没有它们，
 *       等于把这条假设变成回归。</li>
 * </ol>
 */
public class KafkaAlertSinkTest {

    @Test
    public void producerConfigIsAcceptedByKafkaClient() {
        Properties props = KafkaAlertSink.producerProps("localhost:9092", "1");
        // 构造成功即说明：参数名合法、acks 与幂等不冲突、没有因缺少压缩库而 NoClassDefFoundError
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            assertNotNull(producer);
        } finally {
            // 不连接任何 broker，close 只是停掉 sender 线程
            producer.close(Duration.ofMillis(1000));
        }
    }

    /**
     * 最关键的一条约束：<b>{@code send()} 不能在被调用线程上做网络 IO</b>。
     *
     * <p>KafkaProducer 在拿不到元数据时会阻塞 {@code max.block.ms}（我们配成 5s，默认值是 60s）。
     * 若有人图省事把它改回同步调用，主源故障叠加 Kafka 不可达就会把维表查询整条链路拖死——
     * 这里用一个「连不上」的地址，直接量出调用耗时。
     */
    @Test
    public void sendReturnsImmediatelyEvenWhenBrokerIsUnreachable() {
        KafkaAlertSink sink = new KafkaAlertSink("localhost:9092", "dual_lookup_alert", "1");
        try {
            long start = System.currentTimeMillis();
            sink.send("{\"alert\":\"test\"}");
            long elapsed = System.currentTimeMillis() - start;
            assertTrue("send() 必须只是入队；耗时 " + elapsed
                    + "ms 说明它开始在调用线程上等 Kafka 元数据了", elapsed < 2_000);
        } finally {
            sink.close(200);
        }
    }

    @Test
    public void securityPropertiesAndClientIdArePassedThrough() {
        Properties props = KafkaAlertSink.producerProps("localhost:9092", "1", "client-1",
                Collections.singletonMap("security.protocol", "SASL_PLAINTEXT"),
                5000, 5000, 15000, 2);
        org.junit.Assert.assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
        org.junit.Assert.assertEquals("client-1", props.getProperty(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void deadLetterWritesBoundedJsonlFile() throws Exception {
        Path dir = Files.createTempDirectory("dual-alert-spool");
        Path file = dir.resolve("it-client.jsonl");
        try {
            Configuration c = new Configuration();
            c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
            c.setInteger(DualLookupOptions.ALERT_THRESHOLD.key(), 1);
            c.setString(DualLookupOptions.ALERT_KAFKA_BOOTSTRAP.key(), "localhost:9092");
            c.setString(DualLookupOptions.ALERT_KAFKA_TOPIC.key(), "dual_lookup_alert");
            c.setString(DualLookupOptions.ALERT_SPOOL_DIR.key(), dir.toString());
            DualLookupOptions.Config cfg = DualLookupOptions.Config.from(c, "it_table");

            KafkaAlertSink sink = new KafkaAlertSink(cfg, "it-client", AlertMetrics.NOOP);
            try {
                sink.spool("{\"alert\":\"test\"}", "boom\nreason");
            } finally {
                sink.close(100);
            }
            String line = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            assertTrue(line.contains("\"payload\":{\"alert\":\"test\"}"));
            assertTrue("原因必须转义成单行 JSON: " + line, line.contains("boom\\nreason"));
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    public void closeIsIdempotentAndSendAfterCloseIsIgnored() {
        KafkaAlertSink sink = new KafkaAlertSink("localhost:9092", "dual_lookup_alert", "1");
        sink.close(100);
        // 作业关闭可能被触发两次（取消 + 异常退出），不能因此抛异常
        sink.close(100);
        // 关闭后到达的告警直接丢弃，不再创建生产者
        sink.send("{\"alert\":\"late\"}");
    }
}
