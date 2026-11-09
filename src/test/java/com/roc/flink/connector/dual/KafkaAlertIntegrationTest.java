package com.roc.flink.connector.dual;

import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assume;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertTrue;

/**
 * 真实 Kafka 集成测试（默认跳过）。
 *
 * <p>启用方式：
 * <pre>
 *   mvn test -Ddual.lookup.kafka.bootstrap=localhost:9092 \
 *            -Ddual.lookup.kafka.topic=dual_lookup_alert_it
 * </pre>
 *
 * <p>要求目标 topic 已存在（或 broker 允许自动创建）。测试只验证「能经过真实客户端送达」，
 * 失败原因/重试/死信由单元测试覆盖，避免 CI 依赖外部 broker。
 */
public class KafkaAlertIntegrationTest {

    @Test
    public void alertReachesRealKafkaTopic() throws Exception {
        String bootstrap = System.getProperty("dual.lookup.kafka.bootstrap", "").trim();
        Assume.assumeTrue("未设置 -Ddual.lookup.kafka.bootstrap，跳过真实 Kafka 集成测试",
                !bootstrap.isEmpty());
        String topic = System.getProperty("dual.lookup.kafka.topic", "dual_lookup_alert_it");

        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setInteger(DualLookupOptions.ALERT_THRESHOLD.key(), 1);
        c.setString(DualLookupOptions.ALERT_KAFKA_BOOTSTRAP.key(), bootstrap);
        c.setString(DualLookupOptions.ALERT_KAFKA_TOPIC.key(), topic);
        c.setInteger(DualLookupOptions.ALERT_COOLDOWN_MINUTES.key(), 0);
        c.setBoolean(DualLookupOptions.ALERT_RECOVERY_ENABLED.key(), false);
        c.setInteger(DualLookupOptions.ALERT_RETRY_MAX.key(), 1);
        c.setInteger(DualLookupOptions.ALERT_RETRY_BACKOFF_MS.key(), 100);
        c.setInteger(DualLookupOptions.ALERT_RETRY_BACKOFF_MAX_MS.key(), 100);
        c.setInteger(DualLookupOptions.ALERT_KAFKA_MAX_BLOCK_MS.key(), 3_000);
        c.setInteger(DualLookupOptions.ALERT_KAFKA_REQUEST_TIMEOUT_MS.key(), 3_000);
        c.setInteger(DualLookupOptions.ALERT_KAFKA_DELIVERY_TIMEOUT_MS.key(), 5_000);
        c.setInteger(DualLookupOptions.ALERT_KAFKA_CLOSE_TIMEOUT_MS.key(), 5_000);
        DualLookupOptions.Config cfg = DualLookupOptions.Config.from(c, "it_table");

        String marker = UUID.randomUUID().toString();
        String payload = "{\"alert\":\"integration-test\",\"id\":\"" + marker + "\"}";

        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(GROUP_ID_CONFIG, "dual-lookup-it-" + marker);
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaAlertSink sink = new KafkaAlertSink(cfg, "dual-lookup-it-" + marker, AlertMetrics.NOOP);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            sink.send(payload);

            long deadline = System.currentTimeMillis() + 15_000L;
            boolean found = false;
            while (!found && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500L));
                for (ConsumerRecord<String, String> record : records) {
                    if (payload.equals(record.value())) {
                        found = true;
                        break;
                    }
                }
            }
            assertTrue("15 秒内未在 topic " + topic + " 收到告警：" + payload, found);
        } finally {
            sink.close(5_000L);
        }
    }
}
