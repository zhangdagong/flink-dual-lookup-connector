package com.roc.flink.connector.dual;

import com.roc.flink.connector.dual.FailoverAlertNotifier.Sink;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 「连续降级告警」的测试。
 *
 * <p>覆盖四个最容易「看起来对、实际错」的点：
 * <ol>
 *   <li><b>阈值是边沿而非电平</b>：只有恰好等于阈值那一批推一条，之后的第 n+1、n+2 批不能再推——
 *       否则持续故障会把 Kafka 灌成日志洪峰，而这正是告警最需要保持克制的时刻；</li>
 *   <li><b>恢复要重新武装</b>：主源成功一批后计数必须归零，否则第二次故障永远不会再告警
 *       （这比「多推一条」严重得多：故障不再被通知）；</li>
 *   <li><b>JSON 转义</b>：失败原因里带引号/换行是常态，不转义会让下游解析整条消息失败；</li>
 *   <li><b>旁路不能反噬</b>：Sink 抛异常时绝不能冒泡到降级链路。</li>
 * </ol>
 *
 * <p>测试用 {@link RecordingSink} 替掉 Kafka，因此不依赖任何外部服务。
 */
public class FailoverAlertNotifierTest {

    private static final String DEFAULT = DualLookupOptions.DEFAULT_ALERT_MESSAGE;

    private static FailoverAlertNotifier notifier(int threshold, Sink sink) {
        return new FailoverAlertNotifier("dim_account", "hbase", "doris", "1/4",
                threshold, DEFAULT, sink);
    }

    private static FailoverAlertNotifier notifier(int threshold, Sink sink, long cooldownMs,
                                                  String cooldownKey, boolean recovery) {
        return new FailoverAlertNotifier("dim_account", "hbase", "doris", "1/4",
                threshold, DEFAULT, sink, cooldownMs, cooldownKey, recovery, 5_000L,
                AlertMetrics.NOOP);
    }

    private static FailoverAlertNotifier notifier(int threshold, Sink sink, long cooldownMs,
                                                  String cooldownKey, boolean recovery,
                                                  String recoveryTemplate) {
        return new FailoverAlertNotifier("dim_account", "hbase", "doris", "1/4",
                threshold, DEFAULT, sink, cooldownMs, cooldownKey, recovery, 5_000L,
                AlertMetrics.NOOP, recoveryTemplate);
    }

    @Test
    public void cooldownSuppressesRepeatWithinWindow() {
        RecordingSink sink = new RecordingSink();
        String key = "cooldown-" + System.nanoTime();
        FailoverAlertNotifier n = notifier(1, sink, 60_000L, key, false);

        n.onFailover(1, new RuntimeException("down"));
        assertEquals(1, sink.payloads.size());

        n.onPrimarySuccess(); // 结束本轮
        n.onFailover(1, new RuntimeException("down again")); // 冷却窗口内
        assertEquals("冷却窗口内不得重复推送", 1, sink.payloads.size());

        // 直接清空 JVM 冷却表，避免依赖 sleep/机器速度；生产语义不变
        FailoverAlertNotifier.clearCooldownForTests();
        n.onPrimarySuccess();
        n.onFailover(1, new RuntimeException("down after cooldown"));
        assertEquals("冷却窗口结束后应能再次告警", 2, sink.payloads.size());
    }

    @Test
    public void recoveryIsSentAfterAnAlertEpisode() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(1, sink, 0L, "recovery-" + System.nanoTime(), true);

        n.onFailover(3, new RuntimeException("down"));
        n.onPrimarySuccess();

        assertEquals("降级 + 恢复应各一条", 2, sink.payloads.size());
        assertTrue("恢复消息要能被下游区分: " + sink.payloads.get(1),
                sink.payloads.get(1).contains("\"alert\":\"dual-lookup-recovered\""));
        assertEquals(0, n.currentStreak());
    }

    @Test
    public void recoveryUsesCustomJsonTemplate() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(1, sink, 0L, "recovery-custom-" + System.nanoTime(), true,
                "{\"event\":\"recovered\",\"table\":\"${table}\",\"keys\":${affectedKeys}}");

        n.onFailover(3, new RuntimeException("down"));
        n.onPrimarySuccess();

        assertEquals("{\"event\":\"recovered\",\"table\":\"dim_account\",\"keys\":0}",
                sink.payloads.get(1));
    }

    @Test
    public void closeIsIdempotent() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(1, sink);
        n.close();
        n.close();
        assertEquals("close 必须幂等", 1, sink.closeCalls);
    }

    @Test
    public void firesExactlyAtThresholdAndOnlyOncePerEpisode() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(3, sink);

        n.onFailover(10, new RuntimeException("down"));   // 1
        n.onFailover(10, new RuntimeException("down"));   // 2
        assertEquals("未到阈值绝不能推", 0, sink.payloads.size());

        n.onFailover(10, new RuntimeException("down"));   // 3 -> 触发
        assertEquals(1, sink.payloads.size());

        n.onFailover(10, new RuntimeException("down"));   // 4
        n.onFailover(10, new RuntimeException("down"));   // 5
        assertEquals("越过阈值后不得重复推送：持续故障每 N 批推一条会把 Kafka 变成日志洪峰",
                1, sink.payloads.size());
        // 计数本身仍然继续，只是不再触发
        assertEquals(5, n.currentStreak());
    }

    @Test
    public void primarySuccessRearmsTheAlert() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(2, sink);

        n.onFailover(5, new RuntimeException("down"));
        n.onFailover(5, new RuntimeException("down"));
        assertEquals(1, sink.payloads.size());

        n.onPrimarySuccess();
        assertEquals("恢复后计数必须归零，否则下一次故障永远不会再告警", 0, n.currentStreak());

        n.onFailover(7, new RuntimeException("down again"));
        n.onFailover(7, new RuntimeException("down again"));
        assertEquals("第二次故障必须能再次告警", 2, sink.payloads.size());
        // 第二轮的 affectedKeys 只统计新一轮：5+5 不能被带过来
        assertTrue("新一轮影响面应重新累计（2 批 × 7 = 14）: " + sink.payloads.get(1),
                sink.payloads.get(1).contains("\"affectedKeys\":14,"));
    }

    @Test
    public void payloadCarriesRenderedValues() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(2, sink);

        n.onFailover(30, new IllegalStateException("HBase lookup timeout after 500ms"));
        n.onFailover(12, new IllegalStateException("HBase lookup timeout after 500ms"));

        String json = sink.payloads.get(0);
        assertTrue("表名必须出现在告警里（一个作业可能有多张维表）: " + json,
                json.contains("\"table\":\"dim_account\""));
        assertTrue(json.contains("\"primary\":\"hbase\""));
        assertTrue(json.contains("\"standby\":\"doris\""));
        assertTrue("阈值与连续批数：数字字段必须裸写、不带引号: " + json,
                json.contains("\"threshold\":2") && json.contains("\"consecutive\":2"));
        assertTrue("影响面 = 前 n 批 key 数之和（30+12）: " + json,
                json.contains("\"affectedKeys\":42"));
        assertTrue("失败原因要能定位根因: " + json,
                json.contains("\"reason\":\"IllegalStateException: HBase lookup timeout after 500ms\""));
        assertTrue("子任务号用于多并行度下区分来源: " + json, json.contains("\"subtask\":\"1/4\""));
        assertTrue("时间戳（epoch 毫秒）应存在: " + json, json.matches(".*\"timestamp\":\\d{13}.*"));
        assertTrue("带时区的 ISO 时间应存在: " + json,
                json.matches(".*\"datetime\":\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}\".*"));
        assertTrue("整体必须仍是 JSON 对象: " + json, json.startsWith("{") && json.endsWith("}"));
    }

    @Test
    public void reasonIsJsonEscaped() {
        RecordingSink sink = new RecordingSink();
        FailoverAlertNotifier n = notifier(1, sink);

        n.onFailover(1, new RuntimeException("bad \"quote\" at\nline 2\ttab"));

        String json = sink.payloads.get(0);
        assertTrue("引号必须转义，否则整条 JSON 会被打断: " + json,
                json.contains("bad \\\"quote\\\" at"));
        assertTrue("换行/制表符必须转成转义序列，不能原样出现: " + json,
                json.contains("\\nline 2\\ttab"));
        assertTrue("告警体里不应存在裸换行: " + json, json.indexOf('\n') < 0);
    }

    @Test
    public void customTemplateAndUnknownPlaceholderFallback() {
        RecordingSink sink = new RecordingSink();
        // 自定义模板：只保留业务需要的字段，并故意带一个未登记的占位符
        // （建表期会拦下这种情况；这里验证「即使发生也只原样保留、不抛异常」的兜底行为）
        FailoverAlertNotifier n = new FailoverAlertNotifier("dim_account", "hbase", "doris", "",
                1, "{\"t\":\"${table}\",\"n\":${consecutive},\"x\":\"${nope}\"}", sink);

        n.onFailover(1, null);

        String json = sink.payloads.get(0);
        assertEquals("{\"t\":\"dim_account\",\"n\":1,\"x\":\"${nope}\"}", json);
    }

    @Test
    public void sinkFailureDoesNotPropagate() {
        FailoverAlertNotifier n = notifier(1, new Sink() {
            @Override
            public void send(String payload) {
                throw new IllegalStateException("broker unreachable");
            }

            @Override
            public void close(long timeoutMs) {
            }
        });
        // 告警通道炸掉时，降级链路必须照常继续——这里只要求「不抛」
        n.onFailover(1, new RuntimeException("primary down"));
        n.onPrimarySuccess();
        assertEquals(0, n.currentStreak());
    }

    @Test
    public void createReturnsNullWhenDisabled() {
        assertNull("未启用时必须返回 null，让热路径只付一次判空",
                FailoverAlertNotifier.create(alertConfig(0, null, null), "t", "1/1"));
        assertNotNull("启用且配全时应正常装配",
                FailoverAlertNotifier.create(alertConfig(5, "localhost:9092", "alert_topic"), "t", "1/1"));
    }

    /** 用真实的配置解析路径造 Config（它的构造器是私有的，只能从 WITH 选项走） */
    private static DualLookupOptions.Config alertConfig(int threshold, String bootstrap, String topic) {
        Configuration c = new Configuration();
        c.setString(DualLookupOptions.DORIS_JDBC_URL.key(), "jdbc:mysql://localhost:9030/dim");
        c.setInteger(DualLookupOptions.ALERT_THRESHOLD.key(), threshold);
        if (bootstrap != null) {
            c.setString(DualLookupOptions.ALERT_KAFKA_BOOTSTRAP.key(), bootstrap);
        }
        if (topic != null) {
            c.setString(DualLookupOptions.ALERT_KAFKA_TOPIC.key(), topic);
        }
        return DualLookupOptions.Config.from(c, "t");
    }

    // ---------------- 模板校验 ----------------

    @Test
    public void validateAcceptsDefaultTemplate() {
        FailoverAlertNotifier.validateTemplate(DEFAULT);
        // 数组形态也允许（有些平台要求批量格式）
        FailoverAlertNotifier.validateTemplate("[{\"table\":\"${table}\"}]");
    }

    @Test
    public void validateRejectsUnknownPlaceholder() {
        try {
            FailoverAlertNotifier.validateTemplate("{\"n\":${consecutvie}}");
            fail("占位符拼错必须在建表期报错，否则告警字段会永远缺失");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("consecutvie"));
        }
    }

    @Test
    public void validateRejectsNonJsonAndUnclosedPlaceholder() {
        try {
            FailoverAlertNotifier.validateTemplate("主源连续降级 ${consecutive} 批");
            fail("非 JSON 模板应被拒绝");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("JSON"));
        }
        try {
            // 少写一个 }：占位符没闭合，必须报「闭合」而不是笼统的「不是 JSON」
            FailoverAlertNotifier.validateTemplate("{\"n\":${x");
            fail("占位符未闭合应被拒绝");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("闭合"));
        }
        try {
            FailoverAlertNotifier.validateTemplate("   ");
            fail("空模板应被拒绝");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("不能为空"));
        }
    }

    /** 记录型 Sink：把收到的消息存下来供断言，不碰网络 */
    static class RecordingSink implements FailoverAlertNotifier.Sink {

        final List<String> payloads = Collections.synchronizedList(new ArrayList<String>());
        volatile boolean closed;
        int closeCalls;

        @Override
        public void send(String payload) {
            payloads.add(payload);
        }

        @Override
        public void close(long timeoutMs) {
            closed = true;
            closeCalls++;
        }
    }
}
