package com.roc.flink.connector.dual;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 「连续 N 批降级」告警器：数连续降级批数，到阈值时把一条自定义 JSON 推到旁路（{@link Sink}，默认 Kafka）。
 *
 * <p><b>为什么是「连续」而不是「累计」？</b>主源偶发抖动几分钟后会自己恢复，累计口径会把
 * 几小时的零星降级攒成一次误报；连续口径则只在「主源一直没缓过来」时触发，正好对应
 * 需要人介入的场景。主源任何一批成功都会把计数清零，因此一次故障只可能触发一次。
 *
 * <p><b>触发时机是边沿而非电平</b>：计数<b>恰好等于</b>阈值的那一批推一条，之后继续降级也
 * 不再重复推送（计数仍在涨，供日志/后续观测使用）。理由有两条：
 * <ol>
 *   <li>持续故障时按批重复推送会把 Kafka 变成日志洪峰——这正是降级 WARN 当初要限流的原因；</li>
 *   <li>告警系统的收敛去重是常态，一条「已经连续 N 批降级」的消息足以把人和告警平台叫起来，
 *       而「故障还在持续」由统计日志的 {@code 降级=} 字段与限流 WARN 持续体现。</li>
 * </ol>
 * 需要重新武装只有一条路径：主源恢复成功。
 *
 * <p><b>旁路原则</b>（本类最重要的约束）：告警是观测手段，绝不能影响数据链路。
 * 因此 ① 渲染与编解码都在调用方约束下做少量字符串操作；② 真正的网络 IO 交给
 * {@link Sink} 自己的线程；③ 任何 {@link Throwable} 都就地吞掉——推送失败只影响告警，
 * 不影响这一批维表查询的正常下发。
 *
 * <p><b>线程安全</b>：降级/恢复回调来自各源自己的 IO 线程，可能并发进入，
 * 因此计数用 {@link AtomicInteger}。{@code incrementAndGet()} 的返回值是全局唯一的，
 * 于是「恰好等于阈值」这个判断天然只会被一个线程命中，不需要额外的 CAS 标志位。
 */
final class FailoverAlertNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverAlertNotifier.class);

    /** 日志统一前缀，与统计/降级日志保持一致，便于 grep */
    private static final String PREFIX = "[dual-lookup] ";

    /** 关闭时给旁路的等待上限：告警不值得让作业取消多等 */
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 5_000L;

    /**
     * JVM 内同表、同 topic 的最近一次降级告警时间。
     *
     * <p>告警计数本身是每子任务独立的，冷却窗放在静态表里可以让同一 TM 上的多个子任务
     * 在窗口内只发一条；跨 TM 仍可能各发一条，完全去重需要下游按 table + 时间窗聚合。
     */
    private static final ConcurrentHashMap<String, Long> LAST_ALERT_AT = new ConcurrentHashMap<>();

    /**
     * 模板里允许出现的占位符白名单。
     *
     * <p>为什么要白名单而不是「认识的替换、不认识的留着」：模板写错一个字母（{@code ${consecutvie}}）
     * 时，错误的占位符会原样出现在告警体里——JSON 仍然合法、消息照样发出，只是关键字段永远缺失。
     * 这类问题在告警里最难发现（告警本来就很少成功触发），必须在建表期就拦下。
     */
    static final Set<String> PLACEHOLDERS = Collections.unmodifiableSet(new LinkedHashSet<>(
            Arrays.asList("table", "primary", "standby", "threshold", "consecutive",
                    "affectedKeys", "reason", "subtask", "timestamp", "datetime")));

    /** 时间字段格式：带时区偏移的 ISO-8601，下游无需再猜时区 */
    private static final DateTimeFormatter ISO = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    /** 告警消息的出口；抽成接口是为了让「计数与渲染」能在没有 Kafka 的环境里被完整测试 */
    interface Sink {

        /** 投递一条消息；实现必须立即返回，不得在调用线程上做网络 IO */
        void send(String payload);

        /** 尽力冲刷并释放资源；实现必须容忍重复调用，且不得抛出异常 */
        void close(long timeoutMs);
    }

    private final String tableName;
    private final String primary;
    private final String standby;
    private final String subtaskLabel;
    /** 触发阈值（连续降级批数），必须 > 0 */
    private final int threshold;
    /** 降级 JSON 模板（含 {@code ${xxx}} 占位符） */
    private final String template;
    /** 恢复 JSON 模板（与降级模板使用同一套占位符） */
    private final String recoveryTemplate;
    private final Sink sink;
    /** 同表同 topic 的降级告警冷却毫秒数；0 = 不去重 */
    private final long cooldownMs;
    /** 冷却键：table + topic；为空时不做冷却 */
    private final String cooldownKey;
    /** 主源恢复后是否发 recovery */
    private final boolean recoveryEnabled;
    /** close 等待上限 */
    private final long closeTimeoutMs;
    private final AlertMetrics metrics;

    /** 当前连续降级批数；主源成功即归零 */
    private final AtomicInteger streak = new AtomicInteger();
    /** 本轮连续降级所波及的 key 数（用于回答「这次故障影响了多少条数据」） */
    private final AtomicLong streakKeys = new AtomicLong();
    /** 本轮是否真正触发过降级告警；决定恢复时是否发 recovery */
    private boolean alertActive;
    /** close 幂等 */
    private final AtomicBoolean closed = new AtomicBoolean();

    FailoverAlertNotifier(String tableName, String primary, String standby, String subtaskLabel,
                          int threshold, String template, Sink sink) {
        this(tableName, primary, standby, subtaskLabel, threshold, template, sink,
                0L, "", false, DEFAULT_CLOSE_TIMEOUT_MS, AlertMetrics.NOOP);
    }

    FailoverAlertNotifier(String tableName, String primary, String standby, String subtaskLabel,
                          int threshold, String template, Sink sink, long cooldownMs,
                          String cooldownKey, boolean recoveryEnabled, long closeTimeoutMs,
                          AlertMetrics metrics) {
        this(tableName, primary, standby, subtaskLabel, threshold, template, sink, cooldownMs,
                cooldownKey, recoveryEnabled, closeTimeoutMs, metrics,
                DualLookupOptions.DEFAULT_ALERT_RECOVERY_MESSAGE);
    }

    FailoverAlertNotifier(String tableName, String primary, String standby, String subtaskLabel,
                          int threshold, String template, Sink sink, long cooldownMs,
                          String cooldownKey, boolean recoveryEnabled, long closeTimeoutMs,
                          AlertMetrics metrics, String recoveryTemplate) {
        this.tableName = tableName;
        this.primary = primary;
        this.standby = standby;
        this.subtaskLabel = subtaskLabel == null ? "" : subtaskLabel;
        this.threshold = threshold;
        this.template = template == null || template.trim().isEmpty()
                ? DualLookupOptions.DEFAULT_ALERT_MESSAGE : template;
        this.recoveryTemplate = recoveryTemplate == null || recoveryTemplate.trim().isEmpty()
                ? DualLookupOptions.DEFAULT_ALERT_RECOVERY_MESSAGE : recoveryTemplate;
        this.sink = sink;
        this.cooldownMs = Math.max(0L, cooldownMs);
        this.cooldownKey = cooldownKey == null ? "" : cooldownKey;
        this.recoveryEnabled = recoveryEnabled;
        this.closeTimeoutMs = Math.max(1L, closeTimeoutMs);
        this.metrics = metrics == null ? AlertMetrics.NOOP : metrics;
    }

    /**
     * 按配置装配告警器；未启用时返回 {@code null}。
     *
     * <p>返回 null 而不是「返回一个什么都不做的对象」，是为了让热路径只付一次引用判空，
     * 既不建线程也不建 Kafka 生产者——关闭告警的用户不该为它承担任何开销。
     */
    static FailoverAlertNotifier create(DualLookupOptions.Config cfg, String tableName, String subtaskLabel) {
        return create(cfg, tableName, subtaskLabel, AlertMetrics.NOOP);
    }

    static FailoverAlertNotifier create(DualLookupOptions.Config cfg, String tableName, String subtaskLabel,
                                        AlertMetrics metrics) {
        if (cfg.alertThreshold <= 0) {
            return null;
        }
        try {
            String clientId = cfg.alertKafkaClientId == null || cfg.alertKafkaClientId.trim().isEmpty()
                    ? buildClientId(tableName, subtaskLabel) : cfg.alertKafkaClientId.trim();
            String cooldownKey = tableName + "\u0000" + cfg.alertKafkaTopic;
            return new FailoverAlertNotifier(tableName, cfg.primary, cfg.standby, subtaskLabel,
                    cfg.alertThreshold, cfg.alertKafkaMessage,
                    new KafkaAlertSink(cfg, clientId, metrics),
                    cfg.alertCooldownMinutes * 60_000L, cooldownKey, cfg.alertRecovery,
                    cfg.alertKafkaCloseTimeoutMs, metrics, cfg.alertKafkaRecoveryMessage);
        } catch (Throwable t) {
            // 告警装配失败（Kafka 类缺失、线程池/对象创建失败等）不能影响算子启动
            LOG.warn(PREFIX + "表={} 连续降级告警初始化失败（已禁用告警，不影响维表查询）：{}",
                    tableName, t.toString());
            return null;
        }
    }

    /** Kafka client.id 带 table/subtask，避免多子任务在 Kafka 日志与指标里混在一起 */
    private static String buildClientId(String tableName, String subtaskLabel) {
        String raw = "dual-lookup-alert-" + tableName
                + (subtaskLabel == null || subtaskLabel.isEmpty() ? "" : "-" + subtaskLabel);
        String safe = raw.replaceAll("[^A-Za-z0-9._-]", "_");
        return safe.length() <= 180 ? safe : safe.substring(0, 180);
    }

    /**
     * 记一次降级。
     *
     * @param keys  本批 key 数（累加进「本轮影响面」，让告警能说明影响多少条数据）
     * @param cause 主源失败原因（已剥掉 CompletableFuture 包装）
     */
    synchronized void onFailover(int keys, Throwable cause) {
        if (closed.get()) {
            return;
        }
        // 先累加 key 数再自增批次：反过来的话，读到「恰好等于阈值」的线程可能还没看到本批的 key
        long affectedKeys = streakKeys.addAndGet(Math.max(0, keys));
        int consecutive = streak.incrementAndGet();
        if (consecutive != threshold) {
            return; // 未到阈值，或已经越过（本次故障已推过），都不重复推送
        }
        metrics.triggered();
        if (!acquireCooldown()) {
            metrics.suppressed();
            LOG.info(PREFIX + "表={} 连续 {} 批降级，但距上次告警不足冷却窗口（{} 分钟），本次不重复推送",
                    tableName, consecutive, cooldownMs / 60_000L);
            return;
        }
        alertActive = true;
        try {
            fire(consecutive, affectedKeys, cause);
        } catch (Throwable t) {
            // 告警是旁路：渲染/序列化/投递的任何问题都不该冒泡到降级链路上
            LOG.warn(PREFIX + "表={} 连续降级告警推送失败（不影响维表查询）：{}", tableName, t.toString());
        }
    }

    /** 主源恢复成功：故障周期结束，重新武装；若本轮真正告警过，再推一条 recovery。 */
    synchronized void onPrimarySuccess() {
        if (closed.get()) {
            return;
        }
        boolean wasActive = alertActive;
        alertActive = false;
        if (streak.get() != 0) {
            streak.set(0);
            streakKeys.set(0);
        }
        if (wasActive && recoveryEnabled) {
            try {
                fireRecovery();
            } catch (Throwable t) {
                LOG.warn(PREFIX + "表={} 恢复告警推送失败（不影响维表查询）：{}", tableName, t.toString());
            }
        }
    }

    /**
     * 同表同 topic 的告警冷却。用 compute 保证同一 JVM 内多个子任务并发时只有一个能拿到窗口；
     * 跨 TM 的重复仍需下游按 table + 时间窗去重。
     */
    private boolean acquireCooldown() {
        if (cooldownMs <= 0L || cooldownKey.isEmpty()) {
            return true;
        }
        long now = System.currentTimeMillis();
        AtomicBoolean allowed = new AtomicBoolean(false);
        LAST_ALERT_AT.compute(cooldownKey, (key, previous) -> {
            if (previous != null && now - previous < cooldownMs) {
                return previous;
            }
            allowed.set(true);
            return now;
        });
        return allowed.get();
    }

    /** 当前连续降级批数（测试与诊断用） */
    int currentStreak() {
        return streak.get();
    }

    /** 仅供测试清理 JVM 级冷却表，避免用例之间或机器时钟抖动造成偶发失败 */
    static void clearCooldownForTests() {
        LAST_ALERT_AT.clear();
    }

    /** 关闭旁路；幂等，不抛异常，避免影响算子的正常关闭流程 */
    void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            sink.close(closeTimeoutMs);
        } catch (Throwable t) {
            LOG.warn(PREFIX + "表={} 告警通道关闭异常（已忽略）：{}", tableName, t.toString());
        }
    }

    private void fire(int consecutive, long affectedKeys, Throwable cause) {
        Alert alert = new Alert(tableName, primary, standby, subtaskLabel, threshold,
                consecutive, affectedKeys, reasonOf(cause), System.currentTimeMillis());
        sink.send(render(template, alert));
        // 这里只表示「已提交给旁路」；真正的 Kafka 投递结果由 KafkaAlertSink 的异步日志给出。
        // 不能写「已推送」——send() 只入队，队列满/生产者创建失败时消息可能还没发出去。
        LOG.warn(PREFIX + "表={} 已连续 {} 批降级至备源 {}（波及 {} 个 key），已触发告警投递（结果见后续 Kafka 日志）",
                tableName, consecutive, standby, affectedKeys);
    }

    /** 恢复消息使用独立的 recovery 模板，避免把降级模板误当成恢复消息渲染 */
    private void fireRecovery() {
        long timestamp = System.currentTimeMillis();
        Alert alert = new Alert(tableName, primary, standby, subtaskLabel, threshold,
                0, 0L, "primary recovered", timestamp);
        sink.send(render(recoveryTemplate, alert));
        metrics.recovery();
        LOG.info(PREFIX + "表={} 主源 {} 已恢复，已触发恢复告警投递（结果见后续 Kafka 日志）",
                tableName, primary);
    }

    /** 失败原因的可读描述：类名 + 消息；消息为空时只留类名（避免出现 "null" 尾巴） */
    private static String reasonOf(Throwable cause) {
        if (cause == null) {
            return "";
        }
        String message = cause.getMessage();
        String name = cause.getClass().getSimpleName();
        return (message == null || message.isEmpty()) ? name : name + ": " + message;
    }

    // ---------------- 模板渲染 ----------------

    /**
     * 把模板里的 {@code ${占位符}} 替换成实际值。
     *
     * <p><b>占位符替换成「已转义的裸内容」，不带引号</b>：模板作者决定它放在引号里（字符串字段）
     * 还是裸写（数字字段）。这样一套规则同时满足两种写法，且数字被「转义」后仍是原样数字。
     *
     * <p>未识别的占位符原样保留（建表期已被 {@link #validateTemplate} 拦下，正常运行不会走到）；
     * 不抛异常是有意的——渲染发生在降级链路上，不能因为一个字符串细节把整批查询带崩。
     */
    static String render(String template, Alert alert) {
        StringBuilder out = new StringBuilder(template.length() + 96);
        int cursor = 0;
        while (true) {
            int start = template.indexOf("${", cursor);
            if (start < 0) {
                out.append(template, cursor, template.length());
                break;
            }
            int end = template.indexOf('}', start + 2);
            if (end < 0) {
                out.append(template, cursor, template.length());
                break;
            }
            out.append(template, cursor, start);
            String name = template.substring(start + 2, end);
            String raw = alert.value(name);
            if (raw == null) {
                out.append(template, start, end + 1);
            } else {
                out.append(escapeJson(raw));
            }
            cursor = end + 1;
        }
        return out.toString();
    }

    /**
     * 转义成可安全放进 JSON 字符串字面量的内容（不加外层引号）。
     *
     * <p>为什么必须转义：失败原因里经常带引号（如 {@code No enum constant "A"B"}）、
     * 换行（HBase 的异常栈摘要），不转义就会把下游的 JSON 解析直接打挂——
     * 而告警解析失败往往没人发现。
     */
    static String escapeJson(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                default:
                    if (ch < 0x20) {
                        out.append(String.format("\\u%04x", (int) ch));
                    } else {
                        out.append(ch);
                    }
            }
        }
        return out.toString();
    }

    /**
     * 建表期校验模板，让「模板写错」在 CREATE TABLE 阶段就报错。
     *
     * <p>校验三件事，<b>顺序有意为之</b>：① 非空；② 占位符必须闭合且在白名单内；③ 外形像 JSON
     * （首尾必须是 <code>{}</code> 或 <code>[]</code>）。
     *
     * <p>为什么占位符先于 JSON 判断：{@code {"a":"${x"} 这类「少写了 }」的手误，若先做 JSON 判断，
     * 报出来的是「不是 JSON」——而模板明明是 JSON，只是占位符没闭合。先扫占位符才能给出
     * 「占位符没有闭合」这个能直接指导修改的错误。
     *
     * <p>JSON 只做外形检查而不解析，是刻意的：不为此引入任何 JSON 库，而漏写花括号、误写成纯文本
     * 已经是最常见的手误；真正的语法错误在联调时第一眼就能看出来。
     *
     * @throws IllegalArgumentException 校验不通过
     */
    static void validateTemplate(String template) {
        String t = template == null ? "" : template.trim();
        if (t.isEmpty()) {
            throw new IllegalArgumentException("[dual-lookup] lookup.alert.kafka.message 不能为空");
        }
        Set<String> unknown = new LinkedHashSet<>();
        int cursor = 0;
        while (true) {
            int start = t.indexOf("${", cursor);
            if (start < 0) {
                break;
            }
            int end = t.indexOf('}', start + 2);
            if (end < 0) {
                throw new IllegalArgumentException(
                        "[dual-lookup] lookup.alert.kafka.message 里的占位符没有闭合（缺少 }）: "
                                + t.substring(start));
            }
            String name = t.substring(start + 2, end);
            if (!PLACEHOLDERS.contains(name)) {
                unknown.add(name);
            }
            cursor = end + 1;
        }
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException(
                    "[dual-lookup] lookup.alert.kafka.message 含未知占位符 " + unknown
                            + "，可用：" + PLACEHOLDERS);
        }
        char first = t.charAt(0);
        char last = t.charAt(t.length() - 1);
        boolean jsonLike = (first == '{' && last == '}') || (first == '[' && last == ']');
        if (!jsonLike) {
            throw new IllegalArgumentException(
                    "[dual-lookup] lookup.alert.kafka.message 必须是一段 JSON（以 { } 或 [ ] 包裹），当前为: " + t);
        }
    }

    /**
     * 一次触发时的取值快照（不可变）。
     *
     * <p>为什么先冻结成对象再渲染：渲染发生在降级回调线程上，若在渲染过程中计数器又被别的批次改动，
     * 同一条消息里就会出现「consecutive=10 但 affectedKeys 是 11 批的量」这种自相矛盾的数字。
     */
    static final class Alert {

        final String table;
        final String primary;
        final String standby;
        final String subtask;
        final int threshold;
        final int consecutive;
        final long affectedKeys;
        final String reason;
        final long timestamp;

        Alert(String table, String primary, String standby, String subtask, int threshold,
              int consecutive, long affectedKeys, String reason, long timestamp) {
            this.table = table;
            this.primary = primary;
            this.standby = standby;
            this.subtask = subtask;
            this.threshold = threshold;
            this.consecutive = consecutive;
            this.affectedKeys = affectedKeys;
            this.reason = reason;
            this.timestamp = timestamp;
        }

        /** 占位符取值；不认识返回 null（渲染时原样保留） */
        String value(String name) {
            switch (name) {
                case "table":
                    return table;
                case "primary":
                    return primary;
                case "standby":
                    return standby;
                case "subtask":
                    return subtask;
                case "threshold":
                    return Integer.toString(threshold);
                case "consecutive":
                    return Integer.toString(consecutive);
                case "affectedKeys":
                    return Long.toString(affectedKeys);
                case "reason":
                    return reason;
                case "timestamp":
                    return Long.toString(timestamp);
                case "datetime":
                    return ISO.format(Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()));
                default:
                    return null;
            }
        }
    }
}
