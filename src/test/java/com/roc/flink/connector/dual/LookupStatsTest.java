package com.roc.flink.connector.dual;

import org.junit.Test;

import java.lang.reflect.Field;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 统计输出的测试。
 *
 * <p>覆盖面刻意压在「看起来对、实际错」的地方——这些点一旦回归，日志依然打得像模像样，
 * 靠人眼根本发现不了：
 * <ol>
 *   <li><b>阶段值而非累计值</b>：{@code 本窗口=} 段只描述刚刚过去的那个间隔；</li>
 *   <li><b>同一行还要带累计视角</b>：{@code 自启动=} 段必须真的在累计，而不是把阶段值抄一遍；</li>
 *   <li><b>总处理量是主源 + 备源</b>：降级批次会被两源各处理一次，必须分别统计并汇总；</li>
 *   <li><b>{@code 最长=} 是区间最大值</b>：不能像计数那样用「累计相减」换算，必须逐窗口清零；</li>
 *   <li><b>命中率的分母</b>：只算成功返回的查询。失败批次不能进分母；</li>
 *   <li><b>失败分类</b>：阶段增量、无残留，且零值分类不出现。</li>
 * </ol>
 *
 * <p>测试直接断言 {@link LookupStats#snapshotLine()} 的文本，不去捕获日志输出——
 * 捕获日志的测试既依赖日志实现、又难以覆盖边界。
 */
public class LookupStatsTest {

    /** 默认参数的统计对象（配置回执传 null，本类大部分用例与服务端配置无关） */
    private static LookupStats stats() {
        return new LookupStats("dim_account", "hbase", "doris", 360);
    }

    @Test
    public void snapshotIsIntervalValueNotCumulative() {
        LookupStats s = stats();

        // 第一批阶段：2 批、100 个 key、累计 30ms（单批最长 20ms）
        s.recordPrimaryBatch(50, 10_000_000L);
        s.recordPrimaryBatch(50, 20_000_000L);
        String line1 = s.snapshotLine();
        assertTrue("首行应只有一个表名，首次窗口与自启动重合时只输出一份: " + line1,
                line1.contains("表=dim_account | 本窗口=自启动=360s"));
        assertTrue("业务数据量 = 主源 100，总处理量此时也是 100: " + line1,
                line1.contains("业务数据量=100 总处理key数=100 速率=0.3/s 主源平均批大小=50 总体平均批大小=50"));
        assertTrue("平均耗时 30ms/2 批、最长 20ms: " + line1,
                line1.contains("主源(hbase)[批次=2 key数=100 失败=0 平均=15.0ms 最长=20.0ms 命中率=-]"));
        assertTrue("备源没有样本时必须显示 -，0 会被误读成秒回: " + line1,
                line1.contains("备源(doris)[批次=0 key数=0 失败=0 平均=- 最长=- 命中率=-]"));

        // 第二批阶段：只有 1 批、1 个 key——必须只报增量，绝不能是 101/3
        s.recordPrimaryBatch(1, 5_000_000L);
        String line2 = s.snapshotLine();
        assertTrue("第二次打点必须是「本阶段增量」，不能把历史累计再报一遍: " + line2,
                line2.contains("总处理key数=1 速率=1.0/s"));
        assertTrue("批次数与耗时都应是增量: " + line2,
                line2.contains("主源(hbase)[批次=1 key数=1 失败=0 平均=5.0ms 最长=5.0ms"));
        assertTrue("本阶段没有备源样本: " + line2,
                line2.contains("备源(doris)[批次=0 key数=0 失败=0 平均=- 最长=- 命中率=-]"));
    }

    /**
     * 每行都要同时给出「本阶段」和「自启动累计」两个视角，且后者必须是真正的累计值。
     *
     * <p>首次窗口两者重合时只输出一份；第二次开始必须出现 {@code || 自启动=...}，
     * 并且累计值是真正的总量，而不是把阶段值抄一遍。
     */
    @Test
    public void everySnapshotCarriesCumulativeView() {
        LookupStats s = stats();

        s.recordPrimaryBatch(50, 10_000_000L);   // 阶段 1：50 个 key
        String line1 = s.snapshotLine();
        assertTrue("首次窗口与自启动重合，只输出一份: " + line1,
                line1.contains("表=dim_account | 本窗口=自启动=360s"));
        assertTrue("首行的总处理量应等于阶段段: " + line1,
                line1.contains("总处理key数=50"));

        s.recordPrimaryBatch(10, 5_000_000L);    // 阶段 2：只有 10 个 key
        String line2 = s.snapshotLine();
        assertTrue("阶段段只报增量 10: " + line2, line2.contains("总处理key数=10"));
        assertTrue("累计段必须累加到 60，而不是把阶段值 10 抄一遍: " + line2,
                line2.contains("|| 自启动=") && line2.contains("总处理key数=60"));
        assertTrue("累计平均耗时 = 6ms / 2 批、累计最长 10ms: " + line2,
                line2.contains("主源(hbase)[批次=2 key数=60 失败=0 平均=7.5ms 最长=10.0ms"));
    }

    @Test
    public void failoverCarriesKeyCountAndShare() {
        LookupStats s = stats();

        // 主源超时 500ms 后降级，备源只用 3ms
        s.recordPrimaryBatch(2, 500_000_000L);
        s.recordPrimaryFail(new TimeoutException("HBase lookup timeout after 500ms"));
        s.recordFailover(2);
        s.recordStandbyBatch(2, 3_000_000L);

        String line = s.snapshotLine();
        assertTrue("失败批次的耗时同样要统计，否则主源恶化过程会被掩盖: " + line,
                line.contains("主源(hbase)[批次=1 key数=2 失败=1(超时=1) 平均=500.0ms"));
        assertTrue("备源耗时应只含自身查询时间: " + line,
                line.contains("备源(doris)[批次=1 key数=2 失败=0 平均=3.0ms 最长=3.0ms"));
        assertTrue("降级次数要带上影响面（key 数 + 占主源 key 的比例）: " + line,
                line.contains("降级=1次(2个key,100.0%)"));
        assertTrue("业务数据量 = 主源 2，总处理量 = 主源 2 + 备源 2 = 4: " + line,
                line.contains("业务数据量=2 总处理key数=4"));
        // 总体平均批大小 = 4 个 key / (1 主批 + 1 备批) = 2
        assertTrue(line.contains("总体平均批大小=2 "));
    }

    /**
     * 命中率是本连接器唯一能照出「维表大面积查不到」的指标——因为查不到不算异常，
     * 它在 {@code 失败=}/{@code 降级=} 上完全隐身。
     */
    @Test
    public void hitsAreCountedPerSource() {
        LookupStats s = stats();

        s.recordPrimaryBatch(10, 1_000_000L);
        s.recordHits("hbase", 7, 10);      // 主源成功返回 10 个 key，命中 7 个
        s.recordStandbyBatch(4, 1_000_000L);
        s.recordHits("doris", 4, 4);       // 备源（降级）返回 4 个 key，全命中

        String line = s.snapshotLine();
        assertTrue("主源命中率 7/10: " + line,
                line.contains("主源(hbase)[批次=1 key数=10 失败=0 平均=1.0ms 最长=1.0ms 命中率=70.0%]"));
        assertTrue("备源命中率 4/4，不能与主源混在一个分母里: " + line,
                line.contains("备源(doris)[批次=1 key数=4 失败=0 平均=1.0ms 最长=1.0ms 命中率=100.0%]"));
    }

    @Test
    public void failedBatchesDoNotDiluteHitRate() {
        LookupStats s = stats();

        s.recordPrimaryBatch(3, 1_000_000L);
        s.recordPrimaryFail(new TimeoutException("t"));
        s.recordFailover(3);

        String line = s.snapshotLine();
        assertTrue("失败批次没有命中率可言，绝不能进分母: " + line,
                line.contains("主源(hbase)[批次=1 key数=3 失败=1(超时=1)")
                        && line.contains("命中率=-]"));
    }

    /**
     * {@code 最长=} 是<b>区间最大值</b>，它不能像计数那样用「累计相减」换算：
     * 第二个窗口的单批耗时比第一个窗口小，相减会得到负数——所以必须逐窗口清零。
     */
    @Test
    public void maxIsWindowedAndResetsEverySnapshot() {
        LookupStats s = stats();

        s.recordPrimaryBatch(1, 20_000_000L);   // 20ms
        String line1 = s.snapshotLine();
        assertTrue("第一窗口最长 20ms: " + line1, line1.contains("平均=20.0ms 最长=20.0ms"));

        s.recordPrimaryBatch(1, 5_000_000L);    // 5ms，比上一窗口更短
        String line2 = s.snapshotLine();
        assertTrue("区间最大值必须逐窗口清零，不能拿累计值相减: " + line2,
                line2.contains("平均=5.0ms 最长=5.0ms"));
        assertTrue("累计段仍应记得历史最长值 20ms: " + line2,
                line2.contains("|| 自启动=") && line2.contains("最长=20.0ms"));
    }

    @Test
    public void failKindsAreClassifiedAndZeroKindsAreOmitted() {
        LookupStats s = stats();

        s.recordPrimaryFail(new TimeoutException("timeout"));
        s.recordPrimaryFail(new ConnectException("connection refused"));
        s.recordPrimaryFail(new RejectedExecutionException("doris queue full"));
        s.recordPrimaryFail(new SQLException("access denied"));
        s.recordPrimaryFail(new IllegalStateException("something else"));
        s.recordPrimaryBatch(5, 1_000_000L);

        String line = s.snapshotLine();
        assertTrue("5 类各一次，且只列非零项: " + line,
                line.contains("失败=5(超时=1,连接=1,池满=1,数据库=1,其他=1)"));

        // 第二轮只有超时：上一轮的分类不能残留，否则会重复计数
        s.recordPrimaryFail(new TimeoutException("again"));
        String line2 = s.snapshotLine();
        assertTrue("分类计数同样必须是阶段增量: " + line2, line2.contains("失败=1(超时=1)"));
    }

    /**
     * 配置回执：只在启动时打一次，作用在于事后——拿到一段 TM 日志时能确认「当时是什么参数」。
     */
    @Test
    public void configLineAndRate() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360,
                "primary=hbase standby=doris batch.size=50 batch.max-wait=5ms lookup.timeout=500ms"
                        + " stats.interval=360s failover.interval=10s");

        String cfg = s.configLine();
        assertTrue("配置行要能直接 grep 到表名: " + cfg,
                cfg.startsWith("[dual-lookup] 表=dim_account 配置["));
        assertTrue("配置行要包含真正影响行为的参数: " + cfg,
                cfg.contains("primary=hbase") && cfg.contains("batch.size=50")
                        && cfg.contains("lookup.timeout=500ms") && cfg.contains("stats.interval=360s"));

        s.recordPrimaryBatch(720, 1_000_000L);
        String line = s.snapshotLine();
        assertTrue("首行吞吐按配置间隔算（720/360）: " + line, line.contains("速率=2.0/s"));
    }

    @Test
    public void totalLineKeepsCumulativeViewForJobShutdown() {
        LookupStats s = stats();

        s.recordPrimaryBatch(2, 2_000_000L);
        s.snapshotLine();                       // 推进快照游标，模拟已打过一行阶段日志
        s.recordPrimaryBatch(3, 4_000_000L);    // 又在下一个阶段跑了 1 批

        // 打点是阶段值，因此收尾行必须给出「自启动以来」的视角
        String total = s.totalLine();
        assertTrue("累计汇总应含全部 5 个 key: " + total,
                total.contains("表=dim_account | 自启动=")
                        && total.contains("业务数据量=5 总处理key数=5"));
        assertTrue("累计平均耗时 = 6ms / 2 批: " + total,
                total.contains("主源(hbase)[批次=2 key数=5 失败=0 平均=3.0ms"));
        assertTrue(total.contains("降级=0"));
    }

    /** 并行子任务编号要出现在表名后面，用于区分同表的多条统计日志 */
    @Test
    public void subtaskLabelDistinguishesParallelInstances() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360, null, "2/4");
        s.recordPrimaryBatch(1, 1_000_000L);

        String line = s.snapshotLine();
        assertTrue("统计行应带子任务编号: " + line,
                line.contains("表=dim_account 子任务=2/4 |"));
        assertTrue("配置回执也应带子任务编号: " + s.configLine(),
                s.configLine().contains("表=dim_account 子任务=2/4 配置["));
    }

    @Test
    public void disabledIntervalDoesNotStartSchedulerAndCloseIsIdempotent() throws Exception {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 0);
        s.start();

        Field f = LookupStats.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        assertNull("interval=0 表示关闭打点，不应创建任何线程", f.get(s));

        s.close();
        s.close(); // 重复关闭必须安全（Flink 在取消/失败路径下可能重复调用）
    }

    /**
     * 统计日志的数值格式必须与 JVM 默认 locale 无关。
     *
     * <p>德语等 locale 的小数点是逗号：若不用 {@code Locale.ROOT}，日志会变成
     * {@code 12,3ms}，既影响人读，也会让按点号解析日志的脚本失效。
     */
    @Test
    public void numbersUseRootLocaleRegardlessOfDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            LookupStats s = stats();
            s.recordPrimaryBatch(1, 12_300_000L);

            String line = s.snapshotLine();
            assertTrue("小数分隔符必须是点，不能受 JVM 默认 locale 影响: " + line,
                    line.contains("平均=12.3ms"));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
