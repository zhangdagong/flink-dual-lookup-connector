package com.roc.flink.connector.dual;

import org.junit.Test;

import java.lang.reflect.Field;
import java.net.ConnectException;
import java.sql.SQLException;
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
 *   <li><b>阶段值而非累计值</b>：{@code window=} 段只描述刚刚过去的那个间隔；</li>
 *   <li><b>同一行还要带累计视角</b>：{@code since-start} 段必须真的在累计，而不是把阶段值抄一遍
 *       （那就等于两个视角都没了）；</li>
 *   <li><b>{@code max=} 是区间最大值</b>：不能像计数那样用「累计相减」换算，必须逐窗口清零，
 *       否则第二个窗口会算出负数或无意义的数；</li>
 *   <li><b>{@code hit=} 的分母</b>：只算成功返回的查询。若把失败批次也算进去，
 *       主源一故障（压根没查成）命中率就会莫名下跌，指标随即失去意义；</li>
 *   <li><b>{@code fail=} 的分类</b>：阶段增量、无残留，且零值分类不出现；</li>
 *   <li><b>各源平均耗时</b>：主备独立，且「没有样本」必须显示 {@code -} 而不是 0
 *       （0 会被误读成「秒回」）。</li>
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
        assertTrue("首行窗口应等于配置的间隔: " + line1,
                line1.contains("table=dim_account window=360s"));
        assertTrue("首行吞吐 = 100 key / 360s: " + line1, line1.contains("totalKeys=100 rate=0.3/s"));
        assertTrue("平均批大小与含备源的对照值: " + line1,
                line1.contains("avgBatch=50 avgBatchWithStandby=50"));
        assertTrue("平均耗时 30ms/2 批、最长 20ms: " + line1,
                line1.contains("hbase[batches=2 keys=100 fail=0 avg=15.0ms max=20.0ms"));
        assertTrue("备源没有样本时必须显示 -，0 会被误读成秒回: " + line1,
                line1.contains("doris[batches=0 fail=0 avg=- max=- hit=-]"));

        // 第二批阶段：只有 1 批、1 个 key——必须只报增量，绝不能是 101/3
        s.recordPrimaryBatch(1, 5_000_000L);
        String line2 = s.snapshotLine();
        assertTrue("第二次打点必须是「本阶段增量」，不能把历史累计再报一遍: " + line2,
                line2.contains("totalKeys=1 rate=1.0/s"));
        assertTrue("批次数与耗时都应是增量: " + line2,
                line2.contains("hbase[batches=1 keys=1 fail=0 avg=5.0ms max=5.0ms"));
        assertTrue("本阶段没有备源样本: " + line2, line2.contains("doris[batches=0 fail=0 avg=- max=- hit=-]"));
    }

    /**
     * 每行都要同时给出「本阶段」和「自启动累计」两个视角，且后者必须是真正的累计值。
     *
     * <p>这个断言的真正价值在于：让「累计段」不能靠复制阶段值蒙混过关——一旦有人偷懒这么写，
     * 两个视角就退化成同一个数字，日志看起来更紧凑，但长期运行的总量信息彻底丢失，
     * 而单看任何一行都发现不了。
     */
    @Test
    public void everySnapshotCarriesCumulativeView() {
        LookupStats s = stats();

        s.recordPrimaryBatch(50, 10_000_000L);   // 阶段 1：50 个 key
        String line1 = s.snapshotLine();
        assertTrue("累计段应带 uptime 而不是窗口（两者语义不同）: " + line1,
                line1.contains("|| table=dim_account since-start uptime="));
        assertTrue("首行的累计段应等于阶段段（都还没有历史）: " + line1,
                line1.contains("totalKeys=50 rate="));

        s.recordPrimaryBatch(10, 5_000_000L);    // 阶段 2：只有 10 个 key
        String line2 = s.snapshotLine();
        assertTrue("阶段段只报增量 10: " + line2, line2.contains("totalKeys=10 rate="));
        assertTrue("累计段必须累加到 60，而不是把阶段值 10 抄一遍: " + line2,
                line2.contains("since-start uptime=") && line2.contains("totalKeys=60 rate="));
        assertTrue("累计平均耗时 = 15ms / 2 批、累计最长 10ms: " + line2,
                line2.contains("hbase[batches=2 keys=60 fail=0 avg=7.5ms max=10.0ms"));
    }

    @Test
    public void failoverCarriesKeyCountAndShare() {
        LookupStats s = stats();

        // 主源超时 500ms 后降级，备源只用 3ms：
        // 若备源耗时从「批次开始」起算，这里会变成 503ms，指标就失去诊断价值了
        s.recordPrimaryBatch(2, 500_000_000L);
        s.recordPrimaryFail(new TimeoutException("HBase lookup timeout after 500ms"));
        s.recordFailover(2);
        s.recordStandbyBatch(3_000_000L);

        String line = s.snapshotLine();
        assertTrue("失败批次的耗时同样要统计，否则主源恶化过程会被掩盖: " + line,
                line.contains("hbase[batches=1 keys=2 fail=1(timeout=1) avg=500.0ms"));
        assertTrue("备源耗时应只含自身查询时间: " + line,
                line.contains("doris[batches=1 fail=0 avg=3.0ms max=3.0ms"));
        assertTrue("降级次数要带上影响面（key 数 + 占比）——「降了 1 批」看不出影响了多少条数据: " + line,
                line.contains("failover=1(2keys,100.0%)"));
        // 含备源分母的平均批大小：2 个 key / (1 主批 + 1 备批) = 1
        assertTrue(line.contains("avgBatchWithStandby=1 "));
    }

    /**
     * 命中率是本连接器唯一能照出「维表大面积查不到」的指标——因为查不到不算异常，
     * 它在 {@code fail=}/{@code failover=} 上完全隐身。
     */
    @Test
    public void hitsAreCountedPerSource() {
        LookupStats s = stats();

        s.recordPrimaryBatch(10, 1_000_000L);
        s.recordHits("hbase", 7, 10);      // 主源成功返回 10 个 key，命中 7 个
        s.recordStandbyBatch(1_000_000L);
        s.recordHits("doris", 4, 4);       // 备源（降级）返回 4 个 key，全命中

        String line = s.snapshotLine();
        assertTrue("主源命中率 7/10: " + line,
                line.contains("keys=10 fail=0 avg=1.0ms max=1.0ms hit=70.0%]"));
        assertTrue("备源命中率 4/4，不能与主源混在一个分母里: " + line,
                line.contains("doris[batches=1 fail=0 avg=1.0ms max=1.0ms hit=100.0%]"));
    }

    @Test
    public void failedBatchesDoNotDiluteHitRate() {
        LookupStats s = stats();

        s.recordPrimaryBatch(3, 1_000_000L);
        s.recordPrimaryFail(new TimeoutException("t"));
        s.recordFailover(3);

        String line = s.snapshotLine();
        assertTrue("失败批次没有命中率可言，绝不能进分母（否则主源一故障命中率就下跌）: " + line,
                line.contains("batches=1 keys=3 fail=1(timeout=1)") && line.contains("hit=-]"));
    }

    /**
     * {@code max=} 是<b>区间最大值</b>，它不能像计数那样用「累计相减」换算：
     * 第二个窗口的单批耗时比第一个窗口小，相减会得到负数——所以必须逐窗口清零。
     * 这也是全类唯一一处刻意的「例外」，最容易在重构时被顺手改坏。
     */
    @Test
    public void maxIsWindowedAndResetsEverySnapshot() {
        LookupStats s = stats();

        s.recordPrimaryBatch(1, 20_000_000L);   // 20ms
        String line1 = s.snapshotLine();
        assertTrue("第一窗口最长 20ms: " + line1, line1.contains("avg=20.0ms max=20.0ms"));

        s.recordPrimaryBatch(1, 5_000_000L);    // 5ms，比上一窗口更短
        String line2 = s.snapshotLine();
        assertTrue("区间最大值必须逐窗口清零，不能拿累计值相减: " + line2,
                line2.contains("avg=5.0ms max=5.0ms"));
        assertTrue("累计段仍应记得历史最长值 20ms: " + line2,
                line2.contains("since-start uptime=") && line2.contains("max=20.0ms"));
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
        assertTrue("5 类各一次，且只列非零项（零值分类常年挂在日志里只会稀释信息）: " + line,
                line.contains("fail=5(timeout=1,connect=1,poolFull=1,db=1,other=1)"));

        // 第二轮只有超时：上一轮的分类不能残留，否则会重复计数
        s.recordPrimaryFail(new TimeoutException("again"));
        String line2 = s.snapshotLine();
        assertTrue("分类计数同样必须是阶段增量: " + line2, line2.contains("fail=1(timeout=1)"));
    }

    /**
     * 配置回执：只在启动时打一次，作用在于事后——拿到一段 TM 日志时能确认「当时是什么参数」。
     * 吞吐率则按真实窗口长度换算（首行是配置间隔，之后是真实间隔）。
     */
    @Test
    public void configLineAndRate() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360,
                "primary=hbase standby=doris batch.size=50 batch.max-wait=5ms lookup.timeout=500ms"
                        + " stats.interval=360s failover.interval=10s");

        String cfg = s.configLine();
        assertTrue("配置行要能直接 grep 到表名: " + cfg, cfg.startsWith("[dual-lookup] table=dim_account cfg["));
        assertTrue("配置行要包含真正影响行为的参数: " + cfg,
                cfg.contains("primary=hbase") && cfg.contains("batch.size=50")
                        && cfg.contains("lookup.timeout=500ms") && cfg.contains("stats.interval=360s"));

        s.recordPrimaryBatch(720, 1_000_000L);
        String line = s.snapshotLine();
        assertTrue("首行吞吐按配置间隔算（720/360）: " + line, line.contains("rate=2.0/s"));
    }

    @Test
    public void totalLineKeepsCumulativeViewForJobShutdown() {
        LookupStats s = stats();

        s.recordPrimaryBatch(2, 2_000_000L);
        s.snapshotLine();                       // 推进快照游标，模拟已打过一行阶段日志
        s.recordPrimaryBatch(3, 4_000_000L);    // 又在下一个阶段跑了 1 批

        // 打点是阶段值，因此收尾行必须给出「自启动以来」的视角，否则长期运行后总量无从得知
        String total = s.totalLine();
        assertTrue("累计汇总应含全部 5 个 key: " + total,
                total.contains("table=dim_account since-start uptime=") && total.contains("totalKeys=5 rate="));
        assertTrue("累计平均耗时 = 6ms / 2 批: " + total,
                total.contains("hbase[batches=2 keys=5 fail=0 avg=3.0ms"));
        assertTrue(total.contains("failover=0"));
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
}
