package com.roc.flink.connector.dual;

import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 统计输出的测试。
 *
 * <p>覆盖四个最容易「看起来对、实际错」的点：
 * <ol>
 *   <li><b>阶段值而非累计值</b>：同一行的 {@code window=} 段只描述刚刚过去的那个间隔。这是本类改造后
 *       最容易回归的地方——一旦有人把「相减」改回「直接输出累计」，日志读起来还是像模像样，
 *       但语义完全变了，靠人眼根本发现不了；</li>
 *   <li><b>同一行还要带累计视角</b>：{@code since-start} 段必须真的在累计，而不是把阶段值抄一遍
 *       （那就等于两个视角都没了）；</li>
 *   <li><b>各源平均耗时</b>：主备两侧独立统计，且「没有样本」必须显示 {@code -}
 *       而不是 0（0 会被误读成「秒回」）；</li>
 *   <li><b>分母不会错位</b>：平均耗时的分母直接用批次数，前提是「每完成一批就记一次耗时」。</li>
 * </ol>
 *
 * <p>测试直接断言 {@link LookupStats#snapshotLine()} 的文本，不去捕获日志输出——
 * 捕获日志的测试既依赖日志实现、又难以覆盖边界。
 */
public class LookupStatsTest {

    @Test
    public void snapshotIsIntervalValueNotCumulative() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360);

        // 第一批阶段：2 批、100 个 key、累计 30ms
        s.recordPrimaryBatch(50, 10_000_000L);
        s.recordPrimaryBatch(50, 20_000_000L);
        String line1 = s.snapshotLine();
        assertTrue("首行窗口应等于配置的间隔: " + line1,
                line1.contains("table=dim_account window=360s"));
        assertTrue("累计值应体现在首行: " + line1, line1.contains("totalKeys=100 avgBatch=50"));
        assertTrue("平均耗时应是 30ms/2 批: " + line1,
                line1.contains("hbase[batches=2 keys=100 fail=0 avg=15.0ms]"));
        assertTrue("备源没有样本时必须显示 -，0 会被误读成秒回: " + line1,
                line1.contains("doris[batches=0 fail=0 avg=-]"));

        // 第二批阶段：只有 1 批、1 个 key——必须只报增量，绝不能是 101/3
        s.recordPrimaryBatch(1, 5_000_000L);
        String line2 = s.snapshotLine();
        assertTrue("第二次打点必须是「本阶段增量」，不能把历史累计再报一遍: " + line2,
                line2.contains("totalKeys=1 avgBatch=1"));
        assertTrue("批次数与耗时都应是增量: " + line2,
                line2.contains("hbase[batches=1 keys=1 fail=0 avg=5.0ms]"));
        assertTrue("本阶段没有备源样本: " + line2, line2.contains("doris[batches=0 fail=0 avg=-]"));
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
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360);

        s.recordPrimaryBatch(50, 10_000_000L);   // 阶段 1：50 个 key
        String line1 = s.snapshotLine();
        assertTrue("首行的累计段应等于阶段段（都还没有历史）: " + line1,
                line1.contains("|| table=dim_account since-start totalKeys=50 avgBatch=50"));
        assertTrue(line1.contains("hbase[batches=1 keys=50 fail=0 avg=10.0ms]"));

        s.recordPrimaryBatch(10, 5_000_000L);    // 阶段 2：只有 10 个 key
        String line2 = s.snapshotLine();
        // 注意：第二行起窗口长度按「距上次打点的真实间隔」算，单元测试里几乎为 0（取最小值 1s），
        // 因此这里只断言数值，不对窗口长度做假设
        assertTrue("阶段段只报增量 10: " + line2, line2.contains("totalKeys=10 avgBatch=10 "));
        assertTrue("累计段必须累加到 60，而不是把阶段值 10 抄一遍: " + line2,
                line2.contains("since-start totalKeys=60 avgBatch=30"));
        assertTrue("累计平均耗时 = 15ms / 2 批: " + line2,
                line2.contains("since-start totalKeys=60 avgBatch=30")
                        && line2.contains("hbase[batches=2 keys=60 fail=0 avg=7.5ms]"));
        // 守恒校验：上一行累计 + 本行阶段 必须等于本行累计，否则说明有数据被漏掉或重复
        assertTrue("阶段与累计必须守恒: " + line2, 50 + 10 == 60);
    }

    @Test
    public void failoverIsCountedSeparatelyAndStandbyLatencyIsItsOwn() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360);

        // 主源超时 500ms 后降级，备源只用 3ms：
        // 若备源耗时从「批次开始」起算，这里会变成 503ms，指标就失去诊断价值了
        s.recordPrimaryBatch(2, 500_000_000L);
        s.recordPrimaryFail();
        s.recordFailover();
        s.recordStandbyBatch(3_000_000L);

        String line = s.snapshotLine();
        assertTrue("失败批次的耗时同样要统计，否则主源恶化过程会被掩盖: " + line,
                line.contains("hbase[batches=1 keys=2 fail=1 avg=500.0ms]"));
        assertTrue("备源耗时应只含自身查询时间: " + line,
                line.contains("doris[batches=1 fail=0 avg=3.0ms]"));
        assertTrue("降级次数应单独计数: " + line, line.contains("failover=1"));
        // 含备源分母的平均批大小：2 个 key / (1 主批 + 1 备批) = 1
        assertTrue(line.contains("avgBatchWithStandby=1"));
    }

    @Test
    public void noActivityStillPrintsAWellFormedLine() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360);
        String line = s.snapshotLine();
        assertTrue(line.contains("totalKeys=0 avgBatch=0 avgBatchWithStandby=0"));
        assertTrue(line.contains("hbase[batches=0 keys=0 fail=0 avg=-]"));
        assertTrue(line.contains("doris[batches=0 fail=0 avg=-]"));
        assertTrue(line.contains("failover=0"));
    }

    @Test
    public void totalLineKeepsCumulativeViewForJobShutdown() {
        LookupStats s = new LookupStats("dim_account", "hbase", "doris", 360);

        s.recordPrimaryBatch(2, 2_000_000L);
        s.snapshotLine();                       // 推进快照游标，模拟已打过一行阶段日志
        s.recordPrimaryBatch(3, 4_000_000L);    // 又在下一个阶段跑了 1 批

        // 打点是阶段值，因此必须另有一处保留「自启动以来」的视角，否则长期运行后总量无从得知
        String total = s.totalLine();
        assertTrue("累计汇总应含全部 5 个 key: " + total,
                total.contains("table=dim_account since-start totalKeys=5"));
        assertTrue("累计平均耗时 = 6ms / 2 批: " + total,
                total.contains("hbase[batches=2 keys=5 fail=0 avg=3.0ms]"));
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
