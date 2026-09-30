package com.roc.flink.connector.dual;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 单源维表读取抽象（批量接口）。
 *
 * <p>本接口是「HBase 与 Doris 可互相替换」的契约层：上层 {@link DualLookupFunction} 只依赖这个接口，
 * 不关心底层是异步 RPC 还是 JDBC，因此主备路由、降级逻辑只写一份，两个实现可自由对调。
 *
 * <p>约定（实现类必须遵守，否则降级逻辑会出错）：
 * <ul>
 *   <li>{@link #batchLookupAsync} 返回的 Future 必须已经在自身内部完成超时控制，
 *       超时以 {@link java.util.concurrent.TimeoutException} 结束，主备路由据此降级；
 *       即：<b>Future 若不正常完成，就一定是「超时或异常」</b>，上层不需要自己再加计时器；</li>
 *   <li>返回值统一为 {@code Map<下标, List<Map<列名, Java对象>>>}，下标对应入参 keys 的顺序，
 *       未命中的 key 不出现在结果里（调用方按空处理）；列名不区分大小写，保证 HBase 与 Doris 结果可互换；</li>
 *   <li>实现类必须可序列化（会随 Function 下发到 TaskManager），因此
 *       {@code AsyncConnection} / {@code HikariDataSource} 这类资源句柄必须声明为 {@code transient}，
 *       在 {@link #open()} 里按需重建。</li>
 * </ul>
 */
public interface LookupReader extends Serializable, AutoCloseable {

    /**
     * 建立连接/连接池。必须幂等：已打开时直接返回。
     *
     * <p>采用惰性打开而不是在算子 open 阶段强制建连：某一源在作业启动时刻不可用
     * （比如主源 HBase 整体不可用）时，作业依然能正常启动并直接走备源。
     *
     * <p>并发要求：实现必须是线程安全的（加 {@code synchronized}，
     * 因为攒批后的批量查询可能由不同的攒批线程触发 open）。
     *
     * @throws Exception 建连失败；调用方会捕获并直接判该源查询失败，从而触发降级
     */
    void open() throws Exception;

    /** 当前连接是否可用；未打开时主备路由会先尝试打开再查询 */
    boolean isOpened();

    /**
     * 按主键批量异步查询。
     *
     * @param keys 已经从 Flink 内部类型转成 Java 对象的主键值列表，顺序与 DDL 主键一致
     * @return 下标 -> 命中行列表；查不到的下标不出现在结果里（空结果不等于故障，不据此降级）
     */
    CompletableFuture<Map<Integer, List<Map<String, Object>>>> batchLookupAsync(List<Object[]> keys);

    /** 释放连接/线程池。实现应容忍重复调用（{@code close} 后再 {@code close}） */
    @Override
    void close() throws Exception;
}
