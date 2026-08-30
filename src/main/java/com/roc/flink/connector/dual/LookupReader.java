package com.roc.flink.connector.dual;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 单源维表读取抽象（批量接口）。
 *
 * <p>约定：
 * <ul>
 *   <li>{@link #batchLookupAsync} 返回的 Future 必须已经在自身内部完成超时控制，
 *       超时以 {@link java.util.concurrent.TimeoutException} 结束，主备路由据此降级；</li>
 *   <li>返回值统一为 {@code Map<下标, List<Map<列名, Java对象>>>}，下标对应入参 keys 的顺序，
 *       未命中的 key 不出现在结果里（调用方按空处理）；列名不区分大小写，保证 HBase 与 Doris 结果可互换；</li>
 *   <li>实现类必须可序列化（会随 Function 下发到 TaskManager）。</li>
 * </ul>
 */
public interface LookupReader extends Serializable, AutoCloseable {

    /**
     * 建立连接/连接池。必须幂等：已打开时直接返回。
     *
     * <p>采用惰性打开而不是在算子 open 阶段强制建连：某一源在作业启动时刻不可用
     * （比如主源 HBase 整体不可用）时，作业依然能正常启动并直接走备源。
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

    /** 释放连接/线程池 */
    @Override
    void close() throws Exception;
}
