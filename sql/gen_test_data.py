"""Kafka 测试数据生成器（配合 TEST_PLAN.md / e2e_test.sql 使用）。

三种模式（直接改下面的 MODE 常量即可，不用命令行参数）：

  MODE = 'basic'  低速率（RATE 条/秒），按固定序列轮转发，便于逐条核对 print 输出。
                  序列里每轮先把 A001 连发 3 次，用于验证「批内重复 key 每条都有结果」
                  （对应 TEST_PLAN 案例 12，回归曾经存在的静默丢数据缺陷）。
  MODE = 'burst'  高速率压测（RATE 条/秒），随机发，用于观察攒批效果（统计日志的 avgBatch）。
                  对应 TEST_PLAN 案例 20/21。
  MODE = 'cust'   E 组 typed rowkey 测试用：BIGINT 主键 1001~1003（HBase/Doris 都有）与
                  9001（两源都没有）。对应 TEST_PLAN 案例 24，配合 e2e_test.sql 第 7 节。

依赖：pip install kafka-python
"""

import json
import random
import time

from kafka import KafkaProducer

# ==================== 硬编码配置（按需直接改这里）====================
BROKER = "localhost:9092"
TOPIC = "test"
MODE = "basic"        # basic | burst | cust
RATE = 5              # 每秒发送条数（burst 模式建议改成 2000）

# ---- 账户维度测试数据（B / C / D 组）----
HIT_KEYS = ["A%03d" % i for i in range(1, 11)]           # A001~A010：HBase 与 Doris 都有 -> 命中主源
DORIS_ONLY_KEYS = ["A%03d" % i for i in range(11, 21)]   # A011~A020：仅 Doris 有 -> 验证「查不到不降级」
NOWHERE_KEYS = ["A90%d" % i for i in range(1, 6)]        # A901~A905：两源都没有

# ---- 客户维度测试数据（E 组，typed rowkey / BIGINT 主键）----
CUST_HIT_KEYS = [1001, 1002, 1003]                       # HBase + Doris 都有
CUST_MISS_KEYS = [9001]                                  # 两源都没有

# ---- 交易流水随机字段的取值范围 ----
TRANS_TYPES = ["PAY", "TRANSFER", "WITHDRAW", "DEPOSIT"]  # F 组 dim_mcc 的 key 取这几个值
CHANNELS = ["APP", "WEB", "ATM", "COUNTER"]
D_C_FLAGS = ["D", "C"]                                    # 借/贷标志


def basic_keys():
    """固定序列轮转：每轮先连发 3 次 A001，再依次走「命中 / 仅 Doris / 都不存在」。

    返回的序列可预测，便于对着 print 输出逐条核对（案例 9~12）。
    """
    seq = ["A001", "A001", "A001"]   # 批内重复 key：3 条都必须拿到 Name01
    seq += HIT_KEYS                  # A001 又会出现一次，无妨
    seq += DORIS_ONLY_KEYS
    seq += NOWHERE_KEYS
    while True:
        for k in seq:
            yield k


def burst_keys():
    """压测序列：80% 命中、10% 仅 Doris、10% 都不存在（随机发，用于观察攒批）。"""
    while True:
        r = random.random()
        if r < 0.8:
            yield random.choice(HIT_KEYS)
        elif r < 0.9:
            yield random.choice(DORIS_ONLY_KEYS)
        else:
            yield random.choice(NOWHERE_KEYS)


def cust_keys():
    """E 组序列：1001~1003 与 9001 交错轮转（BIGINT 主键）。"""
    seq = CUST_HIT_KEYS + CUST_MISS_KEYS
    while True:
        for k in seq:
            yield k


def make_msg(seq, key):
    """按交易流水模型拼一条消息。

    seq 是自增序号（用于生成唯一的流水号），key 是维表主键：
    字符串（account_no）或整数（cust_id，E 组用）。
    """
    msg = {
        "trans_jnls_no": "T%08d" % seq,
        "trans_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "trans_type": random.choice(TRANS_TYPES),
        "trans_chnl": random.choice(CHANNELS),
        "d_c_flag": random.choice(D_C_FLAGS),
        "amount": "%.2f" % random.uniform(1.0, 100000.0),
        "party_name": "P%05d" % random.randint(1, 99999),
    }
    if isinstance(key, int):
        # E 组：BIGINT 主键，Kafka 源表 trans_cust 声明的是 cust_id BIGINT
        msg["cust_id"] = key
    else:
        msg["account_no"] = key
    return msg


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    if MODE == "basic":
        keys = basic_keys()
        interval = 1.0 / RATE
    elif MODE == "burst":
        keys = burst_keys()
        interval = 1.0 / RATE
    elif MODE == "cust":
        keys = cust_keys()
        interval = 1.0 / RATE
    else:
        raise SystemExit("MODE 只能是 basic / burst / cust，当前为: %s" % MODE)

    seq = 0
    print("mode=%s rate=%d/s broker=%s topic=%s，Ctrl+C 停止" % (MODE, RATE, BROKER, TOPIC))
    try:
        while True:
            seq += 1
            key = next(keys)
            msg = make_msg(seq, key)
            producer.send(TOPIC, msg)
            # 低速率模式每 10 条刷一次并打印，便于人工跟读；压测模式每 2000 条刷一次
            if MODE != "burst" and seq % 10 == 0:
                producer.flush()
                print("已发送 %d 条，最近 key=%s" % (seq, key))
            elif MODE == "burst" and seq % 2000 == 0:
                producer.flush()
                print("已发送 %d 条" % seq)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print("共发送 %d 条" % seq)


if __name__ == "__main__":
    main()
