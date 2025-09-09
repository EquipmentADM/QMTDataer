# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:15
# @Author : EquipmentADV
# @File : run_realtime_bridge.py
# @Software : PyCharm
"""脚本：实时订阅→Redis PubSub（QMT 实参接线）

脚本说明：
    - 功能：读取订阅配置，建立 QMT 实时订阅（1m/1h/1d），默认 close-only 发布至 Redis PubSub；
    - 上游：运维/策略侧；
    - 下游：策略/DB 消费端（通过 PubSub）。

用法示例：
    python scripts/run_realtime_bridge.py \
        --codes 000001.SZ,600000.SH --periods 1m,1d --mode close_only \
        --redis-host 127.0.0.1 --redis-port 6379 --topic xt:topic:bar
"""
from __future__ import annotations
import argparse
import logging

from core.qmt_connector import QMTConnector, QMTConfig
from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
from core.pubsub_publisher import PubSubPublisher


def main():
    parser = argparse.ArgumentParser(description="实时订阅→Redis（订阅前预热补齐）")
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--periods", type=str, default="1m")
    parser.add_argument("--mode", type=str, default="close_only", choices=["close_only", "forming_and_close"])
    parser.add_argument("--close-delay-ms", type=int, default=100)
    parser.add_argument("--preload-days", type=int, default=3)
    parser.add_argument("--redis-host", type=str, default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-password", type=str, default=None)
    parser.add_argument("--topic", type=str, default="xt:topic:bar")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    QMTConnector(QMTConfig(mode="none")).listen_and_connect()

    publisher = PubSubPublisher(host=args.redis_host, port=args.redis_port, password=args.redis_password, topic=args.topic)
    cfg = RealtimeConfig(mode=args.mode,
                         periods=[p.strip() for p in args.periods.split(",") if p.strip()],
                         codes=[c.strip() for c in args.codes.split(",") if c.strip()],
                         close_delay_ms=args.close_delay_ms,
                         preload_days=args.preload_days)
    svc = RealtimeSubscriptionService(cfg, publisher)
    svc.run_forever()


if __name__ == "__main__":
    main()

