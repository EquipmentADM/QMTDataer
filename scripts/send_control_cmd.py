# -*- coding = utf-8 -*-
# @Time : 2025/9/10 22:55
# @Author : EquipmentADV
# @File : send_control_cmd.py
# @Software : PyCharm
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import json

import redis


def main() -> None:
    p = argparse.ArgumentParser(description="向控制通道发送订阅/退订/查询命令")
    p.add_argument("--url", default="redis://127.0.0.1:6379/0")
    p.add_argument("--channel", default="xt:ctrl:sub")
    p.add_argument("--action", choices=["subscribe", "unsubscribe", "status"], required=True)
    p.add_argument("--strategy-id", required=True)
    p.add_argument("--codes", default="")
    p.add_argument("--periods", default="")
    p.add_argument("--preload-days", type=int, default=None)
    p.add_argument("--topic", default=None)
    args = p.parse_args()

    cli = redis.from_url(args.url, decode_responses=True)

    payload = {
        "action": args.action,
        "strategy_id": args.strategy_id,
    }
    if args.codes:
        payload["codes"] = [x.strip() for x in args.codes.split(",") if x.strip()]
    if args.periods:
        payload["periods"] = [x.strip() for x in args.periods.split(",") if x.strip()]
    if args.preload_days is not None:
        payload["preload_days"] = args.preload_days
    if args.topic:
        payload["topic"] = args.topic

    cli.publish(args.channel, json.dumps(payload, ensure_ascii=False))
    print("sent:", json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
