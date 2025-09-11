# -*- coding: utf-8 -*-
"""从订阅到接收 Redis 推送的端到端演示脚本（需桥已运行，且控制面 enabled）

类说明：
    - FlowDemo：发送订阅命令 → 订阅 Redis topic → 打印接收的行情条目；可选在结束时自动退订。
功能：
    - 便于在“有实时行情的时段”做一次简单的实验联调；
上下游：
    - 上游：运行中的 run_with_config（control.enabled=true）；
    - 下游：Redis（控制通道与行情 topic）。
"""
from __future__ import annotations
import argparse
import json
import sys
import time
import redis


def send_subscribe(cli, channel: str, strategy_id: str, codes, periods, preload_days=None, topic=None):
    payload = {"action": "subscribe", "strategy_id": strategy_id, "codes": codes, "periods": periods}
    if preload_days is not None:
        payload["preload_days"] = int(preload_days)
    if topic:
        payload["topic"] = topic
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))
    return payload


def send_unsubscribe(cli, channel: str, strategy_id: str, sub_id: str = None, codes=None, periods=None):
    payload = {"action": "unsubscribe", "strategy_id": strategy_id}
    if sub_id:
        payload["sub_id"] = sub_id
    if codes:
        payload["codes"] = codes
    if periods:
        payload["periods"] = periods
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))


def main():
    p = argparse.ArgumentParser(description="行情桥端到端实验：订阅→接收 Redis 推送")
    p.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    p.add_argument("--ctrl-channel", default="xt:ctrl:sub")
    p.add_argument("--ack-prefix", default="xt:ctrl:ack")
    p.add_argument("--topic", default="xt:topic:bar", help="桥发布的行情 topic（需与桥配置一致）")
    p.add_argument("--strategy-id", default="demo")
    p.add_argument("--codes", default="518880.SH")
    p.add_argument("--periods", default="1m")
    p.add_argument("--minutes", type=int, default=2, help="监听分钟数，上班时间内请≥2")
    p.add_argument("--preload-days", type=int, default=1)
    p.add_argument("--auto-unsubscribe", action="store_true")
    args = p.parse_args()

    cli = redis.from_url(args.redis_url, decode_responses=True)

    # 发送订阅命令
    codes = [x.strip() for x in args.codes.split(",") if x.strip()]
    periods = [x.strip() for x in args.periods.split(",") if x.strip()]
    send_subscribe(cli, args.ctrl_channel, args.strategy_id, codes, periods, args.preload_days, args.topic)

    # 订阅 ACK 与 行情 topic
    ack_ch = f"{args.ack_prefix}:{args.strategy_id}"
    ps = cli.pubsub()
    ps.subscribe(ack_ch, args.topic)

    print("已订阅：", ack_ch, "和", args.topic)
    t_end = time.time() + args.minutes * 60
    sub_id = None
    try:
        while time.time() < t_end:
            m = ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not m:
                continue
            ch = m.get("channel"); dat = m.get("data")
            if ch == ack_ch:
                try:
                    ack = json.loads(dat)
                    print("[ACK]", ack)
                    if ack.get("action") == "subscribe" and ack.get("ok"):
                        sub_id = ack.get("sub_id", sub_id)
                except Exception as e:
                    print("[ACK parse err]", e)
            elif ch == args.topic:
                try:
                    bar = json.loads(dat)
                    print("[BAR]", bar.get("code"), bar.get("period"), bar.get("bar_end_ts"),
                          "close=", bar.get("close"), "is_closed=", bar.get("is_closed"))
                except Exception as e:
                    print("[BAR parse err]", e)
    finally:
        if args.auto_unsubscribe and sub_id:
            send_unsubscribe(cli, args.ctrl_channel, args.strategy_id, sub_id=sub_id)
            print("已发送退订：sub_id=", sub_id)
        ps.close()


if __name__ == "__main__":
    sys.exit(main())
