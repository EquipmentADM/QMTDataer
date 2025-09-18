# -*- coding: utf-8 -*-
"""从订阅到接收 Redis 推送的端到端演示脚本（需桥已运行，且 control.enabled=true）

默认行为：
    1. 使用 config/realtime.yml（若存在）中的 redis/control/topic 等设置；
    2. 发送订阅并等待 ACK；
    3. 自动进入行情监听（watch 模式），并在退出前自动退订；
    4. 控制时长：--minutes（默认为 5 分钟）。

命令行参数仍可覆盖默认行为。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Optional

import redis


_DEFAULT_CONFIG_PATH = Path("config/realtime.yml")


def _load_defaults(config_path: Path) -> dict:
    """从 YAML 配置中抓取默认 redis/control/subscription 设置"""
    if not config_path.exists():
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return {
        "redis_url": raw.get("redis", {}).get("url", "redis://127.0.0.1:6379/0"),
        "ctrl_channel": raw.get("control", {}).get("channel", "xt:ctrl:sub"),
        "ack_prefix": raw.get("control", {}).get("ack_prefix", "xt:ctrl:ack"),
        "topic": raw.get("redis", {}).get("topic", "xt:topic:bar"),
        "codes": ",".join(raw.get("subscription", {}).get("codes", ["518880.SH"])),
        "periods": ",".join(raw.get("subscription", {}).get("periods", ["1m"])),
        "mode": raw.get("subscription", {}).get("mode"),
        "preload_days": raw.get("subscription", {}).get("preload_days", 1),
    }


def _split_csv(text: str) -> List[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def send_subscribe(cli, channel: str, strategy_id: str, codes, periods,
                   preload_days=None, topic=None, mode=None):
    payload = {
        "action": "subscribe",
        "strategy_id": strategy_id,
        "codes": codes,
        "periods": periods,
    }
    if preload_days is not None:
        payload["preload_days"] = int(preload_days)
    if topic:
        payload["topic"] = topic
    if mode:
        payload["mode"] = mode
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))
    return payload


def send_unsubscribe(cli, channel: str, strategy_id: str,
                     sub_id: str = None, codes=None, periods=None):
    payload = {"action": "unsubscribe", "strategy_id": strategy_id}
    if sub_id:
        payload["sub_id"] = sub_id
    if codes:
        payload["codes"] = codes
    if periods:
        payload["periods"] = periods
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))


def main():
    defaults = _load_defaults(_DEFAULT_CONFIG_PATH)

    parser = argparse.ArgumentParser(
        description="行情桥端到端实验：订阅→监听→退订")
    parser.add_argument("--redis-url", default=defaults.get("redis_url", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--ctrl-channel", default=defaults.get("ctrl_channel", "xt:ctrl:sub"))
    parser.add_argument("--ack-prefix", default=defaults.get("ack_prefix", "xt:ctrl:ack"))
    parser.add_argument("--topic", default=defaults.get("topic", "xt:topic:bar"))
    parser.add_argument("--strategy-id", default="demo")
    parser.add_argument("--codes", default=defaults.get("codes", "518880.SH"))
    parser.add_argument("--periods", default=defaults.get("periods", "1m"))
    parser.add_argument("--mode", default=defaults.get("mode"))
    parser.add_argument("--preload-days", type=int, default=defaults.get("preload_days", 1))
    parser.add_argument("--ack-timeout", type=float, default=5.0)
    parser.add_argument("--minutes", type=int, default=5,
                       help="监听分钟数（默认 5 分钟）")
    parser.add_argument("--no-watch", action="store_true",
                       help="仅发送订阅并等待 ACK，不进入监听")
    parser.add_argument("--no-unsubscribe", action="store_true",
                       help="退出前不自动退订")
    args = parser.parse_args()

    cli = redis.from_url(args.redis_url, decode_responses=True)
    codes = _split_csv(args.codes)
    periods = _split_csv(args.periods)
    if not codes or not periods:
        parser.error("codes/periods 不能为空")

    send_subscribe(cli, args.ctrl_channel, args.strategy_id,
                   codes, periods, args.preload_days,
                   topic=args.topic, mode=args.mode)

    ack_ch = f"{args.ack_prefix}:{args.strategy_id}"
    channels = [ack_ch]
    if not args.no_watch:
        channels.append(args.topic)
    ps = cli.pubsub()
    ps.subscribe(*channels)
    while ps.get_message(timeout=0.05):
        pass

    print(f"[FLOW] redis={args.redis_url} ctrl={args.ctrl_channel} topic={args.topic}")
    print(f"[FLOW] waiting ACK on {ack_ch} ...")

    sub_id = None
    ack_deadline = time.time() + args.ack_timeout
    while time.time() < ack_deadline:
        msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if not msg:
            continue
        if msg.get("channel") != ack_ch:
            continue
        try:
            ack = json.loads(msg.get("data", "{}"))
        except Exception:
            print(f"[ACK] malformed payload: {msg.get('data')}")
            continue
        print("[ACK]", ack)
        if ack.get("ok") and ack.get("action") == "subscribe":
            sub_id = ack.get("sub_id", sub_id)
        break
    else:
        print("[FLOW] 未在超时时间内收到订阅 ACK")

    if args.no_watch:
        if not args.no_unsubscribe and sub_id:
            send_unsubscribe(cli, args.ctrl_channel, args.strategy_id, sub_id=sub_id)
            print(f"[FLOW] auto unsubscribe sent, sub_id={sub_id}")
        ps.close()
        if sub_id:
            print("[FLOW] 订阅已建立，可启动 scripts/simple_bar_listener.py 观看行情")
        return 0

    print(f"[FLOW] watching topic {args.topic} for {args.minutes} minute(s)...")
    end_ts = time.time() + max(1, args.minutes) * 60
    try:
        while time.time() < end_ts:
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                continue
            channel = msg.get("channel")
            data = msg.get("data")
            if channel == ack_ch:
                try:
                    ack = json.loads(data)
                    print("[ACK]", ack)
                except Exception:
                    print("[ACK parse err]", data)
            elif channel == args.topic:
                try:
                    bar = json.loads(data)
                    print("[BAR]", bar.get("code"), bar.get("period"), bar.get("bar_end_ts"),
                          "close=", bar.get("close"), "is_closed=", bar.get("is_closed"))
                except Exception:
                    print("[BAR raw]", data)
    finally:
        if not args.no_unsubscribe and sub_id:
            send_unsubscribe(cli, args.ctrl_channel, args.strategy_id, sub_id=sub_id)
            print(f"[FLOW] auto unsubscribe sent, sub_id={sub_id}")
        ps.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
