# -*- coding: utf-8 -*-
"""Redis 行情监听脚本（仅接收端）

默认参数：
    - 若存在 config/realtime.yml，则使用其中的 redis.url 与 redis.topic；
    - 否则退回本地 redis://127.0.0.1:6379/0 与 topic=xt:topic:bar。

功能：
    - 订阅指定 Redis PubSub topic；
    - 打印收到的宽表 JSON，支持 --pretty 格式化；
    - 便于在 IDE 直接运行验证桥的实时推送。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

import redis

_DEFAULT_CONFIG_PATH = Path("config/realtime.yml")


def _load_defaults(config_path: Path) -> dict:
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
        "topic": raw.get("redis", {}).get("topic", "xt:topic:bar"),
    }


def main(argv: Optional[list[str]] = None) -> int:
    defaults = _load_defaults(_DEFAULT_CONFIG_PATH)

    parser = argparse.ArgumentParser(description="简单 Redis 行情监听器")
    parser.add_argument("--redis-url", default=defaults.get("redis_url", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--topic", default=defaults.get("topic", "xt:topic:bar"))
    parser.add_argument("--pretty", action="store_true", help="是否格式化打印 JSON")
    parser.add_argument("--wait", type=float, default=0.0,
                       help="订阅后等待秒数再开始消费（可选）")
    args = parser.parse_args(argv)

    cli = redis.from_url(args.redis_url, decode_responses=True)
    ps = cli.pubsub()
    ps.subscribe(args.topic)
    while ps.get_message(timeout=0.05):
        pass

    print(f"[LISTEN] redis={args.redis_url} topic={args.topic}")
    if args.wait > 0:
        time.sleep(args.wait)

    try:
        while True:
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                continue
            data = msg.get("data")
            if data is None:
                continue
            try:
                payload = json.loads(data)
                if args.pretty:
                    text = json.dumps(payload, ensure_ascii=False, indent=2)
                else:
                    text = json.dumps(payload, ensure_ascii=False)
                print(f"[BAR] {text}")
            except json.JSONDecodeError:
                print(f"[BAR-RAW] {data}")
    except KeyboardInterrupt:
        print("\n[LISTEN] stop by user")
    finally:
        ps.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
