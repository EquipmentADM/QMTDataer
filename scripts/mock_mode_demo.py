# -*- coding: utf-8 -*-
"""Mock 行情桥 Demo

用途：
    - 在本地无 QMT/MiniQMT 环境时，快速启动一个“随机游走行情桥”；
    - 将生成的分钟/小时/日线推送至 Redis PubSub，便于后端链路或消费端联调；
    - 可通过参数控制基准价格、波动率、生成节奏与运行时长。

示例：
    python scripts/mock_mode_demo.py --redis-url redis://127.0.0.1:6379/0 \\
        --codes 510050.SH,159915.SZ --periods 1m --minutes 5 --step-seconds 0.5
"""
from __future__ import annotations

import argparse
import threading
import time
from typing import List, Tuple
from urllib.parse import urlparse

from core.pubsub_publisher import PubSubPublisher
from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService


def _parse_csv(text: str) -> List[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def _parse_redis_url(url: str) -> Tuple[str, int, str | None, int]:
    parsed = urlparse(url)
    if parsed.scheme not in {"redis", "rediss"}:
        raise ValueError(f"不支持的 redis URL：{url}")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379
    password = parsed.password
    path = (parsed.path or "").lstrip("/")
    db = int(path) if path else 0
    return host, port, password, db


def main() -> int:
    parser = argparse.ArgumentParser(description="Mock 行情桥 Demo（生成随机游走 bar 并推送 Redis）")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0", help="Redis 连接 URL")
    parser.add_argument("--topic", default="xt:topic:bar", help="发布的 Redis PubSub topic")
    parser.add_argument("--codes", default="MOCK.SH", help="订阅标的，逗号分隔")
    parser.add_argument("--periods", default="1m", help="订阅周期，逗号分隔（1m/1h/1d）")
    parser.add_argument("--mode", default="close_only", choices=["close_only", "forming_and_close"], help="推送模式")
    parser.add_argument("--minutes", type=float, default=3.0, help="运行时长（分钟，默认 3）")
    parser.add_argument("--base-price", type=float, default=10.0, help="Mock 行情基准价")
    parser.add_argument("--volatility", type=float, default=0.002, help="Mock 行情对数收益波动率")
    parser.add_argument("--step-seconds", type=float, default=1.0, help="生成下一根 bar 的节奏（秒）")
    parser.add_argument("--seed", type=int, default=None, help="随机数种子（可选）")
    parser.add_argument("--volume-mean", type=float, default=1_000_000, help="Mock 成交量均值")
    parser.add_argument("--volume-std", type=float, default=200_000, help="Mock 成交量标准差")
    args = parser.parse_args()

    codes = _parse_csv(args.codes)
    periods = _parse_csv(args.periods)
    if not codes:
        parser.error("codes 不能为空")
    if not periods:
        parser.error("periods 不能为空")

    host, port, password, db = _parse_redis_url(args.redis_url)
    publisher = PubSubPublisher(host=host, port=port, password=password, db=db, topic=args.topic)

    mock_cfg = RealtimeConfig.MockConfig(
        enabled=True,
        base_price=args.base_price,
        volatility=args.volatility,
        step_seconds=args.step_seconds,
        seed=args.seed,
        volume_mean=args.volume_mean,
        volume_std=args.volume_std,
        source="mock",
    )

    rt_cfg = RealtimeConfig(
        mode=args.mode,
        periods=periods,
        codes=codes,
        preload_days=0,
        mock=mock_cfg,
    )
    svc = RealtimeSubscriptionService(rt_cfg, publisher)

    worker = threading.Thread(target=svc.run_forever, name="MockBridge", daemon=True)
    worker.start()
    print(f"[MOCK] redis={args.redis_url} topic={args.topic} codes={codes} periods={periods}")
    print(f"[MOCK] base_price={args.base_price} volatility={args.volatility} step={args.step_seconds}s")
    try:
        time.sleep(max(0.1, args.minutes) * 60.0)
    except KeyboardInterrupt:
        print("\n[MOCK] interrupted by user, stopping ...")
    finally:
        svc.stop()
        worker.join(timeout=2.0)
    print("[MOCK] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
