# -*- coding: utf-8 -*-
"""
Redis 行情延迟旁路诊断脚本。

用途：
    - 只监听 Redis 行情 topic，不发送 subscribe/unsubscribe/status 命令。
    - 按 period + bar_end_ts 聚合同一轮 bar，比较 QMTD 发布时间和本监听端收到时间。
    - 用于初步判断延迟跨度更可能来自 QMT/xtdata/QMTD，还是 Redis/监听端链路。

注意：
    - 本脚本不会启动、停止或修改 QMTD。
    - 当前 QMTD payload 的 recv_ts 是秒级时间，因此诊断 6 秒级跨度足够，
      但不适合做毫秒级延迟归因。
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import redis


@dataclass
class BarArrival:
    """单条 bar 的接收观测记录。"""

    code: str
    period: str
    bar_ts: str
    qmtd_recv_ts: Optional[float]
    listener_ts: float


def _parse_local_iso(value: object) -> Optional[float]:
    """解析 QMTD payload 中的本地无时区 ISO 时间。"""
    if not value:
        return None
    try:
        text = str(value).replace(" ", "T")
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _fmt_epoch(value: Optional[float]) -> str:
    """格式化本地 epoch 秒。"""
    if value is None:
        return "-"
    return datetime.fromtimestamp(value).strftime("%H:%M:%S.%f")[:-3]


def _period_finalize_seconds(period: str) -> float:
    """返回轮次固化等待时间，避免把正常迟到 bar 过早排除。"""
    p = (period or "").lower()
    if p in {"1m", "1min", "1minute"}:
        return 35.0
    if p in {"1d", "1day"}:
        return 90.0
    return 35.0


def _print_group_result(key: Tuple[str, str], rows: Dict[str, BarArrival]) -> None:
    """打印单轮延迟诊断结果。"""
    period, bar_ts = key
    items = list(rows.values())
    listener_times = [item.listener_ts for item in items]
    qmtd_times = [item.qmtd_recv_ts for item in items if item.qmtd_recv_ts is not None]
    listener_span = max(listener_times) - min(listener_times)
    qmtd_span = max(qmtd_times) - min(qmtd_times) if len(qmtd_times) >= 2 else 0.0
    redis_delays = [
        item.listener_ts - item.qmtd_recv_ts
        for item in items
        if item.qmtd_recv_ts is not None
    ]
    avg_redis_delay = statistics.mean(redis_delays) if redis_delays else None
    max_redis_delay = max(redis_delays) if redis_delays else None

    if len(qmtd_times) >= 2 and qmtd_span >= max(3.0, listener_span * 0.7):
        guess = "主要像 QMT/xtdata/QMTD 发布前跨度"
    elif len(qmtd_times) >= 2 and listener_span - qmtd_span >= 3.0:
        guess = "主要像 Redis/监听端到达跨度"
    elif len(qmtd_times) < 2:
        guess = "缺少 recv_ts，无法可靠拆分"
    else:
        guess = "两段差异不明显，需结合更多轮次"

    print(
        "[轮次诊断] "
        f"周期={period} bar={bar_ts} 标的数={len(items)} "
        f"监听跨度={listener_span:.3f}s "
        f"QMTD发布时间跨度={qmtd_span:.3f}s "
        f"Redis后段平均延迟={avg_redis_delay:.3f}s " if avg_redis_delay is not None else
        "[轮次诊断] "
        f"周期={period} bar={bar_ts} 标的数={len(items)} "
        f"监听跨度={listener_span:.3f}s "
        f"QMTD发布时间跨度={qmtd_span:.3f}s "
        "Redis后段平均延迟=- "
    )
    print(
        f"  最早监听={_fmt_epoch(min(listener_times))} "
        f"最晚监听={_fmt_epoch(max(listener_times))} "
        f"最早QMTD发布={_fmt_epoch(min(qmtd_times) if qmtd_times else None)} "
        f"最晚QMTD发布={_fmt_epoch(max(qmtd_times) if qmtd_times else None)} "
        f"Redis后段最大延迟={max_redis_delay:.3f}s" if max_redis_delay is not None else
        f"  最早监听={_fmt_epoch(min(listener_times))} "
        f"最晚监听={_fmt_epoch(max(listener_times))} "
        f"最早QMTD发布=- 最晚QMTD发布=- Redis后段最大延迟=-"
    )
    print(f"  初步判断={guess}")
    if listener_span > 6.0:
        print("  WARNING：该轮监听跨度超过 6 秒，建议重点查看 QMTD 日志中对应标的回调时间。")


def main(argv: Optional[List[str]] = None) -> int:
    """脚本入口：被动监听 Redis 并输出延迟拆分诊断。"""
    parser = argparse.ArgumentParser(description="Redis 行情延迟旁路诊断脚本")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--topic", default="xt:topic:bar")
    parser.add_argument("--seconds", type=float, default=180.0, help="运行秒数，默认 180 秒")
    args = parser.parse_args(argv)

    cli = redis.from_url(args.redis_url, decode_responses=True)
    ps = cli.pubsub()
    ps.subscribe(args.topic)
    while ps.get_message(timeout=0.05):
        pass

    groups: Dict[Tuple[str, str], Dict[str, BarArrival]] = {}
    finalized = set()
    start = time.time()
    deadline = start + max(1.0, args.seconds)
    print(f"[诊断] 只监听 Redis topic={args.topic}，运行 {args.seconds:g} 秒，不会控制 QMTD。")
    try:
        while time.time() < deadline:
            now = time.time()
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg and msg.get("channel") == args.topic:
                try:
                    payload = json.loads(msg.get("data") or "{}")
                except Exception:
                    continue
                code = str(payload.get("code") or "")
                period = str(payload.get("period") or "")
                bar_ts = str(payload.get("bar_end_ts") or payload.get("time") or "")
                if not code or not period or not bar_ts:
                    continue
                key = (period, bar_ts)
                rows = groups.setdefault(key, {})
                rows[code] = BarArrival(
                    code=code,
                    period=period,
                    bar_ts=bar_ts,
                    qmtd_recv_ts=_parse_local_iso(payload.get("recv_ts")),
                    listener_ts=now,
                )

            for key, rows in list(groups.items()):
                if key in finalized or not rows:
                    continue
                period, _ = key
                first_listener = min(item.listener_ts for item in rows.values())
                if now - first_listener >= _period_finalize_seconds(period):
                    finalized.add(key)
                    _print_group_result(key, rows)
    finally:
        ps.close()

    for key, rows in sorted(groups.items()):
        if key not in finalized:
            _print_group_result(key, rows)
    print("[诊断] 结束。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
