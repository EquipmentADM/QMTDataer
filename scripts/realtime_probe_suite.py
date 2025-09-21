# -*- coding: utf-8 -*-
"""QMTDataer 实时链路综合探针

默认针对 510050.SH / 159915.SZ 的 1 分钟行情，可按需选择以下实验：
1. 历史链路：调用 HistoryAPI.fetch_bars（封装 get_market_data_ex）验证 count/head/tail；
2. 直连链路：直接使用 xtdata.subscribe_quote 等待一次行情回调；
3. 桥接链路：向控制面发送订阅命令并监听 Redis topic。

示例：
    python scripts/realtime_probe_suite.py --history --direct --bridge
"""
from __future__ import annotations

import argparse
import json
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from xtquant import xtdata  # type: ignore
import redis  # type: ignore

from core.history_api import HistoryAPI, HistoryConfig
from core.local_cache import LocalCache, CacheConfig

DEFAULT_CODES = ["510050.SH", "159915.SZ"]
DEFAULT_PERIOD = "1m"
DEFAULT_TOPIC = "xt:topic:bar"
DEFAULT_CTRL = "xt:ctrl:sub"
DEFAULT_ACK = "xt:ctrl:ack"
DEFAULT_STRATEGY = "probe"


def check_history(codes: List[str], period: str, minutes: int) -> None:
    """历史链路验证：调用 HistoryAPI.fetch_bars 并输出关键字段"""
    cache = LocalCache(CacheConfig())
    api = HistoryAPI(HistoryConfig(), cache=cache)
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(minutes=minutes)
    res = api.fetch_bars(
        codes,
        period,
        start_dt.isoformat(timespec="seconds"),
        end_dt.isoformat(timespec="seconds"),
        return_data=False,
    )
    print("[HISTORY] status=", res["status"],
          "count=", res["count"],
          "gaps=", len(res["gaps"]),
          "head=", res["head_ts"],
          "tail=", res["tail_ts"])


def check_direct_xtdata(codes: List[str], period: str, wait_sec: int) -> None:
    """直连链路验证：直接订阅 xtdata，等待一条回调并打印"""
    event = threading.Event()
    snapshot: Dict[str, Any] = {}

    def _cb(datas: Dict[str, List[Dict[str, Any]]]):
        for code, rows in datas.items():
            for row in rows:
                snapshot.clear()
                snapshot.update({
                    "code": code,
                    "time": row.get("time"),
                    "close": row.get("close"),
                    "isClosed": row.get("isClosed", row.get("closed")),
                })
                event.set()

    for code in codes:
        xtdata.subscribe_quote(stock_code=code, period=period, count=0, callback=_cb)

    print(f"[DIRECT] 等待 {codes} {period} 回调，最长 {wait_sec} 秒…")
    if event.wait(wait_sec):
        print("[DIRECT] sample=", snapshot)
    else:
        print("[DIRECT] 超时未收到 xtdata 回调")


class RedisListener(threading.Thread):
    """订阅 Redis 通道，收集 ACK 与 BAR 消息"""
    daemon = True

    def __init__(self, client: "redis.Redis", channels: List[str]):
        super().__init__(name="RedisListener")
        self._ps = client.pubsub()
        self._ps.subscribe(*channels)
        while self._ps.get_message(timeout=0.05):
            pass
        self._stop = threading.Event()
        self.messages: List[Dict[str, Any]] = []

    def run(self) -> None:
        while not self._stop.is_set():
            msg = self._ps.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if not msg:
                continue
            data = msg.get("data")
            channel = msg.get("channel")
            payload: Dict[str, Any]
            if isinstance(data, bytes):
                try:
                    data = data.decode("utf-8", errors="ignore")
                except Exception:
                    data = repr(data)
            if isinstance(data, str):
                try:
                    payload = json.loads(data)
                except Exception:
                    payload = {"raw": data}
            else:
                payload = {"raw": data}
            payload["channel"] = channel
            self.messages.append(payload)

    def stop(self) -> None:
        self._stop.set()
        self._ps.close()


def check_bridge(redis_url: str, ctrl_channel: str, ack_prefix: str, topic: str,
                 strategy: str, codes: List[str], period: str, minutes: int) -> None:
    """桥接链路验证：向控制面发送订阅，并监听 Redis topic"""
    client = redis.from_url(redis_url, decode_responses=True)
    ack_channel = f"{ack_prefix}:{strategy}"
    listener = RedisListener(client, [ack_channel, topic])
    listener.start()

    subscribe_cmd = {
        "action": "subscribe",
        "strategy_id": strategy,
        "codes": codes,
        "periods": [period],
        "preload_days": 0,
    }
    client.publish(ctrl_channel, json.dumps(subscribe_cmd, ensure_ascii=False))

    deadline = time.time() + minutes * 60
    print(f"[BRIDGE] 监听 {topic}，窗口 {minutes} 分钟")
    try:
        while time.time() < deadline:
            time.sleep(1)
            while listener.messages:
                msg = listener.messages.pop(0)
                if msg.get("channel") == ack_channel:
                    print("[BRIDGE][ACK]", msg)
                else:
                    print("[BRIDGE][BAR]", msg)
    finally:
        listener.stop()
        unsubscribe_cmd = {
            "action": "unsubscribe",
            "strategy_id": strategy,
            "codes": codes,
            "periods": [period],
        }
        client.publish(ctrl_channel, json.dumps(unsubscribe_cmd, ensure_ascii=False))


def main() -> None:
    """命令行入口：根据参数执行历史/直连/桥接实验"""
    parser = argparse.ArgumentParser(description="QMTDataer 实时链路探针")
    parser.add_argument("--codes", default="510050.SH,159915.SZ")
    parser.add_argument("--period", default=DEFAULT_PERIOD)
    parser.add_argument("--history", action="store_true")
    parser.add_argument("--direct", action="store_true")
    parser.add_argument("--bridge", action="store_true")
    parser.add_argument("--minutes", type=int, default=2)
    parser.add_argument("--direct-wait", type=int, default=120)
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--ctrl-channel", default=DEFAULT_CTRL)
    parser.add_argument("--ack-prefix", default=DEFAULT_ACK)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--strategy", default=DEFAULT_STRATEGY)
    args = parser.parse_args()

    codes = [c.strip() for c in args.codes.split(',') if c.strip()]

    run_history = args.history
    run_direct = args.direct
    run_bridge = args.bridge
    if not any((run_history, run_direct, run_bridge)):
        run_history = run_direct = run_bridge = True

    if run_history:
        check_history(codes, args.period, args.minutes)
    if run_direct:
        check_direct_xtdata(codes, args.period, args.direct_wait)
    if run_bridge:
        check_bridge(args.redis_url, args.ctrl_channel, args.ack_prefix,
                     args.topic, args.strategy, codes, args.period, args.minutes)


if __name__ == "__main__":
    main()
