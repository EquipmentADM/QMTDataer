# -*- coding: utf-8 -*-
"""
通过 Redis 查询 QMTD 当前实时订阅状态。

职责：
    - 向控制通道发送 `status` 命令。
    - 监听对应 ACK 通道并等待返回。
    - 以简洁中文格式打印当前活跃行情流、引用计数与最近发布时间。

数据契约：
    - 请求：`{"action": "status", "strategy_id": "<id>"}`。
    - 返回：兼容当前 QMTD status ACK 结构。

外部系统：
    - Redis。
"""
from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, Optional

import redis


def query_status(
    redis_url: str = "redis://127.0.0.1:6379/0",
    ctrl_channel: str = "xt:ctrl:sub",
    ack_prefix: str = "xt:ctrl:ack",
    strategy_id: str = "status_probe",
    timeout: float = 10.0,
) -> Optional[Dict[str, Any]]:
    """
    发送 status 查询并等待 ACK。

    Args:
        redis_url (str): Redis 连接 URL。
        ctrl_channel (str): 控制通道。
        ack_prefix (str): ACK 前缀。
        strategy_id (str): 用于接收 ACK 的策略 ID。
        timeout (float): 等待 ACK 的秒数。

    Returns:
        Optional[Dict[str, Any]]: 成功时返回 ACK JSON，超时返回 None。
    """
    cli = redis.from_url(redis_url, decode_responses=True)
    ack_ch = f"{ack_prefix}:{strategy_id}"
    ps = cli.pubsub()
    ps.subscribe(ack_ch)
    while ps.get_message(timeout=0.05):
        pass

    payload = {"action": "status", "strategy_id": strategy_id}
    cli.publish(ctrl_channel, json.dumps(payload, ensure_ascii=False))

    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if not msg:
                continue
            data = msg.get("data")
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except Exception:
                    continue
        return None
    finally:
        ps.close()


def _print_status(payload: Dict[str, Any]) -> None:
    """
    打印状态查询结果。

    Args:
        payload (Dict[str, Any]): status ACK 结果。

    Returns:
        None
    """
    print("[STATUS ACK]", json.dumps(payload, ensure_ascii=False))
    status = payload.get("status") or {}
    active_subs = status.get("subs") or []
    last_published = status.get("last_published") or {}
    registry_sub_ids = payload.get("subs") or []

    print(f"[STATUS] 当前活跃行情流数量：{len(active_subs)}")
    for item in active_subs:
        code = item.get("code")
        period = item.get("period")
        ref_count = item.get("ref_count")
        print(f"  - {code} {period} ref_count={ref_count}")

    print(f"[STATUS] 最近发布时间数量：{len(last_published)}")
    for key, value in sorted(last_published.items()):
        print(f"  - {key}: {value}")

    print(f"[STATUS] Registry sub_id 数量：{len(registry_sub_ids)}")
    if registry_sub_ids:
        for sub_id in registry_sub_ids[:20]:
            print(f"  - {sub_id}")
        if len(registry_sub_ids) > 20:
            print(f"  ... 其余 {len(registry_sub_ids) - 20} 条未展开")


def main(argv: Optional[list[str]] = None) -> int:
    """
    脚本入口。

    Args:
        argv (Optional[list[str]]): 测试时可显式传参。

    Returns:
        int: 退出码，`0` 成功，`2` 表示超时或失败。
    """
    parser = argparse.ArgumentParser(description="查询 QMTD 当前实时订阅状态")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--ctrl-channel", default="xt:ctrl:sub")
    parser.add_argument("--ack-prefix", default="xt:ctrl:ack")
    parser.add_argument("--strategy-id", default="status_probe")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    payload = query_status(
        redis_url=args.redis_url,
        ctrl_channel=args.ctrl_channel,
        ack_prefix=args.ack_prefix,
        strategy_id=args.strategy_id,
        timeout=args.timeout,
    )
    if payload is None:
        print("[STATUS] 在超时时间内未收到 ACK。")
        return 2
    _print_status(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
