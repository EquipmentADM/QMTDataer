# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import threading
from typing import Optional, Dict, Any, List

try:
    import redis
except Exception as e:  # pragma: no cover
    redis = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

from .registry import Registry, SubscriptionSpec


class ControlPlane(threading.Thread):
    """类说明：控制面消费者线程
    功能：监听 Redis PubSub 通道，处理 subscribe/unsubscribe/status 命令；
    上游：策略管理器（通过 Redis 发布命令）；
    下游：RealtimeSubscriptionService（执行订阅与退订）、Registry（持久化）。
    """
    daemon = True

    def __init__(self, host: str, port: int, password: Optional[str], db: int,
                 channel: str, ack_prefix: str, registry_prefix: str,
                 svc, accept_strategies: Optional[List[str]] = None, logger=None) -> None:
        super().__init__(name="ControlPlane")
        if _IMPORT_ERR is not None:
            raise RuntimeError(f"未能导入 redis：{_IMPORT_ERR}")
        self._r = redis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)
        self._pubsub = self._r.pubsub()
        self._channel = channel
        self._ack_prefix = ack_prefix.rstrip(":")
        self._registry = Registry(host, port, password, db, prefix=registry_prefix)
        self._svc = svc
        self._accept = set(accept_strategies or [])
        # 修复：不要覆盖 Thread._stop
        self._stop_evt = threading.Event()
        self._logger = logger

    def stop(self) -> None:
        self._stop_evt.set()
        try:
            self._pubsub.close()
        except Exception:
            pass

    def _ack(self, strategy_id: str, payload: Dict[str, Any]) -> None:
        ch = f"{self._ack_prefix}:{strategy_id}"
        try:
            self._r.publish(ch, json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass

    def _allowed(self, strategy_id: str) -> bool:
        return (not self._accept) or (strategy_id in self._accept)

    # …（其余方法不变）…

    def run(self) -> None:
        self._pubsub.subscribe(self._channel)
        while not self._stop_evt.is_set():
            msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                continue
            try:
                data = json.loads(msg.get("data", "{}"))
            except Exception:
                continue
            action = str(data.get("action", "")).lower()
            if action == "subscribe":
                self._handle_subscribe(data)
            elif action == "unsubscribe":
                self._handle_unsubscribe(data)
            elif action == "status":
                self._handle_status(data)
            else:
                # 未知命令，忽略
                pass
