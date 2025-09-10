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
        self._stop = threading.Event()
        self._logger = logger

    def stop(self) -> None:
        self._stop.set()
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

    def _handle_subscribe(self, cmd: Dict[str, Any]) -> None:
        strategy_id = str(cmd.get("strategy_id", "")).strip()
        if not strategy_id or not self._allowed(strategy_id):
            self._ack(strategy_id or "unknown", {"ok": False, "error": "strategy not allowed"})
            return
        codes = [str(x).strip() for x in (cmd.get("codes") or []) if str(x).strip()]
        periods = [str(x).strip() for x in (cmd.get("periods") or []) if str(x).strip()]
        mode = str(cmd.get("mode", self._svc.cfg.mode))
        preload_days = int(cmd.get("preload_days", self._svc.cfg.preload_days))
        topic = str(cmd.get("topic", self._svc.publisher.topic))
        if not codes or not periods:
            self._ack(strategy_id, {"ok": False, "error": "codes/periods required"})
            return
        # 生成 sub_id，持久化
        sub_id = self._registry.gen_sub_id()
        spec = SubscriptionSpec(strategy_id=strategy_id, codes=codes, periods=periods,
                                mode=mode, preload_days=preload_days, topic=topic,
                                created_at=int(__import__('time').time()))
        self._registry.save(sub_id, spec)
        # 执行：预热 + 注册订阅（使用服务封装）
        try:
            self._svc.add_subscription(codes=codes, periods=periods, preload_days=preload_days)
            self._ack(strategy_id, {"ok": True, "action": "subscribe", "sub_id": sub_id,
                                    "codes": codes, "periods": periods, "mode": mode, "topic": topic})
        except Exception as e:
            # 回滚注册表
            self._registry.delete(sub_id)
            self._ack(strategy_id, {"ok": False, "error": f"subscribe failed: {e}"})

    def _handle_unsubscribe(self, cmd: Dict[str, Any]) -> None:
        strategy_id = str(cmd.get("strategy_id", "")).strip()
        codes = [str(x).strip() for x in (cmd.get("codes") or []) if str(x).strip()]
        periods = [str(x).strip() for x in (cmd.get("periods") or []) if str(x).strip()]
        sub_id = cmd.get("sub_id")
        # 优先按 sub_id；否则按 codes×periods
        if sub_id:
            meta = self._registry.load(sub_id)
            if not meta:
                self._ack(strategy_id or "unknown", {"ok": False, "error": "sub_id not found"})
                return
            codes = meta.get("codes", []) if not codes else codes
            periods = meta.get("periods", []) if not periods else periods
            self._registry.delete(sub_id)
        if not codes or not periods:
            self._ack(strategy_id or "unknown", {"ok": False, "error": "codes/periods required"})
            return
        try:
            self._svc.remove_subscription(codes=codes, periods=periods)
            self._ack(strategy_id or "unknown", {"ok": True, "action": "unsubscribe",
                                                  "codes": codes, "periods": periods})
        except Exception as e:
            self._ack(strategy_id or "unknown", {"ok": False, "error": f"unsubscribe failed: {e}"})

    def _handle_status(self, cmd: Dict[str, Any]) -> None:
        strategy_id = str(cmd.get("strategy_id", "")).strip() or "unknown"
        st = self._svc.status()
        self._ack(strategy_id, {"ok": True, "action": "status", "status": st,
                                "subs": self._registry.list_all()})

    def run(self) -> None:
        self._pubsub.subscribe(self._channel)
        while not self._stop.is_set():
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