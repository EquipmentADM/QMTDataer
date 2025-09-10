# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 14:26
# @Author : EquipmentADV
# @File : health.py
# @Software : PyCharm
# -*- coding: utf-8 -*-
"""健康上报（M3）
类说明：
    - HealthReporter：后台线程，定期将 Metrics 快照与实例信息写入 Redis（带 TTL）。
功能：
    - 供运维与可视化读取当前实例的存活与吞吐指标；
上下游：
    - 上游：run_with_config（实例化并启动）；
    - 下游：Redis（写入字符串或哈希，当前实现为字符串 JSON）。
"""
from __future__ import annotations
import json
import os
import socket
import threading
import time
from typing import Dict, Optional

try:
    import redis
except Exception:
    redis = None  # type: ignore

from .metrics import Metrics


class HealthReporter(threading.Thread):
    """类说明：健康上报线程"""
    daemon = True

    def __init__(self, host: str, port: int, password: Optional[str], key_prefix: str,
                 metrics: Metrics, interval_sec: int = 5, ttl_sec: int = 20,
                 extra_info: Optional[Dict[str, object]] = None) -> None:
        super().__init__(name="HealthReporter")
        if redis is None:
            raise RuntimeError("未安装 redis 依赖，无法启用健康上报")
        self._cli = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.key_prefix = key_prefix
        self.metrics = metrics
        self.interval = max(1, int(interval_sec))
        self.ttl = max(self.interval * 2, int(ttl_sec))
        self.extra = extra_info or {}
        # 修复：不要覆盖 Thread._stop
        self._stop_evt = threading.Event()
        self._instance_id = self._make_instance_id()

    def _make_instance_id(self) -> str:
        host = socket.gethostname()
        pid = os.getpid()
        tag = self.extra.get("instance_tag") if self.extra else None
        return f"{host}:{pid}:{tag}" if tag else f"{host}:{pid}"

    def stop(self) -> None:
        """方法说明：停止健康上报"""
        self._stop_evt.set()

    def run(self) -> None:
        while not self._stop_evt.is_set():
            payload = {
                "ts": int(time.time()),
                "instance_id": self._instance_id,
                "metrics": self.metrics.snapshot(),
                "extra": self.extra,
            }
            key = f"{self.key_prefix}:{self._instance_id}"
            try:
                self._cli.set(key, json.dumps(payload, ensure_ascii=False), ex=self.ttl)
            except Exception:
                # 健康上报失败不应中断主流程，仅忽略
                pass
            # 用事件等待可即时响应 stop()
            self._stop_evt.wait(self.interval)