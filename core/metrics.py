# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 11:18
# @Author : EquipmentADV
# @File : metrics.py
# @Software : PyCharm
"""最小指标器（M3）

类说明：
    - Metrics：线程安全计数器集合，用于记录发布数、失败数、去重命中等；

功能：
    - 供实时推送路径与发布器调用，以便后续可视化或健康检查；

上下游：
    - 上游：RealtimeSubscriptionService / PubSubPublisher；
    - 下游：日志/未来的监控端点（后续可扩展为 Prometheus）。
"""
from __future__ import annotations
from dataclasses import dataclass
from threading import Lock
from typing import Dict


@dataclass
class Counters:
    published: int = 0
    publish_fail: int = 0
    dedup_hit: int = 0


class Metrics:
    """类说明：最小指标器（线程安全）"""
    def __init__(self) -> None:
        self._lock = Lock()
        self._c = Counters()
    def inc_published(self, n: int = 1) -> None:
        with self._lock:
            self._c.published += n
    def inc_publish_fail(self, n: int = 1) -> None:
        with self._lock:
            self._c.publish_fail += n
    def inc_dedup_hit(self, n: int = 1) -> None:
        with self._lock:
            self._c.dedup_hit += n
    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return {
                "published": self._c.published,
                "publish_fail": self._c.publish_fail,
                "dedup_hit": self._c.dedup_hit,
            }