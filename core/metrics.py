# -*- coding: utf-8 -*-
"""指标收集器（Metrics）

功能：
    - 提供线程安全的内存计数器，兼容原有实例级指标（published/publish_fail/dedup_hit）；
    - 新增满足契约 v0.5 要求的全局计数：bars_published_total / schema_drop_total / late_bars_total；
    - 支持晚到判定、Schema Guard 等场景的快捷打点。
上下游：
    - 上游：RealtimeSubscriptionService、PubSubPublisher、ControlPlane 等在发布或校验阶段调用；
    - 下游：HealthReporter（健康上报）、日志/监控采集端。
历次增改：
    - 2025-09-17：新增全局计数接口（inc_global/snapshot_global/maybe_mark_late 等），保持实例计数兼容；
    - 2025-09-18：补充中文文档，明确功能/上下游/增改记录。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict
import threading

# 北京时间（Asia/Shanghai），用于统一晚到判定
CN_TZ = timezone(timedelta(hours=8))


class Metrics:
    """类说明：线程安全的指标集合

    功能：
        - 实例级计数：追踪单个发布器在当前进程内的成功/失败/去重命中次数；
        - 全局计数：跨实例累加契约 v0.5 所需的 bars/schema_drop/late 指标；
        - 提供晚到判定与 Schema Guard 计数的便捷入口。
    上游：
        - PubSubPublisher、RealtimeSubscriptionService、ControlPlane 等模块；
    下游：
        - HealthReporter、日志输出、后续可观测性平台。
    历次增改：
        - 2025-09-17：首次合并实例/全局计数；
        - 2025-09-18：完善中文注释与文档结构。
    """

    _global_lock = threading.Lock()
    _global_counters = {
        "bars_published_total": 0,
        "schema_drop_total": 0,
        "late_bars_total": 0,
    }

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters = {
            "published": 0,
            "publish_fail": 0,
            "dedup_hit": 0,
        }

    # ------------------------------------------------------------------
    # 实例级计数（向前兼容）
    # ------------------------------------------------------------------
    def inc_published(self, step: int = 1) -> None:
        """方法说明：记录成功发布次数
        功能：实例计数 + 同步累加全局 bars_published_total。
        参数：
            step：增加的步长，默认 1。
        上游：PubSubPublisher、RealtimeSubscriptionService。
        下游：HealthReporter（通过 snapshot 读取）。
        """
        with self._lock:
            self._counters["published"] += step
        self.inc_global("bars_published_total", step)

    def inc_publish_fail(self, step: int = 1) -> None:
        """方法说明：记录发布失败次数
        功能：实例范围内累加失败计数。
        参数：
            step：增加的步长，默认 1。
        上游：PubSubPublisher。
        下游：HealthReporter。
        """
        with self._lock:
            self._counters["publish_fail"] += step

    def inc_dedup_hit(self, step: int = 1) -> None:
        """方法说明：记录去重命中次数
        功能：实例范围内累加去重命中计数。
        参数：
            step：增加的步长，默认 1。
        上游：RealtimeSubscriptionService。
        下游：HealthReporter。
        """
        with self._lock:
            self._counters["dedup_hit"] += step

    def snapshot(self) -> Dict[str, int]:
        """方法说明：获取实例级指标快照
        功能：返回当前实例的 published/publish_fail/dedup_hit。
        返回：包含三个指标的字典副本。
        上游：HealthReporter/测试用例。
        """
        with self._lock:
            return dict(self._counters)

    # ------------------------------------------------------------------
    # 全局计数（跨实例共享）
    # ------------------------------------------------------------------
    @classmethod
    def inc_global(cls, key: str, step: int = 1) -> None:
        """方法说明：累加全局指标
        功能：在进程范围内对指定键进行线程安全的累加。
        参数：
            key：指标键名（bars_published_total/schema_drop_total/late_bars_total 等）；
            step：增加的步长，默认 1。
        上游：实例方法或其他模块直接调用。
        下游：监控/测试通过 snapshot_global 获取。
        """
        with cls._global_lock:
            cls._global_counters[key] = cls._global_counters.get(key, 0) + step

    @classmethod
    def snapshot_global(cls) -> Dict[str, int]:
        """方法说明：获取全局指标快照
        功能：返回所有全局指标的当前数值。
        返回：dict 副本，键包含 bars/schema_drop/late。
        上游：HealthReporter、调试脚本、单元测试。
        """
        with cls._global_lock:
            return dict(cls._global_counters)

    @classmethod
    def reset_global(cls) -> None:
        """方法说明：重置全局指标
        功能：将全局计数置零，主要用于测试隔离。
        上游：单元测试 setUp。
        下游：无。
        """
        with cls._global_lock:
            for k in cls._global_counters:
                cls._global_counters[k] = 0

    @classmethod
    def maybe_mark_late(cls, bar_end_ts: str | None, threshold_sec: int = 3) -> None:
        """方法说明：判断并记录晚到指标
        功能：若当前时间与 bar_end_ts 的差值超过阈值，则将 late_bars_total +1。
        参数：
            bar_end_ts：K 线结束时间（支持 `YYYY-MM-DDTHH:MM:SS+08:00` 或空格分隔格式）；
            threshold_sec：延迟阈值（秒）。
        上游：RealtimeSubscriptionService 发布收盘 bar 前。
        下游：全局指标。
        """
        if not bar_end_ts:
            return
        try:
            ts = bar_end_ts.replace(" ", "T")
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=CN_TZ)
            now = datetime.now(CN_TZ)
            if (now - dt).total_seconds() > threshold_sec:
                cls.inc_global("late_bars_total", 1)
        except Exception:
            # 时间解析失败直接忽略，避免全局指标被污染
            return

    @classmethod
    def mark_schema_drop(cls, step: int = 1) -> None:
        """方法说明：记录 Schema Guard 丢弃次数
        功能：辅助 Schema Guard 在丢弃 payload 时累加 global 指标。
        参数：
            step：增加的步长，默认 1。
        上游：实时回调中的 Schema Guard。
        下游：全局指标。
        """
        cls.inc_global("schema_drop_total", step)
