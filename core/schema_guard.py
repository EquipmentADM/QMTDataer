# -*- coding = utf-8 -*-
# @Time : 2025/9/17 14:38
# @Author : EquipmentADV
# @File : schema_guard.py
# @Software : PyCharm
"""
Schema Guard（v0.5 最小契约校验）

类/方法说明：
    - validate_bar_payload(payload: dict, *, mode: str) -> (bool, str)
      功能：校验实时推送的宽表消息是否满足 v0.5 最小契约；
      上游：RealtimeSubscriptionService 在发布前调用；
      下游：仅日志与指标计数，不抛异常阻断主流程。

语义要点：
    - 必备字段：code/period/bar_end_ts/is_closed/open/high/low/close
    - close_only 下 is_closed 必须为 True
    - bar_end_ts 必须是 +08:00 的时间字符串（ISO 8601，末尾含 "+08:00"）
"""
from __future__ import annotations
from typing import Tuple, Any

_REQUIRED_FIELDS = ("code", "period", "bar_end_ts", "is_closed",
                    "open", "high", "low", "close")

def _is_plus8(ts: Any) -> bool:
    if not isinstance(ts, str):
        return False
    # 允许 "YYYY-MM-DDTHH:MM:SS+08:00" 或 "YYYY-MM-DD HH:MM:SS+08:00"
    return ts.endswith("+08:00") and ("T" in ts or " " in ts)

def validate_bar_payload(payload: dict, *, mode: str) -> Tuple[bool, str]:
    """方法说明：校验 v0.5 最小契约
    功能：检查必备字段、close_only 约束与 +08:00 时区要求
    返回：(ok, reason)；ok=False 时 reason 给出丢弃原因
    """
    if not isinstance(payload, dict):
        return False, "payload 不是 dict"

    for k in _REQUIRED_FIELDS:
        if k not in payload:
            return False, f"缺少必备字段：{k}"

    if mode == "close_only":
        if payload.get("is_closed") is not True:
            return False, "close_only 下必须 is_closed=True"

    if not _is_plus8(payload.get("bar_end_ts")):
        return False, "bar_end_ts 必须为 +08:00 的时间字符串"

    return True, ""