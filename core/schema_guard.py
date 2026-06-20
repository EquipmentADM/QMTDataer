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
    - bar_end_ts 必须是本地无时区 ISO 时间字符串
"""
from __future__ import annotations
from datetime import datetime
import re
from typing import Tuple, Any

_REQUIRED_FIELDS = ("code", "period", "bar_end_ts", "is_closed",
                    "open", "high", "low", "close")
_TZ_SUFFIX_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$")

def _is_local_naive_iso(ts: Any) -> bool:
    if not isinstance(ts, str):
        return False
    text = ts.strip()
    if not text or _TZ_SUFFIX_RE.search(text):
        return False
    try:
        parsed = datetime.fromisoformat(text.replace(" ", "T"))
    except ValueError:
        return False
    return parsed.tzinfo is None

def validate_bar_payload(payload: dict, *, mode: str) -> Tuple[bool, str]:
    """方法说明：校验 v0.5 最小契约
    功能：检查必备字段、close_only 约束与本地无时区时间要求
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

    if not _is_local_naive_iso(payload.get("bar_end_ts")):
        return False, "bar_end_ts 必须为本地无时区 ISO 时间字符串"

    return True, ""
