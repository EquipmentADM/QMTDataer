# -*- coding: utf-8 -*-
"""
QMTD 历史入库 storage backend 选择入口。

Responsibilities:
    - 默认将 QMTD 历史入库切换到新 FD backend
    - 保留 legacy backend 环境变量回滚能力
    - 避免 QMTD 上层散落多个 storage 实现选择判断

Data Contract:
    - 对上层导出 FinancialDataStorage 类名
    - `QMTD_STORAGE_BACKEND=legacy` 时回退旧 storage_simple

Internal Dependencies:
    - core.fd_storage_adapter
    - core.storage_simple

External Systems:
    - None
"""

from __future__ import annotations

import os


def get_storage_backend_name() -> str:
    """
    读取当前 storage backend 名称。

    Returns:
        str: `fd` 或 `legacy`。
    """

    return os.environ.get("QMTD_STORAGE_BACKEND", "fd").strip().lower() or "fd"


if get_storage_backend_name() == "legacy":
    from core.storage_simple import FinancialDataStorage
else:
    from core.fd_storage_adapter import FinancialDataStorage


__all__ = ["FinancialDataStorage", "get_storage_backend_name"]
