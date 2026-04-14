# -*- coding: utf-8 -*-
"""
QMTDataer 统一控制台接入入口。

当前阶段只提供最小模块接入骨架：
    - 提供稳定的 get_module_spec()；
    - 提供模块状态检测；
    - 提供页面布局与回调注册入口；
    - 供统一控制台静态配置加载。
"""
from __future__ import annotations

from dev_dashboard.dashboard_page import build_layout, register_callbacks
from dev_dashboard.dashboard_service import check_available, get_status_detail


def get_module_spec() -> dict[str, object]:
    """返回统一控制台所需的模块描述对象。"""
    return {
        "spec_version": "1.0",
        "module_id": "qmtdataer",
        "module_name": "QMTDataer",
        "module_description": "QMT 实时桥接、历史入库与期货/股票取数测试模块。",
        "component_prefix": "qmtdataer",
        "check_available": check_available,
        "build_layout": build_layout,
        "register_callbacks": register_callbacks,
        "get_status_detail": get_status_detail,
    }
