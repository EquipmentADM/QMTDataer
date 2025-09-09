# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:09
# @Author : EquipmentADV
# @File : qmt_connector.py
# @Software : PyCharm
"""QMTConnector：连接器（可跳过 listen）

类说明：
    - 功能：根据 `mode` 决定是否调用 xtdatacenter.listen；默认 `none` 仅做依赖可用性校验并视为已连接；
    - 上游：脚本入口；
    - 下游：HistoryAPI/RealtimeSubscriptionService。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import logging

try:
    from xtquant import xtdatacenter as xtdc
    from xtquant import xtdata
except Exception as e:  # pragma: no cover
    xtdc = None  # type: ignore
    xtdata = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


@dataclass
class QMTConfig:
    token: str = ""
    mode: str = "none"  # none | legacy（legacy：尝试 listen）
    listen_port_range: Tuple[int, int] = (50100, 50150)


class QMTConnector:
    def __init__(self, cfg: QMTConfig, logger: Optional[logging.Logger] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(f"未能导入 xtquant：{_IMPORT_ERR}")
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)
        self._connected = False

    def listen_and_connect(self) -> None:
        if self.cfg.mode == "none":
            # 跳过 listen：环境限制下依然可用（download/get 均走本地 MiniQMT 通道）
            self._connected = True
            self.logger.info("[QMTConnector] 跳过 listen（mode=none），仅校验模块可用")
            return
        # legacy 模式：尝试 listen（老版本/特定环境）
        try:
            if hasattr(xtdc, "set_token") and self.cfg.token:
                xtdc.set_token(self.cfg.token)
            if hasattr(xtdc, "init"):
                xtdc.init()
            if hasattr(xtdc, "listen"):
                xtdc.listen(self.cfg.listen_port_range[0])
            self._connected = True
            self.logger.info("[QMTConnector] legacy listen 成功")
        except Exception as e:  # pragma: no cover
            raise RuntimeError(f"QMTConnector legacy 模式失败：{e}")

    @property
    def ok(self) -> bool:
        return self._connected