# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:09
# @Author : EquipmentADV
# @File : qmt_connector.py
# @Software : PyCharm
"""QMTConnector：QMT/xtdata 真实接线

类说明：
    - 功能：初始化 xtdatacenter 与 xtdata，建立在线行情会话，维持心跳与重连；
    - 上游：配置文件（qmt.yml）→ QMTConfig；
    - 下游：HistoryAPI（历史在线拉取）、RealtimeSubscriptionService（实时订阅）。

注意：此文件仅负责“通道接通”，不涉及数据落库或订阅逻辑。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import time
import logging

try:
    # QMT 官方 Python 包名通常为 xtquant，内部子模块为 xtdatacenter / xtdata
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
    """配置结构体（类说明）
    功能：承载 QMT/xtdatacenter/xtdata 的连接参数。
    上游：外部配置文件（YAML/ENV）。
    下游：QMTConnector 用于建立在线连接。
    """
    token: str
    endpoint_strategy: str = "auto"  # auto | fixed
    listen_port_range: Tuple[int, int] = (50100, 50150)
    userdata_dir: Optional[str] = None
    connect_timeout_ms: int = 3000
    heartbeat_interval_ms: int = 1000
    reconnect_backoff: Tuple[int, int, int, int] = (500, 1000, 3000, 5000)


class QMTConnector:
    """类说明：QMT 连接器（实参接线版）
    功能：负责初始化并维持与 QMT/xtdata 的在线连接，统一提供行情会话（xtdata）。
    上游：QMTConfig（来自配置文件）。
    下游：HistoryAPI（历史在线拉取）、RealtimeSubscriptionService（实时订阅）。
    """

    def __init__(self, cfg: QMTConfig, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)
        self._connected = False

        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdatacenter/xtdata：{_IMPORT_ERR}\n"
                "请确认已安装 MiniQmt/xtquant 并在同一 Python 环境下运行。"
            )

    def listen_and_connect(self) -> None:
        """方法说明：启动监听并连接 QMT 在线行情
        功能：按端口范围启动 xtdatacenter 监听，并与 xtdata 建立会话。
        上游：进程入口（backfill_history / run_realtime_bridge）。
        下游：生成可供 HistoryAPI/Realtime 使用的会话（通过 xtdata 全局）。
        """
        start_port, end_port = self.cfg.listen_port_range
        backoffs = [0, *self.cfg.reconnect_backoff, *self.cfg.reconnect_backoff]

        self.logger.info("[QMTConnector] 准备初始化 xtdatacenter：端口区间=%s", self.cfg.listen_port_range)

        for idx, wait_ms in enumerate(backoffs):
            try:
                # 1) 设置 Token（如需）
                if hasattr(xtdc, "set_token") and self.cfg.token:
                    xtdc.set_token(self.cfg.token)

                # 2) 启动/监听（可指定范围，避免端口占用）
                if hasattr(xtdc, "init"):
                    xtdc.init()
                if hasattr(xtdc, "listen"):
                    xtdc.listen(port=start_port)
                else:
                    # 少数版本提供 listen_range：
                    if hasattr(xtdc, "listen_range"):
                        xtdc.listen_range(start_port, end_port)

                # 3) 连接 xtdata（部分版本仅需 import 即可，无需显式 connect）
                #    这里尝试一次 get_server_config 以确认可用。
                if hasattr(xtdata, "get_server_config"):
                    _ = xtdata.get_server_config()

                self._connected = True
                self.logger.info("[QMTConnector] 连接成功（xtdata 可用）")
                return
            except Exception as e:  # pragma: no cover
                self.logger.warning("[QMTConnector] 连接失败，重试#%d，err=%s", idx, e)
                time.sleep(max(wait_ms, 100) / 1000)

        raise RuntimeError("QMTConnector: 多次重连失败，请检查 token/端口/网络/权限")

    @property
    def ok(self) -> bool:
        """方法说明：连接健康状态
        功能：供上游健康检查与启动顺序控制。
        上游：外部启动器。
        下游：N/A。
        """
        return self._connected