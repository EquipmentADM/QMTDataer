# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:12
# @Author : EquipmentADV
# @File : realtime_service.py
# @Software : PyCharm
"""
RealtimeSubscriptionService：实时订阅/聚合/去重/发布（QMT 实参接线）

类说明：
    - 功能：按订阅配置订阅 QMT 实时 K 线；将回调片段聚合为 bar，默认 close-only 发布到 Redis PubSub；
    - 上游：QMTConnector（已连接 xtdata）；
    - 下游：PubSubPublisher（Redis PubSub）。

强调：不做订单流；仅 1m/1h/1d。默认 close-only，可配置 forming+close。
"""
from __future__ import annotations
from typing import Dict, Tuple, List, Optional
from datetime import datetime, timedelta, timezone
import threading
import logging

try:
    from xtquant import xtdata
except Exception as e:  # pragma: no cover
    xtdata = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

CN_TZ = timezone(timedelta(hours=8))
ISO = "%Y-%m-%dT%H:%M:%S%z"


class RealtimeConfig:
    """类说明：实时订阅配置
    功能：承载订阅模式（close-only / forming+close）、周期、代码清单与发布参数。
    上游：subscription.yml
    下游：RealtimeSubscriptionService 行为。
    """
    def __init__(self, mode: str, periods: List[str], codes: List[str], close_delay_ms: int = 100):
        assert mode in ("close_only", "forming_and_close")
        for p in periods:
            assert p in ("1m", "1h", "1d")
        self.mode = mode
        self.periods = periods
        self.codes = codes
        self.close_delay_ms = close_delay_ms


class RealtimeSubscriptionService:
    """类说明：实时订阅服务（QMT 实参接线）
    功能：调用 xtdata.subscribe_quote 订阅 codes×periods；在回调中拉取最新 bar，
          根据模式（close-only / forming+close）发布至 PubSub。
    上游：QMTConnector（xtdata 通道已就绪）、RealtimeConfig。
    下游：PubSubPublisher。
    """

    def __init__(self, cfg: RealtimeConfig, publisher, logger: Optional[logging.Logger] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdata：{_IMPORT_ERR}\n"
                "请确认已安装 MiniQmt/xtquant 并在同一 Python 环境下运行。"
            )
        self.cfg = cfg
        self.publisher = publisher
        self.logger = logger or logging.getLogger(__name__)

        # 记录最近发布的收盘 bar，避免重复（code,period）→ last_bar_end_ts
        self._last_published: Dict[Tuple[str, str], str] = {}

    # ----------------------------- 运行入口 -----------------------------
    def run_forever(self) -> None:
        """方法说明：启动订阅并进入回调循环（阻塞）
        功能：对每个 (code, period) 调用 subscribe_quote；开启 xtdata.run() 主循环。
        上游：进程入口脚本。
        下游：回调内发布至 PubSub。
        """
        # 建立订阅
        for code in self.cfg.codes:
            for period in self.cfg.periods:
                self._subscribe_one(code, period)
        self.logger.info("[Realtime] 已订阅 %d×%d 组合，进入 xtdata.run() 阻塞循环",
                         len(self.cfg.codes), len(self.cfg.periods))
        # 进入回调循环（阻塞）
        xtdata.run()

    # ----------------------------- 订阅与回调 -----------------------------
    def _subscribe_one(self, code: str, period: str) -> None:
        """方法说明：订阅单个 (code, period)
        功能：注册回调；回调内拉取最新 bar 并决定是否发布。
        上游：run_forever。
        下游：回调→发布。
        """
        def _cb(*_args, **_kwargs):
            try:
                self._on_event(code, period)
            except Exception as e:  # pragma: no cover
                self.logger.exception("[Realtime] 回调处理异常：%s", e)

        # QMT 常用接口：subscribe_quote(stock_code, period, count=-1, callback=fn)
        # count=-1 表示当日全部；我们主要依赖回调触发，再拉最后一根/若干根。
        xtdata.subscribe_quote(code, period, -1, _cb)
        self.logger.info("[Realtime] 订阅：%s / %s", code, period)

    def _on_event(self, code: str, period: str) -> None:
        """方法说明：收到 QMT 回调后的处理逻辑
        功能：拉取最新一到两根 bar，判断是否已收盘并发布（close-only）；forming 模式则额外发布进行中的 bar。
        上游：_subscribe_one 注册的回调。
        下游：PubSubPublisher。
        """
        # 尝试拉取最近 2 根（避免处于切换边界时拿到的是 forming）
        try:
            df = xtdata.get_market_data(code, period, count=2, fill_data=True)
        except Exception as e:  # pragma: no cover
            self.logger.warning("[Realtime] 拉取最新 bar 失败：%s / %s, %s", code, period, e)
            return
        if df is None or getattr(df, "empty", True):
            return

        bars = self._df_to_bars(code, period, df)
        now = datetime.now(CN_TZ)

        # forming 模式：先尝试发布最末一根（可能未收盘）
        if self.cfg.mode == "forming_and_close":
            if bars:
                last = bars[-1]
                last["is_closed"] = False
                self.publisher.publish(last)

        # close-only 逻辑：寻找“已收盘”的最后一根。判定：当前时间超过 bar_end_ts + close_delay。
        close_delay = timedelta(milliseconds=self.cfg.close_delay_ms)
        for bar in reversed(bars):  # 从末尾向前找第一根满足关闭条件的 bar
            bar_end = datetime.strptime(bar["bar_end_ts"], ISO)
            if now >= bar_end + close_delay:
                key = (code, period)
                if self._last_published.get(key) == bar["bar_end_ts"]:
                    return  # 已发布过
                bar["is_closed"] = True
                self.publisher.publish(bar)
                self._last_published[key] = bar["bar_end_ts"]
                return

    # ----------------------------- DataFrame → 宽表 -----------------------------
    def _df_to_bars(self, code: str, period: str, df) -> List[dict]:
        """方法说明：将 xtdata DataFrame 转为宽表字典列表
        功能：解析列名（time/Time/datetime），生成 open/high/low/close/volume/amount 等字段。
        上游：_on_event。
        下游：PubSubPublisher。
        """
        name_time = None
        for cand in ("time", "Time", "datetime", "bar_time"):
            if cand in df.columns:
                name_time = cand
                break
        if name_time is None:
            raise ValueError("QMT 返回数据不含时间列（time/datetime），无法解析")

        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        rows: List[dict] = []
        for _, r in df.iterrows():
            if isinstance(r[name_time], (int, float)):
                dt_end = datetime.fromtimestamp(float(r[name_time]), CN_TZ)
            else:
                dt_end = datetime.strptime(str(r[name_time]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)
            dt_open = dt_end - delta
            rows.append({
                "code": code,
                "period": period,
                "bar_open_ts": dt_open.strftime(ISO),
                "bar_end_ts": dt_end.strftime(ISO),
                "is_closed": False,  # 默认先置 False，由调用逻辑决定是否 close-only
                "open": float(r.get("open", r.get("Open", 0.0))),
                "high": float(r.get("high", r.get("High", 0.0))),
                "low": float(r.get("low", r.get("Low", 0.0))),
                "close": float(r.get("close", r.get("Close", 0.0))),
                "volume": float(r.get("volume", r.get("Volume", 0.0))),
                "amount": float(r.get("amount", r.get("Amount", 0.0))),
                "dividend_type": "none",  # 实时侧通常为 none；如需复权，改为配置项
                "source": "qmt",
                "recv_ts": datetime.now(CN_TZ).strftime(ISO),
                "gen_ts": datetime.now(CN_TZ).strftime(ISO),
            })
        return rows
