# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:12
# @Author : EquipmentADV
# @File : realtime_service.py
# @Software : PyCharm
"""RealtimeSubscriptionService：实时订阅/发布（订阅前补齐）

类说明：
    - 功能：
        1) 启动时对 codes×periods 做一次**历史预热补齐**（preload_days 配置）；
        2) 订阅回调后拉取最近 2 根，按 close-delay 规则发布至 Redis PubSub；
    - 上游：运行脚本 run_realtime_bridge.py；
    - 下游：MiniQMT（缓存）与 Redis PubSub。
"""
from __future__ import annotations
from typing import Dict, Tuple, List, Optional
from datetime import datetime, timedelta, timezone, date
import logging

from .local_cache import LocalCache, CacheConfig

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
    功能：承载订阅模式（close-only / forming_and_close）、周期、代码清单、收盘延迟与预热天数。
    上游：subscription.yml
    下游：RealtimeSubscriptionService。
    """
    def __init__(self, mode: str, periods: List[str], codes: List[str],
                 close_delay_ms: int = 100, preload_days: int = 3):
        assert mode in ("close_only", "forming_and_close")
        for p in periods:
            assert p in ("1m", "1h", "1d")
        self.mode = mode
        self.periods = periods
        self.codes = codes
        self.close_delay_ms = close_delay_ms
        self.preload_days = preload_days


class RealtimeSubscriptionService:
    """类说明：实时订阅服务（含订阅前补齐）
    功能：预热补齐→订阅→回调取数→发布；不使用 57s 判收盘。
    上游：QMTConnector（可为空操作）、RealtimeConfig。
    下游：PubSubPublisher。
    """

    def __init__(self, cfg: RealtimeConfig, publisher, logger: Optional[logging.Logger] = None,
                 cache: Optional[LocalCache] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdata：{_IMPORT_ERR}\n"
                "请确认已安装 MiniQmt/xtquant 并在同一 Python 环境下运行。"
            )
        self.cfg = cfg
        self.publisher = publisher
        self.logger = logger or logging.getLogger(__name__)
        self.cache = cache or LocalCache(CacheConfig())
        self._last_published: Dict[Tuple[str, str], str] = {}

    # ----------------------------- 运行入口 -----------------------------
    def run_forever(self) -> None:
        """方法说明：启动预热补齐并进入订阅循环
        功能：对每个 (code, period) 先做 `preload_days` 的日期补齐，然后注册订阅并进入 `xtdata.run()`。
        上游：脚本入口。
        下游：回调内发布至 PubSub。
        """
        self._preload_cache()
        for code in self.cfg.codes:
            for period in self.cfg.periods:
                self._subscribe_one(code, period)
        self.logger.info("[Realtime] 进入 xtdata.run() 阻塞循环")
        xtdata.run()

    def _preload_cache(self) -> None:
        """方法说明：订阅前补齐最近 N 天（preload_days）
        功能：避免首轮推送因缺历史而不完整。
        上游：run_forever。
        下游：LocalCache.ensure_downloaded_date_range。
        """
        if self.cfg.preload_days <= 0:
            return
        end = date.today()
        start = end - timedelta(days=max(1, self.cfg.preload_days))
        s = start.strftime("%Y%m%d")
        e = end.strftime("%Y%m%d")
        for period in self.cfg.periods:
            self.logger.info("[Realtime] preload %s %s~%s for %d codes", period, s, e, len(self.cfg.codes))
            self.cache.ensure_downloaded_date_range(self.cfg.codes, period, s, e, incrementally=True)

    # ----------------------------- 订阅与回调 -----------------------------
    def _subscribe_one(self, code: str, period: str) -> None:
        def _cb(*_args, **_kwargs):
            try:
                self._on_event(code, period)
            except Exception as e:  # pragma: no cover
                self.logger.exception("[Realtime] 回调处理异常：%s", e)
        xtdata.subscribe_quote(code, period, -1, _cb)
        self.logger.info("[Realtime] 订阅：%s / %s", code, period)

    def _on_event(self, code: str, period: str) -> None:
        # 拉取最近 2 根
        try:
            df = xtdata.get_market_data(code, period, count=2, fill_data=True)
        except Exception as e:  # pragma: no cover
            self.logger.warning("[Realtime] 拉取最新 bar 失败：%s / %s, %s", code, period, e)
            return
        if df is None or getattr(df, "empty", True):
            return
        bars = self._df_to_bars(code, period, df)
        now = datetime.now(CN_TZ)
        # forming 模式：先发最后一根 forming
        if self.cfg.mode == "forming_and_close" and bars:
            last = dict(bars[-1])
            last["is_closed"] = False
            self.publisher.publish(last)
        # close-only：找满足 close-delay 的最后一根
        close_delay = timedelta(milliseconds=self.cfg.close_delay_ms)
        for bar in reversed(bars):
            bar_end = datetime.strptime(bar["bar_end_ts"], ISO)
            if now >= bar_end + close_delay:
                key = (code, period)
                if self._last_published.get(key) == bar["bar_end_ts"]:
                    return
                bar["is_closed"] = True
                self.publisher.publish(bar)
                self._last_published[key] = bar["bar_end_ts"]
                return

    def _df_to_bars(self, code: str, period: str, df) -> List[dict]:
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
                "is_closed": False,
                "open": float(r.get("open", r.get("Open", 0.0))),
                "high": float(r.get("high", r.get("High", 0.0))),
                "low": float(r.get("low", r.get("Low", 0.0))),
                "close": float(r.get("close", r.get("Close", 0.0))),
                "volume": float(r.get("volume", r.get("Volume", 0.0))),
                "amount": float(r.get("amount", r.get("Amount", 0.0))),
                "dividend_type": "none",
                "source": "qmt",
                "recv_ts": datetime.now(CN_TZ).strftime(ISO),
                "gen_ts": datetime.now(CN_TZ).strftime(ISO),
            })
        return rows
