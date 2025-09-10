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
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta, timezone
import logging

try:
    from xtquant import xtdata
except Exception:  # pragma: no cover
    xtdata = None  # type: ignore

from .metrics import Metrics

CN_TZ = timezone(timedelta(hours=8))


@dataclass
class RealtimeConfig:
    """类说明：实时订阅配置
    功能：定义订阅模式、周期集、标的集、收盘延迟与预热天数；
    上下游：由配置加载或控制面转换后传入。
    """
    mode: str  # close_only | forming_and_close
    periods: List[str]
    codes: List[str]
    close_delay_ms: int = 100
    preload_days: int = 3


class RealtimeSubscriptionService:
    """类说明：实时订阅服务
    功能：负责订阅/回调→拉取→整形→发布；支持动态增删订阅；
    上游：run_with_config / control_plane；
    下游：Redis（通过 PubSubPublisher）。
    """
    def __init__(self, cfg: RealtimeConfig, publisher, cache=None, logger: Optional[logging.Logger] = None):
        if xtdata is None:
            raise RuntimeError("xtquant.xtdata 未可用")
        self.cfg = cfg
        self.publisher = publisher
        self.cache = cache
        self.logger = logger or logging.getLogger(__name__)
        self._last_published: Dict[Tuple[str, str], str] = {}
        self._subs: Dict[Tuple[str, str], bool] = {}  # 已注册订阅集合

    # ---------- 辅助 ----------
    def _preload_for(self, codes: List[str], period: str, days: int) -> None:
        if not self.cache or days <= 0:
            return
        end = datetime.now(CN_TZ)
        start = end - timedelta(days=days)
        self.cache.ensure_downloaded_date_range(codes, period, start, end, incrementally=True)

    def _register_one(self, code: str, period: str) -> None:
        if (code, period) in self._subs:
            return
        xtdata.subscribe_quote(code, period, 2, lambda *_: self._on_event(code, period))
        self._subs[(code, period)] = True

    def _unregister_one(self, code: str, period: str) -> None:
        try:
            if hasattr(xtdata, "unsubscribe_quote"):
                xtdata.unsubscribe_quote(code, period)
        except Exception:
            pass
        self._subs.pop((code, period), None)

    # ---------- 动态接口 ----------
    def add_subscription(self, codes: List[str], periods: List[str], preload_days: Optional[int] = None) -> None:
        """方法说明：动态新增订阅
        功能：按 period 预热→注册订阅；
        上游：控制面；
        下游：xtdata.subscribe_quote。
        """
        days = self.cfg.preload_days if preload_days is None else max(0, int(preload_days))
        uniq_codes = sorted({c.strip() for c in codes if c.strip()})
        uniq_periods = sorted({p.strip() for p in periods if p.strip()})
        for p in uniq_periods:
            self._preload_for(uniq_codes, p, days)
            for c in uniq_codes:
                self._register_one(c, p)

    def remove_subscription(self, codes: List[str], periods: List[str]) -> None:
        """方法说明：动态撤销订阅
        功能：取消回调并移除订阅集合；
        上游：控制面；
        下游：xtdata.unsubscribe_quote（若可用）。
        """
        uniq_codes = sorted({c.strip() for c in codes if c.strip()})
        uniq_periods = sorted({p.strip() for p in periods if p.strip()})
        for p in uniq_periods:
            for c in uniq_codes:
                self._unregister_one(c, p)

    def status(self) -> Dict[str, Any]:  # type: ignore[override]
        """方法说明：返回运行状态
        功能：展示当前订阅集合与最后一次发布水位；
        上游：控制面查询；
        下游：UI/回执。
        """
        subs = sorted([{"code": c, "period": p} for (c, p) in self._subs.keys()], key=lambda x: (x["code"], x["period"]))
        return {"subs": subs, "last_published": dict(self._last_published)}

    # ---------- 主流程 ----------
    def run_forever(self) -> None:
        # 启动前预热
        self.add_subscription(self.cfg.codes, self.cfg.periods, preload_days=self.cfg.preload_days)
        # 进入事件循环
        xtdata.run()

    # ---------- 回调处理 ----------
    def _on_event(self, code: str, period: str) -> None:
        """方法说明：行情事件回调
        功能：拉取近 2 根，判收盘→发布，进程内去重。
        上游：xtdata.subscribe_quote 回调；
        下游：Redis 发布。
        """
        try:
            df = xtdata.get_market_data(code, period, count=2, fill_data=True)
        except Exception as e:
            return
        if df is None or len(df) == 0:
            return
        # 取最后一根作为 close，倒数第二根作为 forming（可选）
        try:
            last_row = df.iloc[-1]
            ts = last_row["time"]  # 形如 "YYYY-mm-dd HH:MM:SS"
        except Exception:
            return
        key = (code, period)
        if self.cfg.mode == "forming_and_close":
            # 先发布 forming（上一根）
            if len(df) >= 2:
                prev = df.iloc[-2]
                self._publish_bar(code, period, prev, is_closed=False)
        # 发布 close（幂等：相同 bar_end_ts 不重复）
        last_ts = str(ts)
        if self._last_published.get(key) == last_ts:
            return
        self._publish_bar(code, period, last_row, is_closed=True)
        self._last_published[key] = last_ts

    def _publish_bar(self, code: str, period: str, row, is_closed: bool) -> None:
        payload = {
            "code": code,
            "period": period,
            "bar_open_ts": None,  # QMT 未直接给开盘时间，保持 None；可在下游推导
            "bar_end_ts": str(row.get("time")),
            "is_closed": is_closed,
            "open": float(row.get("open", 0.0)),
            "high": float(row.get("high", 0.0)),
            "low": float(row.get("low", 0.0)),
            "close": float(row.get("close", 0.0)),
            "volume": float(row.get("volume", 0.0)),
            "amount": float(row.get("amount", 0.0)),
            "source": "qmt",
            "recv_ts": datetime.now(CN_TZ).isoformat(),
        }
        try:
            self.publisher.publish(payload)
        except Exception:
            pass

