# -*- coding: utf-8 -*-
"""
实时订阅服务（M3 / M3.5）

文件说明（功能 / 特性 / 上下游）：
- 功能：
    * 统一封装 QMT xtdata 的实时订阅能力；
    * 接收 QMT 回调数据（callback(datas)），将其规范化为“宽表”后发布到 Redis（通过外部 Publisher）；
    * 支持启动或动态新增订阅时的历史预热（补齐），以保证首条实时推送的连续性；
    * 支持去重与两种推送模式：'close_only'（默认，仅收盘推送）、'forming_and_close'（生成中也推）；
    * 提供状态查询（当前订阅集 / 最近发布时间等）；
- 特性：
    * 回调签名完全遵循 QMT 官方：callback(datas)，datas 形如 { '600000.SH': [ {...}, ... ] }；
    * 订阅调用使用关键字参数（避免回调被编码进 BSON 的问题）；
    * 去重键采用 (code, period, bar_end_ts)，内部 LRU 控制内存增长；
    * 并发安全：回调在 xtdata 内部线程触发，本类用锁保护关键结构（订阅集 / 去重集）；
- 上下游：
    * 上游：运行入口（scripts/run_with_config.py）及 ControlPlane（动态订阅 / 退订）；
    * 下游：PubSubPublisher（将标准化“宽表”消息发布到 Redis）。

注意：
- QMT/MiniQMT 的历史数据获取通常需要先 download_history_data，再通过 get_* 读取；本服务中的“预热”步骤会在新增订阅时按配置进行补齐；
- 实时回调的数据字段（尤其时间与收盘标记）在不同品种/站点可能存在差异，此处做了通用容错（例如 isClose/isClosed/closed 多字段容错）。
"""

from __future__ import annotations
import logging
import math
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Tuple, Optional
from collections import deque
from datetime import datetime, timedelta, timezone, timezone

CN_TZ = timezone(timedelta(hours=8))

# QMT xtdata
try:  # pragma: no cover
    from xtquant import xtdata  # type: ignore
except Exception as _e:  # pragma: no cover
    xtdata = None  # type: ignore
    _XT_IMPORT_ERR = _e
else:  # pragma: no cover
    _XT_IMPORT_ERR = None


# =========================
# 配置数据类
# =========================

@dataclass
class RealtimeConfig:
    """类说明：实时订阅配置
    功能：描述订阅行为（模式 / 周期 / 标的 / 预热等）。
    上下游：由外部加载（config_loader）并传入 RealtimeSubscriptionService。
    """
    mode: str = "close_only"                     # 'close_only' | 'forming_and_close'
    periods: List[str] = field(default_factory=lambda: ["1m"])
    codes: List[str] = field(default_factory=list)
    close_delay_ms: int = 100                    # 若做延迟收盘判定（当前版本未启用队列延迟，仅保留配置）
    preload_days: int = 3                        # 新增订阅时历史预热天数（0 表示不预热）

    # 可选：去重容量限制（LRU）
    dedup_max_size: int = 50000

    @dataclass
    class MockConfig:
        """类说明：Mock 行情配置
        功能：控制是否启用随机游走行情，以及相关行为参数。
        """
        enabled: bool = False
        base_price: float = 10.0
        volatility: float = 0.002       # 对数收益标准差
        step_seconds: float = 1.0       # 每根 bar 生成的时间间隔（秒）
        seed: Optional[int] = None
        volume_mean: float = 1_000_000
        volume_std: float = 200_000
        source: str = "mock"

    mock: MockConfig = field(default_factory=MockConfig)


# =========================
# 内部状态结构
# =========================

@dataclass
class _BarState:
    """类说明：单个标的/周期的 bar 状态缓存
    功能：保存当前 forming bar，记录最近一次已发布的 bar 结束时间；
    上下游：仅供 RealtimeSubscriptionService 内部使用。
    """
    current_payload: Optional[Dict[str, Any]] = None
    current_dt: Optional[datetime] = None
    last_published_dt: Optional[datetime] = None

# =========================
# Mock 行情生成器
# =========================

class MockBarFeeder(threading.Thread):
    """类说明：Mock 行情生成线程
    功能：根据订阅集合构建随机游走的 bar 序列，并以 datas 形式投喂 RealtimeSubscriptionService。
    """

    @dataclass
    class _State:
        last_dt: Optional[datetime] = None
        last_price: Optional[float] = None

    _PERIOD_SECONDS = {
        "1m": 60,
        "1h": 3600,
        "1d": 86400,
    }

    def __init__(self, svc: "RealtimeSubscriptionService", cfg: RealtimeConfig.MockConfig,
                 logger: Optional[logging.Logger] = None) -> None:
        super().__init__(name="MockBarFeeder", daemon=True)
        self._svc = svc
        self._cfg = cfg
        self._log = logger or logging.getLogger("MockBarFeeder")
        self._stop_evt = threading.Event()
        self._states: Dict[Tuple[str, str], MockBarFeeder._State] = {}
        self._rng = random.Random(cfg.seed)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        self._log.info("mock feeder started (step=%.2fs base=%.2f vol=%.4f)", float(self._cfg.step_seconds or 1.0),
                       self._cfg.base_price, self._cfg.volatility)
        step = max(0.1, float(self._cfg.step_seconds or 1.0))
        while not self._stop_evt.is_set():
            try:
                self._emit_cycle()
            except Exception:  # pragma: no cover
                self._log.exception("mock feeder emit failed")
            finally:
                self._stop_evt.wait(step)
        self._log.info("mock feeder stopped")

    # ------------------------------------------------------------------
    # 内部逻辑
    # ------------------------------------------------------------------
    def _emit_cycle(self) -> None:
        subs = set(self._svc._list_subscriptions())
        if not subs:
            return
        for key in list(self._states.keys()):
            if key not in subs:
                self._states.pop(key, None)

        now = datetime.now(CN_TZ)
        for code, period in subs:
            delta = self._period_delta(period)
            if not delta:
                self._log.debug("mock feeder skip unsupported period=%s", period)
                continue
            state = self._states.setdefault((code, period), MockBarFeeder._State())
            if state.last_dt is None or state.last_price is None:
                base_dt = self._align_base(now, delta) - delta
                base_price = self._initial_price(code)
                seed_row = self._build_row(code, period, base_dt, base_price, close_price=base_price)
                self._svc._on_datas(period, {code: [seed_row]})
                state.last_dt = base_dt
                state.last_price = base_price

            next_dt = state.last_dt + delta
            next_price = self._next_price(state.last_price)
            row = self._build_row(code, period, next_dt, state.last_price, close_price=next_price)
            state.last_dt = next_dt
            state.last_price = next_price
            self._svc._on_datas(period, {code: [row]})

    @staticmethod
    def _period_delta(period: str) -> Optional[timedelta]:
        secs = MockBarFeeder._PERIOD_SECONDS.get(period)
        if not secs:
            return None
        return timedelta(seconds=secs)

    @staticmethod
    def _align_base(now: datetime, delta: timedelta) -> datetime:
        secs = int(delta.total_seconds())
        epoch = int(now.timestamp())
        aligned_epoch = (epoch // secs) * secs
        return datetime.fromtimestamp(aligned_epoch, CN_TZ)

    def _initial_price(self, code: str) -> float:
        jitter = (abs(hash(code)) % 500) / 100.0
        base = max(0.01, self._cfg.base_price + jitter)
        return round(base, 4)

    def _next_price(self, prev: float) -> float:
        drift = self._rng.gauss(0, self._cfg.volatility)
        price = prev * math.exp(drift)
        return round(max(0.01, price), 4)

    def _build_row(self, code: str, period: str, bar_dt: datetime, open_price: float, close_price: float) -> Dict[str, Any]:
        spread = abs(close_price - open_price) or 0.01
        swing = abs(self._rng.gauss(0, spread * 0.3))
        high = max(open_price, close_price) + swing
        low = min(open_price, close_price) - swing
        high = round(max(high, open_price, close_price), 4)
        low = round(max(0.01, min(low, open_price, close_price)), 4)
        volume = max(1, int(abs(self._rng.gauss(self._cfg.volume_mean, self._cfg.volume_std))))
        amount = round(close_price * volume, 2)
        return {
            "time": bar_dt.isoformat(),
            "code": code,
            "period": period,
            "open": round(open_price, 4),
            "high": high,
            "low": low,
            "close": round(close_price, 4),
            "volume": volume,
            "amount": amount,
            "isClosed": True,
            "source": self._cfg.source,
        }
# =========================
# 主服务类
# =========================

class RealtimeSubscriptionService:
    """类说明：实时订阅服务
    功能：
        - 在 xtdata 中注册订阅（subscribe_quote），使用官方签名的回调 callback(datas) 接收行情；
        - 将回调数据规范化为“宽表”并通过 Publisher 推送到 Redis；
        - 在新增订阅时可先进行历史预热，确保时序连续；
        - 提供订阅状态查询；
    上游：
        - run_with_config.py 在启动时构造本服务并调用 run_forever()；
        - ControlPlane 在运行时调用 add_subscription/remove_subscription 动态变更订阅；
    下游：
        - PubSubPublisher（负责 Redis 发布），本类不关心 Redis 细节；
    """

    def __init__(self, cfg: RealtimeConfig, publisher, cache=None, logger: Optional[logging.Logger] = None):
        """
        :param cfg: RealtimeConfig，订阅行为配置
        :param publisher: 发布器对象，需提供 publish(dict) 方法（下游为 Redis PubSub）
        :param cache: 可选的历史预热实现（需提供 ensure_downloaded_date_range(codes, periods, days)）
        :param logger: 可选日志器
        """

        self.cfg = cfg
        self.publisher = publisher
        self.cache = cache
        self._log = logger or logging.getLogger(__name__)
        self._mock_feeder: Optional[MockBarFeeder] = None

        # 并发保护
        self._lock = threading.RLock()

        # 当前订阅集（(code, period)）
        self._subs: set[Tuple[str, str]] = set()

        # 去重：LRU + 集合
        self._dedup_set: set[Tuple[Any, ...]] = set()
        self._dedup_q: deque[Tuple[Any, ...]] = deque()
        self._dedup_max = int(self.cfg.dedup_max_size or 50000)

        # 最近发布时间（观测用途）
        self._last_pub_ts: Dict[Tuple[str, str], float] = {}
        # bar 状态机缓存（key = (code, period)）
        self._bar_states: Dict[Tuple[str, str], _BarState] = {}

        if not self.cfg.mock.enabled and xtdata is None:
            raise RuntimeError(f"缺少依赖或 QMT/MiniQMT 未正确安装：{_XT_IMPORT_ERR}")

    # ----------------------------------------------------------------------
    # 入口：阻塞运行
    # ----------------------------------------------------------------------
    def run_forever(self) -> None:
        """方法说明：阻塞运行生命周期"""
        if self.cfg.codes and self.cfg.periods:
            self.add_subscription(self.cfg.codes, self.cfg.periods, preload_days=self.cfg.preload_days)

        if self.cfg.mock.enabled:
            self._log.info("[RT] mock 行情模式启用（step=%.2fs, base=%.2f, vol=%.4f）",
                           float(self.cfg.mock.step_seconds or 1.0),
                           self.cfg.mock.base_price,
                           self.cfg.mock.volatility)
            self._mock_feeder = MockBarFeeder(self, self.cfg.mock, logger=self._log.getChild("MockFeeder"))
            self._mock_feeder.start()
            try:
                while True:
                    time.sleep(1.0)
            except KeyboardInterrupt:
                self._log.info("[RT] 接收到中断信号，准备停止 Mock 行情。")
            finally:
                self.stop()
                if self._mock_feeder:
                    self._mock_feeder.join(timeout=2.0)
            return

        if xtdata is None:
            raise RuntimeError(f"缺少依赖或 QMT/MiniQMT 未正确安装：{_XT_IMPORT_ERR}")

        self._log.info("[RT] xtdata.run() 开始阻塞运行")
        try:
            xtdata.run()
        finally:
            self._log.info("[RT] xtdata.run() 结束")

    # ----------------------------------------------------------------------
    # 动态增删订阅
    # ----------------------------------------------------------------------
    def add_subscription(self, codes: List[str], periods: List[str], preload_days: Optional[int] = None) -> None:
        """方法说明：新增订阅
        功能：
            - 可选地先进行历史预热（补齐若干天）；
            - 为每个 (code, period) 调用 _register_one 完成订阅注册；
        :param codes: 标的列表
        :param periods: 周期列表（'1m' | '1h' | '1d' 等）
        :param preload_days: 覆盖 cfg.preload_days；为 0 或 None 则不预热
        """
        days = int(preload_days if preload_days is not None else self.cfg.preload_days or 0)
        if not self.cfg.mock.enabled and days > 0:
            self._preload_history(codes, periods, days)

        with self._lock:
            for c in codes:
                for p in periods:
                    if (c, p) in self._subs:
                        continue
                    if not self.cfg.mock.enabled:
                        self._register_one(c, p)
                    self._subs.add((c, p))
                    if self.cfg.mock.enabled:
                        self._log.info("[RT] Mock 订阅已登记: %s %s", c, p)
                    else:
                        self._log.info("[RT] 订阅已注册: %s %s", c, p)

    def remove_subscription(self, codes: List[str], periods: List[str]) -> None:
        """方法说明：移除订阅
        功能：调用 xtdata.unsubscribe_quote，并更新内部订阅集
        """
        with self._lock:
            for c in codes:
                for p in periods:
                    if (c, p) not in self._subs:
                        continue
                    if not self.cfg.mock.enabled:
                        try:
                            xtdata.unsubscribe_quote(stock_code=c, period=p)
                        except TypeError:
                            try:
                                xtdata.unsubscribe_quote(c)
                            except Exception as e:
                                self._log.warning("[RT] unsubscribe failed: %s %s err=%s", c, p, e)
                                continue
                        except Exception as e:
                            self._log.warning("[RT] unsubscribe failed: %s %s err=%s", c, p, e)
                            continue
                    self._subs.discard((c, p))
                    self._bar_states.pop((c, p), None)
                    if self.cfg.mock.enabled:
                        self._log.info("[RT] Mock 订阅已移除: %s %s", c, p)
                    else:
                        self._log.info("[RT] 订阅已移除: %s %s", c, p)

    def _list_subscriptions(self) -> List[Tuple[str, str]]:
        with self._lock:
            return list(self._subs)

    def stop(self) -> None:
        """方法说明：停止实时服务（目前用于 Mock 模式）"""
        if self.cfg.mock.enabled and self._mock_feeder:
            self._mock_feeder.stop()

    # ----------------------------------------------------------------------
    # 订阅注册与回调处理
    # ----------------------------------------------------------------------
    @staticmethod
    def _normalize_bar_end_ts(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        try:
            if isinstance(raw, (int, float)):
                ts = float(raw)
                if ts > 1e12:
                    dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
                else:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                return dt.astimezone(CN_TZ).isoformat()
            s = str(raw).strip()
            if not s:
                return None
            if s.isdigit():
                if len(s) == 14:
                    dt = datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
                    return dt.isoformat()
                if len(s) == 8:
                    dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=CN_TZ)
                    return dt.isoformat()
            if "T" not in s:
                s = s.replace(" ", "T")
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                if "+" not in s:
                    s = f"{s}+08:00"
                dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=CN_TZ)
            return dt.astimezone(CN_TZ).isoformat()
        except Exception:
            return None

    def _register_one(self, code: str, period: str) -> None:
        """方法说明：注册单标的/单周期订阅（官方签名回调）
        功能：
            - 向 xtdata.subscribe_quote 发起订阅（使用关键字参数）；
            - 绑定符合签名的回调函数 _cb(datas)；
        回调参数 datas：
            - 字典：{ stock_code: [data1, data2, ...] }
        """
        def _cb(datas: Dict[str, List[Dict[str, Any]]]):
            try:
                self._on_datas(period, datas)
            except Exception:
                self._log.exception("[RT] 回调处理异常 period=%s", period)

        # 官方建议：订阅实时部分时 count=0；历史由预热负责
        xtdata.subscribe_quote(
            stock_code=code,
            period=period,
            start_time="",
            end_time="",
            count=0,
            callback=_cb
        )

    def _on_datas(self, period: str, datas: Dict[str, List[Dict[str, Any]]]) -> None:
        """方法说明：处理 QMT 回调数据（datas）
        功能：
            - 先做字段归一化；
            - 按时间戳排序后交给 bar 状态机；
        """
        if not datas:
            return

        for code, rows in datas.items():
            if not rows:
                continue
            normalized_rows: List[Tuple[datetime, Dict[str, Any]]] = []
            for row in rows:
                payload = self._build_payload_from_row(code, period, row)
                if not payload:
                    continue
                bar_iso = self._normalize_bar_end_ts(payload.get("bar_end_ts"))
                if not bar_iso:
                    continue
                payload["bar_end_ts"] = bar_iso
                try:
                    bar_dt = datetime.fromisoformat(bar_iso)
                except Exception:
                    self._log.debug("[RT] bar_end_ts 无法解析: code=%s period=%s ts=%s", code, period, bar_iso)
                    continue
                normalized_rows.append((bar_dt, payload))

            if not normalized_rows:
                continue

            normalized_rows.sort(key=lambda item: item[0])
            for bar_dt, payload in normalized_rows:
                self._handle_bar_update(code, period, bar_dt, payload)

    # ----------------------------------------------------------------------
    # bar 状态机：基于时间戳判定收盘
    # ----------------------------------------------------------------------
    def _handle_bar_update(self, code: str, period: str,
                           bar_dt: datetime, payload: Dict[str, Any]) -> None:
        """方法说明：维护单标的/周期的 bar 状态并在需要时发布"""
        key = (code, period)
        to_publish: List[Dict[str, Any]] = []
        store_payload = dict(payload)
        store_payload["code"] = code
        store_payload["period"] = period
        # forming 阶段统一视为未收盘，等待时间戳前进触发最终发布
        store_payload["is_closed"] = False

        with self._lock:
            state = self._bar_states.setdefault(key, _BarState())

            if state.current_dt is None:
                state.current_dt = bar_dt
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)
            elif bar_dt < state.current_dt:
                if state.last_published_dt and bar_dt <= state.last_published_dt:
                    return
                self._log.debug("[RT] 检测到乱序 bar，已跳过 code=%s period=%s ts=%s current=%s",
                                code, period, bar_dt.isoformat(), state.current_dt.isoformat())
                return
            elif bar_dt == state.current_dt:
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)
            else:
                if state.current_payload:
                    finalized = dict(state.current_payload)
                    finalized["is_closed"] = True
                    to_publish.append(finalized)
                    state.last_published_dt = state.current_dt
                state.current_dt = bar_dt
                state.current_payload = store_payload
                if self.cfg.mode == "forming_and_close":
                    forming_payload = dict(store_payload)
                    forming_payload["is_closed"] = False
                    to_publish.append(forming_payload)

        for item in to_publish:
            self._publish_payload(item)

    def _publish_payload(self, payload: Dict[str, Any]) -> None:
        """方法说明：统一处理去重与时间戳刷新后推送到 Redis"""
        code = payload.get("code")
        period = payload.get("period")
        bar_ts = payload.get("bar_end_ts")
        if not code or not period or not bar_ts:
            return
        is_closed = bool(payload.get("is_closed"))
        if self.cfg.mode == "close_only" and not is_closed:
            return
        if self.cfg.mode == "forming_and_close":
            dkey = (code, period, bar_ts, 1 if is_closed else 0)
        else:
            dkey = (code, period, bar_ts)
        if self._is_dup_and_mark(dkey):
            return
        enriched = dict(payload)
        enriched.setdefault("source", "qmt")
        enriched["recv_ts"] = datetime.now(CN_TZ).isoformat()
        self.publisher.publish(enriched)
        with self._lock:
            self._last_pub_ts[(code, period)] = time.time()

    # ----------------------------------------------------------------------
    # 预热（历史补齐）
    # ----------------------------------------------------------------------
    def _preload_history(self, codes: List[str], periods: List[str], days: int) -> None:
        """方法说明：历史预热
        功能：
            - 若提供了 cache.ensure_downloaded_date_range，则优先使用；
            - 否则回退至逐标的/逐周期调用 xtdata.download_history_data；
        注意：
            - xtdata.download_history_data(stock_code: str, period: str, start_time: str, end_time: str)
              仅接受单只股票代码，且需要 YYYYMMDD（或更精细）格式；
        """
        if days <= 0:
            return

        if self.cache and hasattr(self.cache, "ensure_downloaded_date_range"):
            try:
                self.cache.ensure_downloaded_date_range(codes, periods, days=days)
                self._log.info("[RT] 历史预热(使用cache)完成 days=%d codes=%d periods=%d", days, len(codes), len(periods))
                return
            except Exception as e:
                self._log.warning("[RT] 历史预热(cache)异常，将使用直接下载：%s", e)

        end = datetime.today().date()
        start = end - timedelta(days=days)
        start_s = start.strftime("%Y%m%d")
        end_s = end.strftime("%Y%m%d")

        for c in codes:
            for p in periods:
                try:
                    xtdata.download_history_data(
                        stock_code=c,
                        period=p,
                        start_time=start_s,
                        end_time=end_s,
                        incrementally=True
                    )
                except Exception as e:
                    self._log.warning("[RT] 历史下载异常: %s %s err=%s", c, p, e)
        self._log.info("[RT] 历史预热完成 days=%d", days)

    # ----------------------------------------------------------------------
    # 构建“宽表”payload
    # ----------------------------------------------------------------------
    def _build_payload_from_row(self, code: str, period: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw_ts = row.get("time") or row.get("Time") or row.get("barTime") or row.get("bar_time")
        bar_end_ts = self._normalize_bar_end_ts(raw_ts)
        if bar_end_ts is None:
            return None

        is_closed = row.get("isClosed")
        if is_closed is None:
            is_closed = row.get("isClose")
        if is_closed is None:
            is_closed = row.get("closed")

        src = row.get("source") or "qmt"
        payload = {
            "code": code,
            "period": period,
            "bar_end_ts": bar_end_ts,
            "is_closed": bool(is_closed) if is_closed is not None else None,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "preClose": row.get("preClose"),
            "suspendFlag": row.get("suspendFlag"),
            "openInterest": row.get("openInterest"),
            "settlementPrice": row.get("settlementPrice") or row.get("settelementPrice"),
            "source": src,
            "recv_ts": datetime.now(CN_TZ).isoformat(),
        }
        return payload


    # ----------------------------------------------------------------------
    # 去重（LRU）
    # ----------------------------------------------------------------------
    def _is_dup_and_mark(self, key: Tuple[Any, ...]) -> bool:
        """方法说明：判断是否重复并写入 LRU 结构
        功能：
            - 若 key 已在集合中：返回 True；
            - 否则：写入集合与队列，若超容量则弹出最旧项；
        """
        with self._lock:
            if key in self._dedup_set:
                return True
            self._dedup_set.add(key)
            self._dedup_q.append(key)
            if len(self._dedup_q) > self._dedup_max:
                old = self._dedup_q.popleft()
                self._dedup_set.discard(old)
        return False

    # ----------------------------------------------------------------------
    # 状态查询
    # ----------------------------------------------------------------------
    def status(self) -> Dict[str, Any]:
        """方法说明：返回服务状态
        内容：
            - subs：当前订阅数组 [{'code':..., 'period':...}, ...]
            - last_published：最近一次发布时间（epoch 秒）按 (code, period) 组织
        """
        with self._lock:
            subs = sorted([{"code": c, "period": p} for (c, p) in self._subs],
                          key=lambda x: (x["code"], x["period"]))
            last_pub = {f"{c}|{p}": ts for (c, p), ts in self._last_pub_ts.items()}
        return {"subs": subs, "last_published": last_pub}
