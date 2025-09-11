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
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Tuple, Optional
from collections import deque
from datetime import datetime, timedelta

# QMT xtdata
try:
    from xtquant import xtdata
except Exception as _e:  # pragma: no cover
    raise RuntimeError(f"缺少依赖或 QMT/MiniQMT 未正确安装：{_e}")


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

        # 并发保护
        self._lock = threading.RLock()

        # 当前订阅集（(code, period)）
        self._subs: set[Tuple[str, str]] = set()

        # 去重：LRU + 集合
        self._dedup_set: set[Tuple[str, str, Any]] = set()
        self._dedup_q: deque[Tuple[str, str, Any]] = deque()
        self._dedup_max = int(self.cfg.dedup_max_size or 50000)

        # 最近发布时间（观测用途）
        self._last_pub_ts: Dict[Tuple[str, str], float] = {}

    # ----------------------------------------------------------------------
    # 入口：阻塞运行
    # ----------------------------------------------------------------------
    def run_forever(self) -> None:
        """方法说明：阻塞运行生命周期
        功能：
            - 对 cfg.codes/cfg.periods 做一次初始订阅（含可选历史预热）；
            - 阻塞调用 xtdata.run() 以驱动回调循环；
        注意：
            - xtdata.run() 内部是一个消息循环，按官方建议需保持运行；
        """
        # 初始订阅（可能触发历史预热）
        if self.cfg.codes and self.cfg.periods:
            self.add_subscription(self.cfg.codes, self.cfg.periods, preload_days=self.cfg.preload_days)

        # 阻塞运行 QMT 回调循环
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
        if days > 0:
            self._preload_history(codes, periods, days)

        with self._lock:
            for c in codes:
                for p in periods:
                    if (c, p) in self._subs:
                        continue
                    self._register_one(c, p)
                    self._subs.add((c, p))
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
                    try:
                        xtdata.unsubscribe_quote(c, p)
                    except Exception as e:
                        self._log.warning("[RT] 取消订阅异常: %s %s err=%s", c, p, e)
                    self._subs.discard((c, p))
                    self._log.info("[RT] 订阅已移除: %s %s", c, p)

    # ----------------------------------------------------------------------
    # 订阅注册与回调处理
    # ----------------------------------------------------------------------
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
            - 遍历 datas 中每个代码的若干条 data；
            - 规范化为“宽表” payload；
            - 根据模式/去重策略决定是否发布；
        """
        if not datas:
            return

        for code, rows in datas.items():
            if not rows:
                continue
            for row in rows:
                payload = self._build_payload_from_row(code, period, row)

                # 去重键：code + period + bar_end_ts（bar_end_ts 缺失则无法去重，直接跳过或直接推送）
                dkey = (payload.get("code"), payload.get("period"), payload.get("bar_end_ts"))

                # 模式过滤
                if self.cfg.mode == "close_only":
                    # 仅在明确收盘时推送（默认为 False，避免误推 forming）
                    if payload.get("is_closed") is not True:
                        continue

                # 去重判定
                if dkey[2] is not None and self._is_dup_and_mark(dkey):
                    continue

                self.publisher.publish(payload)
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
    def _build_payload_from_row(self, code: str, period: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """方法说明：将单条 QMT 回调数据标准化为“宽表”payload
        字段约定（可能缺失则为 None）：
            - code, period
            - bar_end_ts：K线结束时间（原样透传 QMT 的 time 字段，可能为字符串或毫秒时间戳）
            - is_closed：是否收盘（容错 isClosed/isClose/closed，多字段择优；默认 False）
            - open, high, low, close, volume, amount
            - preClose, suspendFlag, openInterest, settlementPrice
        """
        # 时间字段容错：'time' / 'Time' / 'barTime'
        bar_ts = row.get("time", None)
        if bar_ts is None:
            bar_ts = row.get("Time", None)
        if bar_ts is None:
            bar_ts = row.get("barTime", None)

        # 收盘标记容错
        is_closed = row.get("isClosed", None)
        if is_closed is None:
            is_closed = row.get("isClose", None)
        if is_closed is None:
            is_closed = row.get("closed", None)
        if is_closed is None:
            is_closed = False  # 默认认为未收盘，避免在 close_only 中误推

        payload = {
            "code": code,
            "period": period,
            "bar_end_ts": bar_ts,  # 保持原样，消费方可自行转 ISO 或毫秒
            "is_closed": bool(is_closed),

            # 常见宽表字段
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),

            # 其他字段
            "preClose": row.get("preClose"),
            "suspendFlag": row.get("suspendFlag"),
            "openInterest": row.get("openInterest"),
            "settlementPrice": row.get("settlementPrice") or row.get("settelementPrice"),
        }
        return payload

    # ----------------------------------------------------------------------
    # 去重（LRU）
    # ----------------------------------------------------------------------
    def _is_dup_and_mark(self, key: Tuple[str, str, Any]) -> bool:
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
