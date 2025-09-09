# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:11
# @Author : EquipmentADV
# @File : history_api.py
# @Software : PyCharm
"""
HistoryAPI：历史数据在线拉取（QMT 实参接线）

类说明：
    - 功能：面向外部提供“codes×period×时间窗×复权”的在线拉取能力；以**新鲜数据**为主；不依赖 miniQMT 本地库；
    - 上游：QMTConnector 提供的有效 xtdata 会话；
    - 下游：调用方（策略/研究/权威库拉取端）。

注意：按照你的要求，本阶段**不主动写入权威库**，而是按请求返回数据（或仅返回摘要）。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import logging

try:
    from xtquant import xtdata
except Exception as e:  # pragma: no cover
    xtdata = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

ISO = "%Y-%m-%dT%H:%M:%S%z"  # ISO8601（不含冒号的时区偏移），示例：2025-09-05T15:00:00+0800
CN_TZ = timezone(timedelta(hours=8))


@dataclass
class HistoryConfig:
    """类说明：历史拉取配置
    功能：控制默认复权、分段大小（按时间粒度计算）等参数。
    上游：config/history.yml
    下游：HistoryAPI 行为。
    """
    batch_size: int = 3000
    dividend_type: str = "none"  # none | front | back | ratio
    default_period: str = "1m"    # 仅 1m/1h/1d


class HistoryAPI:
    """类说明：历史数据在线拉取 API（实参接线）
    功能：以 xtdata.get_market_data 在线拉取历史 bars，按时间窗分段聚合；
          本阶段不写库，直接返回摘要/数据给调用方或权威库抓取端。
    上游：QMTConnector（已 connect 的 xtdata）。
    下游：调用方（策略/研究/权威库拉取端）。
    """

    def __init__(self, cfg: HistoryConfig, logger: Optional[logging.Logger] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdata：{_IMPORT_ERR}\n"
                "请确认已安装 MiniQmt/xtquant 并在同一 Python 环境下运行。"
            )
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)

    # ----------------------------- 公有主接口 -----------------------------
    def fetch_bars(
        self,
        codes: List[str],
        period: str,
        start_time: str,
        end_time: str,
        dividend_type: str = "none",
        return_data: bool = False,
    ) -> Dict[str, Any]:
        """方法说明：按条件在线拉取历史数据（分段），返回摘要/数据
        功能：调用 xtdata.get_market_data 对每个 code 拉取指定时间窗的 bars（新鲜数据），不依赖本地库。
        上游：外部服务/CLI。
        下游：调用方（数据使用侧或权威库抓取端）。
        参数：见函数签名，时间为 ISO8601（Asia/Shanghai）。
        返回：包含 count/gaps/head_ts/tail_ts（可选 data）。
        """
        assert period in {"1m", "1h", "1d"}, "仅支持 1m/1h/1d"
        segs = self._segment_time_range(period, start_time, end_time, self.cfg.batch_size)
        total_rows = 0
        head_ts, tail_ts = None, None
        collected: List[dict] = []

        for seg_start, seg_end in segs:
            for code in codes:
                rows = self._pull_one(code, period, seg_start, seg_end, dividend_type)
                if not rows:
                    continue
                total_rows += len(rows)
                head_ts = head_ts or rows[0]["bar_open_ts"]
                tail_ts = rows[-1]["bar_end_ts"]
                if return_data:
                    collected.extend(rows)

        gaps = self._detect_gaps(period, start_time, end_time, collected)
        result: Dict[str, Any] = {
            "status": "ok",
            "count": total_rows,
            "gaps": gaps,
            "head_ts": head_ts,
            "tail_ts": tail_ts,
        }
        if return_data:
            result["data"] = collected
        return result

    # ----------------------------- 内部：QMT 拉取 -----------------------------
    def _pull_one(self, code: str, period: str, seg_start: str, seg_end: str, dividend_type: str) -> List[dict]:
        """方法说明：调用 xtdata.get_market_data 拉取单标的一个时间片段
        功能：将 xtdata 返回的数据帧转换为宽表字典列表；时间字段统一为 ISO8601（+08:00）。
        上游：fetch_bars。
        下游：调用方（可能直接使用或写库端使用）。
        """
        # QMT 常用时间格式为 'YYYY-MM-DD HH:MM:SS'，转换自 ISO8601（+08:00）。
        def to_qmt_ts(ts_iso: str) -> str:
            dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(CN_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S")

        qmt_start = to_qmt_ts(seg_start)
        qmt_end = to_qmt_ts(seg_end)

        # 拉取数据（注意：某些版本需要 fill_data=True 以补全空档；此处按需增加）
        try:
            df = xtdata.get_market_data(
                stock_code=code,
                period=period,
                start_time=qmt_start,
                end_time=qmt_end,
                dividend_type=dividend_type,
                fill_data=True,
            )
        except Exception as e:  # pragma: no cover
            self.logger.warning("[HistoryAPI] get_market_data 失败：code=%s, %s", code, e)
            return []

        if df is None or getattr(df, "empty", True):
            return []

        # 字段名兼容：time/Time/datetime（取 bar_end），OHLCV/amount
        # 某些版本列名为 ['time','open','high','low','close','volume','amount']
        name_time = None
        for cand in ("time", "Time", "datetime", "bar_time"):
            if cand in df.columns:
                name_time = cand
                break
        if name_time is None:
            raise ValueError("QMT 返回数据不含时间列（time/datetime），无法解析")

        rows: List[dict] = []
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]

        for _, r in df.iterrows():
            # 将 bar_end_ts 规范为 ISO8601（+08:00，无冒号时区）
            if isinstance(r[name_time], (int, float)):
                # 极少数版本 time 为 epoch 秒
                dt_end = datetime.fromtimestamp(float(r[name_time]), CN_TZ)
            else:
                # 形如 'YYYY-MM-DD HH:MM:SS'
                dt_end = datetime.strptime(str(r[name_time]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CN_TZ)

            dt_open = dt_end - delta
            bar = {
                "code": code,
                "period": period,
                "bar_open_ts": dt_open.strftime(ISO),
                "bar_end_ts": dt_end.strftime(ISO),
                "is_closed": True,  # 历史拉取默认视为已收盘
                "open": float(r.get("open", r.get("Open", 0.0))),
                "high": float(r.get("high", r.get("High", 0.0))),
                "low": float(r.get("low", r.get("Low", 0.0))),
                "close": float(r.get("close", r.get("Close", 0.0))),
                "volume": float(r.get("volume", r.get("Volume", 0.0))),
                "amount": float(r.get("amount", r.get("Amount", 0.0))),
                "dividend_type": dividend_type,
                "source": "qmt",
                "recv_ts": None,
                "gen_ts": datetime.now(CN_TZ).strftime(ISO),
            }
            rows.append(bar)
        return rows

    # ----------------------------- 工具：分段与缺口 -----------------------------
    def _segment_time_range(self, period: str, start_time: str, end_time: str, batch_size: int):
        tz8 = CN_TZ
        start = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(tz8)
        end = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(tz8)
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        seg_span = delta * batch_size
        segs = []
        cur = start
        while cur < end:
            seg_end = min(cur + seg_span, end)
            segs.append((cur.strftime(ISO), seg_end.strftime(ISO)))
            cur = seg_end
        return segs

    def _detect_gaps(self, period: str, start_time: str, end_time: str, rows: List[dict]):
        if not rows:
            return []
        tz8 = CN_TZ
        start = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(tz8)
        end = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(tz8)
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        expected = []
        cur = start
        while cur < end:
            expected.append(cur.strftime(ISO))
            cur += delta
        got = {r["bar_end_ts"] for r in rows if r.get("is_closed")}
        gaps = [ts for ts in expected if ts not in got]
        return gaps[:2000]
