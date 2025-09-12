"""HistoryAPI：历史数据（先补后取 → 宽表）

类说明：
    - 功能：对外暴露“codes×period×时间窗×复权”的获取接口；内部流程：
        1) 调用 LocalCache 先补齐（按日期 'YYYYMMDD'）；
        2) 调用 xtdata.get_market_data(field_list=[], stock_list=[...]) 批量获取；
        3) 过滤 'time==0' 的列并转换为宽表记录；
    - 上游：调用方（策略/权威库拉取端/脚本）；
    - 下游：xtquant.xtdata（MiniQMT 本地库）。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
import logging

from .local_cache import LocalCache, CacheConfig

try:
    from xtquant import xtdata
except Exception as e:  # pragma: no cover
    xtdata = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

ISO = "%Y-%m-%dT%H:%M:%S%z"  # 输出时间格式
CN_TZ = timezone(timedelta(hours=8))


@dataclass
class HistoryConfig:
    """类说明：历史拉取配置
    功能：控制默认复权、是否使用批量获取、fill_data 策略等。
    上游：config/history.yml
    下游：HistoryAPI 行为。
    """
    use_batch_get_in: bool = True
    fill_data_on_get: bool = False
    dividend_type: str = "none"  # none | front | back | ratio


class HistoryAPI:
    """类说明：历史数据 API（先补后取）
    功能：封装 download→get_in→宽表转换；返回摘要与可选数据集。
    上游：脚本/服务。
    下游：xtdata + LocalCache。
    """

    def __init__(self, cfg: HistoryConfig, logger: Optional[logging.Logger] = None,
                 cache: Optional[LocalCache] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdata：{_IMPORT_ERR}\n"
                "请确认已安装 MiniQMT/xtquant 并在同一 Python 环境下运行。"
            )
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)
        self.cache = cache or LocalCache(CacheConfig())

    # ----------------------------- 公有主接口 -----------------------------
    def fetch_bars(
        self,
        codes: List[str],
        period: str,
        start_time: str,  # ISO8601
        end_time: str,    # ISO8601
        dividend_type: str = "none",
        return_data: bool = False,
    ) -> Dict[str, Any]:
        """方法说明：历史数据获取（先补后取）
        功能：给定 codes×period×ISO 时间窗，先按日期补齐，再批量获取，转换为宽表。
        上游：外部服务/CLI。
        下游：xtdata（获取），调用方（消费）。
        返回：摘要（count/gaps/head_ts/tail_ts）与可选 data。
        """
        assert period in {"1m", "1h", "1d"}, "仅支持 1m/1h/1d"
        # 1) ISO→日期（下载按日）
        s_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(CN_TZ)
        e_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(CN_TZ)
        start_yyyymmdd = s_dt.strftime("%Y%m%d")
        end_yyyymmdd = e_dt.strftime("%Y%m%d")

        if period in ("1m", "1h"):
            q_start = s_dt.strftime("%Y%m%d%H%M%S")
            q_end = e_dt.strftime("%Y%m%d%H%M%S")
        else:  # "1d"
            q_start = start_yyyymmdd
            q_end = end_yyyymmdd

        # 2) 先补齐
        self.cache.ensure_downloaded_date_range(codes, period, start_yyyymmdd, end_yyyymmdd, incrementally=True)

        # 3) 批量获取（field→DataFrame 字典）
        print(f"调用xtdata.get_market_data-stock_list:{codes} period:{period} start_time:{start_yyyymmdd} end_time:{end_yyyymmdd} dividend_type:{dividend_type}")
        data_dict = xtdata.get_market_data(
            field_list=[],
            stock_list=codes,
            period=period,
            start_time=q_start,
            end_time=q_end,
            count=-1,
            dividend_type=dividend_type,
            fill_data=self.cfg.fill_data_on_get,
        )
        # 4) 转换为宽表记录
        rows = self._in_dict_to_rows(data_dict, period, dividend_type)

        # 5) 汇总摘要
        total_rows = len(rows)
        head_ts = rows[0]["bar_open_ts"] if rows else None
        tail_ts = rows[-1]["bar_end_ts"] if rows else None
        gaps = self._detect_gaps_simple(period, s_dt, e_dt, rows)

        result: Dict[str, Any] = {
            "status": "ok",
            "count": total_rows,
            "gaps": gaps,
            "head_ts": head_ts,
            "tail_ts": tail_ts,
        }
        if return_data:
            result["data"] = rows
        return result

    # ----------------------------- 内部：转换与缺口 -----------------------------
    def _in_dict_to_rows(self, data_dict: Dict[str, Any], period: str, dividend_type: str) -> List[dict]:
        """方法说明：将 get_market_data(field→DataFrame) 的字典转为宽表行
        功能：以 'time' 字段为基准，过滤 time==0 的列；拼装宽表字段。
        上游：fetch_bars。
        下游：调用方（消费或落库）。
        """
        if not isinstance(data_dict, dict) or "time" not in data_dict:
            return []
        time_df = data_dict["time"]
        # 允许字段缺失；读取前先判存在
        def _get(field: str, code: str, col: str):
            df = data_dict.get(field)
            if df is None:
                return None
            try:
                val = df.loc[code, col]
                return None if val is None else float(val)
            except Exception:
                return None

        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        rows: List[dict] = []
        # 遍历代码与列（列名通常为 YYYYMMDD；分钟/小时可能更细，此处以 time 为准）
        for code in list(time_df.index):
            for col in list(time_df.columns):
                try:
                    t_ms = int(time_df.loc[code, col])
                except Exception:
                    continue
                if t_ms in (0, None):
                    continue  # 无实际值
                dt_end = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc).astimezone(CN_TZ)
                dt_open = dt_end - delta
                rows.append({
                    "code": code,
                    "period": period,
                    "bar_open_ts": dt_open.strftime(ISO),
                    "bar_end_ts": dt_end.strftime(ISO),
                    "is_closed": True,
                    "open": _get("open", code, col),
                    "high": _get("high", code, col),
                    "low": _get("low", code, col),
                    "close": _get("close", code, col),
                    "volume": _get("volume", code, col),
                    "amount": _get("amount", code, col),
                    "pre_close": _get("preClose", code, col),
                    "suspend_flag": _get("suspendFlag", code, col),
                    "open_interest": _get("openInterest", code, col),
                    "settlement_price": _get("settelementPrice", code, col),  # 原名即如此
                    "dividend_type": dividend_type,
                    "source": "qmt",
                    "recv_ts": None,
                    "gen_ts": datetime.now(CN_TZ).strftime(ISO),
                })
        # 全量按时间排序
        rows.sort(key=lambda r: (r["code"], r["bar_end_ts"]))
        return rows

    def _detect_gaps_simple(self, period: str, s_dt: datetime, e_dt: datetime, rows: List[dict]) -> List[str]:
        """方法说明：简化版缺口检测
        功能：按固定步长生成“期望刻度”，与实际 `is_closed=True` 的 `bar_end_ts` 比较得出缺口。
        上游：fetch_bars。
        下游：调用方（展示/补数）。
        """
        if not rows:
            return []
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        expected = []
        cur = s_dt
        while cur <= e_dt:
            expected.append(cur.strftime(ISO))
            cur += delta
        got = {r["bar_end_ts"] for r in rows if r.get("is_closed")}
        return [ts for ts in expected if ts not in got][:2000]