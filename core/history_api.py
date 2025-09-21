# -*- coding: utf-8 -*-
"""HistoryAPI: download + fetch bars from xtdata and convert to contract format."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
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

ISO = "%Y-%m-%dT%H:%M:%S%z"
CN_TZ = timezone(timedelta(hours=8))
_TIME_FIELDS = ("time", "Time", "datetime", "bar_time", "barTime")
_VALUE_FIELDS = (
    "open", "high", "low", "close", "volume", "amount",
    "preClose", "suspendFlag", "openInterest", "settlementPrice", "settelementPrice",
)


@dataclass
class HistoryConfig:
    """Configuration for history fetch behaviour."""

    use_batch_get_in: bool = True
    fill_data_on_get: bool = False
    dividend_type: str = "none"  # none | front | back | ratio


class HistoryAPI:
    """Facade that executes download -> get_market_data_ex -> wide row conversion."""

    def __init__(self, cfg: HistoryConfig, logger: Optional[logging.Logger] = None,
                 cache: Optional[LocalCache] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"Failed to import xtquant/xtdata: {_IMPORT_ERR}\n"
                "Please ensure MiniQMT/xtquant is installed and available in this Python environment."
            )
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)
        self.cache = cache or LocalCache(CacheConfig())
    def fetch_bars(self,
                   codes: List[str],
                   period: str,
                   start_time: str,
                   end_time: str,
                   dividend_type: str = "none",
                   return_data: bool = False) -> Dict[str, Any]:
        """Fetch bars for the given codes/period/time window."""
        assert period in {"1m", "1h", "1d"}, "仅支持 1m/1h/1d"

        s_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(CN_TZ)
        e_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(CN_TZ)
        start_day = s_dt.strftime("%Y%m%d")
        end_day = e_dt.strftime("%Y%m%d")

        if period in ("1m", "1h"):
            q_start = s_dt.strftime("%Y%m%d%H%M%S")
            q_end = e_dt.strftime("%Y%m%d%H%M%S")
        else:
            q_start = start_day
            q_end = end_day

        self.cache.ensure_downloaded_date_range(codes, period, start_day, end_day, incrementally=True)

        data_dict: Dict[str, Any]
        if hasattr(xtdata, "get_market_data_ex"):
            fetch_kwargs = dict(
                field_list=[],
                stock_list=codes,
                period=period,
                start_time=q_start,
                end_time=q_end,
                count=-1,
                dividend_type=dividend_type,
                fill_data=self.cfg.fill_data_on_get,
            )
            try:
                data_dict = xtdata.get_market_data_ex(subscribe=False, **fetch_kwargs)  # type: ignore[arg-type]
            except TypeError:
                data_dict = xtdata.get_market_data_ex(**fetch_kwargs)  # type: ignore[arg-type]
        else:  # pragma: no cover
            data_dict = xtdata.get_market_data(  # type: ignore[attr-defined]
                field_list=[],
                stock_list=codes,
                period=period,
                start_time=q_start,
                end_time=q_end,
                count=-1,
                dividend_type=dividend_type,
                fill_data=self.cfg.fill_data_on_get,
            )

        rows = self._convert_to_rows(data_dict, period, dividend_type)

        result: Dict[str, Any] = {
            "status": "ok",
            "count": len(rows),
            "gaps": self._detect_gaps_simple(period, s_dt, e_dt, rows),
            "head_ts": rows[0]["bar_open_ts"] if rows else None,
            "tail_ts": rows[-1]["bar_end_ts"] if rows else None,
        }
        if return_data:
            result["data"] = rows
        return result

    def _convert_to_rows(self, data_dict: Dict[str, Any], period: str, dividend_type: str) -> List[Dict[str, Any]]:
        if not isinstance(data_dict, dict):
            return []
        time_field = next((name for name in _TIME_FIELDS if name in data_dict), None)
        if time_field is None:
            return []
        time_df = data_dict[time_field]
        if not hasattr(time_df, "index") or not hasattr(time_df, "columns"):
            return []

        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]

        def _lookup(field: str, code: str, col: str) -> Optional[float]:
            df = data_dict.get(field)
            if df is None:
                return None
            try:
                val = df.loc[code, col]
            except Exception:
                try:
                    series = df[col]
                    val = series.loc[code] if hasattr(series, "loc") else series.iloc[0]
                except Exception:
                    return None
            if hasattr(val, "iloc"):
                try:
                    val = val.iloc[0]
                except Exception:
                    val = None
            try:
                return None if val is None else float(val)
            except Exception:
                return None

        rows: List[Dict[str, Any]] = []
        for code in list(time_df.index):
            for col in list(time_df.columns):
                try:
                    raw_ts = time_df.loc[code, col]
                except Exception:
                    try:
                        series = time_df[col]
                        raw_ts = series.loc[code] if hasattr(series, "loc") else series.iloc[0]
                    except Exception:
                        continue
                bar_end_ts = self._normalize_bar_end_ts(raw_ts)
                if bar_end_ts is None:
                    continue
                dt_end = datetime.fromisoformat(bar_end_ts)
                dt_open = (dt_end - delta).astimezone(CN_TZ)
                row = {
                    "code": code,
                    "period": period,
                    "bar_open_ts": dt_open.strftime(ISO),
                    "bar_end_ts": bar_end_ts,
                    "is_closed": True,
                    "dividend_type": dividend_type,
                    "source": "qmt",
                    "recv_ts": None,
                }
                for field in _VALUE_FIELDS:
                    row[field if field != "settelementPrice" else "settlementPrice"] = _lookup(field, code, col)
                rows.append(row)
        rows.sort(key=lambda r: (r["code"], r["bar_end_ts"]))
        return rows

    @staticmethod
    def _normalize_bar_end_ts(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        try:
            if isinstance(raw, (int, float)):
                value = float(raw)
                if value > 1e12:
                    dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
                else:
                    dt = datetime.fromtimestamp(value, tz=timezone.utc)
                return dt.astimezone(CN_TZ).strftime(ISO)
            text = str(raw).strip()
            if not text:
                return None
            if text.isdigit():
                if len(text) == 14:
                    dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
                    return dt.strftime(ISO)
                if len(text) == 8:
                    dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=CN_TZ)
                    return dt.strftime(ISO)
            if "T" not in text:
                text = text.replace(" ", "T")
            if text.endswith("Z"):
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                if "+" not in text:
                    text = f"{text}+08:00"
                dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=CN_TZ)
            return dt.astimezone(CN_TZ).strftime(ISO)
        except Exception:
            return None

    def _detect_gaps_simple(self, period: str, s_dt: datetime, e_dt: datetime,
                            rows: List[Dict[str, Any]]) -> List[str]:
        if not rows:
            return []
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        expected = []
        cursor = s_dt
        while cursor <= e_dt:
            expected.append(cursor.strftime(ISO))
            cursor += delta
        got = {r["bar_end_ts"] for r in rows if r.get("is_closed")}
        return [ts for ts in expected if ts not in got][:2000]
