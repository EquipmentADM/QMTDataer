# -*- coding: utf-8 -*-
"""
xtdata 数据源适配与最小入库工具

包含：
    - BaseMarketDataSource：统一 fetch 接口定义；
    - XtdataSource：基于 xtquant.xtdata 的实现，支持 download+get 的最小链路。

设计说明：
    - 仅处理常见 K 线周期（1m/5m/15m/30m/60m/1d 等，xtdata 支持为准）；
    - fetch 返回标准 DataFrame，至少包含 time/open/high/low/close/volume/amount；
    - time 统一为 ISO8601（+08:00）字符串，便于落盘/序列化。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import pandas as pd

CN_TZ = timezone(timedelta(hours=8))
logger = logging.getLogger(__name__)


class BaseMarketDataSource:
    """行情数据源适配器基类，统一 fetch 接口。"""

    def fetch(
        self,
        symbol: str,
        cycle: str,
        market: str,
        specific: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        子类需实现的核心方法：
          - 根据传入参数返回标准 DataFrame（包含时间列与 OHLCV）。
        """
        raise NotImplementedError


@dataclass
class XtdataSource(BaseMarketDataSource):
    """
    基于 xtquant.xtdata 的行情数据源适配器。

    主要职责：
        1) 按需调用 download_history_data 进行本地补齐；
        2) 通过 get_market_data_ex / get_market_data 读取本地库；
        3) 规范化为 DataFrame 并返回。
    """

    xtdata: Any
    download: bool = True

    def fetch(
        self,
        symbol: str,
        cycle: str,
        market: str,
        specific: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        拉取单个标的的行情数据。

        参数：
            symbol  : 标的代码，如 510300.SH
            cycle   : 周期字符串，如 1m/1d
            market  : 市场名称（此处仅透传，xtdata 不依赖）
            specific: 合成标记（此处仅透传，xtdata 不依赖）
            start   : 开始时间，xtdata 支持 YYYYMMDD 或 YYYYMMDDHHMMSS，为空表示最早
            end     : 结束时间，为空表示当前
        返回：
            DataFrame，包含 time/open/high/low/close/volume/amount 等列，time 为 ISO8601(+08:00)
        """
        if self.download:
            self._download(symbol, cycle, start or "", end or "")

        data_dict = self._get_market_data(symbol, cycle, start or "", end or "")
        if not data_dict:
            raise ValueError(f"xtdata 返回为空：{symbol}-{cycle}-{start}-{end}")

        df = self._normalize(data_dict, symbol)
        if df.empty:
            raise ValueError(f"xtdata 返回空 DataFrame：{symbol}-{cycle}-{start}-{end}")
        return df

    def _download(self, symbol: str, period: str, start: str, end: str) -> None:
        """调用 xtdata.download_history_data 做本地补齐"""
        try:
            self.xtdata.download_history_data(
                stock_code=symbol,
                period=period,
                start_time=start,
                end_time=end,
                incrementally=True,
            )
        except Exception as e:  # pragma: no cover
            logger.warning("[XtdataSource] download_history_data 失败: %s %s err=%s", symbol, period, e)

    def _get_market_data(self, symbol: str, period: str, start: str, end: str) -> Dict[str, Any]:
        """优先使用 get_market_data_ex，失败则回退 get_market_data"""
        kwargs = dict(
            field_list=[],
            stock_list=[symbol],
            period=period,
            start_time=start,
            end_time=end,
            count=-1,
            dividend_type="none",
            fill_data=False,
        )
        try:
            data_dict = self.xtdata.get_market_data_ex(**kwargs)  # type: ignore[attr-defined]
        except Exception:
            data_dict = self.xtdata.get_market_data(**kwargs)
        if not isinstance(data_dict, dict):
            return {}
        return data_dict

    def _normalize(self, data_dict: Dict[str, Any], symbol: str) -> pd.DataFrame:
        """
        将 xtdata 返回的数据转为标准宽表。
        兼容两种常见结构：
            1) {field: DataFrame}，其中 time 是单独的 DataFrame（旧写法）；
            2) {code: DataFrame}，每个 DataFrame 含 time/open/high/low/close/volume/amount 列（新版本常见）。
        """
        # ---- 结构 1：field -> DataFrame ----
        if "time" in data_dict:
            time_df: pd.DataFrame = data_dict["time"]
            rows: List[Dict[str, Any]] = []
            for code in time_df.index:
                for idx in time_df.columns:
                    row: Dict[str, Any] = {"code": code}
                    row["time"] = self._format_time(time_df.loc[code, idx])
                    for field in ("open", "high", "low", "close", "volume", "amount"):
                        df_field = data_dict.get(field)
                        if isinstance(df_field, pd.DataFrame) and code in df_field.index and idx in df_field.columns:
                            row[field] = df_field.loc[code, idx]
                    rows.append(row)
            return pd.DataFrame(rows)

        # ---- 结构 2：code -> DataFrame ----
        rows: List[Dict[str, Any]] = []
        time_keys = ("time", "Time", "datetime", "bar_time")
        for code, df_code in data_dict.items():
            if not isinstance(df_code, pd.DataFrame):
                continue
            time_col = next((c for c in time_keys if c in df_code.columns), None)
            if time_col is None:
                continue
            for _, r in df_code.iterrows():
                rec: Dict[str, Any] = {"code": code, "time": self._format_time(r[time_col])}
                for field in ("open", "high", "low", "close", "volume", "amount"):
                    if field in df_code.columns:
                        rec[field] = r[field]
                rows.append(rec)
        return pd.DataFrame(rows)

    @staticmethod
    def _format_time(raw: Any) -> str:
        """将时间统一为本地无时区的 ISO8601 字符串（YYYY-MM-DDTHH:MM:SS）。"""
        if pd.isna(raw):
            return ""
        try:
            if isinstance(raw, (int, float)):
                ts = float(raw)
                if ts >= 1e12:
                    dt = datetime.fromtimestamp(ts / 1000.0, tz=CN_TZ)
                elif ts >= 1e9:
                    dt = datetime.fromtimestamp(ts, tz=CN_TZ)
                else:
                    # YYYYMMDD 或秒级时间戳，尝试字符串处理
                    s = str(int(ts)).zfill(8)
                    return XtdataSource._format_time(s)
                return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
            s = str(raw).strip()
            if len(s) == 14 and s.isdigit():
                dt = datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
                return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
            if len(s) == 8 and s.isdigit():
                dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=CN_TZ)
                return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
            # 回退直接解析，若缺时区则补 +08:00
            if "T" not in s and " " in s:
                s = s.replace(" ", "T")
            if "Z" in s:
                s = s.replace("Z", "+00:00")
            if "+" not in s:
                s = f"{s}+08:00"
            return (
                datetime.fromisoformat(s)
                .astimezone(CN_TZ)
                .replace(tzinfo=None)
                .strftime("%Y-%m-%dT%H:%M:%S")
            )
        except Exception:
            return str(raw)
