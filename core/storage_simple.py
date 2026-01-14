# -*- coding: utf-8 -*-
"""
简化版本地行情存储工具（对齐原始 FinancialDataStorage 的核心入库约定）。
Responsibilities:
    - 校验/映射 market、cycle、specific，保持与原始目录/文件名规则一致
    - 将标准化的 DataFrame 落盘到文件树（csv/pkl），默认覆盖写入
    - 提供按时间范围过滤的小工具，支持上游 ingest 在落盘前截取区间
Data Contract:
    - DataFrame 至少包含业务所需字段（通常为 time/open/high/low/close/volume/amount），本类不做字段校验，仅负责落盘
Internal Dependencies:
    - pandas
External Systems:
    - 无
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd


@dataclass
class FinancialDataStorage:
    """
    本地行情存储适配器（文件树）。
    设计取舍：
        - 保留原始 FinancialDataStorage 的目录/命名约定与映射校验，便于兼容旧数据。
        - 仅实现入库相关的核心能力；高级功能（缺口补齐、合并等）需在上层组合。
    """

    root_dir: str
    market_list: List[str] = field(default_factory=lambda: ["Futures_data", "SS_stock_data", "Index_data", "US_stock_data", "H_stock_data", "Crypto_data"])
    file_type_list: List[str] = field(default_factory=lambda: ["csv", "pkl"])
    specific_list: List[str] = field(default_factory=lambda: ["主力连续", "次主力连续", "888", "original", "original-daly"])

    directory_mapping: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "Futures_data": ["Futures_data", "期货", "futures", "FUTURES", "F"],
            "SS_stock_data": ["SS_stock_data", "A股", "上证股票", "上证", "ss_stock", "ss", "SS"],
            "Index_data": ["Index_data", "指数", "index", "IDX"],
            "US_stock_data": ["US_stock_data", "美股", "us_stock", "US"],
            "H_stock_data": ["H_stock_data", "港股", "h_stock", "HK"],
            "Crypto_data": ["Crypto_data", "加密货币", "crypto", "Crypto", "CRYPTO", "cy", "Cy", "CY"],
        }
    )
    cycle_list: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "30m", "1h", "1d"])
    cycle_mapping: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "1m": ["1m", "1min", "1M", "1Min", "1MIN", "1MiN"],
            "5m": ["5m", "5min", "5M", "5Min", "5MIN"],
            "15m": ["15m", "15min", "15M", "15Min"],
            "30m": ["30m", "30min", "30M", "30Min"],
            "1h": ["1h", "1H", "60min", "60M", "60m"],
            "1d": ["1d", "1D", "1day", "1Day", "1DAY", "1DaY"],
        }
    )
    _cycle_dir_map: Dict[str, str] = field(
        default_factory=lambda: {
            "1m": "1min",
            "5m": "5min",
            "15m": "15min",
            "30m": "30min",
            # 小时/日不改动
            "1h": "1h",
            "1d": "1d",
        }
    )
    specific_mapping: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "主力连续": ["主力连续", "主力", "主连", "Main", "continuous_main"],
            "次主力连续": ["次主力连续", "次主力", "次连", "Sub", "continuous_sub"],
            "888": ["888", "recent", "latest"],
            "original": ["original", "o", "O", "org"],
            "original-daly": ["original-daly", "daly"],
        }
    )

    def validate_cycle(self, cycle_input: str) -> str:
        """
        校验并标准化周期输入，支持多写法映射。
        Args:
            cycle_input (str): 原始周期字符串。
        Returns:
            str: 标准化后的周期。
        """
        if cycle_input in self.cycle_list:
            return cycle_input
        for standard, variations in self.cycle_mapping.items():
            if cycle_input in variations:
                return standard
        raise ValueError(f"Unsupported cycle input: {cycle_input}")

    def validate_market(self, directory_input: str) -> str:
        """
        校验并标准化市场输入，支持多写法映射。
        Args:
            directory_input (str): 原始市场字符串。
        Returns:
            str: 标准化后的市场名。
        """
        if directory_input in self.directory_mapping:
            return directory_input
        for standard, variations in self.directory_mapping.items():
            if directory_input in variations:
                return standard
        raise ValueError(f"Unsupported directory input: {directory_input}")

    def validate_specific(self, specific_input: str) -> str:
        """
        校验并标准化 specific 输入，支持多写法映射。
        Args:
            specific_input (str): 合成/子目录标记。
        Returns:
            str: 标准化后的 specific。
        """
        specific_input = specific_input or "original"
        if specific_input in self.specific_list:
            return specific_input
        for standard, variations in self.specific_mapping.items():
            if specific_input in variations:
                return standard
        raise ValueError(f"Unsupported specific input: {specific_input}")

    @staticmethod
    def _normalize_extension(file_type: str) -> str:
        """统一文件扩展名写法，补充前导点。"""
        return file_type if file_type.startswith(".") else f".{file_type}"

    @staticmethod
    def _parse_time_series(series: pd.Series) -> pd.Series:
        """
        统一解析时间列：
          - 数值型：根据数量级自动选择秒/毫秒
          - 字符串/对象：使用 pandas to_datetime 解析
        返回 tz-naive 的 datetime64[ns] 序列，解析失败为 NaT。
        """
        s = series
        if pd.api.types.is_numeric_dtype(s):
            # 判断数量级：>=1e12 视为毫秒，>=1e9 视为秒
            max_abs = s.dropna().abs().max() if len(s.dropna()) > 0 else 0
            unit = "ms" if max_abs >= 1e12 else "s" if max_abs >= 1e9 else None
            if unit:
                parsed = pd.to_datetime(s, errors="coerce", unit=unit)
            else:
                parsed = pd.to_datetime(s, errors="coerce")
        else:
            parsed = pd.to_datetime(s, errors="coerce")
        # 去除时区信息
        try:
            parsed = parsed.dt.tz_localize(None)
        except Exception:
            pass
        return parsed

    def _build_filename(
        self,
        market: str,
        symbol: str,
        cycle: str,
        specific: str,
        file_type: str,
    ) -> str:
        """生成单文件名，遵循原始命名规则。"""
        ext = self._normalize_extension(file_type)
        cycle_dir = self._cycle_dir_map.get(cycle, cycle)
        if market == "Futures_data":
            if specific in self.specific_list and specific != "888":
                return f"{symbol}{specific}合成{ext}"
            if specific in self.specific_list or specific == "888":
                return f"{symbol}888{ext}"
            re_res = re.match(r"^([a-zA-Z]{0,2})(\d{3,4})", specific)
            if not re_res:
                raise ValueError(f"无效的specific 参数: {specific}")
            return f"{symbol}{re_res.group(2)}{ext}"
        if market in {"Crypto_data", "Index_data", "SS_stock_data", "US_stock_data", "H_stock_data"}:
            return f"{symbol}_{cycle_dir}{ext}"
        raise ValueError(f"暂未支持的market: {market}")

    def _build_target_dir(self, market: str, symbol: str, cycle: str, specific: str) -> str:
        """构造目标目录并确保存在。"""
        cycle_dir = self._cycle_dir_map.get(cycle, cycle)
        path = Path(self.root_dir) / market / symbol / cycle_dir
        if specific:
            path = path / specific
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        target_dir: str,
        *,
        symbol: str,
        cycle: str,
        specific: str,
        market: str,
        file_type: str,
        overwrite: bool = False,
    ) -> None:
        """
        将 DataFrame 按规则落盘。
        Args:
            df (pd.DataFrame): 待保存数据。
            target_dir (str): 目标目录。
            symbol/cycle/specific/market/file_type: 用于命名。
            overwrite (bool): 是否允许覆盖已有文件。
        """
        ext = self._normalize_extension(file_type)
        filename = self._build_filename(market, symbol, cycle, specific, file_type)
        file_path = Path(target_dir) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"{file_path} 已存在，若需覆盖请设置 overwrite=True")
        if ext == ".csv":
            df.to_csv(file_path, index=False, encoding="utf-8-sig")
        elif ext == ".pkl":
            df.to_pickle(file_path)
        else:
            raise ValueError(f"不支持的文件类型: {file_type}")

    def filter_df_by_date(
        self,
        df: pd.DataFrame,
        start_date: Optional[str],
        end_date: Optional[str],
        time_columns: Sequence[str],
        allow_sort: bool = True,
    ) -> pd.DataFrame:
        """
        按时间范围过滤 DataFrame（用于落盘前截取区间）。
        Args:
            df (pd.DataFrame): 输入数据，包含时间列。
            start_date (str | None): 起始（含），支持 pandas 解析。
            end_date (str | None): 结束（含），支持 pandas 解析。
            time_columns (Sequence[str]): 时间列名称列表。
            allow_sort (bool): 是否在过滤后按时间升序排序。
        Returns:
            pd.DataFrame: 过滤后的结果。
        """
        if not time_columns:
            return df
        df_out = df.copy()
        for col in time_columns:
            if col not in df_out.columns:
                continue
            ts = self._parse_time_series(df_out[col])
            mask = pd.Series(True, index=df_out.index)
            sd = pd.to_datetime(start_date, errors="coerce") if start_date else None
            ed = pd.to_datetime(end_date, errors="coerce") if end_date else None
            if isinstance(sd, pd.Timestamp) and sd.tzinfo is not None:
                sd = sd.tz_convert(None)
            if isinstance(ed, pd.Timestamp) and ed.tzinfo is not None:
                ed = ed.tz_convert(None)
            if start_date:
                mask &= ts >= sd
            if end_date:
                mask &= ts <= ed
            df_out = df_out.loc[mask]
        if allow_sort and time_columns:
            first_col = time_columns[0]
            if first_col in df_out.columns:
                df_out = df_out.sort_values(by=first_col)
        return df_out

    def merge_and_save(
        self,
        new_df: pd.DataFrame,
        target_dir: str,
        *,
        symbol: str,
        cycle: str,
        specific: str,
        market: str,
        file_type: str,
        time_column: str = "time",
        dropna_time: bool = True,
        prefer_new: bool = True,
    ) -> str:
        """
        将新数据与已有文件按时间列合并去重后落盘，用于增量更新。
        Args:
            new_df (pd.DataFrame): 新数据（需包含 time_column）。
            target_dir (str): 目标目录。
            symbol/cycle/specific/market/file_type: 命名与路径参数。
            time_column (str): 时间列名称，默认 time。
            dropna_time (bool): 是否丢弃时间列空值。
            prefer_new (bool): 时间重复时是否保留新数据。
        Returns:
            str: 写入文件的完整路径。
        """
        ext = self._normalize_extension(file_type)
        filename = self._build_filename(market, symbol, cycle, specific, file_type)
        file_path = Path(target_dir) / filename

        # 读已有数据（若存在）
        existing = pd.DataFrame()
        if file_path.exists():
            try:
                if ext == ".csv":
                    existing = pd.read_csv(file_path)
                elif ext == ".pkl":
                    existing = pd.read_pickle(file_path)
            except Exception:
                existing = pd.DataFrame()

        # 解析时间列，记录调试信息
        existing_rows = len(existing)
        new_rows = len(new_df)
        exist_time_range = (None, None)
        new_time_range = (None, None)
        if time_column in existing.columns:
            exist_parsed = self._parse_time_series(existing[time_column])
            if not exist_parsed.dropna().empty:
                exist_time_range = (exist_parsed.min(), exist_parsed.max())
            existing[time_column] = exist_parsed
        if time_column in new_df.columns:
            new_parsed = self._parse_time_series(new_df[time_column])
            if not new_parsed.dropna().empty:
                new_time_range = (new_parsed.min(), new_parsed.max())
            new_df = new_df.copy()
            new_df[time_column] = new_parsed

        # 统一时间列并合并
        frames = [existing, new_df]
        merged = pd.concat(frames, ignore_index=True)
        if time_column in merged.columns:
            merged[time_column] = self._parse_time_series(merged[time_column])
            if dropna_time:
                merged = merged.dropna(subset=[time_column])
            merged = merged.drop_duplicates(subset=[time_column], keep="last" if prefer_new else "first")
            merged = merged.sort_values(by=time_column)
            # 再次记录合并后的时间范围
            merged_time = merged[time_column]
            merged_time_range = (None, None)
            if not merged_time.dropna().empty:
                merged_time_range = (merged_time.min(), merged_time.max())
            print(f"[DEBUG merge] existing={existing_rows} {exist_time_range}, new={new_rows} {new_time_range}, merged={len(merged)} {merged_time_range}")
            # 将时间列标准化为无时区的 ISO 字符串
            try:
                merged[time_column] = pd.to_datetime(merged[time_column], errors="coerce").dt.tz_localize(None)
                merged[time_column] = merged[time_column].dt.strftime("%Y-%m-%dT%H:%M:%S")
            except Exception:
                pass

        # 保存
        self._save_dataframe(
            merged,
            target_dir,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            market=market,
            file_type=file_type,
            overwrite=True,
        )
        return str(file_path)
