# -*- coding: utf-8 -*-
"""
新 FD 数据库写入适配器。

Responsibilities:
    - 在 QMTD 上层调用形态不大改的前提下，转接新 FD 标准写入接口。
    - 模拟当前 storage_simple.FinancialDataStorage 的入库必要方法。
    - 为历史入库提供可回滚的 fd backend。

Data Contract:
    - df 参数必须是 pandas DataFrame。
    - merge_and_save 使用 FD merge_write_dataframe，按 time_column 合并去重。
    - target_dir 参数仅用于兼容旧调用，实际路径由 FD 标准规则生成。

External Systems:
    - 新 FD 原型项目 src 目录。
    - 本地 FD 数据库目录。
"""
from __future__ import annotations

import os
from pathlib import Path
import sys
from typing import Any, Optional, Sequence

import pandas as pd

from core.time_utils import parse_local_naive_time_series


DEFAULT_FD_REPO = Path(r"D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型")


def _resolve_fd_src(fd_repo: Optional[str] = None) -> Path:
    """
    解析 FD 项目 src 目录。

    Args:
        fd_repo (Optional[str]): FD 项目根目录或 src 目录。

    Returns:
        Path: 可导入 fd_core 的 src 目录。
    """

    raw = fd_repo or os.environ.get("FD_REPO") or os.environ.get("FD_CORE_SRC") or str(DEFAULT_FD_REPO)
    path = Path(raw)
    if path.name == "src":
        return path
    return path / "src"


def ensure_fd_core_on_path(fd_repo: Optional[str] = None) -> Path:
    """
    确保当前进程可以导入 fd_core。

    Args:
        fd_repo (Optional[str]): FD 项目根目录或 src 目录。

    Returns:
        Path: 已加入 sys.path 的 src 目录。
    """

    src_path = _resolve_fd_src(fd_repo)
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    return src_path


class FDStorageAdapter:
    """
    QMTD 旧 storage 接口到新 FD FinancialDataStorage 的适配器。

    该类是过渡层：上层仍可调用 `_save_dataframe`、`merge_and_save`
    等旧方法，内部实际走 FD 标准写入契约。
    """

    backend_name = "fd"

    def __init__(self, root_dir: str, fd_repo: Optional[str] = None) -> None:
        """
        初始化 FD 适配器。

        Args:
            root_dir (str): FD 数据库根目录。
            fd_repo (Optional[str]): FD 项目根目录或 src 目录。

        Returns:
            None: 仅创建内部 FD storage 实例。
        """

        self.fd_src = ensure_fd_core_on_path(fd_repo)
        from fd_core import FinancialDataStorage

        self._storage = FinancialDataStorage(root_dir=root_dir, initialize=False)
        self.root_dir = root_dir
        self.file_type_list = self._storage.file_type_list
        self.last_summary: Optional[dict[str, Any]] = None

    @staticmethod
    def _parse_time_series(series: pd.Series) -> pd.Series:
        """
        解析时间列为 tz-naive datetime64[ns]。

        Args:
            series (pd.Series): 待解析时间序列。

        Returns:
            pd.Series: tz-naive datetime64[ns] 序列。
        """

        return parse_local_naive_time_series(series)

    def _normalize_existing_time_file(
        self,
        *,
        symbol: str,
        cycle: str,
        specific: str,
        market: str,
        file_type: str,
        time_column: str,
    ) -> None:
        """
        合并前规范化已有文件的时间列。

        Args:
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。
            market (str): 市场输入。
            file_type (str): 文件类型。
            time_column (str): 时间列名。

        Returns:
            None: 若文件不存在或无时间列则不处理。
        """

        target_dir = self._build_target_dir(market, symbol, cycle, specific)
        filename = self._build_filename(market, symbol, cycle, specific, file_type)
        file_path = Path(target_dir) / filename
        if not file_path.exists():
            return

        ext = str(file_type).lower().lstrip(".")
        if ext == "csv":
            existing = pd.read_csv(file_path)
        elif ext == "pkl":
            existing = pd.read_pickle(file_path)
        else:
            return
        if time_column not in existing.columns or existing.empty:
            return

        existing = existing.copy()
        existing[time_column] = self._parse_time_series(existing[time_column])
        self._storage.write_dataframe(
            df=existing,
            market=market,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            file_type=file_type,
            overwrite=True,
        )

    def validate_cycle(self, cycle_input: str) -> str:
        """
        标准化周期输入。

        Args:
            cycle_input (str): 原始周期输入。

        Returns:
            str: FD 标准周期，例如 1min、1d。
        """

        return self._storage.validate_cycle(cycle_input)

    def validate_market(self, directory_input: str) -> str:
        """
        标准化市场输入。

        Args:
            directory_input (str): 原始市场输入。

        Returns:
            str: FD 标准市场目录名。
        """

        return self._storage.validate_market(directory_input)

    def validate_specific(self, specific_input: str) -> str:
        """
        标准化 specific 输入。

        Args:
            specific_input (str): 原始 specific 输入。

        Returns:
            str: FD 标准 specific。
        """

        return self._storage.validate_specific(specific_input)

    def _build_target_dir(self, market: str, symbol: str, cycle: str, specific: str) -> str:
        """
        构造 FD 标准目标目录。

        Args:
            market (str): 市场输入。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。

        Returns:
            str: 目标目录。
        """

        return self._storage._build_target_dir(market, symbol, cycle, specific)

    def _build_filename(
        self,
        market: str,
        symbol: str,
        cycle: str,
        specific: str,
        file_type: str,
    ) -> str:
        """
        构造 FD 标准文件名。

        Args:
            market (str): 市场输入。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。
            file_type (str): 文件类型。

        Returns:
            str: 标准文件名。
        """

        return self._storage._build_filename(market, symbol, cycle, specific, file_type)

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
        通过 FD 标准接口直接写入 DataFrame。

        Args:
            df (pd.DataFrame): 待写入数据。
            target_dir (str): 兼容旧参数，实际不参与路径决策。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。
            market (str): 市场输入。
            file_type (str): 文件类型。
            overwrite (bool): 是否允许覆盖。

        Returns:
            None: 仅执行写入。
        """

        _ = target_dir
        path = self._storage.write_dataframe(
            df=df,
            market=market,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            file_type=file_type,
            overwrite=overwrite,
        )
        self.last_summary = {"path": path, "rows_after": len(df), "rows_added": len(df)}

    def filter_df_by_date(
        self,
        df: pd.DataFrame,
        start_date: Optional[str],
        end_date: Optional[str],
        time_columns: Sequence[str],
        allow_sort: bool = True,
    ) -> pd.DataFrame:
        """
        按时间范围过滤 DataFrame。

        Args:
            df (pd.DataFrame): 输入数据。
            start_date (Optional[str]): 起始时间。
            end_date (Optional[str]): 结束时间。
            time_columns (Sequence[str]): 时间列候选。
            allow_sort (bool): 是否允许排序。

        Returns:
            pd.DataFrame: 过滤后的数据。
        """

        return self._storage.filter_df_by_date(
            df=df,
            start_date=start_date,
            end_date=end_date,
            time_columns=time_columns,
            allow_sort=allow_sort,
        )

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
        通过 FD 标准合并接口写入 DataFrame。

        Args:
            new_df (pd.DataFrame): 新数据。
            target_dir (str): 兼容旧参数，实际不参与路径决策。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。
            market (str): 市场输入。
            file_type (str): 文件类型。
            time_column (str): 合并时间列。
            dropna_time (bool): 是否丢弃空时间。
            prefer_new (bool): 重复时间是否保留新数据。

        Returns:
            str: 写入文件路径。
        """

        _ = target_dir
        target_dir_use = self._build_target_dir(market, symbol, cycle, specific)
        filename = self._build_filename(market, symbol, cycle, specific, file_type)
        file_path = Path(target_dir_use) / filename
        ext = str(file_type).lower().lstrip(".")

        existing = pd.DataFrame()
        file_existed = file_path.exists()
        if file_existed:
            if ext == "csv":
                existing = pd.read_csv(file_path)
            elif ext == "pkl":
                existing = pd.read_pickle(file_path)
            if not existing.empty and time_column not in existing.columns:
                raise ValueError(f"已有文件缺少时间列，无法合并: {time_column}")

        rows_before = int(len(existing))
        df_to_write = new_df.copy()
        if not existing.empty and time_column in existing.columns:
            existing[time_column] = self._parse_time_series(existing[time_column])
        if time_column in df_to_write.columns:
            # FD 当前合并时会把新旧时间列拼成一个 Series 后统一解析。
            # 若旧文件是空格格式而新数据是 T 格式，pandas 严格解析可能把新行转成 NaT。
            # 因此 QMTD 在调用 FD merge 前先把新数据时间列转为 tz-naive datetime64[ns]。
            df_to_write[time_column] = self._parse_time_series(df_to_write[time_column])

        merged = pd.concat([existing, df_to_write], ignore_index=True)
        if time_column in merged.columns:
            merged[time_column] = self._parse_time_series(merged[time_column])
            if dropna_time:
                merged = merged.dropna(subset=[time_column])
            keep_rule = "last" if prefer_new else "first"
            merged = merged.drop_duplicates(subset=[time_column], keep=keep_rule)
            merged = merged.sort_values(time_column, kind="mergesort").reset_index(drop=True)

        written_path = self._storage.write_dataframe(
            df=merged,
            market=market,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            file_type=file_type,
            overwrite=True,
        )
        rows_after = int(len(merged))
        self.last_summary = {
            "path": written_path,
            "market": self.validate_market(market),
            "symbol": symbol,
            "cycle": self.validate_cycle(cycle),
            "specific": self.validate_specific(specific),
            "file_type": ext,
            "time_column": time_column,
            "rows_before": rows_before,
            "rows_new": int(len(new_df)),
            "rows_after": rows_after,
            "rows_added": max(0, rows_after - rows_before),
            "file_existed": file_existed,
        }
        if time_column in merged.columns and not merged.empty:
            self.last_summary["start_time"] = merged[time_column].min()
            self.last_summary["end_time"] = merged[time_column].max()
        return str(written_path)
