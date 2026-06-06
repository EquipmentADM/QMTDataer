# -*- coding: utf-8 -*-
"""
QMTD 到新 FD 数据库的写入适配器。

Responsibilities:
    - 以 QMTD 旧 storage_simple.py 的方法形态承接上层调用
    - 将路径、文件名、写入和合并写入规则转交给 FD 标准实现
    - 保留 legacy backend 回滚前提下，默认支持新 FD 数据库上线

Data Contract:
    - DataFrame 写入仍要求包含 QMTD 上游生成的 time/open/high/low/close/volume 等字段
    - merge_and_save 返回写入文件路径，保持 QMTD 上层调用兼容

Internal Dependencies:
    - FD 独立项目原型 src/fd_core

External Systems:
    - D:/Work/Quant/financial_database
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd


DEFAULT_FD_REPO = Path(r"D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型")
DEFAULT_FD_SRC = DEFAULT_FD_REPO / "src"

QMTD_CYCLE_MAPPING = {
    "1m": "1m",
    "1min": "1m",
    "1M": "1m",
    "1Min": "1m",
    "1MIN": "1m",
    "5m": "5m",
    "5min": "5m",
    "15m": "15m",
    "15min": "15m",
    "30m": "30m",
    "30min": "30m",
    "1h": "1h",
    "1H": "1h",
    "60min": "1h",
    "60m": "1h",
    "1d": "1d",
    "1D": "1d",
    "1day": "1d",
}


def resolve_fd_src(fd_repo: Optional[str] = None, fd_src: Optional[str] = None) -> Path:
    """
    解析 FD 源码 src 路径。

    Args:
        fd_repo (Optional[str]): FD 原型项目根目录。
        fd_src (Optional[str]): FD 源码 src 目录。

    Returns:
        Path: 可导入 fd_core 的 src 路径。
    """

    if fd_src:
        return Path(fd_src)
    env_src = os.environ.get("FD_CORE_SRC")
    if env_src:
        return Path(env_src)

    repo = Path(fd_repo or os.environ.get("FD_REPO", DEFAULT_FD_REPO))
    return repo / "src"


def ensure_fd_core_on_path(fd_repo: Optional[str] = None, fd_src: Optional[str] = None) -> Path:
    """
    确保当前进程可以导入 fd_core。

    Args:
        fd_repo (Optional[str]): FD 原型项目根目录。
        fd_src (Optional[str]): FD 源码 src 目录。

    Returns:
        Path: 实际加入 sys.path 的 src 路径。
    """

    source_path = resolve_fd_src(fd_repo=fd_repo, fd_src=fd_src)
    if not source_path.exists():
        raise FileNotFoundError(f"FD src 路径不存在: {source_path}")
    if str(source_path) not in sys.path:
        sys.path.insert(0, str(source_path))
    return source_path


class FinancialDataStorage:
    """
    QMTD 写入新 FD 的兼容存储类。

    该类名字保持为 `FinancialDataStorage`，用于最小化 QMTD 上层改动。
    默认写入新 FD；如需回滚，可通过 `core.storage_backend` 切回 legacy。
    """

    _cycle_dir_map = {
        "1m": "1min",
        "1min": "1min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "30min": "30min",
        "1h": "1h",
        "1d": "1d",
    }

    cycle_list = ["1m", "5m", "15m", "30m", "1h", "1d"]
    file_type_list = ["csv", "pkl"]

    def __init__(
        self,
        root_dir: str,
        fd_repo: Optional[str] = None,
        fd_src: Optional[str] = None,
        initialize: bool = False,
    ) -> None:
        """
        初始化新 FD 适配存储。

        Args:
            root_dir (str): FD 数据库根目录。
            fd_repo (Optional[str]): FD 原型项目根目录。
            fd_src (Optional[str]): FD 源码 src 目录。
            initialize (bool): 是否初始化 FD 市场目录。

        Returns:
            None: 该方法仅创建内部 FD storage。
        """

        self.root_dir = root_dir
        self.fd_src = ensure_fd_core_on_path(fd_repo=fd_repo, fd_src=fd_src)

        from fd_core import FinancialDataStorage as FDCoreStorage

        self._storage = FDCoreStorage(root_dir=root_dir, initialize=initialize)
        self.file_type_list = list(getattr(self._storage, "file_type_list", self.file_type_list))

    @staticmethod
    def _parse_time_series(series: pd.Series) -> pd.Series:
        """
        解析时间序列为 tz-naive datetime64[ns]。

        Args:
            series (pd.Series): 待解析时间列。

        Returns:
            pd.Series: 解析后的时间序列。
        """

        ensure_fd_core_on_path()
        from fd_core.filtering import _to_datetime64_ns

        return _to_datetime64_ns(series)

    @staticmethod
    def _normalize_extension(file_type: str) -> str:
        """
        规范化文件扩展名。

        Args:
            file_type (str): 文件类型。

        Returns:
            str: 带点号的扩展名。
        """

        return file_type if file_type.startswith(".") else f".{file_type}"

    def validate_cycle(self, cycle_input: str) -> str:
        """
        标准化周期输入，保持 QMTD 上层 xtdata 周期语义。

        Args:
            cycle_input (str): 原始周期输入。

        Returns:
            str: QMTD 标准周期。
        """

        if cycle_input in QMTD_CYCLE_MAPPING:
            return QMTD_CYCLE_MAPPING[cycle_input]
        raise ValueError(f"Unsupported cycle input: {cycle_input}")

    def validate_market(self, directory_input: str) -> str:
        """
        标准化市场输入。

        Args:
            directory_input (str): 原始市场输入。

        Returns:
            str: FD 标准市场目录。
        """

        return self._storage.validate_market(directory_input)

    def validate_specific(self, specific_input: str) -> str:
        """
        标准化 specific 输入。

        Args:
            specific_input (str): 原始 specific。

        Returns:
            str: FD 标准 specific。
        """

        return self._storage.validate_specific(specific_input or "original")

    def _build_target_dir(self, market: str, symbol: str, cycle: str, specific: str) -> str:
        """
        构造 FD 标准目标目录。

        Args:
            market (str): 市场输入。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。

        Returns:
            str: 目标目录路径。
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
            str: 文件名。
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
        按 FD 标准规则写入 DataFrame。

        Args:
            df (pd.DataFrame): 待写入数据表。
            target_dir (str): 兼容旧参数，实际路径由 FD 规则决定。
            symbol (str): 标的代码。
            cycle (str): 周期输入。
            specific (str): specific 输入。
            market (str): 市场输入。
            file_type (str): 文件类型。
            overwrite (bool): 是否覆盖已有文件。

        Returns:
            None: 该方法仅执行写入。
        """

        _ = target_dir
        self._storage.write_dataframe(
            df=df,
            market=market,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            file_type=file_type,
            overwrite=overwrite,
        )

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
            df (pd.DataFrame): 输入数据表。
            start_date (Optional[str]): 起始时间。
            end_date (Optional[str]): 结束时间。
            time_columns (Sequence[str]): 候选时间列。
            allow_sort (bool): 是否允许排序。

        Returns:
            pd.DataFrame: 过滤后的数据表。
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
        按 FD 标准合并写入接口保存 DataFrame。

        Args:
            new_df (pd.DataFrame): 新数据。
            target_dir (str): 兼容旧参数，实际路径由 FD 规则决定。
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
        return self._storage.merge_write_dataframe(
            df=new_df,
            market=market,
            symbol=symbol,
            cycle=cycle,
            specific=specific,
            file_type=file_type,
            time_column=time_column,
            dropna_time=dropna_time,
            prefer_new=prefer_new,
        )

    def check_runtime(self) -> dict[str, str]:
        """
        返回 QMTD 使用新 FD 的运行时自检信息。

        Returns:
            dict[str, str]: FD src、root 和关键能力状态。
        """

        has_merge = hasattr(self._storage, "merge_write_dataframe")
        if not has_merge:
            raise RuntimeError("当前 FD storage 缺少 merge_write_dataframe，不能作为 QMTD fd backend。")
        root_path = Path(self.root_dir)
        if not root_path.exists():
            raise FileNotFoundError(f"FD 数据库根目录不存在: {root_path}")
        return {
            "backend": "fd",
            "fd_src": str(self.fd_src),
            "fd_root": str(root_path),
            "merge_write_dataframe": "ok",
        }
