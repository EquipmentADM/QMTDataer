# -*- coding: utf-8 -*-
"""
QMTD 新 FD storage backend 测试。

Responsibilities:
    - 验证 QMTD 默认历史入库 backend 已切换到新 FD
    - 验证 fd_storage_adapter 能保持 QMTD 上层调用形态
    - 验证合并写入、路径命名和运行时自检

Data Contract:
    - 测试只写入临时目录
    - 不连接 miniQMT、Redis 或真实 FD 生产库

Internal Dependencies:
    - core.fd_storage_adapter
    - core.ingestor
    - core.storage_backend

External Systems:
    - FD 独立项目原型源码
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from core.fd_storage_adapter import FinancialDataStorage as FDAdapterStorage
from core.ingestor import MarketDataIngestor
from core.storage_backend import FinancialDataStorage, get_storage_backend_name
from core.xtdata_source import BaseMarketDataSource


class StaticSource(BaseMarketDataSource):
    """
    返回固定 DataFrame 的测试数据源。
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def fetch(self, symbol, cycle, market, specific, start=None, end=None) -> pd.DataFrame:
        """
        返回固定测试数据。

        Returns:
            pd.DataFrame: 测试输入数据副本。
        """

        return self.df.copy()


class FDStorageAdapterTestCase(unittest.TestCase):
    """
    验证 QMTD 新 FD 写入后端。
    """

    def test_default_backend_is_fd_adapter(self) -> None:
        """
        默认 backend 应指向新 FD adapter。

        Returns:
            None: 通过断言验证默认 backend。
        """

        self.assertEqual(get_storage_backend_name(), "fd")
        self.assertIs(FinancialDataStorage, FDAdapterStorage)

    def test_fd_adapter_builds_fd_standard_path(self) -> None:
        """
        adapter 应使用 FD 标准目录和文件名。

        Returns:
            None: 通过断言验证 1m 到 1min 的落盘规则。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialDataStorage(root_dir=tmpdir)
            cycle = storage.validate_cycle("1m")
            target_dir = storage._build_target_dir("SS_stock_data", "510050.SH", cycle, "original")
            filename = storage._build_filename("SS_stock_data", "510050.SH", cycle, "original", "csv")

            self.assertEqual(cycle, "1m")
            self.assertEqual(Path(target_dir), Path(tmpdir) / "SS_stock_data" / "510050.SH" / "1min" / "original")
            self.assertEqual(filename, "510050.SH_1min.csv")

    def test_fd_adapter_merge_and_runtime_check(self) -> None:
        """
        adapter 应能合并写入并通过运行时自检。

        Returns:
            None: 通过断言验证合并结果。
        """

        first_df = pd.DataFrame(
            {
                "time": ["2026-01-01T09:30:00", "2026-01-01T09:31:00"],
                "open": [1.0, 2.0],
                "high": [1.1, 2.1],
                "low": [0.9, 1.9],
                "close": [1.0, 2.0],
                "volume": [100, 200],
                "amount": [1000.0, 2000.0],
            }
        )
        second_df = pd.DataFrame(
            {
                "time": ["2026-01-01T09:31:00", "2026-01-01T09:32:00"],
                "open": [20.0, 3.0],
                "high": [21.0, 3.1],
                "low": [19.0, 2.9],
                "close": [20.0, 3.0],
                "volume": [2000, 300],
                "amount": [20000.0, 3000.0],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialDataStorage(root_dir=tmpdir)
            ingestor = MarketDataIngestor(storage)
            ingestor.ingest_symbol(
                source=StaticSource(first_df),
                market="SS_stock_data",
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                file_type="csv",
                time_column="time",
                merge=True,
            )
            out_path = ingestor.ingest_symbol(
                source=StaticSource(second_df),
                market="SS_stock_data",
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                file_type="csv",
                time_column="time",
                merge=True,
            )

            saved = pd.read_csv(out_path)
            runtime = storage.check_runtime()

            self.assertEqual(len(saved), 3)
            self.assertEqual(float(saved.loc[1, "open"]), 20.0)
            self.assertEqual(Path(out_path).name, "510050.SH_1min.csv")
            self.assertEqual(runtime["backend"], "fd")
            self.assertEqual(runtime["merge_write_dataframe"], "ok")


if __name__ == "__main__":
    unittest.main()
