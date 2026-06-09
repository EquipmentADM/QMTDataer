# -*- coding: utf-8 -*-
"""
FDStorageAdapter 单元测试。

Responsibilities:
    - 验证 QMTD fd backend 可以导入新 FD。
    - 验证直接写入与合并写入符合 FD 标准目录。
    - 验证重复时间点保留新数据。

External Systems:
    - 本测试只写临时目录，不连接 miniQMT、Redis 或真实数据库。
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from core.fd_storage_adapter import FDStorageAdapter


class TestFDStorageAdapter(unittest.TestCase):
    """
    新 FD 写入适配器测试。
    """

    def test_write_and_merge_stock_data(self) -> None:
        """
        验证股票数据可按 FD 标准路径写入并合并。

        Returns:
            None: 通过断言验证行为。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = FDStorageAdapter(root_dir=tmpdir)
            existing = pd.DataFrame(
                {
                    "time": ["2026-01-01T09:30:00"],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [100],
                    "amount": [1000],
                }
            )
            new = pd.DataFrame(
                {
                    "time": ["2026-01-01T09:30:00", "2026-01-01T09:31:00"],
                    "open": [2.0, 3.0],
                    "high": [2.0, 3.0],
                    "low": [2.0, 3.0],
                    "close": [2.0, 3.0],
                    "volume": [200, 300],
                    "amount": [2000, 3000],
                }
            )

            target_dir = adapter._build_target_dir("SS_stock_data", "510050.SH", "1m", "original")
            adapter._save_dataframe(
                existing,
                target_dir,
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                market="SS_stock_data",
                file_type="csv",
                overwrite=True,
            )
            out_path = adapter.merge_and_save(
                new,
                target_dir,
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                market="SS_stock_data",
                file_type="csv",
            )

            self.assertEqual(
                Path(out_path),
                Path(tmpdir) / "SS_stock_data" / "510050.SH" / "1min" / "original" / "510050.SH_1min.csv",
            )
            loaded = pd.read_csv(out_path)
            self.assertEqual(loaded["close"].tolist(), [2.0, 3.0])
            self.assertEqual(adapter.last_summary["rows_added"], 1)

    def test_futures_path_uses_fd_specific_rule(self) -> None:
        """
        验证期货主力连续路径与文件名使用 FD 标准规则。

        Returns:
            None: 通过断言验证行为。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = FDStorageAdapter(root_dir=tmpdir)
            target_dir = adapter._build_target_dir("Futures_data", "rb", "1m", "主力连续")
            filename = adapter._build_filename("Futures_data", "rb", "1m", "主力连续", "csv")

            self.assertEqual(Path(target_dir), Path(tmpdir) / "Futures_data" / "rb" / "1min" / "主力连续")
            self.assertEqual(filename, "rb主力连续合成.csv")

    def test_time_column_contract_after_merge(self) -> None:
        """
        验证 FD backend 写出时间列满足 QMTD/FD 协同契约。

        契约重点不是字符串是否包含 T，而是：
            - pd.to_datetime 可解析；
            - 解析后无 NaT；
            - 时间升序；
            - 同一时间点唯一。

        Returns:
            None: 通过断言验证行为。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = FDStorageAdapter(root_dir=tmpdir)
            existing = pd.DataFrame(
                {
                    "time": ["2026-01-01 09:29:00", "2026-01-01 09:30:00"],
                    "open": [0.5, 1.0],
                    "high": [0.5, 1.0],
                    "low": [0.5, 1.0],
                    "close": [0.5, 1.0],
                    "volume": [50, 100],
                    "amount": [500, 1000],
                }
            )
            df = pd.DataFrame(
                {
                    "time": [
                        "2026-01-01T09:31:00",
                        "2026-01-01 09:30:00",
                        "2026-01-01T09:30:00",
                    ],
                    "open": [3.0, 1.0, 2.0],
                    "high": [3.0, 1.0, 2.0],
                    "low": [3.0, 1.0, 2.0],
                    "close": [3.0, 1.0, 2.0],
                    "volume": [300, 100, 200],
                    "amount": [3000, 1000, 2000],
                }
            )
            target_dir = adapter._build_target_dir("SS_stock_data", "510050.SH", "1m", "original")
            adapter._save_dataframe(
                existing,
                target_dir,
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                market="SS_stock_data",
                file_type="csv",
                overwrite=True,
            )
            out_path = adapter.merge_and_save(
                df,
                target_dir,
                symbol="510050.SH",
                cycle="1m",
                specific="original",
                market="SS_stock_data",
                file_type="csv",
            )

            loaded = pd.read_csv(out_path)
            parsed = pd.to_datetime(loaded["time"], errors="coerce")

            self.assertFalse(parsed.isna().any())
            self.assertTrue(parsed.is_monotonic_increasing)
            self.assertFalse(parsed.duplicated().any())
            self.assertEqual(loaded["close"].tolist(), [0.5, 2.0, 3.0])


if __name__ == "__main__":
    unittest.main()
