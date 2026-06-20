# -*- coding: utf-8 -*-
"""
行情获取到入库的时间链路测试。

本测试覆盖：
    - xtdata 风格返回结构；
    - epoch 毫秒时间戳；
    - XtdataSource 标准化；
    - MarketDataIngestor 编排；
    - storage 落盘后的时间可解析、升序、无 UTC 墙上时间偏移。
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from core.fd_storage_adapter import FDStorageAdapter
from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
from core.xtdata_source import XtdataSource


class _FakeXtdata:
    """模拟 xtdata 的最小取数接口。"""

    def download_history_data(self, **kwargs) -> bool:
        """模拟历史下载成功。"""

        return True

    def get_market_data_ex(self, **kwargs) -> dict:
        """返回 code -> DataFrame 结构，time 使用 epoch 毫秒。"""

        symbol = kwargs["stock_list"][0]
        times = [
            int(pd.Timestamp("2026-01-14 09:30:00", tz="Asia/Shanghai").timestamp() * 1000),
            int(pd.Timestamp("2026-01-14 09:31:00", tz="Asia/Shanghai").timestamp() * 1000),
        ]
        return {
            symbol: pd.DataFrame(
                {
                    "time": times,
                    "open": [1.0, 1.1],
                    "high": [1.2, 1.3],
                    "low": [0.9, 1.0],
                    "close": [1.1, 1.2],
                    "volume": [100, 200],
                    "amount": [1000, 2000],
                }
            )
        }


class TestIngestTimeChain(unittest.TestCase):
    """校验从取数到落盘的时间戳端到端契约。"""

    def _run_chain(self, storage) -> pd.Series:
        """
        执行一次完整入库链路并返回落盘时间列。

        Args:
            storage: 兼容 MarketDataIngestor 的 storage 对象。

        Returns:
            pd.Series: 落盘后解析出的时间列。
        """

        source = XtdataSource(xtdata=_FakeXtdata(), download=True)
        ingestor = MarketDataIngestor(storage)
        out_path = ingestor.ingest_symbol(
            source=source,
            market="SS_stock_data",
            symbol="510050.SH",
            cycle="1m",
            specific="original",
            file_type="csv",
            time_column="time",
            merge=True,
        )
        loaded = pd.read_csv(out_path)
        parsed = pd.to_datetime(loaded["time"], errors="coerce")

        self.assertEqual(Path(out_path).parts[-3:], ("1min", "original", "510050.SH_1min.csv"))
        self.assertFalse(parsed.isna().any())
        self.assertTrue(parsed.is_monotonic_increasing)
        self.assertFalse(parsed.duplicated().any())
        return parsed

    def test_legacy_chain_writes_beijing_naive_time(self) -> None:
        """
        验证 legacy 后端完整链路不会写出 UTC 墙上时间。

        Returns:
            None: 通过断言验证行为。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            parsed = self._run_chain(FinancialDataStorage(root_dir=tmpdir))

        self.assertEqual(parsed.tolist(), [pd.Timestamp("2026-01-14 09:30:00"), pd.Timestamp("2026-01-14 09:31:00")])
        self.assertNotIn(pd.Timestamp("2026-01-14 01:30:00"), parsed.tolist())

    def test_fd_chain_writes_beijing_naive_time(self) -> None:
        """
        验证 FD 后端完整链路不会写出 UTC 墙上时间。

        Returns:
            None: 通过断言验证行为。
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            parsed = self._run_chain(FDStorageAdapter(root_dir=tmpdir))

        self.assertEqual(parsed.tolist(), [pd.Timestamp("2026-01-14 09:30:00"), pd.Timestamp("2026-01-14 09:31:00")])
        self.assertNotIn(pd.Timestamp("2026-01-14 01:30:00"), parsed.tolist())


if __name__ == "__main__":
    unittest.main()
