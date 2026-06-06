# -*- coding: utf-8 -*-
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.xtdata_source import XtdataSource
from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
import unittest


def _fake_xtdata():
    """构造一个简易 xtdata 替身，模拟 download 与 get 返回结构"""

    def download_history_data(stock_code, period, start_time, end_time, incrementally=True):
        return True

    def get_market_data_ex(**kwargs):
        # 构造 {field: DataFrame} 结构，time 为两条记录
        import pandas as pd
        idx = pd.Index(kwargs["stock_list"], name="code")
        time_df = pd.DataFrame([[1690000000, 1690003600]], index=idx)
        open_df = pd.DataFrame([[1.0, 1.1]], index=idx)
        high_df = pd.DataFrame([[1.2, 1.2]], index=idx)
        low_df = pd.DataFrame([[0.9, 1.0]], index=idx)
        close_df = pd.DataFrame([[1.05, 1.15]], index=idx)
        vol_df = pd.DataFrame([[100, 120]], index=idx)
        amt_df = pd.DataFrame([[1000, 1200]], index=idx)
        return {
            "time": time_df,
            "open": open_df,
            "high": high_df,
            "low": low_df,
            "close": close_df,
            "volume": vol_df,
            "amount": amt_df,
        }

    def get_market_data(**kwargs):
        return get_market_data_ex(**kwargs)

    return SimpleNamespace(
        download_history_data=download_history_data,
        get_market_data_ex=get_market_data_ex,
        get_market_data=get_market_data,
    )


class TestXtdataSource(unittest.TestCase):
    def test_fetch_and_ingest(self):
        fake_xt = _fake_xtdata()
        source = XtdataSource(xtdata=fake_xt, download=True)
        df = source.fetch(symbol="MOCK.SH", cycle="1m", market="SS_stock_data", specific="original")
        self.assertFalse(df.empty)
        self.assertTrue(set(["time", "open", "high", "low", "close", "volume", "amount"]).issubset(df.columns))
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialDataStorage(root_dir=str(tmpdir))
            ingestor = MarketDataIngestor(storage)
            out_path = ingestor.ingest_symbol(
                source=source,
                market="SS_stock_data",
                symbol="MOCK.SH",
                cycle="1m",
                specific="original",
                start=None,
                end=None,
                file_type="csv",
            )
            self.assertTrue(Path(out_path).exists())
            saved = pd.read_csv(out_path)
            self.assertFalse(saved.empty)


if __name__ == "__main__":
    unittest.main()
