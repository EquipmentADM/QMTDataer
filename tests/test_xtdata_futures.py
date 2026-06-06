# -*- coding: utf-8 -*-
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ingest_runner import (
    DEFAULT_FUTURES_SYMBOLS,
    DEFAULT_SYMBOLS_RECENT,
    _build_targets,
    build_profile,
)
from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
from core.xtdata_futures import build_xt_futures_ingest_params, parse_xt_futures_code
from core.xtdata_source import MappedXtdataSource, XtdataSource


def _fake_code_df_xtdata():
    """构造一个返回 code->DataFrame 结构的 xtdata 替身。"""

    calls = {"download": [], "get": []}

    def download_history_data(stock_code, period, start_time, end_time, incrementally=True):
        calls["download"].append(
            {
                "stock_code": stock_code,
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "incrementally": incrementally,
            }
        )
        return True

    def get_market_data_ex(**kwargs):
        calls["get"].append(kwargs)
        code = kwargs["stock_list"][0]
        df = pd.DataFrame(
            {
                "time": ["20260401000000", "20260402000000"],
                "open": [3200.0, 3210.0],
                "high": [3220.0, 3230.0],
                "low": [3180.0, 3190.0],
                "close": [3215.0, 3205.0],
                "volume": [1000, 1200],
                "amount": [10000.0, 12000.0],
            }
        )
        return {code: df}

    def get_market_data(**kwargs):
        return get_market_data_ex(**kwargs)

    xt = SimpleNamespace(
        download_history_data=download_history_data,
        get_market_data_ex=get_market_data_ex,
        get_market_data=get_market_data,
        _calls=calls,
    )
    return xt


class TestXtdataFuturesMapping(unittest.TestCase):
    def test_parse_main_continuous(self):
        target = parse_xt_futures_code("rb00.SF")
        self.assertEqual(target.market, "Futures_data")
        self.assertEqual(target.symbol, "rb")
        self.assertEqual(target.specific, "主力连续")

    def test_parse_sub_continuous(self):
        target = parse_xt_futures_code("rb01.SF")
        self.assertEqual(target.symbol, "rb")
        self.assertEqual(target.specific, "次主力连续")

    def test_parse_888_and_contract(self):
        target_888 = parse_xt_futures_code("rb888.SF")
        target_contract = parse_xt_futures_code("rb2505.SF")
        self.assertEqual(target_888.specific, "888")
        self.assertEqual(target_contract.specific, "rb2505")

    def test_build_ingest_params(self):
        params = build_xt_futures_ingest_params("rb00.SF")
        self.assertEqual(
            params,
            {"market": "Futures_data", "symbol": "rb", "specific": "主力连续"},
        )


class TestIngestRunnerFutures(unittest.TestCase):
    def test_recent_profile_uses_recent_pool(self):
        profile = build_profile("recent-backfill")
        self.assertEqual(profile.symbols, DEFAULT_SYMBOLS_RECENT)

    def test_default_futures_pool_non_empty(self):
        self.assertTrue(DEFAULT_FUTURES_SYMBOLS)
        self.assertIn("rb00.SF", DEFAULT_FUTURES_SYMBOLS)
        self.assertIn("IF00.IF", DEFAULT_FUTURES_SYMBOLS)

    def test_build_targets_mixed_stock_and_futures(self):
        profile = build_profile(
            "full-backfill",
            symbols=("510050.SH", "rb00.SF"),
            cycles=("1d",),
        )
        targets = _build_targets(profile)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].fetch_symbol, "510050.SH")
        self.assertEqual(targets[0].storage_symbol, "510050.SH")
        self.assertEqual(targets[1].fetch_symbol, "rb00.SF")
        self.assertEqual(targets[1].storage_symbol, "rb")
        self.assertEqual(targets[1].market, "Futures_data")
        self.assertEqual(targets[1].specific, "主力连续")

    def test_mapped_source_ingest_to_futures_path(self):
        fake_xt = _fake_code_df_xtdata()
        inner = XtdataSource(xtdata=fake_xt, download=True)
        source = MappedXtdataSource(inner=inner, fetch_symbol="rb00.SF")

        with tempfile.TemporaryDirectory() as tmpdir:
            storage = FinancialDataStorage(root_dir=str(tmpdir))
            ingestor = MarketDataIngestor(storage)
            out_path = ingestor.ingest_symbol(
                source=source,
                market="Futures_data",
                symbol="rb",
                cycle="1d",
                specific="主力连续",
                file_type="csv",
            )

            out_file = Path(out_path)
            self.assertTrue(out_file.exists())
            self.assertIn(str(Path("Futures_data") / "rb" / "1d" / "主力连续"), str(out_file))
            self.assertEqual(out_file.name, "rb主力连续合成.csv")

            saved = pd.read_csv(out_file)
            self.assertFalse(saved.empty)
            self.assertEqual(saved["time"].iloc[0], "2026-04-01T00:00:00")
            self.assertEqual(fake_xt._calls["download"][0]["stock_code"], "rb00.SF")
            self.assertEqual(fake_xt._calls["get"][0]["stock_list"], ["rb00.SF"])


if __name__ == "__main__":
    unittest.main()
