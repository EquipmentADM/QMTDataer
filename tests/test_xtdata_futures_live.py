# -*- coding: utf-8 -*-
"""
xtdata 期货真实环境集成测试。

使用说明：
    - 本测试依赖本机已启动并登录的 MiniQMT / xtdata 环境；
    - 本测试会调用 `download_history_data` 与 `get_market_data_ex`；
    - 该调用会更新 MiniQMT 的本地行情缓存；
    - 本测试不会写入 FD 数据库目录，不会改动 `D:/Work/Quant/financial_database` 下的文件。

适用场景：
    - 验证当前环境下常用期货主连代码是否可拉取历史数据；
    - 验证 QMTDataer 中的“xtdata 代码 -> FD 目标对象”映射是否与真实接口行为一致。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.xtdata_futures import parse_xt_futures_code
from core.xtdata_source import XtdataSource


LIVE_TARGETS = (
    ("rb00.SF", "主力连续"),
    ("au00.SF", "主力连续"),
    ("IF00.IF", "主力连续"),
)


class TestXtdataFuturesLive(unittest.TestCase):
    """真实 xtdata 环境下的期货历史拉取测试。"""

    @classmethod
    def setUpClass(cls) -> None:
        """导入 xtdata；若环境不可用则整组跳过。"""
        try:
            from xtquant import xtdata  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise unittest.SkipTest(f"当前环境无法导入 xtdata: {exc}")
        cls.xtdata = xtdata

    def test_live_daily_fetch_for_common_futures(self) -> None:
        """
        检测 3 个常用品种主连历史是否可正常拉取。

        断言：
            - 代码映射为 `Futures_data / 品种 / 主力连续`
            - 拉取结果非空
            - 标准化后包含 time/open/high/low/close/volume/amount
        """
        source = XtdataSource(xtdata=self.xtdata, download=True)

        for code, expected_specific in LIVE_TARGETS:
            with self.subTest(code=code):
                target = parse_xt_futures_code(code)
                self.assertEqual(target.market, "Futures_data")
                self.assertEqual(target.specific, expected_specific)

                df = source.fetch(
                    symbol=code,
                    cycle="1d",
                    market=target.market,
                    specific=target.specific,
                    start="20260301",
                    end="20260408",
                )

                self.assertFalse(df.empty, msg=f"{code} 未拉到任何历史数据")
                self.assertTrue(
                    {"time", "open", "high", "low", "close", "volume", "amount"}.issubset(df.columns),
                    msg=f"{code} 标准化结果缺少核心列",
                )
                self.assertGreaterEqual(len(df), 5, msg=f"{code} 拉到的有效日线数量过少")
                self.assertEqual(df["code"].iloc[0], code)


if __name__ == "__main__":
    unittest.main()
