# -*- coding: utf-8 -*-
"""
legacy storage 时间解析测试。

默认后端已切到 FD，但 legacy 后端仍作为回滚路径保留，
因此其时间解析必须与 FD 后端保持同一语义。
"""
from __future__ import annotations

import unittest

import pandas as pd

from core.storage_simple import FinancialDataStorage


class TestStorageSimpleTime(unittest.TestCase):
    """校验 legacy storage 的时间解析契约。"""

    def test_epoch_millisecond_uses_beijing_wall_time(self) -> None:
        """
        验证 epoch 毫秒不会被解析为 UTC 墙上时间。

        Returns:
            None: 通过断言验证行为。
        """

        epoch_ms = int(pd.Timestamp("2026-01-14 15:00:00", tz="Asia/Shanghai").timestamp() * 1000)

        parsed = FinancialDataStorage._parse_time_series(pd.Series([epoch_ms]))

        self.assertEqual(parsed.iloc[0], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertNotEqual(parsed.iloc[0], pd.Timestamp("2026-01-14 07:00:00"))


if __name__ == "__main__":
    unittest.main()
