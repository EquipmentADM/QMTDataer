# -*- coding: utf-8 -*-
"""
时间标准化工具测试。

这些测试用于防止 xtdata epoch 时间戳被误解析为 UTC 墙上时间，
从而再次生成 01:30/07:00 与 09:30/15:00 混入的分钟线文件。
"""
from __future__ import annotations

import unittest

import pandas as pd

from core.time_utils import parse_local_naive_time_series


class TestTimeUtils(unittest.TestCase):
    """校验 QMTD 入库时间解析契约。"""

    def test_epoch_millisecond_converts_to_beijing_naive(self) -> None:
        """
        验证 epoch 毫秒按 UTC 绝对时间转为北京时间无时区。

        Returns:
            None: 通过断言验证行为。
        """

        epoch_ms = int(pd.Timestamp("2026-01-14 15:00:00", tz="Asia/Shanghai").timestamp() * 1000)

        parsed = parse_local_naive_time_series(pd.Series([epoch_ms]))

        self.assertEqual(parsed.iloc[0], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertNotEqual(parsed.iloc[0], pd.Timestamp("2026-01-14 07:00:00"))

    def test_epoch_string_and_local_business_strings(self) -> None:
        """
        验证数值字符串、业务时间字符串和带时区字符串都转为本地无时区。

        Returns:
            None: 通过断言验证行为。
        """

        epoch_sec = int(pd.Timestamp("2026-01-14 09:30:00", tz="Asia/Shanghai").timestamp())
        values = pd.Series(
            [
                str(epoch_sec),
                "20260114150000",
                "2026-01-14T07:00:00Z",
                "2026-01-14T15:00:00+08:00",
                "2026-01-14 15:00:00",
            ]
        )

        parsed = parse_local_naive_time_series(values)

        self.assertEqual(parsed.iloc[0], pd.Timestamp("2026-01-14 09:30:00"))
        self.assertEqual(parsed.iloc[1], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertEqual(parsed.iloc[2], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertEqual(parsed.iloc[3], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertEqual(parsed.iloc[4], pd.Timestamp("2026-01-14 15:00:00"))
        self.assertFalse(parsed.isna().any())


if __name__ == "__main__":
    unittest.main()
