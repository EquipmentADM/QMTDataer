# -*- coding: utf-8 -*-
"""
实时行情 schema guard 测试。
"""
from __future__ import annotations

import unittest

from core.schema_guard import validate_bar_payload


def _payload(bar_end_ts: str) -> dict:
    """构造最小实时行情 payload。"""

    return {
        "code": "510050.SH",
        "period": "1m",
        "bar_end_ts": bar_end_ts,
        "is_closed": True,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
    }


class TestSchemaGuard(unittest.TestCase):
    """校验实时行情时间戳契约。"""

    def test_accept_local_naive_iso(self) -> None:
        """
        验证当前 Redis 实时行情契约允许本地无时区 ISO。

        Returns:
            None: 通过断言验证行为。
        """

        ok, reason = validate_bar_payload(_payload("2026-01-14T15:00:00"), mode="close_only")

        self.assertTrue(ok, reason)

    def test_reject_timezone_timestamp(self) -> None:
        """
        验证带时区时间不再符合当前 Redis 实时行情契约。

        Returns:
            None: 通过断言验证行为。
        """

        ok, reason = validate_bar_payload(_payload("2026-01-14T15:00:00+08:00"), mode="close_only")

        self.assertFalse(ok)
        self.assertIn("本地无时区", reason)


if __name__ == "__main__":
    unittest.main()
