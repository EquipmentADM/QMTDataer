# -*- coding: utf-8 -*-
"""
实时订阅引用计数测试。

Responsibilities:
    - 验证同一 `(code, period)` 重复订阅不会重复注册底层行情。
    - 验证退订只在引用计数归零时取消底层行情。

Internal Dependencies:
    - core.realtime_service.RealtimeSubscriptionService

External Systems:
    - None，测试通过 fake xtdata 隔离 MiniQMT 依赖。
"""
from __future__ import annotations

import unittest
from unittest import mock

from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService


class _FakePublisher:
    """
    收集发布消息的空发布器。
    """

    def __init__(self) -> None:
        self.messages = []

    def publish(self, payload):
        """记录发布内容。

        Args:
            payload (dict): 待发布行情消息。

        Returns:
            None
        """
        self.messages.append(payload)


class _FakeXtdata:
    """
    记录订阅与退订调用的 xtdata 替身。
    """

    def __init__(self) -> None:
        self.subscribe_calls = []
        self.unsubscribe_calls = []
        self._next_sub_id = 100

    def subscribe_quote(self, **kwargs):
        """记录底层订阅调用。

        Args:
            **kwargs: `xtdata.subscribe_quote` 的关键字参数。

        Returns:
            int: 模拟 xtdata 返回的订阅 ID。
        """
        self.subscribe_calls.append(kwargs)
        self._next_sub_id += 1
        return self._next_sub_id

    def unsubscribe_quote(self, sub_id):
        """记录底层退订调用。

        Args:
            sub_id (int): 订阅 ID。

        Returns:
            None
        """
        self.unsubscribe_calls.append(sub_id)


class TestRealtimeSubscriptionRefs(unittest.TestCase):
    """
    实时订阅引用计数测试集。
    """

    def test_duplicate_subscription_uses_ref_count(self) -> None:
        """校验重复订阅只增加引用，不重复调用底层订阅。

        Returns:
            None
        """
        fake_xtdata = _FakeXtdata()
        with mock.patch("core.realtime_service.xtdata", fake_xtdata):
            svc = RealtimeSubscriptionService(
                RealtimeConfig(mode="close_only", periods=["1m"], codes=[], preload_days=0),
                publisher=_FakePublisher(),
            )
            svc.add_subscription(["510050.SH"], ["1m"], preload_days=0)
            svc.add_subscription(["510050.SH"], ["1m"], preload_days=0)

            self.assertEqual(len(fake_xtdata.subscribe_calls), 1)
            self.assertEqual(svc._sub_ref_counts[("510050.SH", "1m")], 2)
            self.assertEqual(svc.status()["subs"][0]["ref_count"], 2)

            svc.remove_subscription(["510050.SH"], ["1m"])
            self.assertEqual(len(fake_xtdata.unsubscribe_calls), 0)
            self.assertIn(("510050.SH", "1m"), svc._subs)
            self.assertEqual(svc._sub_ref_counts[("510050.SH", "1m")], 1)

            svc.remove_subscription(["510050.SH"], ["1m"])
            self.assertEqual(len(fake_xtdata.unsubscribe_calls), 1)
            self.assertEqual(fake_xtdata.unsubscribe_calls[0], 101)
            self.assertNotIn(("510050.SH", "1m"), svc._subs)
            self.assertNotIn(("510050.SH", "1m"), svc._sub_ref_counts)
            self.assertEqual(svc.status()["subs"], [])


if __name__ == "__main__":
    unittest.main()
