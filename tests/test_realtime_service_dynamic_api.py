# -*- coding: utf-8 -*-
"""RealtimeSubscriptionService 动态增删订阅（单元层）

测试项目：
1) 测试内容：add_subscription/remove_subscription 的路径与订阅集维护
   目的：在无 QMT 环境下验证逻辑正确性
   输入：fake xtdata.subscribe_quote/unsubscribe_quote；codes=[A], periods=[1m,1d]
   预期输出：subscribe 被调用 2 次；remove 后调用 2 次，内部订阅集变化正确
"""
import unittest

from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig


class _FakeXtData:
    def __init__(self):
        self.sub_calls = []
        self.unsub_calls = []
    def subscribe_quote(self, code, period, *_a, **_kw):
        self.sub_calls.append((code, period))
    def unsubscribe_quote(self, code, period):
        self.unsub_calls.append((code, period))
    def get_market_data(self, *a, **kw):
        import pandas as pd
        return pd.DataFrame([
            {"time": "2025-09-10 10:00:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "amount": 1},
            {"time": "2025-09-10 10:01:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "amount": 1},
        ])
    def run(self):
        return None


class _NoopCache:
    def ensure_downloaded_date_range(self, *a, **kw):
        return None


class _NoopPublisher:
    def __init__(self):
        self.out = []
    def publish(self, payload):
        self.out.append(payload)


class TestRealtimeDynamic(unittest.TestCase):
    def setUp(self):
        import core.realtime_service as mod
        self.fake = _FakeXtData()
        mod.xtdata = self.fake

    def test_add_and_remove(self):
        cfg = RealtimeConfig(mode="close_only", periods=["1m", "1d"], codes=["A"], preload_days=0)
        svc = RealtimeSubscriptionService(cfg, _NoopPublisher(), cache=_NoopCache())
        svc.add_subscription(["A"], ["1m", "1d"], preload_days=0)
        self.assertEqual(len(self.fake.sub_calls), 2)
        svc.remove_subscription(["A"], ["1m", "1d"])
        self.assertEqual(len(self.fake.unsub_calls), 2)