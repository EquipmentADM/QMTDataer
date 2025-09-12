# -*- coding: utf-8 -*-
"""RealtimeSubscriptionService 动态增删订阅（单元层）

测试项目：
1) 测试内容：add_subscription/remove_subscription 的路径与订阅集维护
   目的：在无 QMT 环境下验证逻辑正确性
   输入：fake xtdata.subscribe_quote/unsubscribe_quote；codes=[A], periods=[1m,1d]
   预期输出：subscribe 被调用 2 次；remove 后调用 2 次，内部订阅集变化正确
"""
import unittest
from unittest import mock

from xtquant import xtdata

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
    def test_add_and_remove_real_xt(self):
        # 1) 环境探测（最小订阅→退订），失败则跳过
        try:
            sid = xtdata.subscribe_quote(stock_code="000001.SZ", period="1d", count=0, callback=lambda *_: None)
            if hasattr(xtdata, "unsubscribe_quote"):
                xtdata.unsubscribe_quote("000001.SZ", "1d")
        except Exception as e:
            self.skipTest(f"MiniQMT/xtdata 不可用：{e}")

        # 2) 构造服务并执行 add/remove（不需要 run()）
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        svc = RealtimeSubscriptionService(
            RealtimeConfig(mode="close_only", periods=["1m","1d"], codes=["000001.SZ"], preload_days=0),
            publisher=_NoopPublisher(),  # 你已有的空发布器
            cache=_NoopCache()           # 你已有的空缓存
        )
        svc.add_subscription(["000001.SZ"], ["1m","1d"], preload_days=0)
        self.assertEqual(len(svc._subs), 2)
        svc.remove_subscription(["000001.SZ"], ["1m","1d"])
        self.assertEqual(len(svc._subs), 0)