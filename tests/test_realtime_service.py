"""RealtimeSubscriptionService 单元测试

类说明：
    - 覆盖订阅注册、回调拉取、close-only 与 forming 发布、去重与异常路径；
    - 上游：无；
    - 下游：被测对象 core.realtime_service.RealtimeSubscriptionService。
"""
import unittest
import sys
import types
import importlib
from datetime import datetime, timedelta, timezone
from unittest import mock
import pandas as pd


class _FakePublisher:
    """简易发布器，用于捕获发布的消息"""
    def __init__(self):
        self.messages = []
    def publish(self, message):
        self.messages.append(message)


def _install_fake_xtdata_for_realtime(df_tail: pd.DataFrame):
    """安装假的 xtdata：subscribe_quote 记录回调；get_market_data 返回指定 df。"""
    xtquant = types.ModuleType("xtquant")
    xtdata = types.ModuleType("xtquant.xtdata")

    callbacks = []

    def subscribe_quote(stock_code, period, count, callback):
        callbacks.append((stock_code, period, callback))

    def get_market_data(stock_code, period, count=None, fill_data=True):
        return df_tail

    def run():
        # 不阻塞；测试中不需要真正的事件循环
        return None

    xtdata.subscribe_quote = subscribe_quote
    xtdata.get_market_data = get_market_data
    xtdata.run = run

    sys.modules["xtquant"] = xtquant
    sys.modules["xtquant.xtdata"] = xtdata
    return callbacks


def _reload_realtime():
    if "core.realtime_service" in sys.modules:
        importlib.reload(sys.modules["core.realtime_service"])
    else:
        import core.realtime_service  # noqa


class TestRealtimeService(unittest.TestCase):
    """类说明：实时订阅服务测试
    功能：订阅注册、收盘判定与发布、forming 模式、去重与异常。
    上游：无。
    下游：RealtimeSubscriptionService。
    """

    def _make_df(self, end_dt: datetime, period: str = "1m", n: int = 2):
        tz = timezone(timedelta(hours=8))
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        times = [end_dt - i * delta for i in range(n)][::-1]
        return pd.DataFrame({
            "time": [t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
            "open": [1.0] * n,
            "high": [2.0] * n,
            "low": [0.5] * n,
            "close": [1.5] * n,
            "volume": [100.0] * n,
            "amount": [1000.0] * n,
        })

    def test_subscribe_and_run(self):
        """测试内容：订阅注册与主循环
        目的：验证 subscribe_quote 被调用 N 次且 run() 被触发。
        输入：codes=2, periods=2。
        预期输出：subscribe_quote 调用 4 次，xtdata.run 调用 1 次。
        """
        end = datetime(2025, 1, 1, 10, 0, tzinfo=timezone(timedelta(hours=8)))
        df = self._make_df(end)
        callbacks = _install_fake_xtdata_for_realtime(df)
        _reload_realtime()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m", "1d"], codes=["000001.SZ", "600000.SH"])
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.run") as mrun, mock.patch("xtquant.xtdata.subscribe_quote") as msub:
            svc.run_forever()
            self.assertEqual(msub.call_count, 4)
            self.assertEqual(mrun.call_count, 1)

    def test_close_only_publish(self):
        """测试内容：close-only 模式按收盘发布
        目的：当前时间超过 bar_end+delay 才发布。
        输入：构造末尾 bar_end 在 2 秒之前，close_delay_ms=100。
        预期输出：发布 1 条，is_closed=True。
        """
        now = datetime.now(timezone(timedelta(hours=8)))
        df = self._make_df(now - timedelta(seconds=2), n=2)
        _install_fake_xtdata_for_realtime(df)
        _reload_realtime()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], close_delay_ms=100)
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.get_market_data", return_value=df):
            svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 1)
        self.assertTrue(pub.messages[0]["is_closed"])  # 预期收盘

    def test_forming_and_close_publish(self):
        """测试内容：forming_and_close 模式双发布
        目的：先发布 forming（is_closed=False），后发布 close（is_closed=True）。
        输入：同一末尾 bar，模拟两次事件；close_delay_ms=0。
        预期输出：发布 2 条，先 False 后 True。
        """
        now = datetime.now(timezone(timedelta(hours=8)))
        df = self._make_df(now - timedelta(seconds=1), n=1)
        _install_fake_xtdata_for_realtime(df)
        _reload_realtime()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="forming_and_close", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.get_market_data", return_value=df):
            svc._on_event("000001.SZ", "1m")  # forming
            svc._on_event("000001.SZ", "1m")  # close（因为 delay=0）
        self.assertEqual(len(pub.messages), 2)
        self.assertFalse(pub.messages[0]["is_closed"])  # 第一次 forming
        self.assertTrue(pub.messages[1]["is_closed"])   # 第二次 close

    def test_idempotent_no_duplicate_publish(self):
        """测试内容：同一 bar_end_ts 不重复发布
        目的：验证幂等去重。
        输入：两次回调返回相同末尾 bar。
        预期输出：只发布一次。
        """
        now = datetime.now(timezone(timedelta(hours=8)))
        df = self._make_df(now - timedelta(seconds=2), n=2)
        _install_fake_xtdata_for_realtime(df)
        _reload_realtime()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.get_market_data", return_value=df):
            svc._on_event("000001.SZ", "1m")
            svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 1)

    def test_exception_in_get_market_data(self):
        """测试内容：拉取异常时不崩溃
        目的：保障回调的健壮性。
        输入：get_market_data 抛异常。
        预期输出：不发布消息，函数正常返回。
        """
        _install_fake_xtdata_for_realtime(pd.DataFrame())
        _reload_realtime()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.get_market_data", side_effect=RuntimeError("boom")):
            svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 0)
