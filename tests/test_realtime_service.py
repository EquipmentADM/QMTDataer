"""RealtimeSubscriptionService 单元测试（M2.5 版，修订）

修订点：
    - 修复偶发 AttributeError: module 'xtquant' has no attribute 'xtdata'，在假包安装时把子模块绑定到顶层属性；
    - 为避免模块缓存导致用旧依赖，reload 改为“先从 sys.modules 移除后再 import”；
    - 其余逻辑与先前一致。

类说明：
    - 覆盖订阅前预热（preload）、订阅注册、close-only 发布、forming_and_close 发布、幂等去重、异常兜底；
    - 上游：无；
    - 下游：被测对象 core.realtime_service.RealtimeSubscriptionService。

注意：通过注入假 xtquant.xtdata（subscribe_quote/get_market_data/run）。
"""
import sys
import types
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
import pandas as pd


CN_TZ = timezone(timedelta(hours=8))
ISO = "%Y-%m-%dT%H:%M:%S%z"


class _FakePublisher:
    """简易发布器：收集发布消息"""
    def __init__(self):
        self.messages = []
    def publish(self, msg):
        self.messages.append(msg)


class _FakeCache:
    """简易 LocalCache：记录 ensure_downloaded 调用"""
    def __init__(self):
        self.calls = []
    def ensure_downloaded_date_range(self, codes, period, s, e, incrementally=True):
        self.calls.append((tuple(codes), period, s, e, incrementally))


def _install_fake_xtdata():
    """安装假的 xtquant.xtdata，并把子模块挂到顶层 xtquant 属性，避免 AttributeError。"""
    xtquant = types.ModuleType("xtquant")
    xtdata = types.ModuleType("xtquant.xtdata")

    def subscribe_quote(stock_code, period, count, callback):
        # 仅记录，测试中不直接触发；_on_event 由测试手动调用
        return None

    def run():
        # 不阻塞，供 run_forever 调用
        return None

    def get_market_data(stock_code, period, count=2, fill_data=True):
        # 默认生成两条已收盘 bar（方便 close-only 测试）
        now = datetime.now(CN_TZ)
        t1 = (now - timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")
        t2 = (now - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        return pd.DataFrame({
            "time": [t1, t2],
            "open": [1.0, 1.1],
            "high": [1.2, 1.2],
            "low":  [0.9, 1.0],
            "close": [1.05, 1.08],
            "volume": [100.0, 110.0],
            "amount": [1000.0, 1100.0],
        })

    xtdata.subscribe_quote = subscribe_quote
    xtdata.run = run
    xtdata.get_market_data = get_market_data

    # 将子模块绑定到顶层属性，便于 mock.patch("xtquant.xtdata.run") 正常定位
    xtquant.xtdata = xtdata

    sys.modules["xtquant"] = xtquant
    sys.modules["xtquant.xtdata"] = xtdata


def _reload_realtime_fresh():
    sys.modules.pop("core.realtime_service", None)
    import core.realtime_service  # noqa


class TestRealtimeService(unittest.TestCase):
    """类说明：实时订阅服务测试（M2.5 修订）
    功能：预热、订阅注册、发布逻辑、去重与异常路径。
    上游：无。
    下游：RealtimeSubscriptionService。
    """

    def _make_df(self, end_dt: datetime, period: str = "1m", n: int = 2):
        delta = {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}[period]
        times = [end_dt - i * delta for i in range(n)][::-1]
        return pd.DataFrame({
            "time": [t.strftime("%Y-%m-%d %H:%M:%S") for t in times],
            "open": [1.0 + i * 0.01 for i in range(n)],
            "high": [1.2] * n,
            "low":  [0.9] * n,
            "close": [1.05] * n,
            "volume": [100.0] * n,
            "amount": [1000.0] * n,
        })

    def test_preload_and_registration(self):
        """测试内容：订阅前预热 + 订阅注册 + run()
        目的：验证 ensure_downloaded_date_range 被调用、subscribe_quote 注册次数、run 执行；
        输入：codes=2，periods=2，preload_days=3；
        预期输出：cache.calls 按 periods 记录 2 次；subscribe_quote 调用 4 次；run 调用 1 次。
        """
        _install_fake_xtdata()
        _reload_realtime_fresh()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        cache = _FakeCache()
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m", "1d"], codes=["000001.SZ", "600000.SH"], preload_days=3)
        svc = RealtimeSubscriptionService(cfg, pub, cache=cache)
        with mock.patch("xtquant.xtdata.run") as mrun, mock.patch("xtquant.xtdata.subscribe_quote") as msub:
            svc.run_forever()
            # 预热按 period 调用 2 次（codes 作为整体传入）
            self.assertEqual(len(cache.calls), 2)
            self.assertEqual(msub.call_count, 4)
            self.assertEqual(mrun.call_count, 1)

    def test_close_only_publish_and_idempotent(self):
        """测试内容：close-only 模式发布 + 幂等去重
        目的：事件回调触发一次发布，再触发不应重复发布；
        输入：get_market_data 返回两根已收盘的 1m K；close_delay_ms=0；
        预期输出：第一次 _on_event 发布 1 条 is_closed=True；第二次不再发布（总计仍为 1 条）。
        """
        _install_fake_xtdata()
        _reload_realtime_fresh()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 1)
        self.assertTrue(pub.messages[0]["is_closed"])  # 收盘
        last_ts = pub.messages[0]["bar_end_ts"]
        svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 1)
        self.assertEqual(svc._last_published[("000001.SZ", "1m")], last_ts)

    def test_forming_and_close_dual_publish(self):
        """测试内容：forming_and_close 模式双发布
        目的：先发布 forming（is_closed=False），再发布 close（is_closed=True）；
        输入：close_delay_ms=0；get_market_data 返回最后一根已到时；
        预期输出：两条消息，第一条 is_closed=False，第二条 is_closed=True。
        """
        _install_fake_xtdata()
        _reload_realtime_fresh()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="forming_and_close", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 2)
        self.assertFalse(pub.messages[0]["is_closed"])  # forming
        self.assertTrue(pub.messages[1]["is_closed"])   # close

    def test_get_market_data_exception(self):
        """测试内容：拉取异常时不崩溃
        目的：保障回调健壮性；
        输入：get_market_data 抛异常；
        预期输出：不发布任何消息。
        """
        _install_fake_xtdata()
        _reload_realtime_fresh()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], close_delay_ms=0)
        svc = RealtimeSubscriptionService(cfg, pub)
        with mock.patch("xtquant.xtdata.get_market_data", side_effect=RuntimeError("boom")):
            svc._on_event("000001.SZ", "1m")
        self.assertEqual(len(pub.messages), 0)

    def test_preload_days_zero_disable_cache(self):
        """测试内容：preload_days=0 时不触发补齐
        目的：验证 _preload_cache 分支；
        输入：preload_days=0；
        预期输出：cache.calls 为空，subscribe_quote 与 run 仍被调用。
        """
        _install_fake_xtdata()
        _reload_realtime_fresh()
        from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
        cache = _FakeCache()
        pub = _FakePublisher()
        cfg = RealtimeConfig(mode="close_only", periods=["1m"], codes=["000001.SZ"], preload_days=0)
        svc = RealtimeSubscriptionService(cfg, pub, cache=cache)
        with mock.patch("xtquant.xtdata.run") as mrun, mock.patch("xtquant.xtdata.subscribe_quote") as msub:
            svc.run_forever()
            self.assertEqual(len(cache.calls), 0)
            self.assertEqual(msub.call_count, 1)
            self.assertEqual(mrun.call_count, 1)
