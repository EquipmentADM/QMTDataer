# -*- coding: utf-8 -*-
from datetime import datetime
import threading
import time
import unittest
from unittest import mock

from core.realtime_service import CN_TZ, MockBarFeeder, RealtimeConfig, RealtimeSubscriptionService


class DummyPublisher:
    """测试用发布器：记录 RealtimeSubscriptionService 实际发布的 payload。"""

    def __init__(self) -> None:
        self.payloads = []
        self._lock = threading.Lock()

    def publish(self, payload):
        with self._lock:
            self.payloads.append(payload)

    def count(self) -> int:
        with self._lock:
            return len(self.payloads)


def _build_mock_service(codes=None, step_seconds: float = 0.05):
    """构造启用 Mock 的实时服务，避免测试触碰真实 xtdata。"""
    publisher = DummyPublisher()
    mock_cfg = RealtimeConfig.MockConfig(
        enabled=True,
        base_price=100.0,
        volatility=0.001,
        step_seconds=step_seconds,
        seed=42,
        volume_mean=1000,
        volume_std=100,
        source="mock",
    )
    cfg = RealtimeConfig(
        mode="close_only",
        periods=["1m"],
        codes=list(codes or []),
        preload_days=0,
        mock=mock_cfg,
    )
    svc = RealtimeSubscriptionService(cfg, publisher)
    return svc, publisher, mock_cfg


class TestMockModeFeeder(unittest.TestCase):
    def test_mock_mode_generates_bars(self):
        """验证 Mock 模式能产生基础行情 payload。"""
        svc, publisher, _ = _build_mock_service(codes=["MOCK.SH"])
        with mock.patch("core.realtime_service.xtdata", None):
            worker = threading.Thread(target=svc.run_forever, daemon=True)
            worker.start()
            try:
                time.sleep(0.6)
            finally:
                svc.stop()
                worker.join(timeout=2.0)

        self.assertTrue(publisher.payloads, "应当收到至少一条 mock bar")
        self.assertTrue(all(bar["code"] == "MOCK.SH" for bar in publisher.payloads))
        self.assertTrue(all(bar["period"] == "1m" for bar in publisher.payloads))
        self.assertTrue(all(bar.get("source") == "mock" for bar in publisher.payloads))

    def test_mock_blank_start_subscribe_and_unsubscribe(self):
        """验证 Mock 空启动后动态订阅与退订。"""
        svc, publisher, _ = _build_mock_service(codes=[])

        with mock.patch("core.realtime_service.xtdata", None):
            worker = threading.Thread(target=svc.run_forever, daemon=True)
            worker.start()
            try:
                time.sleep(0.15)
                self.assertEqual(publisher.count(), 0, "空启动不应主动推送行情")
                self.assertEqual(svc.status()["subs"], [])

                svc.add_subscription(["MOCK2.SH"], ["1m"], preload_days=0)
                deadline = time.time() + 1.5
                while publisher.count() == 0 and time.time() < deadline:
                    time.sleep(0.03)

                self.assertGreater(publisher.count(), 0, "动态订阅后应生成 Mock 行情")
                self.assertEqual(svc.status()["subs"], [{"code": "MOCK2.SH", "period": "1m", "ref_count": 1}])
                self.assertTrue(all(bar["code"] == "MOCK2.SH" for bar in publisher.payloads))
                self.assertTrue(all(bar.get("source") == "mock" for bar in publisher.payloads))

                svc.remove_subscription(["MOCK2.SH"], ["1m"])
                deadline = time.time() + 1.0
                while svc.status()["subs"] and time.time() < deadline:
                    time.sleep(0.03)
                self.assertEqual(svc.status()["subs"], [])

                time.sleep(0.12)
                count_after_remove = publisher.count()
                time.sleep(0.18)
                self.assertEqual(publisher.count(), count_after_remove)
            finally:
                svc.stop()
                worker.join(timeout=2.0)

    def test_cn_stock_minute_clock_boundaries(self):
        """验证 A 股 1m 模拟时钟按日内交易时段跳转。"""
        cases = [
            (datetime(2026, 1, 14, 9, 29, 30, tzinfo=CN_TZ), "2026-01-14T09:30:00"),
            (datetime(2026, 1, 14, 10, 12, 34, tzinfo=CN_TZ), "2026-01-14T10:13:00"),
            (datetime(2026, 1, 14, 11, 30, 0, tzinfo=CN_TZ), "2026-01-14T13:00:00"),
            (datetime(2026, 1, 14, 11, 45, 0, tzinfo=CN_TZ), "2026-01-14T13:00:00"),
            (datetime(2026, 1, 14, 15, 0, 0, tzinfo=CN_TZ), "2026-01-15T09:30:00"),
            (datetime(2026, 1, 14, 20, 0, 0, tzinfo=CN_TZ), "2026-01-15T09:30:00"),
        ]
        for source_dt, expected in cases:
            with self.subTest(source_dt=source_dt):
                actual = MockBarFeeder._ceil_cn_stock_minute(source_dt)
                self.assertEqual(actual.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S"), expected)

        self.assertEqual(
            MockBarFeeder._next_cn_stock_minute(datetime(2026, 1, 14, 11, 30, tzinfo=CN_TZ))
            .replace(tzinfo=None)
            .strftime("%Y-%m-%dT%H:%M:%S"),
            "2026-01-14T13:00:00",
        )

    def test_mock_1m_uses_global_clock_for_multiple_symbols(self):
        """验证同一轮 Mock 推送中多个标的共享同一个 1m 时间戳。"""
        svc, publisher, mock_cfg = _build_mock_service(codes=[])
        svc.add_subscription(["MOCK_A.SH", "MOCK_B.SH"], ["1m"], preload_days=0)
        feeder = MockBarFeeder(svc, mock_cfg)
        feeder._mock_clock_dt = datetime(2026, 1, 14, 10, 0, tzinfo=CN_TZ)

        with mock.patch("core.realtime_service.xtdata", None):
            feeder._emit_cycle()

        self.assertEqual(len(publisher.payloads), 2)
        published_times = {bar["bar_end_ts"] for bar in publisher.payloads}
        self.assertEqual(published_times, {"2026-01-14T10:00:00"})
        self.assertEqual(feeder._mock_clock_dt.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S"), "2026-01-14T10:01:00")

    def test_global_clock_prefers_latest_history_time(self):
        """验证全局模拟时钟优先使用订阅集合里的最大历史时间。"""
        svc, publisher, mock_cfg = _build_mock_service(codes=[])
        svc.add_subscription(["MOCK_A.SH", "MOCK_B.SH"], ["1m"], preload_days=0)
        feeder = MockBarFeeder(svc, mock_cfg)

        baselines = {
            ("MOCK_A.SH", "1m"): MockBarFeeder._HistoryBaseline(
                base_price=100.0,
                vol=0.001,
                latest_dt=datetime(2026, 1, 14, 10, 0, tzinfo=CN_TZ),
            ),
            ("MOCK_B.SH", "1m"): MockBarFeeder._HistoryBaseline(
                base_price=101.0,
                vol=0.001,
                latest_dt=datetime(2026, 1, 14, 11, 30, tzinfo=CN_TZ),
            ),
        }

        with mock.patch.object(feeder, "_history_baseline", side_effect=lambda code, period: baselines[(code, period)]):
            feeder._emit_cycle()

        self.assertEqual(len(publisher.payloads), 2)
        published_times = {bar["bar_end_ts"] for bar in publisher.payloads}
        self.assertEqual(published_times, {"2026-01-14T13:00:00"})
        self.assertEqual(feeder._mock_clock_dt.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S"), "2026-01-14T13:01:00")

    def test_late_subscription_aligns_to_existing_global_clock(self):
        """验证运行中新订阅标的直接对齐当前全局模拟时钟。"""
        svc, publisher, mock_cfg = _build_mock_service(codes=[])
        svc.add_subscription(["MOCK_A.SH", "MOCK_B.SH"], ["1m"], preload_days=0)
        feeder = MockBarFeeder(svc, mock_cfg)
        feeder._mock_clock_dt = datetime(2026, 1, 14, 10, 0, tzinfo=CN_TZ)

        with mock.patch("core.realtime_service.xtdata", None):
            feeder._emit_cycle()
            publisher.payloads.clear()

            svc.add_subscription(["MOCK_C.SH"], ["1m"], preload_days=0)
            feeder._emit_cycle()

        by_code = {bar["code"]: bar["bar_end_ts"] for bar in publisher.payloads}
        self.assertEqual(by_code["MOCK_A.SH"], "2026-01-14T10:01:00")
        self.assertEqual(by_code["MOCK_B.SH"], "2026-01-14T10:01:00")
        self.assertEqual(by_code["MOCK_C.SH"], "2026-01-14T10:01:00")

    def test_unsubscribe_keeps_global_clock_for_remaining_symbols(self):
        """验证退订一个标的不影响其他标的按全局时钟继续推进。"""
        svc, publisher, mock_cfg = _build_mock_service(codes=[])
        svc.add_subscription(["MOCK_A.SH", "MOCK_B.SH"], ["1m"], preload_days=0)
        feeder = MockBarFeeder(svc, mock_cfg)
        feeder._mock_clock_dt = datetime(2026, 1, 14, 10, 0, tzinfo=CN_TZ)

        with mock.patch("core.realtime_service.xtdata", None):
            feeder._emit_cycle()
            publisher.payloads.clear()

            svc.remove_subscription(["MOCK_B.SH"], ["1m"])
            feeder._emit_cycle()

            self.assertEqual([bar["code"] for bar in publisher.payloads], ["MOCK_A.SH"])
            self.assertEqual(publisher.payloads[0]["bar_end_ts"], "2026-01-14T10:01:00")

            svc.remove_subscription(["MOCK_A.SH"], ["1m"])
            feeder._emit_cycle()
            count_after_all_removed = publisher.count()

            svc.add_subscription(["MOCK_C.SH"], ["1m"], preload_days=0)
            feeder._emit_cycle()

        self.assertEqual(count_after_all_removed, 1)
        self.assertEqual(publisher.payloads[-1]["code"], "MOCK_C.SH")
        self.assertEqual(publisher.payloads[-1]["bar_end_ts"], "2026-01-14T10:02:00")


if __name__ == "__main__":
    unittest.main()
