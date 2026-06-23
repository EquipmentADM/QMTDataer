# -*- coding: utf-8 -*-
import threading
import time
import unittest
from unittest import mock

from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService


class DummyPublisher:
    def __init__(self) -> None:
        self.payloads = []
        self._lock = threading.Lock()

    def publish(self, payload):
        with self._lock:
            self.payloads.append(payload)

    def count(self) -> int:
        with self._lock:
            return len(self.payloads)


class TestMockModeFeeder(unittest.TestCase):
    def test_mock_mode_generates_bars(self):
        publisher = DummyPublisher()
        mock_cfg = RealtimeConfig.MockConfig(
            enabled=True,
            base_price=100.0,
            volatility=0.001,
            step_seconds=0.05,
            seed=42,
            volume_mean=1000,
            volume_std=100,
            source="mock",
        )
        cfg = RealtimeConfig(
            mode="close_only",
            periods=["1m"],
            codes=["MOCK.SH"],
            preload_days=0,
            mock=mock_cfg,
        )
        with mock.patch("core.realtime_service.xtdata", None):
            svc = RealtimeSubscriptionService(cfg, publisher)

            worker = threading.Thread(target=svc.run_forever, daemon=True)
            worker.start()
            try:
                time.sleep(0.6)  # 等待 mock feeder 产出若干条
            finally:
                svc.stop()
                worker.join(timeout=2.0)

        self.assertTrue(publisher.payloads, "应当收到至少一条 mock bar")
        self.assertTrue(all(bar["code"] == "MOCK.SH" for bar in publisher.payloads))
        self.assertTrue(all(bar["period"] == "1m" for bar in publisher.payloads))
        self.assertTrue(all(bar.get("source") == "mock" for bar in publisher.payloads))

    def test_mock_blank_start_subscribe_and_unsubscribe(self):
        """测试内容：Mock 空启动后动态订阅与退订。

        目的：
            - 验证初始 codes=[] 时不会主动推送；
            - 验证运行中 add_subscription 后可生成 Mock 行情；
            - 验证 remove_subscription 后活跃订阅清空，后续不再继续推送该流。
        """
        publisher = DummyPublisher()
        mock_cfg = RealtimeConfig.MockConfig(
            enabled=True,
            base_price=100.0,
            volatility=0.001,
            step_seconds=0.05,
            seed=11,
            volume_mean=1000,
            volume_std=100,
            source="mock",
        )
        cfg = RealtimeConfig(
            mode="close_only",
            periods=["1m"],
            codes=[],
            preload_days=0,
            mock=mock_cfg,
        )

        with mock.patch("core.realtime_service.xtdata", None):
            svc = RealtimeSubscriptionService(cfg, publisher)
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

                # 给 feeder 一个周期消化退订，随后确认不再持续新增该流。
                time.sleep(0.12)
                count_after_remove = publisher.count()
                time.sleep(0.18)
                self.assertEqual(publisher.count(), count_after_remove)
            finally:
                svc.stop()
                worker.join(timeout=2.0)


if __name__ == "__main__":
    unittest.main()
