# -*- coding: utf-8 -*-
import threading
import time
import unittest

from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService


class DummyPublisher:
    def __init__(self) -> None:
        self.payloads = []
        self._lock = threading.Lock()

    def publish(self, payload):
        with self._lock:
            self.payloads.append(payload)


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


if __name__ == "__main__":
    unittest.main()
