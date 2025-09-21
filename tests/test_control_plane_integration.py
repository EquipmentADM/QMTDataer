# -*- coding: utf-8 -*-
"""ControlPlane integration test (real Redis; realtime service replaced with fake).

Scenario:
    1. send subscribe command -> expect ACK + registry persist + fake service add_subscription called;
    2. send status command -> expect ACK containing status and registry listing;
    3. send unsubscribe (by sub_id) -> expect ACK + remove_subscription invoked + registry cleaned.

Notes:
    - Preload is explicitly set to 0 to avoid waiting for LocalCache downloads during tests.
    - Requires a reachable Redis instance (see tests/_helpers.py for connection parameters).
"""
import json
import time
import unittest

import redis as redislib

from tests._helpers import redis_params_from_env, random_suffix
from core.control_plane import ControlPlane
from core.registry import Registry


class _FakeService:
    """Lightweight realtime service stub used for integration tests."""

    def __init__(self) -> None:
        self.add_calls = []
        self.remove_calls = []
        self.cfg = type("Cfg", (), {"mode": "close_only", "preload_days": 0})
        self.publisher = type("Pub", (), {"topic": "xt:topic:bar"})
        self._subs = set()

    def add_subscription(self, codes, periods, preload_days=None):
        self.add_calls.append((tuple(codes), tuple(periods), preload_days))
        for code in codes:
            for period in periods:
                self._subs.add((code, period))

    def remove_subscription(self, codes, periods):
        self.remove_calls.append((tuple(codes), tuple(periods)))
        for code in codes:
            for period in periods:
                self._subs.discard((code, period))

    def status(self):
        subs = sorted([
            {"code": code, "period": period} for (code, period) in self._subs
        ], key=lambda item: (item["code"], item["period"]))
        return {"subs": subs, "last_published": {}}


class TestControlPlaneIntegration(unittest.TestCase):
    def setUp(self) -> None:
        p = redis_params_from_env()
        self.cli = redislib.from_url(p["url"], decode_responses=True)
        self.channel = f"xt:ctrl:sub:test:{random_suffix()}"
        self.ack_prefix = f"xt:ctrl:ack:test:{random_suffix()}"
        self.reg_prefix = f"xt:bridge:reg:{random_suffix()}"
        self.strategy = "demo"
        self.ack_ch = f"{self.ack_prefix}:{self.strategy}"

        self.ps = self.cli.pubsub()
        self.ps.subscribe(self.ack_ch)
        while self.ps.get_message(timeout=0.01):
            pass

        self.svc = _FakeService()
        self.cp = ControlPlane(
            host=p["host"], port=p["port"], password=p["password"], db=p["db"],
            channel=self.channel, ack_prefix=self.ack_prefix,
            registry_prefix=self.reg_prefix, svc=self.svc
        )
        self.cp.start()

        self.registry = Registry(p["host"], p["port"], p["password"], p["db"], prefix=self.reg_prefix)

        for _ in range(60):  # wait up to 3 seconds
            if getattr(self.cp, "_pubsub", None) is not None:
                break
            time.sleep(0.05)
        else:
            self.fail("ControlPlane did not subscribe to control channel in time")

    def tearDown(self) -> None:
        try:
            self.cp.stop()
            self.cp.join(timeout=1.0)
        except Exception:
            pass

        keys = self.cli.keys(self.reg_prefix + ":*")
        if keys:
            self.cli.delete(*keys)

        try:
            self.ps.close()
        except Exception:
            pass

    def _await_ack(self, timeout: float = 10.0):
        """Poll ACK channel until a JSON message arrives or timeout elapses."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            message = self.ps.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if not message:
                continue
            data = message.get("data")
            if isinstance(data, bytes):
                try:
                    data = data.decode("utf-8", errors="ignore")
                except Exception:
                    continue
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except Exception:
                    continue
        return None

    def test_subscribe_status_unsubscribe(self):
        # Explicitly disable preload in the command to avoid wait-for-download delays.
        cmd_sub = {
            "action": "subscribe",
            "strategy_id": self.strategy,
            "codes": ["518880.SH"],
            "periods": ["1m"],
            "preload_days": 0,
        }
        self.cli.publish(self.channel, json.dumps(cmd_sub, ensure_ascii=False))
        ack = self._await_ack()
        self.assertIsNotNone(ack)
        self.assertTrue(ack.get("ok"))
        self.assertEqual(ack.get("action"), "subscribe")
        self.assertIn("sub_id", ack)
        self.assertEqual(len(self.svc.add_calls), 1)
        sub_id = ack["sub_id"]
        self.assertIn(sub_id, self.registry.list_all())

        cmd_st = {"action": "status", "strategy_id": self.strategy}
        self.cli.publish(self.channel, json.dumps(cmd_st, ensure_ascii=False))
        ack2 = self._await_ack()
        self.assertIsNotNone(ack2)
        self.assertTrue(ack2.get("ok"))
        self.assertEqual(ack2.get("action"), "status")
        self.assertIn("status", ack2)

        cmd_un = {"action": "unsubscribe", "strategy_id": self.strategy, "sub_id": sub_id}
        self.cli.publish(self.channel, json.dumps(cmd_un, ensure_ascii=False))
        ack3 = self._await_ack()
        self.assertIsNotNone(ack3)
        self.assertTrue(ack3.get("ok"))
        self.assertEqual(ack3.get("action"), "unsubscribe")
        self.assertEqual(len(self.svc.remove_calls), 1)
        self.assertEqual(self.registry.list_all(), [])
