# -*- coding: utf-8 -*-
"""ControlPlane 集成测试（使用真 Redis；RealtimeService 用假实现）

测试项目：
1) 测试内容：subscribe/unsubscribe/status 三类命令的处理、ACK 回执、注册表落盘
   目的：验证控制面线程真实消费 Redis 通道，能调用服务方法并写入注册表
   输入：随机 control channel / ack_prefix / registry_prefix；策略 ID=demo
   预期输出：
      - subscribe 后 ACK {ok:True, action:subscribe, sub_id:...}，服务 add_subscription 被调用一次
      - status 返回当前 subs 与 last_published 字段
      - 按 sub_id 退订后 ACK {ok:True, action:unsubscribe}，服务 remove_subscription 被调用一次，注册表清空
"""
import json
import time
import unittest
import redis as redislib

from tests._helpers import redis_params_from_env, random_suffix
from core.control_plane import ControlPlane
from core.registry import Registry


class _FakeService:
    """类说明：假实时服务
    功能：记录调用参数，并维护一个内存订阅集用于 status 输出。
    上游：ControlPlane；
    下游：无。
    """
    def __init__(self):
        self.add_calls = []
        self.remove_calls = []
        self.cfg = type("Cfg", (), {"mode": "close_only", "preload_days": 0})
        self.publisher = type("Pub", (), {"topic": "xt:topic:bar"})
        self._subs = set()
    def add_subscription(self, codes, periods, preload_days=None):
        self.add_calls.append((tuple(codes), tuple(periods), preload_days))
        for c in codes:
            for p in periods:
                self._subs.add((c, p))
    def remove_subscription(self, codes, periods):
        self.remove_calls.append((tuple(codes), tuple(periods)))
        for c in codes:
            for p in periods:
                self._subs.discard((c, p))
    def status(self):
        subs = sorted([{"code": c, "period": p} for (c, p) in self._subs], key=lambda x: (x["code"], x["period"]))
        return {"subs": subs, "last_published": {}}


class TestControlPlaneIntegration(unittest.TestCase):
    def setUp(self):
        p = redis_params_from_env()
        self.cli = redislib.from_url(p["url"], decode_responses=True)
        self.channel = f"xt:ctrl:sub:test:{random_suffix()}"
        self.ack_prefix = f"xt:ctrl:ack:test:{random_suffix()}"
        self.reg_prefix = f"xt:bridge:reg:{random_suffix()}"
        self.strategy = "demo"
        self.ack_ch = f"{self.ack_prefix}:{self.strategy}"
        self.ps = self.cli.pubsub()
        self.ps.subscribe(self.ack_ch)
        # 为避免命名不一致，增加别名
        self.pubsub = self.ps
        while self.ps.get_message(timeout = 0.01):
            pass
        self.svc = _FakeService()
        self.cp = ControlPlane(host=p["host"], port=p["port"], password=p["password"], db=p["db"],
                               channel=self.channel, ack_prefix=self.ack_prefix,
                               registry_prefix=self.reg_prefix, svc=self.svc)
        self.cp.start()
        self.registry = Registry(p["host"], p["port"], p["password"], p["db"], prefix=self.reg_prefix)

    def tearDown(self):
        try:
            self.cp.stop(); self.cp.join(timeout=1.0)
        except Exception:
            pass
        keys = self.cli.keys(self.reg_prefix + ":*")
        if keys:
            self.cli.delete(*keys)
        try:
            self.ps.close()
        except Exception:
            pass

    def _await_ack(self, timeout=2.0):
        """测试辅助：等待 ACK，一直等到超时；兼容 bytes/str 的 data。"""
        t0 = time.time()
        while time.time() - t0 < timeout:
            m = self.ps.get_message(ignore_subscribe_messages = True, timeout = 0.2)
            if not m:
                continue
            data = m.get("data")
            if isinstance(data, bytes):  # 兼容 bytes
                try:
                    data = data.decode("utf-8", errors = "ignore")
                except Exception:
                    continue
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except Exception:
                    continue
        return None

    def test_subscribe_status_unsubscribe(self):
        cmd_sub = {"action": "subscribe", "strategy_id": self.strategy,
                   "codes": ["518880.SH"], "periods": ["1m"], "preload_days": 0}
        self.cli.publish(self.channel, json.dumps(cmd_sub, ensure_ascii=False))
        ack = self._await_ack(); self.assertIsNotNone(ack)
        self.assertTrue(ack.get("ok")); self.assertEqual(ack.get("action"), "subscribe")
        self.assertIn("sub_id", ack); self.assertEqual(len(self.svc.add_calls), 1)
        sub_id = ack["sub_id"]
        self.assertIn(sub_id, self.registry.list_all())
        cmd_st = {"action": "status", "strategy_id": self.strategy}
        self.cli.publish(self.channel, json.dumps(cmd_st, ensure_ascii=False))
        ack2 = self._await_ack(); self.assertIsNotNone(ack2)
        self.assertTrue(ack2.get("ok")); self.assertEqual(ack2.get("action"), "status")
        self.assertIn("status", ack2)
        cmd_un = {"action": "unsubscribe", "strategy_id": self.strategy, "sub_id": sub_id}
        self.cli.publish(self.channel, json.dumps(cmd_un, ensure_ascii=False))
        ack3 = self._await_ack(); self.assertIsNotNone(ack3)
        self.assertTrue(ack3.get("ok")); self.assertEqual(ack3.get("action"), "unsubscribe")
        self.assertEqual(len(self.svc.remove_calls), 1)
        self.assertEqual(self.registry.list_all(), [])