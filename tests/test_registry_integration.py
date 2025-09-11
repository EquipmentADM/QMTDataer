# -*- coding: utf-8 -*-
"""Registry 集成测试（使用真 Redis）

测试项目：
1) 测试内容：保存/查询/按策略列举/删除订阅规格的持久化行为
   目的：验证注册表 Redis 键集正确、可重启恢复的元信息完整
   输入：随机 registry_prefix，构造一个 SubscriptionSpec 并 save()；随后 list/load/delete
   预期输出：
      - <prefix>:subs 集合含 sub_id
      - <prefix>:sub:<sub_id> 哈希含字段（strategy_id/codes/periods/...）
      - <prefix>:strategy:<strategy_id>:subs 集合含 sub_id
      - delete 后上述键被清除
"""
import time
import unittest
import redis as redislib

from tests._helpers import redis_params_from_env, random_suffix
from core.registry import Registry, SubscriptionSpec


class TestRegistryIntegration(unittest.TestCase):
    def setUp(self):
        p = redis_params_from_env()
        self.r = redislib.from_url(p["url"], decode_responses=True)
        self.prefix = f"xt:bridge:test:{random_suffix()}"
        self.registry = Registry(p["host"], p["port"], p["password"], p["db"], prefix=self.prefix)

    def tearDown(self):
        keys = self.r.keys(self.prefix + ":*")
        if keys:
            self.r.delete(*keys)

    def test_save_load_list_delete(self):
        spec = SubscriptionSpec(strategy_id="stratA", codes=["518880.SH"], periods=["1m", "1d"],
                                mode="close_only", preload_days=1, topic="xt:topic:test",
                                created_at=int(time.time()))
        sub_id = self.registry.gen_sub_id()
        self.registry.save(sub_id, spec)
        self.assertIn(sub_id, self.r.smembers(self.prefix + ":subs"))
        h = self.r.hgetall(self.prefix + f":sub:{sub_id}")
        self.assertEqual(h.get("strategy_id"), "stratA")
        self.assertIn("codes", h)
        self.assertIn("periods", h)
        self.assertIn(sub_id, self.r.smembers(self.prefix + ":strategy:stratA:subs"))
        self.assertIn(sub_id, self.registry.list_all())
        self.assertIsNotNone(self.registry.load(sub_id))
        self.assertIn(sub_id, self.registry.list_by_strategy("stratA"))
        self.registry.delete(sub_id)
        self.assertNotIn(sub_id, self.r.smembers(self.prefix + ":subs"))
        self.assertFalse(self.r.exists(self.prefix + f":sub:{sub_id}"))
        self.assertNotIn(sub_id, self.r.smembers(self.prefix + ":strategy:stratA:subs"))
