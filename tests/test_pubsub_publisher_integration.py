# -*- coding: utf-8 -*-
"""PubSubPublisher 集成测试（使用真 Redis）

测试项目：
1) 测试内容：发布 JSON 到唯一 topic，订阅端能收到且字段完整
   目的：验证发布器与 Redis PubSub 实际联通
   输入：随机 topic，发布一条 payload
   预期输出：订阅端收到 1 条消息，JSON 反序列化后字段匹配
"""
import json
import time
import unittest
import redis as redislib

from tests._helpers import redis_params_from_env, random_suffix
from core.pubsub_publisher import PubSubPublisher


class TestPublisherIntegration(unittest.TestCase):
    def setUp(self):
        p = redis_params_from_env()
        self.cli = redislib.from_url(p["url"], decode_responses=True)
        self.topic = f"xt:topic:it:{random_suffix()}"
        self.pubsub = self.cli.pubsub()
        self.pubsub.subscribe(self.topic)
        while self.pubsub.get_message(timeout=0.01):
            pass

    def tearDown(self):
        try:
            self.pubsub.close()
        except Exception:
            pass

    def test_publish_and_receive(self):
        p = redis_params_from_env()
        pub = PubSubPublisher(host=p["host"], port=p["port"], password=p["password"], db=p["db"], topic=self.topic)
        payload = {"code": "518880.SH", "period": "1m", "close": 7.77, "is_closed": True}
        pub.publish(payload)
        t0 = time.time(); got = None
        while time.time() - t0 < 2:
            m = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if m and m.get("data"):
                got = json.loads(m["data"]) if isinstance(m["data"], str) else None
                break
        self.assertIsNotNone(got)
        self.assertEqual(got["code"], "518880.SH")
        self.assertEqual(got["period"], "1m")
        self.assertTrue(got["is_closed"])
