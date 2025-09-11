# -*- coding: utf-8 -*-
"""pubsub_publisher（M3 增强版）单元测试

每个测试方法均包含：测试内容、目的、输入、预期输出。
"""
import sys
import types
import importlib
import unittest
from unittest import mock

def _install_fake_redis(publish_side_effect=None):
    """安装假的 redis.Redis 到 sys.modules，允许设置 publish 的副作用。"""
    redis = types.ModuleType("redis")

    class FakeRedis:
        def __init__(self, *a, **kw):
            pass
        def publish(self, topic, payload):
            if publish_side_effect:
                return publish_side_effect(topic, payload)
            return 1

    redis.Redis = FakeRedis
    sys.modules["redis"] = redis


def _reload_pubsub():
    sys.modules.pop("core.pubsub_publisher", None)
    import core.pubsub_publisher  # noqa


class TestPubSubPublisherM3(unittest.TestCase):
    """类说明：发布器与指标联动测试"""

    def test_publish_ok_metrics_and_unicode(self):
        """测试内容：成功发布且计数+中文不转义
        目的：验证 metrics.published++，payload 保留中文字符
        输入：FakeRedis 正常返回；发送包含中文的字典
        预期输出：published=1，payload 字符串包含“中文”
        """
        captured = {"payload": None}
        def side_effect(topic, payload):
            captured["payload"] = payload
            return 1
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        from core.metrics import Metrics
        metrics = Metrics()
        pub = PubSubPublisher(topic="xt:topic:bar", metrics=metrics)
        pub.publish({"备注": "中文"})
        self.assertIn("中文", captured["payload"])
        self.assertEqual(metrics.snapshot()["published"], 1)

    def test_publish_retry_and_raise_metrics(self):
        """测试内容：持续失败直至抛出异常
        目的：验证失败重试与 metrics.publish_fail 增加
        输入：FakeRedis.publish 每次都抛异常；max_retries=3
        预期输出：RuntimeError 抛出，publish_fail=3
        """
        def side_effect(topic, payload):
            raise RuntimeError("net down")
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        from core.metrics import Metrics
        metrics = Metrics()
        pub = PubSubPublisher(metrics=metrics)
        with self.assertRaises(RuntimeError):
            pub.publish({"k": 1}, max_retries=3, backoff_ms=1)
        self.assertEqual(metrics.snapshot()["publish_fail"], 3)

    def test_constructor_without_redis_dependency(self):
        """测试内容：缺少 redis 依赖
        目的：验证模块导入期捕获的 _IMPORT_ERR 会导致构造时报错
        输入：拦截对 redis 的导入，强制抛 ImportError；再导入模块
        预期输出：构造 PubSubPublisher 抛 RuntimeError
        """
        # 清理模块缓存，确保重新导入时会尝试 import redis
        for k in ["redis", "core.pubsub_publisher"]:
            sys.modules.pop(k, None)

        import builtins
        import importlib

        real_import = builtins.__import__

        def blocked(name, *a, **kw):
            if name == "redis":
                raise ImportError("No module named 'redis' (blocked by test)")
            return real_import(name, *a, **kw)

        # 在导入 core.pubsub_publisher 期间阻断对 redis 的导入
        with mock.patch("builtins.__import__", side_effect = blocked):
            mod = importlib.import_module("core.pubsub_publisher")
            # 模块导入成功，但 _IMPORT_ERR 已经被设置；构造时应当抛 RuntimeError
            with self.assertRaises(RuntimeError):
                mod.PubSubPublisher()
