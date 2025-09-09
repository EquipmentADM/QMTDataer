"""PubSubPublisher 单元测试（M2.5 版）

类说明：
    - 覆盖正常发布、重试成功、重试耗尽异常、中文序列化；
    - 上游：无；
    - 下游：被测对象 core.pubsub_publisher.PubSubPublisher。

注意：不依赖真实 Redis，通过注入假模块 redis.Redis 实现。
"""
import sys
import types
import importlib
import unittest


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
    if "core.pubsub_publisher" in sys.modules:
        importlib.reload(sys.modules["core.pubsub_publisher"])
    else:
        import core.pubsub_publisher  # noqa


class TestPubSubPublisher(unittest.TestCase):
    """类说明：发布器行为测试
    功能：发布成功/重试/失败、中文序列化。
    上游：无。
    下游：PubSubPublisher。
    """

    def test_publish_ok(self):
        """测试内容：正常发布
        目的：验证 JSON 序列化与 publish 被调用；
        输入：topic='xt:topic:bar'，payload 为字典；
        预期输出：FakeRedis.publish 返回>0，不抛异常。
        """
        calls = {"n": 0}
        def side_effect(topic, payload):
            calls["n"] += 1
            self.assertEqual(topic, "xt:topic:bar")
            self.assertIsInstance(payload, str)
            return 1
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        pub = PubSubPublisher(topic="xt:topic:bar")
        pub.publish({"code": "000001.SZ", "period": "1m"})
        self.assertEqual(calls["n"], 1)

    def test_retry_then_success(self):
        """测试内容：前两次失败第三次成功
        目的：验证重试逻辑。
        输入：publish 副作用：两次抛异常→一次成功；
        预期输出：不抛异常，总调用 3 次。
        """
        seq = {"i": 0}
        def side_effect(topic, payload):
            seq["i"] += 1
            if seq["i"] < 3:
                raise RuntimeError("net down")
            return 1
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        pub = PubSubPublisher()
        pub.publish({"k": 1})
        self.assertEqual(seq["i"], 3)

    def test_retry_exhaust_raise(self):
        """测试内容：达到最大重试仍失败
        目的：验证抛出 RuntimeError。
        输入：每次都抛异常；
        预期输出：RuntimeError。
        """
        def side_effect(topic, payload):
            raise RuntimeError("always fail")
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        pub = PubSubPublisher()
        with self.assertRaises(RuntimeError):
            pub.publish({"k": 1}, max_retries=3)

    def test_unicode_payload(self):
        """测试内容：非 ASCII 字符序列化
        目的：验证 ensure_ascii=False 生效；
        输入：包含中文的字典；
        预期输出：publish 收到的 JSON 字符串中包含原始中文字符。
        """
        container = {}
        def side_effect(topic, payload):
            container["payload"] = payload
            return 1
        _install_fake_redis(publish_side_effect=side_effect)
        _reload_pubsub()
        from core.pubsub_publisher import PubSubPublisher
        pub = PubSubPublisher()
        pub.publish({"备注": "中文"})
        self.assertIn("中文", container["payload"])