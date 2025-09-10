"""health 单元测试（M3.2）

说明：使用假 redis 客户端，验证 HealthReporter 的写入循环与停止。
"""
import sys
import types
import time
import unittest

from core.metrics import Metrics


class _FakeRedis:
    def __init__(self, *a, **kw):
        self.set_calls = []
    def set(self, key, val, ex=None):
        # 记录最后一次写入
        self.set_calls.append((key, val, ex))
        return True


def _install_fake_redis():
    r = types.ModuleType("redis")
    r.Redis = _FakeRedis
    sys.modules["redis"] = r


def _reload_health():
    sys.modules.pop("core.health", None)
    import core.health  # noqa


class TestHealth(unittest.TestCase):
    """类说明：健康上报线程测试"""

    def test_health_reporter_runs_and_stops(self):
        """测试内容：上报循环与停止
        目的：验证在短时间内至少发生一次 set 写入，且 stop() 能终止线程
        输入：interval_sec=0.05, ttl_sec=1，metrics 有初始快照
        预期输出：_FakeRedis.set 被调用≥1 次，写入键前缀正确
        """
        _install_fake_redis()
        _reload_health()
        from core.health import HealthReporter

        m = Metrics()
        m.inc_published(2)
        hr = HealthReporter(host="127.0.0.1", port=6379, password=None,
                            key_prefix="xt:bridge:health", metrics=m,
                            interval_sec=0.05, ttl_sec=1,
                            extra_info={"instance_tag": "T"})
        hr.start()
        time.sleep(0.2)  # 等待至少 1~2 次循环
        hr.stop()
        hr.join(timeout=1.0)
        rcli = hr._cli  # type: ignore[attr-defined]
        self.assertTrue(len(rcli.set_calls) >= 1)
        key, val, ex = rcli.set_calls[-1]
        self.assertTrue(key.startswith("xt:bridge:health:"))
        self.assertIsInstance(ex, int)