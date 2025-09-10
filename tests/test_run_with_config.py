# -*- coding: utf-8 -*-
"""run_with_config 脚本集成测试（不真正进入阻塞循环）

每个测试方法均包含：测试内容、目的、输入、预期输出。
"""
import os
import sys
import tempfile
import types
import unittest
from unittest import mock


class TestRunWithConfig(unittest.TestCase):
    """类说明：配置化启动器的集成校验（通过 patch 避免真实依赖）"""

    def _write_yaml(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def test_main_happy_path(self):
        """测试内容：读取配置并依次初始化组件
        目的：验证 QMTConnector.listen_and_connect、PubSubPublisher、RealtimeSubscriptionService.run_forever 都被调用一次
        输入：最小有效的 realtime.yml 内容
        预期输出：各组件调用次数为 1，且参数按配置传递
        """
        y = """
qmt:
  mode: none
redis:
  host: 127.0.0.1
  port: 6379
  password: null
  topic: xt:topic:test
subscription:
  codes: [000001.SZ, 600000.SH]
  periods: [1m, 1d]
  mode: close_only
  close_delay_ms: 50
  preload_days: 1
logging:
  level: INFO
"""
        path = self._write_yaml(y)

        # 伪造各组件：记录入参并返回可用对象
        class FakeConnector:
            def __init__(self, *_a, **_kw): pass
            def listen_and_connect(self):
                TestRunWithConfig._conn_called = True
        class FakePublisher:
            def __init__(self, **kw): TestRunWithConfig._pub_args = kw
        class FakeRtCfg:
            def __init__(self, **kw): TestRunWithConfig._rt_cfg = kw
        class FakeService:
            def __init__(self, cfg, publisher):
                TestRunWithConfig._svc_args = (cfg, publisher)
            def run_forever(self):
                TestRunWithConfig._run_called = True

        import scripts.run_with_config as runner
        with mock.patch.object(runner, "QMTConnector", FakeConnector), \
             mock.patch.object(runner, "PubSubPublisher", FakePublisher), \
             mock.patch.object(runner, "RealtimeConfig", FakeRtCfg), \
             mock.patch.object(runner, "RealtimeSubscriptionService", FakeService):
            # 构造 argv 调用 main
            argv_backup = sys.argv
            sys.argv = [argv_backup[0], "--config", path]
            try:
                runner.main()
            finally:
                sys.argv = argv_backup

        self.assertTrue(getattr(TestRunWithConfig, "_conn_called", False))
        self.assertTrue(getattr(TestRunWithConfig, "_run_called", False))
        pub_args = getattr(TestRunWithConfig, "_pub_args", {})
        self.assertEqual(pub_args.get("topic"), "xt:topic:test")
        rt_cfg = getattr(TestRunWithConfig, "_rt_cfg", {})
        self.assertEqual(rt_cfg.get("periods"), ["1m", "1d"])
        os.remove(path)
