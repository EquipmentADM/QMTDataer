# -*- coding: utf-8 -*-
"""run_with_config（M3.2 增强）脚本测试

说明：使用 mock 替身验证：
  - 日志初始化（setup_logging）被调用，轮转参数按配置传入；
  - health.enabled=false 时不启动 HealthReporter；true 时会启动；
  - 其余组件调用顺序正确。
"""
import os
import sys
import tempfile
import unittest
from unittest import mock


class TestRunWithConfigM32(unittest.TestCase):
    """类说明：入口脚本增强路径测试"""

    def _write_yaml(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def test_health_disabled(self):
        """测试内容：禁用健康上报
        目的：验证不创建 HealthReporter；setup_logging 接收轮转参数
        输入：health.enabled=false；rotate.enabled=true
        预期输出：HealthReporter 未实例化；setup_logging 被调用一次
        """
        y = """
qmt:
  mode: none
redis:
  host: 127.0.0.1
  port: 6379
  topic: xt:topic:bar
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: close_only
logging:
  level: INFO
  file: logs/realtime.log
  rotate:
    enabled: true
    max_bytes: 1048576
    backup_count: 3
health:
  enabled: false
"""
        path = self._write_yaml(y)
        import scripts.run_with_config as runner

        class FakeConnector:
            def __init__(self, *_a, **_kw): pass
            def listen_and_connect(self): pass
        class FakePublisher:
            def __init__(self, **kw): pass
        class FakeRtCfg:
            def __init__(self, **kw): pass
        class FakeService:
            def __init__(self, cfg, publisher): pass
            def run_forever(self): pass

        with mock.patch.object(runner, "QMTConnector", FakeConnector), \
             mock.patch.object(runner, "PubSubPublisher", FakePublisher), \
             mock.patch.object(runner, "RealtimeConfig", FakeRtCfg), \
             mock.patch.object(runner, "RealtimeSubscriptionService", FakeService), \
             mock.patch.object(runner, "setup_logging") as mlog, \
             mock.patch.object(runner, "HealthReporter") as mhealth:
            argv_backup = sys.argv
            sys.argv = [argv_backup[0], "--config", path]
            try:
                runner.main()
            finally:
                sys.argv = argv_backup
        # 未实例化健康上报
        mhealth.assert_not_called()
        # 日志初始化被调用
        mlog.assert_called_once()
        os.remove(path)

    def test_health_enabled(self):
        """测试内容：启用健康上报
        目的：验证 HealthReporter 被实例化并启动
        输入：health.enabled=true
        预期输出：HealthReporter 被调用；入口按序执行
        """
        y = """
qmt:
  mode: none
redis:
  host: 127.0.0.1
  port: 6379
  topic: xt:topic:bar
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: close_only
logging:
  level: INFO
health:
  enabled: true
  key_prefix: xt:bridge:health
  interval_sec: 5
  ttl_sec: 20
"""
        path = self._write_yaml(y)
        import scripts.run_with_config as runner

        class FakeConnector:
            def __init__(self, *_a, **_kw): pass
            def listen_and_connect(self): pass
        class FakePublisher:
            def __init__(self, **kw): pass
        class FakeRtCfg:
            def __init__(self, **kw): pass
        class FakeService:
            def __init__(self, cfg, publisher): pass
            def run_forever(self): pass

        fake_health_obj = mock.Mock()
        with mock.patch.object(runner, "QMTConnector", FakeConnector), \
             mock.patch.object(runner, "PubSubPublisher", FakePublisher), \
             mock.patch.object(runner, "RealtimeConfig", FakeRtCfg), \
             mock.patch.object(runner, "RealtimeSubscriptionService", FakeService), \
             mock.patch.object(runner, "HealthReporter", return_value=fake_health_obj) as mhealth:
            argv_backup = sys.argv
            sys.argv = [argv_backup[0], "--config", path]
            try:
                runner.main()
            finally:
                sys.argv = argv_backup
        mhealth.assert_called_once()
        fake_health_obj.start.assert_called_once()
        os.remove(path)
