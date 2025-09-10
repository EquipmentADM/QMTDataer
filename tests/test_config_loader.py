"""config_loader 单元测试（M3）

每个测试方法均包含：测试内容、目的、输入、预期输出。
"""
import os
import tempfile
import unittest


class TestConfigLoader(unittest.TestCase):
    """类说明：配置加载与校验测试
    功能：验证 YAML 加载、默认值填充与异常分支。
    上游：无。
    下游：core.config_loader.load_config。
    """

    def _write_yaml(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def test_load_ok(self):
        """测试内容：完整 YAML 正常加载
        目的：验证字段映射、大小写规范化与默认值不触发
        输入：包含 qmt/redis/subscription/logging 的有效配置
        预期输出：AppConfig 对象内的字段与 YAML 一致
        """
        y = """
qmt:
  mode: none
  token: ""
redis:
  host: 127.0.0.1
  port: 6379
  password: null
  topic: xt:topic:bar
subscription:
  codes: [518880.SH, 513880.SH]
  periods: [1m, 1d]
  mode: close_only
  close_delay_ms: 150
  preload_days: 2
logging:
  level: DEBUG
  json: false
  file: null
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        cfg = load_config(path)
        self.assertEqual(cfg.qmt.mode, "none")
        self.assertEqual(cfg.redis.topic, "xt:topic:bar")
        self.assertEqual(cfg.subscription.codes, ["518880.SH", "513880.SH"])
        self.assertEqual(cfg.subscription.periods, ["1m", "1d"])
        self.assertEqual(cfg.subscription.mode, "close_only")
        self.assertEqual(cfg.subscription.close_delay_ms, 150)
        self.assertEqual(cfg.subscription.preload_days, 2)
        self.assertEqual(cfg.logging.level, "DEBUG")
        os.remove(path)

    def test_invalid_period_raises(self):
        """测试内容：非法周期
        目的：校验周期集合仅支持 1m/1h/1d
        输入：periods: [5m]
        预期输出：ValueError
        """
        y = """
subscription:
  codes: [000001.SZ]
  periods: [5m]
  mode: close_only
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        with self.assertRaises(ValueError):
            load_config(path)
        os.remove(path)

    def test_empty_codes_raises(self):
        """测试内容：codes 为空
        目的：保证订阅标的不能为空
        输入：codes: []
        预期输出：ValueError
        """
        y = """
subscription:
  codes: []
  periods: [1m]
  mode: close_only
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        with self.assertRaises(ValueError):
            load_config(path)
        os.remove(path)

    def test_invalid_mode_raises(self):
        """测试内容：订阅模式非法
        目的：限定 mode 只能是 close_only 或 forming_and_close
        输入：mode: invalid
        预期输出：ValueError
        """
        y = """
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: invalid
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        with self.assertRaises(ValueError):
            load_config(path)
        os.remove(path)

    def test_invalid_qmt_mode_raises(self):
        """测试内容：QMT 连接模式非法
        目的：限定 qmt.mode 只能为 none 或 legacy
        输入：qmt.mode: other
        预期输出：ValueError
        """
        y = """
qmt:
  mode: other
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: close_only
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        with self.assertRaises(ValueError):
            load_config(path)
        os.remove(path)

    def test_defaults_when_missing_sections(self):
        """测试内容：缺少非必需节时的默认值
        目的：当仅提供最小 subscription 时，其他节采用默认
        输入：仅 subscription（codes/periods/mode）
        预期输出：qmt.mode=none、redis.host=127.0.0.1、logging.level=INFO
        """
        y = """
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: close_only
"""
        path = self._write_yaml(y)
        from core.config_loader import load_config
        cfg = load_config(path)
        self.assertEqual(cfg.qmt.mode, "none")
        self.assertEqual(cfg.redis.host, "127.0.0.1")
        self.assertEqual(cfg.logging.level, "INFO")
        os.remove(path)
