# -*- coding: utf-8 -*-
"""config_loader 对 Redis URL 的解析测试

测试项目：
1) 测试内容：解析 redis://127.0.0.1:6379/0 为 host/port/password/db
   目的：确保在 M3.5 中对 URL 的兼容实现正确
   输入：临时 YAML 文件
   预期输出：AppConfig.redis.host/port/db 与 URL 匹配
"""
import os
import tempfile
import unittest

from core.config_loader import load_config


class TestConfigLoaderRedisURL(unittest.TestCase):
    def _write_yaml(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yml"); os.close(fd)
        with open(path, "w", encoding="utf-8") as f: f.write(text)
        return path

    def test_parse_url(self):
        y = """
qmt:
  mode: none
redis:
  url: "redis://127.0.0.1:6379/0"
  topic: xt:topic:bar
subscription:
  codes: [000001.SZ]
  periods: [1m]
  mode: close_only
"""
        path = self._write_yaml(y)
        cfg = load_config(path)
        self.assertEqual(cfg.redis.host, "127.0.0.1")
        self.assertEqual(cfg.redis.port, 6379)
        self.assertEqual(cfg.redis.db, 0)
        os.remove(path)