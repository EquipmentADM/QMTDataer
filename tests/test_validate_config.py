# -*- coding: utf-8 -*-
"""validate_config 脚本测试（M3.2）

说明：通过临时 YAML 与 stdout 捕获，验证打印输出。
"""
import os
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout


class TestValidateConfig(unittest.TestCase):
    """类说明：配置校验脚本测试"""

    def _write_yaml(self, text: str) -> str:
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def test_validate_prints_summary(self):
        """测试内容：打印关键信息
        目的：验证 main() 读取 YAML 后输出预期文本
        输入：最小可用配置（含 subscription、redis、logging）
        预期输出：包含“配置加载成功”“Redis”“订阅”等字段
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
"""
        path = self._write_yaml(y)
        import scripts.validate_config as mod
        buf = io.StringIO()
        argv_bak = sys.argv
        sys.argv = [argv_bak[0], "--config", path]
        try:
            with redirect_stdout(buf):
                mod.main()
        finally:
            sys.argv = argv_bak
            os.remove(path)
        out = buf.getvalue()
        self.assertIn("配置加载成功", out)
        self.assertIn("Redis", out)
        self.assertIn("订阅", out)
