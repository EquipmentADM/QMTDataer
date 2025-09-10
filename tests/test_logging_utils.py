"""logging_utils 单元测试（M3.2）

每个测试方法均包含：测试内容、目的、输入、预期输出。
"""
import io
import os
import time
import tempfile
import unittest
import logging

from core.logging_utils import setup_logging, _JsonFormatter


class TestLoggingUtils(unittest.TestCase):
    """类说明：日志初始化与轮转测试
    功能：验证控制台/文件输出、JSON 格式与轮转行为。
    上游：无。
    下游：core.logging_utils.setup_logging。
    """

    def tearDown(self):
        # 还原 root handlers，避免影响其他用例
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        root.setLevel(logging.WARNING)

    def test_console_text_logging(self):
        """测试内容：控制台文本日志
        目的：验证初始化后 root logger 存在 StreamHandler 且为文本格式
        输入：level=DEBUG, to_file=None, json_mode=False
        预期输出：存在一个 StreamHandler；其 formatter 不是 JSON 格式器
        """
        setup_logging(level="DEBUG", to_file=None, json_mode=False)
        root = logging.getLogger()
        shs = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        self.assertTrue(len(shs) >= 1)
        self.assertFalse(isinstance(shs[0].formatter, _JsonFormatter))

    def test_file_rotation_and_json(self):
        """测试内容：文件日志 + 轮转 + JSON 格式
        目的：写入超过 max_bytes 的日志，触发 .1 轮转文件生成
        输入：to_file=临时文件, rotate_enabled=True, max_bytes=200, backup_count=1, json_mode=True
        预期输出：日志文件与 .1 轮转文件均存在
        """
        fd, log_path = tempfile.mkstemp(prefix="logutil-", suffix=".log")
        os.close(fd)
        try:
            setup_logging(level="INFO", to_file=log_path, json_mode=True,
                          rotate_enabled=True, max_bytes=200, backup_count=1)
            logger = logging.getLogger("T")
            # 写入多行触发轮转
            for _ in range(50):
                logger.info("x" * 20)
            # 轮转是同步触发的，稍等文件系统
            time.sleep(0.05)
            self.assertTrue(os.path.exists(log_path))
            self.assertTrue(os.path.exists(log_path + ".1"))
        finally:
            for p in [log_path, log_path + ".1", log_path + ".2"]:
                try:
                    os.remove(p)
                except OSError:
                    pass