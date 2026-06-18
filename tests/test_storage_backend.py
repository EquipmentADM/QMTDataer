# -*- coding: utf-8 -*-
"""
storage backend 构造逻辑测试。

职责：
    - 验证默认 backend 已切换为 fd；
    - 验证 QMTD_STORAGE_BACKEND=legacy 回滚入口；
    - 验证 fd backend 可读取 FD_REPO 与 FD_DATA_ROOT。
"""
from __future__ import annotations

import os
import unittest
from unittest import mock

from core.storage_backend import resolve_storage_backend_config


class TestStorageBackend(unittest.TestCase):
    """存储后端配置解析测试。"""

    def test_default_backend_is_fd(self) -> None:
        """
        测试内容：未设置环境变量时默认使用 fd backend。

        目的：验证生产默认入库链路已切到新 FD。
        """
        with mock.patch.dict(os.environ, {}, clear=True):
            config = resolve_storage_backend_config(root="D:/db")

        self.assertEqual(config.backend, "fd")
        self.assertEqual(config.root, "D:/db")

    def test_legacy_backend_can_be_forced_by_env(self) -> None:
        """
        测试内容：显式设置 QMTD_STORAGE_BACKEND=legacy。

        目的：确认默认切 fd 后仍保留旧实现回滚入口。
        """
        with mock.patch.dict(os.environ, {"QMTD_STORAGE_BACKEND": "legacy"}, clear=True):
            config = resolve_storage_backend_config(root="D:/db")

        self.assertEqual(config.backend, "legacy")
        self.assertEqual(config.root, "D:/db")

    def test_fd_backend_uses_env_root(self) -> None:
        """
        测试内容：fd backend 读取 FD_REPO 与 FD_DATA_ROOT。

        目的：验证生产环境可通过环境变量覆盖 FD 项目路径与数据根目录。
        """
        env = {
            "QMTD_STORAGE_BACKEND": "fd",
            "FD_REPO": "D:/fd_repo",
            "FD_DATA_ROOT": "D:/fd_data",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = resolve_storage_backend_config(root="D:/legacy_data")

        self.assertEqual(config.backend, "fd")
        self.assertEqual(config.root, "D:/fd_data")
        self.assertEqual(config.fd_repo, "D:/fd_repo")

    def test_invalid_backend_raises(self) -> None:
        """
        测试内容：传入未知 backend 名称。

        目的：避免配置错误时静默走错入库链路。
        """
        with self.assertRaises(ValueError):
            resolve_storage_backend_config(root="D:/db", backend="bad")


if __name__ == "__main__":
    unittest.main()
