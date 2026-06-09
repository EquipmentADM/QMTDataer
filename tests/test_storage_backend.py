# -*- coding: utf-8 -*-
"""
storage backend 构造逻辑测试。

Responsibilities:
    - 验证 legacy/fd backend 的配置解析。
    - 验证环境变量可控制 fd backend 与 FD_DATA_ROOT。

External Systems:
    - 不写真实数据库，不连接 miniQMT 或 Redis。
"""
from __future__ import annotations

import os
import unittest
from unittest import mock

from core.storage_backend import resolve_storage_backend_config


class TestStorageBackend(unittest.TestCase):
    """
    存储后端配置解析测试。
    """

    def test_default_backend_is_legacy(self) -> None:
        """
        默认后端应保持 legacy，确保可回滚。

        Returns:
            None: 通过断言验证行为。
        """

        with mock.patch.dict(os.environ, {}, clear=True):
            config = resolve_storage_backend_config(root="D:/db")

        self.assertEqual(config.backend, "legacy")
        self.assertEqual(config.root, "D:/db")

    def test_fd_backend_uses_env_root(self) -> None:
        """
        fd backend 应允许 FD_DATA_ROOT 覆盖数据根目录。

        Returns:
            None: 通过断言验证行为。
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
        未知后端名称应直接报错，避免静默走错链路。

        Returns:
            None: 通过断言验证行为。
        """

        with self.assertRaises(ValueError):
            resolve_storage_backend_config(root="D:/db", backend="bad")


if __name__ == "__main__":
    unittest.main()
