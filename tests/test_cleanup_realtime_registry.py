# -*- coding: utf-8 -*-
"""
cleanup_realtime_registry 脚本测试。

Responsibilities:
    - 校验参数冲突与缺失范围时的返回码。
    - 校验 dry-run 和执行删除时对 Registry 的调用链。
"""
from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

import scripts.cleanup_realtime_registry as cleanup_cli


class TestCleanupRealtimeRegistry(unittest.TestCase):
    """
    Registry 清理脚本测试。
    """

    def test_run_cleanup_requires_scope(self):
        """
        未指定 strategy 或 all 时应返回 2。
        """
        rc = cleanup_cli.run_cleanup()
        self.assertEqual(rc, 2)

    def test_run_cleanup_rejects_conflict_scope(self):
        """
        同时传 strategy 与 all 时应返回 2。
        """
        rc = cleanup_cli.run_cleanup(strategy_id="demo", clear_all=True)
        self.assertEqual(rc, 2)

    def test_dry_run_by_strategy(self):
        """
        dry-run 模式下只列出目标，不执行删除。
        """
        registry = mock.Mock()
        registry.list_by_strategy.return_value = ["sub-a", "sub-b"]
        with mock.patch.object(cleanup_cli, "Registry", return_value=registry):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cleanup_cli.run_cleanup(strategy_id="demo", execute=False)
        self.assertEqual(rc, 0)
        registry.list_by_strategy.assert_called_once_with("demo")
        registry.delete_by_strategy.assert_not_called()
        self.assertIn("dry-run", buf.getvalue())

    def test_execute_clear_all(self):
        """
        执行全量清理时应调用 Registry.clear_all。
        """
        registry = mock.Mock()
        registry.list_all.return_value = ["sub-a"]
        registry.clear_all.return_value = ["sub-a"]
        with mock.patch.object(cleanup_cli, "Registry", return_value=registry):
            rc = cleanup_cli.run_cleanup(clear_all=True, execute=True)
        self.assertEqual(rc, 0)
        registry.list_all.assert_called_once()
        registry.clear_all.assert_called_once()


if __name__ == "__main__":
    unittest.main()
