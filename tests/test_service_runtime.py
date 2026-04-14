"""
bridge 服务运行时管理测试。

Responsibilities:
    - 验证启动锁、runtime.json 的生成与清理；
    - 验证陈旧锁识别与清理逻辑；
    - 验证运行时快照最小字段是否完整。
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bridge_service.service_runtime import ServiceRuntime


class TestServiceRuntime(unittest.TestCase):
    """service_runtime.py 的最小单元测试。"""

    def test_prepare_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=19931)
            info = runtime.prepare()
            self.assertTrue(runtime.lock_file.exists())
            self.assertTrue(runtime.runtime_file.exists())

            payload = json.loads(runtime.runtime_file.read_text(encoding="utf-8"))
            self.assertEqual(payload["module_id"], "qmtdataer")
            self.assertEqual(payload["instance_id"], info.instance_id)
            self.assertEqual(payload["port"], 19931)

            runtime.cleanup()
            self.assertFalse(runtime.lock_file.exists())
            self.assertFalse(runtime.runtime_file.exists())

    def test_stale_lock_will_be_cleaned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            runtime = ServiceRuntime(base_dir=base_dir)
            runtime.runtime_dir.mkdir(parents=True, exist_ok=True)
            runtime.lock_file.write_text(
                json.dumps({"pid": 999999, "created_at": "2026-04-13T10:00:00"}, ensure_ascii=False),
                encoding="utf-8",
            )
            with patch("bridge_service.service_runtime.is_pid_running", return_value=False):
                runtime.prepare()
            self.assertTrue(runtime.lock_file.exists())

    def test_get_runtime_snapshot_without_runtime_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=19932)
            runtime.instance_id = "demo-instance"
            runtime.started_at = "2026-04-13T10:00:00"
            snapshot = runtime.get_runtime_snapshot()
            self.assertEqual(snapshot["instance_id"], "demo-instance")
            self.assertEqual(snapshot["port"], 19932)


if __name__ == "__main__":
    unittest.main()
