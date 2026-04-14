"""
bridge 服务任务执行器测试。

Responsibilities:
    - 验证任务创建、状态流转与近期任务列表；
    - 验证 ingest_run 任务对 run_profile 的调用封装；
    - 不依赖真实 xtdata、MiniQMT 或实际下载行为。
"""
from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from bridge_service.service_tasks import ServiceTaskManager


class TestServiceTasks(unittest.TestCase):
    """service_tasks.py 的最小单元测试。"""

    def test_submit_and_finish_success(self) -> None:
        manager = ServiceTaskManager()
        with patch(
            "bridge_service.service_tasks.run_profile",
            return_value={"total": 2, "updated": 2, "failed_count": 0},
        ):
            task = manager.submit_task("ingest_run", {"mode": "recent-backfill"})
            for _ in range(50):
                current = manager.get_task(task.task_id)
                if current and current.status == "success":
                    break
                time.sleep(0.05)

        final = manager.get_task(task.task_id)
        self.assertIsNotNone(final)
        self.assertEqual(final.status, "success")
        self.assertIsNotNone(final.result)
        self.assertEqual(final.result["mode"], "recent-backfill")

    def test_submit_invalid_task_type(self) -> None:
        manager = ServiceTaskManager()
        task = manager.submit_task("unknown_task", {})
        for _ in range(50):
            current = manager.get_task(task.task_id)
            if current and current.status == "failed":
                break
            time.sleep(0.05)
        final = manager.get_task(task.task_id)
        self.assertIsNotNone(final)
        self.assertEqual(final.status, "failed")
        self.assertIn("不支持", final.error)

    def test_list_recent_tasks(self) -> None:
        manager = ServiceTaskManager()
        with patch(
            "bridge_service.service_tasks.run_profile",
            return_value={"total": 1, "updated": 1, "failed_count": 0},
        ):
            t1 = manager.submit_task("ingest_run", {"mode": "recent-backfill"})
            t2 = manager.submit_task("ingest_run", {"mode": "full-backfill"})
            for _ in range(50):
                tasks = manager.list_recent_tasks(limit=2)
                if len(tasks) >= 2 and all(t["status"] in {"running", "success", "failed"} for t in tasks):
                    break
                time.sleep(0.05)

        tasks = manager.list_recent_tasks(limit=2)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]["task_id"], t2.task_id)
        self.assertEqual(tasks[1]["task_id"], t1.task_id)

