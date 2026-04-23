"""
bridge 服务 HTTP 接口测试。

Responsibilities:
    - 验证 /health 与 /status_detail 的基本返回结构；
    - 验证 /probe_history 能走通最小 JSON 链路；
    - 避免测试依赖真实 xtdata 或 MiniQMT 环境。
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from bridge_service.service_api import BridgeServiceState, create_http_server
from bridge_service.service_runtime import ServiceRuntime


class TestServiceApi(unittest.TestCase):
    """service_api.py 的最小接口测试。"""

    def test_health_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=19941)
            runtime.prepare()
            state = BridgeServiceState(runtime=runtime)
            payload = state.health_payload(deep=False)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["service"], "qmtdataer")
            self.assertEqual(payload["port"], 19941)
            runtime.cleanup()

    def test_http_probe_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=0)
            runtime.prepare()
            state = BridgeServiceState(
                runtime=runtime,
                probe_func=lambda symbol, period, start, end: {
                    "ok": True,
                    "symbol": symbol,
                    "period": period,
                    "start": start,
                    "end": end,
                    "rows": 2,
                },
            )
            server = create_http_server("127.0.0.1", 0, state)
            state.shutdown_callback = server.shutdown
            port = int(server.server_address[1])
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            time.sleep(0.05)

            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as resp:
                    health = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(health["ok"])

                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/probe_history",
                    data=json.dumps(
                        {
                            "symbol": "510050.SH",
                            "period": "1d",
                            "start": "20260301",
                            "end": "20260413",
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=3) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(result["ok"])
                self.assertEqual(result["symbol"], "510050.SH")
            finally:
                server.shutdown()
                server.server_close()
                runtime.cleanup()

    def test_status_detail_degraded_when_xtdata_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=19942)
            runtime.prepare()
            state = BridgeServiceState(runtime=runtime)
            with patch("bridge_service.service_api._import_xtdata", side_effect=ImportError("no xtdata")):
                payload = state.status_detail_payload()
            self.assertEqual(payload["status"], "degraded")
            self.assertFalse(payload["dependency"]["ok"])
            runtime.cleanup()

    def test_service_main_import_chain_ok(self) -> None:
        """
        验证 service_main.py 在子进程场景下可完成项目内导入链。

        说明：
            - 该测试不启动 HTTP 服务；
            - 仅在子进程中 import service_main，确保 `core.ingest_runner` 不再导入失败。
        """
        command = [
            sys.executable,
            "-c",
            "import service_main; print('OK')",
        ]
        proc = subprocess.run(
            command,
            cwd=str(Path(__file__).resolve().parent.parent),
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("OK", proc.stdout)

    def test_submit_task_and_query_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = ServiceRuntime(base_dir=Path(tmpdir), port=0)
            runtime.prepare()
            state = BridgeServiceState(runtime=runtime)
            with patch(
                "bridge_service.service_tasks.run_profile",
                return_value={"total": 1, "updated": 1, "failed_count": 0},
            ):
                submit = state.submit_task(
                    {
                        "task_type": "ingest_run",
                        "payload": {"mode": "recent-backfill"},
                    }
                )
                task_id = submit["task"]["task_id"]
                for _ in range(50):
                    payload = state.get_task_payload(task_id)
                    if payload["task"]["status"] == "success":
                        break
                    time.sleep(0.05)

            queried = state.get_task_payload(task_id)
            self.assertTrue(queried["ok"])
            self.assertEqual(queried["task"]["status"], "success")
            recent = state.list_recent_tasks_payload(limit=5)
            self.assertTrue(recent["ok"])
            self.assertTrue(len(recent["tasks"]) >= 1)
            runtime.cleanup()


if __name__ == "__main__":
    unittest.main()
