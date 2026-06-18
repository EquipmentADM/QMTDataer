# -*- coding: utf-8 -*-
"""
ingest_runner 配置与调度逻辑测试。

Responsibilities:
    - 校验三种 profile 的默认语义是否符合约定。
    - 校验 build_profile 覆盖逻辑与 run_profile 调度关系。

Internal Dependencies:
    - core.ingest_runner
"""
from __future__ import annotations

import unittest
from unittest import mock

from core import ingest_runner


class TestIngestRunner(unittest.TestCase):
    """
    入库执行器模式测试。
    """

    def test_profile_defaults(self):
        """
        校验三种模式的关键默认参数。

        Returns:
            None
        """
        full_download = ingest_runner.build_profile("full-download")
        full_backfill = ingest_runner.build_profile("full-backfill")
        recent_backfill = ingest_runner.build_profile("recent-backfill")

        self.assertFalse(full_download.merge)
        self.assertTrue(full_backfill.merge)
        self.assertTrue(recent_backfill.auto_start)
        self.assertGreaterEqual(recent_backfill.lookback, 1)

    def test_build_profile_overrides(self):
        """
        校验 profile 覆盖参数是否生效。

        Returns:
            None
        """
        profile = ingest_runner.build_profile(
            "full-backfill",
            symbols=("AAA.SH", "BBB.SH"),
            cycles=("1d",),
            lookback=9,
            merge=False,
        )
        self.assertEqual(profile.symbols, ("AAA.SH", "BBB.SH"))
        self.assertEqual(profile.cycles, ("1d",))
        self.assertEqual(profile.lookback, 9)
        self.assertFalse(profile.merge)

    def test_fd_daily_end_uses_next_day_at_request_layer(self):
        """
        测试内容：fd backend 的日线显式 end 在 QMTD 请求层做 end+1。

        目的：兼容业务侧“包含结束日”的使用习惯，同时保持 FD core 半开区间契约。
        """
        self.assertEqual(
            ingest_runner._resolve_cycle_end_time("20260610", "1d", "fd"),
            "20260611",
        )
        self.assertEqual(
            ingest_runner._resolve_cycle_end_time("20260610", "1m", "fd"),
            "20260610",
        )
        self.assertEqual(
            ingest_runner._resolve_cycle_end_time("20260610", "1d", "legacy"),
            "20260610",
        )

    def test_run_profile_delegate(self):
        """
        校验 run_profile 会调用 run_ingest，且传入构建后的 profile。

        Returns:
            None
        """
        with mock.patch.object(ingest_runner, "run_ingest", return_value={"ok": True}) as mocked:
            result = ingest_runner.run_profile("full-backfill", lookback=7)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(mocked.call_count, 1)
        profile = mocked.call_args.args[0]
        self.assertEqual(profile.name, "full-backfill")
        self.assertEqual(profile.lookback, 7)


if __name__ == "__main__":
    unittest.main()
