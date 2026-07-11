# -*- coding: utf-8 -*-
"""
flow_demo_subscribe_and_listen 接收统计逻辑测试。
"""
from __future__ import annotations

import unittest
from unittest import mock

from tests import flow_demo_subscribe_and_listen
from tests.flow_demo_subscribe_and_listen import ReceiveStats


class TestFlowDemoListenerStats(unittest.TestCase):
    """验证 Redis 行情监听统计口径。"""

    def test_explicit_expected_codes_complete_group(self) -> None:
        """显式预期标的池收齐后，应输出全量轮次统计。"""
        stats = ReceiveStats(["A.SH", "B.SH", "C.SH"], ["1m"])

        self.assertIsNone(stats.record("A.SH", "1m", "2026-06-29T09:51:00", 100.0))
        self.assertIsNone(stats.record("B.SH", "1m", "2026-06-29T09:51:00", 101.0))
        metric = stats.record("C.SH", "1m", "2026-06-29T09:51:00", 103.0)

        self.assertIsNotNone(metric)
        self.assertTrue(metric.is_complete)
        self.assertEqual(metric.expected_count, 3)
        self.assertEqual(metric.expected_label, "3/3")
        self.assertEqual(metric.span_sec, 3.0)
        self.assertAlmostEqual(metric.avg_recv, 101.3333333333)
        self.assertAlmostEqual(metric.mean_abs_error_sec, 1.1111111111)

    def test_auto_discover_group_finalized_by_half_period(self) -> None:
        """自动发现模式不预设标的池，半周期后按实际出现标的固化统计。"""
        stats = ReceiveStats([], ["1m"], auto_discover=True)

        self.assertIsNone(stats.record("A.SH", "1m", "2026-06-29T09:51:00", 100.0))
        self.assertIsNone(stats.record("B.SH", "1m", "2026-06-29T09:51:00", 101.0))
        items = stats.finalize_due_groups(131.0)

        self.assertEqual(len(items), 1)
        metric = items[0]
        self.assertTrue(metric.is_complete)
        self.assertEqual(metric.received_count, 2)
        self.assertIsNone(metric.expected_count)
        self.assertEqual(metric.expected_label, "2")
        self.assertEqual(metric.codes, ["A.SH", "B.SH"])
        self.assertEqual(metric.missing_codes, [])

        summary = stats.build_summary()
        self.assertEqual(summary["stat_groups"], 1)
        self.assertEqual(summary["full_groups"], 1)
        self.assertEqual(summary["partial_groups"], 0)
        self.assertEqual(summary["period_summary"]["1m"]["avg_count"], 2.0)

    def test_explicit_expected_codes_reports_missing_and_extra(self) -> None:
        """显式预期模式应报告缺失和额外标的。"""
        stats = ReceiveStats(["A.SH", "B.SH"], ["1m"])

        stats.record("A.SH", "1m", "2026-06-29T09:51:00", 100.0)
        stats.record("C.SH", "1m", "2026-06-29T09:51:00", 101.0)
        items = stats.finalize_due_groups(131.0)

        self.assertEqual(len(items), 1)
        metric = items[0]
        self.assertFalse(metric.is_complete)
        self.assertEqual(metric.expected_label, "2/2")
        self.assertEqual(metric.missing_codes, ["B.SH"])
        self.assertEqual(metric.extra_codes, ["C.SH"])
        self.assertEqual(metric.codes, ["A.SH", "C.SH"])

        summary = stats.build_summary()
        self.assertEqual(summary["stat_groups"], 1)
        self.assertEqual(summary["full_groups"], 0)
        self.assertEqual(summary["partial_groups"], 1)

    def test_duplicate_code_does_not_count_as_extra_bar(self) -> None:
        """同一轮同一标的重复到达时只更新接收时点，不增加总 bar 数。"""
        stats = ReceiveStats(["A.SH", "B.SH"], ["1m"])

        self.assertIsNone(stats.record("A.SH", "1m", "2026-06-29T09:51:00", 100.0))
        self.assertIsNone(stats.record("A.SH", "1m", "2026-06-29T09:51:00", 101.0))
        metric = stats.record("B.SH", "1m", "2026-06-29T09:51:00", 102.0)

        self.assertIsNotNone(metric)
        self.assertEqual(stats.total_bars, 2)
        self.assertEqual(metric.first_recv, 101.0)
        self.assertEqual(metric.last_recv, 102.0)

    def test_late_missing_code_does_not_reopen_finalized_group(self) -> None:
        """固化后的轮次不再被迟到标的重开。"""
        stats = ReceiveStats(["A.SH", "B.SH"], ["1m"])

        stats.record("A.SH", "1m", "2026-06-29T09:51:00", 100.0)
        stats.finalize_due_groups(131.0)
        late_metric = stats.record("B.SH", "1m", "2026-06-29T09:51:00", 132.0)

        self.assertIsNone(late_metric)
        summary = stats.build_summary()
        self.assertEqual(summary["stat_groups"], 1)
        self.assertEqual(summary["partial_groups"], 1)
        self.assertEqual(summary["full_groups"], 0)

    def test_metric_splits_qmtd_publish_span_and_redis_delay(self) -> None:
        """同轮统计应能拆分 QMTD 发布时间跨度和 Redis 后段延迟。"""
        stats = ReceiveStats(["A.SH", "B.SH"], ["1m"])

        stats.record("A.SH", "1m", "2026-06-29T09:51:00", 110.0, qmtd_recv_ts=100.0)
        metric = stats.record("B.SH", "1m", "2026-06-29T09:51:00", 113.0, qmtd_recv_ts=102.0)

        self.assertIsNotNone(metric)
        self.assertEqual(metric.span_sec, 3.0)
        self.assertEqual(metric.qmtd_span_sec, 2.0)
        self.assertEqual(metric.redis_delay_avg_sec, 10.5)
        self.assertEqual(metric.redis_delay_max_sec, 11.0)

        summary = stats.build_summary()["period_summary"]["1m"]
        self.assertEqual(summary["avg_qmtd_span_sec"], 2.0)
        self.assertEqual(summary["max_qmtd_span_sec"], 2.0)
        self.assertEqual(summary["avg_redis_delay_sec"], 10.5)
        self.assertEqual(summary["max_redis_delay_sec"], 11.0)

    def test_default_main_is_listen_only_and_auto_discover(self) -> None:
        """默认运行不应主动发送 subscribe，且不要求 codes。"""

        class FakePubSub:
            def subscribe(self, *channels):
                self.channels = channels

            def get_message(self, *args, **kwargs):
                return None

            def close(self):
                return None

        fake_cli = mock.Mock()
        fake_cli.pubsub.return_value = FakePubSub()

        fake_now = iter([1000.0, 1100.0, 1100.0, 1100.0, 1100.0])
        with mock.patch.object(flow_demo_subscribe_and_listen.redis, "from_url", return_value=fake_cli), \
             mock.patch.object(flow_demo_subscribe_and_listen.time, "time", side_effect=lambda: next(fake_now, 1100.0)), \
             mock.patch.object(flow_demo_subscribe_and_listen, "send_subscribe") as mocked_subscribe, \
             mock.patch.object(flow_demo_subscribe_and_listen, "send_unsubscribe") as mocked_unsubscribe:
            rc = flow_demo_subscribe_and_listen.main(["--minutes", "1"])

        self.assertEqual(rc, 0)
        mocked_subscribe.assert_not_called()
        mocked_unsubscribe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
