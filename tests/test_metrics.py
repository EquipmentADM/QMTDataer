# -*- coding: utf-8 -*-
"""metrics 单元测试（M3.x）"""
import threading
import unittest
from datetime import datetime, timedelta

from core.metrics import Metrics, CN_TZ


class TestMetrics(unittest.TestCase):
    """类说明：最小指标器的计数与线程安全测试"""

    def setUp(self):
        Metrics.reset_global()

    def test_basic_counters(self):
        """测试内容：基础计数
        目的：验证 inc_published/inc_publish_fail/inc_dedup_hit 与 snapshot 返回
        输入：依次调用三种计数接口
        预期输出：snapshot 中的三个计数分别为 1/2/3
        """
        m = Metrics()
        m.inc_published()
        m.inc_publish_fail(2)
        m.inc_dedup_hit(3)
        snap = m.snapshot()
        self.assertEqual(snap["published"], 1)
        self.assertEqual(snap["publish_fail"], 2)
        self.assertEqual(snap["dedup_hit"], 3)

    def test_thread_safety(self):
        """测试内容：多线程计数
        目的：验证并发自增不会丢失
        输入：开启 10 个线程，每个线程执行 1000 次 published++
        预期输出：最终 published=10000
        """
        m = Metrics()

        def worker():
            for _ in range(1000):
                m.inc_published()

        ths = [threading.Thread(target=worker) for _ in range(10)]
        for t in ths:
            t.start()
        for t in ths:
            t.join()
        self.assertEqual(m.snapshot()["published"], 10000)

    def test_global_counters(self):
        """测试内容：全局计数器与晚到判定
        目的：验证全局指标自增、Schema Drop 与 late 判定
        输入：inc_published/maybe_mark_late/mark_schema_drop
        预期输出：snapshot_global 中的计数按预期增加
        """
        m = Metrics()
        m.inc_published(2)
        Metrics.mark_schema_drop()
        past = (datetime.now(CN_TZ) - timedelta(seconds=10)).isoformat()
        Metrics.maybe_mark_late(past, threshold_sec=3)
        # 无效输入不会增加计数
        Metrics.maybe_mark_late("bad-timestamp")

        snap_global = Metrics.snapshot_global()
        self.assertEqual(snap_global["bars_published_total"], 2)
        self.assertEqual(snap_global["schema_drop_total"], 1)
        self.assertEqual(snap_global["late_bars_total"], 1)
