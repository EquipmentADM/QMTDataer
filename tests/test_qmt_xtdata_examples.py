# -*- coding: utf-8 -*-
"""QMT xtdata 样例测试（仅在本机 QMT/MiniQMT 可用时运行）

测试项目：
1) 测试内容：按 1d 周期对两只标的做 download_history_data，然后 get_market_data_in 获取字典
   目的：提供最小“真实 QMT 接口”的可运行样例，验证接口返回结构
   输入：codes=[518880.SH, 513880.SH], period=1d, 日期区间近几日
   预期输出：get_market_data_in 返回包含 'time','open','close' 等键，且 'time' 具备 DataFrame 形态

2) （可选，需设置环境变量 RUN_QMT_REALTIME_TEST=1 且有实时行情）
   测试内容：订阅 tick（或 L1）并在短时间内接收至少一次回调
   目的：验证本机行情推送与 xtdata.run 的基本通路
   输入：codes=[任意一只活跃标的], period=1m, 等待≤10秒
   预期输出：收到≥1次回调计数
"""
import os
import unittest
import datetime as dt
import threading

try:
    from xtquant import xtdata
    _XT_OK = True
except Exception:
    _XT_OK = False


@unittest.skipUnless(_XT_OK, "xtquant 未可用，跳过 QMT 样例测试")
class TestQmtXtDataExamples(unittest.TestCase):
    def test_download_and_get_in(self):
        codes = ["518880.SH", "513880.SH"]
        period = "1d"
        end = dt.date.today(); start = end - dt.timedelta(days=15)
        for stock in codes:
            xtdata.download_history_data(stock_code=stock, period=period,
                                         start_time=start.strftime("%Y%m%d"), end_time=end.strftime("%Y%m%d"),
                                         incrementally=True)

            data = xtdata.get_market_data_in(field_list=[], stock_list=stock, period=period,
                                             start_time=start.strftime("%Y%m%d"), end_time=end.strftime("%Y%m%d"),
                                             count=-1, dividend_type='none', fill_data=False)
            for k in ["time", "open", "high", "low", "close"]:
                self.assertIn(k, data)
            self.assertTrue(hasattr(data["time"], "shape"))

    @unittest.skipUnless(os.getenv("RUN_QMT_REALTIME_TEST") == "1", "需要 RUN_QMT_REALTIME_TEST=1 且有实时行情")
    def test_subscribe_minimal_realtime(self):
        code = os.getenv("QMT_TEST_CODE", "518880.SH")
        period = "1m"
        got = {"n": 0}
        stop = threading.Event()
        def on_cb(*_a, **_kw):
            got["n"] += 1
            stop.set()
        xtdata.subscribe_quote(code, period, 2, on_cb)
        thr = threading.Thread(target=xtdata.run, daemon=True)
        thr.start()
        stop.wait(10)  # 最多等 10 秒
        self.assertGreaterEqual(got["n"], 1)