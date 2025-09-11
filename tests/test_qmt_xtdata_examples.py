# -*- coding: utf-8 -*-
"""QMT xtdata 样例测试（仅在本机 QMT/MiniQMT 可用时运行）

测试项目：
1) 历史补齐 + 获取行情（优先 get_market_data_ex，回退 get_market_data_in）
   目的：验证“先补后取”的 QMT 历史获取路径
   输入：codes=[518880.SH, 513880.SH], period=1d, 近 15 天
   预期：返回 dict，含 'time/open/high/low/close' 等 key；time 具 DataFrame 形态

2) （可选，需 RUN_QMT_REALTIME_TEST=1）最新分笔接口 get_full_tick 基本可调用
"""
import os
import unittest
import datetime as dt
from dataclasses import field

try:
    from xtquant import xtdata
    _XT_OK = True
except Exception:
    _XT_OK = False


@unittest.skipUnless(_XT_OK, "xtquant 未可用，跳过 QMT 样例测试")
class TestQmtXtDataExamples(unittest.TestCase):
    def test_download_and_get(self):
        """测试内容：历史补齐 + 获取行情
        目的：验证“先补后取”的历史路径
        输入：两只标的、1d、近 15 天
        预期输出：返回 dict 且包含关键字段
        """
        codes = ["518880.SH", "513880.SH"]
        period = "1d"
        end = dt.date.today()
        start = end - dt.timedelta(days=15)

        # 逐标的补齐（download_history_data 仅接受 str）
        for code in codes:
            xtdata.download_history_data(
                code,
                period=period,
                start_time=start.strftime("%Y%m%d"),
                end_time=end.strftime("%Y%m%d"),
                incrementally=True
            )

        field_list = ["time", "open", "high", "low", "close"]

        # 获取：优先 ex，回退 in
        if hasattr(xtdata, "get_market_data_ex"):
            data = xtdata.get_market_data_ex(
                field_list=[], stock_list=codes, period=period,
                start_time=start.strftime("%Y%m%d"),
                end_time=end.strftime("%Y%m%d"),
                count=-1,
                dividend_type="none",
                fill_data=False,
                # subscribe=False  # 避免在测试中产生订阅副作用
            )
        elif hasattr(xtdata, "get_market_data_in"):
            data = xtdata.get_market_data_in(
                field_list=field_list,
                stock_list=codes,
                period=period,
                start_time=start.strftime("%Y%m%d"),
                end_time=end.strftime("%Y%m%d"),
                count=-1,
                dividend_type="none",
                fill_data=False
            )
        else:
            self.skipTest("xtdata 不支持 get_market_data_ex/in")
        for stock in codes:
            for k in field_list:
                self.assertIn(k, data[stock])
            # time 应具备 DataFrame 形态（弱断言）
            self.assertTrue(hasattr(data[stock]["time"], "shape"))

    @unittest.skipUnless(os.getenv("RUN_QMT_REALTIME_TEST") == "1", "需要 RUN_QMT_REALTIME_TEST=1 且有实时行情")
    def test_get_full_tick_basic(self):
        """测试内容：get_full_tick 最小可调用
        目的：在盘中验证可获得 dict 结构
        输入：codes（环境变量 QMT_TEST_CODE 或默认 518880.SH）
        预期输出：返回 dict（内容可能为空，视行情而定）
        """
        codes = [os.getenv("QMT_TEST_CODE", "518880.SH")]
        if hasattr(xtdata, "get_full_tick"):
            data = xtdata.get_full_tick(codes)
            print(f"xtdata.get_full_tick(codes): {data}")
            self.assertIsInstance(data, dict)
        else:
            self.skipTest("xtdata 不支持 get_full_tick")
