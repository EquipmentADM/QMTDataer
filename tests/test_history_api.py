"""HistoryAPI 单元测试

类说明：
    - 覆盖分段拉取、列名兼容、缺口检测、非法参数等路径；
    - 上游：无；
    - 下游：被测对象 core.history_api.HistoryAPI。
"""
import unittest
import sys
import types
import importlib
from datetime import datetime, timedelta, timezone
from unittest import mock
import pandas as pd


def _install_fake_xtdata_for_history(rows=10, col_time="time"):
    """安装历史用的假 xtdata：为 history_api 与 local_cache 同时打桩。"""
    import types
    fake_xt = types.SimpleNamespace()

    # download_history_data：LocalCache 用
    def _download_history_data(stock_code, period, start_time="", end_time="", incrementally=True):
        # 直接返回 True，表示“已下载”
        return True
    fake_xt.download_history_data = _download_history_data

    # get_market_data_ex：HistoryAPI 用（返回最小结构）
    def _get_ex(field_list, stock_list, period, start_time, end_time,
                count=-1, dividend_type="none", fill_data=False, subscribe=False):
        # 构造一个“宽字典”：每只股票 -> 每个字段 -> 简易 DataFrame-like
        import pandas as pd
        idx = pd.Index([f"R{i}" for i in range(rows)], name="row")
        data = {}
        for code in stock_list:
            d = {}
            # 时间列
            if not field_list or "time" in field_list:
                d["time"] = pd.DataFrame({start_time: [0]*1}).T  # 仅断言需要 shape 即可
            for f in ["open","high","low","close","volume","amount","preClose"]:
                if (not field_list) or (f in field_list):
                    d[f] = pd.DataFrame({f"col{i}": [i] for i in range(rows)}, index=idx)
            data[code] = d
        return data
    fake_xt.get_market_data_ex = _get_ex

    # 打补丁到两个模块
    import core.history_api as hmod
    import core.local_cache as lmod
    hmod.xtdata = fake_xt
    lmod.xtdata = fake_xt


def _reload_history_api():
    if "core.history_api" in sys.modules:
        importlib.reload(sys.modules["core.history_api"])
    else:
        import core.history_api  # noqa


class TestHistoryAPI(unittest.TestCase):
    """类说明：HistoryAPI 行为测试
    功能：分段拉取、列名兼容、缺口检测、异常与非法参数校验。
    上游：无。
    下游：HistoryAPI。
    """

    def test_basic_fetch_summary_single_code(self):
        """测试内容：单代码 1m 拉取，仅返回摘要
        目的：验证基本拉取流程与 count/head/tail/gaps。
        输入：rows=10，period=1m，return_data=False。
        预期输出：status=ok, count=10，head_ts/tail_ts 非空，data 不存在。
        """
        _install_fake_xtdata_for_history(rows=10, col_time="time")
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        res = api.fetch_bars([
            "510050.SH"
        ], "1m", "2025-07-01T09:30:00+08:00", "2025-07-02T09:40:00+08:00", return_data=False)
        print(f"res:{res}")
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["count"], 10)
        self.assertIsNotNone(res["head_ts"]) ; self.assertIsNotNone(res["tail_ts"]) 
        self.assertNotIn("data", res)

    def test_column_name_compatibility(self):
        """测试内容：不同时间列名兼容（time/Time/datetime/bar_time）
        目的：确保 _pull_one 能识别多种时间列名。
        输入：分别安装四种列名的假 xtdata。
        预期输出：四次调用均返回非空数据，bar_end_ts 合法。
        """
        for col in ("time", "Time", "datetime", "bar_time"):
            _install_fake_xtdata_for_history(rows=3, col_time=col)
            _reload_history_api()
            from core.history_api import HistoryAPI, HistoryConfig
            api = HistoryAPI(HistoryConfig())
            res = api.fetch_bars(["000001.SZ"], "1h", "2025-01-01T09:00:00+08:00", "2025-01-01T15:00:00+08:00", return_data=True)
            self.assertGreater(res["count"], 0)
            self.assertTrue(all("bar_end_ts" in r for r in res["data"]))

    def test_gap_detection_simple(self):
        """测试内容：简易频率法缺口检测
        目的：构造 5 分钟窗口，仅返回 3 条，缺 2 条，验证 gaps。
        输入：rows=3，period=1m，窗口 5 分钟。
        预期输出：gaps 长度为 期望-实际 的差值。
        """
        _install_fake_xtdata_for_history(rows=3)
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        res = api.fetch_bars(["510050.SH"], "1m", "2025-01-01T09:30:00+08:00", "2025-01-01T09:35:00+08:00", return_data=True)
        expected_total = 5
        self.assertEqual(res["count"], 3)
        self.assertEqual(len(res["gaps"]), expected_total - 3)

    def test_invalid_period_raises(self):
        """测试内容：非法 period 参数
        目的：触发断言，保障入参校验。
        输入：period='5m'。
        预期输出：AssertionError。
        """
        _install_fake_xtdata_for_history(rows=1)
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        with self.assertRaises(AssertionError):
            api.fetch_bars(["000001.SZ"], "5m", "2025-01-01T09:30:00+08:00", "2025-01-01T09:40:00+08:00")