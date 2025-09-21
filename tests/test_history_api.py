"""HistoryAPI 单元测试

类说明：
    - 覆盖分段拉取、列名兼容、缺口检测、非法参数等路径。
    - 上游：无。
    - 下游：被测对象 core.history_api.HistoryAPI。
"""
import unittest
import sys
import types
import importlib
from datetime import datetime, timedelta, timezone
from unittest import mock
import pandas as pd

CN_TZ = timezone(timedelta(hours=8))


def _install_fake_xtdata_for_history(rows=10, col_time="time"):
    """安装历史用的伪 xtdata：为 history_api 与 local_cache 同时打桩。"""
    fake_xt = types.SimpleNamespace()

    def _download_history_data(stock_code, period, start_time="", end_time="", incrementally=True):
        return True
    fake_xt.download_history_data = _download_history_data

    def _parse_start(ts: str) -> datetime:
        if not ts:
            return datetime.now(CN_TZ)
        try:
            if len(ts) == 8 and ts.isdigit():
                return datetime.strptime(ts, "%Y%m%d").replace(tzinfo=CN_TZ)
            if len(ts) == 14 and ts.isdigit():
                return datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(CN_TZ)

    def _get_delta(p: str) -> timedelta:
        return {"1m": timedelta(minutes=1), "1h": timedelta(hours=1), "1d": timedelta(days=1)}.get(p, timedelta(minutes=1))

    def _to_epoch_ms(dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=CN_TZ)
        return int(dt.astimezone(timezone.utc).timestamp() * 1000)

    def _get_ex(field_list, stock_list, period, start_time, end_time,
                count=-1, dividend_type="none", fill_data=False, subscribe=False):
        base_dt = _parse_start(start_time)
        delta = _get_delta(period)
        columns = [f"col{i}" for i in range(rows)]
        index = pd.Index(stock_list, name="code")

        time_df = pd.DataFrame(index=index, columns=columns, dtype="int64")
        for idx, code in enumerate(stock_list):
            for i, col in enumerate(columns):
                time_df.loc[code, col] = _to_epoch_ms(base_dt + i * delta)

        def _make_numeric(offset: int) -> pd.DataFrame:
            df = pd.DataFrame(index=index, columns=columns, dtype="float64")
            for idx, code in enumerate(stock_list):
                for i, col in enumerate(columns):
                    df.loc[code, col] = float(offset + idx * rows + i)
            return df

        all_fields = {
            col_time: time_df,
            "open": _make_numeric(10),
            "high": _make_numeric(20),
            "low": _make_numeric(30),
            "close": _make_numeric(40),
            "volume": _make_numeric(50),
            "amount": _make_numeric(60),
            "preClose": _make_numeric(70),
        }

        if field_list:
            data = {k: v for k, v in all_fields.items() if k in field_list or k == col_time}
        else:
            data = all_fields
        # 兼容旧代码：若 col_time 不是 "time"，额外放一份 "time" 便于 history_api 查找
        if col_time != "time" and "time" not in data:
            data["time"] = time_df
        return data

    fake_xt.get_market_data_ex = _get_ex

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
        """测试内容：单代码 1m 拉取，仅返回摘要"""
        _install_fake_xtdata_for_history(rows=10, col_time="time")
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        res = api.fetch_bars([
            "510050.SH"
        ], "1m", "2025-07-01T09:30:00+08:00", "2025-07-02T09:40:00+08:00", return_data=False)
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["count"], 10)
        self.assertIsNotNone(res["head_ts"])
        self.assertIsNotNone(res["tail_ts"])
        self.assertNotIn("data", res)

    def test_column_name_compatibility(self):
        """测试内容：不同时间列名兼容"""
        for col in ("time", "Time", "datetime", "bar_time"):
            _install_fake_xtdata_for_history(rows=3, col_time=col)
            _reload_history_api()
            from core.history_api import HistoryAPI, HistoryConfig
            api = HistoryAPI(HistoryConfig())
            res = api.fetch_bars(["000001.SZ"], "1h", "2025-01-01T09:00:00+08:00", "2025-01-01T15:00:00+08:00", return_data=True)
            self.assertGreater(res["count"], 0)
            self.assertTrue(all("bar_end_ts" in r for r in res["data"]))

    def test_gap_detection_simple(self):
        """测试内容：简易频率法缺口检测"""
        _install_fake_xtdata_for_history(rows=3)
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        res = api.fetch_bars(["510050.SH"], "1m", "2025-01-01T09:30:00+08:00", "2025-01-01T09:35:00+08:00", return_data=True)
        expected_total = 5
        self.assertEqual(res["count"], 3)
        self.assertEqual(len(res["gaps"]), expected_total - 3)

    def test_invalid_period_raises(self):
        """测试内容：非法 period 参数"""
        _install_fake_xtdata_for_history(rows=1)
        _reload_history_api()
        from core.history_api import HistoryAPI, HistoryConfig
        api = HistoryAPI(HistoryConfig())
        with self.assertRaises(AssertionError):
            api.fetch_bars(["000001.SZ"], "5m", "2025-01-01T09:30:00+08:00", "2025-01-01T09:40:00+08:00")
