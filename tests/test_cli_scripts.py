"""脚本层（CLI）测试

类说明：
    - 覆盖 backfill_history 与 run_realtime_bridge 的参数解析与装配调用；
    - 上游：无；
    - 下游：scripts.backfill_history / scripts.run_realtime_bridge。
"""
import unittest
from unittest import mock
import sys


class TestCLIScripts(unittest.TestCase):
    """类说明：脚本装配测试
    功能：验证 CLI 参数到核心对象的方法调用链；避免真实阻塞与外部依赖。
    上游：无。
    下游：脚本入口模块。
    """

    def test_backfill_history_cli_chain(self):
        """测试内容：backfill_history 解析参数并调用 fetch_bars
        目的：验证 CLI 到 HistoryAPI 的调用链。
        输入：传入 --codes/--period/--start/--end/--dividend/--return-data。
        预期输出：QMTConnector.listen_and_connect 与 HistoryAPI.fetch_bars 各调用 1 次。
        """
        argv = [
            "prog", "--codes", "000001.SZ,600000.SH",
            "--period", "1m",
            "--start", "2025-01-01T09:30:00+08:00",
            "--end",   "2025-01-01T15:00:00+08:00",
            "--dividend", "none",
            "--return-data",
        ]
        with mock.patch("sys.argv", argv), \
             mock.patch("core.qmt_connector.QMTConnector.listen_and_connect") as mconn, \
             mock.patch("core.history_api.HistoryAPI.fetch_bars", return_value={"status": "ok", "count": 0}) as mfetch:
            from scripts import backfill_history  # noqa: F401  # 导入触发 main()
            # 通过 if __name__ == '__main__' 保护，直接 import 不会执行 main；此处模拟为直接调用
            backfill_history.main()
            self.assertEqual(mconn.call_count, 1)
            self.assertEqual(mfetch.call_count, 1)

    def test_run_realtime_bridge_cli_chain(self):
        """测试内容：run_realtime_bridge 解析参数并启动实时服务
        目的：验证 CLI 到 RealtimeSubscriptionService 的调用链。
        输入：传入 --codes/--periods/--mode/--redis-* 参数。
        预期输出：QMTConnector.listen_and_connect / PubSubPublisher 构造 / RealtimeSubscriptionService.run_forever 被调用。
        """
        argv = [
            "prog",
            "--codes", "000001.SZ,600000.SH",
            "--periods", "1m,1d",
            "--mode", "close_only",
            "--redis-host", "127.0.0.1",
            "--redis-port", "6379",
            "--topic", "xt:topic:bar",
        ]
        with mock.patch("sys.argv", argv), \
             mock.patch("core.qmt_connector.QMTConnector.listen_and_connect") as mconn, \
             mock.patch("core.pubsub_publisher.PubSubPublisher.__init__", return_value=None) as mpub, \
             mock.patch("core.realtime_service.RealtimeSubscriptionService.run_forever") as mrun:
            from scripts import run_realtime_bridge
            run_realtime_bridge.main()
            self.assertEqual(mconn.call_count, 1)
            self.assertEqual(mpub.call_count, 1)
            self.assertEqual(mrun.call_count, 1)
