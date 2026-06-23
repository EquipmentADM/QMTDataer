"""脚本层（CLI）测试

类说明：
    - 覆盖 backfill_history 与 run_realtime_bridge 的参数解析与装配调用；
    - 上游：无；
    - 下游：scripts.backfill_history / scripts.run_realtime_bridge。
"""
import unittest
from unittest import mock
import sys
import os
import tempfile
import io
from contextlib import redirect_stdout


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

    def test_run_realtime_control_forces_blank_control_mode(self):
        """测试内容：实时控制面入口强制空白启动
        目的：验证入口会清空初始订阅、开启控制面并关闭启动预热。
        输入：配置文件内包含初始 codes 且 control.enabled=false。
        预期输出：传入 run_from_config 的配置已切换为空白控制面模式。
        """
        y = """
qmt:
  mode: none
redis:
  host: 127.0.0.1
  port: 6379
  topic: xt:topic:bar
subscription:
  codes: [510050.SH]
  periods: [1m]
  mode: close_only
  preload_days: 3
control:
  enabled: false
  channel: xt:ctrl:sub
  ack_prefix: xt:ctrl:ack
"""
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        with open(path, "w", encoding="utf-8") as f:
            f.write(y)

        from scripts import run_realtime_control
        with mock.patch.object(run_realtime_control, "run_from_config") as mrun:
            run_realtime_control.main(["--config", path])

        cfg = mrun.call_args.args[0]
        self.assertEqual(cfg.subscription.codes, [])
        self.assertEqual(cfg.subscription.preload_days, 0)
        self.assertTrue(cfg.control.enabled)
        self.assertEqual(cfg.control.channel, "xt:ctrl:sub")
        os.remove(path)

    def test_run_realtime_control_prefers_control_config(self):
        """测试内容：实时控制面入口默认优先读取 realtime_control.yml
        目的：避免空白启动继续依赖 run_config.yml。
        输入：不传 --config，模拟 control 配置存在。
        预期输出：load_config 首次命中 config/realtime_control.yml。
        """
        from scripts import run_realtime_control

        fake_cfg = mock.Mock()
        fake_cfg.subscription.codes = []
        fake_cfg.subscription.preload_days = 0
        fake_cfg.subscription.periods = ["1m"]
        fake_cfg.control.enabled = True
        fake_cfg.control.channel = "xt:ctrl:sub"
        fake_cfg.control.ack_prefix = "xt:ctrl:ack"
        fake_cfg.redis.topic = "xt:topic:bar"

        with mock.patch.object(run_realtime_control.Path, "exists", return_value=True), \
             mock.patch.object(run_realtime_control, "load_config", return_value=fake_cfg) as mocked_load, \
             mock.patch.object(run_realtime_control, "run_from_config"):
            run_realtime_control.main([])

        loaded_path = mocked_load.call_args.args[0].replace("\\", "/")
        self.assertTrue(loaded_path.endswith("config/realtime_control.yml"))

    def test_show_realtime_status_prints_active_subs(self):
        """测试内容：状态查询脚本打印当前活跃订阅
        目的：验证 Redis 查询入口能输出 ref_count 与最近发布时间。
        输入：mock status ACK。
        预期输出：返回码为 0，输出包含 code/period/ref_count。
        """
        from scripts import show_realtime_status

        fake_payload = {
            "ok": True,
            "action": "status",
            "status": {
                "subs": [{"code": "510050.SH", "period": "1m", "ref_count": 2}],
                "last_published": {"510050.SH|1m": 123.0},
            },
            "subs": ["sub-a"],
        }

        with mock.patch.object(show_realtime_status, "query_status", return_value=fake_payload):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = show_realtime_status.main(["--strategy-id", "demo"])

        output = buf.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn("510050.SH 1m ref_count=2", output)
        self.assertIn("510050.SH|1m: 123.0", output)
