# -*- coding: utf-8 -*-
"""
虚拟实时行情空白控制面入口测试。
"""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock


class TestRealtimeMockControlScript(unittest.TestCase):
    """校验 `run_realtime_mock_control.py` 的配置装配行为。"""

    def test_mock_control_forces_blank_mock_mode(self) -> None:
        """
        验证入口会强制切换为虚拟空白控制面。

        Returns:
            None: 通过断言验证行为。
        """

        yml = """
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
mock:
  enabled: false
  step_seconds: 5
  source: qmt
control:
  enabled: false
  channel: xt:ctrl:sub
  ack_prefix: xt:ctrl:ack
"""
        fd, path = tempfile.mkstemp(suffix=".yml")
        os.close(fd)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(yml)

            from scripts import run_realtime_mock_control

            with mock.patch.object(run_realtime_mock_control, "run_from_config") as mocked_run:
                run_realtime_mock_control.main(["--config", path, "--step-seconds", "0.2", "--seed", "7"])

            cfg = mocked_run.call_args.args[0]
            self.assertEqual(cfg.subscription.codes, [])
            self.assertEqual(cfg.subscription.preload_days, 0)
            self.assertTrue(cfg.control.enabled)
            self.assertTrue(cfg.mock.enabled)
            self.assertEqual(cfg.mock.source, "mock")
            self.assertEqual(cfg.mock.step_seconds, 0.2)
            self.assertEqual(cfg.mock.seed, 7)
        finally:
            os.remove(path)

    def test_mock_control_prefers_mock_control_config(self) -> None:
        """
        验证默认优先读取 `config/realtime_mock_control.yml`。

        Returns:
            None: 通过断言验证行为。
        """

        from scripts import run_realtime_mock_control

        fake_cfg = mock.Mock()
        fake_cfg.subscription.codes = []
        fake_cfg.subscription.preload_days = 0
        fake_cfg.subscription.periods = ["1m"]
        fake_cfg.control.enabled = True
        fake_cfg.control.channel = "xt:ctrl:sub"
        fake_cfg.control.ack_prefix = "xt:ctrl:ack"
        fake_cfg.redis.topic = "xt:topic:bar"
        fake_cfg.mock.enabled = True
        fake_cfg.mock.source = "mock"
        fake_cfg.mock.step_seconds = 5.0
        fake_cfg.mock.seed = None

        with mock.patch.object(run_realtime_mock_control.Path, "exists", return_value=True), \
             mock.patch.object(run_realtime_mock_control, "load_config", return_value=fake_cfg) as mocked_load, \
             mock.patch.object(run_realtime_mock_control, "run_from_config"):
            run_realtime_mock_control.main([])

        loaded_path = mocked_load.call_args.args[0].replace("\\", "/")
        self.assertTrue(loaded_path.endswith("config/realtime_mock_control.yml"))

    def test_btlive_mock_pool_defaults_to_five_seconds(self) -> None:
        """
        验证 BTLive 常开 Mock 行情入口默认使用 5 秒节奏。

        Returns:
            None: 通过断言验证默认配置。
        """

        from scripts import run_btlive_mock_1min_pool

        fake_cfg = mock.Mock()
        fake_cfg.subscription.codes = []
        fake_cfg.subscription.periods = []
        fake_cfg.subscription.mode = ""
        fake_cfg.subscription.preload_days = 3
        fake_cfg.redis.topic = ""
        fake_cfg.mock.enabled = False
        fake_cfg.mock.step_seconds = 60.0
        fake_cfg.mock.source = "qmt"
        fake_cfg.mock.seed = None
        fake_cfg.control.enabled = False

        with mock.patch.object(run_btlive_mock_1min_pool, "_load_base_config", return_value=fake_cfg):
            cfg = run_btlive_mock_1min_pool.build_btlive_mock_config()

        self.assertEqual(cfg.subscription.periods, ["1m"])
        self.assertEqual(cfg.subscription.mode, "close_only")
        self.assertTrue(cfg.mock.enabled)
        self.assertEqual(cfg.mock.source, "mock")
        self.assertEqual(cfg.mock.step_seconds, 5.0)


if __name__ == "__main__":
    unittest.main()
