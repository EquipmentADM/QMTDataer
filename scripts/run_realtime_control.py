# -*- coding: utf-8 -*-
"""
实时行情控制面空白启动入口。

职责：
    - 复用现有 YAML 配置中的 Redis、日志、QMT 与推送 topic 参数。
    - 清空初始订阅标的，服务启动后只等待 Redis 控制面订阅命令。
    - 强制开启 ControlPlane，确保可以接收 subscribe、unsubscribe、status。

数据契约：
    - 配置文件结构与 `scripts/run_with_config.py` 使用的 AppConfig 保持一致。
    - 启动后不主动订阅任何 `(code, period)`，订阅来源必须是 Redis 控制通道。
    - 本入口启动后，当前 QMTD 实例是同一 Redis topic 的真实行情权威源。
    - 下游订阅不能选择真/假行情，真/假由 QMTD 启动入口决定。
    - 不要与 `run_realtime_mock_control.py` 共用同一 topic 同时运行。

外部系统：
    - MiniQMT / xtdata：真实行情连接。
    - Redis：控制命令、ACK 与行情推送。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.config_loader import load_config
from scripts.run_with_config import build_demo_app_config, run_from_config


def build_control_config(config_path: Optional[str] = None):
    """
    构造真实行情空白控制面配置。

    Args:
        config_path (Optional[str]): 可选 YAML 配置文件路径。未提供时优先使用
            `config/realtime_control.yml`，其次回退到 `config/run_config.yml`，
            再不存在则使用 Demo 配置。

    Returns:
        AppConfig: 已清空初始订阅、开启控制面并关闭启动预热的配置对象。
    """
    cfg_path = Path(config_path) if config_path else None
    if cfg_path is None:
        default_candidates = [
            BASE_DIR / "config/realtime_control.yml",
            BASE_DIR / "config/run_config.yml",
        ]
        cfg_path = next((item for item in default_candidates if item.exists()), None)

    if cfg_path and cfg_path.exists():
        cfg = load_config(str(cfg_path), allow_empty_subscription=True)
        print(f"[CTRL] 加载配置：{cfg_path}")
    else:
        cfg = build_demo_app_config()
        print("[CTRL] 未找到配置文件，使用 Demo 配置并切换为空白控制面模式。")

    cfg.subscription.codes = []
    cfg.subscription.preload_days = 0
    cfg.control.enabled = True
    return cfg


def main(argv: Optional[list[str]] = None) -> None:
    """
    启动实时行情控制面并进入阻塞运行。

    Args:
        argv (Optional[list[str]]): 命令行参数，测试时可显式传入。

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="QMTD 实时行情控制面空白启动入口")
    parser.add_argument(
        "--config",
        help="YAML 配置路径；默认优先使用 config/realtime_control.yml",
        required=False,
    )
    args = parser.parse_args(argv)

    cfg = build_control_config(args.config)
    print(
        "[CTRL] 真实行情空白启动：source=real initial_codes=0 "
        f"periods={cfg.subscription.periods} ctrl={cfg.control.channel} ack={cfg.control.ack_prefix} "
        f"topic={cfg.redis.topic}"
    )
    print("[CTRL] 唯一行情器规则：请勿与 Mock 控制入口共用同一 topic 同时运行。")
    run_from_config(cfg)


if __name__ == "__main__":
    main()
