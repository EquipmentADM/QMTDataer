# -*- coding: utf-8 -*-
"""
虚拟实时行情控制面空白启动入口。

职责：
    - 与 `run_realtime_control.py` 对称，但强制启用 Mock 行情。
    - 清空初始订阅标的，启动后只等待 Redis 控制面订阅命令。
    - 用于 BTLive 或策略端在无真实 QMT 行情需求时按协议申请虚拟行情。

说明：
    该脚本启动后整个 QMTD 实例都是虚拟行情模式。BTLive 的订阅请求当前不区分
    真/假行情，真假由 QMTD 启动入口决定。
    同一 Redis 行情 topic 只能有一个权威行情源，不要与真实行情控制入口共用
    同一 topic 同时运行。
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


def build_mock_control_config(
    config_path: Optional[str] = None,
    *,
    step_seconds: Optional[float] = None,
    seed: Optional[int] = None,
):
    """
    构造虚拟行情空白控制面配置。

    Args:
        config_path (Optional[str]): 可选 YAML 配置路径。
        step_seconds (Optional[float]): 可选 Mock 生成节奏覆盖值。
        seed (Optional[int]): 可选随机种子覆盖值。

    Returns:
        AppConfig: 已清空初始订阅、开启控制面、强制启用 Mock 的配置对象。
    """
    cfg_path = Path(config_path) if config_path else None
    if cfg_path is None:
        default_candidates = [
            BASE_DIR / "config/realtime_mock_control.yml",
            BASE_DIR / "config/realtime_control.yml",
            BASE_DIR / "config/run_config.yml",
        ]
        cfg_path = next((item for item in default_candidates if item.exists()), None)

    if cfg_path and cfg_path.exists():
        cfg = load_config(str(cfg_path), allow_empty_subscription=True)
        print(f"[MOCK-CTRL] 加载配置：{cfg_path}")
    else:
        cfg = build_demo_app_config()
        print("[MOCK-CTRL] 未找到配置文件，使用 Demo 配置并切换为虚拟空白控制面模式。")

    cfg.subscription.codes = []
    cfg.subscription.preload_days = 0
    cfg.control.enabled = True
    cfg.mock.enabled = True
    cfg.mock.source = "mock"
    if step_seconds is not None:
        cfg.mock.step_seconds = float(step_seconds)
    if seed is not None:
        cfg.mock.seed = int(seed)
    return cfg


def main(argv: Optional[list[str]] = None) -> None:
    """
    启动虚拟实时行情控制面并进入阻塞运行。

    Args:
        argv (Optional[list[str]]): 命令行参数，测试时可显式传入。

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="QMTD 虚拟实时行情控制面空白启动入口")
    parser.add_argument(
        "--config",
        help="YAML 配置路径；默认优先使用 config/realtime_mock_control.yml",
        required=False,
    )
    parser.add_argument("--step-seconds", type=float, help="Mock 生成节奏，单位秒")
    parser.add_argument("--seed", type=int, help="Mock 随机种子")
    args = parser.parse_args(argv)

    cfg = build_mock_control_config(
        args.config,
        step_seconds=args.step_seconds,
        seed=args.seed,
    )
    print(
        "[MOCK-CTRL] 虚拟行情空白启动：source=mock initial_codes=0 "
        f"periods={cfg.subscription.periods} ctrl={cfg.control.channel} ack={cfg.control.ack_prefix} "
        f"topic={cfg.redis.topic} step={cfg.mock.step_seconds}s"
    )
    print("[MOCK-CTRL] 唯一行情器规则：该实例收到的订阅都会生成 Mock 行情，请勿与真实入口共用同一 topic。")
    run_from_config(cfg)


if __name__ == "__main__":
    main()
