# -*- coding: utf-8 -*-
"""
BTLive 标准 1min Mock 实时行情常开入口。

职责：
    - 固定推送 BTLive/QMTD 联调用的 5 个常用标的。
    - 强制启用 Mock 行情，不依赖真实 xtdata 实时订阅。
    - 使用 close_only 模式，默认每 60 秒推送一次完整 1min bar。
    - 保持 Redis topic 与真实实时行情一致，便于 BTLive 无缝切换测试。

使用方式：
    直接在 IDE 中右键运行本文件即可。
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from core.config_loader import load_config
from scripts.run_with_config import build_demo_app_config, run_from_config


BTLIVE_MOCK_CODES = [
    "510900.SH",
    "513130.SH",
    "510050.SH",
    "518880.SH",
    "513880.SH",
]


def _load_base_config(config_path: Optional[str]):
    """
    加载基础配置。

    说明：
        - 优先使用显式传入的配置。
        - 未传入时复用 `config/run_config.yml`。
        - 配置不存在时回退 Demo 配置，便于临时环境直接启动。
    """
    if config_path:
        path = Path(config_path)
        print(f"[BTLIVE-MOCK] 加载配置：{path}")
        return load_config(str(path))

    default_path = BASE_DIR / "config" / "run_config.yml"
    if default_path.exists():
        print(f"[BTLIVE-MOCK] 加载默认配置：{default_path}")
        return load_config(str(default_path))

    print("[BTLIVE-MOCK] 未找到 config/run_config.yml，使用 Demo 配置。")
    return build_demo_app_config()


def build_btlive_mock_config(
    *,
    config_path: Optional[str] = None,
    step_seconds: float = 60.0,
    seed: Optional[int] = None,
):
    """
    构造 BTLive 标准 1min Mock 行情配置。

    Args:
        config_path (Optional[str]): 可选基础 yml 配置路径。
        step_seconds (float): Mock 生成节奏，默认 60 秒。
        seed (Optional[int]): 随机种子，默认沿用配置。

    Returns:
        AppConfig: 已覆盖为 BTLive Mock 测试池的配置对象。
    """
    cfg = _load_base_config(config_path)

    cfg.subscription.codes = list(BTLIVE_MOCK_CODES)
    cfg.subscription.periods = ["1m"]
    cfg.subscription.mode = "close_only"
    cfg.subscription.preload_days = 0

    cfg.redis.topic = "xt:topic:bar"

    cfg.mock.enabled = True
    cfg.mock.step_seconds = float(step_seconds)
    cfg.mock.source = "mock"
    if seed is not None:
        cfg.mock.seed = int(seed)

    # 保留控制面，便于运行中通过 Redis status 查询当前活跃订阅。
    cfg.control.enabled = True
    return cfg


def main(argv: Optional[list[str]] = None) -> None:
    """
    脚本入口。

    Args:
        argv (Optional[list[str]]): 测试或命令行传入的参数列表。

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="BTLive 标准 1min Mock 行情常开入口")
    parser.add_argument("--config", help="可选基础 YAML 配置路径", required=False)
    parser.add_argument("--step-seconds", type=float, default=60.0, help="Mock 推送节奏，默认 60 秒")
    parser.add_argument("--seed", type=int, help="可选随机种子", required=False)
    args = parser.parse_args(argv)

    cfg = build_btlive_mock_config(
        config_path=args.config,
        step_seconds=args.step_seconds,
        seed=args.seed,
    )
    print(
        "[BTLIVE-MOCK] 启动标准 1min Mock 行情："
        f"codes={cfg.subscription.codes} periods={cfg.subscription.periods} "
        f"mode={cfg.subscription.mode} topic={cfg.redis.topic} "
        f"step={cfg.mock.step_seconds}s timestamp=local_naive_iso"
    )
    run_from_config(cfg)


if __name__ == "__main__":
    main()
