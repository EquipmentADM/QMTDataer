# -*- coding: utf-8 -*-
"""运行入口（强制 Mock 行情版本）

功能：
    - 与 `scripts/run_with_config.py` 行为类似，但无论配置文件是否开启 mock，都强制启用随机游走行情；
    - 支持通过命令行覆盖 mock 参数（基准价、波动率、步长、种子、成交量均值/方差）；
    - 便于在无 QMT/MiniQMT 环境下直接加载现有配置，验证下游消费链路。
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from core.config_loader import load_config
from scripts.run_with_config import run_from_config, build_demo_app_config, BASE_DIR


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="QMT realtime bridge (mock forced)")
    parser.add_argument("--config", help="YAML 配置路径，可复用真实环境配置", required=False)
    parser.add_argument("--base-price", type=float, help="Mock 基准价（覆盖配置）")
    parser.add_argument("--volatility", type=float, help="Mock 对数收益波动率（覆盖配置）")
    parser.add_argument("--step-seconds", type=float, help="Mock 生成节奏（秒）")
    parser.add_argument("--seed", type=int, help="Mock 随机种子")
    parser.add_argument("--volume-mean", type=float, help="Mock 成交量均值")
    parser.add_argument("--volume-std", type=float, help="Mock 成交量标准差")
    args = parser.parse_args(argv)

    cfg_path: Optional[Path] = None
    cfg = None

    if args.config:
        cfg_path = Path(args.config)
        cfg = load_config(str(cfg_path))
    else:
        default_cfg = BASE_DIR / "config/run_config.yml"
        if default_cfg.exists():
            cfg_path = default_cfg
            cfg = load_config(str(default_cfg))
        else:
            print("[MOCK] 未提供 --config，且未找到 config/run_config.yml，改用 Demo 默认配置。")
            cfg = build_demo_app_config()

    # 强制启用 mock 行情
    cfg.mock.enabled = True
    if args.base_price is not None:
        cfg.mock.base_price = args.base_price
    if args.volatility is not None:
        cfg.mock.volatility = args.volatility
    if args.step_seconds is not None:
        cfg.mock.step_seconds = args.step_seconds
    else:
        cfg.mock.step_seconds = 5.0
    if args.seed is not None:
        cfg.mock.seed = args.seed
    if args.volume_mean is not None:
        cfg.mock.volume_mean = args.volume_mean
    if args.volume_std is not None:
        cfg.mock.volume_std = args.volume_std

    if cfg_path:
        print(f"[MOCK] 加载配置：{cfg_path}")
    else:
        print("[MOCK] 使用 Demo 默认配置")
    print(f"[MOCK] Mock 参数：base={cfg.mock.base_price} vol={cfg.mock.volatility} step={cfg.mock.step_seconds}s seed={cfg.mock.seed}")
    run_from_config(cfg)


if __name__ == "__main__":
    main()
