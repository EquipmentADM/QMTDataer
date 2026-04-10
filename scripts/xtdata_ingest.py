# -*- coding: utf-8 -*-
"""
xtdata 入库参数化主入口，统一管理三种入库模式。

Responsibilities:
    - 提供 full-download、full-backfill、recent-backfill 三个子命令。
    - 支持覆盖 symbols、cycles、root、start、end 等运行参数。
    - 复用 core.ingest_runner 的统一执行逻辑，避免脚本重复维护。

Data Contract:
    - symbols、cycles 使用逗号分隔字符串输入，内部会转换为 tuple[str, ...]。
    - 未显式传入的参数沿用模式默认值。

Internal Dependencies:
    - core.ingest_runner

External Systems:
    - xtquant.xtdata（或兼容 xtdata）
    - 本地文件系统
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ingest_runner import list_profile_names, run_profile


def _split_csv(text: Optional[str]) -> Optional[tuple[str, ...]]:
    """
    将逗号分隔字符串转换为元组。

    Args:
        text (Optional[str]): 输入文本。

    Returns:
        Optional[tuple[str, ...]]: 非空时返回元组，空输入返回 None。
    """
    if not text:
        return None
    items = tuple(item.strip() for item in text.split(",") if item.strip())
    return items or None


def _build_parser() -> argparse.ArgumentParser:
    """
    构建命令行参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器对象。
    """
    parser = argparse.ArgumentParser(description="xtdata 入库主入口")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    for mode in list_profile_names():
        sub = subparsers.add_parser(mode, help=f"运行 {mode} 模式")
        sub.add_argument("--symbols", help="逗号分隔标的列表")
        sub.add_argument("--cycles", help="逗号分隔周期列表")
        sub.add_argument("--root", help="输出根目录")
        sub.add_argument("--market", help="市场目录名")
        sub.add_argument("--specific", help="子目录或合成标记")
        sub.add_argument("--start", help="起始时间，xtdata 格式")
        sub.add_argument("--end", help="结束时间，xtdata 格式")
        sub.add_argument("--lookback", type=int, help="auto_start 场景回溯 bar 数")
        sub.add_argument("--skip-download", action="store_true", help="跳过 download_history_data")
        auto_group = sub.add_mutually_exclusive_group()
        auto_group.add_argument("--auto-start", action="store_true", help="启用 auto_start")
        auto_group.add_argument("--no-auto-start", action="store_true", help="禁用 auto_start")
        merge_group = sub.add_mutually_exclusive_group()
        merge_group.add_argument("--merge", action="store_true", help="启用 merge 模式写入")
        merge_group.add_argument("--no-merge", action="store_true", help="禁用 merge 模式写入")
    return parser


def _build_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """
    根据命令行参数生成 profile 覆盖参数。

    Args:
        args (argparse.Namespace): 解析后的参数对象。

    Returns:
        dict[str, Any]: 可直接传给 run_profile 的覆盖参数。
    """
    overrides: dict[str, Any] = {}
    symbols = _split_csv(args.symbols)
    cycles = _split_csv(args.cycles)
    if symbols:
        overrides["symbols"] = symbols
    if cycles:
        overrides["cycles"] = cycles

    for field in ("root", "market", "specific", "start", "end", "lookback"):
        value = getattr(args, field, None)
        if value is not None:
            overrides[field] = value

    if getattr(args, "skip_download", False):
        overrides["skip_download"] = True
    if getattr(args, "auto_start", False):
        overrides["auto_start"] = True
    if getattr(args, "no_auto_start", False):
        overrides["auto_start"] = False
    if getattr(args, "merge", False):
        overrides["merge"] = True
    if getattr(args, "no_merge", False):
        overrides["merge"] = False
    return overrides


def main(argv: Optional[list[str]] = None) -> int:
    """
    命令行入口。

    Args:
        argv (Optional[list[str]]): 可选参数列表，默认读取 sys.argv。

    Returns:
        int: 成功返回 0，失败返回 2。
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)
    overrides = _build_overrides(args)
    try:
        result = run_profile(args.mode, **overrides)
    except Exception as exc:
        logging.error("入库任务失败: %s", exc)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
