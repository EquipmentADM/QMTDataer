# -*- coding: utf-8 -*-
"""
FD backend 写入结果的 dashboard service 验收脚本。

Responsibilities:
    - 调用 FD DashboardService 扫描 QMTD 写入后的目标文件。
    - 对库存条目执行文件巡检与质量检查。
    - 为 QMTD 切换 fd backend 提供可重复的轻量验收入口。

Data Contract:
    - 只读取本地 FD 数据库，不写入、不连接 miniQMT、不连接 Redis。
    - 默认检查本轮验证样本：510050.SH、518880.SH、513880.SH、rb 主力连续。

External Systems:
    - 新 FD 原型项目 src 目录。
    - 本地 FD 数据库目录。
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Iterable


DEFAULT_FD_REPO = Path(r"D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型")
DEFAULT_FD_ROOT = Path(r"D:\Work\Quant\financial_database")
DEFAULT_TARGETS = (
    "SS_stock_data:510050.SH",
    "SS_stock_data:518880.SH",
    "SS_stock_data:513880.SH",
    "Futures_data:rb",
)


def _ensure_fd_src(fd_repo: str) -> Path:
    """
    确保 fd_core/fd_services 可以导入。

    Args:
        fd_repo (str): FD 项目根目录或 src 目录。

    Returns:
        Path: 已加入 sys.path 的 src 目录。
    """

    repo_path = Path(fd_repo)
    src_path = repo_path if repo_path.name == "src" else repo_path / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    return src_path


def _parse_targets(items: Iterable[str]) -> list[tuple[str, str]]:
    """
    解析 market:symbol 形式的目标列表。

    Args:
        items (Iterable[str]): 目标字符串列表。

    Returns:
        list[tuple[str, str]]: 标准化后的目标元组。
    """

    targets: list[tuple[str, str]] = []
    for item in items:
        if ":" not in item:
            raise ValueError(f"目标格式错误，应为 market:symbol，当前为: {item}")
        market, symbol = item.split(":", 1)
        targets.append((market.strip(), symbol.strip()))
    return targets


def _build_parser() -> argparse.ArgumentParser:
    """
    构建命令行参数解析器。

    Returns:
        argparse.ArgumentParser: 参数解析器。
    """

    parser = argparse.ArgumentParser(description="QMTD fd backend dashboard service 验收脚本")
    parser.add_argument("--fd-repo", default=os.environ.get("FD_REPO", str(DEFAULT_FD_REPO)), help="FD 项目根目录")
    parser.add_argument("--root", default=os.environ.get("FD_DATA_ROOT", str(DEFAULT_FD_ROOT)), help="FD 数据库根目录")
    parser.add_argument(
        "--target",
        action="append",
        dest="targets",
        help="验收目标，格式 market:symbol；可重复传入",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """
    脚本入口。

    Args:
        argv (list[str] | None): 可选命令行参数。

    Returns:
        int: 验收通过返回 0，否则返回 2。
    """

    parser = _build_parser()
    args = parser.parse_args(argv)
    _ensure_fd_src(args.fd_repo)

    from fd_services.dashboard_service import DashboardService

    targets = _parse_targets(args.targets or DEFAULT_TARGETS)
    service = DashboardService(root_dir=args.root)
    failed: list[str] = []

    print(f"[FD验收] root={args.root}")
    for market, symbol in targets:
        snapshot = service.inventory_service.snapshot(
            market=market,
            search_text=symbol,
            cycles=["1d", "1min"],
            file_types=["csv"],
            readable_only=True,
        )
        inventory_df = snapshot["inventory_df"]
        print(f"[FD验收] {market}/{symbol} 库存条目={len(inventory_df)}")
        if inventory_df.empty:
            failed.append(f"{market}/{symbol}: 库存为空")
            continue

        for _, row in inventory_df.iterrows():
            entry = row.to_dict()
            inspect = service.inspect_entry(entry, preview_rows=1)
            quality = service.check_entry(entry)
            readable = bool(inspect.get("readable"))
            quality_ok = bool(quality.get("ok"))
            print(
                "[FD验收] "
                f"{entry.get('market')}/{entry.get('symbol')} "
                f"{entry.get('cycle')}/{entry.get('specific')} "
                f"readable={readable} quality_ok={quality_ok} "
                f"start={inspect.get('start_time')} end={inspect.get('end_time')}"
            )
            if not readable:
                failed.append(f"{entry.get('symbol')} {entry.get('cycle')}: 文件不可读")
            if not quality_ok:
                failed.append(f"{entry.get('symbol')} {entry.get('cycle')}: 质量检查失败 {quality.get('reason')}")

    if failed:
        for item in failed:
            print(f"[FD验收失败] {item}")
        return 2
    print("[FD验收] 全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
