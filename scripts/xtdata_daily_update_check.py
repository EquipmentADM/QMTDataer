# -*- coding: utf-8 -*-
"""
QMTD 日常行情更新与验收脚本。

Responsibilities:
    - 执行 recent-backfill 近期补齐任务。
    - 对补齐后的目标文件进行独立读取校验。
    - 输出适合定时任务和计划发布 AI 识别的中文汇总。

Data Contract:
    - 依赖 core.ingest_runner 中的 recent-backfill profile。
    - 校验目标文件必须包含 time 列。
    - time 列必须可被 pandas 解析为无时区本地时间。
    - time 列不得存在 NaT，不得存在重复时间。

Internal Dependencies:
    - core.ingest_runner
    - core.storage_backend

External Systems:
    - MiniQMT / xtdata
    - FD 生产行情库
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ingest_runner import (  # noqa: E402
    _build_file_path,
    _build_targets,
    build_profile,
    run_profile,
)
from core.storage_backend import build_storage_backend, resolve_storage_backend_config  # noqa: E402


@dataclass(frozen=True)
class FileCheckResult:
    """
    单个行情文件的验收结果。

    Attributes:
        label: 人类可读的标的与周期名称。
        path: 被检查的文件路径。
        rows: 文件行数。
        max_time: 文件 time 列最大时间。
        missing: 文件是否缺失。
        nat_count: time 列无法解析的行数。
        dup_count: time 列重复数量。
        target_matched: 最新日期是否命中目标日期。
    """

    label: str
    path: Path
    rows: int = 0
    max_time: pd.Timestamp | None = None
    missing: bool = False
    nat_count: int = 0
    dup_count: int = 0
    target_matched: bool = False


def _configure_stdout() -> None:
    """
    尽量将标准输出设为 UTF-8，降低 Windows 终端中文乱码概率。

    Returns:
        None
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _parse_target_date(text: str) -> date:
    """
    解析目标校验日期。

    Args:
        text: 日期字符串，支持 YYYY-MM-DD、YYYYMMDD 或 today。

    Returns:
        date: 用于校验最新行情日期的目标日期。
    """
    if text.strip().lower() == "today":
        return pd.Timestamp.now().date()
    parsed = pd.to_datetime(text, errors="raise")
    return parsed.date()


def _check_one_file(label: str, path: Path, target_date: date) -> FileCheckResult:
    """
    检查单个落盘文件的时间列质量。

    Args:
        label: 标的与周期展示名。
        path: CSV 文件路径。
        target_date: 期望最新日期。

    Returns:
        FileCheckResult: 文件检查结果。
    """
    if not path.exists():
        return FileCheckResult(label=label, path=path, missing=True)

    df = pd.read_csv(path)
    if "time" not in df.columns:
        return FileCheckResult(
            label=label,
            path=path,
            rows=len(df),
            nat_count=len(df),
            dup_count=0,
            target_matched=False,
        )

    ts = pd.to_datetime(df["time"], errors="coerce")
    max_time = ts.max() if not ts.empty else pd.NaT
    max_time_out = None if pd.isna(max_time) else pd.Timestamp(max_time)
    target_matched = bool(max_time_out is not None and max_time_out.date() == target_date)

    return FileCheckResult(
        label=label,
        path=path,
        rows=len(df),
        max_time=max_time_out,
        nat_count=int(ts.isna().sum()),
        dup_count=int(ts.duplicated().sum()),
        target_matched=target_matched,
    )


def _iter_file_checks(profile_name: str, target_date: date) -> list[FileCheckResult]:
    """
    按 profile 枚举全部目标文件并执行校验。

    Args:
        profile_name: 入库 profile 名称。
        target_date: 期望最新日期。

    Returns:
        list[FileCheckResult]: 全部文件验收结果。
    """
    profile = build_profile(profile_name)
    storage_config = resolve_storage_backend_config(
        root=profile.root,
        backend=profile.storage_backend or None,
        fd_repo=profile.fd_repo or None,
    )
    storage = build_storage_backend(storage_config)
    targets = _build_targets(profile)

    results: list[FileCheckResult] = []
    for cycle in profile.cycles:
        for target in targets:
            path = _build_file_path(storage, target, cycle, file_type="csv", root=storage_config.root)
            label = f"{target.display_name} {cycle}"
            results.append(_check_one_file(label=label, path=path, target_date=target_date))
    return results


def _print_detail(title: str, results: Iterable[FileCheckResult], limit: int = 30) -> None:
    """
    打印异常明细，避免定时任务日志过长。

    Args:
        title: 明细标题。
        results: 需要打印的检查结果。
        limit: 最大打印数量。

    Returns:
        None
    """
    items = list(results)
    if not items:
        return
    print(title)
    for item in items[:limit]:
        max_text = "-" if item.max_time is None else item.max_time.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"  - {item.label} | 行数={item.rows} 最新={max_text} "
            f"NaT={item.nat_count} 重复={item.dup_count} 路径={item.path}"
        )
    if len(items) > limit:
        print(f"  - 其余 {len(items) - limit} 项已省略。")


def run_daily_update(profile_name: str, target_date: date, skip_ingest: bool = False) -> int:
    """
    执行日常更新并进行验收。

    Args:
        profile_name: 入库 profile 名称，默认 recent-backfill。
        target_date: 期望最新日期。
        skip_ingest: 是否跳过入库，只做校验。

    Returns:
        int: 进程退出码，0 表示通过，1 表示入库失败，2 表示验收不通过。
    """
    print(f"[日常更新] profile={profile_name} 目标日期={target_date.isoformat()} skip_ingest={skip_ingest}")

    if not skip_ingest:
        try:
            result = run_profile(profile_name)
        except Exception as exc:
            print(f"[日常更新失败] 入库任务异常：{exc}")
            return 1
        print(
            "[日常更新完成] "
            f"总任务={result.get('total')} 有效更新={result.get('updated')} "
            f"未更新={result.get('not_updated')} 失败={result.get('failed_count')} "
            f"用时={result.get('elapsed_sec'):.1f}s"
        )

    checks = _iter_file_checks(profile_name=profile_name, target_date=target_date)
    missing = [x for x in checks if x.missing]
    not_target = [x for x in checks if not x.missing and not x.target_matched]
    nat_bad = [x for x in checks if not x.missing and x.nat_count != 0]
    dup_bad = [x for x in checks if not x.missing and x.dup_count != 0]

    latest_values = [x.max_time for x in checks if x.max_time is not None]
    latest_min = min(latest_values) if latest_values else None
    latest_max = max(latest_values) if latest_values else None

    print(
        "[验收汇总] "
        f"文件数={len(checks)} 缺失={len(missing)} "
        f"未到目标日期={len(not_target)} NaT异常={len(nat_bad)} 重复时间={len(dup_bad)}"
    )
    print(
        "[验收范围] "
        f"最早最新时间={latest_min.strftime('%Y-%m-%d %H:%M:%S') if latest_min is not None else '-'} "
        f"最晚最新时间={latest_max.strftime('%Y-%m-%d %H:%M:%S') if latest_max is not None else '-'}"
    )

    _print_detail("[缺失文件明细]", missing)
    _print_detail("[未到目标日期明细]", not_target)
    _print_detail("[NaT异常明细]", nat_bad)
    _print_detail("[重复时间明细]", dup_bad)

    if missing or not_target or nat_bad or dup_bad:
        print("[验收失败] 更新任务完成，但落盘文件未全部满足目标日期或质量要求。")
        return 2

    print("[验收通过] 全部目标文件已更新到目标日期，且 time 列无 NaT、无重复。")
    return 0


def main() -> None:
    """
    命令行入口。

    Returns:
        None
    """
    _configure_stdout()
    parser = argparse.ArgumentParser(description="执行 QMTD 日常行情更新并校验落盘结果。")
    parser.add_argument("--profile", default="recent-backfill", help="入库 profile，默认 recent-backfill。")
    parser.add_argument("--target-date", default="today", help="验收日期，支持 today、YYYY-MM-DD、YYYYMMDD。")
    parser.add_argument("--skip-ingest", action="store_true", help="只校验，不执行入库。")
    args = parser.parse_args()

    target_date = _parse_target_date(args.target_date)
    raise SystemExit(
        run_daily_update(
            profile_name=args.profile,
            target_date=target_date,
            skip_ingest=args.skip_ingest,
        )
    )


if __name__ == "__main__":
    main()
