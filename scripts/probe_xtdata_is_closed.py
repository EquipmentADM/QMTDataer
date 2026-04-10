# -*- coding: utf-8 -*-
"""
xtdata 实时字段探针：验证行情源是否提供 isClosed（或同义字段）。

Responsibilities:
    - 订阅指定标的与周期（默认 1m）并在固定时长内收集回调数据。
    - 统计 isClosed / isClose / closed 字段的出现率与取值分布。
    - 输出样本与汇总，判断“字段是否来自行情源”。

Data Contract:
    - 输入回调 datas 结构遵循 xtdata 约定：{code: [row, ...]}。
    - 本脚本仅做观测，不修改或推导收盘语义。

Notes:
    - 默认运行 120 秒，可通过 --duration-sec 调整。
    - 默认会尝试后台启动 xtdata.run()，若环境不需要可加 --no-run-loop。
"""
from __future__ import annotations

import argparse
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


TARGET_FIELDS = ("isClosed", "isClose", "closed")


@dataclass
class _FieldCounter:
    """单个字段统计。"""

    present: int = 0
    true_count: int = 0
    false_count: int = 0
    none_count: int = 0
    other_count: int = 0

    def mark(self, value: Any) -> None:
        """记录字段取值分布。"""
        self.present += 1
        if value is True:
            self.true_count += 1
        elif value is False:
            self.false_count += 1
        elif value is None:
            self.none_count += 1
        else:
            self.other_count += 1


@dataclass
class _ProbeStats:
    """全局探测统计。"""

    callback_count: int = 0
    row_count: int = 0
    per_code_rows: Dict[str, int] = field(default_factory=dict)
    fields: Dict[str, _FieldCounter] = field(
        default_factory=lambda: {name: _FieldCounter() for name in TARGET_FIELDS}
    )
    samples: List[Dict[str, Any]] = field(default_factory=list)

    def add_row(self, code: str, row: Dict[str, Any], sample_limit: int) -> None:
        """写入一条 row 的统计信息。"""
        self.row_count += 1
        self.per_code_rows[code] = self.per_code_rows.get(code, 0) + 1
        for field in TARGET_FIELDS:
            if field in row:
                self.fields[field].mark(row.get(field))
        if len(self.samples) < sample_limit:
            self.samples.append(
                {
                    "code": code,
                    "time": row.get("time") or row.get("Time") or row.get("bar_time") or row.get("barTime"),
                    "isClosed": row.get("isClosed"),
                    "isClose": row.get("isClose"),
                    "closed": row.get("closed"),
                    "close": row.get("close"),
                }
            )


def _build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="探测 xtdata 回调中是否包含 isClosed 字段")
    parser.add_argument("--codes", default="510050.SH", help="逗号分隔标的列表")
    parser.add_argument("--period", default="1m", help="订阅周期，默认 1m")
    parser.add_argument("--duration-sec", type=int, default=120, help="运行时长（秒），默认 120")
    parser.add_argument("--sample-limit", type=int, default=20, help="打印样本条数上限")
    parser.add_argument("--heartbeat-sec", type=int, default=10, help="进度打印间隔（秒）")
    parser.add_argument("--no-run-loop", action="store_true", help="不启动后台 xtdata.run()")
    return parser


def _start_run_loop_if_needed(xtdata: Any, no_run_loop: bool) -> Optional[threading.Thread]:
    """按参数决定是否后台启动 xtdata.run()。"""
    if no_run_loop or not hasattr(xtdata, "run"):
        return None

    def _worker() -> None:
        try:
            xtdata.run()
        except Exception as exc:
            print(f"[RUN] xtdata.run() 退出：{exc}")

    thread = threading.Thread(target=_worker, name="xtdata-run-loop", daemon=True)
    thread.start()
    return thread


def _subscribe_codes(xtdata: Any, codes: List[str], period: str, callback) -> None:
    """为每个标的注册订阅回调。"""
    for code in codes:
        try:
            xtdata.subscribe_quote(
                stock_code=code,
                period=period,
                start_time="",
                end_time="",
                count=0,
                callback=callback,
            )
            print(f"[SUB] ok code={code} period={period}")
        except TypeError:
            xtdata.subscribe_quote(stock_code=code, period=period, count=0, callback=callback)
            print(f"[SUB] ok (fallback) code={code} period={period}")
        except Exception as exc:
            print(f"[SUB] fail code={code} period={period} err={exc}")


def _unsubscribe_codes(xtdata: Any, codes: List[str], period: str) -> None:
    """尝试取消订阅，避免遗留会话。"""
    if not hasattr(xtdata, "unsubscribe_quote"):
        return
    for code in codes:
        try:
            xtdata.unsubscribe_quote(stock_code=code, period=period)
        except TypeError:
            try:
                xtdata.unsubscribe_quote(code, period)
            except Exception:
                pass
        except Exception:
            pass


def _print_summary(stats: _ProbeStats, duration_sec: int) -> None:
    """打印最终统计结果。"""
    print("\n========== Probe Summary ==========")
    print(f"duration_sec={duration_sec}")
    print(f"callback_count={stats.callback_count}")
    print(f"row_count={stats.row_count}")
    print(f"rows_by_code={stats.per_code_rows}")
    for field in TARGET_FIELDS:
        item = stats.fields[field]
        print(
            f"{field}: present={item.present}, true={item.true_count}, false={item.false_count}, "
            f"none={item.none_count}, other={item.other_count}"
        )

    any_present = any(stats.fields[field].present > 0 for field in TARGET_FIELDS)
    any_true = any(stats.fields[field].true_count > 0 for field in TARGET_FIELDS)
    if stats.row_count == 0:
        print("[CONCLUSION] 未接收到任何 row，无法判断是否有 isClosed 字段。")
    elif not any_present:
        print("[CONCLUSION] 已接收 row，但未发现 isClosed/isClose/closed 字段。")
    elif any_true:
        print("[CONCLUSION] 行情源回调中存在 close 标志字段，且观测到 True。")
    else:
        print("[CONCLUSION] 行情源回调中存在 close 标志字段，但本窗口未观测到 True。")

    if stats.samples:
        print("\n---------- Samples ----------")
        for idx, sample in enumerate(stats.samples, start=1):
            print(f"{idx:02d} {sample}")


def main() -> int:
    """主入口：订阅回调并统计字段出现情况。"""
    parser = _build_parser()
    args = parser.parse_args()

    try:
        from xtquant import xtdata  # type: ignore
    except Exception as exc:
        print(f"[ERROR] 无法导入 xtquant.xtdata: {exc}")
        return 2

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if not codes:
        print("[ERROR] --codes 不能为空")
        return 2

    lock = threading.Lock()
    stats = _ProbeStats()

    def _on_datas(datas: Dict[str, List[Dict[str, Any]]]) -> None:
        with lock:
            stats.callback_count += 1
            for code, rows in datas.items():
                if not rows:
                    continue
                for row in rows:
                    if isinstance(row, dict):
                        stats.add_row(code, row, args.sample_limit)

    _start_run_loop_if_needed(xtdata, args.no_run_loop)
    _subscribe_codes(xtdata, codes, args.period, _on_datas)

    print(
        f"[RUN] start probe codes={codes} period={args.period} duration={args.duration_sec}s "
        f"run_loop={'off' if args.no_run_loop else 'on'}"
    )
    end_ts = time.time() + max(1, args.duration_sec)
    next_heartbeat = time.time() + max(1, args.heartbeat_sec)
    try:
        while time.time() < end_ts:
            time.sleep(0.2)
            if time.time() >= next_heartbeat:
                with lock:
                    print(
                        f"[HEARTBEAT] callbacks={stats.callback_count} rows={stats.row_count} "
                        f"isClosed_present={stats.fields['isClosed'].present}"
                    )
                next_heartbeat += max(1, args.heartbeat_sec)
    except KeyboardInterrupt:
        print("[RUN] 手动中断，提前结束。")
    finally:
        _unsubscribe_codes(xtdata, codes, args.period)

    with lock:
        _print_summary(stats, args.duration_sec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
