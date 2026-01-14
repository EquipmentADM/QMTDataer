# -*- coding: utf-8 -*-
"""
xtdata 全量回溯/补齐一键脚本。
职责:
    - 固定全量参数（不跳过 download，不自动起始），从 20000101 拉取至“今天+1 天”
    - 适合在发现历史缺口或目录/格式调整后做一次全量补齐
使用方式:
    - 直接运行本文件即可
"""
from __future__ import annotations

from scripts.xtdata_ingest_integration_test import DEFAULT_SYMBOLS, DEFAULT_ROOT, run_ingest


def main() -> None:
    """全量回溯入口。"""
    run_ingest(
        symbols=DEFAULT_SYMBOLS,
        cycles=["1d", "1m"],
        root=DEFAULT_ROOT,
        start="20000101",
        end="",
        skip_download=False,  # 强制 download_history_data
        auto_start=False,     # 不基于历史回溯，直接全量覆盖合并
        lookback=0,
    )


if __name__ == "__main__":
    main()
