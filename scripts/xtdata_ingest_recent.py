# -*- coding: utf-8 -*-
"""
xtdata 近期增量更新一键脚本。
职责:
    - 调用 run_ingest 按已有文件最新时间回溯少量 bar，增量拉取并合并
    - 适合日常定时更新，减少下载量
使用方式:
    - 直接运行本文件即可
"""
from __future__ import annotations

from scripts.xtdata_ingest_integration_test import DEFAULT_SYMBOL_TEST, DEFAULT_ROOT, run_ingest



def main() -> None:
    """近期增量更新入口。"""
    run_ingest(
        symbols=DEFAULT_SYMBOL_TEST,
        cycles=["1d", "1m"],
        root=DEFAULT_ROOT,
        start="20000101",
        end="",
        skip_download=False,
        auto_start=True,
        lookback=2,
    )


if __name__ == "__main__":
    main()
