# -*- coding: utf-8 -*-
"""
xtdata 全量更新一键脚本。
职责:
    - 调用 run_ingest 执行全量下载+写入，覆盖默认标的的 1d/1m 数据
    - 参数固定为全量（不回溯起始、不跳过 download）
使用方式:
    - 直接运行本文件即可
"""
from __future__ import annotations

from scripts.xtdata_ingest_integration_test import (
    DEFAULT_SYMBOLS,
    DEFAULT_ROOT,
    run_ingest,
)


def main() -> None:
    """全量更新入口。"""
    run_ingest(
        symbols=DEFAULT_SYMBOLS,
        cycles=["1d", "1m"],
        root=DEFAULT_ROOT,
        start="20000101",
        end="",
        skip_download=False,
        auto_start=False,
        lookback=2,
    )


if __name__ == "__main__":
    main()
