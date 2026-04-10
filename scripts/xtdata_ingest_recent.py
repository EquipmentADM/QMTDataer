# -*- coding: utf-8 -*-
"""
xtdata 近期补齐一键脚本。

Responsibilities:
    - 以 recent-backfill 模式运行入库任务。
    - 适用于日常增量更新场景，默认开启 auto_start + lookback。

Internal Dependencies:
    - core.ingest_runner

External Systems:
    - xtquant.xtdata（或兼容 xtdata）
    - 本地文件系统
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.ingest_runner import run_profile


def main() -> None:
    """
    一键执行 recent-backfill 模式。

    Returns:
        None
    """
    run_profile("recent-backfill")


if __name__ == "__main__":
    main()
