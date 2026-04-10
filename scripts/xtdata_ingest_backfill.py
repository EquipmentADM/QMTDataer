# -*- coding: utf-8 -*-
"""
xtdata 全量补齐一键脚本。

Responsibilities:
    - 以 full-backfill 模式运行入库任务。
    - 适用于全区间补洞场景，默认采用 merge 写入策略。

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
    一键执行 full-backfill 模式。

    Returns:
        None
    """
    run_profile("full-backfill")


if __name__ == "__main__":
    main()
