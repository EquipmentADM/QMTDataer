# -*- coding: utf-8 -*-
"""
xtdata 补数落盘（命令行版，简化功能）

功能：
    - 读取 xtquant.xtdata，本地 download + get，生成标准 DataFrame；
    - 通过 FinancialDataStorage 按目录规则落盘（默认 CSV）；
    - 可指定标的列表、周期、起止时间、目标根目录。

示例：
    python scripts/xtdata_ingest_simple.py \\
        --root ./xtdata_output --market SS_stock_data --cycle 1d \\
        --symbols 518880.SH,513880.SH --start 20000101 --end "" --file-type csv
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
from core.xtdata_source import XtdataSource


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="xtdata 补数落盘（简版）")
    parser.add_argument("--root", type=str, required=True, help="目标根目录，例如 D:/Work/Quant/financial_database")
    parser.add_argument("--market", type=str, default="SS_stock_data", help="市场目录名")
    parser.add_argument("--symbols", type=str, required=True, help="逗号分隔的标的列表")
    parser.add_argument("--cycle", type=str, default="1d", help="周期，如 1d/1m")
    parser.add_argument("--specific", type=str, default="original", help="子目录/合成标记，默认 original")
    parser.add_argument("--start", type=str, default="", help="开始时间 YYYYMMDD 或 YYYYMMDDHHMMSS，空表示最早")
    parser.add_argument("--end", type=str, default="", help="结束时间，空表示最新")
    parser.add_argument("--file-type", type=str, default="csv", choices=["csv", "pkl"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        from xtquant import xtdata  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"无法导入 xtquant.xtdata：{exc}")

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise ValueError("symbols 不能为空")

    storage = FinancialDataStorage(root_dir=args.root)
    ingestor = MarketDataIngestor(storage)
    source = XtdataSource(xtdata=xtdata, download=True)

    for sym in symbols:
        logging.info("[INGEST] %s %s %s %s-%s", args.market, sym, args.cycle, args.start or "<begin>", args.end or "<now>")
        out_path = ingestor.ingest_symbol(
            source=source,
            market=args.market,
            symbol=sym,
            cycle=args.cycle,
            specific=args.specific,
            start=args.start or None,
            end=args.end or None,
            file_type=args.file_type,
        )
        logging.info("[DONE] %s", out_path)


if __name__ == "__main__":
    main()
