# -*- coding: utf-8 -*-
"""
xtdata 手工补数示范脚本（直接在文件内填写参数后右键运行）

使用说明：
    1) 确保本机已安装并可导入 `xtquant.xtdata`，且 MiniQMT 已登录；
    2) 按下方“可编辑区域”修改目标目录、标的列表、周期、时间范围等；
    3) 右键运行本脚本（或 `python scripts/demo_xtdata_manual.py`），即可将数据写入本地。

落盘规则：
    ROOT/
      {market}/
        {symbol}/
          {cycle}/
            {specific}/
              {symbol}_{cycle}.csv
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from core.ingestor import MarketDataIngestor
from core.storage_simple import FinancialDataStorage
from core.xtdata_source import XtdataSource


# ========== 可编辑参数区域 ==========
ROOT_DIR: Path = Path(r"./xtdata_output")
MARKET: str = "SS_stock_data"
SYMBOL_LIST: List[str] = [
    "159915.SZ",
    "518880.SH",
    "513880.SH",
    "513100.SH",
    "513030.SH",
    "513080.SH",
    "513180.SH",
    "510300.SH",
    "511010.SH",
    "159980.SZ",
    "563300.SH",
]
CYCLE: str = "1d"
SPECIFIC: str = "original"
START: str = ""  # 留空表示最早
END: str = ""    # 留空表示最新
FILE_TYPE: str = "csv"
# ====================================


def main() -> None:
    """示范流程：下载+获取 -> 标准化 -> 落盘"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        from xtquant import xtdata  # type: ignore
    except Exception as exc:
        raise RuntimeError(f"无法导入 xtquant.xtdata：{exc}")

    storage = FinancialDataStorage(root_dir=str(ROOT_DIR))
    ingestor = MarketDataIngestor(storage)
    source = XtdataSource(xtdata=xtdata, download=True)

    for sym in SYMBOL_LIST:
        logging.info("[DEMO] 开始补数 %s %s %s %s-%s", MARKET, sym, CYCLE, START or "<begin>", END or "<now>")
        out_path = ingestor.ingest_symbol(
            source=source,
            market=MARKET,
            symbol=sym,
            cycle=CYCLE,
            specific=SPECIFIC,
            start=START or None,
            end=END or None,
            file_type=FILE_TYPE,
        )
        logging.info("[DONE] %s 写入完成", out_path)


if __name__ == "__main__":
    main()
