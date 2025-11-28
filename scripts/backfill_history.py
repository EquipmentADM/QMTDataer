# -*- coding = utf-8 -*-
# @Time : 2025/9/9 15:15
# @Author : EquipmentADV
# @File : backfill_history.py
# @Software : PyCharm
"""脚本：历史在线拉取（不落库版，供权威库拉取端调用）

脚本说明：
    - 功能：装配 HistoryAPI（QMT 实参接线），按参数拉取新鲜数据并输出 JSON 摘要或数据；
    - 上游：运维/权威库拉取端；
    - 下游：调用方（用于落库或直接消费）。

用法示例：
    python scripts/backfill_history.py --codes 000001.SZ,600000.SH \
        --period 1m --start 2025-01-01T09:30:00+08:00 --end 2025-01-01T15:00:00+08:00 \
        --dividend none --return-data
"""
from __future__ import annotations
import argparse
import logging
import json

from core.qmt_connector import QMTConnector, QMTConfig
from core.history_api import HistoryAPI, HistoryConfig


def main():
    parser = argparse.ArgumentParser(description="历史拉取（先补后取，宽表输出）")
    parser.add_argument("--codes", type=str, required=True)
    parser.add_argument("--period", type=str, default="1d", choices=["1m", "1h", "1d"])
    parser.add_argument("--start", type=str, required=True, help="ISO8601，如 2025-01-01T09:30:00+08:00")
    parser.add_argument("--end", type=str, required=True, help="ISO8601")
    parser.add_argument("--dividend", type=str, default="none", choices=["none", "front", "back", "ratio"])
    parser.add_argument("--return-data", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    qmt_cfg = QMTConfig(mode="none")
    QMTConnector(qmt_cfg).listen_and_connect()

    api = HistoryAPI(HistoryConfig(use_batch_get_in=True, fill_data_on_get=False, dividend_type=args.dividend))
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    result = api.fetch_bars(
        codes=codes,
        period=args.period,
        start_time=args.start,
        end_time=args.end,
        dividend_type=args.dividend,
        return_data=args.return_data,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
