# -*- coding: utf-8 -*-
"""Minimal QMT xtdata probe (download → get_market_data_ex).

The script can be launched directly from PyCharm without parameters. It will:
    1. call download_history_data for every code to ensure MiniQMT cache is warm;
    2. call get_market_data_ex to fetch a wide dictionary (field -> DataFrame);
    3. print shapes / non-zero counts for quick inspection;
    4. optionally assemble a row-style preview and write it to CSV.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

try:
    from xtquant import xtdata
except Exception as exc:  # pragma: no cover
    print("Failed to import xtquant.xtdata", exc, file=sys.stderr)
    raise

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    pd = None
    print("Warning: pandas not installed", exc)

CODES: List[str] = ["518880.SH", "513880.SH"]
PERIOD: str = "1d"          # supported: 1m / 1h / 1d
START: str = "20130101"      # YYYYMMDD
END: str = "20250909"
DIVIDEND: str = "none"       # none | front | back | ratio
FILL_DATA: bool = False
PREVIEW_LAST_N_DATES: int = 5
SAVE_CSV: bool = False

CN_TZ = timezone(timedelta(hours=8))


def download_range(codes: List[str], period: str, start_yyyymmdd: str, end_yyyymmdd: str) -> None:
    """Download the required date range into MiniQMT local cache."""
    print(f"[DOWNLOAD] period={period} {start_yyyymmdd}~{end_yyyymmdd} codes={len(codes)}")
    for code in codes:
        try:
            xtdata.download_history_data(
                code,
                period=period,
                start_time=start_yyyymmdd,
                end_time=end_yyyymmdd,
                incrementally=True,
            )
            print(f"  - ok: {code}")
        except Exception as exc:  # pragma: no cover
            print(f"  - fail: {code} -> {exc}")


def fetch_market_data(codes: List[str], period: str, start_yyyymmdd: str, end_yyyymmdd: str,
                      dividend: str = "none", fill_data: bool = False) -> Dict[str, Any]:
    """Invoke get_market_data_ex and print basic statistics."""
    print(f"\n[FETCH] period={period} {start_yyyymmdd}~{end_yyyymmdd} dividend={dividend} fill_data={fill_data}")
    data = xtdata.get_market_data_ex(
        field_list=[],
        stock_list=codes,
        period=period,
        start_time=start_yyyymmdd,
        end_time=end_yyyymmdd,
        count=-1,
        dividend_type=dividend,
        fill_data=fill_data,
        subscribe=False,
    )
    if not isinstance(data, dict) or not data:
        print("  (empty result)")
        return {}
    for field, df in data.items():
        rows, cols = getattr(df, "shape", (None, None))
        print(f"  [{field:>12}] shape={rows}x{cols}")
        if field.lower() == "time":
            try:
                zero_cnt = (df == 0).sum(axis=1)
                nonzero_cnt = (df != 0).sum(axis=1)
                for code in df.index[:5]:
                    print(f"      time zeros[{code}]={int(zero_cnt.loc[code])} nonzeros={int(nonzero_cnt.loc[code])}")
            except Exception:
                pass
    return data


def build_row_preview(data: Dict[str, Any], last_n: int = 5):
    """Assemble a small row-style preview from the wide dictionary."""
    if pd is None or not data:
        return None
    time_df = None
    for key in ("time", "Time", "datetime", "bar_time"):
        if key in data:
            time_df = data[key]
            break
    if time_df is None:
        print("(preview skipped: no time field)")
        return None

    rows: List[dict] = []
    for code in list(time_df.index):
        nonzero_cols = [col for col in time_df.columns if time_df.loc[code, col] not in (0, None)]
        for col in nonzero_cols[-last_n:]:
            t_val = time_df.loc[code, col]
            try:
                dt = datetime.fromtimestamp(float(t_val) / 1000.0, tz=timezone.utc).astimezone(CN_TZ)
            except Exception:
                continue
            record = {
                "code": code,
                "trade_date": col,
                "bar_end_ts": dt.isoformat(),
            }
            for field in ("open", "high", "low", "close", "volume", "amount", "preClose"):
                if field in data:
                    try:
                        record[field] = float(data[field].loc[code, col])
                    except Exception:
                        record[field] = None
            rows.append(record)
    if not rows:
        print("(preview skipped: no non-zero bars)")
        return None

    df_rows = pd.DataFrame(rows)
    print("\n[PREVIEW] sample rows=", len(df_rows))
    print(df_rows.head(min(10, len(df_rows))).to_string(index=False))
    return df_rows


def main() -> None:
    download_range(CODES, PERIOD, START, END)
    data = fetch_market_data(CODES, PERIOD, START, END, DIVIDEND, FILL_DATA)
    df_rows = build_row_preview(data, PREVIEW_LAST_N_DATES)
    if SAVE_CSV and pd is not None and df_rows is not None:
        out = "probe_preview.csv"
        df_rows.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\npreview written to {out}")


if __name__ == "__main__":
    main()
