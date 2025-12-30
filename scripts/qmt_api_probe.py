# -*- coding: utf-8 -*-
"""
xtdata 快速探针脚本：下载 + get_market_data_ex，打印基本统计与示例。
Responsibilities:
    - 先触发 download_history_data 预热本地缓存
    - 调用 get_market_data_ex 拉取数据并打印各字段形状
    - 可选输出行式预览，便于人工检查
Data Contract:
    - 需在已登录的 miniqmt/xtdata 环境运行，目标标的需有行情权限
Notes:
    - 默认探测 518880.SH / 513880.SH 的 1d 数据；可按需修改常量
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

try:
    from xtquant import xtdata
except Exception as exc:  # pragma: no cover
    print("无法导入 xtquant.xtdata", exc, file=sys.stderr)
    raise

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    pd = None
    print("警告：未安装 pandas，预览功能将跳过", exc)

CODES: List[str] = ["518880.SH", "513880.SH"]
PERIOD: str = "1d"          # 支持 1m / 1h / 1d
START: str = "20130101"     # YYYYMMDD
END: str = "20250909"
DIVIDEND: str = "none"      # none | front | back | ratio
FILL_DATA: bool = False
PREVIEW_LAST_N_DATES: int = 5
SAVE_CSV: bool = False

CN_TZ = timezone(timedelta(hours=8))


def download_range(codes: List[str], period: str, start_yyyymmdd: str, end_yyyymmdd: str) -> None:
    """
    下载指定区间到本地缓存。
    Args:
        codes (List[str]): 标的列表。
        period (str): 周期。
        start_yyyymmdd (str): 起始日期 YYYYMMDD。
        end_yyyymmdd (str): 结束日期 YYYYMMDD。
    """
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


def fetch_market_data(
    codes: List[str],
    period: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    dividend: str = "none",
    fill_data: bool = False,
) -> Dict[str, Any]:
    """
    调用 get_market_data_ex 并打印基础统计。
    Args:
        codes (List[str]): 标的列表。
        period (str): 周期。
        start_yyyymmdd (str): 起始日期。
        end_yyyymmdd (str): 结束日期。
        dividend (str): 复权方式。
        fill_data (bool): 是否补齐缺口。
    Returns:
        Dict[str, Any]: xtdata 返回的原始字典。
    """
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
    )
    if not isinstance(data, dict) or not data:
        print("  (empty result)")
        return {}
    for field, df in data.items():
        rows, cols = getattr(df, "shape", (None, None))
        print(f"  [{field:>12}] shape={rows}x{cols}")
        if hasattr(df, "index") and hasattr(df, "columns"):
            try:
                zero_cnt = (df == 0).sum(axis=1)
                nonzero_cnt = (df != 0).sum(axis=1)
                for code in df.index[:5]:
                    print(f"      zeros[{code}]={int(zero_cnt.loc[code])} nonzeros={int(nonzero_cnt.loc[code])}")
            except Exception:
                pass
        # 额外输出前几列名，便于确认返回结构
        if hasattr(df, "columns"):
            cols_preview = list(df.columns[:5])
            print(f"      cols preview: {cols_preview}")
    # 若是 code->DataFrame 模式，打印第一个标的的列名与前几行
    first_df = next((v for v in data.values() if hasattr(v, "head")), None)
    if first_df is not None:
        print(f"\n  [STRUCT] detect code->DataFrame, columns={list(first_df.columns)}")
        try:
            print(first_df.head(3))
        except Exception:
            pass
    return data


def build_row_preview(data: Dict[str, Any], last_n: int = 5) -> Optional["pd.DataFrame"]:
    """
    生成行式预览，便于肉眼检查。
    Args:
        data (dict): get_market_data_ex 返回的字典。
        last_n (int): 每标的取最近 N 条非零记录。
    Returns:
        pd.DataFrame | None: 预览数据，若无法生成则返回 None。
    """
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
    """主入口：下载 -> 拉取 -> 打印预览（可选写 CSV）。"""
    download_range(CODES, PERIOD, START, END)
    data = fetch_market_data(CODES, PERIOD, START, END, DIVIDEND, FILL_DATA)
    df_rows = build_row_preview(data, PREVIEW_LAST_N_DATES)
    if SAVE_CSV and pd is not None and df_rows is not None:
        out = "probe_preview.csv"
        df_rows.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n预览已写入 {out}")


if __name__ == "__main__":
    main()
