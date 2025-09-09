# -*- coding: utf-8 -*-
"""PyCharm 直跑版｜QMT 极简 API 探针（只做 download→get_in，不做 listen）

用途：
  - 在 PyCharm 里直接点“Run”即可运行（无需命令行参数）。
  - 先调用 xtdata.download_history_data 把所需区间写入 MiniQMT 本地库；
  - 再调用 xtdata.get_market_data(field_list=[], stock_list=..., ...) 批量获取多标数据；
  - 打印各字段 DataFrame 的形状、日期列范围、以及 'time' 字段为 0 的占比；
  - 额外预览一个“宽表→行式”样例（仅取每个代码最近 N 个交易日），便于你确认字段对齐。

重要说明：
  - 不会触发 xtdatacenter.listen，规避 “server only support xt user mode”。
  - 仅使用 xtquant.xtdata；请确保 MiniQMT/xtquant 已安装并能正常下载行情。

类/方法说明与上下游：
  - 本脚本仅面向开发者自检，**上游**：MiniQMT（行情下载存储），**下游**：终端打印/可选CSV样例。
"""
from __future__ import annotations
import sys
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta

try:
    from xtquant import xtdata
except Exception as e:
    print("无法导入 xtquant.xtdata，请检查 MiniQMT / Python 环境：", e, file=sys.stderr)
    raise

try:
    import pandas as pd
except Exception as e:
    pd = None  # 仅影响样例整理，不影响基础探测
    print("警告：未安装 pandas，样例整理将跳过：", e)

# ===================== 可直接修改的固定参数（在 PyCharm 里改这里） =====================
CODES: List[str] = [
    "518880.SH",
    "513880.SH",
]
PERIOD: str = "1d"                 # 仅支持："1m" | "1h" | "1d"
START: str = "20130729"            # 下载/获取起始日期 YYYYMMDD
END: str = "20250909"              # 下载/获取结束日期 YYYYMMDD
DIVIDEND: str = "none"             # "none" | "front" | "back" | "ratio"
FILL_DATA: bool = False            # 建议 False：不返回填空bar，便于识别真实覆盖
PREVIEW_LAST_N_DATES: int = 5      # 每个代码预览最近 N 个交易日组装为行式样例
SAVE_CSV: bool = False             # 是否把样例写到本地 CSV（probe_preview.csv）
# ======================================================================================

CN_TZ = timezone(timedelta(hours=8))


def download_range(codes: List[str], period: str, start_yyyymmdd: str, end_yyyymmdd: str) -> None:
    """方法说明：按日期下载历史数据（先补后取的“补”）
    功能：对每个代码执行 download_history_data(incrementally=True)。
    上游：脚本固定参数。
    下游：MiniQMT 本地数据库（写入）。
    """
    print(f"\n[DOWNLOAD] period={period} {start_yyyymmdd}~{end_yyyymmdd} codes={len(codes)}")
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
        except Exception as e:
            print(f"  - fail: {code} -> {e}")


def get_market_data_in_dump(codes: List[str], period: str, start_yyyymmdd: str, end_yyyymmdd: str,
                            dividend: str = "none", fill_data: bool = False) -> Dict[str, Any]:
    """方法说明：批量获取多标的数据字典（field → DataFrame）
    功能：调用 get_market_data(field_list=[], stock_list=codes, ...)，返回原始字典并打印结构摘要；
          其中 'time' 字段为毫秒时间戳，若无实际值则为 0。
    上游：download_range 之后。
    下游：stdout 打印 / 进一步转换。
    """
    print(f"\n[GET_IN] period={period} {start_yyyymmdd}~{end_yyyymmdd} fill_data={fill_data} dividend={dividend}")
    data = xtdata.get_market_data(
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
        print("  (返回不是 dict 或为空) ->", type(data))
        return {}
    for field, df in data.items():
        # df 为：index=代码，columns=YYYYMMDD（或更细分辨率），值为该字段数值
        rows, cols = getattr(df, "shape", (None, None))
        col_names = list(getattr(df, "columns", []))
        head_cols = col_names[:3]
        tail_cols = col_names[-3:]
        print(f"  [{field:>12}] shape={rows}x{cols}")
        print(f"              cols head={head_cols} ... tail={tail_cols}")
        if field == "time":
            try:
                zero_cnt = (df == 0).sum(axis=1)
                nonzero_cnt = (df != 0).sum(axis=1)
                for code in df.index[:5]:
                    print(f"              time zeros[{code}]={int(zero_cnt.loc[code])} nonzeros={int(nonzero_cnt.loc[code])}")
            except Exception:
                pass
    return data


def build_row_preview(data: Dict[str, Any], last_n: int = 5) -> 'pd.DataFrame | None':
    """方法说明：从 get_market_data_in 的返回构建一个“行式”样例 DataFrame（仅预览）
    功能：
      - 以 'time' 字段为基准，筛选每个代码最近 last_n 个“有值”的交易日；
      - 组装为列：code, trade_date(YYYYMMDD), bar_end_ts(ISO), open/high/low/close/volume/amount, preClose, suspendFlag...
    上游：get_market_data_in_dump 的返回。
    下游：终端预览 / 可选 CSV。
    """
    if pd is None or not data or "time" not in data:
        print("(跳过样例组装：缺少 pandas 或 time 字段)")
        return None
    time_df: pd.DataFrame = data["time"]
    # 为每个代码挑选最近 last_n 个非 0 的日期列
    rows: List[dict] = []
    for code in list(time_df.index):
        # 找到该代码非 0 的日期列（有实际值）
        nonzero_cols = [c for c in time_df.columns if time_df.loc[code, c] not in (0, None)]
        take_cols = nonzero_cols[-last_n:]
        for col in take_cols:
            t_ms = int(time_df.loc[code, col])
            dt = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc).astimezone(CN_TZ)
            rec = {
                "code": code,
                "trade_date": col,
                "bar_end_ts": dt.isoformat(),
            }
            # 把常见字段拼上（没有就跳过）
            for f in ("open", "high", "low", "close", "volume", "amount", "preClose", "suspendFlag", "openInterest", "settelementPrice"):
                if f in data:
                    try:
                        rec[f] = float(data[f].loc[code, col])
                    except Exception:
                        rec[f] = None
            rows.append(rec)
    if not rows:
        print("(样例组装：未找到任何非 0 的 time 值)")
        return None
    df_rows = pd.DataFrame(rows)
    print("\n[PREVIEW rows] 样例行数=", len(df_rows))
    print(df_rows.head(min(10, len(df_rows))).to_string(index=False))
    return df_rows


def main() -> None:
    """方法说明：脚本入口（PyCharm 直接 Run）
    功能：顺序执行“先补后取→打印结构→样例预览→可选落 CSV”。
    上游：固定参数（脚本顶部）。
    下游：终端输出/可选 CSV 文件。
    """
    # 1) 先补（按日期）
    download_range(CODES, PERIOD, START, END)

    # 2) 再取（多标批量）并打印结构摘要
    data = get_market_data_in_dump(CODES, PERIOD, START, END, DIVIDEND, FILL_DATA)

    # 3) 样例组装（行式预览）
    df_rows = build_row_preview(data, PREVIEW_LAST_N_DATES)

    # 4) 可选：写 CSV 文件
    if SAVE_CSV and df_rows is not None:
        out = "probe_preview.csv"
        try:
            df_rows.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"\n已写出样例到 {out}")
        except Exception as e:
            print("写 CSV 失败：", e)


if __name__ == "__main__":
    main()
