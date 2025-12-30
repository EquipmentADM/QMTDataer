# -*- coding: utf-8 -*-
"""
用 xtdata：先本地下载(同周期) -> 再获取 -> 各标的导出/更新 CSV（单一模式）
目录结构：
  ROOT/
    {code}/
      {period_dir}/
        original/
          {code}_{period_dir}.csv

周期目录命名规则示例：
  1m -> 1min, 5m -> 5min, 10m -> 10min, 15m -> 15min, 1d -> 1d, 1w -> 1w, 1s -> 1s
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List

from xtquant import xtdata  # type: ignore
import pandas as pd
import numpy as np


# ===== 直接改这里 =====
ROOT_DIR: Path = Path(r"D:\Work\Quant\financial_database\SS_stock_data")
CODES: List[str] = ["518880.SH", "513880.SH"]   # 标的列表
PERIOD: str = "1m"      # 直接使用你想取的周期；不考虑合成/基础周期映射
START: str = ""          # 留空 -> 尽可能多
END: str = ""            # 留空 -> 尽可能多
COUNT: int = -1          # -1：按时间范围（当 START/END 皆空时通常返回本地可用的全部）
FIELD_LIST: List[str] = []  # 空列表 = 全字段（time/open/high/low/close/volume/amount/...）
# =====================


def period_to_dirname(period: str) -> str:
    """
    功能：将 xtdata 的周期字符串转为目录命名。
    上下游：
      上游：PERIOD
      下游：作为二级目录与文件名的一部分
    规则：
      - '1m' -> '1min'、'5m'->'5min'、'10m'->'10min'、'15m'->'15min'
      - '1d' -> '1d'
      - '1w' -> '1w'
      - '1s' -> '1s'
      - 其他 m 结尾 -> {N}min；s 结尾 -> {N}s；d/w 结尾 -> 原样
    """
    p = period.strip().lower()
    if p.endswith("m"):
        n = p[:-1]
        return f"{int(n)}min"
    if p.endswith("s"):
        n = p[:-1]
        return f"{int(n)}s"
    if p.endswith("d") or p.endswith("w"):
        return p
    # 兜底：按原样
    return p


def _ensure_time_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    功能：确保存在 'time' 列。
    - 若 'time' 在索引且索引名为 'time'，则 reset_index() 成为列；
    - 否则若本就有 'time' 列则原样返回；
    - 否则抛错（保持单一模式简洁）。
    """
    if "time" in df.columns:
        return df
    if str(getattr(df.index, "name", "")).lower() == "time":
        return df.reset_index()
    raise ValueError("DataFrame 缺少 'time' 列（且索引名也不是 'time'）")


def _format_time_column(df: pd.DataFrame, col: str = "time") -> pd.DataFrame:
    """
    功能：将 time 列统一格式化为 'YYYYMMDD HH:MM:SS'（如 '20150106 00:00:00'）。
    策略：
      1) 数值：>=1e12 视为毫秒时间戳；[1e9,1e12) 视为秒；其余尝试直接 to_datetime
      2) 字符串：长度14 -> '%Y%m%d%H%M%S'；长度8 -> '%Y%m%d'(补 00:00:00)；其他交给 to_datetime
      3) 失败 -> 空字符串
    """
    if col not in df.columns:
        raise KeyError(f"DataFrame 不包含列：{col}")

    s = df[col]

    if np.issubdtype(s.dtype, np.number):
        ms_mask = s >= 1_000_000_000_000
        sec_mask = (~ms_mask) & (s >= 1_000_000_000)
        dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        if ms_mask.any():
            dt.loc[ms_mask] = pd.to_datetime(s.loc[ms_mask], unit="ms", utc=False, errors="coerce")
        if sec_mask.any():
            dt.loc[sec_mask] = pd.to_datetime(s.loc[sec_mask], unit="s", utc=False, errors="coerce")
        other_mask = ~(ms_mask | sec_mask)
        if other_mask.any():
            dt.loc[other_mask] = pd.to_datetime(s.loc[other_mask], utc=False, errors="coerce")
    else:
        s_str = s.astype(str).str.strip()
        dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        mask14 = s_str.str.len() == 14
        if mask14.any():
            dt.loc[mask14] = pd.to_datetime(s_str.loc[mask14], format="%Y%m%d%H%M%S", errors="coerce")
        mask8 = s_str.str.len() == 8
        if mask8.any():
            dt.loc[mask8] = pd.to_datetime(s_str.loc[mask8], format="%Y%m%d", errors="coerce")
        other_mask = ~(mask14 | mask8)
        if other_mask.any():
            dt.loc[other_mask] = pd.to_datetime(s_str.loc[other_mask], errors="coerce")

    out = dt.dt.strftime("%Y%m%d %H:%M:%S").fillna("")
    df = df.copy()
    df[col] = out
    return df


def _download_locally_before_fetch(codes: List[str], period: str, start: str, end: str) -> None:
    """
    功能：获取前先本地下载同周期数据（不考虑合成问题）。
    约定：incrementally 参数不使用；start/end 允许空串以尽可能下载全量。
    """
    for code in codes:
        xtdata.download_history_data(code, period, start, end)  # 不传 incrementally


def build_paths(root: Path, code: str, period: str) -> Path:
    """
    功能：构建文件保存最终路径（含目录），不创建。
    结构：root / {code} / {period_dir} / original / {code}_{period_dir}.csv
    """
    period_dir = period_to_dirname(period)
    dir_path = root / code / period_dir / "original"
    file_path = dir_path / f"{code}_{period_dir}.csv"
    return file_path


def save_or_update_csv(final_df: pd.DataFrame, root: Path, code: str, period: str) -> Path:
    """
    功能：保存或更新 CSV（若文件已存在则合并去重并按时间排序后覆盖）。
    上游：已完成 time 列格式化为 'YYYYMMDD HH:MM:SS' 的 DataFrame
    下游：磁盘文件
    规则：
      - 目录不存在则创建
      - 存在CSV：读取为字符串列，按 'time' 去重（保留较新记录），按 'time' 升序
      - 不存在CSV：直接保存
    """
    out_path = build_paths(root, code, period)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        # 读旧文件（time 列按 str 读入）
        old = pd.read_csv(out_path, dtype={"time": "string"})
        # 合并 & 去重（以 time 为键，保留新数据的值）
        merged = pd.concat([old, final_df], axis=0, ignore_index=True)
        merged = merged.drop_duplicates(subset=["time"], keep="last")
        # 按时间升序（字符串比较在该格式下等价于时间顺序）
        merged = merged.sort_values("time", kind="mergesort").reset_index(drop=True)
        merged.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path
    else:
        final_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path


def main() -> None:
    """主流程：下载 -> 获取 -> 逐标的格式化时间 -> 三层路径保存/更新。"""
    # 1) 先下载（同周期；不考虑合成）
    _download_locally_before_fetch(CODES, PERIOD, START, END)

    # 2) 获取（单一模式：返回 dict[code -> DataFrame]；field_list 空=全字段）
    kwargs = dict(
        field_list=FIELD_LIST,
        stock_list=CODES,
        period=PERIOD,
        start_time=START,
        end_time=END,
        count=COUNT,
        dividend_type="none",
        fill_data=False,
    )
    try:
        data_dict: Dict[str, Any] = xtdata.get_market_data_ex(**kwargs)  # type: ignore[attr-defined]
    except Exception:
        data_dict = xtdata.get_market_data(**kwargs)

    if not isinstance(data_dict, dict) or not data_dict:
        raise RuntimeError("xtdata 返回空数据")

    # 3) 逐标的：确保/格式化 time -> 保存或更新 CSV
    for code, df in data_dict.items():
        if code not in CODES:
            continue
        if not isinstance(df, pd.DataFrame) or df.empty:
            print(f"[WARN] {code} 无数据")
            continue

        df = _ensure_time_column(df)
        df = _format_time_column(df, "time")

        out_path = save_or_update_csv(df, ROOT_DIR, code, PERIOD)
        print(f"[DONE] {code} -> {out_path}（新增或合并后总计 {len(pd.read_csv(out_path))} 条）")


if __name__ == "__main__":
    main()
