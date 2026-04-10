# -*- coding: utf-8 -*-
"""
历史行情 API 试验脚本：先测股票，再测期货主力连续。

用途：
    1. 复用项目里已经验证过的 xtdata 历史取数流程：
       download_history_data -> get_market_data_ex
    2. 先用 510050.SH 做环境与基础链路校验；
    3. 再尝试 rb00 及若干常见主力连续别名，观察期货历史是否能拉下来。

说明：
    - 本脚本只做“历史 API 能否拉到数据”的实验，不涉及实时订阅。
    - 若股票也失败，通常优先怀疑当前 xtdata / MiniQMT 环境本身有问题。
    - 若股票成功、期货失败，则优先怀疑期货代码写法或权限/市场支持问题。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

import pandas as pd

try:
    from xtquant import xtdata
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"无法导入 xtquant.xtdata，请确认当前环境可用：{exc}")


CN_TZ = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class ProbeTarget:
    """单个探测目标。"""

    symbol: str
    period: str
    start: str
    end: str
    note: str


def _safe_download(symbol: str, period: str, start: str, end: str) -> None:
    """按项目当前通用流程，先尝试补齐本地缓存。"""
    try:
        xtdata.download_history_data(
            stock_code=symbol,
            period=period,
            start_time=start,
            end_time=end,
            incrementally=True,
        )
        print(f"  [下载] 已触发 download_history_data: {symbol} {period} {start}~{end}")
    except Exception as exc:
        print(f"  [下载] 调用失败，继续直接 get: {symbol} err={exc}")


def _safe_get(symbol: str, period: str, start: str, end: str) -> Dict[str, Any]:
    """优先调用 get_market_data_ex，保持与项目现有历史链路一致。"""
    kwargs = dict(
        field_list=[],
        stock_list=[symbol],
        period=period,
        start_time=start,
        end_time=end,
        count=-1,
        dividend_type="none",
        fill_data=False,
    )
    return xtdata.get_market_data_ex(**kwargs)


def _format_ts(raw: Any) -> str:
    """将常见时间值转成便于日志阅读的本地时间字符串。"""
    if pd.isna(raw):
        return ""
    try:
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts >= 1e12:
                dt = datetime.fromtimestamp(ts / 1000.0, tz=CN_TZ)
            elif ts >= 1e9:
                dt = datetime.fromtimestamp(ts, tz=CN_TZ)
            else:
                s = str(int(ts))
                if len(s) == 8:
                    dt = datetime.strptime(s, "%Y%m%d").replace(tzinfo=CN_TZ)
                elif len(s) == 14:
                    dt = datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
                else:
                    return str(raw)
            return dt.astimezone(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        s = str(raw).strip()
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d %H:%M:%S")
        if len(s) == 14 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
        return s
    except Exception:
        return str(raw)


def _normalize_preview(data: Dict[str, Any], symbol: str) -> pd.DataFrame:
    """
    兼容 xtdata 两种常见返回结构，并抽取预览宽表。

    返回列尽量统一为：
        time/open/high/low/close/volume/amount
    """
    if not isinstance(data, dict) or not data:
        return pd.DataFrame()

    # ---- 结构一：field -> DataFrame ----
    if "time" in data and isinstance(data.get("time"), pd.DataFrame):
        time_df: pd.DataFrame = data["time"]
        if symbol not in time_df.index:
            return pd.DataFrame()
        rows = []
        for col in time_df.columns:
            row = {"time": _format_ts(time_df.loc[symbol, col])}
            for field in ("open", "high", "low", "close", "volume", "amount"):
                field_df = data.get(field)
                if isinstance(field_df, pd.DataFrame) and symbol in field_df.index and col in field_df.columns:
                    row[field] = field_df.loc[symbol, col]
            rows.append(row)
        return pd.DataFrame(rows)

    # ---- 结构二：code -> DataFrame ----
    df_code = data.get(symbol)
    if isinstance(df_code, pd.DataFrame):
        df = df_code.copy()
        time_col = next((c for c in ("time", "Time", "datetime", "bar_time") if c in df.columns), None)
        if time_col is None:
            return pd.DataFrame()
        df["time"] = df[time_col].map(_format_ts)
        keep_cols = [c for c in ("time", "open", "high", "low", "close", "volume", "amount") if c in df.columns]
        return df[keep_cols].reset_index(drop=True)

    # ---- 兜底：取第一个 DataFrame 看看 ----
    first_df = next((v for v in data.values() if isinstance(v, pd.DataFrame)), None)
    if first_df is None:
        return pd.DataFrame()
    df = first_df.copy()
    time_col = next((c for c in ("time", "Time", "datetime", "bar_time") if c in df.columns), None)
    if time_col is None:
        return pd.DataFrame()
    df["time"] = df[time_col].map(_format_ts)
    keep_cols = [c for c in ("time", "open", "high", "low", "close", "volume", "amount") if c in df.columns]
    return df[keep_cols].reset_index(drop=True)


def probe_one(target: ProbeTarget) -> bool:
    """对单个目标执行一次完整探测，并打印关键结果。"""
    print("\n" + "=" * 88)
    print(
        f"[开始探测] 标的={target.symbol} 周期={target.period} 区间={target.start}~{target.end} 说明={target.note}"
    )

    _safe_download(target.symbol, target.period, target.start, target.end)

    try:
        data = _safe_get(target.symbol, target.period, target.start, target.end)
    except Exception as exc:
        print(f"[失败] get_market_data_ex 调用异常：{exc}")
        return False

    if not isinstance(data, dict) or not data:
        print("[失败] xtdata 返回空字典。")
        return False

    print(f"[返回] 顶层 keys: {list(data.keys())[:8]}")
    preview = _normalize_preview(data, target.symbol)
    if preview.empty:
        print("[失败] 返回结构存在，但未能解析出有效 K 线预览。")
        first_df = next((v for v in data.values() if isinstance(v, pd.DataFrame)), None)
        if first_df is not None:
            print(f"[辅助] 首个 DataFrame 列名: {list(first_df.columns)}")
            print(first_df.head(3).to_string())
        return False

    print(f"[成功] 实际拉到 {len(preview)} 条记录。")
    print(f"[时间] 首条={preview['time'].iloc[0]} 末条={preview['time'].iloc[-1]}")
    print("[样例] 前 3 行：")
    print(preview.head(3).to_string(index=False))
    print("[样例] 后 3 行：")
    print(preview.tail(3).to_string(index=False))
    return True


def build_targets() -> Iterable[ProbeTarget]:
    """构造本次实验目标集合。"""
    today = datetime.now(CN_TZ).strftime("%Y%m%d")
    thirty_days_ago = (datetime.now(CN_TZ) - timedelta(days=30)).strftime("%Y%m%d")
    one_year_ago = (datetime.now(CN_TZ) - timedelta(days=365)).strftime("%Y%m%d")

    # 先测股票，确认基础流程与环境正常。
    yield ProbeTarget(
        symbol="510050.SH",
        period="1d",
        start=thirty_days_ago,
        end=today,
        note="股票基准样例，先确认环境与调用流程正常。",
    )

    # 再逐个测试期货主力连续常见写法。
    futures_candidates = (
        "rb00",
        "rb00.SF",
        "rb00.SHF",
        "rb00.SHFE",
        "rb888",
        "rb888.SF",
        "rb888.SHF",
        "rb888.SHFE",
    )
    for symbol in futures_candidates:
        yield ProbeTarget(
            symbol=symbol,
            period="1d",
            start=one_year_ago,
            end=today,
            note="期货主力连续候选写法测试。",
        )


def main() -> None:
    """主入口。"""
    success_count = 0
    total_count = 0
    for target in build_targets():
        total_count += 1
        if probe_one(target):
            success_count += 1

    print("\n" + "=" * 88)
    print(f"[探测结束] 总任务={total_count} 成功={success_count} 失败={total_count - success_count}")


if __name__ == "__main__":
    main()
