# -*- coding: utf-8 -*-
"""
xtdata 集成校验脚本（函数化版本）：直接在 __main__ 中填写参数运行。
Responsibilities:
    - 校验 xtdata 连通性
    - 基于 XtdataSource + MarketDataIngestor + FinancialDataStorage 拉取并落盘
    - 可选自动起始时间（根据已有文件最新时间回溯 N 条）、可选跳过 download
Data Contract:
    - 需已登录 miniqmt/xtdata 环境且有行情权限
    - 输出目录需可写
Notes:
    - 默认根目录 D:/Work/Quant/financial_database
    - 默认周期 1d/1m，默认标的为预设 ETF 列表
用法：
    直接右键/运行本文件。根据需要在 __main__ 中切换不同示例调用（注释/取消注释）。
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

# ---- sys.path 处理 ----
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.xtdata_source import XtdataSource  # noqa: E402
from core.ingestor import MarketDataIngestor  # noqa: E402
from core.storage_simple import FinancialDataStorage  # noqa: E402

DEFAULT_ROOT = "D:/Work/Quant/financial_database"
DEFAULT_CYCLES = ["1d", "1m"]
DEFAULT_SYMBOLS: List[str] = [
    "159915.SZ",  # 创业板ETF
    "518880.SH",  # 黄金ETF
    "513880.SH",  # 恒生科技ETF
    "513100.SH",  # 纳斯达克100ETF
    "513030.SH",  # 德国ETF
    "513080.SH",  # 法国CAC40ETF
    "513180.SH",  # 日本东证ETF
    "510300.SH",  # 沪深300ETF
    "511010.SH",  # 国债ETF（1-3年期）
    "159980.SZ",  # 有色ETF
    # "513730.SH",  # 东南亚科技ETF
    "563300.SH",  # 中证2000ETF
    "563680.SH",
    "515790.SH",
    "513130.SH",
    "512480.SH",
    "510050.SH",
    "000001.SH",
    "563580.SH",
    "510880.SH",
    "159985.SZ",
    "513400.SH",
    "513520.SH",
    "513600.SH",
    "159119.SZ",
]


def _import_xtdata():
    """
    动态导入 xtdata，便于在无环境时给出友好提示。
    Returns:
        module | None: 成功导入的 xtdata 模块。
    """
    try:
        from xtquant import xtdata  # type: ignore
        return xtdata
    except Exception:
        try:
            import xtdata  # type: ignore
            return xtdata
        except Exception as exc:  # pragma: no cover
            print(f"[ERROR] 无法导入 xtdata，请确认已安装并在有 miniqmt 的环境中运行，原因: {exc}")
            return None


def _probe_xtdata(xtdata_mod, symbol: str, cycle: str, start: str, end: str) -> bool:
    """
    小范围探测，确认 xtdata 可用且有数据。
    """
    try:
        xtdata_mod.download_history_data(
            stock_code=symbol,
            period=cycle,
            start_time=start,
            end_time=end,
            incrementally=True,
        )
    except Exception as exc:  # pragma: no cover
        print(f"[WARN] download_history_data 失败，可能是权限/时间范围问题，将继续尝试读取：{exc}")
    try:
        data_dict = xtdata_mod.get_market_data_ex(
            stock_list=[symbol],
            period=cycle,
            start_time=start,
            end_time=end,
            count=-1,
            dividend_type="none",
            fill_data=False,
            field_list=[],
        )
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] get_market_data_ex 调用失败：{exc}")
        return False
    if not isinstance(data_dict, dict) or not data_dict:
        print("[ERROR] 探测失败：返回为空，可能未连接 miniqmt 或无行情权限")
        return False

    # 兼容两种结构
    if "time" in data_dict:
        time_df = data_dict.get("time")
        if time_df is None or time_df.empty:
            print("[ERROR] 探测失败：time 为空")
            return False
        print(f"[OK] 探测成功：{symbol} {cycle} 返回 {time_df.shape[1]} 根 K 线（field 模式）")
        return True

    first_df = next((v for v in data_dict.values() if hasattr(v, "columns")), None)
    if first_df is None or first_df.empty:
        print("[ERROR] 探测失败：返回的 DataFrame 为空")
        return False
    time_cols = [c for c in ("time", "Time", "datetime", "bar_time") if c in first_df.columns]
    if not time_cols or first_df[time_cols[0]].dropna().empty:
        print("[ERROR] 探测失败：时间列为空或缺失")
        return False
    print(f"[OK] 探测成功：{symbol} {cycle} 返回 {len(first_df)} 行（code->DataFrame 模式）")
    return True


def _validate_file(path: Path) -> Tuple[bool, str]:
    """
    基础质量校验：列齐全、非空、时间升序。
    """
    required_cols = {"time", "open", "high", "low", "close", "volume", "amount"}
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        return False, f"读取失败: {exc}"
    if df.empty:
        return False, "文件为空"
    if not required_cols.issubset(df.columns):
        return False, f"缺少列: {required_cols - set(df.columns)}"
    if not df["time"].is_monotonic_increasing:
        return False, "time 未按升序排列"
    return True, ""


def _infer_freq_timedelta(cycle: str):
    """
    基于周期字符串推断 Timedelta，用于回溯 lookback 个 bar。
    """
    c = cycle.lower()
    if c.endswith("m"):
        try:
            minutes = int(c.rstrip("m"))
            return pd.to_timedelta(minutes, unit="m")
        except Exception:
            return pd.to_timedelta(1, unit="m")
    if c.endswith("h"):
        try:
            hours = int(c.rstrip("h"))
            return pd.to_timedelta(hours, unit="h")
        except Exception:
            return pd.to_timedelta(1, unit="h")
    if c.endswith("d"):
        return pd.to_timedelta(1, unit="d")
    return None


def run_ingest(
    symbols: Iterable[str] = DEFAULT_SYMBOLS,
    cycles: Iterable[str] = DEFAULT_CYCLES,
    root: str = DEFAULT_ROOT,
    market: str = "SS_stock_data",
    specific: str = "original",
    start: str = "20000101",
    end: str = "",
    skip_download: bool = False,
    auto_start: bool = False,
    lookback: int = 2,
) -> None:
    """
    执行一次批量拉取与落盘。
    Args:
        symbols (Iterable[str]): 标的列表。
        cycles (Iterable[str]): 周期列表。
        root (str): 输出根目录。
        market (str): 市场名称。
        specific (str): 合成标记/子目录。
        start (str): 起始时间（xtdata 格式）。
        end (str): 结束时间。
        skip_download (bool): 是否跳过 download_history_data。
        auto_start (bool): 是否基于已有文件最新时间回溯 lookback 条作为 start。
        lookback (int): 回溯条数。
    """
    xtdata_mod = _import_xtdata()
    if xtdata_mod is None:
        sys.exit(1)

    probe_symbol = next(iter(symbols))
    probe_cycle = next(iter(cycles))
    if not _probe_xtdata(xtdata_mod, probe_symbol, probe_cycle, start, end):
        sys.exit(1)

    storage = FinancialDataStorage(root_dir=root)
    ingestor = MarketDataIngestor(storage)
    ok_files: List[Path] = []
    failed: List[str] = []

    for cycle in cycles:
        source = XtdataSource(xtdata=xtdata_mod, download=not skip_download)
        for symbol in symbols:
            start_use = start
            if auto_start:
                target_dir = storage._build_target_dir(market, symbol, cycle, specific)
                filename = storage._build_filename(market, symbol, cycle, specific, "csv")
                file_path = Path(target_dir) / filename
                if file_path.exists():
                    try:
                        df_exist = pd.read_csv(file_path, usecols=["time"])
                        if not df_exist.empty:
                            latest = pd.to_datetime(df_exist["time"], errors="coerce").dropna().max()
                            if pd.notna(latest):
                                freq = _infer_freq_timedelta(cycle)
                                if freq is not None and lookback > 0:
                                    latest = latest - freq * lookback
                                if cycle.lower().endswith("m") or cycle.lower().endswith("h"):
                                    start_use = latest.strftime("%Y%m%d%H%M%S")
                                else:
                                    start_use = latest.strftime("%Y%m%d")
                                print(f"[AUTO] {symbol} {cycle} 使用基于历史的 start={start_use}")
                    except Exception:
                        pass

            print(f"[RUN] 拉取 {symbol} {cycle} ...")
            try:
                out_path = ingestor.ingest_symbol(
                    source=source,
                    market=market,
                    symbol=symbol,
                    cycle=cycle,
                    specific=specific,
                    start=start_use,
                    end=end,
                    file_type="csv",
                    time_column="time",
                    merge=True,
                )
                ok, reason = _validate_file(Path(out_path))
                if ok:
                    ok_files.append(Path(out_path))
                    print(f"[OK] 写入完成: {out_path}")
                else:
                    failed.append(f"{symbol}-{cycle}: {reason}")
                    print(f"[FAIL] 校验失败 {symbol}-{cycle}: {reason}")
            except Exception as exc:  # pragma: no cover
                failed.append(f"{symbol}-{cycle}: {exc}")
                print(f"[ERROR] 处理 {symbol}-{cycle} 失败: {exc}")

    print("\n[SUMMARY]")
    print(f"- 成功文件数: {len(ok_files)}")
    if ok_files:
        for p in ok_files:
            print(f"  * {p}")
    if failed:
        print(f"- 失败项: {len(failed)}")
        for item in failed:
            print(f"  * {item}")
        sys.exit(1)
    else:
        print("- 全部通过")


if __name__ == "__main__":
    # ---- 示例 1：快速增量更新（建议默认用这个）----
    run_ingest(
        symbols=DEFAULT_SYMBOLS,
        cycles=["1d", "1m"],
        root=DEFAULT_ROOT,
        start="20000101",
        end="",
        skip_download=True,            # 不调用 download_history_data，直接 get
        auto_start=True,               # 基于已有文件时间回溯 lookback 个 bar
        lookback=10,
    )

    # ---- 示例 2：全量拉取（需要时取消注释）----
    # run_ingest(
    #     symbols=DEFAULT_SYMBOLS,
    #     cycles=["1d", "1m"],
    #     root=DEFAULT_ROOT,
    #     start="20000101",
    #     end="",
    #     skip_download=False,
    #     auto_start=False,
    # )
