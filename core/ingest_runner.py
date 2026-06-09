# -*- coding: utf-8 -*-
"""
xtdata 入库执行器。

职责：
    - 统一管理全量下载、全量补齐、近期补齐三种入库模式；
    - 同时支持股票与期货目标池；
    - 将“xtdata 取数代码”和“FD 落盘对象”拆开处理；
    - 输出结构化中文日志，便于观察每个任务的抓取与落盘结果。
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Optional

import pandas as pd

from core.ingestor import MarketDataIngestor
from core.storage_backend import build_storage_backend, resolve_storage_backend_config
from core.storage_simple import FinancialDataStorage
from core.xtdata_futures import is_xt_futures_code, parse_xt_futures_code
from core.xtdata_source import MappedXtdataSource, XtdataSource


CN_TZ = timezone(timedelta(hours=8))
DEFAULT_ROOT = "D:/Work/Quant/financial_database"
DEFAULT_MARKET = "SS_stock_data"
DEFAULT_SPECIFIC = "original"
DEFAULT_CYCLES = ("1d", "1m")

DEFAULT_STOCK_SYMBOLS = (
    "159915.SZ",
    "518880.SH",
    "513880.SH",
    "513100.SH",
    "513030.SH",
    "513080.SH",
    "513180.SH",
    "510300.SH",
    "511090.SH",
    "511010.SH",
    "159980.SZ",
    "159552.SZ",
    "563300.SH",
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
    "164824.SZ",
    "510210.SH",
    "510500.SH",
    "159845.SZ",
    "588000.SH",
    "515080.SH",
    "159996.SZ",
    "159930.SZ",
    "159611.SZ",
    "510410.SH",
    "515220.SH",
    "513310.SH",
    "510230.SH",
    "512000.SH",
    "512710.SH",
    "512670.SH",
    "159929.SZ",
    "515400.SH",
    "515880.SH",
    "159732.SZ",
    "516160.SH",
    "515030.SH",
    "512800.SH",
    "513750.SH",
    "562500.SH",
    "513120.SH",
    "561800.SH",
    "513190.SH",
    "512770.SH",
    "560610.SH",
    "510150.SH",
    "516020.SH",
    "515980.SH",
    "513590.SH",
    "515000.SH",
    "515230.SH",
    "515120.SH",
    "513530.SH",
    "512070.SH",
    "516760.SH",
    "159707.SZ",
    "588200.SH",
    "516860.SH",
    "159583.SZ",
    "501312.SH",
    "513060.SH",
    "159509.SZ",
    "159326.SZ",
    "159697.SZ",
)

DEFAULT_STOCK_SYMBOLS_RECENT = (
    "513880.SH",
    "518880.SH",
    "000001.SH",
)

# 常用期货池：黑色系、化工、股指、贵金属。
DEFAULT_FUTURES_SYMBOLS = (
    "rb00.SF",
    "hc00.SF",
    "i00.DF",
    "j00.DF",
    "jm00.DF",
    "MA00.ZF",
    "TA00.ZF",
    "FG00.ZF",
    "pp00.DF",
    "eg00.DF",
    "IF00.IF",
    "IH00.IF",
    "IC00.IF",
    "IM00.IF",
    "au00.SF",
    "ag00.SF",
)

DEFAULT_FUTURES_SYMBOLS_RECENT = (
    "rb00.SF",
    "MA00.ZF",
    "IF00.IF",
    "au00.SF",
)

DEFAULT_SYMBOLS = DEFAULT_STOCK_SYMBOLS + DEFAULT_FUTURES_SYMBOLS
DEFAULT_SYMBOLS_RECENT = DEFAULT_STOCK_SYMBOLS_RECENT + DEFAULT_FUTURES_SYMBOLS_RECENT


@dataclass(frozen=True)
class IngestTarget:
    """单个入库目标。"""

    fetch_symbol: str
    storage_symbol: str
    market: str
    specific: str
    display_name: str


@dataclass(frozen=True)
class IngestProfile:
    """
    入库模式配置对象。

    `symbols` 支持混合写入：
        - 股票：直接写如 `510050.SH`
        - 期货：直接写如 `rb00.SF`
    执行时会自动识别期货代码并映射为 FD 目标。
    """

    name: str
    symbols: tuple[str, ...]
    cycles: tuple[str, ...]
    root: str = DEFAULT_ROOT
    market: str = DEFAULT_MARKET
    specific: str = DEFAULT_SPECIFIC
    start: str = "20000101"
    end: str = ""
    skip_download: bool = False
    auto_start: bool = False
    lookback: int = 2
    merge: bool = True
    storage_backend: str = ""
    fd_repo: str = ""


def _default_profiles() -> dict[str, IngestProfile]:
    """返回默认模式配置。"""
    return {
        "full-download": IngestProfile(
            name="full-download",
            symbols=DEFAULT_SYMBOLS,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=False,
            lookback=0,
            merge=False,
        ),
        "full-backfill": IngestProfile(
            name="full-backfill",
            symbols=DEFAULT_SYMBOLS,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=False,
            lookback=2,
            merge=True,
        ),
        "recent-backfill": IngestProfile(
            name="recent-backfill",
            symbols=DEFAULT_SYMBOLS_RECENT,
            cycles=DEFAULT_CYCLES,
            skip_download=False,
            auto_start=True,
            lookback=2,
            merge=True,
        ),
    }


def list_profile_names() -> tuple[str, ...]:
    """返回可用模式名。"""
    return tuple(_default_profiles().keys())


def build_profile(name: str, **overrides: Any) -> IngestProfile:
    """根据模式名构建 profile，并允许字段覆盖。"""
    profiles = _default_profiles()
    if name not in profiles:
        raise ValueError(f"未知模式: {name}，可选 {list_profile_names()}")
    profile = profiles[name]
    if not overrides:
        return profile

    fields = set(profile.__dataclass_fields__.keys())
    safe_overrides = {k: v for k, v in overrides.items() if k in fields and v is not None}
    if "symbols" in safe_overrides:
        safe_overrides["symbols"] = tuple(safe_overrides["symbols"])
    if "cycles" in safe_overrides:
        safe_overrides["cycles"] = tuple(safe_overrides["cycles"])
    return replace(profile, **safe_overrides)


def _import_xtdata() -> Any:
    """动态导入 xtdata 模块。"""
    try:
        from xtquant import xtdata  # type: ignore

        return xtdata
    except Exception:
        try:
            import xtdata  # type: ignore

            return xtdata
        except Exception as exc:
            raise RuntimeError("无法导入 xtdata，请确认 MiniQMT 环境可用。") from exc


def _resolve_end_time(end_text: str) -> str:
    """解析结束时间；为空时默认取“当前日期 + 1 天”。"""
    if end_text:
        return end_text
    return (pd.Timestamp.now(tz=CN_TZ) + pd.Timedelta(days=1)).strftime("%Y%m%d")


def _probe_xtdata(xtdata_mod: Any, target: IngestTarget, cycle: str, start: str, end: str) -> None:
    """运行前探针：提前验证 xtdata 可用性。"""
    try:
        xtdata_mod.download_history_data(
            stock_code=target.fetch_symbol,
            period=cycle,
            start_time=start,
            end_time=end,
            incrementally=True,
        )
    except Exception:
        pass

    data_dict = xtdata_mod.get_market_data_ex(
        stock_list=[target.fetch_symbol],
        period=cycle,
        start_time=start,
        end_time=end,
        count=-1,
        dividend_type="none",
        fill_data=False,
        field_list=[],
    )
    if not isinstance(data_dict, dict) or not data_dict:
        raise RuntimeError(
            f"xtdata 探针返回空数据，可能未连接 MiniQMT 或无行情权限: {target.fetch_symbol} {cycle}"
        )


def _infer_freq_timedelta(cycle: str) -> Optional[pd.Timedelta]:
    """根据周期字符串推断时间间隔。"""
    cycle_low = cycle.lower()
    if cycle_low.endswith("m"):
        try:
            return pd.to_timedelta(int(cycle_low[:-1]), unit="m")
        except Exception:
            return pd.to_timedelta(1, unit="m")
    if cycle_low.endswith("h"):
        try:
            return pd.to_timedelta(int(cycle_low[:-1]), unit="h")
        except Exception:
            return pd.to_timedelta(1, unit="h")
    if cycle_low.endswith("d"):
        return pd.to_timedelta(1, unit="d")
    return None


def _time_range_from_series(series: pd.Series) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """解析时间列并返回最小/最大时间。"""
    ts = FinancialDataStorage._parse_time_series(series)
    ts = ts.dropna()
    if ts.empty:
        return None, None
    return ts.min(), ts.max()


def _format_ts(ts: Optional[pd.Timestamp]) -> str:
    """格式化时间戳用于中文输出。"""
    if ts is None or pd.isna(ts):
        return "-"
    try:
        return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def _build_file_path(
    storage: Any,
    target: IngestTarget,
    cycle: str,
    file_type: str = "csv",
    root: str = DEFAULT_ROOT,
) -> Path:
    """构造目标文件路径。"""
    _ = root
    target_dir = storage._build_target_dir(target.market, target.storage_symbol, cycle, target.specific)
    filename = storage._build_filename(target.market, target.storage_symbol, cycle, target.specific, file_type)
    return Path(target_dir) / filename


def _read_existing_summary(file_path: Path) -> dict[str, Any]:
    """读取现有文件摘要信息。"""
    if not file_path.exists():
        return {"exists": False, "rows": 0, "time_min": None, "time_max": None}
    try:
        df = pd.read_csv(file_path)
    except Exception:
        return {"exists": True, "rows": 0, "time_min": None, "time_max": None}
    if "time" in df.columns and not df.empty:
        tmin, tmax = _time_range_from_series(df["time"])
    else:
        tmin, tmax = None, None
    return {"exists": True, "rows": len(df), "time_min": tmin, "time_max": tmax}


def _calc_start_use(
    profile: IngestProfile,
    cycle: str,
    existing_max: Optional[pd.Timestamp],
) -> str:
    """根据 auto_start 与 lookback 计算本次任务起始时间。"""
    if not profile.auto_start or existing_max is None:
        return profile.start
    latest = existing_max
    freq = _infer_freq_timedelta(cycle)
    if freq is not None and profile.lookback > 0:
        latest = latest - freq * profile.lookback
    if cycle.lower().endswith(("m", "h")):
        return latest.strftime("%Y%m%d%H%M%S")
    return latest.strftime("%Y%m%d")


def _validate_output_file(path: Path) -> tuple[bool, str]:
    """校验输出文件是否满足最小要求。"""
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


def _build_targets(profile: IngestProfile) -> list[IngestTarget]:
    """将 profile.symbols 转为具体执行目标列表。"""
    targets: list[IngestTarget] = []
    for raw_symbol in profile.symbols:
        if is_xt_futures_code(raw_symbol):
            future = parse_xt_futures_code(raw_symbol)
            targets.append(
                IngestTarget(
                    fetch_symbol=future.source_code,
                    storage_symbol=future.symbol,
                    market=future.market,
                    specific=future.specific,
                    display_name=f"{future.source_code} -> {future.market}/{future.symbol}/{future.specific}",
                )
            )
        else:
            targets.append(
                IngestTarget(
                    fetch_symbol=raw_symbol,
                    storage_symbol=raw_symbol,
                    market=profile.market,
                    specific=profile.specific,
                    display_name=raw_symbol,
                )
            )
    return targets


def run_ingest(profile: IngestProfile) -> dict[str, Any]:
    """按指定 profile 执行一次入库任务。"""
    if not profile.symbols:
        raise ValueError("profile.symbols 不能为空。")
    if not profile.cycles:
        raise ValueError("profile.cycles 不能为空。")

    targets = _build_targets(profile)
    start_wall = perf_counter()
    xtdata_mod = _import_xtdata()
    end_use = _resolve_end_time(profile.end)
    _probe_xtdata(
        xtdata_mod=xtdata_mod,
        target=targets[0],
        cycle=profile.cycles[0],
        start=profile.start,
        end=end_use,
    )

    storage_config = resolve_storage_backend_config(
        root=profile.root,
        backend=profile.storage_backend or None,
        fd_repo=profile.fd_repo or None,
    )
    storage = build_storage_backend(storage_config)
    ingestor = MarketDataIngestor(storage)

    total_tasks = len(targets) * len(profile.cycles)
    updated_tasks = 0
    not_updated_tasks = 0
    failed_tasks = 0
    ok_files: list[str] = []
    failed_msgs: list[str] = []

    print(
        f"[入库总览] 模式={profile.name} 标的={len(targets)} 周期={len(profile.cycles)} "
        f"总任务={total_tasks} backend={storage_config.backend} root={storage_config.root}"
    )
    print(
        f"[入库参数] start={profile.start} end={end_use} skip_download={profile.skip_download} "
        f"auto_start={profile.auto_start} lookback={profile.lookback} merge={profile.merge}"
    )

    seq = 0
    for cycle in profile.cycles:
        base_source = XtdataSource(xtdata=xtdata_mod, download=not profile.skip_download)
        for target in targets:
            seq += 1
            file_path = _build_file_path(storage, target, cycle, file_type="csv", root=storage_config.root)
            existing = _read_existing_summary(file_path)
            start_use = _calc_start_use(profile, cycle, existing["time_max"])
            source = MappedXtdataSource(inner=base_source, fetch_symbol=target.fetch_symbol)

            print(
                f"[任务 {seq}/{total_tasks}] {target.display_name} {cycle} | 原有行={existing['rows']} "
                f"原有时间={_format_ts(existing['time_min'])}~{_format_ts(existing['time_max'])} "
                f"| 预计起始={start_use} 结束={end_use}"
            )

            fetch_meta: dict[str, Any] = {"rows": 0, "time_min": None, "time_max": None}

            def _capture_fetch(df: pd.DataFrame) -> pd.DataFrame:
                fetch_meta["rows"] = len(df)
                if "time" in df.columns and not df.empty:
                    tmin, tmax = _time_range_from_series(df["time"])
                    fetch_meta["time_min"] = tmin
                    fetch_meta["time_max"] = tmax
                return df

            try:
                out_path = ingestor.ingest_symbol(
                    source=source,
                    market=target.market,
                    symbol=target.storage_symbol,
                    cycle=cycle,
                    specific=target.specific,
                    start=start_use,
                    end=end_use,
                    file_type="csv",
                    time_column="time",
                    merge=profile.merge,
                    preprocess=_capture_fetch,
                )
            except Exception as exc:
                failed_tasks += 1
                msg = f"{target.fetch_symbol}-{cycle}: {exc}"
                failed_msgs.append(msg)
                print(f"[失败] {target.display_name} {cycle} | 原因={exc}")
                continue

            valid, reason = _validate_output_file(Path(out_path))
            if not valid:
                failed_tasks += 1
                msg = f"{target.fetch_symbol}-{cycle}: {reason}"
                failed_msgs.append(msg)
                print(f"[失败] {target.display_name} {cycle} | 原因=输出校验失败: {reason}")
                continue

            final = _read_existing_summary(Path(out_path))
            added = max(0, final["rows"] - existing["rows"])

            no_update_reason = ""
            if profile.merge:
                if added <= 0:
                    if fetch_meta["rows"] == 0:
                        no_update_reason = "抓取为空"
                    elif (
                        existing["time_max"] is not None
                        and fetch_meta["time_max"] is not None
                        and fetch_meta["time_max"] <= existing["time_max"]
                    ):
                        no_update_reason = "抓取最大时间未超过原有"
                    else:
                        no_update_reason = "合并去重后无新增"
            elif fetch_meta["rows"] == 0:
                no_update_reason = "抓取为空"

            if no_update_reason:
                not_updated_tasks += 1
                status_text = f"未更新（{no_update_reason}）"
            else:
                updated_tasks += 1
                status_text = "已更新"

            print(
                f"[结果] {target.display_name} {cycle} | 抓取行={fetch_meta['rows']} "
                f"抓取时间={_format_ts(fetch_meta['time_min'])}~{_format_ts(fetch_meta['time_max'])} "
                f"| 写入后行={final['rows']} 新增={added} | {status_text}"
            )
            ok_files.append(out_path)

    elapsed = perf_counter() - start_wall
    print(
        f"[入库汇总] 用时={elapsed:.1f}s 总任务={total_tasks} "
        f"有效更新={updated_tasks} 未更新={not_updated_tasks} 失败={failed_tasks}"
    )

    result = {
        "mode": profile.name,
        "ok_files": ok_files,
        "failed": failed_msgs,
        "total": total_tasks,
        "updated": updated_tasks,
        "not_updated": not_updated_tasks,
        "failed_count": failed_tasks,
        "elapsed_sec": elapsed,
        "storage_backend": storage_config.backend,
        "root": storage_config.root,
        "fd_repo": storage_config.fd_repo,
    }
    if failed_msgs:
        raise RuntimeError(f"入库任务存在失败项: {failed_msgs}")
    return result


def run_profile(name: str, **overrides: Any) -> dict[str, Any]:
    """按模式名运行入库任务。"""
    profile = build_profile(name, **overrides)
    return run_ingest(profile)
