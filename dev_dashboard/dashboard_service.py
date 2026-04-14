# -*- coding: utf-8 -*-
"""
QMTDataer 控制台模块服务层。

职责：
    - 提供模块可用性检测；
    - 提供最小历史取数探针；
    - 向控制台页面返回结构化状态与测试结果。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd


CN_TZ = timezone(timedelta(hours=8))


def _import_xtdata() -> Any:
    """动态导入 xtdata，兼容常见安装方式。"""
    try:
        from xtquant import xtdata  # type: ignore

        return xtdata
    except Exception:
        import xtdata  # type: ignore

        return xtdata


def _format_ts(raw: Any) -> str:
    """将 xtdata 返回时间统一转为本地无时区 ISO 字符串。"""
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
                s = str(int(ts)).zfill(8)
                return _format_ts(s)
            return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")

        text = str(raw).strip()
        if len(text) == 14 and text.isdigit():
            dt = datetime.strptime(text, "%Y%m%d%H%M%S").replace(tzinfo=CN_TZ)
            return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
        if len(text) == 8 and text.isdigit():
            dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=CN_TZ)
            return dt.astimezone(CN_TZ).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
        if "T" not in text and " " in text:
            text = text.replace(" ", "T")
        if "Z" in text:
            text = text.replace("Z", "+00:00")
        if "+" not in text:
            text = f"{text}+08:00"
        return (
            datetime.fromisoformat(text)
            .astimezone(CN_TZ)
            .replace(tzinfo=None)
            .strftime("%Y-%m-%dT%H:%M:%S")
        )
    except Exception:
        return str(raw)


def _normalize_preview(data_dict: dict[str, Any], symbol: str) -> pd.DataFrame:
    """兼容 xtdata 两类常见返回结构，抽取标准预览宽表。"""
    if not isinstance(data_dict, dict) or not data_dict:
        return pd.DataFrame()

    if "time" in data_dict and isinstance(data_dict.get("time"), pd.DataFrame):
        time_df: pd.DataFrame = data_dict["time"]
        if symbol not in time_df.index:
            return pd.DataFrame()
        rows: list[dict[str, Any]] = []
        for col in time_df.columns:
            row = {"time": _format_ts(time_df.loc[symbol, col])}
            for field in ("open", "high", "low", "close", "volume", "amount"):
                field_df = data_dict.get(field)
                if isinstance(field_df, pd.DataFrame) and symbol in field_df.index and col in field_df.columns:
                    row[field] = field_df.loc[symbol, col]
            rows.append(row)
        return pd.DataFrame(rows)

    df_code = data_dict.get(symbol)
    if isinstance(df_code, pd.DataFrame):
        work = df_code.copy()
        time_col = next((c for c in ("time", "Time", "datetime", "bar_time") if c in work.columns), None)
        if time_col is None:
            return pd.DataFrame()
        work["time"] = work[time_col].map(_format_ts)
        keep_cols = [c for c in ("time", "open", "high", "low", "close", "volume", "amount") if c in work.columns]
        return work[keep_cols].reset_index(drop=True)

    return pd.DataFrame()


def check_available() -> dict[str, Any]:
    """
    供统一控制台调用的模块可用性检测。

    第一阶段按“同解释器直载”约定处理：
        - dash 未安装：视为模块不可挂载；
        - xtdata 无法导入或未连接：视为模块不可用，但主控仍应显示占位页。
    """
    checked_at = datetime.now(CN_TZ).replace(tzinfo=None).isoformat(timespec="seconds")

    try:
        import dash  # noqa: F401
    except Exception as exc:
        return {
            "available": False,
            "status": "dash_missing",
            "severity": "error",
            "message": "当前环境未安装 Dash，无法挂载 QMTDataer 模块页面。",
            "detail": {"checked_at": checked_at, "error": str(exc)},
            "actions": ["确认统一控制台与模块运行在同一 Python 环境", "安装 Dash 依赖"],
        }

    try:
        xtdata = _import_xtdata()
    except Exception as exc:
        return {
            "available": False,
            "status": "xtdata_import_failed",
            "severity": "error",
            "message": "无法导入 xtdata。",
            "detail": {"checked_at": checked_at, "error": str(exc)},
            "actions": ["检查 xtquant 是否已安装", "确认模块与主控使用同一解释器"],
        }

    try:
        if hasattr(xtdata, "get_client"):
            xtdata.get_client()
    except Exception as exc:
        return {
            "available": False,
            "status": "not_connected",
            "severity": "error",
            "message": "xtdata 当前未连接 MiniQMT。",
            "detail": {"checked_at": checked_at, "error": str(exc)},
            "actions": ["检查 MiniQMT 是否已启动", "检查当前环境能否连接 xtdata 服务"],
        }

    return {
        "available": True,
        "status": "available",
        "severity": "info",
        "message": "QMTDataer 模块可用。",
        "detail": {"checked_at": checked_at},
        "actions": ["可进入模块页面进行历史取数测试"],
    }


def get_status_detail() -> dict[str, Any]:
    """返回更详细的模块状态信息。"""
    base = check_available()
    base["detail"] = {
        **base.get("detail", {}),
        "module": "qmtdataer",
        "python_time": datetime.now(CN_TZ).replace(tzinfo=None).isoformat(timespec="seconds"),
    }
    return base


def probe_history(symbol: str, period: str, start: str, end: str) -> dict[str, Any]:
    """
    执行一次最小历史取数测试。

    说明：
        - 只做 download + get_market_data_ex + 结果摘要；
        - 不做文件保存；
        - 不进入统一任务中心。
    """
    xtdata = _import_xtdata()
    xtdata.download_history_data(
        stock_code=symbol,
        period=period,
        start_time=start,
        end_time=end,
        incrementally=True,
    )
    data = xtdata.get_market_data_ex(
        field_list=[],
        stock_list=[symbol],
        period=period,
        start_time=start,
        end_time=end,
        count=-1,
        dividend_type="none",
        fill_data=False,
    )
    preview = _normalize_preview(data, symbol)
    if preview.empty:
        return {
            "ok": False,
            "symbol": symbol,
            "period": period,
            "start": start,
            "end": end,
            "rows": 0,
            "message": "xtdata 返回空结果或未能解析有效 K 线。",
        }

    return {
        "ok": True,
        "symbol": symbol,
        "period": period,
        "start": start,
        "end": end,
        "rows": len(preview),
        "time_min": preview["time"].iloc[0],
        "time_max": preview["time"].iloc[-1],
        "head": preview.head(2).to_dict(orient="records"),
        "tail": preview.tail(2).to_dict(orient="records"),
    }


def default_stock_probe_params() -> dict[str, str]:
    """返回股票默认测试参数。"""
    today = datetime.now(CN_TZ).strftime("%Y%m%d")
    start = (datetime.now(CN_TZ) - timedelta(days=30)).strftime("%Y%m%d")
    return {"symbol": "510050.SH", "period": "1d", "start": start, "end": today}


def default_futures_probe_params() -> dict[str, str]:
    """返回期货默认测试参数。"""
    today = datetime.now(CN_TZ).strftime("%Y%m%d")
    start = (datetime.now(CN_TZ) - timedelta(days=365)).strftime("%Y%m%d")
    return {"symbol": "rb00.SF", "period": "1d", "start": start, "end": today}
