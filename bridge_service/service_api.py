"""
QMTDataer bridge 服务最小 HTTP 接口。

Responsibilities:
    - 提供 /health、/status_detail、/probe_history、/shutdown 接口；
    - 将服务内部状态转换为统一控制端可读取的结构化 JSON；
    - 承担第一阶段的“服务能否被主控访问与识别”的最小协议层。

Data Contract:
    - 所有接口均返回 application/json；
    - 时间字段统一使用本地无时区 ISO8601 字符串；
    - /probe_history 只返回测试结果，不做落盘、不做入库。

External Systems:
    - xtquant.xtdata 或顶层 xtdata（如环境已安装）
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd

from bridge_service.service_runtime import ServiceRuntime


logger = logging.getLogger(__name__)
CN_TZ = timezone(timedelta(hours=8))


def _now_iso() -> str:
    """返回当前本地时间的无时区 ISO8601 字符串。"""
    return datetime.now(CN_TZ).replace(tzinfo=None).isoformat(timespec="seconds")


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
                return _format_ts(str(int(ts)).zfill(8))
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


def _check_xtdata_dependency() -> dict[str, Any]:
    """
    执行一次轻量 xtdata 可用性检测。

    说明：
        - 普通 /health 不调用该函数，避免高频依赖探测；
        - /health?deep=true 与 /status_detail 调用该函数。
    """
    detail = {"checked_at": _now_iso()}
    try:
        xtdata = _import_xtdata()
    except Exception as exc:
        return {
            "ok": False,
            "status": "xtdata_import_failed",
            "message": "无法导入 xtdata。",
            "detail": {**detail, "error": str(exc)},
        }

    try:
        if hasattr(xtdata, "get_client"):
            xtdata.get_client()
    except Exception as exc:
        return {
            "ok": False,
            "status": "not_connected",
            "message": "xtdata 当前未连接 MiniQMT。",
            "detail": {**detail, "error": str(exc)},
        }

    return {
        "ok": True,
        "status": "available",
        "message": "xtdata 可用。",
        "detail": detail,
    }


def probe_history(
    symbol: str,
    period: str,
    start: str,
    end: str,
) -> dict[str, Any]:
    """
    执行一次最小历史取数测试。

    说明：
        - 只做 download + get_market_data_ex + 结果摘要；
        - 不做文件保存；
        - 只用于 bridge 链路测试。
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
        "rows": int(len(preview)),
        "time_min": preview["time"].iloc[0],
        "time_max": preview["time"].iloc[-1],
        "head": preview.head(2).to_dict(orient="records"),
        "tail": preview.tail(2).to_dict(orient="records"),
    }


@dataclass
class BridgeServiceState:
    """bridge 服务内存态。"""

    runtime: ServiceRuntime
    service_name: str = "qmtdataer"
    session_mode: str = "shared"
    last_error: str = ""
    last_probe_result: Optional[dict[str, Any]] = None
    last_probe_at: Optional[str] = None
    shutdown_requested: bool = False
    shutdown_reason: Optional[str] = None
    shutdown_callback: Optional[Callable[[], None]] = None
    probe_func: Callable[[str, str, str, str], dict[str, Any]] = field(default=probe_history)

    def health_payload(self, deep: bool = False) -> dict[str, Any]:
        """生成 /health 返回内容。"""
        snapshot = self.runtime.get_runtime_snapshot()
        payload = {
            "ok": True,
            "status": "healthy",
            "service": self.service_name,
            "instance_id": snapshot.get("instance_id"),
            "pid": snapshot.get("pid"),
            "started_at": snapshot.get("started_at"),
            "port": snapshot.get("port"),
        }
        if deep:
            dep = _check_xtdata_dependency()
            payload["dependency"] = dep
            if not dep.get("ok", False):
                payload["ok"] = False
                payload["status"] = "degraded"
        return payload

    def status_detail_payload(self) -> dict[str, Any]:
        """生成 /status_detail 返回内容。"""
        snapshot = self.runtime.get_runtime_snapshot()
        dep = _check_xtdata_dependency()
        return {
            "ok": dep.get("ok", False),
            "status": "healthy" if dep.get("ok", False) else "degraded",
            "service": self.service_name,
            "instance_id": snapshot.get("instance_id"),
            "pid": snapshot.get("pid"),
            "started_at": snapshot.get("started_at"),
            "host": snapshot.get("host"),
            "port": snapshot.get("port"),
            "runtime_file": snapshot.get("runtime_file"),
            "lock_file": snapshot.get("lock_file"),
            "owner": snapshot.get("owner"),
            "session_mode": snapshot.get("session_mode", self.session_mode),
            "dependency": dep,
            "last_probe_at": self.last_probe_at,
            "last_probe_result": self.last_probe_result,
            "last_error": self.last_error,
            "checked_at": _now_iso(),
        }

    def handle_probe_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """处理 /probe_history 请求。"""
        symbol = str(payload.get("symbol") or "").strip()
        period = str(payload.get("period") or "").strip()
        start = str(payload.get("start") or "").strip()
        end = str(payload.get("end") or "").strip()
        if not symbol or not period or not start:
            raise ValueError("probe_history 缺少必要参数：symbol/period/start")

        result = self.probe_func(symbol=symbol, period=period, start=start, end=end)
        self.last_probe_at = _now_iso()
        self.last_probe_result = result
        return result

    def request_shutdown(self, reason: str = "http") -> None:
        """标记服务进入关闭流程，并触发外部 shutdown 回调。"""
        self.shutdown_requested = True
        self.shutdown_reason = reason
        if self.shutdown_callback is not None:
            self.shutdown_callback()


class BridgeHTTPServer(ThreadingHTTPServer):
    """带最小服务状态的 HTTPServer。"""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], handler_cls: type[BaseHTTPRequestHandler], state: BridgeServiceState):
        super().__init__(server_address, handler_cls)
        self.state = state


def make_handler(state: BridgeServiceState) -> type[BaseHTTPRequestHandler]:
    """根据当前服务状态生成请求处理器类。"""

    class QMTBridgeHandler(BaseHTTPRequestHandler):
        server_version = "QMTDataerBridge/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            """将标准请求日志接入 logging。"""
            logger.info("%s - %s", self.address_string(), fmt % args)

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict[str, Any]:
            """读取并解析 JSON 请求体。"""
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                query = parse_qs(parsed.query)
                deep = str(query.get("deep", ["false"])[0]).lower() in {"1", "true", "yes"}
                self._send_json(200, state.health_payload(deep=deep))
                return

            if parsed.path == "/status_detail":
                self._send_json(200, state.status_detail_payload())
                return

            self._send_json(
                404,
                {"ok": False, "status": "not_found", "message": f"未找到接口：{parsed.path}"},
            )

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            try:
                payload = self._read_json_body()
            except Exception as exc:
                self._send_json(
                    400,
                    {"ok": False, "status": "bad_request", "message": f"请求体不是合法 JSON：{exc}"},
                )
                return

            if parsed.path == "/probe_history":
                try:
                    result = state.handle_probe_request(payload)
                except Exception as exc:
                    state.last_error = str(exc)
                    self._send_json(
                        500,
                        {"ok": False, "status": "probe_failed", "message": str(exc)},
                    )
                    return
                self._send_json(200, result)
                return

            if parsed.path == "/shutdown":
                state.request_shutdown(reason="http")
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "status": "shutdown_requested",
                        "service": state.service_name,
                        "instance_id": state.runtime.get_runtime_snapshot().get("instance_id"),
                    },
                )
                return

            self._send_json(
                404,
                {"ok": False, "status": "not_found", "message": f"未找到接口：{parsed.path}"},
            )

    return QMTBridgeHandler


def create_http_server(host: str, port: int, state: BridgeServiceState) -> BridgeHTTPServer:
    """创建 bridge HTTP 服务。"""
    handler_cls = make_handler(state)
    return BridgeHTTPServer((host, port), handler_cls, state)


def shutdown_server_async(server: ThreadingHTTPServer) -> None:
    """
    异步关闭 HTTP 服务。

    说明：
        - server.shutdown() 不能在当前请求线程内直接阻塞调用；
        - 因此统一切到后台线程执行。
    """

    def _worker() -> None:
        try:
            server.shutdown()
        except Exception:
            logger.exception("关闭 HTTP 服务时出现异常")

    threading.Thread(target=_worker, daemon=True).start()
