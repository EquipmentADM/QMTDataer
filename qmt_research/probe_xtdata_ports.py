# -*- coding: utf-8 -*-
"""
xtdata 端口探针脚本。

功能：
    - 对候选端口先做 TCP 监听探测；
    - 对监听端口调用 xtdata.reconnect(ip, port, remember_if_success=False)；
    - 再通过 get_market_data_ex 拉取一个极小历史样本，确认该端口是否真能提供 xtdata 服务。

使用场景：
    - MiniQMT / 大 QMT 同时或分别提供 xtdata 服务时，确认实际可用端口；
    - 研究不同 QMT 客户端的 xtdata 端口暴露行为。

注意：
    - 本脚本是研究工具，不参与 QMTD 正式实时行情和历史入库链路；
    - 建议在 QMTD 服务未运行时使用，避免 reconnect 干扰业务进程。
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import socket
import sys
import time
from typing import Iterable


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORTS = [58600, 58610, 58612]
DEFAULT_SYMBOL = "510050.SH"
DEFAULT_PERIOD = "1d"


@dataclass
class ProbeResult:
    """端口探测结果。"""

    port: int
    tcp_open: bool
    reconnect_ok: bool = False
    data_ok: bool = False
    rows: int = 0
    message: str = ""
    meta: str = ""
    elapsed_sec: float = 0.0


def _parse_ports(text: str | None) -> list[int]:
    """解析逗号分隔端口列表。"""
    if not text:
        return []
    ports: list[int] = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        ports.append(int(part))
    return ports


def _parse_range(text: str | None) -> list[int]:
    """解析形如 58600-58630 的端口范围。"""
    if not text:
        return []
    start_text, end_text = text.split("-", 1)
    start = int(start_text.strip())
    end = int(end_text.strip())
    if start > end:
        start, end = end, start
    return list(range(start, end + 1))


def _merge_ports(*groups: Iterable[int]) -> list[int]:
    """合并端口并保持首次出现顺序。"""
    seen: set[int] = set()
    result: list[int] = []
    for group in groups:
        for port in group:
            if port in seen:
                continue
            seen.add(port)
            result.append(port)
    return result


def _is_tcp_open(host: str, port: int, timeout: float) -> bool:
    """判断本机端口是否处于 TCP 监听状态。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


def _count_rows(data: object, symbol: str) -> int:
    """从 xtdata 常见返回结构中估算样本行数。"""
    if not isinstance(data, dict) or not data:
        return 0
    by_code = data.get(symbol)
    if hasattr(by_code, "shape"):
        try:
            return int(by_code.shape[0])
        except Exception:
            return 0
    time_data = data.get("time")
    if hasattr(time_data, "shape"):
        try:
            return int(time_data.shape[-1])
        except Exception:
            return 0
    first_value = next(iter(data.values()), None)
    if hasattr(first_value, "shape"):
        try:
            return int(first_value.shape[0])
        except Exception:
            return 0
    return 0


def _read_client_meta(client: object) -> str:
    """读取 xtdata client 的连接元信息；失败时保留错误摘要。"""
    parts: list[str] = []
    for name in ("get_peer_addr", "get_data_dir", "get_app_dir", "get_server_tag"):
        func = getattr(client, name, None)
        if not callable(func):
            continue
        try:
            value = func()
            if isinstance(value, bytes):
                value = f"bytes[{len(value)}]"
            parts.append(f"{name}={value!r}")
        except Exception as exc:
            parts.append(f"{name}=ERR({type(exc).__name__}: {exc})")
    return "; ".join(parts)


def probe_one_port(
    *,
    xtdata: object,
    host: str,
    port: int,
    symbol: str,
    period: str,
    tcp_timeout: float,
) -> ProbeResult:
    """探测单个端口是否为可用 xtdata 服务端口。"""
    started = time.perf_counter()
    if not _is_tcp_open(host, port, tcp_timeout):
        return ProbeResult(
            port=port,
            tcp_open=False,
            message="端口未监听",
            elapsed_sec=time.perf_counter() - started,
        )

    try:
        xtdata.reconnect(ip=host, port=port, remember_if_success=False)
        client = xtdata.get_client()
        meta = _read_client_meta(client)
        reconnect_msg = f"reconnect 成功 client={client!r}"
    except Exception as exc:
        return ProbeResult(
            port=port,
            tcp_open=True,
            reconnect_ok=False,
            message=f"reconnect 失败：{type(exc).__name__}: {exc}",
            elapsed_sec=time.perf_counter() - started,
        )

    try:
        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=[symbol],
            period=period,
            start_time="",
            end_time="",
            count=5,
            dividend_type="none",
            fill_data=False,
        )
        rows = _count_rows(data, symbol)
        if rows <= 0:
            return ProbeResult(
                port=port,
                tcp_open=True,
                reconnect_ok=True,
                data_ok=False,
                rows=0,
                message=f"{reconnect_msg}；行情请求返回空样本",
                meta=meta,
                elapsed_sec=time.perf_counter() - started,
            )
        return ProbeResult(
            port=port,
            tcp_open=True,
            reconnect_ok=True,
            data_ok=True,
            rows=rows,
            message=f"{reconnect_msg}；行情样本 rows={rows}",
            meta=meta,
            elapsed_sec=time.perf_counter() - started,
        )
    except Exception as exc:
        return ProbeResult(
            port=port,
            tcp_open=True,
            reconnect_ok=True,
            data_ok=False,
            message=f"{reconnect_msg}；行情请求失败：{type(exc).__name__}: {exc}",
            meta=meta,
            elapsed_sec=time.perf_counter() - started,
        )


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="探测本机 xtdata 可用端口")
    parser.add_argument("--host", default=DEFAULT_HOST, help="目标地址，默认 127.0.0.1")
    parser.add_argument("--ports", default="", help="逗号分隔端口，例如 58610,58612")
    parser.add_argument("--range", dest="port_range", default="", help="端口范围，例如 58600-58630")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="验证用标的，默认 510050.SH")
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="验证用周期，默认 1d")
    parser.add_argument("--tcp-timeout", type=float, default=0.2, help="TCP 探测超时秒数")
    return parser


def main(argv: list[str] | None = None) -> int:
    """脚本入口。"""
    args = build_parser().parse_args(argv)
    ports = _merge_ports(_parse_ports(args.ports), _parse_range(args.port_range), DEFAULT_PORTS)

    try:
        from xtquant import xtdata  # type: ignore
    except Exception as exc:
        print(f"[失败] 无法导入 xtquant.xtdata：{type(exc).__name__}: {exc}")
        return 2
    if hasattr(xtdata, "enable_hello"):
        xtdata.enable_hello = False

    print("[探测开始]")
    print(f"xtdata={getattr(xtdata, '__file__', xtdata)}")
    print(f"host={args.host} ports={ports} symbol={args.symbol} period={args.period}")
    print("说明：先测 TCP 监听，再测 reconnect，最后用 get_market_data_ex 验证数据。")

    results: list[ProbeResult] = []
    for port in ports:
        result = probe_one_port(
            xtdata=xtdata,
            host=args.host,
            port=port,
            symbol=args.symbol,
            period=args.period,
            tcp_timeout=args.tcp_timeout,
        )
        results.append(result)
        status = "可用" if result.data_ok else "不可用"
        print(
            f"[{status}] port={result.port} tcp={result.tcp_open} reconnect={result.reconnect_ok} "
            f"data={result.data_ok} rows={result.rows} 耗时={result.elapsed_sec:.2f}s | {result.message}"
        )
        if result.meta:
            print(f"       元信息：{result.meta}")

    usable = [item for item in results if item.data_ok]
    if usable:
        print("[结论] 发现可用 xtdata 端口：" + ", ".join(str(item.port) for item in usable))
        return 0
    print("[结论] 未发现可用 xtdata 端口。若 QMT 已启动，请确认 xtdata 服务功能是否开启或端口是否不同。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
