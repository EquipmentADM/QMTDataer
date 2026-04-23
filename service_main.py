"""
QMTDataer bridge 服务启动入口。

Responsibilities:
    - 读取最小启动参数并绑定固定本地端口；
    - 启动前抢锁并写入 runtime.json；
    - 暴露 /health、/status_detail、/probe_history、/shutdown 接口；
    - 在收到关闭信号时执行最小优雅退出与清理。
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path
from typing import Optional

# ---- 启动导入路径修正 ----
# 说明：
#     - bridge 模式由主控通过 subprocess 从外部环境拉起；
#     - 某些启动场景下，Python 的 sys.path 解析可能先命中其他同名包（如 core）；
#     - 因此在导入 bridge_service 前，先将当前项目根目录显式插入 sys.path 首位，
#       避免 `core.ingest_runner` 等项目内模块被错误解析。
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bridge_service.service_api import BridgeServiceState, create_http_server, shutdown_server_async
from bridge_service.service_runtime import ServiceRuntime


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 18931
MODULE_ID = "qmtdataer"


def build_arg_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="QMTDataer bridge 服务启动入口")
    parser.add_argument("--host", default=DEFAULT_HOST, help="服务绑定地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="服务绑定端口，默认 18931")
    parser.add_argument(
        "--base-dir",
        default=str(Path(__file__).resolve().parent),
        help="运行时目录根路径，默认使用当前项目根目录",
    )
    return parser


def setup_logging() -> None:
    """初始化最小日志配置。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _install_signal_handlers(server, state: BridgeServiceState) -> None:
    """注册最小优雅关闭信号处理。"""

    def _handle_signal(signum: int, _frame) -> None:
        logging.getLogger(__name__).info("收到退出信号：%s，准备关闭服务。", signum)
        state.request_shutdown(reason=f"signal:{signum}")

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            signal.signal(sig, _handle_signal)

    state.shutdown_callback = lambda: shutdown_server_async(server)


def run_service(host: str, port: int, base_dir: str) -> int:
    """
    启动 bridge 服务。

    返回：
        int：进程退出码；0 表示正常退出。
    """
    logger = logging.getLogger(__name__)
    runtime = ServiceRuntime(
        module_id=MODULE_ID,
        service=MODULE_ID,
        host=host,
        port=port,
        base_dir=Path(base_dir),
    )
    try:
        runtime_info = runtime.prepare()
    except Exception as exc:
        logger.error("服务启动失败：无法完成运行时准备：%s", exc)
        return 2

    state = BridgeServiceState(runtime=runtime)
    try:
        server = create_http_server(host, port, state)
    except Exception as exc:
        logger.error("服务启动失败：无法绑定 %s:%s，错误：%s", host, port, exc)
        runtime.cleanup()
        return 3

    _install_signal_handlers(server, state)
    logger.info(
        "QMTDataer bridge 服务已启动：host=%s port=%s instance_id=%s pid=%s",
        host,
        port,
        runtime_info.instance_id,
        runtime_info.pid,
    )

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，开始关闭服务。")
    finally:
        try:
            server.server_close()
        finally:
            runtime.cleanup()
        logger.info("QMTDataer bridge 服务已退出。")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    """服务命令行入口。"""
    setup_logging()
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    return run_service(host=args.host, port=args.port, base_dir=args.base_dir)


if __name__ == "__main__":
    sys.exit(main())
