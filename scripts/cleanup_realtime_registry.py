# -*- coding: utf-8 -*-
"""
实时行情 Registry 清理脚本。

Responsibilities:
    - 查看当前 Redis Registry 中保存的协议 `sub_id`。
    - 支持按 `strategy_id` 清理，或清空某个 registry_prefix 下的全部历史记录。
    - 默认 dry-run，仅展示将删除的内容；显式传 `--execute` 才真正删除。

Data Contract:
    - Redis Registry 键结构由 `core.registry.Registry` 统一定义。
    - 本脚本只清理协议注册表，不直接控制底层 xtdata 实时订阅。

Internal Dependencies:
    - core.registry.Registry

External Systems:
    - Redis
"""
from __future__ import annotations

import argparse
from typing import Optional

from core.registry import Registry


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数解析器。

    Returns:
        argparse.ArgumentParser: 已配置参数的解析器。
    """
    parser = argparse.ArgumentParser(description="清理 QMTD 实时行情 Registry 历史记录")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--password", default=None, help="Redis password")
    parser.add_argument("--db", type=int, default=0, help="Redis db")
    parser.add_argument("--prefix", default="xt:bridge", help="Registry 前缀")
    parser.add_argument("--strategy-id", default=None, help="仅清理某个 strategy_id 的注册记录")
    parser.add_argument("--all", action="store_true", help="清空整个 prefix 下的全部注册记录")
    parser.add_argument("--execute", action="store_true", help="真正执行删除；默认仅预览")
    return parser


def run_cleanup(
    host: str = "127.0.0.1",
    port: int = 6379,
    password: Optional[str] = None,
    db: int = 0,
    prefix: str = "xt:bridge",
    strategy_id: Optional[str] = None,
    clear_all: bool = False,
    execute: bool = False,
) -> int:
    """执行 Registry 清理或预览。

    Args:
        host (str): Redis host。
        port (int): Redis port。
        password (Optional[str]): Redis password。
        db (int): Redis db。
        prefix (str): Registry 前缀。
        strategy_id (Optional[str]): 指定策略 ID 时，仅处理该策略的记录。
        clear_all (bool): 是否清空整个 prefix 下的全部记录。
        execute (bool): 是否真正执行删除。默认 False，仅预览。

    Returns:
        int: 进程返回码，`0` 表示成功，`2` 表示参数非法。
    """
    if strategy_id and clear_all:
        print("[REGISTRY] 参数冲突：--strategy-id 与 --all 只能二选一。")
        return 2
    if not strategy_id and not clear_all:
        print("[REGISTRY] 未指定清理范围。请使用 --strategy-id <id> 或 --all。")
        return 2

    registry = Registry(host, port, password, db, prefix=prefix)
    scope_desc = f"strategy_id={strategy_id}" if strategy_id else f"prefix={prefix} 全量"
    if strategy_id:
        targets = registry.list_by_strategy(strategy_id)
    else:
        targets = registry.list_all()

    print(f"[REGISTRY] 范围：{scope_desc}")
    print(f"[REGISTRY] 命中 sub_id 数量：{len(targets)}")
    for sub_id in targets[:20]:
        print(f"  - {sub_id}")
    if len(targets) > 20:
        print(f"  ... 其余 {len(targets) - 20} 条未展开")

    if not execute:
        print("[REGISTRY] 当前为 dry-run，未实际删除。传入 --execute 后执行。")
        return 0

    if strategy_id:
        deleted = registry.delete_by_strategy(strategy_id)
    else:
        deleted = registry.clear_all()
    print(f"[REGISTRY] 已删除 {len(deleted)} 条注册记录。")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    """脚本入口。

    Args:
        argv (Optional[list[str]]): 测试时可显式传入参数。

    Returns:
        int: 进程返回码。
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_cleanup(
        host=args.host,
        port=args.port,
        password=args.password,
        db=args.db,
        prefix=args.prefix,
        strategy_id=args.strategy_id,
        clear_all=args.all,
        execute=args.execute,
    )


if __name__ == "__main__":
    raise SystemExit(main())
