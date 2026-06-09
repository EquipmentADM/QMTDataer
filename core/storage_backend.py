# -*- coding: utf-8 -*-
"""
历史入库存储后端构造工具。

Responsibilities:
    - 统一创建 legacy / fd 两类 storage backend。
    - 读取环境变量开关，支持快速灰度与回滚。
    - 避免 ingest_runner 直接关心具体 storage 类。

Data Contract:
    - backend 支持 legacy、fd；为空时读取 QMTD_STORAGE_BACKEND，仍为空则使用 legacy。
    - fd_repo 为空时读取 FD_REPO 或 FD_CORE_SRC。
    - fd backend 可通过 FD_DATA_ROOT 覆盖数据根目录。
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


@dataclass(frozen=True)
class StorageBackendConfig:
    """
    存储后端解析结果。

    Attributes:
        backend (str): 后端名称，legacy 或 fd。
        root (str): 实际使用的数据根目录。
        fd_repo (str): FD 项目根目录或 src 目录。
    """

    backend: str
    root: str
    fd_repo: str = ""


def resolve_storage_backend_config(
    root: str,
    backend: Optional[str] = None,
    fd_repo: Optional[str] = None,
) -> StorageBackendConfig:
    """
    解析存储后端配置。

    Args:
        root (str): profile 中的数据根目录。
        backend (Optional[str]): 显式后端名称。
        fd_repo (Optional[str]): 显式 FD 项目路径。

    Returns:
        StorageBackendConfig: 标准化后的后端配置。
    """

    backend_use = (backend or os.environ.get("QMTD_STORAGE_BACKEND") or "legacy").strip().lower()
    if backend_use not in {"legacy", "fd"}:
        raise ValueError(f"未知 storage backend: {backend_use}，可选 legacy/fd")

    root_use = root
    fd_repo_use = fd_repo or os.environ.get("FD_REPO") or os.environ.get("FD_CORE_SRC") or ""
    if backend_use == "fd":
        root_use = os.environ.get("FD_DATA_ROOT") or root_use

    return StorageBackendConfig(backend=backend_use, root=root_use, fd_repo=fd_repo_use)


def build_storage_backend(config: StorageBackendConfig):
    """
    根据配置创建存储后端实例。

    Args:
        config (StorageBackendConfig): 后端配置。

    Returns:
        object: 兼容 MarketDataIngestor 所需方法的 storage 对象。
    """

    if config.backend == "legacy":
        from core.storage_simple import FinancialDataStorage

        storage = FinancialDataStorage(root_dir=config.root)
        setattr(storage, "backend_name", "legacy")
        return storage

    from core.fd_storage_adapter import FDStorageAdapter

    return FDStorageAdapter(root_dir=config.root, fd_repo=config.fd_repo or None)
