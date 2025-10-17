# -*- coding: utf-8 -*-
# @Time    : 2025/9/10 11:18
# @Author  : EquipmentADV
# @File    : config_loader.py
# @Software: PyCharm
"""
配置加载器（M3 / M3.5 兼容）

类/方法说明：
    - QMTSection：QMT 连接相关配置（本工程仅用于占位与模式选择）；
    - RedisSection：Redis 连接与发布主题；
    - RotateSection：日志轮转配置；
    - LoggingSection：日志配置；
    - SubscriptionSection：订阅规格（标的、周期、模式、预热等）；
    - ControlSection：控制面（动态订阅）配置；
    - HealthSection：健康上报配置；
    - AppConfig：顶层配置聚合；
    - load_config(path)：从 YAML 加载配置为 AppConfig，并做基础校验与默认值填充。

功能：
    - 将 YAML 配置映射到强类型数据类，做基本校验与默认值填充；
    - 支持 redis:// URL 解析（host/port/password/db）。

上下游：
    - 上游：外部 YAML 配置文件（configs/realtime.yml）；
    - 下游：运行脚本（scripts/run_with_config.py）与核心服务（QMTConnector/RealtimeSubscriptionService/PubSubPublisher/ControlPlane/HealthReporter）。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse
import os

try:
    import yaml
except Exception as e:  # pragma: no cover
    raise RuntimeError("缺少依赖：PyYAML。请先安装：pip install pyyaml") from e


# ----------------- 常量与允许值 -----------------
_ALLOWED_PERIODS = {"1m", "1h", "1d"}  # 若后续支持更多，扩展此集合即可
_ALLOWED_MODES = {"close_only", "forming_and_close"}
_ALLOWED_QMT_MODES = {"none", "legacy"}


# ----------------- 数据类定义 -----------------
@dataclass
class QMTSection:
    """类说明：QMT 配置段
    功能：指示 QMT 接线模式；token 预留（如需鉴权）
    上游：YAML qmt；
    下游：QMTConnector。
    """
    mode: str = "none"     # none|legacy
    token: str = ""


@dataclass
class RedisSection:
    """类说明：Redis 配置段
    功能：提供连接参数与发布主题；支持 url 解析为 host/port/password/db
    上游：YAML redis；
    下游：PubSubPublisher / ControlPlane / Registry / HealthReporter。
    """
    url: Optional[str] = None
    host: str = "127.0.0.1"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    topic: str = "xt:topic:bar"


@dataclass
class RotateSection:
    """类说明：日志轮转配置
    功能：控制是否按大小轮转、单文件大小与保留份数
    上游：YAML logging.rotate；
    下游：logging_utils.setup_logging。
    """
    enabled: bool = False
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5


@dataclass
class LoggingSection:
    """类说明：日志配置段"""
    level: str = "INFO"
    json: bool = False
    file: Optional[str] = None
    rotate: Optional[RotateSection] = None


@dataclass
class SubscriptionSection:
    """类说明：订阅配置段
    功能：定义初始订阅集合与行为参数；
    下游：RealtimeSubscriptionService。
    """
    codes: List[str] = field(default_factory=list)
    periods: List[str] = field(default_factory=lambda: ["1m"])  # 1m/1h/1d
    mode: str = "close_only"          # close_only | forming_and_close
    close_delay_ms: int = 100         # 推送收盘条时的延迟判定（ms）
    preload_days: int = 3             # 启动预加载历史天数


@dataclass
class MockSection:
    """类说明：Mock 行情配置段"""
    enabled: bool = False
    base_price: float = 10.0
    volatility: float = 0.002
    step_seconds: float = 1.0
    seed: Optional[int] = None
    volume_mean: float = 1_000_000
    volume_std: float = 200_000
    source: str = "mock"


@dataclass
class ControlSection:
    """类说明：控制面配置段
    功能：动态订阅命令通道、ACK 前缀与注册表前缀等；
    下游：ControlPlane / Registry。
    """
    enabled: bool = False
    channel: str = "xt:ctrl:sub"
    ack_prefix: str = "xt:ctrl:ack"
    registry_prefix: str = "xt:bridge"
    accept_strategies: List[str] = field(default_factory=list)


@dataclass
class HealthSection:
    """类说明：健康上报配置段
    功能：启用后按 interval_sec 周期向 Redis 写入心跳 JSON（带 TTL）；
    下游：HealthReporter。
    """
    enabled: bool = False
    key_prefix: str = "xt:bridge:health"
    interval_sec: int = 5
    ttl_sec: int = 20
    instance_tag: Optional[str] = None


@dataclass
class AppConfig:
    """类说明：顶层聚合配置"""
    qmt: QMTSection = field(default_factory=QMTSection)
    redis: RedisSection = field(default_factory=RedisSection)
    subscription: SubscriptionSection = field(default_factory=SubscriptionSection)
    mock: MockSection = field(default_factory=MockSection)
    logging: LoggingSection = field(default_factory=LoggingSection)
    control: ControlSection = field(default_factory=ControlSection)
    health: HealthSection = field(default_factory=HealthSection)


# ----------------- 工具函数 -----------------
def _parse_redis_url(url: str) -> Dict[str, Any]:
    """方法说明：解析 redis:// 或 rediss:// URL
    功能：返回 host/port/password/db 字段；忽略 query。
    上游：load_config；
    下游：RedisSection 构造。
    """
    u = urlparse(url)
    if u.scheme not in ("redis", "rediss"):
        raise ValueError(f"redis.url 非法 scheme：{u.scheme}")
    host = u.hostname or "127.0.0.1"
    port = u.port or 6379
    password = u.password
    # path 形如 "/0"
    path = (u.path or "/0").lstrip("/")
    try:
        db = int(path) if path else 0
    except Exception:
        db = 0
    return {"host": host, "port": port, "password": password, "db": db}


def _as_list(obj: Any) -> List[str]:
    """方法说明：将任意输入规整为字符串列表"""
    if obj is None:
        return []
    if isinstance(obj, (list, tuple, set)):
        return [str(x).strip() for x in obj if str(x).strip()]
    return [str(obj).strip()] if str(obj).strip() else []


# ----------------- 主加载函数 -----------------
def load_config(path: str) -> AppConfig:
    """方法说明：从 YAML 加载配置
    功能：读取 YAML，做基本校验与默认填充，返回 AppConfig。
    上游：scripts/run_with_config.py；
    下游：核心服务初始化。
    """
    with open(path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    qmt_raw = raw.get("qmt", {}) or {}
    redis_raw = raw.get("redis", {}) or {}
    sub_raw = raw.get("subscription", {}) or {}
    mock_raw = raw.get("mock", {}) or {}
    log_raw = raw.get("logging", {}) or {}
    ctl_raw = raw.get("control", {}) or {}
    health_raw = raw.get("health", {}) or {}

    # --- QMT ---
    qmt_sec = QMTSection(
        mode=str(qmt_raw.get("mode", "none")).lower(),
        token=str(qmt_raw.get("token", "")),
    )
    if qmt_sec.mode not in _ALLOWED_QMT_MODES:
        raise ValueError(f"qmt.mode 不合法：{qmt_sec.mode}，允许值：{_ALLOWED_QMT_MODES}")

    # --- Redis（支持 url 覆盖）---
    url = redis_raw.get("url")
    if url:
        parsed = _parse_redis_url(str(url))
        host = parsed["host"]
        port = parsed["port"]
        password = parsed["password"]
        db = parsed["db"]
    else:
        host = str(redis_raw.get("host", "127.0.0.1"))
        port = int(redis_raw.get("port", 6379))
        password = redis_raw.get("password", None)
        db = int(redis_raw.get("db", 0))
    topic = str(redis_raw.get("topic", "xt:topic:bar"))

    redis_sec = RedisSection(
        url=url if url else None,
        host=host,
        port=port,
        password=password,
        db=db,
        topic=topic,
    )

    # --- Subscription ---
    codes = _as_list(sub_raw.get("codes"))
    periods = _as_list(sub_raw.get("periods")) or ["1m"]
    mode = str(sub_raw.get("mode", "close_only")).lower()
    close_delay_ms = int(sub_raw.get("close_delay_ms", 100))
    preload_days = int(sub_raw.get("preload_days", 3))

    if not codes:
        raise ValueError("subscription.codes 不能为空")
    if not periods:
        raise ValueError("subscription.periods 不能为空")
    for p in periods:
        if p not in _ALLOWED_PERIODS:
            raise ValueError(f"subscription.periods 包含不支持的周期：{p}，允许：{_ALLOWED_PERIODS}")
    if mode not in _ALLOWED_MODES:
        raise ValueError(f"subscription.mode 不合法：{mode}，允许：{_ALLOWED_MODES}")

    sub_sec = SubscriptionSection(
        codes=codes,
        periods=periods,
        mode=mode,
        close_delay_ms=close_delay_ms,
        preload_days=preload_days,
    )

    # --- Mock ---
    mock_seed_raw = mock_raw.get("seed", None)
    mock_seed = None if mock_seed_raw in (None, "") else int(mock_seed_raw)
    mock_sec = MockSection(
        enabled=bool(mock_raw.get("enabled", False)),
        base_price=float(mock_raw.get("base_price", 10.0)),
        volatility=float(mock_raw.get("volatility", 0.002)),
        step_seconds=float(mock_raw.get("step_seconds", 1.0)),
        seed=mock_seed,
        volume_mean=float(mock_raw.get("volume_mean", 1_000_000)),
        volume_std=float(mock_raw.get("volume_std", 200_000)),
        source=str(mock_raw.get("source", "mock")),
    )

    # --- Logging ---
    rotate_sec = None
    if isinstance(log_raw.get("rotate"), dict):
        r = log_raw["rotate"]
        rotate_sec = RotateSection(
            enabled=bool(r.get("enabled", False)),
            max_bytes=int(r.get("max_bytes", 10 * 1024 * 1024)),
            backup_count=int(r.get("backup_count", 5)),
        )
    log_sec = LoggingSection(
        level=str(log_raw.get("level", "INFO")).upper(),
        json=bool(log_raw.get("json", False)),
        file=log_raw.get("file", None),
        rotate=rotate_sec,
    )

    # --- Control ---
    ctl_sec = ControlSection(
        enabled=bool(ctl_raw.get("enabled", False)),
        channel=str(ctl_raw.get("channel", "xt:ctrl:sub")),
        ack_prefix=str(ctl_raw.get("ack_prefix", "xt:ctrl:ack")),
        registry_prefix=str(ctl_raw.get("registry_prefix", "xt:bridge")),
        accept_strategies=[str(x) for x in (ctl_raw.get("accept_strategies") or [])],
    )

    # --- Health ---
    health_sec = HealthSection(
        enabled=bool(health_raw.get("enabled", False)),
        key_prefix=str(health_raw.get("key_prefix", "xt:bridge:health")),
        interval_sec=int(health_raw.get("interval_sec", 5)),
        ttl_sec=int(health_raw.get("ttl_sec", 20)),
        instance_tag=health_raw.get("instance_tag", None),
    )

    return AppConfig(
        qmt=qmt_sec,
        redis=redis_sec,
        subscription=sub_sec,
        mock=mock_sec,
        logging=log_sec,
        control=ctl_sec,
        health=health_sec,
    )
