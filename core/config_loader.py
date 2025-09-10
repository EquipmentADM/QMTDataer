# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 11:18
# @Author : EquipmentADV
# @File : config_loader.py
# @Software : PyCharm
"""配置加载器（M3）

类/方法说明：
    - AppConfig：顶层配置数据类，包含 qmt/redis/subscription/logging 四部分；
    - load_config(path)：从 YAML 加载并校验，返回 AppConfig；

功能：
    - 将 YAML 配置映射到强类型数据类，做基本校验与默认值填充；

上下游：
    - 上游：外部 YAML 配置文件（configs/realtime.yml）；
    - 下游：运行脚本（scripts/run_with_config.py）与核心服务（QMTConnector/RealtimeSubscriptionService/PubSubPublisher）。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import os

try:
    import yaml
except Exception as e:  # pragma: no cover
    raise RuntimeError("缺少依赖：PyYAML。请先安装：pip install pyyaml") from e


# ----------------- 数据类定义 -----------------
@dataclass
class QMTSection:
    mode: str = "none"     # none|legacy
    token: str = ""

@dataclass
class RedisSection:
    host: str = "127.0.0.1"
    port: int = 6379
    password: Optional[str] = None
    topic: str = "xt:topic:bar"

@dataclass
class SubscriptionSection:
    codes: List[str] = field(default_factory=list)
    periods: List[str] = field(default_factory=lambda: ["1m"])  # 1m/1h/1d
    mode: str = "close_only"  # close_only | forming_and_close
    close_delay_ms: int = 100
    preload_days: int = 3

@dataclass
class LoggingSection:
    level: str = "INFO"
    json: bool = False
    file: Optional[str] = None

@dataclass
class AppConfig:
    qmt: QMTSection = field(default_factory=QMTSection)
    redis: RedisSection = field(default_factory=RedisSection)
    subscription: SubscriptionSection = field(default_factory=SubscriptionSection)
    logging: LoggingSection = field(default_factory=LoggingSection)


# ----------------- 加载与校验 -----------------
_ALLOWED_PERIODS = {"1m", "1h", "1d"}
_ALLOWED_MODES = {"close_only", "forming_and_close"}
_ALLOWED_QMT_MODES = {"none", "legacy"}

# ----------------- 工具 -----------------

def _parse_redis_url(url: str) -> Dict[str, Any]:
    """方法说明：解析 redis:// URL
    功能：返回 host/port/password/db 字段；忽略 scheme 与 query。
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
    try:
        db = int((u.path or "/0").lstrip("/")) if (u.path or "/0").lstrip("/") else 0
    except Exception:
        db = 0
    return {"host": host, "port": port, "password": password, "db": db}


def _env_override(val: Any, env_key: Optional[str]) -> Any:
    """方法说明：支持通过环境变量覆盖配置项
    功能：若 env_key 存在则返回其值，否则返回原值。
    上游：load_config 内部；
    下游：配置构造。
    """
    if not env_key:
        return val
    return os.getenv(env_key, val)


def load_config(path: str) -> AppConfig:
    """方法说明：从 YAML 加载配置
    功能：读取 YAML，做基本校验与默认填充，返回 AppConfig。
    上游：scripts/run_with_config.py
    下游：核心服务初始化。
    """
    with open(path, "r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    qmt = raw.get("qmt", {})
    redis = raw.get("redis", {})
    sub = raw.get("subscription", {})
    log = raw.get("logging", {})
    ctl = raw.get("control", {})
    health = raw.get("health", None)

    # QMT
    qmt_sec = QMTSection(
        mode=str(qmt.get("mode", "none")).lower(),
        token=str(qmt.get("token", "")),
    )
    if qmt_sec.mode not in _ALLOWED_QMT_MODES:
        raise ValueError(f"qmt.mode 不合法：{qmt_sec.mode}，允许值：{_ALLOWED_QMT_MODES}")

        # Redis（支持 url 覆盖）
    url = redis.get("url")
    if url:
        parsed = _parse_redis_url(str(url))
        host = parsed["host"]
        port = parsed["port"]
        password = parsed["password"]
        db = parsed["db"]
    else:
        host = str(redis.get("host", "127.0.0.1"))
        port = int(redis.get("port", 6379))
        password = redis.get("password", None)
        db = int(redis.get("db", 0))

    redis_sec = RedisSection(
        url = url,
        host = host,
        port = port,
        password = password,
        db = db,
        topic = str(redis.get("topic", "xt:topic:bar")),
    )

    # Subscription
    codes = [str(c).strip() for c in (sub.get("codes") or []) if str(c).strip()]
    periods = [str(p).strip() for p in (sub.get("periods") or []) if str(p).strip()]
    mode = str(sub.get("mode", "close_only")).lower()
    close_delay_ms = int(sub.get("close_delay_ms", 100))
    preload_days = int(sub.get("preload_days", 3))

    if not codes:
        raise ValueError("subscription.codes 不能为空")
    if not periods:
        raise ValueError("subscription.periods 不能为空")
    for p in periods:
        if p not in _ALLOWED_PERIODS:
            raise ValueError(f"subscription.periods 包含不支持的周期：{p}")
    if mode not in _ALLOWED_MODES:
        raise ValueError(f"subscription.mode 不合法：{mode}，允许：{_ALLOWED_MODES}")

    sub_sec = SubscriptionSection(
        codes=codes,
        periods=periods,
        mode=mode,
        close_delay_ms=close_delay_ms,
        preload_days=preload_days,
    )

    log_sec = LoggingSection(
        level=str(log.get("level", "INFO")).upper(),
        json=bool(log.get("json", False)),
        file=log.get("file", None),
        rotate=log.get("rotate", None),
    )

    ctl_sec = ControlSection(
        enabled=bool(ctl.get("enabled", False)),
        channel=str(ctl.get("channel", "xt:ctrl:sub")),
        ack_prefix=str(ctl.get("ack_prefix", "xt:ctrl:ack")),
        registry_prefix=str(ctl.get("registry_prefix", "xt:bridge")),
        accept_strategies=[str(x) for x in (ctl.get("accept_strategies") or [])],
    )

    return AppConfig(qmt=qmt_sec, redis=redis_sec, subscription=sub_sec, logging=log_sec)