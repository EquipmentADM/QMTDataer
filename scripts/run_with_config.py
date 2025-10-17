# -*- coding: utf-8 -*-
"""
运行入口（支持 --config 配置启动；也支持“无参直跑”Demo 启动）

类/方法说明：
    - build_demo_app_config()：构造一个最小可运行的默认配置（读取 REDIS_URL 或用本地默认）；
    - run_from_config(cfg)：按 AppConfig 启动 QMTConnector、Publisher、(可选)HealthReporter、(可选)ControlPlane、RealtimeSubscriptionService；
    - main(argv=None)：解析命令行参数；若 --config 缺失，则自动走 Demo 配置并打印提示。

功能：
    - 单进程承载所有订阅；
    - 可选健康上报、控制面动态订阅；
    - 日志支持控制台/文件与按大小轮转。

上下游：
    - 上游：配置文件或环境变量（Demo 模式下使用）；
    - 下游：
        * core.qmt_connector.QMTConnector：与 QMT/MiniQMT 建连、确保 xtdata 可用；
        * core.realtime_service.RealtimeSubscriptionService：历史预热+实时订阅与推送；
        * core.pubsub_publisher.PubSubPublisher：将 K 线推送到 Redis；
        * core.control_plane.ControlPlane（可选）：接收订阅命令（subscribe/unsubscribe/status）；
        * core.health.HealthReporter（可选）：上报健康信息到 Redis。
"""
from __future__ import annotations
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

# 配置与日志
from core.config_loader import (
    load_config,
    AppConfig, QMTSection, RedisSection, SubscriptionSection, MockSection,
    LoggingSection, RotateSection, ControlSection, HealthSection
)
from core.logging_utils import setup_logging

# 运行所需组件
from core.qmt_connector import QMTConfig, QMTConnector
from core.pubsub_publisher import PubSubPublisher
from core.realtime_service import RealtimeConfig, RealtimeSubscriptionService
from core.control_plane import ControlPlane
from core.health import HealthReporter
from core.metrics import Metrics


# ----------------- Demo 配置构造 -----------------
def build_demo_app_config() -> AppConfig:
    """方法说明：构造一个最小可运行的默认配置（Demo）
    功能：便于直接运行本脚本时（未给 --config）也能快速体验；
    上游：main()；
    下游：run_from_config()。
    """
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    return AppConfig(
        qmt=QMTSection(mode="none", token=""),
        redis=RedisSection(
            url=redis_url,
            topic="xt:topic:bar",
        ),
        subscription=SubscriptionSection(
            codes=["510050.SH", "159915.SZ"],
            periods=["1m"],
            mode="close_only",
            close_delay_ms=100,
            preload_days=1,  # Demo 下默认预热 1 天更快
        ),
        mock=MockSection(enabled=False),
        logging=LoggingSection(
            level="INFO",
            json=False,
            file="logs/realtime_bridge.log",
            rotate=RotateSection(enabled=True, max_bytes=10 * 1024 * 1024, backup_count=5)
        ),
        control=ControlSection(
            enabled=True,
            channel="xt:ctrl:sub",
            ack_prefix="xt:ctrl:ack",
            registry_prefix="xt:bridge",
            accept_strategies=[]
        ),
        health=HealthSection(
            enabled=False,  # Demo 默认关闭；需要可在环境变量里打开
            key_prefix="xt:bridge:health",
            interval_sec=5,
            ttl_sec=20,
            instance_tag="demo"
        )
    )


# ----------------- 核心运行逻辑 -----------------
def run_from_config(cfg: AppConfig) -> None:
    """方法说明：按给定 AppConfig 启动实时行情桥"""
    # 1) 日志初始化
    rotate = cfg.logging.rotate or RotateSection()
    setup_logging(
        level=cfg.logging.level,
        to_file=cfg.logging.file,
        json_mode=cfg.logging.json,
        rotate_enabled=rotate.enabled,
        max_bytes=rotate.max_bytes,
        backup_count=rotate.backup_count
    )
    logging.info("[BOOT] load config ok: codes=%d periods=%d mode=%s topic=%s mock=%s",
                 len(cfg.subscription.codes), len(cfg.subscription.periods),
                 cfg.subscription.mode, cfg.redis.topic,
                 "on" if cfg.mock.enabled else "off")

    mock_cfg = RealtimeConfig.MockConfig(
        enabled=cfg.mock.enabled,
        base_price=cfg.mock.base_price,
        volatility=cfg.mock.volatility,
        step_seconds=cfg.mock.step_seconds,
        seed=cfg.mock.seed,
        volume_mean=cfg.mock.volume_mean,
        volume_std=cfg.mock.volume_std,
        source=cfg.mock.source,
    )

    # 2) 连接 QMT/MiniQMT（Mock 模式下可跳过）
    if not mock_cfg.enabled:
        qc = QMTConnector(QMTConfig(mode=cfg.qmt.mode, token=cfg.qmt.token))
        qc.listen_and_connect()
        logging.info("[BOOT] QMT connector ok (mode=%s)", cfg.qmt.mode)
    else:
        logging.info("[BOOT] mock mode active, skip QMT connector")

    # 3) Publisher
    publisher = PubSubPublisher(
        host=cfg.redis.host, port=cfg.redis.port,
        password=cfg.redis.password, db=cfg.redis.db,
        topic=cfg.redis.topic
    )

    # 4) Realtime Service
    rt_cfg = RealtimeConfig(
        mode=cfg.subscription.mode,
        periods=cfg.subscription.periods,
        codes=cfg.subscription.codes,
        close_delay_ms=cfg.subscription.close_delay_ms,
        preload_days=cfg.subscription.preload_days,
        mock=mock_cfg,
    )
    svc = RealtimeSubscriptionService(rt_cfg, publisher)

    # 5) 可选：健康上报
    metrics = Metrics()
    health_thr = None
    try:
        h = cfg.health
        if isinstance(h, HealthSection) and h.enabled:
            extra = {
                "codes": cfg.subscription.codes,
                "periods": cfg.subscription.periods,
                "mode": cfg.subscription.mode,
                "topic": cfg.redis.topic,
                "instance_tag": h.instance_tag,
            }
            health_thr = HealthReporter(
                host=cfg.redis.host, port=cfg.redis.port, password=cfg.redis.password,
                key_prefix=h.key_prefix, metrics=metrics,
                interval_sec=int(h.interval_sec), ttl_sec=int(h.ttl_sec),
                extra_info=extra
            )
            health_thr.start()
            logging.info("[BOOT] health reporter started")
    except Exception as e:
        logging.warning("[BOOT] health reporter disabled: %s", e)

    # 6) 可选：控制面（动态订阅）
    ctrl_thr = None
    if cfg.control.enabled:
        try:
            ctrl_thr = ControlPlane(
                host=cfg.redis.host, port=cfg.redis.port, password=cfg.redis.password, db=cfg.redis.db,
                channel=cfg.control.channel, ack_prefix=cfg.control.ack_prefix,
                registry_prefix=cfg.control.registry_prefix, svc=svc,
                accept_strategies=cfg.control.accept_strategies, logger=logging.getLogger("ControlPlane")
            )
            ctrl_thr.start()
            logging.info("[BOOT] control plane started channel=%s ack=%s", cfg.control.channel, cfg.control.ack_prefix)
        except Exception as e:
            logging.warning("[BOOT] control plane disabled: %s", e)

    # 7) 阻塞运行（内部会做历史预热 + 实时订阅 + 推送）
    logging.info("[BOOT] starting realtime service ...")
    try:
        svc.run_forever()
    finally:
        # 优雅退出
        try:
            svc.stop()
        except Exception:
            pass
        if ctrl_thr:
            try:
                ctrl_thr.stop()
                ctrl_thr.join(timeout=1.0)
            except Exception:
                pass
        if health_thr:
            try:
                health_thr.stop()
                health_thr.join(timeout=1.0)
            except Exception:
                pass


def main(argv: Optional[list] = None) -> None:
    """Parse CLI arguments, load configuration, and bring up the realtime bridge."""
    parser = argparse.ArgumentParser(description='QMT realtime bridge (config driven)')
    parser.add_argument('--config', help='Path to YAML config file', required=False)
    args = parser.parse_args(argv)

    if not args.config:
        default_cfg = Path('config/run_config.yml')
        if default_cfg.exists():
            cfg = load_config(str(default_cfg))
            return run_from_config(cfg)
        print('\n[INFO] No --config provided. Falling back to demo configuration (set REDIS_URL to override).')
        print('       Hint: in production please pass --config explicitly.\n')
        cfg = build_demo_app_config()
        return run_from_config(cfg)

    cfg = load_config(args.config)
    return run_from_config(cfg)

if __name__ == "__main__":
    # 允许“直接运行”：
    # - 若未给 --config，则走 Demo 配置（读取 REDIS_URL 或默认 redis://127.0.0.1:6379/0）；
    # - 若给了 --config，则按你的 YAML 启动。
    main()
