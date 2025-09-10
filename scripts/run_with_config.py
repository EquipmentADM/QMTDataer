# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 11:19
# @Author : EquipmentADV
# @File : run_with_config.py
# @Software : PyCharm
"""配置化运行入口（M3）

类说明：
    - 从 YAML 文件加载配置，初始化 QMTConnector/RealtimeSubscriptionService/PubSubPublisher，并进入订阅循环；

功能：
    - 满足“单进程承载全部订阅，基于配置运行”的交付要求；

上下游：
    - 上游：configs/realtime.yml；
    - 下游：QMT/MiniQMT、Redis PubSub。
"""
from __future__ import annotations
import argparse
import logging

from core.config_loader import load_config
from core.qmt_connector import QMTConnector, QMTConfig
from core.realtime_service import RealtimeSubscriptionService, RealtimeConfig
from core.pubsub_publisher import PubSubPublisher
from core.metrics import Metrics
from core.logging_utils import setup_logging
from core.health import HealthReporter
from core.control_plane import ControlPlane


def main() -> None:
    parser = argparse.ArgumentParser(description="QMT 行情桥｜配置化运行入口（M3.5 控制面）")
    parser.add_argument("--config", required=True, help="YAML 配置文件路径，如 configs/realtime.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    setup_logging(level=cfg.logging.level,
                  to_file=cfg.logging.file,
                  json_mode=bool(cfg.logging.json),
                  rotate_enabled=bool(cfg.logging.rotate and cfg.logging.rotate.get("enabled", False)),
                  max_bytes=(cfg.logging.rotate.get("max_bytes", 10 * 1024 * 1024) if cfg.logging.rotate else 10 * 1024 * 1024),
                  backup_count=(cfg.logging.rotate.get("backup_count", 5) if cfg.logging.rotate else 5))

    logging.info("[BOOT] config ok: codes=%d periods=%d mode=%s topic=%s",
                 len(cfg.subscription.codes), len(cfg.subscription.periods),
                 cfg.subscription.mode, cfg.redis.topic)

    # QMT 连接（mode=none 默认不 listen）
    qmt_conn = QMTConnector(QMTConfig(mode=cfg.qmt.mode, token=cfg.qmt.token))
    qmt_conn.listen_and_connect()
    logging.info("[BOOT] QMT connector ok (mode=%s)", cfg.qmt.mode)

    # 发布器 + 指标（支持 db）
    metrics = Metrics()
    publisher = PubSubPublisher(host=cfg.redis.host, port=cfg.redis.port,
                                password=cfg.redis.password, db=cfg.redis.db,
                                topic=cfg.redis.topic, metrics=metrics)

    # 健康上报（可选）——你当前可在 yml 中设 health.enabled=false 关闭
    health_thr = None
    try:
        health_cfg = cfg.health or {}
        if health_cfg.get("enabled"):
            extra = {
                "codes": cfg.subscription.codes,
                "periods": cfg.subscription.periods,
                "mode": cfg.subscription.mode,
                "topic": cfg.redis.topic,
            }
            health_thr = HealthReporter(host=cfg.redis.host, port=cfg.redis.port,
                                        password=cfg.redis.password,
                                        key_prefix=health_cfg.get("key_prefix", "xt:bridge:health"),
                                        metrics=metrics,
                                        interval_sec=int(health_cfg.get("interval_sec", 5)),
                                        ttl_sec=int(health_cfg.get("ttl_sec", 20)),
                                        extra_info=extra)
            health_thr.start()
            logging.info("[BOOT] health reporter started")
    except Exception as e:
        logging.warning("[BOOT] health reporter disabled: %s", e)

    # 实时服务
    rt_cfg = RealtimeConfig(mode=cfg.subscription.mode,
                            periods=cfg.subscription.periods,
                            codes=cfg.subscription.codes,
                            close_delay_ms=cfg.subscription.close_delay_ms,
                            preload_days=cfg.subscription.preload_days)
    svc = RealtimeSubscriptionService(rt_cfg, publisher)

    # 控制面（可选）
    ctl_thr = None
    if cfg.control.enabled:
        ctl_thr = ControlPlane(host=cfg.redis.host, port=cfg.redis.port,
                               password=cfg.redis.password, db=cfg.redis.db,
                               channel=cfg.control.channel, ack_prefix=cfg.control.ack_prefix,
                               registry_prefix=cfg.control.registry_prefix,
                               svc=svc, accept_strategies=cfg.control.accept_strategies,
                               logger=logging.getLogger("control"))
        ctl_thr.start()
        logging.info("[BOOT] control plane started (channel=%s)", cfg.control.channel)

    # 运行（阻塞）
    logging.info("[BOOT] starting realtime service ...")
    svc.run_forever()


if __name__ == "__main__":
    main()
