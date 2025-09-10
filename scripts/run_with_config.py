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


def _setup_logging(level: str = "INFO", to_file: str | None = None) -> None:
    """方法说明：初始化日志
    功能：根据配置设置日志级别与输出；
    上游：main；
    下游：logging root。
    """
    fmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format=fmt,
                        filename=to_file if to_file else None)


def main() -> None:
    """方法说明：脚本入口
    功能：读取配置 → 连接 QMT → 预热订阅并运行。
    上游：命令行；
    下游：QMT/MiniQMT、Redis。
    """
    parser = argparse.ArgumentParser(description="QMT 行情桥｜配置化运行入口（M3）")
    parser.add_argument("--config", required=True, help="YAML 配置文件路径，如 configs/realtime.yml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    _setup_logging(cfg.logging.level, cfg.logging.file)

    logging.info("[BOOT] load config ok: codes=%d periods=%d mode=%s topic=%s",
                 len(cfg.subscription.codes), len(cfg.subscription.periods),
                 cfg.subscription.mode, cfg.redis.topic)

    # QMT 连接（mode=none 默认不 listen）
    qmt_conn = QMTConnector(QMTConfig(mode=cfg.qmt.mode, token=cfg.qmt.token))
    qmt_conn.listen_and_connect()
    logging.info("[BOOT] QMT connector ok (mode=%s)", cfg.qmt.mode)

    # 发布器 + 指标
    metrics = Metrics()
    publisher = PubSubPublisher(host=cfg.redis.host, port=cfg.redis.port,
                                password=cfg.redis.password, topic=cfg.redis.topic,
                                metrics=metrics)

    # 实时服务
    rt_cfg = RealtimeConfig(mode=cfg.subscription.mode,
                            periods=cfg.subscription.periods,
                            codes=cfg.subscription.codes,
                            close_delay_ms=cfg.subscription.close_delay_ms,
                            preload_days=cfg.subscription.preload_days)
    svc = RealtimeSubscriptionService(rt_cfg, publisher)

    # 运行（阻塞）
    logging.info("[BOOT] starting realtime service ...")
    svc.run_forever()


if __name__ == "__main__":
    main()
