# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:12
# @Author : EquipmentADV
# @File : pubsub_publisher.py
# @Software : PyCharm
"""
PubSubPublisher：Redis PubSub 发布器

类说明：
    - 功能：将标准化 bar 消息（宽表 JSON）发布至 Redis PubSub；提供超时、重试与指标打点（留钩子）。
    - 上游：RealtimeSubscriptionService（实时订阅聚合侧）。
    - 下游：策略管理器 / 本地数据库消费端（通过 PubSub 订阅）。
"""
from __future__ import annotations
import json
from typing import Optional, Dict, Any
import logging

try:
    import redis
except Exception as e:  # pragma: no cover
    redis = None  # type: ignore
    _REDIS_ERR = e
else:
    _REDIS_ERR = None


class PubSubPublisher:
    """类说明：Redis PubSub 发布器（JSON 序列化）
    功能：负责将消息发布至指定 topic；支持简单重试；
    上游：RealtimeSubscriptionService。
    下游：策略/DB 消费端。
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, password: Optional[str] = None,
                 topic: str = "xt:topic:bar", serializer: str = "json",
                 logger: Optional[logging.Logger] = None) -> None:
        if _REDIS_ERR is not None:
            raise RuntimeError(f"未能导入 redis-py：{_REDIS_ERR}. 请先 pip install redis")
        self.logger = logger or logging.getLogger(__name__)
        self.topic = topic
        self.serializer = serializer
        self.cli = redis.Redis(host=host, port=port, password=password, decode_responses=True)

    def publish(self, message: Dict[str, Any], max_retries: int = 3) -> None:
        """方法说明：发布单条消息到 Redis PubSub
        功能：将字典转为 JSON 并发布至 topic；失败自动重试。
        上游：RealtimeSubscriptionService。
        下游：Redis PubSub 渠道。
        """
        payload = json.dumps(message, ensure_ascii=False)
        for i in range(max_retries):
            try:
                self.cli.publish(self.topic, payload)
                # 指标钩子：可在此处记录发布耗时/成功计数
                return
            except Exception as e:  # pragma: no cover
                self.logger.warning("[PubSubPublisher] 发布失败，重试=%d, err=%s", i + 1, e)
        raise RuntimeError("PubSubPublisher: 达到最大重试次数仍失败")
