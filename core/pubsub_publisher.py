# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 15:12
# @Author : EquipmentADV
# @File : pubsub_publisher.py
# @Software : PyCharm
"""Redis PubSub 发布器（M3）

类说明：
    - PubSubPublisher：将字典消息序列化为 JSON，并发布到 Redis PubSub 主题；

功能：
    - 支持重试；中文字符不转义；集成最小指标；

上下游：
    - 上游：RealtimeSubscriptionService；
    - 下游：Redis PubSub。
"""
from __future__ import annotations
import json
from typing import Optional, Dict, Any
import time
import logging

try:
    import redis
except Exception as e:  # pragma: no cover
    redis = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

from .metrics import Metrics


class PubSubPublisher:
    """类说明：Redis PubSub 发布器
    功能：提供 publish(dict) → Redis PubSub；失败自动重试；统计指标。
    上游：RealtimeSubscriptionService。
    下游：Redis。
    """
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, password: Optional[str] = None,
                 topic: str = "xt:topic:bar", metrics: Optional[Metrics] = None,
                 logger: Optional[logging.Logger] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(f"未能导入 redis：{_IMPORT_ERR}")
        self._cli = redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self.topic = topic
        self.metrics = metrics or Metrics()
        self.logger = logger or logging.getLogger(__name__)

    def publish(self, payload: Dict[str, Any], max_retries: int = 3, backoff_ms: int = 100) -> None:
        """方法说明：发布一条消息
        功能：JSON 序列化并发布至 PubSub；失败重试。
        上游：RealtimeSubscriptionService。
        下游：Redis PubSub。
        """
        data = json.dumps(payload, ensure_ascii=False)
        for i in range(max_retries):
            try:
                self._cli.publish(self.topic, data)
                self.metrics.inc_published()
                return
            except Exception as e:  # pragma: no cover
                self.metrics.inc_publish_fail()
                if i == max_retries - 1:
                    self.logger.error("[PubSubPublisher] 发布失败（耗尽重试）：%s", e)
                    raise RuntimeError(f"publish failed: {e}")
                time.sleep(backoff_ms / 1000.0)
