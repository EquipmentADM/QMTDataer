# -*- codeing = utf-8 -*-
# @Time : 2025/9/11 11:50
# @Author : EquipmentADV
# @File : _helpers.py
# @Software : PyCharm
# -*- coding: utf-8 -*-
"""测试辅助：解析 REDIS_URL、生成随机前缀/通道名。
类说明：
    - 提供从环境变量解析 Redis URL 的工具函数；
    - 生成随机后缀用于隔离测试键名/通道名；
功能：
    - 便于在真 Redis 上做隔离的集成测试；
上下游：
    - 上游：各个测试文件；
    - 下游：redis 连接与键名/通道名。
"""
import os
from urllib.parse import urlparse
import uuid


def redis_params_from_env(default_url: str = "redis://127.0.0.1:6379/0"):
    """方法说明：从环境变量 REDIS_URL 解析 Redis 连接参数
    功能：返回 host/port/password/db/url 字段字典
    上游：测试用例；
    下游：redis.from_url / redis.Redis 初始化。
    """
    url = os.getenv("REDIS_URL", default_url)
    u = urlparse(url)
    return {
        "host": u.hostname or "127.0.0.1",
        "port": u.port or 6379,
        "password": u.password,
        "db": int((u.path or "/0").lstrip("/") or 0),
        "url": url,
    }


def random_suffix(n: int = 6) -> str:
    """方法说明：生成随机十六进制后缀，用于键名/通道隔离"""
    return uuid.uuid4().hex[:n]