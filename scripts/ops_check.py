# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 14:27
# @Author : EquipmentADV
# @File : ops_check.py
# @Software : PyCharm
"""运维自检脚本（M3）

类说明：
    - 快速检查环境依赖是否可用：xtquant.xtdata 是否可导入、Redis 是否可连接；

功能：
    - 提前发现环境问题（路径、依赖、网络）；

上下游：
    - 上游：运维；
    - 下游：xtquant/redis。
"""
from __future__ import annotations
import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="运维自检：xtquant/redis 可用性")
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-password", default=None)
    args = parser.parse_args()

    ok = True

    # 检查 xtquant.xtdata 可导入
    try:
        from xtquant import xtdata  # noqa: F401
        print("xtquant.xtdata: OK")
    except Exception as e:
        print(f"xtquant.xtdata: FAIL - {e}")
        ok = False

    # 检查 redis 连接
    try:
        import redis
        cli = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_password)
        cli.ping()
        print(f"redis: OK - {args.redis_host}:{args.redis_port}")
    except Exception as e:
        print(f"redis: FAIL - {e}")
        ok = False

    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
