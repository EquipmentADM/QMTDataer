# -*- codeing = utf-8 -*-
# @Time : 2025/9/10 14:27
# @Author : EquipmentADV
# @File : validate_config.py
# @Software : PyCharm
"""配置校验脚本（M3）

类说明：
    - 从指定 YAML 读取配置，做合法性校验并打印关键信息；

功能：
    - 上线前快速验证配置是否正确，避免运行期报错；

上下游：
    - 上游：运维/CI；
    - 下游：core.config_loader。
"""
from __future__ import annotations
import argparse
from core.config_loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="校验 realtime.yml 配置并打印关键信息")
    parser.add_argument("--config", required=True, help="YAML 配置路径")
    args = parser.parse_args()
    cfg = load_config(args.config)
    print("配置加载成功：")
    print(f"  QMT.mode = {cfg.qmt.mode}")
    print(f"  Redis = {cfg.redis.host}:{cfg.redis.port} topic={cfg.redis.topic}")
    print(f"  订阅：codes={len(cfg.subscription.codes)} 个, periods={cfg.subscription.periods}, mode={cfg.subscription.mode}")
    print(f"  预热天数 = {cfg.subscription.preload_days}, close_delay_ms = {cfg.subscription.close_delay_ms}")


if __name__ == "__main__":
    main()
