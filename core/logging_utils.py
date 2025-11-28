# -*- coding = utf-8 -*-
# @Time : 2025/9/10 14:26
# @Author : EquipmentADV
# @File : logging_utils.py
# @Software : PyCharm
"""日志初始化工具（M3）

类/方法说明：
    - setup_logging：按配置初始化日志；支持控制台/文件、可选轮转、可选 JSON 格式。

功能：
    - 为运行脚本提供一致的日志初始化方式；

上下游：
    - 上游：scripts/run_with_config.py；
    - 下游：logging root。
"""
from __future__ import annotations
import json
import logging
import logging.handlers
from typing import Optional


class _JsonFormatter(logging.Formatter):
    """类说明：简易 JSON 日志格式器"""
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO", to_file: Optional[str] = None,
                  json_mode: bool = False, rotate_enabled: bool = False,
                  max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5) -> None:
    """方法说明：初始化日志
    功能：根据配置设置日志级别与输出目的地；支持文件轮转与 JSON 格式。
    上游：运行脚本；
    下游：logging root。
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 清理旧 handler（便于单元或重复初始化场景）
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt_text = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    fmt_json = _JsonFormatter()
    formatter = fmt_json if json_mode else fmt_text

    # 控制台输出
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    root.addHandler(sh)

    # 文件输出（可选）
    if to_file:
        if rotate_enabled:
            fh = logging.handlers.RotatingFileHandler(
                to_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
        else:
            fh = logging.FileHandler(to_file, encoding="utf-8")
        fh.setFormatter(formatter)
        root.addHandler(fh)
