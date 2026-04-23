"""
QMTDataer 核心业务包。

Responsibilities:
    - 作为项目内部核心模块的明确包入口；
    - 避免 bridge 服务启动时将 `core` 误解析为外部同名模块；
    - 为 service 进程、脚本与测试提供稳定的包导入路径。
"""
