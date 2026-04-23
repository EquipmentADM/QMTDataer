"""
QMTDataer 项目 AI 交接说明。
"""

# QMTDataer 项目 AI 交接说明

本文档用于让新的 AI 在最短时间内继承当前项目上下文，减少重复摸索与误改风险。
内容重点覆盖：

- 全局开发要求与注意事项
- 当前项目结构与模块职责
- bridge 服务现状
- 历史下载 / 补数 / 入库链路现状
- 默认规则、测试方式与后续工作方向

---

## 1. 项目定位

QMTDataer 当前不是统一控制台主项目，而是一个将接入统一控制台的 **bridge 模块**。
该项目主要承担：

- QMT / xtdata 历史数据测试与获取
- 股票 / 期货历史下载、补数、入库
- 实时桥接与 Mock 行情（已有旧链路）
- 通过本地 HTTP bridge 服务向统一控制台暴露能力

当前阶段的正式接入方式已经从“Dash 插件直载”切换为：

- **QMTD 独立服务进程**
- **主控通过 HTTP bridge 调用**

---

## 2. 全局开发要求

## 2.1 交流与注释

- 所有回答、说明、注释、docstring 以 **中文为主**
- 代码标识符保持英文技术命名
- 注释重点写：
  - 作用
  - 数据约束
  - 设计目的
  - 边界条件
- 不写低价值注释（如“给变量赋值”）

## 2.2 编码要求

- 所有源码、文档、配置文件默认使用 **UTF-8**
- 必须避免中文乱码
- 时间字符串优先统一为 **本地无时区 ISO8601**
  - `YYYY-MM-DDTHH:MM:SS`

## 2.3 注释规范

本项目新增或修改代码时，应遵守：

- [Python_注释规范_v1.1.md](/Users/EquipmentADM/PycharmProjects/QMTDataer/docs/Python_注释规范_v1.1.md)

重点要求：

- 文件头有结构化 docstring
- 类、函数、方法有中文 docstring
- 模块分区清楚
- 注释说明“为什么这么做”，而不是重复代码

## 2.4 Git 要求

- 不回滚用户未授权处理的改动
- 不使用危险命令：
  - `git reset --hard`
  - `git checkout --`
- 提交尽量按功能块拆分
- 提交说明简洁明确，优先描述：
  - 新增什么能力
  - 修复什么问题
  - 是否影响接口 / 目录结构

---

## 3. 当前项目结构

当前推荐理解为四层：

```text
QMTDataer/
  service_main.py               # bridge 服务稳定入口（主控拉起用）
  bridge_service/               # 正式 bridge 服务实现
  core/                         # 核心业务逻辑
  dev_dashboard/                # 本地调试页（非正式接入层）
  scripts/                      # 独立脚本入口
  tests/                        # 单元测试
  docs/                         # 文档
```

### 3.1 根目录约束

根目录当前只应保留稳定入口和高层结构，不应继续堆放实现细节文件。
正式 bridge 启动入口是：

- [service_main.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/service_main.py)

### 3.2 bridge 服务实现

bridge 正式实现位于：

- [bridge_service](/Users/EquipmentADM/PycharmProjects/QMTDataer/bridge_service)

当前核心文件：

- [service_runtime.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/bridge_service/service_runtime.py)
- [service_api.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/bridge_service/service_api.py)
- [service_tasks.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/bridge_service/service_tasks.py)

### 3.3 本地调试页

旧 Dash 骨架已下沉为本地调试页，不再是正式接入层：

- [dev_dashboard](/Users/EquipmentADM/PycharmProjects/QMTDataer/dev_dashboard)

其定位仅是：

- 本地验证状态
- 本地验证 probe
- 本地调试页面结构

正式页面壳应由统一控制台维护，不应继续在 QMTD 端做正式页面壳。

### 3.4 core 核心业务层

核心业务逻辑位于：

- [core](/Users/EquipmentADM/PycharmProjects/QMTDataer/core)

重点文件：

- [xtdata_source.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/xtdata_source.py)
- [storage_simple.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/storage_simple.py)
- [ingestor.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/ingestor.py)
- [ingest_runner.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/ingest_runner.py)
- [xtdata_futures.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/xtdata_futures.py)
- [realtime_service.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/realtime_service.py)

---

## 4. 当前 bridge 服务现状

## 4.1 启动方式

统一控制台当前按固定配置接入 QMTD：

- `python_exe = C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe`
- `workdir = C:\Users\EquipmentADM\PycharmProjects\QMTDataer`
- `entry_file = service_main.py`
- `host = 127.0.0.1`
- `port = 18931`

## 4.2 已实现接口

当前已稳定可用的接口：

- `GET /health`
- `GET /health?deep=true`
- `GET /status_detail`
- `POST /probe_history`
- `POST /submit_task`
- `GET /task/{task_id}`
- `GET /tasks/recent`
- `POST /shutdown`

## 4.3 /health 最少返回字段

当前 `/health` 至少返回：

- `ok`
- `status`
- `service`
- `instance_id`
- `pid`
- `started_at`
- `port`

## 4.4 运行时约束

bridge 服务当前已具备：

- 启动锁
- `instance_id`
- `runtime.json`
- 固定端口监听
- 优雅关闭基础能力

运行时文件默认位于：

- `runtime/qmtdataer.runtime.json`
- `runtime/qmtdataer.lock`

## 4.5 已修复的历史问题

曾出现主控拉起服务时报：

- `ModuleNotFoundError: No module named 'core.ingest_runner'`

已修复方式：

- 在 [service_main.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/service_main.py) 启动前显式插入项目根目录到 `sys.path`
- 新增 [core/__init__.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/__init__.py)

不要轻易移除这两处修复。

---

## 5. 当前任务接口设计

## 5.1 当前任务类型

当前最小任务层已经接入，统一任务类型为：

- `task_type = "ingest_run"`

## 5.2 当前支持模式

`payload.mode` 当前支持：

- `full-download`
- `full-backfill`
- `recent-backfill`

## 5.3 统一任务返回字段

当前任务结构统一为：

- `task_id`
- `module_id`
- `task_type`
- `status`
- `message`
- `created_at`
- `started_at`
- `finished_at`
- `progress`
- `result`
- `error`
- `logs`

## 5.4 当前任务状态

当前状态集：

- `pending`
- `running`
- `success`
- `failed`

`cancelled` 尚未接入。

## 5.5 当前执行策略

当前任务执行器在：

- [service_tasks.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/bridge_service/service_tasks.py)

第一阶段策略：

- 后台线程执行
- 默认串行任务
- 不做数据库级任务持久化
- 复用现有 `run_profile(...)` 业务逻辑

---

## 6. 历史下载 / 补数 / 入库链路

当前历史链路已经可用，主要复用：

- `XtdataSource`
- `MappedXtdataSource`
- `MarketDataIngestor`
- `FinancialDataStorage`
- `run_profile / run_ingest`

### 6.1 关键链路

取数与入库主链路：

- [xtdata_source.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/xtdata_source.py)
- [ingestor.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/ingestor.py)
- [storage_simple.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/storage_simple.py)
- [ingest_runner.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/ingest_runner.py)

### 6.2 三类一键模式

当前三类模式在 `core.ingest_runner` 中固定：

- `full-download`
- `full-backfill`
- `recent-backfill`

### 6.3 默认周期

默认周期：

- `1d`
- `1m`

落盘时：

- `1m -> 1min`
- `1d -> 1d`

### 6.4 默认路径规则

股票：

- `SS_stock_data/<symbol>/<cycle>/original/...`

期货：

- `Futures_data/<symbol>/<cycle>/<specific>/...`

### 6.5 时间格式规则

当前落盘和 bridge 返回中，时间统一为：

- 本地无时区 ISO8601
- 例如：`2026-04-17T10:00:00`

---

## 7. 股票与期货默认池

当前默认池定义在：

- [ingest_runner.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/ingest_runner.py)

### 7.1 默认全量池

`DEFAULT_SYMBOLS` 为：

- 股票默认池 `DEFAULT_STOCK_SYMBOLS`
- 期货默认池 `DEFAULT_FUTURES_SYMBOLS`

### 7.2 recent 精简池

`DEFAULT_SYMBOLS_RECENT` 为：

- 股票精简池 `DEFAULT_STOCK_SYMBOLS_RECENT`
- 期货精简池 `DEFAULT_FUTURES_SYMBOLS_RECENT`

### 7.3 说明

后续如需调整默认池，应优先修改 `core/ingest_runner.py`，不要在多个脚本里各自维护一份。

---

## 8. 期货映射规则

期货 xtdata 代码映射规则已在项目中固化，参考：

- [xtdata_futures.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/core/xtdata_futures.py)

当前关键规则：

- `rb00.SF -> Futures_data / rb / 主力连续`
- `rb01.SF -> Futures_data / rb / 次主力连续`
- `rb888.SF -> Futures_data / rb / 888`
- `rb2505.SF -> Futures_data / rb / rb2505`

不要私自把：

- `00`
- `01`
- `888`

混成同一语义。

---

## 9. 脚本入口现状

历史脚本入口位于：

- [scripts/xtdata_ingest.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/scripts/xtdata_ingest.py)
- [scripts/xtdata_ingest_full.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/scripts/xtdata_ingest_full.py)
- [scripts/xtdata_ingest_backfill.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/scripts/xtdata_ingest_backfill.py)
- [scripts/xtdata_ingest_recent.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/scripts/xtdata_ingest_recent.py)

这些入口仍然有价值，因为：

- QMTD 任务接口底层仍复用 `core.ingest_runner`
- 独立脚本仍可用于手工运行和排障

---

## 10. 测试现状

当前与 bridge / 任务 / 调试页相关的测试主要有：

- [test_service_runtime.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_service_runtime.py)
- [test_service_api.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_service_api.py)
- [test_service_tasks.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_service_tasks.py)
- [test_dashboard_plugin.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_dashboard_plugin.py)

其他历史链路相关测试：

- [test_xtdata_source.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_xtdata_source.py)
- [test_xtdata_futures.py](/Users/EquipmentADM/PycharmProjects/QMTDataer/tests/test_xtdata_futures.py)

### 10.1 推荐回归命令

在 `QMTDataer` conda 环境下：

```bash
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe -m unittest tests.test_service_runtime tests.test_service_api tests.test_service_tasks tests.test_dashboard_plugin -v
```

语法检查：

```bash
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe -m py_compile service_main.py bridge_service\service_api.py bridge_service\service_runtime.py bridge_service\service_tasks.py
```

---

## 11. 当前已知阶段性结论

### 11.1 bridge 主链路已打通

主控与 QMTD 已完成：

- `/health`
- `/status_detail`
- `/probe_history`
- `/shutdown`

联调通过。

### 11.2 股票 / 期货 probe 已通过

已确认：

- 股票 `510050.SH 1d` 可返回结果
- 期货 `rb00.SF 1d` 可返回结果

### 11.3 任务接口已可用

已确认：

- `/submit_task`
- `/task/{task_id}`
- `/tasks/recent`

HTTP 层与任务状态流转均已打通。

---

## 12. 页面设计方向（重要）

当前已经不再建议把 QMTD 端当作正式页面项目。
正式页面壳应由统一控制台实现。

但从业务视角，主控端后续应把 QMTD 页面设计为：

- **QMT 数据工作台**

建议的核心区块：

1. 服务状态区
2. 股票 / 期货快速测试区
3. 历史任务提交区
4. 近期任务区
5. 任务详情区
6. 默认规则说明区

页面目标不是“测试接口”，而是让用户真正完成：

- 状态判断
- 历史下载 / 补数 / 入库
- 任务回看
- 失败排障

---

## 13. 后续必要工作

当前最应该继续做的，不是继续改调试页，而是：

### 必须做

- 冻结任务 API 契约
- 冻结 `ingest_run` payload 结构
- 统一错误返回与状态码
- 补一份 bridge / task 简短接口说明

### 后续可做

- `cancel_task`
- 任务持久化
- 更完整的任务日志结构
- `/restart`
- `/service_meta` 或 `/supported_capabilities`

---

## 14. 修改本项目时的提醒

修改时请优先遵守以下原则：

- 不轻易改根目录 `service_main.py` 的稳定入口语义
- 不把正式页面逻辑重新堆回 QMTD 端
- 不重写已有历史入库逻辑，优先复用 `core.ingest_runner`
- 不破坏当前 bridge 已联通的最小接口
- 修改 bridge 导入链时，注意 `sys.path` 与 `core/__init__.py`

---

## 15. 给后续 AI 的一句话总结

当前 QMTDataer 已经完成：

- bridge 服务骨架
- 主控联通
- 历史 probe 接口
- 最小任务接口
- 股票 / 期货历史入库链路

后续工作重点应放在：

- **稳定任务契约**
- **服务接口说明**
- **支撑主控端页面落地**

而不是重新回到“测试页面联通阶段”。
