# scripts 目录说明

本目录的脚本按用途分为三类：实时桥运行、xtdata 入库、辅助排查。

## 0. 实时行情唯一源规则

同一套 Redis 行情 topic 在同一运行环境内只能有一个权威 QMTD 实时行情器。

- 真行情和 Mock 行情由启动脚本决定，不由下游 `subscribe` 请求决定。
- 下游只能声明 `codes`、`periods`、`mode` 与 `topic`，不能在订阅时选择真/假行情。
- `run_realtime_control.py` 是真实行情空白控制入口。
- `run_realtime_mock_control.py` 是虚拟行情空白控制入口。
- 两个入口不能同时作为同一个 `xt:topic:bar` 的权威发布源运行。
- 如需同时运行真/假两套服务，必须显式拆分 Redis topic 和控制通道。

## 1. 实时桥主链脚本

- `run_with_config.py`
  - 用途：配置驱动的实时桥主入口。
  - 推荐：生产和日常联调统一使用这个入口。

- `run_config_fake.py`
  - 用途：强制 Mock 行情运行入口。
  - 场景：无 QMT/MiniQMT 环境时验证下游链路。

- `run_realtime_control.py`
  - 用途：真实实时行情控制面空白启动入口。
  - 场景：不预设初始订阅，启动后等待 Redis 控制通道的 `subscribe/unsubscribe/status`。
  - 默认：优先读取 `config/realtime_control.yml`。
  - 注意：不要与 Mock 控制入口共用同一行情 topic 同时运行。

- `run_realtime_mock_control.py`
  - 用途：虚拟实时行情控制面空白启动入口。
  - 场景：不预设初始订阅，启动后等待 Redis 控制通道申请 Mock 行情。
  - 默认：优先读取 `config/realtime_mock_control.yml`。
  - 注意：该实例收到的所有下游订阅都会生成 Mock 行情。

- `run_btlive_mock_1min_pool.py`
  - 用途：BTLive 标准 1min Mock 行情常开入口。
  - 场景：固定 5 个常用标的，给 BTLive 做无真实 QMT 依赖的链路验证。
  - 注意：这是主动常开 Mock 行情入口，不要与真实行情入口共用同一 topic。

- `run_realtime_bridge.py`
  - 用途：早期参数化实时桥入口（兼容保留）。
  - 说明：建议优先使用 `run_with_config.py`。

## 2. xtdata 入库脚本

### 参数化主入口

- `xtdata_ingest.py`
  - 用途：统一管理三种入库模式。
  - 模式：`full-download`、`full-backfill`、`recent-backfill`。

示例：

```bash
python -m scripts.xtdata_ingest full-download
python -m scripts.xtdata_ingest full-backfill --symbols 518880.SH,513880.SH
python -m scripts.xtdata_ingest recent-backfill --lookback 3
```

### 一键运行脚本（固定模式）

- `xtdata_ingest_full.py`
  - 模式：`full-download`
  - 语义：全量下载并重建写入。

- `xtdata_ingest_backfill.py`
  - 模式：`full-backfill`
  - 语义：全区间补齐，merge 去重写入。

- `xtdata_ingest_recent.py`
  - 模式：`recent-backfill`
  - 语义：基于本地最新时间回溯 N 根后增量补齐。

### 兼容入口

- `xtdata_ingest_integration_test.py`
  - 用途：历史脚本名兼容入口。
  - 默认：`full-backfill`。

- `xtdata_ingest_simple.py`
  - 用途：手工参数化落盘工具（保留）。
  - 场景：临时指定 market/symbol/cycle 小范围入库。

## 3. 联调与排查脚本

- `backfill_history.py`
  - 用途：历史区间拉取并输出 JSON（不落库）。

- `validate_config.py`
  - 用途：配置文件结构与关键字段校验。

- `ops_check.py`
  - 用途：检查 xtquant/redis 基础可用性。

- `send_control_cmd.py`
  - 用途：向控制面发送 `subscribe/unsubscribe/status` 命令。

- `show_realtime_status.py`
  - 用途：发送 `status` 并等待 ACK，直接打印当前活跃订阅、引用计数和最近发布时间。

- `cleanup_realtime_registry.py`
  - 用途：清理 Redis Registry 中残留的实时订阅 `sub_id` 记录。
  - 场景：诊断时发现顶层 `subs` 有历史遗留记录，可先 dry-run 预览，再按策略或全量清理。

- `simple_bar_listener.py`
  - 用途：监听 Redis topic，查看实时推送。

- `realtime_probe_suite.py`
  - 用途：历史/直连/桥接综合探针。

- `probe_xtdata_is_closed.py`
  - 用途：1m 实时回调窗口探测，统计 `isClosed/isClose/closed` 出现率与取值。
  - 场景：确认 close 标志是否来自行情源，以及在 2 分钟等短窗口内是否可观测。

- `qmt_api_probe.py`
  - 用途：xtdata API 探针与字段预览。

## 4. 归档脚本

- `archive/demo_xtdata_manual.py`
  - 说明：手工改常量运行脚本，保留用于历史参考。

- `archive/dump_xtdata_csv.py`
  - 说明：平行 CSV 导出实现，和主入库链路分离。
  - 建议：新需求优先走 `xtdata_ingest.py` 主线。
