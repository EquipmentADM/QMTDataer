# QMTDataer 使用指南

本文档面向在下游系统中接入 QMTDataer 行情桥的开发者，梳理当前仓库（除 tests 目录外）的核心能力、配置方法与 API 约定，帮助完成部署、调试与联调。

## 1. 架构概览
- **QMTConnector (`core/qmt_connector.py`)**：校验/建立与 MiniQMT 的链接，`mode=none` 时仅验证依赖可用，`mode=legacy` 会调用 `xtdatacenter.listen`。
- **RealtimeSubscriptionService (`core/realtime_service.py`)**：负责历史预热、实时订阅、去重与 payload 组装，按契约 v0.5 推送到 Redis。
- **PubSubPublisher (`core/pubsub_publisher.py`)**：将宽表 JSON 发布到 Redis PubSub 主题，带最小重试与指标计数。
- **ControlPlane (`core/control_plane.py`)**：监听控制通道 `subscribe/status/unsubscribe` 命令，联动 `Registry` 与实时服务。
- **Registry (`core/registry.py`)**：以 Redis 存储订阅规格，为重启恢复提供来源。
- **HistoryAPI (`core/history_api.py`)**：封装“先补后取”的历史查询流程（`download_history_data` → `get_market_data_ex` → 宽表）。
- **Metrics (`core/metrics.py`)**：线程安全的指标集合，兼容实例级计数并提供全局指标（`bars_published_total` 等）。
- **HealthReporter (`core/health.py`)**：可选线程，周期性写入健康信息至 Redis。
- **入口脚本 (`scripts/run_with_config.py`)**：解析配置、初始化上述组件并阻塞运行。

下游消费链路：下游策略或数据仓库从 Redis PubSub 主题（默认 `xt:topic:bar`）订阅宽表 JSON 消息；若需要动态订阅，则通过控制通道发送命令。

## 2. 运行前提
1. **环境依赖**：
   - Python 3.9+（推荐使用仓库主机提供的 Anaconda 环境 `QMTDataer`）。
   - `xtquant` / MiniQMT 行情组件（来自券商提供的 QMT/MiniQMT 安装包）。
   - `redis` Python 驱动、`PyYAML`、`numpy/pandas`（随 xtquant 提供）。
2. **外部服务**：可访问的 Redis 实例；若需要实时行情，MiniQMT 行情需连通。
3. **仓库结构**：保持 `config/`、`core/`、`scripts/` 等目录不变；tests 仅作参考，无需部署到生产。

## 3. 配置文件（`config/realtime.yml`）
以下字段可按环境调整：

### 3.1 `qmt`
| 字段 | 说明 | 示例 |
|------|------|------|
| `mode` | `none`：跳过 listen，仅校验依赖；`legacy`：调用 `xtdatacenter.listen` | `none` |
| `token` | 部分券商需要的鉴权 Token，可留空 | `""` |

### 3.2 `redis`
| 字段 | 说明 | 示例 |
|------|------|------|
| `url` | 优先使用 Redis URL，形如 `redis://user:pass@host:port/db` | `redis://127.0.0.1:6379/0` |
| `host/port/password/db` | 当 `url` 缺失时生效 | `127.0.0.1`/`6379`/`null`/`0` |
| `topic` | 发布行情的 PubSub 主题 | `xt:topic:bar` |

### 3.3 `subscription`
| 字段 | 说明 | 示例 |
|------|------|------|
| `codes` | 启动时就绪的标的列表 | `[518880.SH, 513880.SH]` |
| `periods` | 周期，当前支持 `1m/1h/1d` | `[1m, 1d]` |
| `mode` | `close_only`（仅收盘推送）或 `forming_and_close` | `close_only` |
| `close_delay_ms` | 收盘延迟判定（毫秒） | `100` |
| `preload_days` | 启动预热天数（历史补齐） | `3` |

### 3.4 `logging`
控制控制台/文件输出及轮转：
- `level`: `DEBUG/INFO/WARNING/ERROR`
- `json`: `true` 时输出单行 JSON
- `file`: 文件路径（可 null）
- `rotate`: `{enabled, max_bytes, backup_count}`

### 3.5 `health`
启用健康上报需提供：
- `enabled`: `true`
- `key_prefix`: Redis Key 前缀（最终 key: `prefix:host:pid[:tag]`）
- `interval_sec`: 写入频率（秒）
- `ttl_sec`: 键过期时间
- `instance_tag`: 可选实例标识

### 3.6 `control`
控制面命令通道：
- `enabled`: `true` 表示启动控制线程
- `channel`: 订阅命令通道，例如 `xt:ctrl:sub`
- `ack_prefix`: 回执前缀，ACK 发布在 `ack_prefix:{strategy_id}`
- `registry_prefix`: 订阅信息保存在 `registry_prefix:*`
- `accept_strategies`: 白名单列表，空表示全部允许

## 4. 启动方式
### 4.1 使用配置文件
```bash
python scripts/run_with_config.py --config config/realtime.yml
```
- 启动过程中会输出 `[BOOT]` 开头的日志，包含契约版本、订阅数量、Redis 目标等。
- 进程将阻塞在 `RealtimeSubscriptionService.run_forever()`；Ctrl+C 可停止。

### 4.2 Demo 模式
若不传 `--config`，脚本会读取 `REDIS_URL`（默认为 `redis://127.0.0.1:6379/0`），其余参数采用内置 Demo 值，便于快速体验。

## 5. 控制面 API（Redis 命令）
当 `control.enabled=true` 时，可通过 Redis PubSub 控制通道动态调整订阅。

### 5.1 命令结构
- 通道：`control.channel`（默认 `xt:ctrl:sub`）
- Payload：JSON 字符串，至少包含 `action` 和 `strategy_id`
- 回执：发布到 `ack_prefix:{strategy_id}`（默认 `xt:ctrl:ack:{strategy_id}`）

#### 订阅示例
```json
{
  "action": "subscribe",
  "strategy_id": "demo",
  "codes": ["518880.SH"],
  "periods": ["1m"],
  "preload_days": 1,
  "mode": "close_only",
  "topic": "xt:topic:bar"
}
```
ACK 示例：
```json
{
  "ok": true,
  "action": "subscribe",
  "sub_id": "sub-20250917-113005-1a2b3c4d",
  "codes": ["518880.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "topic": "xt:topic:bar"
}
```

#### 状态查询
```json
{"action": "status", "strategy_id": "demo"}
```
返回 `subs`（当前订阅集合）与 `status`（实时服务快照）。

#### 退订
```json
{"action": "unsubscribe", "strategy_id": "demo", "sub_id": "sub-..."}
```
- 若提供 `sub_id`，控制面会查 Registry 还原 codes/periods；
- 亦可直接传 `codes/periods`。

### 5.2 Registry 键约定
- `xt:bridge:subs`：所有 `sub_id` 集合
- `xt:bridge:sub:{sub_id}`：订阅详情哈希
- `xt:bridge:strategy:{strategy_id}:subs`：策略下的订阅集合

## 6. 实时行情契约（v0.5）
RealtimeSubscriptionService 生成的 payload 满足 `docs/contract_v0.5.md` 的约束：

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | str | 标的代码（例 `518880.SH`）|
| `period` | str | `1m/1h/1d` |
| `bar_end_ts` | str | `YYYY-MM-DDTHH:MM:SS+08:00`（收盘时间）|
| `is_closed` | bool | close_only 下始终 `true` |
| `open/high/low/close` | float | K 线价格 |
| `volume/amount` | float | 量额 |
| `preClose/suspendFlag/openInterest/settlementPrice` | 可选透传 |
| `source` | str | 固定 `qmt` |
| `recv_ts` | str | 桥接收到回调的时间戳 |

**去重与模式**：
- `close_only`：只在 `is_closed=true` 时发布一次；
- `forming_and_close`：forming 阶段也会推送，去重键包括 `is_closed`。

**Schema Guard**：当 payload 缺少必备字段或时间格式错误时，事件会被丢弃并计入 `schema_drop_total`。

**晚到判定**：`Metrics.maybe_mark_late` 以 `bar_end_ts` 对当前时间进行判断，超过 `late_threshold_sec`（默认 3s）即计数。下游可结合全局指标监控延迟。

## 7. 历史 API 使用
`core/history_api.py` 提供统一历史拉取入口，流程：按日补齐 → 按秒/日获取 → 转换宽表。

### 7.1 初始化
```python
from core.history_api import HistoryAPI, HistoryAPIConfig
from core.local_cache import LocalCache, CacheConfig

cache = LocalCache(CacheConfig(date_chunk_days=30))
api = HistoryAPI(cache=cache, cfg=HistoryAPIConfig(date_chunk_days=7))
```
- `HistoryAPIConfig.date_chunk_days`：下载时的日期分块；
- `LocalCache` 会对 `download_history_data` 做重试与分块。

### 7.2 调用 `fetch_bars`
```python
result = api.fetch_bars(
    codes=["518880.SH", "513880.SH"],
    period="1m",
    start_time="2025-09-17T09:30:00+08:00",
    end_time="2025-09-17T15:00:00+08:00",
    dividend_type="none",
    return_data=True,
)
```
- `period`：`1m/1h` 使用秒级窗口，`1d` 使用日级；
- `return_data=True` 时返回宽表行列表，否则仅提供摘要；
- 摘要字段：`status`（固定 `ok`）、`count`（行数）、`gaps`（缺口时间列表）、`head_ts/tail_ts`（首尾）。

### 7.3 补齐策略
`LocalCache.ensure_downloaded_date_range` 会按 `codes × period × 日期块` 调用 `download_history_data(..., incrementally=True)`，确保 `get_market_data_ex` 能读取到本地库。

## 8. 指标与健康采集
- **实例指标**：通过 `Metrics().snapshot()` 获取 `published/publish_fail/dedup_hit`，用于健康上报或调试。
- **全局指标**：`Metrics.snapshot_global()` 返回 `bars_published_total/schema_drop_total/late_bars_total`。
- **健康上报**：启用 `HealthReporter` 后，Redis 会写入键 `key_prefix:hostname:pid[:instance_tag]`，值为：
  ```json
  {
    "ts": 1694930000,
    "instance_id": "host:pid[:tag]",
    "metrics": {"published": 10, ...},
    "extra": {"codes": [...], "periods": [...], ...}
  }
  ```
  可用于 Prometheus-exporter 或脚本监控。

## 9. 日志与排查
- 日志初始化由 `core/logging_utils.py` 完成，支持控制台与文件输出。
- 关键日志前缀：`[BOOT]`（启动信息）、`[RT]`（实时服务）、`[CP]`（控制面）、`[PubSubPublisher]`（发布器）。
- `logs/` 目录若开启轮转，会生成 `realtime_bridge.log` 及历史备份。

## 10. 下游接入建议
1. **订阅消费**：使用 Redis 客户端订阅 `redis.topic`，解析 JSON payload，关注 `is_closed` 字段确保只处理收盘条。
2. **补数策略**：当下游断连时，可在恢复后调用 `HistoryAPI.fetch_bars` 补齐缺口（依据 `last bar_end_ts`）。
3. **延迟监控**：周期性读取 `Metrics.snapshot_global()` 或健康上报中的 `metrics`，关注 `late_bars_total` 增量。
4. **订阅管理**：策略侧建议记录 `sub_id`（控制面 ACK 返回），以便在异常或切换时快速退订。

## 11. 常见问题
- **看不到 Redis 推送**：检查 `subscription.mode` 是否为 `close_only`（1m 仅在收盘时送一次），确认标的在交易时段有成交；确认 Redis 连接与 topic 一致。
- **ControlPlane 没响应**：确认 `control.enabled=true`，控制命令 JSON 中包含 `strategy_id/codes/periods`，以及 ACK 通道订阅正确。
- **MiniQMT 数据为空**：确保 `download_history_data` 可用，且 `LocalCache` 配置的日期范围覆盖目标时间窗。
- **健康上报不生效**：确认 Redis 权限允许 `SET`，并检查日志中是否有 `[BOOT] health reporter disabled`。

## 12. 附录：示例命令速查
| 场景 | 命令 |
|------|------|
| 启动服务 | `python scripts/run_with_config.py --config config/realtime.yml` |
| Demo 启动 | `REDIS_URL=redis://host:port/db python scripts/run_with_config.py` |
| 手工订阅 | `redis-cli PUBLISH xt:ctrl:sub '{"action":"subscribe",...}'` |
| 查看健康键 | `redis-cli --raw GET xt:bridge:health:host:pid` |
| 调用历史 API | 参考 §7 Python 样例 |

> 若后续扩展更多功能（如 forming 推送、ClickHouse 落库等），请同步更新此文档和契约文件 `docs/contract_v0.5.md`。
## 13. Mock 行情模式

当研发/联调环境无法接入 QMT/MiniQMT 时，可启用“全天假数据”模式，由系统自动生成随机游走的 bar 数据并推送到 Redis。核心思路：

- 在配置中开启 mock.enabled=true，实时服务将跳过 QMT 连接并启动内置的 MockBarFeeder；
- 每个订阅的 (code, period) 都会按随机游走生成 open/high/low/close 等字段，source 标记为 mock；
- 生成节奏（step_seconds）、基准价、波动率、种子值等均可配置，方便复现。

### 13.1 配置示例
`yaml
subscription:
  codes: ["MOCK.SH"]
  periods: ["1m"]
  mode: close_only
  preload_days: 0

mock:
  enabled: true
  base_price: 15.0        # 初始价格
  volatility: 0.002       # 对数收益标准差
  step_seconds: 0.5       # 生成节奏（秒）
  seed: 42                # 可选随机种子
  volume_mean: 800000
  volume_std: 120000
`

### 13.2 启动方式
1. 使用现有入口：python scripts/run_with_config.py --config config/realtime_mock.yml。若 mock.enabled=true，脚本会自动跳过 QMT 连接并启用随机游走。
2. 快速体验：python scripts/mock_mode_demo.py --codes MOCK.SH --periods 1m --minutes 2，适合临时演示或在 CI 中生成一小段模拟行情。

### 13.3 搭配 Redis 后端测试
1. 准备 Redis（本地或远程），确认 edis.url 可达。
2. 启动 Mock 行情桥，例如：
   `ash
   python scripts/mock_mode_demo.py \
       --redis-url redis://127.0.0.1:6379/0 \
       --topic xt:topic:bar \
       --codes MOCK1.SH,MOCK2.SH \
       --periods 1m \
       --minutes 5 \
       --step-seconds 0.5
   `
3. 另起终端运行监听脚本：python scripts/simple_bar_listener.py --redis-url redis://127.0.0.1:6379/0 --pretty，可观察 source=mock 的 bar 持续推送。
4. 如需测试控制面增删订阅，可继续使用 scripts/flow_demo_subscribe_and_listen.py，Mock 模式与真实模式共用协议。
