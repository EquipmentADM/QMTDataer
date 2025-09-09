# QMT 行情桥（历史获取 + 实时订阅 → Redis PubSub）

> **版本**：M0 方案冻结（v1）  
> **范围**：仅设计与配置样例；不含实现代码与测试  
> **关键约束（按用户要求）**：
> 1) 不优先使用 miniQMT 本地库；历史以**在线新鲜拉取**为主，权威数据写入**后方数据库**。  
> 2) Redis 通道先走 **PubSub**（不启用 Streams）。  
> 3) **消息模型采用宽表**（一次性包含所需字段）。  
> 4) 仅做 **bar 级** 数据（1m / 1h / 1d），考虑去重，不做订单流。  
> 5) 主要功能：**历史数据接口** + **按订阅配置推送实时数据**。

---

## 0. 摘要
本项目建立 QMT/xtdata 在线连接：
- 历史：按 `codes × period × time window × dividend_type` 在线拉取**新鲜数据**，**分段下载**并**写入后方数据库**（权威库），必要时返回给调用方；
- 实时：根据订阅配置订阅分钟/小时/日线，进行**聚合/去重/成形判断（forming/close）**，默认 **close-only** 推送至 Redis PubSub，供策略与本地数据库异步消费；
- 如消费方短暂离线，**以权威库补拉**覆盖缺口（后续可选升级 Streams 以支持回放）。

---

## 1. 设计原则与取舍
- **数据源优先级**：在线新鲜下载 → 写入权威库（ClickHouse/PostgreSQL/Parquet Lakehouse 等），**不依赖 miniQMT 本地库**。
- **消息通道**：首期采用 **Redis PubSub**（主题广播），降低实现复杂度；回放诉求由“权威库补拉”承担。
- **消息模型（宽表）**：统一字段集（见 §5），减少下游解析成本；以 `(code, period, bar_end_ts, dividend_type)` 为幂等主键。
- **周期与成形**：仅支持 `1m / 1h / 1d`；默认 **close-only** 推送，可配置 forming 模式（带 `is_closed=false`）。
- **时区与交易日历**：统一 `Asia/Shanghai`（中国市场无夏令时）；多市场支持时按市场维度标注并切换交易日历。

---

## 2. 总体架构与时序
```
[ QMT / xtdata ]  <--在线拉取/订阅-->  [QMTConnector]
                                        │
            ┌────────────────────────────┴───────────────────────────┐
            │                                                        │
   [HistoryAPI] —— 在线拉取 → [后方数据库(权威)] ←——（补拉/回查）——— [Strategy/DB]
            │                                                        │
            └────────（可返回结果集给调用方）──────────────────────────┘

                    [RealtimeSubscriptionService]
                               │（回调/聚合/去重/成形判断）
                               ▼
                         [PubSubPublisher] ——→ Redis PubSub ——→ 策略/DB
```
**历史链路时序**（简化）：
1) 调用方请求 → HistoryAPI 读取权威库最新 `last_ts`；  
2) 按 `start/end` **分段在线下载**；  
3) 每段写入权威库（upsert by 主键）；  
4) 返回汇总：`count / gaps / head_ts / tail_ts`（可选附数据）。

**实时链路时序**（简化）：
1) 读取订阅配置 → 订阅 codes×periods；  
2) 回调片段 → 聚合为 bar；去重与成形判断；  
3) 默认仅在 **bar close** 时发布到 `xt:topic:bar`；forming 模式则在 bar 周期内多次发布；  
4) 策略/DB 消费；如离线则通过 HistoryAPI 按 `last_ts` 补拉。

---

## 3. 模块说明（类说明 / 方法说明）

### 3.1 QMTConnector（连接与心跳）
- **类说明**：管理 QMT/xtdatacenter/xtdata 的初始化、最佳站点选择、心跳与重连。
- **上游**：QMT/xtdatacenter；**下游**：HistoryAPI、RealtimeSubscriptionService。
- **关键职责**：
  - 启动/监听端口；建立与 xtdata 的稳定连接；
  - 连接状态上报（用于运维监控）；
  - 统一提供行情会话句柄。

### 3.2 HistoryAPI（历史数据在线拉取）
- **类说明**：对外提供统一历史拉取接口，在线下载**新鲜数据**并写入权威库；可返回数据集。
- **上游**：QMT 在线行情；**下游**：权威库、调用方。
- **方法说明**：
  - `fetch_bars(codes, period, start_time, end_time, dividend_type='none', backfill=True, return_data=True)`  
    - **功能**：分段下载+落库；返回下载摘要或数据集；
    - **输入**：`codes[List[str]]`, `period in {'1m','1h','1d'}`, `start_time/end_time (ISO8601, Asia/Shanghai)`, `dividend_type`, `backfill`, `return_data`；
    - **输出**：`{status, count, gaps[], head_ts, tail_ts, data?}`；
    - **上下游**：QMT（上游），权威库/调用方（下游）。

### 3.3 RealtimeSubscriptionService（订阅/聚合/去重/成形）
- **类说明**：按配置发起实时订阅，对回调片段做聚合、去重与成形判断。
- **上游**：QMT 实时回调；**下游**：PubSubPublisher。
- **方法说明**：
  - `run(config_path)`  
    - **功能**：读取订阅配置，建立订阅并处理回调，按模式发布（close-only / forming+close）。
    - **输出**：运行指标（发布条数、延迟分位、去重/丢弃计数）。

### 3.4 PubSubPublisher（发布器）
- **类说明**：将标准化 bar 消息发布至 Redis PubSub；提供序列化、超时与重试、指标打点。
- **上游**：RealtimeSubscriptionService；**下游**：策略/DB。
- **方法说明**：
  - `publish(message, topic)`  
    - **功能**：序列化并发布消息；记录发布耗时与失败重试统计。

---

## 4. 目录结构（冻结）
```
qmt_bridge/
├─ core/
│  ├─ qmt_connector.py          # 连接/站点/心跳（类说明/方法说明占位）
│  ├─ history_api.py            # 历史在线拉取（类说明/方法说明占位）
│  ├─ realtime_service.py       # 订阅/聚合/去重/成形（类说明/方法说明占位）
│  ├─ pubsub_publisher.py       # Redis PubSub 发布（类说明/方法说明占位）
│  └─ models.py                 # 宽表数据模型/枚举（说明占位）
├─ config/
│  ├─ qmt.yml                   # QMT 连接
│  ├─ history.yml               # 历史拉取
│  ├─ subscription.yml          # 实时订阅
│  └─ backend.yml               # 权威库
└─ docs/
   ├─ DESIGN.md
   ├─ SCHEMA.md
   ├─ OPERATIONS.md
   └─ CHANGELOG.md
```

---

## 5. 消息模型（宽表）

### 5.1 K 线宽表字段定义（冻结）
- 维度：
  - `code`：证券代码（如 `000001.SZ`）
  - `period`：`1m | 1h | 1d`
  - `bar_open_ts`：bar 开始时间（ISO8601，Asia/Shanghai）
  - `bar_end_ts`：bar 结束/收盘时间（唯一键的一部分）
  - `is_closed`：`true|false`（默认仅在 `true` 时发布）
- 价格量额：`open, high, low, close, volume, amount`  
- 复权：`dividend_type`：`none|front|back|ratio`（默认 `none`）
- 源与时间：`source='qmt'`, `recv_ts`（回调接收时间）, `gen_ts`（本地聚合生成时间）
- 质量元信息（可选）：`gap_flag`, `partial_fill_ratio`
- **幂等键**：`(code, period, bar_end_ts, dividend_type)`

### 5.2 JSON Schema（简化）
```json
{
  "type": "object",
  "required": ["code", "period", "bar_open_ts", "bar_end_ts", "is_closed", "open", "high", "low", "close", "volume", "amount", "dividend_type"],
  "properties": {
    "code": {"type": "string"},
    "period": {"type": "string", "enum": ["1m", "1h", "1d"]},
    "bar_open_ts": {"type": "string", "format": "date-time"},
    "bar_end_ts": {"type": "string", "format": "date-time"},
    "is_closed": {"type": "boolean"},
    "open": {"type": "number"},
    "high": {"type": "number"},
    "low": {"type": "number"},
    "close": {"type": "number"},
    "volume": {"type": "number"},
    "amount": {"type": "number"},
    "dividend_type": {"type": "string", "enum": ["none", "front", "back", "ratio"]},
    "source": {"type": "string"},
    "recv_ts": {"type": "string", "format": "date-time"},
    "gen_ts": {"type": "string", "format": "date-time"},
    "gap_flag": {"type": "boolean"},
    "partial_fill_ratio": {"type": "number"}
  }
}
```

### 5.3 发布消息示例（1m close-only）
```json
{
  "code": "000001.SZ",
  "period": "1m",
  "bar_open_ts": "2025-09-05T14:59:00+08:00",
  "bar_end_ts": "2025-09-05T15:00:00+08:00",
  "is_closed": true,
  "open": 10.23,
  "high": 10.25,
  "low": 10.20,
  "close": 10.22,
  "volume": 312000,
  "amount": 3180000.0,
  "dividend_type": "none",
  "source": "qmt",
  "recv_ts": "2025-09-05T15:00:00.120+08:00",
  "gen_ts": "2025-09-05T15:00:00.130+08:00"
}
```

---

## 6. Redis PubSub 主题与序列化
- **默认主题**：`xt:topic:bar`（统一入口，消息内区分 `code/period`）。
- **可选分拆**：`xt:topic:bar:{period}` 或 `xt:topic:bar:{code}:{period}`（按需扩展）。
- **序列化**：默认 `JSON`；如需性能/带宽，再评估 `MsgPack`（兼容性优先）。

---

## 7. 配置样例（冻结为初版样例文件）
> 说明：以下内容对应仓库 `config/` 目录。占位变量用 `${VAR}` 表示。

### 7.1 `config/qmt.yml`
```yaml
# QMT / xtdatacenter / xtdata 连接配置
# 注意：此处不以 miniQMT 本地库为权威，仅作为通道依赖

token: ${QMT_TOKEN}
endpoint_strategy: auto            # auto | fixed
listen_port_range: [50100, 50150]  # 建议预留一段连续端口
userdata_dir: D:/QMT/userdata_mini # 仅通道依赖，避免C盘权限问题
connect_timeout_ms: 3000
heartbeat_interval_ms: 1000
reconnect_backoff: [500, 1000, 3000, 5000]  # 指数退避毫秒
```

### 7.2 `config/history.yml`
```yaml
# 历史在线拉取配置（新鲜数据为主）

default_period: 1m
periods_allow: [1m, 1h, 1d]
dividend_type: none                 # none | front | back | ratio
batch_size: 3000                    # 分段下载每批行数
max_concurrency: 2                  # 并发分段数（受QMT与网络限制）
backfill: true                      # 在线拉取后写入权威库
return_data: false                  # 对服务端：默认只返回摘要；可按需返回数据
# 可选：交易日历/时段设置（多市场扩展用）
calendar:
  timezone: Asia/Shanghai
  market: CN
```

### 7.3 `config/subscription.yml`
```yaml
# 实时订阅与发布配置

mode: close_only                  # close_only | forming_and_close
periods: [1m, 1h, 1d]
codes:
  - 000001.SZ
  - 600000.SH
  - 000300.SH                     # 指数示例：如需支持，需确保数据源可用
pubsub:
  topic: xt:topic:bar
  serializer: json                # json | msgpack
  publish_timeout_ms: 500
  retry:
    max_retries: 3
    backoff_ms: [100, 300, 800]
# 节流/聚合（仅对 forming 模式生效）
forming:
  min_emit_interval_ms: 200       # 聚合发射最小间隔
```

### 7.4 `config/backend.yml`
```yaml
# 权威数据库配置（三选一示例）

engine: clickhouse                # clickhouse | postgresql | parquet
# ClickHouse 示例
dsn: "clickhouse://user:pass@host:9000/xt"  # 或使用环境变量注入
# PostgreSQL 示例（如选用）
# dsn: "postgresql://user:pass@host:5432/xt"
# Parquet/Lakehouse 示例（如选用）
# dsn: "file:///data/xt_lake/bars"

schema:
  table: bars_wide
  upsert_keys: [code, period, bar_end_ts, dividend_type]
  # 字段类型（建议在建表时一次到位，以下为参考）
  columns:
    code: String
    period: LowCardinality(String)
    bar_open_ts: DateTime64(3, 'Asia/Shanghai')
    bar_end_ts: DateTime64(3, 'Asia/Shanghai')
    is_closed: UInt8
    open: Float64
    high: Float64
    low: Float64
    close: Float64
    volume: Float64
    amount: Float64
    dividend_type: LowCardinality(String)
    source: LowCardinality(String)
    recv_ts: DateTime64(3, 'Asia/Shanghai')
    gen_ts: DateTime64(3, 'Asia/Shanghai')
    gap_flag: UInt8
    partial_fill_ratio: Float64
```

---

## 8. 运维与监控（首期）
- **指标**：
  - 连接：心跳、重连次数、端口监听状态；
  - 历史：分段批次数、单段耗时、落库成功/失败、缺口计数；
  - 实时：发布条数、端到端延迟（回调→发布）p50/p95/p99、去重/丢弃计数；
- **告警**：连接断开、延迟超阈、发布失败率、补拉失败；
- **日志**：结构化 JSON，字段建议包含：`code, period, bar_end_ts, is_closed, latency_ms, retry_count, err_code`；
- **时钟**：统一 NTP 校时，毫秒级时间戳。

---

## 9. 风险与对策
| 风险 | 场景 | 对策 |
|---|---|---|
| 大窗口在线拉取 | 请求/内存压力 | 分段批量、限并发、断点续拉；每段落库（避免一次性堆内存） |
| 回调抖动/短时断连 | 丢 bar 或乱序 | 默认 close-only；以权威库 `last_ts` 触发补拉覆盖 |
| PubSub 无回放 | 消费者离线丢数据 | 以 HistoryAPI 补拉兜底；后续可增 Streams 实现回放 |
| 复权口径不一致 | 指数/标的对不上 | 宽表携带 `dividend_type`，必要时多视图并行 |
| 时区/时间漂移 | 去重异常 | 统一 Asia/Shanghai；严格使用 `bar_end_ts` 作为幂等键 |

---

## 10. 变更记录（相对上一轮计划）
- 删除“优先本地（get_local_data）”策略，改为**在线新鲜下载 → 权威库**模式；
- Redis 通道限定为 **PubSub**，回放依赖**补拉**；
- 固化 **宽表** Schema 与幂等主键；
- 聚焦 **bar 级周期**（1m/1h/1d），去重但不涉及订单流；
- 两大功能聚焦：**历史接口**与**订阅推送**；
- 统一 **Asia/Shanghai** 时区（便于中国市场场景）。

---

### 附录 A：术语
- **权威库**：后方数据库（ClickHouse/PostgreSQL/Lakehouse），承载历史拉取与缺口补拉的单一事实来源。
- **forming/close**：bar 在周期内的临时状态（forming）与收盘状态（close）。

### 附录 B：后续实施路线（展望）
- M1：`QMTConnector`/`HistoryAPI` 骨架 + 分段在线拉取与落库；
- M2：`RealtimeSubscriptionService` + `PubSubPublisher` 跑通；
- M3：断连自愈 + 基于 `last_ts` 的自动补拉；
- M4：与策略/DB 的消费样例与运维手册。

