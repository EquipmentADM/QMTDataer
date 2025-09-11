# M3.5 测试清单（真Redis + QMT样例 + 流程实验）

> 版本：M3.5｜覆盖 Registry / ControlPlane / Publisher（真 Redis），Realtime 动态 API（假 xtquant），QMT 历史与（可选）实时样例；另附端到端流程实验脚本。

---

## 0. 前置准备

- **Redis**：已部署；建议通过环境变量指定：
  - `REDIS_URL=redis://127.0.0.1:6379/0`（若不设置，默认使用该值）。
- **QMT/MiniQMT（可选）**：如需运行 QMT 样例测试，需确保本机 `xtquant` 可导入且 MiniQMT 已连行情；实时样例需在交易时段。
- **控制面**：端到端实验脚本依赖行情桥已启动且 `control.enabled=true`。

---

## 1. 测试套件总览

| 文件 | 级别 | 依赖 | 覆盖点 |
|---|---|---|---|
| `tests/test_registry_integration.py` | 集成 | 真 Redis | Registry 的 save/load/list/delete 与键设计一致性 |
| `tests/test_pubsub_publisher_integration.py` | 集成 | 真 Redis | 发布器发布→订阅端接收 JSON 消息 |
| `tests/test_control_plane_integration.py` | 集成 | 真 Redis、假 Service | 控制面订阅/退订/查询、ACK 回执、注册表落盘 |
| `tests/test_realtime_service_dynamic_api.py` | 单元 | 假 xtquant | Realtime 动态增删订阅与内部订阅集维护 |
| `tests/test_config_loader_redis_url.py` | 单元 | 无 | `redis.url` 解析为 host/port/db |
| `tests/test_qmt_xtdata_examples.py` | 条件 | QMT（可选） | 历史补齐 + `get_market_data_in`；（可选）最小实时订阅回调 |
| `scripts/flow_demo_subscribe_and_listen.py` | 实验脚本 | 真 Redis、已运行的桥 | 控制面下发订阅 → 监听 ACK 与行情 topic，打印接收内容 |

> 运行方式见各章节“运行示例”。

---

## 2. 单项测试说明

### 2.1 `tests/test_registry_integration.py`（真 Redis）

- **测试内容**：保存/查询/按策略列举/删除订阅规格的持久化行为。
- **目的**：验证注册表的键集合与数据一致性，确保可用于“重启恢复”和观测。
- **输入**：随机 `registry_prefix`；构造 `SubscriptionSpec(strategy_id,codes,periods,mode,preload_days,topic,created_at)`；调用 `save→list/load→delete`。
- **预期输出**：
  - `<prefix>:subs` 集合包含 `sub_id`；
  - `<prefix>:sub:<sub_id>` 哈希包含 `strategy_id/codes/periods/...`；
  - `<prefix>:strategy:<strategy_id>:subs` 集合包含 `sub_id`；
  - `delete` 后上述键被清除。
- **运行示例**：
  ```bash
  set REDIS_URL=redis://127.0.0.1:6379/0
  python -m unittest tests/test_registry_integration.py -v
  ```

---

### 2.2 `tests/test_pubsub_publisher_integration.py`（真 Redis）

- **测试内容**：发布 JSON 到随机 topic，订阅端能收到且字段匹配。
- **目的**：验证发布器与 Redis PubSub 实际联通与序列化正确。
- **输入**：随机 `topic`；调用 `PubSubPublisher.publish(payload)`，订阅端 `pubsub.get_message()` 接收。
- **预期输出**：收到 1 条 JSON 消息，`code/period/is_closed` 等字段与发送一致。
- **运行示例**：
  ```bash
  python -m unittest tests/test_pubsub_publisher_integration.py -v
  ```

---

### 2.3 `tests/test_control_plane_integration.py`（真 Redis + 假 Service）

- **测试内容**：处理 `subscribe / status / unsubscribe`，发送 ACK，写入/删除注册表，驱动服务方法。
- **目的**：验证控制面线程在真 Redis 通道上工作、协议正确、可与后端服务解耦联动。
- **输入**：随机 `channel`、`ack_prefix`、`registry_prefix` 与 `strategy_id`；向控制通道 `PUBLISH` JSON 命令。
- **预期输出**：
  - 订阅：ACK `{ok: true, action: 'subscribe', sub_id: ...}`；服务 `add_subscription()` 被调用 1 次；注册表含该 `sub_id`；
  - 状态：ACK `{ok: true, action: 'status', status: {...}}`；
  - 退订（按 `sub_id`）：ACK `{ok: true, action: 'unsubscribe'}`；服务 `remove_subscription()` 被调用 1 次；注册表为空。
- **运行示例**：
  ```bash
  python -m unittest tests/test_control_plane_integration.py -v
  ```

---

### 2.4 `tests/test_realtime_service_dynamic_api.py`（假 xtquant）

- **测试内容**：`add_subscription / remove_subscription` 的执行路径与订阅集变更。
- **目的**：在无 QMT 环境下，验证动态增删订阅逻辑正确。
- **输入**：fake `xtdata.subscribe_quote / unsubscribe_quote`；`codes=[A]`，`periods=[1m,1d]`。
- **预期输出**：`subscribe` 调用 2 次；`remove` 调用 2 次；内部订阅集合随之增减。
- **运行示例**：
  ```bash
  python -m unittest tests/test_realtime_service_dynamic_api.py -v
  ```

---

### 2.5 `tests/test_config_loader_redis_url.py`（配置解析）

- **测试内容**：把 `redis.url: "redis://127.0.0.1:6379/0"` 解析成 `host/port/db/password`。
- **目的**：确保 M3.5 中对 Redis URL 的兼容实现正确。
- **输入**：最小 YAML；调用 `load_config()`。
- **预期输出**：`host=127.0.0.1`、`port=6379`、`db=0`。
- **运行示例**：
  ```bash
  python -m unittest tests/test_config_loader_redis_url.py -v
  ```

---

### 2.6 `tests/test_qmt_xtdata_examples.py`（QMT 样例｜条件执行）

> 说明：仅当本机 `xtquant` 可导入时执行；实时样例需在交易时段，并设置 `RUN_QMT_REALTIME_TEST=1`。

- **历史样例**
  - **测试内容**：`download_history_data → get_market_data_in` 基本流程。
  - **目的**：验证“先补后取”的 QMT 历史获取路径。
  - **输入**：`codes=[518880.SH, 513880.SH]`，`period=1d`，近 15 天日期区间。
  - **预期输出**：返回 `dict`，包含 `time/open/high/low/close` 等键；`time` 具 DataFrame 形态。
  - **运行示例**：
    ```bash
    python -m unittest tests/test_qmt_xtdata_examples.py::TestQmtXtDataExamples::test_download_and_get_in -v
    ```

- **实时最小样例（可选）**
  - **测试内容**：`subscribe_quote + run` 等待≤10s 收到≥1次回调。
  - **目的**：验证本机实时通路可用。
  - **输入**：`RUN_QMT_REALTIME_TEST=1`，`QMT_TEST_CODE`（默认 518880.SH），`period=1m`。
  - **预期输出**：回调计数 ≥1。
  - **运行示例**：
    ```bash
    set RUN_QMT_REALTIME_TEST=1
    set QMT_TEST_CODE=518880.SH
    python -m unittest tests/test_qmt_xtdata_examples.py::TestQmtXtDataExamples::test_subscribe_minimal_realtime -v
    ```

---

## 3. 端到端流程实验脚本（盘中演示）

文件：`scripts/flow_demo_subscribe_and_listen.py`

- **测试内容**：
  1) 通过控制通道向行情桥下发 `subscribe`；
  2) 同时订阅 `ACK` 通道与行情 `topic`；
  3) 打印收到的 `ACK`（含 `sub_id`）与每条 K 线（`code/period/bar_end_ts/close/is_closed`）；
  4) （可选）结束时自动 `unsubscribe`。
- **目的**：在真实行情时段，快速验证“策略端下发 → 桥订阅 → 推送 Redis → 对端接收”的全链路。
- **输入**：`--redis-url`、`--ctrl-channel`、`--ack-prefix`、`--topic`、`--strategy-id`、`--codes`、`--periods`、`--minutes`、`--preload-days`、`--auto-unsubscribe`。
- **预期输出**：
  - `[ACK] {ok:true, action:'subscribe', sub_id:...}`；
  - `[BAR] <code> <period> <bar_end_ts> close=<...> is_closed=<...>` 持续打印；
  - （启用 `--auto-unsubscribe`）结束时打印退订发送提示。
- **运行示例**：
  ```bash
  # 终端A：先启动行情桥（确保 control.enabled=true 且 topic 与下方一致）
  python scripts/run_with_config.py --config configs/realtime.yml

  # 终端B：流程实验（盘中执行更易收到数据）
  python scripts/flow_demo_subscribe_and_listen.py \
    --redis-url redis://127.0.0.1:6379/0 \
    --ctrl-channel xt:ctrl:sub \
    --ack-prefix xt:ctrl:ack \
    --topic xt:topic:bar \
    --strategy-id demo \
    --codes 518880.SH \
    --periods 1m \
    --minutes 3 \
    --preload-days 1 \
    --auto-unsubscribe
  ```

---

## 4. 常见问题（FAQ）

1. **收不到 BAR**：
   - 确认桥在运行且 `subscription.mode=close_only` 时，1 分钟线仅在“收盘时刻”推送；盘中无成交或站点异常也会影响。
   - 确认 `topic` 一致；确认 Redis 网络与权限正常。  
2. **ACK 没返回**：
   - 检查控制通道名与 ACK 前缀是否与桥配置一致；检查 JSON 命令是否包含 `strategy_id/codes/periods`。  
3. **键污染**：
   - 测试使用随机前缀/通道；若需清理，`KEYS <prefix>:*` + `DEL`。

---

## 5. 一键运行建议

- Windows（PowerShell）：
  ```powershell
  $env:REDIS_URL = "redis://127.0.0.1:6379/0"
  python -m unittest discover -s tests -p "test_*m3.5*.py" -v
  ```
- Linux/macOS：
  ```bash
  export REDIS_URL=redis://127.0.0.1:6379/0
  python -m unittest discover -s tests -p "test_*m3.5*.py" -v
  ```

> 说明：文件名未强制以 `m3.5` 结尾，亦可按上表逐个执行。

