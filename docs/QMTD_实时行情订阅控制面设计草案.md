# QMTD 实时行情订阅控制面设计草案

## 1. 文档目的

本文记录 QMTDataer 实时行情订阅控制面的当前设计结论、已有实现、缺口与待定事项。

本文只讨论实时行情订阅、退订、ACK 与 Redis 推送链路，不讨论历史行情下载、FD 数据库入库、
统一控制台 HTTP bridge 页面与任务接口。

## 2. 已确认原则

### 2.1 历史行情与实时行情分离

QMTD 内部目前应视为两个业务面：

- 历史行情面：负责 xtdata 历史数据拉取、补全、合并、落盘到 FD 数据库目录。
- 实时行情面：负责接收策略端订阅需求，向 MiniQMT 订阅实时 bar，并推送到 Redis topic。

两者可以共享 xtdata 依赖、配置结构、日志工具、Redis 连接参数，但业务链路应保持分离。

### 2.2 HTTP bridge 不介入实时订阅

统一控制台当前定位为历史数据管理、服务状态查看、任务发起与结果展示。

实时行情订阅属于策略运行时能力，不纳入 HTTP bridge 的第一阶段接口范围。当前不要求统一控制台
提供实时订阅可视化页面，也不要求通过 HTTP bridge 发起实时行情订阅。

### 2.3 实时订阅控制面使用 Redis

实时订阅控制面继续使用 Redis PubSub：

- 策略端或 BTLive 端向控制通道发送订阅、退订、状态查询命令。
- QMTD ControlPlane 监听控制通道并执行命令。
- QMTD 通过策略专属 ACK 通道返回结构化结果。
- 实时 bar 数据推送到统一行情 topic。

这条链路与 HTTP bridge 解耦，避免统一控制台成为策略运行时强依赖。

## 3. 当前已有实现

### 3.1 核心代码

- `core/control_plane.py`：Redis 控制面线程，处理 `subscribe`、`unsubscribe`、`status`。
- `core/registry.py`：订阅注册表，按 `sub_id` 保存订阅规格。
- `core/realtime_service.py`：实时订阅服务，负责 xtdata 订阅、bar 状态机、Redis 推送。
- `tests/flow_demo_subscribe_and_listen.py`：端到端演示脚本，包含发送订阅、监听 ACK、监听行情、
  自动退订流程。

### 3.2 默认通道

- 控制通道：`xt:ctrl:sub`
- ACK 前缀：`xt:ctrl:ack`
- ACK 通道：`xt:ctrl:ack:<strategy_id>`
- 行情 topic：通常为 `xt:topic:bar`
- 注册表前缀：`xt:bridge`

### 3.3 当前命令

订阅命令：

```json
{
  "action": "subscribe",
  "strategy_id": "ma_cross_demo",
  "codes": ["510050.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "preload_days": 0,
  "topic": "xt:topic:bar"
}
```

退订命令：

```json
{
  "action": "unsubscribe",
  "strategy_id": "ma_cross_demo",
  "sub_id": "sub-20260423-103000-xxxxxxxx"
}
```

状态命令：

```json
{
  "action": "status",
  "strategy_id": "ma_cross_demo"
}
```

### 3.4 当前 ACK 形式

订阅成功：

```json
{
  "ok": true,
  "action": "subscribe",
  "sub_id": "sub-20260423-103000-xxxxxxxx",
  "codes": ["510050.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "topic": "xt:topic:bar"
}
```

退订成功：

```json
{
  "ok": true,
  "action": "unsubscribe",
  "codes": ["510050.SH"],
  "periods": ["1m"]
}
```

状态成功：

```json
{
  "ok": true,
  "action": "status",
  "status": {
    "subs": [
      {"code": "510050.SH", "period": "1m"}
    ],
    "last_published": {
      "510050.SH|1m": 1770000000.0
    }
  },
  "subs": ["sub-20260423-103000-xxxxxxxx"]
}
```

## 4. 当前缺口

### 4.1 正式协议文档已补充

当前已新增 `docs/QMTD_实时行情Redis控制协议.md`，用于固化命令字段、ACK 字段、
错误返回、通道约定、幂等规则和退订规则。

后续如果协议字段发生变化，应优先同步协议文档，再调整示例和测试。

### 4.2 缺少策略端 ACK 监听示例

`tests/flow_demo_subscribe_and_listen.py` 已经能作为端到端演示，但它仍偏测试脚本。

后续建议补一个更接近策略端调用方式的示例，例如：

- `examples/realtime_strategy_subscribe_client.py`
- 只封装订阅、等待 ACK、状态查询、退订。
- 不承担 QMTD 启动职责。
- 不与 HTTP bridge 发生关系。

### 4.3 多策略引用计数尚不完善

当前 `RealtimeSubscriptionService` 内部使用 `_subs: set[(code, period)]` 保存活跃订阅。

这意味着：

- 多个策略订阅同一个 `(code, period)` 时，服务端只会订阅一次，这是合理的。
- 但任意一个策略退订同一个 `(code, period)` 时，当前服务可能直接取消底层订阅。
- 这会影响仍然依赖该行情的其他策略。

因此目前不能认为多策略订阅引用计数已经完整。

### 4.4 空白启动入口已补充

`RealtimeSubscriptionService.run_forever()` 本身支持没有初始 `codes` 时不注册初始订阅。

当前已新增 `scripts/run_realtime_control.py`，并为 `core.config_loader.load_config()` 增加
`allow_empty_subscription` 参数。该入口会清空初始订阅标的、开启 ControlPlane，并等待 Redis
控制命令。

## 5. 引用计数设计

### 5.1 计数口径

引用计数按行情流 key 统计：

```text
stream_key = (code, period)
```

例如：

```text
("510050.SH", "1m")
("rb00.SF", "1m")
```

订阅和退订都必须以这个口径统计。

### 5.2 注册关系

建议同时维护两层关系：

- `sub_id -> SubscriptionSpec`
- `stream_key -> ref_count`
- `stream_key -> set[sub_id]`

其中 `SubscriptionSpec` 仍保存：

- `strategy_id`
- `codes`
- `periods`
- `mode`
- `preload_days`
- `topic`
- `created_at`

### 5.3 订阅规则

收到订阅命令后：

1. 生成 `sub_id`。
2. 保存 `SubscriptionSpec`。
3. 展开 `codes × periods` 得到多个 `stream_key`。
4. 对每个 `stream_key` 增加引用。
5. 只有当该 `stream_key` 引用从 `0` 变为 `1` 时，才调用底层 xtdata 订阅。
6. 返回订阅 ACK。

### 5.4 退订规则

收到退订命令后：

1. 优先根据 `sub_id` 找到原始 `SubscriptionSpec`。
2. 展开 `codes × periods` 得到多个 `stream_key`。
3. 对每个 `stream_key` 减少引用。
4. 只有当该 `stream_key` 引用降为 `0` 时，才调用底层 xtdata 退订。
5. 删除 `sub_id` 相关注册信息。
6. 返回退订 ACK。

### 5.5 异常退订的现实影响

如果某个策略忘记退订：

- 结果是该 `stream_key` 继续保持订阅，造成多余行情推送。
- 这比误退订导致其他策略断行情更安全。

后续可以通过过期策略清理、心跳绑定或人工状态检查解决长时间遗留订阅问题。

## 6. 状态查询设计

实时状态查询主要服务于调试与运行观测，不应成为 BTLive 对 QMTD 的强控制入口。

建议状态返回保持轻量，但比当前更业务化：

```json
{
  "ok": true,
  "action": "status",
  "status": {
    "active_streams": [
      {
        "code": "510050.SH",
        "period": "1m",
        "ref_count": 2,
        "last_published_ts": "2026-04-23T10:30:00"
      }
    ],
    "strategy_subscriptions": [
      {
        "strategy_id": "ma_cross_demo",
        "sub_ids": ["sub-20260423-103000-xxxxxxxx"]
      }
    ]
  }
}
```

第一阶段不要求状态接口提供复杂控制能力，只需要能解释当前服务是否正在推、哪些行情在推、
大致是谁引用。

## 7. 空白启动模式设计

### 7.1 目标

QMTD 实时服务可以在没有初始订阅的情况下启动：

- 启动 Redis publisher。
- 启动 ControlPlane。
- 不主动订阅任何行情。
- 等待策略端通过 Redis 控制通道申请行情。

### 7.2 建议入口

建议新增或整理一个专用入口，例如：

```text
scripts/run_realtime_control.py
```

该入口职责：

- 读取 Redis、topic、control 配置。
- 允许 `subscription.codes` 为空。
- 默认开启 `control.enabled`。
- 默认 `preload_days=0`，避免策略订阅阶段被历史下载阻塞。
- 启动 `RealtimeSubscriptionService` 并阻塞运行。

### 7.3 配置调整方向

可以选择两种方式之一：

- 为 `load_config()` 增加 `allow_empty_subscription=True` 参数。
- 新增轻量配置加载函数，专用于实时控制面空白启动。

更推荐第一种，因为能复用现有配置结构，同时让“是否允许空订阅”成为调用方明确选择。

## 8. 与 BTLive 的关系

当前项目中未发现正式的 BTLive 生产侧控制实现。已有的是 QMTD 项目内的演示和测试脚本：

- `tests/flow_demo_subscribe_and_listen.py`
- `scripts/send_control_cmd.py`

因此当前应按以下边界推进：

- QMTD 提供 Redis 控制协议、ACK、实时 bar topic。
- BTLive 或策略端自行实现协议客户端。
- QMTD 可以提供一个示例客户端，降低 BTLive 接入成本。
- 不要求 BTLive 通过 HTTP bridge 控制 QMTD。

## 9. 后续工作拆分

### 9.1 第一优先级

- 实现服务层按 `(code, period)` 的引用计数。
- 将 `tests/flow_demo_subscribe_and_listen.py` 整理为策略端 ACK 示例或新增 `examples` 示例。

### 9.2 第二优先级

- 状态接口增加 `ref_count`、`sub_id`、`strategy_id` 的可解释信息。
- 增加订阅恢复策略：服务重启后是否根据 Registry 重放订阅。
- 增加遗留订阅清理策略：例如按策略心跳、TTL 或人工清理。

### 9.3 暂不做

- 不把实时订阅控制接入 HTTP bridge。
- 不做统一控制台实时行情可视化。
- 不把 BTLive 设计成 QMTD 的强控制上游。
- 不在第一阶段做跨进程复杂调度或主控统一实时任务中心。

## 10. 当前结论

实时行情订阅控制面已有可运行雏形，但还不是完整的多策略订阅管理系统。

当前最关键的下一步不是扩展 HTTP bridge，而是补齐引用计数和策略端 ACK 示例。这样可以让 QMTD
作为独立实时行情服务运行，BTLive 只按协议申请和消费行情，双方保持解耦。
