# QMTD 实时行情 Redis 控制协议

## 1. 协议定位

本文定义策略端或 BTLive 端与 QMTD 实时行情服务之间的 Redis 控制协议。

该协议只负责实时行情需求声明与结果确认，不负责：

- 启动或停止 QMTD 服务进程。
- 管理统一控制台 HTTP bridge。
- 发起历史行情下载或 FD 数据库入库任务。
- 管理策略生命周期。

## 2. 通道约定

默认通道如下：

- 控制通道：`xt:ctrl:sub`
- ACK 前缀：`xt:ctrl:ack`
- 策略 ACK 通道：`xt:ctrl:ack:<strategy_id>`
- 行情推送 topic：`xt:topic:bar`
- 订阅注册表前缀：`xt:bridge`

部署时可以通过 QMTD 配置文件覆盖这些值，但同一套运行环境内应保持策略端与 QMTD 配置一致。

## 3. 通用字段

所有控制命令均使用 JSON 字符串发布到控制通道。

### 3.1 请求通用字段

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `action` | string | 是 | 命令类型：`subscribe`、`unsubscribe`、`status` |
| `strategy_id` | string | 是 | 策略或调用方标识，用于 ACK 通道和订阅归属 |

### 3.2 ACK 通用字段

QMTD 处理命令后，将 ACK JSON 发布到：

```text
<ack_prefix>:<strategy_id>
```

ACK 通用字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `ok` | bool | 是 | 命令是否处理成功 |
| `action` | string | 否 | 成功时通常返回对应命令类型 |
| `error` | string | 否 | 失败原因 |

## 4. subscribe

### 4.1 请求

```json
{
  "action": "subscribe",
  "strategy_id": "ma_cross_demo",
  "codes": ["510050.SH", "518880.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "preload_days": 0,
  "topic": "xt:topic:bar"
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `codes` | list[string] | 是 | 订阅标的列表 |
| `periods` | list[string] | 是 | 订阅周期列表，例如 `1m`、`1d` |
| `mode` | string | 否 | 推送模式，默认取 QMTD 服务配置 |
| `preload_days` | int | 否 | 订阅前历史预热天数，建议策略运行时传 `0` |
| `topic` | string | 否 | 行情推送 topic，默认取 QMTD 服务配置 |

### 4.2 成功 ACK

```json
{
  "ok": true,
  "action": "subscribe",
  "sub_id": "sub-20260423-103000-xxxxxxxx",
  "codes": ["510050.SH", "518880.SH"],
  "periods": ["1m"],
  "mode": "close_only",
  "topic": "xt:topic:bar"
}
```

`sub_id` 是本次订阅请求的唯一标识，后续退订应优先使用该字段。

### 4.3 失败 ACK

```json
{
  "ok": false,
  "error": "codes/periods required"
}
```

常见失败原因：

- `strategy_id` 为空。
- `strategy_id` 不在允许列表中。
- `codes` 或 `periods` 为空。
- QMTD 底层订阅失败。

## 5. unsubscribe

### 5.1 按 sub_id 退订

推荐方式：

```json
{
  "action": "unsubscribe",
  "strategy_id": "ma_cross_demo",
  "sub_id": "sub-20260423-103000-xxxxxxxx"
}
```

### 5.2 按 codes 与 periods 退订

兼容方式：

```json
{
  "action": "unsubscribe",
  "strategy_id": "ma_cross_demo",
  "codes": ["510050.SH"],
  "periods": ["1m"]
}
```

按 `codes × periods` 退订时，调用方必须确保退订口径与订阅口径一致。

### 5.3 成功 ACK

```json
{
  "ok": true,
  "action": "unsubscribe",
  "codes": ["510050.SH"],
  "periods": ["1m"]
}
```

### 5.4 失败 ACK

```json
{
  "ok": false,
  "error": "sub_id not found"
}
```

常见失败原因：

- `sub_id` 不存在。
- 未提供 `sub_id`，且 `codes` 或 `periods` 为空。
- QMTD 底层退订失败。

## 6. status

### 6.1 请求

```json
{
  "action": "status",
  "strategy_id": "ma_cross_demo"
}
```

### 6.2 当前 ACK

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

当前 `subs` 字段包含注册表里的 `sub_id` 列表；`status.subs` 包含实时服务当前活跃行情流。

### 6.3 后续增强方向

后续建议将状态增强为更可诊断的结构：

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

该增强只用于诊断，不应变成 BTLive 对 QMTD 的服务治理接口。

## 7. 幂等与重复命令规则

### 7.1 subscribe

当前实现会为每次成功订阅生成新的 `sub_id`。因此同一策略重复发送相同 `codes` 与 `periods`：

- 会产生多个订阅记录。
- 后续需要分别退订，或由后续引用计数机制统一处理。

目标设计中，底层行情流应按 `(code, period)` 做引用计数。同一行情流被多次引用时，只应向 xtdata
注册一次底层订阅。

### 7.2 unsubscribe

推荐按 `sub_id` 退订，因为它能精确对应一次订阅请求。

后续引用计数目标规则：

- 每次退订只减少对应 `sub_id` 展开的 `(code, period)` 引用。
- 只有某个 `(code, period)` 的引用计数降为 `0` 时，才真正取消底层 xtdata 订阅。
- 如果策略漏退订，最坏情况是多保留一个行情流，不应误关其他策略仍在使用的行情。

### 7.3 status

`status` 是只读命令，不改变订阅状态。

## 8. 错误处理原则

QMTD 应尽量返回结构化 ACK，而不是让调用方只能依赖日志。

错误 ACK 最低要求：

```json
{
  "ok": false,
  "error": "human readable error"
}
```

后续可扩展为：

```json
{
  "ok": false,
  "error": "codes/periods required",
  "error_code": "BAD_REQUEST",
  "detail": {
    "action": "subscribe"
  }
}
```

第一阶段先保持兼容当前实现，不强制引入 `error_code`。

## 9. 调用方建议

策略端或 BTLive 端应遵守：

- 启动策略前发送 `subscribe`。
- 必须监听 `xt:ctrl:ack:<strategy_id>` 并确认 `ok=true`。
- 保存 `sub_id`。
- 策略停止时优先按 `sub_id` 发送 `unsubscribe`。
- 不通过本协议启动、停止或配置 QMTD 服务。
- 不把 `status` 当作服务治理接口，只用于诊断或订阅确认。

## 10. 现有示例

当前可参考：

- `tests/flow_demo_subscribe_and_listen.py`
- `scripts/send_control_cmd.py`

后续建议新增更贴近策略端的示例客户端，把订阅、等待 ACK、退订封装为可复用函数。
