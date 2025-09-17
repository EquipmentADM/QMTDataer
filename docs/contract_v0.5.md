# 发布契约 v0.5

## 主题与语义
- 主题（Topic）：`xt:topic:bar`（统一通道）
- 推送模式：`close_only`（仅收盘推送）
- 幂等键：`(code, period, bar_end_ts)`（在 `close_only` 下，同一根仅推送一次）

## 必备字段（下游只依赖这些）
- `code`：字符串，标的代码，如 `518880.SH`
- `period`：字符串，`1m|1h|1d`
- `bar_end_ts`：**+08:00** 的时间字符串（该 Bar 的结束时刻），例如：`2025-09-11T09:35:00+08:00`
- `is_closed`：布尔，**必须为 `true`**（close_only）
- `open, high, low, close`：数值

> 其余字段（`volume/amount/preClose/suspendFlag/openInterest/settlementPrice/recv_ts/source` 等）为可选透传，不参与时钟与幂等。

## 收盘判定
- 优先依据回调中的 `isClosed=true`（或兼容字段 `isClose/closed`）
- 若缺少字段，以“分钟自然结束 + `close_delay_ms`”或“检测到下一根开出”作为兜底

## 错误与丢弃
- 发布前进行 **Schema Guard**：缺少必备字段、`is_closed!=true`、`bar_end_ts` 非 +08:00 格式等，**丢弃且计数**（不阻断主流程）
- 指标：`schema_drop_total`（丢弃次数）、`bars_published_total`（成功发布次数）、`late_bars_total`（晚到发布计数）

## 示例
```json
{
  "code": "518880.SH",
  "period": "1m",
  "bar_end_ts": "2025-09-11T09:35:00+08:00",
  "is_closed": true,
  "open": 7.73,
  "high": 7.78,
  "low": 7.71,
  "close": 7.75,
  "volume": 3456789,
  "amount": 2674000000,
  "source": "qmt",
  "recv_ts": "2025-09-11T09:35:01.234567+08:00"
}
