# QMTD fd backend 默认切换方案

## 2026-06-18 执行状态

本方案已执行：

- `core/storage_backend.py` 默认 backend 已从 `legacy` 切换为 `fd`。
- 显式设置 `QMTD_STORAGE_BACKEND=legacy` 仍可回滚。
- 生产目录 `D:\Work\Quant\financial_database` 已完成 ETF + 期货主连 recent-backfill 验证。
- `1m` 已确认落盘为 FD 标准目录 `1min`。
- FD DashboardService 验收通过。

后续如需回滚，执行：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
```

## 当前状态

当前 QMTD 已具备 fd backend，但默认 backend 仍为 `legacy`。

本文件只描述默认切换方案，不执行切换。

## 默认值切换涉及文件

正式切换时，最小改动集中在：

- `core/storage_backend.py`

当前默认逻辑：

```python
backend_use = (backend or os.environ.get("QMTD_STORAGE_BACKEND") or "legacy").strip().lower()
```

切换为 fd 默认时，候选改法：

```python
backend_use = (backend or os.environ.get("QMTD_STORAGE_BACKEND") or "fd").strip().lower()
```

同步需要更新：

- `tests/test_storage_backend.py`
- `docs/QMTD_FD_backend切换验证记录.md`
- `docs/usage_guide.md` 或后续统一运行说明文档

不应修改：

- `core/storage_simple.py`
- Redis 实时控制相关文件
- Mock 行情相关文件
- bridge 服务进程骨架

## 切换前置条件

正式默认切换前应满足：

- `FD_REPO` 在运行环境中稳定可用。
- `FD_DATA_ROOT` 指向 `D:\Work\Quant\financial_database`。
- fd backend recent-backfill 小批量验证通过。
- FD DashboardService 库存、巡检、质量检查通过。
- legacy 回滚命令已验证。

## 推荐切换方式

### 阶段 1：环境变量启用

不改代码默认值，仅在运行环境中启用 fd：

```powershell
$env:QMTD_STORAGE_BACKEND = "fd"
$env:FD_REPO = "D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型"
$env:FD_DATA_ROOT = "D:\Work\Quant\financial_database"
```

适用场景：

- 灰度运行。
- 手工验证。
- 发现问题后快速回滚。

### 阶段 2：代码默认切换

在阶段 1 稳定后，再把 `core/storage_backend.py` 默认值从 `legacy` 改为 `fd`。

此时仍允许显式回滚：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
```

## 回滚命令

临时回滚到 legacy：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
Remove-Item Env:FD_DATA_ROOT -ErrorAction SilentlyContinue
```

如果已做代码默认切换，也可以通过环境变量强制 legacy：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
```

## 切换后第一轮验收清单

### 1. recent-backfill 运行

ETF 小批量：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "510050.SH,510300.SH,510500.SH,159915.SZ,518880.SH,513880.SH,513100.SH,513030.SH,512480.SH,588000.SH" --cycles "1d,1m" --lookback 2
```

期货主连：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "rb00.SF,MA00.ZF,IF00.IF" --cycles "1d,1m" --lookback 2
```

### 2. 任务统计

记录：

- 总任务数。
- `updated`。
- `not_updated`。
- `failed`。
- `storage_backend` 是否为 `fd`。
- `root` 是否为 `D:\Work\Quant\financial_database`。

验收标准：

- `failed=0`。
- `not_updated` 需要可解释，通常是已有最大时间不早于抓取最大时间。

### 3. 文件级检查

对写入文件检查：

- `1m` 落盘目录必须为 `1min`。
- `pd.to_datetime(time)` 可解析。
- `NaT=0`。
- 时间重复数为 `0`。
- 时间升序为 `True`。
- 合并后末尾时间不倒退。

### 4. DashboardService 验收

运行：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\fd_backend_dashboard_check.py
```

验收标准：

- 库存可扫描。
- 文件可读。
- `quality_ok=True`。
- 覆盖 ETF 与期货主连。

## 日线 end 兼容处理评估

当前观察到：

- legacy 对 `rb00.SF 1d` 的 `end=20260610` 会保留 `2026-06-10 00:00:00`。
- fd backend 按 FD 半开区间契约过滤，最后到 `2026-06-09 00:00:00`。

该差异暂不视为失败。

是否需要做兼容处理：

- 若 QMTD 业务希望日线 end 行为与 legacy 保持一致，可对日线任务的 `end` 做 `end+1`。
- 若 QMTD 接受 FD 半开区间契约，则不处理。

建议：

- 默认切换前暂不做日线 `end+1`。
- 继续记录真实任务中的日线 end 差异。
- 若下游明确要求闭区间行为，再在 QMTD 层处理。

处理位置：

- 优先放在 QMTD 任务参数层，例如 `ingest_runner` 计算 `end_use` 时对日线 fd backend 做兼容。
- 或放在 QMTD adapter 前置过滤参数处理。
- 不修改 FD 核心半开区间契约。

不建议：

- 不在 FD 核心里为 QMTD 特判。
- 不在 xtdata_source 中处理落盘 end 语义。

## 不执行事项

默认切换前不执行：

- 不删除 `legacy backend`。
- 不删除 `storage_simple.py`。
- 不改变 Redis 实时行情链路。
- 不改变 Mock 行情链路。
- 不把 fd backend 设为默认，除非完成正式切换审批。
