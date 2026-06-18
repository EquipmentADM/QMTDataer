# QMTD fd backend 切换验证记录

## 结论

QMTD 已完成新 FD backend 的第一轮切换实现与正式生产默认切换前验证。

当前默认 backend 仍保持 `legacy`，没有切换为 `fd`。

## 切换范围

本轮只涉及历史数据入库链路：

- `core/fd_storage_adapter.py`
- `core/storage_backend.py`
- `core/ingest_runner.py`
- `core/ingestor.py`
- `core/xtdata_source.py`

不涉及：

- Redis 实时控制链路
- 实时行情推送
- Mock 行情
- bridge 服务进程骨架
- 默认生产 backend 切换

## backend 开关

使用 fd backend：

```powershell
$env:QMTD_STORAGE_BACKEND = "fd"
$env:FD_REPO = "D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型"
$env:FD_DATA_ROOT = "D:\Work\Quant\financial_database"
```

回滚 legacy backend：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
Remove-Item Env:FD_DATA_ROOT -ErrorAction SilentlyContinue
```

若不设置 `QMTD_STORAGE_BACKEND`，默认仍为 `legacy`。

## 时间列契约

FD 侧确认：时间列不以字符串是否包含 `T` 作为契约。

QMTD 侧当前按以下规则验证：

- 时间列优先使用 `time`。
- 时间语义为本地 timezone-naive datetime。
- 写出后必须可被 `pd.to_datetime` 解析。
- 解析后无 `NaT`。
- 时间升序。
- 同一时间点唯一。
- 字符串格式不承诺稳定，允许 `YYYY-MM-DD HH:MM:SS` 与 `YYYY-MM-DDTHH:MM:SS`。

## 已修复问题

验证中发现新 FD 当前 `merge_write_dataframe` 在混合时间字符串格式场景下可能丢新增行：

- 旧文件为 `YYYY-MM-DD HH:MM:SS` 或日期格式。
- 新数据为 `YYYY-MM-DDTHH:MM:SS`。
- pandas 严格格式推断后，部分新行会变为 `NaT`。
- FD merge 丢弃 `NaT` 后导致抓取有新增但落盘无新增。

QMTD 侧修复方式：

- adapter 在写入前自行执行 robust merge。
- 新旧数据时间列统一解析为 timezone-naive datetime64。
- 按时间列去重、排序。
- 最终仍调用 FD `write_dataframe` 完成标准路径、文件名和落盘。

## 单元测试

测试命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe -m unittest tests.test_fd_storage_adapter tests.test_storage_backend tests.test_ingest_runner tests.test_xtdata_source tests.test_xtdata_futures tests.test_xtdata_ingest_scripts -v
```

结果：

```text
Ran 24 tests
OK
```

覆盖重点：

- fd adapter 写入与合并。
- storage backend 默认 legacy 与 fd 开关。
- 时间列可解析、无 NaT、升序、无重复。
- `1min -> 1m` xtdata 取数周期兼容。
- 股票与期货路径映射。

## 生产目录验证

生产目录：

```text
D:\Work\Quant\financial_database
```

### ETF 单标的小样本

命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "510050.SH" --cycles "1d,1m" --lookback 2
```

结果：

- `510050.SH 1d`：补到 `2026-06-09`，行数 `5174`。
- `510050.SH 1min`：补到 `2026-06-09 15:00:00`，行数 `86225`。
- 失败数：`0`。

### 多 ETF 小批量

命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "510050.SH,518880.SH,513880.SH" --cycles "1d,1m" --lookback 2
```

结果：

- 总任务：`6`。
- 有效更新：`4`。
- 未更新：`2`，原因是已有数据已到最新。
- 失败：`0`。

### 期货主力连续

命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "rb00.SF" --cycles "1d,1m" --lookback 2
```

结果：

- `rb00.SF -> Futures_data/rb/主力连续 1d`：补到 `2026-06-09`，行数 `4179`。
- `rb00.SF -> Futures_data/rb/主力连续 1min`：补到 `2026-06-09 21:51:00`，行数 `98010`。
- 失败数：`0`。

## 文件级质量验证

本轮涉及的生产文件均满足：

- `pd.to_datetime` 可解析。
- `NaT=0`。
- 时间重复数为 `0`。
- 时间升序为 `True`。

已验证对象：

- `510050.SH`：`1d`、`1min`
- `518880.SH`：`1d`、`1min`
- `513880.SH`：`1d`、`1min`
- `rb 主力连续`：`1d`、`1min`

## legacy/fd 对比中的 end 边界差异

临时目录同输入对比中发现一个可解释差异：

- 标的：`rb00.SF`
- 周期：`1d`
- start：`20260601`
- end：`20260610`
- legacy 写入 `8` 行，包含 `2026-06-10 00:00:00`。
- fd backend 写入 `7` 行，最后时间为 `2026-06-09 00:00:00`。

原因：

- FD 时间过滤契约按半开区间处理 end 边界。
- 因此 `end=20260610` 时，FD 不写入时间等于 `2026-06-10 00:00:00` 的日线 bar。
- legacy 旧实现保留了该边界日线。

结论：

- 该差异记录为 FD 半开区间契约差异。
- 暂不视为失败。
- 若后续希望 QMTD 的日线行为兼容旧 legacy，应在 QMTD adapter 或任务参数层对日线 end 做 `end+1` 兼容处理。
- 不应为此修改 FD 核心半开区间契约。

## FD dashboard service 验收

验收脚本：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\fd_backend_dashboard_check.py
```

验收结果：

- 库存扫描可见目标文件。
- 文件巡检可读。
- 质量检查 `quality_ok=True`。
- 覆盖 `510050.SH`、`518880.SH`、`513880.SH`、`rb 主力连续`。

## 未切换事项

本轮未执行：

- 默认 backend 切到 `fd`。
- 全量大池生产回填。
- FD dashboard 图形界面人工点击验收。

下一步建议：

1. 提交当前 fd backend 切换成果。
2. 保持默认 `legacy` 运行一段观察。
3. 再讨论是否把默认 backend 切为 `fd`。
## 2026-06-18 默认切换与生产 recent-backfill 验证

### 切换结论

QMTD 已将默认 storage backend 从 `legacy` 切换为 `fd`。

回滚入口保留：

```powershell
$env:QMTD_STORAGE_BACKEND = "legacy"
```

未删除旧实现：

- `core/storage_simple.py` 保留。
- `legacy backend` 构造逻辑保留。
- Redis 实时行情链路未修改。
- Mock 行情链路未修改。

### 日线 end 边界处理

本轮已在 QMTD 请求层处理日线 `end+1` 兼容：

- FD core 继续保持半开区间契约。
- QMTD 仅在 `backend=fd`、周期为日线、且调用方显式传入 `end` 时，把日线请求 end 加 1 天。
- 分钟线不做 end+1。
- `end=""` 场景原本已解析为“当前日期+1天”，不重复追加。

### 生产 recent-backfill

运行环境：

```powershell
$env:FD_REPO = "D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型"
$env:FD_DATA_ROOT = "D:\Work\Quant\financial_database"
Remove-Item Env:QMTD_STORAGE_BACKEND -ErrorAction SilentlyContinue
```

运行命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\xtdata_ingest.py recent-backfill --symbols "510050.SH,510300.SH,510500.SH,159915.SZ,518880.SH,513880.SH,513100.SH,513030.SH,512480.SH,588000.SH,rb00.SF,MA00.ZF,IF00.IF,au00.SF" --cycles "1d,1m" --root "D:\Work\Quant\financial_database" --lookback 2
```

任务结果：

- 标的：10 个 ETF + 4 个期货主连。
- 周期：`1d`、`1m`。
- 总任务：`28`。
- updated：`28`。
- not_updated：`0`。
- failed：`0`。
- storage_backend：`fd`。
- root：`D:\Work\Quant\financial_database`。

### 样本最后时间

- `510050.SH 1d`：`2026-06-18 00:00:00`，行数 `5181`。
- `510050.SH 1min`：`2026-06-18 15:00:00`，行数 `87912`。
- `513880.SH 1d`：`2026-06-18 00:00:00`，行数 `1694`。
- `513880.SH 1min`：`2026-06-18 15:00:00`，行数 `90170`。
- `rb 主力连续 1d`：`2026-06-18 00:00:00`，行数 `4186`。
- `rb 主力连续 1min`：`2026-06-18 15:00:00`，行数 `100387`。
- `au 主力连续 1d`：`2026-06-18 00:00:00`，行数 `4480`。
- `au 主力连续 1min`：`2026-06-18 15:00:00`，行数 `160127`。

### 文件质量检查

抽样覆盖：

- `SS_stock_data/510050.SH/1d/original`
- `SS_stock_data/510050.SH/1min/original`
- `SS_stock_data/513880.SH/1d/original`
- `SS_stock_data/513880.SH/1min/original`
- `Futures_data/rb/1d/主力连续`
- `Futures_data/rb/1min/主力连续`
- `Futures_data/au/1d/主力连续`
- `Futures_data/au/1min/主力连续`

检查结果：

- `time` 可被 `pd.to_datetime` 解析。
- `NaT=0`。
- 重复时间数 `0`。
- 时间升序 `True`。
- `quality_ok=True`。
- `1m` 写入 FD 后实际落盘目录为 `1min`。

### FD DashboardService 验收

运行命令：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe scripts\fd_backend_dashboard_check.py --target SS_stock_data:510050.SH --target SS_stock_data:513880.SH --target Futures_data:rb --target Futures_data:au
```

验收结果：

- 库存可扫描。
- 文件可读。
- `quality_ok=True`。
- 覆盖 ETF 与期货主连。
- 验收脚本返回码 `0`。
