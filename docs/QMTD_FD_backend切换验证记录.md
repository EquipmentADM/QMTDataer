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
