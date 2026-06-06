# QMTD FD 新数据库切换说明

## 1. 当前切换状态

QMTD 历史入库链路已经默认切换到新 FD backend。

当前默认链路：

```text
QMTD ingest_runner / ingestor
  -> core.storage_backend
  -> core.fd_storage_adapter
  -> FD 独立项目原型 fd_core.FinancialDataStorage
  -> D:/Work/Quant/financial_database
```

旧 `core.storage_simple.FinancialDataStorage` 暂时保留，只作为回滚 backend。

## 2. 默认配置

默认使用：

```powershell
QMTD_STORAGE_BACKEND=fd
FD_REPO=D:\Work\Quant\PythonProject\FD独立化迁移\FD独立项目原型
FD_DATA_ROOT=D:\Work\Quant\financial_database
```

其中 `FD_REPO` 可以不显式设置，当前代码默认使用上述路径。

## 3. 回滚方式

如果需要回滚旧写入链，重启 QMTD 服务前设置：

```powershell
$env:QMTD_STORAGE_BACKEND="legacy"
```

恢复新 FD backend：

```powershell
Remove-Item Env:\QMTD_STORAGE_BACKEND
```

或显式设置：

```powershell
$env:QMTD_STORAGE_BACKEND="fd"
```

## 4. 验证命令

### 4.1 backend 自检

```powershell
python -c "from core.storage_backend import FinancialDataStorage, get_storage_backend_name; s=FinancialDataStorage('D:/Work/Quant/financial_database'); print(get_storage_backend_name()); print(s.check_runtime() if hasattr(s, 'check_runtime') else 'legacy')"
```

预期：

```text
fd
{'backend': 'fd', ... 'merge_write_dataframe': 'ok'}
```

### 4.2 单元测试

```powershell
python -m unittest tests.test_fd_storage_adapter tests.test_xtdata_source tests.test_xtdata_futures tests.test_ingest_runner
```

### 4.3 小样本真实入库

停机窗口内建议先跑：

```powershell
python -m scripts.xtdata_ingest recent-backfill --symbols 510050.SH --cycles 1d --lookback 2 --root D:/Work/Quant/financial_database
```

随后用 FD dashboard 刷新库存，检查：

- `SS_stock_data/510050.SH/1d/original/510050.SH_1d.csv`
- 行数是否增加或保持合理
- 起止时间是否正确
- 质量检查是否可读

## 5. 注意事项

- QMTD 仍负责 miniQMT / xtdata 下载和 bridge 任务执行。
- FD 不接管 Redis 实时链路。
- 新 FD backend 不支持 FD V1 以外的新增周期语义；如后续启用 `5m / 15m`，需要先在 FD 侧正式扩展周期支持。
- 当前 FD 原型仍位于迁移目录，正式迁出为独立项目后，只应修改 `FD_REPO`，不应修改 QMTD 核心代码。

## 6. 验收标准

本次停机切换至少需要确认：

- `storage_backend` 默认返回 `fd`。
- 历史入库专项测试通过。
- 至少一个 ETF `recent-backfill` 真实任务通过。
- FD dashboard 能扫描和质检 QMTD 写入文件。
- 设置 `QMTD_STORAGE_BACKEND=legacy` 后可以回到旧 `storage_simple`。
