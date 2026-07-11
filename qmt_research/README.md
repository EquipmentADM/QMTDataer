# QMT 连接研究脚本

本目录用于放置 QMT / MiniQMT / xtdata 连接行为的研究脚本，避免把实验代码混入正式实时行情、历史入库和 bridge 服务链路。

当前脚本：

- `probe_xtdata_ports.py`：探测本机候选端口是否存在可用的 xtdata 服务，并用一次轻量历史行情请求验证端口是否真正可用。

运行示例：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe qmt_research\probe_xtdata_ports.py
```

指定端口：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe qmt_research\probe_xtdata_ports.py --ports 58610,58612
```

小范围扫描：

```powershell
C:\Users\EquipmentADM\.conda\envs\QMTDataer\python.exe qmt_research\probe_xtdata_ports.py --range 58600-58630
```

注意事项：

- 本脚本会调用 `xtdata.reconnect(..., remember_if_success=False)`，原则上不持久化修改 xtdata 默认连接记录。
- 建议在 QMTD 实时服务未运行时使用，避免同进程或并发服务状态干扰判断。
- 端口监听成功不等于 xtdata 可用，最终以 `get_market_data_ex` 能否返回有效数据为准。
