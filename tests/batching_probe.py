# -*- coding: utf-8 -*-
"""
batching_probe.py
目的：验证“回调 data 一次带多个标的”的成因。
支持多种模式切换，观察多标的订阅在不同条件下的回调形态。

运行：python batching_probe.py

注意：
- 需要 xtquant 环境可用，并已登录。
- 建议在活跃时段运行更容易复现多标的合并。
"""

import time
import csv
import os
import threading
from typing import Any, Dict, List
from xtquant import xtdata

# ====== 配置区域 ======
CODES: List[str] = ['510050.SH', '159915.SZ', '510330.SH']  # 1~10个标的
PERIOD = "1m"            # 可改为 "tick" 对比
COUNT = -1               # -1 仅后续；1 预热一条
CSV_PATH = "batching_probe_events.csv"

# 模式开关（逐个尝试）
MODE_SEPARATE_CALLBACKS = False    # True：每个标的绑定不同回调函数（验证“共享回调导致聚合”的可能性）
MODE_INDUCE_BLOCKING = False       # True：回调里故意 sleep，观察是否更易批量（验证“阻塞导致队列合并”）
SLEEP_MS_IN_CALLBACK = 20          # 回调里 sleep 的毫秒数（仅当 MODE_INDUCE_BLOCKING=True 时生效）
USE_MULTIPROCESS = False           # True：多进程模式，每个进程订阅一个标的（验证“服务端/SDK层批处理”）
# =====================

# ====== 工具：记录事件到CSV ======
def _init_csv(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "ts", "hhmmss", "thread_id", "n_codes", "codes", "period", "mode",
                "delta_ms_since_last", "same_millis"
            ])
            writer.writeheader()

def _append_csv(path: str, row: Dict[str, Any]):
    with open(path, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=[
            "ts", "hhmmss", "thread_id", "n_codes", "codes", "period", "mode",
            "delta_ms_since_last", "same_millis"
        ]).writerow(row)

_last_ts_lock = threading.Lock()
_last_ts = 0.0

def _mark_and_get_delta_ms(now: float) -> float:
    global _last_ts
    with _last_ts_lock:
        prev = _last_ts
        _last_ts = now
    return (now - prev) * 1000 if prev > 0 else -1.0

# ====== 回调模板 ======
def generic_callback(datas: Dict[str, Any], tag: str):
    """
    通用回调：打印一次回调收到了多少代码、哪些代码、与上次事件的间隔等。
    :param datas: 回调 push 原始数据（字典：{code: [kline_dict] 或 {..}}）
    :param tag: 回调标识（用于区分不同回调实例）
    """
    t = time.time()
    hhmmss = time.strftime("%H:%M:%S", time.localtime(t))
    thread_id = threading.get_ident()

    # 提取本次回调携带的代码集
    codes = [k for k in datas.keys() if isinstance(k, str) and (".SH" in k or ".SZ" in k)]
    n_codes = len(codes)

    # 判断“同毫秒”（多个标的 time 字段是否完全相同）
    same_millis = None
    times = set()
    for code, v in datas.items():
        if isinstance(v, list) and v:
            x = v[-1]
            if isinstance(x, dict) and "time" in x:
                times.add(int(x["time"]))
        elif isinstance(v, dict) and "time" in v:
            times.add(int(v["time"]))
    if len(times) > 0:
        same_millis = (len(times) == 1)

    delta_ms = _mark_and_get_delta_ms(t)

    mode_desc = f"sep_cb={MODE_SEPARATE_CALLBACKS},block={MODE_INDUCE_BLOCKING},mp={USE_MULTIPROCESS}"

    print(f"[{hhmmss}] tag={tag} tid={thread_id} n_codes={n_codes} codes={codes} "
          f"same_millis={same_millis} Δ={delta_ms:.1f}ms")

    _append_csv(CSV_PATH, {
        "ts": f"{t:.3f}",
        "hhmmss": hhmmss,
        "thread_id": thread_id,
        "n_codes": n_codes,
        "codes": ",".join(codes),
        "period": PERIOD,
        "mode": mode_desc,
        "delta_ms_since_last": f"{delta_ms:.1f}",
        "same_millis": same_millis,
    })

    # 故意增加阻塞，用于实验④
    if MODE_INDUCE_BLOCKING and SLEEP_MS_IN_CALLBACK > 0:
        time.sleep(SLEEP_MS_IN_CALLBACK / 1000.0)

# ====== 单进程实验 ======
def experiment_single_process():
    _init_csv(CSV_PATH)

    if MODE_SEPARATE_CALLBACKS:
        # 每个标的绑定不同的回调函数（验证“共享回调是否导致聚合”）
        for i, code in enumerate(CODES):
            def mk_cb(idx, c):
                def _cb(datas):
                    generic_callback(datas, tag=f"cb{idx}:{c}")
                return _cb
            xtdata.subscribe_quote(code, period=PERIOD, count=COUNT, callback=mk_cb(i, code))
    else:
        # 所有标的共享同一个回调（常规用法）
        def _cb(datas):
            generic_callback(datas, tag="shared")
        for code in CODES:
            xtdata.subscribe_quote(code, period=PERIOD, count=COUNT, callback=_cb)

    print(f"[ready] single-process, period={PERIOD}, codes={CODES}, "
          f"sep_cb={MODE_SEPARATE_CALLBACKS}, block={MODE_INDUCE_BLOCKING}")
    print(f"[hint] CSV: {os.path.abspath(CSV_PATH)}")
    xtdata.run()

# ====== 多进程实验（可选） ======
def experiment_multiprocess():
    """
    多进程：每个进程只订阅一个标的，各自独立回调。
    若仍能观察到“同毫秒”更新/强同步现象，更倾向于服务端/通道层行为（推测③）。
    """
    import multiprocessing as mp

    def worker(code: str):
        _init_csv(f"{code}_{CSV_PATH}")
        def _cb(datas):
            generic_callback(datas, tag=f"proc:{code}")
        xtdata.subscribe_quote(code, period=PERIOD, count=COUNT, callback=_cb)
        print(f"[ready] process for {code}, period={PERIOD}")
        xtdata.run()

    procs = []
    for code in CODES:
        p = mp.Process(target=worker, args=(code,), daemon=False)
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

# ====== 主入口 ======
if __name__ == "__main__":
    try:
        if USE_MULTIPROCESS:
            experiment_multiprocess()
        else:
            experiment_single_process()
    except KeyboardInterrupt:
        print("\n[exit] 用户中断")
