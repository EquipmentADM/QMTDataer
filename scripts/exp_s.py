# -*- coding: utf-8 -*-
"""
xt_quote_inspector.py
用途：对比 xtdata 回调推送的数据 与 回调内同步拉取的 get_market_data_ex 的数据，
     逐次、逐字段打印差异，并记录到 CSV，便于你研究推送机制、时序与字段变化规律。

运行方式：
    python xt_quote_inspector.py

依赖：
    - 你的运行环境已正确安装并登录 xtdata
    - Python 3.8+
"""

import csv
import json
import os
import sys
import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple, List

import pandas as pd

# ========== 你的订阅配置 ==========
STOCK_LIST = ['510050.SH', '159915.SZ', '510330.SH']   # 多标的对比是否有“时差”
PERIOD = "1m"
CSV_PATH = "quote_events.csv"             # 输出事件流水，Excel/CSV都能打开
PRINT_COMPACT = True                      # True：单行紧凑；False：多行更详细
MAX_PRETTY_VAL_LEN = 200                  # 控制打印值的最大长度，避免控制台刷屏
# ==================================

# ---- 这里假定已安装 xtdata 并可导入；如你需在别处改成 from xtquant import xtdata 请自行调整 ----
from xtquant import xtdata


# ========== 工具函数（可单测） ==========
def dict_diff(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, tuple]:
    """
    功能：对比两个“已规范化为标量值”的字典，输出变化键 -> (old, new)。
    约定：传入前务必用 normalize_* 把 DataFrame/Series/列表等转成标量。
    """
    changed = {}
    old = old or {}
    new = new or {}
    for k in set(old.keys()) | set(new.keys()):
        ov = old.get(k, None)
        nv = new.get(k, None)
        if ov != nv:  # 现在均为标量/可比较对象，不会触发 pandas 对齐
            changed[k] = (ov, nv)
    return changed



def _shorten(v: Any, max_len: int = MAX_PRETTY_VAL_LEN) -> str:
    """控制打印长度，便于阅读控制台输出。"""
    try:
        s = json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        s = str(v)
    if len(s) > max_len:
        return s[:max_len] + f"...(+{len(s)-max_len} chars)"
    return s

def normalize_pull_payload(pull_data: Any) -> Dict[str, Any]:
    """
    将 get_market_data_ex 返回的 { code: DataFrame } 规范化为：{ "代码.字段": 标量 }
    - 取每个 DataFrame 的最后一行 .iloc[-1] → dict
    - 非 DataFrame 情况尽量字符串化保底
    """
    flat = {}
    if isinstance(pull_data, dict):
        for code, v in pull_data.items():
            if isinstance(v, pd.DataFrame) and not v.empty:
                row = v.iloc[-1].to_dict()
                for kk, vv in row.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            elif isinstance(v, pd.Series):
                row = v.to_dict()
                for kk, vv in row.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            elif isinstance(v, dict):
                for kk, vv in v.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            else:
                flat[f"{code}.value"] = _to_scalar(v)
    else:
        flat["raw"] = json.dumps(pull_data, ensure_ascii=False, default=str)
    return flat


def _to_scalar(v: Any) -> Any:
    """
    尽量把 numpy/pandas 标量/时间戳转成 Python 标量，避免后续比较触发矛盾。
    """
    try:
        # pandas/NumPy 数值/时间戳
        if hasattr(v, "item"):
            return v.item()
    except Exception:
        pass
    # pandas.Timestamp -> int 毫秒 或 iso 字符串（任选一种，你已有毫秒字段，这里保持原值）
    return v


def normalize_push_payload(push_data: Any) -> Dict[str, Any]:
    """
    将回调 push 的数据规范化为：{ "代码.字段": 标量 }。
    你的回调 data 形如：{ "159915.SZ": [ {time, open, ...} ] }
    - 若列表非空，取最后一个元素（通常只有一条）
    - 若是嵌套/异常结构，尽量字符串化保底
    """
    flat = {}
    if isinstance(push_data, dict):
        for code, v in push_data.items():
            # 典型：v 是 [ {kline字段...} ]
            if isinstance(v, list) and v:
                last = v[-1]
                if isinstance(last, dict):
                    for kk, vv in last.items():
                        flat[f"{code}.{kk}"] = _to_scalar(vv)
                else:
                    flat[f"{code}.value"] = _to_scalar(last)
            elif isinstance(v, dict):
                for kk, vv in v.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            else:
                flat[f"{code}.value"] = _to_scalar(v)
    else:
        # 无法识别结构时，存一份字符串化的原样
        flat["raw"] = json.dumps(push_data, ensure_ascii=False, default=str)
    return flat


def try_extract_symbol(payload: Any) -> Optional[str]:
    """
    功能：从回调的 data 中“尽力”探测标的代码字段名。
    可能的键名：'code'/'stock_code'/'security_id'/'symbol' 等；
    如果 data 是列表或嵌套 dict，会尝试向里探测第一项。
    上游：回调入参 data
    下游：用于建立“每标的”的状态表与分桶统计
    """
    candidates = ['code', 'stock_code', 'security_id', 'symbol', 'sec_code']
    def _probe(d):
        if not isinstance(d, dict):
            return None
        for k in candidates:
            if k in d and isinstance(d[k], str):
                return d[k]
        # 有些结构像 {'510050.SH': {...}, '159915.SZ': {...}}，则取第一层键名
        # 仅在键名看起来像代码时启用
        for k in d.keys():
            if isinstance(k, str) and ('.SH' in k or '.SZ' in k):
                return k
        return None

    if isinstance(payload, dict):
        sym = _probe(payload)
        if sym:
            return sym
        # 若是 { 'data': {...} } 这种
        for v in payload.values():
            sym = try_extract_symbol(v)
            if sym:
                return sym
    elif isinstance(payload, list) and payload:
        return try_extract_symbol(payload[0])
    return None


def extract_flat_snapshot_from_get_market(data_ex: Any) -> Dict[str, Any]:
    """
    功能：将 xtdata.get_market_data_ex 返回的结构转成扁平 dict，便于直接比较/打印。
    注意：不同版本/行情源返回结构可能不同，这里做“尽力扁平化”，你可按实际调整。
    上游：回调内 get_market_data_ex 的原始返回
    下游：dict_diff / 打印
    """
    # 典型结构可能类似：{ '510050.SH': {'time':xxx, 'open':..., ...}, '159915.SZ': {...}}
    flat: Dict[str, Any] = {}
    if isinstance(data_ex, dict):
        for k, v in data_ex.items():
            # 若 v 就是字典，直接并入（加前缀避免冲突）
            if isinstance(v, dict):
                for kk, vv in v.items():
                    flat[f"{k}.{kk}"] = vv
            else:
                flat[str(k)] = v
    else:
        # 不认识的结构，直接字符串化
        flat['raw'] = data_ex
    return flat
# ======================================


@dataclass
class LastState:
    """维护某“标的@周期”的上一次观测状态"""
    last_push: Dict[str, Any] = field(default_factory=dict)   # 回调侧：上次推送扁平化后的值
    last_pull: Dict[str, Any] = field(default_factory=dict)   # 拉取侧：上次 get_market_data_ex 的扁平值
    last_event_ts: float = 0.0                                # 上次事件时间（回调触发点，epoch 秒）
    seq: int = 0                                              # 事件序号（用于可视化排序）


class CallbackInspector:
    """
    功能：订阅多标的报价，捕捉每次回调，结构化打印：
        - 回调 data 的关键信息（尽量提取 symbol）
        - 回调里同步拉取 get_market_data_ex 的扁平快照
        - 计算 push(回调) 与 pull(拉取) 的字段级差异
        - 记录时间戳、线程ID、事件序号、延迟（回调->拉取）等
        - 写入 CSV 便于后续深度分析（Excel/Pandas）

    上游：xtdata.subscribe_quote 的回调数据
    下游：控制台与 CSV
    """

    def __init__(self, stock_list: List[str], period: str, csv_path: str):
        self.stock_list = stock_list
        self.period = period
        self.csv_path = csv_path
        self.lock = threading.Lock()
        # 以 (symbol, period) 维度维护“上一次状态”
        self.states: Dict[Tuple[str, str], LastState] = defaultdict(LastState)
        self._init_csv()

    def _init_csv(self):
        # CSV 字段
        self.csv_fields = [
            'event_time', 'event_time_readable', 'thread_id', 'seq',
            'symbol', 'period', 'pull_latency_ms',
            'push_keys', 'pull_keys', 'push_pull_diff_keys',
            'push_brief', 'pull_brief'
        ]
        if (not os.path.exists(self.csv_path)):
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.csv_fields)
                writer.writeheader()

    def _write_csv_row(self, row: Dict[str, Any]):
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_fields)
            writer.writerow(row)

    def _flatten_push_payload(self, data: Any) -> Dict[str, Any]:
        """
        将回调 data 尽力扁平化为 dict，便于比较/打印。
        这里遵循“少做假设，多做字符串化”的原则，避免因结构差异报错。
        """
        if isinstance(data, dict):
            # 浅层展开一层
            flat = {}
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    flat[k] = v  # 保留原型（打印时会截断），不递归展开避免过深
                else:
                    flat[k] = v
            return flat
        elif isinstance(data, list):
            # 只放入长度与第一项，避免太长
            return {"list_len": len(data), "first_item": data[0] if data else None}
        else:
            return {"value": data}

    def _pretty_print_event(self,
                            symbol: str,
                            push_flat: Dict[str, Any],
                            pull_flat: Dict[str, Any],
                            latency_ms: float,
                            seq: int):
        """控制台打印（紧凑 / 多行）"""
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        header = f"[{now}] seq={seq} symbol={symbol} period={self.period} latency={latency_ms:.1f}ms"
        if PRINT_COMPACT:
            # 紧凑单行，关键信息先行
            diff = dict_diff(push_flat, pull_flat)
            line = (
                header
                + " | push_keys="
                + ",".join(sorted(push_flat.keys()))
                + " | pull_keys="
                + ",".join(sorted(pull_flat.keys()))
                + " | diff_keys="
                + ",".join(sorted(diff.keys()))
                + " | push="
                + _shorten(push_flat)
                + " | pull="
                + _shorten(pull_flat)
            )
            print(line)
        else:
            print("=" * 100)
            print(header)
            print("  [PUSH] 回调数据（扁平）：", _shorten(push_flat))
            print("  [PULL] 拉取数据（扁平）：", _shorten(pull_flat))
            diff = dict_diff(push_flat, pull_flat)
            if diff:
                print("  [DIFF] 字段变化：")
                for k, (ov, nv) in diff.items():
                    print(f"    - {k}: {_shorten(ov)} -> {_shorten(nv)}")
            else:
                print("  [DIFF] 无字段变化")
            print("=" * 100)

    # ----------------- 回调入口 -----------------
    def on_quote(self, data: Any):
        """
        xtdata.subscribe_quote 的回调函数。
        关键步骤：
          1) 记录回调触发时间 t_push
          2) 尝试识别 symbol（标的），若识别不到则标记为 'UNKNOWN'
          3) 在回调内同步拉取 get_market_data_ex，记录 t_pull，并计算 latency
          4) 扁平化 push/pull，计算 diff
          5) 控制台打印 + 写入 CSV
          6) 更新 last_state
        """
        t_push = time.time()
        thread_id = threading.get_ident()

        # 1) 尽量识别 symbol
        sym = try_extract_symbol(data)
        if not sym:
            # 如果回调里没有明显的 symbol 字段，就标记为未知；同时尽量从订阅列表猜测
            sym = 'UNKNOWN'

        key = (sym, self.period)
        with self.lock:
            st = self.states[key]
            st.seq += 1
            seq = st.seq

        # 2) 在回调内做一次拉取
        t_before_pull = time.time()
        pulled = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=STOCK_LIST,
            period=self.period,
            count=1
        )
        t_pull = time.time()
        latency_ms = (t_pull - t_push) * 1000.0

        # 3) 扁平化
        push_flat = normalize_push_payload(data)
        pull_flat_all = normalize_pull_payload(pulled)

        # 若能确定 sym，在拉取结果中优先筛选与该 symbol 相关的键，避免“看不清重点”
        if sym != 'UNKNOWN':
            # 只保留以 "symbol." 开头的键
            prefix = f"{sym}."
            pull_flat = {k: v for k, v in pull_flat_all.items() if k.startswith(prefix)}
            if not pull_flat:
                # 如果没筛到，退回全量，避免信息缺失
                pull_flat = pull_flat_all
        else:
            pull_flat = pull_flat_all

        # 4) 打印
        self._pretty_print_event(sym, push_flat, pull_flat, latency_ms, seq)

        # 5) 写 CSV
        row = {
            'event_time': f"{t_push:.3f}",
            'event_time_readable': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t_push)),
            'thread_id': thread_id,
            'seq': seq,
            'symbol': sym,
            'period': self.period,
            'pull_latency_ms': f"{latency_ms:.1f}",
            'push_keys': " ".join(sorted(push_flat.keys())),
            'pull_keys': " ".join(sorted(pull_flat.keys())),
            'push_pull_diff_keys': " ".join(sorted(dict_diff(push_flat, pull_flat).keys())),
            'push_brief': _shorten(push_flat),
            'pull_brief': _shorten(pull_flat),
        }
        self._write_csv_row(row)

        # 6) 更新 last_state（如需做“与上一次 push/pull 的变化”，可在此处对比 st.last_push / st.last_pull）
        with self.lock:
            st.last_push = push_flat
            st.last_pull = pull_flat
            st.last_event_ts = t_push


# ========== 主流程 ==========
def main():
    inspector = CallbackInspector(STOCK_LIST, PERIOD, CSV_PATH)

    def _cb(data):
        # 包一层，避免 xtdata 直接绑定实例方法在某些版本里出问题
        inspector.on_quote(data)

    # 同时订阅多个标的，便于观察“是否存在时差”
    rid = xtdata.subscribe_quote(STOCK_LIST[0], period=PERIOD, callback=_cb)
    rid2 = xtdata.subscribe_quote(STOCK_LIST[1], period=PERIOD, callback=_cb)

    time.sleep(1.0)
    print(f"[ready] subscribed rid={rid}, rid2={rid2}, period={PERIOD}, stocks={STOCK_LIST}")
    print(f"[hint] 事件流水将写入 CSV：{os.path.abspath(CSV_PATH)}")
    print(f"[hint] Ctrl+C 可退出。")

    xtdata.run()  # 进入事件循环


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[exit] 用户中断，已退出。")
