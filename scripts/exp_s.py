# -*- coding: utf-8 -*-
"""
xt_quote_inspector.py（翻新版）
功能概述：
  - 对比 xtdata 回调推送（push） 与 回调内同步拉取 get_market_data_ex（pull）的字段，按“每标的”逐条打印与落CSV；
  - 支持 1~10 个标的同时订阅；量化回调→拉取的毫秒延迟；
  - 字段统一与白名单比较，避免接口层噪声（如拼写差异、非核心字段）干扰判断；
  - 规范化 DataFrame/Series/列表为“代码.字段 -> 标量”，彻底规避 pandas 对齐/布尔歧义错误。

类/方法说明（简述上下游）：
  - class QuoteInspector：负责订阅、回调处理、打印与CSV记录
    - start(): 对 codes 启动 subscribe_quote（上游：xtdata.subscribe_quote；下游：回调 on_quote）
    - run_loop(): 进入事件循环（上游：业务入口；下游：xtdata.run）
    - on_quote(datas): 回调入口；对每个标的做 push/pull 规范化、比较与记录
  - normalize_push_payload / normalize_pull_payload：输入为 push/pull 原始结构（上游：xtdata），
    输出扁平标量字典（下游：diff/打印/CSV）
  - dict_diff_common: 仅比较两侧共同键的差异，减少“某侧缺字段”的噪声
  - canonize_fields: 字段重命名与白名单过滤，输出“更干净”的可比键

使用：
  python xt_quote_inspector.py
依赖：
  - pandas
  - xtquant.xtdata（你的环境需已登录可用）
"""

import csv
import json
import os
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from xtquant import xtdata  # 若你的环境是 from xtdata import xtdata，请自行调整

# ======== 配置 ========
STOCK_LIST: List[str] = ['510050.SH', '159915.SZ', '510330.SH']  # 支持1~10个，超出将报错
PERIOD: str = "1m"
COUNT: int = -1                         # -1：只接收后续；1：预热一条
CSV_PATH: str = "quote_events.csv"
PRINT_COMPACT: bool = True              # True: 单行紧凑；False: 多行详细
MAX_PRETTY_VAL_LEN: int = 200
CORE_WHITELIST = {"time", "open", "high", "low", "close", "volume", "amount", "openInterest"}
CANON_RENAME = {
    # 统一拼写错误：拉取端常见拼写 "settelementPrice" -> "settlementPrice"
    "settelementPrice": "settlementPrice",
}
# =====================


# ================= 工具函数（可单测） =================
def _shorten(v: Any, max_len: int = MAX_PRETTY_VAL_LEN) -> str:
    """控制打印长度，避免刷屏。"""
    try:
        s = json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        s = str(v)
    return s if len(s) <= max_len else s[:max_len] + f"...(+{len(s)-max_len} chars)"


def _to_scalar(v: Any) -> Any:
    """尽量把 numpy/pandas 标量/时间戳转 Python 标量，避免 pandas 布尔歧义。"""
    try:
        if hasattr(v, "item"):
            return v.item()
    except Exception:
        pass
    return v


def normalize_push_payload(push_data: Any) -> Dict[str, Any]:
    """
    将回调 push 的数据规范化为 { "代码.字段": 标量 }。
    典型结构：{ "159915.SZ": [ {time, open, ...} ] } 或 { "510050.SH": {...} }
    - 若值为列表，取最后一个元素（一般只有一条）；若为字典，逐项展平。
    - 无法识别结构时，保底字符串化。
    上游：xtdata.subscribe_quote 回调 data
    下游：字段对比/打印/CSV
    """
    flat: Dict[str, Any] = {}
    if isinstance(push_data, dict):
        for code, v in push_data.items():
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
        flat["raw"] = json.dumps(push_data, ensure_ascii=False, default=str)
    return flat


def normalize_pull_payload(pull_data: Any) -> Dict[str, Any]:
    """
    将 get_market_data_ex 返回的 { code: DataFrame/Series/dict } 规范化为 { "代码.字段": 标量 }
    - DataFrame：取最后一行 .iloc[-1].to_dict() 展平；
    - Series：to_dict()；
    - dict：逐项展平。
    上游：xtdata.get_market_data_ex
    下游：字段对比/打印/CSV
    """
    flat: Dict[str, Any] = {}
    if isinstance(pull_data, dict):
        for code, v in pull_data.items():
            if isinstance(v, pd.DataFrame) and not v.empty:
                row = v.iloc[-1].to_dict()
                for kk, vv in row.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            elif isinstance(v, pd.Series):
                for kk, vv in v.to_dict().items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            elif isinstance(v, dict):
                for kk, vv in v.items():
                    flat[f"{code}.{kk}"] = _to_scalar(vv)
            else:
                flat[f"{code}.value"] = _to_scalar(v)
    else:
        flat["raw"] = json.dumps(pull_data, ensure_ascii=False, default=str)
    return flat


def canonize_fields(flat: Dict[str, Any],
                    whitelist: Optional[set] = CORE_WHITELIST,
                    rename: Optional[Dict[str, str]] = CANON_RENAME) -> Dict[str, Any]:
    """
    字段统一与白名单过滤：
      - 将末级字段名按 rename 更名（如 settelementPrice -> settlementPrice）；
      - 若提供 whitelist，仅保留白名单字段（如核心价量），降低噪声。
    上游：normalize_* 的输出
    下游：dict_diff_common
    """
    out: Dict[str, Any] = {}
    for k, v in flat.items():
        # 取“末级字段”名
        parts = k.split(".")
        if len(parts) >= 2:
            base = parts[-1]
            code = ".".join(parts[:-1])
        else:
            base = parts[-1]
            code = ""

        # 重命名
        if base in rename:
            base = rename[base]
        new_key = f"{code}.{base}" if code else base

        # 白名单过滤
        if whitelist is None or base in whitelist:
            out[new_key] = v
    return out


def dict_diff_common(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Tuple[Any, Any]]:
    """
    仅比较两侧“共同键”的差异，减少某侧缺字段导致的噪声。
    上游：canonize_fields 的结果
    下游：打印与CSV的 diff_keys
    """
    diff: Dict[str, Tuple[Any, Any]] = {}
    common = set(a.keys()) & set(b.keys())
    for k in sorted(common):
        if a[k] != b[k]:
            diff[k] = (a[k], b[k])
    return diff


def extract_codes_from_push(push_data: Any) -> List[str]:
    """
    从回调 push 的原始数据提取本次触发的代码列表。
    典型结构为 dict 的顶层键即代码；无法识别则返回空列表。
    """
    if isinstance(push_data, dict):
        return [k for k in push_data.keys() if isinstance(k, str) and (".SH" in k or ".SZ" in k)]
    return []
# =====================================================


@dataclass
class LastState:
    """维护某“标的@周期”的上一次观测状态（便于后续扩展做环比等）。"""
    last_push: Dict[str, Any] = field(default_factory=dict)
    last_pull: Dict[str, Any] = field(default_factory=dict)
    last_event_ts: float = 0.0
    seq: int = 0


class QuoteInspector:
    """
    订阅-回调观测器
    作用：对多个标的的“回调推送 vs 拉取快照”进行逐条对齐、打印与CSV记录。
    上游：业务入口（提供 codes/period/count）→ start() / run_loop()
    下游：控制台与 CSV 输出，用于你研究：分钟内滚动、跨标的时差、延迟、字段一致性等。
    """

    def __init__(self, codes: List[str], period: str, count: int, csv_path: str):
        """
        :param codes: 标的列表（1~10）
        :param period: 订阅周期（如 '1m'）
        :param count: 订阅 count 语义（-1 仅后续；1 预热一条）
        :param csv_path: CSV 路径
        """
        if not (1 <= len(codes) <= 10):
            raise ValueError("本工具支持订阅 1~10 个标的，请调整 STOCK_LIST。")
        self.codes = codes
        self.period = period
        self.count = count
        self.csv_path = csv_path
        self._lock = threading.Lock()
        # 按 (symbol, period) 维护独立序号，便于阅读
        self._states: Dict[Tuple[str, str], LastState] = {}

        self._init_csv()

    def _init_csv(self):
        self._csv_fields = [
            "event_time", "event_time_readable", "thread_id", "seq",
            "symbol", "period", "latency_ms",
            "push_keys", "pull_keys", "diff_keys",
            "push_brief", "pull_brief"
        ]
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=self._csv_fields).writeheader()

    def _csv_row(self, row: Dict[str, Any]):
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=self._csv_fields).writerow(row)

    def _pretty_print(self, symbol: str, push_flat: Dict[str, Any],
                      pull_flat: Dict[str, Any], latency_ms: float, seq: int):
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        head = f"[{now}] seq={seq} symbol={symbol} period={self.period} latency={latency_ms:.1f}ms"

        # 共同键差异
        diff = dict_diff_common(push_flat, pull_flat)
        push_keys = ",".join(sorted(push_flat.keys()))
        pull_keys = ",".join(sorted(pull_flat.keys()))
        diff_keys = ",".join(sorted(diff.keys()))

        if PRINT_COMPACT:
            line = (head
                    + f" | push_keys={push_keys} | pull_keys={pull_keys} | diff_keys={diff_keys}"
                    + f" | push={_shorten(push_flat)} | pull={_shorten(pull_flat)}")
            print(line)
        else:
            print("=" * 100)
            print(head)
            print("  [PUSH] ", _shorten(push_flat))
            print("  [PULL] ", _shorten(pull_flat))
            if diff:
                print("  [DIFF]")
                for k, (ov, nv) in diff.items():
                    print(f"    - {k}: {ov} -> {nv}")
            else:
                print("  [DIFF] 无")
            print("=" * 100)

        # CSV
        self._csv_row({
            "event_time": f"{time.time():.3f}",
            "event_time_readable": now,
            "thread_id": threading.get_ident(),
            "seq": seq,
            "symbol": symbol,
            "period": self.period,
            "latency_ms": f"{latency_ms:.1f}",
            "push_keys": " ".join(sorted(push_flat.keys())),
            "pull_keys": " ".join(sorted(pull_flat.keys())),
            "diff_keys": " ".join(sorted(diff.keys())),
            "push_brief": _shorten(push_flat),
            "pull_brief": _shorten(pull_flat),
        })

    # ---------------- 回调入口 ----------------
    def on_quote(self, datas: Any):
        """
        订阅回调：一次触发可能包含 1~N 个代码（取决于源端聚合方式），
        本函数会对其中每个代码单独做 push/pull 对齐与输出。
        """
        t_push = time.time()
        codes = extract_codes_from_push(datas)
        if not codes:
            # 无法识别，则按全量拉取但不分代码打印
            codes = []

        # 在回调内拉取一次快照（包含订阅列表全部），方便从中切片出当前代码
        pulled_raw = xtdata.get_market_data_ex(field_list=[],
                                               stock_list=self.codes,
                                               period=self.period,
                                               count=1)
        t_pull = time.time()
        latency_ms = (t_pull - t_push) * 1000.0

        # 统一规范化 + 字段清洗
        push_all = canonize_fields(normalize_push_payload(datas))
        pull_all = canonize_fields(normalize_pull_payload(pulled_raw))

        # 若 codes 非空：分别对每个代码打印一行；否则按“UNKNOWN”打印一次
        target_codes = codes if codes else ["UNKNOWN"]

        for sym in target_codes:
            if sym == "UNKNOWN":
                push_flat = push_all
                pull_flat = pull_all
            else:
                pref = f"{sym}."
                push_flat = {k: v for k, v in push_all.items() if k.startswith(pref)}
                pull_flat = {k: v for k, v in pull_all.items() if k.startswith(pref)}
                # 如果该代码在 push/pull 不存在（极少数异常），退回全量，避免丢失观测
                if not push_flat:
                    push_flat = push_all
                if not pull_flat:
                    pull_flat = pull_all

            # 独立维护每个代码的 seq
            key = (sym, self.period)
            with self._lock:
                st = self._states.setdefault(key, LastState())
                st.seq += 1
                seq = st.seq
                st.last_push = push_flat
                st.last_pull = pull_flat
                st.last_event_ts = t_push

            self._pretty_print(sym, push_flat, pull_flat, latency_ms, seq)

    # ---------------- 生命周期 ----------------
    def start(self):
        """发起对 self.codes 的订阅；随后调用 run_loop() 进入事件循环。"""
        for c in self.codes:
            xtdata.subscribe_quote(c, period=self.period, count=self.count, callback=self.on_quote)

    def run_loop(self):
        """进入事件循环，阻塞主线程退出。"""
        print(f"[ready] subscribed period={self.period}, count={self.count}, stocks={self.codes}")
        print(f"[hint] CSV：{os.path.abspath(self.csv_path)}")
        print("[hint] Ctrl+C 退出。")
        xtdata.run()


def main():
    insp = QuoteInspector(STOCK_LIST, PERIOD, COUNT, CSV_PATH)
    insp.start()
    insp.run_loop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[exit] 用户中断，已退出。")
