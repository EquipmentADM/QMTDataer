# -*- coding: utf-8 -*-
"""
QMTD Redis 控制面与行情 topic 监听脚本。

用途：
    - 默认只监听行情 topic，不主动向 QMTD 发送订阅命令。
    - 可通过 `--subscribe` 显式向 QMTD 控制通道发送订阅/退订命令。
    - 按同一周期、同一 bar 时间戳统计 Redis 接收时差。
    - 默认自动发现实际出现的标的池；显式传入 `--codes` 时才校验预期标的。

使用示例：
    python tests/flow_demo_subscribe_and_listen.py
    python tests/flow_demo_subscribe_and_listen.py --minutes 10
    python tests/flow_demo_subscribe_and_listen.py --subscribe --codes 510050.SH,159915.SZ

说明：
    - 默认 `--minutes 0`，表示不限制运行时间，手动 Ctrl+C 退出。
    - 默认每 30 分钟输出一次累计统计摘要。
    - 本脚本不会启动 QMTD，需要先启动真实或虚拟实时行情服务。
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import redis

_DEFAULT_CONFIG_PATH = Path("config/realtime.yml")
_AUTO_DISCOVER_CODES = "__AUTO__"


@dataclass
class ReceiveRecord:
    """单条 bar 的接收观测记录。"""

    listener_ts: float
    qmtd_recv_ts: Optional[float] = None


@dataclass
class GroupMetric:
    """单轮同周期 bar 接收统计结果。"""

    period: str
    bar_ts: str
    received_count: int
    expected_count: Optional[int]
    first_recv: float
    last_recv: float
    avg_recv: float
    span_sec: float
    mean_abs_error_sec: float
    qmtd_first_recv: Optional[float] = None
    qmtd_last_recv: Optional[float] = None
    qmtd_span_sec: Optional[float] = None
    redis_delay_avg_sec: Optional[float] = None
    redis_delay_max_sec: Optional[float] = None
    codes: List[str] = field(default_factory=list)
    missing_codes: List[str] = field(default_factory=list)
    extra_codes: List[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        """是否收齐该轮全部显式预期标的；自动发现模式下永远视作完整。"""
        return not self.missing_codes

    @property
    def expected_label(self) -> str:
        """输出用的数量标签。"""
        if self.expected_count is None:
            return f"{self.received_count}"
        return f"{self.received_count}/{self.expected_count}"


class ReceiveStats:
    """按 `(period, bar_ts)` 聚合接收时点并输出统计。"""

    def __init__(self, expected_codes: Iterable[str], periods: Iterable[str], auto_discover: bool = False) -> None:
        self.auto_discover = auto_discover
        expected_set = set(expected_codes)
        self.expected_codes_by_period: Dict[str, Set[str]] = {
            period: set(expected_set) for period in periods
        }
        self.groups: Dict[Tuple[str, str], Dict[str, ReceiveRecord]] = {}
        self.completed_keys: Set[Tuple[str, str]] = set()
        self.finalized_keys: Set[Tuple[str, str]] = set()
        self.metrics: List[GroupMetric] = []
        self.partial_metrics: List[GroupMetric] = []
        self.total_bars = 0
        self.observed_codes_by_period: Dict[str, Set[str]] = {period: set() for period in periods}

    def record(
        self,
        code: str,
        period: str,
        bar_ts: str,
        recv_ts: float,
        qmtd_recv_ts: Optional[float] = None,
    ) -> Optional[GroupMetric]:
        """记录一条 bar 的 Redis 接收时间。"""
        if not code or not period or not bar_ts:
            return None
        key = (period, bar_ts)
        group = self.groups.setdefault(key, {})
        if code not in group:
            self.total_bars += 1
        group[code] = ReceiveRecord(listener_ts=recv_ts, qmtd_recv_ts=qmtd_recv_ts)
        self.observed_codes_by_period.setdefault(period, set()).add(code)

        if key in self.completed_keys or key in self.finalized_keys:
            return None
        if self.auto_discover:
            return None

        expected_codes = self.expected_codes_by_period.get(period, set())
        if not expected_codes or not expected_codes.issubset(group.keys()):
            return None

        metric = self._build_metric(period, bar_ts, group, expected_codes)
        self.completed_keys.add(key)
        self.metrics.append(metric)
        return metric

    def finalize_due_groups(self, now_ts: float) -> List[GroupMetric]:
        """
        将超过半周期的轮次固化为统计结果。

        自动发现模式：
            不预设标的池，该轮实际出现哪些标的，就统计哪些标的。

        显式预期模式：
            已收到标的纳入延迟统计；缺失和额外标的单独列入完整性统计。
        """
        finalized: List[GroupMetric] = []
        for period, bar_ts in sorted(self.groups.keys()):
            key = (period, bar_ts)
            if key in self.completed_keys or key in self.finalized_keys:
                continue
            group = self.groups[key]
            if not group:
                continue
            first_recv = min(item.listener_ts for item in group.values())
            if now_ts - first_recv <= self._half_period_seconds(period):
                continue

            expected_codes = None if self.auto_discover else self.expected_codes_by_period.get(period, set())
            metric = self._build_metric(period, bar_ts, group, expected_codes)
            self.finalized_keys.add(key)
            self.metrics.append(metric)
            if not metric.is_complete:
                self.partial_metrics.append(metric)
            finalized.append(metric)
        return finalized

    def build_waiting_snapshot(self, limit: int = 5) -> List[GroupMetric]:
        """生成仍在半周期等待窗口内的最近若干轮快照。"""
        snapshots: List[GroupMetric] = []
        for period, bar_ts in sorted(self.groups.keys())[-limit:]:
            key = (period, bar_ts)
            if key in self.completed_keys or key in self.finalized_keys:
                continue
            expected_codes = None if self.auto_discover else self.expected_codes_by_period.get(period, set())
            snapshots.append(self._build_metric(period, bar_ts, self.groups[key], expected_codes))
        return snapshots

    def build_partial_snapshot(self, limit: int = 5) -> List[GroupMetric]:
        """生成最近若干个未齐轮次快照。"""
        return self.partial_metrics[-limit:]

    def build_summary(self) -> Dict[str, object]:
        """生成累计统计摘要。"""
        by_period: Dict[str, List[GroupMetric]] = {}
        for item in self.metrics:
            by_period.setdefault(item.period, []).append(item)

        period_summary = {}
        for period, items in by_period.items():
            spans = [item.span_sec for item in items]
            mean_abs_errors = [item.mean_abs_error_sec for item in items]
            qmtd_spans = [item.qmtd_span_sec for item in items if item.qmtd_span_sec is not None]
            redis_delay_avgs = [item.redis_delay_avg_sec for item in items if item.redis_delay_avg_sec is not None]
            redis_delay_maxes = [item.redis_delay_max_sec for item in items if item.redis_delay_max_sec is not None]
            counts = [item.received_count for item in items]
            full_items = [item for item in items if item.is_complete]
            partial_items = [item for item in items if not item.is_complete]
            period_summary[period] = {
                "stat_groups": len(items),
                "full_groups": len(full_items),
                "partial_groups": len(partial_items),
                "avg_count": statistics.mean(counts) if counts else 0.0,
                "min_count": min(counts) if counts else 0,
                "max_count": max(counts) if counts else 0,
                "avg_span_sec": statistics.mean(spans) if spans else 0.0,
                "max_span_sec": max(spans) if spans else 0.0,
                "avg_mean_abs_error_sec": statistics.mean(mean_abs_errors) if mean_abs_errors else 0.0,
                "max_mean_abs_error_sec": max(mean_abs_errors) if mean_abs_errors else 0.0,
                "avg_qmtd_span_sec": statistics.mean(qmtd_spans) if qmtd_spans else None,
                "max_qmtd_span_sec": max(qmtd_spans) if qmtd_spans else None,
                "avg_redis_delay_sec": statistics.mean(redis_delay_avgs) if redis_delay_avgs else None,
                "max_redis_delay_sec": max(redis_delay_maxes) if redis_delay_maxes else None,
                "last_bar_ts": items[-1].bar_ts if items else "",
                "observed_codes": sorted(self.observed_codes_by_period.get(period, set())),
            }

        return {
            "total_bars": self.total_bars,
            "stat_groups": len(self.metrics),
            "full_groups": len([item for item in self.metrics if item.is_complete]),
            "partial_groups": len(self.partial_metrics),
            "period_summary": period_summary,
            "waiting_groups": len(self.groups) - len(self.completed_keys) - len(self.finalized_keys),
        }

    @staticmethod
    def _build_metric(
        period: str,
        bar_ts: str,
        group: Dict[str, ReceiveRecord],
        expected_codes: Optional[Set[str]],
    ) -> GroupMetric:
        recv_values = [item.listener_ts for item in group.values()]
        qmtd_values = [item.qmtd_recv_ts for item in group.values() if item.qmtd_recv_ts is not None]
        redis_delays = [
            item.listener_ts - item.qmtd_recv_ts
            for item in group.values()
            if item.qmtd_recv_ts is not None
        ]
        first_recv = min(recv_values) if recv_values else 0.0
        last_recv = max(recv_values) if recv_values else 0.0
        avg_recv = statistics.mean(recv_values) if recv_values else 0.0
        mean_abs_error = statistics.mean([abs(item - avg_recv) for item in recv_values]) if recv_values else 0.0
        qmtd_first = min(qmtd_values) if qmtd_values else None
        qmtd_last = max(qmtd_values) if qmtd_values else None
        qmtd_span = (qmtd_last - qmtd_first) if qmtd_first is not None and qmtd_last is not None else None
        redis_delay_avg = statistics.mean(redis_delays) if redis_delays else None
        redis_delay_max = max(redis_delays) if redis_delays else None
        codes = sorted(group.keys())
        expected_count = len(expected_codes) if expected_codes else None
        missing_codes = sorted(expected_codes.difference(group.keys())) if expected_codes else []
        extra_codes = sorted(set(group.keys()).difference(expected_codes)) if expected_codes else []
        return GroupMetric(
            period=period,
            bar_ts=bar_ts,
            received_count=len(group),
            expected_count=expected_count,
            first_recv=first_recv,
            last_recv=last_recv,
            avg_recv=avg_recv,
            span_sec=last_recv - first_recv,
            mean_abs_error_sec=mean_abs_error,
            qmtd_first_recv=qmtd_first,
            qmtd_last_recv=qmtd_last,
            qmtd_span_sec=qmtd_span,
            redis_delay_avg_sec=redis_delay_avg,
            redis_delay_max_sec=redis_delay_max,
            codes=codes,
            missing_codes=missing_codes,
            extra_codes=extra_codes,
        )

    @staticmethod
    def _half_period_seconds(period: str) -> float:
        """返回周期的一半秒数；无法识别时按 60 秒周期处理。"""
        p = (period or "").lower()
        mapping = {
            "1m": 60.0,
            "1min": 60.0,
            "1minute": 60.0,
            "1h": 3600.0,
            "1d": 86400.0,
            "1day": 86400.0,
        }
        return mapping.get(p, 60.0) / 2.0


def _load_defaults(config_path: Path) -> dict:
    """从配置文件读取默认连接与订阅参数。"""
    if not config_path.exists():
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        return {}
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return {
        "redis_url": raw.get("redis", {}).get("url", "redis://127.0.0.1:6379/0"),
        "ctrl_channel": raw.get("control", {}).get("channel", "xt:ctrl:sub"),
        "ack_prefix": raw.get("control", {}).get("ack_prefix", "xt:ctrl:ack"),
        "topic": raw.get("redis", {}).get("topic", "xt:topic:bar"),
        "codes": ",".join(raw.get("subscription", {}).get("codes", [])),
        "periods": ",".join(raw.get("subscription", {}).get("periods", ["1m"])),
        "mode": raw.get("subscription", {}).get("mode"),
        "preload_days": raw.get("subscription", {}).get("preload_days", 1),
    }


def _split_csv(text: str) -> List[str]:
    """把逗号分隔字符串拆成去空格列表。"""
    return [item.strip() for item in text.split(",") if item.strip()]


def _fmt_ts(epoch_sec: float) -> str:
    """把接收时点格式化成本地时间字符串。"""
    if not epoch_sec:
        return "-"
    return datetime.fromtimestamp(epoch_sec).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _fmt_seconds(value: Optional[float]) -> str:
    """格式化秒数，空值显示为短横线。"""
    if value is None:
        return "-"
    return f"{value:.3f}s"


def _parse_payload_time(value: object) -> Optional[float]:
    """解析 QMTD payload 中的本地无时区时间字符串。"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace(" ", "T")).timestamp()
    except Exception:
        return None


def _preview_codes(codes: List[str], limit: int = 8) -> str:
    """压缩显示标的列表。"""
    if not codes:
        return "-"
    preview = ",".join(codes[:limit])
    if len(codes) > limit:
        preview += f"...等{len(codes)}个"
    return preview


def _print_group_metric(metric: GroupMetric) -> None:
    """打印单轮接收统计。"""
    print(
        "[周期接收统计] "
        f"周期={metric.period} bar={metric.bar_ts} "
        f"标的数={metric.expected_label} "
        f"最早={_fmt_ts(metric.first_recv)} "
        f"最晚={_fmt_ts(metric.last_recv)} "
        f"平均={_fmt_ts(metric.avg_recv)} "
        f"跨度={metric.span_sec:.3f}s "
        f"平均绝对误差={metric.mean_abs_error_sec:.3f}s "
        f"QMTD发布跨度={_fmt_seconds(metric.qmtd_span_sec)} "
        f"Redis后段平均延迟={_fmt_seconds(metric.redis_delay_avg_sec)} "
        f"Redis后段最大延迟={_fmt_seconds(metric.redis_delay_max_sec)} "
        f"标的={_preview_codes(metric.codes)}"
    )


def _print_finalized_metric(metric: GroupMetric) -> None:
    """打印半周期后固化的单轮统计。"""
    if metric.is_complete:
        _print_group_metric(metric)
        return
    print(
        "[WARNING 周期覆盖率不足] "
        f"周期={metric.period} bar={metric.bar_ts} "
        f"标的数={metric.expected_label} "
        f"跨度={metric.span_sec:.3f}s "
        f"QMTD发布跨度={_fmt_seconds(metric.qmtd_span_sec)} "
        f"Redis后段最大延迟={_fmt_seconds(metric.redis_delay_max_sec)} "
        f"缺失={_preview_codes(metric.missing_codes)} "
        f"额外={_preview_codes(metric.extra_codes)} "
        "说明=已按收到标的统计延迟，覆盖率不足需单独关注"
    )


def _print_summary(stats: ReceiveStats, title: str) -> None:
    """打印累计接收统计摘要。"""
    summary = stats.build_summary()
    print(f"\n[累计统计] {title}")
    print(
        f"  总接收bar数={summary['total_bars']} "
        f"统计轮次={summary['stat_groups']} "
        f"全量轮次={summary['full_groups']} "
        f"覆盖不足轮次={summary['partial_groups']} "
        f"等待中轮次={summary['waiting_groups']}"
    )
    period_summary = summary["period_summary"]
    if not period_summary:
        print("  暂无统计轮次。")
    for period, item in period_summary.items():
        print(
            f"  周期={period} 统计轮次={item['stat_groups']} "
            f"全量轮次={item['full_groups']} 覆盖不足轮次={item['partial_groups']} "
            f"平均每轮标的数={item['avg_count']:.2f} 最少={item['min_count']} 最多={item['max_count']} "
            f"平均跨度={item['avg_span_sec']:.3f}s 最大跨度={item['max_span_sec']:.3f}s "
            f"平均绝对误差均值={item['avg_mean_abs_error_sec']:.3f}s "
            f"平均绝对误差最大={item['max_mean_abs_error_sec']:.3f}s "
            f"QMTD平均发布跨度={_fmt_seconds(item['avg_qmtd_span_sec'])} "
            f"QMTD最大发布跨度={_fmt_seconds(item['max_qmtd_span_sec'])} "
            f"Redis后段平均延迟={_fmt_seconds(item['avg_redis_delay_sec'])} "
            f"Redis后段最大延迟={_fmt_seconds(item['max_redis_delay_sec'])} "
            f"最近bar={item['last_bar_ts']}"
        )
        print(f"    已发现标的池={_preview_codes(item['observed_codes'], limit=20)}")

    waiting = stats.build_waiting_snapshot()
    if waiting:
        print("  最近等待中轮次：")
        for item in waiting:
            print(
                f"    周期={item.period} bar={item.bar_ts} "
                f"已收={item.expected_label} 标的={_preview_codes(item.codes)}"
            )

    partial_items = stats.build_partial_snapshot()
    if partial_items:
        print("  最近覆盖不足轮次：")
        for item in partial_items:
            print(
                f"    周期={item.period} bar={item.bar_ts} "
                f"已收={item.expected_label} "
                f"跨度={item.span_sec:.3f}s "
                f"缺失={_preview_codes(item.missing_codes)} "
                f"额外={_preview_codes(item.extra_codes)}"
            )
    if summary["partial_groups"]:
        print("  WARNING：存在覆盖不足轮次，请检查停牌、订阅池或行情源是否漏推。")
    print()


def send_subscribe(cli, channel: str, strategy_id: str, codes, periods,
                   preload_days=None, topic=None, mode=None):
    """向控制通道发布 subscribe 命令。"""
    payload = {
        "action": "subscribe",
        "strategy_id": strategy_id,
        "codes": codes,
        "periods": periods,
    }
    if preload_days is not None:
        payload["preload_days"] = int(preload_days)
    if topic:
        payload["topic"] = topic
    if mode:
        payload["mode"] = mode
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))
    return payload


def send_unsubscribe(cli, channel: str, strategy_id: str,
                     sub_id: str = None, codes=None, periods=None):
    """向控制通道发布 unsubscribe 命令。"""
    payload = {"action": "unsubscribe", "strategy_id": strategy_id}
    if sub_id:
        payload["sub_id"] = sub_id
    if codes:
        payload["codes"] = codes
    if periods:
        payload["periods"] = periods
    cli.publish(channel, json.dumps(payload, ensure_ascii=False))


def main(argv: Optional[List[str]] = None):
    """脚本入口：完成可选订阅、监听、统计和可选退订流程。"""
    defaults = _load_defaults(_DEFAULT_CONFIG_PATH)

    parser = argparse.ArgumentParser(description="QMTD 行情端到端实验：订阅、监听、统计、退订")
    parser.add_argument("--redis-url", default=defaults.get("redis_url", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--ctrl-channel", default=defaults.get("ctrl_channel", "xt:ctrl:sub"))
    parser.add_argument("--ack-prefix", default=defaults.get("ack_prefix", "xt:ctrl:ack"))
    parser.add_argument("--topic", default=defaults.get("topic", "xt:topic:bar"))
    parser.add_argument("--strategy-id", default="demo")
    parser.add_argument("--codes", default=_AUTO_DISCOVER_CODES,
                        help="显式预期标的池；默认自动发现实际出现的标的，不校验缺失")
    parser.add_argument("--periods", default=defaults.get("periods", "1m"))
    parser.add_argument("--mode", default=defaults.get("mode"))
    parser.add_argument("--preload-days", type=int, default=defaults.get("preload_days", 1))
    parser.add_argument("--ack-timeout", type=float, default=5.0,
                        help="等待订阅 ACK 的秒数，预热耗时较长时可调大")
    parser.add_argument("--minutes", type=int, default=0,
                        help="监听分钟数；0 表示不限制运行时间，默认 0")
    parser.add_argument("--summary-interval-minutes", type=float, default=30.0,
                        help="累计统计摘要输出间隔，默认 30 分钟")
    parser.add_argument("--subscribe", action="store_true",
                        help="显式向 QMTD 发送订阅命令；默认仅监听，不主动订阅")
    parser.add_argument("--no-watch", action="store_true",
                        help="仅在 --subscribe 时有效：只发送订阅并等待 ACK，不进入监听")
    parser.add_argument("--no-unsubscribe", action="store_true",
                        help="仅在 --subscribe 时有效：退出前不自动退订")
    args = parser.parse_args(argv)

    cli = redis.from_url(args.redis_url, decode_responses=True)
    auto_discover = args.codes == _AUTO_DISCOVER_CODES and not args.subscribe
    codes = [] if auto_discover else _split_csv(args.codes)
    periods = _split_csv(args.periods)
    if args.subscribe and not codes:
        parser.error("--subscribe 模式下 codes 不能为空")
    if not periods:
        parser.error("periods 不能为空")

    ack_ch = f"{args.ack_prefix}:{args.strategy_id}"
    channels = [args.topic]
    if args.subscribe:
        channels.append(ack_ch)
    ps = cli.pubsub()
    ps.subscribe(*channels)
    while ps.get_message(timeout=0.05):
        pass

    print(f"[FLOW] Redis={args.redis_url} 控制通道={args.ctrl_channel} 行情topic={args.topic}")
    print(f"[FLOW] 周期={periods} strategy_id={args.strategy_id}")
    if auto_discover:
        print("[FLOW] 统计模式=自动发现实际标的池，不预设缺失标的")
    else:
        print(f"[FLOW] 统计模式=显式预期标的池，标的数={len(codes)}")
    print(f"[FLOW] 当前模式={'发送订阅并监听' if args.subscribe else '仅监听已有行情，不发送订阅'}")

    sub_id = None
    if args.subscribe:
        send_subscribe(cli, args.ctrl_channel, args.strategy_id,
                       codes, periods, args.preload_days,
                       topic=args.topic, mode=args.mode)
        print(f"[FLOW] 等待 ACK：{ack_ch} timeout={args.ack_timeout}s")

        ack_deadline = time.time() + args.ack_timeout
        while time.time() < ack_deadline:
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if not msg:
                continue
            if msg.get("channel") != ack_ch:
                continue
            try:
                ack = json.loads(msg.get("data", "{}"))
            except Exception:
                print(f"[ACK] 无法解析：{msg.get('data')}")
                continue
            print("[ACK]", ack)
            if ack.get("ok") and ack.get("action") == "subscribe":
                sub_id = ack.get("sub_id", sub_id)
            break
        else:
            print("[FLOW] 未在超时时间内收到订阅 ACK，可调大 --ack-timeout 或检查 QMTD 控制面。")

    if args.no_watch:
        if args.subscribe and not args.no_unsubscribe and sub_id:
            send_unsubscribe(cli, args.ctrl_channel, args.strategy_id, sub_id=sub_id)
            print(f"[FLOW] 已发送自动退订，sub_id={sub_id}")
        ps.close()
        if args.subscribe and sub_id:
            print("[FLOW] 订阅已建立，可启动监听脚本或下游系统观察行情。")
        return 0

    if args.minutes > 0:
        end_ts = time.time() + args.minutes * 60
        print(f"[FLOW] 开始监听，运行时间={args.minutes}分钟。")
    else:
        end_ts = None
        print("[FLOW] 开始监听，运行时间=不限，按 Ctrl+C 退出。")

    stats = ReceiveStats(codes, periods, auto_discover=auto_discover)
    summary_interval = max(1.0, args.summary_interval_minutes * 60.0)
    next_summary_ts = time.time() + summary_interval

    try:
        while end_ts is None or time.time() < end_ts:
            msg = ps.get_message(ignore_subscribe_messages=True, timeout=1.0)
            now_ts = time.time()
            for metric in stats.finalize_due_groups(now_ts):
                _print_finalized_metric(metric)
            if now_ts >= next_summary_ts:
                _print_summary(stats, f"每 {args.summary_interval_minutes:g} 分钟定时摘要")
                next_summary_ts = now_ts + summary_interval
            if not msg:
                continue
            channel = msg.get("channel")
            data = msg.get("data")
            if channel == ack_ch:
                try:
                    ack = json.loads(data)
                    print("[ACK]", ack)
                except Exception:
                    print("[ACK parse err]", data)
            elif channel == args.topic:
                try:
                    bar = json.loads(data)
                    recv_ts = time.time()
                    code = bar.get("code")
                    period = bar.get("period")
                    bar_ts = bar.get("bar_end_ts") or bar.get("time") or ""
                    print("[BAR]", code, period, bar_ts,
                          "close=", bar.get("close"), "is_closed=", bar.get("is_closed"))
                    metric = stats.record(
                        str(code or ""),
                        str(period or ""),
                        str(bar_ts or ""),
                        recv_ts,
                        qmtd_recv_ts=_parse_payload_time(bar.get("recv_ts")),
                    )
                    if metric:
                        _print_group_metric(metric)
                except Exception:
                    print("[BAR raw]", data)
    except KeyboardInterrupt:
        print("\n[FLOW] 收到 Ctrl+C，准备退出。")
    finally:
        # 退出前再固化一次已超过半周期的轮次，尽量减少摘要遗漏。
        for metric in stats.finalize_due_groups(time.time()):
            _print_finalized_metric(metric)
        _print_summary(stats, "退出前最终摘要")
        if args.subscribe and not args.no_unsubscribe and sub_id:
            send_unsubscribe(cli, args.ctrl_channel, args.strategy_id, sub_id=sub_id)
            print(f"[FLOW] 已发送自动退订，sub_id={sub_id}")
        ps.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
