"""QMT API 探针脚本（固定参数快速拉通）

类/脚本说明：
    - 目的：在单元测试前，先以**固定参数**调用 QMT 的关键 API，观察**真实返回结构**，
      包括：服务配置、历史行情（按 count / 按时间窗）、交易日历/交易时段、除权因子、
      以及两种实时探针（轮询 / 订阅）。
    - 上游：MiniQmt + xtquant（xtdatacenter / xtdata）。
    - 下游：开发/测试人员（stdout 日志与样例打印），或后续自动化对接工具。

使用示例：
    # 1) 快速检查（默认参数：000001.SZ, 1m/1d；当日/近窗）
    python scripts/qmt_api_probe.py --quick

    # 2) 全量检查（可能较慢）
    python scripts/qmt_api_probe.py --all

    # 3) 指定代码与时间窗
    python scripts/qmt_api_probe.py \
        --codes 000001.SZ,600000.SH \
        --start 2025-01-02T09:30:00+08:00 \
        --end   2025-01-02T15:00:00+08:00 \
        --periods 1m,1d --dividend none

    # 4) 实时探针（轮询 N 秒，不订阅）
    python scripts/qmt_api_probe.py --realtime-poll 5 --codes 000001.SZ --periods 1m

    # 5) 实时探针（订阅 N 秒，可能阻塞，谨慎使用）
    python scripts/qmt_api_probe.py --realtime-sub 5 --codes 000001.SZ --periods 1m

注意：
    - 本脚本会尝试初始化 xtdatacenter 并校验 xtdata 可用；
    - 订阅模式下将调用 xtdata.run()，这里用线程包裹并尽力在超时后停止；某些版本
      无 stop() 时需人工 Ctrl+C 结束（日志会提示）。
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

try:
    from xtquant import xtdatacenter as xtdc
    from xtquant import xtdata
except Exception as e:
    print("[FATAL] 无法导入 xtquant（xtdatacenter/xtdata）：", e, file=sys.stderr)
    sys.exit(2)

try:
    import pandas as pd
except Exception as e:  # pragma: no cover
    pd = None  # type: ignore

CN_TZ = timezone(timedelta(hours=8))
ISO = "%Y-%m-%dT%H:%M:%S%z"  # 输出统一为 +0800 形式


@dataclass
class ProbeConfig:
    """配置结构体（类说明）
    功能：承载探针执行所需的基础参数（代码、周期、时间窗、复权、订阅时长等）。
    上游：命令行参数/CI变量。
    下游：ApiProbe 方法。
    """
    token: str
    codes: List[str]
    periods: List[str]
    start: Optional[str] = None
    end: Optional[str] = None
    dividend: str = "none"
    realtime_poll_sec: int = 0
    realtime_sub_sec: int = 0


class ApiProbe:
    """类说明：QMT API 探针工具
    功能：提供一组方法，以固定/可配参数调用 QMT 核心 API 并打印摘要与样例。
    上游：ProbeConfig。
    下游：开发/测试人员（stdout）。
    """
    def __init__(self, cfg: ProbeConfig, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------- 启动与基础连通性 -------------------------
    def connect(self, port: int = 50100) -> None:
        """方法说明：初始化/监听并验证连接
        功能：设置 token、初始化/监听 xtdatacenter，并粗检 xtdata 可用性。
        上游：脚本入口。
        下游：后续所有探针方法。
        """
        print(xtdc.__dc.__quote_token)
        self.logger.info("[connect] init+listen... port=%d", port)
        # if hasattr(xtdc, "set_token") and self.cfg.token:
        #     xtdc.set_token(self.cfg.token)
        # if hasattr(xtdc, "init"):
        #     xtdc.init()
        # if hasattr(xtdc, "listen"):
        #     xtdc.listen(port)
        # elif hasattr(xtdc, "listen_range"):
        #     xtdc.listen_range(port, port + 20)
        # else:
        #     self.logger.warning("[connect] 未发现 listen 接口，可能版本已自动监听")

        # 粗检服务配置
        if hasattr(xtdata, "get_server_config"):
            try:
                cfg = xtdata.get_server_config()
                print("\n== get_server_config ==\n", json.dumps(cfg, ensure_ascii=False, indent=2))
            except Exception as e:  # pragma: no cover
                self.logger.warning("get_server_config 调用失败：%s", e)

    # --------------------------- 历史行情探针 ---------------------------
    def probe_market_data_by_count(self, code: str, period: str, count: int = 5, dividend: str = "none") -> None:
        """方法说明：按 count 拉取最近 N 条历史 K 线
        功能：便于快速查看返回的列/类型/样例。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        self.logger.info("[history-count] code=%s period=%s count=%d dividend=%s", code, period, count, dividend)
        df = xtdata.get_market_data(stock_code=code, period=period, count=count, dividend_type=dividend, fill_data=True)
        self._print_df_info("get_market_data(count)", df)

    def probe_market_data_by_time(self, code: str, period: str, start_iso: str, end_iso: str, dividend: str = "none") -> None:
        """方法说明：按时间窗拉取历史 K 线
        功能：验证时间参数格式与返回结构（含缺口/集合竞价等）。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        def to_qmt_ts(ts_iso: str) -> str:
            dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(CN_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        start_qmt = to_qmt_ts(start_iso)
        end_qmt = to_qmt_ts(end_iso)
        self.logger.info("[history-time] code=%s period=%s %s ~ %s dividend=%s", code, period, start_qmt, end_qmt, dividend)
        df = xtdata.get_market_data(stock_code=code, period=period, start_time=start_qmt, end_time=end_qmt, dividend_type=dividend, fill_data=True)
        self._print_df_info("get_market_data(time)", df)

    # ------------------------- 交易日历/时段探针 ------------------------
    def probe_trading_calendar(self, market: str = "CN", year: int = None) -> None:
        """方法说明：获取交易日历（年度）
        功能：查看返回结构（列表/表格/字典），便于后续缺口计算对齐。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        fn = getattr(xtdata, "get_trading_calendar", None)
        if not fn:
            self.logger.warning("当前 xtdata 版本缺少 get_trading_calendar")
            return
        year = year or datetime.now(CN_TZ).year
        cal = fn(market=market, year=year)
        print("\n== get_trading_calendar ==\n", str(cal)[:1000])

    def probe_trading_time(self, market: str = "CN", date: Optional[str] = None) -> None:
        """方法说明：获取某天的交易时段定义
        功能：查看竞价/连续/收盘等时段结构，便于实时/历史对齐。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        fn = getattr(xtdata, "get_trading_time", None)
        if not fn:
            self.logger.warning("当前 xtdata 版本缺少 get_trading_time")
            return
        date = date or datetime.now(CN_TZ).strftime("%Y-%m-%d")
        tm = fn(market=market, date=date)
        print("\n== get_trading_time ==\n", json.dumps(tm, ensure_ascii=False, indent=2) if isinstance(tm, (dict, list)) else str(tm))

    # --------------------------- 除权因子探针 ---------------------------
    def probe_divid_factors(self, code: str) -> None:
        """方法说明：获取除权因子
        功能：核验复权序列，便于后续对齐指数/标的。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        fn = getattr(xtdata, "get_divid_factors", None)
        if not fn:
            self.logger.warning("当前 xtdata 版本缺少 get_divid_factors")
            return
        fac = fn(code)
        print("\n== get_divid_factors ==\n", str(fac)[:1000])

    # ----------------------------- 实时探针 -----------------------------
    def probe_realtime_polling(self, code: str, period: str, seconds: int = 5) -> None:
        """方法说明：实时轮询探针（不订阅）
        功能：每秒拉一次最近两根 K 线，打印末尾样例，避免订阅阻塞。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        self.logger.info("[realtime-poll] code=%s period=%s seconds=%d", code, period, seconds)
        for i in range(max(1, seconds)):
            try:
                df = xtdata.get_market_data(code, period, count=2, fill_data=True)
                self._print_df_info(f"poll@{i}", df, max_rows=1)
            except Exception as e:  # pragma: no cover
                self.logger.warning("轮询失败：%s", e)
            time.sleep(1)

    def probe_realtime_subscribe(self, code: str, period: str, seconds: int = 5, max_print: int = 5) -> None:
        """方法说明：实时订阅探针（谨慎使用）
        功能：调用 subscribe_quote 注册回调，后台线程执行 xtdata.run()，
              在设定时长后尝试停止；打印少量回调样例。
        上游：connect 后调用。
        下游：stdout 打印。
        """
        self.logger.info("[realtime-sub] code=%s period=%s seconds=%d", code, period, seconds)
        printed = {"n": 0}
        stop_evt = threading.Event()

        def on_evt(*_a, **_k):
            if printed["n"] >= max_print:
                return
            try:
                df = xtdata.get_market_data(code, period, count=1, fill_data=True)
                self._print_df_info("sub-callback", df, max_rows=1)
                printed["n"] += 1
            except Exception as e:  # pragma: no cover
                self.logger.warning("回调拉取失败：%s", e)

        xtdata.subscribe_quote(code, period, -1, on_evt)

        def run_loop():
            try:
                xtdata.run()
            except Exception as e:  # pragma: no cover
                self.logger.warning("xtdata.run 异常退出：%s", e)

        th = threading.Thread(target=run_loop, daemon=True)
        th.start()
        stop_evt.wait(timeout=max(1, seconds))
        # 尝试停止（若有 stop 接口）
        if hasattr(xtdata, "stop"):
            try:
                xtdata.stop()
            except Exception:  # pragma: no cover
                pass
        self.logger.info("[realtime-sub] 结束（可能仍需 Ctrl+C 终止进程视版本而定）")

    # ----------------------------- 工具方法 -----------------------------
    def _print_df_info(self, title: str, df, max_rows: int = 3) -> None:
        """方法说明：打印 DataFrame 摘要信息
        功能：统一打印 shape/列名/前几行与推断的时间列。
        上游：各探针方法。
        下游：stdout。
        """
        print(f"\n== {title} ==")
        if df is None:
            print("(None)")
            return
        if pd is None or not hasattr(df, "head"):
            print(str(df)[:1000])
            return
        print("shape:", getattr(df, "shape", None))
        print("columns:", list(getattr(df, "columns", [])))
        try:
            # 打印前几行（尽量不爆屏）
            print(df.head(max_rows).to_string(index=False))
        except Exception:
            print(str(df)[:1000])


# ----------------------------- CLI 入口 -----------------------------

def main():
    parser = argparse.ArgumentParser(description="QMT API 探针脚本（固定参数快速拉通）")
    parser.add_argument("--token", type=str, default="${QMT_TOKEN}")
    parser.add_argument("--codes", type=str, default="000001.SZ")
    parser.add_argument("--periods", type=str, default="1m,1d")
    parser.add_argument("--start", type=str, default=None, help="ISO8601，例如 2025-01-02T09:30:00+08:00")
    parser.add_argument("--end", type=str, default=None, help="ISO8601，例如 2025-01-02T15:00:00+08:00")
    parser.add_argument("--dividend", type=str, default="none", choices=["none", "front", "back", "ratio"])
    parser.add_argument("--quick", action="store_true", help="快速检查：少量接口")
    parser.add_argument("--all", action="store_true", help="全量检查：可能较慢")
    parser.add_argument("--realtime-poll", type=int, default=0, metavar="N", help="轮询实时 N 秒（不订阅）")
    parser.add_argument("--realtime-sub", type=int, default=0, metavar="N", help="订阅实时 N 秒（可能阻塞）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = ProbeConfig(
        token=args.token,
        codes=[c.strip() for c in args.codes.split(",") if c.strip()],
        periods=[p.strip() for p in args.periods.split(",") if p.strip()],
        start=args.start,
        end=args.end,
        dividend=args.dividend,
        realtime_poll_sec=args.realtime_poll,
        realtime_sub_sec=args.realtime_sub,
    )

    probe = ApiProbe(cfg)
    probe.connect()

    # 快速 or 全量
    if args.quick or (not args.quick and not args.all):
        # Quick：每类接口给一到两个典型调用
        code = cfg.codes[0]
        # 历史：按 count
        probe.probe_market_data_by_count(code, "1m", count=5, dividend=cfg.dividend)
        # 历史：按时间窗（若没给 start/end，用今天收盘前 1 小时窗口）
        if cfg.start and cfg.end:
            s, e = cfg.start, cfg.end
        else:
            now = datetime.now(CN_TZ)
            s = (now - timedelta(hours=1)).strftime(ISO)
            e = now.strftime(ISO)
        probe.probe_market_data_by_time(code, "1m", s, e, dividend=cfg.dividend)
        # 日历/时段
        probe.probe_trading_calendar("CN")
        probe.probe_trading_time("CN")
        # 因子
        probe.probe_divid_factors(code)
    else:
        # All：遍历 codes×periods
        for code in cfg.codes:
            for p in cfg.periods:
                probe.probe_market_data_by_count(code, p, count=10, dividend=cfg.dividend)
                if cfg.start and cfg.end:
                    probe.probe_market_data_by_time(code, p, cfg.start, cfg.end, dividend=cfg.dividend)
        probe.probe_trading_calendar("CN")
        probe.probe_trading_time("CN")
        for code in cfg.codes:
            probe.probe_divid_factors(code)

    # 实时探针（可选）
    if cfg.realtime_poll_sec > 0:
        for code in cfg.codes:
            for p in cfg.periods:
                probe.probe_realtime_polling(code, p, seconds=cfg.realtime_poll_sec)

    if cfg.realtime_sub_sec > 0:
        # 订阅仅对第一个 code×period 做演示，避免过多输出
        probe.probe_realtime_subscribe(cfg.codes[0], cfg.periods[0], seconds=cfg.realtime_sub_sec)


if __name__ == "__main__":
    main()
