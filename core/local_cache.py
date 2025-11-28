# -*- coding = utf-8 -*-
# @Time : 2025/9/9 22:24
# @Author : EquipmentADV
# @File : local_cache.py
# @Software : PyCharm
"""LocalCache：MiniQMT 本地数据补齐编排器

类说明：
    - 功能：按日期对指定 codes×period×区间调用 `download_history_data` 进行**增量补齐**；
    - 上游：HistoryAPI（历史获取）、RealtimeSubscriptionService（订阅预热）；
    - 下游：xtquant.xtdata（MiniQMT 本地库写入）。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, timedelta
import logging

try:
    from xtquant import xtdata
except Exception as e:  # pragma: no cover
    xtdata = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


@dataclass
class CacheConfig:
    """类说明：补齐编排配置
    功能：控制下载的并发/重试/分块等参数（当前最小实现：串行+重试）。
    上游：配置文件（history.yml / realtime.yml）。
    下游：LocalCache 行为。
    """
    retry_times: int = 3
    date_chunk_days: int = 60  # 按日期分块（跨度较大时，降低单次失败影响）


class LocalCache:
    """类说明：MiniQMT 本地数据补齐编排器
    功能：将请求区间按日期切块，逐块对每个 code 调用 `download_history_data`（incrementally=True）。
    上游：HistoryAPI、RealtimeSubscriptionService。
    下游：xtdata（MiniQMT）。
    """

    def __init__(self, cfg: Optional[CacheConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        if _IMPORT_ERR is not None:
            raise RuntimeError(
                f"未能导入 xtquant/xtdata：{_IMPORT_ERR}，请先确认 MiniQMT 环境可用。"
            )
        self.cfg = cfg or CacheConfig()
        self.logger = logger or logging.getLogger(__name__)

    # ----------------------------- 公有方法 -----------------------------
    def ensure_downloaded_date_range(self, codes: List[str], period: str,
                                     start_yyyymmdd: str, end_yyyymmdd: str,
                                     incrementally: bool = True) -> None:
        """方法说明：确保区间内数据已下载（按日期分块串行下载）
        功能：将大区间按 `date_chunk_days` 切块，对每个 code 逐块调用 `download_history_data`。
        上游：HistoryAPI.fetch_bars / RealtimeSubscriptionService.run_forever。
        下游：xtdata.download_history_data。
        参数：
            - codes: 标的代码列表
            - period: '1m'|'1h'|'1d'
            - start_yyyymmdd/end_yyyymmdd: 下载起止日期（闭区间），格式 'YYYYMMDD'
            - incrementally: 是否增量下载（True 可显著降低开销）
        """
        # 日期分块
        cur = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
        end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
        step = timedelta(days=self.cfg.date_chunk_days)
        chunks = []
        while cur <= end:
            chunk_end = min(cur + step - timedelta(days=1), end)
            chunks.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
            cur = chunk_end + timedelta(days=1)

        for code in codes:
            for s, e in chunks:
                self._download_one(code, period, s, e, incrementally)

    # ----------------------------- 内部方法 -----------------------------
    def _download_one(self, code: str, period: str,
                       start_yyyymmdd: str, end_yyyymmdd: str,
                       incrementally: bool) -> None:
        """方法说明：单 code 单日期块下载（带重试）
        功能：对单块调用 xtdata.download_history_data，失败按重试策略进行重试。
        上游：ensure_downloaded_date_range。
        下游：xtdata.download_history_data。
        """
        for i in range(self.cfg.retry_times):
            try:
                xtdata.download_history_data(
                    code,
                    period=period,
                    start_time=start_yyyymmdd,
                    end_time=end_yyyymmdd,
                    incrementally=incrementally,
                )
                self.logger.info("[LocalCache] downloaded %s %s %s~%s", code, period, start_yyyymmdd, end_yyyymmdd)
                return
            except Exception as e:  # pragma: no cover
                self.logger.warning("[LocalCache] download fail(%d/%d): %s %s %s~%s: %s",
                                    i + 1, self.cfg.retry_times, code, period, start_yyyymmdd, end_yyyymmdd, e)
        raise RuntimeError(f"LocalCache: 多次下载失败 {code} {period} {start_yyyymmdd}~{end_yyyymmdd}")