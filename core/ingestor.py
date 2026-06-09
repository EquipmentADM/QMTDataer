# -*- coding: utf-8 -*-
"""
简化版行情入库协调器。
职责:
    - 调用数据源（BaseMarketDataSource）拉取单标行情；
    - 可选预处理；
    - 可选按时间列截取范围；
    - 调用 FinancialDataStorage 按约定落盘。
Data Contract:
    - 数据源需返回包含时间列的 DataFrame（至少 time/open/high/low/close/volume 这些核心字段）；
    - 本类不改变数据字段语义，仅做流程编排与落盘。
"""
from __future__ import annotations

import os
from typing import Any, Callable, Optional

import pandas as pd

from core.xtdata_source import BaseMarketDataSource


class MarketDataIngestor:
    """
    行情入库协调器：负责“取数 -> 标准化 -> 校验 -> 落盘”全流程。
    """

    def __init__(self, storage: Any) -> None:
        self.storage = storage

    def ingest_symbol(
        self,
        source: BaseMarketDataSource,
        market: str,
        symbol: str,
        cycle: str,
        specific: str = "original",
        start: Optional[str] = None,
        end: Optional[str] = None,
        file_type: str = "csv",
        preprocess: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        time_column: Optional[str] = None,
        merge: bool = False,
    ) -> str:
        """
        从数据源拉取行情并保存到本地目录。
        Args:
            source (BaseMarketDataSource): 实现 fetch 的数据源适配器。
            market (str): 目标市场目录，如 'SS_stock_data'、'Crypto_data'。
            symbol (str): 品种名称，应与目录结构一致。
            cycle (str): 周期字符串。
            specific (str): 合成标记或子目录，默认 'original'。
            start (str | None): 起始时间（含），透传给数据源。
            end (str | None): 结束时间（含），透传给数据源。
            file_type (str): 落盘格式，默认 'csv'。
            preprocess (Callable | None): 可选 DataFrame 预处理回调（如增补字段、复权等）。
            time_column (str | None): 指定时间列，若存在则在落盘前按 start/end 过滤。
            merge (bool): 若为 True，则与已有文件按时间列合并去重后再落盘，适合增量更新。
        Returns:
            str: 成功写入的本地文件完整路径。
        """
        cycle_std = self.storage.validate_cycle(cycle)
        market_std = self.storage.validate_market(market)
        specific_std = self.storage.validate_specific(specific)
        file_type = file_type or "csv"
        if file_type not in self.storage.file_type_list:
            raise ValueError(f"Unsupported file_type: {file_type}")

        df = source.fetch(
            symbol=symbol,
            cycle=cycle_std,
            market=market_std,
            specific=specific_std,
            start=start,
            end=end,
        )
        if df is None or df.empty:
            raise ValueError(f"Data source returned empty result for {symbol}-{cycle_std}")

        if preprocess is not None:
            df = preprocess(df)
        if time_column and time_column in df.columns:
            df = self.storage.filter_df_by_date(
                df,
                start_date=start,
                end_date=end,
                time_columns=(time_column,),
                allow_sort=True,
            )

        target_dir = self.storage._build_target_dir(market_std, symbol, cycle_std, specific_std)
        filename = self.storage._build_filename(market_std, symbol, cycle_std, specific_std, file_type)
        if merge:
            out_path = self.storage.merge_and_save(
                df,
                target_dir,
                symbol=symbol,
                cycle=cycle_std,
                specific=specific_std,
                market=market_std,
                file_type=file_type,
                time_column=time_column or "time",
                dropna_time=True,
                prefer_new=True,
            )
        else:
            self.storage._save_dataframe(
                df,
                target_dir,
                symbol=symbol,
                cycle=cycle_std,
                specific=specific_std,
                market=market_std,
                file_type=file_type,
                overwrite=True,
            )
            out_path = os.path.join(target_dir, filename)
        return out_path
