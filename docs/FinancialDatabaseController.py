# -*- coding = utf-8 -*-
# @Time : 2024/4/16 16:31
# @Author : EquipmentADV
# @File : FinancialDatabaseController.py
# @Software : PyCharm
# -*- coding = utf-8 -*-
# @Time : 2024/3/17 10:48
# @Author : EquipmentADV
# @File : 数据库模块_v10.py
# @Software : PyCharm

# 2.3新增松鼠数据接口数据补充
# 2.3.4 大通用数据获取 带时间戳的数据获取

import os
import re
import shutil
import glob
from pathlib import Path
from typing import Callable, Optional, Sequence

import numpy as np
import pandas as pd
import lib
import mplfinance as mpf
import matplotlib.pyplot as plt
import warnings
import data_getter.bina_data_get as bina
from datetime import datetime

from lib import logging

from urllib3.exceptions import InsecureRequestWarning
import requests
from ssquant.SQDATA import TakeData


class BaseMarketDataSource:
    """行情数据源适配器基类，统一 fetch 接口。"""

    def fetch(
        self,
        symbol: str,
        cycle: str,
        market: str,
        specific: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        子类需实现的核心方法：
          - 根据传入参数返回标准 DataFrame（包含时间列与 OHLCV）。
        """
        raise NotImplementedError


class MarketDataIngestor:
    """
    行情入库协调器：负责“取数 -> 标准化 -> 校验 -> 落盘”全流程。

    示例：
    torage = FinancialDataStorage(root_dir='D:/Work/Quant/financial_database')
    ingestor = MarketDataIngestor(storage)
    source = SomeApiSource(...)
    ingestor.ingest_symbol(source, market='Crypto_data', symbol='BTCUSDT', cycle='1m')
    """

    def __init__(self, storage: "FinancialDataStorage") -> None:
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
    ) -> str:
        """
        从数据源拉取行情并保存到本地目录。

        参数
        ----
        source     : 实现 BaseMarketDataSource.fetch 的数据源适配器。
        market     : 目标市场目录，例如 'SS_stock_data'、'Crypto_data'。
        symbol     : 品种名称，应与目录结构一致。
        cycle      : 周期字符串，将通过 FinancialDataStorage.validate_cycle 标准化。
        specific   : 合成标记或子目录，默认 'original'。
        start/end  : 可选起止时间，传递给数据源作为取数范围。
        file_type  : 落盘格式，默认 'csv'。
        preprocess : 可选 DataFrame 预处理回调（如增补字段、复权等）。
        time_column: 指定时间列名称，可在落盘前调用 filter_df_by_date。

        返回
        ----
        str : 成功写入的本地文件完整路径。
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
        return os.path.join(target_dir, filename)


class CsvLocalSource(BaseMarketDataSource):
    """
    示例数据源：从本地 CSV 目录读取行情，用于测试入库流程或补录数据。

    目录结构需与 FinancialDataStorage 保持一致，例如：
    <root>/<market>/<symbol>/<cycle>/<specific>/<symbol>_<cycle>.csv
    """

    def __init__(self, root_dir: str, encoding: str = "utf-8") -> None:
        self.root_dir = root_dir
        self.encoding = encoding

    def fetch(
        self,
        symbol: str,
        cycle: str,
        market: str,
        specific: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        path = Path(self.root_dir) / market / symbol / cycle / specific
        filename = f"{symbol}_{cycle}.csv"
        file_path = path / filename
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        df = pd.read_csv(file_path, encoding=self.encoding)
        return df


def convert_time_format(time_str):
    """转换时间格式为标准格式 YYYY-MM-DD HH:MM"""
    return pd.to_datetime(time_str)

def _to_datetime64_ns(s: pd.Series) -> pd.Series:
    """
    功能：把任意常见格式的时间列转换为 pandas 的 datetime64[ns]（naive）。
    适配输入：
      1) 数值：自动判断毫秒(>=1e12)或秒([1e9,1e12))时间戳；其余用 to_datetime 兜底
      2) 字符串：
         - 长度14：'%Y%m%d%H%M%S'（如 '20180108000000'）
         - 长度8 ：'%Y%m%d'（如 '20180108'）=> 补当日 00:00:00
         - 其他  ：交给 pandas 自动解析（如 '2020/9/15 15:00', '2018-01-08 00:00:00'）
    返回：datetime64[ns] 的 Series（可能含 NaT）
    """
    # 数值型：优先按时间戳处理
    if np.issubdtype(s.dtype, np.number):
        ms_mask = s >= 1_000_000_000_000
        sec_mask = (~ms_mask) & (s >= 1_000_000_000)
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        if ms_mask.any():
            out.loc[ms_mask] = pd.to_datetime(s.loc[ms_mask], unit="ms", errors="coerce")
        if sec_mask.any():
            out.loc[sec_mask] = pd.to_datetime(s.loc[sec_mask], unit="s", errors="coerce")
        # 其余数值兜底解析（很少用）
        rest = ~(ms_mask | sec_mask)
        if rest.any():
            out.loc[rest] = pd.to_datetime(s.loc[rest], errors="coerce")
        return out

    # 字符串类：按长度优先匹配紧凑格式，再兜底自动解析
    s_str = s.astype(str).str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    m14 = s_str.str.len() == 14
    if m14.any():
        out.loc[m14] = pd.to_datetime(s_str.loc[m14], format="%Y%m%d%H%M%S", errors="coerce")

    m8 = s_str.str.len() == 8
    if m8.any():
        # 仅日期：按 '%Y%m%d' 解析，得到当日 00:00:00
        out.loc[m8] = pd.to_datetime(s_str.loc[m8], format="%Y%m%d", errors="coerce")

    others = ~(m14 | m8)
    if others.any():
        # pandas 自动解析各类分隔格式（'2020/9/15 15:00'、'2018-01-08 00:00:00' 等）
        out.loc[others] = pd.to_datetime(s_str.loc[others], errors="coerce")

    return out


class FinancialDataStorage:
    # TODO:初始化没有统一构建
    # TODO:对于symbol, cycle, specific等常用参数进行可行性检验的统一参数
    def __init__(self, root_dir, initialize=False):
        self.directories = [
            "Futures_data",
            "SS_stock_data",
            "Index_data",
            "US_stock_data",
            "H_stock_data",
            "Crypto_data"
        ]
        self.directory_mapping = {
            'Futures_data': ['Futures_data', '期货', 'futures', 'FUTURES', 'F'],
            'SS_stock_data': ['SS_stock_data', 'A股', '上证股票', '上证', 'ss_stock', 'ss', 'SS'],
            'Index_data': ['Index_data', '指数', 'index', 'IDX'],
            'US_stock_data': ['US_stock_data', '美股', 'us_stock', 'US'],
            'H_stock_data': ['H_stock_data', '港股', 'h_stock', 'HK'],
            'Crypto_data': ['Crypto_data', '加密货币', 'crypto', 'Crypto', 'CRYPTO', 'cy', 'Cy', 'CY']
        }

        self.cycle_list = ['1min', '30min','1h', '1d']
        self.cycle_mapping = {
            '1min': ['1min', '1M', '1m', '1Min', '1MIN', '1MiN'],
            '30min': ['30min', '30m', '30M', '30Min'],
            '1h': ['1h', '1H', '60min', '60M', '60m'],
            '1day': ['1day', '1D', '1d', '1Day', '1DAY', '1DaY']
        }

        self.cycle_expected_counts = {
            '1m': 1440,
            '5m': 288,
            '15m': 96,
            '30m':48,
            '1h': 24,
            '1d': 1,
            # ……
        }

        self.specific_list = ['主力连续', '次主力连续', '888', 'original', 'original-daly']
        self.specific_mapping = {
            '主力连续': ['主力连续', '主力', '主连', 'Main', 'continuous_main'],
            '次主力连续': ['次主力连续', '次主力', '次连', 'Sub', 'continuous_sub'],
            '888': ['888', 'recent', 'latest'],
            'original': ['original', 'o', 'O', 'org'],
            'original-daly': ['original-daly', 'daly']
        }

        self.file_type_list = ['csv', 'pkl']

        self.cycle = '1min'
        self.specific = '主力连续'
        self.file_type = 'pkl'
        self.market = 'Futures_data'

        self.root_dir = root_dir
        self.market_symbol_map = {}

        # 确保根目录存在
        if not lib.ensure_directory(self.root_dir):
            logging.warning(f"根目录不存在,已创建:{self.root_dir}")
        if initialize:
            self.ensure_directory_structure()

        #
        self._session = requests.Session()
        self._session.verify = False
        # 2) 全局关闭 InsecureRequestWarning
        warnings.filterwarnings(
            'ignore',
            message = 'Unverified HTTPS request',
            category = InsecureRequestWarning
        )

        # 检测品种
        self.scan_markets()
        logging.info("行情数据库初始化完成")

    def _build_target_dir(self, market: str, symbol: str, cycle: str, specific: str) -> str:
        path = os.path.join(self.root_dir, market, symbol, cycle)
        if specific:
            path = os.path.join(path, specific)
        os.makedirs(path, exist_ok = True)
        if not lib.ensure_directory(path):
            logging.info(f"合成路径不存在，创建文件夹: {path}")
        return path

    @staticmethod
    def _normalize_extension(file_type: str) -> str:
        return file_type if file_type.startswith('.') else f".{file_type}"

    def _build_filename(
        self,
        market: str,
        symbol: str,
        cycle: str,
        specific: str,
        file_type: str,
    ) -> str:
        ext = self._normalize_extension(file_type)
        if market == "Futures_data":
            if specific in self.specific_list and specific != '888':
                return f"{symbol}{specific}合成{ext}"
            if specific in self.specific_list or specific == '888':
                return f"{symbol}888{ext}"
            re_res = re.match(r"^([a-zA-Z]{0,2})(\d{3,4})", specific)
            if not re_res:
                raise ValueError(f"无效的 specific 参数: {specific}")
            return f"{symbol}{re_res.group(2)}{ext}"
        if market in {"Crypto_data", "Index_data", "SS_stock_data", "US_stock_data", "H_stock_data"}:
            return f"{symbol}_{cycle}{ext}"
        raise ValueError(f"暂未支持的 market: {market}")

    def _save_dataframe(
        self,
        df: pd.DataFrame,
        target_dir: str,
        *,
        symbol: str,
        cycle: str,
        specific: str,
        market: str,
        file_type: str,
        overwrite: bool = False,
    ) -> None:
        ext = self._normalize_extension(file_type)
        filename = self._build_filename(market, symbol, cycle, specific, file_type)
        file_path = os.path.join(target_dir, filename)
        if os.path.exists(file_path) and not overwrite:
            raise FileExistsError(f"{file_path} 已存在，若需覆盖请设置 overwrite=True")

        df_to_save = df.copy()
        if ext == '.csv':
            df_to_save.to_csv(file_path, index=False)
        elif ext == '.pkl':
            df_to_save.to_pickle(file_path)
        else:
            raise ValueError(f"不支持的文件类型: {file_type}")

    def _parse_date(self, date_str: str) -> datetime:
        # 支持 'YYYY-MM-DD' 或 'YYYYMMDD' 两种格式
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        raise ValueError(f"Invalid date format: {date_str}. Expect 'YYYY-MM-DD' or 'YYYYMMDD'.")

    def validate_cycle(self, cycle_input):
        """
        检查和标准化周期输入。

        :param cycle_input: 原始输入的周期字符串
        :return: 标准化后的周期字符串
        :raises ValueError: 如果输入的周期不符合任何已定义的周期格式
        """
        if cycle_input in self.cycle_list:
            return cycle_input

        # 遍历映射字典，查找匹配项
        for standard, variations in self.cycle_mapping.items():
            if cycle_input in variations:
                return standard  # 返回匹配的标准周期格式
        # 如果没有找到匹配项，抛出异常
        raise ValueError(f"Unsupported cycle input: {cycle_input}")

    def validate_specific(self, specific_input):
        """
        检查和标准化 specific 输入。

        :param specific_input: 原始输入的 specific 字符串
        :return: 标准化后的 specific 字符串
        :raises ValueError: 如果输入的 specific 不符合任何已定义的格式
        """
        if specific_input in self.specific_list:
            return specific_input

        for standard, variations in self.specific_mapping.items():
            if specific_input in variations:
                return standard  # 返回匹配的标准 specific 格式

        raise ValueError(f"Unsupported specific input: {specific_input}")

    def validate_market(self, directory_input):
        """
        检查和标准化目录输入。

        :param directory_input: 原始输入的目录字符串
        :return: 标准化后的目录字符串
        :raises ValueError: 如果输入的目录不符合任何已定义的格式
        """
        if directory_input in self.directory_mapping:
            return directory_input

        for standard, variations in self.directory_mapping.items():
            if directory_input in variations:
                return standard  # 返回匹配的标准目录格式

        raise ValueError(f"Unsupported directory input: {directory_input}")

    def change_root_dir(self, new_root_dir):
        """
        更改金融数据存储的根目录路径。
        如果目录不存在，则创建目录，并更改路径。
        修改成功返回0, 目录不存在返回1。

        :param new_root_dir: 新的根目录路径。
        :return: 修改结果状态码。
        """
        if not os.path.exists(new_root_dir):
            os.makedirs(new_root_dir)
            logging.warning(f"目录 {new_root_dir} 不存在，已创建。")
        self.root_dir = new_root_dir
        logging.info(f"根目录已更改为 {new_root_dir}。")
        return 0  # 成功更改根目录

    def ensure_directory_structure(self):
        """
        确保根目录下存在指定的文件夹结构。
        """

        for directory in self.directories:
            if not lib.ensure_directory(os.path.join(self.root_dir, directory)):
                logging.info(f"创建文件夹: {os.path.join(self.root_dir, directory)}")

    def scan_markets(self):
        """
        遍历所有市场目录，存储各市场对应的品种(symbol)。
        数据存储在 self.market_symbol_map 字典中。
        """
        market_symbol_map = {}

        for market in self.directories:
            market_path = os.path.join(self.root_dir, market)
            if os.path.exists(market_path) and os.path.isdir(market_path):
                # 获取该市场下的所有品种
                symbols = [item for item in os.listdir(market_path) if os.path.isdir(os.path.join(market_path, item))]
                market_symbol_map[market] = set(symbols)

        self.market_symbol_map = market_symbol_map  # 存储到 self
        logging.info(f"市场扫描完成: {self.market_symbol_map}")
        return market_symbol_map

    def get_market_by_symbol(self, symbol):
        """
        根据给定的品种(symbol)，返回其所属市场(market)。
        :param symbol: 品种名称 (如 'AAPL'、'BTC'、'CU' 等)
        :return: 该品种所属的市场名称 (如 'Crypto', 'Futures_data')，找不到则报 Warning 并返回 None
        """
        for market, symbols in self.market_symbol_map.items():
            if symbol in symbols:
                return market

        # 如果找不到品种，发出警告
        warnings.warn(f"Warning: 品种 '{symbol}' 未找到，可能不存在于数据库中。", category = UserWarning)
        return None  # 返回 None，保持方法的逻辑一致

    def data_entry(self, source_dir, cycle="1min", target_dir=""):
        """
        数据初步录入函数。

        :param cycle:
        :param source_dir: 数据读取目录。
        :param target_dir: 目标文件夹目录。
        :return: 返回包含错误路径的列表。
        """
        error_path = []
        # 正则表达式，匹配形如 "任意字母数字n 任意数字4 .csv"
        pattern = re.compile(r"([a-zA-Z]{1,2})(\d{3,4})\.csv$")
        # 扩展匹配 "主力连续" 或 "次主力连续"
        extended_pattern = re.compile(r"([a-zA-Z]{1,2})(主力连续|次主力连续)\.csv$")

        target_dir = os.path.join(self.root_dir, "Futures_data")

        for filename in os.listdir(source_dir):
            file_path = os.path.join(source_dir, filename)

            # 检查是否为.csv文件
            if not filename.endswith('.csv'):
                error_path.append((file_path, "not .csv"))
                continue

            # 使用正则表达式匹配文件名
            match = pattern.match(filename)
            extended_match = extended_pattern.match(filename)
            if match:
                varieties, date = match.groups()
            elif extended_match:
                varieties, type_ = extended_match.groups()
            else:
                error_path.append((file_path, "filename pattern mismatch"))
                continue

            target_folder = os.path.join(target_dir, varieties)
            target_folder = os.path.join(target_folder, cycle)
            if extended_match:  # 处理 "主力连续" 或 "次主力连续" 的情况
                target_folder = os.path.join(target_folder, type_)

            # 检查目标文件夹是否存在，不存在则创建
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)

            # 复制文件到目标文件夹
            target_file_path = os.path.join(target_folder, filename)
            # 检查文件是否存在，存在则添加后缀
            if extended_match:
                # 主连/次连数据重复存储
                copy_counter = 1
                while os.path.exists(target_file_path):
                    name_part, ext = os.path.splitext(filename)
                    target_file_path = os.path.join(target_folder, f"{name_part}-{copy_counter}{ext}")
                    copy_counter += 1
            elif os.path.exists(target_file_path):
                # 月数据不重复存储
                error_path.append((file_path, "file exists"))
            shutil.copyfile(file_path, target_file_path)

        return error_path

    # TODO:没用了,应当删除,现做保留以参考

    def single_data_entry(self, data, symbol_type, symbol, cycle, check=False):
        """
        导入单个行情文件
        :param data: DataFrame 数据
        :param symbol_type: 标的所属分类
        :param symbol: 标的名称
        :param cycle: 标的周期
        :param check: 是否校验文件
        """

        # 验证并获取正确的周期
        real_cycle = self.validate_cycle(cycle)  # 假设此函数已实现，且返回标准周期格式

        # 检查并创建文件夹路径
        if symbol_type == 'Futures_data':
            symbol_match = re.compile(r"^([a-zA-Z]{1,2})(\d{3,4})$").match(symbol)
            if not symbol_match:
                raise ValueError("Symbol does not match the expected pattern.")
            real_symbol = symbol_match.group(1)
            real_symbol_number = symbol_match.group(2)
            data_file_path = os.path.join(self.root_dir, symbol_type, real_symbol, real_cycle, real_symbol_number)
        else:
            data_file_path = os.path.join(self.root_dir, symbol_type, symbol, real_cycle)

        # 检查并创建路径
        if not os.path.exists(data_file_path):
            os.makedirs(data_file_path)
            logging.info(f"Created directory: {data_file_path}")

        # 定义文件路径
        data_path = os.path.join(data_file_path, symbol)
        csv_path = data_path + '.csv'
        pkl_path = data_path + '.pkl'

        # 检查文件是否存在
        if os.path.isfile(csv_path) and check:
            logging.warning(f"File {csv_path} already exists and will not be overwritten.")
            return

        # 保存数据到CSV和Pickle
        data.to_csv(csv_path, index = False)
        logging.info(f"Data saved to CSV file: {csv_path}")

        data.to_pickle(pkl_path)
        logging.info(f"Data saved to pickle file: {pkl_path}")

    # 计算复权数据
    def apply_adjustment_factors(self, data):

        # 获取第一行数据
        first_row = data.iloc[0]

        # 检查小数位数的函数
        def check_decimal_places(value):
            if pd.isna(value):
                return 0
            value_str = f"{value:.10f}"  # 转换为字符串，保留足够多的小数位以检查
            return len(value_str.split('.')[1].rstrip('0'))  # 移除尾随的0并计算小数位数

        # 检查第一行的每个价格的小数位数，并保留最大值
        decimal_places = max(check_decimal_places(first_row['open']),
                             check_decimal_places(first_row['high']),
                             check_decimal_places(first_row['low']),
                             check_decimal_places(first_row['close']))
        # 使用向量化操作计算复权因子
        # 初始化一个与数据等长的复权因子数组，初始值为1
        factors = pd.Series(1, index = data.index)

        # 找到合约变化的位置
        contract_changes = data['symbol'] != data['symbol'].shift(1)

        # 计算复权因子
        factors[contract_changes] = data['close'].shift(1) / data['open'][contract_changes]
        factors = factors.cumprod()
        factors.iloc[0] = 1  # 确保第一行的复权因子为1

        # 应用复权因子
        data[['open', 'high', 'low', 'close']] = data[['open', 'high', 'low', 'close']].multiply(factors,
                                                                                                 axis = 0).round(
            decimal_places)
        data['rbt'] = factors
        return data

    # TODO:复权存在哪没写

    def describe_df(self, df):
        """
        打印给定 DataFrame 的详细描述，包括列名、数据类型、缺失值数量、唯一值数量、内存使用情况，以及对索引的描述。
        :param df: 要描述的 pandas DataFrame
        """
        # 创建一个新的 DataFrame 来存储信息
        description = pd.DataFrame(index = df.columns)
        description['Data Type'] = df.dtypes  # 数据类型
        description['Non-Null Count'] = df.notnull().sum()  # 非空值计数
        description['Null Count'] = df.isnull().sum()  # 空值计数
        description['Unique Count'] = df.nunique()  # 唯一值计数
        description['Memory Usage'] = df.memory_usage(deep = True)  # 内存使用

        # 索引描述
        index_desc = {
            'Index Type': type(df.index),
            'Index Dtype': df.index.dtype,
            'Num Levels' if isinstance(df.index, pd.MultiIndex) else 'Index Length': len(df.index),
            'Is Unique': df.index.is_unique,
            'Memory Usage': df.index.memory_usage(deep = True)
        }

        # 打印索引信息
        print("Index Description:")
        for key, value in index_desc.items():
            print(f"{key}: {value}")

        # 显示列的 DataFrame
        print("\nColumn Description:")
        print(description)

        # 可选：返回这个描述性 DataFrame，以便需要时可以用于进一步分析
        return description

    def continuous_index_composition(self, folder_path, symbol):
        """
        主连信息检索,合并多个 DataFrame，按时间排序，并保存为 CSV 和 pickle 文件
        :param folder_path:合并的路径
        :param symbol:标的名称
        :return:
        """
        # 定义新的列名称
        new_columns = ['market_code', 'symbol', 'time', 'open', 'high', 'low', 'close', 'amount', 'volume', 'stock']

        # 获取文件夹中所有的CSV文件路径
        # csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        pattern = re.compile(r'^[a-zA-Z]+(?:主力连续|次主力连续)(?:-\d+)?\.csv$')  # 正则表达式匹配所有.csv文件
        csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if pattern.match(f)]

        logging.info("csv_files:", csv_files)

        in_df_list = []

        for file_path in csv_files:
            # 读取CSV文件内容
            df = pd.read_csv(file_path, encoding = 'gbk')
            # self.describe_dataframe(df)

            # 更新列名称
            df.columns = new_columns

            # 转换时间格式
            df['time'] = df['time'].apply(convert_time_format)

            # 转换数据类型为浮点型
            float_columns = ['open', 'high', 'low', 'close', 'amount', 'volume', 'stock']
            df[float_columns] = df[float_columns].astype(float)
            self.describe_df(df)

            # 打印每个DataFrame的第一行和最后一行数据的时间
            in_df_list.append(df)
            print(f"File: {os.path.basename(file_path)}")
            print(f"First Time: {df['time'].iloc[0]}")
            print(f"Last Time: {df['time'].iloc[-1]}\n")

        # 合并 DataFrame
        combined_df = pd.concat(in_df_list, ignore_index = True)
        # 按时间列排序
        combined_df.sort_values('time', inplace = True)

        # 检查时间重复
        if combined_df['time'].duplicated().any():
            logging.warning("存在重复的时间项，处理前请先解决这些问题。")
            duplicated_times = combined_df[combined_df['time'].duplicated(keep = False)]
            print(duplicated_times)  # 打印出重复的时间项，便于调试
            return  # 返回前结束函数

        logging.info("combined_df:")
        self.describe_df(combined_df)

        # 生成完整的文件路径
        file_name = str(symbol) + os.path.basename(folder_path) + "合成"

        csv_path = os.path.join(folder_path, f"{file_name}.csv")
        pkl_path = os.path.join(folder_path, f"{file_name}.pkl")

        # 保存为 CSV 文件
        combined_df.to_csv(csv_path, index = False)
        logging.info(f"数据已保存到 CSV 文件：{csv_path}")

        # 保存为 pickle 文件
        combined_df.to_pickle(pkl_path)
        logging.info(f"数据已保存到 pickle 文件：{pkl_path}")

    def get_specific_file(self, symbol: str, cycle: str, specific: str, file_type: str, market: str = "Futures_data",
                          check_only: bool = False,
                          time_process: bool = False) -> pd.DataFrame or bool:
        """
        读取或检查指定目录中的特定文件，并根据 `time_process` 处理时间列。

        :param symbol: 品种的名称，如 'i'（铁矿石）。
        :param cycle: 数据的时间周期，如 '1min'。
        :param specific: 具体表参数，如 '主力连续' '次主力连续' 或 'i2108' ‘888’。
        :param file_type: 文件类型，如 'csv' 或 'pkl'。
        :param market: 市场类型。
        :param check_only: 如果为 True，则只检查文件是否存在，不实际读取。
        :param time_process: 是否自动对数据中的时间列转换为 datetime64[ns]。
        :return: DataFrame 对象，如果没有找到文件则返回 None；如果 check_only 为 True，返回布尔值。
        """
        cycle = self.validate_cycle(cycle)
        specific = self.validate_specific(specific)
        market = self.validate_market(market)
        logging.debug(f"market:{market}")
        target_dir = os.path.join(self.root_dir, market, symbol, cycle)

        # 根据市场查看对应的参数
        if market == "Futures_data":
            if specific in self.specific_list or re.match(r"(888)", specific):
                specific_dir = specific if specific in self.specific_list and specific != '888' else '888'
                target_dir = os.path.join(target_dir, specific_dir)
                file_name = f"{symbol}{specific + '合成'}.{file_type}" if specific in self.specific_list and specific != '888' else f"{symbol}888.{file_type}"
            else:
                re_res = re.match(r"^([a-zA-Z]{0,2})(\d{3,4})", specific)
                if not re_res:
                    raise ValueError("无效的 specific 参数")
                file_name = f"{symbol}{re_res.group(2)}.{file_type}"

        elif market == "Crypto_data":
            if specific != "original":
                raise ValueError("无效的 specific 参数")
            else:
                target_dir = os.path.join(target_dir, specific)
                file_name = f"{symbol}_{cycle}.{file_type}"
        elif market == "Index_data":
            if specific != "original":
                raise ValueError("无效的 specific 参数")
            else:
                target_dir = os.path.join(target_dir, specific)
                file_name = f"{symbol}_{cycle}.{file_type}"
        elif market == "SS_stock_data":
            if specific != "original":
                raise ValueError("无效的 specific 参数")
            else:
                target_dir = os.path.join(target_dir, specific)
                file_name = f"{symbol}_{cycle}.{file_type}"
        else:
            raise ValueError(f"参数 market: {market} 不合法或暂未支持")

        if not os.path.exists(target_dir):
            if not check_only:
                logging.warning(f"目录 {target_dir} 不存在。")
            return None if not check_only else False

        file_path = os.path.join(target_dir, file_name)

        if not os.path.exists(file_path):
            if not check_only:
                logging.warning(f"文件 {file_path} 不存在。")
            return None if not check_only else False

        if check_only:
            return True

        # 读取文件
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'pkl':
            df = pd.read_pickle(file_path)
        else:
            raise ValueError(f"不支持的文件类型 {file_type}。")

        # 处理时间列
        if time_process:
            time_columns = [col for col in df.columns if col.lower() in ['open_time', 'time', 'datetime']]
            for col in time_columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors = 'coerce')  # 处理时间格式，遇到错误填充 NaT
                    # print(f"列 {col} 已转换为 datetime64[ns] 格式")
                except Exception as e:
                    logging.error(f"时间列 {col} 转换失败: {e}")

        df.reset_index(drop = True, inplace = True)
        return df

    def set_simpleGet_param(self, cycle=None, specific=None, file_type=None, market=None):
        if cycle:
            logging.info(f"cycle set:{self.cycle}->{cycle}")
            self.cycle = cycle
        if specific:
            logging.info(f"specific set:{self.specific}->{specific}")
            self.specific = specific
        if file_type:
            logging.info(f"file_type set:{self.file_type}->{file_type}")
            self.file_type = file_type
        if market:
            logging.info(f"market set:{self.market}->{market}")
            self.market = market

    def simple_get(self, symbol, cycle=None, specific=None, file_type=None, market=None):
        if not cycle:
            cycle = self.cycle
        if not specific:
            specific = self.specific
        if not file_type:
            file_type = self.file_type
        if not market:
            market = self.market
            logging.debug(f"market set:{self.market}->{market}")
        return self.get_specific_file(symbol, cycle, specific, file_type, market = market)

    def simple_get_last(self, symbol, cycle=None, specific=None, file_type=None, market=None):
        df = self.simple_get(symbol, cycle, specific, file_type, market)
        return df.tail(1)

    def get_date_with_time(self):
        # todo:按照时间获取数据
        pass

    def list_varieties(self, symbol=None, contract_number=None):
        """
        列出指定或所有品种及其合约号在不同周期下的数据文件情况。

        该方法遍历指定目录下的品种和合约号，检查每个品种的每个合约号在给定周期下的数据文件是否存在。
        如果未指定品种和合约号，则默认检查所有可用的品种和合约号。

        :param symbol: 可选，品种的名称，如 'i'（铁矿石）。如果未指定，则检查所有品种。
        :param contract_number: 可选，合约号或合成类型，如 '主力连续'、'次主力连续'、'888' 或具体合约号如 'i2108'。
                                如果未指定，则检查所有默认的合约号和合成类型。
        :return: 包含所有找到的品种、合约号及其对应周期的字典。
                 字典格式为 {品种: {合约号: {周期集合}}}。
        """

        varieties_info = {}

        # 如果未指定品种，则获取所有品种
        symbols = [symbol] if symbol else os.listdir(os.path.join(self.root_dir, "Futures_data"))

        for sym in symbols:
            symbol_dir = os.path.join(self.root_dir, "Futures_data", sym)
            if not os.path.isdir(symbol_dir):
                continue

            # 如果未指定合约号，获取所有合约号
            contract_numbers = [contract_number] if contract_number else self.specific_list + ['888']
            found_any = False  # 用于跟踪是否找到任何文件

            for cn in contract_numbers:
                for cycle in self.cycle_list:
                    csv_exists = self.get_specific_file(sym, cycle, cn, 'csv', check_only = True)
                    pkl_exists = self.get_specific_file(sym, cycle, cn, 'pkl', check_only = True)

                    if csv_exists or pkl_exists:
                        found_any = True
                        if sym not in varieties_info:
                            varieties_info[sym] = {}
                        if cn not in varieties_info[sym]:
                            varieties_info[sym][cn] = set()
                        varieties_info[sym][cn].add(cycle)

            # 如果未找到任何文件，则不将该品种添加到输出中
            if not found_any:
                continue

        for variety, contracts in varieties_info.items():
            contract_descriptions = [f"{cn}({','.join(sorted(cycles))})" for cn, cycles in contracts.items()]
            logging.info(f"{variety}-[{', '.join(contract_descriptions)}]")

        return varieties_info

    def filter_df_by_date(
            self,
            df: pd.DataFrame,
            start_date: Optional[str],
            end_date: Optional[str],
            time_columns: Sequence[str] = ("time", "open_time", "datetime"),
            assume_sorted: bool = True,
            allow_sort: bool = False,
    ) -> pd.DataFrame:
        """
        功能：按时间半开区间 [start_date, end_date) 截取数据，并把时间列统一为 datetime64[ns]。
        上游：任意时间格式的行情/因子 DataFrame
        下游：回测/统计等需要时间为 datetime 的模块

        参数：
          df            : 输入数据；推荐已按时间升序（assume_sorted=True）
          start_date    : 起始（含）；字符串或 None；仅日期则视为当日 00:00:00
          end_date      : 终止（不含）；字符串或 None；仅日期则视为当日 00:00:00
          time_columns  : 候选时间列名顺序（默认 'time'、'open_time'、'datetime'）
          assume_sorted : 视为时间已单调递增，使用二分切片以提升性能
          allow_sort    : 若检测到不单调，是否先排序再使用二分；否则退回布尔掩码

        返回：
          过滤后的 DataFrame（不会修改原 df），其中时间列 dtype 为 datetime64[ns]。
        """
        if df.empty:
            logging.info("输入 DataFrame 为空。")
            return df.copy()

        # 1) 选出时间列
        time_name = next((c for c in time_columns if c in df.columns), None)
        if time_name is None:
            raise ValueError(f"未找到时间列，候选：{list(time_columns)}")

        # 2) 统一转为 datetime64[ns]
        if not np.issubdtype(df[time_name].dtype, np.datetime64):
            dft = df.copy()
            dft[time_name] = _to_datetime64_ns(df[time_name])
        else:
            dft = df.copy()

        # 3) 解析区间边界（允许 None；仅日期自动为 00:00:00）
        start_dt = pd.to_datetime(start_date) if start_date else None
        end_dt = pd.to_datetime(end_date) if end_date else None
        if start_dt is not None and end_dt is not None and not (start_dt < end_dt):
            raise ValueError(f"无效区间：start({start_dt}) 需早于 end({end_dt})")

        # 4) 打印原始区间信息（注意 NaT 的情况）
        def _fmt(x):
            try:
                return x.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(x)

        logging.info(
            f"原始数据 {len(dft)} 行，时间列 '{time_name}'；"
            f"最小：{_fmt(dft[time_name].min())}，最大：{_fmt(dft[time_name].max())}"
        )

        # 5) 执行区间筛选：优先二分切片（更快），否则布尔掩码
        if assume_sorted:
            mono = dft[time_name].is_monotonic_increasing
            if not mono and allow_sort:
                dft = dft.sort_values(time_name, kind = "mergesort").reset_index(drop = True)
                mono = True

            if mono:
                vals = dft[time_name].values  # numpy datetime64[ns]
                left = 0
                right = len(dft)
                if start_dt is not None:
                    left = vals.searchsorted(np.datetime64(start_dt), side = "left")
                if end_dt is not None:
                    right = vals.searchsorted(np.datetime64(end_dt), side = "left")
                out = dft.iloc[left:right]
            else:
                # 退回布尔掩码
                mask = np.ones(len(dft), dtype = bool)
                if start_dt is not None:
                    mask &= (dft[time_name] >= start_dt)
                if end_dt is not None:
                    mask &= (dft[time_name] < end_dt)
                out = dft.loc[mask]
        else:
            mask = np.ones(len(dft), dtype = bool)
            if start_dt is not None:
                mask &= (dft[time_name] >= start_dt)
            if end_dt is not None:
                mask &= (dft[time_name] < end_dt)
            out = dft.loc[mask]

        logging.info(
            f"筛选区间 [{_fmt(start_dt) if start_dt is not None else '-inf'}, "
            f"{_fmt(end_dt) if end_dt is not None else '+inf'}) -> {len(out)} 行"
        )

        # 6) 返回：保证时间列 dtype 为 datetime64[ns]
        # （此时已是 datetime64[ns]；直接返回）
        return out

    def split_data_into_sets(self, df, train_ratio, test_ratio, forecast_horizon=1,
                             feature_columns=None, return_timestamp=False):
        """
        根据提供的比例，将数据帧分割为训练集、测试集和验证集，并为每个数据集计算收益率和特征集。

        :param df: pd.DataFrame 要划分的数据帧。
        :param train_ratio: int 训练集所占的百分比。
        :param test_ratio: int 测试集所占的百分比。
        :param forecast_horizon: int, 默认 1 预测步长。
        :param feature_columns: list 具体使用的特征列名称列表。如果未指定，则使用默认的特征列。
        :param return_timestamp: bool, 默认 False 是否在返回值中包含时间戳。
        :return: list 包含三个元组，每个元组包括X_train, y_train和可选的时间戳列表。
        """

        # 确定要使用的特征列，如果未指定，则使用默认特征列
        features = feature_columns or ['open', 'high', 'low', 'close', 'amount']
        # logging.info(f"features:{features}")

        # 如果需要返回时间戳，但时间列不存在，则抛出异常
        if return_timestamp and ('time' not in df.columns and 'open_time' not in df.columns and 'datetime' not in df.columns):
            raise ValueError("时间戳列 'time' 不存在于数据帧中，但要求返回时间戳。")
        else:
            if 'time' in df.columns:
                time_name = 'time'
            elif 'open_time' in df.columns:
                time_name = 'open_time'
            elif 'datetime' in df.columns:
                time_name = 'datetime'
            else:
                raise ValueError("时间戳列 'time' 或 'open_time' 不存在于数据帧中")
        # 计算每个数据集的分界点
        total_rows = len(df)
        train_end_idx = int(total_rows * (train_ratio / 100))
        test_end_idx = train_end_idx + int(total_rows * (test_ratio / 100))

        # 划分数据集为训练集、测试集和验证集
        split_datasets = [
            df.iloc[:train_end_idx],  # 训练集
            df.iloc[train_end_idx:test_end_idx],  # 测试集
            df.iloc[test_end_idx:] if train_ratio + test_ratio < 100 else pd.DataFrame()  # 验证集
        ]

        # 处理每个数据集：计算特征矩阵和目标向量
        processed_datasets = []
        for subset in split_datasets:
            if not subset.empty:
                subset = subset.copy()
                # 计算预测目标收益率
                subset['ret'] = subset['close'].pct_change(periods = forecast_horizon).shift(-forecast_horizon)
                subset.dropna(subset = ['ret'], inplace = True)

                # 获取时间戳（如果需要）
                if return_timestamp and time_name in subset.columns:
                    timestamps = subset[time_name]
                else:
                    timestamps = None

                # 删除时间列以便创建特征矩阵
                if time_name in subset.columns:
                    subset = subset.drop(columns = [time_name])

                if time_name in features:
                    features.remove(time_name)

                # logging.info(f"features:{features}")

                # 生成特征矩阵X和目标向量y
                X = np.nan_to_num(subset[features].to_numpy())
                y = np.nan_to_num(subset['ret'].values)

                # 将处理后的结果添加到结果列表
                if return_timestamp:
                    processed_datasets.append((X, y, timestamps))
                else:

                    processed_datasets.append((X, y))

        return processed_datasets

    def file_coverage(
            self,
            market: str,
            symbol: str,
            cycle: str,
            specific: str,
            file_type: str,
            start_date: str,
            end_date: str
    ) -> dict:
        """
        调用底层工具，返回指定区间的文件统计：
          - count: 已存在文件数
          - dates: 排序后的已有日期
          - missing: 缺失日期
        """
        target = self._build_target_dir(market, symbol, cycle, specific)
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        # logging.info(f"target: {target}")
        # logging.info(f"file_type: {file_type}")
        existing = bina.get_existing_date_set(target, symbol, cycle, file_type)
        missing = bina.get_missing_dates(existing, start, end)
        return {"count": len(existing), "dates": sorted(existing), "missing": missing}

    def fetch_and_merge_data(
            self,
            symbol: str,
            cycle: str,
            specific: str,
            file_type: str,
            start_date: str,
            end_date: str
    ) -> str:
        """
        完整执行：下载缺失 → 二次扫描 → 行数校验 → 合并 → 返回 CSV 输出路径
        """
        target = self._build_target_dir("Crypto_data", symbol, cycle, specific)

        # 1. 计算缺失并下载
        coverage = self.file_coverage("Crypto_data", symbol, cycle, specific,
                                      file_type, start_date, end_date)

        bina.validate_symbol_cycle(symbol, cycle, self._session)

        for dt in coverage['missing']:
            logging.info(f"Downloading missing file for {dt:%Y-%m-%d}")
            bina.download_and_extract(symbol, cycle, dt, target,
                                      session = self._session
                                      )

        # 2. 再次扫描，确保无缺失
        coverage2 = self.file_coverage("Crypto_data", symbol, cycle, specific,
                                       file_type, start_date, end_date)
        if coverage2['missing']:
            raise RuntimeError(f"Still missing after download: {coverage2['missing']}")

        # 3. **行数校验**
        #    从 dict 里取出对应周期的预期行数
        expected = self.cycle_expected_counts.get(cycle)
        if expected is None:
            logging.warning(f"No expected-row setting for cycle {cycle}, skipping row-check.")
        else:
            dates = sorted(coverage2['dates'])
            last_date = dates[-1]
            for dt in dates:
                fn = os.path.join(
                    target,
                    f"{symbol}-{cycle}-{dt:%Y-%m-%d}"
                    + (file_type if file_type.startswith('.') else f".{file_type}")
                )
                try:
                    df = pd.read_csv(fn)
                except Exception as e:
                    raise RuntimeError(f"读取文件失败 {fn}: {e}")

                row_count = len(df)
                if dt == last_date:
                    # 最后一天：只要不超过 expected 即可
                    if row_count > expected:
                        raise RuntimeError(
                            f"{fn} 行数异常：{row_count} > {expected}（最新文件行数超出预期）"
                        )
                else:
                    # 其它所有日期：必须 exact match
                    if row_count != expected:
                        raise RuntimeError(
                            f"{fn} 行数异常：{row_count} != {expected} "
                            f"(日期 {dt:%Y-%m-%d} 应该有完整 {expected} 行)"
                        )

        # 4. 合并所有文件
        ext = file_type if file_type.startswith('.') else f".{file_type}"
        original_dir = self._build_target_dir("Crypto_data", symbol, cycle, 'original')
        os.makedirs(original_dir, exist_ok = True)
        if not lib.ensure_directory(original_dir):
            logging.info(f"创建文件夹: {original_dir}")
        output_file = os.path.join(original_dir, f"{symbol}_{cycle}{ext}")

        # 按照覆盖后的日期列表，拼出文件路径
        dates = sorted(coverage2['dates'])
        input_files = [
            os.path.join(target, f"{symbol}-{cycle}-{dt:%Y-%m-%d}{ext}")
            for dt in dates
        ]
        merged_df, count = bina.merge_csv_files(output_file, input_files)
        logging.info(f"Merged {count} files, total rows: {len(merged_df)}")
        return output_file

    def merge_with_mapping(
            self,
            input_files: list[str],
            output_file: str,
            mapping: dict[str, str] = None
    ) -> tuple[pd.DataFrame, int]:
        """
        合并并根据 mapping 重命名/重排列列。
        mapping: {new_name: old_name}
        """
        df, count = bina.merge_csv_files(output_file, input_files)
        if mapping:
            if len(set(mapping.values())) != len(mapping):
                raise ValueError("mapping values must be unique")
            # 重命名列
            df = df.rename(columns = {v: k for k, v in mapping.items()})
            # 重排列
            cols = list(mapping.keys()) + [c for c in df.columns if c not in mapping.keys()]
            df = df[cols]
            df.to_csv(output_file, index = False, encoding = "utf-8-sig")
        return df, count


class SQData_manager:
    """
    松鼠的数据库
    """

    def __init__(self, finaDC: FinancialDataStorage):
        self.finaDC = finaDC
        self.client = TakeData(username = '905071836@qq.com', password = 'xiaoluoya1234')

    def get_data(self, symbol, start_date, end_date, frequ, adjust_type):
        # 品种: symbol, 不区分大小写
        # 起始时间: start_date,
        # 结束时间: end_date(包含当天),
        # 周期kline_period: 1M..5M..NM(分钟), 1D(天), 1W(周), 1Y(月)
        # 复权adjust_type: 0(不复权) 1(后复权)
        try:
            data = self.client.get_data(
                symbol = symbol,
                start_date = start_date,
                end_date = end_date,
                kline_period = frequ,
                adjust_type = adjust_type
            )
            return data
        except Exception as e:
            logging.error(f"Error getting data for {symbol}: {str(e)}")
            return None

    def get_symbol_test(self, symbol):
        data = self.client.get_data(
            symbol = symbol,
            start_date = '2023-09-01',
            end_date = '2023-09-05',
            kline_period = '1M',
            adjust_type = 1
        )
        if data is None:
            return False
        else:
            if data.shape[0] < 1:
                return False
            else:
                return True

    def data_get_from_list(self, start_date, end_date, frequ, adjust_type, symbol_list: list = None):
        """
        批量获取数据
        :param symbol_list:
        :param start_date:开始时间
        :param end_date:结束时间
        :param frequ:频率
        :param adjust_type:{1:复权,0:不复权}
        :return:
        """
        if not symbol_list:
            symbol_list = ['i', 'hc', 'rb', 'eb', 'ts', 'sa', 'ur', 'ag', 'fg']
        for sym in symbol_list:
            symbol = sym + '888'
            if self.get_symbol_test(symbol):
                data = self.get_data(symbol, start_date, end_date, frequ, adjust_type)
                data = self.check_columns(data)
                self.finaDC.single_data_entry(data, 'Crypto_data', symbol, frequ)
            else:
                logging.warning(f"Warning: {symbol_list} can not get")

    def check_columns(self, total_df):
        total_df.rename(columns = {'datetime': 'time'}, inplace = True)
        total_df['time'] = pd.to_datetime(total_df['time'])
        return total_df


class DataQualityChecker:
    """
        扩展行情数据质量检测器
    1. 缺失值检查
    2. 平盘K线检测
    3. 零成交量检测
    4. 跳空检测
    5. 数据起止时间、长度
    6. 重复时间戳检测
    7. 时间间隔一致性检测
    针对包含 open, high, low, close, volume 列的 DataFrame，提供多维度数据质量检测。
    """
    def __init__(self, df: pd.DataFrame, freq: str = None):
        """
        初始化方法
        :param df: 包含 'open','high','low','close','volume' 列，索引必须为 DatetimeIndex
        :param freq: 预期数据频率（如 '1H','1D'），若为 None 则自动推断
        """
        required = {'open', 'high', 'low', 'close', 'volume'}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"缺少必要列: {missing}")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("索引必须是 DatetimeIndex")
        self.df = df.sort_index()
        # 推断或使用指定频率
        self.freq = freq or pd.infer_freq(self.df.index)

    def check_missing(self):
        """
        检查缺失值
        :return: dict，每列的缺失值数量
        """
        return self.df.isna().sum().to_dict()

    def check_date_range(self):
        """
        检查数据起止时间和总条数
        :return: dict，包含 'start'(起始时间),'end'(结束时间),'records'(记录总数)
        """
        start = self.df.index.min()
        end = self.df.index.max()
        length = len(self.df)
        return {'start': start, 'end': end, 'records': length}

    def check_duplicate_timestamps(self):
        """
        检测重复时间戳
        :return: dict，'duplicate_count'(重复条数),'duplicates'(重复的时间戳列表)
        """
        dup_idx = self.df.index[self.df.index.duplicated(keep=False)]
        return {
            'duplicate_count': len(dup_idx),
            'duplicates': sorted(set(dup_idx.tolist()))
        }

    def check_time_gaps(self):
        """
        检测时间间隔缺失
        :return: dict，'expected_freq'(预期频率),'missing_count'(缺失条数),'missing'(缺失时间列表)
        """
        if self.freq is None:
            return {'expected_freq': None, 'missing_count': None, 'missing': []}
        expected = pd.date_range(self.df.index.min(), self.df.index.max(), freq=self.freq)
        missing = expected.difference(self.df.index)
        return {
            'expected_freq': self.freq,
            'missing_count': len(missing),
            'missing': missing.tolist()
        }

    def check_flat_candles(self):
        """
        检测平盘K线段（open=high=low=close）
        :return: dict，'segments'(每段起止及长度列表),'max_flat_length'(最长平盘段长度)
        """
        flat = (self.df['open'] == self.df['high']) & \
               (self.df['open'] == self.df['low'])  & \
               (self.df['open'] == self.df['close'])
        segments, start, prev_ts, count = [], None, None, 0
        for ts, is_flat in zip(self.df.index, flat):
            if is_flat:
                if start is None:
                    start = ts
                    count = 1
                else:
                    count += 1
            else:
                if start is not None:
                    segments.append({'start': start, 'end': prev_ts, 'length': count})
                    start, count = None, 0
            prev_ts = ts
        if start is not None:
            segments.append({'start': start, 'end': prev_ts, 'length': count})
        max_flat = max((seg['length'] for seg in segments), default=0)
        return {'segments': segments, 'max_flat_length': max_flat}

    def check_zero_volume(self):
        """
        检测零成交量段
        :return: dict，'segments'(每段起止及长度列表),'zero_bars'(零量总条数),'zero_ratio'(零量占比)
        """
        zv = self.df['volume'] == 0
        segments, start, prev_ts, count = [], None, None, 0
        for ts, is_zero in zip(self.df.index, zv):
            if is_zero:
                if start is None:
                    start = ts
                    count = 1
                else:
                    count += 1
            else:
                if start is not None:
                    segments.append({'start': start, 'end': prev_ts, 'length': count})
                    start, count = None, 0
            prev_ts = ts
        if start is not None:
            segments.append({'start': start, 'end': prev_ts, 'length': count})
        total = len(self.df)
        zero_count = sum(seg['length'] for seg in segments)
        return {'segments': segments, 'zero_bars': zero_count, 'zero_ratio': zero_count/total}

    def detect_price_gaps(self, threshold: float = 0.2):
        """
        检测价格跳空
        :param threshold: 跳空阈值，按当日 open 与前日 close 的比例计算
        :return: list，每个跳空条目包含 'date'(跳空日期),'gap_pct'(跳空比例)
        """
        prev_close = self.df['close'].shift(1)
        gap_pct = (self.df['open'] - prev_close) / prev_close
        gaps = gap_pct[gap_pct.abs() > threshold]
        return [{'date': idx, 'gap_pct': pct} for idx, pct in gaps.items()]

    def summary(self):
        """
        汇总所有检测结果
        :return: dict，包含 'missing','date_range','duplicates','time_gaps',
                       'flat_candles','zero_volume','price_gaps'
        """
        return {
            'missing':      self.check_missing(),
            'date_range':   self.check_date_range(),
            'duplicates':   self.check_duplicate_timestamps(),
            'time_gaps':    self.check_time_gaps(),
            'flat_candles': self.check_flat_candles(),
            'zero_volume':  self.check_zero_volume(),
            'price_gaps':   self.detect_price_gaps(),
        }



# 示例用法
# df = pd.read_csv('market_data.csv', index_col=0, parse_dates=True)
# checker = DataQualityChecker(df)
# report = checker.summary()
# print(report)


def pretty_print_dict(d):
    print("\n".join(f"{k}: {v}" for k, v in d.items()))



if __name__ == "__main__":
    from Back_cta import read_json

    # 获取当前脚本所在目录，动态生成 config.json 的路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'config.json')

    # 读取 config.json
    config = read_json(config_path)
    storage = FinancialDataStorage(config.get('FinaDC_path'))

    # print(storage.market_symbol_map)
    # print(storage.get_market_by_symbol("hc"))
    # get_df = storage.get_specific_file(symbol='hc', cycle='1H', specific='888', file_type='pkl',market='Futures_data',check_only=True)
    # get_df2 = storage.get_specific_file(symbol='BTC', cycle='1H', specific='original', file_type='csv',market='Crypto_data',check_only=True)
    # print(get_df)
    # print(get_df2)
    # btc_df = storage.get_specific_file(symbol = 'BTC', cycle = '1H', specific = 'original', file_type = 'csv',
    #                                     market = 'Crypto_data', check_only = False, time_process = True)
    # print(f"btc_df:{btc_df}")

    # lib.describe_dataframe(btc_df)
    # storage.list_varieties("i")
    # storage.list_varieties("hc")
    storage.fetch_and_merge_data(
        symbol = "BTCUSDT",
        cycle = "1m",
        specific = "original-daly",
        file_type = ".csv",
        start_date = "20200101",
        end_date = "20250205"
    )

    # storage.fetch_and_merge_data(
    #     symbol = "ETHUSDT",
    #     cycle = "1h",
    #     specific = "original-daly",
    #     file_type = ".csv",
    #     start_date = "20200101",
    #     end_date = "20250205"
    # )
    #
    # storage.fetch_and_merge_data(
    #     symbol = "SOLUSDT",
    #     cycle = "1h",
    #     specific = "original-daly",
    #     file_type = ".csv",
    #     start_date = "20200915",
    #     end_date = "20250205"
    # )

    storage.scan_markets()
    print(f"market_symbol_map:{pretty_print_dict(storage.market_symbol_map)}")

    print(f"get_market_by_symbol(hc):{storage.get_market_by_symbol('hc')}")
    print(f"get_market_by_symbol(BTCUSDT):{storage.get_market_by_symbol('BTCUSDT')}")
