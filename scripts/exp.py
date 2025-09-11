# -*- codeing = utf-8 -*-
# @Time : 2025/9/9 16:55
# @Author : EquipmentADV
# @File : exp.py
# @Software : PyCharm
from xtquant import xtdata
import time

# 设定一个标的列表
code_list  = [
        # '159915.SZ',  # 创业板ETF
        '518880.SH',  # 黄金ETF
        '513880.SH',  # 恒生科技ETF
        # '513100.SH',  # 纳斯达克100ETF
        # '513030.SH',  # 德国ETF
        # '513080.SH',  # 法国CAC40ETF
        # '513180.SH',  # 日本东证ETF
        # '510300.SH',  # 沪深300ETF
        # '511010.SH',  # 国债ETF（1-3年期）
        # '159980.SZ',  # 有色ETF
        # # '513730.SH',  # 东南亚科技ETF
        # '563300.SH',  # 中证2000ETF
    ]
# 设定获取数据的周期
period = "1d"

for stock in code_list:
    xtdata.download_history_data(stock,
                                 period=period,
                                 start_time='',
                                 end_time='',
                                 incrementally=True)

get_market_data_in = xtdata.get_market_data(field_list = ['time', 'open','volume'],
                                            stock_list = code_list,
                                            period = period,
                                            start_time = '',
                                            end_time = '',
                                            count = 999999,
                                            dividend_type = 'none',
                                            fill_data = False)
print(get_market_data_in)
