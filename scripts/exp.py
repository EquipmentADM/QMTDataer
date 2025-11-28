# -*- coding = utf-8 -*-
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

code_list = ['159915.SZ']
# # 设定获取数据的周期
# period = "1m"
period = "1m"


# for stock in code_list:
#     xtdata.download_history_data(stock,
#                                  period=period,
#                                  start_time='',
#                                  end_time='',
#                                  incrementally=True)
#
# sim_field = ['time', 'open','volume']
# get_market_data_in = xtdata.get_market_data(field_list = [],
#                                             stock_list = code_list,
#                                             period = period,
#                                             start_time = '',
#                                             end_time = '',
#                                             count = 10,
#                                             dividend_type = 'none',
#                                             fill_data = False)
# print(get_market_data_in)


def callandprint(data):

    print(f"新调用：{time.time()}")
    print(f"data:{data}")
    kline_in_callabck = xtdata.get_market_data_ex(field_list=[], stock_list=['510050.SH','159915.SZ'], period = period, count=1)  # 在回调中获取klines数据
    print(f"kline_in_callabck:{kline_in_callabck}")

getnum = xtdata.subscribe_quote('510050.SH', period='1m', callback=callandprint)
# getnum2 = xtdata.subscribe_quote('159915.SZ', period='1m', count=1, callback=callandprint)
#
# time.sleep(1)
#
# print(f"getnum: {getnum}")
# print(f"getnum2: {getnum2}")
#
# xtdata.run()
