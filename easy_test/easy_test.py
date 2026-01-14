from xtquant import xtdata
import pandas as pd

code = "513880.SH"
period = "1m"
start = "20000101"
end = "99991231"

# # 先下载
# xtdata.download_history_data(stock_code=code, period=period, start_time=start, end_time=end, incrementally=True)

# 再取数据
data = xtdata.get_market_data_ex(
    stock_list=[code],
    period=period,
    start_time=start,
    end_time=end,
    count=-1,
    dividend_type="none",
    fill_data=False,
    field_list=[],
)

# code->DataFrame 模式
df = None
if isinstance(data, dict):
    # 兼容两种结构
    if "time" in data and "close" in data:
        df = data["close"].loc[code].to_frame("close")
        df["time"] = data["time"].loc[code]
    else:
        for v in data.values():
            if isinstance(v, pd.DataFrame) and "close" in v.columns:
                df = v
                break

if df is None or df.empty:
    print("no data")
else:
    df["time"] = pd.to_datetime(df["time"], unit="ms", errors="coerce")  # 如是毫秒时间戳
    df = df.dropna(subset=["time"])
    print("head:")
    print(df.head(5))
    print("\ntail:")
    print(df.tail(5))
    print("max time:", df["time"].max())
