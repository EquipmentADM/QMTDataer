from pathlib import Path
content = """# -*- coding: utf-8 -*-
"""脚本：调用 xtdata 获取历史 K 线并保存为 CSV 文件

使用方法：
    1. 确保当前环境可以 import xtquant.xtdata，并且 MiniQMT 已经登录。
    2. 根据需求修改脚本顶部的常量（标的、周期、时间窗口、输出路径等）。
    3. 直接运行：python scripts/dump_xtdata_csv.py
       执行完成后，会把查询到的宽表数据写到 SAVE_PATH 指定的 CSV 文件。

字段说明：
    CODES      标的列表，例如 ['510050.SH', '159915.SZ']；
    PERIOD     周期，例如 1m/5m/15m/30m/60m/1d（具体取决于 xtdata 支持情况）；
    START/END  时间窗口，支持 YYYYMMDD 或 YYYYMMDDHHMMSS；
    COUNT      限制返回条数，-1 表示不限制；
    SAVE_PATH  输出 CSV 路径。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

try:
    from xtquant import xtdata  # type: ignore
except Exception as exc:  # pragma: no cover
    raise RuntimeError(f"无法导入 xtquant.xtdata：{exc}")

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    raise RuntimeError(f"无法导入 pandas：{exc}")

# ------------------ 配置区域 ------------------
CODES: List[str] = ["510050.SH", "159915.SZ"]
PERIOD: str = "1m"
START: str = "20240901093000"
END: str = "20240901100000"
COUNT: int = -1
SAVE_PATH: Path = Path("dump_xtdata.csv")
# ------------------ 配置区域 ------------------


def _normalize_dataframe(data_dict: Dict[str, Any]) -> pd.DataFrame:
    """将 xtdata 返回的 {field: DataFrame} 结构转换为列式 DataFrame"""
    time_df = data_dict.get("time")
    if time_df is None:
        raise ValueError("xtdata 返回结果中缺少 'time' 字段")

    frames = {"time": time_df.T}
    for field, df in data_dict.items():
        if field == "time":
            continue
        frames[field] = df.T

    df = pd.concat(frames, axis=1)
    df.columns = ["_".join(map(str, col)).strip("_") for col in df.columns]
    df = df.reset_index().rename(columns={"index": "code"})
    return df


def main() -> None:
    print(f"[INFO] 请求 {CODES} {PERIOD} {START}~{END} count={COUNT}")
    kwargs = dict(
        field_list=[],
        stock_list=CODES,
        period=PERIOD,
        start_time=START,
        end_time=END,
        count=COUNT,
        dividend_type="none",
        fill_data=False,
    )

    data_dict: Dict[str, Any] | None = None
    if hasattr(xtdata, "get_market_data_ex"):
        try:
            data_dict = xtdata.get_market_data_ex(**kwargs)
        except TypeError:
            pass
    if data_dict is None:
        data_dict = xtdata.get_market_data(**kwargs)

    if not isinstance(data_dict, dict) or not data_dict:
        raise RuntimeError("xtdata 返回空数据，请检查时间窗口和本地缓存")

    df = _normalize_dataframe(data_dict)
    df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")
    print(f"[DONE] 已写出 {len(df)} 条记录到 {SAVE_PATH}")


if __name__ == "__main__":
    main()
"""
Path('scripts/dump_xtdata_csv.py').write_text(content, encoding='utf-8')
