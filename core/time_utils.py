# -*- coding: utf-8 -*-
"""
时间列标准化工具。

本模块用于 QMTD 内部数据入库前后的时间解析，核心契约是：
    - 输入可为 xtdata epoch 秒/毫秒、YYYYMMDD、YYYYMMDDHHMMSS、常见时间字符串；
    - 输出统一为本地北京时间语义的 tz-naive datetime64[ns]；
    - 对 epoch 秒/毫秒必须先按 UTC 绝对时间解析，再转换为北京时间墙上时间。
"""
from __future__ import annotations

import re

import pandas as pd


CN_TZ_NAME = "Asia/Shanghai"
_TZ_SUFFIX_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$")


def _epoch_to_local_naive(values: pd.Series, unit: str) -> pd.Series:
    """
    将 epoch 秒/毫秒转换为北京时间无时区序列。

    Args:
        values (pd.Series): 数值型 epoch 序列。
        unit (str): pandas 支持的时间单位，通常为 s 或 ms。

    Returns:
        pd.Series: datetime64[ns]，表示北京时间墙上时间。
    """

    parsed = pd.to_datetime(values, errors="coerce", unit=unit, utc=True)
    return parsed.dt.tz_convert(CN_TZ_NAME).dt.tz_localize(None)


def parse_local_naive_time_series(series: pd.Series) -> pd.Series:
    """
    解析时间列为北京时间语义的 tz-naive datetime64[ns]。

    Args:
        series (pd.Series): 待解析的时间列。

    Returns:
        pd.Series: 解析后的时间序列；无法解析的值为 NaT。
    """

    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    if pd.api.types.is_numeric_dtype(series):
        num = pd.to_numeric(series, errors="coerce")
        valid = num.notna()
        abs_num = num.abs()

        # YYYYMMDDHHMMSS 与 YYYYMMDD 是本地业务时间，不按 epoch 处理。
        m14 = valid & abs_num.between(10_000_000_000_000, 99_999_999_999_999)
        if m14.any():
            parsed.loc[m14] = pd.to_datetime(num.loc[m14].astype("int64").astype(str), format="%Y%m%d%H%M%S", errors="coerce")

        m8 = valid & abs_num.between(19_000_000, 20_999_999)
        if m8.any():
            parsed.loc[m8] = pd.to_datetime(num.loc[m8].astype("int64").astype(str), format="%Y%m%d", errors="coerce")

        m13 = valid & parsed.isna() & abs_num.between(1_000_000_000_000, 9_999_999_999_999)
        if m13.any():
            parsed.loc[m13] = _epoch_to_local_naive(num.loc[m13], "ms")

        m10 = valid & parsed.isna() & abs_num.between(1_000_000_000, 9_999_999_999)
        if m10.any():
            parsed.loc[m10] = _epoch_to_local_naive(num.loc[m10], "s")

        remain = valid & parsed.isna()
        if remain.any():
            parsed.loc[remain] = pd.to_datetime(num.loc[remain], errors="coerce")
        return pd.to_datetime(parsed, errors="coerce")

    text = series.astype("string").str.strip()
    non_empty = text.notna() & (text != "")
    if not non_empty.any():
        return parsed

    m14 = non_empty & text.str.fullmatch(r"\d{14}")
    if m14.any():
        parsed.loc[m14] = pd.to_datetime(text.loc[m14], format="%Y%m%d%H%M%S", errors="coerce")

    m8 = non_empty & text.str.fullmatch(r"\d{8}")
    if m8.any():
        parsed.loc[m8] = pd.to_datetime(text.loc[m8], format="%Y%m%d", errors="coerce")

    m13 = non_empty & text.str.fullmatch(r"\d{13}")
    if m13.any():
        values = pd.to_numeric(text.loc[m13], errors="coerce")
        parsed.loc[m13] = _epoch_to_local_naive(values, "ms")

    m10 = non_empty & text.str.fullmatch(r"\d{10}")
    if m10.any():
        values = pd.to_numeric(text.loc[m10], errors="coerce")
        parsed.loc[m10] = _epoch_to_local_naive(values, "s")

    text_norm = text.str.replace("T", " ", regex=False)
    remain = non_empty & parsed.isna()
    tz_mask = remain & text_norm.str.contains(_TZ_SUFFIX_RE, na=False)
    if tz_mask.any():
        tz_parsed = pd.to_datetime(text_norm.loc[tz_mask].str.replace("Z", "+00:00", regex=False), errors="coerce", utc=True)
        parsed.loc[tz_mask] = tz_parsed.dt.tz_convert(CN_TZ_NAME).dt.tz_localize(None)

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y%m%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
    )
    for fmt in formats:
        remain = non_empty & parsed.isna()
        if not remain.any():
            break
        parsed.loc[remain] = pd.to_datetime(text_norm.loc[remain], format=fmt, errors="coerce")

    remain = non_empty & parsed.isna()
    if remain.any():
        try:
            parsed.loc[remain] = pd.to_datetime(text_norm.loc[remain], errors="coerce", format="mixed")
        except TypeError:
            parsed.loc[remain] = text_norm.loc[remain].apply(lambda x: pd.to_datetime(x, errors="coerce"))

    return pd.to_datetime(parsed, errors="coerce")
