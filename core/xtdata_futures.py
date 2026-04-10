# -*- coding: utf-8 -*-
"""
xtdata / QMT 期货代码到 FD 入库参数的本地映射工具。

职责：
    - 解析 xtdata 期货代码中的品种、合约片段与交易所后缀；
    - 将外部代码稳定映射为 FD 所需的 market / symbol / specific；
    - 固化当前项目采用的连续合约语义：
      `00 -> 主力连续`，`01 -> 次主力连续`，`888 -> 888`。
"""
from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class XtFuturesTarget:
    """xtdata 期货代码映射后的 FD 目标对象。"""

    source_code: str
    market: str
    symbol: str
    specific: str
    exchange_suffix: str
    contract_token: str
    contract_kind: str


def parse_xt_futures_code(code: str) -> XtFuturesTarget:
    """
    将 xtdata / QMT 的期货代码解析为 FD 入库目标。

    当前映射规则：
        - rb00.SF  -> Futures_data / rb / 主力连续
        - rb01.SF  -> Futures_data / rb / 次主力连续
        - rb888.SF -> Futures_data / rb / 888
        - rb2505.SF -> Futures_data / rb / rb2505
    """
    if not isinstance(code, str):
        raise ValueError(f"期货代码必须是字符串，当前收到: {type(code)!r}")

    normalized_code = code.strip()
    match = re.fullmatch(r"([A-Za-z]{1,2})(\d{2,4})\.([A-Za-z]{2,8})", normalized_code)
    if not match:
        raise ValueError(
            "xtdata 期货代码格式不合法，期望形如 `rb00.SF`、`rb2505.SF`，"
            f"当前收到: {code}"
        )

    variety, contract_token, exchange_suffix = match.groups()
    symbol = variety.lower()
    exchange_suffix = exchange_suffix.upper()

    if contract_token == "00":
        specific = "主力连续"
        contract_kind = "main_continuous"
    elif contract_token == "01":
        specific = "次主力连续"
        contract_kind = "sub_continuous"
    elif contract_token == "888":
        specific = "888"
        contract_kind = "recent_continuous"
    elif len(contract_token) in (3, 4):
        specific = f"{symbol}{contract_token}"
        contract_kind = "contract"
    else:
        raise ValueError(f"当前未定义该期货代码的 FD 映射规则: {code}")

    return XtFuturesTarget(
        source_code=normalized_code,
        market="Futures_data",
        symbol=symbol,
        specific=specific,
        exchange_suffix=exchange_suffix,
        contract_token=contract_token,
        contract_kind=contract_kind,
    )


def is_xt_futures_code(text: str) -> bool:
    """判断给定代码是否符合当前 xtdata 期货代码格式。"""
    if not isinstance(text, str):
        return False
    return re.fullmatch(r"[A-Za-z]{1,2}\d{2,4}\.[A-Za-z]{2,8}", text.strip()) is not None


def build_xt_futures_ingest_params(code: str) -> dict[str, str]:
    """生成可直接传给入库流程的期货参数字典。"""
    target = parse_xt_futures_code(code)
    return {
        "market": target.market,
        "symbol": target.symbol,
        "specific": target.specific,
    }
