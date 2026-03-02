"""职责：定义币圈预处理所需的常量和摘要模型。"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

SPOT_PRODUCT = "coin-binance-candle-csv-1h"

SWAP_PRODUCT = "coin-binance-swap-candle-csv-1h"

PREPROCESS_PRODUCT = "coin-binance-spot-swap-preprocess-pkl-1h"

OUTPUT_SPOT_DICT = "spot_dict.pkl"

OUTPUT_SWAP_DICT = "swap_dict.pkl"

OUTPUT_PIVOT_SPOT = "market_pivot_spot.pkl"

OUTPUT_PIVOT_SWAP = "market_pivot_swap.pkl"

TIMESTAMP_FILE_NAME = "timestamp.txt"

CSV_ENCODINGS = ("gbk", "utf-8-sig", "utf-8")

TAIL_READ_MAX_LINES = 4096

TAIL_READ_MAX_BYTES = 8 * 1024 * 1024

TAIL_APPEND_SAFE_MAX_DATA_ROWS = TAIL_READ_MAX_LINES

PIVOT_DEFRAG_THRESHOLD = 32

SPECIAL_SPOT_TO_SWAP_ALIAS = {
    "DODO-USDT": "DODOX-USDT",
    "LUNA-USDT": "LUNA2-USDT",
    "RAY-USDT": "RAYSOL-USDT",
    "1000SATS-USDT": "1000SATS-USDT",
}

SPECIAL_SWAP_TO_SPOT_ALIAS = {v: k for k, v in SPECIAL_SPOT_TO_SWAP_ALIAS.items()}

SWAP_SPLIT_MAP = {
    "LUNA-USDT": ["LUNA-USDT", "LUNA2-USDT"],
    "DODO-USDT": ["DODO-USDT", "DODOX-USDT"],
    "RAY-USDT": ["RAY-USDT", "RAYSOL-USDT"],
}

SPLIT_SYMBOL_PATTERN = re.compile(r"^(?P<base>.+)_(?P<tag>SP|SW)(?P<index>\d+)-USDT$")

RELIST_GAP_THRESHOLD = pd.to_timedelta("1days")

RELIST_CHANGE_THRESHOLD = 0.01

FRAME_COLUMNS = [
    "candle_begin_time",
    "symbol",
    "open",
    "high",
    "close",
    "low",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "funding_fee",
    "avg_price_1m",
    "avg_price_5m",
    "是否交易",
    "first_candle_time",
    "last_candle_time",
    "symbol_spot",
    "symbol_swap",
    "is_spot",
]

PIVOT_FIELDS = {
    "spot": [
        ("open", "open"),
        ("close", "close"),
        ("vwap1m", "avg_price_1m"),
    ],
    "swap": [
        ("open", "open"),
        ("close", "close"),
        ("funding_rate", "funding_fee"),
        ("vwap1m", "avg_price_1m"),
    ],
}

@dataclass(frozen=True)
class PreprocessSummary:
    """内置预处理执行摘要。"""

    spot_symbols: int
    swap_symbols: int
    output_dir: str
    mode: str
    changed_symbols: int
