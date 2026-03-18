"""职责：集中维护全局常量和产品规则元信息。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict

DEFAULT_API_BASE = "https://api.quantclass.cn/api/data"

# internal 包位于仓库根目录下，因此需要回退一级才是项目根目录。
BASE_DIR = Path(__file__).resolve().parent.parent

DEFAULT_DATA_ROOT = BASE_DIR.parent / "xbx_data"

DEFAULT_WORK_DIR = BASE_DIR.parent / ".cache" / "quantclass"

DEFAULT_SECRETS_FILE = BASE_DIR / "xbx_apiKey.md"

DEFAULT_USER_CONFIG_FILE = BASE_DIR / "user_config.json"

DEFAULT_USER_SECRETS_FILE = BASE_DIR / "user_secrets.env"

DEFAULT_CATALOG_FILE = BASE_DIR / "catalog.txt"

DEFAULT_PROGRESS_EVERY = 500

SYNC_META_DIRNAME = ".quantclass_sync"

DEFAULT_REPORT_RETENTION_DAYS = 365

# GUI 默认并发线程数（CLI 默认 1，GUI 可适度提高）
DEFAULT_GUI_WORKERS = 4

TIMESTAMP_FILE_NAME = "timestamp.txt"

PRODUCT_MODE_LOCAL_SCAN = "local_scan"

PRODUCT_MODE_EXPLICIT_LIST = "explicit_list"

PRODUCT_MODES = {PRODUCT_MODE_LOCAL_SCAN, PRODUCT_MODE_EXPLICIT_LIST}

LEGACY_STATUS_DB_REL = Path("code") / "data" / "FuelBinStat.db"

LEGACY_STATUS_JSON_REL = Path("code") / "data" / "products-status.json"

META_STATUS_DB_REL = Path(SYNC_META_DIRNAME) / "status" / "FuelBinStat.db"

META_STATUS_JSON_REL = Path(SYNC_META_DIRNAME) / "status" / "products-status.json"

META_REPORT_DIR_REL = Path(SYNC_META_DIRNAME) / "log"

TRADING_PRODUCTS = {"stock-trading-data-pro", "stock-trading-data"}

INDEX_PRODUCTS = {"stock-main-index-data"}

AGGREGATE_SPLIT_COLS: Dict[str, str] = {
    "stock-trading-data-pro": "股票代码",
    "stock-trading-data": "股票代码",
    "stock-main-index-data": "index_code",
    "stock-call-auction-data": "股票代码",
    "stock-chip-distribution": "股票代码",
    "stock-fin-pre-fore-data-xbx": "股票代码",
    "stock-popular-concept-detail": "股票代码",
    "stock-trading-date": "股票代码",
    "stock-interest-rate": "指标名称",
    "coin-cap": "symbol",
    # --- 以下为批量新增的日聚合拆分产品 ---
    # 因子系列（按股票代码拆分）
    "stock-anti-trend-factors": "股票代码",
    "stock-energy-factors": "股票代码",
    "stock-multi-factor-series": "股票代码",
    "stock-oscillator-factors": "股票代码",
    "stock-technical-factors": "股票代码",
    "stock-trend-factors": "股票代码",
    "stock-volume-price-factors": "股票代码",
    # 资金流（按股票代码拆分）
    "stock-money-flow": "股票代码",
    # 分时收盘价（按股票代码拆分）
    "stock-15m-close-price": "股票代码",
    "stock-5m-close-price": "股票代码",
    # 非 A 股交易数据（按股票代码拆分）
    "stock-hk-stock-data": "股票代码",
    "stock-us-trading-data": "股票代码",
    # 可转债（按债券代码拆分）
    "stock-basic-bond": "债券代码",
    # 指数（按 index_code 拆分）
    "stock-1h-index-data": "index_code",
    "stock-us-main-index-data": "index_code",
    "stock-asset-classification": "index_code",
    # 币种行情（按 symbol 拆分）
    "coin-coinmarketcap": "symbol",
    # ETF（按基金代码拆分）
    "stock-etf-trading-data": "基金代码",
    # 策略（按策略名称拆分）
    "stock-equity": "策略名称",
    "stock-ind-element-equity": "策略名称",
}

# 非聚合拆分但需要增量合并的产品（不在 AGGREGATE_SPLIT_COLS 中）
_INDIVIDUAL_MERGE_PRODUCTS = {
    "stock-fin-data-xbx",
    "stock-notices-title",
    "coin-binance-candle-csv-1h",
    "coin-binance-swap-candle-csv-1h",
}

KNOWN_DATASETS = tuple(sorted(set(AGGREGATE_SPLIT_COLS) | _INDIVIDUAL_MERGE_PRODUCTS))

ENCODING_CANDIDATES = ("utf-8-sig", "gb18030", "utf-8", "gbk")

UTF8_BOM = b"\xef\xbb\xbf"

DATE_NAME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{8}$")

RUN_MODES = {"local", "catalog"}

STRATEGY_MERGE_KNOWN = "merge_known"

STRATEGY_MIRROR_UNKNOWN = "mirror_unknown"

REASON_OK = "ok"

REASON_UNKNOWN_LOCAL_PRODUCT = "unknown_local_product"

REASON_INVALID_EXPLICIT_PRODUCT = "invalid_explicit_product"

REASON_NO_LOCAL_PRODUCTS = "no_local_products"

REASON_NO_VALID_OUTPUT = "no_valid_output"

REASON_NETWORK_ERROR = "network_error"

REASON_NO_DATA_FOR_DATE = "no_data_for_date"

REASON_EXTRACT_ERROR = "extract_error"

REASON_MERGE_ERROR = "merge_error"

REASON_UNEXPECTED_ERROR = "unexpected_error"

REASON_MIRROR_FALLBACK = "mirror_fallback"

REASON_MIRROR_UNKNOWN = "mirror_unknown"

REASON_UNKNOWN_HEADER_MERGE = "unknown_header_merge"

REASON_UP_TO_DATE = "up_to_date"

REASON_PREPROCESS_OK = "preprocess_ok"

REASON_PREPROCESS_FAILED = "preprocess_failed"

REASON_PREPROCESS_DRY_RUN = "preprocess_dry_run"

REASON_PREPROCESS_SKIPPED_NO_DELTA = "preprocess_skipped_no_delta"

REASON_PREPROCESS_INCREMENTAL_OK = "preprocess_incremental_ok"

REASON_PREPROCESS_FULL_REBUILD_OK = "preprocess_full_rebuild_ok"

REASON_PREPROCESS_FALLBACK_FULL_OK = "preprocess_fallback_full_ok"

EXIT_CODE_SUCCESS = 0

EXIT_CODE_GENERAL_FAILURE = 1

EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE = 2

EXIT_CODE_NO_EXECUTABLE_PRODUCTS = 3

PREPROCESS_PRODUCT = "coin-binance-spot-swap-preprocess-pkl-1h"

PREPROCESS_TRIGGER_PRODUCTS = {
    "coin-binance-candle-csv-1h",
    "coin-binance-swap-candle-csv-1h",
}

BUSINESS_DAY_ONLY_PRODUCTS = {
    "stock-trading-data",
    "stock-trading-data-pro",
    "stock-main-index-data",
}

DISCOVERY_IGNORED_PRODUCTS = {PREPROCESS_PRODUCT}

LOG_LEVELS = {
    "ERROR": 0,
    "INFO": 1,
    "DEBUG": 2,
}

REQUEST_POLICIES: Dict[str, Dict[str, int]] = {
    # latest/get-download-link：短超时 + 少重试，避免 no-data 密集场景拖慢全局。
    "latest": {"max_attempts": 3, "timeout_seconds": 15, "backoff_cap_seconds": 3},
    "download_link": {"max_attempts": 3, "timeout_seconds": 15, "backoff_cap_seconds": 3},
    # 文件流下载：保持稳健策略。
    "file_download": {"max_attempts": 5, "timeout_seconds": 60, "backoff_cap_seconds": 8},
    # 兜底策略（兼容旧调用）。
    "default": {"max_attempts": 5, "timeout_seconds": 60, "backoff_cap_seconds": 8},
}

META_HEALTH_BASELINE_REL = Path(SYNC_META_DIRNAME) / "log" / "health_baseline.json"

# 财务/公告类产品，日期连续性检查不适用
FINANCIAL_PRODUCTS = {"stock-fin-data-xbx", "stock-fin-pre-fore-data-xbx"}
NOTICE_PRODUCTS = {"stock-notices-title"}

# API 日期缓存 TTL（秒），5 分钟内的缓存直接使用，过期则回退 HTTP 查询
API_DATE_CACHE_TTL_SECONDS = 300


def normalize_product_name(product: str) -> str:
    """
    统一产品名写法。

    官方接口常见两种写法：
    - stock-trading-data-pro
    - stock-trading-data-pro-daily
    这里统一去掉 -daily，后续拼接 URL 时再补上。
    """

    product = product.strip()
    if product.endswith("-daily"):
        return product[:-6]
    return product
