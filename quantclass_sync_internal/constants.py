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

DEFAULT_REPORT_DIR = BASE_DIR / "log"

DEFAULT_PROGRESS_EVERY = 500

SYNC_META_DIRNAME = ".quantclass_sync"

DEFAULT_REPORT_RETENTION_DAYS = 365

TIMESTAMP_FILE_NAME = "timestamp.txt"

PRODUCT_MODE_LOCAL_SCAN = "local_scan"

PRODUCT_MODE_EXPLICIT_LIST = "explicit_list"

PRODUCT_MODES = {PRODUCT_MODE_LOCAL_SCAN, PRODUCT_MODE_EXPLICIT_LIST}

LEGACY_STATUS_DB_REL = Path("code") / "data" / "FuelBinStat.db"

LEGACY_STATUS_JSON_REL = Path("code") / "data" / "products-status.json"

META_STATUS_DB_REL = Path(SYNC_META_DIRNAME) / "status" / "FuelBinStat.db"

META_STATUS_JSON_REL = Path(SYNC_META_DIRNAME) / "status" / "products-status.json"

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
}

KNOWN_DATASETS = tuple(sorted(set(AGGREGATE_SPLIT_COLS) | {"stock-fin-data-xbx"}))

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

REASON_NETWORK_ERROR = "network_error"

REASON_NO_DATA_FOR_DATE = "no_data_for_date"

REASON_EXTRACT_ERROR = "extract_error"

REASON_MERGE_ERROR = "merge_error"

REASON_MIRROR_FALLBACK = "mirror_fallback"

REASON_UNKNOWN_HEADER_MERGE = "unknown_header_merge"

REASON_UP_TO_DATE = "up_to_date"

REASON_PREPROCESS_OK = "preprocess_ok"

REASON_PREPROCESS_FAILED = "preprocess_failed"

REASON_PREPROCESS_DRY_RUN = "preprocess_dry_run"

REASON_PREPROCESS_SKIPPED_NO_DELTA = "preprocess_skipped_no_delta"

REASON_PREPROCESS_INCREMENTAL_OK = "preprocess_incremental_ok"

REASON_PREPROCESS_FULL_REBUILD_OK = "preprocess_full_rebuild_ok"

REASON_PREPROCESS_FALLBACK_FULL_OK = "preprocess_fallback_full_ok"

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
