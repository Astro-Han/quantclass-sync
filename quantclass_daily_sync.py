#!/usr/bin/env python3
"""QuantClass 每日数据更新脚本（本地存量驱动极简版）。"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import time
import traceback
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

import requests

try:
    import typer
    from apscheduler.schedulers.background import BackgroundScheduler
    from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
    from rich.console import Console
except ModuleNotFoundError as exc:  # pragma: no cover - 环境缺依赖时给出中文提示
    print("缺少运行依赖；请先执行 `python3 -m pip install -r requirements.txt` 再运行脚本。", file=sys.stderr)
    raise SystemExit(2) from exc

# 可选依赖：如果压缩包是 .7z，需要 py7zr 才能解压。
try:
    import py7zr  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    py7zr = None

# 可选依赖：如果压缩包是 .rar，需要 rarfile 才能解压。
try:
    import rarfile  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    rarfile = None

DEFAULT_API_BASE = "https://api.quantclass.cn/api/data"
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = BASE_DIR.parent / "xbx_data"
DEFAULT_WORK_DIR = BASE_DIR.parent / ".cache" / "quantclass"
DEFAULT_SECRETS_FILE = BASE_DIR / "xbx_apiKey.md"
DEFAULT_CATALOG_FILE = BASE_DIR / "catalog.txt"
DEFAULT_PROGRESS_EVERY = 500
DEFAULT_STATUS_DB = DEFAULT_DATA_ROOT / "code" / "data" / "FuelBinStat.db"
DEFAULT_STATUS_JSON = DEFAULT_DATA_ROOT / "code" / "data" / "products-status.json"
DEFAULT_ZIP_CACHE_DIR = DEFAULT_WORK_DIR / "zip"
DEFAULT_FULL_BACKUP_DIR = DEFAULT_WORK_DIR / "full_backup"

# 已知数据产品（这些产品允许做增量合并）
TRADING_PRODUCTS = {"stock-trading-data-pro", "stock-trading-data"}
INDEX_PRODUCTS = {"stock-main-index-data"}
# 聚合日文件拆分配置（把 2026-02-06.csv 拆成按代码/指标名的单文件）
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

# 读取 CSV 时的编码兜底顺序
ENCODING_CANDIDATES = ("utf-8-sig", "gb18030", "utf-8", "gbk")
DATE_NAME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{8}$")

RUN_MODES = {"local", "catalog"}

STRATEGY_MERGE_KNOWN = "merge_known"
STRATEGY_MIRROR_UNKNOWN = "mirror_unknown"

REASON_OK = "ok"
REASON_UNKNOWN_LOCAL_PRODUCT = "unknown_local_product"
REASON_INVALID_EXPLICIT_PRODUCT = "invalid_explicit_product"
REASON_NO_LOCAL_PRODUCTS = "no_local_products"
REASON_NETWORK_ERROR = "network_error"
REASON_EXTRACT_ERROR = "extract_error"
REASON_MERGE_ERROR = "merge_error"
REASON_MIRROR_FALLBACK = "mirror_fallback"
REASON_UNKNOWN_HEADER_MERGE = "unknown_header_merge"
REASON_FULL_DATA_LINK_MISSING = "full_data_link_missing"
REASON_FULL_DATA_EXPIRED = "full_data_expired"

LOG_LEVELS = {
    "ERROR": 0,
    "INFO": 1,
    "DEBUG": 2,
}

RICH_CONSOLE = Console()

def utc_now_iso() -> str:
    """返回 UTC 时间字符串（ISO 格式）。"""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

class ConsoleLogger:
    """极简日志器（中文 message + 英文事件码）。"""

    def __init__(self, level: str = "INFO", run_id: str = "") -> None:
        level = level.upper()
        if level not in LOG_LEVELS:
            level = "INFO"
        self.level = level
        self.run_id = run_id

    def _enabled(self, level: str) -> bool:
        return LOG_LEVELS[level] <= LOG_LEVELS[self.level]

    def _emit(self, level: str, message: str, event: str = "INFO", **fields: object) -> None:
        if not self._enabled(level):
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rid = f"[{self.run_id}] " if self.run_id else ""
        extra = ""
        if fields:
            show = [f"{k}={v}" for k, v in fields.items()]
            extra = " | " + " ".join(show)
        styles = {"ERROR": "bold red", "INFO": "cyan", "DEBUG": "dim"}
        style = styles.get(level, "")
        RICH_CONSOLE.print(f"{ts} [{level}] {rid}[{event}] {message}{extra}", style=style)

    def error(self, message: str, event: str = "ERROR", **fields: object) -> None:
        self._emit("ERROR", message, event=event, **fields)

    def info(self, message: str, event: str = "INFO", **fields: object) -> None:
        self._emit("INFO", message, event=event, **fields)

    def debug(self, message: str, event: str = "DEBUG", **fields: object) -> None:
        self._emit("DEBUG", message, event=event, **fields)

LOGGER = ConsoleLogger(level="INFO")
PROGRESS_EVERY = DEFAULT_PROGRESS_EVERY

def log_error(message: str, event: str = "ERROR", **fields: object) -> None:
    LOGGER.error(message, event=event, **fields)

def log_info(message: str, event: str = "INFO", **fields: object) -> None:
    LOGGER.info(message, event=event, **fields)

def log_debug(message: str, event: str = "DEBUG", **fields: object) -> None:
    LOGGER.debug(message, event=event, **fields)

class FatalRequestError(RuntimeError):
    """参数或权限问题，立即失败，不重试。"""

class ProductSyncError(RuntimeError):
    """单产品执行错误（携带 reason_code）。"""

    def __init__(self, message: str, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code

@dataclass(frozen=True)
class DatasetRule:
    """已知产品统一规则。"""

    name: str
    encoding: str
    has_note: bool
    key_cols: Tuple[str, ...]
    sort_cols: Tuple[str, ...]

@dataclass
class CsvPayload:
    """CSV 解析后的统一结构。"""

    note: Optional[str]
    header: List[str]
    rows: List[List[str]]
    encoding: str
    delimiter: str = ","

@dataclass
class SyncStats:
    """文件同步统计信息（每产品/全局共用）。"""

    created_files: int = 0
    updated_files: int = 0
    unchanged_files: int = 0
    skipped_files: int = 0
    rows_added: int = 0

    def merge(self, other: "SyncStats") -> None:
        self.created_files += other.created_files
        self.updated_files += other.updated_files
        self.unchanged_files += other.unchanged_files
        self.skipped_files += other.skipped_files
        self.rows_added += other.rows_added

@dataclass
class DiscoveredProduct:
    """本地发现到的产品。"""

    name: str
    source: str
    valid: bool

@dataclass
class ProductPlan:
    """单产品执行计划。"""

    name: str
    strategy: str

@dataclass
class RunEvent:
    """运行事件（用于后续审计和可观测性）。"""

    ts: str
    product: str
    stage: str
    status: str
    reason_code: str
    detail: str

@dataclass
class ProductRunResult:
    """单产品执行结果。"""

    product: str
    status: str
    strategy: str = ""
    reason_code: str = REASON_OK
    date_time: str = ""
    mode: str = "network"
    elapsed_seconds: float = 0.0
    stats: SyncStats = field(default_factory=SyncStats)
    source_path: str = ""
    error: str = ""

@dataclass
class RunReport:
    """整次运行报告。"""

    schema_version: str
    run_id: str
    started_at: str
    mode: str
    ended_at: str = ""
    duration_seconds: float = 0.0
    discovered_total: int = 0
    planned_total: int = 0
    success_total: int = 0
    failed_total: int = 0
    skipped_total: int = 0
    products: List[ProductRunResult] = field(default_factory=list)
    events: List[RunEvent] = field(default_factory=list)
    summary: SyncStats = field(default_factory=SyncStats)


class CommandContext(BaseModel):
    """命令上下文（把运行参数统一收口，避免各函数参数漂移）。"""

    run_id: str
    data_root: Path
    api_key: str = ""
    hid: str = ""
    secrets_file: Path = DEFAULT_SECRETS_FILE
    dry_run: bool = False
    report_file: Optional[Path] = None
    stop_on_error: bool = False
    verbose: bool = False
    mode: str = "network"
    api_base: str = DEFAULT_API_BASE
    catalog_file: Path = DEFAULT_CATALOG_FILE
    work_dir: Path = DEFAULT_WORK_DIR


class CommandResult(BaseModel):
    """命令执行结果（用于命令级状态回传与总结）。"""

    command: str
    status: str
    reason_code: str = REASON_OK
    elapsed_seconds: float = 0.0
    report_file: Optional[str] = None
    detail: str = ""


class ProductStatus(BaseModel):
    """产品状态模型（官方字段兼容，数据库为单一真源）。"""

    model_config = ConfigDict(extra="ignore")

    name: str
    display_name: Optional[str] = None
    full_data: Optional[str] = None
    last_update_time: Optional[str] = None
    next_update_time: Optional[str] = None
    data_time: Optional[str] = None
    data_content_time: Optional[str] = None
    is_auto_update: int = 0
    can_auto_update: int = 1
    add_time: Optional[str] = None
    is_listed: int = 1
    full_data_download_url: Optional[str] = None
    full_data_download_expires: Optional[str] = None
    ts: Optional[str] = None

    @field_validator("full_data_download_expires", mode="before")
    @classmethod
    def _normalize_full_data_expires(cls, value: object) -> Optional[str]:
        """
        统一过期字段为字符串。

        说明：
        - 历史状态库里可能存在 int/float（例如 0、时间戳），这里做兼容归一。
        - 0 视为“无有效过期时间”。
        """

        if value in (None, "", 0, "0"):
            return None
        text = str(value).strip()
        return text or None

    @field_validator("is_auto_update", "can_auto_update", "is_listed", mode="before")
    @classmethod
    def _normalize_int_flags(cls, value: object, info: ValidationInfo) -> int:
        """
        统一整型标志位，兼容历史状态库里的 NULL/空串/字符串数字。
        """

        defaults = {
            "is_auto_update": 0,
            "can_auto_update": 1,
            "is_listed": 1,
        }
        default_value = defaults.get(info.field_name, 0)
        if value in (None, ""):
            return default_value
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)

        text = str(value).strip()
        if not text:
            return default_value
        try:
            return int(text)
        except Exception:
            return default_value

    def to_json_record(self) -> Dict[str, object]:
        """导出 products-status.json 单产品记录（camelCase 字段）。"""

        return {
            "name": self.name,
            "displayName": self.display_name,
            "fullData": self.full_data,
            "lastUpdateTime": self.last_update_time,
            "nextUpdateTime": self.next_update_time,
            "dataTime": self.data_time,
            "dataContentTime": self.data_content_time,
            "isAutoUpdate": self.is_auto_update,
            "canAutoUpdate": self.can_auto_update,
            "addTime": self.add_time,
            "isListed": self.is_listed,
            "fullDataDownloadUrl": self.full_data_download_url,
            "fullDataDownloadExpires": self.full_data_download_expires,
            "ts": self.ts or "",
        }


class SchedulerConfig(BaseModel):
    """调度器配置（定时任务框架，仅预留接口，默认不启动）。"""

    enabled: bool = False
    timezone: str = "Asia/Shanghai"
    interval_minutes: int = Field(default=60, ge=1)

def run_report_to_dict(report: RunReport) -> Dict[str, object]:
    """RunReport 转 dict，便于输出 JSON。"""

    return asdict(report)

RULES: Dict[str, DatasetRule] = {
    # 交易数据：中文字段 + 备注行 + gb18030
    "stock-trading-data-pro": DatasetRule(
        name="stock-trading-data-pro",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 交易数据（基础版）：结构与 pro 同源，主键规则保持一致
    "stock-trading-data": DatasetRule(
        name="stock-trading-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 指数数据：纯英文字段 + 无备注行 + utf-8-sig
    "stock-main-index-data": DatasetRule(
        name="stock-main-index-data",
        encoding="utf-8-sig",
        has_note=False,
        key_cols=("index_code", "candle_end_time"),
        sort_cols=("candle_end_time",),
    ),
    # 财务数据：英文字段 + 备注行 + gb18030
    "stock-fin-data-xbx": DatasetRule(
        name="stock-fin-data-xbx",
        encoding="gb18030",
        has_note=True,
        key_cols=("stock_code", "report_date", "publish_date"),
        sort_cols=("report_date", "publish_date"),
    ),
    # 集合竞价数据：按 股票代码 拆分后，以 交易日期 增量合并
    "stock-call-auction-data": DatasetRule(
        name="stock-call-auction-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 筹码分布：按 股票代码 拆分后，以 交易日期 增量合并
    "stock-chip-distribution": DatasetRule(
        name="stock-chip-distribution",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 业绩预告：按 股票代码 拆分后，使用多列联合主键做去重
    "stock-fin-pre-fore-data-xbx": DatasetRule(
        name="stock-fin-pre-fore-data-xbx",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "业绩预告首次披露日期", "业绩预告日期", "预告对应财报日期"),
        sort_cols=("业绩预告首次披露日期", "业绩预告日期"),
    ),
    # 人气概念：按 股票代码 拆分后，以 交易日期 增量合并
    "stock-popular-concept-detail": DatasetRule(
        name="stock-popular-concept-detail",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 每日股票汇总：按 股票代码 拆分后，以 交易日期 增量合并
    "stock-trading-date": DatasetRule(
        name="stock-trading-date",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 利率数据：按 指标名称 拆分后，以 日期 增量合并
    "stock-interest-rate": DatasetRule(
        name="stock-interest-rate",
        encoding="gb18030",
        has_note=True,
        key_cols=("指标名称", "日期"),
        sort_cols=("日期",),
    ),
    # period_offset 也按“备注 + 表头 + 日期主键”处理
    "period_offset.csv": DatasetRule(
        name="period_offset.csv",
        encoding="gb18030",
        has_note=True,
        key_cols=("交易日期",),
        sort_cols=("交易日期",),
    ),
}

# 产品发现与计划

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

def split_products(raw_products: Sequence[str]) -> List[str]:
    """
    解析命令行产品列表，支持空格分隔和逗号分隔。
    """

    products: List[str] = []
    for item in raw_products:
        for part in item.split(","):
            part = normalize_product_name(part.strip())
            if part:
                products.append(part)

    seen = set()
    result: List[str] = []
    for item in products:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

def is_product_identifier(raw: str) -> bool:
    """
    判断文本是否像产品英文名。

    规则：
    - 只允许小写字母/数字/连字符
    - 至少包含一个字母（避免把日期误判成产品名）
    """

    s = raw.strip().lower()
    if not s:
        return False
    if not re.fullmatch(r"[a-z0-9-]+", s):
        return False
    return any(ch.isalpha() for ch in s)

def load_products_from_catalog(path: Path) -> List[str]:
    """
    从 catalog.txt 读取产品列表。

    兼容两种写法：
    1) 每行一个产品英文名
    2) 三列格式（产品中文名 / 产品英文名 / 日期）
    """

    if not path.exists():
        raise RuntimeError(f"产品清单文件不存在: {path}")

    products: List[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if is_product_identifier(s):
            products.append(normalize_product_name(s.lower()))
            continue

        for token in re.split(r"[\t,| ]+", s):
            t = token.strip()
            if is_product_identifier(t):
                products.append(normalize_product_name(t.lower()))
                break

    seen = set()
    result: List[str] = []
    for item in products:
        if item not in seen:
            seen.add(item)
            result.append(item)
    if not result:
        raise RuntimeError(f"产品清单为空或格式无法识别: {path}")
    return result

def _dir_has_data_files(path: Path) -> bool:
    """
    判断目录内是否存在数据文件（.csv/.ts）。

    这里使用“递归查找任意一个命中即返回”，
    避免把空目录误认为“已有产品”。
    """

    for candidate in path.rglob("*"):
        if candidate.is_file() and candidate.suffix.lower() in {".csv", ".ts"}:
            return True
    return False

def discover_local_products(data_root: Path, catalog_products: Sequence[str]) -> List[DiscoveredProduct]:
    """
    扫描 data_root 一级目录，识别本地已有产品。

    定义：
    - 目录下递归存在 .csv/.ts，才算“本地已有产品”。
    - 是否有效（valid）由 catalog 产品集合判定。
    """

    catalog_set = {normalize_product_name(x) for x in catalog_products}
    discovered: List[DiscoveredProduct] = []

    if not data_root.exists() or not data_root.is_dir():
        return discovered

    for item in sorted(data_root.iterdir(), key=lambda x: x.name):
        if not item.is_dir():
            continue
        if not _dir_has_data_files(item):
            continue
        product_name = normalize_product_name(item.name)
        discovered.append(
            DiscoveredProduct(
                name=product_name,
                source="local",
                valid=product_name in catalog_set,
            )
        )
    return discovered

def resolve_products_by_mode(
    mode: str,
    raw_products: Sequence[str],
    catalog_products: Sequence[str],
    discovered_local: Sequence[DiscoveredProduct],
) -> Tuple[List[str], List[str], List[str]]:
    """
    解析最终产品清单。

    返回三部分：
    1) planned_products：实际要执行的产品
    2) unknown_local_products：本地存在但不在 catalog 的目录
    3) invalid_explicit_products：用户显式指定但不在 catalog 的产品
    """

    mode = (mode or "local").strip().lower()
    if mode not in RUN_MODES:
        mode = "local"

    catalog_norm = [normalize_product_name(x) for x in catalog_products]
    catalog_set = set(catalog_norm)

    explicit = split_products(raw_products)
    explicit_valid = [x for x in explicit if x in catalog_set]
    invalid_explicit = [x for x in explicit if x not in catalog_set]

    unknown_local = [x.name for x in discovered_local if not x.valid]
    local_valid = [x.name for x in discovered_local if x.valid]

    if explicit:
        selected = explicit_valid
    elif mode == "catalog":
        selected = list(catalog_norm)
    else:
        selected = list(local_valid)

    seen = set()
    result: List[str] = []
    for item in selected:
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result, unknown_local, invalid_explicit

def build_product_plan(products: Sequence[str]) -> List[ProductPlan]:
    """
    为产品生成执行计划。

    规则：
    - 命中 RULES：merge_known（增量合并）
    - 未命中 RULES：mirror_unknown（镜像写入）
    """

    plans: List[ProductPlan] = []
    for product in products:
        strategy = STRATEGY_MERGE_KNOWN if product in RULES else STRATEGY_MIRROR_UNKNOWN
        plans.append(ProductPlan(name=product, strategy=strategy))
    return plans

# 凭证与网络请求

def load_secrets_from_file(path: Path) -> Tuple[str, str]:
    """
    从本地文件读取 api_key / hid（若不存在则返回空字符串）。
    """

    if not path.exists():
        return "", ""

    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    pairs: Dict[str, str] = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" in s:
            k, v = s.split("=", 1)
        elif ":" in s:
            k, v = s.split(":", 1)
        else:
            continue
        key = k.strip().lower().replace("-", "_")
        value = v.strip().strip("\"'")
        if value:
            pairs[key] = value

    api_key_candidates = ["xbx_api_key", "quantclass_api_key", "api_key", "apikey", "key"]
    hid_candidates = ["xbx_id", "quantclass_hid", "hid", "uuid"]

    api_key = next((pairs[k] for k in api_key_candidates if k in pairs), "")
    hid = next((pairs[k] for k in hid_candidates if k in pairs), "")
    return api_key, hid

def resolve_credentials(cli_api_key: str, cli_hid: str, secrets_file: Path) -> Tuple[str, str]:
    """
    凭证优先级（高 -> 低）：
    1) 命令行参数
    2) 环境变量
    3) 本地 secrets 文件
    """

    api_key = (cli_api_key or os.environ.get("QUANTCLASS_API_KEY", "")).strip()
    hid = (cli_hid or os.environ.get("QUANTCLASS_HID", "")).strip()

    if api_key and hid:
        return api_key, hid

    file_api_key, file_hid = load_secrets_from_file(secrets_file)
    if not api_key:
        api_key = file_api_key
    if not hid:
        hid = file_hid

    return api_key, hid

def request_data(method: str, url: str, headers: Dict[str, str], **kwargs) -> requests.Response:
    """
    统一 HTTP 请求入口（带重试）。

    设计思路：
    - 网络波动/服务器偶发 5xx：重试
    - 参数错误/权限不足（4xx）：立即报错，不重试
    """

    status_messages = {
        404: "参数错误",
        403: "无下载权限，请检查下载次数和 api-key",
        401: "超出当日下载次数",
        400: "下载时间超出限制",
        500: "服务器内部错误，请稍后重试",
    }

    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        log_debug(f"HTTP {method} attempt={attempt}/{max_attempts} url={url.split('?')[0]}")
        try:
            response = requests.request(method=method, url=url, headers=headers, timeout=60, **kwargs)
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                hint = ""
                err_text = str(exc)
                if "Failed to resolve" in err_text or "NameResolutionError" in err_text:
                    hint = "（DNS 解析失败：请检查网络、DNS 或代理设置）"
                raise RuntimeError(f"网络请求失败: {exc}{hint}") from exc
            time.sleep(min(2 ** (attempt - 1), 8))
            continue

        if response.status_code == 200:
            return response

        message = status_messages.get(response.status_code, f"未知错误（HTTP {response.status_code}）")
        if response.status_code in {400, 401, 403, 404}:
            raise FatalRequestError(message)
        if attempt >= max_attempts:
            raise RuntimeError(message)
        time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError("请求失败：超过最大重试次数。")

def normalize_latest_time(raw_text: str) -> str:
    """
    latest 接口可能返回逗号分隔或空白分隔的一串时间，
    这里取最大值作为“最新版本”。
    """

    candidates = [x.strip() for x in re.split(r"[,\s]+", raw_text) if x.strip()]
    if not candidates:
        raise RuntimeError("接口未返回可用的 date_time。")
    return max(candidates)

def get_latest_time(api_base: str, product: str, hid: str, headers: Dict[str, str]) -> str:
    """调用 latest 接口获取指定产品最新时间。"""

    url = f"{api_base}/fetch/{product}-daily/latest?uuid={hid}"
    res = request_data("GET", url=url, headers=headers)
    return normalize_latest_time(res.text)

def get_download_link(api_base: str, product: str, date_time: str, hid: str, headers: Dict[str, str]) -> str:
    """根据产品和时间获取真实下载链接。"""

    url = f"{api_base}/get-download-link/{product}-daily/{date_time}?uuid={hid}"
    res = request_data("GET", url=url, headers=headers)
    download_link = res.text.strip()
    if not download_link:
        raise RuntimeError(f"{product} {date_time} 未返回下载链接。")
    return download_link

def build_file_name(file_url: str, product: str, date_time: str) -> str:
    """
    从下载链接提取文件名；提取失败时使用兜底名。
    """

    parsed = urlparse(file_url)
    name = Path(unquote(parsed.path)).name
    if name:
        return name
    return f"{product}_{date_time}.zip"

def save_file(file_url: str, file_path: Path, headers: Dict[str, str]) -> None:
    """流式下载文件到本地，避免占用过多内存。"""

    file_path.parent.mkdir(parents=True, exist_ok=True)
    res = request_data("GET", url=file_url, headers=headers, stream=True)
    with file_path.open("wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

# 解压与路径映射

def _ensure_within(base: Path, target: Path) -> None:
    """
    安全解压检查。

    防止压缩包成员路径通过 ../../ 逃逸到目标目录之外。
    """

    try:
        target.resolve().relative_to(base.resolve())
    except Exception as exc:
        raise RuntimeError(f"解压路径越界: {target}") from exc

def safe_extract_zip(path: Path, save_path: Path) -> None:
    """安全解压 zip。"""

    with zipfile.ZipFile(path) as zf:
        for member in zf.infolist():
            target = save_path / member.filename
            _ensure_within(save_path, target)
        zf.extractall(save_path)

def safe_extract_tar(path: Path, save_path: Path) -> None:
    """安全解压 tar。"""

    with tarfile.open(path) as tf:
        for member in tf.getmembers():
            member_name = _normalize_member_name(member.name)
            target = save_path / member_name
            _ensure_within(save_path, target)

            # tar 里的软链接/硬链接可能指向目标目录外，需额外校验 linkname。
            if member.issym() or member.islnk():
                link_name = _normalize_member_name(getattr(member, "linkname", ""))
                if not link_name:
                    raise RuntimeError(f"tar 链接目标为空: {member.name}")
                if link_name.startswith("/") or re.match(r"^[a-zA-Z]:[\\/]", link_name):
                    raise RuntimeError(f"tar 链接目标为绝对路径，已拒绝: {member.name} -> {link_name}")
                link_target = save_path / Path(member_name).parent / link_name
                _ensure_within(save_path, link_target)
        tf.extractall(save_path)


def _normalize_member_name(name: str) -> str:
    """把压缩包成员名统一成 POSIX 风格，避免反斜杠绕过路径检查。"""

    return name.replace("\\", "/")


def safe_extract_rar(path: Path, save_path: Path) -> None:
    """
    安全解压 rar。

    先逐成员检查路径，再执行解压，避免路径越界写入。
    """

    if rarfile is None:
        raise RuntimeError("当前环境未安装 rarfile，无法解压 .rar 文件。")

    with rarfile.RarFile(path) as rf:
        members = rf.infolist()
        for member in members:
            member_name = _normalize_member_name(getattr(member, "filename", ""))
            if not member_name:
                continue
            _ensure_within(save_path, save_path / member_name)
        for member in members:
            rf.extract(member, path=save_path)


def safe_extract_7z(path: Path, save_path: Path) -> None:
    """
    安全解压 7z。

    先读取成员名做路径检查，再执行解压。
    """

    if py7zr is None:
        raise RuntimeError("当前环境未安装 py7zr，无法解压 .7z 文件。")

    with py7zr.SevenZipFile(path, "r") as sf:
        member_names = sf.getnames()

    for member_name in member_names:
        normalized = _normalize_member_name(member_name)
        if not normalized:
            continue
        _ensure_within(save_path, save_path / normalized)

    with py7zr.SevenZipFile(path, "r") as sf:
        sf.extractall(path=save_path)

def extract_archive(path: Path, save_path: Path) -> None:
    """
    处理下载文件到可遍历目录。

    支持两类输入：
    1) 压缩包：zip/tar/rar/7z
    2) 直出文件：csv/ts（直接复制）
    """

    lower_name = path.name.lower()
    save_path.mkdir(parents=True, exist_ok=True)

    if lower_name.endswith((".csv", ".ts")):
        shutil.copy2(path, save_path / path.name)
        return

    if lower_name.endswith(".zip"):
        safe_extract_zip(path, save_path)
        return

    if lower_name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        safe_extract_tar(path, save_path)
        return

    if lower_name.endswith(".rar"):
        safe_extract_rar(path, save_path)
        return

    if lower_name.endswith(".7z"):
        safe_extract_7z(path, save_path)
        return

    if zipfile.is_zipfile(path):
        safe_extract_zip(path, save_path)
        return
    if tarfile.is_tarfile(path):
        safe_extract_tar(path, save_path)
        return

    raise RuntimeError(f"不支持的压缩格式: {path.name}")

def normalize_source_relpath(src_rel_path: Path, product: str) -> Path:
    """
    规范化解压目录内相对路径，去掉无关包装层。

    常见无关包装层：
    - product/...
    - product-daily/...
    - 2026-02-06/...（日期目录）
    """

    parts = list(src_rel_path.parts)
    if not parts:
        return src_rel_path

    if parts and parts[0] in KNOWN_DATASETS:
        parts = parts[1:]
    elif parts and normalize_product_name(parts[0]) == product:
        parts = parts[1:]

    if parts and DATE_NAME_PATTERN.fullmatch(parts[0]):
        parts = parts[1:]

    if not parts:
        return Path(src_rel_path.name)
    return Path(*parts)

def is_daily_aggregate_file(src_rel_path: Path) -> bool:
    """判断是否为按天聚合文件（例如 2026-02-06.csv）。"""

    if src_rel_path.suffix.lower() != ".csv":
        return False
    return bool(DATE_NAME_PATTERN.fullmatch(src_rel_path.stem))

def infer_target_relpath(src_rel_path: Path, product: str) -> Optional[Path]:
    """
    推断源文件应落到 xbx_data 下的相对路径。

    已知规则产品会做路径归一；
    未知产品会保持原始相对结构镜像到 xbx_data/<product>/ 下。
    """

    src_rel_path = normalize_source_relpath(src_rel_path, product)

    if src_rel_path.name == "period_offset.csv":
        return Path("period_offset.csv")
    if src_rel_path.name == "period_offset.ts":
        return Path("period_offset.ts")

    if product in TRADING_PRODUCTS or product in INDEX_PRODUCTS:
        if re.fullmatch(r"[a-z]{2}\d{6}\.csv", src_rel_path.name):
            return Path(product) / src_rel_path.name
        return None

    if product == "stock-fin-data-xbx":
        parent = src_rel_path.parent.name
        if re.fullmatch(r"[a-z]{2}\d{6}", parent) and src_rel_path.suffix.lower() == ".csv":
            return Path(product) / parent / src_rel_path.name
        match = re.match(r"^([a-z]{2}\d{6})_", src_rel_path.name)
        if match:
            return Path(product) / match.group(1) / src_rel_path.name
        return None

    if product == "period_offset":
        if src_rel_path.suffix.lower() == ".csv":
            return Path("period_offset.csv")
        if src_rel_path.suffix.lower() == ".ts":
            return Path("period_offset.ts")

    # 通用兜底：未知产品按原相对路径镜像
    if not src_rel_path.parts:
        return Path(product) / src_rel_path.name
    return Path(product) / src_rel_path

def infer_rule(rel_path: Path) -> Optional[DatasetRule]:
    """根据目标相对路径选择已知规则。"""

    if rel_path.name == "period_offset.csv":
        return RULES["period_offset.csv"]
    if not rel_path.parts:
        return None
    return RULES.get(rel_path.parts[0])

# CSV 读取与合并

def decode_text(path: Path, preferred_encoding: Optional[str]) -> Tuple[str, str]:
    """尝试多个编码读取文本，返回 (文本, 实际编码)。"""

    data = path.read_bytes()
    encodings = [preferred_encoding] if preferred_encoding else []
    encodings.extend([enc for enc in ENCODING_CANDIDATES if enc != preferred_encoding])
    for encoding in encodings:
        if not encoding:
            continue
        try:
            return data.decode(encoding), encoding
        except Exception:
            continue
    raise RuntimeError(f"无法识别文件编码: {path}")

def looks_like_header(row: Sequence[str]) -> bool:
    """粗略判断某一行是否像表头。"""

    if not row:
        return False
    first = row[0].strip().lstrip("\ufeff")
    known_first_col = {
        "股票代码",
        "candle_end_time",
        "candle_begin_time",
        "stock_code",
        "index_code",
        "symbol",
        "交易日期",
        "日期",
        "date",
    }
    if first in known_first_col:
        return True
    if first.startswith(("股票代码", "candle_end_time", "candle_begin_time", "stock_code", "symbol")):
        return True
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", first):
        return True
    if re.search(r"(date|time|code|symbol|股票|交易|指数|日期)", first, re.IGNORECASE):
        return True
    return False

def detect_delimiter(lines: Sequence[str]) -> str:
    """从样本文本推断分隔符。"""

    candidates = [",", "\t", ";", "|"]
    samples = [line for line in lines if line.strip()]
    if not samples:
        return ","
    sample = samples[1] if len(samples) > 1 else samples[0]

    best = ","
    best_score = -1
    for delimiter in candidates:
        score = sample.count(delimiter)
        if score > best_score:
            best = delimiter
            best_score = score
    return best if best_score > 0 else ","

def parse_csv_line(line: str, delimiter: str = ",") -> List[str]:
    """用 csv.reader 解析单行，兼容引号和转义。"""

    return next(csv.reader([line], delimiter=delimiter))

def read_csv_payload(path: Path, preferred_encoding: Optional[str] = None) -> CsvPayload:
    """
    读取 CSV，兼容两种结构：
    1) 第一行是表头
    2) 第一行备注 + 第二行表头
    """

    text, encoding = decode_text(path, preferred_encoding)
    lines = [line for line in text.splitlines() if line.strip() != ""]
    if not lines:
        return CsvPayload(note=None, header=[], rows=[], encoding=encoding, delimiter=",")

    delimiter = detect_delimiter(lines[:3])
    first = parse_csv_line(lines[0], delimiter=delimiter)
    second = parse_csv_line(lines[1], delimiter=delimiter) if len(lines) > 1 else []

    note: Optional[str] = None
    if looks_like_header(first):
        header = first
        data_start = 1
    elif looks_like_header(second):
        note = lines[0].lstrip("\ufeff")
        header = second
        data_start = 2
    else:
        header = first
        data_start = 1

    rows = [parse_csv_line(line, delimiter=delimiter) for line in lines[data_start:]]
    return CsvPayload(note=note, header=header, rows=rows, encoding=encoding, delimiter=delimiter)

def _normalize_header_cells(header: Sequence[str]) -> List[str]:
    """统一表头比较口径：去首尾空白并移除 BOM。"""

    return [col.strip().lstrip("\ufeff") for col in header]

def _headers_equal(left: Sequence[str], right: Sequence[str]) -> bool:
    """判断两份表头是否一致。"""

    return _normalize_header_cells(left) == _normalize_header_cells(right)

def align_rows(rows: Iterable[Sequence[str]], source_header: Sequence[str], target_header: Sequence[str]) -> List[List[str]]:
    """
    按列名对齐行数据，避免新旧 CSV 列顺序不同导致字段错位。
    """

    source_index = {col: idx for idx, col in enumerate(source_header)}
    target_rows: List[List[str]] = []
    for row in rows:
        row_list = list(row)
        new_row = [""] * len(target_header)
        for target_idx, col in enumerate(target_header):
            source_idx = source_index.get(col)
            if source_idx is not None and source_idx < len(row_list):
                new_row[target_idx] = row_list[source_idx]
        target_rows.append(new_row)
    return target_rows

def row_key(row: Sequence[str], key_indices: Sequence[int]) -> Tuple[str, ...]:
    """生成一行的主键。"""

    if not key_indices:
        return tuple(row)
    key = tuple(row[idx] if idx < len(row) else "" for idx in key_indices)
    if all(not cell for cell in key):
        return tuple(row)
    return key

def sortable_value(value: str) -> Tuple[int, object]:
    """
    把字符串转换成稳定可排序结构，兼容日期/数字/普通字符串。
    """

    value = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return (0, value)
    if re.fullmatch(r"\d{8}", value):
        return (0, f"{value[0:4]}-{value[4:6]}-{value[6:8]}")
    try:
        return (1, float(value))
    except Exception:
        return (2, value)

def merge_payload(existing: Optional[CsvPayload], incoming: CsvPayload, rule: DatasetRule) -> Tuple[CsvPayload, int]:
    """
    增量合并（只追加或覆盖变化部分，不重写全部历史）。

    合并策略：
    1) 以旧表头为准（无旧文件则用新表头）
    2) 新旧都按列名对齐
    3) 按主键去重（新数据覆盖旧数据）
    4) 按 sort_cols 排序，保证输出稳定
    """

    target_header = existing.header if existing and existing.header else incoming.header
    if not target_header:
        return (
            CsvPayload(
                note=existing.note if existing else incoming.note,
                header=[],
                rows=[],
                encoding=rule.encoding,
                delimiter=existing.delimiter if existing else incoming.delimiter,
            ),
            0,
        )

    existing_rows = align_rows(existing.rows, existing.header, target_header) if existing else []
    incoming_rows = align_rows(incoming.rows, incoming.header, target_header)

    key_cols = [col for col in rule.key_cols if col in target_header]
    key_indices = [target_header.index(col) for col in key_cols]

    merged_map: Dict[Tuple[str, ...], List[str]] = {}
    for row in existing_rows:
        merged_map[row_key(row, key_indices)] = row
    before_count = len(merged_map)

    for row in incoming_rows:
        merged_map[row_key(row, key_indices)] = row

    rows = list(merged_map.values())

    sort_indices = [target_header.index(col) for col in rule.sort_cols if col in target_header]
    if sort_indices:
        rows.sort(key=lambda row: tuple(sortable_value(row[idx]) for idx in sort_indices))

    note = None
    if rule.has_note:
        note = existing.note if existing and existing.note is not None else incoming.note
        if note is None:
            note = ""

    merged = CsvPayload(
        note=note,
        header=list(target_header),
        rows=rows,
        encoding=rule.encoding,
        delimiter=existing.delimiter if existing else incoming.delimiter,
    )
    return merged, max(0, len(merged_map) - before_count)

def write_csv_payload(path: Path, payload: CsvPayload, rule: DatasetRule, dry_run: bool) -> None:
    """把合并结果写回 CSV（dry-run 时跳过写盘）。"""

    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    delimiter = payload.delimiter or ","
    with path.open("w", encoding=rule.encoding, newline="") as f:
        if rule.has_note and payload.note is not None:
            f.write(payload.note.rstrip("\r\n"))
            f.write("\n")
        writer = csv.writer(f, delimiter=delimiter, lineterminator="\n")
        writer.writerow(payload.header)
        writer.writerows(payload.rows)

def sync_payload_to_target(incoming: CsvPayload, target: Path, rule: DatasetRule, dry_run: bool) -> Tuple[str, int]:
    """
    把已解析好的 CSV 同步到目标文件。
    返回：(状态, 新增行数)
    """

    if not incoming.header:
        return "skipped", 0

    existing = read_csv_payload(target, preferred_encoding=rule.encoding) if target.exists() else None
    merged, added_rows = merge_payload(existing, incoming, rule)

    if existing and merged.note == existing.note and merged.header == existing.header and merged.rows == existing.rows:
        return "unchanged", 0

    write_csv_payload(target, merged, rule, dry_run=dry_run)
    if existing:
        return "updated", added_rows
    return "created", len(merged.rows)

def sync_csv_file(src: Path, target: Path, rule: DatasetRule, dry_run: bool) -> Tuple[str, int]:
    """同步单个 CSV（读取 -> 合并 -> 写回）。"""

    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    return sync_payload_to_target(incoming=incoming, target=target, rule=rule, dry_run=dry_run)

def normalize_split_value(raw_value: str) -> str:
    """
    规范化拆分值，避免文件名越界或非法字符。

    例如“10年中债国债到期收益率”会原样保留；
    若值里包含 / 或 \\，会替换成下划线。
    """

    value = raw_value.strip()
    value = value.replace("/", "_").replace("\\", "_")
    return value

def sync_daily_aggregate_file(src: Path, product: str, data_root: Path, dry_run: bool) -> SyncStats:
    """
    处理按天聚合 CSV（例如 2026-02-06.csv）。

    这一步会按产品配置的“拆分字段”切成单文件，
    再用该产品的规则做增量合并，保持现有目录结构不变。
    """

    stats = SyncStats()
    rule = RULES[product]
    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    if not incoming.header:
        stats.skipped_files += 1
        return stats

    split_col = AGGREGATE_SPLIT_COLS.get(product)
    if not split_col:
        stats.skipped_files += 1
        return stats
    if split_col not in incoming.header:
        stats.skipped_files += 1
        log_info(f"[{product}] 未找到拆分字段，已跳过: {split_col}", event="SYNC_FAIL")
        return stats

    split_idx = incoming.header.index(split_col)
    grouped_rows: Dict[str, List[List[str]]] = {}
    for row in incoming.rows:
        split_raw = row[split_idx] if split_idx < len(row) else ""
        split_value = normalize_split_value(split_raw)
        if not split_value:
            continue
        grouped_rows.setdefault(split_value, []).append(list(row))

    if not grouped_rows:
        stats.skipped_files += 1
        return stats

    total_codes = len(grouped_rows)
    log_info(
        f"[{product}] 按天聚合文件拆分：file={src.name}, keys={total_codes}",
        event="SYNC_OK",
        split_col=split_col,
    )

    for idx, (split_value, rows) in enumerate(grouped_rows.items(), start=1):
        target = data_root / product / f"{split_value}.csv"
        payload = CsvPayload(
            note=incoming.note,
            header=list(incoming.header),
            rows=rows,
            encoding=incoming.encoding,
            delimiter=incoming.delimiter,
        )
        result, added_rows = sync_payload_to_target(incoming=payload, target=target, rule=rule, dry_run=dry_run)
        apply_file_result(stats, result=result, added_rows=added_rows)

        if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_codes:
            log_info(
                f"[{product}] 拆分进度 {idx}/{total_codes}",
                event="SYNC_OK",
                created=stats.created_files,
                updated=stats.updated_files,
                unchanged=stats.unchanged_files,
            )

    return stats

def sync_raw_file(src: Path, target: Path, dry_run: bool) -> str:
    """
    镜像写入（按原文件路径复制，不做字段级合并）。
    """

    src_bytes = src.read_bytes()
    existed_before = target.exists()
    if existed_before and target.read_bytes() == src_bytes:
        return "unchanged"

    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)

    return "updated" if existed_before else "created"

def apply_file_result(stats: SyncStats, result: str, added_rows: int = 0) -> None:
    """把单文件结果累加到统计对象。"""

    if result == "created":
        stats.created_files += 1
        stats.rows_added += max(0, added_rows)
    elif result == "updated":
        stats.updated_files += 1
        stats.rows_added += max(0, added_rows)
    elif result == "unchanged":
        stats.unchanged_files += 1
    else:
        stats.skipped_files += 1

# 同步主流程（文件级）

def iter_candidate_files(root: Path) -> Iterable[Path]:
    """遍历 extract 目录中需要处理的文件（.csv/.ts）。"""

    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".csv", ".ts"}:
            yield path

def sync_known_product(product: str, extract_path: Path, data_root: Path, dry_run: bool) -> Tuple[SyncStats, str]:
    """
    已知规则产品同步。

    处理逻辑：
    - 匹配到规则时做增量合并
    - 个别无法映射文件会降级镜像（reason_code=mirror_fallback）
    """

    stats = SyncStats()
    reason_code = REASON_OK

    files = sorted(iter_candidate_files(extract_path))
    total_files = len(files)
    for idx, src in enumerate(files, start=1):
        src_rel_path = src.relative_to(extract_path)
        normalized_rel_path = normalize_source_relpath(src_rel_path, product)

        if product in AGGREGATE_SPLIT_COLS and is_daily_aggregate_file(normalized_rel_path):
            agg = sync_daily_aggregate_file(src=src, product=product, data_root=data_root, dry_run=dry_run)
            stats.merge(agg)
            continue

        rel_path = infer_target_relpath(normalized_rel_path, product)
        if rel_path is None:
            stats.skipped_files += 1
            reason_code = REASON_MIRROR_FALLBACK
            log_info(f"[{product}] 无法映射路径，已跳过: {src_rel_path}", event="SYNC_FAIL")
            continue

        target = data_root / rel_path

        if src.suffix.lower() == ".ts":
            result = sync_raw_file(src=src, target=target, dry_run=dry_run)
            apply_file_result(stats, result=result)
            reason_code = REASON_MIRROR_FALLBACK
        else:
            rule = infer_rule(rel_path)
            if rule is None:
                result = sync_raw_file(src=src, target=target, dry_run=dry_run)
                apply_file_result(stats, result=result)
                reason_code = REASON_MIRROR_FALLBACK
            else:
                result, added_rows = sync_csv_file(src=src, target=target, rule=rule, dry_run=dry_run)
                apply_file_result(stats, result=result, added_rows=added_rows)

        if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_files:
            log_info(
                f"[{product}] 同步进度 {idx}/{total_files}",
                event="SYNC_OK",
                created=stats.created_files,
                updated=stats.updated_files,
                unchanged=stats.unchanged_files,
                skipped=stats.skipped_files,
            )

    return stats, reason_code

def sync_unknown_product(product: str, extract_path: Path, data_root: Path, dry_run: bool) -> Tuple[SyncStats, str]:
    """
    未知规则产品同步。

    轻量规则：
    1) 若本地已存在同名目标 CSV 且表头一致，则执行自动合并。
       自动合并采用“整行去重”（把整行当主键）策略，尽量避免重复写入。
    2) 其它情况保持镜像写入，不做字段级推断。
    """

    stats = SyncStats()
    did_unknown_header_merge = False
    files = sorted(iter_candidate_files(extract_path))
    total_files = len(files)

    for idx, src in enumerate(files, start=1):
        src_rel_path = src.relative_to(extract_path)
        normalized_rel_path = normalize_source_relpath(src_rel_path, product)
        rel_path = infer_target_relpath(normalized_rel_path, product)
        if rel_path is None:
            stats.skipped_files += 1
            continue

        target = data_root / rel_path
        if src.suffix.lower() == ".csv" and target.exists():
            # 轻量自动合并前置条件：
            # - 同名目标文件已存在
            # - 新旧表头一致
            # 命中后再走“整行去重合并”，否则走镜像写入。
            try:
                incoming = read_csv_payload(src)
                existing = read_csv_payload(target, preferred_encoding=incoming.encoding)
                if incoming.header and existing.header and _headers_equal(incoming.header, existing.header):
                    auto_rule = DatasetRule(
                        name=f"{product}:unknown_header_merge",
                        encoding=existing.encoding,
                        has_note=existing.note is not None,
                        key_cols=tuple(),
                        sort_cols=tuple(),
                    )
                    result, added_rows = sync_payload_to_target(
                        incoming=incoming,
                        target=target,
                        rule=auto_rule,
                        dry_run=dry_run,
                    )
                    apply_file_result(stats, result=result, added_rows=added_rows)
                    did_unknown_header_merge = True
                    log_debug(f"[{product}] 命中轻量自动合并: {src_rel_path}")
                    if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_files:
                        log_info(
                            f"[{product}] 轻量合并进度 {idx}/{total_files}",
                            event="SYNC_OK",
                            created=stats.created_files,
                            updated=stats.updated_files,
                            unchanged=stats.unchanged_files,
                            skipped=stats.skipped_files,
                        )
                    continue
            except Exception as exc:
                log_debug(f"[{product}] 轻量合并条件检查失败，改走镜像: {src_rel_path}, err={exc}")

        result = sync_raw_file(src=src, target=target, dry_run=dry_run)
        apply_file_result(stats, result=result)

        if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_files:
            log_info(
                f"[{product}] 镜像进度 {idx}/{total_files}",
                event="SYNC_OK",
                created=stats.created_files,
                updated=stats.updated_files,
                unchanged=stats.unchanged_files,
                skipped=stats.skipped_files,
            )

    if did_unknown_header_merge:
        return stats, REASON_UNKNOWN_HEADER_MERGE
    return stats, REASON_MIRROR_FALLBACK

def sync_from_extract(
    plan: ProductPlan,
    extract_path: Path,
    data_root: Path,
    dry_run: bool,
) -> Tuple[SyncStats, str]:
    """
    从 extract 目录同步到数据目录。

    这一步是“真正落库”的核心入口：
    - merge_known：已知规则产品做增量合并
    - mirror_unknown：未知规则产品做镜像写入
    """

    if plan.strategy == STRATEGY_MIRROR_UNKNOWN:
        return sync_unknown_product(
            product=plan.name,
            extract_path=extract_path,
            data_root=data_root,
            dry_run=dry_run,
        )

    return sync_known_product(
        product=plan.name,
        extract_path=extract_path,
        data_root=data_root,
        dry_run=dry_run,
    )

# 单产品编排与报告

def process_product(
    plan: ProductPlan,
    date_time: Optional[str],
    api_base: str,
    hid: str,
    headers: Dict[str, str],
    data_root: Path,
    work_dir: Path,
    dry_run: bool,
) -> Tuple[str, str, SyncStats, str, str]:
    """
    处理单个产品完整流程。

    流程：
    1) 获取 latest（若未指定 date_time）
    2) 获取下载链接并下载
    3) 解压到 extract
    4) 根据策略同步到 data_root

    返回：
    (product, actual_time, stats, source_path, reason_code)
    """

    product = normalize_product_name(plan.name)
    t0 = time.time()

    log_info(f"[{product}] 开始处理，策略={plan.strategy}", event="PRODUCT_PLAN")

    actual_time = _resolve_actual_time(
        product=product,
        date_time=date_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
    )
    download_path, extract_path = _download_and_prepare_extract(
        product=product,
        actual_time=actual_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
        work_dir=work_dir,
    )
    _extract_product_archive(product=product, download_path=download_path, extract_path=extract_path)

    # 4) 落库
    try:
        stats, reason_code = sync_from_extract(plan=plan, extract_path=extract_path, data_root=data_root, dry_run=dry_run)
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 落库失败；可能原因：文件结构异常或合并规则不匹配；"
                f"建议：先用 --dry-run 排查。原始错误：{exc}"
            ),
            reason_code=REASON_MERGE_ERROR,
        ) from exc

    elapsed = time.time() - t0
    log_info(
        f"[{product}] 处理完成，用时 {elapsed:.2f}s",
        event="SYNC_OK",
        created=stats.created_files,
        updated=stats.updated_files,
        unchanged=stats.unchanged_files,
        skipped=stats.skipped_files,
        rows_added=stats.rows_added,
    )

    return product, actual_time, stats, str(extract_path), reason_code

def _resolve_actual_time(
    product: str,
    date_time: Optional[str],
    api_base: str,
    hid: str,
    headers: Dict[str, str],
) -> str:
    """解析单产品实际下载时间（优先用户指定，否则取 latest）。"""

    if date_time:
        return date_time
    try:
        return get_latest_time(api_base=api_base, product=product, hid=hid, headers=headers)
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 获取最新时间失败；可能原因：网络异常、权限不足或接口限制；"
                f"建议：检查 APIKEY/HID 与网络后重试。原始错误：{exc}"
            ),
            reason_code=REASON_NETWORK_ERROR,
        ) from exc

def _download_and_prepare_extract(
    product: str,
    actual_time: str,
    api_base: str,
    hid: str,
    headers: Dict[str, str],
    work_dir: Path,
) -> Tuple[Path, Path]:
    """下载单产品文件并准备 extract 目录。"""

    try:
        file_url = get_download_link(api_base=api_base, product=product, date_time=actual_time, hid=hid, headers=headers)
        file_name = build_file_name(file_url, product, actual_time)
        product_work = work_dir / product / actual_time
        download_path = product_work / file_name
        extract_path = product_work / "extract"

        if extract_path.exists():
            shutil.rmtree(extract_path)
        extract_path.mkdir(parents=True, exist_ok=True)
        if not download_path.exists() or download_path.stat().st_size == 0:
            save_file(file_url=file_url, file_path=download_path, headers=headers)
        log_info(f"[{product}] 下载完成: {download_path}", event="DOWNLOAD_OK")
        return download_path, extract_path
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 下载失败；可能原因：网络波动、下载额度限制或链接失效；"
                f"建议：稍后重试并确认下载权限。原始错误：{exc}"
            ),
            reason_code=REASON_NETWORK_ERROR,
        ) from exc

def _extract_product_archive(product: str, download_path: Path, extract_path: Path) -> None:
    """解压单产品文件。"""

    try:
        extract_archive(download_path, extract_path)
        log_info(f"[{product}] 解压完成: {extract_path}", event="EXTRACT_OK")
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 解压失败；可能原因：压缩包损坏或格式不支持；"
                f"建议：删除缓存后重试。原始错误：{exc}"
            ),
            reason_code=REASON_EXTRACT_ERROR,
        ) from exc

def write_run_report(path: Path, report: RunReport) -> None:
    """将本次运行报告写入 JSON 文件。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = run_report_to_dict(report)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

# 状态库与命令行（Typer 子命令模式）

def build_scheduler_placeholder(config: SchedulerConfig) -> BackgroundScheduler:
    """
    构建调度器（定时任务框架：只初始化，不启动）。

    说明：
    - 这里只做接口预留，方便后续接自动更新。
    - 当前版本默认不注册任何定时任务。
    """

    scheduler = BackgroundScheduler(timezone=config.timezone)
    log_debug("调度器已初始化（仅预留接口，当前不启动）。", event="SCHEDULER_INIT", enabled=config.enabled)
    return scheduler


def status_db_path(data_root: Path) -> Path:
    """返回状态数据库路径。"""

    return data_root / "code" / "data" / "FuelBinStat.db"


def status_json_path(data_root: Path) -> Path:
    """返回 products-status.json 路径。"""

    return data_root / "code" / "data" / "products-status.json"


def ensure_status_table(conn: sqlite3.Connection) -> None:
    """确保状态表存在（product_status）。"""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_status (
            name TEXT PRIMARY KEY,
            display_name TEXT,
            full_data TEXT,
            last_update_time TEXT,
            next_update_time TEXT,
            data_time TEXT,
            data_content_time TEXT,
            is_auto_update INTEGER DEFAULT 0,
            can_auto_update INTEGER DEFAULT 1,
            add_time TEXT,
            is_listed INTEGER DEFAULT 1,
            full_data_download_url TEXT,
            full_data_download_expires TEXT,
            ts TEXT
        )
        """
    )
    conn.commit()


def connect_status_db(data_root: Path, read_only: bool = False) -> sqlite3.Connection:
    """连接状态库（sqlite3：Python 内置轻量数据库）。"""

    db_path = status_db_path(data_root)
    if read_only:
        # dry-run 只能读现有状态，不允许隐式建库建表。
        if not db_path.exists():
            raise RuntimeError(f"状态库不存在（只读模式无法初始化）: {db_path}")
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_status_table(conn)
    return conn


def load_product_status(conn: sqlite3.Connection, product: str) -> Optional[ProductStatus]:
    """读取单产品状态。"""

    row = conn.execute("SELECT * FROM product_status WHERE name = ?", (product,)).fetchone()
    if not row:
        return None
    return ProductStatus(**dict(row))


def upsert_product_status(conn: sqlite3.Connection, status: ProductStatus) -> None:
    """写入或更新单产品状态。"""

    payload = status.model_dump()
    if not payload.get("add_time"):
        payload["add_time"] = utc_now_iso()
    payload["ts"] = utc_now_iso()

    conn.execute(
        """
        INSERT INTO product_status (
            name, display_name, full_data, last_update_time, next_update_time,
            data_time, data_content_time, is_auto_update, can_auto_update,
            add_time, is_listed, full_data_download_url, full_data_download_expires, ts
        ) VALUES (
            :name, :display_name, :full_data, :last_update_time, :next_update_time,
            :data_time, :data_content_time, :is_auto_update, :can_auto_update,
            :add_time, :is_listed, :full_data_download_url, :full_data_download_expires, :ts
        )
        ON CONFLICT(name) DO UPDATE SET
            display_name=excluded.display_name,
            full_data=excluded.full_data,
            last_update_time=excluded.last_update_time,
            next_update_time=excluded.next_update_time,
            data_time=excluded.data_time,
            data_content_time=excluded.data_content_time,
            is_auto_update=excluded.is_auto_update,
            can_auto_update=excluded.can_auto_update,
            add_time=COALESCE(product_status.add_time, excluded.add_time),
            is_listed=excluded.is_listed,
            full_data_download_url=excluded.full_data_download_url,
            full_data_download_expires=excluded.full_data_download_expires,
            ts=excluded.ts
        """,
        payload,
    )
    conn.commit()


def list_product_status(conn: sqlite3.Connection) -> List[ProductStatus]:
    """读取全部产品状态。"""

    rows = conn.execute("SELECT * FROM product_status ORDER BY name").fetchall()
    return [ProductStatus(**dict(row)) for row in rows]


def export_status_json(conn: sqlite3.Connection, output_path: Path) -> None:
    """导出官方兼容 products-status.json。"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, object] = {}
    for item in list_product_status(conn):
        payload[item.name] = item.to_json_record()
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_run_event(report: RunReport, product: str, stage: str, status: str, reason_code: str, detail: str) -> None:
    report.events.append(
        RunEvent(
            ts=utc_now_iso(),
            product=product,
            stage=stage,
            status=status,
            reason_code=reason_code,
            detail=detail,
        )
    )


def _append_success_result(
    report: RunReport,
    plan: ProductPlan,
    product: str,
    actual_time: str,
    stats: SyncStats,
    source_path: str,
    reason_code: str,
    elapsed: float,
) -> None:
    report.products.append(
        ProductRunResult(
            product=product,
            status="ok",
            strategy=plan.strategy,
            reason_code=reason_code,
            date_time=actual_time,
            mode="network",
            elapsed_seconds=elapsed,
            stats=stats,
            source_path=source_path,
        )
    )
    _append_run_event(report, product, "SYNC", "ok", reason_code, f"elapsed={elapsed:.2f}s")


def _append_error_result(
    report: RunReport,
    plan: ProductPlan,
    reason_code: str,
    requested_date_time: str,
    elapsed: float,
    error_message: str,
) -> None:
    report.products.append(
        ProductRunResult(
            product=plan.name,
            status="error",
            strategy=plan.strategy,
            reason_code=reason_code,
            date_time=requested_date_time,
            mode="network",
            elapsed_seconds=elapsed,
            error=error_message,
        )
    )
    _append_run_event(report, plan.name, "SYNC", "error", reason_code, error_message)


def _record_discovery_skips(report: RunReport, unknown_local: Sequence[str], invalid_explicit: Sequence[str]) -> None:
    """把“本地未知目录/无效显式产品”写入报告。"""

    for product in sorted(unknown_local):
        report.products.append(
            ProductRunResult(
                product=product,
                status="skipped",
                strategy="skip",
                reason_code=REASON_UNKNOWN_LOCAL_PRODUCT,
                mode="discover",
                error="本地目录不在 catalog 产品清单中，已跳过。",
            )
        )
        _append_run_event(report, product, "DISCOVER", "skipped", REASON_UNKNOWN_LOCAL_PRODUCT, "本地目录不在 catalog")

    for product in sorted(invalid_explicit):
        report.products.append(
            ProductRunResult(
                product=product,
                status="skipped",
                strategy="skip",
                reason_code=REASON_INVALID_EXPLICIT_PRODUCT,
                mode="explicit",
                error="显式指定产品不在 catalog 清单中，已跳过。",
            )
        )
        _append_run_event(report, product, "PLAN", "skipped", REASON_INVALID_EXPLICIT_PRODUCT, "显式产品不在 catalog")


def resolve_report_path(ctx: CommandContext, command: str) -> Path:
    """解析报告输出路径。"""

    if ctx.report_file:
        return ctx.report_file.resolve()
    return (ctx.work_dir / f"run_report_{ctx.run_id}_{command}.json").resolve()


def load_catalog_or_raise(catalog_file: Path) -> List[str]:
    """读取 catalog，失败时抛异常。"""

    return load_products_from_catalog(catalog_file)


def build_headers_or_raise(ctx: CommandContext) -> Tuple[Dict[str, str], str]:
    """构建请求头并校验凭证。"""

    api_key, hid = resolve_credentials(cli_api_key=ctx.api_key, cli_hid=ctx.hid, secrets_file=ctx.secrets_file.resolve())
    if not api_key:
        raise RuntimeError(
            f"缺少 api-key；可能原因：命令行/环境变量/本地密钥文件都未提供；建议：配置 --api-key 或更新 {ctx.secrets_file}。"
        )
    if not hid:
        raise RuntimeError(f"缺少 hid；可能原因：命令行/环境变量/本地密钥文件都未提供；建议：配置 --hid 或更新 {ctx.secrets_file}。")

    return {
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/100.0.4896.127 Safari/537.36"
        ),
        "content-type": "application/json",
        "api-key": api_key,
    }, hid


def _execute_plans(
    plans: Sequence[ProductPlan],
    command_ctx: CommandContext,
    report: RunReport,
    requested_date_time: str = "",
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[SyncStats, bool, float]:
    """执行产品计划并返回汇总统计。"""

    headers, hid = build_headers_or_raise(command_ctx)
    total = SyncStats()
    has_error = False
    t_run_start = time.time()

    for plan in plans:
        t_product_start = time.time()
        try:
            product, actual_time, stats, source_path, reason_code = process_product(
                plan=plan,
                date_time=requested_date_time or None,
                api_base=command_ctx.api_base.rstrip("/"),
                hid=hid,
                headers=headers,
                data_root=command_ctx.data_root,
                work_dir=command_ctx.work_dir,
                dry_run=command_ctx.dry_run,
            )
            elapsed = time.time() - t_product_start
            total.merge(stats)
            _append_success_result(report, plan, product, actual_time, stats, source_path, reason_code, elapsed)
            if conn is not None and not command_ctx.dry_run:
                old_status = load_product_status(conn, product)
                status = old_status or ProductStatus(name=product, display_name=product)
                status.last_update_time = utc_now_iso()
                status.data_time = actual_time
                status.data_content_time = actual_time
                upsert_product_status(conn, status)
            continue
        except ProductSyncError as exc:
            reason_code = exc.reason_code
            message = str(exc)
        except Exception as exc:
            reason_code = REASON_MERGE_ERROR
            message = str(exc)

        has_error = True
        elapsed = time.time() - t_product_start
        _append_error_result(report, plan, reason_code, requested_date_time, elapsed, message)
        log_error(f"[{plan.name}] 处理失败: {message}", event="SYNC_FAIL", reason_code=reason_code)
        if command_ctx.verbose:
            log_debug(traceback.format_exc(), event="DEBUG")
        if command_ctx.stop_on_error:
            log_error("已开启 stop-on-error，任务提前停止。", event="RUN_SUMMARY")
            break

    return total, has_error, t_run_start


def _finalize_and_write_report(
    report: RunReport,
    total: SyncStats,
    has_error: bool,
    t_run_start: float,
    report_path: Path,
) -> int:
    """汇总结果并写入报告。"""

    report.summary = total
    report.ended_at = utc_now_iso()
    report.duration_seconds = time.time() - t_run_start
    report.success_total = sum(1 for x in report.products if x.status == "ok")
    report.failed_total = sum(1 for x in report.products if x.status == "error")
    report.skipped_total = sum(1 for x in report.products if x.status == "skipped")

    log_info(
        "本次运行汇总完成。",
        event="RUN_SUMMARY",
        discovered_total=report.discovered_total,
        planned_total=report.planned_total,
        success_total=report.success_total,
        failed_total=report.failed_total,
        skipped_total=report.skipped_total,
        created=total.created_files,
        updated=total.updated_files,
        unchanged=total.unchanged_files,
        skipped_files=total.skipped_files,
        rows_added=total.rows_added,
        duration_seconds=round(report.duration_seconds, 2),
    )

    write_run_report(report_path, report)
    log_info("运行报告已写入。", event="RUN_SUMMARY", report_file=str(report_path))
    return 1 if has_error else 0


def _parse_expire_ts(raw_value: Optional[object]) -> Optional[float]:
    """把过期字段转换为 Unix 时间戳（秒）。"""

    if not raw_value:
        return None
    s = str(raw_value).strip()
    if not s:
        return None
    if re.fullmatch(r"\d+", s):
        value = int(s)
        if value > 10**12:
            value = value // 1000
        return float(value)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _is_link_expired(raw_value: Optional[object]) -> bool:
    """判断全量链接是否已过期。"""

    ts = _parse_expire_ts(raw_value)
    if ts is None:
        return False
    return ts < time.time()


def _extract_url_and_expire_from_response(resp: requests.Response) -> Tuple[str, Optional[str]]:
    """从候选响应中抽取下载地址和过期时间。"""

    if resp.status_code in {301, 302, 303, 307, 308}:
        location = (resp.headers.get("Location") or "").strip()
        if location.startswith("http"):
            return location, None
        return "", None

    text = (resp.text or "").strip()
    if text.startswith("http"):
        return text, None

    try:
        payload = resp.json()
    except Exception:
        return "", None

    candidates: List[object] = [
        payload.get("url"),
        payload.get("download_url"),
        payload.get("downloadUrl"),
    ]
    if isinstance(payload.get("data"), dict):
        data = payload["data"]
        candidates.extend([data.get("url"), data.get("download_url"), data.get("downloadUrl")])
    url = next((x for x in candidates if isinstance(x, str) and x.startswith("http")), "")

    expires_candidates: List[object] = [
        payload.get("expires"),
        payload.get("expire"),
        payload.get("expiredAt"),
        payload.get("expiresAt"),
    ]
    if isinstance(payload.get("data"), dict):
        data = payload["data"]
        expires_candidates.extend([data.get("expires"), data.get("expire"), data.get("expiredAt"), data.get("expiresAt")])
    expires = next((str(x) for x in expires_candidates if x is not None and str(x).strip()), None)
    return url, expires


def resolve_full_data_link(
    api_base: str,
    product: str,
    full_data_name: str,
    hid: str,
    headers: Dict[str, str],
) -> Tuple[str, Optional[str]]:
    """
    获取全量数据链接。

    说明：
    - 官方公开文档未固定 full_data_link 端点时，这里采用“候选地址轮询”。
    - 只要任意候选返回有效 URL，就视为成功。
    """

    base = api_base.rstrip("/")
    product = normalize_product_name(product)
    candidates = [
        f"{base}/full-data-link/{product}/{full_data_name}?uuid={hid}",
        f"{base}/full-data-link/{full_data_name}?uuid={hid}",
        f"{base}/get-full-data-link/{product}/{full_data_name}?uuid={hid}",
        f"{base}/get-full-data-link/{full_data_name}?uuid={hid}",
        f"https://www.quantclass.cn/api/product/data-route/{full_data_name}",
        f"https://www.quantclass.cn/api/product/data-route/{product}",
    ]

    errors: List[str] = []
    for url in candidates:
        try:
            resp = requests.get(url, headers=headers, timeout=30, allow_redirects=False)
            if resp.status_code in {401, 403, 404, 500}:
                errors.append(f"{url} -> {resp.status_code}")
                continue
            if resp.status_code not in {200, 301, 302, 303, 307, 308}:
                errors.append(f"{url} -> {resp.status_code}")
                continue
            download_url, expires = _extract_url_and_expire_from_response(resp)
            if download_url:
                return download_url, expires
            errors.append(f"{url} -> empty_url")
        except Exception as exc:
            errors.append(f"{url} -> {exc}")

    raise RuntimeError(
        "全量链接获取失败；可能原因：接口端点变化或权限不足；建议：先用 full_data_link 检查产品名。"
        + (f" 调试信息：{'; '.join(errors[:3])}" if errors else "")
    )


def _locate_extracted_product_dir(extract_root: Path, product: str) -> Optional[Path]:
    """在解压目录中定位产品目录。"""

    if not extract_root.exists() or not extract_root.is_dir():
        return None

    direct = [
        p
        for p in extract_root.iterdir()
        if p.is_dir() and normalize_product_name(p.name) == product and _dir_has_data_files(p)
    ]
    if direct:
        return sorted(direct, key=lambda x: len(x.parts))[0]

    nested = sorted(
        [
            p
            for p in extract_root.rglob("*")
            if p.is_dir() and normalize_product_name(p.name) == product and _dir_has_data_files(p)
        ],
        key=lambda x: len(x.parts),
    )
    if nested:
        return nested[0]

    has_direct_files = any(
        p.is_file() and p.suffix.lower() in {".csv", ".ts"}
        for p in extract_root.iterdir()
    )
    if has_direct_files:
        # 兼容“文件直接落在根目录”的全量包形态。
        return extract_root
    return None


def _replace_product_dir_with_backup(product: str, source_dir: Path, data_root: Path, run_id: str, dry_run: bool) -> Tuple[Path, Path]:
    """执行“先备份后覆盖”全量恢复（原子替换 + 失败回滚）。"""

    target_dir = data_root / product
    backup_dir = DEFAULT_FULL_BACKUP_DIR / run_id / product
    if dry_run:
        return backup_dir, target_dir

    if not source_dir.exists() or not source_dir.is_dir():
        raise RuntimeError(f"全量恢复源目录不存在或不可用: {source_dir}")

    target_dir.parent.mkdir(parents=True, exist_ok=True)
    backup_dir.parent.mkdir(parents=True, exist_ok=True)

    # staging/old 放在 target 同级目录，确保 rename 原子性。
    staging_dir = target_dir.parent / f".{product}.staging.{run_id}"
    old_dir = target_dir.parent / f".{product}.old.{run_id}"
    for tmp_dir in (staging_dir, old_dir):
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    shutil.copytree(source_dir, staging_dir)

    if target_dir.exists():
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        shutil.copytree(target_dir, backup_dir)

    try:
        if target_dir.exists():
            os.replace(target_dir, old_dir)
        os.replace(staging_dir, target_dir)
    except Exception as exc:
        # 回滚顺序：先清理半成品目标，再恢复旧目录。
        if target_dir.exists() and old_dir.exists():
            shutil.rmtree(target_dir)
            os.replace(old_dir, target_dir)
        elif old_dir.exists() and not target_dir.exists():
            os.replace(old_dir, target_dir)

        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        raise RuntimeError(f"全量恢复替换失败，已尝试回滚: {exc}") from exc
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)

    if old_dir.exists():
        shutil.rmtree(old_dir)

    return backup_dir, target_dir


def _new_report(run_id: str, mode: str) -> RunReport:
    return RunReport(schema_version="3.0", run_id=run_id, started_at=utc_now_iso(), mode=mode)


app = typer.Typer(
    help="QuantClass 数据同步脚本（官方命令兼容 + 本地安全增强）",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)


@app.callback()
def global_options(
    ctx: typer.Context,
    data_root: Path = typer.Option(DEFAULT_DATA_ROOT, "--data-root", help="数据根目录。"),
    api_key: str = typer.Option("", "--api-key", help="QuantClass API Key。"),
    hid: str = typer.Option("", "--hid", help="QuantClass HID。"),
    secrets_file: Path = typer.Option(DEFAULT_SECRETS_FILE, "--secrets-file", help="本地密钥文件路径。"),
    dry_run: bool = typer.Option(False, "--dry-run", help="演练模式（不写业务数据和状态文件）。"),
    report_file: Optional[Path] = typer.Option(None, "--report-file", help="报告输出路径（JSON）。"),
    stop_on_error: bool = typer.Option(False, "--stop-on-error", help="遇错即停。"),
    verbose: bool = typer.Option(False, "--verbose", help="显示调试日志。"),
) -> None:
    """全局参数（所有子命令共享）。"""

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    global LOGGER, PROGRESS_EVERY
    LOGGER = ConsoleLogger(level="DEBUG" if verbose else "INFO", run_id=run_id)
    PROGRESS_EVERY = max(1, DEFAULT_PROGRESS_EVERY)

    data_root = data_root.resolve()
    if not data_root.exists():
        raise typer.BadParameter(f"data-root 不存在：{data_root}")

    command_ctx = CommandContext(
        run_id=run_id,
        data_root=data_root,
        api_key=api_key,
        hid=hid,
        secrets_file=secrets_file.resolve(),
        dry_run=dry_run,
        report_file=report_file.resolve() if report_file else None,
        stop_on_error=stop_on_error,
        verbose=verbose,
        mode="network",
        api_base=DEFAULT_API_BASE,
        catalog_file=DEFAULT_CATALOG_FILE.resolve(),
        work_dir=DEFAULT_WORK_DIR.resolve(),
    )
    ctx.obj = command_ctx
    build_scheduler_placeholder(SchedulerConfig())


def _ctx(ctx: typer.Context) -> CommandContext:
    value = ctx.obj
    if not isinstance(value, CommandContext):
        raise RuntimeError("运行上下文初始化失败；请通过命令行调用子命令。")
    return value


def _extract_command_context(args: tuple, kwargs: dict) -> Optional[CommandContext]:
    """从命令参数中提取 CommandContext（用于异常时决定是否打印调试堆栈）。"""

    raw_ctx = kwargs.get("ctx") or (args[0] if args else None)
    if isinstance(raw_ctx, typer.Context):
        obj = raw_ctx.obj
        if isinstance(obj, CommandContext):
            return obj
    return None


def _handle_command_exception(command_name: str, exc: Exception, reason_code: str, args: tuple, kwargs: dict) -> None:
    """统一命令级异常输出，避免把冗长 traceback 直接暴露给普通用户。"""

    log_error(
        f"{command_name} 执行失败；可能原因：{exc}；建议：检查参数、网络和密钥后重试。",
        event="CMD_DONE",
        reason_code=reason_code,
    )
    command_ctx = _extract_command_context(args, kwargs)
    if command_ctx and command_ctx.verbose:
        log_debug(traceback.format_exc(), event="DEBUG")


def command_guard(command_name: str):
    """
    命令级异常兜底装饰器。

    目的：把未处理异常转换为清晰中文报错 + 非零退出码。
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except typer.Exit:
                raise
            except ProductSyncError as exc:
                _handle_command_exception(command_name, exc, exc.reason_code, args, kwargs)
                raise typer.Exit(code=1)
            except Exception as exc:
                _handle_command_exception(command_name, exc, REASON_MERGE_ERROR, args, kwargs)
                raise typer.Exit(code=1)

        return wrapper

    return decorator


@app.command("init")
@command_guard("init")
def cmd_init(ctx: typer.Context) -> None:
    """
    初始化产品状态快照。

    这一步只更新状态文件，不下载数据。
    """

    command_ctx = _ctx(ctx)
    t0 = time.time()
    log_info("开始执行 init。", event="CMD_START")

    catalog = load_catalog_or_raise(command_ctx.catalog_file)
    discovered = discover_local_products(command_ctx.data_root, catalog)
    local_set = {x.name for x in discovered}

    if command_ctx.dry_run:
        elapsed = time.time() - t0
        log_info(
            "dry-run：init 仅完成状态扫描预演，未写入状态库与状态 JSON。",
            event="CMD_DONE",
            products=len(catalog),
            discovered_local=len(local_set),
            elapsed=round(elapsed, 2),
        )
        return

    conn = connect_status_db(command_ctx.data_root)
    try:
        for product in catalog:
            old = load_product_status(conn, product)
            status = old or ProductStatus(name=product, display_name=product)
            status.display_name = status.display_name or product
            status.is_listed = 1
            status.can_auto_update = 1
            if product in local_set:
                status.last_update_time = utc_now_iso()
            upsert_product_status(conn, status)
        export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        conn.close()

    elapsed = time.time() - t0
    log_info("init 执行完成。", event="CMD_DONE", products=len(catalog), elapsed=round(elapsed, 2))


@app.command("one_data")
@command_guard("one_data")
def cmd_one_data(
    ctx: typer.Context,
    product: str = typer.Argument(..., help="产品英文名（可带 -daily）。"),
    date_time: str = typer.Option("", "--date-time", help="指定下载日期（可选）。"),
) -> None:
    """更新单个产品。"""

    command_ctx = _ctx(ctx)
    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, "one_data")
    plan = build_product_plan([normalize_product_name(product)])
    report.planned_total = len(plan)

    log_info("开始执行 one_data。", event="CMD_START", product=product)
    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        total, has_error, t_run_start = _execute_plans(
            plans=plan,
            command_ctx=command_ctx,
            report=report,
            requested_date_time=date_time.strip(),
            conn=conn,
        )
        if conn is not None:
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()
    exit_code = _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
    log_info("one_data 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command("all_data")
@command_guard("all_data")
def cmd_all_data(
    ctx: typer.Context,
    mode: str = typer.Option("local", "--mode", help="local=本地存量更新；catalog=全量轮询。"),
    products: List[str] = typer.Option([], "--products", help="显式产品（可重复传参，也支持逗号分隔）。"),
) -> None:
    """批量更新产品。"""

    command_ctx = _ctx(ctx)
    mode = (mode or "local").strip().lower()
    if mode not in RUN_MODES:
        raise typer.BadParameter("mode 仅支持 local 或 catalog")

    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, "all_data")
    catalog_products = load_catalog_or_raise(command_ctx.catalog_file)

    log_info("开始扫描本地产品目录。", event="DISCOVER_START", data_root=str(command_ctx.data_root), mode=mode)
    discovered = discover_local_products(data_root=command_ctx.data_root, catalog_products=catalog_products)
    report.discovered_total = len(discovered)
    log_info("本地产品扫描完成。", event="DISCOVER_DONE", discovered_total=report.discovered_total)

    planned_products, unknown_local, invalid_explicit = resolve_products_by_mode(
        mode=mode,
        raw_products=products,
        catalog_products=catalog_products,
        discovered_local=discovered,
    )
    _record_discovery_skips(report, unknown_local, invalid_explicit)

    if mode == "local" and not planned_products and not products:
        report.failed_total = 1
        report.ended_at = utc_now_iso()
        report.duration_seconds = 0.0
        write_run_report(report_path, report)
        log_error(
            "未发现可更新的本地合法产品；可能原因：data_root 下没有命中 catalog 的目录；建议：先准备本地产品目录。",
            event="RUN_SUMMARY",
            reason_code=REASON_NO_LOCAL_PRODUCTS,
        )
        raise typer.Exit(code=1)

    plans = build_product_plan(planned_products)
    report.planned_total = len(plans)
    if not plans:
        report.ended_at = utc_now_iso()
        report.duration_seconds = 0.0
        write_run_report(report_path, report)
        log_error("执行清单为空，任务结束。", event="RUN_SUMMARY")
        raise typer.Exit(code=1)

    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        total, has_error, t_run_start = _execute_plans(plans, command_ctx, report, requested_date_time="", conn=conn)
        if conn is not None:
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()

    exit_code = _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
    log_info("all_data 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command("full_data_link")
@command_guard("full_data_link")
def cmd_full_data_link(
    ctx: typer.Context,
    product: str = typer.Argument(..., help="产品英文名。"),
    full_data_name: str = typer.Argument(..., help="全量数据名称。"),
) -> None:
    """拉取并缓存全量下载链接。"""

    command_ctx = _ctx(ctx)
    t0 = time.time()
    product = normalize_product_name(product)
    log_info("开始执行 full_data_link。", event="CMD_START", product=product, full_data_name=full_data_name)

    headers, hid = build_headers_or_raise(command_ctx)
    url, expires = resolve_full_data_link(
        api_base=command_ctx.api_base,
        product=product,
        full_data_name=full_data_name,
        hid=hid,
        headers=headers,
    )

    if command_ctx.dry_run:
        log_info(
            "dry-run：已获取全量链接，但未写入状态库与状态 JSON。",
            event="CMD_DONE",
            product=product,
            has_expires=bool(expires),
        )
    else:
        conn = connect_status_db(command_ctx.data_root)
        try:
            old = load_product_status(conn, product)
            status = old or ProductStatus(name=product, display_name=product)
            status.full_data = full_data_name
            status.full_data_download_url = url
            status.full_data_download_expires = expires
            status.last_update_time = utc_now_iso()
            upsert_product_status(conn, status)
            export_status_json(conn, status_json_path(command_ctx.data_root))
        finally:
            conn.close()

    elapsed = time.time() - t0
    log_info("full_data_link 执行完成。", event="CMD_DONE", product=product, elapsed=round(elapsed, 2))


@app.command("full_data")
@command_guard("full_data")
def cmd_full_data(
    ctx: typer.Context,
    product: str = typer.Argument(..., help="产品英文名。"),
) -> None:
    """
    下载并恢复全量数据（先备份后覆盖）。

    覆盖前会自动备份旧目录，失败时可以回滚。
    """

    command_ctx = _ctx(ctx)
    product = normalize_product_name(product)
    t0 = time.time()
    log_info("开始执行 full_data。", event="CMD_START", product=product)

    conn: Optional[sqlite3.Connection] = None
    try:
        try:
            conn = connect_status_db(command_ctx.data_root, read_only=command_ctx.dry_run)
        except RuntimeError as exc:
            raise ProductSyncError(
                message=(
                    f"产品 {product} 缺少可读取的状态库；可能原因：尚未执行 full_data_link；"
                    f"建议：先运行 full_data_link {product} <full_data_name>。原始错误：{exc}"
                ),
                reason_code=REASON_FULL_DATA_LINK_MISSING,
            ) from exc

        status = load_product_status(conn, product)
        if status is None or not status.full_data_download_url:
            raise ProductSyncError(
                message=(
                    f"产品 {product} 缺少全量下载链接；可能原因：尚未执行 full_data_link；"
                    f"建议：先运行 full_data_link {product} <full_data_name>。"
                ),
                reason_code=REASON_FULL_DATA_LINK_MISSING,
            )

        if _is_link_expired(status.full_data_download_expires):
            raise ProductSyncError(
                message=(
                    f"产品 {product} 的全量链接已过期；可能原因：链接超过有效期；"
                    f"建议：先重新执行 full_data_link {product}。"
                ),
                reason_code=REASON_FULL_DATA_EXPIRED,
            )

        zip_cache_dir = DEFAULT_ZIP_CACHE_DIR
        zip_name = Path(unquote(urlparse(status.full_data_download_url).path)).name or f"{product}.zip"
        zip_path = zip_cache_dir / zip_name

        headers, _ = build_headers_or_raise(command_ctx)
        if not command_ctx.dry_run:
            zip_cache_dir.mkdir(parents=True, exist_ok=True)
            save_file(status.full_data_download_url, zip_path, headers=headers)
            log_info("全量压缩包下载完成。", event="DOWNLOAD_OK", file=str(zip_path))
        else:
            log_info("dry-run：跳过全量压缩包下载。", event="DOWNLOAD_OK", file=str(zip_path))

        extract_root = command_ctx.work_dir / "full_extract" / product / command_ctx.run_id
        if not command_ctx.dry_run:
            if extract_root.exists():
                shutil.rmtree(extract_root)
            extract_root.mkdir(parents=True, exist_ok=True)
            extract_archive(zip_path, extract_root)
            log_info("全量压缩包解压完成。", event="EXTRACT_OK", path=str(extract_root))
            source_dir = _locate_extracted_product_dir(extract_root, product)
            if source_dir is None:
                raise ProductSyncError(
                    message=(
                        f"产品 {product} 解压后未定位到产品数据目录；可能原因：全量包结构变化；"
                        "建议：手动检查 zip 内容后重试。"
                    ),
                    reason_code=REASON_EXTRACT_ERROR,
                )
        else:
            # dry-run 不下载不解压，只保留流程与日志口径。
            source_dir = extract_root

        backup_dir, target_dir = _replace_product_dir_with_backup(
            product=product,
            source_dir=source_dir,
            data_root=command_ctx.data_root,
            run_id=command_ctx.run_id,
            dry_run=command_ctx.dry_run,
        )
        if command_ctx.dry_run:
            log_info(
                "dry-run：full_data 仅完成流程演练，未执行备份与覆盖。",
                event="SYNC_OK",
                backup=str(backup_dir),
                target=str(target_dir),
                dry_run=True,
            )
        else:
            log_info(
                "全量恢复完成（原子替换，失败可回滚）。",
                event="SYNC_OK",
                backup=str(backup_dir),
                target=str(target_dir),
                dry_run=False,
            )

        if not command_ctx.dry_run:
            status.last_update_time = utc_now_iso()
            status.data_time = status.data_time or utc_now_iso()
            upsert_product_status(conn, status)
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()

    elapsed = time.time() - t0
    log_info("full_data 执行完成。", event="CMD_DONE", product=product, elapsed=round(elapsed, 2))


if __name__ == "__main__":
    app()
