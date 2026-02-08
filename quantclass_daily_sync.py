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
DEFAULT_USER_CONFIG_FILE = BASE_DIR / "user_config.json"
DEFAULT_USER_SECRETS_FILE = BASE_DIR / "user_secrets.env"
DEFAULT_CATALOG_FILE = BASE_DIR / "catalog.txt"
DEFAULT_PROGRESS_EVERY = 500
SYNC_META_DIRNAME = ".quantclass_sync"
DEFAULT_METADATA_ROOT = DEFAULT_DATA_ROOT / SYNC_META_DIRNAME
DEFAULT_STATUS_DB = DEFAULT_METADATA_ROOT / "status" / "FuelBinStat.db"
DEFAULT_STATUS_JSON = DEFAULT_METADATA_ROOT / "status" / "products-status.json"
DEFAULT_REPORT_RETENTION_DAYS = 365
TIMESTAMP_FILE_NAME = "timestamp.txt"
PRODUCT_MODE_LOCAL_SCAN = "local_scan"
PRODUCT_MODE_EXPLICIT_LIST = "explicit_list"
PRODUCT_MODES = {PRODUCT_MODE_LOCAL_SCAN, PRODUCT_MODE_EXPLICIT_LIST}

LEGACY_STATUS_DB_REL = Path("code") / "data" / "FuelBinStat.db"
LEGACY_STATUS_JSON_REL = Path("code") / "data" / "products-status.json"
LEGACY_REPORT_DIR_REL = Path("code") / "data" / "log" / "quantclass"
META_STATUS_DB_REL = Path(SYNC_META_DIRNAME) / "status" / "FuelBinStat.db"
META_STATUS_JSON_REL = Path(SYNC_META_DIRNAME) / "status" / "products-status.json"
META_REPORT_DIR_REL = Path(SYNC_META_DIRNAME) / "log" / "quantclass"

# ========================= 新手阅读路线图（先看这里） =========================
# 这份脚本分成 4 层。第一次读代码，建议按这个顺序：
#
# 1) 命令入口层（Typer 命令行框架）：
#    - global_options / cmd_init / cmd_one_data / cmd_all_data
#    - 你在终端输入命令，最先进入这一层。
#
# 2) 编排层（把“要做什么”串起来）：
#    - _execute_plans / process_product / sync_from_extract
#    - 这一层决定每个产品要不要更新、怎么更新、失败怎么记报告。
#
# 3) 文件同步层（真正处理 CSV/TS 文件）：
#    - sync_known_product / sync_unknown_product / merge_payload / sync_csv_file
#    - 这一层做“合并、去重、镜像复制”。
#
# 4) 基础能力层（通用工具）：
#    - 凭证读取、HTTP 请求、解压、安全检查、状态库读写、报告输出。
#
# 术语提示：
# - 门控（先判断是否需要更新）：先比对本地与 API 的最新日期，再决定是否下载。
# - 编排（把多个步骤组织成流程）：把“拉取 -> 解压 -> 合并 -> 记录”串成稳定管道。
# ============================================================================

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
REASON_UP_TO_DATE = "up_to_date"

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


@dataclass(frozen=True)
class RuntimePaths:
    """单次运行使用的状态与日志路径集合。"""

    metadata_root: Path
    status_db: Path
    status_json: Path
    report_dir: Path
    source: str  # metadata / legacy


class UserConfig(BaseModel):
    """用户配置模型（setup 写入，update 读取）。"""

    data_root: Path
    product_mode: str = PRODUCT_MODE_LOCAL_SCAN
    default_products: List[str] = Field(default_factory=list)
    secrets_file: Path = DEFAULT_USER_SECRETS_FILE
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)

    @field_validator("product_mode", mode="before")
    @classmethod
    def _normalize_product_mode(cls, value: object) -> str:
        mode = str(value or PRODUCT_MODE_LOCAL_SCAN).strip().lower()
        if mode not in PRODUCT_MODES:
            raise ValueError("product_mode 仅支持 local_scan 或 explicit_list")
        return mode

    @field_validator("default_products", mode="before")
    @classmethod
    def _normalize_default_products(cls, value: object) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return split_products([value])
        if isinstance(value, list):
            return split_products([str(x) for x in value])
        raise ValueError("default_products 必须是字符串或字符串列表")


class CommandContext(BaseModel):
    """命令上下文（把运行参数统一收口，避免各函数参数漂移）。"""

    run_id: str
    data_root: Path
    data_root_from_cli: bool = False
    api_key: str = ""
    hid: str = ""
    secrets_file: Path = DEFAULT_SECRETS_FILE
    secrets_file_from_cli: bool = False
    config_file: Path = DEFAULT_USER_CONFIG_FILE
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
    """产品状态模型（仅保留更新链路所需字段）。"""

    model_config = ConfigDict(extra="ignore")

    name: str
    display_name: Optional[str] = None
    last_update_time: Optional[str] = None
    next_update_time: Optional[str] = None
    data_time: Optional[str] = None
    data_content_time: Optional[str] = None
    is_auto_update: int = 0
    can_auto_update: int = 1
    add_time: Optional[str] = None
    is_listed: int = 1
    ts: Optional[str] = None

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
            "lastUpdateTime": self.last_update_time,
            "nextUpdateTime": self.next_update_time,
            "dataTime": self.data_time,
            "dataContentTime": self.data_content_time,
            "isAutoUpdate": self.is_auto_update,
            "canAutoUpdate": self.can_auto_update,
            "addTime": self.add_time,
            "isListed": self.is_listed,
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


def _write_text_atomic(path: Path, content: str, encoding: str = "utf-8") -> None:
    """
    原子写入文本文件。

    原子写入（要么完整写成功、要么不改变旧文件）可以避免配置/密钥写半截。
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}"
    tmp_path.write_text(content, encoding=encoding)
    os.replace(tmp_path, path)


@dataclass(frozen=True)
class TextFileSnapshot:
    """文本文件快照（用于 setup 失败回滚）。"""

    exists: bool
    content: str = ""
    mode: Optional[int] = None


def snapshot_text_file(path: Path) -> TextFileSnapshot:
    """保存文件当前状态。"""

    if not path.exists():
        return TextFileSnapshot(exists=False)
    mode: Optional[int] = None
    try:
        mode = path.stat().st_mode & 0o777
    except Exception:
        mode = None
    content = path.read_text(encoding="utf-8", errors="ignore")
    return TextFileSnapshot(exists=True, content=content, mode=mode)


def restore_text_file_snapshot(path: Path, snapshot: TextFileSnapshot) -> None:
    """恢复文件到快照状态。"""

    if snapshot.exists:
        _write_text_atomic(path, snapshot.content)
        if snapshot.mode is not None:
            try:
                os.chmod(path, snapshot.mode)
            except Exception:
                pass
        return
    if path.exists():
        path.unlink()


def save_user_config_atomic(path: Path, config: UserConfig) -> None:
    """保存用户配置（原子写入）。"""

    payload = config.model_dump(mode="json")
    _write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def load_user_config_or_raise(path: Path) -> UserConfig:
    """读取用户配置，失败时给出可操作提示。"""

    if not path.exists():
        raise RuntimeError(f"未找到用户配置文件：{path}；请先执行 setup。")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"用户配置文件读取失败：{path}；请检查 JSON 格式或重新执行 setup。原始错误：{exc}") from exc

    try:
        config = UserConfig(**raw)
    except Exception as exc:
        raise RuntimeError(f"用户配置内容无效：{path}；请重新执行 setup。原始错误：{exc}") from exc
    return config


def save_user_secrets_atomic(path: Path, api_key: str, hid: str) -> None:
    """保存用户密钥文件（原子写入）。"""

    body = f"QUANTCLASS_API_KEY={api_key.strip()}\nQUANTCLASS_HID={hid.strip()}\n"
    _write_text_atomic(path, body)
    try:
        os.chmod(path, 0o600)
    except Exception:
        # 非关键路径（部分系统无 chmod 权限），失败不阻断主流程。
        pass


def save_setup_artifacts_atomic(
    config_path: Path,
    config: UserConfig,
    secrets_path: Path,
    api_key: str,
    hid: str,
) -> None:
    """
    setup 双文件写入（带回滚）。

    任一文件写失败时，把配置和密钥都恢复到写入前状态，避免“半成功”。
    """

    # 先拍快照：后续任何写入失败时，都可以恢复“调用 setup 前”的文件状态。
    config_snapshot = snapshot_text_file(config_path)
    secrets_snapshot = snapshot_text_file(secrets_path)
    try:
        save_user_secrets_atomic(secrets_path, api_key=api_key, hid=hid)
        save_user_config_atomic(config_path, config)
    except Exception as exc:
        rollback_errors: List[str] = []
        for path, snapshot in ((secrets_path, secrets_snapshot), (config_path, config_snapshot)):
            try:
                restore_text_file_snapshot(path, snapshot)
            except Exception as rollback_exc:
                rollback_errors.append(f"{path}: {rollback_exc}")
        if rollback_errors:
            detail = "；".join(rollback_errors)
            raise RuntimeError(f"setup 文件写入失败且回滚不完整：{detail}") from exc
        raise


def load_user_secrets_or_raise(path: Path) -> Tuple[str, str]:
    """读取用户密钥并校验完整性。"""

    api_key, hid = load_secrets_from_file(path)
    if not api_key:
        raise RuntimeError(f"密钥文件缺少 API Key：{path}；请重新执行 setup。")
    if not hid:
        raise RuntimeError(f"密钥文件缺少 HID：{path}；请重新执行 setup。")
    return api_key, hid

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

    当前规范：
    1) 每行一个产品英文名（product_id）
    2) 允许空行和 # 注释行
    """

    if not path.exists():
        raise RuntimeError(f"产品清单文件不存在: {path}")

    products: List[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    for lineno, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if not is_product_identifier(s):
            raise RuntimeError(
                f"产品清单格式错误：{path}:{lineno} -> `{s}`；"
                "请使用“每行一个产品英文名”的写法。"
            )
        products.append(normalize_product_name(s.lower()))

    seen = set()
    result: List[str] = []
    for item in products:
        if item not in seen:
            seen.add(item)
            result.append(item)
    if not result:
        raise RuntimeError(f"产品清单为空：{path}；请至少配置一个产品英文名。")
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
    凭证优先级（高 -> 低，兼容命令）：
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


def resolve_credentials_for_update(cli_api_key: str, cli_hid: str, secrets_file: Path) -> Tuple[str, str, str]:
    """
    update 专用凭证优先级（高 -> 低）：
    1) 命令行参数
    2) setup 写入的 secrets 文件
    3) 环境变量
    """

    cli_api = cli_api_key.strip()
    cli_hid_value = cli_hid.strip()
    file_api, file_hid = load_secrets_from_file(secrets_file)
    env_api = os.environ.get("QUANTCLASS_API_KEY", "").strip()
    env_hid = os.environ.get("QUANTCLASS_HID", "").strip()

    api_key = cli_api or file_api or env_api
    hid = cli_hid_value or file_hid or env_hid

    api_source = "cli" if cli_api else ("setup_secrets" if file_api else ("env" if env_api else "missing"))
    hid_source = "cli" if cli_hid_value else ("setup_secrets" if file_hid else ("env" if env_hid else "missing"))
    if api_source == hid_source:
        credential_source = api_source
    else:
        credential_source = f"mixed(api={api_source},hid={hid_source})"
    return api_key, hid, credential_source

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
        # 统一做路径归一化（把 daily 后缀等差异映射到稳定产品名）。
        src_rel_path = src.relative_to(extract_path)
        normalized_rel_path = normalize_source_relpath(src_rel_path, product)

        if product in AGGREGATE_SPLIT_COLS and is_daily_aggregate_file(normalized_rel_path):
            # 聚合日文件（一个文件含多标的）先拆分，再逐个同步。
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
            # .ts 文件按“镜像复制”处理，不做 CSV 结构化合并。
            result = sync_raw_file(src=src, target=target, dry_run=dry_run)
            apply_file_result(stats, result=result)
            reason_code = REASON_MIRROR_FALLBACK
        else:
            rule = infer_rule(rel_path)
            if rule is None:
                # 没命中规则时降级镜像，保持可用性优先。
                result = sync_raw_file(src=src, target=target, dry_run=dry_run)
                apply_file_result(stats, result=result)
                reason_code = REASON_MIRROR_FALLBACK
            else:
                # 命中规则时做增量合并（可减少重复写入）。
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

    # 这里是策略分发器（根据 plan.strategy 选择具体同步实现）。
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

    # 第 1 步：确定本次要下载的业务日期（用户指定日期优先，否则取 latest）。
    actual_time = _resolve_actual_time(
        product=product,
        date_time=date_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
    )
    # 第 2 步：下载文件并准备解压目录。
    download_path, extract_path = _download_and_prepare_extract(
        product=product,
        actual_time=actual_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
        work_dir=work_dir,
    )
    # 第 3 步：解压下载文件（支持 zip/tar/rar/7z）。
    _extract_product_archive(product=product, download_path=download_path, extract_path=extract_path)

    # 第 4 步：把 extract 目录中的数据同步到 data_root（这是“真正写业务数据”的阶段）。
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


def resolve_runtime_paths(data_root: Path) -> RuntimePaths:
    """
    解析运行期状态/日志路径。

    规则：
    - 默认使用新路径：<data_root>/.quantclass_sync/*
    - 若检测到旧路径已有状态数据，且新路径尚无状态数据，则回退旧路径读取（避免迁移期分裂）
    """

    data_root = data_root.resolve()
    metadata_root = data_root / SYNC_META_DIRNAME

    new_status_db = data_root / META_STATUS_DB_REL
    new_status_json = data_root / META_STATUS_JSON_REL
    new_report_dir = data_root / META_REPORT_DIR_REL
    new_has_state = new_status_db.exists() or new_status_json.exists()

    legacy_status_db = data_root / LEGACY_STATUS_DB_REL
    legacy_status_json = data_root / LEGACY_STATUS_JSON_REL
    legacy_report_dir = data_root / LEGACY_REPORT_DIR_REL
    legacy_has_state = legacy_status_db.exists() or legacy_status_json.exists()

    # 迁移保护：旧路径有状态且新路径还没初始化时，优先读旧路径，避免同一批数据写到两套状态库。
    if legacy_has_state and not new_has_state:
        return RuntimePaths(
            metadata_root=metadata_root,
            status_db=legacy_status_db,
            status_json=legacy_status_json,
            report_dir=legacy_report_dir,
            source="legacy",
        )

    return RuntimePaths(
        metadata_root=metadata_root,
        status_db=new_status_db,
        status_json=new_status_json,
        report_dir=new_report_dir,
        source="metadata",
    )


def status_db_path(data_root: Path) -> Path:
    """返回状态数据库路径。"""

    return resolve_runtime_paths(data_root).status_db


def status_json_path(data_root: Path) -> Path:
    """返回 products-status.json 路径。"""

    return resolve_runtime_paths(data_root).status_json


def report_dir_path(data_root: Path) -> Path:
    """返回运行报告目录。"""

    return resolve_runtime_paths(data_root).report_dir


def normalize_data_date(raw: str) -> Optional[str]:
    """把输入日期统一归一成 YYYY-MM-DD。"""

    text = (raw or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    if re.fullmatch(r"\d{8}", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def read_local_timestamp_date(data_root: Path, product: str) -> Optional[str]:
    """读取本地 timestamp.txt 第一列日期。"""

    path = data_root / product / TIMESTAMP_FILE_NAME
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8-sig", errors="ignore").strip()
    except Exception:
        return None
    if not text:
        return None
    first = text.split(",", 1)[0].strip()
    return normalize_data_date(first)


def write_local_timestamp(data_root: Path, product: str, data_date: str) -> None:
    """回写本地 timestamp.txt（格式：数据日期,本地写入时间）。"""

    normalized = normalize_data_date(data_date)
    if not normalized:
        return
    path = data_root / product / TIMESTAMP_FILE_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    local_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.write_text(f"{normalized},{local_now}\n", encoding="utf-8")


def should_skip_by_timestamp(local_date: Optional[str], api_latest_date: Optional[str]) -> bool:
    """判断本地是否已是最新版本。"""

    if not local_date or not api_latest_date:
        return False
    return local_date >= api_latest_date


def cleanup_work_cache_aggressive(work_dir: Path) -> None:
    """激进清理工作缓存目录。"""

    if not work_dir.exists():
        return
    for child in work_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink()
            except FileNotFoundError:
                pass
    work_dir.mkdir(parents=True, exist_ok=True)


def cleanup_report_logs(report_dir: Path, retention_days: int = DEFAULT_REPORT_RETENTION_DAYS) -> None:
    """清理过期 run_report 日志文件。"""

    if retention_days <= 0:
        return
    if not report_dir.exists():
        return
    cutoff_ts = time.time() - retention_days * 24 * 3600
    for path in report_dir.glob("run_report_*.json"):
        try:
            if path.stat().st_mtime < cutoff_ts:
                path.unlink()
        except FileNotFoundError:
            continue


def ensure_status_table(conn: sqlite3.Connection) -> None:
    """确保状态表存在（product_status）。"""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_status (
            name TEXT PRIMARY KEY,
            display_name TEXT,
            last_update_time TEXT,
            next_update_time TEXT,
            data_time TEXT,
            data_content_time TEXT,
            is_auto_update INTEGER DEFAULT 0,
            can_auto_update INTEGER DEFAULT 1,
            add_time TEXT,
            is_listed INTEGER DEFAULT 1,
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
            name, display_name, last_update_time, next_update_time,
            data_time, data_content_time, is_auto_update, can_auto_update,
            add_time, is_listed, ts
        ) VALUES (
            :name, :display_name, :last_update_time, :next_update_time,
            :data_time, :data_content_time, :is_auto_update, :can_auto_update,
            :add_time, :is_listed, :ts
        )
        ON CONFLICT(name) DO UPDATE SET
            display_name=excluded.display_name,
            last_update_time=excluded.last_update_time,
            next_update_time=excluded.next_update_time,
            data_time=excluded.data_time,
            data_content_time=excluded.data_content_time,
            is_auto_update=excluded.is_auto_update,
            can_auto_update=excluded.can_auto_update,
            add_time=COALESCE(product_status.add_time, excluded.add_time),
            is_listed=excluded.is_listed,
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
    return (report_dir_path(ctx.data_root) / f"run_report_{ctx.run_id}_{command}.json").resolve()


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


def _append_gate_skip_result(
    report: RunReport,
    plan: ProductPlan,
    product_name: str,
    api_latest_raw: str,
    api_latest_date: Optional[str],
    local_date: Optional[str],
    elapsed_seconds: float,
) -> None:
    """记录 timestamp 门控命中的跳过结果。"""

    report.products.append(
        ProductRunResult(
            product=product_name,
            status="skipped",
            strategy=plan.strategy,
            reason_code=REASON_UP_TO_DATE,
            date_time=api_latest_date or api_latest_raw,
            mode="gate",
            elapsed_seconds=elapsed_seconds,
            error=f"本地 timestamp 已是最新（local={local_date}, api={api_latest_date}）。",
        )
    )
    _append_run_event(
        report,
        product_name,
        "GATE",
        "skipped",
        REASON_UP_TO_DATE,
        f"local={local_date} api={api_latest_date}",
    )


def _resolve_requested_date_for_plan(
    plan: ProductPlan,
    command_ctx: CommandContext,
    hid: str,
    headers: Dict[str, str],
    requested_date_time: str,
    force_update: bool,
    report: RunReport,
    t_product_start: float,
) -> Tuple[str, bool]:
    """
    解析单产品执行时间，并处理 timestamp 门控。

    返回：
    - requested_date_for_plan: 传给下载流程的日期（空字符串代表继续走 latest）
    - skipped_by_gate: 是否已经被门控判定为“跳过”
    """

    # 用户手动指定了日期，就以用户输入为准，不再做 latest/timestamp 判断。
    requested_date_for_plan = requested_date_time.strip()
    if force_update or requested_date_for_plan:
        return requested_date_for_plan, False

    product_name = normalize_product_name(plan.name)
    try:
        # 1) 读取 API 最新日期（latest）
        api_latest_raw = get_latest_time(
            api_base=command_ctx.api_base.rstrip("/"),
            product=product_name,
            hid=hid,
            headers=headers,
        )
        # 2) 读取本地 timestamp 第一列日期
        api_latest_date = normalize_data_date(api_latest_raw)
        local_date = read_local_timestamp_date(command_ctx.data_root, product_name)
        # 3) 如果本地已经不落后，则直接跳过，不进入下载链路
        if should_skip_by_timestamp(local_date, api_latest_date):
            elapsed = time.time() - t_product_start
            _append_gate_skip_result(
                report=report,
                plan=plan,
                product_name=product_name,
                api_latest_raw=api_latest_raw,
                api_latest_date=api_latest_date,
                local_date=local_date,
                elapsed_seconds=elapsed,
            )
            log_info(
                f"[{product_name}] timestamp 门控命中，跳过更新。",
                event="SYNC_SKIP",
                local_date=local_date,
                api_latest_date=api_latest_date,
                decision="skip",
            )
            return api_latest_raw, True

        # 本地落后：继续执行更新
        log_info(
            f"[{product_name}] timestamp 门控通过，执行更新。",
            event="PRODUCT_PLAN",
            local_date=local_date or "",
            api_latest_date=api_latest_date or "",
            decision="run",
        )
        return api_latest_raw, False
    except Exception as exc:
        # 门控异常时采用 fail-open（失败放行）策略，避免“该更新却被误跳过”。
        log_info(
            f"[{plan.name}] timestamp 门控异常，回退执行更新。",
            event="PRODUCT_PLAN",
            decision="fallback_run",
            error=str(exc),
        )
        return requested_date_for_plan, False


def _upsert_product_status_after_success(
    conn: Optional[sqlite3.Connection],
    command_ctx: CommandContext,
    product: str,
    actual_time: str,
) -> None:
    """在成功路径统一更新状态库与 timestamp 文件。"""

    if conn is None or command_ctx.dry_run:
        return
    old_status = load_product_status(conn, product)
    status = old_status or ProductStatus(name=product, display_name=product)
    status.last_update_time = utc_now_iso()
    status.data_time = actual_time
    status.data_content_time = actual_time
    upsert_product_status(conn, status)
    write_local_timestamp(command_ctx.data_root, product, actual_time)


def _execute_plans(
    plans: Sequence[ProductPlan],
    command_ctx: CommandContext,
    report: RunReport,
    requested_date_time: str = "",
    conn: Optional[sqlite3.Connection] = None,
    force_update: bool = False,
) -> Tuple[SyncStats, bool, float]:
    """
    执行产品计划并返回汇总统计。

    整体流程（单次运行）：
    1) 先构建请求头与凭证（API Key/HID）
    2) 逐个产品执行“门控判断 -> 下载解压 -> 同步落库 -> 记录结果”
    3) 累加统计并在必要时中断（stop-on-error）
    """

    headers, hid = build_headers_or_raise(command_ctx)
    total = SyncStats()
    has_error = False
    t_run_start = time.time()

    for plan in plans:
        t_product_start = time.time()
        debug_trace = ""
        # A. 判断这个产品本次应不应该跑（门控命中会直接 continue）。
        requested_date_for_plan, skipped_by_gate = _resolve_requested_date_for_plan(
            plan=plan,
            command_ctx=command_ctx,
            hid=hid,
            headers=headers,
            requested_date_time=requested_date_time,
            force_update=force_update,
            report=report,
            t_product_start=t_product_start,
        )
        if skipped_by_gate:
            continue

        try:
            # B. 真正执行单产品同步（网络 + 解压 + 文件同步）。
            product, actual_time, stats, source_path, reason_code = process_product(
                plan=plan,
                date_time=requested_date_for_plan or None,
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
            # C. 成功后统一回写状态库与 timestamp（dry-run 下不会写）。
            _upsert_product_status_after_success(conn=conn, command_ctx=command_ctx, product=product, actual_time=actual_time)
            continue
        except ProductSyncError as exc:
            # 可预期业务错误：带有明确 reason_code。
            reason_code = exc.reason_code
            message = str(exc)
            debug_trace = traceback.format_exc()
        except Exception as exc:
            # 兜底未知异常：统一归并为 merge_error，避免丢失错误。
            reason_code = REASON_MERGE_ERROR
            message = str(exc)
            debug_trace = traceback.format_exc()

        # D. 失败路径：写报告 + 打日志 + 按 stop-on-error 决定是否中断。
        has_error = True
        elapsed = time.time() - t_product_start
        _append_error_result(report, plan, reason_code, requested_date_for_plan, elapsed, message)
        log_error(f"[{plan.name}] 处理失败: {message}", event="SYNC_FAIL", reason_code=reason_code)
        if command_ctx.verbose and debug_trace:
            log_debug(debug_trace, event="DEBUG")
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


def _new_report(run_id: str, mode: str) -> RunReport:
    return RunReport(schema_version="3.0", run_id=run_id, started_at=utc_now_iso(), mode=mode)


# CLI（命令行接口）根对象：所有子命令都挂在 app 上。
app = typer.Typer(
    help="QuantClass 数据同步工具（推荐 setup + update，兼容旧命令）",
    no_args_is_help=False,
    add_completion=False,
    pretty_exceptions_enable=False,
)


@app.callback(invoke_without_command=True)
def global_options(
    ctx: typer.Context,
    data_root: Optional[Path] = typer.Option(None, "--data-root", help="数据根目录（兼容命令可用）。"),
    api_key: str = typer.Option("", "--api-key", help="QuantClass API Key（高级参数）。", hidden=True),
    hid: str = typer.Option("", "--hid", help="QuantClass HID（高级参数）。", hidden=True),
    secrets_file: Optional[Path] = typer.Option(None, "--secrets-file", help="本地密钥文件路径（兼容命令可用）。"),
    config_file: Path = typer.Option(DEFAULT_USER_CONFIG_FILE, "--config-file", help="用户配置文件路径（setup/update）。"),
    dry_run: bool = typer.Option(False, "--dry-run", help="演练模式（不写业务数据和状态文件）。"),
    report_file: Optional[Path] = typer.Option(
        None, "--report-file", help="报告输出路径（JSON，高级参数）。", hidden=True
    ),
    stop_on_error: bool = typer.Option(False, "--stop-on-error", help="遇错即停（高级参数）。", hidden=True),
    verbose: bool = typer.Option(True, "--verbose/--no-verbose", help="显示调试日志（默认开启，可用 --no-verbose 关闭）。"),
) -> None:
    """
    全局参数（所有子命令共享）。

    这是 Typer 的回调（callback：每次执行任意子命令前都会先调用）。
    这里完成三件事：
    1) 初始化日志器（带 run_id，方便按次排障）
    2) 校验关键路径参数
    3) 把运行上下文写入 ctx.obj，供后续子命令复用
    """

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    global LOGGER, PROGRESS_EVERY
    LOGGER = ConsoleLogger(level="DEBUG" if verbose else "INFO", run_id=run_id)
    PROGRESS_EVERY = max(1, DEFAULT_PROGRESS_EVERY)

    resolved_data_root = data_root.resolve() if data_root else DEFAULT_DATA_ROOT.resolve()
    resolved_secrets_file = secrets_file.resolve() if secrets_file else DEFAULT_SECRETS_FILE.resolve()
    resolved_config_file = config_file.resolve()
    runtime_paths = resolve_runtime_paths(resolved_data_root)
    log_debug(
        "运行路径已解析。",
        event="PATHS",
        data_root=str(resolved_data_root),
        status_db=str(runtime_paths.status_db),
        status_json=str(runtime_paths.status_json),
        report_dir=str(runtime_paths.report_dir),
        source=runtime_paths.source,
    )

    # CommandContext 是“本次运行共享配置”，后续命令都从这里读取参数。
    command_ctx = CommandContext(
        run_id=run_id,
        data_root=resolved_data_root,
        data_root_from_cli=data_root is not None,
        api_key=api_key,
        hid=hid,
        secrets_file=resolved_secrets_file,
        secrets_file_from_cli=secrets_file is not None,
        config_file=resolved_config_file,
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

    # 无子命令时做“首次引导”：
    # - 首次（无配置）：自动进入 setup
    # - 非首次（已有配置）：显示帮助
    if ctx.invoked_subcommand is None:
        if not resolved_config_file.exists():
            if not sys.stdin.isatty():
                log_error(
                    f"未检测到配置文件：{resolved_config_file}；请先执行 setup（交互）或 setup --non-interactive。",
                    event="SETUP",
                )
                raise typer.Exit(code=1)
            log_info("未检测到用户配置，自动进入 setup。", event="SETUP", config_file=str(resolved_config_file))
            # 这里显式传 ctx，避免自动引导时丢失 setup 所需上下文参数。
            ctx.invoke(cmd_setup, ctx=ctx)
            raise typer.Exit(code=0)
        typer.echo(ctx.get_help())
        raise typer.Exit(code=0)


def _ctx(ctx: typer.Context) -> CommandContext:
    value = ctx.obj
    if not isinstance(value, CommandContext):
        raise RuntimeError("运行上下文初始化失败；请通过命令行调用子命令。")
    return value


def _extract_command_context(args: tuple, kwargs: dict) -> Optional[CommandContext]:
    """从命令参数中提取 CommandContext（用于异常时决定是否打印调试堆栈）。"""

    raw_ctx = kwargs.get("ctx") or (args[0] if args else None)
    if raw_ctx is None:
        return None
    obj = getattr(raw_ctx, "obj", None)
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


def _cleanup_after_command(command_ctx: Optional[CommandContext]) -> None:
    """
    命令结束后统一执行缓存清理（失败不影响主流程）。

    设计目的：就算命令中途报错，也尽量保证缓存不会持续膨胀。
    """

    work_dir = command_ctx.work_dir if command_ctx is not None else DEFAULT_WORK_DIR.resolve()
    data_root = command_ctx.data_root if command_ctx is not None else DEFAULT_DATA_ROOT.resolve()
    try:
        cleanup_work_cache_aggressive(work_dir)
        log_debug("工作缓存已清理。", event="CACHE_CLEANUP", work_dir=str(work_dir))
    except Exception as exc:
        log_debug(f"工作缓存清理失败（已忽略）: {exc}", event="CACHE_CLEANUP")

    try:
        cleanup_report_logs(report_dir_path(data_root), retention_days=DEFAULT_REPORT_RETENTION_DAYS)
    except Exception as exc:
        log_debug(f"报告日志清理失败（已忽略）: {exc}", event="CACHE_CLEANUP")


def command_guard(command_name: str):
    """
    命令级异常兜底装饰器。

    目的：把未处理异常转换为清晰中文报错 + 非零退出码。
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # 正常执行子命令主逻辑。
                return func(*args, **kwargs)
            except typer.Exit:
                # 业务层主动退出（例如参数校验失败）直接向上抛。
                raise
            except ProductSyncError as exc:
                # 业务可识别错误：保留 reason_code，方便报告聚合。
                _handle_command_exception(command_name, exc, exc.reason_code, args, kwargs)
                raise typer.Exit(code=1)
            except Exception as exc:
                # 兜底未知错误：统一映射为 merge_error，避免漏报。
                _handle_command_exception(command_name, exc, REASON_MERGE_ERROR, args, kwargs)
                raise typer.Exit(code=1)
            finally:
                # 无论成功/失败，都会执行清理（finally 总会执行）。
                _cleanup_after_command(_extract_command_context(args, kwargs))

        return wrapper

    return decorator


def ensure_data_root_ready(data_root: Path, create_if_missing: bool = False) -> Path:
    """校验 data_root；需要时可自动创建目录。"""

    data_root = data_root.expanduser().resolve()
    if data_root.exists():
        if not data_root.is_dir():
            raise RuntimeError(f"data_root 不是目录：{data_root}")
        return data_root
    if create_if_missing:
        data_root.mkdir(parents=True, exist_ok=True)
        return data_root
    raise RuntimeError(f"data_root 不存在：{data_root}")


def _build_command_ctx_with_overrides(base_ctx: CommandContext, data_root: Path, secrets_file: Path) -> CommandContext:
    """基于基础上下文生成覆盖后的运行上下文。"""

    data_root = data_root.expanduser().resolve()
    secrets_file = secrets_file.expanduser().resolve()
    runtime_paths = resolve_runtime_paths(data_root)
    log_debug(
        "已应用运行配置。",
        event="PATHS",
        data_root=str(data_root),
        status_db=str(runtime_paths.status_db),
        status_json=str(runtime_paths.status_json),
        report_dir=str(runtime_paths.report_dir),
        source=runtime_paths.source,
    )
    return base_ctx.model_copy(
        update={
            "data_root": data_root,
            "secrets_file": secrets_file,
        }
    )


def _resolve_command_paths(
    base_ctx: CommandContext,
    require_user_config: bool = False,
) -> Tuple[Path, Path, Optional[UserConfig], str, str]:
    """
    统一解析 data_root / secrets_file 来源。

    优先级：
    1) 命令行显式参数
    2) user_config.json
    3) 代码默认值
    """

    user_config: Optional[UserConfig] = None
    if base_ctx.config_file.exists():
        # 只要配置文件存在，就强制校验其可读性，避免损坏配置被静默忽略。
        user_config = load_user_config_or_raise(base_ctx.config_file)
    elif require_user_config:
        raise RuntimeError(f"未找到用户配置文件：{base_ctx.config_file}；请先执行 setup。")

    data_root_source = "cli" if base_ctx.data_root_from_cli else "default"
    secrets_source = "cli" if base_ctx.secrets_file_from_cli else "default"
    data_root = base_ctx.data_root
    secrets_file = base_ctx.secrets_file

    if user_config is not None:
        if not base_ctx.data_root_from_cli:
            data_root = user_config.data_root.resolve()
            data_root_source = "config"
        if not base_ctx.secrets_file_from_cli:
            secrets_file = user_config.secrets_file.resolve()
            secrets_source = "config"

    return data_root.resolve(), secrets_file.resolve(), user_config, data_root_source, secrets_source


def run_update_with_settings(
    command_ctx: CommandContext,
    mode: str = "local",
    products: Optional[Sequence[str]] = None,
    force_update: bool = False,
    command_name: str = "all_data",
    fallback_products: Optional[Sequence[str]] = None,
) -> int:
    """
    通用批量更新执行器（update/all_data 共用）。

    fallback_products 用于“本地扫描为空”时的回退清单。
    """

    ensure_data_root_ready(command_ctx.data_root, create_if_missing=False)

    mode = (mode or "local").strip().lower()
    if mode not in RUN_MODES:
        raise typer.BadParameter("mode 仅支持 local 或 catalog")

    product_args = list(products or [])
    fallback_args = list(fallback_products or [])

    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, command_name)
    catalog_products = load_catalog_or_raise(command_ctx.catalog_file)
    catalog_set = {normalize_product_name(x) for x in catalog_products}

    log_info("开始扫描本地产品目录。", event="DISCOVER_START", data_root=str(command_ctx.data_root), mode=mode)
    discovered = discover_local_products(data_root=command_ctx.data_root, catalog_products=catalog_products)
    report.discovered_total = len(discovered)
    log_info("本地产品扫描完成。", event="DISCOVER_DONE", discovered_total=report.discovered_total)

    planned_products, unknown_local, invalid_explicit = resolve_products_by_mode(
        mode=mode,
        raw_products=product_args,
        catalog_products=catalog_products,
        discovered_local=discovered,
    )
    _record_discovery_skips(report, unknown_local, invalid_explicit)

    # update 模式：本地扫描为空时，可回退到默认产品清单。
    # 这样新用户即使 data_root 里暂时没有目录，也能按 setup 配置完成首轮更新。
    if mode == "local" and not planned_products and not product_args and fallback_args:
        fallback = split_products(fallback_args)
        fallback_valid = [x for x in fallback if x in catalog_set]
        fallback_invalid = [x for x in fallback if x not in catalog_set]
        if fallback_invalid:
            _record_discovery_skips(report, unknown_local=[], invalid_explicit=fallback_invalid)
        if fallback_valid:
            planned_products = fallback_valid
            log_info(
                "本地扫描为空，已回退到默认产品清单。",
                event="PLAN",
                fallback_total=len(fallback_valid),
            )

    if mode == "local" and not planned_products and not product_args:
        report.failed_total = 1
        report.ended_at = utc_now_iso()
        report.duration_seconds = 0.0
        write_run_report(report_path, report)
        log_error(
            "未发现可更新产品；可先执行 setup 配置默认产品清单。",
            event="RUN_SUMMARY",
            reason_code=REASON_NO_LOCAL_PRODUCTS,
        )
        return 1

    plans = build_product_plan(planned_products)
    report.planned_total = len(plans)
    if not plans:
        report.ended_at = utc_now_iso()
        report.duration_seconds = 0.0
        write_run_report(report_path, report)
        log_error("执行清单为空，任务结束。", event="RUN_SUMMARY")
        return 1

    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        total, has_error, t_run_start = _execute_plans(
            plans,
            command_ctx,
            report,
            requested_date_time="",
            conn=conn,
            force_update=force_update,
        )
        if conn is not None:
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()

    return _finalize_and_write_report(report, total, has_error, t_run_start, report_path)


@app.command("setup")
@command_guard("setup")
def cmd_setup(
    ctx: typer.Context,
    non_interactive: bool = typer.Option(False, "--non-interactive", help="非交互模式（需显式传参数）。"),
    skip_check: bool = typer.Option(False, "--skip-check", help="跳过连通性检查。"),
    data_root: str = typer.Option("", "--data-root", help="数据根目录。"),
    api_key: str = typer.Option("", "--api-key", help="用户 API Key。"),
    hid: str = typer.Option("", "--hid", help="用户 HID。"),
    product_mode: str = typer.Option(PRODUCT_MODE_LOCAL_SCAN, "--product-mode", help="local_scan 或 explicit_list。"),
    products: List[str] = typer.Option([], "--products", help="默认产品列表（可重复传参，也支持逗号分隔）。"),
) -> None:
    """
    初始化用户配置（首次运行推荐）。

    结果：
    1) 写入 user_config.json
    2) 写入 user_secrets.env
    3) 可选执行连通性检查
    """

    base_ctx = _ctx(ctx)
    existing_config: Optional[UserConfig] = None
    if base_ctx.config_file.exists():
        try:
            existing_config = load_user_config_or_raise(base_ctx.config_file)
        except Exception:
            # 旧配置损坏时允许重建，不阻断 setup。
            existing_config = None

    if non_interactive:
        raw_data_root = data_root.strip() or (str(existing_config.data_root) if existing_config else "")
        raw_api_key = api_key.strip() or os.environ.get("QUANTCLASS_API_KEY", "").strip()
        raw_hid = hid.strip() or os.environ.get("QUANTCLASS_HID", "").strip()
        mode = (product_mode or PRODUCT_MODE_LOCAL_SCAN).strip().lower()
        default_products = split_products(products)
    else:
        default_root = data_root.strip() or (str(existing_config.data_root) if existing_config else str(base_ctx.data_root))
        raw_data_root = typer.prompt("请输入数据目录(data_root)", default=default_root).strip()

        default_api_key = api_key.strip() or os.environ.get("QUANTCLASS_API_KEY", "").strip()
        default_hid = hid.strip() or os.environ.get("QUANTCLASS_HID", "").strip()
        raw_api_key = typer.prompt("请输入 API Key", default=default_api_key, hide_input=True).strip()
        raw_hid = typer.prompt("请输入 HID", default=default_hid, hide_input=True).strip()

        default_mode = (product_mode or (existing_config.product_mode if existing_config else PRODUCT_MODE_LOCAL_SCAN)).strip()
        mode = typer.prompt("产品策略（local_scan / explicit_list）", default=default_mode).strip().lower()

        default_products_seed = ",".join(products) if products else ",".join(existing_config.default_products if existing_config else [])
        products_line = typer.prompt("默认产品列表（逗号分隔，可留空）", default=default_products_seed).strip()
        default_products = split_products([products_line]) if products_line else []

    if not raw_data_root:
        raise RuntimeError("setup 缺少 data_root；请提供数据目录。")
    data_root_path = ensure_data_root_ready(Path(raw_data_root), create_if_missing=True)
    setup_ctx = _build_command_ctx_with_overrides(base_ctx, data_root=data_root_path, secrets_file=base_ctx.secrets_file)
    ctx.obj = setup_ctx

    if mode not in PRODUCT_MODES:
        raise RuntimeError("product_mode 仅支持 local_scan 或 explicit_list。")
    if mode == PRODUCT_MODE_EXPLICIT_LIST and not default_products:
        raise RuntimeError("product_mode=explicit_list 时必须提供至少一个默认产品。")

    catalog = load_catalog_or_raise(base_ctx.catalog_file)
    catalog_set = {normalize_product_name(x) for x in catalog}
    invalid_defaults = [x for x in default_products if x not in catalog_set]
    if invalid_defaults:
        raise RuntimeError(f"默认产品不在 catalog 中：{', '.join(invalid_defaults)}")

    if not raw_api_key:
        raise RuntimeError("setup 缺少 API Key。")
    if not raw_hid:
        raise RuntimeError("setup 缺少 HID。")

    if base_ctx.secrets_file_from_cli:
        secrets_path = base_ctx.secrets_file
    elif existing_config is not None:
        secrets_path = existing_config.secrets_file.resolve()
    else:
        secrets_path = DEFAULT_USER_SECRETS_FILE.resolve()

    # 默认先做连通性检查，再写文件：
    # 这样检查失败时不会留下“新配置写了一半”的状态。
    if not skip_check:
        probe_product = default_products[0] if default_products else (catalog[0] if catalog else "stock-trading-data")
        headers = {
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/100.0.4896.127 Safari/537.36"
            ),
            "content-type": "application/json",
            "api-key": raw_api_key,
        }
        try:
            get_latest_time(api_base=setup_ctx.api_base.rstrip("/"), product=probe_product, hid=raw_hid, headers=headers)
        except Exception as exc:
            raise RuntimeError(f"连通性检查失败；请检查 API Key/HID 或网络。原始错误：{exc}") from exc
        log_info("连通性检查通过。", event="SETUP", probe_product=probe_product)

    # 真正落盘时用“配置+密钥”一体化写入，任一步失败都会回滚。
    now = utc_now_iso()
    user_config = UserConfig(
        data_root=data_root_path,
        product_mode=mode,
        default_products=default_products,
        secrets_file=secrets_path,
        created_at=existing_config.created_at if existing_config else now,
        updated_at=now,
    )
    save_setup_artifacts_atomic(
        config_path=base_ctx.config_file,
        config=user_config,
        secrets_path=secrets_path,
        api_key=raw_api_key,
        hid=raw_hid,
    )

    log_info(
        "setup 完成。下一步建议先执行 update --dry-run。",
        event="SETUP",
        config_file=str(base_ctx.config_file),
        secrets_file=str(secrets_path),
    )


@app.command("update")
@command_guard("update")
def cmd_update(
    ctx: typer.Context,
    dry_run: bool = typer.Option(False, "--dry-run", help="演练模式（不写业务数据和状态文件）。"),
    verbose: bool = typer.Option(False, "--verbose", help="显示调试日志。"),
    products: List[str] = typer.Option([], "--products", help="临时覆盖默认产品清单。"),
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    一键更新入口（日常只需这个命令）。
    """

    base_ctx = _ctx(ctx)
    data_root, secrets_file, user_config, data_root_source, secrets_source = _resolve_command_paths(
        base_ctx,
        require_user_config=True,
    )
    if user_config is None:
        raise RuntimeError(f"未找到用户配置文件：{base_ctx.config_file}；请先执行 setup。")

    if verbose and LOGGER.level != "DEBUG":
        LOGGER.level = "DEBUG"
    run_ctx = _build_command_ctx_with_overrides(base_ctx, data_root=data_root, secrets_file=secrets_file)
    # update 明确固定优先级：CLI > setup secrets > ENV，
    # 解析后写回 run_ctx，避免后续流程再次按“旧优先级”重算。
    api_key, hid, credential_source = resolve_credentials_for_update(
        cli_api_key=run_ctx.api_key,
        cli_hid=run_ctx.hid,
        secrets_file=run_ctx.secrets_file.resolve(),
    )
    run_ctx = run_ctx.model_copy(
        update={
            "dry_run": base_ctx.dry_run or dry_run,
            "verbose": base_ctx.verbose or verbose,
            "api_key": api_key,
            "hid": hid,
        }
    )
    ctx.obj = run_ctx
    ensure_data_root_ready(run_ctx.data_root, create_if_missing=False)
    load_user_secrets_or_raise(run_ctx.secrets_file)
    log_debug(
        "update 运行来源已解析。",
        event="SETUP",
        data_root_source=data_root_source,
        secrets_source=secrets_source,
        credential_source=credential_source,
    )

    # update 产品优先级：
    # 1) 命令行 --products 临时覆盖
    # 2) explicit_list 使用配置里的 default_products
    # 3) local_scan 先扫本地，扫不到再走 fallback default_products
    explicit_products = split_products(products)
    fallback_products: List[str] = []
    selected_products: List[str] = explicit_products
    if not explicit_products:
        if user_config.product_mode == PRODUCT_MODE_EXPLICIT_LIST:
            if not user_config.default_products:
                raise RuntimeError("配置了 explicit_list，但 default_products 为空；请重新执行 setup。")
            selected_products = user_config.default_products
        else:
            fallback_products = user_config.default_products

    exit_code = run_update_with_settings(
        command_ctx=run_ctx,
        mode="local",
        products=selected_products,
        force_update=force_update,
        command_name="update",
        fallback_products=fallback_products,
    )
    log_info("update 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command("init")
@command_guard("init")
def cmd_init(ctx: typer.Context) -> None:
    """
    初始化产品状态快照（兼容命令）。

    这一步只更新状态文件，不下载数据。
    """

    command_ctx = _ctx(ctx)
    data_root, secrets_file, _user_config, data_root_source, secrets_source = _resolve_command_paths(command_ctx)
    command_ctx = _build_command_ctx_with_overrides(command_ctx, data_root, secrets_file)
    ctx.obj = command_ctx
    log_debug(
        "init 运行来源已解析。",
        event="PATHS",
        data_root_source=data_root_source,
        secrets_source=secrets_source,
    )
    ensure_data_root_ready(command_ctx.data_root, create_if_missing=True)
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
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    更新单个产品（兼容命令）。

    适合场景：排障、验证单个产品、减少批量更新的等待时间。
    """

    command_ctx = _ctx(ctx)
    data_root, secrets_file, _user_config, data_root_source, secrets_source = _resolve_command_paths(command_ctx)
    command_ctx = _build_command_ctx_with_overrides(command_ctx, data_root, secrets_file)
    ctx.obj = command_ctx
    log_debug(
        "one_data 运行来源已解析。",
        event="PATHS",
        data_root_source=data_root_source,
        secrets_source=secrets_source,
    )
    ensure_data_root_ready(command_ctx.data_root, create_if_missing=False)
    # one_data 的最小执行单元就是一个 ProductPlan。
    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, "one_data")
    plan = build_product_plan([normalize_product_name(product)])
    report.planned_total = len(plan)

    log_info("开始执行 one_data。", event="CMD_START", product=product)
    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        # 实际执行（含门控、下载、解压、落库、结果记录）。
        total, has_error, t_run_start = _execute_plans(
            plans=plan,
            command_ctx=command_ctx,
            report=report,
            requested_date_time=date_time.strip(),
            conn=conn,
            force_update=force_update,
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
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    批量更新产品（兼容命令）。

    mode=local：按本地已有产品更新（日常推荐）。
    mode=catalog：按 catalog 清单轮询（补齐或巡检时使用）。
    """

    command_ctx = _ctx(ctx)
    data_root, secrets_file, _user_config, data_root_source, secrets_source = _resolve_command_paths(command_ctx)
    command_ctx = _build_command_ctx_with_overrides(command_ctx, data_root, secrets_file)
    ctx.obj = command_ctx
    log_debug(
        "all_data 运行来源已解析。",
        event="PATHS",
        data_root_source=data_root_source,
        secrets_source=secrets_source,
    )
    exit_code = run_update_with_settings(
        command_ctx=command_ctx,
        mode=mode,
        products=products,
        force_update=force_update,
        command_name="all_data",
    )
    log_info("all_data 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)


if __name__ == "__main__":
    app()
