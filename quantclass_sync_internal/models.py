"""职责：定义运行时数据模型、异常和日志基础设施。"""

from __future__ import annotations

import os
import secrets
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar, Dict, List, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from rich.console import Console

from .constants import (
    AGGREGATE_SPLIT_COLS,
    DEFAULT_API_BASE,
    DEFAULT_CATALOG_FILE,
    DEFAULT_PROGRESS_EVERY,
    DEFAULT_SECRETS_FILE,
    DEFAULT_USER_CONFIG_FILE,
    DEFAULT_USER_SECRETS_FILE,
    DEFAULT_WORK_DIR,
    LOG_LEVELS,
    PRODUCT_MODE_LOCAL_SCAN,
    PRODUCT_MODES,
    REASON_OK,
    normalize_product_name,
)

def utc_now_iso() -> str:
    """返回 UTC 时间字符串（ISO 格式）。"""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def new_run_id() -> str:
    """生成高冲突安全的 run_id（微秒 + pid + 短随机后缀）。"""

    now = datetime.now()
    return f"{now.strftime('%Y%m%d-%H%M%S-%f')}-p{os.getpid()}-{secrets.token_hex(4)}"

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

RICH_CONSOLE = Console()

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

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        request_url: str = "",
        response_body: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.request_url = request_url
        self.response_body = response_body

class ProductSyncError(RuntimeError):
    """单产品执行错误（携带 reason_code）。"""

    def __init__(self, message: str, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class EmptyDownloadLinkError(RuntimeError):
    """API 返回了空的下载链接。"""

    pass

@dataclass(frozen=True)
class DatasetRule:
    """已知产品统一规则。"""

    name: str
    encoding: str
    has_note: bool
    key_cols: Tuple[str, ...]
    sort_cols: Tuple[str, ...]
    date_filter_col: Optional[str] = None

@dataclass
class CsvPayload:
    """CSV 解析后的统一结构。"""

    note: Optional[str]
    header: List[str]
    rows: List[List[str]]
    encoding: str
    delimiter: str = ","
    # merge_payload 排序后设为 True，sync_payload_to_target 据此跳过冗余校验
    pre_sorted: bool = False

@dataclass
class SyncStats:
    """文件同步统计信息（每产品/全局共用）。"""

    created_files: int = 0
    updated_files: int = 0
    unchanged_files: int = 0
    skipped_files: int = 0
    rows_added: int = 0
    sorted_checked_files: int = 0
    sorted_violation_files: int = 0
    sorted_auto_repaired_files: int = 0

    # 需要累加的字段白名单（类变量，不参与序列化）：新增字段时只需在此处追加，merge 自动处理
    _MERGE_FIELDS: ClassVar[Tuple[str, ...]] = (
        "created_files",
        "updated_files",
        "unchanged_files",
        "skipped_files",
        "rows_added",
        "sorted_checked_files",
        "sorted_violation_files",
        "sorted_auto_repaired_files",
    )

    def merge(self, other: "SyncStats") -> None:
        """遍历白名单字段做累加，避免新增字段时手动遗漏。"""
        for f in self._MERGE_FIELDS:
            setattr(self, f, getattr(self, f) + getattr(other, f))

@dataclass
class SortAudit:
    """单文件排序质量统计。"""

    checked_files: int = 0
    violation_files: int = 0
    auto_repaired_files: int = 0

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
    http_attempts: int = 0
    http_failures: int = 0

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
    sorted_checked_files: int = 0
    sorted_violation_files: int = 0
    sorted_auto_repaired_files: int = 0
    reason_code_counts: Dict[str, int] = field(default_factory=dict)
    phase_plan_seconds: float = 0.0
    phase_sync_seconds: float = 0.0
    phase_postprocess_seconds: float = 0.0
    products: List[ProductRunResult] = field(default_factory=list)
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
    http_attempts_by_product: Dict[str, int] = Field(default_factory=dict)
    http_failures_by_product: Dict[str, int] = Field(default_factory=dict)

    def __repr__(self) -> str:
        # 隐藏敏感字段，防止日志或异常堆栈泄露凭证
        safe = {k: ("***" if k in ("api_key", "hid") else v) for k, v in self.__dict__.items()}
        return f"CommandContext({safe})"

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
    # 公告标题：按日期落地为日文件，入库时按 公告日期+股票代码+标题 保持稳定顺序
    "stock-notices-title": DatasetRule(
        name="stock-notices-title",
        encoding="gb18030",
        has_note=True,
        key_cols=("公告日期", "股票代码", "公告标题"),
        sort_cols=("公告日期", "股票代码", "公告标题"),
    ),
    # 币安现货 1h K线（coin_preprocess 产出，按 symbol 拆分为单文件）
    "coin-binance-candle-csv-1h": DatasetRule(
        name="coin-binance-candle-csv-1h",
        encoding="gb18030",
        has_note=True,
        key_cols=("candle_begin_time", "symbol"),
        sort_cols=("candle_begin_time",),
    ),
    # 币安合约 1h K线（同上，多 fundingRate 列）
    "coin-binance-swap-candle-csv-1h": DatasetRule(
        name="coin-binance-swap-candle-csv-1h",
        encoding="gb18030",
        has_note=True,
        key_cols=("candle_begin_time", "symbol"),
        sort_cols=("candle_begin_time",),
    ),
    # --- 以下为批量新增产品规则 ---
    # 因子系列（按股票代码拆分，以交易日期增量合并）
    "stock-anti-trend-factors": DatasetRule(
        name="stock-anti-trend-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-energy-factors": DatasetRule(
        name="stock-energy-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-multi-factor-series": DatasetRule(
        name="stock-multi-factor-series",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-oscillator-factors": DatasetRule(
        name="stock-oscillator-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-technical-factors": DatasetRule(
        name="stock-technical-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-trend-factors": DatasetRule(
        name="stock-trend-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-volume-price-factors": DatasetRule(
        name="stock-volume-price-factors",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 资金流（按股票代码拆分）
    "stock-money-flow": DatasetRule(
        name="stock-money-flow",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 分时收盘价（按股票代码拆分）
    "stock-15m-close-price": DatasetRule(
        name="stock-15m-close-price",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-5m-close-price": DatasetRule(
        name="stock-5m-close-price",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 非 A 股交易数据（按股票代码拆分）
    "stock-hk-stock-data": DatasetRule(
        name="stock-hk-stock-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-us-trading-data": DatasetRule(
        name="stock-us-trading-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("股票代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 可转债（按债券代码拆分）
    "stock-basic-bond": DatasetRule(
        name="stock-basic-bond",
        encoding="gb18030",
        has_note=True,
        key_cols=("债券代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 指数（按 index_code 拆分）
    "stock-1h-index-data": DatasetRule(
        name="stock-1h-index-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("index_code", "candle_end_time"),
        sort_cols=("candle_end_time",),
    ),
    "stock-us-main-index-data": DatasetRule(
        name="stock-us-main-index-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("index_code", "candle_end_time"),
        sort_cols=("candle_end_time",),
    ),
    "stock-asset-classification": DatasetRule(
        name="stock-asset-classification",
        encoding="gb18030",
        has_note=True,
        key_cols=("index_code", "date"),
        sort_cols=("date",),
    ),
    # 币种行情快照（按 symbol 拆分，symbol 在 key_cols index=1）
    "coin-coinmarketcap": DatasetRule(
        name="coin-coinmarketcap",
        encoding="gb18030",
        has_note=True,
        key_cols=("candle_begin_time", "symbol"),
        sort_cols=("candle_begin_time",),
    ),
    # ETF（按基金代码拆分）
    "stock-etf-trading-data": DatasetRule(
        name="stock-etf-trading-data",
        encoding="gb18030",
        has_note=True,
        key_cols=("基金代码", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 策略（按策略名称拆分）
    "stock-equity": DatasetRule(
        name="stock-equity",
        encoding="gb18030",
        has_note=True,
        key_cols=("策略名称", "交易日期"),
        sort_cols=("交易日期",),
    ),
    "stock-ind-element-equity": DatasetRule(
        name="stock-ind-element-equity",
        encoding="gb18030",
        has_note=True,
        key_cols=("策略名称", "交易日期"),
        sort_cols=("交易日期",),
    ),
    # 币圈市值快照：按 symbol 拆分后，以 日期+symbol 为主键增量合并
    "coin-cap": DatasetRule(
        name="coin-cap",
        encoding="gb18030",
        has_note=True,
        key_cols=("candle_begin_time", "symbol"),
        sort_cols=("candle_begin_time", "symbol"),
        date_filter_col="candle_begin_time",
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

# 拆分列配置与规则联动校验：避免 AGGREGATE_SPLIT_COLS 与 RULES 长期漂移。
# 拆分列不在 key_cols[0] 的产品需在此注册索引
_SPLIT_COL_KEY_INDEX = {"coin-cap": 1, "coin-coinmarketcap": 1}
for _product_name, _split_col in AGGREGATE_SPLIT_COLS.items():
    _rule = RULES.get(_product_name)
    if _rule is None:
        raise RuntimeError(f"拆分列配置引用了未知产品规则: {_product_name}")
    _key_idx = _SPLIT_COL_KEY_INDEX.get(_product_name, 0)
    if len(_rule.key_cols) <= _key_idx or _rule.key_cols[_key_idx] != _split_col:
        raise RuntimeError(
            f"拆分列配置与规则主键不一致: product={_product_name}, split_col={_split_col}, key_cols={_rule.key_cols}"
        )

def _deduplicate(items: Sequence[str]) -> List[str]:
    """保序去重: 保留第一次出现的元素, 丢弃后续重复项."""
    seen: set = set()
    result: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

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

    return _deduplicate(products)

@dataclass(frozen=True)
class TextFileSnapshot:
    """文本文件快照（用于 setup 失败回滚）。"""

    exists: bool
    content: str = ""
    mode: Optional[int] = None
