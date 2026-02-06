#!/usr/bin/env python3
"""
QuantClass 每日数据更新脚本（下载 + 解压 + 增量合并 + 格式统一）。

特点：
1) 复用官方接口：fetch/latest + get-download-link
2) 兼容 zip/tar/rar/7z
3) 按当前仓库已有格式写回（编码、备注行、表头）
4) 按主键去重，避免重复写入

----------------------------
给零基础同学的阅读建议：
----------------------------
你可以把这个脚本理解成 4 步流水线：

第 1 步：向 QuantClass API 询问“最新可下载时间”与“下载链接”
第 2 步：下载压缩包到本地临时目录，并解压
第 3 步：把解压后的 CSV 映射到你当前仓库结构（xbx_data/...）
第 4 步：按“统一格式规则”做增量合并（去重 + 排序 + 保留原编码）

脚本不会主动删除你的历史数据；默认是“有则合并、无则新建”。
如果你先用 --dry-run（演练模式），脚本只计算结果，不会真正写入数据文件。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import tarfile
import time
import traceback
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

import requests

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


# API 根地址（官方接口前缀）
DEFAULT_API_BASE = "https://api.quantclass.cn/api/data"
# 默认要更新的 3 个产品（正好对应你当前 xbx_data 里的 3 类主数据）
DEFAULT_PRODUCTS = [
    "stock-trading-data-pro",
    "stock-main-index-data",
    "stock-fin-data-xbx",
]
# 读取 CSV 时的编码兜底顺序（因为你的历史数据里既有 gb18030 也有 utf-8-sig）
ENCODING_CANDIDATES = ("utf-8-sig", "gb18030", "utf-8", "gbk")
# 已知数据目录名，用于把“解压后的文件”映射回“目标写入路径”
KNOWN_DATASETS = ("stock-trading-data-pro", "stock-main-index-data", "stock-fin-data-xbx")
DATE_NAME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{8}$")
DEFAULT_SECRETS_FILE = Path("xbx_data/xbx_apiKey.md")
DEFAULT_PROGRESS_EVERY = 500
DEFAULT_LOG_FORMAT = "text"
DEFAULT_LOG_LANG = "zh-CN"

LOG_LEVELS = {
    "ERROR": 0,
    "INFO": 1,
    "DEBUG": 2,
}
LOG_FORMATS = {"text", "json"}


def utc_now_iso() -> str:
    """返回 UTC 时间字符串（ISO 格式）。"""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ConsoleLogger:
    """
    极简日志器（面向终端输出）。

    - text: 兼容原有格式，适合人眼直接阅读
    - json: 结构化日志，适合后续采集/检索/聚合
    """

    def __init__(
        self,
        level: str = "INFO",
        run_id: str = "",
        log_format: str = DEFAULT_LOG_FORMAT,
        lang: str = DEFAULT_LOG_LANG,
    ) -> None:
        level = level.upper()
        if level not in LOG_LEVELS:
            level = "INFO"
        log_format = log_format.lower()
        if log_format not in LOG_FORMATS:
            log_format = DEFAULT_LOG_FORMAT
        self.level = level
        self.run_id = run_id
        self.log_format = log_format
        self.lang = lang

    def _enabled(self, level: str) -> bool:
        return LOG_LEVELS[level] <= LOG_LEVELS[self.level]

    def _emit(self, level: str, message: str, event: str = "log", **fields: object) -> None:
        if not self._enabled(level):
            return
        if self.log_format == "json":
            payload: Dict[str, object] = {
                "ts": utc_now_iso(),
                "level": level,
                "run_id": self.run_id,
                "event": event,
                "lang": self.lang,
                "message": message,
            }
            for key, value in fields.items():
                payload[key] = value
            print(json.dumps(payload, ensure_ascii=False, default=str))
            return

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rid = f"[{self.run_id}] " if self.run_id else ""
        print(f"{ts} [{level}] {rid}{message}")

    def error(self, message: str, event: str = "error", **fields: object) -> None:
        self._emit("ERROR", message, event=event, **fields)

    def info(self, message: str, event: str = "info", **fields: object) -> None:
        self._emit("INFO", message, event=event, **fields)

    def debug(self, message: str, event: str = "debug", **fields: object) -> None:
        self._emit("DEBUG", message, event=event, **fields)


LOGGER = ConsoleLogger(level="INFO")
PROGRESS_EVERY = DEFAULT_PROGRESS_EVERY


def log_error(message: str, event: str = "error", **fields: object) -> None:
    LOGGER.error(message, event=event, **fields)


def log_info(message: str, event: str = "info", **fields: object) -> None:
    LOGGER.info(message, event=event, **fields)


def log_debug(message: str, event: str = "debug", **fields: object) -> None:
    LOGGER.debug(message, event=event, **fields)


class FatalRequestError(RuntimeError):
    """参数或权限问题，立即失败，不重试。"""


@dataclass(frozen=True)
class DatasetRule:
    """
    每一种数据的“统一格式规则”。

    name: 规则名（通常等于目录名）
    encoding: 最终写盘编码（保证和你现有文件风格一致）
    has_note: 是否有“第1行备注、第2行表头”的结构
    key_cols: 主键列（用于去重）
    sort_cols: 排序列（让文件行顺序稳定）
    """

    name: str
    encoding: str
    has_note: bool
    key_cols: Tuple[str, ...]
    sort_cols: Tuple[str, ...]


@dataclass
class CsvPayload:
    """
    把一个 CSV 文件解析后的结构化内容放在这里。
    """

    note: Optional[str]
    header: List[str]
    rows: List[List[str]]
    encoding: str


@dataclass
class SyncStats:
    """
    每个产品（或全局）更新统计信息。
    """

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
class ProductRunResult:
    """
    单个产品一次执行结果。
    """

    product: str
    status: str
    date_time: str = ""
    mode: str = "network"
    elapsed_seconds: float = 0.0
    stats: SyncStats = field(default_factory=SyncStats)
    source_path: str = ""
    error: str = ""


@dataclass
class RunReport:
    """
    一次脚本运行的完整报告。
    """

    schema_version: str
    run_id: str
    started_at: str
    mode: str
    ended_at: str = ""
    duration_seconds: float = 0.0
    products: List[ProductRunResult] = field(default_factory=list)
    summary: SyncStats = field(default_factory=SyncStats)


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
    # period_offset 也按“备注 + 表头 + 日期主键”处理
    "period_offset.csv": DatasetRule(
        name="period_offset.csv",
        encoding="gb18030",
        has_note=True,
        key_cols=("交易日期",),
        sort_cols=("交易日期",),
    ),
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


def split_products(raw_products: Sequence[str]) -> List[str]:
    """
    把命令行传入的 products 参数解析成“去重后的产品列表”。

    支持两种写法：
    1) --products a b c
    2) --products a,b,c
    """

    products: List[str] = []
    for item in raw_products:
        for part in item.split(","):
            part = normalize_product_name(part.strip())
            if part:
                products.append(part)
    # 保序去重
    seen = set()
    result: List[str] = []
    for item in products:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


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

    api_key_candidates = [
        "xbx_api_key",
        "quantclass_api_key",
        "api_key",
        "apikey",
        "key",
    ]
    hid_candidates = [
        "xbx_id",
        "quantclass_hid",
        "hid",
        "uuid",
    ]

    api_key = next((pairs[k] for k in api_key_candidates if k in pairs), "")
    hid = next((pairs[k] for k in hid_candidates if k in pairs), "")
    return api_key, hid


def resolve_credentials(
    cli_api_key: str,
    cli_hid: str,
    secrets_file: Optional[Path],
) -> Tuple[str, str]:
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

    if secrets_file is not None:
        file_api_key, file_hid = load_secrets_from_file(secrets_file)
        if not api_key:
            api_key = file_api_key
        if not hid:
            hid = file_hid

    return api_key, hid


def find_latest_cached_extract(work_dir: Path, product: str) -> Tuple[str, Path]:
    """
    在 work_dir 下找到指定产品的最新缓存 extract 目录。
    返回：(cache_date, extract_path)。
    """

    product_dir = work_dir / product
    if not product_dir.exists():
        raise RuntimeError(f"未找到缓存目录: {product_dir}")

    candidates = []
    for d in product_dir.iterdir():
        if d.is_dir() and (d / "extract").is_dir():
            candidates.append(d)
    if not candidates:
        raise RuntimeError(f"未找到可用缓存 extract: {product_dir}")

    latest = sorted(candidates, key=lambda x: x.name)[-1]
    return latest.name, latest / "extract"


def normalize_latest_time(raw_text: str) -> str:
    """
    官方 latest 接口可能返回逗号分隔或空白分隔的一串时间，
    这里取其中最大的时间值作为“最新版本”。
    """

    candidates = [x.strip() for x in re.split(r"[,\s]+", raw_text) if x.strip()]
    if not candidates:
        raise RuntimeError("接口未返回可用的 date_time。")
    return max(candidates)


def request_data(method: str, url: str, headers: Dict[str, str], **kwargs) -> requests.Response:
    """
    统一 HTTP 请求入口（带重试）。

    设计思路：
    - 网络波动/服务器偶发 5xx：重试
    - 参数错误/权限不足（4xx 中的业务错误）：立即报错，不重试
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
            # timeout 防止请求卡死
            response = requests.request(method=method, url=url, headers=headers, timeout=60, **kwargs)
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                hint = ""
                err_text = str(exc)
                if "Failed to resolve" in err_text or "NameResolutionError" in err_text:
                    hint = "（DNS 解析失败：请检查网络、DNS 或代理设置）"
                raise RuntimeError(f"网络请求失败: {exc}{hint}") from exc
            # 指数退避：1s,2s,4s,8s...，避免短时间疯狂重试
            time.sleep(min(2 ** (attempt - 1), 8))
            continue

        if response.status_code == 200:
            return response

        message = status_messages.get(response.status_code, f"未知错误（HTTP {response.status_code}）")
        if response.status_code in {400, 401, 403, 404}:
            # 这类错误通常是权限、参数、下载额度问题，重试也无意义
            raise FatalRequestError(message)
        if attempt >= max_attempts:
            raise RuntimeError(message)
        log_info(f"HTTP {response.status_code}，重试中（{attempt}/{max_attempts}）: {url.split('?')[0]}")
        time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError("请求失败：超过最大重试次数。")


def get_latest_time(api_base: str, product: str, hid: str, headers: Dict[str, str]) -> str:
    """
    调 latest 接口拿该产品可下载的最新 date_time。
    """

    url = f"{api_base}/fetch/{product}-daily/latest?uuid={hid}"
    res = request_data("GET", url=url, headers=headers)
    return normalize_latest_time(res.text)


def get_download_link(
    api_base: str, product: str, date_time: str, hid: str, headers: Dict[str, str]
) -> str:
    """
    用产品名 + date_time 换取“真实下载链接”。
    """

    url = f"{api_base}/get-download-link/{product}-daily/{date_time}?uuid={hid}"
    res = request_data("GET", url=url, headers=headers)
    download_link = res.text.strip()
    if not download_link:
        raise RuntimeError(f"{product} {date_time} 未返回下载链接。")
    return download_link


def build_file_name(file_url: str, product: str, date_time: str) -> str:
    """
    从下载链接提取文件名。
    如果提取失败，兜底生成一个可用名字。
    """

    parsed = urlparse(file_url)
    name = Path(unquote(parsed.path)).name
    if name:
        return name
    return f"{product}_{date_time}.zip"


def save_file(file_url: str, file_path: Path, headers: Dict[str, str]) -> None:
    """
    流式下载文件到本地，避免一次性占用太多内存。
    """

    file_path.parent.mkdir(parents=True, exist_ok=True)
    res = request_data("GET", url=file_url, headers=headers, stream=True)
    with file_path.open("wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def _ensure_within(base: Path, target: Path) -> None:
    """
    安全检查：确保解压目标路径没有“跳出”到指定目录之外。
    防止恶意压缩包通过 ../../ 覆盖其它文件。
    """

    try:
        target.resolve().relative_to(base.resolve())
    except Exception as exc:
        raise RuntimeError(f"解压路径越界: {target}") from exc


def safe_extract_zip(path: Path, save_path: Path) -> None:
    """
    安全解压 zip：逐个成员检查路径后再 extractall。
    """

    with zipfile.ZipFile(path) as zf:
        for member in zf.infolist():
            target = save_path / member.filename
            _ensure_within(save_path, target)
        zf.extractall(save_path)


def safe_extract_tar(path: Path, save_path: Path) -> None:
    """
    安全解压 tar：逐个成员检查路径后再 extractall。
    """

    with tarfile.open(path) as tf:
        for member in tf.getmembers():
            target = save_path / member.name
            _ensure_within(save_path, target)
        tf.extractall(save_path)


def extract_archive(path: Path, save_path: Path) -> None:
    """
    把下载文件处理为“可遍历的数据目录”。

    两种常见返回都支持：
    1) 压缩包：zip/tar/rar/7z -> 解压到 save_path
    2) 直出文件：csv/ts -> 直接复制到 save_path

    说明：
    有些产品某天会直接给 .csv（不是压缩包），
    这里做兼容，避免再报“不支持的压缩格式”。
    """

    lower_name = path.name.lower()
    save_path.mkdir(parents=True, exist_ok=True)

    # 接口有时会直接返回 CSV/TS 文件，而不是压缩包。
    # 这种情况不需要解压，直接复制到工作目录即可。
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
        if rarfile is None:
            raise RuntimeError("当前环境未安装 rarfile，无法解压 .rar 文件。")
        with rarfile.RarFile(path) as rf:
            rf.extractall(save_path)
        return

    if lower_name.endswith(".7z"):
        if py7zr is None:
            raise RuntimeError("当前环境未安装 py7zr，无法解压 .7z 文件。")
        with py7zr.SevenZipFile(path, "r") as sf:
            sf.extractall(path=save_path)
        return

    # 兜底：某些链接文件名可能没有标准后缀，但内容本身其实是 zip/tar。
    if zipfile.is_zipfile(path):
        safe_extract_zip(path, save_path)
        return
    if tarfile.is_tarfile(path):
        safe_extract_tar(path, save_path)
        return

    raise RuntimeError(f"不支持的压缩格式: {path.name}")


def normalize_source_relpath(src_rel_path: Path, product: str) -> Path:
    """
    规范化“解压目录内的相对路径”，去掉无关包装层。

    常见无关包装层示例：
    - stock-trading-data-pro/...
    - stock-trading-data-pro-daily/...
    - 2026-02-06/...（日期目录）
    """

    parts = list(src_rel_path.parts)
    if not parts:
        return src_rel_path

    # 去掉前缀里的产品目录名（如果有）
    if parts and parts[0] in KNOWN_DATASETS:
        parts = parts[1:]
    elif parts and normalize_product_name(parts[0]) == product:
        parts = parts[1:]

    # 去掉可能存在的外层日期目录包装
    if parts and DATE_NAME_PATTERN.fullmatch(parts[0]):
        parts = parts[1:]

    if not parts:
        return Path(src_rel_path.name)
    return Path(*parts)


def is_daily_aggregate_file(src_rel_path: Path) -> bool:
    """
    判断是否为“按天聚合”文件（例如 2026-02-06.csv / 20260206.csv）。
    """

    if src_rel_path.suffix.lower() != ".csv":
        return False
    stem = src_rel_path.stem
    return bool(DATE_NAME_PATTERN.fullmatch(stem))


def infer_target_relpath(src_rel_path: Path, product: str) -> Optional[Path]:
    """
    推断“解压后的源文件”应该写回到 xbx_data 的哪个相对路径。

    例子：
    - 源文件（相对 extract）sh600000.csv
      -> stock-trading-data-pro/sh600000.csv
    - 源文件（相对 extract）sz000001/sz000001_一般企业.csv
      -> stock-fin-data-xbx/sz000001/sz000001_一般企业.csv
    """

    src_rel_path = normalize_source_relpath(src_rel_path, product)
    parts = src_rel_path.parts

    if src_rel_path.name == "period_offset.csv":
        return Path("period_offset.csv")
    if src_rel_path.name == "period_offset.ts":
        return Path("period_offset.ts")

    if product in ("stock-trading-data-pro", "stock-main-index-data"):
        # 标准文件名：sh600000.csv / sz000001.csv / bj920000.csv
        if re.fullmatch(r"[a-z]{2}\d{6}\.csv", src_rel_path.name):
            return Path(product) / src_rel_path.name
        return None

    if product == "stock-fin-data-xbx":
        parent = src_rel_path.parent.name
        # 财务数据目录一般是代码目录（如 sz000001）
        if re.fullmatch(r"[a-z]{2}\d{6}", parent) and src_rel_path.suffix.lower() == ".csv":
            return Path(product) / parent / src_rel_path.name
        # 兜底：从文件名里提取代码（如 sz000001_一般企业.csv）
        match = re.match(r"^([a-z]{2}\d{6})_", src_rel_path.name)
        if match:
            return Path(product) / match.group(1) / src_rel_path.name
        return None

    if product == "period_offset":
        if src_rel_path.suffix.lower() == ".csv":
            return Path("period_offset.csv")
        if src_rel_path.suffix.lower() == ".ts":
            return Path("period_offset.ts")

    return None


def infer_rule(rel_path: Path) -> Optional[DatasetRule]:
    """
    根据目标相对路径选择对应的格式规则。
    """

    if rel_path.name == "period_offset.csv":
        return RULES["period_offset.csv"]
    if not rel_path.parts:
        return None
    return RULES.get(rel_path.parts[0])


def decode_text(path: Path, preferred_encoding: Optional[str]) -> Tuple[str, str]:
    """
    尝试用多个编码解码文本，返回 (文本内容, 实际编码)。
    """

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
    """
    粗略判断某一行是否像“表头行”。
    """

    if not row:
        return False
    first = row[0].strip().lstrip("\ufeff")
    known_first_col = {"股票代码", "candle_end_time", "stock_code", "交易日期"}
    if first in known_first_col:
        return True
    return first.startswith(("股票代码", "candle_end_time", "stock_code"))


def parse_csv_line(line: str) -> List[str]:
    """
    用 csv.reader 解析单行，正确处理逗号/引号。
    """

    return next(csv.reader([line]))


def read_csv_payload(path: Path, preferred_encoding: Optional[str] = None) -> CsvPayload:
    """
    读取 CSV，兼容两种结构：
    1) 直接表头（无备注）
    2) 第1行备注 + 第2行表头
    """

    text, encoding = decode_text(path, preferred_encoding)
    lines = text.splitlines()
    lines = [line for line in lines if line.strip() != ""]
    if not lines:
        return CsvPayload(note=None, header=[], rows=[], encoding=encoding)

    first = parse_csv_line(lines[0])
    second = parse_csv_line(lines[1]) if len(lines) > 1 else []

    note: Optional[str] = None
    if looks_like_header(first):
        # 第一行就是表头
        header = first
        data_start = 1
    elif looks_like_header(second):
        # 第一行备注，第二行表头
        note = lines[0].lstrip("\ufeff")
        header = second
        data_start = 2
    else:
        # 兜底：把第一行当表头（不常见）
        header = first
        data_start = 1

    rows = [parse_csv_line(line) for line in lines[data_start:]]
    return CsvPayload(note=note, header=header, rows=rows, encoding=encoding)


def align_rows(rows: Iterable[Sequence[str]], source_header: Sequence[str], target_header: Sequence[str]) -> List[List[str]]:
    """
    按“列名”对齐行数据。

    为什么要这样做：
    当新旧文件列顺序不完全一致时，直接按位置拼接会错位；
    按列名对齐可以确保字段不会串列。
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
    """
    生成一行的去重键（主键）。
    """

    if not key_indices:
        return tuple(row)
    key = tuple(row[idx] if idx < len(row) else "" for idx in key_indices)
    if all(not cell for cell in key):
        return tuple(row)
    return key


def sortable_value(value: str) -> Tuple[int, object]:
    """
    把字符串值转成可排序的统一结构，兼容：
    - yyyy-mm-dd
    - yyyymmdd
    - 数值
    - 其它字符串
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
    核心合并函数：旧数据 + 新数据 -> 合并后数据。

    合并策略：
    1) 以旧表头为准（若旧文件不存在，则用新表头）
    2) 新旧行都先按列名对齐到同一表头
    3) 按主键去重：新数据会覆盖同主键的旧数据
    4) 按 rule.sort_cols 排序，保证文件顺序稳定
    """

    target_header = existing.header if existing and existing.header else incoming.header
    if not target_header:
        return CsvPayload(note=existing.note if existing else incoming.note, header=[], rows=[], encoding=rule.encoding), 0

    # 先对齐列，避免新旧 CSV 的列顺序差异带来字段错位
    existing_rows = (
        align_rows(existing.rows, existing.header, target_header) if existing else []
    )
    incoming_rows = align_rows(incoming.rows, incoming.header, target_header)

    # 计算主键列索引
    key_cols = [col for col in rule.key_cols if col in target_header]
    key_indices = [target_header.index(col) for col in key_cols]

    # 用 dict 做去重：同一个 key 后写入的行会覆盖先写入的行
    merged_map: Dict[Tuple[str, ...], List[str]] = {}
    for row in existing_rows:
        merged_map[row_key(row, key_indices)] = row
    before_count = len(merged_map)

    for row in incoming_rows:
        merged_map[row_key(row, key_indices)] = row

    rows = list(merged_map.values())
    sort_indices = [target_header.index(col) for col in rule.sort_cols if col in target_header]
    if sort_indices:
        # 排序后每次写盘结果更稳定，便于后续 diff/核对
        rows.sort(key=lambda row: tuple(sortable_value(row[idx]) for idx in sort_indices))

    note = None
    if rule.has_note:
        # 优先保留旧文件备注；若旧文件不存在，则用新文件备注
        note = existing.note if existing and existing.note is not None else incoming.note
        if note is None:
            note = ""

    merged = CsvPayload(note=note, header=list(target_header), rows=rows, encoding=rule.encoding)
    return merged, max(0, len(merged_map) - before_count)


def write_csv_payload(path: Path, payload: CsvPayload, rule: DatasetRule, dry_run: bool) -> None:
    """
    把合并后的内容写回 CSV（可被 --dry-run 跳过）。
    """

    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=rule.encoding, newline="") as f:
        if rule.has_note and payload.note is not None:
            f.write(payload.note.rstrip("\r\n"))
            f.write("\n")
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(payload.header)
        writer.writerows(payload.rows)


def sync_payload_to_target(
    incoming: CsvPayload, target: Path, rule: DatasetRule, dry_run: bool
) -> Tuple[str, int]:
    """
    把“已解析好的 CSV 内容”同步到目标文件。
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
    """
    同步一个 CSV 文件（读取 -> 合并 -> 判断是否变化 -> 写回）。
    返回：(状态, 新增行数)
    """

    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    return sync_payload_to_target(incoming=incoming, target=target, rule=rule, dry_run=dry_run)


def sync_daily_aggregate_file(
    src: Path, product: str, data_root: Path, dry_run: bool
) -> SyncStats:
    """
    处理“按天聚合 CSV”：
    - 交易数据：按 股票代码 拆分
    - 指数数据：按 index_code 拆分

    拆分后再分别合并进各代码文件，保证与现有目录结构一致。
    """

    stats = SyncStats()
    rule = RULES[product]
    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    if not incoming.header:
        stats.skipped_files += 1
        return stats

    code_col = "股票代码" if product == "stock-trading-data-pro" else "index_code"
    if code_col not in incoming.header:
        stats.skipped_files += 1
        return stats

    code_idx = incoming.header.index(code_col)
    grouped_rows: Dict[str, List[List[str]]] = {}
    for row in incoming.rows:
        code = row[code_idx].strip() if code_idx < len(row) else ""
        if not code:
            continue
        grouped_rows.setdefault(code, []).append(list(row))

    if not grouped_rows:
        stats.skipped_files += 1
        return stats

    total_codes = len(grouped_rows)
    log_info(f"[{product}] aggregate split file={src.name}, codes={total_codes}, rows={len(incoming.rows)}")

    for idx, (code, rows) in enumerate(grouped_rows.items(), start=1):
        target = data_root / product / f"{code}.csv"
        payload = CsvPayload(
            note=incoming.note,
            header=list(incoming.header),
            rows=rows,
            encoding=incoming.encoding,
        )
        result, added_rows = sync_payload_to_target(
            incoming=payload, target=target, rule=rule, dry_run=dry_run
        )
        if result == "created":
            stats.created_files += 1
            stats.rows_added += added_rows
        elif result == "updated":
            stats.updated_files += 1
            stats.rows_added += added_rows
        elif result == "unchanged":
            stats.unchanged_files += 1
        else:
            stats.skipped_files += 1

        if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_codes:
            log_info(
                f"[{product}] aggregate progress {idx}/{total_codes} "
                f"(created={stats.created_files}, updated={stats.updated_files}, unchanged={stats.unchanged_files})"
            )

    return stats


def sync_text_file(src: Path, target: Path, dry_run: bool) -> str:
    """
    同步文本文件（当前主要用于 period_offset.ts）。
    """

    src_bytes = src.read_bytes()
    existed_before = target.exists()
    if existed_before and target.read_bytes() == src_bytes:
        return "unchanged"
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
    return "updated" if existed_before else "created"


def iter_candidate_files(root: Path) -> Iterable[Path]:
    """
    遍历解压目录中可能需要同步的文件（目前只处理 .csv/.ts）。
    """

    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".csv", ".ts"}:
            yield path


def sync_from_extract(product: str, extract_path: Path, data_root: Path, dry_run: bool) -> SyncStats:
    """
    从 extract 目录同步到数据目录（在线下载和 cache-only 共用）。
    """

    stats = SyncStats()
    files = sorted(iter_candidate_files(extract_path))
    total_files = len(files)
    log_info(f"[{product}] start sync from extract: files={total_files}, path={extract_path}")

    for idx, src in enumerate(files, start=1):
        src_rel_path = src.relative_to(extract_path)
        normalized_rel_path = normalize_source_relpath(src_rel_path, product)

        # 交易/指数若拿到“按天聚合 CSV”（例如 2026-02-06.csv），
        # 需要先按代码拆分再合并，否则会破坏你现有目录结构。
        if product in ("stock-trading-data-pro", "stock-main-index-data") and is_daily_aggregate_file(
            normalized_rel_path
        ):
            aggregate_stats = sync_daily_aggregate_file(
                src=src, product=product, data_root=data_root, dry_run=dry_run
            )
            stats.merge(aggregate_stats)
        else:
            rel_path = infer_target_relpath(normalized_rel_path, product)
            if rel_path is None:
                # 无法识别目标位置的文件，先跳过并计数
                stats.skipped_files += 1
                log_info(f"[{product}] skipped unmapped file: {src_rel_path}")
                continue
            target = data_root / rel_path

            if src.suffix.lower() == ".ts":
                result = sync_text_file(src=src, target=target, dry_run=dry_run)
                if result == "created":
                    stats.created_files += 1
                elif result == "updated":
                    stats.updated_files += 1
                else:
                    stats.unchanged_files += 1
            else:
                rule = infer_rule(rel_path)
                if rule is None:
                    # 没有规则就不冒险写入，避免污染数据
                    stats.skipped_files += 1
                    log_info(f"[{product}] skipped file without rule: {rel_path}")
                    continue

                result, added_rows = sync_csv_file(src=src, target=target, rule=rule, dry_run=dry_run)
                if result == "created":
                    stats.created_files += 1
                    stats.rows_added += added_rows
                elif result == "updated":
                    stats.updated_files += 1
                    stats.rows_added += added_rows
                elif result == "unchanged":
                    stats.unchanged_files += 1
                else:
                    stats.skipped_files += 1

        if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total_files:
            log_info(
                f"[{product}] progress {idx}/{total_files} "
                f"(created={stats.created_files}, updated={stats.updated_files}, "
                f"unchanged={stats.unchanged_files}, skipped={stats.skipped_files})"
            )

    return stats


def process_product(
    product: str,
    date_time: Optional[str],
    api_base: str,
    hid: str,
    headers: Dict[str, str],
    data_root: Path,
    work_dir: Path,
    keep_archive: bool,
    dry_run: bool,
) -> Tuple[str, str, SyncStats]:
    """
    处理“单个产品”的完整流程。

    流程：
    1) 确定 date_time（若未指定则调 latest）
    2) 获取下载链接并下载文件（可能是压缩包，也可能是 CSV）
    3) 解压或直接放入临时目录
    4) 若是“按天聚合 CSV”，先拆分到代码粒度；否则按文件粒度直接同步
    5) 返回该产品的统计结果
    """

    product = normalize_product_name(product)
    stats = SyncStats()
    t0 = time.time()
    log_info(f"[{product}] start network mode")

    if not date_time:
        # 没指定时间时，自动拉最新可下载版本
        date_time = get_latest_time(api_base=api_base, product=product, hid=hid, headers=headers)
        log_info(f"[{product}] latest date_time={date_time}")

    file_url = get_download_link(
        api_base=api_base, product=product, date_time=date_time, hid=hid, headers=headers
    )
    file_name = build_file_name(file_url, product, date_time)
    log_info(f"[{product}] file={file_name}")

    product_work = work_dir / product / date_time
    download_path = product_work / file_name
    extract_path = product_work / "extract"

    # 每次都清空本次解压目录，保证本轮处理输入是“干净目录”
    if extract_path.exists():
        shutil.rmtree(extract_path)
    extract_path.mkdir(parents=True, exist_ok=True)

    # 断点容错：如果下载文件已存在且大小正常，就复用，避免重复下载
    if not download_path.exists() or download_path.stat().st_size == 0:
        save_file(file_url=file_url, file_path=download_path, headers=headers)
        log_info(f"[{product}] downloaded -> {download_path}")
    else:
        log_info(f"[{product}] reuse cache -> {download_path}")

    # 这里会自动兼容两类返回：
    # - 压缩包：解压
    # - CSV/TS：直接复制到 extract_path
    extract_archive(download_path, extract_path)
    log_info(f"[{product}] extracted -> {extract_path}")
    stats = sync_from_extract(product=product, extract_path=extract_path, data_root=data_root, dry_run=dry_run)

    if not keep_archive and not dry_run:
        # 默认不保留压缩包，节省磁盘空间（可用 --keep-archive 保留）
        download_path.unlink(missing_ok=True)

    elapsed = time.time() - t0
    log_info(
        f"[{product}] done in {elapsed:.2f}s: "
        f"created={stats.created_files}, updated={stats.updated_files}, "
        f"unchanged={stats.unchanged_files}, skipped={stats.skipped_files}, rows_added={stats.rows_added}"
    )
    return product, date_time, stats


def write_run_report(path: Path, report: RunReport) -> None:
    """
    将本次运行报告写入 JSON 文件。
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = run_report_to_dict(report)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """
    命令行参数定义。
    """

    parser = argparse.ArgumentParser(description="QuantClass 日更拉取 + 本地数据格式统一脚本")
    parser.add_argument(
        "--products",
        nargs="+",
        default=DEFAULT_PRODUCTS,
        help="产品名（支持空格分隔或逗号分隔），示例：stock-trading-data-pro stock-fin-data-xbx",
    )
    parser.add_argument(
        "--date-time",
        default="",
        help="指定 date_time；留空时自动取接口 latest。",
    )
    parser.add_argument("--api-key", default="", help="接口 api-key（优先命令行，其次读取环境变量 QUANTCLASS_API_KEY）")
    parser.add_argument("--hid", default="", help="接口 hid（优先命令行，其次读取环境变量 QUANTCLASS_HID）")
    parser.add_argument(
        "--secrets-file",
        type=Path,
        default=DEFAULT_SECRETS_FILE,
        help="本地密钥文件（默认 xbx_data/xbx_apiKey.md）。当命令行和环境变量缺失时作为兜底。",
    )
    parser.add_argument("--api-base", default=DEFAULT_API_BASE, help="接口根地址")
    parser.add_argument("--data-root", type=Path, default=Path("xbx_data"), help="数据根目录")
    parser.add_argument("--work-dir", type=Path, default=Path(".cache/quantclass"), help="下载与解压临时目录")
    parser.add_argument("--keep-archive", action="store_true", help="保留下载压缩包")
    parser.add_argument("--dry-run", action="store_true", help="演练模式：只计算不落盘")
    parser.add_argument("--cache-only", action="store_true", help="仅使用本地缓存，不联网下载。")
    parser.add_argument("--continue-on-error", action="store_true", help="某个产品失败时继续处理后续产品。")
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY, help="每处理 N 个项目输出一次进度日志。")
    parser.add_argument("--report-file", type=Path, default=None, help="将本次运行结果写入 JSON 文件。")
    parser.add_argument("--report-json", action="store_true", help="在终端额外打印一行 JSON 报告。")
    parser.add_argument(
        "--log-format",
        choices=("text", "json"),
        default=DEFAULT_LOG_FORMAT,
        help="日志格式：text(默认，兼容旧输出) 或 json(结构化日志)。",
    )
    parser.add_argument("--verbose", action="store_true", help="输出详细日志（DEBUG）。")
    parser.add_argument("--quiet", action="store_true", help="只输出错误和最终摘要。")
    return parser


def main() -> int:
    """
    程序入口。

    你可以把 main 理解成：
    - 读参数
    - 校验必要信息（api_key / hid / data_root）
    - 按产品逐个执行 process_product
    - 汇总输出结果
    """

    parser = build_parser()
    args = parser.parse_args()

    if args.verbose and args.quiet:
        parser.error("--verbose 和 --quiet 不能同时使用")
    if args.progress_every <= 0:
        parser.error("--progress-every 必须是正整数")

    # 初始化日志器
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_level = "DEBUG" if args.verbose else ("ERROR" if args.quiet else "INFO")
    global LOGGER, PROGRESS_EVERY
    LOGGER = ConsoleLogger(level=log_level, run_id=run_id, log_format=args.log_format, lang=DEFAULT_LOG_LANG)
    PROGRESS_EVERY = max(1, args.progress_every)

    products = split_products(args.products)
    if not products:
        parser.error("products 不能为空")

    data_root = args.data_root.resolve()
    work_dir = args.work_dir.resolve()
    if not data_root.exists():
        parser.error(f"data-root 不存在: {data_root}")

    mode = "cache-only" if args.cache_only else "network"
    report = RunReport(
        schema_version="1.0",
        run_id=run_id,
        started_at=utc_now_iso(),
        mode=mode,
    )

    log_info(
        f"任务开始 mode={mode}, products={products}, dry_run={args.dry_run}, "
        f"data_root={data_root}, work_dir={work_dir}",
        event="run_start",
        mode=mode,
        products=products,
        dry_run=args.dry_run,
        data_root=str(data_root),
        work_dir=str(work_dir),
    )

    api_key = ""
    hid = ""
    if not args.cache_only:
        secrets_file = args.secrets_file.resolve() if args.secrets_file else None
        api_key, hid = resolve_credentials(
            cli_api_key=args.api_key,
            cli_hid=args.hid,
            secrets_file=secrets_file,
        )
        if not api_key:
            parser.error(
                "缺少 api-key。请通过 --api-key、环境变量 QUANTCLASS_API_KEY，"
                f"或 secrets 文件 {secrets_file} 提供。"
            )
        if not hid:
            parser.error(
                "缺少 hid。请通过 --hid、环境变量 QUANTCLASS_HID，"
                f"或 secrets 文件 {secrets_file} 提供。"
            )
    else:
        log_info("cache-only 模式：跳过 API 凭证校验和网络下载。", event="cache_only_enabled")

    headers = {
        # 官方示例里使用浏览器 UA，这里保持兼容
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/100.0.4896.127 Safari/537.36"
        ),
        "content-type": "application/json",
        "api-key": api_key,
    }

    requested_date_time = args.date_time.strip() or ""
    total = SyncStats()
    has_error = False
    t_run_start = time.time()

    for raw_product in products:
        product = normalize_product_name(raw_product)
        t_product_start = time.time()
        try:
            if args.cache_only:
                cache_date, extract_path = find_latest_cached_extract(work_dir=work_dir, product=product)
                stats = sync_from_extract(
                    product=product,
                    extract_path=extract_path,
                    data_root=data_root,
                    dry_run=args.dry_run,
                )
                actual_time = cache_date
                result_mode = "cache-only"
                source_path = str(extract_path)
            else:
                product_name, actual_time, stats = process_product(
                    product=product,
                    date_time=requested_date_time or None,
                    api_base=args.api_base.rstrip("/"),
                    hid=hid,
                    headers=headers,
                    data_root=data_root,
                    work_dir=work_dir,
                    keep_archive=args.keep_archive,
                    dry_run=args.dry_run,
                )
                product = product_name
                result_mode = "network"
                source_path = str((work_dir / product / actual_time / "extract").resolve())

            elapsed = time.time() - t_product_start
            total.merge(stats)
            report.products.append(
                ProductRunResult(
                    product=product,
                    status="ok",
                    date_time=actual_time,
                    mode=result_mode,
                    elapsed_seconds=elapsed,
                    stats=stats,
                    source_path=source_path,
                )
            )
            if not args.quiet:
                if args.log_format == "json":
                    print(
                        json.dumps(
                            {
                                "ts": utc_now_iso(),
                                "level": "INFO",
                                "run_id": run_id,
                                "event": "product_summary",
                                "lang": DEFAULT_LOG_LANG,
                                "message": "产品处理完成",
                                "product": product,
                                "date_time": actual_time,
                                "mode": result_mode,
                                "created": stats.created_files,
                                "updated": stats.updated_files,
                                "unchanged": stats.unchanged_files,
                                "skipped": stats.skipped_files,
                                "rows_added": stats.rows_added,
                                "elapsed_seconds": round(elapsed, 2),
                            },
                            ensure_ascii=False,
                        )
                    )
                else:
                    print(
                        f"[{product}] date_time={actual_time} "
                        f"created={stats.created_files} updated={stats.updated_files} "
                        f"unchanged={stats.unchanged_files} skipped={stats.skipped_files} "
                        f"rows_added={stats.rows_added} elapsed={elapsed:.2f}s"
                    )
        except Exception as exc:
            has_error = True
            elapsed = time.time() - t_product_start
            report.products.append(
                ProductRunResult(
                    product=product,
                    status="error",
                    date_time=requested_date_time,
                    mode=mode,
                    elapsed_seconds=elapsed,
                    error=str(exc),
                )
            )
            log_error(
                f"[{product}] failed in {elapsed:.2f}s: {exc}",
                event="product_failed",
                product=product,
                elapsed_seconds=round(elapsed, 2),
                error=str(exc),
            )
            if args.verbose:
                log_debug(traceback.format_exc(), event="traceback")
            if not args.continue_on_error:
                log_error("遇到错误即停止（可加 --continue-on-error 继续后续产品）。", event="stop_on_error")
                break

    report.summary = total
    report.ended_at = utc_now_iso()
    report.duration_seconds = time.time() - t_run_start

    error_count = sum(1 for x in report.products if x.status != "ok")
    if args.log_format == "json":
        print(
            json.dumps(
                {
                    "ts": utc_now_iso(),
                    "level": "INFO",
                    "run_id": run_id,
                    "event": "run_summary",
                    "lang": DEFAULT_LOG_LANG,
                    "message": "本次运行汇总",
                    "created": total.created_files,
                    "updated": total.updated_files,
                    "unchanged": total.unchanged_files,
                    "skipped": total.skipped_files,
                    "rows_added": total.rows_added,
                    "duration_seconds": round(report.duration_seconds, 2),
                    "errors": error_count,
                },
                ensure_ascii=False,
            )
        )
    else:
        print(
            "SUMMARY "
            f"created={total.created_files} updated={total.updated_files} "
            f"unchanged={total.unchanged_files} skipped={total.skipped_files} "
            f"rows_added={total.rows_added} duration={report.duration_seconds:.2f}s "
            f"errors={error_count}"
        )

    if args.report_file is not None:
        report_path = args.report_file.resolve()
        write_run_report(path=report_path, report=report)
        log_info(f"report written: {report_path}", event="report_written", report_path=str(report_path))

    if args.report_json:
        print(json.dumps(run_report_to_dict(report), ensure_ascii=False))

    return 1 if has_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
