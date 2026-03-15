"""职责：数据状态聚合查询，供 CLI status 命令和 GUI 共用。

只读取本地信息（timestamp.txt + run_report JSON），不调用 API。
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .constants import ENCODING_CANDIDATES, KNOWN_DATASETS, TIMESTAMP_FILE_NAME
from .models import log_error
from .status_store import read_local_timestamp_date, read_or_backfill_product_last_status, report_dir_path

# --- 产品状态总览 ---

# 状态颜色阈值（自然日）
_DAYS_YELLOW = 1  # >= 1 天落后: 黄色
_DAYS_RED = 4     # >= 4 天落后: 红色


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """解析 YYYY-MM-DD 日期字符串。"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def _days_behind(local_date_str: Optional[str], today: Optional[date] = None) -> Optional[int]:
    """计算落后天数（自然日）。返回 None 表示无本地数据。"""
    local_date = _parse_date(local_date_str)
    if local_date is None:
        return None
    if today is None:
        today = date.today()
    diff = (today - local_date).days
    return max(0, diff)


def _status_color(days_behind: Optional[int], last_status: str) -> str:
    """根据落后天数和上次结果决定状态颜色。

    返回值: "green" / "yellow" / "red" / "gray"
    """
    if last_status == "error":
        return "red"
    if days_behind is None:
        return "gray"
    if days_behind < _DAYS_YELLOW:
        return "green"
    if days_behind < _DAYS_RED:
        return "yellow"
    return "red"


def get_products_overview(
    data_root: Path,
    catalog_products: Sequence[str],
    today: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """返回所有产品的状态总览列表。

    每个产品包含:
    - name: 产品名
    - local_date: 本地数据日期 (YYYY-MM-DD 或 None)
    - days_behind: 落后天数 (int 或 None)
    - last_status: 上次同步状态 ("ok" / "error" / "skipped" / "")
    - last_error: 上次错误信息 (str)
    - status_color: 状态颜色 ("green" / "yellow" / "red" / "gray")
    """
    log_dir = report_dir_path(data_root)
    last_results = read_or_backfill_product_last_status(log_dir)

    overview: List[Dict[str, Any]] = []
    for product in catalog_products:
        local_date = read_local_timestamp_date(data_root, product)
        behind = _days_behind(local_date, today)
        last = last_results.get(product, {})
        last_status = last.get("status", "")
        color = _status_color(behind, last_status)
        overview.append({
            "name": product,
            "local_date": local_date,
            "days_behind": behind,
            "last_status": last_status,
            "last_error": last.get("error", ""),
            "status_color": color,
        })
    return overview


# --- 运行摘要 ---

def get_latest_run_summary(log_dir: Path) -> Optional[Dict[str, Any]]:
    """解析最新的 run_report JSON，返回运行摘要。

    返回 dict 包含:
    - run_id, started_at, ended_at, duration_seconds
    - success_total, failed_total, skipped_total
    - failed_products: [{product, error, reason_code}]
    - report_file: 报告文件路径

    如果没有报告文件，返回 None。
    """
    report_files = sorted(log_dir.glob("run_report_*.json"))
    if not report_files:
        return None
    latest = report_files[-1]
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    failed_products = [
        {
            "product": item.get("product", ""),
            "error": item.get("error", ""),
            "reason_code": item.get("reason_code", ""),
        }
        for item in data.get("products", [])
        if item.get("status") == "error"
    ]

    return {
        "run_id": data.get("run_id", ""),
        "started_at": data.get("started_at", ""),
        "ended_at": data.get("ended_at", ""),
        "duration_seconds": data.get("duration_seconds", 0),
        "success_total": data.get("success_total", 0),
        "failed_total": data.get("failed_total", 0),
        "skipped_total": data.get("skipped_total", 0),
        "failed_products": failed_products,
        "report_file": str(latest),
    }


def get_run_history(log_dir: Path, n: int = 10) -> List[Dict[str, Any]]:
    """返回最近 N 次运行的摘要列表（按时间降序）。

    每条包含: run_id, started_at, duration_seconds, success_total, failed_total, skipped_total, report_file
    """
    # n <= 0 时无意义，直接返回空列表
    if n <= 0:
        return []
    report_files = sorted(log_dir.glob("run_report_*.json"))
    # 取最近 n 个，按时间降序（[-n:] 当 n >= len 时返回全部，无需分支）
    recent = list(report_files[-n:])
    recent.reverse()

    history: List[Dict[str, Any]] = []
    for path in recent:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        history.append({
            "run_id": data.get("run_id", ""),
            "started_at": data.get("started_at", ""),
            "duration_seconds": data.get("duration_seconds", 0),
            "success_total": data.get("success_total", 0),
            "failed_total": data.get("failed_total", 0),
            "skipped_total": data.get("skipped_total", 0),
            "report_file": str(path),
        })
    return history


def get_run_detail(log_dir: Path, report_file: str) -> Dict[str, Any]:
    """读取指定运行报告的产品明细。

    安全检查：report_file 必须在 log_dir 内，防止路径遍历。

    返回结构:
    {
        "ok": True/False,
        "started_at": "...",
        "duration_seconds": 123,
        "success_total": N, "failed_total": N, "skipped_total": N,
        "products": [{"product": str, "status": str, "elapsed_seconds": float, "error": str}, ...]
    }
    """
    # 路径安全检查：report_file 解析后必须在 log_dir 目录内（防止路径遍历）
    report_path = Path(report_file).resolve()
    log_dir_resolved = log_dir.resolve()
    if not report_path.is_relative_to(log_dir_resolved):
        return {"ok": False, "error": "非法路径：报告文件不在日志目录内"}

    if not report_path.exists():
        return {"ok": False, "error": "报告文件不存在"}

    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"报告文件读取失败：{exc}"}

    # 提取产品列表，失败排前面
    products = []
    for item in data.get("products", []):
        products.append({
            "product": item.get("product", ""),
            "status": item.get("status", ""),
            "elapsed_seconds": item.get("elapsed_seconds", 0),
            "error": item.get("error", ""),
        })
    status_order = {"error": 0, "skipped": 1, "ok": 2}
    products.sort(key=lambda p: status_order.get(p["status"], 3))

    return {
        "ok": True,
        "started_at": data.get("started_at", ""),
        "duration_seconds": data.get("duration_seconds", 0),
        "success_total": data.get("success_total", 0),
        "failed_total": data.get("failed_total", 0),
        "skipped_total": data.get("skipped_total", 0),
        "products": products,
    }


# --- 数据健康检查 ---

# 每产品最多检查前 N 个 CSV 文件（按 iterdir 顺序），避免大产品卡 UI
_CSV_CHECK_LIMIT = 100

# 状态目录名（工具自身写入的元数据，不参与健康检查）
_META_DIR_NAME = ".quantclass_sync"

# 健康检查支持的问题类型集合
_KNOWN_TYPES = {"missing_data", "csv_unreadable", "orphan_temp"}


def _check_missing_data(
    data_root: Path, catalog_products: Sequence[str],
) -> List[Dict[str, Any]]:
    """检查有 timestamp.txt 但目录下无数据文件的产品。"""
    issues: List[Dict[str, Any]] = []
    for product in catalog_products:
        product_dir = data_root / product
        ts_file = product_dir / TIMESTAMP_FILE_NAME
        if not ts_file.is_file():
            continue
        # 有 timestamp.txt，检查目录下是否有数据（文件或子目录）
        # 排除隐藏条目和 timestamp.txt 本身，剩余任意条目即视为有数据
        try:
            has_data = any(
                not entry.name.startswith(".")
                and entry.name != TIMESTAMP_FILE_NAME
                for entry in product_dir.iterdir()
            )
        except OSError:
            continue
        if not has_data:
            issues.append({
                "type": "missing_data",
                "product": product,
                "detail": "有 timestamp.txt 但目录下无数据文件",
                "file": "",
            })
    return issues


def _check_csv_unreadable(data_root: Path) -> List[Dict[str, Any]]:
    """检查 KNOWN_DATASETS 中 CSV 是否可正常读取（解码 + 非空）。

    轻量检查：open + 读前两行，每产品上限 _CSV_CHECK_LIMIT 个文件。
    仅检查产品目录一级下的 CSV，子目录内文件不检测。
    """
    issues: List[Dict[str, Any]] = []
    for product in KNOWN_DATASETS:
        product_dir = data_root / product
        if not product_dir.is_dir():
            continue
        try:
            csv_files = [
                f for f in product_dir.iterdir()
                if f.is_file() and f.suffix == ".csv" and not f.name.startswith(".")
            ]
        except OSError:
            continue
        for csv_path in csv_files[:_CSV_CHECK_LIMIT]:
            rel_path = f"{product}/{csv_path.name}"
            # 尝试所有候选编码
            readable = False
            for enc in ENCODING_CANDIDATES:
                try:
                    with open(csv_path, "r", encoding=enc) as fh:
                        first_line = fh.readline()
                        if first_line.strip():
                            readable = True
                            break
                except (OSError, UnicodeDecodeError, ValueError):
                    continue
            if not readable:
                issues.append({
                    "type": "csv_unreadable",
                    "product": product,
                    "detail": f"{rel_path}: 无法解码或内容为空",
                    "file": rel_path,
                })
    return issues


def _check_orphan_temp(data_root: Path) -> List[Dict[str, Any]]:
    """检查 data_root 下残留的临时文件（含 .tmp- 的文件名）。

    按产品目录逐个扫描一级文件（atomic_temp_path 生成的 tmp 文件与目标同目录）。
    同时检查 data_root 根级文件。跳过 .quantclass_sync 状态目录。
    """
    issues: List[Dict[str, Any]] = []

    # 扫描 data_root 根级文件
    try:
        for entry in data_root.iterdir():
            if entry.is_file() and ".tmp-" in entry.name:
                issues.append({
                    "type": "orphan_temp",
                    "product": "",
                    "detail": entry.name,
                    "file": entry.name,
                })
    except OSError:
        pass

    # 逐产品目录扫描一级文件
    try:
        subdirs = [
            d for d in data_root.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]
    except OSError:
        return issues

    for product_dir in subdirs:
        product = product_dir.name
        try:
            for entry in product_dir.iterdir():
                if entry.is_file() and ".tmp-" in entry.name:
                    rel = f"{product}/{entry.name}"
                    issues.append({
                        "type": "orphan_temp",
                        "product": product,
                        "detail": rel,
                        "file": rel,
                    })
        except OSError:
            continue

    return issues


def check_data_health(
    data_root: Path, catalog_products: Sequence[str],
) -> Dict[str, Any]:
    """扫描 data_root，返回数据健康报告。

    检测三类问题：文件缺失(missing_data)、CSV 不可读(csv_unreadable)、
    残留临时文件(orphan_temp)。

    data_root 不存在时返回空报告，单个子检查异常不中断其他检查。
    """
    t0 = time.monotonic()

    # data_root 不存在，返回空报告
    if not data_root.is_dir():
        return {
            "issues": [],
            "summary": {
                "missing_data": 0,
                "csv_unreadable": 0,
                "orphan_temp": 0,
                "total": 0,
            },
            "scanned_products": 0,
            "elapsed_seconds": 0.0,
        }

    # 三个子检查各自隔离，一个失败不影响其他
    issues: List[Dict[str, Any]] = []
    for check_fn, args in [
        (_check_missing_data, (data_root, catalog_products)),
        (_check_csv_unreadable, (data_root,)),
        (_check_orphan_temp, (data_root,)),
    ]:
        try:
            issues.extend(check_fn(*args))
        except Exception as exc:
            log_error(f"健康检查子任务 {check_fn.__name__} 异常：{exc}", event="HEALTH_CHECK")

    # 统计各类型计数（只统计已知类型，确保 total = 三类之和）
    summary: Dict[str, int] = {"missing_data": 0, "csv_unreadable": 0, "orphan_temp": 0}
    for issue in issues:
        issue_type = issue["type"]
        if issue_type in _KNOWN_TYPES:
            summary[issue_type] += 1
    summary["total"] = len(issues)

    elapsed = round(time.monotonic() - t0, 2)
    return {
        "issues": issues,
        "summary": summary,
        "scanned_products": len(catalog_products),
        "elapsed_seconds": elapsed,
    }
