"""职责：数据状态聚合查询，供 CLI status 命令和 GUI 共用。

只读取本地信息（timestamp.txt + run_report JSON），不调用 API。
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .reporting import read_or_backfill_product_last_status
from .status_store import read_local_timestamp_date, report_dir_path

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


def _load_latest_report_products(log_dir: Path) -> Dict[str, Dict[str, Any]]:
    """读取每产品累积状态文件，返回 {product_name: {status, reason_code, error}}。

    委托 reporting.read_or_backfill_product_last_status 处理：
    - 正常读取：直接解析 product_last_status.json
    - 升级过渡：文件缺失时从历史 run_report 回填并持久化
    - 损坏自愈：JSON 损坏时重新回填修复
    所有写入与 _update_product_last_status 共享同一把文件锁。
    """
    return read_or_backfill_product_last_status(log_dir)


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
    last_results = _load_latest_report_products(log_dir)

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
    # 取最近 n 个，按时间降序
    recent = report_files[-n:] if len(report_files) > n else report_files
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
    # 路径安全检查：必须在 log_dir 内
    report_path = Path(report_file).resolve()
    log_dir_resolved = log_dir.resolve()
    if not str(report_path).startswith(str(log_dir_resolved) + os.sep) and report_path != log_dir_resolved:
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
