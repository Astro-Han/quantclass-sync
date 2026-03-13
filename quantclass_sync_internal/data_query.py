"""职责：数据状态聚合查询，供 CLI status 命令和 GUI 共用。

只读取本地信息（timestamp.txt + run_report JSON），不调用 API。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

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


from .reporting import PRODUCT_LAST_STATUS_FILE as _PRODUCT_LAST_STATUS_FILE


def _backfill_from_reports(log_dir: Path, status_path: Path) -> Dict[str, Dict[str, Any]]:
    """从历史 run_report JSON 回填累积状态（一次性迁移）。

    升级后首次运行时 product_last_status.json 尚不存在，
    扫描所有历史报告按时间顺序合并，写入累积文件供后续快速读取。
    同一产品在多份报告中出现时，后写入的报告覆盖先写入的（与正常累积逻辑一致）。
    """
    report_files = sorted(log_dir.glob("run_report_*.json"))
    if not report_files:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for path in report_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for item in data.get("products", []):
            name = item.get("product", "")
            if name:
                result[name] = {
                    "status": item.get("status", ""),
                    "reason_code": item.get("reason_code", ""),
                    "error": item.get("error", ""),
                }
    # 写入累积文件，后续读取走快路径
    if result:
        try:
            status_path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except OSError:
            pass
    return result


def _load_latest_report_products(log_dir: Path) -> Dict[str, Dict[str, Any]]:
    """读取每产品累积状态文件，返回 {product_name: {status, reason_code, error}}。

    该文件由每次运行结束时的 _update_product_last_status 增量维护，
    覆盖所有历史运行中出现过的产品，不受报告数量限制。

    文件不存在时（升级过渡期），从历史 run_report 回填一次并持久化。
    """
    status_path = log_dir / _PRODUCT_LAST_STATUS_FILE
    if not status_path.exists():
        return _backfill_from_reports(log_dir, status_path)
    try:
        return json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


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
