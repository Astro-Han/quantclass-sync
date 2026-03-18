"""职责：数据状态聚合查询，供 CLI status 命令和 GUI 共用。

只读取本地信息（timestamp.txt + run_report JSON），不调用 API。
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .constants import (
    ENCODING_CANDIDATES, KNOWN_DATASETS, TIMESTAMP_FILE_NAME,
    BUSINESS_DAY_ONLY_PRODUCTS, FINANCIAL_PRODUCTS, NOTICE_PRODUCTS,
)
from .models import log_error, RULES
from .status_store import (
    normalize_data_date, read_local_timestamp_date,
    read_or_backfill_product_last_status,
    report_dir_path, status_db_path, PRODUCT_LAST_STATUS_FILE,
)

# --- CSV 日期推断 ---


def infer_local_date_from_csv(data_root: Path, product: str, rule) -> Optional[str]:
    """从 CSV 数据内容推断本地最新日期（当 timestamp.txt 缺失时使用）。

    策略：按文件名倒序取最后 20 个文件，提取日期列最大值。
    尽力推断而非精确，推断偏低时只会多回补几天，无数据损坏风险。
    无 CSV 文件或无 rule 时返回 None（真正的首次同步）。
    """
    if not rule:
        return None

    # 确定日期列：优先 date_filter_col，其次 sort_cols 第一列
    date_col = rule.date_filter_col or (rule.sort_cols[0] if rule.sort_cols else None)
    if not date_col:
        return None

    product_dir = data_root / product
    csv_files = _list_csv_files(product_dir)
    if not csv_files:
        return None

    # 按文件名倒序排，取最后 20 个（比纯随机更可靠地覆盖最新日期）
    csv_files_sorted = sorted(csv_files, key=lambda f: f.name, reverse=True)
    samples = csv_files_sorted[:20]

    max_date = None
    for f in samples:
        try:
            header, rows = _read_csv_full(f, rule)
            if not header or not rows or date_col not in header:
                continue
            idx = header.index(date_col)
            for row in rows:
                if idx < len(row) and row[idx].strip():
                    # normalize_data_date 校验格式合法性，过滤伪日期
                    d = normalize_data_date(row[idx].strip()[:10])
                    if d and (max_date is None or d > max_date):
                        max_date = d
        except Exception:
            continue
    return max_date


# --- 产品状态总览 ---

# 状态颜色阈值（自然日）
_DAYS_YELLOW = 1  # >= 1 天落后: 黄色
_DAYS_RED = 4     # >= 4 天落后: 红色
# 缓存宽限期：上次同步记录的 API 日期在此天数内视为可信，超出后降级回 today。
# 3 天覆盖普通周末和含周一假期的三天长周末。
_STALE_GRACE_DAYS = 3


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """解析日期字符串，支持 YYYY-MM-DD 和 YYYY-MM-DDTHH:MM:SS 两种格式。"""
    if not date_str:
        return None
    try:
        # 优先尝试 ISO datetime（checked_at 字段），截取日期部分
        return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
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
    api_latest_dates: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """返回所有产品的状态总览列表。

    每个产品包含:
    - name: 产品名
    - local_date: 本地数据日期 (YYYY-MM-DD 或 None)
    - days_behind: 落后天数 (int 或 None)
    - last_status: 上次同步状态 ("ok" / "error" / "skipped" / "")
    - last_error: 上次错误信息 (str)
    - status_color: 状态颜色 ("green" / "yellow" / "red" / "gray")

    api_latest_dates: 传入时用 API 实时日期作为参考，跳过缓存和宽限期逻辑。
    """
    if today is None:
        today = date.today()
    log_dir = report_dir_path(data_root)
    last_results = read_or_backfill_product_last_status(log_dir)

    overview: List[Dict[str, Any]] = []
    for product in catalog_products:
        local_date = read_local_timestamp_date(data_root, product)
        last = last_results.get(product, {})
        last_status = last.get("status", "")

        # 优先用传入的 API 实时日期（检查更新按钮场景）
        api_date = _parse_date((api_latest_dates or {}).get(product, ""))
        if api_date is not None:
            ref_date = api_date
        else:
            # 用缓存的 API 日期作为参考，避免周末/假日误报落后；
            # 缓存超过宽限期或无缓存时降级回 today，提示可能有新数据。
            # 宽限期从"上次查询/同步时间"算起（checked_at 优先，降级到 date_time）
            cached_api_date = _parse_date(last.get("date_time", ""))
            freshness_anchor = _parse_date(last.get("checked_at", "")) or cached_api_date
            cache_fresh = (
                cached_api_date is not None
                and freshness_anchor is not None
                and (today - freshness_anchor).days <= _STALE_GRACE_DAYS
            )
            ref_date = cached_api_date if cache_fresh else today

        behind = _days_behind(local_date, ref_date)
        # last_status=error 时 _status_color 强制返回 red，不被 api_latest_dates 覆盖（符合预期）
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
        if isinstance(item, dict) and item.get("status") == "error"
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
        # 阶段耗时：前端用于展示"探测 Xs + 同步 Xs"
        "phase_plan_seconds": data.get("phase_plan_seconds"),
        "phase_sync_seconds": data.get("phase_sync_seconds"),
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
        # 从 products 列表中提取失败产品名（status == "error"）
        products_list = data.get("products", [])
        failed_products = [
            p.get("product", "") for p in products_list
            if isinstance(p, dict) and p.get("status") == "error"
        ]
        history.append({
            "run_id": data.get("run_id", ""),
            "started_at": data.get("started_at", ""),
            "duration_seconds": data.get("duration_seconds", 0),
            "success_total": data.get("success_total", 0),
            "failed_total": data.get("failed_total", 0),
            "skipped_total": data.get("skipped_total", 0),
            "failed_products": failed_products,
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
        if not isinstance(item, dict):
            continue
        # files_count = 新建文件数 + 更新文件数（从 stats 子对象读取，缺省为 0）
        # 防御性处理：stats 可能为 None 或非 dict
        stats = item.get("stats") or {}
        if not isinstance(stats, dict):
            stats = {}
        files_count = stats.get("created_files", 0) + stats.get("updated_files", 0)
        products.append({
            "product": item.get("product", ""),
            "status": item.get("status", ""),
            "elapsed_seconds": item.get("elapsed_seconds", 0),
            "error": item.get("error", ""),
            "files_count": files_count,
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


def _issue(type_, severity, category, product, detail, file, repairable, repair_action):
    """构造标准 issue 字典。"""
    return {
        "type": type_, "severity": severity, "category": category,
        "product": product, "detail": detail, "file": file,
        "repairable": repairable, "repair_action": repair_action,
    }


def _build_summary(issues, scanned_products, scanned_files, elapsed):
    """构建检查结果摘要。"""
    by_severity = {"error": 0, "warning": 0}
    by_repair = {"auto_repairable": 0, "needs_resync": 0, "needs_investigation": 0}
    for i in issues:
        by_severity[i["severity"]] = by_severity.get(i["severity"], 0) + 1
        if i["repairable"]:
            by_repair["auto_repairable"] += 1
        elif i["repair_action"] == "needs_resync":
            by_repair["needs_resync"] += 1
        else:
            by_repair["needs_investigation"] += 1
    return {
        "total": len(issues), "by_severity": by_severity, "by_repair": by_repair,
        "scanned_products": scanned_products, "scanned_files": scanned_files,
        "elapsed_seconds": round(elapsed, 1),
    }


def _list_data_files(product_dir):
    """递归列出数据文件，排除 timestamp.txt/.tmp-/隐藏文件。"""
    result = []
    if not product_dir.exists():
        return result
    for f in product_dir.rglob("*"):
        if f.is_file() and not f.name.startswith(".") and f.name != TIMESTAMP_FILE_NAME:
            result.append(f)
    return result


def _list_csv_files(product_dir):
    """递归列出 .csv 文件。"""
    if not product_dir.exists():
        return []
    return [f for f in product_dir.rglob("*.csv") if not f.name.startswith(".")]


def _list_temp_files(product_dir):
    """递归列出 .tmp- 前缀文件/目录（用 rglob 覆盖子目录中的残留临时文件）。"""
    if not product_dir.exists():
        return []
    return [f for f in product_dir.rglob("*") if f.name.startswith(".tmp-")]


def _looks_like_note(line):
    """判断首行是否是备注行（非数据行）。"""
    note_keywords = ["温馨提示", "数据", "微信", "仅供", "请勿"]
    return any(kw in line for kw in note_keywords)


def _read_csv_head_tail(csv_path):
    """读取 CSV 首行（表头）和最后一个非空行。返回 (header_fields, last_fields) 或 (None, None)。

    使用 csv.reader 解析，正确处理含逗号的带引号字段。
    """
    import csv
    import io
    from .csv_engine import decode_text
    text, _ = decode_text(csv_path, preferred_encoding=None)
    if not text or not text.strip():
        return None, None
    lines = text.strip().split("\n")
    # 跳过备注行（如果有）
    start = 0
    if len(lines) > 1 and _looks_like_note(lines[0]):
        start = 1
    if start >= len(lines):
        return None, None
    # 用 csv.reader 解析表头行，正确处理带引号字段
    header = next(csv.reader(io.StringIO(lines[start])))
    if len(lines) <= start + 1:
        return header, None  # 只有表头，无数据行
    # 用 csv.reader 解析末行，正确处理带引号字段
    last = next(csv.reader(io.StringIO(lines[-1])))
    return header, last


def _read_csv_full(csv_path, rule):
    """读取完整 CSV，返回 (header_list, rows_list_of_lists) 或 (None, None)。

    使用 csv.reader 解析，正确处理含逗号的带引号字段。
    """
    import csv
    import io
    from .csv_engine import decode_text
    text, _ = decode_text(csv_path, preferred_encoding=rule.encoding if rule else None)
    if not text or not text.strip():
        return None, None
    lines = text.strip().split("\n")
    # 跳过备注行（有备注且行数大于 1）
    start = 1 if rule and rule.has_note and len(lines) > 1 else 0
    if start >= len(lines):
        return None, None
    # 用 csv.reader 解析表头行，正确处理带引号字段
    header = next(csv.reader(io.StringIO(lines[start])))
    rows = []
    for line in lines[start + 1:]:
        if line.strip():
            rows.append(next(csv.reader(io.StringIO(line))))
    return header, rows


def _check_content_integrity(product, product_dir, rule):
    """检查 #5 重复行, #6 关键字段空值。仅 KNOWN_DATASETS 中有 key_cols 的产品。"""
    issues = []
    csv_files = _list_csv_files(product_dir)
    key_cols = rule.key_cols
    # 合并 key_cols + sort_cols 去重保序，用于空值检查
    check_cols = list(dict.fromkeys(list(key_cols) + list(rule.sort_cols)))

    for csv_file in csv_files:
        try:
            header, rows = _read_csv_full(csv_file, rule)
            if header is None or not rows:
                continue
            rel_path = csv_file.relative_to(product_dir).as_posix()

            # #5 重复行：按 key_cols 构造主键，统计重复数量
            key_indices = [header.index(c) for c in key_cols if c in header]
            if key_indices:
                seen = set()
                dup_count = 0
                for row in rows:
                    key = tuple(row[i] for i in key_indices if i < len(row))
                    if key in seen:
                        dup_count += 1
                    else:
                        seen.add(key)
                if dup_count > 0:
                    issues.append(_issue("duplicate_rows", "warning", "content_integrity",
                                         product, f"发现 {dup_count} 行主键重复",
                                         rel_path, True, "dedup_rows"))

            # #6 关键字段空值：检查 key_cols + sort_cols 中的空值
            check_indices = [header.index(c) for c in check_cols if c in header]
            null_count = 0
            null_cols = set()
            for row in rows:
                for ci in check_indices:
                    if ci < len(row) and (not row[ci] or row[ci].strip() == ""):
                        null_count += 1
                        null_cols.add(header[ci])
            if null_count > 0:
                cols_str = "/".join(sorted(null_cols))
                issues.append(_issue("null_key_fields", "warning", "content_integrity",
                                     product, f"{cols_str} 有 {null_count} 个空值",
                                     rel_path, False, "needs_investigation"))
        except Exception:
            continue
    return issues


def _check_file_integrity(data_root, product, product_dir, rule):
    """检查 #1 缺失数据, #2 残留临时文件, #3 CSV 完整性。返回 (file_count, issues)。"""
    issues = []
    file_count = 0

    if not product_dir.exists():
        return 0, issues

    # #1 缺失数据：有 timestamp 但无数据文件
    ts = read_local_timestamp_date(data_root, product)
    data_files = _list_data_files(product_dir)
    file_count = len(data_files)
    if ts and not data_files:
        issues.append(_issue("missing_data", "error", "file_integrity",
                             product, "有 timestamp.txt 但无数据文件", "",
                             False, "needs_resync"))

    # #2 残留临时文件
    for f in _list_temp_files(product_dir):
        issues.append(_issue("orphan_temp", "warning", "file_integrity",
                             product, f"残留临时文件: {f.name}", f.name,
                             True, "delete_temp"))

    # #3 CSV 完整性（首行可读 + 尾部残行检查）
    csv_files = _list_csv_files(product_dir)
    for csv_file in csv_files:
        try:
            header, last_line = _read_csv_head_tail(csv_file)
            if header is None:
                issues.append(_issue("csv_unreadable", "error", "file_integrity",
                                     product, "文件无法解码或为空",
                                     csv_file.relative_to(product_dir).as_posix(),
                                     False, "needs_resync"))
            elif last_line is not None and len(last_line) != len(header):
                issues.append(_issue("tail_corruption", "error", "file_integrity",
                                     product,
                                     f"末尾行不完整（期望{len(header)}列，实际{len(last_line)}列）",
                                     csv_file.relative_to(product_dir).as_posix(),
                                     True, "truncate_tail"))
        except Exception:
            issues.append(_issue("csv_unreadable", "error", "file_integrity",
                                 product, "文件读取异常",
                                 csv_file.relative_to(product_dir).as_posix(),
                                 False, "needs_resync"))

    return file_count, issues


def _check_infrastructure(data_root):
    """检查 #4 FuelBinStat.db 和 product_last_status.json 完整性。"""
    issues = []

    # 检查 SQLite 状态数据库
    db_path = status_db_path(data_root)
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(str(db_path))
            conn.execute("SELECT 1 FROM product_status LIMIT 1")
            conn.close()
        except Exception as e:
            issues.append(_issue("infra_db_corrupt", "error", "file_integrity",
                                 "(global)", f"状态数据库损坏: {e}", db_path.name,
                                 True, "rebuild_status_db"))

    # 检查产品状态 JSON 文件
    rdir = report_dir_path(data_root)
    json_path = rdir / PRODUCT_LAST_STATUS_FILE
    if json_path.exists():
        try:
            import json
            json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            issues.append(_issue("infra_json_corrupt", "error", "file_integrity",
                                 "(global)", f"产品状态 JSON 损坏: {e}", json_path.name,
                                 False, "needs_investigation"))
    return issues


def _load_trading_calendar(data_root):
    """从 data_root/period_offset.csv 加载 A 股交易日历。返回 set[str] 或 None。"""
    po_path = data_root / "period_offset.csv"
    if not po_path.exists():
        return None
    try:
        from .csv_engine import decode_text
        text, _ = decode_text(po_path, preferred_encoding=None)
        if not text:
            return None
        dates = set()
        for line in text.strip().split("\n"):
            if not line.strip():
                continue
            first_col = line.split(",")[0].strip()
            # 跳过备注行和表头：只要 YYYY-MM-DD 格式
            if len(first_col) == 10 and first_col[4] == "-" and first_col[7] == "-":
                dates.add(first_col)
        return dates if dates else None
    except Exception:
        return None


def _generate_calendar_days(start_date, end_date):
    """生成 start 到 end 之间的所有自然日（含首尾）。"""
    from datetime import datetime, timedelta
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = set()
    current = start
    while current <= end:
        dates.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _generate_weekdays(start_date, end_date):
    """生成 start 到 end 之间的所有工作日（周一到周五，近似，节假日可能误报）。"""
    from datetime import datetime, timedelta
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = set()
    current = start
    while current <= end:
        if current.weekday() < 5:  # 0=周一, 4=周五
            dates.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def _sample_max_date(csv_files, rule, date_col, sample_size=20):
    """从抽样文件中提取最大日期值（YYYY-MM-DD 格式）。"""
    import random
    samples = random.sample(csv_files, min(sample_size, len(csv_files)))
    max_date = None
    for f in samples:
        try:
            header, rows = _read_csv_full(f, rule)
            if not header or not rows or date_col not in header:
                continue
            idx = header.index(date_col)
            for row in rows:
                if idx < len(row) and row[idx].strip():
                    d = row[idx].strip()[:10]
                    if len(d) == 10 and d[4] == "-":
                        if max_date is None or d > max_date:
                            max_date = d
        except Exception:
            continue
    return max_date


def _sample_min_date(csv_files, rule, date_col, sample_size=20):
    """从抽样文件中提取最小日期值（YYYY-MM-DD 格式），用于确定连续性检查的起点。"""
    import random
    samples = random.sample(csv_files, min(sample_size, len(csv_files)))
    min_date = None
    for f in samples:
        try:
            header, rows = _read_csv_full(f, rule)
            if not header or not rows or date_col not in header:
                continue
            idx = header.index(date_col)
            for row in rows:
                if idx < len(row) and row[idx].strip():
                    d = row[idx].strip()[:10]
                    if len(d) == 10 and d[4] == "-":
                        if min_date is None or d < min_date:
                            min_date = d
        except Exception:
            continue
    return min_date


def _extract_actual_dates(csv_files, rule, date_col):
    """从 CSV 文件中提取实际存在的日期集合。大产品（>100 文件）抽样以控制性能。"""
    import random
    # 超过 100 个文件时抽样，避免对大产品全量读取导致检查超时
    if len(csv_files) > 100:
        samples = random.sample(csv_files, 100)
    else:
        samples = csv_files
    dates = set()
    for f in samples:
        try:
            header, rows = _read_csv_full(f, rule)
            if not header or not rows or date_col not in header:
                continue
            idx = header.index(date_col)
            for row in rows:
                if idx < len(row) and row[idx].strip():
                    d = row[idx].strip()[:10]
                    if len(d) == 10 and d[4] == "-":
                        dates.add(d)
        except Exception:
            continue
    return dates


def _check_temporal_integrity(data_root, product, rule, trading_calendar):
    """检查 #7 timestamp-数据日期一致性, #8 日期连续性。"""
    issues = []
    if not rule:
        return issues

    # 跳过财务/公告类产品（不适用日期连续性检查）
    if product in FINANCIAL_PRODUCTS or product in NOTICE_PRODUCTS:
        return issues

    ts_date = read_local_timestamp_date(data_root, product)
    inferred_mode = False
    if not ts_date:
        # 无 timestamp 时从 CSV 推断，启用推断模式
        inferred = infer_local_date_from_csv(data_root, product, rule)
        if not inferred:
            return issues  # 真正无数据，跳过
        ts_date = inferred
        inferred_mode = True

    # 确定日期列：优先 date_filter_col，其次 sort_cols 第一列
    date_col = rule.date_filter_col or (rule.sort_cols[0] if rule.sort_cols else None)
    if not date_col:
        return issues

    product_dir = data_root / product
    csv_files = _list_csv_files(product_dir)
    if not csv_files:
        return issues

    # #7 timestamp-数据日期一致性：推断模式跳过（endpoint 和 max_date 来自同一数据源，比较无意义）
    max_date = _sample_max_date(csv_files, rule, date_col, sample_size=20)
    if max_date and not inferred_mode:
        if max_date > ts_date:
            issues.append(_issue("date_exceeds_timestamp", "error", "temporal_integrity",
                                 product, f"CSV 最大日期 {max_date} > timestamp {ts_date}",
                                 "", False, "needs_investigation"))
        else:
            ts_dt = datetime.strptime(ts_date, "%Y-%m-%d")
            max_dt = datetime.strptime(max_date, "%Y-%m-%d")
            gap_days = (ts_dt - max_dt).days
            if gap_days > 5:
                issues.append(_issue("timestamp_data_gap", "warning", "temporal_integrity",
                                     product,
                                     f"timestamp {ts_date} 远超数据最大日期 {max_date}（差 {gap_days} 天）",
                                     "", False, "needs_resync"))

    # #8 日期连续性：用 min_date 作为起点检查期间是否有缺失日期
    # 推断模式下用 max_date（_sample_max_date 随机抽样）作为终点，
    # 避免用 inferred（文件名倒序抽样）当终点时 end > max_date 产生虚假缺口
    if not max_date:
        return issues
    end_date = max_date if inferred_mode else ts_date

    # 抽样最小日期，作为连续性检查的起始点
    min_date = _sample_min_date(csv_files, rule, date_col, sample_size=20)
    if not min_date:
        return issues

    is_crypto = product.startswith("coin-")
    expected = None
    if is_crypto:
        # 加密货币全天候交易，期望每日都有数据
        expected = _generate_calendar_days(min_date, end_date)
    elif product in BUSINESS_DAY_ONLY_PRODUCTS and trading_calendar:
        # A 股交易日产品，用精确交易日历
        expected = {d for d in trading_calendar if min_date <= d <= end_date}
    elif product in BUSINESS_DAY_ONLY_PRODUCTS:
        # 无交易日历时降级为工作日近似（节假日可能误报）
        expected = _generate_weekdays(min_date, end_date)

    if expected is None:
        return issues

    # 从 CSV 文件内容提取实际日期集合，对比期望集合找出缺失
    actual_dates = _extract_actual_dates(csv_files, rule, date_col)
    missing = sorted(expected - actual_dates)
    # 缺失超过 30 天时不报告（可能是数据本身不连续，如分钟线等，避免大量误报）
    if missing and len(missing) <= 30:
        dates_str = ", ".join(missing[:5])
        if len(missing) > 5:
            dates_str += f" 等共 {len(missing)} 天"
        detail = f"缺失日期: {dates_str}"
        if len(csv_files) > 100:
            detail += "（基于抽样检测，可能有偏差）"
        if product in BUSINESS_DAY_ONLY_PRODUCTS and not trading_calendar:
            detail += "（近似检测，节假日可能误报）"
        issues.append(_issue("missing_trading_days", "warning", "temporal_integrity",
                             product, detail, "", False, "needs_resync"))

    return issues


def _load_health_baseline(data_root):
    """从 health_baseline.json 加载上次文件数基线。返回 dict 或 None。"""
    rdir = report_dir_path(data_root)
    baseline_path = rdir / "health_baseline.json"
    if not baseline_path.exists():
        return None
    try:
        return json.loads(baseline_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_health_baseline(data_root, product_file_counts, issues):
    """保存新基线到 health_baseline.json。有覆盖完整性告警时不更新基线。"""
    # 有 coverage_integrity 告警说明本次数据可能异常，不应覆盖基线
    has_coverage_warning = any(i.get("category") == "coverage_integrity" for i in issues)
    if has_coverage_warning:
        return
    rdir = report_dir_path(data_root)
    baseline_path = rdir / "health_baseline.json"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps(product_file_counts, ensure_ascii=False), encoding="utf-8")


def _check_coverage_integrity(product, product_dir, file_count, baseline):
    """检查 #9 标的数量稳定性：文件数相比基线下降超过 20% 时告警。"""
    issues = []
    if not baseline or product not in baseline:
        return issues
    prev_count = baseline.get(product, 0)
    if prev_count > 0 and file_count < prev_count * 0.8:
        drop_pct = round((1 - file_count / prev_count) * 100)
        issues.append(_issue("file_count_drop", "warning", "coverage_integrity",
                             product,
                             f"文件数从 {prev_count} 降至 {file_count}（-{drop_pct}%）",
                             "", False, "needs_investigation"))
    return issues


def _check_format_integrity(product, product_dir):
    """检查 #10 列名一致性：同产品内抽样比对，发现不同列名组合时告警。"""
    issues = []
    csv_files = _list_csv_files(product_dir)
    if len(csv_files) < 2:
        return issues
    import random
    samples = random.sample(csv_files, min(20, len(csv_files)))
    column_sets = []
    for f in samples:
        try:
            header, _ = _read_csv_head_tail(f)
            if header:
                column_sets.append(frozenset(header))
        except Exception:
            continue
    if len(set(column_sets)) > 1:
        unique_count = len(set(column_sets))
        issues.append(_issue("column_inconsistency", "warning", "format_integrity",
                             product,
                             f"抽样 {len(samples)} 个文件发现 {unique_count} 种不同列名组合",
                             "", False, "needs_investigation"))
    return issues


def repair_data_issues(
    data_root: Path,
    issues: List[Dict],
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]:
    """修复可自动修复的 issues。返回 {repaired: [...], failed: [...]}"""
    # 只处理标记为可修复的 issue
    repairable = [i for i in issues if i.get("repairable")]
    repaired = []
    failed = []

    for idx, issue in enumerate(repairable):
        if progress_callback:
            progress_callback(idx, len(repairable), issue["product"], "repairing")
        try:
            action = issue["repair_action"]
            if action == "truncate_tail":
                _repair_truncate_tail(data_root, issue)
            elif action == "delete_temp":
                _repair_delete_temp(data_root, issue)
            elif action == "dedup_rows":
                _repair_dedup_rows(data_root, issue)
            elif action == "rebuild_status_db":
                _repair_rebuild_status_db(data_root)
            else:
                failed.append({**issue, "error": f"未知修复动作: {action}"})
                continue
            repaired.append(issue)
        except Exception as e:
            failed.append({**issue, "error": str(e)})

    return {"repaired": repaired, "failed": failed}


def _repair_truncate_tail(data_root: Path, issue: Dict) -> None:
    """截断 CSV 最后一个不完整行（末尾行列数与表头不符时执行）。

    用 csv.reader 统计字段数，正确处理含逗号的带引号字段。
    """
    import csv
    import io
    from .csv_engine import decode_text
    product = issue["product"]
    csv_path = data_root / product / issue["file"]
    rule = RULES.get(product)
    encoding = rule.encoding if rule else None
    text, detected_enc = decode_text(csv_path, preferred_encoding=encoding)
    if not text:
        return
    lines = text.split("\n")
    # 移除尾部空行
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return
    # 用 csv.reader 统计末行字段数，确认是不完整行后才截断
    last_fields = next(csv.reader(io.StringIO(lines[-1])))
    # 找到表头以比较列数（跳过可能的备注行）
    header_idx = 0
    if len(lines) > 1 and _looks_like_note(lines[0]):
        header_idx = 1
    if header_idx < len(lines):
        header_fields = next(csv.reader(io.StringIO(lines[header_idx])))
        if len(last_fields) != len(header_fields):
            lines.pop()  # 末行列数不符，移除残行
    csv_path.write_text("\n".join(lines) + "\n", encoding=detected_enc or "utf-8")


def _repair_delete_temp(data_root: Path, issue: Dict) -> None:
    """删除残留临时文件或目录。"""
    import shutil
    product = issue["product"]
    # (root) 表示 data_root 根级临时文件
    if product == "(root)":
        target = data_root / issue["file"]
    else:
        target = data_root / product / issue["file"]
    if target.is_dir():
        shutil.rmtree(target)
    elif target.exists():
        target.unlink()


def _repair_dedup_rows(data_root: Path, issue: Dict) -> None:
    """按 key_cols 去重，保留最后出现的行，重写文件。"""
    product = issue["product"]
    csv_path = data_root / product / issue["file"]
    rule = RULES.get(product)
    if not rule:
        raise ValueError(f"产品 {product} 无 RULES 定义，无法去重")
    header, rows = _read_csv_full(csv_path, rule)
    if not header or not rows:
        return
    # 构造 key_cols 在 header 中的索引
    key_indices = [header.index(c) for c in rule.key_cols if c in header]
    if not key_indices:
        return
    # 后出现的行覆盖先出现的（保留最后一条）
    seen: Dict[tuple, int] = {}
    for i, row in enumerate(rows):
        key = tuple(row[j] for j in key_indices if j < len(row))
        seen[key] = i
    unique_rows = [rows[i] for i in sorted(seen.values())]
    # 重写文件：有备注行时保留首行备注，用 csv.writer 正确处理含逗号字段
    import io as _io
    import csv as _csv
    output_parts = []
    if rule.has_note:
        from .csv_engine import decode_text
        text, _ = decode_text(csv_path, preferred_encoding=rule.encoding)
        first_line = text.split("\n")[0] if text else ""
        output_parts.append(first_line + "\n")
    # 写表头和数据行
    buf = _io.StringIO()
    writer = _csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(unique_rows)
    output_parts.append(buf.getvalue())
    csv_path.write_text("".join(output_parts), encoding=rule.encoding)


def _repair_rebuild_status_db(data_root: Path) -> None:
    """从各产品 timestamp.txt 重建 FuelBinStat.db 基础记录。

    先删除损坏的数据库文件，再重新创建，避免在损坏文件上重连失败。
    """
    from .status_store import connect_status_db, upsert_product_status
    from .models import ProductStatus
    db_path = status_db_path(data_root)
    if db_path.exists():
        db_path.unlink()  # 删除损坏的数据库文件
    conn = connect_status_db(data_root)
    try:
        for product in KNOWN_DATASETS:
            ts = read_local_timestamp_date(data_root, product)
            if ts:
                # 用 timestamp 日期作为 data_time 填充最小信息，其余字段保持默认
                status = ProductStatus(name=product, data_time=ts)
                upsert_product_status(conn, status)
    finally:
        conn.close()


def check_data_health(
    data_root: Path,
    catalog_products: Sequence[str],
    progress_callback: Optional[Callable] = None,
) -> Dict[str, Any]:
    """数据质量全面检查。progress_callback(current, total, product, phase) 每产品调用。"""
    start = time.time()
    issues: List[Dict[str, Any]] = []
    scanned_files = 0
    total_products = len(catalog_products)
    # 记录每产品文件数，用于覆盖完整性基线保存
    product_file_counts: Dict[str, int] = {}

    # 加载 A 股交易日历（仅一次，供所有产品的时间完整性检查使用）
    trading_calendar = _load_trading_calendar(data_root)
    # 加载覆盖完整性基线（仅一次）
    baseline = _load_health_baseline(data_root)

    for idx, product in enumerate(catalog_products):
        if progress_callback:
            progress_callback(idx, total_products, product, "checking")
        product_dir = data_root / product
        rule = RULES.get(product)

        # 文件完整性检查 (#1-3)
        file_count, fi_issues = _check_file_integrity(data_root, product, product_dir, rule)
        issues.extend(fi_issues)
        scanned_files += file_count
        product_file_counts[product] = file_count

        # 内容完整性（仅 KNOWN_DATASETS 中有 key_cols 的产品）
        if rule and rule.key_cols:
            ci_issues = _check_content_integrity(product, product_dir, rule)
            issues.extend(ci_issues)

        # 时间完整性 (#7-8)
        ti_issues = _check_temporal_integrity(data_root, product, rule, trading_calendar)
        issues.extend(ti_issues)

        # 覆盖完整性 (#9)
        cov_issues = _check_coverage_integrity(product, product_dir, file_count, baseline)
        issues.extend(cov_issues)

        # 格式完整性 (#10)
        fmt_issues = _check_format_integrity(product, product_dir)
        issues.extend(fmt_issues)

    # 基础设施检查 (#4, 全局一次性)
    infra_issues = _check_infrastructure(data_root)
    issues.extend(infra_issues)

    # 残留临时文件（data_root 根级）
    if data_root.exists():
        for f in data_root.iterdir():
            if f.name.startswith(".tmp-"):
                issues.append(_issue("orphan_temp", "warning", "file_integrity",
                                     "(root)", f"残留临时文件: {f.name}", f.name,
                                     True, "delete_temp"))

    # 保存覆盖完整性基线（有 coverage_integrity 告警时不更新）
    _save_health_baseline(data_root, product_file_counts, issues)

    summary = _build_summary(issues, total_products, scanned_files, time.time() - start)
    return {"issues": issues, "summary": summary}
