"""职责：聚合运行结果并输出标准化报告。"""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import IO, Dict, Iterable, Optional, Protocol, Sequence

from .config import atomic_temp_path
from .constants import (
    EXIT_CODE_GENERAL_FAILURE,
    EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE,
    EXIT_CODE_NO_EXECUTABLE_PRODUCTS,
    EXIT_CODE_SUCCESS,
    REASON_INVALID_EXPLICIT_PRODUCT,
    REASON_NETWORK_ERROR,
    REASON_NO_DATA_FOR_DATE,
    REASON_OK,
    REASON_UNKNOWN_LOCAL_PRODUCT,
)
from .http_client import _http_metrics_for_product
from .models import CommandContext, RunReport, SyncStats, ProductRunResult, run_report_to_dict, utc_now_iso, log_info
from .status_store import report_dir_path

try:
    import fcntl
except ImportError:  # pragma: no cover — Windows 无 fcntl
    fcntl = None  # type: ignore[assignment]


class HasReasonCode(Protocol):
    reason_code: str


NETWORK_OR_REMOTE_FAILURE_REASONS = {
    REASON_NETWORK_ERROR,
    REASON_NO_DATA_FOR_DATE,
}


def build_reason_code_counts(items: Iterable[HasReasonCode]) -> Dict[str, int]:
    """按 reason_code 聚合计数，输出稳定排序 dict。"""

    counts: Dict[str, int] = defaultdict(int)
    for item in items:
        counts[item.reason_code] += 1
    return dict(sorted(counts.items()))

def write_run_report(path: Path, report: RunReport) -> None:
    """将本次运行报告写入 JSON 文件（原子写入，避免报告写半截）。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = run_report_to_dict(report)
    with atomic_temp_path(path, tag="report") as tmp:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def _append_result(
    report: RunReport,
    *,
    product: str,
    status: str,
    strategy: str = "",
    reason_code: str = REASON_OK,
    date_time: str = "",
    mode: str = "network",
    elapsed: float = 0.0,
    stats: Optional[SyncStats] = None,
    source_path: str = "",
    error: str = "",
) -> None:
    """统一把产品结果写入 report.products。

    - stats 为 None 时用空 SyncStats（避免调用方每次都显式传 SyncStats()）
    """
    http_attempts, http_failures = _http_metrics_for_product(product)
    report.products.append(
        ProductRunResult(
            product=product,
            status=status,
            strategy=strategy,
            reason_code=reason_code,
            date_time=date_time,
            mode=mode,
            elapsed_seconds=elapsed,
            stats=stats if stats is not None else SyncStats(),
            source_path=source_path,
            error=error,
            http_attempts=http_attempts,
            http_failures=http_failures,
        )
    )

def _record_discovery_skips(report: RunReport, unknown_local: Sequence[str], invalid_explicit: Sequence[str]) -> None:
    """把“本地未知目录/无效显式产品”写入报告。"""

    for product in sorted(unknown_local):
        _append_result(
            report,
            product=product,
            status="skipped",
            strategy="skip",
            reason_code=REASON_UNKNOWN_LOCAL_PRODUCT,
            mode="discover",
            error="本地目录不在 catalog 产品清单中，已跳过。",
        )

    for product in sorted(invalid_explicit):
        _append_result(
            report,
            product=product,
            status="skipped",
            strategy="skip",
            reason_code=REASON_INVALID_EXPLICIT_PRODUCT,
            mode="explicit",
            error="显式指定产品不在 catalog 清单中，已跳过。",
        )

def resolve_report_path(ctx: CommandContext, command: str) -> Path:
    """解析报告输出路径。"""

    if ctx.report_file:
        return ctx.report_file.resolve()
    return (report_dir_path(ctx.data_root) / f"run_report_{ctx.run_id}_{command}.json").resolve()


def decide_exit_code(
    *,
    report: Optional[RunReport],
    has_error: bool,
    no_executable_products: bool = False,
) -> int:
    """统一决策进程退出码。"""

    if no_executable_products:
        return EXIT_CODE_NO_EXECUTABLE_PRODUCTS
    if not has_error:
        return EXIT_CODE_SUCCESS

    failed_reason_codes = {
        item.reason_code
        for item in report.products
        if item.status == "error" and item.reason_code
    } if report is not None else set()
    if failed_reason_codes and failed_reason_codes.issubset(NETWORK_OR_REMOTE_FAILURE_REASONS):
        return EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE
    return EXIT_CODE_GENERAL_FAILURE

PRODUCT_LAST_STATUS_FILE = "product_last_status.json"

_LOCK_FILE = "product_last_status.lock"


def _flock_exclusive(fd: IO) -> None:
    """对文件描述符加排他锁。Windows 无 fcntl 时降级为无锁（与旧版行为一致）。"""
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX)


def _scan_reports_for_backfill(log_dir: Path) -> Dict[str, Dict[str, str]]:
    """扫描历史 run_report 构建产品状态（内部函数，调用方负责加锁）。

    按文件名字典序升序扫描，后写报告覆盖先写报告，与正常累积逻辑一致。
    """
    report_files = sorted(log_dir.glob("run_report_*.json"))
    result: Dict[str, Dict[str, str]] = {}
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
    return result


def read_or_backfill_product_last_status(log_dir: Path) -> Dict[str, Dict[str, str]]:
    """读取累积状态文件，缺失或损坏时从历史报告回填。

    与 _update_product_last_status 共享同一把文件锁，避免回填与同步写入竞争。
    流程：
    1. 快路径（无锁）：文件存在且可解析，直接返回。
    2. 慢路径（加锁）：双重检查后，扫描历史报告回填并原子写入。
       即使结果为空 dict 也落盘，作为"已回填"哨兵，避免重复扫描。
    """
    status_path = log_dir / PRODUCT_LAST_STATUS_FILE

    # 快路径：文件存在且可解析，无需加锁
    if status_path.exists():
        try:
            result = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(result, dict):
                return result
        except (OSError, json.JSONDecodeError):
            pass  # 损坏，走慢路径修复

    # 慢路径：加锁回填（或修复损坏文件）
    log_dir.mkdir(parents=True, exist_ok=True)
    lock_path = log_dir / _LOCK_FILE
    with open(lock_path, "w") as lock_fd:
        _flock_exclusive(lock_fd)
        # 双重检查：等锁期间另一个进程可能已完成写入
        if status_path.exists():
            try:
                result = json.loads(status_path.read_text(encoding="utf-8"))
                if isinstance(result, dict):
                    return result
            except (OSError, json.JSONDecodeError):
                pass  # 仍然损坏，继续回填修复
        # 从历史报告回填
        result = _scan_reports_for_backfill(log_dir)
        # 原子写入（即使为空 dict，也作为哨兵标记避免重复扫描）
        with atomic_temp_path(status_path, tag="last_status") as tmp:
            tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result


def _update_product_last_status(log_dir: Path, report: RunReport) -> None:
    """累积更新每产品最后状态文件。

    每次运行后把本轮涉及的产品状态写入 product_last_status.json，
    未涉及的产品保留上次的状态。同一报告内同产品出现多次时取最后一条（catch-up 场景）。

    使用 fcntl.flock 排他锁保护读-合并-写循环，防止并发进程丢更新。
    原子写入防写半截，文件锁防丢更新，职责正交。
    """
    status_path = log_dir / PRODUCT_LAST_STATUS_FILE
    lock_path = log_dir / _LOCK_FILE
    log_dir.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lock_fd:
        # 进程级排他锁：同一时刻只有一个进程能进入读-合并-写循环
        _flock_exclusive(lock_fd)
        # 读取已有累积状态
        existing: Dict[str, Dict[str, str]] = {}
        if status_path.exists():
            try:
                existing = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        # 用本轮结果覆盖（同产品多次出现时后面的覆盖前面的）
        for item in report.products:
            existing[item.product] = {
                "status": item.status,
                "reason_code": item.reason_code,
                "error": item.error,
            }
        # 原子写入
        with atomic_temp_path(status_path, tag="last_status") as tmp:
            tmp.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")


def _finalize_and_write_report(
    report: RunReport,
    total: SyncStats,
    has_error: bool,
    t_run_start: float,
    report_path: Path,
    dry_run: bool = False,
    log_dir: Optional[Path] = None,
) -> int:
    """汇总结果并写入报告。dry_run 时只写报告，不更新累积状态文件。

    log_dir: 累积状态文件所在目录。默认 None 时使用 report_path.parent，
    当 --report-file 指向外部路径时，调用方应显式传入 report_dir_path(data_root)。
    """

    report.summary = total
    report.ended_at = utc_now_iso()
    report.duration_seconds = round(time.time() - t_run_start, 2)
    status_counts = Counter(x.status for x in report.products)
    report.success_total = status_counts.get("ok", 0)
    report.failed_total = status_counts.get("error", 0)
    report.skipped_total = status_counts.get("skipped", 0)
    report.reason_code_counts = build_reason_code_counts(report.products)
    report.sorted_checked_files = total.sorted_checked_files
    report.sorted_violation_files = total.sorted_violation_files
    report.sorted_auto_repaired_files = total.sorted_auto_repaired_files
    report.phase_plan_seconds = round(report.phase_plan_seconds, 4)
    report.phase_sync_seconds = round(report.phase_sync_seconds, 4)
    report.phase_postprocess_seconds = round(report.phase_postprocess_seconds, 4)

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
        sorted_checked=total.sorted_checked_files,
        sorted_violation=total.sorted_violation_files,
        sorted_auto_repaired=total.sorted_auto_repaired_files,
        reason_codes=len(report.reason_code_counts),
        phase_plan_seconds=report.phase_plan_seconds,
        phase_sync_seconds=report.phase_sync_seconds,
        phase_postprocess_seconds=report.phase_postprocess_seconds,
        duration_seconds=round(report.duration_seconds, 2),
    )

    write_run_report(report_path, report)
    if not dry_run:
        _update_product_last_status(log_dir or report_path.parent, report)
    log_info("运行报告已写入。", event="RUN_SUMMARY", report_file=str(report_path))
    return decide_exit_code(report=report, has_error=has_error)

def _new_report(run_id: str, mode: str) -> RunReport:
    return RunReport(schema_version="3.2", run_id=run_id, started_at=utc_now_iso(), mode=mode)
