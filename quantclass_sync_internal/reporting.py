"""职责：聚合运行结果并输出标准化报告。"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Optional, Protocol, Sequence

from .constants import (
    REASON_INVALID_EXPLICIT_PRODUCT,
    REASON_OK,
    REASON_UNKNOWN_LOCAL_PRODUCT,
)
from .http_client import _http_metrics_for_product
from .models import CommandContext, RunEvent, RunReport, SyncStats, ProductRunResult, run_report_to_dict, utc_now_iso, log_info
from .status_store import report_dir_path


class HasReasonCode(Protocol):
    reason_code: str


def build_reason_code_counts(items: Iterable[HasReasonCode]) -> Dict[str, int]:
    """按 reason_code 聚合计数，输出稳定排序 dict。"""

    counts: Dict[str, int] = defaultdict(int)
    for item in items:
        counts[item.reason_code] += 1
    return dict(sorted(counts.items()))

def write_run_report(path: Path, report: RunReport) -> None:
    """将本次运行报告写入 JSON 文件。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = run_report_to_dict(report)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

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
    stage: str = "SYNC",
    event_detail: str = "",
) -> None:
    """统一把产品结果写入 report.products 并触发 RunEvent。

    - stats 为 None 时用空 SyncStats（避免调用方每次都显式传 SyncStats()）
    - event_detail 为空时自动推导：有 error 用 error，否则用 elapsed 信息
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
    if not event_detail:
        event_detail = error if error else f"elapsed={elapsed:.2f}s"
    _append_run_event(report, product, stage, status, reason_code, event_detail)

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
            stage="DISCOVER",
            event_detail="本地目录不在 catalog",
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
            stage="PLAN",
            event_detail="显式产品不在 catalog",
        )

def resolve_report_path(ctx: CommandContext, command: str) -> Path:
    """解析报告输出路径。"""

    if ctx.report_file:
        return ctx.report_file.resolve()
    return (report_dir_path(ctx.data_root) / f"run_report_{ctx.run_id}_{command}.json").resolve()

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
    log_info("运行报告已写入。", event="RUN_SUMMARY", report_file=str(report_path))
    return 1 if has_error else 0

def _new_report(run_id: str, mode: str) -> RunReport:
    return RunReport(schema_version="3.1", run_id=run_id, started_at=utc_now_iso(), mode=mode)

