"""职责：执行产品级文件同步（已知规则合并 / 未知规则镜像）。"""

from __future__ import annotations

import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .constants import (
    AGGREGATE_SPLIT_COLS,
    DATE_NAME_PATTERN,
    INDEX_PRODUCTS,
    KNOWN_DATASETS,
    REASON_MERGE_ERROR,
    REASON_MIRROR_FALLBACK,
    REASON_MIRROR_UNKNOWN,
    REASON_OK,
    REASON_UNKNOWN_HEADER_MERGE,
    STRATEGY_MIRROR_UNKNOWN,
    TRADING_PRODUCTS,
)
from .csv_engine import (
    _headers_equal,
    is_rows_sorted,
    read_csv_payload,
    resolve_sort_indices,
    row_sort_key,
    sync_csv_file,
    sync_payload_to_target,
    write_csv_payload,
)
from .models import (
    CsvPayload,
    DatasetRule,
    ProductPlan,
    RULES,
    SortAudit,
    SyncStats,
    log_debug,
    log_error,
    log_info,
    normalize_product_name,
    PROGRESS_EVERY,
)

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

    if src_rel_path.name in {"period_offset.csv", "period_offset.ts"}:
        if product == "period_offset":
            return Path(src_rel_path.name)
        return Path(product) / src_rel_path

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
        if src_rel_path.name == "period_offset.csv":
            return Path("period_offset.csv")
        if src_rel_path.name == "period_offset.ts":
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

def normalize_split_value(raw_value: str) -> str:
    """
    规范化拆分值，避免文件名越界或非法字符。

    例如“10年中债国债到期收益率”会原样保留；
    若值里包含 / 或 \\，会替换成下划线。
    """

    value = raw_value.strip()
    value = value.replace("/", "_").replace("\\", "_")
    return value

def _normalize_date_token(raw_value: str) -> Optional[str]:
    """
    将行内日期标准化为 YYYY-MM-DD。

    兼容：
    - 2026-02-28
    - 2026/02/28
    - 2026-02-28 00:00:00
    - 20260228
    """

    value = raw_value.strip().lstrip("\ufeff")
    if not value:
        return None

    dash_or_slash = re.match(r"^(\d{4})[-/](\d{2})[-/](\d{2})(?:$|[ T])", value)
    if dash_or_slash:
        return f"{dash_or_slash.group(1)}-{dash_or_slash.group(2)}-{dash_or_slash.group(3)}"

    compact = re.match(r"^(\d{8})(?:$|[ T])", value)
    if compact:
        d = compact.group(1)
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
    return None

def _normalize_file_date(stem: str) -> Optional[str]:
    """将日文件名（2026-02-28 / 20260228）标准化为 YYYY-MM-DD。"""

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", stem):
        return stem
    if re.fullmatch(r"\d{8}", stem):
        return f"{stem[0:4]}-{stem[4:6]}-{stem[6:8]}"
    return None

def _log_sync_progress(product: str, idx: int, total: int, stats: SyncStats, label: str = "同步进度") -> None:
    """按 PROGRESS_EVERY 间隔打印同步进度日志."""
    if idx % max(PROGRESS_EVERY, 1) == 0 or idx == total:
        log_info(f"[{product}] {label} {idx}/{total}", event="SYNC_OK",
                 created=stats.created_files, updated=stats.updated_files,
                 unchanged=stats.unchanged_files, skipped=stats.skipped_files)

def sync_daily_aggregate_file(src: Path, product: str, data_root: Path, dry_run: bool) -> Tuple[SyncStats, str]:
    """
    处理按天聚合 CSV（例如 2026-02-06.csv）。

    这一步会按产品配置的“拆分字段”切成单文件，
    再用该产品的规则做增量合并，保持现有目录结构不变。
    """

    stats = SyncStats()
    reason_code = REASON_OK
    rule = RULES[product]
    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    if not incoming.header:
        stats.skipped_files += 1
        return stats, reason_code

    split_col = AGGREGATE_SPLIT_COLS.get(product)
    if not split_col:
        stats.skipped_files += 1
        return stats, reason_code
    if split_col not in incoming.header:
        stats.skipped_files += 1
        log_info(f"[{product}] 未找到拆分字段，已跳过: {split_col}", event="SYNC_FAIL")
        return stats, REASON_MERGE_ERROR

    rows_for_split = incoming.rows
    if rule.date_filter_col:
        expected_date = _normalize_file_date(src.stem)
        if expected_date is None:
            stats.skipped_files += 1
            log_info(
                f"[{product}] 文件名不是合法日期，无法执行日期过滤，已跳过: {src.name}",
                event="SYNC_SKIP",
                filter_col=rule.date_filter_col,
            )
            return stats, reason_code
        if rule.date_filter_col not in incoming.header:
            stats.skipped_files += 1
            log_info(
                f"[{product}] 缺少日期过滤列，已跳过: {rule.date_filter_col}",
                event="SYNC_FAIL",
                file=src.name,
            )
            return stats, REASON_MERGE_ERROR

        date_idx = incoming.header.index(rule.date_filter_col)
        filtered_rows: List[List[str]] = []
        dropped_invalid_date = 0
        dropped_cross_day = 0
        for row in incoming.rows:
            raw_value = row[date_idx] if date_idx < len(row) else ""
            row_date = _normalize_date_token(raw_value)
            if row_date is None:
                dropped_invalid_date += 1
                continue
            if row_date != expected_date:
                dropped_cross_day += 1
                continue
            normalized_row = list(row)
            if date_idx < len(normalized_row):
                # 统一日期格式，避免同日不同格式（如日期/日期时间）被主键误判为不同行。
                normalized_row[date_idx] = expected_date
            filtered_rows.append(normalized_row)

        rows_for_split = filtered_rows
        if dropped_invalid_date or dropped_cross_day:
            log_info(
                f"[{product}] 已按文件日期过滤聚合行: {src.name}",
                event="SYNC_OK",
                expected_date=expected_date,
                kept_rows=len(rows_for_split),
                dropped_cross_day=dropped_cross_day,
                dropped_invalid_date=dropped_invalid_date,
            )
        if not rows_for_split:
            stats.skipped_files += 1
            log_info(
                f"[{product}] 日期过滤后无有效数据，视为异常: {src.name}",
                event="SYNC_FAIL",
                expected_date=expected_date,
            )
            return stats, REASON_MERGE_ERROR

    split_idx = incoming.header.index(split_col)
    grouped_rows: Dict[str, List[List[str]]] = {}
    for row in rows_for_split:
        split_raw = row[split_idx] if split_idx < len(row) else ""
        split_value = normalize_split_value(split_raw)
        if not split_value:
            continue
        grouped_rows.setdefault(split_value, []).append(list(row))

    if not grouped_rows:
        stats.skipped_files += 1
        return stats, reason_code

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
        result, added_rows, sort_audit = sync_payload_to_target(incoming=payload, target=target, rule=rule, dry_run=dry_run)
        apply_file_result(stats, result=result, added_rows=added_rows, sort_audit=sort_audit)

        _log_sync_progress(product, idx, total_codes, stats, label="拆分进度")

    return stats, reason_code

def sync_raw_file(src: Path, target: Path, dry_run: bool) -> str:
    """
    镜像写入（按原文件路径复制，不做字段级合并）。
    """

    existed_before = target.exists()
    if existed_before:
        try:
            if _files_equal_by_chunk(src, target):
                return "unchanged"
        except FileNotFoundError:
            # TOCTOU：比较阶段目标文件被并发删除/替换时，按“发生变化”继续覆盖写入。
            existed_before = target.exists()

    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target.parent / f".{target.name}.tmp-raw-{os.getpid()}-{time.time_ns()}"
        try:
            shutil.copy2(src, tmp_path)
            os.replace(tmp_path, target)
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

    return "updated" if existed_before else "created"

def _files_equal_by_chunk(src: Path, target: Path, chunk_size: int = 1024 * 1024) -> bool:
    """按块比较两个文件内容是否一致，避免整文件读入内存。"""

    try:
        if src.stat().st_size != target.stat().st_size:
            return False

        with src.open("rb") as src_file, target.open("rb") as target_file:
            while True:
                src_chunk = src_file.read(chunk_size)
                target_chunk = target_file.read(chunk_size)
                if src_chunk != target_chunk:
                    return False
                if not src_chunk:
                    return True
    except FileNotFoundError:
        return False

def apply_file_result(stats: SyncStats, result: str, added_rows: int = 0, sort_audit: Optional[SortAudit] = None) -> None:
    """把单文件结果累加到统计对象。"""

    if sort_audit is not None:
        stats.sorted_checked_files += sort_audit.checked_files
        stats.sorted_violation_files += sort_audit.violation_files
        stats.sorted_auto_repaired_files += sort_audit.auto_repaired_files

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
            agg, agg_reason = sync_daily_aggregate_file(src=src, product=product, data_root=data_root, dry_run=dry_run)
            stats.merge(agg)
            if agg_reason != REASON_OK:
                reason_code = agg_reason
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
        else:
            rule = infer_rule(rel_path)
            if rule is None:
                # 没命中规则时降级镜像，保持可用性优先。
                result = sync_raw_file(src=src, target=target, dry_run=dry_run)
                apply_file_result(stats, result=result)
                reason_code = REASON_MIRROR_FALLBACK
            else:
                # 命中规则时做增量合并（可减少重复写入）。
                result, added_rows, sort_audit = sync_csv_file(src=src, target=target, rule=rule, dry_run=dry_run)
                apply_file_result(stats, result=result, added_rows=added_rows, sort_audit=sort_audit)

        _log_sync_progress(product, idx, total_files, stats)

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
                    result, added_rows, sort_audit = sync_payload_to_target(
                        incoming=incoming,
                        target=target,
                        rule=auto_rule,
                        dry_run=dry_run,
                    )
                    apply_file_result(stats, result=result, added_rows=added_rows, sort_audit=sort_audit)
                    did_unknown_header_merge = True
                    log_debug(f"[{product}] 命中轻量自动合并: {src_rel_path}")
                    _log_sync_progress(product, idx, total_files, stats, label="轻量合并进度")
                    continue
            except Exception as exc:
                log_debug(f"[{product}] 轻量合并条件检查失败，改走镜像: {src_rel_path}, err={exc}")

        result = sync_raw_file(src=src, target=target, dry_run=dry_run)
        apply_file_result(stats, result=result)

        _log_sync_progress(product, idx, total_files, stats, label="镜像进度")

    if did_unknown_header_merge:
        return stats, REASON_UNKNOWN_HEADER_MERGE
    return stats, REASON_MIRROR_UNKNOWN

def write_csv_payload_atomic(path: Path, payload: CsvPayload, rule: DatasetRule, dry_run: bool) -> None:
    """原子写回 CSV，避免排序修复中途失败导致文件损坏。"""

    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.tmp-sortfix-{os.getpid()}-{time.time_ns()}"
    try:
        write_csv_payload(tmp_path, payload, rule, dry_run=False)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

def sortable_products() -> List[str]:
    """返回支持排序修复的产品列表（排除非目录型数据）。"""

    return sorted([name for name, rule in RULES.items() if rule.sort_cols and "." not in name])

def repair_sort_product_files(
    product: str,
    data_root: Path,
    dry_run: bool = False,
    strict: bool = False,
) -> Tuple[SyncStats, int]:
    """
    对单产品做全量排序修复。

    返回：
    - stats：修复统计
    - error_count：处理失败的文件数
    """

    rule = RULES.get(product)
    if rule is None:
        raise RuntimeError(f"产品 {product} 未配置排序规则，无法执行 repair_sort。")

    product_root = data_root / product
    if not product_root.exists():
        return SyncStats(), 0

    files = sorted([p for p in product_root.rglob("*") if p.is_file() and p.suffix.lower() == ".csv"])
    stats = SyncStats()
    error_count = 0

    for idx, path in enumerate(files, start=1):
        try:
            payload = read_csv_payload(path, preferred_encoding=rule.encoding)
            if not payload.header or not payload.rows:
                stats.unchanged_files += 1
                continue

            sort_indices = resolve_sort_indices(payload.header, rule)
            if not sort_indices:
                stats.skipped_files += 1
                continue

            stats.sorted_checked_files += 1
            if is_rows_sorted(payload.rows, sort_indices):
                stats.unchanged_files += 1
                continue

            stats.sorted_violation_files += 1
            stats.sorted_auto_repaired_files += 1
            sorted_rows = sorted(payload.rows, key=lambda row: row_sort_key(row, sort_indices))
            repaired_payload = CsvPayload(
                note=payload.note,
                header=list(payload.header),
                rows=sorted_rows,
                encoding=payload.encoding,
                delimiter=payload.delimiter,
            )
            write_csv_payload_atomic(path=path, payload=repaired_payload, rule=rule, dry_run=dry_run)
            stats.updated_files += 1
        except Exception as exc:
            error_count += 1
            stats.skipped_files += 1
            log_error(f"[{product}] 排序修复失败: {path} | err={exc}", event="SORT_REPAIR")
            if strict:
                raise RuntimeError(f"{product} 排序修复失败（严格模式）：{path}") from exc
        finally:
            if idx % max(PROGRESS_EVERY, 1) == 0 or idx == len(files):
                log_info(
                    f"[{product}] 排序修复进度 {idx}/{len(files)}",
                    event="SORT_REPAIR",
                    repaired=stats.updated_files,
                    unchanged=stats.unchanged_files,
                    skipped=stats.skipped_files,
                    sorted_checked=stats.sorted_checked_files,
                    sorted_violation=stats.sorted_violation_files,
                    sorted_auto_repaired=stats.sorted_auto_repaired_files,
                    errors=error_count,
                    dry_run=dry_run,
                )

    return stats, error_count

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
