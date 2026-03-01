"""职责：处理 CSV 编码识别、解析、去重合并和排序。"""

from __future__ import annotations

import csv
import math
import re
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .constants import ENCODING_CANDIDATES, UTF8_BOM
from .models import CsvPayload, DatasetRule, SortAudit, log_debug, log_info

def decode_text(path: Path, preferred_encoding: Optional[str]) -> Tuple[str, str]:
    """尝试多个编码读取文本，返回 (文本, 实际编码)。"""

    data = path.read_bytes()
    if data.startswith(UTF8_BOM):
        try:
            return data.decode("utf-8-sig"), "utf-8-sig"
        except Exception:
            pass

    encodings = [preferred_encoding] if preferred_encoding else []
    encodings.extend([enc for enc in ENCODING_CANDIDATES if enc != preferred_encoding])
    for encoding in encodings:
        if not encoding:
            continue
        try:
            text = data.decode(encoding)
            actual_encoding = encoding
            if encoding == "utf-8-sig":
                actual_encoding = "utf-8-sig" if data.startswith(UTF8_BOM) else "utf-8"
            return text, actual_encoding
        except Exception:
            continue
    raise RuntimeError(f"无法识别文件编码: {path}")

def choose_output_encoding(existing: Optional[CsvPayload], incoming: CsvPayload, rule: DatasetRule) -> str:
    """选择写回编码：优先保留本地已有编码，其次跟随下载源，最后回退规则默认值。"""

    if existing and existing.encoding:
        return existing.encoding
    if incoming.encoding:
        return incoming.encoding
    return rule.encoding

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
    non_empty_lines = [line for line in text.splitlines() if line.strip() != ""]
    if not non_empty_lines:
        return CsvPayload(note=None, header=[], rows=[], encoding=encoding, delimiter=",")

    delimiter = detect_delimiter(non_empty_lines[:3])
    parsed_rows: List[List[str]] = []
    for row in csv.reader(StringIO(text), delimiter=delimiter):
        if not row:
            continue
        if all(cell.strip() == "" for cell in row):
            continue
        parsed_rows.append(list(row))
    if not parsed_rows:
        return CsvPayload(note=None, header=[], rows=[], encoding=encoding, delimiter=delimiter)

    first = parsed_rows[0]
    second = parsed_rows[1] if len(parsed_rows) > 1 else []

    note: Optional[str] = None
    if looks_like_header(first):
        header = first
        data_start = 1
    elif looks_like_header(second):
        note = non_empty_lines[0].lstrip("\ufeff")
        header = second
        data_start = 2
    else:
        header = first
        data_start = 1

    rows = [list(row) for row in parsed_rows[data_start:]]
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
        numeric = float(value)
        if math.isfinite(numeric):
            return (1, numeric)
        # nan/inf 排序不稳定，统一按普通字符串处理。
        return (2, value)
    except Exception:
        return (2, value)

SORT_TIE_BREAKER_COLS: Dict[str, Tuple[str, ...]] = {
    # 财务类补充稳定排序键，避免同日多条记录顺序抖动。
    "stock-fin-data-xbx": ("stock_code", "statement_format", "抓取时间"),
    "stock-fin-pre-fore-data-xbx": ("股票代码", "预告对应财报日期", "业绩预告类型"),
}

def resolve_sort_indices(header: Sequence[str], rule: DatasetRule) -> List[int]:
    """根据规则解析可用排序列下标（含 tie-breaker）。"""

    cols: List[str] = []
    for col in rule.sort_cols:
        if col in header and col not in cols:
            cols.append(col)
    for col in SORT_TIE_BREAKER_COLS.get(rule.name, ()):
        if col in header and col not in cols:
            cols.append(col)
    return [header.index(col) for col in cols]

def row_sort_key(row: Sequence[str], sort_indices: Sequence[int]) -> Tuple[Tuple[int, object], ...]:
    """生成稳定排序键。"""

    return tuple(sortable_value(row[idx] if idx < len(row) else "") for idx in sort_indices)

def is_rows_sorted(rows: Sequence[Sequence[str]], sort_indices: Sequence[int]) -> bool:
    """判断行数据是否按指定列单调非递减。"""

    if not sort_indices:
        return True
    prev: Optional[Tuple[Tuple[int, object], ...]] = None
    for row in rows:
        current = row_sort_key(row, sort_indices)
        if prev is not None and current < prev:
            return False
        prev = current
    return True

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
                encoding=incoming.encoding or (existing.encoding if existing else rule.encoding),
                delimiter=existing.delimiter if existing else incoming.delimiter,
            ),
            0,
        )

    existing_rows: List[List[str]] = []
    if existing:
        if _headers_equal(existing.header, target_header):
            existing_rows = [list(row) for row in existing.rows]
        else:
            existing_rows = align_rows(existing.rows, existing.header, target_header)
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

    sort_indices = resolve_sort_indices(target_header, rule)
    if sort_indices:
        rows.sort(key=lambda row: row_sort_key(row, sort_indices))

    note = None
    if rule.has_note:
        note = existing.note if existing and existing.note is not None else incoming.note
        if note is None:
            note = ""

    merged = CsvPayload(
        note=note,
        header=list(target_header),
        rows=rows,
        encoding=incoming.encoding or (existing.encoding if existing else rule.encoding),
        delimiter=existing.delimiter if existing else incoming.delimiter,
    )
    return merged, max(0, len(merged_map) - before_count)

def write_csv_payload(path: Path, payload: CsvPayload, rule: DatasetRule, dry_run: bool) -> None:
    """把合并结果写回 CSV（dry-run 时跳过写盘）。"""

    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    delimiter = payload.delimiter or ","
    with path.open("w", encoding=payload.encoding or rule.encoding, newline="") as f:
        if rule.has_note and payload.note is not None:
            f.write(payload.note.rstrip("\r\n"))
            f.write("\n")
        writer = csv.writer(f, delimiter=delimiter, lineterminator="\n")
        writer.writerow(payload.header)
        writer.writerows(payload.rows)

def sync_payload_to_target(incoming: CsvPayload, target: Path, rule: DatasetRule, dry_run: bool) -> Tuple[str, int, SortAudit]:
    """
    把已解析好的 CSV 同步到目标文件。
    返回：(状态, 新增行数)
    """

    audit = SortAudit()
    if not incoming.header:
        return "skipped", 0, audit

    existing = read_csv_payload(target, preferred_encoding=incoming.encoding or rule.encoding) if target.exists() else None
    output_encoding = choose_output_encoding(existing=existing, incoming=incoming, rule=rule)
    merged, added_rows = merge_payload(existing, incoming, rule)
    merged.encoding = output_encoding

    sort_indices = resolve_sort_indices(merged.header, rule)
    if sort_indices:
        audit.checked_files = 1
        if not is_rows_sorted(merged.rows, sort_indices):
            audit.violation_files = 1
            merged.rows = sorted(merged.rows, key=lambda row: row_sort_key(row, sort_indices))
            audit.auto_repaired_files = 1
            log_info(
                f"[{rule.name}] 检测到排序异常，已自动修复: {target}",
                event="SYNC_OK",
                repaired=True,
            )

    if existing and merged.note == existing.note and merged.header == existing.header and merged.rows == existing.rows:
        if merged.encoding == existing.encoding:
            return "unchanged", 0, audit
        log_debug(
            f"[{rule.name}] 内容未变化，但检测到编码漂移，触发重写: {target}",
            from_encoding=existing.encoding,
            to_encoding=merged.encoding,
        )

    write_csv_payload(target, merged, rule, dry_run=dry_run)
    if existing:
        return "updated", added_rows, audit
    return "created", len(merged.rows), audit

def sync_csv_file(src: Path, target: Path, rule: DatasetRule, dry_run: bool) -> Tuple[str, int, SortAudit]:
    """同步单个 CSV（读取 -> 合并 -> 写回）。"""

    incoming = read_csv_payload(src, preferred_encoding=rule.encoding)
    return sync_payload_to_target(incoming=incoming, target=target, rule=rule, dry_run=dry_run)
