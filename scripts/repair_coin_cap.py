#!/usr/bin/env python3
"""一次性修复 coin-cap 历史文件：去重排序 + 清理日期命名遗留文件。"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from quantclass_sync_internal.constants import DATE_NAME_PATTERN, DEFAULT_DATA_ROOT
from quantclass_sync_internal.csv_engine import merge_payload, read_csv_payload, write_csv_payload
from quantclass_sync_internal.models import CsvPayload, RULES

PRODUCT = "coin-cap"


@dataclass
class RepairStats:
    scanned_symbol_files: int = 0
    rewrite_candidates: int = 0
    date_delete_candidates: int = 0
    rewritten_symbol_files: int = 0
    removed_date_files: int = 0
    backup_files: int = 0
    skipped_empty: int = 0
    failed_files: int = 0


def _is_date_named_csv(path: Path) -> bool:
    return path.suffix.lower() == ".csv" and bool(DATE_NAME_PATTERN.fullmatch(path.stem))


def _collect_coin_cap_files(product_root: Path) -> Tuple[List[Path], List[Path]]:
    symbol_files: List[Path] = []
    date_files: List[Path] = []
    for path in sorted(product_root.glob("*.csv")):
        if _is_date_named_csv(path):
            date_files.append(path)
        else:
            symbol_files.append(path)
    return symbol_files, date_files


def _build_repaired_payload(path: Path) -> Tuple[CsvPayload, CsvPayload, bool]:
    rule = RULES[PRODUCT]
    original = read_csv_payload(path, preferred_encoding=rule.encoding)
    if not original.header or not original.rows:
        return original, original, False
    repaired, _added = merge_payload(existing=None, incoming=original, rule=rule)
    changed = (
        original.note != repaired.note
        or original.header != repaired.header
        or original.rows != repaired.rows
        or original.encoding != repaired.encoding
    )
    return original, repaired, changed


def _backup_files(paths: List[Path], backup_root: Path) -> int:
    if not paths:
        return 0
    backup_root.mkdir(parents=True, exist_ok=True)
    copied = 0
    for src in paths:
        dst = backup_root / src.name
        shutil.copy2(src, dst)
        copied += 1
    return copied


def repair_coin_cap(data_root: Path, dry_run: bool, backup: bool) -> RepairStats:
    stats = RepairStats()
    product_root = data_root / PRODUCT
    if not product_root.exists() or not product_root.is_dir():
        raise RuntimeError(f"目录不存在：{product_root}")

    symbol_files, date_files = _collect_coin_cap_files(product_root)
    stats.scanned_symbol_files = len(symbol_files)
    stats.date_delete_candidates = len(date_files)

    rewritten_payloads: Dict[Path, CsvPayload] = {}
    for path in symbol_files:
        try:
            original, repaired, changed = _build_repaired_payload(path)
        except Exception as exc:
            stats.failed_files += 1
            print(f"[WARN] 修复失败，已跳过: {path.name} ({exc})")
            continue
        if not original.header or not original.rows:
            stats.skipped_empty += 1
            continue
        if changed:
            rewritten_payloads[path] = repaired

    stats.rewrite_candidates = len(rewritten_payloads)

    if not dry_run and backup:
        backup_root = data_root / f"{PRODUCT}.backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        backup_targets = sorted(set(rewritten_payloads.keys()) | set(date_files))
        stats.backup_files = _backup_files(backup_targets, backup_root)

    if not dry_run:
        for path, payload in rewritten_payloads.items():
            write_csv_payload(path, payload, RULES[PRODUCT], dry_run=False)
            stats.rewritten_symbol_files += 1

        for path in date_files:
            path.unlink(missing_ok=True)
            stats.removed_date_files += 1

    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="修复 coin-cap 历史文件：按主键去重排序，清理日期命名遗留文件。",
    )
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help=f"数据根目录（默认：{DEFAULT_DATA_ROOT}）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只输出修复计划，不实际写入。")
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="关闭自动备份（默认开启，且仅在非 dry-run 生效）。",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    data_root = Path(args.data_root).expanduser().resolve()
    backup = not args.no_backup

    stats = repair_coin_cap(data_root=data_root, dry_run=args.dry_run, backup=backup)

    print(f"target={data_root / PRODUCT}")
    print(f"dry_run={args.dry_run} backup={backup}")
    print(f"scanned_symbol_files={stats.scanned_symbol_files}")
    print(f"rewrite_candidates={stats.rewrite_candidates}")
    print(f"date_delete_candidates={stats.date_delete_candidates}")
    print(f"rewritten_symbol_files={stats.rewritten_symbol_files}")
    print(f"removed_date_files={stats.removed_date_files}")
    print(f"backup_files={stats.backup_files}")
    print(f"skipped_empty={stats.skipped_empty}")
    print(f"failed_files={stats.failed_files}")

    if stats.failed_files > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
