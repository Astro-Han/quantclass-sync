"""职责：维护 pivot 产物与原子化 pkl 写入。"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

from .constants import PIVOT_DEFRAG_THRESHOLD, PIVOT_FIELDS

def _make_market_pivot(data_dict: Dict[str, pd.DataFrame], market_type: str) -> Dict[str, pd.DataFrame]:
    """
    按字段构建透视表字典，便于策略侧批量读取。

    输出结构：
    - spot: open/close/vwap1m
    - swap: open/close/funding_rate/vwap1m
    """

    field_pairs = PIVOT_FIELDS[market_type]
    pivot_map: Dict[str, pd.DataFrame] = {}
    for out_name, source_col in field_pairs:
        series_list: List[pd.Series] = []
        for symbol, frame in data_dict.items():
            series = frame.set_index("candle_begin_time")[source_col]
            series.name = symbol
            series_list.append(series)
        if not series_list:
            pivot = pd.DataFrame()
        else:
            pivot = pd.concat(series_list, axis=1, sort=True).sort_index()
        pivot_map[out_name] = pivot
    return pivot_map

def _patch_market_pivot(
    pivot_map: Dict[str, pd.DataFrame],
    data_dict: Dict[str, pd.DataFrame],
    market_type: str,
    changed_symbols: Set[str],
    removed_symbols: Set[str],
) -> Dict[str, pd.DataFrame]:
    """按受影响 symbol 对 pivot 做局部 patch，避免每次全量 concat。"""

    if not changed_symbols and not removed_symbols:
        return pivot_map

    field_pairs = PIVOT_FIELDS[market_type]
    result: Dict[str, pd.DataFrame] = dict(pivot_map)

    # 预构建索引缓存，避免每个字段重复 set_index。
    indexed_frames: Dict[str, pd.DataFrame] = {}
    for symbol in sorted(changed_symbols):
        frame = data_dict.get(symbol)
        if frame is None or frame.empty or "candle_begin_time" not in frame.columns:
            continue
        indexed_frames[symbol] = frame.set_index("candle_begin_time")

    for out_name, source_col in field_pairs:
        pivot = result.get(out_name)
        if not isinstance(pivot, pd.DataFrame):
            pivot = pd.DataFrame()

        changed_series_list: List[pd.Series] = []
        for symbol, indexed in indexed_frames.items():
            if source_col not in indexed.columns:
                continue
            series = indexed[source_col]
            series.name = symbol
            changed_series_list.append(series)

        changed_df = pd.concat(changed_series_list, axis=1, sort=True) if changed_series_list else pd.DataFrame()
        # 兼容旧逻辑：若基线已有索引，列更新只按既有索引对齐，不做索引扩展。
        if not changed_df.empty and len(pivot.index) > 0:
            changed_df = changed_df.reindex(pivot.index)
        overlap_cols = set(changed_df.columns.tolist())
        drop_cols = set(removed_symbols) | overlap_cols

        if drop_cols:
            drop_targets = [col for col in pivot.columns if col in drop_cols]
            if drop_targets:
                pivot_base = pivot.drop(columns=drop_targets, errors="ignore")
            else:
                pivot_base = pivot
        else:
            pivot_base = pivot

        if changed_df.empty:
            pivot_new = pivot_base
            changed_col_count = 0
        else:
            pivot_new = pd.concat([pivot_base, changed_df], axis=1, sort=True)
            changed_col_count = changed_df.shape[1]

        pivot_new = pivot_new.sort_index()
        if PIVOT_DEFRAG_THRESHOLD > 0 and changed_col_count >= PIVOT_DEFRAG_THRESHOLD:
            pivot_new = pivot_new.copy()
        result[out_name] = pivot_new

    return result

def _safe_unlink(path: Path) -> None:
    """安全删除文件（不存在时忽略）。"""

    try:
        path.unlink()
    except FileNotFoundError:
        return

def _write_pickles_atomically(payloads: Dict[Path, object]) -> None:
    """
    原子写入多个 pkl 文件。

    策略：先写临时文件，再做批量替换；替换失败时回滚旧文件。
    """

    run_token = f"{os.getpid()}-{time.time_ns()}"
    temp_files: Dict[Path, Path] = {}
    backups: Dict[Path, Path] = {}
    created_targets: List[Path] = []

    try:
        for target, obj in payloads.items():
            target.parent.mkdir(parents=True, exist_ok=True)
            temp_path = target.parent / f".{target.name}.tmp-{run_token}"
            pd.to_pickle(obj, temp_path)
            temp_files[target] = temp_path

        for target in payloads:
            if target.exists():
                backup_path = target.parent / f".{target.name}.bak-{run_token}"
                os.replace(target, backup_path)
                backups[target] = backup_path
            else:
                created_targets.append(target)

        for target, temp_path in temp_files.items():
            os.replace(temp_path, target)

        for backup in backups.values():
            _safe_unlink(backup)
    except Exception:
        # 清理未消费的临时文件
        for temp_path in temp_files.values():
            _safe_unlink(temp_path)

        # 回滚：先删除本次新建文件，再恢复备份。
        for target in created_targets:
            _safe_unlink(target)
        for target, backup in backups.items():
            if backup.exists():
                _safe_unlink(target)
                os.replace(backup, target)

        raise
    finally:
        for temp_path in temp_files.values():
            _safe_unlink(temp_path)
        for backup in backups.values():
            _safe_unlink(backup)

def _validate_integrity(
    spot_dir: Path,
    swap_dir: Path,
    spot_source_files: int,
    swap_source_files: int,
    spot_symbols: int,
    swap_symbols: int,
) -> None:
    """
    严格完整性校验。

    两侧目录都要存在，且都能读取出有效 symbol。
    """

    if not spot_dir.exists() or not swap_dir.exists():
        raise RuntimeError(
            f"严格完整性检查失败：spot_dir_exists={spot_dir.exists()} swap_dir_exists={swap_dir.exists()}。"
        )

    if spot_source_files <= 0 or swap_source_files <= 0:
        raise RuntimeError(
            f"严格完整性检查失败：spot_files={spot_source_files} swap_files={swap_source_files}。"
        )

    if spot_symbols <= 0 or swap_symbols <= 0:
        raise RuntimeError(
            f"严格完整性检查失败：spot_symbols={spot_symbols} swap_symbols={swap_symbols}。"
        )

