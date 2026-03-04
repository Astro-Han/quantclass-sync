"""职责：维护 pivot 产物与原子化 pkl 写入。"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Dict, IO, List, Set

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)

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

    # 预构建索引缓存，避免每个字段重复 set_index
    indexed_frames: Dict[str, pd.DataFrame] = {}
    for symbol, frame in data_dict.items():
        if not frame.empty and "candle_begin_time" in frame.columns:
            indexed_frames[symbol] = frame.set_index("candle_begin_time")

    pivot_map: Dict[str, pd.DataFrame] = {}
    for out_name, source_col in field_pairs:
        series_list: List[pd.Series] = []
        for symbol, indexed in indexed_frames.items():
            # 缺少该列的 symbol 直接跳过，不报错
            if source_col not in indexed.columns:
                continue
            series = indexed[source_col]
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

def _acquire_output_locks(payloads: Dict[Path, object]) -> List[IO[str]]:
    """
    按目录获取写锁，避免多进程并发写同一批产物。

    在非 Unix 平台（Windows 等）fcntl 不可用时，跳过锁操作并记录警告。
    """

    # fcntl 在非 Unix 平台不可用（如 Windows），跳过锁以保持跨平台兼容性
    if fcntl is None:
        LOGGER.warning("fcntl 不可用（非 Unix 平台），跳过文件锁")
        return []

    lock_handles: List[IO[str]] = []
    lock_dirs = sorted({target.parent for target in payloads}, key=lambda path: str(path))
    try:
        for lock_dir in lock_dirs:
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_path = lock_dir / ".preprocess.lock"
            lock_handle = lock_path.open("a+")
            try:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            except Exception:
                lock_handle.close()
                raise
            lock_handles.append(lock_handle)
    except Exception:
        # 释放已获取的锁，避免资源泄漏
        for handle in lock_handles:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
            except Exception:
                pass
        raise
    return lock_handles

def _write_pickles_atomically(payloads: Dict[Path, object]) -> None:
    """
    原子写入多个 pkl 文件。

    策略：先写临时文件，再做批量替换；替换失败时回滚旧文件。
    """

    run_token = f"{os.getpid()}-{time.time_ns()}"
    temp_files: Dict[Path, Path] = {}
    backups: Dict[Path, Path] = {}
    created_targets: List[Path] = []
    restored_backups: Set[Path] = set()
    success = False
    lock_handles = _acquire_output_locks(payloads)

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
        success = True
    except Exception:
        # 清理未消费的临时文件
        for temp_path in temp_files.values():
            _safe_unlink(temp_path)

        # 回滚：先删除本次新建文件，再恢复备份。
        for target in created_targets:
            _safe_unlink(target)
        for target, backup in backups.items():
            if backup.exists():
                try:
                    _safe_unlink(target)
                    os.replace(backup, target)
                    restored_backups.add(backup)
                except Exception:
                    # 回滚恢复失败时保留 backup，避免 finally 再删除导致无法人工恢复。
                    pass

        raise
    finally:
        for temp_path in temp_files.values():
            _safe_unlink(temp_path)
        if success:
            for backup in backups.values():
                _safe_unlink(backup)
        else:
            for backup in restored_backups:
                _safe_unlink(backup)
        for lock_handle in reversed(lock_handles):
            # fcntl 可用时才执行解锁（_acquire_output_locks 在 fcntl 为 None 时返回空列表，
            # 所以这里实际只会在 fcntl 不为 None 时执行；加判断以防止未来潜在路径变化）
            try:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                lock_handle.close()

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
