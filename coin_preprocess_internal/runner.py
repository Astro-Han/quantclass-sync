"""职责：执行预处理编排（全量重建与增量 patch）。"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence, Set, Tuple

import pandas as pd

from .constants import (
    FRAME_COLUMNS,
    OUTPUT_PIVOT_SPOT,
    OUTPUT_PIVOT_SWAP,
    OUTPUT_SPOT_DICT,
    OUTPUT_SWAP_DICT,
    RELIST_CHANGE_THRESHOLD,
    RELIST_GAP_THRESHOLD,
    PreprocessSummary,
    TIMESTAMP_FILE_NAME,
)
from .csv_source import (
    _build_incremental_rows,
    _collect_source_csv_map,
    _load_symbol_dict,
    _read_symbol_csv,
    _read_symbol_csv_tail,
    _split_symbol_frames,
)
from .pivot import _make_market_pivot, _patch_market_pivot, _validate_integrity, _write_pickles_atomically
from .symbol_mapper import (
    _apply_special_symbol_mapping,
    _extract_base_symbol,
    _group_split_symbols_by_source,
)

def _read_baseline_runtime(output_dir: Path) -> Optional[pd.Timestamp]:
    """读取 timestamp.txt 第二列（运行时间）作为增量基线。"""

    ts_file = output_dir / TIMESTAMP_FILE_NAME
    if not ts_file.exists():
        return None
    text = ts_file.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        return None
    parts = [x.strip() for x in text.split(",", 1)]
    if len(parts) < 2 or not parts[1]:
        return None
    parsed = pd.to_datetime(parts[1], errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)

def _load_existing_baseline(output_dir: Path) -> Optional[Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]]:
    """读取历史四个 pkl 作为增量 patch 基线。缺任一文件则返回 None。"""

    spot_dict_path = output_dir / OUTPUT_SPOT_DICT
    swap_dict_path = output_dir / OUTPUT_SWAP_DICT
    pivot_spot_path = output_dir / OUTPUT_PIVOT_SPOT
    pivot_swap_path = output_dir / OUTPUT_PIVOT_SWAP
    required = [spot_dict_path, swap_dict_path, pivot_spot_path, pivot_swap_path]
    if not all(path.exists() for path in required):
        return None

    spot_dict = pd.read_pickle(spot_dict_path)
    swap_dict = pd.read_pickle(swap_dict_path)
    pivot_spot = pd.read_pickle(pivot_spot_path)
    pivot_swap = pd.read_pickle(pivot_swap_path)

    if not isinstance(spot_dict, dict) or not isinstance(swap_dict, dict):
        raise RuntimeError("历史字典产物格式异常：spot_dict/swap_dict 必须是 dict。")
    if not isinstance(pivot_spot, dict) or not isinstance(pivot_swap, dict):
        raise RuntimeError("历史 pivot 产物格式异常：market_pivot_* 必须是 dict。")

    return spot_dict, swap_dict, pivot_spot, pivot_swap

def _max_candle_time(frames: Sequence[pd.DataFrame]) -> Optional[pd.Timestamp]:
    """返回一组 frame 中最大的 candle_begin_time。"""

    candidates: List[pd.Timestamp] = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        value = pd.to_datetime(frame["candle_begin_time"].max(), errors="coerce")
        if pd.isna(value):
            continue
        candidates.append(pd.Timestamp(value))
    if not candidates:
        return None
    return max(candidates)

def _has_relist_break(prev_time: pd.Timestamp, prev_close: float, next_time: pd.Timestamp, next_open: float) -> bool:
    """判断边界处是否触发 relist 切段。"""

    if prev_close == 0:
        return False
    if (next_time - prev_time) <= RELIST_GAP_THRESHOLD:
        return False
    change = next_open / prev_close - 1.0
    return abs(change) >= RELIST_CHANGE_THRESHOLD

def _try_tail_append_symbol(
    source_file: Path,
    source_symbol: str,
    data_dict: Dict[str, pd.DataFrame],
    existing_keys: Sequence[str],
    is_swap: bool,
) -> Tuple[bool, Set[str]]:
    """
    尝试走"尾部追加"增量路径。

    返回：
    - bool: 是否命中增量追加（True 表示不需要全文件重算该 symbol）
    - Set[str]: 受影响 split symbol 键
    """

    keys = [key for key in existing_keys if key in data_dict]
    if not keys:
        return False, set()

    existing_frames = [data_dict[key] for key in keys if not data_dict[key].empty]
    last_time = _max_candle_time(existing_frames)
    if last_time is None:
        return False, set()

    # 选"最新分段"做尾部追加，历史分段不变。
    active_key = max(
        keys,
        key=lambda key: pd.to_datetime(data_dict[key]["candle_begin_time"].max(), errors="coerce"),
    )
    active_frame = data_dict[active_key]
    if active_frame.empty:
        return False, set()

    tail_raw = _read_symbol_csv_tail(source_file)
    if tail_raw.empty or "candle_begin_time" not in tail_raw.columns:
        return False, set()

    tail_raw["candle_begin_time"] = pd.to_datetime(
        tail_raw["candle_begin_time"],
        errors="coerce",
        format="mixed",
    )
    tail_raw = tail_raw.dropna(subset=["candle_begin_time"])
    tail_raw = tail_raw.sort_values("candle_begin_time").drop_duplicates(subset=["candle_begin_time"], keep="last")
    if tail_raw.empty:
        return False, set()

    tail_min = pd.to_datetime(tail_raw["candle_begin_time"].min(), errors="coerce")
    if pd.isna(tail_min) or pd.Timestamp(tail_min) > last_time:
        # 尾部窗口没覆盖到旧边界，无法证明"只需追加"，回退单 symbol 全量重算。
        return False, set()

    new_raw = tail_raw[pd.to_datetime(tail_raw["candle_begin_time"], errors="coerce") > last_time]
    if new_raw.empty:
        # mtime 变化但尾部没有新增行，视为该 symbol 无需重算。
        return True, set()

    prev_close = float(active_frame["close"].iloc[-1])
    next_time = pd.Timestamp(pd.to_datetime(new_raw["candle_begin_time"].min(), errors="coerce"))
    next_open = float(new_raw.sort_values("candle_begin_time").iloc[0].get("open", prev_close) or prev_close)
    if _has_relist_break(last_time, prev_close, next_time, next_open):
        # 边界触发 relist，必须全文件重算该 symbol 才能保证分段正确。
        return False, set()

    appended_rows = _build_incremental_rows(
        new_raw=new_raw,
        existing_frame=active_frame,
        source_symbol=source_symbol,
        is_swap=is_swap,
    )
    if appended_rows.empty:
        return True, set()

    merged = pd.concat([active_frame, appended_rows], ignore_index=True)
    merged = merged.drop_duplicates(subset=["candle_begin_time"], keep="last").sort_values("candle_begin_time")
    merged = merged.reset_index(drop=True)

    first_time = pd.to_datetime(merged["first_candle_time"].iloc[0], errors="coerce")
    if pd.isna(first_time):
        first_time = pd.to_datetime(merged["candle_begin_time"].iloc[0], errors="coerce")
    merged["first_candle_time"] = pd.Timestamp(first_time)
    merged["last_candle_time"] = pd.to_datetime(merged["candle_begin_time"].max(), errors="coerce")

    data_dict[active_key] = merged[FRAME_COLUMNS]
    return True, {active_key}

def _rebuild_source_symbol(
    source_file: Path,
    source_symbol: str,
    is_swap: bool,
    data_dict: Dict[str, pd.DataFrame],
    grouped_by_source: Dict[str, List[str]],
) -> Tuple[Set[str], Set[str]]:
    """
    全文件重算单个 source symbol，并更新 split 键。

    返回：
    - removed_symbols: 被移除的旧 split 键
    - added_symbols: 新增/重建后的 split 键
    """

    removed_symbols: Set[str] = set()
    for key in grouped_by_source.get(source_symbol, []):
        if key in data_dict:
            removed_symbols.add(key)
            data_dict.pop(key, None)

    raw_df = _read_symbol_csv(source_file)
    rebuilt = _split_symbol_frames(raw_df, source_symbol=source_symbol, is_swap=is_swap)
    for key, frame in rebuilt.items():
        data_dict[key] = frame

    added_symbols = set(rebuilt.keys())
    return removed_symbols, added_symbols

def _resolve_source_delta(
    source_files: Dict[str, Path],
    baseline_runtime: pd.Timestamp,
    baseline_sources: Set[str],
) -> Tuple[Set[str], Set[str]]:
    """按 timestamp + 文件名集合识别 source symbol 的新增/变更/删除。"""

    current_sources = set(source_files.keys())
    added_sources = current_sources - baseline_sources
    removed_sources = baseline_sources - current_sources

    changed_sources: Set[str] = set(added_sources)
    # 注意：这里使用"本地时间戳"比较，避免 epoch/时区换算造成误判。
    baseline_local = pd.Timestamp(baseline_runtime)
    for symbol in current_sources - added_sources:
        path = source_files[symbol]
        try:
            mtime_local = pd.Timestamp.fromtimestamp(path.stat().st_mtime)
            if mtime_local > baseline_local:
                changed_sources.add(symbol)
        except OSError:
            continue
    return changed_sources, removed_sources

def _run_full_rebuild(spot_dir: Path, swap_dir: Path, output_dir: Path, mode: str) -> PreprocessSummary:
    """执行全量重建流程。"""

    spot_dict, spot_source_files = _load_symbol_dict(spot_dir, is_swap=False)
    swap_dict, swap_source_files = _load_symbol_dict(swap_dir, is_swap=True)

    _validate_integrity(
        spot_dir=spot_dir,
        swap_dir=swap_dir,
        spot_source_files=spot_source_files,
        swap_source_files=swap_source_files,
        spot_symbols=len(spot_dict),
        swap_symbols=len(swap_dict),
    )

    _apply_special_symbol_mapping(spot_dict, swap_dict)
    market_pivot_spot = _make_market_pivot(spot_dict, market_type="spot")
    market_pivot_swap = _make_market_pivot(swap_dict, market_type="swap")

    payloads = {
        output_dir / OUTPUT_SPOT_DICT: spot_dict,
        output_dir / OUTPUT_SWAP_DICT: swap_dict,
        output_dir / OUTPUT_PIVOT_SPOT: market_pivot_spot,
        output_dir / OUTPUT_PIVOT_SWAP: market_pivot_swap,
    }
    _write_pickles_atomically(payloads)

    return PreprocessSummary(
        spot_symbols=len(spot_dict),
        swap_symbols=len(swap_dict),
        output_dir=str(output_dir),
        mode=mode,
        changed_symbols=len(spot_dict) + len(swap_dict),
    )

def _patch_one_side(
    sources: Dict[str, Path],
    data_dict: Dict[str, pd.DataFrame],
    grouped: Dict[str, List[str]],
    removed_sources: Set[str],
    changed_sources: Set[str],
    is_swap: bool,
) -> Tuple[Set[str], Set[str]]:
    """处理单侧（spot 或 swap）的删除 + 变更，返回 (removed_symbols, changed_symbols)。"""

    removed_symbols: Set[str] = set()
    changed_symbols: Set[str] = set()

    # 删除源 symbol：把对应 split 键从产物移除。
    for source_symbol in sorted(removed_sources):
        for key in grouped.get(source_symbol, []):
            if key in data_dict:
                data_dict.pop(key, None)
                removed_symbols.add(key)

    # 变更源 symbol：先尝试尾部追加，失败则仅重算该 symbol。
    for source_symbol in sorted(changed_sources):
        source_file = sources.get(source_symbol)
        if source_file is None:
            continue
        tail_ok, tail_affected = _try_tail_append_symbol(
            source_file=source_file,
            source_symbol=source_symbol,
            data_dict=data_dict,
            existing_keys=grouped.get(source_symbol, []),
            is_swap=is_swap,
        )
        if tail_ok:
            changed_symbols.update(tail_affected)
            continue

        removed, added = _rebuild_source_symbol(
            source_file=source_file,
            source_symbol=source_symbol,
            is_swap=is_swap,
            data_dict=data_dict,
            grouped_by_source=grouped,
        )
        removed_symbols.update(removed)
        changed_symbols.update(added)

    return removed_symbols, changed_symbols

def _run_incremental_patch(
    spot_dir: Path,
    swap_dir: Path,
    output_dir: Path,
    baseline_runtime: pd.Timestamp,
    spot_dict: Dict[str, pd.DataFrame],
    swap_dict: Dict[str, pd.DataFrame],
    market_pivot_spot: Dict[str, pd.DataFrame],
    market_pivot_swap: Dict[str, pd.DataFrame],
) -> PreprocessSummary:
    """执行无 sidecar 的增量 patch。"""

    spot_sources = _collect_source_csv_map(spot_dir)
    swap_sources = _collect_source_csv_map(swap_dir)

    spot_grouped = _group_split_symbols_by_source(spot_dict)
    swap_grouped = _group_split_symbols_by_source(swap_dict)

    changed_spot_sources, removed_spot_sources = _resolve_source_delta(
        source_files=spot_sources,
        baseline_runtime=baseline_runtime,
        baseline_sources=set(spot_grouped.keys()),
    )
    changed_swap_sources, removed_swap_sources = _resolve_source_delta(
        source_files=swap_sources,
        baseline_runtime=baseline_runtime,
        baseline_sources=set(swap_grouped.keys()),
    )

    spot_removed_symbols, spot_changed_symbols = _patch_one_side(
        sources=spot_sources,
        data_dict=spot_dict,
        grouped=spot_grouped,
        removed_sources=removed_spot_sources,
        changed_sources=changed_spot_sources,
        is_swap=False,
    )
    swap_removed_symbols, swap_changed_symbols = _patch_one_side(
        sources=swap_sources,
        data_dict=swap_dict,
        grouped=swap_grouped,
        removed_sources=removed_swap_sources,
        changed_sources=changed_swap_sources,
        is_swap=True,
    )

    _validate_integrity(
        spot_dir=spot_dir,
        swap_dir=swap_dir,
        spot_source_files=len(spot_sources),
        swap_source_files=len(swap_sources),
        spot_symbols=len(spot_dict),
        swap_symbols=len(swap_dict),
    )

    _apply_special_symbol_mapping(spot_dict, swap_dict)

    market_pivot_spot = _patch_market_pivot(
        pivot_map=market_pivot_spot,
        data_dict=spot_dict,
        market_type="spot",
        changed_symbols=spot_changed_symbols,
        removed_symbols=spot_removed_symbols,
    )
    market_pivot_swap = _patch_market_pivot(
        pivot_map=market_pivot_swap,
        data_dict=swap_dict,
        market_type="swap",
        changed_symbols=swap_changed_symbols,
        removed_symbols=swap_removed_symbols,
    )

    payloads = {
        output_dir / OUTPUT_SPOT_DICT: spot_dict,
        output_dir / OUTPUT_SWAP_DICT: swap_dict,
        output_dir / OUTPUT_PIVOT_SPOT: market_pivot_spot,
        output_dir / OUTPUT_PIVOT_SWAP: market_pivot_swap,
    }
    _write_pickles_atomically(payloads)

    changed_count = (
        len(changed_spot_sources)
        + len(changed_swap_sources)
        + len(removed_spot_sources)
        + len(removed_swap_sources)
    )
    return PreprocessSummary(
        spot_symbols=len(spot_dict),
        swap_symbols=len(swap_dict),
        output_dir=str(output_dir),
        mode="incremental_patch",
        changed_symbols=changed_count,
    )
