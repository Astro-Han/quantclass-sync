"""职责：执行预处理编排（全量重建与增量 patch）。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

from .constants import (
    FRAME_COLUMNS,
    OUTPUT_PIVOT_SPOT,
    OUTPUT_PIVOT_SWAP,
    OUTPUT_SPOT_DICT,
    OUTPUT_SWAP_DICT,
    RELIST_CHANGE_THRESHOLD,
    RELIST_GAP_THRESHOLD,
    TAIL_APPEND_SAFE_MAX_DATA_ROWS,
    PreprocessSummary,
    TIMESTAMP_FILE_NAME,
)
from .csv_source import (
    _build_incremental_rows,
    _collect_source_csv_map,
    _load_symbol_dict,
    _read_symbol_csv,
    _read_symbol_csv_tail,
    _symbol_csv_exceeds_data_row_limit,
    _split_symbol_frames,
)
from .pivot import _make_market_pivot, _patch_market_pivot, _validate_integrity, _write_pickles_atomically
from .symbol_mapper import (
    _apply_special_symbol_mapping,
    _extract_base_symbol,
    _group_split_symbols_by_source,
)

# 复用主包的原子写入上下文管理器，避免重复实现同样的模式
from quantclass_sync_internal.config import atomic_temp_path

LOGGER = logging.getLogger(__name__)

def _read_baseline_runtime(output_dir: Path) -> Optional[pd.Timestamp]:
    """读取 timestamp.txt 第二列（运行时间）作为增量基线。"""

    ts_file = output_dir / TIMESTAMP_FILE_NAME
    if not ts_file.exists():
        return None
    try:
        text = ts_file.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception as exc:
        LOGGER.warning(
            "读取预处理 runtime 时间失败，降级 full_rebuild: path=%s error_type=%s",
            ts_file,
            type(exc).__name__,
        )
        return None
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

def _resolve_output_data_date(spot_dict: Dict[str, pd.DataFrame], swap_dict: Dict[str, pd.DataFrame]) -> str:
    """计算预处理产物对应的数据日期（YYYY-MM-DD）。"""

    latest = _max_candle_time([*spot_dict.values(), *swap_dict.values()])
    if latest is None:
        return pd.Timestamp.now().strftime("%Y-%m-%d")
    return pd.Timestamp(latest).strftime("%Y-%m-%d")

def _write_runtime_timestamp(output_dir: Path, data_date: str) -> None:
    """写入 timestamp.txt（格式：数据日期,本地运行时间），原子写入避免半截文件。"""

    path = output_dir / TIMESTAMP_FILE_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    local_now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with atomic_temp_path(path, tag="pts") as tmp:
            tmp.write_text(f"{data_date},{local_now}\n", encoding="utf-8")
    except Exception as exc:
        # timestamp 写入失败不影响 pkl 产物可用性，保守记录告警。
        LOGGER.warning(
            "写入预处理 timestamp 失败，保留 pkl 产物: path=%s error_type=%s",
            path,
            type(exc).__name__,
        )

def _has_relist_break(prev_time: pd.Timestamp, prev_close: float, next_time: pd.Timestamp, next_open: float) -> bool:
    """判断边界处是否触发 relist 切段。"""

    if prev_close == 0:
        return True
    if (next_time - prev_time) <= RELIST_GAP_THRESHOLD:
        return False
    change = next_open / prev_close - 1.0
    return abs(change) >= RELIST_CHANGE_THRESHOLD

def _has_internal_relist_break(new_raw: pd.DataFrame) -> bool:
    """判断增量窗口内部相邻行是否触发 relist 切段。"""

    if new_raw.empty or len(new_raw) < 2:
        return False

    ordered = new_raw.sort_values("candle_begin_time", kind="mergesort").reset_index(drop=True)

    # 向量化解析时间列
    times = pd.to_datetime(ordered["candle_begin_time"], errors="coerce", format="mixed")

    # 向量化解析 close/open（_safe_float 对非数值返回 nan）
    close_vals = pd.to_numeric(ordered["close"], errors="coerce")
    open_vals = pd.to_numeric(ordered["open"], errors="coerce")

    # 对齐相邻行：prev_* 取 [0..n-2]，curr_* 取 [1..n-1]
    prev_time = times.iloc[:-1].values
    curr_time = times.iloc[1:].values
    prev_close = close_vals.iloc[:-1].values
    curr_open = open_vals.iloc[1:].values

    # NaN 时间或 NaN 价格的行跳过（不触发 break）
    valid = (
        ~pd.isnull(prev_time)
        & ~pd.isnull(curr_time)
        & ~pd.isnull(prev_close)
        & ~pd.isnull(curr_open)
    )

    # 注意 _has_relist_break 逻辑：
    #   prev_close == 0 → 直接返回 True（relist break）
    #   时间差 <= 阈值  → 返回 False
    #   否则按价格变动判断
    # 向量化分三条件计算，最终取 OR

    # 条件 A：prev_close == 0（且数据有效）
    cond_zero = valid & (prev_close == 0.0)

    # 条件 B：时间差超阈值 且 价格变动超阈值
    time_diff = pd.Series(curr_time, dtype="datetime64[ns]") - pd.Series(prev_time, dtype="datetime64[ns]")
    gap_mask = time_diff > RELIST_GAP_THRESHOLD

    # 只在 prev_close != 0 且有效时计算价格变动，避免除以零
    safe_close = pd.Series(prev_close, dtype="float64")
    safe_open = pd.Series(curr_open, dtype="float64")
    nonzero_valid = valid & (safe_close != 0.0) & safe_close.notna() & safe_open.notna()

    change = pd.Series(float("nan"), index=safe_close.index)
    change[nonzero_valid] = (safe_open[nonzero_valid] / safe_close[nonzero_valid] - 1.0).abs()
    cond_change = gap_mask & (change >= RELIST_CHANGE_THRESHOLD)

    return bool((cond_zero | cond_change).any())

def _frame_max_candle_time(frame: pd.DataFrame) -> Optional[pd.Timestamp]:
    """返回单个 frame 的最大 candle_begin_time（脏时间自动跳过）。"""

    if frame is None or frame.empty or "candle_begin_time" not in frame.columns:
        return None
    parsed = pd.to_datetime(frame["candle_begin_time"], errors="coerce", format="mixed")
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max())

def _safe_float(value: object, fallback: float) -> float:
    """把值安全转换成 float，失败时返回 fallback。"""

    if value is None:
        return fallback
    try:
        result = float(value)
    except (TypeError, ValueError):
        return fallback
    if pd.isna(result):
        return fallback
    return result

def _build_overlap_snapshot(data_dict: Dict[str, pd.DataFrame], keys: Sequence[str]) -> pd.DataFrame:
    """把同源 split symbol 合并成按时间索引的快照，便于重叠区对比。"""

    frames: List[pd.DataFrame] = []
    keep_cols = ("candle_begin_time", "open", "close", "avg_price_1m", "funding_fee")
    for key in keys:
        frame = data_dict.get(key)
        if frame is None or frame.empty or "candle_begin_time" not in frame.columns:
            continue
        cols = [col for col in keep_cols if col in frame.columns]
        if "candle_begin_time" not in cols:
            continue
        part = frame[cols].copy()
        part["candle_begin_time"] = pd.to_datetime(part["candle_begin_time"], errors="coerce", format="mixed")
        part = part.dropna(subset=["candle_begin_time"])
        if not part.empty:
            frames.append(part)

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values("candle_begin_time", kind="mergesort").drop_duplicates(
        subset=["candle_begin_time"],
        keep="last",
    )
    return merged.set_index("candle_begin_time")

def _overlap_matches_existing(overlap_raw: pd.DataFrame, overlap_snapshot: pd.DataFrame, is_swap: bool) -> bool:
    """
    判断尾部重叠区是否与现有产物一致。

    返回 True 表示可视为“无实质变更”；返回 False 则应回退单 symbol 重建。
    """

    if overlap_raw.empty or overlap_snapshot.empty:
        return False
    if "candle_begin_time" not in overlap_raw.columns:
        return False

    compare_pairs = [
        ("open", "open"),
        ("close", "close"),
        ("avg_price_1m", "avg_price_1m"),
    ]
    if is_swap:
        compare_pairs.append(("fundingRate", "funding_fee"))

    sorted_overlap = overlap_raw.sort_values("candle_begin_time", kind="mergesort").copy()
    sorted_overlap["__cmp_ts__"] = pd.to_datetime(
        sorted_overlap["candle_begin_time"],
        errors="coerce",
        format="mixed",
    )
    sorted_overlap = sorted_overlap.dropna(subset=["__cmp_ts__"]).reset_index(drop=True)
    if sorted_overlap.empty:
        return False

    # 与逐行逻辑保持一致：同一时点若存在重复，使用最后一条旧快照记录。
    if overlap_snapshot.index.has_duplicates:
        overlap_snapshot = overlap_snapshot[~overlap_snapshot.index.duplicated(keep="last")]

    raw_ts = pd.DatetimeIndex(sorted_overlap["__cmp_ts__"])
    if not pd.Index(raw_ts).isin(overlap_snapshot.index).all():
        return False

    existing_aligned = overlap_snapshot.reindex(raw_ts).reset_index(drop=True)
    row_compared = pd.Series(0, index=sorted_overlap.index, dtype="int64")
    for raw_col, old_col in compare_pairs:
        if raw_col not in sorted_overlap.columns or old_col not in existing_aligned.columns:
            continue

        raw_values = pd.to_numeric(sorted_overlap[raw_col], errors="coerce")
        valid_mask = raw_values.notna()
        if not valid_mask.any():
            continue

        old_values = pd.to_numeric(existing_aligned[old_col], errors="coerce")
        if old_values[valid_mask].isna().any():
            return False

        if ((raw_values[valid_mask] - old_values[valid_mask]).abs() > 1e-12).any():
            return False

        row_compared.loc[valid_mask] += 1

    # 当前行没有可比字段，无法证明一致性，保守回退重建。
    if (row_compared == 0).any():
        return False
    return int(row_compared.sum()) > 0

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

    existing_frames: List[pd.DataFrame] = []
    for key in keys:
        frame = data_dict.get(key)
        if frame is None or frame.empty or "candle_begin_time" not in frame.columns:
            continue
        existing_frames.append(frame)
    last_time = _max_candle_time(existing_frames)
    if last_time is None:
        return False, set()

    # 选"最新分段"做尾部追加，历史分段不变；NaT 时间不会参与排序。
    active_candidates: List[Tuple[str, pd.Timestamp]] = []
    for key in keys:
        max_time = _frame_max_candle_time(data_dict.get(key, pd.DataFrame()))
        if max_time is not None:
            active_candidates.append((key, max_time))
    if not active_candidates:
        return False, set()
    active_key = max(active_candidates, key=lambda item: item[1])[0]
    active_frame = data_dict[active_key]
    if active_frame.empty:
        return False, set()

    if _symbol_csv_exceeds_data_row_limit(source_file, row_limit=TAIL_APPEND_SAFE_MAX_DATA_ROWS):
        # 大文件只看尾窗无法证明“纯追加”，保守回退单 symbol 全量重算。
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
    tail_raw = tail_raw.sort_values("candle_begin_time", kind="mergesort").drop_duplicates(
        subset=["candle_begin_time"],
        keep="last",
    )
    if tail_raw.empty:
        return False, set()

    tail_min = pd.to_datetime(tail_raw["candle_begin_time"].min(), errors="coerce")
    if pd.isna(tail_min) or pd.Timestamp(tail_min) > last_time:
        # 尾部窗口没覆盖到旧边界，无法证明"只需追加"，回退单 symbol 全量重算。
        return False, set()

    overlap_snapshot = _build_overlap_snapshot(data_dict, keys)
    overlap_raw = tail_raw[pd.to_datetime(tail_raw["candle_begin_time"], errors="coerce") <= last_time]
    if not _overlap_matches_existing(overlap_raw=overlap_raw, overlap_snapshot=overlap_snapshot, is_swap=is_swap):
        return False, set()

    new_raw = tail_raw[pd.to_datetime(tail_raw["candle_begin_time"], errors="coerce") > last_time]
    if new_raw.empty:
        # mtime 变化但无新增行：保守回退单 symbol 全量重算，避免漏算。
        return False, set()

    prev_close = _safe_float(active_frame["close"].iloc[-1], fallback=0.0)
    next_time = pd.Timestamp(pd.to_datetime(new_raw["candle_begin_time"].min(), errors="coerce"))
    next_open_raw = new_raw.sort_values("candle_begin_time").iloc[0].get("open", pd.NA)
    next_open = _safe_float(next_open_raw, fallback=prev_close)
    if _has_relist_break(last_time, prev_close, next_time, next_open):
        # 边界触发 relist，必须全文件重算该 symbol 才能保证分段正确。
        return False, set()
    if _has_internal_relist_break(new_raw):
        # 新增窗口内部触发 relist，同样必须全文件重算该 symbol。
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
    merged = merged.drop_duplicates(subset=["candle_begin_time"], keep="last").sort_values(
        "candle_begin_time",
        kind="mergesort",
    )
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
    baseline_epoch = float(pd.Timestamp(baseline_runtime).to_pydatetime().timestamp())
    for symbol in current_sources - added_sources:
        path = source_files[symbol]
        try:
            mtime_epoch = float(path.stat().st_mtime)
            # 文件系统 mtime 可能只有秒级精度；同秒按变更处理更安全。
            if mtime_epoch >= baseline_epoch:
                changed_sources.add(symbol)
        except OSError as exc:
            LOGGER.warning(
                "读取文件时间失败，按变更处理以保证安全: symbol=%s error_type=%s",
                symbol,
                type(exc).__name__,
            )
            changed_sources.add(symbol)
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
    _write_runtime_timestamp(
        output_dir=output_dir,
        data_date=_resolve_output_data_date(spot_dict=spot_dict, swap_dict=swap_dict),
    )

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
    _write_runtime_timestamp(
        output_dir=output_dir,
        data_date=_resolve_output_data_date(spot_dict=spot_dict, swap_dict=swap_dict),
    )

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
