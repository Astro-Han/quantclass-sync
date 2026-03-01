#!/usr/bin/env python3
"""币圈合成数据内置预处理（严格完整性 + 原子写入 + 无 sidecar 增量 patch）。"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

SPOT_PRODUCT = "coin-binance-candle-csv-1h"
SWAP_PRODUCT = "coin-binance-swap-candle-csv-1h"
PREPROCESS_PRODUCT = "coin-binance-spot-swap-preprocess-pkl-1h"

OUTPUT_SPOT_DICT = "spot_dict.pkl"
OUTPUT_SWAP_DICT = "swap_dict.pkl"
OUTPUT_PIVOT_SPOT = "market_pivot_spot.pkl"
OUTPUT_PIVOT_SWAP = "market_pivot_swap.pkl"

TIMESTAMP_FILE_NAME = "timestamp.txt"
CSV_ENCODINGS = ("gbk", "utf-8-sig", "utf-8")
TAIL_READ_MAX_LINES = 4096
TAIL_READ_MAX_BYTES = 8 * 1024 * 1024
# pivot 列更新后触发去碎片的阈值（0 表示关闭）。
PIVOT_DEFRAG_THRESHOLD = 32

# 与历史预处理语义对齐的特殊映射：spot -> swap 别名。
SPECIAL_SPOT_TO_SWAP_ALIAS = {
    "DODO-USDT": "DODOX-USDT",
    "LUNA-USDT": "LUNA2-USDT",
    "RAY-USDT": "RAYSOL-USDT",
    "1000SATS-USDT": "1000SATS-USDT",
}
SPECIAL_SWAP_TO_SPOT_ALIAS = {v: k for k, v in SPECIAL_SPOT_TO_SWAP_ALIAS.items()}

# 特殊币在 swap 侧可能拆分为多个交易对。
SWAP_SPLIT_MAP = {
    "LUNA-USDT": ["LUNA-USDT", "LUNA2-USDT"],
    "DODO-USDT": ["DODO-USDT", "DODOX-USDT"],
    "RAY-USDT": ["RAY-USDT", "RAYSOL-USDT"],
}

SPLIT_SYMBOL_PATTERN = re.compile(r"^(?P<base>.+)_(?P<tag>SP|SW)(?P<index>\d+)-USDT$")

# relist 判定阈值（与历史脚本口径一致：1 天且跳变 >=1% 才切段）。
RELIST_GAP_THRESHOLD = pd.to_timedelta("1days")
RELIST_CHANGE_THRESHOLD = 0.01

FRAME_COLUMNS = [
    "candle_begin_time",
    "symbol",
    "open",
    "high",
    "close",
    "low",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "funding_fee",
    "avg_price_1m",
    "avg_price_5m",
    "是否交易",
    "first_candle_time",
    "last_candle_time",
    "symbol_spot",
    "symbol_swap",
    "is_spot",
]

PIVOT_FIELDS = {
    "spot": [
        ("open", "open"),
        ("close", "close"),
        ("vwap1m", "avg_price_1m"),
    ],
    "swap": [
        ("open", "open"),
        ("close", "close"),
        ("funding_rate", "funding_fee"),
        ("vwap1m", "avg_price_1m"),
    ],
}


@dataclass(frozen=True)
class PreprocessSummary:
    """内置预处理执行摘要。"""

    spot_symbols: int
    swap_symbols: int
    output_dir: str
    mode: str
    changed_symbols: int


def _normalize_symbol(symbol: str) -> str:
    """统一 symbol 字符串口径。"""

    return symbol.strip().upper()


def _extract_base_symbol(symbol: str) -> str:
    """从 symbol 提取基础交易对（含 -USDT）。"""

    text = _normalize_symbol(symbol)
    matched = SPLIT_SYMBOL_PATTERN.fullmatch(text)
    if matched:
        return f"{matched.group('base')}-USDT"
    return text


def _extract_split_index(symbol: str, expected_tag: str) -> Optional[int]:
    """提取分段索引（例如 LUNA_SP0-USDT -> 0）。"""

    matched = SPLIT_SYMBOL_PATTERN.fullmatch(_normalize_symbol(symbol))
    if not matched:
        return None
    if matched.group("tag") != expected_tag:
        return None
    return int(matched.group("index"))


def _build_split_symbol(source_symbol: str, side_tag: str, segment_index: int, segment_total: int) -> str:
    """生成切段后 symbol 名，最后一段保留原 symbol。"""

    normalized = _normalize_symbol(source_symbol)
    if segment_total <= 1 or segment_index == segment_total - 1:
        return normalized
    base = normalized[:-5] if normalized.endswith("-USDT") else normalized
    return f"{base}_{side_tag}{segment_index}-USDT"


def _iter_symbol_csv_files(root: Path) -> List[Path]:
    """遍历产品目录下的 USDT 交易对 CSV。"""

    if not root.exists() or not root.is_dir():
        return []
    return sorted(
        p for p in root.glob("*-USDT.csv") if p.is_file() and not p.name.startswith(".")
    )


def _collect_source_csv_map(root: Path) -> Dict[str, Path]:
    """返回 source_symbol -> csv_path 映射（symbol 统一大写）。"""

    result: Dict[str, Path] = {}
    for path in _iter_symbol_csv_files(root):
        result[_normalize_symbol(path.stem)] = path
    return result


def _try_read_with_encodings(path: Path, reader_fn: Callable[[str], Any], error_prefix: str = "读取失败") -> Any:
    """按 CSV_ENCODINGS 顺序尝试 reader_fn, 成功即返回, 全部失败则抛出 RuntimeError."""
    errors: List[str] = []
    for encoding in CSV_ENCODINGS:
        try:
            return reader_fn(encoding)
        except Exception as exc:
            errors.append(f"{encoding}: {exc}")
    raise RuntimeError(f"{error_prefix}: {path}; 尝试编码: {' | '.join(errors)}")


def _read_symbol_csv(path: Path) -> pd.DataFrame:
    """
    读取单个币种 CSV。

    数据源首行通常是备注，因此固定 skiprows=1，再用多编码兜底读取。
    """

    def _reader(encoding: str) -> pd.DataFrame:
        return pd.read_csv(
            path,
            encoding=encoding,
            skiprows=1,
            parse_dates=["candle_begin_time"],
            low_memory=False,
        )

    return _try_read_with_encodings(path, _reader, error_prefix="读取失败")


def _read_csv_header_line(path: Path, encoding: str) -> str:
    """读取 CSV 表头行（兼容首行备注）。"""

    with path.open("r", encoding=encoding, errors="ignore") as fp:
        first = fp.readline().strip()
        second = fp.readline().strip()
    if first.lower().startswith("candle_begin_time"):
        return first
    if second:
        return second
    raise RuntimeError(f"无法读取 CSV 表头: {path}")


def _read_tail_lines(path: Path, encoding: str, max_lines: int, max_bytes: int) -> List[str]:
    """从文件尾部读取若干行，避免全文件扫描。"""

    with path.open("rb") as fp:
        fp.seek(0, os.SEEK_END)
        file_size = fp.tell()
        if file_size <= 0:
            return []
        read_size = min(file_size, max_bytes)
        fp.seek(file_size - read_size)
        data = fp.read(read_size)

    text = data.decode(encoding, errors="ignore")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    return lines[-max_lines:]


def _read_symbol_csv_tail(path: Path, max_lines: int = TAIL_READ_MAX_LINES) -> pd.DataFrame:
    """
    读取 symbol CSV 尾部窗口。

    目的：优先用"尾部追加"判断是否可增量 patch；
    若尾部不足覆盖边界，外层会回退到全文件重算该 symbol。
    """

    def _reader(encoding: str) -> pd.DataFrame:
        header = _read_csv_header_line(path, encoding=encoding)
        expected_cols = max(1, len(header.split(",")))
        tail_lines = _read_tail_lines(path, encoding=encoding, max_lines=max_lines, max_bytes=TAIL_READ_MAX_BYTES)
        valid_lines = [
            line
            for line in tail_lines
            if not line.startswith("备注")
            and not line.lower().startswith("candle_begin_time")
            and line.count(",") >= expected_cols - 1
        ]
        if not valid_lines:
            return pd.DataFrame()
        text = header + "\n" + "\n".join(valid_lines) + "\n"
        frame = pd.read_csv(StringIO(text), low_memory=False)
        if "candle_begin_time" in frame.columns:
            frame["candle_begin_time"] = pd.to_datetime(
                frame["candle_begin_time"],
                errors="coerce",
                format="mixed",
            )
        return frame

    return _try_read_with_encodings(path, _reader, error_prefix="读取尾部窗口失败")


def _detect_relist_segments(raw_df: pd.DataFrame) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    识别 relist 切段区间。

    规则：时间间隔 > 1 天 且前收盘到当前开盘跳变 >= 1%。
    """

    data = raw_df.sort_values("candle_begin_time").drop_duplicates(subset=["candle_begin_time"], keep="last")
    if data.empty:
        return []

    times = list(data["candle_begin_time"])
    if len(times) == 1:
        return [(times[0], times[0])]

    break_positions: List[int] = []
    for idx in range(1, len(data)):
        prev_row = data.iloc[idx - 1]
        curr_row = data.iloc[idx]
        time_delta = curr_row["candle_begin_time"] - prev_row["candle_begin_time"]
        if time_delta <= RELIST_GAP_THRESHOLD:
            continue

        prev_close = prev_row.get("close")
        curr_open = curr_row.get("open")
        if pd.isna(prev_close) or pd.isna(curr_open) or float(prev_close) == 0.0:
            continue
        price_change = float(curr_open) / float(prev_close) - 1.0
        if abs(price_change) >= RELIST_CHANGE_THRESHOLD:
            break_positions.append(idx)

    if not break_positions:
        return [(times[0], times[-1])]

    segments: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    start_idx = 0
    for pos in break_positions:
        segments.append((times[start_idx], times[pos - 1]))
        start_idx = pos
    segments.append((times[start_idx], times[-1]))
    return segments


def _fill_standard_columns(df: pd.DataFrame, symbol_name: str, is_swap: bool) -> None:
    """就地填充 open/high/low/symbol/volume/funding_fee/是否交易 等标准列（close 需已就绪）。"""

    df["open"] = df.get("open", pd.Series(dtype="float64")).fillna(df["close"])
    df["high"] = df.get("high", pd.Series(dtype="float64")).fillna(df["close"])
    df["low"] = df.get("low", pd.Series(dtype="float64")).fillna(df["close"])
    df["symbol"] = df.get("symbol", pd.Series(dtype="object")).ffill().fillna(symbol_name)

    for col in (
        "volume",
        "quote_volume",
        "trade_num",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ):
        df[col] = df.get(col, pd.Series(dtype="float64")).fillna(0)

    df["avg_price_1m"] = df.get("avg_price_1m", pd.Series(dtype="float64")).fillna(df["open"])
    df["avg_price_5m"] = df.get("avg_price_5m", pd.Series(dtype="float64")).fillna(df["open"])
    if is_swap:
        df["funding_fee"] = df.get("fundingRate", pd.Series(dtype="float64")).fillna(0)
    else:
        df["funding_fee"] = 0
    df["是否交易"] = (df["volume"] > 0).astype("int8")


def _prepare_symbol_frame(raw_df: pd.DataFrame, symbol: str, is_swap: bool) -> pd.DataFrame:
    """把单币种原始 K 线整理为统一结构（20 列口径）。"""

    if raw_df.empty:
        return pd.DataFrame()
    if "candle_begin_time" not in raw_df.columns:
        raise RuntimeError(f"{symbol} 缺少 candle_begin_time 列。")

    data = raw_df.sort_values("candle_begin_time").drop_duplicates(
        subset=["candle_begin_time"], keep="last"
    )
    if data.empty:
        return pd.DataFrame()

    first_candle_time = data["candle_begin_time"].min()
    last_candle_time = data["candle_begin_time"].max()
    benchmark_start = first_candle_time.replace(minute=0, second=0, microsecond=0, hour=0)
    benchmark = pd.DataFrame(
        {
            "candle_begin_time": pd.date_range(
                start=benchmark_start,
                end=last_candle_time,
                freq="1h",
            )
        }
    )
    merged = benchmark.merge(data, how="left", on="candle_begin_time", sort=True)

    merged["close"] = merged.get("close", pd.Series(dtype="float64")).ffill()
    _fill_standard_columns(merged, symbol_name=symbol, is_swap=is_swap)
    merged["first_candle_time"] = first_candle_time
    merged["last_candle_time"] = last_candle_time
    merged["symbol_spot"] = symbol if not is_swap else ""
    merged["symbol_swap"] = symbol if is_swap else ""
    merged["is_spot"] = 0 if is_swap else 1
    return merged[FRAME_COLUMNS].reset_index(drop=True)


def _build_incremental_rows(
    new_raw: pd.DataFrame,
    existing_frame: pd.DataFrame,
    source_symbol: str,
    is_swap: bool,
) -> pd.DataFrame:
    """把新增原始行转换成与历史 frame 同结构的增量行。"""

    if new_raw.empty:
        return pd.DataFrame(columns=FRAME_COLUMNS)

    data = new_raw.sort_values("candle_begin_time").drop_duplicates(subset=["candle_begin_time"], keep="last").copy()
    data["candle_begin_time"] = pd.to_datetime(data["candle_begin_time"], errors="coerce", format="mixed")
    data = data.dropna(subset=["candle_begin_time"])
    if data.empty:
        return pd.DataFrame(columns=FRAME_COLUMNS)

    prev_close = float(existing_frame["close"].iloc[-1]) if not existing_frame.empty else 0.0
    if "close" not in data.columns:
        data["close"] = prev_close
    data["close"] = data["close"].ffill()
    if pd.isna(data["close"].iloc[0]):
        data.loc[data.index[0], "close"] = prev_close
    data["close"] = data["close"].ffill().fillna(prev_close)

    _fill_standard_columns(data, symbol_name=source_symbol, is_swap=is_swap)
    base_first = pd.to_datetime(existing_frame["first_candle_time"].iloc[0])
    data["first_candle_time"] = base_first
    data["last_candle_time"] = data["candle_begin_time"].max()
    data["symbol_spot"] = source_symbol if not is_swap else ""
    data["symbol_swap"] = source_symbol if is_swap else ""
    data["is_spot"] = 0 if is_swap else 1
    return data[FRAME_COLUMNS].reset_index(drop=True)


def _split_symbol_frames(raw_df: pd.DataFrame, source_symbol: str, is_swap: bool) -> Dict[str, pd.DataFrame]:
    """按 relist 规则拆分单币种，并返回 symbol->DataFrame。"""

    source_symbol = _normalize_symbol(source_symbol)
    segments = _detect_relist_segments(raw_df)
    if not segments:
        return {}

    side_tag = "SW" if is_swap else "SP"
    data = raw_df.sort_values("candle_begin_time").drop_duplicates(
        subset=["candle_begin_time"], keep="last"
    )

    result: Dict[str, pd.DataFrame] = {}
    for idx, (start_time, end_time) in enumerate(segments):
        part_symbol = _build_split_symbol(source_symbol, side_tag=side_tag, segment_index=idx, segment_total=len(segments))
        part_df = data[(data["candle_begin_time"] >= start_time) & (data["candle_begin_time"] <= end_time)]
        normalized = _prepare_symbol_frame(part_df, symbol=part_symbol, is_swap=is_swap)
        if not normalized.empty:
            result[part_symbol] = normalized
    return result


def _pick_first_existing(candidates: Sequence[str], existing: Sequence[str]) -> str:
    """按优先级选择首个存在的候选 symbol。"""

    existing_set = set(existing)
    seen = set()
    for item in candidates:
        symbol = _normalize_symbol(item)
        if symbol in seen:
            continue
        seen.add(symbol)
        if symbol in existing_set:
            return symbol
    return ""


def _candidate_swap_symbols_for_spot(spot_symbol: str) -> List[str]:
    """生成 spot->swap 的候选映射列表（按优先级）。"""

    normalized = _normalize_symbol(spot_symbol)
    base_symbol = _extract_base_symbol(normalized)
    split_idx = _extract_split_index(normalized, expected_tag="SP")

    candidates: List[str] = [normalized, base_symbol]

    if split_idx is not None:
        base_no_suffix = base_symbol[:-5]
        candidates.append(f"{base_no_suffix}_SW{split_idx}-USDT")

    alias = SPECIAL_SPOT_TO_SWAP_ALIAS.get(base_symbol, "")
    if alias:
        candidates.append(alias)
        if split_idx is not None:
            alias_no_suffix = alias[:-5]
            candidates.append(f"{alias_no_suffix}_SW{split_idx}-USDT")

    candidates.extend(SWAP_SPLIT_MAP.get(base_symbol, []))

    if not base_symbol.startswith("1000"):
        candidates.append(f"1000{base_symbol}")

    return candidates


def _candidate_spot_symbols_for_swap(swap_symbol: str) -> List[str]:
    """生成 swap->spot 的候选映射列表（按优先级）。"""

    normalized = _normalize_symbol(swap_symbol)
    base_symbol = _extract_base_symbol(normalized)
    split_idx = _extract_split_index(normalized, expected_tag="SW")

    candidates: List[str] = [normalized, base_symbol]

    if split_idx is not None:
        base_no_suffix = base_symbol[:-5]
        candidates.append(f"{base_no_suffix}_SP{split_idx}-USDT")

    alias = SPECIAL_SWAP_TO_SPOT_ALIAS.get(base_symbol, "")
    if alias:
        candidates.append(alias)
        if split_idx is not None:
            alias_no_suffix = alias[:-5]
            candidates.append(f"{alias_no_suffix}_SP{split_idx}-USDT")

    for spot_symbol, swap_symbols in SWAP_SPLIT_MAP.items():
        if base_symbol in swap_symbols:
            candidates.append(spot_symbol)
            if split_idx is not None:
                candidates.append(f"{spot_symbol[:-5]}_SP{split_idx}-USDT")

    if base_symbol.startswith("1000"):
        candidates.append(base_symbol[4:])

    return candidates


def _apply_special_symbol_mapping(
    spot_dict: Dict[str, pd.DataFrame],
    swap_dict: Dict[str, pd.DataFrame],
) -> None:
    """
    依据历史规则回填 symbol_spot / symbol_swap。

    目的：尽量保持与旧预处理产物的字段语义一致。
    """

    spot_keys = sorted(spot_dict.keys())
    swap_keys = sorted(swap_dict.keys())

    for spot_symbol, spot_df in spot_dict.items():
        mapped_swap = _pick_first_existing(_candidate_swap_symbols_for_spot(spot_symbol), swap_keys)
        spot_df["symbol_spot"] = _normalize_symbol(spot_symbol)
        spot_df["symbol_swap"] = mapped_swap

    for swap_symbol, swap_df in swap_dict.items():
        mapped_spot = _pick_first_existing(_candidate_spot_symbols_for_swap(swap_symbol), spot_keys)
        swap_df["symbol_swap"] = _normalize_symbol(swap_symbol)
        swap_df["symbol_spot"] = mapped_spot


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


def _load_symbol_dict(product_dir: Path, is_swap: bool) -> Tuple[Dict[str, pd.DataFrame], int]:
    """读取产品目录并返回 symbol->DataFrame 与源文件数量。"""

    files = _iter_symbol_csv_files(product_dir)
    result: Dict[str, pd.DataFrame] = {}

    for path in files:
        source_symbol = _normalize_symbol(path.stem)
        raw_df = _read_symbol_csv(path)
        split_result = _split_symbol_frames(raw_df, source_symbol=source_symbol, is_swap=is_swap)
        result.update(split_result)

    return result, len(files)


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


def _group_split_symbols_by_source(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    """把 split symbol 聚合到 source symbol 级别。"""

    grouped: Dict[str, List[str]] = {}
    for symbol in data_dict.keys():
        base_symbol = _extract_base_symbol(symbol)
        grouped.setdefault(base_symbol, []).append(symbol)
    return grouped


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


def run_coin_preprocess_builtin(data_root: Path) -> PreprocessSummary:
    """
    执行内置预处理并写入 pkl 产物。

    模式：
    1) 默认尝试 incremental_patch（无 sidecar，仅用 timestamp + mtime）
    2) 基线缺失时执行 full_rebuild
    3) incremental 失败时自动回退 full_rebuild
    """

    root = data_root.expanduser().resolve()
    spot_dir = root / SPOT_PRODUCT
    swap_dir = root / SWAP_PRODUCT
    output_dir = root / PREPROCESS_PRODUCT
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline = _load_existing_baseline(output_dir)
    baseline_runtime = _read_baseline_runtime(output_dir)
    if baseline is None or baseline_runtime is None:
        return _run_full_rebuild(spot_dir=spot_dir, swap_dir=swap_dir, output_dir=output_dir, mode="full_rebuild")

    spot_dict, swap_dict, market_pivot_spot, market_pivot_swap = baseline
    try:
        return _run_incremental_patch(
            spot_dir=spot_dir,
            swap_dir=swap_dir,
            output_dir=output_dir,
            baseline_runtime=baseline_runtime,
            spot_dict=spot_dict,
            swap_dict=swap_dict,
            market_pivot_spot=market_pivot_spot,
            market_pivot_swap=market_pivot_swap,
        )
    except Exception as incremental_exc:
        # 增量失败时自动回退全量，优先保证可用性与一致性。
        try:
            return _run_full_rebuild(
                spot_dir=spot_dir,
                swap_dir=swap_dir,
                output_dir=output_dir,
                mode="fallback_full_rebuild",
            )
        except Exception as rebuild_exc:
            raise RuntimeError(
                "增量 patch 与全量回退均失败；"
                f"incremental_error={incremental_exc}; full_rebuild_error={rebuild_exc}"
            ) from rebuild_exc
