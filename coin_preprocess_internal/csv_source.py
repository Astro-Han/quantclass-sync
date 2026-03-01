"""职责：读取币圈源 CSV，并把原始数据整理为标准化 frame。"""

from __future__ import annotations

import os
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd

from .constants import (
    CSV_ENCODINGS,
    FRAME_COLUMNS,
    RELIST_CHANGE_THRESHOLD,
    RELIST_GAP_THRESHOLD,
    TAIL_READ_MAX_BYTES,
    TAIL_READ_MAX_LINES,
)
from .symbol_mapper import _build_split_symbol, _normalize_symbol

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

