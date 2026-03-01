"""职责：处理 symbol 规范化、split 命名和跨市场映射规则。"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence

import pandas as pd

from .constants import (
    SPECIAL_SPOT_TO_SWAP_ALIAS,
    SPECIAL_SWAP_TO_SPOT_ALIAS,
    SPLIT_SYMBOL_PATTERN,
    SWAP_SPLIT_MAP,
)

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

def _group_split_symbols_by_source(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    """把 split symbol 聚合到 source symbol 级别。"""

    grouped: Dict[str, List[str]] = {}
    for symbol in data_dict.keys():
        base_symbol = _extract_base_symbol(symbol)
        grouped.setdefault(base_symbol, []).append(symbol)
    return grouped

