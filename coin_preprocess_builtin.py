#!/usr/bin/env python3
"""币圈合成数据内置预处理（兼容入口：导出稳定 + 内部实现拆分）。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import pandas as pd

from coin_preprocess_internal.constants import (
    OUTPUT_PIVOT_SPOT,
    OUTPUT_PIVOT_SWAP,
    OUTPUT_SPOT_DICT,
    OUTPUT_SWAP_DICT,
    PREPROCESS_PRODUCT,
    SPOT_PRODUCT,
    SWAP_PRODUCT,
    TIMESTAMP_FILE_NAME,
    PreprocessSummary,
)
from coin_preprocess_internal.csv_source import (
    _build_incremental_rows,
    _collect_source_csv_map,
    _read_symbol_csv,
    _read_symbol_csv_tail,
    _split_symbol_frames,
)
from coin_preprocess_internal.pivot import _patch_market_pivot
from coin_preprocess_internal.runner import (
    _load_existing_baseline,
    _read_baseline_runtime,
    _run_full_rebuild,
)
from coin_preprocess_internal import runner as _runner

LOGGER = logging.getLogger(__name__)

# 兼容导出：测试会 patch 这两个符号。
_rebuild_source_symbol = _runner._rebuild_source_symbol


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
    """兼容包装：调用内部增量 patch，并同步可 patch 依赖。"""

    _runner._rebuild_source_symbol = _rebuild_source_symbol
    return _runner._run_incremental_patch(
        spot_dir=spot_dir,
        swap_dir=swap_dir,
        output_dir=output_dir,
        baseline_runtime=baseline_runtime,
        spot_dict=spot_dict,
        swap_dict=swap_dict,
        market_pivot_spot=market_pivot_spot,
        market_pivot_swap=market_pivot_swap,
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

    try:
        baseline = _load_existing_baseline(output_dir)
    except Exception as exc:
        LOGGER.warning(
            "读取历史 baseline 失败，将回退 full_rebuild: error_type=%s",
            type(exc).__name__,
        )
        baseline = None
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
            ) from incremental_exc
