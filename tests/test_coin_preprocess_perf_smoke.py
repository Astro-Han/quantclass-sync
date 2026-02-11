import tempfile
import unittest
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from pandas.errors import PerformanceWarning

from coin_preprocess_builtin import (
    OUTPUT_PIVOT_SPOT,
    OUTPUT_PIVOT_SWAP,
    OUTPUT_SPOT_DICT,
    OUTPUT_SWAP_DICT,
    PREPROCESS_PRODUCT,
    SPOT_PRODUCT,
    SWAP_PRODUCT,
    _patch_market_pivot,
    _run_incremental_patch,
    run_coin_preprocess_builtin,
)


class CoinPreprocessPerfSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / SPOT_PRODUCT).mkdir(parents=True, exist_ok=True)
        (self.root / SWAP_PRODUCT).mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_symbol_csv(self, product: str, symbol: str, is_swap: bool) -> None:
        if is_swap:
            content = "\n".join(
                [
                    "备注,,,,,,,,,,,,,,",
                    "candle_begin_time,open,high,low,close,volume,quote_volume,trade_num,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,Spread,symbol,avg_price_1m,avg_price_5m,fundingRate",
                    f"2026-02-09 00:00:00,1,1.1,0.9,1,10,10,1,5,5,,{symbol},1,1,0.0001",
                    "",
                ]
            )
        else:
            content = "\n".join(
                [
                    "备注,,,,,,,,,,,,,",
                    "candle_begin_time,open,high,low,close,volume,quote_volume,trade_num,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,Spread,symbol,avg_price_1m,avg_price_5m",
                    f"2026-02-09 00:00:00,1,1.1,0.9,1,10,10,1,5,5,,{symbol},1,1",
                    "",
                ]
            )
        (self.root / product / f"{symbol}.csv").write_text(content, encoding="utf-8")

    def test_incremental_without_delta_does_not_rebuild_symbol(self) -> None:
        self._write_symbol_csv(SPOT_PRODUCT, "AAA-USDT", is_swap=False)
        self._write_symbol_csv(SWAP_PRODUCT, "BBB-USDT", is_swap=True)
        run_coin_preprocess_builtin(self.root)

        output_dir = self.root / PREPROCESS_PRODUCT
        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        swap_dict = pd.read_pickle(output_dir / OUTPUT_SWAP_DICT)
        pivot_spot = pd.read_pickle(output_dir / OUTPUT_PIVOT_SPOT)
        pivot_swap = pd.read_pickle(output_dir / OUTPUT_PIVOT_SWAP)

        baseline_runtime = pd.Timestamp(datetime.now() + timedelta(hours=1))
        with patch("coin_preprocess_builtin._rebuild_source_symbol") as rebuild_mock:
            summary = _run_incremental_patch(
                spot_dir=self.root / SPOT_PRODUCT,
                swap_dir=self.root / SWAP_PRODUCT,
                output_dir=output_dir,
                baseline_runtime=baseline_runtime,
                spot_dict=spot_dict,
                swap_dict=swap_dict,
                market_pivot_spot=pivot_spot,
                market_pivot_swap=pivot_swap,
            )

        rebuild_mock.assert_not_called()
        self.assertEqual("incremental_patch", summary.mode)
        self.assertEqual(0, summary.changed_symbols)

    def test_patch_market_pivot_does_not_emit_performance_warning(self) -> None:
        timestamps = pd.to_datetime(["2026-02-09 00:00:00", "2026-02-09 01:00:00"])
        changed_symbols = {f"SYM{i:03d}-USDT" for i in range(140)}

        data_dict = {}
        for idx, symbol in enumerate(sorted(changed_symbols)):
            base = float(idx + 1)
            data_dict[symbol] = pd.DataFrame(
                {
                    "candle_begin_time": timestamps,
                    "open": [base, base + 0.1],
                    "close": [base + 0.2, base + 0.3],
                    "avg_price_1m": [base + 0.4, base + 0.5],
                }
            )

        pivot_map = {
            "open": pd.DataFrame(index=timestamps),
            "close": pd.DataFrame(index=timestamps),
            "vwap1m": pd.DataFrame(index=timestamps),
        }

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _patch_market_pivot(
                pivot_map=pivot_map,
                data_dict=data_dict,
                market_type="spot",
                changed_symbols=changed_symbols,
                removed_symbols=set(),
            )

        perf_warnings = [item for item in caught if issubclass(item.category, PerformanceWarning)]
        self.assertEqual([], perf_warnings)


if __name__ == "__main__":
    unittest.main()
