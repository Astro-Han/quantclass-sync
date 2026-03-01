import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple
from unittest.mock import patch

import pandas as pd

from coin_preprocess_internal.constants import FRAME_COLUMNS
from coin_preprocess_internal.csv_source import _prepare_symbol_frame
from coin_preprocess_internal.runner import _overlap_matches_existing, _resolve_source_delta
from coin_preprocess_builtin import (
    OUTPUT_PIVOT_SPOT,
    OUTPUT_PIVOT_SWAP,
    OUTPUT_SPOT_DICT,
    OUTPUT_SWAP_DICT,
    PREPROCESS_PRODUCT,
    SPOT_PRODUCT,
    SWAP_PRODUCT,
    TIMESTAMP_FILE_NAME,
    _patch_market_pivot,
    _rebuild_source_symbol,
    _run_incremental_patch,
    run_coin_preprocess_builtin,
)


class CoinPreprocessBuiltinTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / SPOT_PRODUCT).mkdir(parents=True, exist_ok=True)
        (self.root / SWAP_PRODUCT).mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_symbol_csv(self, product: str, symbol: str, rows: List[Tuple[str, float, float, float, float, float]], is_swap: bool) -> None:
        if is_swap:
            title = "备注,,,,,,,,,,,,,,"
            header = (
                "candle_begin_time,open,high,low,close,volume,quote_volume,trade_num,"
                "taker_buy_base_asset_volume,taker_buy_quote_asset_volume,Spread,symbol,"
                "avg_price_1m,avg_price_5m,fundingRate"
            )
        else:
            title = "备注,,,,,,,,,,,,,"
            header = (
                "candle_begin_time,open,high,low,close,volume,quote_volume,trade_num,"
                "taker_buy_base_asset_volume,taker_buy_quote_asset_volume,Spread,symbol,"
                "avg_price_1m,avg_price_5m"
            )

        body = [title, header]
        for ts, open_p, high_p, low_p, close_p, volume in rows:
            base = (
                f"{ts},{open_p},{high_p},{low_p},{close_p},{volume},{volume * close_p},2,"
                f"{volume / 2},{(volume * close_p) / 2},,{symbol},{open_p},{close_p}"
            )
            if is_swap:
                base = f"{base},0.0001"
            body.append(base)
        body.append("")

        (self.root / product / f"{symbol}.csv").write_text("\n".join(body), encoding="utf-8")

    def _append_symbol_row(
        self,
        product: str,
        symbol: str,
        row: Tuple[str, float, float, float, float, float],
        is_swap: bool,
    ) -> None:
        path = self.root / product / f"{symbol}.csv"
        text = path.read_text(encoding="utf-8").rstrip("\n")
        ts, open_p, high_p, low_p, close_p, volume = row
        line = (
            f"{ts},{open_p},{high_p},{low_p},{close_p},{volume},{volume * close_p},2,"
            f"{volume / 2},{(volume * close_p) / 2},,{symbol},{open_p},{close_p}"
        )
        if is_swap:
            line = f"{line},0.0002"
        path.write_text(text + "\n" + line + "\n", encoding="utf-8")

    def _prepare_basic_dual_side(self) -> None:
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "AAA-USDT",
            [
                ("2026-02-09 00:00:00", 1.0, 2.0, 0.9, 1.5, 10),
                ("2026-02-09 01:00:00", 1.5, 2.2, 1.4, 2.0, 20),
            ],
            is_swap=False,
        )
        self._write_symbol_csv(
            SWAP_PRODUCT,
            "BBB-USDT",
            [
                ("2026-02-09 00:00:00", 3.0, 3.2, 2.8, 3.1, 50),
                ("2026-02-09 01:00:00", 3.1, 3.4, 3.0, 3.3, 55),
            ],
            is_swap=True,
        )

    def _write_runtime_timestamp(self, dt: datetime) -> None:
        output_dir = self.root / PREPROCESS_PRODUCT
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / TIMESTAMP_FILE_NAME).write_text(
            f"2026-02-09,{dt.strftime('%Y-%m-%d %H:%M:%S')}\n",
            encoding="utf-8",
        )

    def test_baseline_missing_runs_full_rebuild(self) -> None:
        self._prepare_basic_dual_side()

        summary = run_coin_preprocess_builtin(self.root)

        self.assertEqual("full_rebuild", summary.mode)
        self.assertEqual(1, summary.spot_symbols)
        self.assertEqual(1, summary.swap_symbols)
        output_dir = self.root / PREPROCESS_PRODUCT
        self.assertTrue((output_dir / OUTPUT_SPOT_DICT).exists())
        self.assertTrue((output_dir / OUTPUT_SWAP_DICT).exists())
        self.assertTrue((output_dir / OUTPUT_PIVOT_SPOT).exists())
        self.assertTrue((output_dir / OUTPUT_PIVOT_SWAP).exists())

    def test_incremental_patch_appends_single_symbol(self) -> None:
        self._prepare_basic_dual_side()
        first = run_coin_preprocess_builtin(self.root)
        self.assertEqual("full_rebuild", first.mode)

        baseline = datetime.now()
        self._write_runtime_timestamp(baseline)
        self._append_symbol_row(
            SPOT_PRODUCT,
            "AAA-USDT",
            ("2026-02-09 02:00:00", 2.0, 2.4, 1.9, 2.2, 30),
            is_swap=False,
        )
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 2, time.time() + 2))

        second = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", second.mode)

        output_dir = self.root / PREPROCESS_PRODUCT
        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        self.assertIn("AAA-USDT", spot_dict)
        self.assertEqual(pd.Timestamp("2026-02-09 02:00:00"), pd.to_datetime(spot_dict["AAA-USDT"]["candle_begin_time"].max()))

    def test_mtime_changed_without_new_rows_rebuilds_symbol(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now() + timedelta(hours=1))
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 7200, time.time() + 7200))

        with patch("coin_preprocess_builtin._rebuild_source_symbol", wraps=_rebuild_source_symbol) as rebuild_mock:
            summary = run_coin_preprocess_builtin(self.root)

        self.assertEqual("incremental_patch", summary.mode)
        rebuild_mock.assert_called_once()
        self.assertEqual("AAA-USDT", rebuild_mock.call_args.kwargs["source_symbol"])

    def test_mtime_changed_without_new_rows_rebuilds_on_overlap_diff(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now())
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "AAA-USDT",
            [
                ("2026-02-09 00:00:00", 1.0, 2.0, 0.9, 1.5, 10),
                ("2026-02-09 01:00:00", 1.5, 2.2, 1.4, 9.9, 20),
            ],
            is_swap=False,
        )
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 2, time.time() + 2))

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", summary.mode)

        spot_dict = pd.read_pickle(self.root / PREPROCESS_PRODUCT / OUTPUT_SPOT_DICT)
        close_at_0100 = (
            spot_dict["AAA-USDT"]
            .set_index("candle_begin_time")
            .loc[pd.Timestamp("2026-02-09 01:00:00"), "close"]
        )
        self.assertEqual(9.9, float(close_at_0100))

    def test_symbol_deletion_removes_keys(self) -> None:
        self._prepare_basic_dual_side()
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "CCC-USDT",
            [("2026-02-09 00:00:00", 5.0, 5.1, 4.8, 4.9, 12)],
            is_swap=False,
        )
        self._write_symbol_csv(
            SWAP_PRODUCT,
            "DDD-USDT",
            [("2026-02-09 00:00:00", 6.0, 6.1, 5.8, 5.9, 12)],
            is_swap=True,
        )
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now())
        (self.root / SPOT_PRODUCT / "AAA-USDT.csv").unlink()

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", summary.mode)

        output_dir = self.root / PREPROCESS_PRODUCT
        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        self.assertNotIn("AAA-USDT", spot_dict)
        self.assertIn("CCC-USDT", spot_dict)

    def test_relist_new_gap_triggers_single_symbol_rebuild(self) -> None:
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "LUNA-USDT",
            [
                ("2026-02-01 00:00:00", 1.0, 1.1, 0.9, 1.0, 10),
                ("2026-02-01 01:00:00", 1.0, 1.1, 0.9, 1.0, 11),
            ],
            is_swap=False,
        )
        self._write_symbol_csv(
            SWAP_PRODUCT,
            "LUNA2-USDT",
            [
                ("2026-02-01 00:00:00", 2.0, 2.1, 1.9, 2.0, 20),
                ("2026-02-01 01:00:00", 2.0, 2.1, 1.9, 2.0, 21),
            ],
            is_swap=True,
        )
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now())
        self._append_symbol_row(
            SPOT_PRODUCT,
            "LUNA-USDT",
            ("2026-02-03 00:00:00", 1.6, 1.7, 1.5, 1.6, 12),
            is_swap=False,
        )
        self._append_symbol_row(
            SWAP_PRODUCT,
            "LUNA2-USDT",
            ("2026-02-03 00:00:00", 3.2, 3.3, 3.1, 3.2, 22),
            is_swap=True,
        )

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", summary.mode)

        output_dir = self.root / PREPROCESS_PRODUCT
        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        swap_dict = pd.read_pickle(output_dir / OUTPUT_SWAP_DICT)
        self.assertIn("LUNA_SP0-USDT", spot_dict)
        self.assertIn("LUNA-USDT", spot_dict)
        self.assertIn("LUNA2_SW0-USDT", swap_dict)
        self.assertIn("LUNA2-USDT", swap_dict)

    def test_next_open_zero_does_not_skip_relist_detection(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now())
        self._append_symbol_row(
            SPOT_PRODUCT,
            "AAA-USDT",
            ("2026-02-11 00:00:00", 0.0, 0.1, 0.0, 0.0, 18),
            is_swap=False,
        )
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 2, time.time() + 2))

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", summary.mode)

        spot_dict = pd.read_pickle(self.root / PREPROCESS_PRODUCT / OUTPUT_SPOT_DICT)
        self.assertIn("AAA_SP0-USDT", spot_dict)
        self.assertIn("AAA-USDT", spot_dict)

    def test_internal_relist_break_in_new_raw_triggers_single_symbol_rebuild(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)

        self._write_runtime_timestamp(datetime.now() + timedelta(hours=1))
        self._append_symbol_row(
            SPOT_PRODUCT,
            "AAA-USDT",
            ("2026-02-09 02:00:00", 2.0, 2.2, 1.9, 2.0, 18),
            is_swap=False,
        )
        self._append_symbol_row(
            SPOT_PRODUCT,
            "AAA-USDT",
            ("2026-02-11 03:00:00", 3.2, 3.4, 3.1, 3.3, 18),
            is_swap=False,
        )
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 7200, time.time() + 7200))

        with patch("coin_preprocess_builtin._rebuild_source_symbol", wraps=_rebuild_source_symbol) as rebuild_mock:
            summary = run_coin_preprocess_builtin(self.root)

        self.assertEqual("incremental_patch", summary.mode)
        rebuild_mock.assert_called_once()
        self.assertEqual("AAA-USDT", rebuild_mock.call_args.kwargs["source_symbol"])

        spot_dict = pd.read_pickle(self.root / PREPROCESS_PRODUCT / OUTPUT_SPOT_DICT)
        self.assertIn("AAA_SP0-USDT", spot_dict)
        self.assertIn("AAA-USDT", spot_dict)

    def test_incremental_failure_fallback_to_full_rebuild(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)
        self._write_runtime_timestamp(datetime.now())

        with patch("coin_preprocess_builtin._run_incremental_patch", side_effect=RuntimeError("inject_fail")):
            summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("fallback_full_rebuild", summary.mode)

    def test_incremental_and_rebuild_fail_keeps_incremental_as_cause(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)
        self._write_runtime_timestamp(datetime.now())

        with patch("coin_preprocess_builtin._run_incremental_patch", side_effect=RuntimeError("incremental_fail")), patch(
            "coin_preprocess_builtin._run_full_rebuild",
            side_effect=RuntimeError("rebuild_fail"),
        ):
            with self.assertRaises(RuntimeError) as cm:
                run_coin_preprocess_builtin(self.root)
        self.assertIsNotNone(cm.exception.__cause__)
        self.assertIn("incremental_fail", str(cm.exception.__cause__))
        self.assertIn("rebuild_fail", str(cm.exception))

    def test_resolve_source_delta_logs_warning_on_stat_error(self) -> None:
        class _BadPath:
            def stat(self):
                raise OSError("stat-failed")

        baseline_runtime = pd.Timestamp("2026-02-10 00:00:00")
        with patch("coin_preprocess_internal.runner.LOGGER.warning") as warn_mock:
            changed, removed = _resolve_source_delta(
                source_files={"AAA-USDT": _BadPath()},
                baseline_runtime=baseline_runtime,
                baseline_sources={"AAA-USDT"},
            )
        self.assertEqual({"AAA-USDT"}, changed)
        self.assertEqual(set(), removed)
        warn_mock.assert_called_once()

    def test_baseline_read_exception_logs_warning_then_full_rebuild(self) -> None:
        self._prepare_basic_dual_side()

        with patch("coin_preprocess_builtin._load_existing_baseline", side_effect=RuntimeError("boom")), patch(
            "coin_preprocess_builtin.LOGGER.warning"
        ) as warn_mock:
            summary = run_coin_preprocess_builtin(self.root)

        self.assertEqual("full_rebuild", summary.mode)
        warn_mock.assert_called_once()
        self.assertEqual("RuntimeError", warn_mock.call_args.args[1])

    def test_overlap_matches_existing_true_for_identical_rows(self) -> None:
        overlap_raw = pd.DataFrame(
            {
                "candle_begin_time": [
                    "bad-time",
                    "2026-02-09 00:00:00",
                    "2026-02-09 01:00:00",
                    "2026-02-09 01:00:00",
                ],
                "open": [1.0, 1.0, 2.0, 2.0],
                "close": [1.5, 1.5, 2.5, 2.5],
                "avg_price_1m": [1.2, 1.2, 2.2, 2.2],
            }
        )
        overlap_snapshot = pd.DataFrame(
            {
                "open": [1.0, 2.0],
                "close": [1.5, 2.5],
                "avg_price_1m": [1.2, 2.2],
            },
            index=pd.to_datetime(["2026-02-09 00:00:00", "2026-02-09 01:00:00"]),
        )

        matched = _overlap_matches_existing(overlap_raw=overlap_raw, overlap_snapshot=overlap_snapshot, is_swap=False)
        self.assertTrue(matched)

    def test_overlap_matches_existing_false_when_row_has_no_comparable_values(self) -> None:
        overlap_raw = pd.DataFrame(
            {
                "candle_begin_time": ["2026-02-09 00:00:00", "2026-02-09 01:00:00"],
                "open": [1.0, float("nan")],
                "close": [1.5, float("nan")],
                "avg_price_1m": [1.2, float("nan")],
            }
        )
        overlap_snapshot = pd.DataFrame(
            {
                "open": [1.0, 2.0],
                "close": [1.5, 2.5],
                "avg_price_1m": [1.2, 2.2],
            },
            index=pd.to_datetime(["2026-02-09 00:00:00", "2026-02-09 01:00:00"]),
        )

        matched = _overlap_matches_existing(overlap_raw=overlap_raw, overlap_snapshot=overlap_snapshot, is_swap=False)
        self.assertFalse(matched)

    def test_corrupted_baseline_pickle_falls_back_to_full_rebuild(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)
        self._write_runtime_timestamp(datetime.now())

        output_dir = self.root / PREPROCESS_PRODUCT
        (output_dir / OUTPUT_SPOT_DICT).write_text("corrupted_pickle_payload", encoding="utf-8")

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("full_rebuild", summary.mode)

        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        self.assertIn("AAA-USDT", spot_dict)

    def test_spot_only_fails_and_keeps_existing_swap_pickle(self) -> None:
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "AAA-USDT",
            [("2026-02-09 00:00:00", 1.0, 2.0, 0.9, 1.5, 10)],
            is_swap=False,
        )
        swap_dir = self.root / SWAP_PRODUCT
        for path in swap_dir.glob("*"):
            path.unlink()
        swap_dir.rmdir()

        output_dir = self.root / PREPROCESS_PRODUCT
        output_dir.mkdir(parents=True, exist_ok=True)
        old_swap_payload = {"keep": "old_swap_payload"}
        pd.to_pickle(old_swap_payload, output_dir / OUTPUT_SWAP_DICT)

        with self.assertRaises(RuntimeError):
            run_coin_preprocess_builtin(self.root)

        current_swap_payload = pd.read_pickle(output_dir / OUTPUT_SWAP_DICT)
        self.assertEqual(old_swap_payload, current_swap_payload)

    def test_swap_only_fails_and_keeps_existing_spot_pickle(self) -> None:
        self._write_symbol_csv(
            SWAP_PRODUCT,
            "BBB-USDT",
            [("2026-02-09 00:00:00", 3.0, 3.2, 2.8, 3.1, 50)],
            is_swap=True,
        )
        spot_dir = self.root / SPOT_PRODUCT
        for path in spot_dir.glob("*"):
            path.unlink()
        spot_dir.rmdir()

        output_dir = self.root / PREPROCESS_PRODUCT
        output_dir.mkdir(parents=True, exist_ok=True)
        old_spot_payload = {"keep": "old_spot_payload"}
        pd.to_pickle(old_spot_payload, output_dir / OUTPUT_SPOT_DICT)

        with self.assertRaises(RuntimeError):
            run_coin_preprocess_builtin(self.root)

        current_spot_payload = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        self.assertEqual(old_spot_payload, current_spot_payload)

    def test_missing_columns_are_filled_without_all_nan(self) -> None:
        raw_df = pd.DataFrame(
            {
                "candle_begin_time": pd.to_datetime(["2026-02-09 00:00:00", "2026-02-09 01:00:00"]),
                "close": [1.2, 1.3],
            }
        )
        normalized = _prepare_symbol_frame(raw_df, symbol="MISS-USDT", is_swap=False)

        self.assertFalse(normalized["open"].isna().all())
        self.assertFalse(normalized["high"].isna().all())
        self.assertFalse(normalized["low"].isna().all())
        self.assertFalse(normalized["avg_price_1m"].isna().all())
        self.assertTrue((normalized["open"] == normalized["close"]).all())
        self.assertTrue((normalized["high"] == normalized["close"]).all())
        self.assertTrue((normalized["low"] == normalized["close"]).all())

    def test_active_key_selection_tolerates_nat(self) -> None:
        self._prepare_basic_dual_side()
        run_coin_preprocess_builtin(self.root)

        output_dir = self.root / PREPROCESS_PRODUCT
        spot_dict = pd.read_pickle(output_dir / OUTPUT_SPOT_DICT)
        dirty_frame = spot_dict["AAA-USDT"].head(1).copy()
        dirty_frame["candle_begin_time"] = "bad-time-value"
        spot_dict["AAA_SP9-USDT"] = dirty_frame[FRAME_COLUMNS]
        pd.to_pickle(spot_dict, output_dir / OUTPUT_SPOT_DICT)

        self._write_runtime_timestamp(datetime.now())
        self._append_symbol_row(
            SPOT_PRODUCT,
            "AAA-USDT",
            ("2026-02-09 02:00:00", 2.0, 2.4, 1.9, 2.2, 30),
            is_swap=False,
        )
        target = self.root / SPOT_PRODUCT / "AAA-USDT.csv"
        os.utime(target, (time.time() + 2, time.time() + 2))

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("incremental_patch", summary.mode)

    def test_dirty_time_values_are_tolerated(self) -> None:
        self._write_symbol_csv(
            SPOT_PRODUCT,
            "AAA-USDT",
            [
                ("2026-02-09 00:00:00", 1.0, 2.0, 0.9, 1.5, 10),
                ("dirty-time-value", 1.0, 1.1, 0.9, 1.0, 11),
                ("2026-02-09 01:00:00", 1.5, 2.2, 1.4, 2.0, 20),
            ],
            is_swap=False,
        )
        self._write_symbol_csv(
            SWAP_PRODUCT,
            "BBB-USDT",
            [("2026-02-09 00:00:00", 3.0, 3.2, 2.8, 3.1, 50)],
            is_swap=True,
        )

        summary = run_coin_preprocess_builtin(self.root)
        self.assertEqual("full_rebuild", summary.mode)

        spot_dict = pd.read_pickle(self.root / PREPROCESS_PRODUCT / OUTPUT_SPOT_DICT)
        frame = spot_dict["AAA-USDT"]
        parsed = pd.to_datetime(frame["candle_begin_time"], errors="coerce")
        self.assertFalse(parsed.isna().any())
        self.assertEqual(pd.Timestamp("2026-02-09 01:00:00"), parsed.max())

    def test_patch_market_pivot_extends_index_for_new_timestamp(self) -> None:
        ts_base = pd.to_datetime(["2026-02-09 00:00:00", "2026-02-09 01:00:00"])
        ts_new = pd.to_datetime(["2026-02-09 01:00:00", "2026-02-09 02:00:00"])

        pivot_map = {
            "open": pd.DataFrame(
                {
                    "AAA-USDT": [1.0, 1.1],
                    "BBB-USDT": [2.0, 2.1],
                    "REMOVED-USDT": [9.0, 9.1],
                },
                index=ts_base,
            ),
            "close": pd.DataFrame(
                {
                    "AAA-USDT": [1.5, 1.6],
                    "BBB-USDT": [2.5, 2.6],
                    "REMOVED-USDT": [9.5, 9.6],
                },
                index=ts_base,
            ),
            "vwap1m": pd.DataFrame(
                {
                    "AAA-USDT": [1.3, 1.4],
                    "BBB-USDT": [2.3, 2.4],
                    "REMOVED-USDT": [9.3, 9.4],
                },
                index=ts_base,
            ),
        }

        data_dict = {
            "BBB-USDT": pd.DataFrame(
                {
                    "candle_begin_time": ts_new,
                    "open": [3.0, 3.1],
                    "close": [3.5, 3.6],
                    "avg_price_1m": [3.3, 3.4],
                }
            ),
            "CCC-USDT": pd.DataFrame(
                {
                    "candle_begin_time": ts_new,
                    "open": [4.0, 4.1],
                    "close": [4.5, 4.6],
                    "avg_price_1m": [4.3, 4.4],
                }
            ),
            "EMPTY-USDT": pd.DataFrame(),
        }

        changed_symbols = {"BBB-USDT", "CCC-USDT", "EMPTY-USDT"}
        removed_symbols = {"REMOVED-USDT", "MISS-USDT"}

        actual = _patch_market_pivot(
            pivot_map={key: value.copy() for key, value in pivot_map.items()},
            data_dict=data_dict,
            market_type="spot",
            changed_symbols=changed_symbols,
            removed_symbols=removed_symbols,
        )

        for key in ("open", "close", "vwap1m"):
            self.assertIn(pd.Timestamp("2026-02-09 02:00:00"), actual[key].index)
            self.assertNotIn("REMOVED-USDT", actual[key].columns)

        open_pivot = actual["open"]
        self.assertEqual(3.1, float(open_pivot.loc[pd.Timestamp("2026-02-09 02:00:00"), "BBB-USDT"]))
        self.assertEqual(4.1, float(open_pivot.loc[pd.Timestamp("2026-02-09 02:00:00"), "CCC-USDT"]))

    def test_atomic_commit_rolls_back_when_replace_fails(self) -> None:
        self._prepare_basic_dual_side()
        output_dir = self.root / PREPROCESS_PRODUCT
        output_dir.mkdir(parents=True, exist_ok=True)

        old_spot = {"old": "spot"}
        old_swap = {"old": "swap"}
        old_pivot_spot = {"old": "pivot_spot"}
        old_pivot_swap = {"old": "pivot_swap"}
        pd.to_pickle(old_spot, output_dir / OUTPUT_SPOT_DICT)
        pd.to_pickle(old_swap, output_dir / OUTPUT_SWAP_DICT)
        pd.to_pickle(old_pivot_spot, output_dir / OUTPUT_PIVOT_SPOT)
        pd.to_pickle(old_pivot_swap, output_dir / OUTPUT_PIVOT_SWAP)

        real_replace = os.replace

        def flaky_replace(src: str, dst: str) -> None:
            src_path = Path(src)
            dst_path = Path(dst)
            if dst_path.name == OUTPUT_SWAP_DICT and ".tmp-" in src_path.name:
                raise RuntimeError("inject_replace_failure")
            real_replace(src, dst)

        with patch("coin_preprocess_builtin.os.replace", side_effect=flaky_replace):
            with self.assertRaises(RuntimeError):
                run_coin_preprocess_builtin(self.root)

        self.assertEqual(old_spot, pd.read_pickle(output_dir / OUTPUT_SPOT_DICT))
        self.assertEqual(old_swap, pd.read_pickle(output_dir / OUTPUT_SWAP_DICT))
        self.assertEqual(old_pivot_spot, pd.read_pickle(output_dir / OUTPUT_PIVOT_SPOT))
        self.assertEqual(old_pivot_swap, pd.read_pickle(output_dir / OUTPUT_PIVOT_SWAP))


if __name__ == "__main__":
    unittest.main()
