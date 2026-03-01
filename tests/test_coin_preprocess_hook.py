import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import quantclass_sync as qcs


class CoinPreprocessHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ctx(self, dry_run: bool = False) -> qcs.CommandContext:
        return qcs.CommandContext(
            run_id="test-run",
            data_root=self.root,
            dry_run=dry_run,
        )

    def _stats(self, updated: int = 1, created: int = 0, rows_added: int = 0) -> qcs.SyncStats:
        return qcs.SyncStats(
            created_files=created,
            updated_files=updated,
            unchanged_files=0,
            skipped_files=0,
            rows_added=rows_added,
        )

    def _report_with_source_success(
        self,
        product: str = "coin-binance-candle-csv-1h",
        stats: qcs.SyncStats | None = None,
    ) -> qcs.RunReport:
        report = qcs._new_report("test-run", mode="network")
        report.products.append(
            qcs.ProductRunResult(
                product=product,
                status="ok",
                strategy="merge_known",
                reason_code=qcs.REASON_OK,
                date_time="2026-02-09",
                mode="network",
                elapsed_seconds=1.0,
                stats=stats or self._stats(),
            )
        )
        return report

    def _ensure_preprocess_dir(self) -> None:
        (self.root / qcs.PREPROCESS_PRODUCT).mkdir(parents=True, exist_ok=True)

    def test_skip_when_target_dir_missing(self) -> None:
        report = self._report_with_source_success()
        with patch("quantclass_sync._run_builtin_coin_preprocess") as builtin_mock:
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertFalse(has_error)
        builtin_mock.assert_not_called()
        preprocess_results = [x for x in report.products if x.product == qcs.PREPROCESS_PRODUCT]
        self.assertEqual([], preprocess_results)

    def test_skip_when_no_source_success(self) -> None:
        self._ensure_preprocess_dir()
        report = qcs._new_report("test-run", mode="network")
        with patch("quantclass_sync._run_builtin_coin_preprocess") as builtin_mock:
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertFalse(has_error)
        builtin_mock.assert_not_called()
        preprocess_results = [x for x in report.products if x.product == qcs.PREPROCESS_PRODUCT]
        self.assertEqual([], preprocess_results)

    def test_coin_cap_success_only_does_not_trigger(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(product="coin-cap", stats=self._stats(updated=1))
        with patch("quantclass_sync._run_builtin_coin_preprocess") as builtin_mock:
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertFalse(has_error)
        builtin_mock.assert_not_called()
        preprocess_results = [x for x in report.products if x.product == qcs.PREPROCESS_PRODUCT]
        self.assertEqual([], preprocess_results)

    def test_skip_when_source_success_but_no_effective_delta(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=0, created=0, rows_added=0))
        with patch("quantclass_sync._run_builtin_coin_preprocess") as builtin_mock:
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertFalse(has_error)
        builtin_mock.assert_not_called()
        result = report.products[-1]
        self.assertEqual("skipped", result.status)
        self.assertEqual(qcs.REASON_PREPROCESS_SKIPPED_NO_DELTA, result.reason_code)

    def test_run_success_updates_status_and_timestamp(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=2))
        conn = qcs.connect_status_db(self.root)
        try:
            with patch(
                "quantclass_sync._run_builtin_coin_preprocess",
                return_value=("builtin(mode=incremental)", qcs.REASON_PREPROCESS_INCREMENTAL_OK),
            ) as builtin_mock:
                has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=conn)
            self.assertFalse(has_error)
            builtin_mock.assert_called_once()
            result = report.products[-1]
            self.assertEqual(qcs.PREPROCESS_PRODUCT, result.product)
            self.assertEqual("ok", result.status)
            self.assertEqual(qcs.REASON_PREPROCESS_INCREMENTAL_OK, result.reason_code)
            self.assertEqual("2026-02-09", result.date_time)

            timestamp = self.root / qcs.PREPROCESS_PRODUCT / qcs.TIMESTAMP_FILE_NAME
            self.assertTrue(timestamp.exists())
            self.assertTrue(timestamp.read_text(encoding="utf-8").startswith("2026-02-09,"))

            status = qcs.load_product_status(conn, qcs.PREPROCESS_PRODUCT)
            self.assertIsNotNone(status)
            self.assertEqual("2026-02-09", status.data_time)
        finally:
            conn.close()

    def test_dry_run_only_record_skip(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=1))
        with patch("quantclass_sync._run_builtin_coin_preprocess") as builtin_mock:
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(dry_run=True), report, conn=None)
        self.assertFalse(has_error)
        builtin_mock.assert_not_called()
        result = report.products[-1]
        self.assertEqual("skipped", result.status)
        self.assertEqual(qcs.REASON_PREPROCESS_DRY_RUN, result.reason_code)

    def test_error_when_builtin_preprocess_failed(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=1))
        with patch("quantclass_sync._run_builtin_coin_preprocess", side_effect=RuntimeError("builtin failed")):
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertTrue(has_error)
        result = report.products[-1]
        self.assertEqual("error", result.status)
        self.assertEqual(qcs.REASON_PREPROCESS_FAILED, result.reason_code)
        self.assertIn("builtin failed", result.error)

    def test_fallback_full_reason_is_recorded(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=3))
        with patch(
            "quantclass_sync._run_builtin_coin_preprocess",
            return_value=("builtin(mode=fallback)", qcs.REASON_PREPROCESS_FALLBACK_FULL_OK),
        ):
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertFalse(has_error)
        result = report.products[-1]
        self.assertEqual("ok", result.status)
        self.assertEqual(qcs.REASON_PREPROCESS_FALLBACK_FULL_OK, result.reason_code)

    def test_error_when_builtin_preprocess_returns_non_tuple(self) -> None:
        self._ensure_preprocess_dir()
        report = self._report_with_source_success(stats=self._stats(updated=1))
        with patch("quantclass_sync._run_builtin_coin_preprocess", return_value="legacy-command"):
            has_error = qcs._maybe_run_coin_preprocess(self._ctx(), report, conn=None)
        self.assertTrue(has_error)
        result = report.products[-1]
        self.assertEqual("error", result.status)
        self.assertEqual(qcs.REASON_PREPROCESS_FAILED, result.reason_code)
        self.assertTrue(result.error)

    def test_run_report_contains_at_most_one_preprocess_record(self) -> None:
        preprocess_dir = self.root / qcs.PREPROCESS_PRODUCT
        preprocess_dir.mkdir(parents=True, exist_ok=True)
        (preprocess_dir / "placeholder.csv").write_text("a,b\n1,2\n", encoding="utf-8")

        source_dir = self.root / "coin-binance-candle-csv-1h"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "source.csv").write_text("a,b\n1,2\n", encoding="utf-8")

        captured_reports = []

        def fake_write_run_report(_path: Path, report: qcs.RunReport) -> None:
            captured_reports.append(report)

        def fake_execute_plans(*args, **_kwargs):
            report = args[2]
            report.products.append(
                qcs.ProductRunResult(
                    product="coin-binance-candle-csv-1h",
                    status="ok",
                    strategy="merge_known",
                    reason_code=qcs.REASON_OK,
                    date_time="2026-02-10",
                    mode="network",
                    elapsed_seconds=1.0,
                    stats=qcs.SyncStats(updated_files=1),
                )
            )
            return qcs.SyncStats(), False, time.time()

        with patch("quantclass_sync.load_catalog_or_raise", return_value=["coin-binance-candle-csv-1h"]), patch(
            "quantclass_sync._execute_plans",
            side_effect=fake_execute_plans,
        ), patch("quantclass_sync.write_run_report", side_effect=fake_write_run_report):
            exit_code = qcs.run_update_with_settings(
                command_ctx=self._ctx(dry_run=True),
                mode="local",
                products=None,
                force_update=False,
                command_name="update",
            )

        self.assertEqual(0, exit_code)
        self.assertTrue(captured_reports)
        report = captured_reports[-1]
        preprocess_rows = [x for x in report.products if x.product == qcs.PREPROCESS_PRODUCT]
        self.assertEqual(1, len(preprocess_rows))
        self.assertEqual(qcs.REASON_PREPROCESS_DRY_RUN, preprocess_rows[0].reason_code)


if __name__ == "__main__":
    unittest.main()
