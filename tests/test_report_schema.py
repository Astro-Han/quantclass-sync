import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import quantclass_sync as qcs


class ReportSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ctx(self) -> qcs.CommandContext:
        return qcs.CommandContext(
            run_id="rid-schema",
            data_root=self.root,
            dry_run=True,
        )

    def test_report_schema_31_contains_new_and_legacy_fields(self) -> None:
        report = qcs._new_report("rid-schema", mode="network")
        self.assertEqual("3.1", report.schema_version)

        qcs._append_result(
            report,
            product="stock-trading-data",
            status="ok",
            strategy=qcs.STRATEGY_MERGE_KNOWN,
            reason_code=qcs.REASON_OK,
            date_time="2026-02-11",
        )
        qcs._append_result(
            report,
            product="stock-main-index-data",
            status="skipped",
            strategy=qcs.STRATEGY_MERGE_KNOWN,
            reason_code=qcs.REASON_UP_TO_DATE,
            date_time="2026-02-11",
        )

        report_path = self.root / "run_report_schema_31.json"
        exit_code = qcs._finalize_and_write_report(
            report=report,
            total=qcs.SyncStats(),
            has_error=False,
            t_run_start=time.time() - 1.0,
            report_path=report_path,
        )
        self.assertEqual(0, exit_code)

        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual("3.1", payload["schema_version"])
        self.assertIn("reason_code_counts", payload)
        self.assertIn("phase_plan_seconds", payload)
        self.assertIn("phase_sync_seconds", payload)
        self.assertIn("phase_postprocess_seconds", payload)
        # 兼容字段仍保留
        self.assertIn("success_total", payload)
        self.assertIn("failed_total", payload)
        self.assertIn("skipped_total", payload)

        self.assertEqual(1, payload["reason_code_counts"]["ok"])
        self.assertEqual(1, payload["reason_code_counts"]["up_to_date"])
        self.assertIn("http_attempts", payload["products"][0])
        self.assertIn("http_failures", payload["products"][0])

    def test_finalize_returns_network_exit_code_for_remote_data_failure(self) -> None:
        report = qcs._new_report("rid-network", mode="network")
        qcs._append_result(
            report,
            product="stock-trading-data",
            status="error",
            strategy=qcs.STRATEGY_MERGE_KNOWN,
            reason_code=qcs.REASON_NO_DATA_FOR_DATE,
            date_time="2026-02-11",
            error="404 no data",
        )

        exit_code = qcs._finalize_and_write_report(
            report=report,
            total=qcs.SyncStats(),
            has_error=True,
            t_run_start=time.time() - 1.0,
            report_path=self.root / "run_report_network_error.json",
        )
        self.assertEqual(qcs.EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE, exit_code)

    def test_finalize_returns_general_failure_exit_code_for_non_network_error(self) -> None:
        report = qcs._new_report("rid-generic", mode="network")
        qcs._append_result(
            report,
            product="stock-trading-data",
            status="error",
            strategy=qcs.STRATEGY_MERGE_KNOWN,
            reason_code=qcs.REASON_MERGE_ERROR,
            date_time="2026-02-11",
            error="merge failed",
        )

        exit_code = qcs._finalize_and_write_report(
            report=report,
            total=qcs.SyncStats(),
            has_error=True,
            t_run_start=time.time() - 1.0,
            report_path=self.root / "run_report_generic_error.json",
        )
        self.assertEqual(qcs.EXIT_CODE_GENERAL_FAILURE, exit_code)

    def test_run_update_returns_no_executable_exit_code_when_no_local_products(self) -> None:
        ctx = self._ctx()
        with patch("quantclass_sync.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=[],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=([], [], []),
        ):
            exit_code = qcs.run_update_with_settings(
                command_ctx=ctx,
                mode="local",
                products=None,
                force_update=False,
                command_name="update",
            )

        self.assertEqual(qcs.EXIT_CODE_NO_EXECUTABLE_PRODUCTS, exit_code)

    def test_run_update_returns_no_executable_exit_code_when_plan_empty(self) -> None:
        ctx = self._ctx()
        with patch("quantclass_sync.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=(["stock-trading-data"], [], []),
        ), patch(
            "quantclass_sync_internal.orchestrator.build_product_plan",
            return_value=[],
        ):
            exit_code = qcs.run_update_with_settings(
                command_ctx=ctx,
                mode="local",
                products=None,
                force_update=False,
                command_name="update",
            )

        self.assertEqual(qcs.EXIT_CODE_NO_EXECUTABLE_PRODUCTS, exit_code)


if __name__ == "__main__":
    unittest.main()
