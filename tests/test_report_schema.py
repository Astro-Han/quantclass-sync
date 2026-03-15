import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from quantclass_sync_internal.constants import EXIT_CODE_GENERAL_FAILURE, EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE, EXIT_CODE_NO_EXECUTABLE_PRODUCTS, REASON_MERGE_ERROR, REASON_NO_DATA_FOR_DATE, REASON_OK, REASON_UP_TO_DATE, STRATEGY_MERGE_KNOWN
from quantclass_sync_internal.models import CommandContext, ProductPlan, SyncStats
from quantclass_sync_internal.orchestrator import run_update_with_settings
from quantclass_sync_internal.reporting import _append_result, _finalize_and_write_report, _new_report, write_run_report


class ReportSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ctx(self) -> CommandContext:
        return CommandContext(
            run_id="rid-schema",
            data_root=self.root,
            dry_run=True,
        )

    def test_report_schema_31_contains_new_and_legacy_fields(self) -> None:
        report = _new_report("rid-schema", mode="network")
        self.assertEqual("3.1", report.schema_version)

        _append_result(
            report,
            product="stock-trading-data",
            status="ok",
            strategy=STRATEGY_MERGE_KNOWN,
            reason_code=REASON_OK,
            date_time="2026-02-11",
        )
        _append_result(
            report,
            product="stock-main-index-data",
            status="skipped",
            strategy=STRATEGY_MERGE_KNOWN,
            reason_code=REASON_UP_TO_DATE,
            date_time="2026-02-11",
        )

        report_path = self.root / "run_report_schema_31.json"
        exit_code = _finalize_and_write_report(
            report=report,
            total=SyncStats(),
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
        report = _new_report("rid-network", mode="network")
        _append_result(
            report,
            product="stock-trading-data",
            status="error",
            strategy=STRATEGY_MERGE_KNOWN,
            reason_code=REASON_NO_DATA_FOR_DATE,
            date_time="2026-02-11",
            error="404 no data",
        )

        exit_code = _finalize_and_write_report(
            report=report,
            total=SyncStats(),
            has_error=True,
            t_run_start=time.time() - 1.0,
            report_path=self.root / "run_report_network_error.json",
        )
        self.assertEqual(EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE, exit_code)

    def test_finalize_returns_general_failure_exit_code_for_non_network_error(self) -> None:
        report = _new_report("rid-generic", mode="network")
        _append_result(
            report,
            product="stock-trading-data",
            status="error",
            strategy=STRATEGY_MERGE_KNOWN,
            reason_code=REASON_MERGE_ERROR,
            date_time="2026-02-11",
            error="merge failed",
        )

        exit_code = _finalize_and_write_report(
            report=report,
            total=SyncStats(),
            has_error=True,
            t_run_start=time.time() - 1.0,
            report_path=self.root / "run_report_generic_error.json",
        )
        self.assertEqual(EXIT_CODE_GENERAL_FAILURE, exit_code)

    def test_finalize_rounds_duration_seconds_to_two_decimals(self) -> None:
        report = _new_report("rid-round", mode="network")
        report_path = self.root / "run_report_rounding.json"
        with patch("quantclass_sync_internal.reporting.time.time", return_value=100.127):
            _finalize_and_write_report(
                report=report,
                total=SyncStats(),
                has_error=False,
                t_run_start=100.0,
                report_path=report_path,
            )

        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(0.13, payload["duration_seconds"])

    def test_write_run_report_replace_failure_keeps_original_file_and_cleans_temp(self) -> None:
        report_path = self.root / "run_report_atomic.json"
        report_path.write_text('{"old":"value"}', encoding="utf-8")
        before = report_path.read_text(encoding="utf-8")
        report = _new_report("rid-atomic", mode="network")

        # os.replace 现在在 config.atomic_temp_path 中调用，需 patch config 模块
        with patch("quantclass_sync_internal.config.os.replace", side_effect=RuntimeError("replace failed")):
            with self.assertRaises(RuntimeError):
                write_run_report(report_path, report)

        self.assertEqual(before, report_path.read_text(encoding="utf-8"))
        self.assertEqual([], list(report_path.parent.glob(f".{report_path.name}.tmp-*")))

    def test_decide_exit_code_report_none_with_error_returns_general_failure(self) -> None:
        """report=None + has_error=True 时应返回通用错误码（1），而非网络错误码（2）。"""

        from quantclass_sync_internal.reporting import (
            EXIT_CODE_GENERAL_FAILURE,
            EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE,
            decide_exit_code,
        )

        code = decide_exit_code(report=None, has_error=True)
        self.assertEqual(code, EXIT_CODE_GENERAL_FAILURE)
        self.assertNotEqual(code, EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE)

    def test_run_update_returns_no_executable_exit_code_when_no_local_products(self) -> None:
        ctx = self._ctx()
        with patch("quantclass_sync_internal.orchestrator.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=[],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=([], [], []),
        ):
            exit_code = run_update_with_settings(
                command_ctx=ctx,
                mode="local",
                products=None,
                force_update=False,
                command_name="update",
            )

        self.assertEqual(EXIT_CODE_NO_EXECUTABLE_PRODUCTS, exit_code)

    def test_run_update_returns_no_executable_exit_code_when_plan_empty(self) -> None:
        ctx = self._ctx()
        with patch("quantclass_sync_internal.orchestrator.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=(["stock-trading-data"], [], []),
        ), patch(
            "quantclass_sync_internal.orchestrator.build_product_plan",
            return_value=[],
        ):
            exit_code = run_update_with_settings(
                command_ctx=ctx,
                mode="local",
                products=None,
                force_update=False,
                command_name="update",
            )

        self.assertEqual(EXIT_CODE_NO_EXECUTABLE_PRODUCTS, exit_code)

    def test_run_update_exports_status_json_in_finally_when_postprocess_raises(self) -> None:
        ctx = self._ctx().model_copy(update={"dry_run": False})
        plan = ProductPlan(name="stock-trading-data", strategy=STRATEGY_MERGE_KNOWN)

        with patch("quantclass_sync_internal.orchestrator.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=(["stock-trading-data"], [], []),
        ), patch(
            "quantclass_sync_internal.orchestrator.build_product_plan",
            return_value=[plan],
        ), patch(
            "quantclass_sync_internal.orchestrator._execute_plans",
            return_value=(SyncStats(), False, time.time() - 1.0),
        ), patch(
            "quantclass_sync_internal.orchestrator._maybe_run_coin_preprocess",
            side_effect=RuntimeError("postprocess boom"),
        ), patch(
            "quantclass_sync_internal.orchestrator.export_status_json",
        ) as export_mock:
            with self.assertRaises(RuntimeError):
                run_update_with_settings(
                    command_ctx=ctx,
                    mode="local",
                    products=[],
                    force_update=False,
                    command_name="update",
                )

        export_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
