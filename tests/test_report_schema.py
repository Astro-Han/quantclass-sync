import json
import tempfile
import time
import unittest
from pathlib import Path

import quantclass_sync as qcs


class ReportSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

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


if __name__ == "__main__":
    unittest.main()
