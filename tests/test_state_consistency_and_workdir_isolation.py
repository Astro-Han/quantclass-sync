import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


from quantclass_sync_internal.constants import REASON_OK, STRATEGY_MERGE_KNOWN, TIMESTAMP_FILE_NAME
from quantclass_sync_internal.models import CommandContext, ProductPlan, SyncStats
from quantclass_sync_internal import orchestrator
from quantclass_sync_internal.reporting import _new_report
from quantclass_sync_internal.status_store import write_local_timestamp


class StateConsistencyAndWorkdirIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.ctx = CommandContext(
            run_id="test-run-001",
            data_root=self.root,
            dry_run=False,
            work_dir=self.root / ".cache",
            api_key="test-api-key",
            hid="test-hid",
        )
        self.plan = ProductPlan(name="stock-trading-data", strategy=STRATEGY_MERGE_KNOWN)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_execute_plans_keeps_business_success_when_status_write_fails(self) -> None:
        report = _new_report(self.ctx.run_id, mode="network")

        def fake_process_product(
            plan: ProductPlan,
            date_time: str | None,
            api_base: str,
            hid: str,
            headers: dict[str, str],
            data_root: Path,
            work_dir: Path,
            dry_run: bool,
            run_id: str = "",
        ) -> tuple[str, str, SyncStats, str, str]:
            self.assertEqual(self.ctx.run_id, run_id)
            return plan.name, date_time or "", SyncStats(updated_files=1), "/tmp/src", REASON_OK

        with patch("quantclass_sync_internal.orchestrator.process_product", side_effect=fake_process_product), patch(
            "quantclass_sync_internal.orchestrator.process_product",
            side_effect=fake_process_product,
        ), patch(
            "quantclass_sync_internal.orchestrator._upsert_product_status_after_success",
            side_effect=RuntimeError("status write failed"),
        ):
            _total, has_error, _started_at = orchestrator._execute_plans(
                plans=[self.plan],
                command_ctx=self.ctx,
                report=report,
                requested_date_time="2026-02-11",
                conn=object(),  # 这里仅用于触发成功路径中的状态写分支
                force_update=False,
                catch_up_to_latest=False,
            )

        self.assertFalse(has_error)
        self.assertEqual(["ok"], [item.status for item in report.products])
        self.assertEqual(REASON_OK, report.products[0].reason_code)

    def test_download_and_prepare_extract_scopes_workdir_by_run_id(self) -> None:
        work_dir = self.root / ".cache"
        with patch(
            "quantclass_sync_internal.orchestrator.get_download_link",
            return_value="https://example.com/stock-trading-data/2026-02-11.zip",
        ), patch(
            "quantclass_sync_internal.orchestrator.build_file_name",
            return_value="payload.zip",
        ), patch("quantclass_sync_internal.orchestrator._download_file_atomic") as download_mock:
            download_path, extract_path = orchestrator._download_and_prepare_extract(
                product="stock-trading-data",
                actual_time="2026-02-11",
                api_base="https://api.quantclass.cn/api/data",
                hid="hid",
                headers={"api-key": "k"},
                work_dir=work_dir,
                run_id=self.ctx.run_id,
            )

        expected_base = work_dir / self.ctx.run_id / "stock-trading-data" / "2026-02-11"
        self.assertEqual(expected_base / "payload.zip", download_path)
        self.assertEqual(expected_base / "extract", extract_path)
        self.assertTrue(extract_path.is_dir())
        self.assertEqual(download_path, download_mock.call_args.kwargs["download_path"])

    def test_write_local_timestamp_uses_atomic_replace(self) -> None:
        # os.replace 现在在 config.atomic_temp_path 中调用，需 patch config 模块
        with patch("quantclass_sync_internal.config.os.replace", wraps=os.replace) as replace_mock:
            write_local_timestamp(self.root, "stock-trading-data", "2026-02-11")

        replace_mock.assert_called_once()
        src, dst = replace_mock.call_args.args
        src_path = Path(src)
        dst_path = Path(dst)
        final_path = self.root / "stock-trading-data" / TIMESTAMP_FILE_NAME

        self.assertEqual(final_path, dst_path)
        self.assertEqual(final_path.parent, src_path.parent)
        self.assertTrue(src_path.name.startswith(f".{TIMESTAMP_FILE_NAME}.tmp-"))
        self.assertFalse(src_path.exists())
        self.assertTrue(dst_path.exists())
        self.assertTrue(dst_path.read_text(encoding="utf-8").startswith("2026-02-11,"))


if __name__ == "__main__":
    unittest.main()
