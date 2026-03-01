import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import quantclass_sync as qcs


class UpdateCatchUpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.product = "stock-trading-data"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ctx(self, dry_run: bool = False, stop_on_error: bool = False) -> qcs.CommandContext:
        return qcs.CommandContext(
            run_id="test-catchup",
            data_root=self.root,
            dry_run=dry_run,
            stop_on_error=stop_on_error,
        )

    def _report(self) -> qcs.RunReport:
        return qcs._new_report("test-catchup", mode="network")

    def _plan(self) -> list[qcs.ProductPlan]:
        return qcs.build_product_plan([self.product])

    def _write_local_timestamp(self, data_date: str) -> None:
        path = self.root / self.product / qcs.TIMESTAMP_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{data_date},2026-02-11 10:00:00\n", encoding="utf-8")

    def test_parse_latest_time_candidates_returns_sorted_unique_dates(self) -> None:
        raw = "2026-02-11, 2026-02-10 20260209 2026-02-10"
        self.assertEqual(
            ["2026-02-09", "2026-02-10", "2026-02-11"],
            qcs.parse_latest_time_candidates(raw),
        )

    def test_resolve_catchup_dates_probes_when_latest_only_has_latest(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        def fake_get_download_link(
            api_base: str,
            product: str,
            date_time: str,
            hid: str,
            headers: dict[str, str],
        ) -> str:
            if date_time in {"2026-02-07", "2026-02-10", "2026-02-11"}:
                return f"https://example.com/{product}/{date_time}.zip"
            raise qcs.FatalRequestError("参数错误")

        with patch("quantclass_sync.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync.get_download_link",
            side_effect=fake_get_download_link,
        ):
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-10", "2026-02-11"], queue)

    def test_resolve_catchup_dates_skips_probe_when_latest_has_multiple_dates(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch(
            "quantclass_sync.get_latest_times",
            return_value=["2026-02-07", "2026-02-08", "2026-02-09", "2026-02-10", "2026-02-11"],
        ), patch("quantclass_sync._probe_downloadable_dates") as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-09", "2026-02-10", "2026-02-11"], queue)
        probe_mock.assert_not_called()

    def test_resolve_catchup_dates_calls_probe_when_latest_is_sparse_multi_dates(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync.get_latest_times", return_value=["2026-02-07", "2026-02-11"]), patch(
            "quantclass_sync._probe_downloadable_dates",
            return_value=["2026-02-08", "2026-02-09", "2026-02-10"],
        ) as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-09", "2026-02-10", "2026-02-11"], queue)
        probe_mock.assert_called_once()

    def test_resolve_catchup_dates_calls_probe_when_latest_only_one_date(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync._probe_downloadable_dates",
            return_value=["2026-02-07", "2026-02-10", "2026-02-11"],
        ) as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-10", "2026-02-11"], queue)
        probe_mock.assert_called_once()

    def test_resolve_catchup_dates_normalizes_queue_sorted_unique(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch(
            "quantclass_sync.get_latest_times",
            return_value=["2026-02-11", "2026-02-10", "20260210", "2026-02-09"],
        ), patch("quantclass_sync._probe_downloadable_dates") as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-09", "2026-02-10", "2026-02-11"], queue)
        probe_mock.assert_not_called()

    def test_resolve_catchup_dates_non_business_product_keeps_weekend(self) -> None:
        product = "stock-fin-data-xbx"
        path = self.root / product / qcs.TIMESTAMP_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("2026-02-06,2026-02-11 10:00:00\n", encoding="utf-8")
        plan = qcs.build_product_plan([product])[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync._probe_downloadable_dates",
            return_value=["2026-02-07", "2026-02-10", "2026-02-11"],
        ) as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-07", "2026-02-10", "2026-02-11"], queue)
        probe_mock.assert_called_once()

    def test_large_gap_with_sparse_multi_latest_payload_calls_probe(self) -> None:
        self._write_local_timestamp("2025-01-01")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)
        latest_payload = ["2025-12-01", "2025-12-15", "2025-12-31"]

        with patch("quantclass_sync.get_latest_times", return_value=latest_payload), patch(
            "quantclass_sync._probe_downloadable_dates",
            return_value=["2025-12-02", "2025-12-16", "2025-12-30"],
        ) as probe_mock:
            queue, skipped = qcs._resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=True,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2025-12-01", "2025-12-02", "2025-12-15", "2025-12-16", "2025-12-30", "2025-12-31"], queue)
        probe_mock.assert_called_once()

    def test_execute_plans_catchup_stops_on_first_hard_failure(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = qcs.connect_status_db(self.root)

        executed_dates: list[str] = []

        def fake_process_product(
            plan: qcs.ProductPlan,
            date_time: str | None,
            api_base: str,
            hid: str,
            headers: dict[str, str],
            data_root: Path,
            work_dir: Path,
            dry_run: bool,
        ):
            date = date_time or ""
            executed_dates.append(date)
            if date == "2026-02-09":
                raise qcs.ProductSyncError("simulated merge failure", qcs.REASON_MERGE_ERROR)
            return plan.name, date, qcs.SyncStats(updated_files=1), "/tmp/src", qcs.REASON_OK

        try:
            with patch("quantclass_sync.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync._resolve_requested_dates_for_plan",
                return_value=(["2026-02-07", "2026-02-08", "2026-02-09", "2026-02-10"], False),
            ), patch("quantclass_sync.process_product", side_effect=fake_process_product):
                _total, has_error, _started_at = qcs._execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    requested_date_time="",
                    conn=conn,
                    force_update=False,
                    catch_up_to_latest=True,
                )
        finally:
            conn.close()

        self.assertTrue(has_error)
        self.assertEqual(["2026-02-07", "2026-02-08", "2026-02-09"], executed_dates)
        self.assertEqual(["ok", "ok", "error"], [item.status for item in report.products])
        ts_text = (self.root / self.product / qcs.TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-08,"))

    def test_execute_plans_catchup_skips_no_data_date_and_continues(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = qcs.connect_status_db(self.root)

        executed_dates: list[str] = []

        def fake_process_product(
            plan: qcs.ProductPlan,
            date_time: str | None,
            api_base: str,
            hid: str,
            headers: dict[str, str],
            data_root: Path,
            work_dir: Path,
            dry_run: bool,
        ):
            date = date_time or ""
            executed_dates.append(date)
            if date == "2026-02-09":
                raise qcs.ProductSyncError("simulated 404 no data", qcs.REASON_NO_DATA_FOR_DATE)
            return plan.name, date, qcs.SyncStats(updated_files=1), "/tmp/src", qcs.REASON_OK

        try:
            with patch("quantclass_sync.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync._resolve_requested_dates_for_plan",
                return_value=(["2026-02-07", "2026-02-09", "2026-02-10"], False),
            ), patch("quantclass_sync.process_product", side_effect=fake_process_product):
                _total, has_error, _started_at = qcs._execute_plans(
                    plans=plans,
                    command_ctx=ctx,
                    report=report,
                    requested_date_time="",
                    conn=conn,
                    force_update=False,
                    catch_up_to_latest=True,
                )
        finally:
            conn.close()

        self.assertFalse(has_error)
        self.assertEqual(["2026-02-07", "2026-02-09", "2026-02-10"], executed_dates)
        self.assertEqual(["ok", "skipped", "ok"], [item.status for item in report.products])
        self.assertEqual(qcs.REASON_NO_DATA_FOR_DATE, report.products[1].reason_code)
        ts_text = (self.root / self.product / qcs.TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-10,"))

    def test_execute_plans_stop_on_error_stops_all_products(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=True, stop_on_error=True)
        plans = [
            qcs.ProductPlan(name="stock-trading-data", strategy=qcs.STRATEGY_MERGE_KNOWN),
            qcs.ProductPlan(name="stock-main-index-data", strategy=qcs.STRATEGY_MERGE_KNOWN),
        ]
        called_products: list[str] = []

        def fake_resolve_requested_dates_for_plan(
            plan: qcs.ProductPlan,
            command_ctx: qcs.CommandContext,
            hid: str,
            headers: dict[str, str],
            requested_date_time: str,
            force_update: bool,
            report: qcs.RunReport,
            t_product_start: float,
            catch_up_to_latest: bool = False,
        ) -> tuple[list[str], bool]:
            return (["2026-02-09"], False)

        def fake_process_product(
            plan: qcs.ProductPlan,
            date_time: str | None,
            api_base: str,
            hid: str,
            headers: dict[str, str],
            data_root: Path,
            work_dir: Path,
            dry_run: bool,
        ):
            called_products.append(plan.name)
            if plan.name == "stock-trading-data":
                raise qcs.ProductSyncError("simulated merge failure", qcs.REASON_MERGE_ERROR)
            return plan.name, date_time or "", qcs.SyncStats(updated_files=1), "/tmp/src", qcs.REASON_OK

        with patch("quantclass_sync.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
            "quantclass_sync._resolve_requested_dates_for_plan",
            side_effect=fake_resolve_requested_dates_for_plan,
        ), patch("quantclass_sync.process_product", side_effect=fake_process_product):
            _total, has_error, _started_at = qcs._execute_plans(
                plans=plans,
                command_ctx=ctx,
                report=report,
                requested_date_time="",
                conn=None,
                force_update=False,
                catch_up_to_latest=False,
            )

        self.assertTrue(has_error)
        self.assertEqual(["stock-trading-data"], called_products)
        self.assertEqual(["error"], [item.status for item in report.products])


if __name__ == "__main__":
    unittest.main()
