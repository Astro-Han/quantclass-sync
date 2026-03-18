import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from quantclass_sync_internal.config import build_product_plan
from quantclass_sync_internal.constants import REASON_MERGE_ERROR, REASON_NO_DATA_FOR_DATE, REASON_NO_VALID_OUTPUT, REASON_OK, STRATEGY_MERGE_KNOWN, TIMESTAMP_FILE_NAME
from quantclass_sync_internal.http_client import parse_latest_time_candidates
from quantclass_sync_internal.models import CommandContext, EmptyDownloadLinkError, FatalRequestError, ProductPlan, ProductSyncError, RunReport, SyncStats
from quantclass_sync_internal.orchestrator import _execute_plans, _resolve_requested_dates_for_plan
from quantclass_sync_internal.reporting import _new_report
from quantclass_sync_internal.status_store import _SOURCE_API_CHECK, connect_status_db


class UpdateCatchUpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.product = "stock-trading-data"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ctx(self, dry_run: bool = False, stop_on_error: bool = False) -> CommandContext:
        return CommandContext(
            run_id="test-catchup",
            data_root=self.root,
            dry_run=dry_run,
            stop_on_error=stop_on_error,
        )

    def _report(self) -> RunReport:
        return _new_report("test-catchup", mode="network")

    def _plan(self) -> list[ProductPlan]:
        return build_product_plan([self.product])

    def _write_local_timestamp(self, data_date: str) -> None:
        path = self.root / self.product / TIMESTAMP_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{data_date},2026-02-11 10:00:00\n", encoding="utf-8")

    def test_parse_latest_time_candidates_returns_sorted_unique_dates(self) -> None:
        raw = "2026-02-11, 2026-02-10 20260209 2026-02-10"
        self.assertEqual(
            ["2026-02-09", "2026-02-10", "2026-02-11"],
            parse_latest_time_candidates(raw),
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
            raise FatalRequestError("参数错误")

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync_internal.orchestrator.get_download_link",
            side_effect=fake_get_download_link,
        ):
            queue, skipped = _resolve_requested_dates_for_plan(
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
            "quantclass_sync_internal.orchestrator.get_latest_times",
            return_value=["2026-02-07", "2026-02-08", "2026-02-09", "2026-02-10", "2026-02-11"],
        ), patch("quantclass_sync_internal.orchestrator._probe_downloadable_dates") as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-07", "2026-02-11"]), patch(
            "quantclass_sync_internal.orchestrator._probe_downloadable_dates",
            return_value=["2026-02-08", "2026-02-09", "2026-02-10"],
        ) as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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
        probe_mock.assert_called_once_with(
            api_base=ctx.api_base.rstrip("/"),
            product=self.product,
            hid="hid",
            headers={"api-key": "k"},
            local_date="2026-02-06",
            latest_date="2026-02-11",
        )

    def test_resolve_catchup_dates_calls_probe_when_latest_only_one_date(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync_internal.orchestrator._probe_downloadable_dates",
            return_value=["2026-02-07", "2026-02-10", "2026-02-11"],
        ) as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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
        probe_mock.assert_called_once_with(
            api_base=ctx.api_base.rstrip("/"),
            product=self.product,
            hid="hid",
            headers={"api-key": "k"},
            local_date="2026-02-06",
            latest_date="2026-02-11",
        )

    def test_resolve_catchup_dates_normalizes_queue_sorted_unique(self) -> None:
        self._write_local_timestamp("2026-02-06")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch(
            "quantclass_sync_internal.orchestrator.get_latest_times",
            return_value=["2026-02-11", "2026-02-10", "20260210", "2026-02-09"],
        ), patch("quantclass_sync_internal.orchestrator._probe_downloadable_dates") as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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
        path = self.root / product / TIMESTAMP_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("2026-02-06,2026-02-11 10:00:00\n", encoding="utf-8")
        plan = build_product_plan([product])[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync_internal.orchestrator._probe_downloadable_dates",
            return_value=["2026-02-07", "2026-02-10", "2026-02-11"],
        ) as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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

    def test_non_catchup_latest_keeps_weekend_candidate(self) -> None:
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)
        # 2026-02-07 是周六；非 catch-up 模式应保持 latest 原语义，不在这里做周末裁剪。
        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-07"]):
            queue, skipped = _resolve_requested_dates_for_plan(
                plan=plan,
                command_ctx=ctx,
                hid="hid",
                headers={"api-key": "k"},
                requested_date_time="",
                force_update=False,
                report=report,
                t_product_start=time.time(),
                catch_up_to_latest=False,
            )

        self.assertFalse(skipped)
        self.assertEqual(["2026-02-07"], queue)

    def test_empty_download_link_is_no_data_error(self) -> None:
        from quantclass_sync_internal.orchestrator import _is_no_data_error

        # EmptyDownloadLinkError 应被识别为"无数据"错误
        self.assertTrue(_is_no_data_error(EmptyDownloadLinkError("empty")))

    def test_resolve_catchup_dates_gate_error_falls_back_to_latest(self) -> None:
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-02-11"]), patch(
            "quantclass_sync_internal.orchestrator.read_local_timestamp_date",
            side_effect=RuntimeError("broken timestamp"),
        ):
            queue, skipped = _resolve_requested_dates_for_plan(
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
        self.assertEqual(["2026-02-11"], queue)

    def test_large_gap_with_sparse_multi_latest_payload_calls_probe(self) -> None:
        self._write_local_timestamp("2025-01-01")
        plan = self._plan()[0]
        report = self._report()
        ctx = self._ctx(dry_run=True)
        latest_payload = ["2025-12-01", "2025-12-15", "2025-12-31"]

        with patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=latest_payload), patch(
            "quantclass_sync_internal.orchestrator._probe_downloadable_dates",
            return_value=["2025-12-02", "2025-12-16", "2025-12-30"],
        ) as probe_mock:
            queue, skipped = _resolve_requested_dates_for_plan(
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

    def test_execute_plans_status_write_failure_keeps_ok_result(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = connect_status_db(self.root)

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
        ):
            return plan.name, date_time or "", SyncStats(updated_files=1), "/tmp/src", REASON_OK

        try:
            with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
                return_value=(["2026-02-09"], False),
            ), patch(
                "quantclass_sync_internal.orchestrator.process_product",
                side_effect=fake_process_product,
            ), patch(
                "quantclass_sync_internal.orchestrator._upsert_product_status_after_success",
                side_effect=RuntimeError("status write failed"),
            ):
                total, has_error, _started_at = _execute_plans(
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
        self.assertEqual(1, total.updated_files)
        self.assertEqual(["ok"], [item.status for item in report.products])
        ts_text = (self.root / self.product / TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-06,"))

    def test_execute_plans_no_valid_output_does_not_advance_timestamp(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = connect_status_db(self.root)

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
        ):
            return (
                plan.name,
                date_time or "",
                SyncStats(skipped_files=1),
                "/tmp/src",
                REASON_NO_VALID_OUTPUT,
            )

        try:
            with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
                return_value=(["2026-02-09"], False),
            ), patch(
                "quantclass_sync_internal.orchestrator.process_product",
                side_effect=fake_process_product,
            ):
                total, has_error, _started_at = _execute_plans(
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
        self.assertEqual(1, total.skipped_files)
        self.assertEqual(["skipped"], [item.status for item in report.products])
        self.assertEqual(REASON_NO_VALID_OUTPUT, report.products[0].reason_code)
        ts_text = (self.root / self.product / TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-06,"))

    def test_execute_plans_catchup_stops_on_first_hard_failure(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = connect_status_db(self.root)

        executed_dates: list[str] = []

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
        ):
            date = date_time or ""
            executed_dates.append(date)
            if date == "2026-02-09":
                raise ProductSyncError("simulated merge failure", REASON_MERGE_ERROR)
            return plan.name, date, SyncStats(updated_files=1), "/tmp/src", REASON_OK

        try:
            with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
                return_value=(["2026-02-07", "2026-02-08", "2026-02-09", "2026-02-10"], False),
            ), patch("quantclass_sync_internal.orchestrator.process_product", side_effect=fake_process_product):
                _total, has_error, _started_at = _execute_plans(
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
        ts_text = (self.root / self.product / TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-08,"))

    def test_execute_plans_catchup_skips_no_data_date_and_continues(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=False)
        plans = self._plan()
        conn = connect_status_db(self.root)

        executed_dates: list[str] = []

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
        ):
            date = date_time or ""
            executed_dates.append(date)
            if date == "2026-02-09":
                raise ProductSyncError("simulated 404 no data", REASON_NO_DATA_FOR_DATE)
            return plan.name, date, SyncStats(updated_files=1), "/tmp/src", REASON_OK

        try:
            with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
                "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
                return_value=(["2026-02-07", "2026-02-09", "2026-02-10"], False),
            ), patch("quantclass_sync_internal.orchestrator.process_product", side_effect=fake_process_product):
                _total, has_error, _started_at = _execute_plans(
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
        self.assertEqual(REASON_NO_DATA_FOR_DATE, report.products[1].reason_code)
        ts_text = (self.root / self.product / TIMESTAMP_FILE_NAME).read_text(encoding="utf-8")
        self.assertTrue(ts_text.startswith("2026-02-10,"))

    def test_execute_plans_catchup_empty_queue_records_skip_and_continues_next_product(self) -> None:
        report = self._report()
        ctx = self._ctx(dry_run=True)
        plans = [
            ProductPlan(name="stock-trading-data", strategy=STRATEGY_MERGE_KNOWN),
            ProductPlan(name="stock-main-index-data", strategy=STRATEGY_MERGE_KNOWN),
        ]
        executed: list[tuple[str, str]] = []

        def fake_resolve_requested_dates_for_plan(
            plan: ProductPlan,
            command_ctx: CommandContext,
            hid: str,
            headers: dict[str, str],
            requested_date_time: str,
            force_update: bool,
            report: RunReport,
            t_product_start: float,
            catch_up_to_latest: bool = False,
            lock=None,
            **kwargs,
        ) -> tuple[list[str], bool]:
            if plan.name == "stock-trading-data":
                return ([], False)
            return (["2026-02-10"], False)

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
        ):
            executed.append((plan.name, date_time or ""))
            return plan.name, date_time or "", SyncStats(updated_files=1), "/tmp/src", REASON_OK

        with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
            "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
            side_effect=fake_resolve_requested_dates_for_plan,
        ), patch("quantclass_sync_internal.orchestrator.process_product", side_effect=fake_process_product):
            _total, has_error, _started_at = _execute_plans(
                plans=plans,
                command_ctx=ctx,
                report=report,
                requested_date_time="",
                conn=None,
                force_update=False,
                catch_up_to_latest=True,
            )

        self.assertFalse(has_error)
        self.assertEqual([("stock-main-index-data", "2026-02-10")], executed)
        self.assertEqual(["skipped", "ok"], [item.status for item in report.products])
        self.assertEqual(REASON_NO_DATA_FOR_DATE, report.products[0].reason_code)

    def test_execute_plans_stop_on_error_stops_all_products(self) -> None:
        self._write_local_timestamp("2026-02-06")
        report = self._report()
        ctx = self._ctx(dry_run=True, stop_on_error=True)
        plans = [
            ProductPlan(name="stock-trading-data", strategy=STRATEGY_MERGE_KNOWN),
            ProductPlan(name="stock-main-index-data", strategy=STRATEGY_MERGE_KNOWN),
        ]
        called_products: list[str] = []

        def fake_resolve_requested_dates_for_plan(
            plan: ProductPlan,
            command_ctx: CommandContext,
            hid: str,
            headers: dict[str, str],
            requested_date_time: str,
            force_update: bool,
            report: RunReport,
            t_product_start: float,
            catch_up_to_latest: bool = False,
            lock=None,
            **kwargs,
        ) -> tuple[list[str], bool]:
            return (["2026-02-09"], False)

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
        ):
            called_products.append(plan.name)
            if plan.name == "stock-trading-data":
                raise ProductSyncError("simulated merge failure", REASON_MERGE_ERROR)
            return plan.name, date_time or "", SyncStats(updated_files=1), "/tmp/src", REASON_OK

        with patch("quantclass_sync_internal.orchestrator.build_headers_or_raise", return_value=({"api-key": "k"}, "hid")), patch(
            "quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan",
            side_effect=fake_resolve_requested_dates_for_plan,
        ), patch("quantclass_sync_internal.orchestrator.process_product", side_effect=fake_process_product):
            _total, has_error, _started_at = _execute_plans(
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


class TestApiDateCache(unittest.TestCase):
    """_resolve_requested_dates_for_plan 应优先使用缓存。"""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _ctx(self):
        return CommandContext(
            run_id="test", data_root=self.root,
            dry_run=False, stop_on_error=False,
        )

    def _plan(self, name="stock-trading-data"):
        return build_product_plan([name])[0]

    def _report(self):
        return _new_report("test", mode="network")

    @patch("quantclass_sync_internal.orchestrator.should_skip_by_timestamp", return_value=True)
    @patch("quantclass_sync_internal.orchestrator.get_latest_times")
    def test_cache_hit_skips_http(self, mock_get_latest, mock_skip):
        """缓存新鲜时不调用 get_latest_times。"""
        from datetime import datetime
        fresh_checked_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        cache = {"stock-trading-data": ("2026-03-18", fresh_checked_at)}
        queue, skipped = _resolve_requested_dates_for_plan(
            plan=self._plan(), command_ctx=self._ctx(),
            hid="hid", headers={"api-key": "k"},
            requested_date_time="", force_update=False,
            report=self._report(), t_product_start=time.time(),
            api_date_cache=cache,
        )
        mock_get_latest.assert_not_called()

    @patch("quantclass_sync_internal.orchestrator.should_skip_by_timestamp", return_value=True)
    @patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-03-18"])
    def test_cache_expired_falls_through(self, mock_get_latest, mock_skip):
        """缓存过期时回退 HTTP。"""
        from datetime import datetime, timedelta
        from quantclass_sync_internal.constants import API_DATE_CACHE_TTL_SECONDS
        expired_time = (datetime.now() - timedelta(seconds=API_DATE_CACHE_TTL_SECONDS + 60))
        old_checked_at = expired_time.strftime("%Y-%m-%dT%H:%M:%S")
        cache = {"stock-trading-data": ("2026-03-18", old_checked_at)}
        _resolve_requested_dates_for_plan(
            plan=self._plan(), command_ctx=self._ctx(),
            hid="hid", headers={"api-key": "k"},
            requested_date_time="", force_update=False,
            report=self._report(), t_product_start=time.time(),
            api_date_cache=cache,
        )
        mock_get_latest.assert_called_once()

    @patch("quantclass_sync_internal.orchestrator.should_skip_by_timestamp", return_value=True)
    @patch("quantclass_sync_internal.orchestrator.get_latest_times", return_value=["2026-03-18"])
    def test_cache_miss_falls_through(self, mock_get_latest, mock_skip):
        """产品不在缓存中时回退 HTTP。"""
        _resolve_requested_dates_for_plan(
            plan=self._plan(), command_ctx=self._ctx(),
            hid="hid", headers={"api-key": "k"},
            requested_date_time="", force_update=False,
            report=self._report(), t_product_start=time.time(),
            api_date_cache={},
        )
        mock_get_latest.assert_called_once()


class TestPrefetchApiDates(unittest.TestCase):
    """_prefetch_api_dates 并发预取 API 日期。"""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        # log_dir 路径与 report_dir_path(data_root) 保持一致：data_root/.quantclass_sync/log
        self.log_dir = self.root / ".quantclass_sync" / "log"
        self.log_dir.mkdir(parents=True)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _ctx(self) -> CommandContext:
        """构造测试用 CommandContext，api_base 设为 http://fake。"""
        return CommandContext(
            run_id="test",
            data_root=self.root,
            dry_run=False,
            stop_on_error=False,
            api_base="http://fake",
        )

    @patch("quantclass_sync_internal.orchestrator.get_latest_time")
    def test_fetches_uncached_products(self, mock_get):
        """无缓存时并发调用 get_latest_time。"""
        mock_get.return_value = "2026-03-18"
        from quantclass_sync_internal.orchestrator import _prefetch_api_dates
        cache = _prefetch_api_dates(
            products=["prod-a", "prod-b"],
            command_ctx=self._ctx(), hid="hid", headers={},
        )
        self.assertEqual(mock_get.call_count, 2)
        self.assertIn("prod-a", cache)
        self.assertIn("prod-b", cache)

    @patch("quantclass_sync_internal.orchestrator.get_latest_time")
    def test_skips_fresh_cache(self, mock_get):
        """缓存新鲜时不调用 HTTP。"""
        from quantclass_sync_internal.orchestrator import _prefetch_api_dates
        from quantclass_sync_internal.status_store import update_api_latest_dates
        # 预先写入新鲜缓存
        update_api_latest_dates(self.log_dir, {"prod-a": "2026-03-18"})
        cache = _prefetch_api_dates(
            products=["prod-a"],
            command_ctx=self._ctx(), hid="hid", headers={},
        )
        mock_get.assert_not_called()
        self.assertIn("prod-a", cache)

    @patch("quantclass_sync_internal.orchestrator.get_latest_time")
    def test_partial_cache_only_fetches_missing(self, mock_get):
        """部分缓存命中时只查询缺失的产品。"""
        mock_get.return_value = "2026-03-17"
        from quantclass_sync_internal.orchestrator import _prefetch_api_dates
        from quantclass_sync_internal.status_store import update_api_latest_dates
        update_api_latest_dates(self.log_dir, {"prod-a": "2026-03-18"})
        cache = _prefetch_api_dates(
            products=["prod-a", "prod-b"],
            command_ctx=self._ctx(), hid="hid", headers={},
        )
        # 只查了 prod-b
        self.assertEqual(mock_get.call_count, 1)
        self.assertIn("prod-a", cache)
        self.assertIn("prod-b", cache)

    @patch("quantclass_sync_internal.orchestrator.get_latest_time", side_effect=Exception("network"))
    def test_failure_returns_partial_cache(self, mock_get):
        """预取失败时返回已有缓存，不阻断流程。"""
        from quantclass_sync_internal.orchestrator import _prefetch_api_dates
        cache = _prefetch_api_dates(
            products=["prod-a"],
            command_ctx=self._ctx(), hid="hid", headers={},
        )
        # 失败但不抛异常，返回空缓存
        self.assertEqual(cache, {})

    @patch("quantclass_sync_internal.orchestrator.get_latest_time")
    def test_expired_cache_refetches(self, mock_get):
        """缓存过期时重新查询。"""
        from datetime import datetime, timedelta
        from quantclass_sync_internal.orchestrator import _prefetch_api_dates
        from quantclass_sync_internal.constants import API_DATE_CACHE_TTL_SECONDS
        # 手动写入过期缓存
        expired_time = (datetime.now() - timedelta(seconds=API_DATE_CACHE_TTL_SECONDS + 60))
        status_path = self.log_dir / "product_last_status.json"
        import json
        status_path.write_text(json.dumps({
            "prod-a": {
                "date_time": "2026-03-17",
                "checked_at": expired_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "source": _SOURCE_API_CHECK,
            }
        }))
        mock_get.return_value = "2026-03-18"
        cache = _prefetch_api_dates(
            products=["prod-a"],
            command_ctx=self._ctx(), hid="hid", headers={},
        )
        mock_get.assert_called_once()  # 过期缓存触发重查
        self.assertEqual(cache["prod-a"][0], "2026-03-18")  # 返回新日期


class TestEstimateSyncWorkload(unittest.TestCase):
    """_estimate_sync_workload 预估 API 调用量。"""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _write_timestamp(self, product: str, data_date: str) -> None:
        """写入产品 timestamp.txt。"""
        path = self.root / product / TIMESTAMP_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{data_date},2026-02-11 10:00:00\n", encoding="utf-8")

    def _plan(self, product: str) -> "ProductPlan":
        return build_product_plan([product])[0]

    def test_no_gap_no_confirm(self):
        """所有产品本地日期等于 API 日期时，gap=0，needs_confirm=False。"""
        from quantclass_sync_internal.orchestrator import _estimate_sync_workload
        product = "stock-trading-data"
        self._write_timestamp(product, "2026-03-18")
        # API 日期与本地日期相同，gap=0
        api_date_cache = {"stock-trading-data": ("2026-03-18", "2026-03-18T10:00:00")}
        plans = [self._plan(product)]
        result = _estimate_sync_workload(plans, api_date_cache, self.root, api_call_limit=10)
        self.assertEqual(result.total_calls, 0)
        self.assertFalse(result.needs_confirm)
        # gap=0 的产品不计入 products 列表
        self.assertEqual(len(result.products), 0)

    def test_gap_over_limit_needs_confirm(self):
        """落后天数超阈值时 needs_confirm=True。"""
        from quantclass_sync_internal.orchestrator import _estimate_sync_workload
        product = "stock-trading-data"
        self._write_timestamp(product, "2026-03-03")  # 落后 15 天
        api_date_cache = {"stock-trading-data": ("2026-03-18", "2026-03-18T10:00:00")}
        plans = [self._plan(product)]
        result = _estimate_sync_workload(plans, api_date_cache, self.root, api_call_limit=10)
        self.assertEqual(result.total_calls, 15)
        self.assertTrue(result.needs_confirm)
        self.assertEqual(len(result.products), 1)
        self.assertEqual(result.products[0]["gap_days"], 15)
        self.assertEqual(result.products[0]["estimated_calls"], 15)

    def test_no_timestamp_counts_as_one(self):
        """无 timestamp 且无可推断 CSV 时，预估为 1 次（仅 latest）。"""
        from quantclass_sync_internal.orchestrator import _estimate_sync_workload
        product = "stock-trading-data"
        # 不写 timestamp，产品目录也不存在 CSV
        api_date_cache = {"stock-trading-data": ("2026-03-18", "2026-03-18T10:00:00")}
        plans = [self._plan(product)]
        result = _estimate_sync_workload(plans, api_date_cache, self.root, api_call_limit=10)
        self.assertEqual(result.total_calls, 1)
        self.assertEqual(len(result.products), 1)
        self.assertEqual(result.products[0]["estimated_calls"], 1)
        self.assertEqual(result.products[0]["local_date"], "")

    def test_no_api_cache_counts_as_one(self):
        """产品无 API 缓存日期时，预估为 1 次。"""
        from quantclass_sync_internal.orchestrator import _estimate_sync_workload
        product = "stock-trading-data"
        self._write_timestamp(product, "2026-03-18")
        # 空缓存，该产品没有 API 日期
        api_date_cache: dict = {}
        plans = [self._plan(product)]
        result = _estimate_sync_workload(plans, api_date_cache, self.root, api_call_limit=10)
        self.assertEqual(result.total_calls, 1)
        self.assertEqual(len(result.products), 1)
        self.assertEqual(result.products[0]["api_date"], "")
        self.assertEqual(result.products[0]["estimated_calls"], 1)


if __name__ == "__main__":
    unittest.main()
