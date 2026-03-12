"""data_query 模块测试：产品状态总览、运行摘要、历史记录。"""

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from quantclass_sync_internal.data_query import (
    _days_behind,
    _status_color,
    get_latest_run_summary,
    get_products_overview,
    get_run_history,
)


class TestDaysBehind(unittest.TestCase):
    """落后天数计算。"""

    def test_same_day(self):
        self.assertEqual(_days_behind("2026-03-13", today=date(2026, 3, 13)), 0)

    def test_one_day_behind(self):
        self.assertEqual(_days_behind("2026-03-12", today=date(2026, 3, 13)), 1)

    def test_many_days_behind(self):
        self.assertEqual(_days_behind("2026-03-01", today=date(2026, 3, 13)), 12)

    def test_none_date(self):
        self.assertIsNone(_days_behind(None))

    def test_empty_string(self):
        self.assertIsNone(_days_behind(""))

    def test_invalid_format(self):
        self.assertIsNone(_days_behind("not-a-date"))

    def test_future_date_clamped_to_zero(self):
        """本地日期在"今天"之后，落后天数应为 0 而非负数。"""
        self.assertEqual(_days_behind("2026-03-15", today=date(2026, 3, 13)), 0)


class TestStatusColor(unittest.TestCase):
    """状态颜色判定。"""

    def test_green_zero_days(self):
        self.assertEqual(_status_color(0, "ok"), "green")

    def test_yellow_one_day(self):
        self.assertEqual(_status_color(1, "ok"), "yellow")

    def test_yellow_three_days(self):
        self.assertEqual(_status_color(3, "ok"), "yellow")

    def test_red_four_days(self):
        self.assertEqual(_status_color(4, "ok"), "red")

    def test_red_on_error(self):
        """上次失败时即使 days_behind 为 0，也应为红色。"""
        self.assertEqual(_status_color(0, "error"), "red")

    def test_gray_no_data(self):
        self.assertEqual(_status_color(None, ""), "gray")

    def test_gray_no_data_even_with_ok(self):
        self.assertEqual(_status_color(None, "ok"), "gray")


class TestGetProductsOverview(unittest.TestCase):
    """产品状态总览集成测试。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name) / "data"
        self.data_root.mkdir()
        # 创建 log 目录（report_dir_path 返回脚本目录下的 log，
        # 但我们的测试直接写 timestamp + report）
        self.log_dir = Path(self.tmpdir.name) / "log"
        self.log_dir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_timestamp(self, product: str, date_str: str):
        """写入产品的 timestamp.txt。"""
        product_dir = self.data_root / product
        product_dir.mkdir(parents=True, exist_ok=True)
        (product_dir / "timestamp.txt").write_text(f"{date_str},2026-03-13 22:00:00\n")

    def _write_report(self, filename: str, products: list):
        """写入一个 run_report JSON。"""
        report = {
            "schema_version": "3.1",
            "run_id": "test",
            "started_at": "2026-03-13T00:00:00Z",
            "mode": "network",
            "success_total": sum(1 for p in products if p.get("status") == "ok"),
            "failed_total": sum(1 for p in products if p.get("status") == "error"),
            "skipped_total": sum(1 for p in products if p.get("status") == "skipped"),
            "duration_seconds": 100.0,
            "products": products,
        }
        (self.log_dir / filename).write_text(
            json.dumps(report, ensure_ascii=False), encoding="utf-8"
        )

    def test_basic_overview(self):
        """有 timestamp + 有报告的基本场景。"""
        self._write_timestamp("stock-trading-data", "2026-03-13")
        self._write_timestamp("coin-cap", "2026-03-10")
        self._write_report("run_report_20260313_update.json", [
            {"product": "stock-trading-data", "status": "ok", "reason_code": "ok", "error": ""},
            {"product": "coin-cap", "status": "ok", "reason_code": "ok", "error": ""},
        ])

        # 用 mock 让 report_dir_path 返回我们的 log_dir
        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            overview = get_products_overview(
                self.data_root,
                ["stock-trading-data", "coin-cap", "new-product"],
                today=date(2026, 3, 13),
            )

        self.assertEqual(len(overview), 3)
        # stock-trading-data: 最新，绿色
        self.assertEqual(overview[0]["name"], "stock-trading-data")
        self.assertEqual(overview[0]["days_behind"], 0)
        self.assertEqual(overview[0]["status_color"], "green")
        # coin-cap: 落后 3 天，黄色
        self.assertEqual(overview[1]["name"], "coin-cap")
        self.assertEqual(overview[1]["days_behind"], 3)
        self.assertEqual(overview[1]["status_color"], "yellow")
        # new-product: 无 timestamp，灰色
        self.assertEqual(overview[2]["name"], "new-product")
        self.assertIsNone(overview[2]["local_date"])
        self.assertEqual(overview[2]["status_color"], "gray")

    def test_overview_with_error_product(self):
        """产品上次失败时应为红色。"""
        self._write_timestamp("stock-fin-data-xbx", "2026-03-13")
        self._write_report("run_report_20260313_update.json", [
            {"product": "stock-fin-data-xbx", "status": "error", "reason_code": "network_error", "error": "连接超时"},
        ])

        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            overview = get_products_overview(
                self.data_root,
                ["stock-fin-data-xbx"],
                today=date(2026, 3, 13),
            )

        self.assertEqual(overview[0]["status_color"], "red")
        self.assertEqual(overview[0]["last_error"], "连接超时")

    def test_overview_no_reports(self):
        """没有 run_report 时仍能返回基本信息。"""
        self._write_timestamp("stock-trading-data", "2026-03-12")

        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            overview = get_products_overview(
                self.data_root,
                ["stock-trading-data"],
                today=date(2026, 3, 13),
            )

        self.assertEqual(overview[0]["days_behind"], 1)
        self.assertEqual(overview[0]["last_status"], "")
        self.assertEqual(overview[0]["status_color"], "yellow")


class TestGetLatestRunSummary(unittest.TestCase):
    """最新运行摘要。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.log_dir = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_no_reports(self):
        self.assertIsNone(get_latest_run_summary(self.log_dir))

    def test_basic_summary(self):
        report = {
            "run_id": "20260313-220000",
            "started_at": "2026-03-13T14:00:00Z",
            "ended_at": "2026-03-13T14:05:00Z",
            "duration_seconds": 300.0,
            "success_total": 38,
            "failed_total": 2,
            "skipped_total": 2,
            "products": [
                {"product": "stock-a", "status": "ok"},
                {"product": "stock-b", "status": "error", "error": "超时", "reason_code": "network_error"},
                {"product": "stock-c", "status": "error", "error": "无权限", "reason_code": "fatal_request"},
            ],
        }
        (self.log_dir / "run_report_20260313_update.json").write_text(
            json.dumps(report), encoding="utf-8"
        )

        summary = get_latest_run_summary(self.log_dir)
        self.assertIsNotNone(summary)
        self.assertEqual(summary["run_id"], "20260313-220000")
        self.assertEqual(summary["success_total"], 38)
        self.assertEqual(summary["failed_total"], 2)
        self.assertEqual(len(summary["failed_products"]), 2)
        self.assertEqual(summary["failed_products"][0]["product"], "stock-b")

    def test_picks_latest_report(self):
        """应取文件名排序最新的报告。"""
        for i, ts in enumerate(["20260311", "20260313", "20260312"]):
            report = {
                "run_id": f"run-{ts}",
                "started_at": "",
                "ended_at": "",
                "duration_seconds": 0,
                "success_total": i,
                "failed_total": 0,
                "skipped_total": 0,
                "products": [],
            }
            (self.log_dir / f"run_report_{ts}_update.json").write_text(
                json.dumps(report), encoding="utf-8"
            )

        summary = get_latest_run_summary(self.log_dir)
        self.assertEqual(summary["run_id"], "run-20260313")


class TestGetRunHistory(unittest.TestCase):
    """运行历史列表。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.log_dir = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_empty(self):
        self.assertEqual(get_run_history(self.log_dir), [])

    def test_returns_n_most_recent(self):
        """返回最近 N 条，按时间降序。"""
        for i in range(5):
            ts = f"2026031{i}"
            report = {
                "run_id": f"run-{ts}",
                "started_at": f"2026-03-1{i}T00:00:00Z",
                "duration_seconds": 100 + i,
                "success_total": 10 + i,
                "failed_total": i,
                "skipped_total": 0,
                "products": [],
            }
            (self.log_dir / f"run_report_{ts}_update.json").write_text(
                json.dumps(report), encoding="utf-8"
            )

        history = get_run_history(self.log_dir, n=3)
        self.assertEqual(len(history), 3)
        # 降序：最新的在前
        self.assertEqual(history[0]["run_id"], "run-20260314")
        self.assertEqual(history[1]["run_id"], "run-20260313")
        self.assertEqual(history[2]["run_id"], "run-20260312")

    def test_corrupt_file_skipped(self):
        """损坏的 JSON 文件应被跳过。"""
        (self.log_dir / "run_report_20260313_update.json").write_text("not json")
        report = {"run_id": "ok", "started_at": "", "duration_seconds": 0,
                  "success_total": 1, "failed_total": 0, "skipped_total": 0, "products": []}
        (self.log_dir / "run_report_20260314_update.json").write_text(
            json.dumps(report), encoding="utf-8"
        )

        history = get_run_history(self.log_dir)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["run_id"], "ok")


if __name__ == "__main__":
    unittest.main()
