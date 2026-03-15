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
    get_run_detail,
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
        # log_dir 与 data_root 分离：report_dir_path 在生产中返回脚本目录下的 log，
        # 测试通过 mock report_dir_path 指向这里，避免依赖真实路径结构。
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
        """写入一个 run_report JSON，同时更新 product_last_status.json。"""
        report = {
            "schema_version": "3.2",
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
        # 模拟 _update_product_last_status 的累积写入
        status_path = self.log_dir / "product_last_status.json"
        existing = {}
        if status_path.exists():
            try:
                existing = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        for item in products:
            name = item.get("product", "")
            if name:
                existing[name] = {
                    "status": item.get("status", ""),
                    "reason_code": item.get("reason_code", ""),
                    "error": item.get("error", ""),
                    "date_time": item.get("date_time", ""),
                }
        status_path.write_text(
            json.dumps(existing, ensure_ascii=False), encoding="utf-8"
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
        # coin-cap: 落后 3 天，黄色（status=ok 但无 date_time，降级用 today 比较）
        self.assertEqual(overview[1]["name"], "coin-cap")
        self.assertEqual(overview[1]["days_behind"], 3)
        self.assertEqual(overview[1]["status_color"], "yellow")
        # new-product: 无 timestamp，灰色
        self.assertEqual(overview[2]["name"], "new-product")
        self.assertIsNone(overview[2]["local_date"])
        self.assertEqual(overview[2]["status_color"], "gray")

    def test_overview_uses_api_date_instead_of_today(self):
        """已追平 API 时应显示绿色，即使 today 在 API 日期之后（周末/假日）。

        回归测试 issue #1：落后天数应对比 API 最新日期而非 date.today()。
        """
        # 产品在周五(3/13)已同步到 API 最新
        self._write_timestamp("stock-trading-data", "2026-03-13")
        self._write_report("run_report_20260313_update.json", [
            {"product": "stock-trading-data", "status": "ok", "reason_code": "ok",
             "error": "", "date_time": "2026-03-13"},
        ])

        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            # 周日查看（today=3/15），本地仍是 3/13
            overview = get_products_overview(
                self.data_root,
                ["stock-trading-data"],
                today=date(2026, 3, 15),
            )

        # 应显示 0 天落后（对比 API 日期），而非 2 天落后（对比 today）
        self.assertEqual(overview[0]["days_behind"], 0)
        self.assertEqual(overview[0]["status_color"], "green")

    def test_overview_up_to_date_reason_shows_green(self):
        """门控跳过(reason=up_to_date)时应显示绿色，即使 today 在之后。

        回归测试 issue #1：覆盖 skipped + up_to_date 路径。
        """
        self._write_timestamp("stock-trading-data", "2026-03-13")
        self._write_report("run_report_20260313_update.json", [
            {"product": "stock-trading-data", "status": "skipped",
             "reason_code": "up_to_date", "error": "", "date_time": "2026-03-13"},
        ])

        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            overview = get_products_overview(
                self.data_root,
                ["stock-trading-data"],
                today=date(2026, 3, 15),
            )

        self.assertEqual(overview[0]["days_behind"], 0)
        self.assertEqual(overview[0]["status_color"], "green")

    def test_overview_stale_cache_degrades_to_today(self):
        """缓存的 API 日期超过宽限期后，降级回 today 比较。

        回归测试：防止长期未同步的产品永远显示绿色。
        """
        self._write_timestamp("stock-trading-data", "2026-03-10")
        self._write_report("run_report_20260310_update.json", [
            {"product": "stock-trading-data", "status": "ok", "reason_code": "ok",
             "error": "", "date_time": "2026-03-10"},
        ])

        import unittest.mock
        with unittest.mock.patch(
            "quantclass_sync_internal.data_query.report_dir_path",
            return_value=self.log_dir,
        ):
            # 4 天后查看（超出 3 天宽限期），应降级回 today
            overview = get_products_overview(
                self.data_root,
                ["stock-trading-data"],
                today=date(2026, 3, 14),
            )

        # today - local = 4 天，应显示红色
        self.assertEqual(overview[0]["days_behind"], 4)
        self.assertEqual(overview[0]["status_color"], "red")

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


class TestGetRunDetail(unittest.TestCase):
    """运行报告详情读取。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.log_dir = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_report(self, filename="run_report_20260313_update.json"):
        """写入标准测试报告，返回文件路径。"""
        report = {
            "run_id": "run-20260313",
            "started_at": "2026-03-13T12:00:00Z",
            "duration_seconds": 45.2,
            "success_total": 2,
            "failed_total": 1,
            "skipped_total": 1,
            "products": [
                {"product": "p-ok", "status": "ok", "elapsed_seconds": 10, "error": ""},
                {"product": "p-err", "status": "error", "elapsed_seconds": 5, "error": "HTTP 403"},
                {"product": "p-skip", "status": "skipped", "elapsed_seconds": 0, "error": ""},
                {"product": "p-ok2", "status": "ok", "elapsed_seconds": 8, "error": ""},
            ],
        }
        path = self.log_dir / filename
        path.write_text(json.dumps(report), encoding="utf-8")
        return str(path)

    def test_normal(self):
        """正常读取报告详情。"""
        report_file = self._write_report()
        result = get_run_detail(self.log_dir, report_file)
        self.assertTrue(result["ok"])
        self.assertEqual(result["success_total"], 2)
        self.assertEqual(result["failed_total"], 1)
        self.assertEqual(result["skipped_total"], 1)
        self.assertAlmostEqual(result["duration_seconds"], 45.2)
        # 产品数量正确
        self.assertEqual(len(result["products"]), 4)
        # 排序顺序：error → skipped → ok（稳定排序，同 status 保持原始顺序）
        self.assertEqual(result["products"][0]["status"], "error")
        self.assertEqual(result["products"][0]["product"], "p-err")
        self.assertEqual(result["products"][1]["status"], "skipped")
        self.assertEqual(result["products"][1]["product"], "p-skip")
        self.assertEqual(result["products"][2]["status"], "ok")
        self.assertEqual(result["products"][2]["product"], "p-ok")
        self.assertEqual(result["products"][3]["status"], "ok")
        self.assertEqual(result["products"][3]["product"], "p-ok2")

    def test_file_not_found(self):
        """报告文件不存在返回 ok=False。"""
        result = get_run_detail(self.log_dir, str(self.log_dir / "nonexistent.json"))
        self.assertFalse(result["ok"])
        self.assertIn("不存在", result["error"])

    def test_path_traversal_rejected(self):
        """路径遍历攻击被拒绝。"""
        with tempfile.TemporaryDirectory() as outer_path:
            outer_file = str(Path(outer_path) / "secret.json")
            Path(outer_file).write_text('{"evil": true}', encoding="utf-8")

            result = get_run_detail(self.log_dir, outer_file)
            self.assertFalse(result["ok"])
            self.assertIn("非法路径", result["error"])

    def test_corrupt_json(self):
        """损坏的 JSON 文件返回 ok=False。"""
        path = self.log_dir / "run_report_bad.json"
        path.write_text("not valid json", encoding="utf-8")
        result = get_run_detail(self.log_dir, str(path))
        self.assertFalse(result["ok"])
        self.assertIn("读取失败", result["error"])


class TestReportDirIsolationByDataRoot(unittest.TestCase):
    """回归测试：不同 data_root 的报告应互不干扰。

    Bug: report_dir 以前指向脚本目录下的 log/，两个 data_root 共享同一个
    报告目录，导致 A 的 get_products_overview 可能读到 B 的报告。
    修复：resolve_runtime_paths 把 report_dir 改到
    data_root/.quantclass_sync/log/，按 data_root 隔离。
    """

    def _write_report(self, log_dir: Path, filename: str, products: list):
        """在指定 log_dir 写入 run_report JSON 和 product_last_status.json。"""
        log_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": "3.2",
            "run_id": "test",
            "started_at": "2026-03-13T00:00:00Z",
            "mode": "network",
            "success_total": sum(1 for p in products if p.get("status") == "ok"),
            "failed_total": sum(1 for p in products if p.get("status") == "error"),
            "skipped_total": 0,
            "duration_seconds": 10.0,
            "products": products,
        }
        (log_dir / filename).write_text(
            json.dumps(report, ensure_ascii=False), encoding="utf-8"
        )
        # 累积写入 product_last_status.json
        status_path = log_dir / "product_last_status.json"
        existing = {}
        if status_path.exists():
            try:
                existing = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        for item in products:
            name = item.get("product", "")
            if name:
                existing[name] = {
                    "status": item.get("status", ""),
                    "reason_code": item.get("reason_code", ""),
                    "error": item.get("error", ""),
                    "date_time": item.get("date_time", ""),
                }
        status_path.write_text(
            json.dumps(existing, ensure_ascii=False), encoding="utf-8"
        )

    def test_two_data_roots_do_not_cross_read_reports(self):
        """两个 data_root 的报告互不干扰：A 的查询不读到 B 的报告内容。"""
        with tempfile.TemporaryDirectory() as tmpdir_a, \
                tempfile.TemporaryDirectory() as tmpdir_b:
            data_root_a = Path(tmpdir_a)
            data_root_b = Path(tmpdir_b)

            # data_root_a 下的产品 stock-a 状态为 ok
            (data_root_a / "stock-a").mkdir(parents=True)
            (data_root_a / "stock-a" / "timestamp.txt").write_text(
                "2026-03-13,2026-03-13 22:00:00\n", encoding="utf-8"
            )
            log_dir_a = data_root_a / ".quantclass_sync" / "log"
            self._write_report(log_dir_a, "run_report_20260313_update.json", [
                {"product": "stock-a", "status": "ok", "reason_code": "ok", "error": ""},
            ])

            # data_root_b 下的产品 stock-a 状态为 error（同名产品，不同 data_root）
            (data_root_b / "stock-a").mkdir(parents=True)
            (data_root_b / "stock-a" / "timestamp.txt").write_text(
                "2026-03-12,2026-03-12 22:00:00\n", encoding="utf-8"
            )
            log_dir_b = data_root_b / ".quantclass_sync" / "log"
            self._write_report(log_dir_b, "run_report_20260313_update.json", [
                {"product": "stock-a", "status": "error", "reason_code": "network_error", "error": "B 的错误"},
            ])

            # 查询 data_root_a 的 overview，不 mock report_dir_path，
            # 由 resolve_runtime_paths 自动返回 data_root_a/.quantclass_sync/log/
            overview_a = get_products_overview(
                data_root_a, ["stock-a"], today=date(2026, 3, 13)
            )

            # data_root_a 下的 stock-a 应为 ok（green），不应读到 data_root_b 的 error
            self.assertEqual(len(overview_a), 1)
            self.assertEqual(overview_a[0]["last_status"], "ok")
            self.assertEqual(overview_a[0]["status_color"], "green")
            self.assertNotEqual(
                overview_a[0]["last_error"], "B 的错误",
                "data_root_a 的查询不应读到 data_root_b 的报告内容",
            )


class TestReportHistoryScanRetainsOldProducts(unittest.TestCase):
    """回归测试：累积状态文件保留未参与本轮运行的产品的历史状态。

    Bug: 原先只读最新报告，部分运行后其他产品状态丢失。
    修复：改用 product_last_status.json 累积维护所有产品的最后状态。
    """

    def _write_report(self, log_dir: Path, filename: str, products: list):
        """在指定 log_dir 写入 run_report JSON 和累积更新 product_last_status.json。"""
        log_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "schema_version": "3.2",
            "run_id": "test",
            "started_at": "2026-03-13T00:00:00Z",
            "mode": "network",
            "success_total": sum(1 for p in products if p.get("status") == "ok"),
            "failed_total": sum(1 for p in products if p.get("status") == "error"),
            "skipped_total": 0,
            "duration_seconds": 10.0,
            "products": products,
        }
        (log_dir / filename).write_text(
            json.dumps(report, ensure_ascii=False), encoding="utf-8"
        )
        # 模拟 _update_product_last_status 的累积写入
        status_path = log_dir / "product_last_status.json"
        existing = {}
        if status_path.exists():
            try:
                existing = json.loads(status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        for item in products:
            name = item.get("product", "")
            if name:
                existing[name] = {
                    "status": item.get("status", ""),
                    "reason_code": item.get("reason_code", ""),
                    "error": item.get("error", ""),
                    "date_time": item.get("date_time", ""),
                }
        status_path.write_text(
            json.dumps(existing, ensure_ascii=False), encoding="utf-8"
        )

    def test_old_product_status_retained_from_earlier_report(self):
        """旧报告中的 product-a(error) 在新报告未覆盖时，仍应显示为 error 状态。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir)
            log_dir = data_root / ".quantclass_sync" / "log"

            # 为两个产品创建 timestamp（避免因无 timestamp 而显示为 gray）
            for p in ["product-a", "product-b"]:
                (data_root / p).mkdir(parents=True)
                (data_root / p / "timestamp.txt").write_text(
                    "2026-03-12,2026-03-12 22:00:00\n", encoding="utf-8"
                )

            # 第一个报告（较早）：product-a(error) + product-b(ok)
            self._write_report(log_dir, "run_report_20260312_update.json", [
                {"product": "product-a", "status": "error", "reason_code": "network_error", "error": "连接超时"},
                {"product": "product-b", "status": "ok", "reason_code": "ok", "error": ""},
            ])

            # 第二个报告（较新）：只包含 product-b(ok)，不包含 product-a
            self._write_report(log_dir, "run_report_20260313_update.json", [
                {"product": "product-b", "status": "ok", "reason_code": "ok", "error": ""},
            ])

            # 不 mock report_dir_path，验证真实路径结构
            overview = get_products_overview(
                data_root,
                ["product-a", "product-b"],
                today=date(2026, 3, 13),
            )

            by_name = {item["name"]: item for item in overview}

            # product-a：第二轮没跑它，但累积状态文件保留了第一轮的 error
            self.assertEqual(
                by_name["product-a"]["last_status"], "error",
                "product-a 应从累积状态文件保留 error 状态"
            )
            self.assertEqual(
                by_name["product-a"]["status_color"], "red",
                "error 状态应显示为红色"
            )

            # product-b：最新报告有它，状态为 ok
            self.assertEqual(by_name["product-b"]["last_status"], "ok")


class TestUpdateProductLastStatus(unittest.TestCase):
    """回归测试：_update_product_last_status 的累积语义和同产品覆盖。"""

    def test_same_product_multiple_times_takes_last(self):
        """同一报告内同产品出现多次时（catch-up），应取最后一条。"""
        from quantclass_sync_internal.status_store import _update_product_last_status, PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import RunReport, ProductRunResult, SyncStats

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            report = RunReport(
                schema_version="3.2", run_id="test", started_at="", mode="network",
            )
            # 同产品 p1 先 error 后 ok（模拟 catch-up：第一天失败，第二天成功）
            report.products = [
                ProductRunResult(
                    product="p1", status="error", strategy="merge_known",
                    reason_code="network_error", date_time="2026-03-12",
                    error="day1 fail", stats=SyncStats(),
                ),
                ProductRunResult(
                    product="p1", status="ok", strategy="merge_known",
                    reason_code="ok", date_time="2026-03-13",
                    error="", stats=SyncStats(),
                ),
            ]
            _update_product_last_status(log_dir, report)

            status = json.loads(
                (log_dir / PRODUCT_LAST_STATUS_FILE).read_text(encoding="utf-8")
            )
            # 最后一条（ok）应覆盖前面的（error）
            self.assertEqual(status["p1"]["status"], "ok")
            self.assertEqual(status["p1"]["error"], "")

    def test_accumulates_across_runs(self):
        """多次运行后，未参与本轮的产品保留上次状态。"""
        from quantclass_sync_internal.status_store import _update_product_last_status, PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import RunReport, ProductRunResult, SyncStats

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)

            # 第一轮：p1 error, p2 ok
            r1 = RunReport(schema_version="3.2", run_id="r1", started_at="", mode="network")
            r1.products = [
                ProductRunResult(
                    product="p1", status="error", strategy="merge_known",
                    reason_code="network_error", error="fail", stats=SyncStats(),
                ),
                ProductRunResult(
                    product="p2", status="ok", strategy="merge_known",
                    reason_code="ok", error="", stats=SyncStats(),
                ),
            ]
            _update_product_last_status(log_dir, r1)

            # 第二轮：只有 p2
            r2 = RunReport(schema_version="3.2", run_id="r2", started_at="", mode="network")
            r2.products = [
                ProductRunResult(
                    product="p2", status="ok", strategy="merge_known",
                    reason_code="ok", error="", stats=SyncStats(),
                ),
            ]
            _update_product_last_status(log_dir, r2)

            status = json.loads(
                (log_dir / PRODUCT_LAST_STATUS_FILE).read_text(encoding="utf-8")
            )
            # p1 未参与第二轮，但仍保留第一轮的 error
            self.assertEqual(status["p1"]["status"], "error")
            self.assertEqual(status["p2"]["status"], "ok")


class TestBackfillFromReports(unittest.TestCase):
    """回归测试：升级过渡期，累积文件缺失时从历史 run_report 回填。

    Bug: 升级后 product_last_status.json 尚未生成，
    _load_latest_report_products 直接返回空 dict，导致历史状态丢失。
    修复：缺失时扫描所有 run_report 回填，并持久化供后续快速读取。
    """

    def test_backfill_restores_status_from_reports(self):
        """只有历史 run_report 时，get_products_overview 能恢复 last_status。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir)
            log_dir = data_root / ".quantclass_sync" / "log"
            log_dir.mkdir(parents=True)

            # 创建产品目录和 timestamp
            for p in ["product-a", "product-b"]:
                (data_root / p).mkdir(parents=True)
                (data_root / p / "timestamp.txt").write_text(
                    "2026-03-12,2026-03-12 22:00:00\n", encoding="utf-8"
                )

            # 只写 run_report，不写 product_last_status.json（模拟升级过渡期）
            report = {
                "schema_version": "3.2",
                "run_id": "old-run",
                "started_at": "2026-03-12T00:00:00Z",
                "mode": "network",
                "success_total": 1,
                "failed_total": 1,
                "skipped_total": 0,
                "duration_seconds": 10.0,
                "products": [
                    {"product": "product-a", "status": "error",
                     "reason_code": "network_error", "error": "连接超时"},
                    {"product": "product-b", "status": "ok",
                     "reason_code": "ok", "error": ""},
                ],
            }
            (log_dir / "run_report_20260312_update.json").write_text(
                json.dumps(report, ensure_ascii=False), encoding="utf-8"
            )

            # product_last_status.json 不存在
            self.assertFalse(
                (log_dir / "product_last_status.json").exists()
            )

            overview = get_products_overview(
                data_root, ["product-a", "product-b"], today=date(2026, 3, 13),
            )

            by_name = {item["name"]: item for item in overview}
            # product-a 应从历史报告恢复 error 状态
            self.assertEqual(by_name["product-a"]["last_status"], "error")
            self.assertEqual(by_name["product-a"]["last_error"], "连接超时")
            self.assertEqual(by_name["product-a"]["status_color"], "red")
            # product-b 应恢复 ok 状态
            self.assertEqual(by_name["product-b"]["last_status"], "ok")

            # 回填后 product_last_status.json 应已生成
            self.assertTrue(
                (log_dir / "product_last_status.json").exists(),
                "回填后应自动生成 product_last_status.json 供后续快速读取"
            )

    def test_backfill_merges_multiple_reports(self):
        """多份历史报告按时间顺序合并，后写覆盖先写。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir)
            log_dir = data_root / ".quantclass_sync" / "log"
            log_dir.mkdir(parents=True)

            (data_root / "p1").mkdir(parents=True)
            (data_root / "p1" / "timestamp.txt").write_text(
                "2026-03-13,2026-03-13 22:00:00\n", encoding="utf-8"
            )

            # 报告1：p1 error
            r1 = {
                "schema_version": "3.2", "run_id": "r1",
                "started_at": "2026-03-12T00:00:00Z", "mode": "network",
                "products": [
                    {"product": "p1", "status": "error",
                     "reason_code": "network_error", "error": "fail"},
                ],
            }
            (log_dir / "run_report_20260312_update.json").write_text(
                json.dumps(r1), encoding="utf-8"
            )

            # 报告2（更新）：p1 ok
            r2 = {
                "schema_version": "3.2", "run_id": "r2",
                "started_at": "2026-03-13T00:00:00Z", "mode": "network",
                "products": [
                    {"product": "p1", "status": "ok",
                     "reason_code": "ok", "error": ""},
                ],
            }
            (log_dir / "run_report_20260313_update.json").write_text(
                json.dumps(r2), encoding="utf-8"
            )

            overview = get_products_overview(
                data_root, ["p1"], today=date(2026, 3, 13),
            )
            # 后写报告的 ok 应覆盖先写报告的 error
            self.assertEqual(overview[0]["last_status"], "ok")


def _concurrent_status_worker(product_name: str, log_dir_str: str):
    """并发测试 worker：每个进程写入一个独占产品到 product_last_status.json。"""
    import time
    from quantclass_sync_internal.status_store import _update_product_last_status
    from quantclass_sync_internal.models import RunReport, ProductRunResult, SyncStats

    work_dir = Path(log_dir_str)
    report = RunReport(
        schema_version="3.2", run_id=f"run-{product_name}",
        started_at="", mode="network",
    )
    report.products = [
        ProductRunResult(
            product=product_name, status="ok",
            strategy="merge_known", reason_code="ok",
            error="", stats=SyncStats(),
        ),
    ]
    # 加小延迟增加竞争窗口
    time.sleep(0.01)
    _update_product_last_status(work_dir, report)


class TestConcurrentProductLastStatusWrite(unittest.TestCase):
    """回归测试：多进程并发写入 product_last_status.json 不丢状态。

    Bug: _update_product_last_status 的读-合并-写循环无进程级互斥，
    两个进程同时基于同一旧快照写回会导致后写入方覆盖先写入方的全部更新。
    修复：加 fcntl.flock 排他锁。
    """

    def test_concurrent_writes_no_lost_updates(self):
        """多进程并发更新，最终文件应包含所有进程写入的产品。"""
        import multiprocessing
        from quantclass_sync_internal.status_store import PRODUCT_LAST_STATUS_FILE

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)

            n_workers = 10
            processes = []
            for i in range(n_workers):
                p = multiprocessing.Process(
                    target=_concurrent_status_worker,
                    args=(f"product-{i}", str(log_dir)),
                )
                processes.append(p)

            # 同时启动所有进程
            for p in processes:
                p.start()
            for p in processes:
                p.join(timeout=10)

            status = json.loads(
                (log_dir / PRODUCT_LAST_STATUS_FILE).read_text(encoding="utf-8")
            )

            # 所有 10 个产品都应存在（无丢失）
            missing = [
                f"product-{i}" for i in range(n_workers)
                if f"product-{i}" not in status
            ]
            self.assertEqual(
                missing, [],
                f"并发写入后丢失产品: {missing}。最终文件包含 {len(status)} 个产品。"
            )


class TestCorruptedStatusFileSelfHealing(unittest.TestCase):
    """回归测试：累积文件损坏时自动从历史报告重建。

    Bug: product_last_status.json 存在但 JSON 损坏时，
    直接返回空 dict，不会回退到历史 run_report 重新构建。
    修复：损坏时走慢路径（加锁 + 回填 + 原子写），自动恢复。
    """

    def test_corrupted_json_triggers_rebuild(self):
        """累积文件损坏时，从历史报告重建状态。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            data_root = Path(tmpdir)
            log_dir = data_root / ".quantclass_sync" / "log"
            log_dir.mkdir(parents=True)

            # 创建产品目录和 timestamp
            (data_root / "p1").mkdir(parents=True)
            (data_root / "p1" / "timestamp.txt").write_text(
                "2026-03-12,2026-03-12 22:00:00\n", encoding="utf-8"
            )

            # 写一份正常的 run_report
            report = {
                "schema_version": "3.2", "run_id": "r1",
                "started_at": "2026-03-12T00:00:00Z", "mode": "network",
                "products": [
                    {"product": "p1", "status": "error",
                     "reason_code": "network_error", "error": "timeout"},
                ],
            }
            (log_dir / "run_report_20260312_update.json").write_text(
                json.dumps(report), encoding="utf-8"
            )

            # 写入损坏的 product_last_status.json
            (log_dir / "product_last_status.json").write_text(
                "CORRUPTED{{{not json", encoding="utf-8"
            )

            overview = get_products_overview(
                data_root, ["p1"], today=date(2026, 3, 13),
            )

            # 应从历史报告重建，而非返回空状态
            self.assertEqual(overview[0]["last_status"], "error")
            self.assertEqual(overview[0]["last_error"], "timeout")

            # 重建后文件应可正常解析
            rebuilt = json.loads(
                (log_dir / "product_last_status.json").read_text(encoding="utf-8")
            )
            self.assertIn("p1", rebuilt)
            self.assertEqual(rebuilt["p1"]["status"], "error")


class TestDryRunDoesNotWriteProductLastStatus(unittest.TestCase):
    """回归测试：dry_run 模式下 _finalize_and_write_report 不写累积状态文件。

    Bug: cmd_repair_sort 的两处 _finalize_and_write_report 调用遗漏 dry_run 参数，
    导致 --dry-run 模式下仍会写入 product_last_status.json。
    """

    def test_dry_run_skips_status_update(self):
        """dry_run=True 时，写 run_report 但不写 product_last_status.json。"""
        from quantclass_sync_internal.reporting import (
            _finalize_and_write_report, _new_report, _append_result,
        )
        from quantclass_sync_internal.status_store import PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import SyncStats
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            report_path = log_dir / "run_report_test_dryrun.json"
            report = _new_report("test-dryrun", mode="maintenance")
            _append_result(report, product="p1", status="ok", strategy="repair_sort")
            total = SyncStats()

            _finalize_and_write_report(
                report, total, has_error=False, t_run_start=time.time(),
                report_path=report_path, dry_run=True, log_dir=log_dir,
            )

            # run_report 应该写入（可观测性需要）
            self.assertTrue(report_path.exists())
            # product_last_status.json 不应被创建
            self.assertFalse(
                (log_dir / PRODUCT_LAST_STATUS_FILE).exists(),
                "dry_run=True 不应写入 product_last_status.json",
            )

    def test_non_dry_run_writes_status(self):
        """dry_run=False 时，正常写入 product_last_status.json。"""
        from quantclass_sync_internal.reporting import (
            _finalize_and_write_report, _new_report, _append_result,
        )
        from quantclass_sync_internal.status_store import PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import SyncStats
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            report_path = log_dir / "run_report_test_normal.json"
            report = _new_report("test-normal", mode="maintenance")
            _append_result(report, product="p1", status="ok", strategy="repair_sort")
            total = SyncStats()

            _finalize_and_write_report(
                report, total, has_error=False, t_run_start=time.time(),
                report_path=report_path, dry_run=False, log_dir=log_dir,
            )

            self.assertTrue(report_path.exists())
            status_path = log_dir / PRODUCT_LAST_STATUS_FILE
            self.assertTrue(status_path.exists())
            data = json.loads(status_path.read_text(encoding="utf-8"))
            self.assertEqual(data["p1"]["status"], "ok")


class TestLogDirOverridesReportPathParent(unittest.TestCase):
    """回归测试：log_dir 参数确保状态文件写到正确目录。

    Bug: --report-file /tmp/xxx.json 时，report_path.parent 变为 /tmp/，
    product_last_status.json 写到错误目录，status 命令看不到更新。
    """

    def test_log_dir_used_for_status_file(self):
        """log_dir 指定时，product_last_status.json 写入 log_dir 而非 report_path.parent。"""
        from quantclass_sync_internal.reporting import (
            _finalize_and_write_report, _new_report, _append_result,
        )
        from quantclass_sync_internal.status_store import PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import SyncStats
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            # 模拟 --report-file 指向外部路径
            external_dir = Path(tmpdir) / "external"
            external_dir.mkdir()
            report_path = external_dir / "my_report.json"

            # 正确的 log_dir
            log_dir = Path(tmpdir) / "correct_log_dir"
            log_dir.mkdir()

            report = _new_report("test-logdir", mode="network")
            _append_result(report, product="p1", status="ok")
            total = SyncStats()

            _finalize_and_write_report(
                report, total, has_error=False, t_run_start=time.time(),
                report_path=report_path, dry_run=False, log_dir=log_dir,
            )

            # 报告写到 external_dir
            self.assertTrue(report_path.exists())
            # 状态文件写到 log_dir，而非 external_dir
            self.assertTrue((log_dir / PRODUCT_LAST_STATUS_FILE).exists())
            self.assertFalse((external_dir / PRODUCT_LAST_STATUS_FILE).exists())


class TestEmptyProductsSentinelWrite(unittest.TestCase):
    """边界测试：products 为空时仍写入哨兵文件 {}，避免重复扫描历史报告。"""

    def test_empty_products_writes_sentinel(self):
        """报告无产品记录时，_finalize_and_write_report 仍写入空 {} 到 product_last_status.json。"""
        from quantclass_sync_internal.reporting import (
            _finalize_and_write_report, _new_report,
        )
        from quantclass_sync_internal.status_store import PRODUCT_LAST_STATUS_FILE
        from quantclass_sync_internal.models import SyncStats
        import time

        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)
            report_path = log_dir / "run_report_test_empty.json"
            # 不向 report 添加任何产品记录
            report = _new_report("test-empty", mode="network")
            total = SyncStats()

            _finalize_and_write_report(
                report, total, has_error=False, t_run_start=time.time(),
                report_path=report_path, dry_run=False, log_dir=log_dir,
            )

            status_path = log_dir / PRODUCT_LAST_STATUS_FILE
            self.assertTrue(status_path.exists())
            data = json.loads(status_path.read_text(encoding="utf-8"))
            # 空 dict 哨兵
            self.assertEqual(data, {})


if __name__ == "__main__":
    unittest.main()
