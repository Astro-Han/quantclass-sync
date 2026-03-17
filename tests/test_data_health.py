"""check_data_health 和 repair_data_issues 单元测试。"""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.constants import TIMESTAMP_FILE_NAME
from quantclass_sync_internal.data_query import check_data_health, repair_data_issues


class TestNoIssuesHealthyData(unittest.TestCase):
    """健康数据不产生任何 issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_no_issues_healthy_data(self):
        """有 timestamp + 正常 CSV -> issues 为空。"""
        product = "healthy-prod"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("col1,col2\n1,2\n3,4\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        self.assertEqual(result["issues"], [])
        self.assertEqual(result["summary"]["total"], 0)


class TestMissingDataDetected(unittest.TestCase):
    """有 timestamp.txt 但无数据文件时报告 missing_data。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_missing_data_detected(self):
        """有 timestamp 但无数据文件 -> missing_data error。"""
        product = "missing-prod"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "missing_data"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertEqual(issues[0]["product"], product)
        self.assertFalse(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "needs_resync")


class TestOrphanTempDetected(unittest.TestCase):
    """.tmp- 前缀文件应报告 orphan_temp。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_orphan_temp_in_product_dir(self):
        """产品目录内 .tmp- 文件 -> orphan_temp warning。"""
        product = "some-product"
        pdir = self.data_root / product
        pdir.mkdir()
        tmp_name = ".tmp-atomic-sh600000.csv-12345"
        (pdir / tmp_name).write_text("temp data")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "orphan_temp"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "warning")
        self.assertEqual(issues[0]["product"], product)
        self.assertTrue(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "delete_temp")

    def test_orphan_temp_in_data_root(self):
        """data_root 根级 .tmp- 文件 -> orphan_temp，product 为 (root)。"""
        (self.data_root / ".tmp-orphan-root").write_text("temp")

        result = check_data_health(self.data_root, [])
        issues = [i for i in result["issues"] if i["type"] == "orphan_temp"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["product"], "(root)")


class TestCsvUnreadable(unittest.TestCase):
    """CSV 无法解码时报告 csv_unreadable。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_csv_unreadable_garbled(self):
        """乱码字节序列 -> csv_unreadable error。"""
        product = "bad-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 写入无法被任何候选编码解码的字节序列
        (pdir / "bad.csv").write_bytes(b"\x80\x81\x82\x83\x84\x85")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "csv_unreadable"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertFalse(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "needs_resync")

    def test_csv_unreadable_empty(self):
        """空 CSV 文件 -> csv_unreadable error。"""
        product = "empty-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / "empty.csv").write_text("", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "csv_unreadable"]
        self.assertEqual(len(issues), 1)


class TestTailCorruption(unittest.TestCase):
    """末尾行列数不匹配时报告 tail_corruption。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_tail_corruption_detected(self):
        """末尾行列数 != 表头列数 -> tail_corruption error。"""
        product = "corrupt-product"
        pdir = self.data_root / product
        pdir.mkdir()
        # 表头 3 列，末尾行只有 2 列
        (pdir / "corrupt.csv").write_text("col1,col2,col3\n1,2,3\n4,5\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "tail_corruption"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertTrue(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "truncate_tail")


class TestCsvNormal(unittest.TestCase):
    """正常 CSV 不产生任何 issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_csv_normal(self):
        """表头与数据行列数一致 -> 无 issue。"""
        product = "normal-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "good.csv").write_text("col1,col2,col3\n1,2,3\n4,5,6\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        self.assertEqual(result["issues"], [])


class TestSubdirectoryScan(unittest.TestCase):
    """子目录中的 CSV 也应被检查。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_subdirectory_scan(self):
        """子目录内 CSV 末尾行损坏 -> tail_corruption 被检测到。"""
        product = "deep-product"
        pdir = self.data_root / product
        subdir = pdir / "sh600000"
        subdir.mkdir(parents=True)
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 子目录内 CSV：末尾行列数不对
        (subdir / "sh600000.csv").write_text("a,b,c\n1,2,3\n4,5\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        issues = [i for i in result["issues"] if i["type"] == "tail_corruption"]
        self.assertEqual(len(issues), 1)
        # file 字段应为相对于产品目录的路径
        self.assertIn("sh600000", issues[0]["file"])


class TestInfrastructureDbCorrupt(unittest.TestCase):
    """SQLite 数据库损坏时报告 infra_db_corrupt。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_infrastructure_db_corrupt(self):
        """损坏的 SQLite 文件 -> infra_db_corrupt error。"""
        # 创建状态目录和损坏的 DB 文件
        from quantclass_sync_internal.status_store import status_db_path
        db_path = status_db_path(self.data_root)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.write_bytes(b"this is not a valid sqlite database content !!!")

        result = check_data_health(self.data_root, [])
        issues = [i for i in result["issues"] if i["type"] == "infra_db_corrupt"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertTrue(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "rebuild_status_db")

    def test_infrastructure_db_healthy(self):
        """正常 SQLite 数据库 -> 无 infra 相关 issue。"""
        from quantclass_sync_internal.status_store import status_db_path, ensure_status_table
        db_path = status_db_path(self.data_root)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        ensure_status_table(conn)
        conn.close()

        result = check_data_health(self.data_root, [])
        issues = [i for i in result["issues"] if i["type"] == "infra_db_corrupt"]
        self.assertEqual(len(issues), 0)


class TestInfrastructureJsonCorrupt(unittest.TestCase):
    """product_last_status.json 损坏时报告 infra_json_corrupt。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_infrastructure_json_corrupt(self):
        """损坏的 JSON 文件 -> infra_json_corrupt error。"""
        from quantclass_sync_internal.status_store import report_dir_path, PRODUCT_LAST_STATUS_FILE
        rdir = report_dir_path(self.data_root)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / PRODUCT_LAST_STATUS_FILE).write_text("{not valid json}", encoding="utf-8")

        result = check_data_health(self.data_root, [])
        issues = [i for i in result["issues"] if i["type"] == "infra_json_corrupt"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")

    def test_infrastructure_json_healthy(self):
        """正常 JSON 文件 -> 无 infra_json_corrupt issue。"""
        from quantclass_sync_internal.status_store import report_dir_path, PRODUCT_LAST_STATUS_FILE
        rdir = report_dir_path(self.data_root)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / PRODUCT_LAST_STATUS_FILE).write_text(json.dumps({}), encoding="utf-8")

        result = check_data_health(self.data_root, [])
        issues = [i for i in result["issues"] if i["type"] == "infra_json_corrupt"]
        self.assertEqual(len(issues), 0)


class TestProgressCallback(unittest.TestCase):
    """progress_callback 应被正确调用。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_progress_callback(self):
        """callback 被调用次数等于 catalog_products 数量，参数格式正确。"""
        products = ["prod-a", "prod-b", "prod-c"]
        for p in products:
            (self.data_root / p).mkdir()

        calls = []
        def cb(current, total, product, phase):
            calls.append((current, total, product, phase))

        check_data_health(self.data_root, products, progress_callback=cb)

        self.assertEqual(len(calls), len(products))
        # 第一次调用 current=0，total=3
        self.assertEqual(calls[0][0], 0)
        self.assertEqual(calls[0][1], 3)
        self.assertEqual(calls[0][2], products[0])
        self.assertEqual(calls[0][3], "checking")

    def test_progress_callback_none(self):
        """progress_callback=None 时不报错。"""
        result = check_data_health(self.data_root, [], progress_callback=None)
        self.assertIsNotNone(result)


class TestNewResultStructure(unittest.TestCase):
    """issues 每条必须包含 severity/category/repairable/repair_action 字段。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_new_result_structure(self):
        """issue 字典包含所有新字段。"""
        product = "check-struct"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 触发 missing_data
        result = check_data_health(self.data_root, [product])
        self.assertGreater(len(result["issues"]), 0)

        required_fields = {"type", "severity", "category", "product", "detail",
                           "file", "repairable", "repair_action"}
        for issue in result["issues"]:
            for field in required_fields:
                self.assertIn(field, issue, f"issue 缺少字段: {field}")


class TestSummaryStructure(unittest.TestCase):
    """summary 必须包含 by_severity/by_repair/scanned_files 字段。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_summary_structure(self):
        """summary 包含所有必要字段。"""
        result = check_data_health(self.data_root, [])
        s = result["summary"]

        self.assertIn("total", s)
        self.assertIn("by_severity", s)
        self.assertIn("by_repair", s)
        self.assertIn("scanned_products", s)
        self.assertIn("scanned_files", s)
        self.assertIn("elapsed_seconds", s)

        # by_severity 包含 error/warning
        self.assertIn("error", s["by_severity"])
        self.assertIn("warning", s["by_severity"])

        # by_repair 包含三类
        self.assertIn("auto_repairable", s["by_repair"])
        self.assertIn("needs_resync", s["by_repair"])
        self.assertIn("needs_investigation", s["by_repair"])

    def test_summary_counts_consistent(self):
        """summary.total 等于 issues 列表长度。"""
        product = "count-test"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 触发 missing_data
        result = check_data_health(self.data_root, [product])
        self.assertEqual(result["summary"]["total"], len(result["issues"]))

    def test_scanned_files_count(self):
        """scanned_files 等于实际数据文件数。"""
        product = "file-count"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "a.csv").write_text("col1\n1\n", encoding="utf-8")
        (pdir / "b.csv").write_text("col1\n2\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        self.assertEqual(result["summary"]["scanned_files"], 2)


class TestContentIntegrityDuplicateRows(unittest.TestCase):
    """CSV 中存在主键重复行时报告 duplicate_rows。"""

    # stock-main-index-data: key_cols=("index_code","candle_end_time"), has_note=False, encoding="utf-8-sig"
    PRODUCT = "stock-main-index-data"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()
        # 写 timestamp，避免 missing_data 误报
        (self.pdir / TIMESTAMP_FILE_NAME).write_text(
            "2026-01-01,2026-01-01 10:00:00\n", encoding="utf-8"
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_duplicate_rows_detected(self):
        """CSV 含主键重复行 -> duplicate_rows warning。"""
        # 第 2、3 行 index_code+candle_end_time 完全相同（重复）
        csv_content = (
            "index_code,candle_end_time,close\n"
            "000001,2026-01-01,100\n"
            "000001,2026-01-01,100\n"   # 重复行
            "000002,2026-01-01,200\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="utf-8-sig")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "duplicate_rows"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "warning")
        self.assertEqual(issues[0]["product"], self.PRODUCT)
        self.assertEqual(issues[0]["category"], "content_integrity")
        self.assertTrue(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "dedup_rows")
        self.assertIn("1", issues[0]["detail"])  # detail 包含重复数量 "1"

    def test_no_duplicates(self):
        """主键唯一 -> 无 duplicate_rows issue。"""
        csv_content = (
            "index_code,candle_end_time,close\n"
            "000001,2026-01-01,100\n"
            "000002,2026-01-01,200\n"
            "000001,2026-01-02,110\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="utf-8-sig")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "duplicate_rows"]
        self.assertEqual(len(issues), 0)


class TestContentIntegrityNullKeyFields(unittest.TestCase):
    """CSV 中关键字段存在空值时报告 null_key_fields。"""

    PRODUCT = "stock-main-index-data"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()
        (self.pdir / TIMESTAMP_FILE_NAME).write_text(
            "2026-01-01,2026-01-01 10:00:00\n", encoding="utf-8"
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_null_key_fields_detected(self):
        """key_cols 字段有空值 -> null_key_fields warning。"""
        # candle_end_time 列有空值
        csv_content = (
            "index_code,candle_end_time,close\n"
            "000001,,100\n"   # candle_end_time 为空
            "000002,2026-01-01,200\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="utf-8-sig")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "null_key_fields"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "warning")
        self.assertEqual(issues[0]["product"], self.PRODUCT)
        self.assertEqual(issues[0]["category"], "content_integrity")
        self.assertFalse(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "needs_investigation")
        # detail 应包含列名和空值数量
        self.assertIn("candle_end_time", issues[0]["detail"])
        self.assertIn("1", issues[0]["detail"])

    def test_no_null_fields(self):
        """关键字段均有值 -> 无 null_key_fields issue。"""
        csv_content = (
            "index_code,candle_end_time,close\n"
            "000001,2026-01-01,100\n"
            "000002,2026-01-01,200\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="utf-8-sig")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "null_key_fields"]
        self.assertEqual(len(issues), 0)


class TestContentIntegritySkippedForUnknownProduct(unittest.TestCase):
    """不在 RULES 中的产品不产生 content_integrity 相关 issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_content_check_skipped_for_unknown_product(self):
        """未知产品无 rule -> 不触发 content_integrity 检查。"""
        product = "unknown-custom-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text(
            "2026-01-01,2026-01-01 10:00:00\n", encoding="utf-8"
        )
        # 写一个有重复行的 CSV，但因为没有 rule，不应产生 content_integrity issue
        csv_content = (
            "col_a,col_b\n"
            "x,y\n"
            "x,y\n"  # 重复行
        )
        (pdir / "data.csv").write_text(csv_content, encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        content_issues = [i for i in result["issues"] if i["category"] == "content_integrity"]
        self.assertEqual(len(content_issues), 0)


class TestTradingCalendarLoad(unittest.TestCase):
    """_load_trading_calendar 从 period_offset.csv 加载日期集合。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_trading_calendar_load(self):
        """正常 period_offset.csv -> 返回包含日期的 set。"""
        from quantclass_sync_internal.data_query import _load_trading_calendar
        # 模拟 period_offset.csv，包含表头行和交易日期行
        csv_content = (
            "trade_date,offset\n"
            "2026-01-02,0\n"
            "2026-01-05,1\n"
            "2026-01-06,2\n"
        )
        (self.data_root / "period_offset.csv").write_text(csv_content, encoding="utf-8")
        cal = _load_trading_calendar(self.data_root)
        self.assertIsNotNone(cal)
        self.assertIn("2026-01-02", cal)
        self.assertIn("2026-01-05", cal)
        # 表头行 trade_date 不应被包含（不是 YYYY-MM-DD 格式）
        self.assertNotIn("trade_date", cal)

    def test_trading_calendar_missing(self):
        """period_offset.csv 不存在 -> 返回 None。"""
        from quantclass_sync_internal.data_query import _load_trading_calendar
        cal = _load_trading_calendar(self.data_root)
        self.assertIsNone(cal)


class TestTemporalIntegrityTimestampExceedsData(unittest.TestCase):
    """CSV 最大日期 > timestamp 时报告 date_exceeds_timestamp error。"""

    # 使用 coin-cap 产品：date_filter_col="candle_begin_time"，encoding="gb18030"，has_note=True
    PRODUCT = "coin-cap"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_timestamp_exceeds_data(self):
        """timestamp=2026-01-05，CSV 含 2026-01-10 -> date_exceeds_timestamp error。"""
        # timestamp.txt 格式：YYYY-MM-DD,timestamp
        (self.pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-05,2026-01-05 10:00:00\n")
        # CSV 数据中包含比 timestamp 更新的日期（has_note=True 所以第一行是备注）
        csv_content = (
            "温馨提示：仅供参考\n"
            "candle_begin_time,symbol,close\n"
            "2026-01-10,BTC,50000\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "date_exceeds_timestamp"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "error")
        self.assertEqual(issues[0]["category"], "temporal_integrity")
        self.assertFalse(issues[0]["repairable"])
        self.assertEqual(issues[0]["repair_action"], "needs_investigation")


class TestTemporalIntegrityTimestampDataGap(unittest.TestCase):
    """timestamp 远超 CSV 最大日期时报告 timestamp_data_gap warning。"""

    PRODUCT = "coin-cap"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_timestamp_data_gap(self):
        """timestamp=2026-01-20，CSV 最大日期 2026-01-10，差 10 天 -> timestamp_data_gap warning。"""
        (self.pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-20,2026-01-20 10:00:00\n")
        csv_content = (
            "温馨提示：仅供参考\n"
            "candle_begin_time,symbol,close\n"
            "2026-01-10,BTC,50000\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "timestamp_data_gap"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "warning")
        self.assertIn("10", issues[0]["detail"])

    def test_timestamp_consistent(self):
        """timestamp 与 CSV 最大日期一致（gap <= 5天）-> 无 timestamp 相关 issue。"""
        (self.pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-10,2026-01-10 10:00:00\n")
        csv_content = (
            "温馨提示：仅供参考\n"
            "candle_begin_time,symbol,close\n"
            "2026-01-10,BTC,50000\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [self.PRODUCT])
        ts_issues = [i for i in result["issues"]
                     if i["type"] in ("date_exceeds_timestamp", "timestamp_data_gap")]
        self.assertEqual(len(ts_issues), 0)


class TestTemporalIntegrityDateContinuity(unittest.TestCase):
    """日期连续性检查：加密货币产品缺失某天时报告 missing_trading_days。"""

    PRODUCT = "coin-cap"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_date_continuity_missing_days(self):
        """coin-cap 缺少 2026-01-04 -> missing_trading_days warning。"""
        # timestamp=2026-01-05，CSV 只有 2026-01-03 和 2026-01-05，缺少 2026-01-04
        (self.pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-05,2026-01-05 10:00:00\n")
        csv_content = (
            "温馨提示：仅供参考\n"
            "candle_begin_time,symbol,close\n"
            "2026-01-03,BTC,50000\n"
            "2026-01-05,BTC,51000\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "missing_trading_days"]
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["severity"], "warning")
        self.assertIn("2026-01-04", issues[0]["detail"])
        self.assertEqual(issues[0]["repair_action"], "needs_resync")


class TestTemporalIntegrityFinancialProductSkipped(unittest.TestCase):
    """财务/公告类产品不做时间完整性检查。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_financial_product_skipped(self):
        """stock-fin-data-xbx（财务类）不产生 temporal_integrity issue。"""
        product = "stock-fin-data-xbx"
        pdir = self.data_root / product
        pdir.mkdir()
        # timestamp 远超数据，若不跳过应产生 timestamp_data_gap
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-06-01,2026-06-01 10:00:00\n")
        # 写一个带旧日期的 CSV（stock-fin-data-xbx has_note=True, sort_cols=report_date/publish_date）
        csv_content = (
            "温馨提示：仅供参考\n"
            "report_date,publish_date,股票代码\n"
            "2026-01-01,2026-01-15,000001\n"
        )
        (pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [product])
        ti_issues = [i for i in result["issues"] if i["category"] == "temporal_integrity"]
        self.assertEqual(len(ti_issues), 0)


class TestTemporalIntegrityWeekdayFallback(unittest.TestCase):
    """无交易日历时降级为工作日近似检测，并在 detail 中标注"近似"。"""

    PRODUCT = "stock-trading-data"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_weekday_fallback(self):
        """无 period_offset.csv，缺少某个工作日 -> missing_trading_days，detail 含"近似"。"""
        # 2026-01-05 是周一，2026-01-06 是周二，2026-01-07 是周三
        # timestamp=2026-01-07，CSV 只有 2026-01-05 和 2026-01-07，缺少 2026-01-06（周二）
        (self.pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-07,2026-01-07 10:00:00\n")
        # stock-trading-data: has_note=True, encoding="gb18030", sort_cols=("交易日期",)
        csv_content = (
            "温馨提示：仅供参考\n"
            "股票代码,交易日期,close\n"
            "000001,2026-01-05,10\n"
            "000001,2026-01-07,11\n"
        )
        (self.pdir / "data.csv").write_text(csv_content, encoding="gb18030")

        result = check_data_health(self.data_root, [self.PRODUCT])
        issues = [i for i in result["issues"] if i["type"] == "missing_trading_days"]
        self.assertEqual(len(issues), 1)
        self.assertIn("近似", issues[0]["detail"])
        self.assertIn("2026-01-06", issues[0]["detail"])


class TestCoverageIntegrityNoBaseline(unittest.TestCase):
    """首次运行无基线时，不产生 coverage_integrity issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_coverage_no_baseline(self):
        """无 health_baseline.json -> 不产生 file_count_drop issue。"""
        product = "cov-no-baseline"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("col1\n1\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        cov_issues = [i for i in result["issues"] if i["category"] == "coverage_integrity"]
        self.assertEqual(len(cov_issues), 0)


class TestCoverageIntegrityNormal(unittest.TestCase):
    """文件数稳定时不产生 coverage_integrity issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_coverage_normal(self):
        """文件数与基线相同 -> 无 file_count_drop。"""
        from quantclass_sync_internal.status_store import report_dir_path
        product = "cov-normal"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("col1\n1\n", encoding="utf-8")

        # 手动写入基线：1 个文件
        rdir = report_dir_path(self.data_root)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "health_baseline.json").write_text(
            json.dumps({product: 1}), encoding="utf-8"
        )

        result = check_data_health(self.data_root, [product])
        cov_issues = [i for i in result["issues"] if i["category"] == "coverage_integrity"]
        self.assertEqual(len(cov_issues), 0)


class TestCoverageIntegrityDropDetected(unittest.TestCase):
    """文件数下降超过 20% 时报告 file_count_drop。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_coverage_drop_detected(self):
        """基线 10 个文件，当前 5 个（-50%）-> file_count_drop warning。"""
        from quantclass_sync_internal.status_store import report_dir_path
        product = "cov-drop"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 当前只有 5 个文件
        for i in range(5):
            (pdir / f"data{i}.csv").write_text("col1\n1\n", encoding="utf-8")

        # 基线记录 10 个文件
        rdir = report_dir_path(self.data_root)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "health_baseline.json").write_text(
            json.dumps({product: 10}), encoding="utf-8"
        )

        result = check_data_health(self.data_root, [product])
        cov_issues = [i for i in result["issues"] if i["type"] == "file_count_drop"]
        self.assertEqual(len(cov_issues), 1)
        self.assertEqual(cov_issues[0]["severity"], "warning")
        self.assertEqual(cov_issues[0]["category"], "coverage_integrity")
        self.assertFalse(cov_issues[0]["repairable"])
        self.assertEqual(cov_issues[0]["repair_action"], "needs_investigation")
        # detail 应包含原始数和当前数
        self.assertIn("10", cov_issues[0]["detail"])
        self.assertIn("5", cov_issues[0]["detail"])


class TestCoverageBaselineNotSavedOnWarning(unittest.TestCase):
    """有 coverage_integrity 告警时，基线不应被更新。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_coverage_baseline_not_saved_on_warning(self):
        """检测到文件数下降 -> 基线保持原值，不被当前数覆盖。"""
        from quantclass_sync_internal.status_store import report_dir_path
        product = "cov-no-save"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 当前只有 2 个文件
        for i in range(2):
            (pdir / f"data{i}.csv").write_text("col1\n1\n", encoding="utf-8")

        # 基线记录 10 个文件
        rdir = report_dir_path(self.data_root)
        rdir.mkdir(parents=True, exist_ok=True)
        baseline_path = rdir / "health_baseline.json"
        original_baseline = {product: 10}
        baseline_path.write_text(json.dumps(original_baseline), encoding="utf-8")

        check_data_health(self.data_root, [product])

        # 基线文件应仍为原始值（不被 2 覆盖）
        saved = json.loads(baseline_path.read_text(encoding="utf-8"))
        self.assertEqual(saved.get(product), 10)


class TestFormatIntegrityConsistent(unittest.TestCase):
    """所有 CSV 列名相同时不产生 format_integrity issue。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_format_consistent(self):
        """同产品所有 CSV 列名相同 -> 无 column_inconsistency。"""
        product = "fmt-consistent"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        for i in range(3):
            (pdir / f"data{i}.csv").write_text("date,code,close\n2026-01-01,A,10\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        fmt_issues = [i for i in result["issues"] if i["category"] == "format_integrity"]
        self.assertEqual(len(fmt_issues), 0)


class TestFormatIntegrityInconsistent(unittest.TestCase):
    """不同 CSV 列名不同时报告 column_inconsistency。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_format_inconsistent(self):
        """两个 CSV 列名不同 -> column_inconsistency warning。"""
        product = "fmt-inconsistent"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        # 第一个 CSV：3 列
        (pdir / "data1.csv").write_text("date,code,close\n2026-01-01,A,10\n", encoding="utf-8")
        # 第二个 CSV：列名不同
        (pdir / "data2.csv").write_text("trade_date,symbol,price,volume\n2026-01-01,B,20,100\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        fmt_issues = [i for i in result["issues"] if i["type"] == "column_inconsistency"]
        self.assertEqual(len(fmt_issues), 1)
        self.assertEqual(fmt_issues[0]["severity"], "warning")
        self.assertEqual(fmt_issues[0]["category"], "format_integrity")
        self.assertFalse(fmt_issues[0]["repairable"])
        self.assertEqual(fmt_issues[0]["repair_action"], "needs_investigation")
        # detail 应提及抽样文件数和不同列名组合数
        self.assertIn("2", fmt_issues[0]["detail"])


class TestFormatIntegrityTooFewFiles(unittest.TestCase):
    """CSV 文件数少于 2 时跳过格式完整性检查。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_format_too_few_files(self):
        """只有 1 个 CSV -> 跳过 format_integrity 检查，无 issue。"""
        product = "fmt-single-file"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("date,code,close\n2026-01-01,A,10\n", encoding="utf-8")

        result = check_data_health(self.data_root, [product])
        fmt_issues = [i for i in result["issues"] if i["category"] == "format_integrity"]
        self.assertEqual(len(fmt_issues), 0)


class TestRepairTruncateTail(unittest.TestCase):
    """repair_data_issues 修复 truncate_tail：截断不完整末尾行。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_truncate_tail(self):
        """CSV 末尾行列数不匹配 -> 修复后末尾行列数与表头一致。"""
        product = "trunc-product"
        pdir = self.data_root / product
        pdir.mkdir()
        # 表头 3 列，末尾行只有 2 列（不完整）
        csv_path = pdir / "data.csv"
        csv_path.write_text("col1,col2,col3\n1,2,3\n4,5\n", encoding="utf-8")

        issue = {
            "type": "tail_corruption",
            "severity": "error",
            "category": "file_integrity",
            "product": product,
            "detail": "末尾行不完整",
            "file": "data.csv",
            "repairable": True,
            "repair_action": "truncate_tail",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 1)
        self.assertEqual(len(result["failed"]), 0)
        # 修复后末尾行应为完整的第一条数据行 1,2,3
        lines = [l for l in csv_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        last_line = lines[-1]
        self.assertEqual(len(last_line.split(",")), 3)
        # 残行 "4,5" 应已被移除
        self.assertNotIn("4,5", csv_path.read_text(encoding="utf-8"))


class TestRepairDeleteTempFile(unittest.TestCase):
    """repair_data_issues 修复 delete_temp：删除残留 .tmp- 文件。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_delete_temp_file(self):
        """产品目录内 .tmp- 文件被删除。"""
        product = "some-product"
        pdir = self.data_root / product
        pdir.mkdir()
        tmp_name = ".tmp-atomic-sh600000.csv-12345"
        tmp_path = pdir / tmp_name
        tmp_path.write_text("temp data")

        issue = {
            "type": "orphan_temp",
            "severity": "warning",
            "category": "file_integrity",
            "product": product,
            "detail": f"残留临时文件: {tmp_name}",
            "file": tmp_name,
            "repairable": True,
            "repair_action": "delete_temp",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 1)
        self.assertEqual(len(result["failed"]), 0)
        self.assertFalse(tmp_path.exists())


class TestRepairDeleteTempDir(unittest.TestCase):
    """repair_data_issues 修复 delete_temp：删除残留 .tmp- 目录。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_delete_temp_dir(self):
        """.tmp- 目录（含子文件）被递归删除。"""
        product = "some-product"
        pdir = self.data_root / product
        pdir.mkdir()
        tmp_dir_name = ".tmp-extract-abc123"
        tmp_dir = pdir / tmp_dir_name
        tmp_dir.mkdir()
        (tmp_dir / "inner.csv").write_text("some data")

        issue = {
            "type": "orphan_temp",
            "severity": "warning",
            "category": "file_integrity",
            "product": product,
            "detail": f"残留临时文件: {tmp_dir_name}",
            "file": tmp_dir_name,
            "repairable": True,
            "repair_action": "delete_temp",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 1)
        self.assertEqual(len(result["failed"]), 0)
        self.assertFalse(tmp_dir.exists())

    def test_repair_delete_temp_root(self):
        """data_root 根级 .tmp- 文件（product=(root)）被删除。"""
        tmp_name = ".tmp-orphan-root"
        tmp_path = self.data_root / tmp_name
        tmp_path.write_text("root temp")

        issue = {
            "type": "orphan_temp",
            "severity": "warning",
            "category": "file_integrity",
            "product": "(root)",
            "detail": f"残留临时文件: {tmp_name}",
            "file": tmp_name,
            "repairable": True,
            "repair_action": "delete_temp",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 1)
        self.assertFalse(tmp_path.exists())


class TestRepairDedupRows(unittest.TestCase):
    """repair_data_issues 修复 dedup_rows：去重后保留最后出现的行。"""

    # stock-main-index-data: key_cols=("index_code","candle_end_time"), has_note=False, encoding="utf-8-sig"
    PRODUCT = "stock-main-index-data"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        self.pdir = self.data_root / self.PRODUCT
        self.pdir.mkdir()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_dedup_rows(self):
        """含主键重复行的 CSV -> 修复后行数减少，主键唯一。"""
        csv_path = self.pdir / "data.csv"
        # 行 1 和行 2 主键相同，行 2 是"更新版"（close=999），修复后应保留行 2
        csv_content = (
            "index_code,candle_end_time,close\n"
            "000001,2026-01-01,100\n"
            "000001,2026-01-01,999\n"   # 重复行，应保留此行（最后出现）
            "000002,2026-01-01,200\n"
        )
        csv_path.write_text(csv_content, encoding="utf-8-sig")

        issue = {
            "type": "duplicate_rows",
            "severity": "warning",
            "category": "content_integrity",
            "product": self.PRODUCT,
            "detail": "发现 1 行主键重复",
            "file": "data.csv",
            "repairable": True,
            "repair_action": "dedup_rows",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 1)
        self.assertEqual(len(result["failed"]), 0)

        # 验证修复后文件内容
        repaired_text = csv_path.read_text(encoding="utf-8-sig")
        lines = [l for l in repaired_text.splitlines() if l.strip()]
        # 表头 + 2 条唯一记录
        self.assertEqual(len(lines), 3)
        # 应保留 close=999 的行而不是 close=100
        self.assertIn("999", repaired_text)
        self.assertNotIn("100", repaired_text)


class TestRepairUnknownAction(unittest.TestCase):
    """未知 repair_action 的 issue 应进入 failed 列表。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_unknown_action(self):
        """repair_action='unknown_magic' -> 进入 failed，error 含提示信息。"""
        issue = {
            "type": "some_type",
            "severity": "warning",
            "category": "file_integrity",
            "product": "any-product",
            "detail": "某种问题",
            "file": "data.csv",
            "repairable": True,
            "repair_action": "unknown_magic",
        }
        result = repair_data_issues(self.data_root, [issue])

        self.assertEqual(len(result["repaired"]), 0)
        self.assertEqual(len(result["failed"]), 1)
        self.assertIn("未知修复动作", result["failed"][0]["error"])


class TestRepairMixedResults(unittest.TestCase):
    """混合 issues：可修复 + 不可修复 + 未知动作，结果分类正确。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_repair_returns_repaired_and_failed(self):
        """repairable=False 的 issue 被跳过，只处理 repairable=True。"""
        product = "mix-product"
        pdir = self.data_root / product
        pdir.mkdir()
        tmp_name = ".tmp-mix-file"
        (pdir / tmp_name).write_text("temp")

        # 可修复的临时文件 issue
        repairable_issue = {
            "type": "orphan_temp",
            "severity": "warning",
            "category": "file_integrity",
            "product": product,
            "detail": f"残留临时文件: {tmp_name}",
            "file": tmp_name,
            "repairable": True,
            "repair_action": "delete_temp",
        }
        # 不可修复的 issue（repairable=False，应被跳过）
        non_repairable_issue = {
            "type": "missing_data",
            "severity": "error",
            "category": "file_integrity",
            "product": product,
            "detail": "有 timestamp 但无数据文件",
            "file": "",
            "repairable": False,
            "repair_action": "needs_resync",
        }
        # 未知动作 issue（repairable=True 但动作未知）
        unknown_issue = {
            "type": "some_type",
            "severity": "warning",
            "category": "file_integrity",
            "product": product,
            "detail": "未知问题",
            "file": "",
            "repairable": True,
            "repair_action": "no_such_action",
        }

        result = repair_data_issues(
            self.data_root,
            [repairable_issue, non_repairable_issue, unknown_issue],
        )

        # repairable_issue 成功修复，unknown_issue 进入 failed，non_repairable_issue 被跳过
        self.assertEqual(len(result["repaired"]), 1)
        self.assertEqual(result["repaired"][0]["type"], "orphan_temp")
        self.assertEqual(len(result["failed"]), 1)
        self.assertEqual(result["failed"][0]["type"], "some_type")

    def test_repair_progress_callback_called(self):
        """progress_callback 被调用，参数含 product 和 phase='repairing'。"""
        product = "cb-product"
        pdir = self.data_root / product
        pdir.mkdir()
        tmp_name = ".tmp-cb"
        (pdir / tmp_name).write_text("temp")

        issue = {
            "type": "orphan_temp",
            "severity": "warning",
            "category": "file_integrity",
            "product": product,
            "detail": f"残留临时文件: {tmp_name}",
            "file": tmp_name,
            "repairable": True,
            "repair_action": "delete_temp",
        }
        calls = []
        def cb(current, total, prod, phase):
            calls.append((current, total, prod, phase))

        repair_data_issues(self.data_root, [issue], progress_callback=cb)

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], product)
        self.assertEqual(calls[0][3], "repairing")

    def test_repair_empty_issues(self):
        """空 issues 列表 -> repaired/failed 均为空。"""
        result = repair_data_issues(self.data_root, [])
        self.assertEqual(result["repaired"], [])
        self.assertEqual(result["failed"], [])


if __name__ == "__main__":
    unittest.main()
