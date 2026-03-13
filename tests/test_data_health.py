"""check_data_health 单元测试。"""

import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.constants import KNOWN_DATASETS, TIMESTAMP_FILE_NAME
from quantclass_sync_internal.data_query import (
    _check_csv_unreadable,
    _check_missing_data,
    _check_orphan_temp,
    check_data_health,
)


class TestCheckMissingData(unittest.TestCase):
    """有 timestamp.txt 但无数据文件的检测。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_has_timestamp_no_csv(self):
        """有 timestamp.txt 无 CSV/zip -> 报 missing_data。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["type"], "missing_data")
        self.assertEqual(issues[0]["product"], product)

    def test_has_timestamp_has_csv(self):
        """有 timestamp.txt 且有 CSV -> 不报问题。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("col1,col2\n1,2\n")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 0)

    def test_no_timestamp(self):
        """无 timestamp.txt -> 跳过检查，不报问题。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 0)

    def test_has_timestamp_has_zip(self):
        """有 timestamp.txt 且有 zip 文件 -> 不报问题。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "archive.zip").write_bytes(b"fake zip")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 0)

    def test_has_timestamp_has_extensionless_files(self):
        """有 timestamp.txt 且有无后缀文件 -> 不报问题。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "bj832317").write_text("some data")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 0)

    def test_has_timestamp_has_subdirectories(self):
        """有 timestamp.txt 且有子目录（如 stock-fin-data-xbx 按代码拆分）-> 不报问题。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        subdir = pdir / "bj832317"
        subdir.mkdir()
        (subdir / "bj832317.csv").write_text("col1\n1\n")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 0)

    def test_only_timestamp_file(self):
        """目录里只有 timestamp.txt，无其他文件 -> 报 missing_data。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 1)

    def test_product_dir_not_exists(self):
        """产品目录不存在 -> 不报问题（没有 timestamp.txt）。"""
        issues = _check_missing_data(self.data_root, ["nonexistent"])
        self.assertEqual(len(issues), 0)

    def test_hidden_files_ignored(self):
        """只有隐藏文件（.开头）不算数据文件。"""
        product = "test-product"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / ".hidden.csv").write_text("col1\n1\n")

        issues = _check_missing_data(self.data_root, [product])
        self.assertEqual(len(issues), 1)


class TestCheckCsvUnreadable(unittest.TestCase):
    """CSV 可读性检测（仅 KNOWN_DATASETS）。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)
        # 取一个已知产品用于测试
        self.known_product = KNOWN_DATASETS[0]

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_garbled_csv(self):
        """乱码 CSV -> 报 csv_unreadable。"""
        pdir = self.data_root / self.known_product
        pdir.mkdir(parents=True)
        # 写入无法被任何候选编码解码的字节序列
        (pdir / "bad.csv").write_bytes(b"\x80\x81\x82\x83\x84\x85")

        issues = _check_csv_unreadable(self.data_root)
        unreadable = [i for i in issues if i["product"] == self.known_product]
        self.assertEqual(len(unreadable), 1)
        self.assertEqual(unreadable[0]["type"], "csv_unreadable")
        self.assertIn("bad.csv", unreadable[0]["file"])

    def test_empty_csv(self):
        """空 CSV（0 字节）-> 报 csv_unreadable。"""
        pdir = self.data_root / self.known_product
        pdir.mkdir(parents=True)
        (pdir / "empty.csv").write_text("")

        issues = _check_csv_unreadable(self.data_root)
        unreadable = [i for i in issues if i["product"] == self.known_product]
        self.assertEqual(len(unreadable), 1)
        self.assertIn("empty.csv", unreadable[0]["file"])

    def test_normal_csv(self):
        """正常 CSV -> 不报问题。"""
        pdir = self.data_root / self.known_product
        pdir.mkdir(parents=True)
        (pdir / "good.csv").write_text("col1,col2\n1,2\n", encoding="utf-8")

        issues = _check_csv_unreadable(self.data_root)
        unreadable = [i for i in issues if i["product"] == self.known_product]
        self.assertEqual(len(unreadable), 0)

    def test_non_known_product_skipped(self):
        """非 KNOWN_DATASETS 产品 -> 跳过检查。"""
        unknown = "definitely-not-known-product"
        self.assertNotIn(unknown, KNOWN_DATASETS)
        pdir = self.data_root / unknown
        pdir.mkdir(parents=True)
        (pdir / "bad.csv").write_bytes(b"\x80\x81\x82\x83")

        issues = _check_csv_unreadable(self.data_root)
        self.assertEqual(len(issues), 0)

    def test_hidden_csv_ignored(self):
        """隐藏 CSV 文件（.开头）-> 跳过。"""
        pdir = self.data_root / self.known_product
        pdir.mkdir(parents=True)
        (pdir / ".hidden.csv").write_bytes(b"\x80\x81\x82\x83")

        issues = _check_csv_unreadable(self.data_root)
        self.assertEqual(len(issues), 0)

    def test_gb18030_csv_readable(self):
        """gb18030 编码 CSV -> 正常读取，不报问题。"""
        pdir = self.data_root / self.known_product
        pdir.mkdir(parents=True)
        (pdir / "chinese.csv").write_text("股票代码,交易日期\nsh600000,2026-01-01\n", encoding="gb18030")

        issues = _check_csv_unreadable(self.data_root)
        unreadable = [i for i in issues if i["product"] == self.known_product]
        self.assertEqual(len(unreadable), 0)


class TestCheckOrphanTemp(unittest.TestCase):
    """残留临时文件检测。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_has_tmp_files(self):
        """有 .tmp- 文件 -> 报 orphan_temp。"""
        pdir = self.data_root / "some-product"
        pdir.mkdir()
        tmp_name = ".sh600000.csv.tmp-atomic-12345-999"
        (pdir / tmp_name).write_text("temp data")

        issues = _check_orphan_temp(self.data_root)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["type"], "orphan_temp")
        self.assertEqual(issues[0]["product"], "some-product")
        self.assertIn(".tmp-", issues[0]["file"])

    def test_no_tmp_files(self):
        """无 .tmp- 文件 -> 不报问题。"""
        pdir = self.data_root / "clean-product"
        pdir.mkdir()
        (pdir / "data.csv").write_text("col1\n1\n")

        issues = _check_orphan_temp(self.data_root)
        self.assertEqual(len(issues), 0)

    def test_nested_subdirectory(self):
        """嵌套子目录中的 .tmp- 文件也能检出。"""
        nested = self.data_root / "product" / "subdir"
        nested.mkdir(parents=True)
        (nested / ".data.csv.tmp-atomic-9999-111").write_text("temp")

        issues = _check_orphan_temp(self.data_root)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["product"], "product")

    def test_tmp_in_root_level(self):
        """data_root 根级的 .tmp- 文件，product 应为空。"""
        (self.data_root / ".something.tmp-orphan-1").write_text("temp")

        issues = _check_orphan_temp(self.data_root)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["product"], "")


class TestCheckDataHealthIntegration(unittest.TestCase):
    """check_data_health 集成测试。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.data_root = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_mixed_issues(self):
        """混合多种问题，summary 计数正确。"""
        # missing_data: 有 timestamp 无 csv
        missing_product = "missing-prod"
        mdir = self.data_root / missing_product
        mdir.mkdir()
        (mdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")

        # orphan_temp: 残留临时文件
        orphan_dir = self.data_root / "some-dir"
        orphan_dir.mkdir()
        (orphan_dir / ".x.csv.tmp-atomic-111-222").write_text("tmp")

        result = check_data_health(self.data_root, [missing_product])
        self.assertGreaterEqual(result["summary"]["total"], 2)
        self.assertEqual(result["summary"]["missing_data"], 1)
        self.assertEqual(result["summary"]["orphan_temp"], 1)
        # summary total 等于 issues 列表长度
        self.assertEqual(result["summary"]["total"], len(result["issues"]))

    def test_data_root_not_exists(self):
        """data_root 不存在 -> 返回空报告。"""
        fake_root = Path("/tmp/nonexistent_health_check_test_dir")
        result = check_data_health(fake_root, ["any-product"])
        self.assertEqual(result["issues"], [])
        self.assertEqual(result["summary"]["total"], 0)
        self.assertEqual(result["scanned_products"], 0)

    def test_empty_catalog(self):
        """空 catalog -> 无 missing_data 问题（可能有 orphan_temp）。"""
        result = check_data_health(self.data_root, [])
        self.assertEqual(result["summary"]["missing_data"], 0)
        self.assertEqual(result["scanned_products"], 0)

    def test_summary_counts_consistent(self):
        """summary 各类型计数之和等于 total。"""
        result = check_data_health(self.data_root, [])
        s = result["summary"]
        self.assertEqual(
            s["missing_data"] + s["csv_unreadable"] + s["orphan_temp"],
            s["total"],
        )

    def test_healthy_data(self):
        """数据完全健康 -> issues 为空。"""
        product = "healthy-prod"
        pdir = self.data_root / product
        pdir.mkdir()
        (pdir / TIMESTAMP_FILE_NAME).write_text("2026-01-01,2026-01-01 10:00:00\n")
        (pdir / "data.csv").write_text("col1,col2\n1,2\n")

        result = check_data_health(self.data_root, [product])
        self.assertEqual(result["summary"]["total"], 0)
        self.assertEqual(len(result["issues"]), 0)
        self.assertEqual(result["scanned_products"], 1)

    def test_elapsed_seconds_present(self):
        """返回值包含 elapsed_seconds 且 >= 0。"""
        result = check_data_health(self.data_root, [])
        self.assertIn("elapsed_seconds", result)
        self.assertGreaterEqual(result["elapsed_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
