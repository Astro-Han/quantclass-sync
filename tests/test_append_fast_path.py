"""追加快捷路径单元测试。"""

import csv
import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.csv_engine import _read_head_header, _read_tail_sort_key, row_sort_key, resolve_sort_indices
from quantclass_sync_internal.models import DatasetRule


class TestReadHeadHeader(unittest.TestCase):
    """读取文件首行表头和分隔符。"""

    def _make_rule(self, encoding="utf-8", has_note=False):
        return DatasetRule(
            name="test", encoding=encoding, has_note=has_note,
            key_cols=(), sort_cols=(),
        )

    def _write_csv(self, path, lines, encoding="utf-8"):
        path.write_text("\n".join(lines) + "\n", encoding=encoding)

    def test_normal_csv(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            self._write_csv(p, ["col_a,col_b,col_c", "1,2,3"])
            result = _read_head_header(p, self._make_rule())
            self.assertIsNotNone(result)
            header, delim = result
            self.assertEqual(header, ["col_a", "col_b", "col_c"])
            self.assertEqual(delim, ",")

    def test_with_note(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            self._write_csv(p, ["数据说明", "股票代码,交易日期", "sh600000,2024-01-01"])
            result = _read_head_header(p, self._make_rule(has_note=True))
            self.assertIsNotNone(result)
            header, _ = result
            self.assertIn("股票代码", header)

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("")
            result = _read_head_header(p, self._make_rule())
            self.assertIsNone(result)

    def test_gb18030_encoding(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            self._write_csv(p, ["股票代码,交易日期,收盘价", "sh600000,2024-01-01,10"], encoding="gb18030")
            result = _read_head_header(p, self._make_rule(encoding="gb18030"))
            self.assertIsNotNone(result)
            header, _ = result
            self.assertEqual(header[0], "股票代码")


class TestReadTailSortKey(unittest.TestCase):
    """读取文件尾部排序键。"""

    def _make_rule(self, sort_cols=("交易日期",), encoding="utf-8"):
        return DatasetRule(
            name="test", encoding=encoding, has_note=False,
            key_cols=(), sort_cols=sort_cols,
        )

    def test_normal_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            header = ["股票代码", "交易日期", "收盘价"]
            p.write_text(
                "股票代码,交易日期,收盘价\nsh600000,2024-01-08,10\nsh600000,2024-01-09,11\nsh600000,2024-01-10,12\n",
                encoding="utf-8",
            )
            result = _read_tail_sort_key(p, header, self._make_rule(), ",")
            self.assertIsNotNone(result)
            expected = row_sort_key(["sh600000", "2024-01-10", "12"], resolve_sort_indices(header, self._make_rule()))
            self.assertEqual(result, expected)

    def test_single_row_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("股票代码,交易日期\nsh600000,2024-01-10\n", encoding="utf-8")
            header = ["股票代码", "交易日期"]
            result = _read_tail_sort_key(p, header, self._make_rule(), ",")
            self.assertIsNotNone(result)

    def test_empty_file_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("")
            result = _read_tail_sort_key(p, [], self._make_rule(), ",")
            self.assertIsNone(result)

    def test_gb18030_tail_read(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            header = ["股票代码", "交易日期"]
            lines = ["股票代码,交易日期"]
            for i in range(1, 100):
                lines.append(f"sh600000,2024-01-{i:02d}")
            p.write_text("\n".join(lines) + "\n", encoding="gb18030")
            result = _read_tail_sort_key(p, header, self._make_rule(encoding="gb18030"), ",")
            self.assertIsNotNone(result)

    def test_with_note_file(self):
        """有 note 行的文件，尾部读取不受影响。"""
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("数据说明\n股票代码,交易日期\nsh600000,2024-01-10\n", encoding="utf-8")
            header = ["股票代码", "交易日期"]
            result = _read_tail_sort_key(p, header, self._make_rule(), ",")
            self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
