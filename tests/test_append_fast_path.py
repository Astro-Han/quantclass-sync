"""追加快捷路径单元测试。"""

import csv
import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.csv_engine import _read_head_header
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


if __name__ == "__main__":
    unittest.main()
