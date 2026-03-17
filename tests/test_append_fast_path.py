"""追加快捷路径单元测试。"""

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from quantclass_sync_internal.csv_engine import (
    _read_head_header,
    _read_tail_sort_key,
    row_sort_key,
    resolve_sort_indices,
    _is_strictly_increasing,
    _file_ends_with_newline,
    _append_csv_rows,
    read_csv_payload,
    sync_payload_to_target,
)
from quantclass_sync_internal.models import CsvPayload, DatasetRule, SortAudit


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


class TestIsStrictlyIncreasing(unittest.TestCase):

    def _sort_indices(self, header, sort_cols):
        rule = DatasetRule(name="t", encoding="utf-8", has_note=False,
                          key_cols=(), sort_cols=sort_cols)
        return resolve_sort_indices(header, rule)

    def test_strictly_increasing(self):
        header = ["date", "val"]
        indices = self._sort_indices(header, ("date",))
        rows = [["2024-01-01", "a"], ["2024-01-02", "b"], ["2024-01-03", "c"]]
        self.assertTrue(_is_strictly_increasing(rows, indices))

    def test_equal_keys_rejected(self):
        header = ["date", "val"]
        indices = self._sort_indices(header, ("date",))
        rows = [["2024-01-01", "a"], ["2024-01-01", "b"]]
        self.assertFalse(_is_strictly_increasing(rows, indices))

    def test_decreasing_rejected(self):
        header = ["date", "val"]
        indices = self._sort_indices(header, ("date",))
        rows = [["2024-01-02", "a"], ["2024-01-01", "b"]]
        self.assertFalse(_is_strictly_increasing(rows, indices))

    def test_single_row(self):
        header = ["date"]
        indices = self._sort_indices(header, ("date",))
        self.assertTrue(_is_strictly_increasing([["2024-01-01"]], indices))

    def test_empty(self):
        self.assertTrue(_is_strictly_increasing([], [0]))


class TestFileEndsWithNewline(unittest.TestCase):

    def test_ends_with_newline(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("a,b\n1,2\n")
            self.assertTrue(_file_ends_with_newline(p))

    def test_no_trailing_newline(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_bytes(b"a,b\n1,2")
            self.assertFalse(_file_ends_with_newline(p))

    def test_empty_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("")
            self.assertFalse(_file_ends_with_newline(p))


class TestAppendCsvRows(unittest.TestCase):

    def test_append_rows(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("col_a,col_b\n1,2\n")
            _append_csv_rows(p, [["3", "4"], ["5", "6"]], ",", "utf-8")
            lines = p.read_text().strip().split("\n")
            self.assertEqual(len(lines), 4)  # header + 3 data rows
            self.assertEqual(lines[2], "3,4")
            self.assertEqual(lines[3], "5,6")

    def test_preserves_encoding(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "test.csv"
            p.write_text("股票代码,日期\nsh600000,2024-01-01\n", encoding="gb18030")
            _append_csv_rows(p, [["sh600001", "2024-01-02"]], ",", "gb18030")
            text = p.read_text(encoding="gb18030")
            self.assertIn("sh600001", text)


class TestSyncPayloadFastPath(unittest.TestCase):
    """sync_payload_to_target 追加快捷路径。"""

    RULE = DatasetRule(
        name="test-product", encoding="utf-8", has_note=False,
        key_cols=("code", "date"), sort_cols=("date",),
    )
    HEADER = ["code", "date", "value"]

    def _write_existing(self, path, rows):
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(self.HEADER)
            w.writerows(rows)

    def _make_incoming(self, rows):
        return CsvPayload(
            note=None, header=list(self.HEADER), rows=rows,
            encoding="utf-8", delimiter=",",
        )

    def test_fast_path_hit(self):
        """incoming 日期严格大于已有最大值 -- 命中快捷路径。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [
                ["A", "2024-01-08", "1"],
                ["A", "2024-01-09", "2"],
                ["A", "2024-01-10", "3"],
            ])
            incoming = self._make_incoming([["A", "2024-01-11", "4"]])
            # 用 mock 确认走的是快捷路径
            with patch(
                "quantclass_sync_internal.csv_engine._append_csv_rows",
                wraps=_append_csv_rows,
            ) as mock_append:
                status, added, audit = sync_payload_to_target(
                    incoming, target, self.RULE, dry_run=False
                )
                mock_append.assert_called_once()
            self.assertEqual(status, "updated")
            self.assertEqual(added, 1)
            self.assertEqual(audit.checked_files, 1)
            payload = read_csv_payload(target)
            self.assertEqual(len(payload.rows), 4)
            self.assertEqual(payload.rows[-1][1], "2024-01-11")

    def test_fast_path_miss_overlap(self):
        """incoming 日期 <= 已有最大值 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [["A", "2024-01-10", "3"]])
            incoming = self._make_incoming([["A", "2024-01-10", "3_updated"]])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertIn(status, ("updated", "unchanged"))

    def test_fast_path_miss_new_file(self):
        """target 不存在 -- 正常创建。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "new.csv"
            incoming = self._make_incoming([["A", "2024-01-10", "1"]])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertEqual(status, "created")

    def test_fast_path_miss_no_sort_cols(self):
        """无 sort_cols 的规则 -- 走完整合并。"""
        rule = DatasetRule(
            name="nosort", encoding="utf-8", has_note=False,
            key_cols=(), sort_cols=(),
        )
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [["A", "2024-01-10", "1"]])
            incoming = self._make_incoming([["A", "2024-01-11", "2"]])
            status, added, audit = sync_payload_to_target(
                incoming, target, rule, dry_run=False
            )
            self.assertEqual(status, "updated")

    def test_fast_path_miss_header_mismatch(self):
        """表头不匹配 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [["A", "2024-01-10", "1"]])
            incoming = CsvPayload(
                note=None, header=["code", "date", "new_col"],
                rows=[["A", "2024-01-11", "2"]],
                encoding="utf-8", delimiter=",",
            )
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertIn(status, ("updated", "created"))

    def test_fast_path_miss_duplicate_keys(self):
        """incoming 有重复排序键 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [["A", "2024-01-10", "1"]])
            incoming = self._make_incoming([
                ["A", "2024-01-11", "2"],
                ["A", "2024-01-11", "3"],
            ])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertEqual(status, "updated")

    def test_fast_path_miss_incoming_unsorted(self):
        """incoming 内部逆序 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            self._write_existing(target, [["A", "2024-01-10", "1"]])
            incoming = self._make_incoming([
                ["A", "2024-01-12", "3"],
                ["A", "2024-01-11", "2"],
            ])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertEqual(status, "updated")
            payload = read_csv_payload(target)
            dates = [r[1] for r in payload.rows]
            self.assertEqual(dates, sorted(dates))

    def test_fast_path_miss_delimiter_mismatch(self):
        """分隔符不匹配 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            target.write_text("code\tdate\tvalue\nA\t2024-01-10\t1\n")
            incoming = self._make_incoming([["A", "2024-01-11", "2"]])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertIn(status, ("updated", "created", "unchanged"))

    def test_fast_path_miss_no_trailing_newline(self):
        """文件末尾无换行 -- 回退完整合并。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "test.csv"
            target.write_bytes(b"code,date,value\nA,2024-01-10,1")
            incoming = self._make_incoming([["A", "2024-01-11", "2"]])
            status, added, audit = sync_payload_to_target(
                incoming, target, self.RULE, dry_run=False
            )
            self.assertEqual(status, "updated")
            payload = read_csv_payload(target)
            self.assertEqual(len(payload.rows), 2)


import stat

from quantclass_sync_internal.csv_engine import merge_payload, write_csv_payload


class TestAppendEquivalence(unittest.TestCase):
    """验收标准：快捷追加与完整合并产生完全相同的文件内容。"""

    RULE = DatasetRule(
        name="test-equiv", encoding="utf-8", has_note=False,
        key_cols=("code", "date"), sort_cols=("date",),
    )
    HEADER = ["code", "date", "value"]

    def _write_existing(self, path, rows):
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(self.HEADER)
            w.writerows(rows)

    def _make_incoming(self, rows):
        return CsvPayload(
            note=None, header=list(self.HEADER), rows=rows,
            encoding="utf-8", delimiter=",",
        )

    def test_single_day_equivalence(self):
        """单天追加 vs 完整合并 -- 结果一致。"""
        base_rows = [["A", f"2024-01-{d:02d}", str(d)] for d in range(1, 11)]
        new_rows = [["A", "2024-01-11", "11"]]

        with tempfile.TemporaryDirectory() as d:
            # 路径 A：快捷追加
            fast = Path(d) / "fast.csv"
            self._write_existing(fast, base_rows)
            sync_payload_to_target(self._make_incoming(new_rows), fast, self.RULE, False)

            # 路径 B：直接调 merge_payload + write_csv_payload（绕过快捷路径）
            full = Path(d) / "full.csv"
            self._write_existing(full, base_rows)
            existing = read_csv_payload(full)
            merged, _ = merge_payload(existing, self._make_incoming(new_rows), self.RULE)
            write_csv_payload(full, merged, self.RULE, dry_run=False)

            fast_payload = read_csv_payload(fast)
            full_payload = read_csv_payload(full)
            self.assertEqual(fast_payload.header, full_payload.header)
            self.assertEqual(fast_payload.rows, full_payload.rows)

    def test_five_day_catchup_equivalence(self):
        """5 天连续追加 vs 一次性完整合并 -- 结果一致。"""
        base_rows = [["A", f"2024-01-{d:02d}", str(d)] for d in range(1, 11)]

        with tempfile.TemporaryDirectory() as d:
            # 路径 A：5 次追加
            fast = Path(d) / "fast.csv"
            self._write_existing(fast, base_rows)
            for day in range(11, 16):
                incoming = self._make_incoming([["A", f"2024-01-{day:02d}", str(day)]])
                sync_payload_to_target(incoming, fast, self.RULE, False)

            # 路径 B：直接调 merge_payload
            full = Path(d) / "full.csv"
            self._write_existing(full, base_rows)
            existing = read_csv_payload(full)
            all_new = [["A", f"2024-01-{day:02d}", str(day)] for day in range(11, 16)]
            merged, _ = merge_payload(existing, self._make_incoming(all_new), self.RULE)
            write_csv_payload(full, merged, self.RULE, dry_run=False)

            fast_payload = read_csv_payload(fast)
            full_payload = read_csv_payload(full)
            self.assertEqual(fast_payload.rows, full_payload.rows)


class TestAppendCsvRowsAtomic(unittest.TestCase):
    """验证 _append_csv_rows 的原子写入行为。"""

    def test_append_creates_no_tmp_on_success(self):
        """成功追加后不留临时文件，内容包含旧行和新行。"""
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "data.csv"
            p.write_text("col_a,col_b\n1,2\n", encoding="utf-8")
            _append_csv_rows(p, [["3", "4"], ["5", "6"]], ",", "utf-8")
            # 不应留下 .tmp-append-* 临时文件
            tmp_files = list(Path(d).glob(".tmp-append-*"))
            self.assertEqual(tmp_files, [], f"残留临时文件: {tmp_files}")
            # 原文件内容正确
            lines = p.read_text(encoding="utf-8").strip().split("\n")
            self.assertEqual(lines, ["col_a,col_b", "1,2", "3,4", "5,6"])

    def test_append_preserves_original_on_write_error(self):
        """写入失败时原文件保持不变。"""
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "data.csv"
            original_content = "col_a,col_b\n1,2\n"
            p.write_text(original_content, encoding="utf-8")
            # 将目录设为只读，使 shutil.copy2 创建临时文件后无法写入
            # 改用 patch open 模拟写入中途崩溃
            from unittest.mock import patch, mock_open
            real_open = Path.open

            def fail_on_append(self_path, mode="r", **kwargs):
                if mode == "a":
                    raise OSError("模拟写入崩溃")
                return real_open(self_path, mode, **kwargs)

            with patch.object(Path, "open", fail_on_append):
                with self.assertRaises(OSError):
                    _append_csv_rows(p, [["3", "4"]], ",", "utf-8")

            # 原文件内容不变
            self.assertEqual(p.read_text(encoding="utf-8"), original_content)
            # 临时文件已被清理
            tmp_files = list(Path(d).glob(".tmp-append-*"))
            self.assertEqual(tmp_files, [], f"残留临时文件: {tmp_files}")


if __name__ == "__main__":
    unittest.main()
