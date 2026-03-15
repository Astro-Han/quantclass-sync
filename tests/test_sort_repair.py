import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.config import build_product_plan
from quantclass_sync_internal.constants import REASON_OK, STRATEGY_MERGE_KNOWN
from quantclass_sync_internal.csv_engine import read_csv_payload
from quantclass_sync_internal.file_sync import repair_sort_product_files, sync_known_product


def _write_notices_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "数据由邢不行整理，对数据字段有疑问的，可以直接微信私信邢不行，微信号：xbx6064",
        "公告日期,股票代码,股票名称,公告标题",
    ]
    for row in rows:
        lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n", encoding="gb18030", newline="")


class SortRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_stock_notices_title_is_known_merge_rule(self) -> None:
        plans = build_product_plan(["stock-notices-title"])
        self.assertEqual(1, len(plans))
        self.assertEqual(STRATEGY_MERGE_KNOWN, plans[0].strategy)

    def test_repair_sort_product_files_rewrites_unsorted_notices_csv(self) -> None:
        target = self.root / "stock-notices-title" / "2026-02-10.csv"
        _write_notices_csv(
            target,
            rows=[
                ["2026-02-10", "sh600000", "浦发银行", "公告B"],
                ["2026-02-09", "sh600000", "浦发银行", "公告A"],
            ],
        )

        stats, error_count = repair_sort_product_files(
            product="stock-notices-title",
            data_root=self.root,
            dry_run=False,
            strict=True,
        )

        self.assertEqual(0, error_count)
        self.assertEqual(1, stats.updated_files)
        self.assertEqual(1, stats.sorted_checked_files)
        self.assertEqual(1, stats.sorted_violation_files)
        self.assertEqual(1, stats.sorted_auto_repaired_files)

        payload = read_csv_payload(target, preferred_encoding="gb18030")
        self.assertGreaterEqual(len(payload.rows), 2)
        self.assertLessEqual(payload.rows[0][0], payload.rows[1][0])

    def test_repair_sort_product_files_dry_run_keeps_file_content(self) -> None:
        target = self.root / "stock-notices-title" / "2026-02-11.csv"
        _write_notices_csv(
            target,
            rows=[
                ["2026-02-11", "sh600000", "浦发银行", "公告B"],
                ["2026-02-09", "sh600000", "浦发银行", "公告A"],
            ],
        )
        before = target.read_text(encoding="gb18030")

        stats, error_count = repair_sort_product_files(
            product="stock-notices-title",
            data_root=self.root,
            dry_run=True,
            strict=True,
        )

        self.assertEqual(0, error_count)
        self.assertEqual(1, stats.updated_files)
        self.assertEqual(1, stats.sorted_violation_files)
        self.assertEqual(before, target.read_text(encoding="gb18030"))

    def test_sync_known_product_sorts_stock_notices_title_on_write(self) -> None:
        extract_root = self.root / "extract"
        src = extract_root / "stock-notices-title-daily" / "2026-02-12.csv"
        _write_notices_csv(
            src,
            rows=[
                ["2026-02-12", "sh600001", "邯郸钢铁", "公告B"],
                ["2026-02-11", "sh600001", "邯郸钢铁", "公告A"],
            ],
        )

        stats, reason_code = sync_known_product(
            product="stock-notices-title",
            extract_path=extract_root,
            data_root=self.root,
            dry_run=False,
        )

        self.assertEqual(REASON_OK, reason_code)
        self.assertEqual(1, stats.created_files)
        # merge_payload 已排序后设 pre_sorted=True，跳过冗余校验
        self.assertEqual(0, stats.sorted_checked_files)
        self.assertEqual(0, stats.sorted_violation_files)

        target = self.root / "stock-notices-title" / "2026-02-12.csv"
        payload = read_csv_payload(target, preferred_encoding="gb18030")
        self.assertGreaterEqual(len(payload.rows), 2)
        self.assertLessEqual(payload.rows[0][0], payload.rows[1][0])


if __name__ == "__main__":
    unittest.main()
