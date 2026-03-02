import tempfile
import unittest
from pathlib import Path

import quantclass_sync as qcs
from quantclass_sync_internal.constants import (
    REASON_MIRROR_FALLBACK,
    REASON_MIRROR_UNKNOWN,
    REASON_UNKNOWN_HEADER_MERGE,
)
from quantclass_sync_internal.file_sync import sync_unknown_product


def _write_period_offset_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "数据由邢不行整理，对数据字段有疑问的，可以直接微信私信邢不行，微信号：xbx6064",
        "交易日期,偏移量",
        "2026-03-01,0",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="gb18030", newline="")


def _write_simple_csv(path: Path, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8", newline="")


class MirrorReasonCodeTests(unittest.TestCase):
    def test_ts_file_in_known_product_does_not_mark_mirror_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            extract_root = root / "extract"
            data_root = root / "data"
            extract_root.mkdir(parents=True, exist_ok=True)
            data_root.mkdir(parents=True, exist_ok=True)

            _write_period_offset_csv(extract_root / "period_offset.csv")
            (extract_root / "period_offset.ts").write_text("period_offset_ts_payload\n", encoding="utf-8")

            stats, reason_code = qcs.sync_known_product(
                product="period_offset",
                extract_path=extract_root,
                data_root=data_root,
                dry_run=False,
            )

            self.assertNotEqual(REASON_MIRROR_FALLBACK, reason_code)
            self.assertEqual(qcs.REASON_OK, reason_code)
            self.assertGreaterEqual(stats.created_files, 1)

    def test_unknown_product_mirror_returns_mirror_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            extract_root = root / "extract"
            data_root = root / "data"
            extract_root.mkdir(parents=True, exist_ok=True)
            data_root.mkdir(parents=True, exist_ok=True)

            _write_simple_csv(extract_root / "sample.csv", ["a,b", "1,2"])

            _stats, reason_code = sync_unknown_product(
                product="demo-unknown-product",
                extract_path=extract_root,
                data_root=data_root,
                dry_run=False,
            )

            self.assertEqual(REASON_MIRROR_UNKNOWN, reason_code)

    def test_unknown_header_merge_behavior_is_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            extract_root = root / "extract"
            data_root = root / "data"
            extract_root.mkdir(parents=True, exist_ok=True)
            data_root.mkdir(parents=True, exist_ok=True)

            product = "demo-unknown-product"
            _write_simple_csv(extract_root / "sample.csv", ["a,b", "1,2", "3,4"])
            _write_simple_csv(data_root / product / "sample.csv", ["a,b", "1,2"])

            _stats, reason_code = sync_unknown_product(
                product=product,
                extract_path=extract_root,
                data_root=data_root,
                dry_run=False,
            )

            self.assertEqual(REASON_UNKNOWN_HEADER_MERGE, reason_code)

    def test_unknown_header_merge_aligns_columns_with_normalized_header_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            extract_root = root / "extract"
            data_root = root / "data"
            extract_root.mkdir(parents=True, exist_ok=True)
            data_root.mkdir(parents=True, exist_ok=True)

            product = "demo-unknown-product"
            _write_simple_csv(extract_root / "sample.csv", ["a,b", "2,3"])
            _write_simple_csv(data_root / product / "sample.csv", ["\ufeffa ,b", "1,2"])

            _stats, reason_code = sync_unknown_product(
                product=product,
                extract_path=extract_root,
                data_root=data_root,
                dry_run=False,
            )

            self.assertEqual(REASON_UNKNOWN_HEADER_MERGE, reason_code)
            payload = qcs.read_csv_payload(data_root / product / "sample.csv")
            self.assertEqual(2, len(payload.rows))
            self.assertEqual(["1", "2"], payload.rows[0])
            self.assertEqual(["2", "3"], payload.rows[1])


if __name__ == "__main__":
    unittest.main()
