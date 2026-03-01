import unittest
from pathlib import Path

from quantclass_sync_internal.file_sync import infer_target_relpath


class PeriodOffsetNamespaceTests(unittest.TestCase):
    def test_period_offset_product_maps_special_files_to_global_paths(self) -> None:
        self.assertEqual(Path("period_offset.csv"), infer_target_relpath(Path("period_offset.csv"), "period_offset"))
        self.assertEqual(Path("period_offset.ts"), infer_target_relpath(Path("period_offset.ts"), "period_offset"))

    def test_non_period_offset_product_keeps_special_files_in_own_namespace(self) -> None:
        self.assertEqual(
            Path("demo-product") / "period_offset.csv",
            infer_target_relpath(Path("period_offset.csv"), "demo-product"),
        )
        self.assertEqual(
            Path("demo-product") / "period_offset.ts",
            infer_target_relpath(Path("period_offset.ts"), "demo-product"),
        )
        self.assertEqual(
            Path("stock-trading-data") / "period_offset.csv",
            infer_target_relpath(Path("period_offset.csv"), "stock-trading-data"),
        )
        self.assertEqual(
            Path("stock-trading-data") / "period_offset.ts",
            infer_target_relpath(Path("period_offset.ts"), "stock-trading-data"),
        )


if __name__ == "__main__":
    unittest.main()
