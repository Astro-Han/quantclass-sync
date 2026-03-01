import unittest

import quantclass_sync as qcs


class ImportCompatTests(unittest.TestCase):
    """验证 quantclass_sync 兼容导出面仍可直接使用。"""

    def test_expected_symbols_are_exported(self) -> None:
        expected = [
            "CommandContext",
            "RunReport",
            "SyncStats",
            "build_product_plan",
            "request_data",
            "_execute_plans",
            "_resolve_requested_dates_for_plan",
            "cmd_setup",
            "cmd_update",
            "cmd_repair_sort",
            "app",
        ]
        missing = [name for name in expected if not hasattr(qcs, name)]
        self.assertEqual([], missing)


if __name__ == "__main__":
    unittest.main()
