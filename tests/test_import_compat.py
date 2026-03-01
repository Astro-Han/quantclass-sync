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
            "get_latest_times",
            "get_latest_time",
            "get_download_link",
            "parse_latest_time_candidates",
            "_execute_plans",
            "_probe_downloadable_dates",
            "_resolve_requested_dates_for_plan",
            "_maybe_run_coin_preprocess",
            "run_update_with_settings",
            "resolve_report_path",
            "write_run_report",
            "cmd_setup",
            "cmd_update",
            "cmd_repair_sort",
            "cmd_init",
            "cmd_one_data",
            "cmd_all_data",
            "app",
            "requests",
            "time",
            "sys",
        ]
        missing = [name for name in expected if not hasattr(qcs, name)]
        self.assertEqual([], missing)
        for name in expected:
            self.assertIn(name, qcs.__all__)


if __name__ == "__main__":
    unittest.main()
