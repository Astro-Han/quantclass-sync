import unittest
from types import SimpleNamespace
from unittest.mock import patch

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
            "main",
            "requests",
            "time",
            "sys",
            "REASON_MIRROR_FALLBACK",
            "REASON_NETWORK_ERROR",
            "REASON_EXTRACT_ERROR",
            "REASON_UNKNOWN_LOCAL_PRODUCT",
            "REASON_INVALID_EXPLICIT_PRODUCT",
            "REASON_UNKNOWN_HEADER_MERGE",
            "REASON_PREPROCESS_OK",
            "REASON_PREPROCESS_FULL_REBUILD_OK",
            "REASON_NO_VALID_OUTPUT",
        ]
        missing = [name for name in expected if not hasattr(qcs, name)]
        self.assertEqual([], missing)
        for name in expected:
            self.assertIn(name, qcs.__all__)

    def test_direct_http_exports_honor_patched_requests_module(self) -> None:
        fake_requests = SimpleNamespace(
            RequestException=Exception,
            request=lambda **kwargs: SimpleNamespace(status_code=200, text="2026-02-11"),
        )
        with patch("quantclass_sync.requests", fake_requests):
            actual = qcs.get_latest_time(
                api_base="https://example.invalid",
                product="stock-trading-data",
                hid="h",
                headers={"api-key": "k"},
            )

        self.assertEqual("2026-02-11", actual)

    def test_main_returns_130_when_keyboard_interrupt(self) -> None:
        with patch("quantclass_sync.app", side_effect=KeyboardInterrupt):
            self.assertEqual(130, qcs.main())

    def test_aggregate_split_cols_align_with_rules(self) -> None:
        split_col_key_index = {"coin-cap": 1}
        for product, split_col in qcs.AGGREGATE_SPLIT_COLS.items():
            self.assertIn(product, qcs.RULES)
            rule = qcs.RULES[product]
            key_idx = split_col_key_index.get(product, 0)
            self.assertGreater(len(rule.key_cols), key_idx)
            self.assertEqual(split_col, rule.key_cols[key_idx])


if __name__ == "__main__":
    unittest.main()
