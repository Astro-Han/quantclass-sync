"""check_updates API 方法测试。"""

import unittest
from unittest.mock import patch, MagicMock

from quantclass_sync_internal.gui.api import SyncApi


class TestCheckUpdates(unittest.TestCase):
    """check_updates() 方法测试。"""

    def setUp(self):
        self.api = SyncApi()

    @patch.object(SyncApi, '_resolve_config')
    def test_config_error_returns_not_ok(self, mock_config):
        """配置解析失败时返回 ok=False。"""
        mock_config.return_value = (None, None, None, "配置文件不存在")
        result = self.api.check_updates()
        self.assertFalse(result["ok"])
        self.assertIn("配置", result["error"])

    @patch('quantclass_sync_internal.gui.api.resolve_credentials_for_update')
    @patch.object(SyncApi, '_resolve_config')
    def test_missing_credentials_returns_not_ok(self, mock_config, mock_creds):
        """凭证缺失时返回 ok=False，不发起 API 请求。"""
        mock_config.return_value = (MagicMock(), MagicMock(), ["product-a"], None)
        mock_creds.return_value = ("", "", "none")
        result = self.api.check_updates()
        self.assertFalse(result["ok"])


    @patch('quantclass_sync_internal.gui.api.get_latest_time')
    @patch('quantclass_sync_internal.gui.api.resolve_credentials_for_update')
    @patch('quantclass_sync_internal.gui.api.get_products_overview')
    @patch.object(SyncApi, '_resolve_config')
    def test_partial_failure_returns_ok_with_failed_count(
        self, mock_config, mock_overview, mock_creds, mock_latest,
    ):
        """部分产品 API 失败时，成功的正常返回，failed 计数正确。"""
        mock_config.return_value = (
            MagicMock(), MagicMock(), ["product-a", "product-b"], None,
        )
        mock_creds.return_value = ("key", "hid", "file")

        def _side_effect(api_base, product, hid, headers):
            if product == "product-a":
                return "2026-03-14"
            raise RuntimeError("网络超时")

        mock_latest.side_effect = _side_effect
        mock_overview.return_value = [
            {"name": "product-a", "local_date": "2026-03-13", "days_behind": 1,
             "last_status": "ok", "last_error": "", "status_color": "yellow"},
            {"name": "product-b", "local_date": "2026-03-13", "days_behind": 0,
             "last_status": "ok", "last_error": "", "status_color": "green"},
        ]

        result = self.api.check_updates()

        self.assertTrue(result["ok"])
        self.assertEqual(result["checked"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertIn("product-b", result["failed_products"])
        # product-a 来自 API，product-b 来自缓存
        by_name = {p["name"]: p for p in result["products"]}
        self.assertEqual(by_name["product-a"]["source"], "api")
        self.assertEqual(by_name["product-b"]["source"], "cached")

    @patch('quantclass_sync_internal.gui.api.get_latest_time')
    @patch('quantclass_sync_internal.gui.api.resolve_credentials_for_update')
    @patch.object(SyncApi, '_resolve_config')
    def test_401_returns_not_ok(self, mock_config, mock_creds, mock_latest):
        """401 全局错误时立即返回 ok=False。"""
        from quantclass_sync_internal.models import FatalRequestError

        mock_config.return_value = (
            MagicMock(), MagicMock(), ["product-a"], None,
        )
        mock_creds.return_value = ("key", "hid", "file")
        mock_latest.side_effect = FatalRequestError("超出当日下载次数", status_code=401)

        result = self.api.check_updates()

        self.assertFalse(result["ok"])
        self.assertIn("401", result["error"])


if __name__ == "__main__":
    unittest.main()
