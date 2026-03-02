import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

import quantclass_sync as qcs
from quantclass_sync_internal import orchestrator


class HttpErrorMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        qcs._reset_http_metrics()

    def test_request_data_404_raises_structured_fatal_error(self) -> None:
        with patch("quantclass_sync.requests.request", return_value=SimpleNamespace(status_code=404)):
            with self.assertRaises(qcs.FatalRequestError) as ctx:
                qcs.request_data(
                    "GET",
                    url="https://api.quantclass.cn/api/data/get-download-link/stock-fin-data-xbx-daily/2026-02-09?uuid=abc",
                    headers={"api-key": "k"},
                )

        exc = ctx.exception
        self.assertEqual("资源不存在（该产品该日期无可下载数据）", str(exc))
        self.assertEqual(404, exc.status_code)
        self.assertEqual(
            "https://api.quantclass.cn/api/data/get-download-link/stock-fin-data-xbx-daily/2026-02-09",
            exc.request_url,
        )
        self.assertEqual("", exc.response_body)

    def test_request_data_fatal_error_keeps_response_body_preview(self) -> None:
        response = MagicMock(status_code=401)
        response.text = "quota exceeded, retry tomorrow"

        with patch("quantclass_sync.requests.request", return_value=response):
            with self.assertRaises(qcs.FatalRequestError) as ctx:
                qcs.request_data(
                    "GET",
                    url="https://api.quantclass.cn/api/data/fetch/stock-trading-data-daily/latest?uuid=abc",
                    headers={"api-key": "k"},
                )

        self.assertEqual(401, ctx.exception.status_code)
        self.assertIn("quota exceeded", ctx.exception.response_body)
        response.close.assert_called_once()

    def test_request_data_non_200_retries_close_response_each_attempt(self) -> None:
        responses = []
        for _ in range(3):
            item = MagicMock(status_code=500)
            item.text = "server-error"
            responses.append(item)

        with patch("quantclass_sync.time.sleep"), patch(
            "quantclass_sync.requests.request",
            side_effect=responses,
        ):
            with self.assertRaises(RuntimeError):
                qcs.request_data(
                    "GET",
                    url="https://api.quantclass.cn/api/data/fetch/stock-trading-data-daily/latest?uuid=abc",
                    headers={"api-key": "k"},
                    request_profile="latest",
                )

        for item in responses:
            item.close.assert_called_once()

    def test_probe_downloadable_dates_skips_404_and_continues(self) -> None:
        def fake_get_download_link(
            api_base: str,
            product: str,
            date_time: str,
            hid: str,
            headers: dict[str, str],
        ) -> str:
            if date_time == "2026-02-07":
                raise qcs.FatalRequestError(
                    "资源不存在（该产品该日期无可下载数据）",
                    status_code=404,
                    request_url=f"{api_base}/get-download-link/{product}-daily/{date_time}",
                )
            return f"https://example.com/{product}/{date_time}.zip"

        with patch("quantclass_sync.get_download_link", side_effect=fake_get_download_link):
            result = qcs._probe_downloadable_dates(
                api_base="https://api.quantclass.cn/api/data",
                product="stock-fin-data-xbx",
                hid="hid",
                headers={"api-key": "k"},
                local_date="2026-02-06",
                latest_date="2026-02-08",
            )

        self.assertEqual(["2026-02-08"], result)

    def test_probe_downloadable_dates_skips_legacy_fatal_without_status(self) -> None:
        def fake_get_download_link(
            api_base: str,
            product: str,
            date_time: str,
            hid: str,
            headers: dict[str, str],
        ) -> str:
            if date_time == "2026-02-07":
                raise qcs.FatalRequestError("legacy-no-status")
            return f"https://example.com/{product}/{date_time}.zip"

        with patch("quantclass_sync.get_download_link", side_effect=fake_get_download_link):
            result = qcs._probe_downloadable_dates(
                api_base="https://api.quantclass.cn/api/data",
                product="stock-fin-data-xbx",
                hid="hid",
                headers={"api-key": "k"},
                local_date="2026-02-06",
                latest_date="2026-02-08",
            )

        self.assertEqual(["2026-02-08"], result)

    def test_probe_downloadable_dates_401_is_not_silently_skipped(self) -> None:
        def fake_get_download_link(
            api_base: str,
            product: str,
            date_time: str,
            hid: str,
            headers: dict[str, str],
        ) -> str:
            raise qcs.FatalRequestError(
                "unauthorized",
                status_code=401,
                request_url=f"{api_base}/get-download-link/{product}-daily/{date_time}",
            )

        with patch("quantclass_sync.get_download_link", side_effect=fake_get_download_link):
            with self.assertRaises(qcs.FatalRequestError) as cm:
                qcs._probe_downloadable_dates(
                    api_base="https://api.quantclass.cn/api/data",
                    product="stock-fin-data-xbx",
                    hid="hid",
                    headers={"api-key": "k"},
                    local_date="2026-02-06",
                    latest_date="2026-02-08",
                )

        self.assertEqual(401, cm.exception.status_code)

    def test_probe_downloadable_dates_500_is_not_silently_skipped(self) -> None:
        def fake_get_download_link(
            api_base: str,
            product: str,
            date_time: str,
            hid: str,
            headers: dict[str, str],
        ) -> str:
            raise qcs.FatalRequestError(
                "server error",
                status_code=500,
                request_url=f"{api_base}/get-download-link/{product}-daily/{date_time}",
            )

        with patch("quantclass_sync.get_download_link", side_effect=fake_get_download_link):
            with self.assertRaises(qcs.FatalRequestError) as cm:
                qcs._probe_downloadable_dates(
                    api_base="https://api.quantclass.cn/api/data",
                    product="stock-fin-data-xbx",
                    hid="hid",
                    headers={"api-key": "k"},
                    local_date="2026-02-06",
                    latest_date="2026-02-08",
                )

        self.assertEqual(500, cm.exception.status_code)

    def test_empty_download_link_raises_specific_exception(self) -> None:
        """get_download_link 返回空链接时抛出 EmptyDownloadLinkError。"""

        with patch("quantclass_sync_internal.http_client.request_data") as mock_req:
            mock_req.return_value = MagicMock(text="  ")
            with self.assertRaises(qcs.EmptyDownloadLinkError):
                qcs.get_download_link(
                    api_base="http://test",
                    product="test-product",
                    date_time="2026-01-01",
                    hid="fake",
                    headers={},
                )

    def test_request_data_latest_uses_short_policy(self) -> None:
        with patch("quantclass_sync.time.sleep"), patch(
            "quantclass_sync.requests.request",
            side_effect=requests.RequestException("boom"),
        ) as req_mock:
            with self.assertRaises(RuntimeError):
                qcs.request_data(
                    "GET",
                    url="https://api.quantclass.cn/api/data/fetch/stock-trading-data-daily/latest?uuid=hid",
                    headers={"api-key": "k"},
                    product="stock-trading-data",
                    request_profile="latest",
                )

        self.assertEqual(3, req_mock.call_count)
        for call in req_mock.call_args_list:
            self.assertEqual(15, call.kwargs["timeout"])
        attempts, failures = qcs._http_metrics_for_product("stock-trading-data")
        self.assertEqual(3, attempts)
        self.assertEqual(3, failures)

    def test_request_data_file_download_uses_robust_policy(self) -> None:
        with patch("quantclass_sync.time.sleep"), patch(
            "quantclass_sync.requests.request",
            side_effect=requests.RequestException("boom"),
        ) as req_mock:
            with self.assertRaises(RuntimeError):
                qcs.request_data(
                    "GET",
                    url="https://example.com/file.zip",
                    headers={"api-key": "k"},
                    product="stock-trading-data",
                    request_profile="file_download",
                    stream=True,
                )

        self.assertEqual(5, req_mock.call_count)
        for call in req_mock.call_args_list:
            self.assertEqual(60, call.kwargs["timeout"])
        attempts, failures = qcs._http_metrics_for_product("stock-trading-data")
        self.assertEqual(5, attempts)
        self.assertEqual(5, failures)

    def test_download_file_atomic_replaces_existing_cache_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            download_path = Path(tmpdir) / "stock-trading-data" / "2026-02-11" / "payload.zip"
            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path.write_bytes(b"bad-old-file")

            def fake_save_file(file_url: str, file_path: Path, headers: dict[str, str], product: str = "") -> None:
                file_path.write_bytes(b"new-good-file")

            with patch("quantclass_sync_internal.orchestrator.save_file", side_effect=fake_save_file):
                orchestrator._download_file_atomic(
                    file_url="https://example.com/file.zip",
                    download_path=download_path,
                    headers={"api-key": "k"},
                    product="stock-trading-data",
                )

            self.assertEqual(b"new-good-file", download_path.read_bytes())
            part_files = list(download_path.parent.glob("*.part-*"))
            self.assertEqual([], part_files)

    def test_download_file_atomic_cleans_temp_file_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            download_path = Path(tmpdir) / "stock-trading-data" / "2026-02-11" / "payload.zip"
            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path.write_bytes(b"stable-old-file")

            with patch(
                "quantclass_sync_internal.orchestrator.save_file",
                side_effect=RuntimeError("download failed"),
            ):
                with self.assertRaises(RuntimeError):
                    orchestrator._download_file_atomic(
                        file_url="https://example.com/file.zip",
                        download_path=download_path,
                        headers={"api-key": "k"},
                        product="stock-trading-data",
                    )

            self.assertEqual(b"stable-old-file", download_path.read_bytes())
            part_files = list(download_path.parent.glob("*.part-*"))
            self.assertEqual([], part_files)

    def test_process_product_merge_error_reason_raises_product_sync_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            download_path = root / "cache" / "coin-cap.zip"
            extract_path = root / "cache" / "extract"
            extract_path.mkdir(parents=True, exist_ok=True)

            plan = qcs.ProductPlan(name="coin-cap", strategy=qcs.STRATEGY_MERGE_KNOWN)

            with patch(
                "quantclass_sync_internal.orchestrator._resolve_actual_time",
                return_value="2026-02-28",
            ), patch(
                "quantclass_sync_internal.orchestrator._download_and_prepare_extract",
                return_value=(download_path, extract_path),
            ), patch(
                "quantclass_sync_internal.orchestrator._extract_product_archive",
                return_value=None,
            ), patch(
                "quantclass_sync_internal.orchestrator.sync_from_extract",
                return_value=(qcs.SyncStats(skipped_files=1), qcs.REASON_MERGE_ERROR),
            ):
                with self.assertRaises(qcs.ProductSyncError) as cm:
                    orchestrator.process_product(
                        plan=plan,
                        date_time=None,
                        api_base="https://api.quantclass.cn/api/data",
                        hid="hid",
                        headers={"api-key": "k"},
                        data_root=root / "data",
                        work_dir=root / ".cache",
                        dry_run=True,
                    )

            self.assertEqual(qcs.REASON_MERGE_ERROR, cm.exception.reason_code)

    def test_download_and_prepare_extract_maps_404_from_get_link_to_no_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "quantclass_sync_internal.orchestrator.get_download_link",
                side_effect=qcs.FatalRequestError("not found", status_code=404),
            ):
                with self.assertRaises(qcs.ProductSyncError) as cm:
                    orchestrator._download_and_prepare_extract(
                        product="stock-trading-data",
                        actual_time="2026-02-11",
                        api_base="https://api.quantclass.cn/api/data",
                        hid="hid",
                        headers={"api-key": "k"},
                        work_dir=Path(tmpdir),
                    )

        self.assertEqual(qcs.REASON_NO_DATA_FOR_DATE, cm.exception.reason_code)

    def test_download_and_prepare_extract_maps_404_from_file_download_to_no_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "quantclass_sync_internal.orchestrator.get_download_link",
                return_value="https://example.com/payload.zip",
            ), patch(
                "quantclass_sync_internal.orchestrator._download_file_atomic",
                side_effect=qcs.FatalRequestError("not found", status_code=404),
            ):
                with self.assertRaises(qcs.ProductSyncError) as cm:
                    orchestrator._download_and_prepare_extract(
                        product="stock-trading-data",
                        actual_time="2026-02-11",
                        api_base="https://api.quantclass.cn/api/data",
                        hid="hid",
                        headers={"api-key": "k"},
                        work_dir=Path(tmpdir),
                    )

        self.assertEqual(qcs.REASON_NO_DATA_FOR_DATE, cm.exception.reason_code)


if __name__ == "__main__":
    unittest.main()
