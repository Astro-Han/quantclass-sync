import unittest
from types import SimpleNamespace
from unittest.mock import patch

import quantclass_sync as qcs


class HttpErrorMappingTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
