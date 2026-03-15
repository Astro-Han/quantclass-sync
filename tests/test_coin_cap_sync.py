import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.config import build_product_plan
from quantclass_sync_internal.constants import REASON_MERGE_ERROR, REASON_OK, STRATEGY_MERGE_KNOWN
from quantclass_sync_internal.csv_engine import read_csv_payload
from quantclass_sync_internal.file_sync import sync_known_product


COIN_CAP_NOTE = "数据由邢不行整理，对数据字段有疑问的，可以直接微信私信邢不行，微信号：xbx297,,,,,,,,,,,"
COIN_CAP_HEADER = [
    "candle_begin_time",
    "symbol",
    "id",
    "name",
    "date_added",
    "max_supply",
    "circulating_supply",
    "total_supply",
    "usd_price",
    "max_mcap",
    "circulating_mcap",
    "total_mcap",
]


def _coin_cap_row(day: str, symbol: str, rank_id: int, name: str, usd_price: str) -> list[str]:
    return [
        day,
        symbol,
        str(rank_id),
        name,
        "2020-01-01 00:00:00+00:00",
        "1000000",
        "900000",
        "1000000",
        usd_price,
        "10000000",
        "9000000",
        "10000000",
    ]


def _write_coin_cap_csv(path: Path, rows: list[list[str]], header: list[str] | None = None) -> None:
    actual_header = header or COIN_CAP_HEADER
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [COIN_CAP_NOTE, ",".join(actual_header)]
    lines.extend(",".join(row) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="gb18030", newline="")


class CoinCapSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_coin_cap_is_known_merge_rule(self) -> None:
        plans = build_product_plan(["coin-cap"])
        self.assertEqual(1, len(plans))
        self.assertEqual(STRATEGY_MERGE_KNOWN, plans[0].strategy)

    def test_coin_cap_daily_filter_keeps_same_day_and_splits_by_symbol(self) -> None:
        src = self.root / "extract" / "coin-cap-daily" / "2026-02-28.csv"
        _write_coin_cap_csv(
            src,
            rows=[
                _coin_cap_row("2026-02-28", "BTC-USDT", 1, "Bitcoin", "10"),
                _coin_cap_row("2026-02-27", "BTC-USDT", 1, "Bitcoin", "9"),
                _coin_cap_row("2026-02-28 00:00:00", "ETH-USDT", 2, "Ethereum", "20"),
                _coin_cap_row("2026-02-28", "BTC-USDT", 1, "Bitcoin", "11"),
            ],
        )

        stats, reason_code = sync_known_product(
            product="coin-cap",
            extract_path=self.root / "extract",
            data_root=self.root / "data",
            dry_run=False,
        )

        self.assertEqual(REASON_OK, reason_code)
        self.assertEqual(2, stats.created_files)
        self.assertEqual(0, stats.updated_files)

        btc_payload = read_csv_payload(
            self.root / "data" / "coin-cap" / "BTC-USDT.csv",
            preferred_encoding="gb18030",
        )
        self.assertEqual(1, len(btc_payload.rows))
        self.assertEqual("2026-02-28", btc_payload.rows[0][0])
        self.assertEqual("11", btc_payload.rows[0][8])

        eth_payload = read_csv_payload(
            self.root / "data" / "coin-cap" / "ETH-USDT.csv",
            preferred_encoding="gb18030",
        )
        self.assertEqual(1, len(eth_payload.rows))
        self.assertEqual("2026-02-28", eth_payload.rows[0][0])

    def test_coin_cap_missing_date_filter_col_skips_daily_file(self) -> None:
        src = self.root / "extract" / "coin-cap-daily" / "2026-02-28.csv"
        bad_header = [x for x in COIN_CAP_HEADER if x != "candle_begin_time"]
        _write_coin_cap_csv(
            src,
            header=bad_header,
            rows=[
                [
                    "BTC-USDT",
                    "1",
                    "Bitcoin",
                    "2020-01-01 00:00:00+00:00",
                    "1000000",
                    "900000",
                    "1000000",
                    "10",
                    "10000000",
                    "9000000",
                    "10000000",
                ]
            ],
        )

        stats, reason_code = sync_known_product(
            product="coin-cap",
            extract_path=self.root / "extract",
            data_root=self.root / "data",
            dry_run=False,
        )

        self.assertEqual(REASON_MERGE_ERROR, reason_code)
        self.assertEqual(1, stats.skipped_files)
        self.assertFalse((self.root / "data" / "coin-cap").exists())

    def test_coin_cap_all_rows_filtered_marks_merge_error(self) -> None:
        src = self.root / "extract" / "coin-cap-daily" / "2026-03-01.csv"
        _write_coin_cap_csv(
            src,
            rows=[
                _coin_cap_row("2026-02-28", "BTC-USDT", 1, "Bitcoin", "10"),
                _coin_cap_row("2026-02-27", "ETH-USDT", 2, "Ethereum", "20"),
            ],
        )

        stats, reason_code = sync_known_product(
            product="coin-cap",
            extract_path=self.root / "extract",
            data_root=self.root / "data",
            dry_run=False,
        )

        self.assertEqual(REASON_MERGE_ERROR, reason_code)
        self.assertEqual(1, stats.skipped_files)
        self.assertFalse((self.root / "data" / "coin-cap").exists())

    def test_coin_cap_same_key_updates_existing_symbol_file(self) -> None:
        existing = self.root / "data" / "coin-cap" / "BTC-USDT.csv"
        _write_coin_cap_csv(
            existing,
            rows=[_coin_cap_row("2026-02-28", "BTC-USDT", 1, "Bitcoin", "10")],
        )
        src = self.root / "extract" / "coin-cap-daily" / "2026-02-28.csv"
        _write_coin_cap_csv(
            src,
            rows=[_coin_cap_row("2026-02-28", "BTC-USDT", 1, "Bitcoin", "12")],
        )

        stats, reason_code = sync_known_product(
            product="coin-cap",
            extract_path=self.root / "extract",
            data_root=self.root / "data",
            dry_run=False,
        )

        self.assertEqual(REASON_OK, reason_code)
        self.assertEqual(0, stats.created_files)
        self.assertEqual(1, stats.updated_files)

        payload = read_csv_payload(existing, preferred_encoding="gb18030")
        self.assertEqual(1, len(payload.rows))
        self.assertEqual("12", payload.rows[0][8])

    def test_coin_cap_compact_filename_date_is_supported(self) -> None:
        src = self.root / "extract" / "coin-cap-daily" / "20260228.csv"
        _write_coin_cap_csv(
            src,
            rows=[
                _coin_cap_row("20260228", "BTC-USDT", 1, "Bitcoin", "10"),
                _coin_cap_row("2026-02-27", "BTC-USDT", 1, "Bitcoin", "9"),
                _coin_cap_row("not-a-date", "BTC-USDT", 1, "Bitcoin", "8"),
            ],
        )

        stats, reason_code = sync_known_product(
            product="coin-cap",
            extract_path=self.root / "extract",
            data_root=self.root / "data",
            dry_run=False,
        )

        self.assertEqual(REASON_OK, reason_code)
        self.assertEqual(1, stats.created_files)
        payload = read_csv_payload(
            self.root / "data" / "coin-cap" / "BTC-USDT.csv",
            preferred_encoding="gb18030",
        )
        self.assertEqual(1, len(payload.rows))
        self.assertEqual("2026-02-28", payload.rows[0][0])


if __name__ == "__main__":
    unittest.main()
