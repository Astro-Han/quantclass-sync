import tempfile
import unittest
from pathlib import Path

import pandas as pd

from quantclass_sync_internal.archive import extract_archive
from quantclass_sync_internal.config import build_product_plan, discover_local_products
from quantclass_sync_internal.constants import REASON_OK, STRATEGY_MERGE_KNOWN
from quantclass_sync_internal.csv_engine import read_csv_payload
from quantclass_sync_internal.file_sync import sync_known_product


NEW_PRODUCTS = [
    "coin-okx-candle-csv-1h",
    "coin-hl-spot-candle-csv-1h",
    "coin-hl-swap-candle-csv-1h",
    "stock-1h-trading-data-pro",
    "stock-etf-1h-trading-data",
    "stock-etf-5m-close-price",
]


def _hour_frame(code_col: str, rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            code_col: [r[0] for r in rows],
            "k线结束时间": pd.to_datetime([r[1] for r in rows]),
            "收盘价": [r[2] for r in rows],
        }
    )


class ParquetAndNewProductTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data = self.root / "data"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _sync(self, product: str, name: str, writer) -> tuple:
        extract = self.root / f"extract-{name}"
        src = extract / name
        src.parent.mkdir(parents=True, exist_ok=True)
        writer(src)
        return sync_known_product(product=product, extract_path=extract, data_root=self.data, dry_run=False)

    def test_new_products_are_known_merge_rules(self) -> None:
        plans = build_product_plan(NEW_PRODUCTS)
        self.assertEqual({STRATEGY_MERGE_KNOWN}, {p.strategy for p in plans})

    def test_coin_daily_csv_splits_by_symbol_and_appends(self) -> None:
        header = "candle_begin_time,symbol,open,close\n"

        def day1(path: Path) -> None:
            path.write_text(
                header + "2026-10-27 00:00:00,BTC-USDT,1,2\n2026-10-27 00:00:00,@1154,3,4\n", encoding="utf-8"
            )

        def day2(path: Path) -> None:
            path.write_text(header + "2026-10-28 00:00:00,BTC-USDT,2,3\n", encoding="utf-8")

        stats, reason = self._sync("coin-hl-spot-candle-csv-1h", "2026-10-27.csv", day1)
        self.assertEqual(REASON_OK, reason)
        self.assertEqual(2, stats.created_files)

        stats, reason = self._sync("coin-hl-spot-candle-csv-1h", "2026-10-28.csv", day2)
        self.assertEqual(REASON_OK, reason)
        self.assertEqual(1, stats.updated_files)

        payload = read_csv_payload(self.data / "coin-hl-spot-candle-csv-1h" / "BTC-USDT.csv")
        self.assertIsNone(payload.note)
        self.assertEqual(["2026-10-27 00:00:00", "2026-10-28 00:00:00"], [r[0] for r in payload.rows])
        self.assertTrue((self.data / "coin-hl-spot-candle-csv-1h" / "@1154.csv").exists())

    def test_hour_parquet_splits_to_per_code_parquet(self) -> None:
        def day(path: Path) -> None:
            _hour_frame(
                "股票代码",
                [("sh600000", "2026-10-28 10:30", 1.0), ("sz000001", "2026-10-28 10:30", 2.0)],
            ).to_parquet(path, index=False)

        stats, reason = self._sync("stock-1h-trading-data-pro", "2026-10-28.parquet", day)
        self.assertEqual(REASON_OK, reason)
        self.assertEqual(2, stats.created_files)
        out = pd.read_parquet(self.data / "stock-1h-trading-data-pro" / "sh600000.parquet")
        self.assertEqual(["sh600000"], out["股票代码"].tolist())

        stats, reason = self._sync("stock-1h-trading-data-pro", "2026-10-28.parquet", day)
        self.assertEqual(REASON_OK, reason)
        self.assertEqual(2, stats.unchanged_files)

    def test_hour_parquet_rolling_window_overrides_and_sorts(self) -> None:
        def day1(path: Path) -> None:
            _hour_frame(
                "基金代码",
                [("sh510300", "2026-10-26 10:30", 1.0), ("sh510300", "2026-10-27 10:30", 1.5)],
            ).to_parquet(path, index=False)

        def day2(path: Path) -> None:
            _hour_frame(
                "基金代码",
                [("sh510300", "2026-10-28 10:30", 3.0), ("sh510300", "2026-10-27 10:30", 2.0)],
            ).to_parquet(path, index=False)

        self._sync("stock-etf-1h-trading-data", "2026-10-27.parquet", day1)
        stats, reason = self._sync("stock-etf-1h-trading-data", "2026-10-28.parquet", day2)

        self.assertEqual(REASON_OK, reason)
        self.assertEqual(1, stats.updated_files)
        self.assertEqual(1, stats.rows_added)
        out = pd.read_parquet(self.data / "stock-etf-1h-trading-data" / "sh510300.parquet")
        self.assertEqual([1.0, 2.0, 3.0], out["收盘价"].tolist())

    def test_etf_5m_parquet_writes_framework_readable_csv(self) -> None:
        def day(path: Path) -> None:
            pd.DataFrame(
                {"交易日期": ["2026-10-28"], "基金代码": ["sh510300"], "0930": [4.5], "0935": [float("nan")]}
            ).astype({"0930": "float32", "0935": "float32"}).to_parquet(path, index=False)

        stats, reason = self._sync("stock-etf-5m-close-price", "2026-10-28.parquet", day)
        self.assertEqual(REASON_OK, reason)
        self.assertEqual(1, stats.created_files)

        target = self.data / "stock-etf-5m-close-price" / "sh510300.csv"
        df = pd.read_csv(target, encoding="gbk", skiprows=1, usecols=["交易日期", "股票代码", "0930", "0935"])
        self.assertEqual("sh510300", df.loc[0, "股票代码"])
        self.assertEqual(4.5, df.loc[0, "0930"])
        self.assertTrue(pd.isna(df.loc[0, "0935"]))

    def test_extract_archive_copies_parquet(self) -> None:
        src = self.root / "2026-10-28.parquet"
        _hour_frame("股票代码", [("sh600000", "2026-10-28 10:30", 1.0)]).to_parquet(src, index=False)
        extract_archive(src, self.root / "extract")
        self.assertTrue((self.root / "extract" / "2026-10-28.parquet").exists())

    def test_parquet_only_dir_is_discovered_as_local_product(self) -> None:
        product_dir = self.data / "stock-1h-trading-data-pro"
        product_dir.mkdir(parents=True)
        _hour_frame("股票代码", [("sh600000", "2026-10-28 10:30", 1.0)]).to_parquet(
            product_dir / "sh600000.parquet", index=False
        )
        discovered = discover_local_products(self.data, ["stock-1h-trading-data-pro"])
        self.assertEqual([("stock-1h-trading-data-pro", True)], [(d.name, d.valid) for d in discovered])


if __name__ == "__main__":
    unittest.main()
