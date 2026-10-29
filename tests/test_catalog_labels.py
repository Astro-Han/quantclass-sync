import re
import tempfile
import unittest
from pathlib import Path

from quantclass_sync_internal.config import load_catalog_labels, load_products_from_catalog
from quantclass_sync_internal.constants import DEFAULT_CATALOG_FILE, INFRA_PRODUCTS

APP_JS = DEFAULT_CATALOG_FILE.parent / "quantclass_sync_internal" / "gui" / "assets" / "app.js"


class CatalogLabelTests(unittest.TestCase):
    def test_labeled_and_plain_lines_both_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "catalog.txt"
            path.write_text(
                "# 注释 | 不是产品\nstock-trading-data | 股票历史日线数据 | A股行情\ncoin-cap\n",
                encoding="utf-8",
            )
            self.assertEqual(["stock-trading-data", "coin-cap"], load_products_from_catalog(path))
            labels = load_catalog_labels(path)

        self.assertEqual(("股票历史日线数据", "A股行情"), labels["stock-trading-data"])
        self.assertEqual(("", ""), labels["coin-cap"])

    def test_shipped_catalog_entries_have_known_labels(self) -> None:
        known = set(re.findall(r"\{ name: '([^']+)'", APP_JS.read_text(encoding="utf-8")))
        labels = load_catalog_labels(DEFAULT_CATALOG_FILE)

        for product in load_products_from_catalog(DEFAULT_CATALOG_FILE):
            if product in INFRA_PRODUCTS:
                continue
            label, category = labels[product]
            self.assertTrue(label, product)
            self.assertIn(category, known, product)


if __name__ == "__main__":
    unittest.main()
