import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import quantclass_sync as qcs
from quantclass_sync_internal.csv_engine import sortable_value


class EncodingStrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.rule = qcs.RULES["stock-main-index-data"]
        self.header = ["candle_end_time", "open", "high", "low", "close", "amount", "volume", "index_code"]
        self.rows = [["2024-01-01", "1", "1", "1", "1", "10", "10", "sh000300"]]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_csv_text(self, path: Path, encoding: str, with_bom: bool = False) -> None:
        line = ",".join(self.header) + "\n" + ",".join(self.rows[0]) + "\n"
        if with_bom:
            path.write_bytes(qcs.UTF8_BOM + line.encode("utf-8"))
            return
        path.write_text(line, encoding=encoding, newline="")

    def _payload(self, encoding: str) -> qcs.CsvPayload:
        return qcs.CsvPayload(
            note=None,
            header=list(self.header),
            rows=[list(self.rows[0])],
            encoding=encoding,
            delimiter=",",
        )

    def test_decode_text_utf8_without_bom_keeps_utf8(self) -> None:
        path = self.root / "utf8_no_bom.csv"
        path.write_text("candle_end_time,open\n2024-01-01,1\n", encoding="utf-8", newline="")

        text, encoding = qcs.decode_text(path, preferred_encoding="utf-8-sig")

        self.assertEqual("utf-8", encoding)
        self.assertTrue(text.startswith("candle_end_time,open"))

    def test_decode_text_bom_file_overrides_wrong_preferred_encoding(self) -> None:
        path = self.root / "utf8_bom.csv"
        path.write_bytes(qcs.UTF8_BOM + b"candle_end_time,open\n2024-01-01,1\n")

        text, encoding = qcs.decode_text(path, preferred_encoding="gb18030")

        self.assertEqual("utf-8-sig", encoding)
        self.assertTrue(text.startswith("candle_end_time,open"))

    def test_sync_keeps_existing_encoding_when_only_encoding_differs(self) -> None:
        target = self.root / "sh000300.csv"
        self._write_csv_text(target, encoding="gb18030", with_bom=False)

        result, added, _audit = qcs.sync_payload_to_target(
            incoming=self._payload(encoding="utf-8-sig"),
            target=target,
            rule=self.rule,
            dry_run=False,
        )

        self.assertEqual("unchanged", result)
        self.assertEqual(0, added)
        self.assertFalse(target.read_bytes().startswith(qcs.UTF8_BOM))

    def test_sync_returns_unchanged_when_content_and_encoding_match(self) -> None:
        target = self.root / "sh000300.csv"
        self._write_csv_text(target, encoding="utf-8-sig", with_bom=True)

        result, added, _audit = qcs.sync_payload_to_target(
            incoming=self._payload(encoding="utf-8-sig"),
            target=target,
            rule=self.rule,
            dry_run=False,
        )

        self.assertEqual("unchanged", result)
        self.assertEqual(0, added)

    def test_sync_keeps_existing_bom_when_policy_is_preserve_existing(self) -> None:
        target = self.root / "sh000300.csv"
        self._write_csv_text(target, encoding="utf-8-sig", with_bom=True)

        result, added, _audit = qcs.sync_payload_to_target(
            incoming=self._payload(encoding="utf-8"),
            target=target,
            rule=self.rule,
            dry_run=False,
        )

        self.assertEqual("unchanged", result)
        self.assertEqual(0, added)
        self.assertTrue(target.read_bytes().startswith(qcs.UTF8_BOM))

    def test_sync_created_file_uses_incoming_encoding(self) -> None:
        target = self.root / "new.csv"

        result, added, _audit = qcs.sync_payload_to_target(
            incoming=self._payload(encoding="utf-8-sig"),
            target=target,
            rule=self.rule,
            dry_run=False,
        )

        self.assertEqual("created", result)
        self.assertEqual(1, added)
        self.assertTrue(target.read_bytes().startswith(qcs.UTF8_BOM))

    def test_sync_aligns_rows_using_normalized_headers(self) -> None:
        target = self.root / "normalized-header.csv"
        target.write_text(
            " candle_end_time ,open,high,low,close,amount,volume,\ufeffindex_code\n"
            "2024-01-01,1,1,1,1,10,10,sh000300\n",
            encoding="utf-8",
            newline="",
        )
        incoming = qcs.CsvPayload(
            note=None,
            header=["candle_end_time", "open", "high", "low", "close", "amount", "volume", "index_code"],
            rows=[["2024-01-02", "2", "2", "2", "2", "20", "20", "sh000300"]],
            encoding="utf-8",
            delimiter=",",
        )

        result, added, _audit = qcs.sync_payload_to_target(
            incoming=incoming,
            target=target,
            rule=self.rule,
            dry_run=False,
        )

        self.assertEqual("updated", result)
        self.assertEqual(1, added)
        payload = qcs.read_csv_payload(target, preferred_encoding="utf-8")
        row_by_date = {row[0]: row for row in payload.rows}
        self.assertIn("2024-01-02", row_by_date)
        self.assertEqual("sh000300", row_by_date["2024-01-02"][-1])

    def test_sync_raises_when_normalized_header_has_duplicates(self) -> None:
        incoming = qcs.CsvPayload(
            note=None,
            header=["index_code", " index_code ", "candle_end_time"],
            rows=[["sh000300", "dup", "2024-01-01"]],
            encoding="utf-8",
            delimiter=",",
        )

        with self.assertRaises(RuntimeError):
            qcs.sync_payload_to_target(
                incoming=incoming,
                target=self.root / "dup-header.csv",
                rule=self.rule,
                dry_run=False,
            )

    def test_sync_csv_write_failure_keeps_original_file_and_cleans_temp(self) -> None:
        target = self.root / "atomic-failure.csv"
        self._write_csv_text(target, encoding="utf-8", with_bom=False)
        before = target.read_text(encoding="utf-8")
        incoming = qcs.CsvPayload(
            note=None,
            header=list(self.header),
            rows=[["2024-01-02", "2", "2", "2", "2", "20", "20", "sh000300"]],
            encoding="utf-8",
            delimiter=",",
        )

        with patch("quantclass_sync_internal.csv_engine.os.replace", side_effect=RuntimeError("replace failed")):
            with self.assertRaises(RuntimeError):
                qcs.sync_payload_to_target(
                    incoming=incoming,
                    target=target,
                    rule=self.rule,
                    dry_run=False,
                )

        self.assertEqual(before, target.read_text(encoding="utf-8"))
        self.assertEqual([], list(target.parent.glob(f".{target.name}.tmp-csv-*")))

    def test_sortable_value_treats_non_finite_numbers_as_text(self) -> None:
        self.assertEqual((2, "nan"), sortable_value("nan"))
        self.assertEqual((2, "inf"), sortable_value("inf"))
        self.assertEqual((2, "-inf"), sortable_value("-inf"))
        self.assertEqual((1, 123.0), sortable_value("123"))


if __name__ == "__main__":
    unittest.main()
