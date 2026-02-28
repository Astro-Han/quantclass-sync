import tempfile
import unittest
from pathlib import Path

import quantclass_sync as qcs


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


if __name__ == "__main__":
    unittest.main()
