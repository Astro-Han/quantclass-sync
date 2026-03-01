import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from quantclass_sync_internal.constants import TIMESTAMP_FILE_NAME
from quantclass_sync_internal.models import ProductStatus
from quantclass_sync_internal.status_store import ensure_status_table, read_local_timestamp_date, upsert_product_status


class StatusStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_path = self.root / "status.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        ensure_status_table(conn)
        return conn

    def test_upsert_product_status_default_commits_immediately(self) -> None:
        conn = self._new_conn()
        try:
            upsert_product_status(conn, ProductStatus(name="p1", display_name="p1"))
            probe = sqlite3.connect(self.db_path)
            try:
                row = probe.execute("SELECT name FROM product_status WHERE name = 'p1'").fetchone()
            finally:
                probe.close()
        finally:
            conn.close()

        self.assertIsNotNone(row)

    def test_upsert_product_status_can_defer_commit(self) -> None:
        conn = self._new_conn()
        probe = sqlite3.connect(self.db_path)
        try:
            upsert_product_status(conn, ProductStatus(name="p2", display_name="p2"), commit_immediately=False)
            row_before = probe.execute("SELECT name FROM product_status WHERE name = 'p2'").fetchone()
            conn.commit()
            row_after = probe.execute("SELECT name FROM product_status WHERE name = 'p2'").fetchone()
        finally:
            probe.close()
            conn.close()

        self.assertIsNone(row_before)
        self.assertIsNotNone(row_after)

    def test_read_local_timestamp_date_returns_none_on_os_error(self) -> None:
        data_root = self.root / "data"
        ts_path = data_root / "demo" / TIMESTAMP_FILE_NAME
        ts_path.parent.mkdir(parents=True, exist_ok=True)
        ts_path.write_text("2025-01-01,2025-01-01 12:00:00\n", encoding="utf-8")

        with patch("pathlib.Path.read_text", side_effect=OSError("boom")):
            result = read_local_timestamp_date(data_root, "demo")

        self.assertIsNone(result)

    def test_read_local_timestamp_date_propagates_non_os_error(self) -> None:
        data_root = self.root / "data"
        ts_path = data_root / "demo" / TIMESTAMP_FILE_NAME
        ts_path.parent.mkdir(parents=True, exist_ok=True)
        ts_path.write_text("2025-01-01,2025-01-01 12:00:00\n", encoding="utf-8")

        with patch("pathlib.Path.read_text", side_effect=ValueError("boom")):
            with self.assertRaises(ValueError):
                read_local_timestamp_date(data_root, "demo")


if __name__ == "__main__":
    unittest.main()
