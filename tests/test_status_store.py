import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from quantclass_sync_internal.constants import LEGACY_STATUS_DB_REL, META_STATUS_DB_REL, TIMESTAMP_FILE_NAME
from quantclass_sync_internal.models import ProductStatus
from quantclass_sync_internal.status_store import (
    ensure_status_table,
    export_status_json,
    open_status_db,
    read_local_timestamp_date,
    resolve_runtime_paths,
    upsert_product_status,
    write_local_timestamp,
)


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class StatusStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_path = self.root / "status.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        ensure_status_table(conn)
        return conn

    def _write_valid_status_db(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        try:
            ensure_status_table(conn)
            upsert_product_status(conn, ProductStatus(name="demo", display_name="demo"))
        finally:
            conn.close()

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

    def test_export_status_json_replace_failure_keeps_original_file_and_cleans_temp(self) -> None:
        conn = self._new_conn()
        output_path = self.root / "products-status.json"
        output_path.write_text('{"old":"value"}', encoding="utf-8")
        try:
            upsert_product_status(conn, ProductStatus(name="demo", display_name="demo"))
            before = output_path.read_text(encoding="utf-8")
            # os.replace 现在在 config.atomic_temp_path 中调用，需 patch config 模块
            with patch("quantclass_sync_internal.config.os.replace", side_effect=RuntimeError("replace failed")):
                with self.assertRaises(RuntimeError):
                    export_status_json(conn, output_path)
        finally:
            conn.close()

        self.assertEqual(before, output_path.read_text(encoding="utf-8"))
        self.assertEqual([], list(output_path.parent.glob(f".{output_path.name}.tmp-*")))

    def test_open_status_db_closes_connection_on_success(self) -> None:
        fake_conn = _FakeConn()
        with patch("quantclass_sync_internal.status_store.connect_status_db", return_value=fake_conn):
            with open_status_db(self.root) as conn:
                self.assertIs(fake_conn, conn)
        self.assertTrue(fake_conn.closed)

    def test_open_status_db_closes_connection_on_exception(self) -> None:
        fake_conn = _FakeConn()
        with patch("quantclass_sync_internal.status_store.connect_status_db", return_value=fake_conn):
            with self.assertRaises(RuntimeError):
                with open_status_db(self.root):
                    raise RuntimeError("boom")
        self.assertTrue(fake_conn.closed)

    def test_resolve_runtime_paths_prefers_legacy_when_metadata_is_empty_shell(self) -> None:
        data_root = self.root / "runtime"
        metadata_db = data_root / META_STATUS_DB_REL
        metadata_db.parent.mkdir(parents=True, exist_ok=True)
        metadata_db.write_bytes(b"")

        legacy_db = data_root / LEGACY_STATUS_DB_REL
        self._write_valid_status_db(legacy_db)

        runtime_paths = resolve_runtime_paths(data_root)
        self.assertEqual("legacy", runtime_paths.source)
        self.assertEqual(legacy_db.resolve(), runtime_paths.status_db.resolve())

    def test_write_local_timestamp_rejects_invalid_date(self) -> None:
        with self.assertRaises(ValueError):
            write_local_timestamp(self.root, "demo", "invalid-date")
        self.assertFalse((self.root / "demo" / TIMESTAMP_FILE_NAME).exists())


if __name__ == "__main__":
    unittest.main()
