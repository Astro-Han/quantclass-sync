import io
import os
import shutil
import stat
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import quantclass_sync_internal.archive as archive_module
from quantclass_sync_internal.archive import safe_extract_7z, safe_extract_rar, safe_extract_tar, safe_extract_zip
from quantclass_sync_internal.csv_engine import read_csv_payload
from quantclass_sync_internal.file_sync import sync_raw_file
from quantclass_sync_internal.status_store import cleanup_work_cache_aggressive, normalize_data_date


class SafeExtractTarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _build_tar_with_special_member(self, tar_path: Path, type_flag: bytes) -> None:
        with tarfile.open(tar_path, "w") as tf:
            regular = tarfile.TarInfo("ok.txt")
            regular_payload = b"ok"
            regular.size = len(regular_payload)
            tf.addfile(regular, io.BytesIO(regular_payload))

            bad = tarfile.TarInfo("bad-node")
            bad.type = type_flag
            bad.size = 0
            if type_flag in {tarfile.CHRTYPE, tarfile.BLKTYPE}:
                bad.devmajor = 1
                bad.devminor = 3
            tf.addfile(bad)

    def test_safe_extract_tar_rejects_special_member_types(self) -> None:
        for type_name, type_flag in {
            "fifo": tarfile.FIFOTYPE,
            "char": tarfile.CHRTYPE,
            "block": tarfile.BLKTYPE,
        }.items():
            with self.subTest(type_name=type_name):
                tar_path = self.root / f"{type_name}.tar"
                extract_dir = self.root / f"extract-{type_name}"
                extract_dir.mkdir(parents=True, exist_ok=True)
                self._build_tar_with_special_member(tar_path, type_flag)

                with self.assertRaises(RuntimeError):
                    safe_extract_tar(tar_path, extract_dir)

                self.assertFalse((extract_dir / "ok.txt").exists())

    def test_safe_extract_tar_rejects_fifo_when_isdev_returns_false(self) -> None:
        tar_path = self.root / "fifo-only.tar"
        extract_dir = self.root / "extract-fifo-only"
        extract_dir.mkdir(parents=True, exist_ok=True)
        self._build_tar_with_special_member(tar_path, tarfile.FIFOTYPE)

        with patch("quantclass_sync_internal.archive.tarfile.TarInfo.isdev", return_value=False):
            with self.assertRaises(RuntimeError):
                safe_extract_tar(tar_path, extract_dir)

        self.assertFalse((extract_dir / "ok.txt").exists())


class SafeExtractArchiveDangerousNodesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ensure_symlink_supported(self) -> None:
        probe_target = self.root / "symlink-probe-target.txt"
        probe_target.write_text("probe", encoding="utf-8")
        probe_link = self.root / "symlink-probe-link"
        try:
            probe_link.symlink_to(probe_target)
        except (NotImplementedError, OSError):
            self.skipTest("current platform does not support symlink in this environment")
        finally:
            if probe_link.exists() or probe_link.is_symlink():
                probe_link.unlink()

    def test_safe_extract_zip_rejects_symlink_member_by_external_attr(self) -> None:
        zip_path = self.root / "zip-symlink-entry.zip"
        extract_dir = self.root / "zip-symlink-entry-extract"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("ok.txt", "ok")
            symlink_member = zipfile.ZipInfo("bad-link")
            symlink_member.create_system = 3
            symlink_member.external_attr = (stat.S_IFLNK | 0o777) << 16
            zf.writestr(symlink_member, "../outside")

        with self.assertRaises(RuntimeError):
            safe_extract_zip(zip_path, extract_dir)

        self.assertFalse((extract_dir / "ok.txt").exists())

    def test_safe_extract_zip_post_scan_rejects_symlink(self) -> None:
        self._ensure_symlink_supported()
        zip_path = self.root / "zip-post-scan.zip"
        extract_dir = self.root / "zip-post-scan-extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        outside = self.root / "outside-zip.txt"
        outside.write_text("outside", encoding="utf-8")

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("ok.txt", "ok")

        original_extractall = archive_module.zipfile.ZipFile.extractall

        def extractall_with_symlink(zip_obj, path, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            original_extractall(zip_obj, path, *args, **kwargs)
            bad_link = Path(path) / "bad-zip-link"
            bad_link.symlink_to(outside)

        with patch("quantclass_sync_internal.archive.zipfile.ZipFile.extractall", new=extractall_with_symlink):
            with self.assertRaises(RuntimeError):
                safe_extract_zip(zip_path, extract_dir)

    def test_safe_extract_rar_post_scan_rejects_symlink(self) -> None:
        self._ensure_symlink_supported()
        extract_dir = self.root / "rar-post-scan-extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        outside = self.root / "outside-rar.txt"
        outside.write_text("outside", encoding="utf-8")

        class FakeRarInfo:
            def __init__(self, filename: str) -> None:
                self.filename = filename

        class FakeRarFile:
            def __init__(self, _path: Path) -> None:
                self._members = [FakeRarInfo("ok.txt")]

            def __enter__(self) -> "FakeRarFile":
                return self

            def __exit__(self, _exc_type, _exc, _tb) -> bool:
                return False

            def infolist(self) -> list[FakeRarInfo]:
                return self._members

            def extract(self, member: FakeRarInfo, path: Path) -> None:
                extracted = Path(path) / member.filename
                extracted.parent.mkdir(parents=True, exist_ok=True)
                extracted.write_text("ok", encoding="utf-8")
                bad_link = Path(path) / "bad-rar-link"
                if not bad_link.exists():
                    bad_link.symlink_to(outside)

        with patch.object(archive_module, "rarfile", SimpleNamespace(RarFile=FakeRarFile)):
            with self.assertRaises(RuntimeError):
                safe_extract_rar(self.root / "fake.rar", extract_dir)

    def test_safe_extract_7z_post_scan_rejects_symlink(self) -> None:
        self._ensure_symlink_supported()
        extract_dir = self.root / "sevenzip-post-scan-extract"
        extract_dir.mkdir(parents=True, exist_ok=True)
        outside = self.root / "outside-7z.txt"
        outside.write_text("outside", encoding="utf-8")

        class FakeSevenZipFile:
            def __init__(self, _path: Path, _mode: str) -> None:
                pass

            def __enter__(self) -> "FakeSevenZipFile":
                return self

            def __exit__(self, _exc_type, _exc, _tb) -> bool:
                return False

            def getnames(self) -> list[str]:
                return ["ok.txt"]

            def extractall(self, path: Path) -> None:
                extracted = Path(path) / "ok.txt"
                extracted.parent.mkdir(parents=True, exist_ok=True)
                extracted.write_text("ok", encoding="utf-8")
                bad_link = Path(path) / "bad-7z-link"
                bad_link.symlink_to(outside)

        with patch.object(archive_module, "py7zr", SimpleNamespace(SevenZipFile=FakeSevenZipFile)):
            with self.assertRaises(RuntimeError):
                safe_extract_7z(self.root / "fake.7z", extract_dir)

    def test_scan_extracted_dangerous_nodes_rejects_block_char_and_fifo(self) -> None:
        extract_dir = self.root / "scan-extract"
        extract_dir.mkdir(parents=True, exist_ok=True)

        for node_name, mode, kind in [
            ("bad-block", stat.S_IFBLK | 0o600, "block"),
            ("bad-char", stat.S_IFCHR | 0o600, "char"),
            ("bad-fifo", stat.S_IFIFO | 0o600, "fifo"),
        ]:
            with self.subTest(kind=kind):
                fake_node = extract_dir / node_name
                fake_node.write_text("placeholder", encoding="utf-8")
                with patch("pathlib.Path.rglob", return_value=[fake_node]), patch(
                    "pathlib.Path.lstat",
                    return_value=SimpleNamespace(st_mode=mode),
                ):
                    with self.assertRaises(RuntimeError) as cm:
                        archive_module._scan_extracted_dangerous_nodes(extract_dir)
                self.assertIn(kind, str(cm.exception))


class ReadCsvPayloadMultilineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_read_csv_payload_supports_quoted_newline(self) -> None:
        path = self.root / "multiline.csv"
        path.write_text('c1,c2\n"hello\nworld",42\n', encoding="utf-8", newline="")

        payload = read_csv_payload(path)

        self.assertEqual(["c1", "c2"], payload.header)
        self.assertEqual(1, len(payload.rows))
        self.assertEqual("hello\nworld", payload.rows[0][0])
        self.assertEqual("42", payload.rows[0][1])

    def test_read_csv_payload_with_note_keeps_multiline_field_integrity(self) -> None:
        path = self.root / "multiline-with-note.csv"
        path.write_text('说明备注,,,,\nc1,c2\n"line1\nline2",v\n', encoding="utf-8", newline="")

        payload = read_csv_payload(path)

        self.assertEqual("说明备注,,,,", payload.note)
        self.assertEqual(["c1", "c2"], payload.header)
        self.assertEqual(1, len(payload.rows))
        self.assertEqual("line1\nline2", payload.rows[0][0])


class NormalizeDataDateTests(unittest.TestCase):
    def test_normalize_data_date_validates_calendar_date(self) -> None:
        self.assertIsNone(normalize_data_date("2026-02-30"))
        self.assertIsNone(normalize_data_date("20260230"))
        self.assertEqual("2024-02-29", normalize_data_date("2024-02-29"))
        self.assertEqual("2024-02-29", normalize_data_date("2024-02-29T09:00:00Z"))


class CleanupWorkCacheAggressiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_cleanup_work_cache_aggressive_unlinks_directory_symlink_only(self) -> None:
        work_dir = self.root / "work"
        outside_dir = self.root / "outside"
        work_dir.mkdir(parents=True, exist_ok=True)
        outside_dir.mkdir(parents=True, exist_ok=True)
        outside_file = outside_dir / "keep.txt"
        outside_file.write_text("do-not-touch", encoding="utf-8")

        symlink_path = work_dir / "outside-link"
        try:
            symlink_path.symlink_to(outside_dir, target_is_directory=True)
        except (NotImplementedError, OSError):
            self.skipTest("current platform does not support directory symlink in this environment")

        nested_dir = work_dir / "nested"
        nested_dir.mkdir(parents=True, exist_ok=True)
        (nested_dir / "temp.txt").write_text("temp", encoding="utf-8")
        (work_dir / "temp.txt").write_text("temp", encoding="utf-8")

        cleanup_work_cache_aggressive(work_dir)

        self.assertTrue(work_dir.exists())
        self.assertEqual([], list(work_dir.iterdir()))
        self.assertTrue(outside_file.exists())
        self.assertEqual("do-not-touch", outside_file.read_text(encoding="utf-8"))


class SyncRawFileChunkCompareTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_sync_raw_file_unchanged_does_not_use_read_bytes(self) -> None:
        src = self.root / "src.bin"
        target = self.root / "target.bin"
        payload = b"A" * (2 * 1024 * 1024 + 17)
        src.write_bytes(payload)
        target.write_bytes(payload)

        with patch("pathlib.Path.read_bytes", side_effect=AssertionError("read_bytes should not be called")):
            result = sync_raw_file(src=src, target=target, dry_run=False)

        self.assertEqual("unchanged", result)

    def test_sync_raw_file_dry_run_preserves_existing_target(self) -> None:
        src = self.root / "src.txt"
        target = self.root / "target.txt"
        src.write_text("new", encoding="utf-8")
        target.write_text("old", encoding="utf-8")

        result = sync_raw_file(src=src, target=target, dry_run=True)

        self.assertEqual("updated", result)
        self.assertEqual("old", target.read_text(encoding="utf-8"))

    def test_sync_raw_file_dry_run_for_new_target_returns_created_without_writing(self) -> None:
        src = self.root / "src-new.txt"
        target = self.root / "target-new.txt"
        src.write_text("new", encoding="utf-8")

        result = sync_raw_file(src=src, target=target, dry_run=True)

        self.assertEqual("created", result)
        self.assertFalse(target.exists())

    def test_sync_raw_file_writes_via_temp_then_replace(self) -> None:
        src = self.root / "src-atomic.txt"
        target = self.root / "target-atomic.txt"
        src.write_text("new", encoding="utf-8")

        with patch("quantclass_sync_internal.file_sync.shutil.copy2", wraps=shutil.copy2) as copy_mock, patch(
            "quantclass_sync_internal.file_sync.os.replace",
            wraps=os.replace,
        ) as replace_mock:
            result = sync_raw_file(src=src, target=target, dry_run=False)

        self.assertEqual("created", result)
        copy_dst = Path(copy_mock.call_args.args[1])
        self.assertNotEqual(target, copy_dst)
        self.assertTrue(copy_dst.name.startswith(f".{target.name}.tmp-raw-"))
        replace_mock.assert_called_once_with(copy_dst, target)
        self.assertEqual("new", target.read_text(encoding="utf-8"))

    def test_sync_raw_file_replace_failure_preserves_target_and_cleans_temp(self) -> None:
        src = self.root / "src-replace-fail.txt"
        target = self.root / "target-replace-fail.txt"
        src.write_text("new", encoding="utf-8")
        target.write_text("old", encoding="utf-8")

        with patch("quantclass_sync_internal.file_sync.os.replace", side_effect=RuntimeError("replace failed")):
            with self.assertRaises(RuntimeError):
                sync_raw_file(src=src, target=target, dry_run=False)

        self.assertEqual("old", target.read_text(encoding="utf-8"))
        self.assertEqual([], list(target.parent.glob(f".{target.name}.tmp-raw-*")))


if __name__ == "__main__":
    unittest.main()
