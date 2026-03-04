import io
import multiprocessing
import os
import shutil
import stat
import tarfile
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import quantclass_sync as qcs
import quantclass_sync_internal.archive as archive_module
from quantclass_sync_internal.archive import safe_extract_7z, safe_extract_rar, safe_extract_tar, safe_extract_zip
from quantclass_sync_internal.csv_engine import read_csv_payload
from quantclass_sync_internal.file_sync import sync_raw_file
from quantclass_sync_internal.status_store import cleanup_work_cache_aggressive, normalize_data_date

try:
    import fcntl
except Exception:  # pragma: no cover
    fcntl = None


def _sync_payload_lock_worker(target_path: str, started, finished) -> None:
    payload = qcs.CsvPayload(
        note=None,
        header=["candle_end_time", "open", "high", "low", "close", "amount", "volume", "index_code"],
        rows=[["2024-01-01", "1", "1", "1", "1", "10", "10", "sh000300"]],
        encoding="utf-8",
        delimiter=",",
    )
    started.set()
    qcs.sync_payload_to_target(
        incoming=payload,
        target=Path(target_path),
        rule=qcs.RULES["stock-main-index-data"],
        dry_run=False,
    )
    finished.set()


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

    def test_tar_path_traversal_is_rejected(self) -> None:
        """tar 成员名包含 ../ 路径遍历时应被拒绝提取。"""

        with tempfile.TemporaryDirectory() as tmp:
            tar_path = Path(tmp) / "evil.tar"
            extract_dir = Path(tmp) / "extract"
            extract_dir.mkdir()

            payload = b"malicious content"
            with tarfile.open(tar_path, "w") as tf:
                info = tarfile.TarInfo(name="../../etc/evil.txt")
                info.size = len(payload)
                tf.addfile(info, fileobj=io.BytesIO(payload))

            from quantclass_sync_internal.archive import extract_archive

            with self.assertRaisesRegex(RuntimeError, "解压路径越界"):
                extract_archive(tar_path, extract_dir)

            self.assertFalse((Path(tmp) / "etc" / "evil.txt").exists())
            self.assertFalse((extract_dir / ".." / ".." / "etc" / "evil.txt").exists())

    def test_safe_extract_tar_rejects_symlink_nodes_after_extract(self) -> None:
        tar_path = self.root / "with-symlink.tar"
        extract_dir = self.root / "extract-with-symlink"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, "w") as tf:
            payload = b"ok"
            regular = tarfile.TarInfo("ok.txt")
            regular.size = len(payload)
            tf.addfile(regular, io.BytesIO(payload))

            symlink = tarfile.TarInfo("link-to-ok")
            symlink.type = tarfile.SYMTYPE
            symlink.linkname = "ok.txt"
            tf.addfile(symlink)

        with self.assertRaises(RuntimeError):
            safe_extract_tar(tar_path, extract_dir)


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

    def test_cleanup_work_cache_aggressive_preserves_run_scoped_dirs_by_default(self) -> None:
        work_dir = self.root / "work"
        run_a = work_dir / "20260302-101010"
        run_b = work_dir / "20260302-101011"
        run_a.mkdir(parents=True, exist_ok=True)
        run_b.mkdir(parents=True, exist_ok=True)
        (run_a / "a.txt").write_text("a", encoding="utf-8")
        (run_b / "b.txt").write_text("b", encoding="utf-8")
        (work_dir / "loose.tmp").write_text("tmp", encoding="utf-8")

        cleanup_work_cache_aggressive(work_dir)

        self.assertTrue((run_a / "a.txt").exists())
        self.assertTrue((run_b / "b.txt").exists())
        self.assertFalse((work_dir / "loose.tmp").exists())

    def test_cleanup_work_cache_aggressive_can_cleanup_specific_run_scope(self) -> None:
        work_dir = self.root / "work"
        run_a = work_dir / "20260302-101010"
        run_b = work_dir / "20260302-101011"
        run_a.mkdir(parents=True, exist_ok=True)
        run_b.mkdir(parents=True, exist_ok=True)
        (run_a / "a.txt").write_text("a", encoding="utf-8")
        (run_b / "b.txt").write_text("b", encoding="utf-8")

        cleanup_work_cache_aggressive(work_dir, run_id=run_a.name)

        self.assertFalse(run_a.exists())
        self.assertTrue((run_b / "b.txt").exists())


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

        # os.replace 现在在 config.atomic_temp_path 中调用，需 patch config 模块
        with patch("quantclass_sync_internal.file_sync.shutil.copy2", wraps=shutil.copy2) as copy_mock, patch(
            "quantclass_sync_internal.config.os.replace",
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

        # os.replace 现在在 config.atomic_temp_path 中调用，需 patch config 模块
        with patch("quantclass_sync_internal.config.os.replace", side_effect=RuntimeError("replace failed")):
            with self.assertRaises(RuntimeError):
                sync_raw_file(src=src, target=target, dry_run=False)

        self.assertEqual("old", target.read_text(encoding="utf-8"))
        self.assertEqual([], list(target.parent.glob(f".{target.name}.tmp-raw-*")))

    def test_sync_raw_file_file_not_found_during_compare_continues_with_update(self) -> None:
        src = self.root / "src-race.txt"
        target = self.root / "target-race.txt"
        src.write_text("new", encoding="utf-8")
        target.write_text("old", encoding="utf-8")

        with patch("quantclass_sync_internal.file_sync._files_equal_by_chunk", side_effect=FileNotFoundError):
            result = sync_raw_file(src=src, target=target, dry_run=False)

        self.assertEqual("updated", result)
        self.assertEqual("new", target.read_text(encoding="utf-8"))


class SyncPayloadLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _payload(self) -> qcs.CsvPayload:
        return qcs.CsvPayload(
            note=None,
            header=["candle_end_time", "open", "high", "low", "close", "amount", "volume", "index_code"],
            rows=[["2024-01-01", "1", "1", "1", "1", "10", "10", "sh000300"]],
            encoding="utf-8",
            delimiter=",",
        )

    def test_sync_payload_waits_for_file_lock_before_writing(self) -> None:
        if fcntl is None:
            self.skipTest("fcntl is unavailable on this platform")

        target = self.root / "locked.csv"
        lock_path = target.parent / f".{target.name}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        ctx = multiprocessing.get_context("spawn")
        started = ctx.Event()
        finished = ctx.Event()
        process = ctx.Process(target=_sync_payload_lock_worker, args=(str(target), started, finished))

        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            process.start()
            self.assertTrue(started.wait(timeout=5))
            time.sleep(0.3)
            self.assertFalse(finished.is_set())
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            self.fail("sync payload worker did not exit after lock release")

        self.assertEqual(0, process.exitcode)
        payload = qcs.read_csv_payload(target, preferred_encoding="utf-8")
        self.assertEqual(1, len(payload.rows))

    def test_sync_payload_waits_for_fallback_lock_before_writing(self) -> None:
        target = self.root / "fallback-locked.csv"
        lock_path = target.parent / f".{target.name}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        started = threading.Event()
        finished = threading.Event()
        errors: list[BaseException] = []

        def worker() -> None:
            started.set()
            try:
                qcs.sync_payload_to_target(
                    incoming=self._payload(),
                    target=target,
                    rule=qcs.RULES["stock-main-index-data"],
                    dry_run=False,
                )
            except BaseException as exc:  # pragma: no cover - 仅用于跨线程收集异常
                errors.append(exc)
            finally:
                finished.set()

        with patch("quantclass_sync_internal.csv_engine.fcntl", None), patch("quantclass_sync_internal.csv_engine.msvcrt", None):
            thread = threading.Thread(target=worker, daemon=True)
            thread.start()
            self.assertTrue(started.wait(timeout=5))
            time.sleep(0.3)
            self.assertFalse(finished.is_set())

            os.close(lock_fd)
            lock_path.unlink()

            thread.join(timeout=10)

        self.assertFalse(thread.is_alive())
        self.assertEqual([], errors)
        payload = qcs.read_csv_payload(target, preferred_encoding="utf-8")
        self.assertEqual(1, len(payload.rows))

    def test_sync_payload_raises_when_lock_backend_unavailable(self) -> None:
        target = self.root / "lock-open-fail.csv"

        with patch("quantclass_sync_internal.csv_engine.fcntl", None), patch(
            "quantclass_sync_internal.csv_engine.msvcrt", None
        ), patch("quantclass_sync_internal.csv_engine.os.open", side_effect=OSError("open denied")):
            with self.assertRaises(RuntimeError):
                qcs.sync_payload_to_target(
                    incoming=self._payload(),
                    target=target,
                    rule=qcs.RULES["stock-main-index-data"],
                    dry_run=False,
                )

        self.assertFalse(target.exists())

    def test_sync_payload_fallback_lock_file_is_cleaned_after_write(self) -> None:
        target = self.root / "fallback-cleanup.csv"
        lock_path = target.parent / f".{target.name}.lock"

        with patch("quantclass_sync_internal.csv_engine.fcntl", None), patch("quantclass_sync_internal.csv_engine.msvcrt", None):
            result, added, _audit = qcs.sync_payload_to_target(
                incoming=self._payload(),
                target=target,
                rule=qcs.RULES["stock-main-index-data"],
                dry_run=False,
            )

        self.assertEqual("created", result)
        self.assertEqual(1, added)
        self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
