import fcntl
import multiprocessing as mp
import os
import tempfile
import unittest
from pathlib import Path
from queue import Empty
from unittest.mock import patch

import pandas as pd

from coin_preprocess_internal.pivot import _acquire_output_locks, _write_pickles_atomically


def _atomic_write_worker(output_dir: str, started, results) -> None:
    target = Path(output_dir) / "spot_dict.pkl"
    started.set()
    try:
        _write_pickles_atomically({target: {"worker": "ok"}})
        results.put(("ok", None))
    except Exception as exc:
        results.put(("error", f"{type(exc).__name__}:{exc}"))


class PreprocessAtomicLockTests(unittest.TestCase):
    def test_write_pickles_atomically_calls_flock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            target = output_dir / "spot_dict.pkl"
            with patch("coin_preprocess_internal.pivot.fcntl.flock") as flock_mock:
                _write_pickles_atomically({target: {"k": "v"}})

            ops = [call.args[1] for call in flock_mock.call_args_list]
            self.assertIn(fcntl.LOCK_EX, ops)
            self.assertIn(fcntl.LOCK_UN, ops)

    def test_write_pickles_atomically_waits_for_existing_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            lock_path = output_dir / ".preprocess.lock"
            lock_path.touch()

            ctx = mp.get_context("spawn")
            started = ctx.Event()
            results = ctx.Queue()
            process = ctx.Process(target=_atomic_write_worker, args=(str(output_dir), started, results))

            with lock_path.open("a+") as lock_handle:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                process.start()
                self.assertTrue(started.wait(timeout=3))

                with self.assertRaises(Empty):
                    results.get(timeout=0.4)

                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

            status, payload = results.get(timeout=5)
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
                self.fail("atomic write worker did not exit after lock release")

            self.assertEqual("ok", status)
            self.assertIsNone(payload)
            self.assertEqual({"worker": "ok"}, pd.read_pickle(output_dir / "spot_dict.pkl"))

    def test_acquire_locks_releases_all_on_partial_failure(self) -> None:
        """多目录加锁时第二个目录失败，验证第一个目录的锁被正确释放。"""

        with tempfile.TemporaryDirectory() as tmpdir:
            dir_a = Path(tmpdir) / "a"
            dir_b = Path(tmpdir) / "b"
            dir_a.mkdir()
            dir_b.mkdir()
            payloads = {dir_a / "out.pkl": "data_a", dir_b / "out.pkl": "data_b"}

            original_flock = fcntl.flock
            call_count = 0

            def flock_bomb(fd, op):  # type: ignore[no-untyped-def]
                nonlocal call_count
                call_count += 1
                if call_count >= 2 and op == fcntl.LOCK_EX:
                    raise OSError("模拟加锁失败")
                return original_flock(fd, op)

            with patch("coin_preprocess_internal.pivot.fcntl.flock", side_effect=flock_bomb):
                with self.assertRaises(OSError):
                    _acquire_output_locks(payloads)

            lock_file = (dir_a / ".preprocess.lock").open("a+")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()

    def test_write_pickles_atomically_keeps_backup_when_restore_fails(self) -> None:
        """提交失败且回滚恢复失败时，backup 必须保留供人工恢复。"""

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            spot_target = output_dir / "spot_dict.pkl"
            swap_target = output_dir / "swap_dict.pkl"
            pd.to_pickle({"old": "spot"}, spot_target)
            pd.to_pickle({"old": "swap"}, swap_target)

            real_replace = os.replace
            temp_commit_failed = False

            def flaky_replace(src: str, dst: str) -> None:
                nonlocal temp_commit_failed
                src_path = Path(src)
                dst_path = Path(dst)
                if dst_path == swap_target and ".tmp-" in src_path.name and not temp_commit_failed:
                    temp_commit_failed = True
                    raise RuntimeError("inject_temp_commit_failure")
                if dst_path == swap_target and ".bak-" in src_path.name:
                    raise RuntimeError("inject_restore_failure")
                real_replace(src, dst)

            with patch("coin_preprocess_internal.pivot.os.replace", side_effect=flaky_replace):
                with self.assertRaises(RuntimeError):
                    _write_pickles_atomically(
                        {
                            spot_target: {"new": "spot"},
                            swap_target: {"new": "swap"},
                        }
                    )

            backup_files = list(output_dir.glob(f".{swap_target.name}.bak-*"))
            self.assertEqual(1, len(backup_files))
            self.assertEqual({"old": "swap"}, pd.read_pickle(backup_files[0]))
            self.assertEqual({"old": "spot"}, pd.read_pickle(spot_target))


if __name__ == "__main__":
    unittest.main()
