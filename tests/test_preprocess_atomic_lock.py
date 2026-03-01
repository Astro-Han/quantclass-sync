import fcntl
import multiprocessing as mp
import tempfile
import unittest
from pathlib import Path
from queue import Empty
from unittest.mock import patch

import pandas as pd

from coin_preprocess_internal.pivot import _write_pickles_atomically


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


if __name__ == "__main__":
    unittest.main()
