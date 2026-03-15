import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import typer

from quantclass_sync_internal.cli import cmd_setup, cmd_update, global_options


class _FakeContext:
    def __init__(self) -> None:
        self.invoked_subcommand = None
        self.obj = None
        self.resilient_parsing = False
        self.invocations = []

    def invoke(self, func, **kwargs):
        self.invocations.append((func, kwargs))
        return None

    def get_help(self) -> str:
        return "help"


class DefaultEntryUpdateTests(unittest.TestCase):
    def _call_global_options(self, ctx: _FakeContext, config_file: Path, dry_run: bool = False, verbose: bool = False) -> None:
        global_options(
            ctx=ctx,
            data_root=None,
            api_key="",
            hid="",
            secrets_file=None,
            config_file=config_file,
            dry_run=dry_run,
            report_file=None,
            stop_on_error=False,
            verbose=verbose,
        )

    def test_no_subcommand_with_config_invokes_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            ctx = _FakeContext()

            with self.assertRaises(typer.Exit) as cm:
                self._call_global_options(ctx=ctx, config_file=config_file)

            self.assertEqual(0, cm.exception.exit_code)
            self.assertEqual(1, len(ctx.invocations))
            func, kwargs = ctx.invocations[0]
            self.assertEqual(cmd_update, func)
            self.assertIs(kwargs.get("ctx"), ctx)

    def test_no_subcommand_with_config_passes_global_flags_to_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            ctx = _FakeContext()

            with self.assertRaises(typer.Exit):
                self._call_global_options(ctx=ctx, config_file=config_file, dry_run=True, verbose=True)

            self.assertEqual(1, len(ctx.invocations))
            _func, kwargs = ctx.invocations[0]
            self.assertEqual(True, kwargs.get("dry_run"))
            self.assertEqual(True, kwargs.get("verbose"))

    def test_no_subcommand_without_config_invokes_setup(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "missing.json"
            ctx = _FakeContext()

            with mock.patch.object(sys.stdin, "isatty", return_value=True):
                with self.assertRaises(typer.Exit) as cm:
                    self._call_global_options(ctx=ctx, config_file=config_file)

            self.assertEqual(0, cm.exception.exit_code)
            self.assertEqual(1, len(ctx.invocations))
            func, kwargs = ctx.invocations[0]
            self.assertEqual(cmd_setup, func)
            self.assertIs(kwargs.get("ctx"), ctx)


if __name__ == "__main__":
    unittest.main()
