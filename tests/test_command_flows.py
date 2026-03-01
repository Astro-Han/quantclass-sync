import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import typer

import quantclass_sync as qcs


class _DummyTyperContext:
    def __init__(self, obj: qcs.CommandContext) -> None:
        self.obj = obj


class _FakeConn:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.closed = False

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


class CommandFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data_root = self.root / "data"
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.work_dir = self.root / ".cache"
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.report_path = self.root / "run_report.json"
        self.secrets_file = self.root / "user_secrets.env"
        self.config_file = self.root / "user_config.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _base_ctx(self) -> qcs.CommandContext:
        return qcs.CommandContext(
            run_id="test-command-flow",
            data_root=self.data_root,
            data_root_from_cli=True,
            api_key="",
            hid="",
            secrets_file=self.secrets_file,
            secrets_file_from_cli=True,
            config_file=self.config_file,
            dry_run=True,
            verbose=False,
            work_dir=self.work_dir,
            catalog_file=qcs.DEFAULT_CATALOG_FILE.resolve(),
        )

    def _write_user_config(self, product_mode: str = qcs.PRODUCT_MODE_LOCAL_SCAN) -> None:
        cfg = qcs.UserConfig(
            data_root=self.data_root,
            product_mode=product_mode,
            default_products=["stock-trading-data"],
            secrets_file=self.secrets_file,
        )
        qcs.save_user_config_atomic(self.config_file, cfg)

    def test_cmd_setup_non_interactive_success(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        qcs.cmd_setup(
            ctx=ctx,
            non_interactive=True,
            skip_check=True,
            data_root=str(self.data_root),
            api_key="api_key_x",
            hid="hid_x",
            product_mode=qcs.PRODUCT_MODE_LOCAL_SCAN,
            products=[],
        )
        self.assertTrue(self.config_file.exists())
        self.assertTrue(self.secrets_file.exists())

    def test_cmd_setup_non_interactive_missing_hid_exits(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with self.assertRaises(typer.Exit) as cm:
            qcs.cmd_setup(
                ctx=ctx,
                non_interactive=True,
                skip_check=True,
                data_root=str(self.data_root),
                api_key="api_key_x",
                hid="",
                product_mode=qcs.PRODUCT_MODE_LOCAL_SCAN,
                products=[],
            )
        self.assertEqual(1, cm.exception.exit_code)

    def test_save_user_secrets_atomic_creates_temp_with_mode_600(self) -> None:
        with patch("quantclass_sync_internal.config.os.open", wraps=os.open) as open_mock:
            qcs.save_user_secrets_atomic(self.secrets_file, api_key="k", hid="h")

        open_modes = [call.args[2] for call in open_mock.call_args_list if len(call.args) >= 3]
        self.assertIn(0o600, open_modes)
        self.assertEqual(0o600, self.secrets_file.stat().st_mode & 0o777)

    def test_save_user_secrets_atomic_chmod_failure_raises(self) -> None:
        with patch("quantclass_sync_internal.config.os.chmod", side_effect=PermissionError("chmod denied")):
            with self.assertRaises(PermissionError):
                qcs.save_user_secrets_atomic(self.secrets_file, api_key="k", hid="h")

    def test_cmd_update_success_calls_run_update_with_settings(self) -> None:
        self._write_user_config()
        qcs.save_user_secrets_atomic(self.secrets_file, api_key="k", hid="h")
        ctx = _DummyTyperContext(self._base_ctx())

        with patch(
            "quantclass_sync.resolve_credentials_for_update",
            return_value=("k", "h", "setup_secrets"),
        ), patch("quantclass_sync.run_update_with_settings", return_value=0) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
            )
        run_mock.assert_called_once()

    def test_cmd_update_nonzero_exit_code_bubbles_up(self) -> None:
        self._write_user_config()
        qcs.save_user_secrets_atomic(self.secrets_file, api_key="k", hid="h")
        ctx = _DummyTyperContext(self._base_ctx())

        with patch(
            "quantclass_sync.resolve_credentials_for_update",
            return_value=("k", "h", "setup_secrets"),
        ), patch("quantclass_sync.run_update_with_settings", return_value=2):
            with self.assertRaises(typer.Exit) as cm:
                qcs.cmd_update(
                    ctx=ctx,
                    dry_run=False,
                    verbose=False,
                    products=[],
                    force_update=False,
                )
        self.assertEqual(2, cm.exception.exit_code)

    def test_cmd_update_cli_credentials_allow_missing_secrets_file(self) -> None:
        self._write_user_config()
        ctx = _DummyTyperContext(
            self._base_ctx().model_copy(
                update={
                    "api_key": "cli_api_key",
                    "hid": "cli_hid",
                }
            )
        )

        with patch("quantclass_sync_internal.cli.load_user_secrets_or_raise") as secrets_guard, patch(
            "quantclass_sync.run_update_with_settings",
            return_value=0,
        ) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
            )

        secrets_guard.assert_not_called()
        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertEqual("cli_api_key", command_ctx.api_key)
        self.assertEqual("cli_hid", command_ctx.hid)

    def test_cmd_update_env_credentials_allow_missing_secrets_file(self) -> None:
        self._write_user_config()
        ctx = _DummyTyperContext(self._base_ctx())

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "env_api_key", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ), patch("quantclass_sync_internal.cli.load_user_secrets_or_raise") as secrets_guard, patch(
            "quantclass_sync.run_update_with_settings",
            return_value=0,
        ) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
            )

        secrets_guard.assert_not_called()
        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertEqual("env_api_key", command_ctx.api_key)
        self.assertEqual("env_hid", command_ctx.hid)

    def test_cmd_update_mixed_file_api_and_env_hid_skips_strict_file_check(self) -> None:
        self._write_user_config()
        self.secrets_file.write_text("QUANTCLASS_API_KEY=file_api_key\n", encoding="utf-8")
        ctx = _DummyTyperContext(self._base_ctx())

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ), patch("quantclass_sync_internal.cli.load_user_secrets_or_raise") as secrets_guard, patch(
            "quantclass_sync.run_update_with_settings",
            return_value=0,
        ) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
            )

        secrets_guard.assert_not_called()
        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertEqual("file_api_key", command_ctx.api_key)
        self.assertEqual("env_hid", command_ctx.hid)

    def test_cmd_update_all_credentials_missing_exits(self) -> None:
        self._write_user_config()
        ctx = _DummyTyperContext(self._base_ctx())

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "", "QUANTCLASS_HID": ""},
            clear=False,
        ), patch("quantclass_sync.run_update_with_settings") as run_mock:
            with self.assertRaises(typer.Exit) as cm:
                qcs.cmd_update(
                    ctx=ctx,
                    dry_run=False,
                    verbose=False,
                    products=[],
                    force_update=False,
                )

        self.assertEqual(1, cm.exception.exit_code)
        run_mock.assert_not_called()

    def test_cmd_update_verbose_defaults_to_global_setting(self) -> None:
        self._write_user_config()
        ctx = _DummyTyperContext(self._base_ctx().model_copy(update={"verbose": True}))

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "env_api_key", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ), patch("quantclass_sync.run_update_with_settings", return_value=0) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=None,
                products=[],
                force_update=False,
            )

        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertTrue(command_ctx.verbose)

    def test_cmd_update_no_verbose_overrides_global_debug(self) -> None:
        self._write_user_config()
        ctx = _DummyTyperContext(self._base_ctx().model_copy(update={"verbose": True}))

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "env_api_key", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ), patch("quantclass_sync.run_update_with_settings", return_value=0) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
            )

        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertFalse(command_ctx.verbose)

    def test_cmd_repair_sort_success(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with patch("quantclass_sync.resolve_report_path", return_value=self.report_path), patch(
            "quantclass_sync.sortable_products",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync.repair_sort_product_files",
            return_value=(qcs.SyncStats(updated_files=1), 0),
        ):
            qcs.cmd_repair_sort(ctx=ctx, products=[], strict=False)

        self.assertTrue(self.report_path.exists())

    def test_cmd_repair_sort_strict_error_exits(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with patch("quantclass_sync.resolve_report_path", return_value=self.report_path), patch(
            "quantclass_sync.sortable_products",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync.repair_sort_product_files",
            side_effect=RuntimeError("repair failed"),
        ):
            with self.assertRaises(typer.Exit) as cm:
                qcs.cmd_repair_sort(ctx=ctx, products=[], strict=True)
        self.assertEqual(1, cm.exception.exit_code)

    def test_repair_sort_command_registers_hyphen_alias(self) -> None:
        commands = {
            cmd.name: cmd.callback
            for cmd in qcs.app.registered_commands
            if cmd.name in {"repair_sort", "repair-sort"}
        }
        self.assertIn("repair_sort", commands)
        self.assertIn("repair-sort", commands)
        self.assertIs(commands["repair_sort"], commands["repair-sort"])

    def test_cmd_init_dry_run_executes_without_name_error(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with patch("quantclass_sync.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.cli.discover_local_products",
            return_value=[],
        ):
            qcs.cmd_init(ctx=ctx)

    def test_cmd_init_writes_with_single_commit(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx().model_copy(update={"dry_run": False}))
        conn = _FakeConn()
        with patch("quantclass_sync.load_catalog_or_raise", return_value=["p1", "p2"]), patch(
            "quantclass_sync_internal.cli.discover_local_products",
            return_value=[],
        ), patch(
            "quantclass_sync_internal.cli.connect_status_db",
            return_value=conn,
        ), patch(
            "quantclass_sync_internal.cli.load_product_status",
            return_value=None,
        ), patch(
            "quantclass_sync_internal.cli.export_status_json",
        ), patch(
            "quantclass_sync_internal.cli.upsert_product_status",
        ) as upsert_mock:
            qcs.cmd_init(ctx=ctx)

        self.assertEqual(2, upsert_mock.call_count)
        for call in upsert_mock.call_args_list:
            self.assertIs(False, call.kwargs.get("commit_immediately"))
        self.assertEqual(1, conn.commit_calls)
        self.assertTrue(conn.closed)

    def test_cmd_one_data_executes_single_plan(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        started_at = 1234567890.0
        with patch("quantclass_sync.resolve_report_path", return_value=self.report_path), patch(
            "quantclass_sync_internal.cli._execute_plans",
            return_value=(qcs.SyncStats(), False, started_at),
        ) as execute_mock:
            qcs.cmd_one_data(
                ctx=ctx,
                product="stock-trading-data",
                date_time="",
                force_update=False,
            )

        execute_mock.assert_called_once()
        plans = execute_mock.call_args.kwargs["plans"]
        self.assertEqual(1, len(plans))
        self.assertEqual("stock-trading-data", plans[0].name)
        self.assertTrue(self.report_path.exists())

    def test_cmd_all_data_invalid_mode_raises_bad_parameter(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with self.assertRaises(typer.BadParameter):
            qcs.cmd_all_data(ctx=ctx, mode="bad-mode", products=[], force_update=False)


if __name__ == "__main__":
    unittest.main()
