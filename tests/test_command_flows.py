import os
import tempfile
import unittest
import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import typer

import quantclass_sync as qcs
from quantclass_sync_internal import cli as cli_module
from quantclass_sync_internal import config as config_module
from quantclass_sync_internal import models as models_module


class _DummyTyperContext:
    def __init__(self, obj: qcs.CommandContext) -> None:
        self.obj = obj


class _FakeGlobalContext:
    def __init__(self, invoked_subcommand: str | None = None) -> None:
        self.obj = None
        self.invoked_subcommand = invoked_subcommand
        self.resilient_parsing = False
        self.invocations: list[tuple[object, dict]] = []

    def invoke(self, func, **kwargs):
        self.invocations.append((func, kwargs))
        return None

    def get_help(self) -> str:
        return "help"


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
        config = qcs.load_user_config_or_raise(self.config_file)
        self.assertEqual(self.data_root.resolve(), config.data_root.resolve())
        self.assertEqual(qcs.PRODUCT_MODE_LOCAL_SCAN, config.product_mode)
        self.assertEqual([], config.default_products)
        self.assertEqual(self.secrets_file.resolve(), config.secrets_file.resolve())
        secrets_text = self.secrets_file.read_text(encoding="utf-8")
        self.assertIn("QUANTCLASS_API_KEY=api_key_x", secrets_text)
        self.assertIn("QUANTCLASS_HID=hid_x", secrets_text)
        self.assertEqual(0o600, self.secrets_file.stat().st_mode & 0o777)

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

    def test_cmd_setup_missing_hid_does_not_create_data_root(self) -> None:
        missing_root = self.root / "new-data-root"
        self.assertFalse(missing_root.exists())
        ctx = _DummyTyperContext(self._base_ctx())
        with self.assertRaises(typer.Exit):
            qcs.cmd_setup(
                ctx=ctx,
                non_interactive=True,
                skip_check=True,
                data_root=str(missing_root),
                api_key="api_key_x",
                hid="",
                product_mode=qcs.PRODUCT_MODE_LOCAL_SCAN,
                products=[],
            )
        self.assertFalse(missing_root.exists())

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

    def test_save_setup_artifacts_atomic_rollback_chmod_failure_bubbles_up(self) -> None:
        config_path = self.root / "existing_config.json"
        secrets_path = self.root / "existing_secrets.env"
        config_path.write_text("{\"old\":\"config\"}\n", encoding="utf-8")
        secrets_path.write_text("QUANTCLASS_API_KEY=old\nQUANTCLASS_HID=old\n", encoding="utf-8")
        os.chmod(config_path, 0o600)
        os.chmod(secrets_path, 0o600)

        user_config = qcs.UserConfig(
            data_root=self.data_root,
            product_mode=qcs.PRODUCT_MODE_LOCAL_SCAN,
            default_products=[],
            secrets_file=secrets_path,
        )
        original_chmod = os.chmod
        chmod_calls = {"count": 0}

        def _chmod_side_effect(*args, **kwargs):
            chmod_calls["count"] += 1
            if chmod_calls["count"] == 2:
                raise PermissionError("restore chmod denied")
            return original_chmod(*args, **kwargs)

        with patch("quantclass_sync_internal.config.save_user_config_atomic", side_effect=RuntimeError("config write failed")):
            with patch("quantclass_sync_internal.config.os.chmod", side_effect=_chmod_side_effect):
                with self.assertRaises(RuntimeError) as cm:
                    config_module.save_setup_artifacts_atomic(
                        config_path=config_path,
                        config=user_config,
                        secrets_path=secrets_path,
                        api_key="new_api",
                        hid="new_hid",
                    )
        self.assertIn("回滚不完整", str(cm.exception))
        self.assertEqual("{\"old\":\"config\"}\n", config_path.read_text(encoding="utf-8"))
        self.assertIn("QUANTCLASS_API_KEY=old", secrets_path.read_text(encoding="utf-8"))

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
                workers=1,
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
                    workers=1,
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
                workers=1,
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
                workers=1,
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
                workers=1,
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
                    workers=1,
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
                workers=1,
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
                workers=1,
            )

        run_mock.assert_called_once()
        command_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertFalse(command_ctx.verbose)

    def test_cmd_update_resolves_relative_paths_against_config_file_dir(self) -> None:
        config_dir = self.root / "config-home"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = config_dir / "user_config.json"
        relative_data_root = Path("relative-data")
        relative_secrets_file = Path("secrets/user_secrets.env")
        expected_data_root = (config_dir / relative_data_root).resolve()
        expected_data_root.mkdir(parents=True, exist_ok=True)
        expected_secrets_file = (config_dir / relative_secrets_file).resolve()
        expected_secrets_file.parent.mkdir(parents=True, exist_ok=True)
        qcs.save_user_secrets_atomic(expected_secrets_file, api_key="k", hid="h")
        qcs.save_user_config_atomic(
            config_file,
            qcs.UserConfig(
                data_root=relative_data_root,
                product_mode=qcs.PRODUCT_MODE_LOCAL_SCAN,
                default_products=["stock-trading-data"],
                secrets_file=relative_secrets_file,
            ),
        )
        ctx = _DummyTyperContext(
            self._base_ctx().model_copy(
                update={
                    "config_file": config_file,
                    "data_root_from_cli": False,
                    "secrets_file_from_cli": False,
                    "data_root": self.root / "unused-data-root",
                    "secrets_file": self.root / "unused-secrets-file",
                }
            )
        )

        with patch(
            "quantclass_sync.resolve_credentials_for_update",
            return_value=("k", "h", "setup_secrets"),
        ) as resolve_mock, patch("quantclass_sync.run_update_with_settings", return_value=0) as run_mock:
            qcs.cmd_update(
                ctx=ctx,
                dry_run=False,
                verbose=False,
                products=[],
                force_update=False,
                workers=1,
            )

        self.assertEqual(expected_secrets_file, resolve_mock.call_args.kwargs["secrets_file"])
        run_ctx = run_mock.call_args.kwargs["command_ctx"]
        self.assertEqual(expected_data_root, run_ctx.data_root)
        self.assertEqual(expected_secrets_file, run_ctx.secrets_file)

    def test_command_guard_marks_unknown_exception_as_unexpected_error(self) -> None:
        @cli_module.command_guard("dummy")
        def _boom(ctx):
            raise RuntimeError("boom")

        ctx = _DummyTyperContext(self._base_ctx())
        with patch("quantclass_sync_internal.cli._handle_command_exception") as handler_mock:
            with self.assertRaises(typer.Exit) as cm:
                _boom(ctx=ctx)

        self.assertEqual(1, cm.exception.exit_code)
        self.assertEqual(cli_module.REASON_UNEXPECTED_ERROR, handler_mock.call_args.args[2])

    def test_global_options_syncs_logger_runtime_even_when_exiting(self) -> None:
        config_file = self.root / "exists.json"
        config_file.write_text("{}", encoding="utf-8")
        ctx = _FakeGlobalContext(invoked_subcommand=None)
        models_module.LOGGER = models_module.ConsoleLogger(level="INFO", run_id="old-run-id")

        with self.assertRaises(typer.Exit):
            qcs.global_options(
                ctx=ctx,
                data_root=None,
                api_key="",
                hid="",
                secrets_file=None,
                config_file=config_file,
                dry_run=False,
                report_file=None,
                stop_on_error=False,
                verbose=True,
            )

        self.assertIsNotNone(ctx.obj)
        self.assertEqual(ctx.obj.run_id, models_module.LOGGER.run_id)
        self.assertEqual("DEBUG", models_module.LOGGER.level)

    def test_global_options_run_id_has_subsecond_entropy(self) -> None:
        ctx1 = _FakeGlobalContext(invoked_subcommand="update")
        ctx2 = _FakeGlobalContext(invoked_subcommand="update")

        # 直接 patch new_run_id 返回受控值，验证 run_id 携带微秒级熵
        with patch("quantclass_sync_internal.cli.new_run_id") as mock_fn:
            mock_fn.side_effect = [
                "20260302-093000-111111-p999-aabbccdd",
                "20260302-093000-222222-p999-eeff0011",
            ]
            qcs.global_options(
                ctx=ctx1,
                data_root=None,
                api_key="",
                hid="",
                secrets_file=None,
                config_file=self.config_file,
                dry_run=False,
                report_file=None,
                stop_on_error=False,
                verbose=False,
            )
            qcs.global_options(
                ctx=ctx2,
                data_root=None,
                api_key="",
                hid="",
                secrets_file=None,
                config_file=self.config_file,
                dry_run=False,
                report_file=None,
                stop_on_error=False,
                verbose=False,
            )

        run_id_1 = ctx1.obj.run_id
        run_id_2 = ctx2.obj.run_id
        self.assertNotEqual(run_id_1, run_id_2)
        self.assertTrue(run_id_1.startswith("20260302-093000-111111-p"))
        self.assertTrue(run_id_2.startswith("20260302-093000-222222-p"))

    def test_global_options_run_id_uses_random_hex_suffix(self) -> None:
        ctx = _FakeGlobalContext(invoked_subcommand="update")
        with patch(
            "quantclass_sync_internal.cli.new_run_id",
            return_value="20260302-093000-123456-p999-deadbeef",
        ):
            qcs.global_options(
                ctx=ctx,
                data_root=None,
                api_key="",
                hid="",
                secrets_file=None,
                config_file=self.config_file,
                dry_run=False,
                report_file=None,
                stop_on_error=False,
                verbose=False,
            )

        self.assertTrue(ctx.obj.run_id.endswith("-deadbeef"))

    def test_bind_orchestrator_runtime_rebinds_after_external_override(self) -> None:
        qcs._bind_orchestrator_runtime(probe_callable=qcs._probe_downloadable_dates)
        qcs._orchestrator.get_latest_times = lambda *args, **kwargs: []

        qcs._bind_orchestrator_runtime(probe_callable=qcs._probe_downloadable_dates)

        self.assertIs(qcs.get_latest_times, qcs._orchestrator.get_latest_times)

    def test_run_update_with_all_invalid_explicit_products_keeps_report_consistent(self) -> None:
        report_path = self.root / "run_report_invalid_explicit.json"
        ctx = self._base_ctx().model_copy(update={"dry_run": True})

        with patch("quantclass_sync.resolve_report_path", return_value=report_path), patch(
            "quantclass_sync.load_catalog_or_raise",
            return_value=["stock-trading-data"],
        ), patch(
            "quantclass_sync_internal.orchestrator.discover_local_products",
            return_value=[],
        ), patch(
            "quantclass_sync_internal.orchestrator.resolve_products_by_mode",
            return_value=([], [], ["bad-product"]),
        ):
            exit_code = qcs.run_update_with_settings(
                command_ctx=ctx,
                mode="local",
                products=["bad-product"],
                force_update=False,
                command_name="update",
            )

        self.assertEqual(qcs.EXIT_CODE_NO_EXECUTABLE_PRODUCTS, exit_code)
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(0, payload["failed_total"])
        self.assertEqual(1, payload["skipped_total"])
        self.assertEqual({"invalid_explicit_product": 1}, payload["reason_code_counts"])
        self.assertEqual(["skipped"], [item["status"] for item in payload["products"]])

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

        @contextmanager
        def _open_status_db():
            try:
                yield conn
            finally:
                conn.close()

        with patch("quantclass_sync.load_catalog_or_raise", return_value=["p1", "p2"]), patch(
            "quantclass_sync_internal.cli.discover_local_products",
            return_value=[],
        ), patch(
            "quantclass_sync_internal.cli.open_status_db",
            return_value=_open_status_db(),
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
