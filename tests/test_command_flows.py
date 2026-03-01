import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import typer

import quantclass_sync as qcs


class _DummyTyperContext:
    def __init__(self, obj: qcs.CommandContext) -> None:
        self.obj = obj


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

    def test_cmd_init_dry_run_executes_without_name_error(self) -> None:
        ctx = _DummyTyperContext(self._base_ctx())
        with patch("quantclass_sync.load_catalog_or_raise", return_value=["stock-trading-data"]), patch(
            "quantclass_sync_internal.cli.discover_local_products",
            return_value=[],
        ):
            qcs.cmd_init(ctx=ctx)

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
