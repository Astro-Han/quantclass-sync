import inspect
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import typer

from quantclass_sync_internal import cli as cli_module
from quantclass_sync_internal.config import resolve_credentials, resolve_credentials_for_update


class CredentialPriorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.secrets_file = self.root / "user_secrets.env"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_compat_credential_priority_matches_update(self) -> None:
        self.secrets_file.write_text(
            "QUANTCLASS_API_KEY=file_api_key\nQUANTCLASS_HID=file_hid\n",
            encoding="utf-8",
        )

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "env_api_key", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ):
            compat_api_key, compat_hid = resolve_credentials(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )
            update_api_key, update_hid, credential_source = resolve_credentials_for_update(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )

        self.assertEqual(("file_api_key", "file_hid"), (compat_api_key, compat_hid))
        self.assertEqual((compat_api_key, compat_hid), (update_api_key, update_hid))
        self.assertEqual("setup_secrets", credential_source)

    def test_compat_resolve_credentials_supports_mixed_file_env(self) -> None:
        self.secrets_file.write_text("QUANTCLASS_API_KEY=file_api_key\n", encoding="utf-8")

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "", "QUANTCLASS_HID": "env_hid"},
            clear=False,
        ):
            compat_api_key, compat_hid = resolve_credentials(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )
            update_api_key, update_hid, credential_source = resolve_credentials_for_update(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )

        self.assertEqual(("file_api_key", "env_hid"), (compat_api_key, compat_hid))
        self.assertEqual((compat_api_key, compat_hid), (update_api_key, update_hid))
        self.assertEqual("mixed(api=setup_secrets,hid=env)", credential_source)

    def test_compat_resolve_credentials_ignores_overly_broad_key_names(self) -> None:
        self.secrets_file.write_text("key=wrong_api\nuuid=wrong_hid\n", encoding="utf-8")

        with patch.dict(
            "os.environ",
            {"QUANTCLASS_API_KEY": "", "QUANTCLASS_HID": ""},
            clear=False,
        ):
            compat_api_key, compat_hid = resolve_credentials(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )
            update_api_key, update_hid, credential_source = resolve_credentials_for_update(
                cli_api_key="",
                cli_hid="",
                secrets_file=self.secrets_file,
            )

        self.assertEqual(("", ""), (compat_api_key, compat_hid))
        self.assertEqual((compat_api_key, compat_hid), (update_api_key, update_hid))
        self.assertEqual("missing", credential_source)

    def test_global_options_verbose_default_is_false(self) -> None:
        verbose_option = inspect.signature(cli_module.global_options).parameters["verbose"].default
        self.assertIsInstance(verbose_option, typer.models.OptionInfo)
        self.assertIs(False, verbose_option.default)


if __name__ == "__main__":
    unittest.main()
