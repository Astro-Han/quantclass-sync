from pathlib import Path
import unittest


class ArchitectureTests(unittest.TestCase):
    """校验关键依赖方向，防止入口和低层逆向耦合。"""

    def _read(self, rel_path: str) -> str:
        return Path(rel_path).read_text(encoding="utf-8")

    def test_internal_modules_do_not_import_entrypoint(self) -> None:
        root = Path("quantclass_sync_internal")
        for path in root.glob("*.py"):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("import quantclass_sync", text, msg=str(path))
            self.assertNotIn("from quantclass_sync import", text, msg=str(path))

    def test_low_level_modules_do_not_depend_on_cli(self) -> None:
        low_level = [
            "quantclass_sync_internal/constants.py",
            "quantclass_sync_internal/models.py",
            "quantclass_sync_internal/config.py",
            "quantclass_sync_internal/http_client.py",
            "quantclass_sync_internal/archive.py",
            "quantclass_sync_internal/status_store.py",
            "quantclass_sync_internal/csv_engine.py",
            "quantclass_sync_internal/file_sync.py",
            "quantclass_sync_internal/reporting.py",
        ]
        for path in low_level:
            text = self._read(path)
            self.assertNotIn("from .cli import", text, msg=path)
            self.assertNotIn("import quantclass_sync_internal.cli", text, msg=path)


if __name__ == "__main__":
    unittest.main()
