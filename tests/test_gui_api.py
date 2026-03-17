"""SyncApi GUI API 层单元测试。

覆盖范围:
  - get_overview: 正常返回 / 配置文件不存在
  - get_config: 正常返回 / 配置文件不存在
  - get_sync_progress: 初始状态
  - start_sync: 正常启动 / 重复启动 / 无凭证
  - 进度回调更新
  - 前端字段名验证
"""

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


# 被测模块路径前缀
_API_MOD = "quantclass_sync_internal.gui.api"


def _make_mock_config(tmp_dir: str) -> MagicMock:
    """创建标准 mock UserConfig。"""
    mock_config = MagicMock()
    mock_config.data_root = Path(tmp_dir)
    mock_config.default_products = ["stock-trading-data"]
    mock_config.secrets_file = Path(tmp_dir) / "secrets.env"
    return mock_config


def _make_product_rows(count: int = 3):
    """生成 get_products_overview 返回的 raw product 列表（1 green, 1 red, 1 gray）。"""
    return [
        {
            "name": "product-a",
            "status_color": "green",
            "local_date": "2026-03-13",
            "days_behind": 0,
            "last_status": "ok",
            "last_error": "",
        },
        {
            "name": "product-b",
            "status_color": "red",
            "local_date": "2026-03-09",
            "days_behind": 4,
            "last_status": "error",
            "last_error": "HTTP 403",
        },
        {
            "name": "product-c",
            "status_color": "gray",
            "local_date": None,
            "days_behind": None,
            "last_status": "",
            "last_error": "",
        },
    ][:count]


def _make_raw_run() -> dict:
    """生成 get_latest_run_summary 返回的 raw 运行摘要。"""
    return {
        "run_id": "20260313-120000-000000-p1-abcd",
        "started_at": "2026-03-13T12:00:00Z",
        "ended_at": "2026-03-13T12:00:05Z",
        "duration_seconds": 5.0,
        "success_total": 2,
        "failed_total": 1,
        "skipped_total": 0,
        "failed_products": [
            {"product": "product-b", "error": "HTTP 403", "reason_code": "auth_error"}
        ],
    }


class TestGetOverviewSuccess(unittest.TestCase):
    """get_overview 正常路径：返回产品列表和统计。"""

    def test_get_overview_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # 创建临时配置文件，让 DEFAULT_USER_CONFIG_FILE.resolve().exists() 返回 True
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a", "product-b", "product-c"]), \
                 patch(f"{_API_MOD}.get_products_overview", return_value=_make_product_rows(3)), \
                 patch(f"{_API_MOD}.get_latest_run_summary", return_value=_make_raw_run()), \
                 patch(f"{_API_MOD}.report_dir_path", return_value=Path(tmp_dir) / "log"):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_overview()

        self.assertTrue(result["ok"])
        # 验证产品列表长度
        self.assertEqual(len(result["products"]), 3)
        # 验证颜色统计
        summary = result["summary"]
        self.assertEqual(summary["green"], 1)
        self.assertEqual(summary["red"], 1)
        self.assertEqual(summary["gray"], 1)
        # 验证 last_run 字段已转换
        self.assertIsNotNone(result["last_run"])
        self.assertEqual(result["last_run"]["ok"], 2)
        self.assertEqual(result["last_run"]["error"], 1)


class TestGetOverviewNoConfig(unittest.TestCase):
    """get_overview 配置文件不存在时返回 ok=False。"""

    def test_get_overview_no_config(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            # 指向不存在的路径
            missing_file = Path(tmp_dir) / "nonexistent_config.json"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", missing_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_overview()

        self.assertFalse(result["ok"])
        self.assertIn("未找到", result["error"])


class TestGetConfigSuccess(unittest.TestCase):
    """get_config 配置和凭证均有效时返回 config_exists=True。"""

    def test_get_config_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text('{"data_root": "/some/path"}', encoding="utf-8")

            secrets_file = Path(tmp_dir) / "user_secrets.env"
            secrets_file.write_text(
                "QUANTCLASS_API_KEY=test-key\nQUANTCLASS_HID=test-hid\n",
                encoding="utf-8",
            )

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a", "product-b"]):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertTrue(result["config_exists"])
        self.assertIsInstance(result["data_root"], str)
        self.assertTrue(len(result["data_root"]) > 0)
        self.assertEqual(result["product_count"], 2)


class TestGetConfigNoConfig(unittest.TestCase):
    """get_config 配置文件不存在时 config_exists=False，product_count=0。"""

    def test_get_config_no_config(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_file = Path(tmp_dir) / "nonexistent_config.json"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", missing_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])
        self.assertEqual(result["product_count"], 0)


class TestGetConfigJsonInvalid(unittest.TestCase):
    """get_config: config JSON 损坏时返回 config_exists=False。"""

    def test_json_invalid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{invalid json", encoding="utf-8")

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])


class TestGetConfigDataRootEmpty(unittest.TestCase):
    """get_config: data_root 为空时返回 config_exists=False。"""

    def test_data_root_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text('{"data_root": ""}', encoding="utf-8")

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])


class TestGetConfigDataRootKeyMissing(unittest.TestCase):
    """get_config: config JSON 中无 data_root 字段时返回 config_exists=False。"""

    def test_data_root_key_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text('{"product_mode": "local_scan"}', encoding="utf-8")

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])


class TestGetConfigSecretsMissing(unittest.TestCase):
    """get_config: config 有效但 secrets 文件不存在时返回 config_exists=False。"""

    def test_secrets_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text('{"data_root": "/some/path"}', encoding="utf-8")

            # secrets 文件不存在
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])


class TestGetConfigSecretsEmpty(unittest.TestCase):
    """get_config: secrets 文件存在但 API Key/HID 为空时返回 config_exists=False。"""

    def test_secrets_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text('{"data_root": "/some/path"}', encoding="utf-8")

            secrets_file = Path(tmp_dir) / "user_secrets.env"
            secrets_file.write_text(
                "QUANTCLASS_API_KEY=\nQUANTCLASS_HID=\n", encoding="utf-8"
            )

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertFalse(result["config_exists"])


class TestRunSetupSuccess(unittest.TestCase):
    """run_setup: 保存成功 + 连通性探测成功。"""

    def test_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            data_dir.mkdir()
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            mock_resp = MagicMock()
            mock_resp.status_code = 200

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic") as mock_save, \
                 patch(f"{_API_MOD}.requests.get", return_value=mock_resp):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(data_dir), "test-key", "test-hid")

            self.assertTrue(result["ok"])
            self.assertNotIn("warning", result)
            mock_save.assert_called_once()


class TestRunSetupDirNotFound(unittest.TestCase):
    """run_setup: 目录不存在且 create_dir=False。"""

    def test_dir_not_found(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_dir = Path(tmp_dir) / "nonexistent"

            from quantclass_sync_internal.gui.api import SyncApi
            api = SyncApi()
            result = api.run_setup(str(missing_dir), "key", "hid", create_dir=False)

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "dir_not_found")
        self.assertIn("resolved_path", result)


class TestRunSetupCreateDir(unittest.TestCase):
    """run_setup: 目录不存在且 create_dir=True 时自动创建。"""

    def test_create_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            new_dir = Path(tmp_dir) / "new_data"
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            mock_resp = MagicMock()
            mock_resp.status_code = 200

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic"), \
                 patch(f"{_API_MOD}.requests.get", return_value=mock_resp):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(new_dir), "key", "hid", create_dir=True)

            self.assertTrue(result["ok"])
            self.assertTrue(new_dir.exists())


class TestRunSetupProbeAuthError(unittest.TestCase):
    """run_setup: 保存成功但凭证验证失败（401/403）。"""

    def test_probe_auth_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            data_dir.mkdir()
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            mock_resp = MagicMock()
            mock_resp.status_code = 403

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic"), \
                 patch(f"{_API_MOD}.requests.get", return_value=mock_resp):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(data_dir), "key", "hid")

        self.assertTrue(result["ok"])
        self.assertIn("warning", result)
        self.assertIn("凭证", result["warning"])


class TestRunSetupProbeOther4xx(unittest.TestCase):
    """run_setup: 保存成功但探测返回其他 4xx（非 401/403），应提示"请求异常"。"""

    def test_probe_404(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            data_dir.mkdir()
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            mock_resp = MagicMock()
            mock_resp.status_code = 404

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic"), \
                 patch(f"{_API_MOD}.requests.get", return_value=mock_resp):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(data_dir), "key", "hid")

        self.assertTrue(result["ok"])
        self.assertIn("warning", result)
        self.assertIn("请求异常", result["warning"])
        self.assertNotIn("凭证", result["warning"])


class TestRunSetupProbeNetworkError(unittest.TestCase):
    """run_setup: 保存成功但网络探测失败。"""

    def test_probe_network_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            data_dir.mkdir()
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic"), \
                 patch(f"{_API_MOD}.requests.get", side_effect=ConnectionError("timeout")):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(data_dir), "key", "hid")

        self.assertTrue(result["ok"])
        self.assertIn("warning", result)
        self.assertIn("网络", result["warning"])


class TestRunSetupSaveFails(unittest.TestCase):
    """run_setup: 配置保存失败。"""

    def test_save_fails(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            data_dir.mkdir()
            config_file = Path(tmp_dir) / "user_config.json"
            secrets_file = Path(tmp_dir) / "user_secrets.env"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.save_setup_artifacts_atomic",
                       side_effect=RuntimeError("write failed")):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.run_setup(str(data_dir), "key", "hid")

        self.assertFalse(result["ok"])
        self.assertIn("保存失败", result["error"])


class TestGetSyncProgressInitial(unittest.TestCase):
    """get_sync_progress 初始状态验证。"""

    def test_get_sync_progress_initial(self):
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        progress = api.get_sync_progress()

        self.assertEqual(progress["status"], "idle")
        self.assertEqual(progress["completed"], 0)
        self.assertEqual(progress["total"], 0)


class TestStartSyncSuccess(unittest.TestCase):
    """start_sync 正常启动，等待后 status 变为 done。

    注意：后台线程在 with patch() 块退出后才运行，所以必须用 patch.start()/stop()
    而不是 with 语法，确保 patch 在线程执行期间依然有效。
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_dir = self.tmpdir.name
        self.config_file = Path(tmp_dir) / "user_config.json"
        self.config_file.write_text("{}", encoding="utf-8")
        self.mock_config = _make_mock_config(tmp_dir)
        self.tmp_dir = tmp_dir

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_start_sync_success(self):
        # 使用 patch.start()/addCleanup 确保 patch 在整个测试方法（含后台线程）期间有效
        def fake_run_update(**kwargs):
            return 0  # EXIT_CODE_SUCCESS

        patches = [
            patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", self.config_file),
            patch(f"{_API_MOD}.load_user_config_or_raise", return_value=self.mock_config),
            patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]),
            patch(f"{_API_MOD}.run_update_with_settings", side_effect=fake_run_update),
            patch(f"{_API_MOD}.resolve_credentials_for_update", return_value=("test-key", "test-hid", "env")),
            patch(f"{_API_MOD}.report_dir_path", return_value=Path(self.tmp_dir) / "log"),
            patch(f"{_API_MOD}.get_latest_run_summary", return_value=None),
            patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
            patch(f"{_API_MOD}.DEFAULT_CATALOG_FILE", Path(self.tmp_dir) / "catalog.json"),
            patch(f"{_API_MOD}.DEFAULT_WORK_DIR", Path(self.tmp_dir) / "work"),
            patch(f"{_API_MOD}.DEFAULT_API_BASE", "https://fake.api"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        result = api.start_sync()

        self.assertTrue(result["started"])

        # 等待后台线程完成（最多 3 秒）
        deadline = time.time() + 3.0
        while time.time() < deadline:
            progress = api.get_sync_progress()
            if progress["status"] in ("done", "error"):
                break
            time.sleep(0.1)

        final = api.get_sync_progress()
        self.assertEqual(final["status"], "done")


class TestStartSyncAlreadyRunning(unittest.TestCase):
    """start_sync 重复启动时返回 started=False。

    通过注入慢任务让同步线程真正运行，验证锁内读-判断-写的原子性。
    """

    def test_start_sync_already_running(self):
        import threading

        # 用 Event 让注入的 _run_sync 阻塞，直到测试主动释放
        hold = threading.Event()

        def slow_sync(*args, **kwargs):
            hold.wait(timeout=5)

        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)
            catalog = ["product-a"]

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=catalog):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()

                # patch.object 替换绑定方法，确保 Thread(target=self._run_sync) 能命中
                with patch.object(api, '_run_sync', slow_sync):
                    # 第一次启动成功
                    result1 = api.start_sync()
                    self.assertTrue(result1["started"])

                    # 第二次启动被锁拒绝
                    result2 = api.start_sync()
                    self.assertFalse(result2["started"])
                    self.assertIn("正在进行", result2["message"])

                    # 释放慢任务线程（daemon=True，即使未 join 也不阻塞进程退出）
                    hold.set()


class TestStartSyncNoCredentials(unittest.TestCase):
    """start_sync 无凭证时，后台线程将 progress 置为 error。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_dir = self.tmpdir.name
        self.config_file = Path(tmp_dir) / "user_config.json"
        self.config_file.write_text("{}", encoding="utf-8")
        self.mock_config = _make_mock_config(tmp_dir)
        self.tmp_dir = tmp_dir

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_start_sync_no_credentials(self):
        # 同样用 patch.start()/addCleanup，确保 patch 在后台线程执行时依然有效
        patches = [
            patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", self.config_file),
            patch(f"{_API_MOD}.load_user_config_or_raise", return_value=self.mock_config),
            patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]),
            patch(f"{_API_MOD}.resolve_credentials_for_update", return_value=("", "", "")),
            patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
            patch(f"{_API_MOD}.DEFAULT_CATALOG_FILE", Path(self.tmp_dir) / "catalog.json"),
            patch(f"{_API_MOD}.DEFAULT_WORK_DIR", Path(self.tmp_dir) / "work"),
            patch(f"{_API_MOD}.DEFAULT_API_BASE", "https://fake.api"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        result = api.start_sync()

        # 启动本身应成功（线程已提交）
        self.assertTrue(result["started"])

        # 等待后台线程因无凭证而报错
        deadline = time.time() + 3.0
        while time.time() < deadline:
            progress = api.get_sync_progress()
            if progress["status"] == "error":
                break
            time.sleep(0.1)

        final = api.get_sync_progress()
        self.assertEqual(final["status"], "error")
        self.assertIn("凭证", final["error_message"])


class TestProgressCallbackUpdatesProgress(unittest.TestCase):
    """run_update_with_settings 内部调用 progress_callback 时，进度字段随之更新。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_dir = self.tmpdir.name
        self.config_file = Path(tmp_dir) / "user_config.json"
        self.config_file.write_text("{}", encoding="utf-8")
        self.mock_config = _make_mock_config(tmp_dir)
        self.tmp_dir = tmp_dir

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_progress_callback_updates_progress(self):
        # fake_run_update 接收 progress_callback 并手动调用它，模拟两个产品完成
        def fake_run_update(progress_callback=None, **kwargs):
            if progress_callback:
                progress_callback("product-a", 1, 2)
                progress_callback("product-b", 2, 2)
            return 0  # EXIT_CODE_SUCCESS

        # 用 patch.start()/addCleanup 保证 patch 在后台线程执行期间持续有效
        patches = [
            patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", self.config_file),
            patch(f"{_API_MOD}.load_user_config_or_raise", return_value=self.mock_config),
            patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a", "product-b"]),
            patch(f"{_API_MOD}.run_update_with_settings", side_effect=fake_run_update),
            patch(f"{_API_MOD}.resolve_credentials_for_update", return_value=("key", "hid", "env")),
            patch(f"{_API_MOD}.report_dir_path", return_value=Path(self.tmp_dir) / "log"),
            patch(f"{_API_MOD}.get_latest_run_summary", return_value=None),
            patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
            patch(f"{_API_MOD}.DEFAULT_CATALOG_FILE", Path(self.tmp_dir) / "catalog.json"),
            patch(f"{_API_MOD}.DEFAULT_WORK_DIR", Path(self.tmp_dir) / "work"),
            patch(f"{_API_MOD}.DEFAULT_API_BASE", "https://fake.api"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        api.start_sync()

        # 等待同步线程完成
        deadline = time.time() + 3.0
        while time.time() < deadline:
            progress = api.get_sync_progress()
            if progress["status"] in ("done", "error"):
                break
            time.sleep(0.1)

        # 进度回调被调用后，completed 应最终为 2
        final = api.get_sync_progress()
        self.assertEqual(final["status"], "done")
        self.assertEqual(final["completed"], 2)
        self.assertEqual(final["total"], 2)


class TestOverviewFieldNames(unittest.TestCase):
    """验证 get_overview 返回给前端的字段名符合规范。"""

    def test_product_field_names(self):
        """产品字段应包含 name/color/local_date/behind_days/last_result/last_error。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]), \
                 patch(f"{_API_MOD}.get_products_overview", return_value=_make_product_rows(1)), \
                 patch(f"{_API_MOD}.get_latest_run_summary", return_value=None), \
                 patch(f"{_API_MOD}.report_dir_path", return_value=Path(tmp_dir) / "log"):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_overview()

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["products"]), 1)
        product = result["products"][0]

        # 验证所有预期字段存在
        expected_product_fields = {"name", "color", "local_date", "behind_days", "last_result", "last_error"}
        self.assertEqual(set(product.keys()), expected_product_fields)

    def test_last_run_field_names(self):
        """last_run 字段应包含 ok/error/skipped/duration_seconds/started_at/failed_products。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]), \
                 patch(f"{_API_MOD}.get_products_overview", return_value=_make_product_rows(1)), \
                 patch(f"{_API_MOD}.get_latest_run_summary", return_value=_make_raw_run()), \
                 patch(f"{_API_MOD}.report_dir_path", return_value=Path(tmp_dir) / "log"):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_overview()

        self.assertIsNotNone(result["last_run"])
        last_run = result["last_run"]

        # 验证所有预期字段存在（含阶段耗时字段）
        expected_last_run_fields = {
            "ok", "error", "skipped", "duration_seconds", "started_at", "failed_products",
            "phase_plan_seconds", "phase_sync_seconds",
        }
        self.assertEqual(set(last_run.keys()), expected_last_run_fields)

        # 验证 failed_products 是对象列表（直接透传，不剥离为纯字符串）
        self.assertIsInstance(last_run["failed_products"], list)
        self.assertEqual(len(last_run["failed_products"]), 1)
        fp = last_run["failed_products"][0]
        self.assertEqual(fp["product"], "product-b")
        self.assertEqual(fp["error"], "HTTP 403")
        self.assertEqual(fp["reason_code"], "auth_error")


class TestSyncNonZeroExitCode(unittest.TestCase):
    """run_update_with_settings 返回非零退出码时，progress.status 应为 error。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_dir = self.tmpdir.name
        self.config_file = Path(tmp_dir) / "user_config.json"
        self.config_file.write_text("{}", encoding="utf-8")
        self.mock_config = _make_mock_config(tmp_dir)
        self.tmp_dir = tmp_dir

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_nonzero_exit_code_sets_error_status(self):
        def fake_run_update(progress_callback=None, **kwargs):
            return 1  # EXIT_CODE_GENERAL_FAILURE

        patches = [
            patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", self.config_file),
            patch(f"{_API_MOD}.load_user_config_or_raise", return_value=self.mock_config),
            patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]),
            patch(f"{_API_MOD}.run_update_with_settings", side_effect=fake_run_update),
            patch(f"{_API_MOD}.resolve_credentials_for_update", return_value=("key", "hid", "env")),
            patch(f"{_API_MOD}.report_dir_path", return_value=Path(self.tmp_dir) / "log"),
            patch(f"{_API_MOD}.get_latest_run_summary", return_value=None),
            patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
            patch(f"{_API_MOD}.DEFAULT_CATALOG_FILE", Path(self.tmp_dir) / "catalog.json"),
            patch(f"{_API_MOD}.DEFAULT_WORK_DIR", Path(self.tmp_dir) / "work"),
            patch(f"{_API_MOD}.DEFAULT_API_BASE", "https://fake.api"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        api.start_sync()

        # 等待后台线程完成
        deadline = time.time() + 3.0
        while time.time() < deadline:
            progress = api.get_sync_progress()
            if progress["status"] in ("done", "error"):
                break
            time.sleep(0.1)

        final = api.get_sync_progress()
        self.assertEqual(final["status"], "error")
        self.assertIn("未成功完成", final["error_message"])


class TestGetHistory(unittest.TestCase):
    """get_history：返回历史运行列表。"""

    def test_get_history_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)
            mock_runs = [
                {
                    "run_id": "run-20260313",
                    "started_at": "2026-03-13T12:00:00Z",
                    "duration_seconds": 5.0,
                    "success_total": 10,
                    "failed_total": 1,
                    "skipped_total": 0,
                    "report_file": "/tmp/log/run_report_20260313.json",
                },
            ]

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]), \
                 patch(f"{_API_MOD}.report_dir_path", return_value=Path(tmp_dir) / "log"), \
                 patch(f"{_API_MOD}.get_run_history", return_value=mock_runs):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_history()

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["runs"]), 1)
        self.assertEqual(result["runs"][0]["run_id"], "run-20260313")

    def test_get_history_no_config(self):
        """配置缺失时返回 ok=False。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            # 不创建配置文件

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_history()

        self.assertFalse(result["ok"])
        self.assertIn("未找到", result["error"])


class TestGetRunDetail(unittest.TestCase):
    """get_run_detail：返回运行报告产品明细。"""

    def test_get_run_detail_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)
            mock_detail = {
                "ok": True,
                "started_at": "2026-03-13T12:00:00Z",
                "duration_seconds": 45.0,
                "success_total": 2,
                "failed_total": 1,
                "skipped_total": 0,
                "products": [
                    {"product": "p-err", "status": "error", "elapsed_seconds": 5, "error": "HTTP 403"},
                    {"product": "p-ok", "status": "ok", "elapsed_seconds": 10, "error": ""},
                ],
            }

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]), \
                 patch(f"{_API_MOD}.report_dir_path", return_value=Path(tmp_dir) / "log"), \
                 patch(f"{_API_MOD}.get_run_detail", return_value=mock_detail) as mock_fn:

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_run_detail("/tmp/log/report.json")

                # 验证底层函数被正确调用（log_dir + report_file 透传）
                mock_fn.assert_called_once_with(Path(tmp_dir) / "log", "/tmp/log/report.json")

        self.assertTrue(result["ok"])
        self.assertEqual(result["success_total"], 2)
        self.assertEqual(len(result["products"]), 2)

    def test_get_run_detail_no_config(self):
        """配置缺失时返回 ok=False。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_run_detail("/tmp/log/report.json")

        self.assertFalse(result["ok"])


class TestStartHealthCheck(unittest.TestCase):
    """start_health_check 异步启动测试。"""

    def test_normal_start_returns_ok(self):
        """正常启动返回 ok=True，后台线程完成后 result 可读。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)

            mock_health = {
                "issues": [],
                "summary": {"missing_data": 0, "csv_unreadable": 0, "orphan_temp": 0, "total": 0},
                "scanned_products": 2,
                "elapsed_seconds": 0.01,
            }

            patches = [
                patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file),
                patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config),
                patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["p1", "p2"]),
                patch(f"{_API_MOD}.check_data_health", return_value=mock_health),
            ]
            for p in patches:
                p.start()
                self.addCleanup(p.stop)

            from quantclass_sync_internal.gui.api import SyncApi
            api = SyncApi()
            result = api.start_health_check()
            self.assertTrue(result["ok"])

            # 等待后台线程完成（最多 3 秒）
            import time
            deadline = time.time() + 3.0
            while time.time() < deadline:
                progress = api.get_health_progress()
                if not progress["checking"]:
                    break
                time.sleep(0.05)

            final = api.get_health_result()
            self.assertIsNotNone(final)
            self.assertTrue(final["ok"])
            self.assertIn("health", final)

    def test_during_sync_returns_error(self):
        """同步进行中时拒绝启动健康检查。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        # 手动将 _progress 置为 syncing 状态
        api._progress["status"] = "syncing"
        result = api.start_health_check()
        self.assertFalse(result["ok"])
        self.assertIn("同步进行中", result["error"])

    def test_duplicate_start_returns_error(self):
        """重复启动时返回错误。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        # 手动将 _health_progress["checking"] 置为 True，模拟已在检查中
        api._health_progress["checking"] = True
        result = api.start_health_check()
        self.assertFalse(result["ok"])
        self.assertIn("进行中", result["error"])


class TestGetHealthProgress(unittest.TestCase):
    """get_health_progress 轮询测试。"""

    def test_returns_correct_fields(self):
        """返回字段集合和类型正确。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        progress = api.get_health_progress()
        self.assertIn("checking", progress)
        self.assertIn("current", progress)
        self.assertIn("total", progress)
        self.assertIn("product", progress)
        # 初始值验证
        self.assertFalse(progress["checking"])
        self.assertEqual(progress["current"], 0)
        self.assertEqual(progress["total"], 0)
        self.assertEqual(progress["product"], "")

    def test_initial_result_is_none(self):
        """初始状态下 get_health_result 返回 None。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        self.assertIsNone(api.get_health_result())


class TestRepairHealthIssues(unittest.TestCase):
    """repair_health_issues 修复测试。"""

    def test_no_result_returns_error(self):
        """无可用检查结果时返回 ok=False。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        result = api.repair_health_issues()
        self.assertFalse(result["ok"])
        self.assertIn("无可用检查结果", result["error"])

    def test_failed_result_returns_error(self):
        """检查结果为 ok=False 时返回 ok=False。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        api._health_progress["result"] = {"ok": False, "error": "配置缺失"}
        result = api.repair_health_issues()
        self.assertFalse(result["ok"])

    def test_no_repairable_issues_returns_error(self):
        """无可修复问题时返回 ok=False。"""
        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()
        # 结果中全为不可修复问题
        api._health_progress["result"] = {
            "ok": True,
            "health": {
                "issues": [{"type": "csv_unreadable", "repairable": False}],
            },
        }
        result = api.repair_health_issues()
        self.assertFalse(result["ok"])
        self.assertIn("无可修复问题", result["error"])

    def test_repairable_issues_calls_repair(self):
        """有可修复问题时调用 repair_data_issues 并返回结果。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)

            mock_repair = {"repaired": 1, "failed": 0}

            # 用 patch.start() 逐个激活，repair_patcher 单独保存以拿到 mock 对象
            p1 = patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file)
            p2 = patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config)
            p3 = patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["p1"])
            p4 = patch(f"{_API_MOD}.repair_data_issues", return_value=mock_repair)
            for p in (p1, p2, p3, p4):
                p.start()
                self.addCleanup(p.stop)

            from quantclass_sync_internal.gui.api import SyncApi
            api = SyncApi()
            api._health_progress["result"] = {
                "ok": True,
                "health": {
                    "issues": [{"type": "orphan_temp", "path": "/tmp/x.tmp", "repairable": True}],
                },
            }
            result = api.repair_health_issues()

        self.assertTrue(result["ok"])
        self.assertEqual(result["repair"], mock_repair)


class TestStartSyncRetryFailed(unittest.TestCase):
    """start_sync(retry_failed=True) 从上次 run_summary 提取失败产品并启动同步。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        tmp_dir = self.tmpdir.name
        self.config_file = Path(tmp_dir) / "user_config.json"
        self.config_file.write_text("{}", encoding="utf-8")
        self.mock_config = _make_mock_config(tmp_dir)
        self.tmp_dir = tmp_dir

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_retry_failed_extracts_products(self):
        """run_update_with_settings 应收到失败产品名列表。"""
        received_products = {}

        def fake_run_update(products=None, **kwargs):
            # 记录传入的 products 参数，供断言使用
            received_products["value"] = products
            return 0  # EXIT_CODE_SUCCESS

        patches = [
            patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", self.config_file),
            patch(f"{_API_MOD}.load_user_config_or_raise", return_value=self.mock_config),
            patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a", "product-b"]),
            patch(f"{_API_MOD}.run_update_with_settings", side_effect=fake_run_update),
            patch(f"{_API_MOD}.resolve_credentials_for_update", return_value=("key", "hid", "env")),
            patch(f"{_API_MOD}.report_dir_path", return_value=Path(self.tmp_dir) / "log"),
            patch(f"{_API_MOD}.get_latest_run_summary", return_value=None),
            patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
            patch(f"{_API_MOD}.DEFAULT_CATALOG_FILE", Path(self.tmp_dir) / "catalog.json"),
            patch(f"{_API_MOD}.DEFAULT_WORK_DIR", Path(self.tmp_dir) / "work"),
            patch(f"{_API_MOD}.DEFAULT_API_BASE", "https://fake.api"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        from quantclass_sync_internal.gui.api import SyncApi
        api = SyncApi()

        # 预置 run_summary，包含一个失败产品
        with api._lock:
            api._progress["run_summary"] = {
                "failed_products": [
                    {"product": "product-b", "error": "HTTP 403", "reason_code": "auth_error"}
                ]
            }

        result = api.start_sync(retry_failed=True)
        self.assertTrue(result["started"])

        # 等待后台线程完成
        deadline = time.time() + 3.0
        while time.time() < deadline:
            progress = api.get_sync_progress()
            if progress["status"] in ("done", "error"):
                break
            time.sleep(0.1)

        # 验证 run_update_with_settings 收到了正确的产品列表
        self.assertEqual(received_products.get("value"), ["product-b"])


class TestStartSyncRetryFailedNoFailed(unittest.TestCase):
    """start_sync(retry_failed=True) 但 run_summary 中无失败产品时返回错误。"""

    def test_no_failed_products(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()

                # run_summary 存在但 failed_products 为空
                with api._lock:
                    api._progress["run_summary"] = {"failed_products": []}

                result = api.start_sync(retry_failed=True)

        self.assertFalse(result["started"])
        self.assertEqual(result["message"], "没有失败产品")


class TestStartSyncRetryFailedNoSummary(unittest.TestCase):
    """start_sync(retry_failed=True) 但无 run_summary 时返回错误。"""

    def test_no_run_summary(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                # _progress["run_summary"] 默认为 None，不做额外设置

                result = api.start_sync(retry_failed=True)

        self.assertFalse(result["started"])
        self.assertEqual(result["message"], "没有上次同步记录")


class TestOpenDataDir(unittest.TestCase):
    """open_data_dir 测试。"""

    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    def test_open_data_dir_macos(self, mock_run):
        """macOS 下正常打开已存在目录，返回 ok=True。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.resolve_path_from_config", return_value=Path(tmp_dir)), \
                 patch(f"{_API_MOD}.sys.platform", "darwin"):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.open_data_dir()

        self.assertTrue(result["ok"])
        # subprocess.run 应以 ["open", "--", path] 被调用
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        self.assertEqual(args[0], "open")
        self.assertEqual(args[1], "--")

    def test_open_data_dir_not_macos(self):
        """非 macOS 平台返回 ok=False。"""
        with patch(f"{_API_MOD}.sys.platform", "linux"):
            from quantclass_sync_internal.gui.api import SyncApi
            api = SyncApi()
            result = api.open_data_dir()

        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_open_data_dir_not_exist(self):
        """data_root 目录不存在时返回 ok=False。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")
            mock_config = _make_mock_config(tmp_dir)
            # 指向一个不存在的子目录
            nonexistent = Path(tmp_dir) / "no_such_dir"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.resolve_path_from_config", return_value=nonexistent), \
                 patch(f"{_API_MOD}.sys.platform", "darwin"):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.open_data_dir()

        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_get_config_includes_can_open_dir(self):
        """get_config 返回值包含 can_open_dir 字段。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text(
                '{"data_root": "' + tmp_dir + '"}', encoding="utf-8"
            )
            secrets_file = Path(tmp_dir) / "secrets.env"
            secrets_file.write_text("", encoding="utf-8")

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.DEFAULT_USER_SECRETS_FILE", secrets_file), \
                 patch(f"{_API_MOD}.load_secrets_from_file", return_value=("key", "hid")), \
                 patch(f"{_API_MOD}.resolve_path_from_config", return_value=Path(tmp_dir)), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["product-a"]):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertIn("can_open_dir", result)
        # can_open_dir 必须为 bool 类型
        self.assertIsInstance(result["can_open_dir"], bool)


if __name__ == "__main__":
    unittest.main()
