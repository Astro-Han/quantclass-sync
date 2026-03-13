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
    """get_config 正常路径：返回配置概要。"""

    def test_get_config_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}", encoding="utf-8")

            mock_config = _make_mock_config(tmp_dir)
            catalog = ["product-a", "product-b"]

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=catalog):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_config()

        self.assertTrue(result["ok"])
        self.assertTrue(result["config_exists"])
        # data_root 字段应为字符串且非空
        self.assertIsInstance(result["data_root"], str)
        self.assertTrue(len(result["data_root"]) > 0)
        # product_count 与 catalog 长度一致
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
            patch(f"{_API_MOD}.DEFAULT_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
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
            patch(f"{_API_MOD}.DEFAULT_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
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
            patch(f"{_API_MOD}.DEFAULT_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
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

        # 验证所有预期字段存在
        expected_last_run_fields = {
            "ok", "error", "skipped", "duration_seconds", "started_at", "failed_products"
        }
        self.assertEqual(set(last_run.keys()), expected_last_run_fields)

        # 验证 failed_products 是字符串列表
        self.assertIsInstance(last_run["failed_products"], list)
        self.assertEqual(last_run["failed_products"], ["product-b"])


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
            patch(f"{_API_MOD}.DEFAULT_SECRETS_FILE", Path(self.tmp_dir) / "secrets.env"),
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


class TestGetHealthReport(unittest.TestCase):
    """get_health_report API 测试。"""

    def test_normal_path(self):
        """正常路径：返回 ok=True 和 health 字段。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}")
            mock_config = _make_mock_config(tmp_dir)

            mock_health = {
                "issues": [],
                "summary": {"missing_data": 0, "csv_unreadable": 0, "orphan_temp": 0, "total": 0},
                "scanned_products": 5,
                "elapsed_seconds": 0.1,
            }

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["p1", "p2"]), \
                 patch(f"{_API_MOD}.check_data_health", return_value=mock_health) as mock_fn:

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_health_report()

                mock_fn.assert_called_once()

        self.assertTrue(result["ok"])
        self.assertIn("health", result)
        self.assertEqual(result["health"]["summary"]["total"], 0)

    def test_config_missing(self):
        """配置文件缺失时返回 ok=False。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file):
                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_health_report()

        self.assertFalse(result["ok"])
        self.assertIn("error", result)

    def test_check_raises_exception(self):
        """check_data_health 抛异常时返回 ok=False。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_file = Path(tmp_dir) / "user_config.json"
            config_file.write_text("{}")
            mock_config = _make_mock_config(tmp_dir)

            with patch(f"{_API_MOD}.DEFAULT_USER_CONFIG_FILE", config_file), \
                 patch(f"{_API_MOD}.load_user_config_or_raise", return_value=mock_config), \
                 patch(f"{_API_MOD}.load_catalog_or_raise", return_value=["p1"]), \
                 patch(f"{_API_MOD}.check_data_health", side_effect=RuntimeError("boom")):

                from quantclass_sync_internal.gui.api import SyncApi
                api = SyncApi()
                result = api.get_health_report()

        self.assertFalse(result["ok"])
        self.assertIn("boom", result["error"])


if __name__ == "__main__":
    unittest.main()
