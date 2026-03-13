"""S 层点火测试：验证程序能启动，核心入口不崩溃。

分三组：
  S1 — CLI 点火（subprocess + CliRunner）
  S2 — GUI 点火（模块导入 + 文件完整性）
  S3 — 模块导入链（浅导入所有子模块）
"""

from __future__ import annotations

import importlib
import inspect
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# 项目根目录（tests/ 的上一级）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# CLI 入口脚本
CLI_ENTRY = str(PROJECT_ROOT / "quantclass_sync.py")


class TestS1CLISmoke(unittest.TestCase):
    """S1: CLI 命令行点火测试。"""

    def _run(self, args, cwd=None, timeout=30, env=None):
        """辅助：运行 subprocess，返回 CompletedProcess。"""
        return subprocess.run(
            [sys.executable, CLI_ENTRY] + args,
            capture_output=True,
            text=True,
            cwd=str(cwd or PROJECT_ROOT),
            timeout=timeout,
            env=env,
        )

    # ------------------------------------------------------------------
    # S1.1: --help
    # ------------------------------------------------------------------

    def test_s1_1_help(self):
        """S1.1: --help 退出码 0，stdout 包含核心子命令名。"""
        result = self._run(["--help"])
        self.assertEqual(result.returncode, 0, f"stderr: {result.stderr[:300]}")
        for cmd in ("setup", "update", "status", "gui"):
            self.assertIn(cmd, result.stdout, f"--help 缺少子命令: {cmd}")

    # ------------------------------------------------------------------
    # S1.2: status（无效配置文件）
    # ------------------------------------------------------------------

    def test_s1_2_status_invalid_config(self):
        """S1.2: status 收到无效配置文件时退出码非 0，不打出 Python traceback。

        传一个存在但内容损坏的 config-file，使 load_user_config_or_raise
        抛 pydantic 验证错误，command_guard 捕获后 exit(1)。
        验证的核心是：不暴露原始 traceback 给用户。
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # 创建存在但内容无效的配置文件（空 JSON 缺少必填字段）
            config_path = Path(tmpdir) / "user_config.json"
            config_path.write_text("{}", encoding="utf-8")

            result = self._run(
                ["status", "--config-file", str(config_path)],
                cwd=PROJECT_ROOT,  # 保证 catalog.txt 能被找到
            )
            combined = result.stdout + result.stderr
            self.assertNotEqual(result.returncode, 0, "预期退出码非 0")
            self.assertNotIn(
                "Traceback (most recent call last)",
                combined,
                "不应暴露 Python traceback",
            )

    # ------------------------------------------------------------------
    # S1.3: update --dry-run（in-process + mock HTTP 层）
    # ------------------------------------------------------------------

    def test_s1_3_update_dry_run(self):
        """S1.3: update --dry-run 正常退出码 0（mock run_update_with_settings）。

        用 typer.testing.CliRunner 在当前进程内调用，以便用 mock 绕过 HTTP。
        配置文件和 secrets 文件均写入临时目录，满足 cmd_update 的前置检查。
        """
        from typer.testing import CliRunner
        from quantclass_sync_internal.cli import app

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            data_dir = tmp / "data"
            data_dir.mkdir()

            # 最小 secrets 文件，满足 resolve_credentials_for_update
            secrets_path = tmp / "user_secrets.env"
            secrets_path.write_text(
                "QUANTCLASS_API_KEY=test-key-12345\n"
                "QUANTCLASS_HID=test-hid-12345\n",
                encoding="utf-8",
            )

            # 最小 user_config.json，所有字段使用绝对路径避免歧义
            config_path = tmp / "user_config.json"
            config_path.write_text(
                json.dumps({
                    "data_root": str(data_dir),
                    "product_mode": "local_scan",
                    "default_products": [],
                    "secrets_file": str(secrets_path),
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                }),
                encoding="utf-8",
            )

            runner = CliRunner()
            # mock 掉 run_update_with_settings（使用处路径），直接返回 0
            with patch(
                "quantclass_sync_internal.cli.run_update_with_settings",
                return_value=0,
            ):
                result = runner.invoke(
                    app,
                    [
                        "--config-file", str(config_path),
                        "--data-root", str(data_dir),
                        "update",
                        "--dry-run",
                    ],
                )

            self.assertEqual(
                result.exit_code,
                0,
                f"退出码非 0，输出: {result.output}",
            )

    # ------------------------------------------------------------------
    # S1.4: setup --non-interactive（缺必填参数）
    # ------------------------------------------------------------------

    def test_s1_4_setup_non_interactive_missing_params(self):
        """S1.4: setup --non-interactive 不传 --data-root 时退出码非 0，不打 traceback。

        cmd_setup 内部检查 raw_data_root 为空时抛 RuntimeError，
        command_guard 捕获后 exit(1)。
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._run(
                [
                    "setup",
                    "--non-interactive",
                    "--config-file", str(Path(tmpdir) / "user_config.json"),
                    "--skip-check",
                ],
                cwd=PROJECT_ROOT,
            )
            combined = result.stdout + result.stderr
            self.assertNotEqual(result.returncode, 0, "预期退出码非 0")
            self.assertNotIn(
                "Traceback (most recent call last)",
                combined,
                "不应暴露 Python traceback",
            )

    # ------------------------------------------------------------------
    # S1.5: 无子命令 + 无配置文件（非 tty 环境）
    # ------------------------------------------------------------------

    def test_s1_5_no_subcommand_no_config(self):
        """S1.5: 非 tty 环境下无子命令且无配置文件，输出 setup 引导提示，不抛异常。

        subprocess 的 stdin 是 pipe（非 tty），global_options 会走
        "非交互模式 + 无配置" 分支，输出引导信息后 exit(1)。
        --config-file 指向临时目录下不存在的文件，确保 config_file.exists() = False。
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = Path(tmpdir) / "nonexistent_config.json"
            result = self._run(
                ["--config-file", str(nonexistent)],
                cwd=PROJECT_ROOT,
            )
            combined = result.stdout + result.stderr
            self.assertNotIn(
                "Traceback (most recent call last)",
                combined,
                "不应暴露 Python traceback",
            )
            # 非 tty 下应提示用户执行 setup
            self.assertIn(
                "setup",
                combined,
                f"未找到 setup 引导提示，输出: {combined[:500]}",
            )

    # ------------------------------------------------------------------
    # S1.6: gui 子命令（mock webview）
    # ------------------------------------------------------------------

    def test_s1_6_gui_mock_webview(self):
        """S1.6: launch_gui() 在 mock webview 下正常执行，不抛异常。

        launch_gui 内部 `import webview` 是运行时执行，
        patch sys.modules 即可在不安装 pywebview 的环境下测试。
        """
        mock_webview = MagicMock()
        with patch.dict("sys.modules", {"webview": mock_webview}):
            # 在 mock 生效后再调用，确保函数内 import webview 拿到 mock
            from quantclass_sync_internal.gui import launch_gui
            launch_gui()

        # 验证窗口创建和启动均被调用
        mock_webview.create_window.assert_called_once()
        mock_webview.start.assert_called_once()


class TestS2GUISmoke(unittest.TestCase):
    """S2: GUI 点火测试（模块可导入 + 文件完整性）。"""

    # ------------------------------------------------------------------
    # S2.1: SyncApi 可导入
    # ------------------------------------------------------------------

    def test_s2_1_syncapi_importable(self):
        """S2.1: 从 gui.api 导入 SyncApi 类不抛异常。"""
        from quantclass_sync_internal.gui.api import SyncApi
        self.assertTrue(callable(SyncApi), "SyncApi 应为可调用类")

    # ------------------------------------------------------------------
    # S2.2: SyncApi 方法签名完整
    # ------------------------------------------------------------------

    def test_s2_2_syncapi_methods(self):
        """S2.2: SyncApi 公开方法与已知集合一致（多或少都报错）。

        已知方法集合即当前合同，新增方法时需同步更新此集合。
        """
        from quantclass_sync_internal.gui.api import SyncApi

        # 已知必须存在的公开方法（不含 _ 前缀）
        expected = {
            "get_overview",
            "get_config",
            "start_sync",
            "get_sync_progress",
            "get_history",
            "get_run_detail",
            "get_health_report",
        }

        # 反射获取实际公开方法
        actual = {
            name
            for name, _ in inspect.getmembers(SyncApi, predicate=inspect.isfunction)
            if not name.startswith("_")
        }

        missing = expected - actual
        self.assertFalse(missing, f"SyncApi 缺少方法: {missing}")

        extra = actual - expected
        self.assertFalse(
            extra,
            f"SyncApi 新增了方法 {extra}，请将其加入 S2.2 的 expected 集合",
        )

    # ------------------------------------------------------------------
    # S2.3: 前端文件完整
    # ------------------------------------------------------------------

    def test_s2_3_frontend_assets_exist(self):
        """S2.3: assets 目录下四个核心前端文件均存在。"""
        assets_dir = PROJECT_ROOT / "quantclass_sync_internal" / "gui" / "assets"
        for fname in ("index.html", "app.js", "style.css", "alpine.min.js"):
            self.assertTrue(
                (assets_dir / fname).is_file(),
                f"缺少前端文件: {assets_dir / fname}",
            )

    # ------------------------------------------------------------------
    # S2.4: index.html 引用与 assets 目录一致
    # ------------------------------------------------------------------

    def test_s2_4_html_references_consistent(self):
        """S2.4: index.html 中 src/href 引用的本地文件均存在于 assets 目录。

        解析 HTML 中 src="..." 和 href="..." 属性，
        排除以 http:// / https:// / // 开头的外部链接，
        剩余引用须与 assets 目录实际文件相互覆盖。
        """
        assets_dir = PROJECT_ROOT / "quantclass_sync_internal" / "gui" / "assets"
        html_content = (assets_dir / "index.html").read_text(encoding="utf-8")

        # 提取本地引用（排除外部 URL）
        pattern = re.compile(r'(?:src|href)=["\']([^"\']+)["\']')
        references = set()
        for match in pattern.finditer(html_content):
            ref = match.group(1)
            if not ref.startswith(("http://", "https://", "//")):
                references.add(ref)

        # assets 目录中的非 HTML 文件（不包含 index.html 自身）
        actual_files = {
            f.name
            for f in assets_dir.iterdir()
            if f.is_file() and f.name != "index.html"
        }

        # 每个 HTML 引用必须对应实际文件
        for ref in references:
            self.assertIn(
                ref,
                actual_files,
                f"index.html 引用了不存在的文件: {ref}",
            )

        # 每个资源文件必须被 HTML 引用（无孤立文件）
        unreferenced = actual_files - references
        self.assertFalse(
            unreferenced,
            f"以下 assets 文件未被 index.html 引用: {unreferenced}",
        )


class TestS3ImportChain(unittest.TestCase):
    """S3: 模块导入链测试（浅导入，不实例化对象）。"""

    # ------------------------------------------------------------------
    # S3.1: quantclass_sync 顶层入口
    # ------------------------------------------------------------------

    def test_s3_1_quantclass_sync_importable(self):
        """S3.1: import quantclass_sync 不抛异常，__all__ 中的符号均可访问。"""
        import quantclass_sync
        for name in quantclass_sync.__all__:
            self.assertTrue(
                hasattr(quantclass_sync, name),
                f"quantclass_sync.__all__ 中的 {name} 不可访问",
            )

    # ------------------------------------------------------------------
    # S3.2: quantclass_sync_internal 所有子模块
    # ------------------------------------------------------------------

    def test_s3_2_internal_modules_importable(self):
        """S3.2: quantclass_sync_internal 各子模块可导入（含 gui）。

        gui 顶层（__init__.py）无 webview import，无需 mock；
        gui.api 同样只在运行时 import webview（函数内），导入本身无副作用。
        """
        submodules = [
            "quantclass_sync_internal.models",
            "quantclass_sync_internal.constants",
            "quantclass_sync_internal.orchestrator",
            "quantclass_sync_internal.http_client",
            "quantclass_sync_internal.file_sync",
            "quantclass_sync_internal.status_store",
            "quantclass_sync_internal.config",
            "quantclass_sync_internal.reporting",
            "quantclass_sync_internal.cli",
            "quantclass_sync_internal.data_query",
            "quantclass_sync_internal.archive",
            "quantclass_sync_internal.csv_engine",
            "quantclass_sync_internal.gui",
            "quantclass_sync_internal.gui.api",
        ]
        for mod_name in submodules:
            with self.subTest(module=mod_name):
                try:
                    importlib.import_module(mod_name)
                except Exception as exc:
                    self.fail(f"导入 {mod_name} 失败: {exc}")

    # ------------------------------------------------------------------
    # S3.3: coin_preprocess_internal 所有子模块
    # ------------------------------------------------------------------

    def test_s3_3_coin_preprocess_importable(self):
        """S3.3: coin_preprocess_internal 各子模块可导入。"""
        submodules = [
            "coin_preprocess_internal",
            "coin_preprocess_internal.runner",
            "coin_preprocess_internal.csv_source",
            "coin_preprocess_internal.symbol_mapper",
            "coin_preprocess_internal.pivot",
            "coin_preprocess_internal.constants",
        ]
        for mod_name in submodules:
            with self.subTest(module=mod_name):
                try:
                    importlib.import_module(mod_name)
                except Exception as exc:
                    self.fail(f"导入 {mod_name} 失败: {exc}")


if __name__ == "__main__":
    unittest.main()
