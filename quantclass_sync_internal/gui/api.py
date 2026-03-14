"""职责：GUI 的 Python API 层，作为 pywebview 的 js_api 挂载。

所有公开方法均返回 dict/list，前端通过 await window.pywebview.api.xxx() 调用。
"""

from __future__ import annotations

import json
import os
import requests
import secrets as secrets_mod
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ..config import (
    load_secrets_from_file,
    load_user_config_or_raise,
    resolve_credentials_for_update,
    save_setup_artifacts_atomic,
)
from ..constants import (
    DEFAULT_API_BASE,
    DEFAULT_CATALOG_FILE,
    DEFAULT_USER_CONFIG_FILE,
    DEFAULT_USER_SECRETS_FILE,
    DEFAULT_WORK_DIR,
    EXIT_CODE_SUCCESS,
    PRODUCT_MODE_EXPLICIT_LIST,
)
from ..data_query import (
    check_data_health,
    get_latest_run_summary,
    get_products_overview,
    get_run_detail,
    get_run_history,
)
from ..models import CommandContext, UserConfig, log_error, log_info
from ..orchestrator import load_catalog_or_raise, run_update_with_settings
from ..status_store import report_dir_path


def _new_run_id() -> str:
    """生成高冲突安全的 run_id（微秒时间戳 + pid + 短随机后缀）。"""
    now = datetime.now()
    return f"{now.strftime('%Y%m%d-%H%M%S-%f')}-p{os.getpid()}-{secrets_mod.token_hex(4)}"


# _progress 的初始结构，每次 start_sync 前重置为此形态
_PROGRESS_INIT: Dict[str, Any] = {
    "status": "idle",          # idle / syncing / done / error
    "current_product": "",     # 最近完成的产品名
    "completed": 0,            # 已完成产品数
    "total": 0,                # 本次同步产品总数
    "elapsed_seconds": 0,      # 已用时（秒）
    "error_message": "",       # 出错时的错误信息
    "run_summary": None,       # 同步完成后填充 run_summary dict
}


class SyncApi:
    """pywebview js_api 类。

    线程安全：所有对 _progress 的读写均通过 _lock 保护。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 深拷贝初始值，避免多次运行时共享同一个 dict 引用
        self._progress: Dict[str, Any] = dict(_PROGRESS_INIT)

    # ------------------------------------------------------------------
    # 内部辅助方法
    # ------------------------------------------------------------------

    def _resolve_config(self) -> Tuple[Optional[object], Optional[Path], Optional[list], Optional[str]]:
        """读取 user_config、解析 data_root、加载 catalog。

        返回 (user_config, data_root, catalog, error_message)。
        出错时 user_config/data_root/catalog 均为 None，error_message 为描述字符串。
        """
        config_file = DEFAULT_USER_CONFIG_FILE.resolve()

        # 检查配置文件是否存在
        if not config_file.exists():
            return None, None, None, (
                f"未找到用户配置文件：{config_file}；请先执行 setup 命令完成初始化。"
            )

        # 加载用户配置
        try:
            user_config = load_user_config_or_raise(config_file)
        except Exception as exc:
            return None, None, None, f"用户配置读取失败：{exc}"

        # 解析 data_root（相对路径按配置文件目录展开）
        try:
            raw_root = Path(user_config.data_root)
            expanded = raw_root.expanduser()
            if expanded.is_absolute():
                data_root = expanded.resolve()
            else:
                # 相对路径相对于配置文件所在目录
                data_root = (config_file.parent / expanded).resolve()
        except Exception as exc:
            return None, None, None, f"data_root 路径解析失败：{exc}"

        # 加载产品 catalog
        try:
            catalog = load_catalog_or_raise(DEFAULT_CATALOG_FILE.resolve())
        except Exception as exc:
            return None, None, None, f"产品清单加载失败：{exc}"

        return user_config, data_root, catalog, None

    def _update_progress(self, **kwargs: Any) -> None:
        """线程安全地更新 _progress 字段。"""
        with self._lock:
            self._progress.update(kwargs)

    # ------------------------------------------------------------------
    # 公开 API 方法（供 JS 调用）
    # ------------------------------------------------------------------

    def get_overview(self) -> Dict[str, Any]:
        """返回产品状态总览和最近一次运行摘要。

        返回结构：
        {
            "ok": bool,
            "error": str,           # 出错时才有
            "products": [...],      # 每产品状态列表
            "summary": {            # 颜色统计
                "green": N, "yellow": N, "red": N, "gray": N
            },
            "data_root": str,
            "last_run": dict or None,
        }
        """
        user_config, data_root, catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}

        # 获取产品状态列表
        try:
            raw_products = get_products_overview(data_root, catalog)
        except Exception as exc:
            return {"ok": False, "error": f"产品状态读取失败：{exc}"}

        # 转换为前端友好的字段名
        products = []
        for p in raw_products:
            products.append({
                "name": p["name"],
                "color": p["status_color"],
                "local_date": p["local_date"],
                "behind_days": p["days_behind"],
                "last_result": p["last_status"],
                "last_error": p["last_error"],
            })

        # 颜色统计
        summary = {"green": 0, "yellow": 0, "red": 0, "gray": 0}
        for p in products:
            color = p.get("color", "gray")
            if color in summary:
                summary[color] += 1

        # 最近运行摘要（转换为前端友好格式）
        try:
            log_dir = report_dir_path(data_root)
            raw_run = get_latest_run_summary(log_dir)
        except Exception:
            raw_run = None

        last_run = None
        if raw_run:
            last_run = {
                "ok": raw_run.get("success_total", 0),
                "error": raw_run.get("failed_total", 0),
                "skipped": raw_run.get("skipped_total", 0),
                "duration_seconds": raw_run.get("duration_seconds", 0),
                "started_at": raw_run.get("started_at", ""),
                "failed_products": [
                    fp.get("product", "") for fp in raw_run.get("failed_products", [])
                ],
            }

        return {
            "ok": True,
            "products": products,
            "summary": summary,
            "data_root": str(data_root),
            "last_run": last_run,
        }

    def get_config(self) -> Dict[str, Any]:
        """返回当前配置状态（用于前端判断是否展示 setup 向导）。

        独立检测 config + secrets 文件，不依赖 _resolve_config()。
        任一检测失败均返回 config_exists=False，触发向导。

        返回结构：
        {
            "ok": bool,
            "config_exists": bool,  # 配置和凭证是否完整有效
            "data_root": str,       # config_exists=True 时有值
            "product_count": int,
        }
        """
        _not_ready = {"ok": True, "config_exists": False, "data_root": "", "product_count": 0}
        config_file = DEFAULT_USER_CONFIG_FILE.resolve()

        # --- 1. 检测 user_config.json ---
        if not config_file.exists():
            return _not_ready

        try:
            raw = json.loads(config_file.read_text(encoding="utf-8"))
        except Exception:
            return _not_ready

        if not isinstance(raw, dict):
            return _not_ready

        # 必需字段：data_root 非空字符串
        data_root_raw = raw.get("data_root", "")
        if not isinstance(data_root_raw, str) or not data_root_raw.strip():
            return _not_ready

        # --- 2. 检测 user_secrets.env（固定默认路径） ---
        secrets_file = DEFAULT_USER_SECRETS_FILE.resolve()
        if not secrets_file.exists():
            return _not_ready

        try:
            api_key, hid = load_secrets_from_file(secrets_file)
        except Exception:
            return _not_ready

        if not api_key or not hid:
            return _not_ready

        # --- 3. 通过：返回 config_exists=True ---
        # 解析 data_root 路径
        try:
            expanded = Path(data_root_raw).expanduser()
            if expanded.is_absolute():
                resolved_root = str(expanded.resolve())
            else:
                resolved_root = str((config_file.parent / expanded).resolve())
        except Exception:
            resolved_root = data_root_raw

        # 产品数量（不影响 config_exists 判断）
        product_count = 0
        try:
            catalog = load_catalog_or_raise(DEFAULT_CATALOG_FILE.resolve())
            product_count = len(catalog)
        except Exception:
            pass

        return {
            "ok": True,
            "config_exists": True,
            "data_root": resolved_root,
            "product_count": product_count,
        }

    def run_setup(self, data_root: str, api_key: str, hid: str,
                  create_dir: bool = False) -> dict:
        """GUI setup 向导调用。先保存配置，再验证连通性。

        流程：
        1. 验证 data_root 路径合法性
        2. 调用 save_setup_artifacts_atomic() 原子写入 config + secrets
        3. 轻量 HTTP 探测验证连通性

        返回：
            {ok: True}                                              # 保存+验证均成功
            {ok: True, warning: "连接验证..."}                       # 保存成功但验证失败
            {ok: False, error: "..."}                               # 保存失败
            {ok: False, error_code: "dir_not_found", resolved_path} # 目录不存在
        """
        # 1. 验证 data_root 路径
        try:
            dr = Path(data_root).expanduser().resolve()
        except Exception as exc:
            return {"ok": False, "error": f"路径无效：{exc}"}

        if not dr.exists():
            if not create_dir:
                return {
                    "ok": False,
                    "error_code": "dir_not_found",
                    "resolved_path": str(dr),
                }
            try:
                dr.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                return {"ok": False, "error": f"创建目录失败：{exc}"}

        # 2. 保存配置（原子写入 config + secrets）
        config_file = DEFAULT_USER_CONFIG_FILE.resolve()
        secrets_file = DEFAULT_USER_SECRETS_FILE.resolve()

        try:
            config = UserConfig(
                data_root=str(dr),
                product_mode="local_scan",
                default_products=[],
            )
            save_setup_artifacts_atomic(
                config_path=config_file,
                config=config,
                secrets_path=secrets_file,
                api_key=api_key,
                hid=hid,
            )
        except Exception as exc:
            return {"ok": False, "error": f"配置保存失败：{exc}"}

        # 3. 轻量连通性探测（单次请求，10 秒超时，不重试）
        # 错误信息只返回状态码和通用描述，不返回 URL（URL 含 hid）
        probe_url = f"{DEFAULT_API_BASE}/fetch/stock-trading-data-daily/latest"
        try:
            resp = requests.get(
                probe_url,
                params={"uuid": hid},
                headers={"api-key": api_key},
                timeout=10,
            )
            if resp.status_code < 300:
                return {"ok": True}
            elif resp.status_code in (401, 403):
                return {
                    "ok": True,
                    "warning": (
                        "配置已保存，但连接验证未通过：凭证可能无效"
                        f"（HTTP {resp.status_code}）"
                    ),
                }
            elif resp.status_code < 500:
                return {
                    "ok": True,
                    "warning": (
                        "配置已保存，但连接验证未通过：请求异常"
                        f"（HTTP {resp.status_code}）"
                    ),
                }
            else:
                # 5xx 视为网络/服务端问题
                return {
                    "ok": True,
                    "warning": "配置已保存，但连接验证未通过，请检查网络连接",
                }
        except Exception:
            # 超时、网络错误等非 HTTP 异常
            return {
                "ok": True,
                "warning": "配置已保存，但连接验证未通过，请检查网络连接",
            }

    def start_sync(self) -> Dict[str, Any]:
        """启动同步线程。

        如果已在同步中，返回 {"started": False, "message": "..."}。
        否则启动后台线程，返回 {"started": True, "message": "..."}。
        """
        # 解析配置（锁外执行，不阻塞进度轮询）
        user_config, data_root, catalog, err = self._resolve_config()
        if err:
            return {"started": False, "message": f"配置读取失败，无法启动同步：{err}"}

        # 读-判断-写合并在同一个锁块，防止双击连续启动
        with self._lock:
            if self._progress.get("status") == "syncing":
                return {"started": False, "message": "同步正在进行中，请等待完成后再试。"}
            self._progress = dict(_PROGRESS_INIT)
            self._progress["status"] = "syncing"

        # 启动后台同步线程
        thread = threading.Thread(
            target=self._run_sync,
            args=(user_config, data_root),
            daemon=True,  # 主进程退出时自动结束
            name="gui-sync-worker",
        )
        thread.start()
        log_info("GUI 同步线程已启动。", event="GUI_SYNC")

        return {"started": True, "message": "同步已启动，请通过 get_sync_progress 查询进度。"}

    def get_sync_progress(self) -> Dict[str, Any]:
        """返回当前同步进度（线程安全的浅拷贝）。

        返回结构：
        {
            "status": "idle" / "syncing" / "done" / "error",
            "current_product": str,
            "completed": int,
            "total": int,
            "elapsed_seconds": float,
            "error_message": str,
            "run_summary": dict or None,
        }
        """
        with self._lock:
            return dict(self._progress)

    def get_history(self) -> Dict[str, Any]:
        """返回最近 20 次运行历史摘要。

        返回结构：
        {
            "ok": bool,
            "runs": [
                {"run_id": str, "started_at": str, "duration_seconds": float,
                 "success_total": int, "failed_total": int, "skipped_total": int,
                 "report_file": str},
                ...
            ]
        }
        """
        # 复用 _resolve_config() 获取 data_root，catalog 未使用但避免提取额外轻量方法
        _user_config, data_root, _catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}

        try:
            log_dir = report_dir_path(data_root)
            runs = get_run_history(log_dir, n=20)
        except Exception as exc:
            return {"ok": False, "error": f"历史记录读取失败：{exc}"}

        return {"ok": True, "runs": runs}

    def get_run_detail(self, report_file: str) -> Dict[str, Any]:
        """返回指定运行报告的产品明细。

        report_file: 报告文件绝对路径（来自 get_history 返回值）。
        内部验证路径在 log_dir 内，防止路径遍历。
        """
        _user_config, data_root, _catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}

        log_dir = report_dir_path(data_root)
        return get_run_detail(log_dir, report_file)

    def get_health_report(self) -> Dict[str, Any]:
        """返回数据健康报告。

        检测三类问题：文件缺失、CSV 不可读、残留临时文件。
        返回结构：{"ok": bool, "health": {...}} 或 {"ok": False, "error": "..."}
        """
        _user_config, data_root, catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}

        try:
            health = check_data_health(data_root, catalog)
        except Exception as exc:
            return {"ok": False, "error": f"健康检查执行失败：{exc}"}

        return {"ok": True, "health": health}

    # ------------------------------------------------------------------
    # 同步线程内部逻辑（不对外暴露）
    # ------------------------------------------------------------------

    def _run_sync(self, user_config: object, data_root: Path) -> None:
        """在后台线程中执行完整的同步流程。"""
        t_start = time.time()

        try:
            config_file = DEFAULT_USER_CONFIG_FILE.resolve()
            secrets_file = DEFAULT_USER_SECRETS_FILE.resolve()

            # 解析凭证（CLI 来源为空，走文件或环境变量）
            api_key, hid, credential_source = resolve_credentials_for_update(
                cli_api_key="",   # GUI 不从命令行传凭证
                cli_hid="",
                secrets_file=secrets_file,
            )

            if not api_key or not hid:
                raise RuntimeError(
                    "未找到有效凭证（API Key / HID）；"
                    "请确认 user_secrets.env 已写入，或设置 QUANTCLASS_API_KEY / QUANTCLASS_HID 环境变量。"
                )

            run_id = _new_run_id()

            # 构造运行上下文
            command_ctx = CommandContext(
                run_id=run_id,
                data_root=data_root,
                data_root_from_cli=False,
                api_key=api_key,
                hid=hid,
                secrets_file=secrets_file,
                secrets_file_from_cli=False,
                config_file=config_file,
                dry_run=False,
                report_file=None,
                stop_on_error=False,
                verbose=False,
                mode="network",  # API 接入模式（走真实 HTTP）
                api_base=DEFAULT_API_BASE,
                catalog_file=DEFAULT_CATALOG_FILE.resolve(),
                work_dir=DEFAULT_WORK_DIR.resolve(),
            )

            log_info(
                "GUI 同步开始。",
                event="GUI_SYNC",
                run_id=run_id,
                credential_source=credential_source,
            )

            def progress_callback(product_name: str, completed: int, total: int) -> None:
                """同步进度回调，在每个产品完成时被 orchestrator 调用。"""
                elapsed = time.time() - t_start
                self._update_progress(
                    current_product=product_name,
                    completed=completed,
                    total=total,
                    elapsed_seconds=round(elapsed, 1),
                )

            # 产品选择逻辑，与 CLI update 对齐：
            # explicit_list 模式: default_products 作为主列表
            # local_scan 模式: 本地扫描为主，default_products 作为 fallback
            selected_products: list = []
            fallback_products: list = []
            if getattr(user_config, "product_mode", "") == PRODUCT_MODE_EXPLICIT_LIST:
                if not user_config.default_products:
                    raise RuntimeError("配置了 explicit_list，但 default_products 为空；请重新执行 setup。")
                selected_products = user_config.default_products
            else:
                fallback_products = user_config.default_products or []

            # 执行同步，接收退出码判断业务结果
            exit_code = run_update_with_settings(
                command_ctx=command_ctx,
                mode="local",  # 产品发现策略（按本地已有目录扫描）
                products=selected_products,
                force_update=False,
                command_name="gui_update",
                fallback_products=fallback_products,
                max_workers=4,
                progress_callback=progress_callback,
            )

            # 读取 run_summary 并转换为前端友好格式
            elapsed = time.time() - t_start
            run_summary = None
            try:
                log_dir = report_dir_path(data_root)
                raw_run = get_latest_run_summary(log_dir)
                if raw_run:
                    run_summary = {
                        "ok": raw_run.get("success_total", 0),
                        "error": raw_run.get("failed_total", 0),
                        "skipped": raw_run.get("skipped_total", 0),
                        "duration_seconds": raw_run.get("duration_seconds", 0),
                        "started_at": raw_run.get("started_at", ""),
                        "failed_products": [
                            fp.get("product", "") for fp in raw_run.get("failed_products", [])
                        ],
                    }
            except Exception as summary_exc:
                log_error(f"同步完成但运行摘要读取失败：{summary_exc}", event="GUI_SYNC")

            # 退出码非零表示有产品失败或无可执行产品，标记为 error 并附带 run_summary
            if exit_code != EXIT_CODE_SUCCESS:
                error_msg = "部分产品同步失败" if run_summary and run_summary.get("error", 0) > 0 else "同步未成功完成"
                self._update_progress(
                    status="error",
                    elapsed_seconds=round(elapsed, 1),
                    error_message=error_msg,
                    run_summary=run_summary,
                )
                log_info("GUI 同步结束（有失败）。", event="GUI_SYNC", exit_code=exit_code, elapsed=round(elapsed, 1))
            else:
                self._update_progress(
                    status="done",
                    elapsed_seconds=round(elapsed, 1),
                    run_summary=run_summary,
                )
                log_info("GUI 同步完成。", event="GUI_SYNC", elapsed=round(elapsed, 1))

        except Exception as exc:
            # 预检阶段异常（凭证缺失、配置错误等），尚未进入 run_update_with_settings，
            # 无 run_summary 可填（保持初始值 None），前端据此不展示摘要详情
            elapsed = time.time() - t_start
            error_msg = str(exc)
            log_error(f"GUI 同步出错：{error_msg}", event="GUI_SYNC")
            self._update_progress(
                status="error",
                elapsed_seconds=round(elapsed, 1),
                error_message=error_msg,
            )
