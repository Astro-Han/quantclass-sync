"""职责：GUI 的 Python API 层，作为 pywebview 的 js_api 挂载。

所有公开方法均返回 dict/list，前端通过 await window.pywebview.api.xxx() 调用。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import requests
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ..config import (
    ensure_data_root_ready,
    load_secrets_from_file,
    load_user_config_or_raise,
    resolve_credentials_for_update,
    resolve_path_from_config,
    save_setup_artifacts_atomic,
)
from ..constants import (
    DEFAULT_API_BASE,
    DEFAULT_CATALOG_FILE,
    DEFAULT_USER_CONFIG_FILE,
    DEFAULT_USER_SECRETS_FILE,
    DEFAULT_WORK_DIR,
    DEFAULT_GUI_WORKERS,
    EXIT_CODE_SUCCESS,
    PRODUCT_MODE_EXPLICIT_LIST,
)
from ..data_query import (
    check_data_health,
    get_latest_run_summary,
    get_products_overview,
    get_run_detail,
    get_run_history,
    repair_data_issues,
)
from ..http_client import get_latest_time
from ..models import CommandContext, FatalRequestError, UserConfig, log_error, log_info, new_run_id
from ..orchestrator import _build_headers, load_catalog_or_raise, run_update_with_settings
from ..status_store import report_dir_path, update_api_latest_dates


def _format_run_summary(raw_run: Dict[str, Any]) -> Dict[str, Any]:
    """把 run_report 原始 dict 转换为前端友好格式。

    failed_products 直接透传原始对象列表 [{product, error, reason_code}]，
    供前端展示详情和 retry_failed 功能读取产品名。
    """
    return {
        "ok": raw_run.get("success_total", 0),
        "error": raw_run.get("failed_total", 0),
        "skipped": raw_run.get("skipped_total", 0),
        "duration_seconds": raw_run.get("duration_seconds", 0),
        "started_at": raw_run.get("started_at", ""),
        # 直接透传对象列表，不剥离为纯字符串
        "failed_products": list(raw_run.get("failed_products", [])),
        # 阶段耗时（前端用于展示"探测 Xs + 同步 Xs"）
        "phase_plan_seconds": raw_run.get("phase_plan_seconds"),
        "phase_sync_seconds": raw_run.get("phase_sync_seconds"),
    }


# _progress 的初始结构，每次 start_sync 前重置为此形态
_PROGRESS_INIT: Dict[str, Any] = {
    "status": "idle",          # idle / syncing / confirm_needed / done / error
    "current_product": "",     # 最近完成的产品名
    "completed": 0,            # 已完成产品数
    "total": 0,                # 本次同步产品总数
    "elapsed_seconds": 0,      # 已用时（秒）
    "error_message": "",       # 出错时的错误信息
    "run_summary": None,       # 同步完成后填充 run_summary dict
    "products": [],            # 已完成产品列表 [{name, status, elapsed_seconds, files_count}]
    "all_products": [],        # 全部产品名列表（由 progress_callback 初始化调用时传入）
    "estimate": None,          # EstimateResult 的 dict 表示（confirm_needed 时填充）
}


class SyncApi:
    """pywebview js_api 类。

    线程安全：所有对 _progress 的读写均通过 _lock 保护。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 深拷贝初始值，避免多次运行时共享同一个 dict 引用
        self._progress: Dict[str, Any] = dict(_PROGRESS_INIT)
        self._health_progress: Dict[str, Any] = {
            "checking": False, "current": 0, "total": 0, "product": "", "result": None,
        }
        # 同步确认事件（每次 _run_sync 重置，用于与前端双向通信）
        self._confirm_event: threading.Event = threading.Event()
        self._confirm_result: bool = False
        # 明确标记用户是否主动取消（区别于凭证错误等其他 error 场景）
        self._was_cancelled: bool = False

    # ------------------------------------------------------------------
    # 内部辅助方法
    # ------------------------------------------------------------------

    def _resolve_data_root(self) -> Tuple[Optional[object], Optional[Path], Optional[str]]:
        """读取 user_config 并解析 data_root（不加载 catalog）。

        返回 (user_config, data_root, error_message)。
        出错时 user_config/data_root 均为 None，error_message 为描述字符串。
        """
        config_file = DEFAULT_USER_CONFIG_FILE.resolve()

        if not config_file.exists():
            return None, None, (
                f"未找到用户配置文件：{config_file}；请先执行 setup 命令完成初始化。"
            )

        try:
            user_config = load_user_config_or_raise(config_file)
        except Exception as exc:
            return None, None, f"用户配置读取失败：{exc}"

        try:
            data_root = resolve_path_from_config(
                Path(user_config.data_root), config_file=config_file,
            )
        except Exception as exc:
            return None, None, f"data_root 路径解析失败：{exc}"

        return user_config, data_root, None

    def _resolve_config(self) -> Tuple[Optional[object], Optional[Path], Optional[list], Optional[str]]:
        """读取 user_config、解析 data_root、加载 catalog。

        返回 (user_config, data_root, catalog, error_message)。
        出错时 user_config/data_root/catalog 均为 None，error_message 为描述字符串。
        """
        user_config, data_root, err = self._resolve_data_root()
        if err:
            return None, None, None, err

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

        # 校验 data_root 是否合法（拦截误指向产品子目录）
        try:
            ensure_data_root_ready(data_root, create_if_missing=False)
        except RuntimeError as exc:
            return {"ok": False, "error": str(exc)}

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

        last_run = _format_run_summary(raw_run) if raw_run else None

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
            resolved_root = str(resolve_path_from_config(
                Path(data_root_raw), config_file=config_file,
            ))
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
            # 仅 macOS 支持在 Finder 中打开目录
            "can_open_dir": sys.platform == "darwin",
        }

    def run_setup(self, data_root: str, api_key: str, hid: str,
                  create_dir: bool = False, course_type: str = "basic") -> dict:
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
            # basic 固定 10 次/天，premium 固定 100 次/天
            _limit_map = {"basic": 10, "premium": 100}
            _api_call_limit = _limit_map.get(course_type, 50)
            config = UserConfig(
                data_root=str(dr),
                product_mode="local_scan",
                default_products=[],
                course_type=course_type,
                api_call_limit=_api_call_limit,
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

    def confirm_sync(self) -> Dict[str, Any]:
        """前端点击"继续同步"时调用，唤醒后台线程继续执行。"""
        self._confirm_result = True
        self._confirm_event.set()
        return {"ok": True}

    def cancel_sync(self) -> Dict[str, Any]:
        """前端点击"取消"时调用，唤醒后台线程并标记取消。

        只设置取消标志和事件，最终状态由 _run_sync 的 exit_code==-1 分支统一写入，
        避免与 _run_sync 写 status="idle" 产生竞争。
        """
        self._was_cancelled = True
        self._confirm_result = False
        self._confirm_event.set()
        return {"ok": True}

    def start_sync(self, retry_failed: bool = False) -> Dict[str, Any]:
        """启动同步线程。

        retry_failed=True 时只重跑上次失败的产品。
        如果已在同步中，返回 {"started": False, "message": "..."}。
        否则启动后台线程，返回 {"started": True, "message": "..."}。
        """
        # 解析配置（锁外执行，不阻塞进度轮询）
        user_config, data_root, catalog, err = self._resolve_config()
        if err:
            return {"started": False, "message": f"配置读取失败，无法启动同步：{err}"}

        retry_products = None
        # 读-判断-写合并在同一个锁块，防止双击连续启动
        with self._lock:
            # 检查 worker 线程是否仍在运行（cancel 后 status 变 error，但线程可能未退出）
            if hasattr(self, "_sync_thread") and self._sync_thread and self._sync_thread.is_alive():
                return {"started": False, "message": "同步正在进行中，请等待完成后再试。"}
            # confirm_needed 状态表示同步已在进行中（等待用户确认），也需拦截
            if self._progress.get("status") in ("syncing", "confirm_needed"):
                return {"started": False, "message": "同步正在进行中，请等待完成后再试。"}

            # retry_failed 分支：从上次 run_summary 读取失败产品名
            if retry_failed:
                run_summary = self._progress.get("run_summary")
                if not run_summary:
                    return {"started": False, "message": "没有上次同步记录"}
                failed = run_summary.get("failed_products", [])
                if not failed:
                    return {"started": False, "message": "没有失败产品"}
                # failed_products 是对象列表 [{product, error, reason_code}]，做防御性过滤
                retry_products = [
                    item.get("product", "") for item in failed
                    if isinstance(item, dict) and item.get("product")
                ]

            self._progress = dict(_PROGRESS_INIT)
            self._progress["status"] = "syncing"
            # 显式赋新空列表，防止意外共享引用
            self._progress["products"] = []
            self._progress["all_products"] = []

        # 启动后台同步线程（锁外启动，避免持锁创建线程）
        thread = threading.Thread(
            target=self._run_sync,
            args=(user_config, data_root, retry_products),
            daemon=True,  # 主进程退出时自动结束
            name="gui-sync-worker",
        )
        # 保存线程引用，供下次 start_sync 调用时检查是否仍在运行
        self._sync_thread = thread
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
            p = dict(self._progress)
            # 对列表字段做浅拷贝，防止调用方持有引用后被后台线程修改
            p["products"] = list(self._progress["products"])
            p["all_products"] = list(self._progress["all_products"])
            return p

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
        _user_config, data_root, err = self._resolve_data_root()
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
        _user_config, data_root, err = self._resolve_data_root()
        if err:
            return {"ok": False, "error": err}

        log_dir = report_dir_path(data_root)
        return get_run_detail(log_dir, report_file)

    def open_data_dir(self) -> Dict[str, Any]:
        """在 Finder 中打开数据目录（仅 macOS）。"""
        if sys.platform != "darwin":
            return {"ok": False, "error": "仅支持 macOS"}
        user_config, data_root, _err = self._resolve_data_root()
        if _err:
            return {"ok": False, "error": _err}
        data_root_str = str(data_root)
        if not os.path.isdir(data_root_str):
            return {"ok": False, "error": "目录不存在"}
        try:
            # 用 -- 分隔选项与路径，防止路径以 - 开头时被误解为参数
            result = subprocess.run(["open", "--", data_root_str])
            if result.returncode != 0:
                return {"ok": False, "error": f"打开失败 (exit {result.returncode})"}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        return {"ok": True}

    def start_health_check(self) -> Dict[str, Any]:
        """启动后台健康检查线程。同步中（含等待确认）拒绝，重复启动拒绝。"""
        with self._lock:
            if self._progress.get("status") in ("syncing", "confirm_needed"):
                return {"ok": False, "error": "同步进行中，请稍后再试"}
            if self._health_progress["checking"]:
                return {"ok": False, "error": "检查已在进行中"}
            self._health_progress = {
                "checking": True, "current": 0, "total": 0, "product": "", "result": None,
            }
        thread = threading.Thread(target=self._run_health_check, daemon=True)
        thread.start()
        return {"ok": True}

    def _run_health_check(self) -> None:
        """后台线程执行健康检查。"""
        try:
            _user_config, data_root, catalog, err = self._resolve_config()
            if err:
                with self._lock:
                    self._health_progress["checking"] = False
                    self._health_progress["result"] = {"ok": False, "error": err}
                return
            data_root = Path(data_root)

            def progress_cb(current, total, product, phase):
                with self._lock:
                    self._health_progress.update({
                        "current": current, "total": total, "product": product,
                    })

            ensure_data_root_ready(data_root, create_if_missing=False)
            result = check_data_health(data_root, catalog, progress_callback=progress_cb)
            with self._lock:
                self._health_progress["checking"] = False
                self._health_progress["result"] = {"ok": True, "health": result}
        except Exception as e:
            with self._lock:
                self._health_progress["checking"] = False
                self._health_progress["result"] = {"ok": False, "error": str(e)}

    def get_health_progress(self) -> Dict[str, Any]:
        """轮询健康检查进度。"""
        with self._lock:
            return {
                "checking": self._health_progress["checking"],
                "current": self._health_progress["current"],
                "total": self._health_progress["total"],
                "product": self._health_progress["product"],
            }

    def get_health_result(self) -> Dict[str, Any]:
        """获取最近一次检查结果。"""
        with self._lock:
            return self._health_progress.get("result")

    def repair_health_issues(self) -> Dict[str, Any]:
        """修复可修复的数据问题。同步中（含等待确认）拒绝。"""
        with self._lock:
            if self._progress.get("status") in ("syncing", "confirm_needed"):
                return {"ok": False, "error": "同步进行中，请稍后修复"}
            result = self._health_progress.get("result")
        if not result or not result.get("ok"):
            return {"ok": False, "error": "无可用检查结果"}
        issues = result["health"]["issues"]
        repairable = [i for i in issues if i.get("repairable")]
        if not repairable:
            return {"ok": False, "error": "无可修复问题"}
        _user_config, data_root, _catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}
        repair_result = repair_data_issues(Path(data_root), issues)
        return {"ok": True, "repair": repair_result}

    def check_updates(self) -> Dict[str, Any]:
        """查询 API 获取各产品最新日期，返回实时 overview。

        并发查询，总超时 30 秒。区分全局错误（401/403 立即中止）
        和单产品错误（跳过计入失败列表）。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
        from datetime import date as _date

        user_config, data_root, catalog, err = self._resolve_config()
        if err:
            return {"ok": False, "error": err}

        # 校验 data_root 是否合法
        try:
            ensure_data_root_ready(data_root, create_if_missing=False)
        except RuntimeError as exc:
            return {"ok": False, "error": str(exc)}

        if not catalog:
            return {
                "ok": True, "products": [],
                "summary": {"green": 0, "yellow": 0, "red": 0, "gray": 0},
                "checked": 0, "failed": 0, "failed_products": [],
            }

        # 解析凭证
        secrets_file = DEFAULT_USER_SECRETS_FILE.resolve()
        try:
            api_key, hid, _ = resolve_credentials_for_update(
                cli_api_key="", cli_hid="", secrets_file=secrets_file,
            )
        except Exception as exc:
            return {"ok": False, "error": f"凭证解析失败：{exc}"}

        if not api_key or not hid:
            return {"ok": False, "error": "未找到有效凭证（API Key / HID），请先完成配置。"}

        headers = _build_headers(api_key)
        api_base = DEFAULT_API_BASE

        # 并发查询各产品 API 最新日期
        api_latest_dates: Dict[str, str] = {}
        failed_products: list = []
        # 全局中止信号：401/403 时通知其他 worker 提前退出
        abort_event = threading.Event()
        global_error_holder: list = []

        def _query_one(product: str) -> tuple:
            """查询单个产品，返回 (product, date_str, error)。"""
            if abort_event.is_set():
                return (product, None, "已中止")
            try:
                latest = get_latest_time(api_base, product, hid, headers)
                return (product, latest, None)
            except FatalRequestError as exc:
                if exc.status_code in (401, 403):
                    abort_event.set()
                    global_error_holder.append(exc)
                    return (product, None, str(exc))
                return (product, None, str(exc))
            except Exception as exc:
                return (product, None, str(exc))

        executor = ThreadPoolExecutor(max_workers=max(1, min(8, len(catalog))))
        try:
            futures = {executor.submit(_query_one, p): p for p in catalog}
            for future in as_completed(futures, timeout=30):
                product, latest, error = future.result()
                if error:
                    failed_products.append(product)
                else:
                    api_latest_dates[product] = latest
                # 检测到全局错误后不再等待剩余 future
                if abort_event.is_set():
                    break
        except FuturesTimeoutError:
            pass  # 超时后下面统一处理未完成的产品
        except Exception as exc:
            return {"ok": False, "error": f"检查更新失败：{exc}"}
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        # 全局错误（401/403）：立即返回
        if global_error_holder:
            exc = global_error_holder[0]
            return {"ok": False, "error": f"API 凭证或额度异常（HTTP {exc.status_code}）：{exc}"}

        # 按产品名补漏：未进入 api_latest_dates 也未进入 failed_products 的归入失败
        for product in catalog:
            if product not in api_latest_dates and product not in failed_products:
                failed_products.append(product)

        # 持久化查到的 API 日期，下次打开 GUI 时宽限期逻辑可用
        if api_latest_dates:
            try:
                log_dir = report_dir_path(data_root)
                update_api_latest_dates(log_dir, api_latest_dates)
            except Exception:
                pass  # 持久化失败不影响本次结果

        # 用 API 日期生成实时 overview
        try:
            raw_products = get_products_overview(
                data_root, catalog, today=_date.today(), api_latest_dates=api_latest_dates,
            )
        except Exception as exc:
            return {"ok": False, "error": f"状态计算失败：{exc}"}

        # 转换为前端字段名，附加 source 标记
        products = []
        for p in raw_products:
            source = "api" if p["name"] in api_latest_dates else "cached"
            products.append({
                "name": p["name"],
                "color": p["status_color"],
                "local_date": p["local_date"],
                "behind_days": p["days_behind"],
                "last_result": p["last_status"],
                "last_error": p["last_error"],
                "source": source,
            })

        # 统计卡片
        summary = {"green": 0, "yellow": 0, "red": 0, "gray": 0}
        for p in products:
            color = p.get("color", "gray")
            if color in summary:
                summary[color] += 1

        return {
            "ok": True,
            "products": products,
            "summary": summary,
            "checked": len(api_latest_dates),
            "failed": len(failed_products),
            "failed_products": sorted(failed_products),
        }

    # ------------------------------------------------------------------
    # 同步线程内部逻辑（不对外暴露）
    # ------------------------------------------------------------------

    def _run_sync(self, user_config: object, data_root: Path,
                  retry_products: list = None) -> None:
        """在后台线程中执行完整的同步流程。

        retry_products: 指定只同步的产品列表；None 表示全量同步。
        """
        t_start = time.time()

        # 每次同步重置确认事件和取消标志，防止上次状态残留
        self._confirm_event = threading.Event()
        self._confirm_result = False
        self._was_cancelled = False

        def _gui_confirm(estimate) -> bool:
            """GUI 确认回调：设状态 -> 等前端点击 -> 返回结果。

            在 gui-sync-worker 线程等待，不持 _lock，不阻塞 pywebview 主线程。
            等待超时（300s）视为取消。
            """
            from dataclasses import asdict
            with self._lock:
                self._progress["status"] = "confirm_needed"
                self._progress["estimate"] = asdict(estimate)
            # 等待前端调用 confirm_sync() 或 cancel_sync()
            self._confirm_event.wait(timeout=300)
            # wait 返回后清除确认状态，防止前端轮询时 confirm_needed 卡片重复弹出
            with self._lock:
                self._progress["status"] = "syncing"
                self._progress["estimate"] = None
            return self._confirm_result

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

            run_id = new_run_id()

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

            def progress_callback(product_name: str, completed: int, total: int, *,
                                   elapsed_seconds: float = 0.0, stats=None,
                                   status: str = "ok", all_products=None,
                                   error: str = "", **_kwargs) -> None:
                """同步进度回调，在每个产品完成时被 orchestrator 调用。

                参数：
                  product_name    -- 当前产品名
                  completed       -- 已完成产品数
                  total           -- 产品总数
                  elapsed_seconds -- 该产品耗时（秒，由 orchestrator 填充）
                  stats           -- SyncStats 对象（含 created_files/updated_files）
                  status          -- "init" | "ok" | "error" | "skip"
                  all_products    -- 全部产品名列表（仅 init 调用时传入）
                  error           -- 失败时的错误描述（由 orchestrator 透传）
                """
                with self._lock:
                    # 初始化调用：写入全部产品名并记录总数
                    if all_products is not None:
                        self._progress["all_products"] = list(all_products)
                    if status == "init":
                        self._progress["total"] = total
                        return
                    # 计算本产品同步的文件数（新建 + 更新）
                    files_count = (stats.created_files + stats.updated_files) if stats else 0
                    # 追加到已完成产品列表，包含 error 字段供前端展示失败原因
                    self._progress["products"].append({
                        "name": product_name,
                        "status": status,
                        "elapsed_seconds": round(elapsed_seconds, 2),
                        "files_count": files_count,
                        "error": error,
                    })
                    # 同时更新原有字段，保持前端兼容
                    elapsed = time.time() - t_start
                    self._progress["current_product"] = product_name
                    self._progress["completed"] = completed
                    self._progress["total"] = total
                    self._progress["elapsed_seconds"] = round(elapsed, 1)

            # 产品选择逻辑，与 CLI update 对齐：
            # retry_products 不为空时直接覆盖，只重跑失败产品
            # explicit_list 模式: default_products 作为主列表
            # local_scan 模式: 本地扫描为主，default_products 作为 fallback
            selected_products: list = []
            fallback_products: list = []
            if retry_products is not None:
                # retry_failed 分支：强制指定产品列表，跳过常规发现逻辑
                selected_products = retry_products
            elif getattr(user_config, "product_mode", "") == PRODUCT_MODE_EXPLICIT_LIST:
                if not user_config.default_products:
                    raise RuntimeError("配置了 explicit_list，但 default_products 为空；请重新执行 setup。")
                selected_products = user_config.default_products
            else:
                fallback_products = user_config.default_products or []

            # 执行同步，接收退出码判断业务结果
            # 传入 api_call_limit/course_type 供预估函数使用，confirm_callback 供 GUI 确认流程
            exit_code = run_update_with_settings(
                command_ctx=command_ctx,
                mode="local",  # 产品发现策略（按本地已有目录扫描）
                products=selected_products,
                force_update=False,
                command_name="gui_update",
                fallback_products=fallback_products,
                max_workers=DEFAULT_GUI_WORKERS,
                progress_callback=progress_callback,
                api_call_limit=getattr(user_config, "api_call_limit", 50),
                course_type=getattr(user_config, "course_type", ""),
                confirm_callback=_gui_confirm,
            )

            # 读取 run_summary 并转换为前端友好格式
            elapsed = time.time() - t_start
            run_summary = None
            try:
                log_dir = report_dir_path(data_root)
                raw_run = get_latest_run_summary(log_dir)
                if raw_run:
                    run_summary = _format_run_summary(raw_run)
            except Exception as summary_exc:
                log_error(f"同步完成但运行摘要读取失败：{summary_exc}", event="GUI_SYNC")

            # orchestrator 返回 -1 表示用户主动取消，静默回到 idle（不显示成功也不显示错误）
            if exit_code == -1:
                self._update_progress(status="idle")
                log_info("GUI 同步已取消。", event="GUI_SYNC", elapsed=round(elapsed, 1))
                return

            # 退出码非零表示有产品失败或无可执行产品，标记为 error 并附带 run_summary
            # 注意：cancel_sync() 会直接设 status=error，用 _was_cancelled 精确判断，
            # 避免凭证错误等场景误判为"已取消"
            already_cancelled = self._was_cancelled
            if not already_cancelled:
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
            # 若用户已主动取消（_was_cancelled），静默回到 idle，不展示 error
            if self._was_cancelled:
                self._update_progress(status="idle")
                return
            elapsed = time.time() - t_start
            error_msg = str(exc)
            log_error(f"GUI 同步出错：{error_msg}", event="GUI_SYNC")
            self._update_progress(
                status="error",
                elapsed_seconds=round(elapsed, 1),
                error_message=error_msg,
            )
