#!/usr/bin/env python3
"""QuantClass 兼容入口：对外导出旧符号，并转发到内部模块实现。"""

from __future__ import annotations

import sys
import time

import requests

from quantclass_sync_internal import cli as _cli
from quantclass_sync_internal import http_client as _http
from quantclass_sync_internal import models as _models
from quantclass_sync_internal import orchestrator as _orchestrator
from quantclass_sync_internal import reporting as _reporting
from quantclass_sync_internal.constants import (
    AGGREGATE_SPLIT_COLS,
    BUSINESS_DAY_ONLY_PRODUCTS,
    DEFAULT_API_BASE,
    DEFAULT_CATALOG_FILE,
    DEFAULT_DATA_ROOT,
    DEFAULT_REPORT_RETENTION_DAYS,
    EXIT_CODE_GENERAL_FAILURE,
    EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE,
    EXIT_CODE_NO_EXECUTABLE_PRODUCTS,
    EXIT_CODE_SUCCESS,
    PREPROCESS_PRODUCT,
    PRODUCT_MODE_LOCAL_SCAN,
    REASON_MERGE_ERROR,
    REASON_MIRROR_UNKNOWN,
    REASON_NO_DATA_FOR_DATE,
    REASON_OK,
    REASON_PREPROCESS_DRY_RUN,
    REASON_PREPROCESS_FAILED,
    REASON_PREPROCESS_FALLBACK_FULL_OK,
    REASON_PREPROCESS_INCREMENTAL_OK,
    REASON_PREPROCESS_SKIPPED_NO_DELTA,
    REASON_UP_TO_DATE,
    STRATEGY_MERGE_KNOWN,
    TIMESTAMP_FILE_NAME,
    UTF8_BOM,
)
from quantclass_sync_internal.config import (
    build_product_plan,
    load_user_config_or_raise,
    load_user_secrets_or_raise,
    resolve_credentials_for_update,
    save_user_config_atomic,
    save_user_secrets_atomic,
)
from quantclass_sync_internal.csv_engine import decode_text, read_csv_payload, sync_payload_to_target
from quantclass_sync_internal.file_sync import repair_sort_product_files, sortable_products, sync_known_product
from quantclass_sync_internal.models import (
    CommandContext,
    CsvPayload,
    FatalRequestError,
    ProductPlan,
    ProductRunResult,
    ProductStatus,
    ProductSyncError,
    RULES,
    RunReport,
    SyncStats,
    UserConfig,
    log_debug,
    log_error,
    log_info,
    normalize_product_name,
)
from quantclass_sync_internal.status_store import connect_status_db, load_product_status

# --- 直接暴露给外部 patch 的原子函数（保持旧语义） ---
get_latest_times = _http.get_latest_times
get_latest_time = _http.get_latest_time
get_download_link = _http.get_download_link
parse_latest_time_candidates = _http.parse_latest_time_candidates
_http_metrics_for_product = _http._http_metrics_for_product
_reset_http_metrics = _http._reset_http_metrics

build_headers_or_raise = _orchestrator.build_headers_or_raise
process_product = _orchestrator.process_product
load_catalog_or_raise = _orchestrator.load_catalog_or_raise
_run_builtin_coin_preprocess = _orchestrator._run_builtin_coin_preprocess

resolve_report_path = _reporting.resolve_report_path
write_run_report = _reporting.write_run_report
_append_result = _reporting._append_result
_finalize_and_write_report = _reporting._finalize_and_write_report
_new_report = _reporting._new_report

app = _cli.app

# 保留实现原函数，兼容层会在调用前同步 patch 依赖。
_global_options_impl = _cli.global_options
_cmd_setup_impl = _cli.cmd_setup
_cmd_update_impl = _cli.cmd_update
_cmd_repair_sort_impl = _cli.cmd_repair_sort
_cmd_init_impl = _cli.cmd_init
_cmd_one_data_impl = _cli.cmd_one_data
_cmd_all_data_impl = _cli.cmd_all_data

_probe_downloadable_dates_impl = _orchestrator._probe_downloadable_dates
_resolve_requested_dates_for_plan_impl = _orchestrator._resolve_requested_dates_for_plan
_execute_plans_impl = _orchestrator._execute_plans
_maybe_run_coin_preprocess_impl = _orchestrator._maybe_run_coin_preprocess
run_update_with_settings_impl = _orchestrator.run_update_with_settings
_request_data_impl = _http.request_data


def _bind_http_runtime() -> None:
    """把兼容层里可 patch 的底层依赖同步到 HTTP 模块。"""

    _http.requests = requests
    _http.time = time


def _bind_orchestrator_runtime(*, probe_callable) -> None:
    """把兼容层导出函数绑定到编排模块，保持旧版 patch 语义。"""

    _bind_http_runtime()
    _orchestrator.get_latest_times = get_latest_times
    _orchestrator.get_latest_time = get_latest_time
    _orchestrator.get_download_link = get_download_link
    _orchestrator.build_headers_or_raise = build_headers_or_raise
    _orchestrator.process_product = process_product
    _orchestrator.load_catalog_or_raise = load_catalog_or_raise
    _orchestrator.write_run_report = write_run_report
    _orchestrator.resolve_report_path = resolve_report_path
    _orchestrator._run_builtin_coin_preprocess = _run_builtin_coin_preprocess
    _orchestrator._resolve_requested_dates_for_plan = _resolve_requested_dates_for_plan
    _orchestrator._execute_plans = _execute_plans
    _orchestrator._probe_downloadable_dates = probe_callable
    _reporting.write_run_report = write_run_report


def _bind_cli_runtime() -> None:
    """把兼容层导出函数绑定到 CLI 模块。"""

    # CLI 内部会调用编排层能力，先完成编排依赖绑定。
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates)
    _cli.resolve_credentials_for_update = resolve_credentials_for_update
    _cli.run_update_with_settings = run_update_with_settings
    _cli.resolve_report_path = resolve_report_path
    _cli.load_catalog_or_raise = load_catalog_or_raise
    _cli._build_headers = _orchestrator._build_headers
    _cli.get_latest_time = get_latest_time
    _cli.sys = sys
    _cli.sortable_products = sortable_products
    _cli.repair_sort_product_files = repair_sort_product_files
    # 让 global_options 内部 invoke 到兼容层函数，保持测试里的函数对象一致。
    _cli.cmd_setup = cmd_setup
    _cli.cmd_update = cmd_update
    _cli.cmd_repair_sort = cmd_repair_sort


# --- 兼容层转发函数（保留旧调用点和 patch 语义） ---
def request_data(*args, **kwargs):
    _bind_http_runtime()
    return _request_data_impl(*args, **kwargs)


def _probe_downloadable_dates(*args, **kwargs):
    # 避免把 _probe_downloadable_dates 重新绑定到自己，直接绑定实现函数。
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates_impl)
    return _probe_downloadable_dates_impl(*args, **kwargs)


def _resolve_requested_dates_for_plan(*args, **kwargs):
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates)
    return _resolve_requested_dates_for_plan_impl(*args, **kwargs)


def _execute_plans(*args, **kwargs):
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates)
    return _execute_plans_impl(*args, **kwargs)


def _maybe_run_coin_preprocess(*args, **kwargs):
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates)
    return _maybe_run_coin_preprocess_impl(*args, **kwargs)


def run_update_with_settings(*args, **kwargs):
    _bind_orchestrator_runtime(probe_callable=_probe_downloadable_dates)
    return run_update_with_settings_impl(*args, **kwargs)


def global_options(*args, **kwargs):
    _bind_cli_runtime()
    result = _global_options_impl(*args, **kwargs)
    # CLI 层会重设 LOGGER，这里回写到 models，保证全局日志函数看到最新实例。
    _models.LOGGER = _cli.LOGGER
    _models.PROGRESS_EVERY = _cli.PROGRESS_EVERY
    return result


def cmd_setup(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_setup_impl(*args, **kwargs)


def cmd_update(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_update_impl(*args, **kwargs)


def cmd_repair_sort(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_repair_sort_impl(*args, **kwargs)


def cmd_init(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_init_impl(*args, **kwargs)


def cmd_one_data(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_one_data_impl(*args, **kwargs)


def cmd_all_data(*args, **kwargs):
    _bind_cli_runtime()
    return _cmd_all_data_impl(*args, **kwargs)


__all__ = [
    # constants
    "AGGREGATE_SPLIT_COLS",
    "BUSINESS_DAY_ONLY_PRODUCTS",
    "DEFAULT_API_BASE",
    "DEFAULT_CATALOG_FILE",
    "DEFAULT_DATA_ROOT",
    "DEFAULT_REPORT_RETENTION_DAYS",
    "EXIT_CODE_SUCCESS",
    "EXIT_CODE_GENERAL_FAILURE",
    "EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE",
    "EXIT_CODE_NO_EXECUTABLE_PRODUCTS",
    "PREPROCESS_PRODUCT",
    "PRODUCT_MODE_LOCAL_SCAN",
    "REASON_MERGE_ERROR",
    "REASON_MIRROR_UNKNOWN",
    "REASON_NO_DATA_FOR_DATE",
    "REASON_OK",
    "REASON_PREPROCESS_DRY_RUN",
    "REASON_PREPROCESS_FAILED",
    "REASON_PREPROCESS_FALLBACK_FULL_OK",
    "REASON_PREPROCESS_INCREMENTAL_OK",
    "REASON_PREPROCESS_SKIPPED_NO_DELTA",
    "REASON_UP_TO_DATE",
    "STRATEGY_MERGE_KNOWN",
    "TIMESTAMP_FILE_NAME",
    "UTF8_BOM",
    # models/types
    "CommandContext",
    "CsvPayload",
    "FatalRequestError",
    "ProductPlan",
    "ProductRunResult",
    "ProductStatus",
    "ProductSyncError",
    "RunReport",
    "SyncStats",
    "UserConfig",
    "RULES",
    # util/config
    "build_product_plan",
    "load_user_config_or_raise",
    "load_user_secrets_or_raise",
    "resolve_credentials_for_update",
    "save_user_config_atomic",
    "save_user_secrets_atomic",
    "decode_text",
    "read_csv_payload",
    "sync_payload_to_target",
    "repair_sort_product_files",
    "sortable_products",
    "sync_known_product",
    "connect_status_db",
    "load_product_status",
    # report/orchestrator/http facade
    "get_latest_times",
    "get_latest_time",
    "get_download_link",
    "parse_latest_time_candidates",
    "request_data",
    "build_headers_or_raise",
    "process_product",
    "load_catalog_or_raise",
    "write_run_report",
    "resolve_report_path",
    "_run_builtin_coin_preprocess",
    "_http_metrics_for_product",
    "_reset_http_metrics",
    "_probe_downloadable_dates",
    "_resolve_requested_dates_for_plan",
    "_execute_plans",
    "_maybe_run_coin_preprocess",
    "run_update_with_settings",
    "_append_result",
    "_finalize_and_write_report",
    "_new_report",
    # cli
    "app",
    "global_options",
    "cmd_setup",
    "cmd_update",
    "cmd_repair_sort",
    "cmd_init",
    "cmd_one_data",
    "cmd_all_data",
    # patch helpers：保留旧版 monkey patch 入口；测试可替换这三个符号，
    # 兼容层会在调用前把替换结果回写到内部模块依赖上。
    "requests",
    "time",
    "sys",
    "log_debug",
    "log_error",
    "log_info",
    "normalize_product_name",
]


if __name__ == "__main__":
    app()
