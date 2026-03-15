#!/usr/bin/env python3
"""兼容层迁移脚本：把测试文件中的 quantclass_sync 引用替换为 quantclass_sync_internal。

用法：python3 scripts/migrate_compat_layer.py
"""
import re
import sys
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent.parent / "tests"
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── 符号来源映射 ──
SYMBOL_MODULE = {
    # constants
    "AGGREGATE_SPLIT_COLS": "constants", "DEFAULT_CATALOG_FILE": "constants",
    "EXIT_CODE_GENERAL_FAILURE": "constants", "EXIT_CODE_NETWORK_OR_REMOTE_DATA_FAILURE": "constants",
    "EXIT_CODE_NO_EXECUTABLE_PRODUCTS": "constants", "PREPROCESS_PRODUCT": "constants",
    "PRODUCT_MODE_LOCAL_SCAN": "constants", "REASON_MERGE_ERROR": "constants",
    "REASON_NO_DATA_FOR_DATE": "constants", "REASON_NO_VALID_OUTPUT": "constants",
    "REASON_OK": "constants", "REASON_PREPROCESS_DRY_RUN": "constants",
    "REASON_PREPROCESS_FAILED": "constants", "REASON_PREPROCESS_FALLBACK_FULL_OK": "constants",
    "REASON_PREPROCESS_INCREMENTAL_OK": "constants", "REASON_PREPROCESS_SKIPPED_NO_DELTA": "constants",
    "REASON_UP_TO_DATE": "constants", "STRATEGY_MERGE_KNOWN": "constants",
    "TIMESTAMP_FILE_NAME": "constants", "UTF8_BOM": "constants",
    "PREPROCESS_TRIGGER_PRODUCTS": "constants",
    # models
    "CommandContext": "models", "CsvPayload": "models", "EmptyDownloadLinkError": "models",
    "FatalRequestError": "models", "ProductPlan": "models", "ProductRunResult": "models",
    "ProductSyncError": "models", "RunReport": "models", "SyncStats": "models",
    "UserConfig": "models", "RULES": "models", "normalize_product_name": "models",
    "split_products": "models", "utc_now_iso": "models", "new_run_id": "models",
    "log_info": "models", "log_error": "models", "log_debug": "models",
    "DatasetRule": "models",
    # http_client
    "request_data": "http_client", "get_latest_time": "http_client",
    "get_latest_times": "http_client", "get_download_link": "http_client",
    "_reset_http_metrics": "http_client", "_http_metrics_for_product": "http_client",
    "parse_latest_time_candidates": "http_client",
    # orchestrator
    "process_product": "orchestrator", "run_update_with_settings": "orchestrator",
    "load_catalog_or_raise": "orchestrator", "build_headers_or_raise": "orchestrator",
    "_build_headers": "orchestrator", "_execute_plans": "orchestrator",
    "_resolve_requested_dates_for_plan": "orchestrator",
    "_probe_downloadable_dates": "orchestrator",
    "_run_builtin_coin_preprocess": "orchestrator",
    "_maybe_run_coin_preprocess": "orchestrator",
    # cli
    "app": "cli", "global_options": "cli", "cmd_setup": "cli", "cmd_update": "cli",
    "cmd_init": "cli", "cmd_one_data": "cli", "cmd_all_data": "cli",
    "cmd_repair_sort": "cli", "cmd_status": "cli", "ensure_data_root_ready": "cli",
    # reporting
    "_append_result": "reporting", "_finalize_and_write_report": "reporting",
    "_new_report": "reporting", "resolve_report_path": "reporting",
    "write_run_report": "reporting",
    # csv_engine
    "decode_text": "csv_engine", "read_csv_payload": "csv_engine",
    "sync_payload_to_target": "csv_engine",
    # file_sync
    "repair_sort_product_files": "file_sync", "sortable_products": "file_sync",
    "sync_from_extract": "file_sync", "sync_known_product": "file_sync",
    # config
    "build_product_plan": "config", "load_user_config_or_raise": "config",
    "save_user_config_atomic": "config", "save_user_secrets_atomic": "config",
    # status_store
    "connect_status_db": "status_store", "load_product_status": "status_store",
    "report_dir_path": "status_store", "open_status_db": "status_store",
    "export_status_json": "status_store", "status_json_path": "status_store",
    "upsert_product_status": "status_store", "ProductStatus": "status_store",
}

# ── patch 路径全局替换（对大多数文件安全的默认映射） ──
PATCH_REPLACEMENTS = {
    # http_client 层
    '"quantclass_sync.requests.request"': '"quantclass_sync_internal.http_client.requests.request"',
    '"quantclass_sync.requests"': '"quantclass_sync_internal.http_client.requests"',
    '"quantclass_sync.time.sleep"': '"quantclass_sync_internal.http_client.time.sleep"',
    # orchestrator 层（这些符号在测试中几乎总是 patch orchestrator 命名空间）
    '"quantclass_sync.get_latest_times"': '"quantclass_sync_internal.orchestrator.get_latest_times"',
    '"quantclass_sync.get_download_link"': '"quantclass_sync_internal.orchestrator.get_download_link"',
    '"quantclass_sync._probe_downloadable_dates"': '"quantclass_sync_internal.orchestrator._probe_downloadable_dates"',
    '"quantclass_sync.process_product"': '"quantclass_sync_internal.orchestrator.process_product"',
    '"quantclass_sync.build_headers_or_raise"': '"quantclass_sync_internal.orchestrator.build_headers_or_raise"',
    '"quantclass_sync._resolve_requested_dates_for_plan"': '"quantclass_sync_internal.orchestrator._resolve_requested_dates_for_plan"',
    '"quantclass_sync._run_builtin_coin_preprocess"': '"quantclass_sync_internal.orchestrator._run_builtin_coin_preprocess"',
    '"quantclass_sync._execute_plans"': '"quantclass_sync_internal.orchestrator._execute_plans"',
    '"quantclass_sync.write_run_report"': '"quantclass_sync_internal.orchestrator.write_run_report"',
    '"quantclass_sync.load_catalog_or_raise"': '"quantclass_sync_internal.orchestrator.load_catalog_or_raise"',
    # cli 层
    '"quantclass_sync.run_update_with_settings"': '"quantclass_sync_internal.cli.run_update_with_settings"',
    '"quantclass_sync.resolve_credentials_for_update"': '"quantclass_sync_internal.cli.resolve_credentials_for_update"',
    '"quantclass_sync.resolve_report_path"': '"quantclass_sync_internal.cli.resolve_report_path"',
    '"quantclass_sync.sortable_products"': '"quantclass_sync_internal.cli.sortable_products"',
    '"quantclass_sync.repair_sort_product_files"': '"quantclass_sync_internal.cli.repair_sort_product_files"',
    '"quantclass_sync.app"': '"quantclass_sync_internal.cli.app"',
    # 兼容层专属（标记删除）
    '"quantclass_sync._bind_orchestrator_runtime"': "__DELETE_THIS_PATCH_LINE__",
}


def collect_qcs_symbols(text: str) -> set:
    """从文件内容中收集所有 qcs.XXX 符号名。"""
    return set(re.findall(r"\bqcs\.(\w+)", text))


def build_import_lines(symbols: set) -> str:
    """根据符号集合生成按模块分组的 import 行。"""
    by_module: dict = {}
    unknown = []
    for sym in sorted(symbols):
        module = SYMBOL_MODULE.get(sym)
        if module:
            by_module.setdefault(module, []).append(sym)
        else:
            unknown.append(sym)

    lines = []
    for module in sorted(by_module):
        syms = ", ".join(sorted(by_module[module]))
        lines.append(f"from quantclass_sync_internal.{module} import {syms}")

    if unknown:
        lines.append(f"# TODO: 未知符号需手动映射: {', '.join(sorted(unknown))}")

    return "\n".join(lines)


def migrate_file(path: Path, dry_run: bool = False) -> tuple:
    """迁移单个测试文件，返回 (changed, warnings)。"""
    text = path.read_text(encoding="utf-8")
    original = text
    warnings = []

    # 1. 收集 qcs.XXX 符号
    symbols = collect_qcs_symbols(text)
    if not symbols and "import quantclass_sync as qcs" not in text:
        return False, []

    # 2. 替换 import 行
    if "import quantclass_sync as qcs" in text:
        import_block = build_import_lines(symbols)
        text = text.replace("import quantclass_sync as qcs\n", import_block + "\n")
        # 有些文件可能还有 from quantclass_sync_internal 的 import，去重
        # 但为安全起见不自动去重，留给手动检查

    # 3. 替换 qcs.XXX → XXX
    for sym in sorted(symbols, key=len, reverse=True):  # 长符号优先，避免前缀匹配
        text = re.sub(r"\bqcs\." + sym + r"\b", sym, text)

    # 4. 替换 patch 路径
    for old, new in PATCH_REPLACEMENTS.items():
        if old in text:
            if new == "__DELETE_THIS_PATCH_LINE__":
                # 标记需要手动删除的行
                warnings.append(f"  需手动删除包含 {old} 的 patch 行")
            else:
                text = text.replace(old, new)

    # 5. 检查残留
    remaining = re.findall(r'"quantclass_sync\.[^"]*"', text)
    for r in remaining:
        if "quantclass_sync_internal" not in r:
            warnings.append(f"  残留 patch 路径: {r}")

    remaining_qcs = re.findall(r"\bqcs\.\w+", text)
    for r in remaining_qcs:
        warnings.append(f"  残留 qcs 引用: {r}")

    changed = text != original
    if changed and not dry_run:
        path.write_text(text, encoding="utf-8")

    return changed, warnings


def main():
    dry_run = "--dry-run" in sys.argv

    # 迁移所有测试文件
    test_files = sorted(TESTS_DIR.glob("test_*.py"))
    skip_files = {"test_import_compat.py", "test_architecture.py"}

    total_changed = 0
    all_warnings = []

    for path in test_files:
        if path.name in skip_files:
            continue
        changed, warnings = migrate_file(path, dry_run=dry_run)
        if changed:
            total_changed += 1
            status = "[DRY-RUN] " if dry_run else ""
            print(f"{status}Migrated: {path.name}")
            for w in warnings:
                print(f"  WARNING: {w}")
                all_warnings.append((path.name, w))
        elif warnings:
            for w in warnings:
                print(f"  WARNING ({path.name}): {w}")
                all_warnings.append((path.name, w))

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Total: {total_changed} files migrated")
    if all_warnings:
        print(f"Warnings: {len(all_warnings)} (need manual attention)")

    return 0 if not all_warnings else 1


if __name__ == "__main__":
    sys.exit(main())
