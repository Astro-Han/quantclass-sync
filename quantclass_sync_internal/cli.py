"""职责：定义 CLI 命令入口和命令级异常兜底。"""

from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import List, Optional, Tuple

import typer

from .config import (
    load_user_config_or_raise,
    load_user_secrets_or_raise,
    resolve_credentials_for_update,
    save_setup_artifacts_atomic,
)
from .constants import (
    DEFAULT_API_BASE,
    DEFAULT_CATALOG_FILE,
    DEFAULT_DATA_ROOT,
    DEFAULT_PROGRESS_EVERY,
    DEFAULT_REPORT_RETENTION_DAYS,
    DEFAULT_SECRETS_FILE,
    DEFAULT_USER_CONFIG_FILE,
    DEFAULT_USER_SECRETS_FILE,
    DEFAULT_WORK_DIR,
    PRODUCT_MODE_EXPLICIT_LIST,
    PRODUCT_MODE_LOCAL_SCAN,
    PRODUCT_MODES,
    REASON_MERGE_ERROR,
    REASON_OK,
    RUN_MODES,
)
from .file_sync import repair_sort_product_files, sortable_products
from .models import (
    CommandContext,
    ConsoleLogger,
    LOGGER,
    PROGRESS_EVERY,
    ProductPlan,
    ProductStatus,
    ProductSyncError,
    SyncStats,
    UserConfig,
    log_debug,
    log_error,
    log_info,
    normalize_product_name,
    split_products,
    utc_now_iso,
)
from .orchestrator import _append_result, _execute_plans, _finalize_and_write_report, _new_report, _build_headers, load_catalog_or_raise, run_update_with_settings
from .reporting import resolve_report_path
from .status_store import (
    cleanup_report_logs,
    cleanup_work_cache_aggressive,
    connect_status_db,
    export_status_json,
    load_product_status,
    report_dir_path,
    resolve_runtime_paths,
    status_json_path,
    upsert_product_status,
)

app = typer.Typer(
    help="QuantClass 数据同步工具（推荐 setup + update，兼容旧命令）",
    no_args_is_help=False,
    add_completion=False,
    pretty_exceptions_enable=False,
)

@app.callback(invoke_without_command=True)
def global_options(
    ctx: typer.Context,
    data_root: Optional[Path] = typer.Option(None, "--data-root", help="数据根目录（兼容命令可用）。"),
    api_key: str = typer.Option("", "--api-key", help="QuantClass API Key（高级参数）。", hidden=True),
    hid: str = typer.Option("", "--hid", help="QuantClass HID（高级参数）。", hidden=True),
    secrets_file: Optional[Path] = typer.Option(None, "--secrets-file", help="本地密钥文件路径（兼容命令可用）。"),
    config_file: Path = typer.Option(DEFAULT_USER_CONFIG_FILE, "--config-file", help="用户配置文件路径（setup/update）。"),
    dry_run: bool = typer.Option(False, "--dry-run", help="演练模式（不写业务数据和状态文件）。"),
    report_file: Optional[Path] = typer.Option(
        None, "--report-file", help="报告输出路径（JSON，高级参数）。", hidden=True
    ),
    stop_on_error: bool = typer.Option(False, "--stop-on-error", help="遇错即停（高级参数）。", hidden=True),
    verbose: bool = typer.Option(True, "--verbose/--no-verbose", help="显示调试日志（默认开启，可用 --no-verbose 关闭）。"),
) -> None:
    """
    全局参数（所有子命令共享）。

    这是 Typer 的回调（callback：每次执行任意子命令前都会先调用）。
    这里完成三件事：
    1) 初始化日志器（带 run_id，方便按次排障）
    2) 校验关键路径参数
    3) 把运行上下文写入 ctx.obj，供后续子命令复用
    """

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    global LOGGER, PROGRESS_EVERY
    LOGGER = ConsoleLogger(level="DEBUG" if verbose else "INFO", run_id=run_id)
    PROGRESS_EVERY = max(1, DEFAULT_PROGRESS_EVERY)

    resolved_data_root = data_root.resolve() if data_root else DEFAULT_DATA_ROOT.resolve()
    resolved_secrets_file = secrets_file.resolve() if secrets_file else DEFAULT_SECRETS_FILE.resolve()
    resolved_config_file = config_file.resolve()
    runtime_paths = resolve_runtime_paths(resolved_data_root)
    log_debug(
        "运行路径已解析。",
        event="PATHS",
        data_root=str(resolved_data_root),
        status_db=str(runtime_paths.status_db),
        status_json=str(runtime_paths.status_json),
        report_dir=str(runtime_paths.report_dir),
        source=runtime_paths.source,
    )

    # CommandContext 是“本次运行共享配置”，后续命令都从这里读取参数。
    command_ctx = CommandContext(
        run_id=run_id,
        data_root=resolved_data_root,
        data_root_from_cli=data_root is not None,
        api_key=api_key,
        hid=hid,
        secrets_file=resolved_secrets_file,
        secrets_file_from_cli=secrets_file is not None,
        config_file=resolved_config_file,
        dry_run=dry_run,
        report_file=report_file.resolve() if report_file else None,
        stop_on_error=stop_on_error,
        verbose=verbose,
        mode="network",
        api_base=DEFAULT_API_BASE,
        catalog_file=DEFAULT_CATALOG_FILE.resolve(),
        work_dir=DEFAULT_WORK_DIR.resolve(),
    )
    ctx.obj = command_ctx

    # 无子命令时做“首次引导”：
    # - 首次（无配置）：自动进入 setup
    # - 非首次（已有配置）：默认执行 update
    if ctx.invoked_subcommand is None:
        if not resolved_config_file.exists():
            if not sys.stdin.isatty():
                log_error(
                    f"未检测到配置文件：{resolved_config_file}；请先执行 setup（交互）或 setup --non-interactive。",
                    event="SETUP",
                )
                raise typer.Exit(code=1)
            log_info("未检测到用户配置，自动进入 setup。", event="SETUP", config_file=str(resolved_config_file))
            # 这里显式传 ctx，避免自动引导时丢失 setup 所需上下文参数。
            ctx.invoke(
                cmd_setup,
                ctx=ctx,
                non_interactive=False,
                skip_check=False,
                data_root="",
                api_key="",
                hid="",
                product_mode=PRODUCT_MODE_LOCAL_SCAN,
                products=[],
            )
            raise typer.Exit(code=0)
        # help 场景保持原行为：明确传 --help 时只展示帮助，不自动执行 update。
        argv_flags = set(sys.argv[1:])
        if getattr(ctx, "resilient_parsing", False) or {"-h", "--help"} & argv_flags:
            typer.echo(ctx.get_help())
            raise typer.Exit(code=0)

        log_info("检测到用户配置，默认执行 update。", event="CMD_START", command="update")
        ctx.invoke(
            cmd_update,
            ctx=ctx,
            dry_run=False,
            verbose=False,
            products=[],
            force_update=False,
        )
        raise typer.Exit(code=0)

def _ctx(ctx: typer.Context) -> CommandContext:
    value = ctx.obj
    if not isinstance(value, CommandContext):
        raise RuntimeError("运行上下文初始化失败；请通过命令行调用子命令。")
    return value

def _extract_command_context(args: tuple, kwargs: dict) -> Optional[CommandContext]:
    """从命令参数中提取 CommandContext（用于异常时决定是否打印调试堆栈）。"""

    raw_ctx = kwargs.get("ctx") or (args[0] if args else None)
    if raw_ctx is None:
        return None
    obj = getattr(raw_ctx, "obj", None)
    if isinstance(obj, CommandContext):
        return obj
    return None

def _handle_command_exception(command_name: str, exc: Exception, reason_code: str, args: tuple, kwargs: dict) -> None:
    """统一命令级异常输出，避免把冗长 traceback 直接暴露给普通用户。"""

    log_error(
        f"{command_name} 执行失败；可能原因：{exc}；建议：检查参数、网络和密钥后重试。",
        event="CMD_DONE",
        reason_code=reason_code,
    )
    command_ctx = _extract_command_context(args, kwargs)
    if command_ctx and command_ctx.verbose:
        log_debug(traceback.format_exc(), event="DEBUG")

def _cleanup_after_command(command_ctx: Optional[CommandContext]) -> None:
    """
    命令结束后统一执行缓存清理（失败不影响主流程）。

    设计目的：就算命令中途报错，也尽量保证缓存不会持续膨胀。
    """

    work_dir = command_ctx.work_dir if command_ctx is not None else DEFAULT_WORK_DIR.resolve()
    data_root = command_ctx.data_root if command_ctx is not None else DEFAULT_DATA_ROOT.resolve()
    try:
        cleanup_work_cache_aggressive(work_dir)
        log_debug("工作缓存已清理。", event="CACHE_CLEANUP", work_dir=str(work_dir))
    except Exception as exc:
        log_debug(f"工作缓存清理失败（已忽略）: {exc}", event="CACHE_CLEANUP")

    try:
        cleanup_report_logs(report_dir_path(data_root), retention_days=DEFAULT_REPORT_RETENTION_DAYS)
    except Exception as exc:
        log_debug(f"报告日志清理失败（已忽略）: {exc}", event="CACHE_CLEANUP")

def command_guard(command_name: str):
    """
    命令级异常兜底装饰器。

    目的：把未处理异常转换为清晰中文报错 + 非零退出码。
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # 正常执行子命令主逻辑。
                return func(*args, **kwargs)
            except typer.Exit:
                # 业务层主动退出（例如参数校验失败）直接向上抛。
                raise
            except ProductSyncError as exc:
                # 业务可识别错误：保留 reason_code，方便报告聚合。
                _handle_command_exception(command_name, exc, exc.reason_code, args, kwargs)
                raise typer.Exit(code=1)
            except Exception as exc:
                # 兜底未知错误：统一映射为 merge_error，避免漏报。
                _handle_command_exception(command_name, exc, REASON_MERGE_ERROR, args, kwargs)
                raise typer.Exit(code=1)
            finally:
                # 无论成功/失败，都会执行清理（finally 总会执行）。
                _cleanup_after_command(_extract_command_context(args, kwargs))

        return wrapper

    return decorator

def ensure_data_root_ready(data_root: Path, create_if_missing: bool = False) -> Path:
    """校验 data_root；需要时可自动创建目录。"""

    data_root = data_root.expanduser().resolve()
    if data_root.exists():
        if not data_root.is_dir():
            raise RuntimeError(f"data_root 不是目录：{data_root}")
        return data_root
    if create_if_missing:
        data_root.mkdir(parents=True, exist_ok=True)
        return data_root
    raise RuntimeError(f"data_root 不存在：{data_root}")

def _build_command_ctx_with_overrides(base_ctx: CommandContext, data_root: Path, secrets_file: Path) -> CommandContext:
    """基于基础上下文生成覆盖后的运行上下文。"""

    data_root = data_root.expanduser().resolve()
    secrets_file = secrets_file.expanduser().resolve()
    runtime_paths = resolve_runtime_paths(data_root)
    log_debug(
        "已应用运行配置。",
        event="PATHS",
        data_root=str(data_root),
        status_db=str(runtime_paths.status_db),
        status_json=str(runtime_paths.status_json),
        report_dir=str(runtime_paths.report_dir),
        source=runtime_paths.source,
    )
    return base_ctx.model_copy(
        update={
            "data_root": data_root,
            "secrets_file": secrets_file,
        }
    )

def _resolve_command_paths(
    base_ctx: CommandContext,
    require_user_config: bool = False,
) -> Tuple[Path, Path, Optional[UserConfig], str, str]:
    """
    统一解析 data_root / secrets_file 来源。

    优先级：
    1) 命令行显式参数
    2) user_config.json
    3) 代码默认值
    """

    user_config: Optional[UserConfig] = None
    if base_ctx.config_file.exists():
        # 只要配置文件存在，就强制校验其可读性，避免损坏配置被静默忽略。
        user_config = load_user_config_or_raise(base_ctx.config_file)
    elif require_user_config:
        raise RuntimeError(f"未找到用户配置文件：{base_ctx.config_file}；请先执行 setup。")

    data_root_source = "cli" if base_ctx.data_root_from_cli else "default"
    secrets_source = "cli" if base_ctx.secrets_file_from_cli else "default"
    data_root = base_ctx.data_root
    secrets_file = base_ctx.secrets_file

    if user_config is not None:
        if not base_ctx.data_root_from_cli:
            data_root = user_config.data_root.resolve()
            data_root_source = "config"
        if not base_ctx.secrets_file_from_cli:
            secrets_file = user_config.secrets_file.resolve()
            secrets_source = "config"

    return data_root.resolve(), secrets_file.resolve(), user_config, data_root_source, secrets_source

def _init_command(ctx: typer.Context, command_name: str) -> CommandContext:
    """兼容命令的统一初始化: 解析路径来源 -> 应用覆盖 -> 更新 ctx.obj -> 打印调试日志."""
    command_ctx = _ctx(ctx)
    data_root, secrets_file, _user_config, data_root_source, secrets_source = _resolve_command_paths(command_ctx)
    command_ctx = _build_command_ctx_with_overrides(command_ctx, data_root, secrets_file)
    ctx.obj = command_ctx
    log_debug(f"{command_name} 运行来源已解析.", event="PATHS",
              data_root_source=data_root_source, secrets_source=secrets_source)
    return command_ctx

@app.command("setup")
@command_guard("setup")
def cmd_setup(
    ctx: typer.Context,
    non_interactive: bool = typer.Option(False, "--non-interactive", help="非交互模式（需显式传参数）。"),
    skip_check: bool = typer.Option(False, "--skip-check", help="跳过连通性检查。"),
    data_root: str = typer.Option("", "--data-root", help="数据根目录。"),
    api_key: str = typer.Option("", "--api-key", help="用户 API Key。"),
    hid: str = typer.Option("", "--hid", help="用户 HID。"),
    product_mode: str = typer.Option(PRODUCT_MODE_LOCAL_SCAN, "--product-mode", help="local_scan 或 explicit_list。"),
    products: List[str] = typer.Option([], "--products", help="默认产品列表（可重复传参，也支持逗号分隔）。"),
) -> None:
    """
    初始化用户配置（首次运行推荐）。

    结果：
    1) 写入 user_config.json
    2) 写入 user_secrets.env
    3) 可选执行连通性检查
    """

    base_ctx = _ctx(ctx)
    existing_config: Optional[UserConfig] = None
    if base_ctx.config_file.exists():
        try:
            existing_config = load_user_config_or_raise(base_ctx.config_file)
        except Exception:
            # 旧配置损坏时允许重建，不阻断 setup。
            existing_config = None

    if non_interactive:
        raw_data_root = data_root.strip() or (str(existing_config.data_root) if existing_config else "")
        raw_api_key = api_key.strip() or os.environ.get("QUANTCLASS_API_KEY", "").strip()
        raw_hid = hid.strip() or os.environ.get("QUANTCLASS_HID", "").strip()
        mode = (product_mode or PRODUCT_MODE_LOCAL_SCAN).strip().lower()
        default_products = split_products(products)
    else:
        default_root = data_root.strip() or (str(existing_config.data_root) if existing_config else str(base_ctx.data_root))
        raw_data_root = typer.prompt("请输入数据目录(data_root)", default=default_root).strip()

        default_api_key = api_key.strip() or os.environ.get("QUANTCLASS_API_KEY", "").strip()
        default_hid = hid.strip() or os.environ.get("QUANTCLASS_HID", "").strip()
        raw_api_key = typer.prompt("请输入 API Key", default=default_api_key, hide_input=True).strip()
        raw_hid = typer.prompt("请输入 HID", default=default_hid, hide_input=True).strip()

        # 交互模式默认不再询问产品策略：
        # - product_mode 默认 local_scan
        # - default_products 默认空
        # 如需修改，可在 setup 时传 --product-mode/--products 覆盖，或后续重新执行 setup 调整。
        mode = (product_mode or PRODUCT_MODE_LOCAL_SCAN).strip().lower()
        default_products = split_products(products)

    if not raw_data_root:
        raise RuntimeError("setup 缺少 data_root；请提供数据目录。")
    data_root_path = ensure_data_root_ready(Path(raw_data_root), create_if_missing=True)
    setup_ctx = _build_command_ctx_with_overrides(base_ctx, data_root=data_root_path, secrets_file=base_ctx.secrets_file)
    ctx.obj = setup_ctx

    if mode not in PRODUCT_MODES:
        raise RuntimeError("product_mode 仅支持 local_scan 或 explicit_list。")
    if mode == PRODUCT_MODE_EXPLICIT_LIST and not default_products:
        raise RuntimeError("product_mode=explicit_list 时必须提供至少一个默认产品。")

    catalog = load_catalog_or_raise(base_ctx.catalog_file)
    catalog_set = {normalize_product_name(x) for x in catalog}
    invalid_defaults = [x for x in default_products if x not in catalog_set]
    if invalid_defaults:
        raise RuntimeError(f"默认产品不在 catalog 中：{', '.join(invalid_defaults)}")

    if not raw_api_key:
        raise RuntimeError("setup 缺少 API Key。")
    if not raw_hid:
        raise RuntimeError("setup 缺少 HID。")

    if base_ctx.secrets_file_from_cli:
        secrets_path = base_ctx.secrets_file
    elif existing_config is not None:
        secrets_path = existing_config.secrets_file.resolve()
    else:
        secrets_path = DEFAULT_USER_SECRETS_FILE.resolve()

    # 默认先做连通性检查，再写文件：
    # 这样检查失败时不会留下“新配置写了一半”的状态。
    if not skip_check:
        probe_product = default_products[0] if default_products else (catalog[0] if catalog else "stock-trading-data")
        headers = _build_headers(raw_api_key)
        try:
            get_latest_time(api_base=setup_ctx.api_base.rstrip("/"), product=probe_product, hid=raw_hid, headers=headers)
        except Exception as exc:
            raise RuntimeError(f"连通性检查失败；请检查 API Key/HID 或网络。原始错误：{exc}") from exc
        log_info("连通性检查通过。", event="SETUP", probe_product=probe_product)

    # 真正落盘时用“配置+密钥”一体化写入，任一步失败都会回滚。
    now = utc_now_iso()
    user_config = UserConfig(
        data_root=data_root_path,
        product_mode=mode,
        default_products=default_products,
        secrets_file=secrets_path,
        created_at=existing_config.created_at if existing_config else now,
        updated_at=now,
    )
    save_setup_artifacts_atomic(
        config_path=base_ctx.config_file,
        config=user_config,
        secrets_path=secrets_path,
        api_key=raw_api_key,
        hid=raw_hid,
    )

    log_info(
        "setup 完成。下一步建议先执行 update --dry-run。",
        event="SETUP",
        config_file=str(base_ctx.config_file),
        secrets_file=str(secrets_path),
    )

@app.command("update")
@command_guard("update")
def cmd_update(
    ctx: typer.Context,
    dry_run: bool = typer.Option(False, "--dry-run", help="演练模式（不写业务数据和状态文件）。"),
    verbose: bool = typer.Option(False, "--verbose", help="显示调试日志。"),
    products: List[str] = typer.Option([], "--products", help="临时覆盖默认产品清单。"),
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    一键更新入口（日常只需这个命令）。
    """

    base_ctx = _ctx(ctx)
    data_root, secrets_file, user_config, data_root_source, secrets_source = _resolve_command_paths(
        base_ctx,
        require_user_config=True,
    )
    if user_config is None:
        raise RuntimeError(f"未找到用户配置文件：{base_ctx.config_file}；请先执行 setup。")

    if verbose and LOGGER.level != "DEBUG":
        LOGGER.level = "DEBUG"
    run_ctx = _build_command_ctx_with_overrides(base_ctx, data_root=data_root, secrets_file=secrets_file)
    # update 明确固定优先级：CLI > setup secrets > ENV，
    # 解析后写回 run_ctx，避免后续流程再次按“旧优先级”重算。
    api_key, hid, credential_source = resolve_credentials_for_update(
        cli_api_key=run_ctx.api_key,
        cli_hid=run_ctx.hid,
        secrets_file=run_ctx.secrets_file.resolve(),
    )
    run_ctx = run_ctx.model_copy(
        update={
            "dry_run": base_ctx.dry_run or dry_run,
            "verbose": base_ctx.verbose or verbose,
            "api_key": api_key,
            "hid": hid,
        }
    )
    ctx.obj = run_ctx
    ensure_data_root_ready(run_ctx.data_root, create_if_missing=False)
    load_user_secrets_or_raise(run_ctx.secrets_file)
    log_debug(
        "update 运行来源已解析。",
        event="SETUP",
        data_root_source=data_root_source,
        secrets_source=secrets_source,
        credential_source=credential_source,
    )

    # update 产品优先级：
    # 1) 命令行 --products 临时覆盖
    # 2) explicit_list 使用配置里的 default_products
    # 3) local_scan 先扫本地，扫不到再走 fallback default_products
    explicit_products = split_products(products)
    fallback_products: List[str] = []
    selected_products: List[str] = explicit_products
    if not explicit_products:
        if user_config.product_mode == PRODUCT_MODE_EXPLICIT_LIST:
            if not user_config.default_products:
                raise RuntimeError("配置了 explicit_list，但 default_products 为空；请重新执行 setup。")
            selected_products = user_config.default_products
        else:
            fallback_products = user_config.default_products

    exit_code = run_update_with_settings(
        command_ctx=run_ctx,
        mode="local",
        products=selected_products,
        force_update=force_update,
        command_name="update",
        fallback_products=fallback_products,
    )
    log_info("update 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)

@app.command("repair_sort")
@command_guard("repair_sort")
def cmd_repair_sort(
    ctx: typer.Context,
    products: List[str] = typer.Option([], "--products", help="目标产品（可重复传参，也支持逗号分隔）。"),
    strict: bool = typer.Option(False, "--strict", help="严格模式：遇到文件错误立即失败。"),
) -> None:
    """
    排序修复命令（对历史 CSV 文件做全量排序治理）。
    """

    command_ctx = _init_command(ctx, "repair_sort")
    ensure_data_root_ready(command_ctx.data_root, create_if_missing=False)

    requested_products = split_products(products)
    eligible_products = sortable_products()
    selected_products = requested_products or eligible_products
    invalid_products = [x for x in selected_products if x not in eligible_products]
    if invalid_products:
        raise RuntimeError(f"以下产品不支持 repair_sort：{', '.join(sorted(set(invalid_products)))}")

    report = _new_report(command_ctx.run_id, mode="maintenance")
    report_path = resolve_report_path(command_ctx, "repair_sort")
    report.discovered_total = len(eligible_products)
    report.planned_total = len(selected_products)
    t_run_start = time.time()
    total = SyncStats()
    has_error = False

    if not selected_products:
        log_info("无可修复产品，repair_sort 结束。", event="SORT_REPAIR")
        exit_code = _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
        if exit_code != 0:
            raise typer.Exit(code=exit_code)
        return

    log_info(
        "开始执行 repair_sort。",
        event="CMD_START",
        products=len(selected_products),
        dry_run=command_ctx.dry_run,
        strict=strict,
    )

    for product in selected_products:
        t_product_start = time.time()
        plan = ProductPlan(name=product, strategy="repair_sort")
        try:
            stats, error_count = repair_sort_product_files(
                product=product,
                data_root=command_ctx.data_root,
                dry_run=command_ctx.dry_run,
                strict=strict,
            )
            elapsed = time.time() - t_product_start
            total.merge(stats)
            if error_count > 0:
                has_error = True
                message = f"产品 {product} 排序修复存在文件错误（count={error_count}）。"
                _append_result(
                    report,
                    product=plan.name,
                    status="error",
                    strategy=plan.strategy,
                    reason_code=REASON_MERGE_ERROR,
                    elapsed=elapsed,
                    error=message,
                )
                if strict:
                    break
                continue

            _append_result(
                report,
                product=product,
                status="ok",
                strategy=plan.strategy,
                reason_code=REASON_OK,
                elapsed=elapsed,
                stats=stats,
                source_path=str(command_ctx.data_root / product),
            )
        except Exception as exc:
            has_error = True
            elapsed = time.time() - t_product_start
            _append_result(
                report,
                product=plan.name,
                status="error",
                strategy=plan.strategy,
                reason_code=REASON_MERGE_ERROR,
                elapsed=elapsed,
                error=str(exc),
            )
            if strict:
                break

    exit_code = _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
    log_info("repair_sort 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)

@app.command("init")
@command_guard("init")
def cmd_init(ctx: typer.Context) -> None:
    """
    初始化产品状态快照（兼容命令）。

    这一步只更新状态文件，不下载数据。
    """

    command_ctx = _init_command(ctx, "init")
    ensure_data_root_ready(command_ctx.data_root, create_if_missing=True)
    t0 = time.time()
    log_info("开始执行 init。", event="CMD_START")

    catalog = load_catalog_or_raise(command_ctx.catalog_file)
    discovered = discover_local_products(command_ctx.data_root, catalog)
    local_set = {x.name for x in discovered}

    if command_ctx.dry_run:
        elapsed = time.time() - t0
        log_info(
            "dry-run：init 仅完成状态扫描预演，未写入状态库与状态 JSON。",
            event="CMD_DONE",
            products=len(catalog),
            discovered_local=len(local_set),
            elapsed=round(elapsed, 2),
        )
        return

    conn = connect_status_db(command_ctx.data_root)
    try:
        for product in catalog:
            old = load_product_status(conn, product)
            status = old or ProductStatus(name=product, display_name=product)
            status.display_name = status.display_name or product
            status.is_listed = 1
            status.can_auto_update = 1
            if product in local_set:
                status.last_update_time = utc_now_iso()
            upsert_product_status(conn, status)
        export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        conn.close()

    elapsed = time.time() - t0
    log_info("init 执行完成。", event="CMD_DONE", products=len(catalog), elapsed=round(elapsed, 2))

@app.command("one_data")
@command_guard("one_data")
def cmd_one_data(
    ctx: typer.Context,
    product: str = typer.Argument(..., help="产品英文名（可带 -daily）。"),
    date_time: str = typer.Option("", "--date-time", help="指定下载日期（可选）。"),
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    更新单个产品（兼容命令）。

    适合场景：排障、验证单个产品、减少批量更新的等待时间。
    """

    command_ctx = _init_command(ctx, "one_data")
    ensure_data_root_ready(command_ctx.data_root, create_if_missing=False)
    # one_data 的最小执行单元就是一个 ProductPlan。
    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, "one_data")
    plan = build_product_plan([normalize_product_name(product)])
    report.planned_total = len(plan)

    log_info("开始执行 one_data。", event="CMD_START", product=product)
    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        # 实际执行（含门控、下载、解压、落库、结果记录）。
        total, has_error, t_run_start = _execute_plans(
            plans=plan,
            command_ctx=command_ctx,
            report=report,
            requested_date_time=date_time.strip(),
            conn=conn,
            force_update=force_update,
            catch_up_to_latest=False,
        )
        if conn is not None:
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()
    exit_code = _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
    log_info("one_data 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)

@app.command("all_data")
@command_guard("all_data")
def cmd_all_data(
    ctx: typer.Context,
    mode: str = typer.Option("local", "--mode", help="local=本地存量更新；catalog=全量轮询。"),
    products: List[str] = typer.Option([], "--products", help="显式产品（可重复传参，也支持逗号分隔）。"),
    force_update: bool = typer.Option(False, "--force", help="强制更新：跳过 timestamp 门控。"),
) -> None:
    """
    批量更新产品（兼容命令）。

    mode=local：按本地已有产品更新（日常推荐）。
    mode=catalog：按 catalog 清单轮询（补齐或巡检时使用）。
    """

    command_ctx = _init_command(ctx, "all_data")
    exit_code = run_update_with_settings(
        command_ctx=command_ctx,
        mode=mode,
        products=products,
        force_update=force_update,
        command_name="all_data",
    )
    log_info("all_data 执行完成。", event="CMD_DONE", exit_code=exit_code)
    if exit_code != 0:
        raise typer.Exit(code=exit_code)

