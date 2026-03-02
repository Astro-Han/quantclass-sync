"""职责：串联 update 主流程（计划、下载、同步、预处理、报告）。"""

from __future__ import annotations

import inspect
import os
import shutil
import sqlite3
import traceback
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, NoReturn, Optional, Sequence, Tuple

from .archive import extract_archive
from .config import (
    build_product_plan,
    discover_local_products,
    ensure_data_root_ready,
    load_products_from_catalog,
    resolve_credentials,
    resolve_products_by_mode,
    validate_run_mode,
)
from .constants import (
    BUSINESS_DAY_ONLY_PRODUCTS,
    PREPROCESS_PRODUCT,
    PREPROCESS_TRIGGER_PRODUCTS,
    PRODUCT_MODE_EXPLICIT_LIST,
    PRODUCT_MODE_LOCAL_SCAN,
    REASON_EXTRACT_ERROR,
    REASON_MERGE_ERROR,
    REASON_NETWORK_ERROR,
    REASON_NO_DATA_FOR_DATE,
    REASON_NO_LOCAL_PRODUCTS,
    REASON_PREPROCESS_DRY_RUN,
    REASON_PREPROCESS_FAILED,
    REASON_PREPROCESS_FALLBACK_FULL_OK,
    REASON_PREPROCESS_FULL_REBUILD_OK,
    REASON_PREPROCESS_INCREMENTAL_OK,
    REASON_PREPROCESS_OK,
    REASON_PREPROCESS_SKIPPED_NO_DELTA,
    REASON_UP_TO_DATE,
)
from .http_client import (
    _reset_http_metrics,
    get_download_link,
    get_latest_time,
    get_latest_times,
    save_file,
    build_file_name,
    HTTP_ATTEMPTS_BY_PRODUCT,
    HTTP_FAILURES_BY_PRODUCT,
)
from .file_sync import sync_from_extract
from .models import (
    CommandContext,
    EmptyDownloadLinkError,
    FatalRequestError,
    ProductPlan,
    ProductRunResult,
    ProductStatus,
    ProductSyncError,
    RunReport,
    SyncStats,
    log_debug,
    log_error,
    log_info,
    normalize_product_name,
    split_products,
    utc_now_iso,
)
from .reporting import (
    _append_result,
    _finalize_and_write_report,
    _new_report,
    _record_discovery_skips,
    decide_exit_code,
    resolve_report_path,
)
from .status_store import (
    connect_status_db,
    export_status_json,
    load_product_status,
    normalize_data_date,
    read_local_timestamp_date,
    should_skip_by_timestamp,
    status_json_path,
    upsert_product_status,
    write_local_timestamp,
)

def process_product(
    plan: ProductPlan,
    date_time: Optional[str],
    api_base: str,
    hid: str,
    headers: Dict[str, str],
    data_root: Path,
    work_dir: Path,
    dry_run: bool,
    run_id: str = "",
) -> Tuple[str, str, SyncStats, str, str]:
    """
    处理单个产品完整流程。

    流程：
    1) 获取 latest（若未指定 date_time）
    2) 获取下载链接并下载
    3) 解压到 extract
    4) 根据策略同步到 data_root

    返回：
    (product, actual_time, stats, source_path, reason_code)
    """

    product = normalize_product_name(plan.name)
    t0 = time.time()

    log_info(f"[{product}] 开始处理，策略={plan.strategy}", event="PRODUCT_PLAN")

    # 第 1 步：确定本次要下载的业务日期（用户指定日期优先，否则取 latest）。
    actual_time = _resolve_actual_time(
        product=product,
        date_time=date_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
    )
    # 第 2 步：下载文件并准备解压目录。
    download_path, extract_path = _download_and_prepare_extract(
        product=product,
        actual_time=actual_time,
        api_base=api_base,
        hid=hid,
        headers=headers,
        work_dir=work_dir,
        run_id=run_id,
    )
    # 第 3 步：解压下载文件（支持 zip/tar/rar/7z）。
    _extract_product_archive(product=product, download_path=download_path, extract_path=extract_path)

    # 第 4 步：把 extract 目录中的数据同步到 data_root（这是“真正写业务数据”的阶段）。
    try:
        stats, reason_code = sync_from_extract(plan=plan, extract_path=extract_path, data_root=data_root, dry_run=dry_run)
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 落库失败；可能原因：文件结构异常或合并规则不匹配；"
                f"建议：先用 --dry-run 排查。原始错误：{exc}"
            ),
            reason_code=REASON_MERGE_ERROR,
        ) from exc
    if reason_code == REASON_MERGE_ERROR:
        raise ProductSyncError(
            message=(
                f"产品 {product} 落库检测到数据质量异常；"
                f"为避免错误推进 timestamp，本次按失败处理。"
            ),
            reason_code=REASON_MERGE_ERROR,
        )

    elapsed = time.time() - t0
    log_info(
        f"[{product}] 处理完成，用时 {elapsed:.2f}s",
        event="SYNC_OK",
        created=stats.created_files,
        updated=stats.updated_files,
        unchanged=stats.unchanged_files,
        skipped=stats.skipped_files,
        rows_added=stats.rows_added,
    )

    return product, actual_time, stats, str(extract_path), reason_code

def _resolve_actual_time(
    product: str,
    date_time: Optional[str],
    api_base: str,
    hid: str,
    headers: Dict[str, str],
) -> str:
    """解析单产品实际下载时间（优先用户指定，否则取 latest）。"""

    if date_time:
        return date_time
    try:
        return get_latest_time(api_base=api_base, product=product, hid=hid, headers=headers)
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 获取最新时间失败；可能原因：网络异常、权限不足或接口限制；"
                f"建议：检查 APIKEY/HID 与网络后重试。原始错误：{exc}"
            ),
            reason_code=REASON_NETWORK_ERROR,
        ) from exc

def _is_no_data_request_error(exc: Exception, *, allow_legacy_no_status: bool = False) -> bool:
    """判断是否属于“该日期无可下载数据”的请求错误。"""

    if not isinstance(exc, FatalRequestError):
        return False
    if exc.status_code == 404:
        return True
    if not allow_legacy_no_status or exc.status_code is not None:
        return False
    request_url = (exc.request_url or "").strip()
    # 兼容历史错误对象：可能缺少 status_code/request_url，但语义仍是“当天无数据”。
    return (not request_url) or ("/get-download-link/" in request_url)


def _is_empty_download_link_error(exc: Exception) -> bool:
    """判断是否是"空下载链接"错误。"""

    return isinstance(exc, EmptyDownloadLinkError)


def _raise_download_stage_error(product: str, exc: Exception) -> NoReturn:
    """把下载阶段底层异常统一映射为 ProductSyncError。"""

    if _is_no_data_request_error(exc):
        raise ProductSyncError(
            message=(
                f"产品 {product} 下载失败；该日期无可下载数据（HTTP 404）；"
                f"建议：确认产品与日期是否匹配。原始错误：{exc}"
            ),
            reason_code=REASON_NO_DATA_FOR_DATE,
        ) from exc
    raise ProductSyncError(
        message=(
            f"产品 {product} 下载失败；可能原因：网络波动、下载额度限制或链接失效；"
            f"建议：稍后重试并确认下载权限。原始错误：{exc}"
        ),
        reason_code=REASON_NETWORK_ERROR,
    ) from exc


def _download_and_prepare_extract(
    product: str,
    actual_time: str,
    api_base: str,
    hid: str,
    headers: Dict[str, str],
    work_dir: Path,
    run_id: str = "",
) -> Tuple[Path, Path]:
    """下载单产品文件并准备 extract 目录。"""

    file_url: str
    file_name: str
    try:
        file_url = get_download_link(api_base=api_base, product=product, date_time=actual_time, hid=hid, headers=headers)
        file_name = build_file_name(file_url, product, actual_time)
    except Exception as exc:
        _raise_download_stage_error(product=product, exc=exc)

    run_scope = (run_id or "").strip()
    if run_scope:
        product_work = work_dir / run_scope / product / actual_time
    else:
        product_work = work_dir / product / actual_time
    download_path = product_work / file_name
    extract_path = product_work / "extract"

    try:
        if extract_path.exists():
            shutil.rmtree(extract_path)
        extract_path.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 工作目录准备失败；可能原因：本地权限不足或缓存目录损坏；"
                f"建议：检查 {product_work} 权限后重试。原始错误：{exc}"
            ),
            reason_code=REASON_MERGE_ERROR,
        ) from exc

    try:
        _download_file_atomic(file_url=file_url, download_path=download_path, headers=headers, product=product)
    except Exception as exc:
        _raise_download_stage_error(product=product, exc=exc)

    log_info(f"[{product}] 下载完成: {download_path}", event="DOWNLOAD_OK")
    return download_path, extract_path


def _download_file_atomic(file_url: str, download_path: Path, headers: Dict[str, str], product: str) -> None:
    """下载到临时文件并原子替换，避免脏文件污染缓存。"""

    download_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = download_path.parent / f".{download_path.name}.part-{os.getpid()}-{time.time_ns()}"
    try:
        save_file(file_url=file_url, file_path=tmp_path, headers=headers, product=product)
        if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            raise RuntimeError("下载结果为空文件。")
        os.replace(tmp_path, download_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

def _extract_product_archive(product: str, download_path: Path, extract_path: Path) -> None:
    """解压单产品文件。"""

    try:
        extract_archive(download_path, extract_path)
        log_info(f"[{product}] 解压完成: {extract_path}", event="EXTRACT_OK")
    except Exception as exc:
        raise ProductSyncError(
            message=(
                f"产品 {product} 解压失败；可能原因：压缩包损坏或格式不支持；"
                f"建议：删除缓存后重试。原始错误：{exc}"
            ),
            reason_code=REASON_EXTRACT_ERROR,
        ) from exc

def load_catalog_or_raise(catalog_file: Path) -> List[str]:
    """读取 catalog，失败时抛异常。"""

    return load_products_from_catalog(catalog_file)

def _build_headers(api_key: str) -> Dict[str, str]:
    """构建标准 HTTP 请求头."""
    return {
        "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/100.0.4896.127 Safari/537.36"),
        "content-type": "application/json",
        "api-key": api_key,
    }

def build_headers_or_raise(ctx: CommandContext) -> Tuple[Dict[str, str], str]:
    """构建请求头并校验凭证。"""

    api_key, hid = resolve_credentials(cli_api_key=ctx.api_key, cli_hid=ctx.hid, secrets_file=ctx.secrets_file.resolve())
    if not api_key:
        raise RuntimeError(
            f"缺少 api-key；可能原因：命令行/环境变量/本地密钥文件都未提供；建议：配置 --api-key 或更新 {ctx.secrets_file}。"
        )
    if not hid:
        raise RuntimeError(f"缺少 hid；可能原因：命令行/环境变量/本地密钥文件都未提供；建议：配置 --hid 或更新 {ctx.secrets_file}。")

    return _build_headers(api_key), hid

def _parse_iso_date(raw: str) -> Optional[date]:
    normalized = normalize_data_date(raw)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except Exception:
        return None

def _is_business_day(raw: str) -> bool:
    """判断日期是否为业务日（周一~周五）。"""

    parsed = _parse_iso_date(raw)
    if parsed is None:
        return False
    return parsed.weekday() < 5

def _is_business_day_only_product(product: str) -> bool:
    """是否启用业务日裁剪。"""

    return normalize_product_name(product) in BUSINESS_DAY_ONLY_PRODUCTS

def _normalize_date_queue(
    raw_dates: Sequence[str],
    *,
    product: str,
    local_date: str = "",
    apply_business_day_filter: bool = True,
) -> List[str]:
    """
    标准化日期队列：归一化 -> 去重 -> 升序 -> 可选业务日裁剪 -> 可选 local_date 过滤。
    """

    normalized = sorted({x for x in (normalize_data_date(item) for item in raw_dates) if x})
    if local_date:
        normalized = [x for x in normalized if x > local_date]
    if apply_business_day_filter and _is_business_day_only_product(product):
        normalized = [x for x in normalized if _is_business_day(x)]
    return normalized

def _expected_catchup_dates(local_date: str, latest_date: str, product: str) -> List[str]:
    """计算 local+1 到 latest 的理论回补日期集合（按产品口径过滤）。"""

    start_obj = _parse_iso_date(local_date)
    end_obj = _parse_iso_date(latest_date)
    if start_obj is None or end_obj is None or start_obj >= end_obj:
        return []
    dates = list(_iter_calendar_dates((start_obj + timedelta(days=1)).isoformat(), end_obj.isoformat()))
    if _is_business_day_only_product(product):
        dates = [x for x in dates if _is_business_day(x)]
    return dates

def _iter_calendar_dates(start_date: str, end_date: str) -> Iterable[str]:
    """按天遍历 [start_date, end_date] 区间，输出 YYYY-MM-DD。"""

    start_obj = _parse_iso_date(start_date)
    end_obj = _parse_iso_date(end_date)
    if start_obj is None or end_obj is None or start_obj > end_obj:
        return []
    items: List[str] = []
    current = start_obj
    while current <= end_obj:
        items.append(current.isoformat())
        current += timedelta(days=1)
    return items

def _probe_downloadable_dates(
    api_base: str,
    product: str,
    hid: str,
    headers: Dict[str, str],
    local_date: str,
    latest_date: str,
) -> List[str]:
    """
    回退探测缺口日期。

    规则：
    - 404（含历史兼容的无 status no-data 异常）视为“该日无数据”，继续探测下一天；
    - 空下载链接错误视为“该日无数据”，继续探测下一天；
    - 其它错误视为硬失败，抛出异常交给严格模式处理。
    """

    start_obj = _parse_iso_date(local_date)
    end_obj = _parse_iso_date(latest_date)
    if start_obj is None or end_obj is None or start_obj >= end_obj:
        return []

    found: List[str] = []
    for day in _iter_calendar_dates((start_obj + timedelta(days=1)).isoformat(), end_obj.isoformat()):
        if _is_business_day_only_product(product) and not _is_business_day(day):
            log_debug(f"[{product}] 非业务日已跳过探测: {day}", event="PROBE_SKIP")
            continue
        try:
            get_download_link(api_base=api_base, product=product, date_time=day, hid=hid, headers=headers)
            found.append(day)
        except FatalRequestError as exc:
            if _is_no_data_request_error(exc, allow_legacy_no_status=True):
                log_debug(f"[{product}] 探测到无数据日，已跳过: {day}", event="PROBE_SKIP")
                continue
            raise
        except RuntimeError as exc:
            if _is_empty_download_link_error(exc):
                log_debug(f"[{product}] 探测到空下载链接，已跳过: {day}", event="PROBE_SKIP")
                continue
            raise
    return _normalize_date_queue(found, product=product)

def _should_probe_fallback(
    product: str,
    local_date: str,
    api_latest_date: Optional[str],
    api_latest_candidates: Sequence[str],
) -> bool:
    """判断是否需要启用逐日探测兜底。"""

    if not api_latest_date or local_date >= api_latest_date:
        return False

    local_obj = _parse_iso_date(local_date)
    latest_obj = _parse_iso_date(api_latest_date)
    if local_obj is None or latest_obj is None or local_obj >= latest_obj:
        return False

    expected_dates = _expected_catchup_dates(local_date=local_date, latest_date=api_latest_date, product=product)
    if not expected_dates:
        return False

    covered = set(
        _normalize_date_queue(api_latest_candidates, product=product, local_date=local_date)
    )
    # 候选日期未覆盖完整区间时，才启用逐日探测。
    return len(covered) < len(expected_dates)

def _resolve_requested_dates_for_plan(
    plan: ProductPlan,
    command_ctx: CommandContext,
    hid: str,
    headers: Dict[str, str],
    requested_date_time: str,
    force_update: bool,
    report: RunReport,
    t_product_start: float,
    catch_up_to_latest: bool = False,
) -> Tuple[List[str], bool]:
    """
    解析单产品执行日期列表，并处理 timestamp 门控。

    返回：
    - requested_dates_for_plan: 传给下载流程的日期列表（含空字符串时代表继续走 latest）
    - skipped_by_gate: 是否已经被门控判定为“跳过”
    """

    # 用户手动指定了日期，就以用户输入为准，不再做 latest/timestamp 判断。
    requested_date_for_plan = requested_date_time.strip()
    if force_update or requested_date_for_plan:
        return [requested_date_for_plan], False

    product_name = normalize_product_name(plan.name)
    try:
        # 1) 读取 API 可用日期列表（latest）
        api_latest_candidates = get_latest_times(
            api_base=command_ctx.api_base.rstrip("/"),
            product=product_name,
            hid=hid,
            headers=headers,
        )
        # latest 语义保持原样：这里不做业务日裁剪，只做规范化和去重排序。
        api_latest_candidates = _normalize_date_queue(
            api_latest_candidates,
            product=product_name,
            apply_business_day_filter=False,
        )
        api_latest_date = api_latest_candidates[-1] if api_latest_candidates else None
        # 2) 读取本地 timestamp 第一列日期
        local_date = read_local_timestamp_date(command_ctx.data_root, product_name)
        # 3) 如果本地已经不落后，则直接跳过，不进入下载链路
        if should_skip_by_timestamp(local_date, api_latest_date):
            elapsed = time.time() - t_product_start
            latest_raw = api_latest_date or ""
            _append_result(
                report,
                product=product_name,
                status="skipped",
                strategy=plan.strategy,
                reason_code=REASON_UP_TO_DATE,
                date_time=api_latest_date or latest_raw,
                mode="gate",
                elapsed=elapsed,
                error=f"本地 timestamp 已是最新（local={local_date}, api={api_latest_date}）。",
                stage="GATE",
                event_detail=f"local={local_date} api={api_latest_date}",
            )
            log_info(
                f"[{product_name}] timestamp 门控命中，跳过更新。",
                event="SYNC_SKIP",
                local_date=local_date,
                api_latest_date=api_latest_date,
                decision="skip",
            )
            return [], True

        # 非回补模式：保持“单次只跑 latest 一次”的旧行为。
        if not catch_up_to_latest:
            if api_latest_date:
                return [api_latest_date], False
            return [""], False

        # 回补模式：无本地基线时只能跑 latest 一次。
        if not local_date:
            if api_latest_date:
                return [api_latest_date], False
            return [""], False

        catchup_dates = _normalize_date_queue(api_latest_candidates, product=product_name, local_date=local_date)
        if _should_probe_fallback(
            product=product_name,
            local_date=local_date,
            api_latest_date=api_latest_date,
            api_latest_candidates=api_latest_candidates,
        ):
            # 列表缺失或不完整时，用逐日探测补齐。
            probed_dates = _probe_downloadable_dates(
                api_base=command_ctx.api_base.rstrip("/"),
                product=product_name,
                hid=hid,
                headers=headers,
                local_date=local_date,
                latest_date=api_latest_date,
            )
            catchup_dates = _normalize_date_queue(
                list(catchup_dates) + list(probed_dates),
                product=product_name,
                local_date=local_date,
            )
            if not catchup_dates and api_latest_date:
                catchup_dates = [api_latest_date]

        log_info(
            f"[{product_name}] timestamp 门控通过，执行更新。",
            event="PRODUCT_PLAN",
            local_date=local_date or "",
            api_latest_date=api_latest_date or "",
            catchup_dates=len(catchup_dates),
            decision="run",
        )
        return catchup_dates, False
    except Exception as exc:
        # 门控异常时采用 fail-open（失败放行）策略，避免“该更新却被误跳过”。
        log_info(
            f"[{plan.name}] timestamp 门控异常，回退执行更新。",
            event="PRODUCT_PLAN",
            decision="fallback_run",
            error=str(exc),
        )
        return [requested_date_for_plan], False

def _upsert_product_status_after_success(
    conn: Optional[sqlite3.Connection],
    command_ctx: CommandContext,
    product: str,
    actual_time: str,
) -> None:
    """在成功路径统一更新状态库与 timestamp 文件。"""

    if conn is None or command_ctx.dry_run:
        return
    old_status = load_product_status(conn, product)
    status = old_status or ProductStatus(name=product, display_name=product)
    status.last_update_time = utc_now_iso()
    status.data_time = actual_time
    status.data_content_time = actual_time
    upsert_product_status(conn, status)
    write_local_timestamp(command_ctx.data_root, product, actual_time)


def _callable_accepts_run_id(func: object) -> bool:
    """判断可调用对象是否可接收 run_id 参数（兼容历史测试桩）。"""

    target = func
    side_effect = getattr(func, "side_effect", None)
    if callable(side_effect):
        target = side_effect

    try:
        signature = inspect.signature(target)
    except (TypeError, ValueError):
        return False

    if "run_id" in signature.parameters:
        return True
    return any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())

def _collect_preprocess_source_successes(report: RunReport) -> List[ProductRunResult]:
    """
    收集本轮已成功更新的“预处理依赖源产品”。

    预处理只在这三个源产品至少一个真正更新成功时才触发，
    这样可以避免每次 update 都重复跑一遍重计算任务。
    """

    successes: List[ProductRunResult] = []
    for item in report.products:
        if item.status != "ok":
            continue
        if normalize_product_name(item.product) not in PREPROCESS_TRIGGER_PRODUCTS:
            continue
        successes.append(item)
    return successes

def _has_effective_source_delta(item: ProductRunResult) -> bool:
    """
    判断单个源产品是否存在“有效增量”。

    只把 created/updated/rows_added 视为有效变化，
    避免仅 mtime 触发但内容未变时重复执行重计算。
    """

    stats = item.stats
    return (stats.created_files + stats.updated_files + stats.rows_added) > 0

def _run_builtin_coin_preprocess(command_ctx: CommandContext) -> Tuple[str, str]:
    """
    执行包内置预处理逻辑（默认路径）。

    设计目标：让分发包在用户机器上开箱即用，不依赖额外环境变量。
    """

    try:
        from coin_preprocess_builtin import run_coin_preprocess_builtin
    except Exception as exc:
        raise RuntimeError(
            "内置预处理模块加载失败；请确认依赖已安装（例如 pandas）。"
        ) from exc

    summary = run_coin_preprocess_builtin(command_ctx.data_root)
    reason_by_mode = {
        "incremental_patch": REASON_PREPROCESS_INCREMENTAL_OK,
        "full_rebuild": REASON_PREPROCESS_FULL_REBUILD_OK,
        "fallback_full_rebuild": REASON_PREPROCESS_FALLBACK_FULL_OK,
    }
    reason_code = reason_by_mode.get(summary.mode, REASON_PREPROCESS_OK)
    source_path = (
        f"builtin(mode={summary.mode},changed={summary.changed_symbols},"
        f"spot={summary.spot_symbols},swap={summary.swap_symbols},output={summary.output_dir})"
    )
    return source_path, reason_code

def _resolve_preprocess_data_date(
    command_ctx: CommandContext,
    source_successes: Sequence[ProductRunResult],
) -> str:
    """
    计算预处理产品写入状态时使用的数据日期。

    优先使用“本轮成功源产品”的 date_time；
    若异常缺失，再回退读取本地 timestamp。
    """

    candidates: List[str] = []
    for item in source_successes:
        normalized = normalize_data_date(item.date_time)
        if normalized:
            candidates.append(normalized)
    if candidates:
        return max(candidates)

    for product in PREPROCESS_TRIGGER_PRODUCTS:
        local_date = read_local_timestamp_date(command_ctx.data_root, product)
        if local_date:
            candidates.append(local_date)
    if candidates:
        return max(candidates)
    return datetime.now().date().isoformat()

def _maybe_run_coin_preprocess(
    command_ctx: CommandContext,
    report: RunReport,
    conn: Optional[sqlite3.Connection],
) -> bool:
    """
    条件触发币圈合成预处理。

    返回值：
    - True: 预处理阶段发生错误（应计入本次 update 失败）
    - False: 未触发或执行成功
    """

    phase_start = time.time()
    preprocess_dir = command_ctx.data_root / PREPROCESS_PRODUCT
    if not preprocess_dir.is_dir():
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return False

    source_successes = _collect_preprocess_source_successes(report)
    if not source_successes:
        log_debug(
            "预处理跳过：本轮源产品无成功更新。",
            event="PREPROCESS",
            target=PREPROCESS_PRODUCT,
        )
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return False
    source_effective = [item for item in source_successes if _has_effective_source_delta(item)]
    if not source_effective:
        _append_result(
            report,
            product=PREPROCESS_PRODUCT,
            status="skipped",
            strategy="preprocess_hook",
            reason_code=REASON_PREPROCESS_SKIPPED_NO_DELTA,
            mode="postprocess",
            error="源产品无有效增量（created/updated/rows_added 均为 0），跳过预处理。",
            stage="PREPROCESS",
        )
        log_info(
            "预处理跳过：源产品无有效增量。",
            event="PREPROCESS",
            target=PREPROCESS_PRODUCT,
        )
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return False

    if command_ctx.dry_run:
        _append_result(
            report,
            product=PREPROCESS_PRODUCT,
            status="skipped",
            strategy="preprocess_hook",
            reason_code=REASON_PREPROCESS_DRY_RUN,
            mode="postprocess",
            error="dry-run 模式：未执行预处理命令。",
            stage="PREPROCESS",
        )
        log_info("dry-run 模式下跳过预处理执行。", event="PREPROCESS", target=PREPROCESS_PRODUCT)
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return False

    t0 = time.time()
    raw_cmd = ""
    try:
        # 统一使用仓库内置预处理实现，降低分发后的使用门槛。
        raw_cmd, success_reason_code = _run_builtin_coin_preprocess(command_ctx)
        elapsed = time.time() - t0

        actual_time = _resolve_preprocess_data_date(command_ctx, source_effective)
        _upsert_product_status_after_success(
            conn=conn,
            command_ctx=command_ctx,
            product=PREPROCESS_PRODUCT,
            actual_time=actual_time,
        )
        _append_result(
            report,
            product=PREPROCESS_PRODUCT,
            status="ok",
            strategy="preprocess_hook",
            reason_code=success_reason_code,
            elapsed=elapsed,
            date_time=actual_time,
            mode="postprocess",
            source_path=raw_cmd,
            stage="PREPROCESS",
        )
        log_info(
            "预处理执行成功。",
            event="PREPROCESS",
            target=PREPROCESS_PRODUCT,
            data_time=actual_time,
            command=raw_cmd,
        )
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return False
    except Exception as exc:
        elapsed = time.time() - t0
        message = str(exc)
        _append_result(
            report,
            product=PREPROCESS_PRODUCT,
            status="error",
            strategy="preprocess_hook",
            reason_code=REASON_PREPROCESS_FAILED,
            elapsed=elapsed,
            mode="postprocess",
            error=message,
            source_path=raw_cmd,
            stage="PREPROCESS",
        )
        log_error(f"预处理执行异常: {message}", event="PREPROCESS")
        if command_ctx.verbose:
            log_debug(traceback.format_exc(), event="DEBUG")
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)
        return True

def _execute_plans(
    plans: Sequence[ProductPlan],
    command_ctx: CommandContext,
    report: RunReport,
    requested_date_time: str = "",
    conn: Optional[sqlite3.Connection] = None,
    force_update: bool = False,
    catch_up_to_latest: bool = False,
) -> Tuple[SyncStats, bool, float]:
    """
    执行产品计划并返回汇总统计。

    整体流程（单次运行）：
    1) 先构建请求头与凭证（API Key/HID）
    2) 逐个产品执行“门控判断 -> 解析日期队列 -> 下载解压 -> 同步落库 -> 记录结果”
    3) 累加统计并在必要时中断（stop-on-error）
    """

    _reset_http_metrics()
    headers, hid = build_headers_or_raise(command_ctx)
    total = SyncStats()
    has_error = False
    t_run_start = time.time()

    for plan in plans:
        t_product_start = time.time()
        # A. 判断这个产品本次应不应该跑（门控命中会直接 continue）。
        t_plan_phase = time.time()
        requested_dates_for_plan, skipped_by_gate = _resolve_requested_dates_for_plan(
            plan=plan,
            command_ctx=command_ctx,
            hid=hid,
            headers=headers,
            requested_date_time=requested_date_time,
            force_update=force_update,
            report=report,
            t_product_start=t_product_start,
            catch_up_to_latest=catch_up_to_latest,
        )
        report.phase_plan_seconds += max(0.0, time.time() - t_plan_phase)
        if skipped_by_gate:
            continue

        if not requested_dates_for_plan:
            if catch_up_to_latest:
                elapsed = time.time() - t_product_start
                _append_result(
                    report,
                    product=plan.name,
                    status="skipped",
                    strategy=plan.strategy,
                    reason_code=REASON_NO_DATA_FOR_DATE,
                    elapsed=elapsed,
                    mode="gate",
                    error="catch-up 日期队列为空，已跳过本产品。",
                    stage="PLAN",
                    event_detail="catchup_dates=0",
                )
                log_info(
                    f"[{plan.name}] catch-up 日期队列为空，已跳过。",
                    event="SYNC_SKIP",
                    reason_code=REASON_NO_DATA_FOR_DATE,
                )
                continue
            requested_dates_for_plan = [""]

        # B. 按日期队列执行单产品同步（网络 + 解压 + 文件同步）。
        for requested_date_for_plan in requested_dates_for_plan:
            debug_trace = ""
            t_sync_phase = time.time()
            try:
                process_kwargs = {
                    "plan": plan,
                    "date_time": requested_date_for_plan or None,
                    "api_base": command_ctx.api_base.rstrip("/"),
                    "hid": hid,
                    "headers": headers,
                    "data_root": command_ctx.data_root,
                    "work_dir": command_ctx.work_dir,
                    "dry_run": command_ctx.dry_run,
                }
                if _callable_accepts_run_id(process_product):
                    process_kwargs["run_id"] = command_ctx.run_id
                product, actual_time, stats, source_path, reason_code = process_product(
                    **process_kwargs,
                )
                elapsed = time.time() - t_product_start
                total.merge(stats)
                # 先完成状态落盘，再记录 ok，避免状态写失败时出现 ok/error 双记录。
                _upsert_product_status_after_success(
                    conn=conn,
                    command_ctx=command_ctx,
                    product=product,
                    actual_time=actual_time,
                )
                _append_result(
                    report,
                    product=product,
                    status="ok",
                    strategy=plan.strategy,
                    reason_code=reason_code,
                    date_time=actual_time,
                    elapsed=elapsed,
                    stats=stats,
                    source_path=source_path,
                )
                report.phase_sync_seconds += max(0.0, time.time() - t_sync_phase)
                continue
            except ProductSyncError as exc:
                # 可预期业务错误：带有明确 reason_code。
                reason_code = exc.reason_code
                message = str(exc)
                debug_trace = traceback.format_exc()
            except Exception as exc:
                # 兜底未知异常：统一归并为 merge_error，避免丢失错误。
                reason_code = REASON_MERGE_ERROR
                message = str(exc)
                debug_trace = traceback.format_exc()
            report.phase_sync_seconds += max(0.0, time.time() - t_sync_phase)

            elapsed = time.time() - t_product_start
            # D. 回补模式：无数据日记录为 skipped，继续下一个日期。
            if catch_up_to_latest and reason_code == REASON_NO_DATA_FOR_DATE:
                _append_result(
                    report,
                    product=plan.name,
                    status="skipped",
                    strategy=plan.strategy,
                    reason_code=reason_code,
                    date_time=requested_date_for_plan,
                    elapsed=elapsed,
                    error=message,
                )
                log_info(f"[{plan.name}] 无数据日已跳过: {message}", event="SYNC_SKIP", reason_code=reason_code)
                if command_ctx.verbose and debug_trace:
                    log_debug(debug_trace, event="DEBUG")
                continue

            # E. 失败路径：记录错误；若开启 stop-on-error 则终止整次任务。
            has_error = True
            _append_result(
                report,
                product=plan.name,
                status="error",
                strategy=plan.strategy,
                reason_code=reason_code,
                date_time=requested_date_for_plan,
                elapsed=elapsed,
                error=message,
            )
            log_error(f"[{plan.name}] 处理失败: {message}", event="SYNC_FAIL", reason_code=reason_code)
            if command_ctx.verbose and debug_trace:
                log_debug(debug_trace, event="DEBUG")
            if command_ctx.stop_on_error:
                log_error("已开启 stop-on-error，任务提前停止。", event="RUN_SUMMARY")
                command_ctx.http_attempts_by_product = dict(HTTP_ATTEMPTS_BY_PRODUCT)
                command_ctx.http_failures_by_product = dict(HTTP_FAILURES_BY_PRODUCT)
                return total, has_error, t_run_start
            # 未开启 stop-on-error：仅终止当前产品后续日期，继续下一个产品。
            break

    command_ctx.http_attempts_by_product = dict(HTTP_ATTEMPTS_BY_PRODUCT)
    command_ctx.http_failures_by_product = dict(HTTP_FAILURES_BY_PRODUCT)
    return total, has_error, t_run_start

def run_update_with_settings(
    command_ctx: CommandContext,
    mode: str = "local",
    products: Optional[Sequence[str]] = None,
    force_update: bool = False,
    command_name: str = "all_data",
    fallback_products: Optional[Sequence[str]] = None,
) -> int:
    """
    通用批量更新执行器（update/all_data 共用）。

    fallback_products 用于“本地扫描为空”时的回退清单。
    """

    ensure_data_root_ready(command_ctx.data_root, create_if_missing=False)

    mode = validate_run_mode(mode)
    t_run_start = time.time()

    product_args = list(products or [])
    fallback_args = list(fallback_products or [])

    report = _new_report(command_ctx.run_id, mode="network")
    report_path = resolve_report_path(command_ctx, command_name)
    catalog_products = load_catalog_or_raise(command_ctx.catalog_file)
    catalog_set = {normalize_product_name(x) for x in catalog_products}

    log_info("开始扫描本地产品目录。", event="DISCOVER_START", data_root=str(command_ctx.data_root), mode=mode)
    discovered = discover_local_products(data_root=command_ctx.data_root, catalog_products=catalog_products)
    report.discovered_total = len(discovered)
    log_info("本地产品扫描完成。", event="DISCOVER_DONE", discovered_total=report.discovered_total)

    planned_products, unknown_local, invalid_explicit = resolve_products_by_mode(
        mode=mode,
        raw_products=product_args,
        catalog_products=catalog_products,
        discovered_local=discovered,
    )
    _record_discovery_skips(report, unknown_local, invalid_explicit)

    # update 模式：本地扫描为空时，可回退到默认产品清单。
    # 这样新用户即使 data_root 里暂时没有目录，也能按 setup 配置完成首轮更新。
    if mode == "local" and not planned_products and not product_args and fallback_args:
        fallback = split_products(fallback_args)
        fallback_valid = [x for x in fallback if x in catalog_set]
        fallback_invalid = [x for x in fallback if x not in catalog_set]
        if fallback_invalid:
            _record_discovery_skips(report, unknown_local=[], invalid_explicit=fallback_invalid)
        if fallback_valid:
            planned_products = fallback_valid
            log_info(
                "本地扫描为空，已回退到默认产品清单。",
                event="PLAN",
                fallback_total=len(fallback_valid),
            )

    if mode == "local" and not planned_products and not product_args:
        log_error(
            "未发现可更新产品；可先执行 setup 配置默认产品清单。",
            event="RUN_SUMMARY",
            reason_code=REASON_NO_LOCAL_PRODUCTS,
        )
        _finalize_and_write_report(
            report=report,
            total=SyncStats(),
            has_error=True,
            t_run_start=t_run_start,
            report_path=report_path,
        )
        return decide_exit_code(
            report=report,
            has_error=True,
            no_executable_products=True,
        )

    plans = build_product_plan(planned_products)
    report.planned_total = len(plans)
    if not plans:
        log_error("执行清单为空，任务结束。", event="RUN_SUMMARY", reason_code=REASON_NO_LOCAL_PRODUCTS)
        _finalize_and_write_report(
            report=report,
            total=SyncStats(),
            has_error=True,
            t_run_start=t_run_start,
            report_path=report_path,
        )
        return decide_exit_code(
            report=report,
            has_error=True,
            no_executable_products=True,
        )

    conn: Optional[sqlite3.Connection] = None
    if not command_ctx.dry_run:
        conn = connect_status_db(command_ctx.data_root)
    try:
        total, has_error, t_run_start = _execute_plans(
            plans,
            command_ctx,
            report,
            requested_date_time="",
            conn=conn,
            force_update=force_update,
            catch_up_to_latest=True,
        )
        preprocess_has_error = _maybe_run_coin_preprocess(
            command_ctx=command_ctx,
            report=report,
            conn=conn,
        )
        has_error = has_error or preprocess_has_error
        if conn is not None:
            export_status_json(conn, status_json_path(command_ctx.data_root))
    finally:
        if conn is not None:
            conn.close()

    return _finalize_and_write_report(report, total, has_error, t_run_start, report_path)
