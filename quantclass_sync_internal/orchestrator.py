"""职责：串联 update 主流程（计划、下载、同步、预处理、报告）。"""

from __future__ import annotations

import contextlib
import shutil
import sqlite3
import threading
import traceback
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .archive import extract_archive
from .config import (
    atomic_temp_path,
    build_product_plan,
    discover_local_products,
    ensure_data_root_ready,
    load_products_from_catalog,
    resolve_credentials,
    resolve_products_by_mode,
    validate_run_mode,
)
from .constants import (
    API_DATE_CACHE_TTL_SECONDS,
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
    REASON_NO_VALID_OUTPUT,
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
from .data_query import infer_local_date_from_csv
from .file_sync import sync_from_extract
from .models import (
    CommandContext,
    EmptyDownloadLinkError,
    EstimateResult,
    FatalRequestError,
    ProductPlan,
    ProductRunResult,
    ProductStatus,
    ProductSyncError,
    RULES,
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
    export_status_json,
    load_api_latest_dates,
    load_product_status,
    normalize_data_date,
    open_status_db,
    read_local_timestamp_date,
    report_dir_path,
    should_skip_by_timestamp,
    status_json_path,
    update_api_latest_dates,
    upsert_product_status,
    write_local_timestamp,
)


def _is_cache_fresh(checked_at_str: str) -> bool:
    """检查缓存时间戳是否在 TTL 内。旧格式（无 T）视为过期。"""
    if "T" not in checked_at_str:
        return False
    try:
        checked_at = datetime.strptime(checked_at_str, "%Y-%m-%dT%H:%M:%S")
        return (datetime.now() - checked_at).total_seconds() < API_DATE_CACHE_TTL_SECONDS
    except ValueError:
        return False


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
    if date_time:
        actual_time = date_time
    else:
        try:
            actual_time = get_latest_time(api_base=api_base, product=product, hid=hid, headers=headers)
        except Exception as exc:
            raise ProductSyncError(
                message=(
                    f"产品 {product} 获取最新时间失败；可能原因：网络异常、权限不足或接口限制；"
                    f"建议：检查 APIKEY/HID 与网络后重试。原始错误：{exc}"
                ),
                reason_code=REASON_NETWORK_ERROR,
            ) from exc
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

    # 第 4 步：把 extract 目录中的数据同步到 data_root（这是"真正写业务数据"的阶段）。
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
        append_fast=stats.append_fast_files,
    )

    return product, actual_time, stats, str(extract_path), reason_code

def _is_no_data_error(exc: Exception, *, allow_legacy_no_status: bool = False) -> bool:
    """
    判断是否属于"该日期无可下载数据"的错误，涵盖以下情况：
    - FatalRequestError 404（标准"无数据"响应）
    - FatalRequestError 无 status_code（历史兼容，allow_legacy_no_status=True 时启用）
    - EmptyDownloadLinkError（API 返回空下载链接）
    - RuntimeError 包装的 EmptyDownloadLinkError

    _probe_downloadable_dates 以 allow_legacy_no_status=True 调用，用于兼容历史无 status_code 对象。
    """

    if isinstance(exc, EmptyDownloadLinkError):
        return True
    if not isinstance(exc, FatalRequestError):
        return False
    if exc.status_code == 404:
        return True
    if not allow_legacy_no_status or exc.status_code is not None:
        return False
    request_url = (exc.request_url or "").strip()
    # 兼容历史错误对象：可能缺少 status_code/request_url，但语义仍是"当天无数据"。
    return (not request_url) or ("/get-download-link/" in request_url)


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
        # 获取下载链接失败：404/空链接映射为"无数据"，其它映射为网络错误。
        if _is_no_data_error(exc):
            raise ProductSyncError(
                message=(
                    f"产品 {product} 下载失败；该日期无可下载数据（HTTP 404/空下载链接）；"
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
        # 文件下载失败：404/空链接映射为"无数据"，其它映射为网络错误。
        if _is_no_data_error(exc):
            raise ProductSyncError(
                message=(
                    f"产品 {product} 下载失败；该日期无可下载数据（HTTP 404/空下载链接）；"
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

    log_info(f"[{product}] 下载完成: {download_path}", event="DOWNLOAD_OK")
    return download_path, extract_path


def _download_file_atomic(file_url: str, download_path: Path, headers: Dict[str, str], product: str) -> None:
    """下载到临时文件并原子替换，避免脏文件污染缓存。"""

    download_path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_temp_path(download_path, tag="part") as tmp:
        # save_file 流式写入二进制数据（内部使用 shutil.copyfileobj 或类似方式）
        save_file(file_url=file_url, file_path=tmp, headers=headers, product=product)
        # 空文件校验：下载完成但文件为空，视为失败（不允许推进到 os.replace）
        if not tmp.exists() or tmp.stat().st_size <= 0:
            raise RuntimeError("下载结果为空文件。")

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
    - 404（含历史兼容的无 status no-data 异常）视为"该日无数据"，继续探测下一天；
    - 空下载链接错误视为"该日无数据"，继续探测下一天；
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
        except (FatalRequestError, EmptyDownloadLinkError, RuntimeError) as exc:
            if _is_no_data_error(exc, allow_legacy_no_status=True):
                log_debug(f"[{product}] 探测到无数据日，已跳过: {day}", event="PROBE_SKIP")
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
    lock: Optional[threading.Lock] = None,
    api_date_cache: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[List[str], bool]:
    """
    解析单产品执行日期列表，并处理 timestamp 门控。

    返回：
    - requested_dates_for_plan: 传给下载流程的日期列表（含空字符串时代表继续走 latest）
    - skipped_by_gate: 是否已经被门控判定为"跳过"
    """

    # 用户手动指定了日期，就以用户输入为准，不再做 latest/timestamp 判断。
    requested_date_for_plan = requested_date_time.strip()
    if force_update or requested_date_for_plan:
        return [requested_date_for_plan], False

    product_name = normalize_product_name(plan.name)

    # 缓存检查：check_updates 已查过且未过期时跳过 HTTP
    cache_hit = False
    api_latest_candidates: List[str] = []
    if api_date_cache:
        cached = api_date_cache.get(product_name) or api_date_cache.get(plan.name)
        if cached:
            cached_date, checked_at_str = cached
            if _is_cache_fresh(checked_at_str):
                # 计算缓存年龄用于日志
                try:
                    checked_at = datetime.strptime(checked_at_str, "%Y-%m-%dT%H:%M:%S")
                    age_seconds = (datetime.now() - checked_at).total_seconds()
                except ValueError:
                    age_seconds = 0.0
                log_info(
                    f"[{plan.name}] 使用缓存 API 日期 {cached_date}（{int(age_seconds)}s 前查询）",
                    event="PRODUCT_PLAN", decision="cache_hit",
                )
                api_latest_candidates = [cached_date]
                cache_hit = True

    if not cache_hit:
        try:
            # 1) 读取 API 可用日期列表（latest）
            api_latest_candidates = get_latest_times(
                api_base=command_ctx.api_base.rstrip("/"),
                product=product_name,
                hid=hid,
                headers=headers,
            )
        except Exception as exc:
            # latest 获取失败时保持 fail-open，继续执行旧兜底路径。
            log_info(
                f"[{plan.name}] timestamp 门控异常，回退执行更新。",
                event="PRODUCT_PLAN",
                decision="fallback_run",
                error=str(exc),
            )
            return [requested_date_for_plan], False

    # latest 语义保持原样：这里不做业务日裁剪，只做规范化和去重排序。
    api_latest_candidates = _normalize_date_queue(
        api_latest_candidates,
        product=product_name,
        apply_business_day_filter=False,
    )
    api_latest_date = api_latest_candidates[-1] if api_latest_candidates else None

    try:
        # 2) 读取本地 timestamp 第一列日期
        local_date = read_local_timestamp_date(command_ctx.data_root, product_name)
        # 3) 如果本地已经不落后，则直接跳过，不进入下载链路
        if should_skip_by_timestamp(local_date, api_latest_date):
            elapsed = time.time() - t_product_start
            latest_raw = api_latest_date or ""
            # 并发路径下 lock 由调用方传入，保护 report 写入的线程安全
            with lock if lock is not None else contextlib.nullcontext():
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
                )
            log_info(
                f"[{product_name}] timestamp 门控命中，跳过更新。",
                event="SYNC_SKIP",
                local_date=local_date,
                api_latest_date=api_latest_date,
                decision="skip",
            )
            return [], True

        # 非回补模式：保持"单次只跑 latest 一次"的旧行为。
        if not catch_up_to_latest:
            if api_latest_date:
                return [api_latest_date], False
            return [""], False

        # 回补模式：无 timestamp 时尝试从 CSV 数据推断基线
        if not local_date:
            # mirror_unknown 产品无 rule，infer 内部返回 None 后走 latest-only 路径
            rule = RULES.get(product_name)
            inferred = infer_local_date_from_csv(
                command_ctx.data_root, product_name, rule
            )
            if not inferred:
                # 真正的首次同步或无 rule 产品，只下载 latest
                if api_latest_date:
                    return [api_latest_date], False
                return [""], False
            # 推断后复查门控：已是最新则跳过
            if should_skip_by_timestamp(inferred, api_latest_date):
                elapsed = time.time() - t_product_start
                with lock if lock is not None else contextlib.nullcontext():
                    _append_result(
                        report,
                        product=product_name,
                        status="skipped",
                        strategy=plan.strategy,
                        reason_code=REASON_UP_TO_DATE,
                        date_time=api_latest_date or "",
                        mode="gate",
                        elapsed=elapsed,
                        error=f"CSV 推断日期已是最新（inferred={inferred}, api={api_latest_date}）。",
                    )
                log_info(
                    f"[{product_name}] CSV 推断门控命中，跳过更新。",
                    event="SYNC_SKIP",
                    inferred_date=inferred,
                    api_latest_date=api_latest_date,
                    decision="skip",
                )
                return [], True
            # 有 CSV 数据但无 timestamp，用推断日期走正常回补
            local_date = inferred
            log_info(
                f"[{product_name}] 无 timestamp，从 CSV 推断基线日期。",
                event="PRODUCT_PLAN",
                inferred_date=inferred,
                decision="infer_baseline",
            )

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
        # latest 已拿到时，门控后续异常回退到 latest，避免丢失候选日期。
        fallback_date = api_latest_date or requested_date_for_plan
        log_info(
            f"[{plan.name}] timestamp 门控异常，回退执行更新。",
            event="PRODUCT_PLAN",
            decision="fallback_run",
            error=str(exc),
            fallback_date=fallback_date,
        )
        return [fallback_date], False

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


def _collect_preprocess_source_successes(report: RunReport) -> List[ProductRunResult]:
    """
    收集本轮已成功更新的"预处理依赖源产品"。

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
    判断单个源产品是否存在"有效增量"。

    只把 created/updated/rows_added 视为有效变化，
    避免仅 mtime 触发但内容未变时重复执行重计算。
    """

    stats = item.stats
    return (stats.created_files + stats.updated_files + stats.rows_added) > 0

def _run_builtin_coin_preprocess(command_ctx: CommandContext, progress_callback=None) -> Tuple[str, str]:
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

    summary = run_coin_preprocess_builtin(command_ctx.data_root, progress_callback=progress_callback)
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

    优先使用"本轮成功源产品"的 date_time；
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
    progress_callback=None,
) -> bool:
    """
    条件触发币圈合成预处理。

    返回值：
    - True: 预处理阶段发生错误（应计入本次 update 失败）
    - False: 未触发或执行成功
    """

    phase_start = time.time()
    # 用 try/finally 统一在函数出口累加耗时，各分支只保留业务逻辑
    try:
        preprocess_dir = command_ctx.data_root / PREPROCESS_PRODUCT
        if not preprocess_dir.is_dir():
            return False

        source_successes = _collect_preprocess_source_successes(report)
        if not source_successes:
            log_debug(
                "预处理跳过：本轮源产品无成功更新。",
                event="PREPROCESS",
                target=PREPROCESS_PRODUCT,
            )
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
            )
            log_info(
                "预处理跳过：源产品无有效增量。",
                event="PREPROCESS",
                target=PREPROCESS_PRODUCT,
            )
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
            )
            log_info("dry-run 模式下跳过预处理执行。", event="PREPROCESS", target=PREPROCESS_PRODUCT)
            return False

        # 构造 preprocess_cb：将内部细粒度通知转换为 postprocessing 状态通知
        preprocess_cb = None
        if progress_callback is not None:
            def preprocess_cb(*, detail=""):
                progress_callback("", 0, 0, status="postprocessing", postprocess_detail=detail)

        t0 = time.time()
        raw_cmd = ""
        try:
            # 统一使用仓库内置预处理实现，降低分发后的使用门槛。
            raw_cmd, success_reason_code = _run_builtin_coin_preprocess(command_ctx, progress_callback=preprocess_cb)
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
            )
            log_info(
                "预处理执行成功。",
                event="PREPROCESS",
                target=PREPROCESS_PRODUCT,
                data_time=actual_time,
                command=raw_cmd,
            )
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
            )
            log_error(f"预处理执行异常: {message}", event="PREPROCESS")
            if command_ctx.verbose:
                log_debug(traceback.format_exc(), event="DEBUG")
            return True
    finally:
        # 无论哪个分支退出，统一在此累加后处理阶段耗时
        report.phase_postprocess_seconds += max(0.0, time.time() - phase_start)

def _prefetch_api_dates(
    products: List[str],
    command_ctx: "CommandContext",
    hid: str,
    headers: Dict[str, str],
    max_workers: int = 8,
) -> Dict[str, Tuple[str, str]]:
    """并发预取产品的 API 最新日期，写入缓存并返回。

    已在缓存中且未过期的产品跳过。失败的产品静默跳过，
    Plan 阶段会回退到逐产品 HTTP 查询。
    """
    api_base = command_ctx.api_base.rstrip("/")
    log_dir = report_dir_path(command_ctx.data_root)
    # 1. 读现有缓存，筛出需要查询的产品
    existing_cache = load_api_latest_dates(log_dir)
    uncached = []
    for product in products:
        cached = existing_cache.get(product)
        if cached:
            _, checked_at_str = cached
            if _is_cache_fresh(checked_at_str):
                continue  # 缓存新鲜，跳过
        uncached.append(product)

    if not uncached:
        log_info(
            f"[预取] 全部 {len(products)} 个产品缓存命中，跳过 HTTP",
            event="PREFETCH", decision="all_cached",
        )
        return existing_cache

    # 2. 并发预取未命中的产品
    log_info(
        f"[预取] 并发查询 {len(uncached)}/{len(products)} 个产品",
        event="PREFETCH", decision="fetching",
    )
    fetched: Dict[str, str] = {}  # 写入仅在主线程的 as_completed 循环内，无并发写入
    # abort_event 只能拦截尚未开始的 worker，已在执行的请求会自然完成或超时
    abort_event = threading.Event()
    t_start = time.time()

    def _fetch_one(product: str) -> Tuple[str, Optional[str]]:
        """单产品 HTTP 查询，401/403 触发全局中止。"""
        if abort_event.is_set():
            return product, None
        try:
            date_str = get_latest_time(api_base, product, hid, headers)
            return product, date_str
        except FatalRequestError as exc:
            # 认证失败时中止整个预取
            if exc.status_code in (401, 403):
                abort_event.set()
            return product, None
        except Exception:
            return product, None

    effective_workers = min(max_workers, len(uncached))
    executor = ThreadPoolExecutor(max_workers=effective_workers)
    try:
        futures = {executor.submit(_fetch_one, p): p for p in uncached}
        for future in as_completed(futures, timeout=30):
            try:
                product, date_str = future.result()
                if date_str:
                    fetched[product] = date_str
            except Exception:
                pass
            if abort_event.is_set():
                break
    except (TimeoutError, FuturesTimeoutError):
        log_info("[预取] 超时，放弃剩余查询", event="PREFETCH", decision="timeout")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    elapsed = time.time() - t_start
    log_info(
        f"[预取] 完成，成功 {len(fetched)}/{len(uncached)}，耗时 {elapsed:.1f}s",
        event="PREFETCH", decision="done",
    )

    # 3. 持久化并返回内存合并的缓存（不重读文件，避免竞争和过期条目泄漏）
    if fetched:
        try:
            update_api_latest_dates(log_dir, fetched)
        except Exception:
            pass
    # 合并：保留新鲜的已有缓存 + 刚预取的结果
    checked_at_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    merged: Dict[str, Tuple[str, str]] = dict(existing_cache)
    for product, date_str in fetched.items():
        merged[product] = (date_str, checked_at_now)
    return merged


def _estimate_sync_workload(
    plans: Sequence[ProductPlan],
    api_date_cache: Dict[str, Tuple[str, str]],
    data_root: Path,
    api_call_limit: int = 50,
    course_type: str = "",
) -> EstimateResult:
    """预估同步所需的 API 调用量。

    比较每产品的本地 timestamp 和 API 缓存日期，
    计算缺口天数和预估调用次数。
    门控命中（gap=0）的产品不计入结果列表。
    无 API 缓存日期或无本地日期时，计为 1 次（latest only）。
    """
    products_list = []
    total = 0
    for plan in plans:
        product_name = normalize_product_name(plan.name)
        # 读本地日期（timestamp.txt）
        local_date = read_local_timestamp_date(data_root, product_name)
        if not local_date:
            # 尝试 CSV 推断（仅已知产品，需要 RULES 中的规则确定日期列）
            rule = RULES.get(product_name)
            if rule:
                local_date = infer_local_date_from_csv(data_root, product_name, rule)
        # 读 API 最新日期（来自预取缓存）
        cached = api_date_cache.get(product_name)
        api_date = cached[0] if cached else None
        if not api_date:
            # 无 API 日期，计为 1 次
            products_list.append({
                "name": plan.name, "local_date": local_date or "",
                "api_date": "", "gap_days": 1, "estimated_calls": 1,
            })
            total += 1
            continue
        if not local_date:
            # 无本地数据，计为 1 次
            products_list.append({
                "name": plan.name, "local_date": "",
                "api_date": api_date, "gap_days": 1, "estimated_calls": 1,
            })
            total += 1
            continue
        # 计算日期缺口
        try:
            local_d = date.fromisoformat(local_date)
            api_d = date.fromisoformat(api_date)
            gap = max(0, (api_d - local_d).days)
        except ValueError:
            gap = 1
        if gap == 0:
            continue  # 已是最新，不计入
        products_list.append({
            "name": plan.name, "local_date": local_date,
            "api_date": api_date, "gap_days": gap, "estimated_calls": gap,
        })
        total += gap
    return EstimateResult(
        products=products_list,
        total_calls=total,
        limit=api_call_limit,
        course_type=course_type,
        needs_confirm=total > api_call_limit,
    )


def _print_estimate(estimate: EstimateResult) -> None:
    """打印 CLI 同步预估表，仅在 needs_confirm 时调用。"""
    print(f"\n警告：本次同步预计需要约 {estimate.total_calls} 次 API 调用\n")
    header = f"  {'产品':<36}  {'落后天数':>8}  {'预计调用':>8}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for p in estimate.products:
        print(f"  {p['name']:<36}  {p['gap_days']:>8}  {p['estimated_calls']:>8}")
    print("  " + "-" * (len(header) - 2))
    print(f"  {'合计':<36}  {'':>8}  {estimate.total_calls:>8}\n")
    if estimate.course_type == "basic":
        print("你的账号每产品每天只能同步 1 次，落后较多时建议先从网页下载全量数据，或联系官方重置配额。")
    else:
        print("落后天数较多时建议先从网页下载全量数据。")
    print()


def _execute_plans(
    plans: Sequence[ProductPlan],
    command_ctx: CommandContext,
    report: RunReport,
    requested_date_time: str = "",
    conn: Optional[sqlite3.Connection] = None,
    force_update: bool = False,
    catch_up_to_latest: bool = False,
    max_workers: int = 1,
    progress_callback: Optional[Callable[..., None]] = None,
) -> Tuple[SyncStats, bool, float]:
    """
    执行产品计划并返回汇总统计。

    整体流程（单次运行）：
    1) 先构建请求头与凭证（API Key/HID）
    2) 逐个产品执行"门控判断 -> 解析日期队列 -> 下载解压 -> 同步落库 -> 记录结果"
    3) 累加统计并在必要时中断（stop-on-error）

    max_workers: 并发下载线程数。1 = 串行（默认），>1 = 并行处理产品。
                 stop_on_error=True 时强制串行。
    progress_callback: 产品完成后回调，签名：
        (product_name, completed, total, *, elapsed_seconds, stats, status, **kwargs)
        status="init" 时为初始化调用，kwargs 含 all_products 列表。
        供 GUI 进度展示使用，CLI 不传。
    """

    _reset_http_metrics()
    headers, hid = build_headers_or_raise(command_ctx)
    total = SyncStats()
    has_error = False
    t_run_start = time.time()

    # 并发预取所有产品的 API 最新日期，写入缓存供 Plan 阶段命中（替代单次 load_api_latest_dates）
    product_names = [normalize_product_name(p.name) for p in plans]
    _api_date_cache = _prefetch_api_dates(
        products=product_names,
        command_ctx=command_ctx,
        hid=hid,
        headers=headers,
    )

    # stop-on-error 要求严格顺序控制，强制串行
    effective_workers = max(1, max_workers) if not command_ctx.stop_on_error else 1
    # 保护共享状态的互斥锁（串行时无竞争，开销可忽略）
    _lock = threading.Lock()

    def _run_one_plan(plan: ProductPlan) -> Tuple[bool, float, SyncStats, str, str]:
        """处理单个产品计划。返回 (has_error, elapsed_seconds, stats, status, error_msg)。"""

        plan_has_error = False
        message = ""  # 失败时的错误信息，供 progress_callback 透传给前端
        product_stats = SyncStats()  # 逐产品累积 stats，最终随返回值传出
        t_product_start = time.time()
        # A. 判断这个产品本次应不应该跑（门控命中会直接 return）。
        # _resolve_requested_dates_for_plan 内部的 _append_result 也通过 lock 参数保护。
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
            lock=_lock,
            api_date_cache=_api_date_cache,
        )
        with _lock:
            report.phase_plan_seconds += max(0.0, time.time() - t_plan_phase)
        if skipped_by_gate:
            return (False, 0.0, SyncStats(), "skipped", "")

        if not requested_dates_for_plan:
            if catch_up_to_latest:
                elapsed = time.time() - t_product_start
                # 读取本地 timestamp，用于填充 date_time，避免 GUI 用 today 计算落后天数
                local_date_for_report = read_local_timestamp_date(
                    command_ctx.data_root, normalize_product_name(plan.name)
                )
                # 并发路径：写 report 需加锁，防止 list.append 与其他线程竞争
                with _lock:
                    _append_result(
                        report,
                        product=plan.name,
                        status="skipped",
                        strategy=plan.strategy,
                        reason_code=REASON_NO_DATA_FOR_DATE,
                        date_time=local_date_for_report or "",
                        elapsed=elapsed,
                        mode="gate",
                        error="catch-up 日期队列为空，已跳过本产品。",
                    )
                log_info(
                    f"[{plan.name}] catch-up 日期队列为空，已跳过。",
                    event="SYNC_SKIP",
                    reason_code=REASON_NO_DATA_FOR_DATE,
                )
                return (False, 0.0, SyncStats(), "skipped", "")
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
                    "run_id": command_ctx.run_id,
                }
                product, actual_time, stats, source_path, reason_code = process_product(
                    **process_kwargs,
                )
                elapsed = time.time() - t_product_start
                if reason_code == REASON_NO_VALID_OUTPUT:
                    # 并发路径：total.merge + _append_result 在同一锁作用域，保证原子可见性
                    with _lock:
                        total.merge(stats)
                        product_stats.merge(stats)  # 累积本产品 stats 用于进度回调
                        _append_result(
                            report,
                            product=product,
                            status="skipped",
                            strategy=plan.strategy,
                            reason_code=reason_code,
                            date_time=actual_time,
                            elapsed=elapsed,
                            stats=stats,
                            source_path=source_path,
                            error="同步未产生可用输出，已跳过状态推进。",
                        )
                        report.phase_sync_seconds += max(0.0, time.time() - t_sync_phase)
                    log_info(
                        f"[{plan.name}] 同步无有效产出，已跳过状态推进。",
                        event="SYNC_SKIP",
                        reason_code=reason_code,
                    )
                    continue

                # 成功路径：total.merge + 状态持久化 + _append_result 在同一锁作用域
                status_persist_warning = ""
                with _lock:
                    total.merge(stats)
                    product_stats.merge(stats)  # 累积本产品 stats 用于进度回调
                    try:
                        _upsert_product_status_after_success(
                            conn=conn,
                            command_ctx=command_ctx,
                            product=product,
                            actual_time=actual_time,
                        )
                    except Exception as status_exc:
                        status_persist_warning = (
                            f"状态持久化失败（已忽略，不影响本次成功结果）: {status_exc}"
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
                if status_persist_warning:
                    log_info(
                        f"[{plan.name}] {status_persist_warning}",
                        event="SYNC_WARN",
                        reason_code=reason_code,
                    )
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
            with _lock:
                report.phase_sync_seconds += max(0.0, time.time() - t_sync_phase)

            elapsed = time.time() - t_product_start
            # D. 回补模式：无数据日记录为 skipped，继续下一个日期。
            if catch_up_to_latest and reason_code == REASON_NO_DATA_FOR_DATE:
                # 并发路径：写 report 需加锁
                with _lock:
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

            # E. 失败路径：记录错误。
            plan_has_error = True
            # 并发路径：写 report 需加锁
            with _lock:
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
            # 仅终止当前产品后续日期，继续下一个产品。
            break

        elapsed = time.time() - t_product_start
        status = "error" if plan_has_error else "ok"
        # 失败时透传 message（在异常捕获路径赋值），供 progress_callback 显示失败原因
        error_msg = message if plan_has_error else ""
        return (plan_has_error, elapsed, product_stats, status, error_msg)

    # 进度计数器，用于 progress_callback 上报（串行路径直接递增，并行路径需加锁）
    _completed = 0

    def _notify_progress(product_name: str, elapsed_seconds: float = 0.0,
                         stats: Optional[SyncStats] = None, status: str = "ok",
                         error: str = "", **kwargs) -> None:
        """安全调用进度回调（异常不影响主流程）。

        串行路径直接调用（无竞争，因为 for 循环是串行的）。
        并行路径由调用方在 _lock 内调用。
        error 在失败路径传递，供前端展示具体失败原因。
        kwargs 用于传递 all_products 等额外参数。
        """
        nonlocal _completed
        _completed += 1
        if progress_callback is not None:
            try:
                progress_callback(product_name, _completed, len(plans),
                                  elapsed_seconds=elapsed_seconds, stats=stats,
                                  status=status, error=error, **kwargs)
            except Exception:
                pass

    # 初始化调用：传出全部产品名，供 GUI 在进度条出现前渲染列表
    if progress_callback is not None:
        try:
            progress_callback("", 0, len(plans),
                              elapsed_seconds=0.0, stats=None, status="init",
                              all_products=[plan.name for plan in plans])
        except Exception:
            pass

    if effective_workers <= 1:
        # === 串行路径 ===
        for plan in plans:
            (plan_error, elapsed, p_stats, p_status, p_error) = _run_one_plan(plan)
            _notify_progress(plan.name, elapsed_seconds=elapsed, stats=p_stats,
                             status=p_status, error=p_error)
            if plan_error:
                has_error = True
                if command_ctx.stop_on_error:
                    log_error("已开启 stop-on-error，任务提前停止。", event="RUN_SUMMARY")
                    command_ctx.http_attempts_by_product = dict(HTTP_ATTEMPTS_BY_PRODUCT)
                    command_ctx.http_failures_by_product = dict(HTTP_FAILURES_BY_PRODUCT)
                    return total, has_error, t_run_start
    else:
        # === 并行路径（stop_on_error 时已强制 effective_workers=1，不会进这里） ===
        log_info(
            f"并发下载启动，线程数={effective_workers}，产品数={len(plans)}。",
            event="CONCURRENT_START",
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {executor.submit(_run_one_plan, plan): plan for plan in plans}
            # as_completed 在主线程顺序消费，has_error 写入无并发竞争，无需加锁
            for future in as_completed(futures):
                plan = futures[future]
                try:
                    (plan_error, elapsed, p_stats, p_status, p_error) = future.result()
                    with _lock:
                        _notify_progress(plan.name, elapsed_seconds=elapsed,
                                         stats=p_stats, status=p_status, error=p_error)
                    if plan_error:
                        has_error = True
                except Exception as exc:
                    # _run_one_plan 内部未捕获的异常（理论上不应发生，但做防御性兜底）
                    has_error = True
                    with _lock:
                        _notify_progress(plan.name, elapsed_seconds=0.0,
                                         stats=SyncStats(), status="error",
                                         error=f"并发执行异常（未预期）: {exc}")
                    log_error(
                        f"[{plan.name}] 并发执行异常: {exc}",
                        event="SYNC_FAIL",
                        reason_code=REASON_MERGE_ERROR,
                    )
                    # 补写 report，避免该产品在报告中完全缺失；加锁保证并发安全
                    with _lock:
                        _append_result(
                            report,
                            product=plan.name,
                            status="error",
                            strategy=plan.strategy,
                            reason_code=REASON_MERGE_ERROR,
                            error=f"并发执行异常（未预期）: {exc}",
                        )

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
    max_workers: int = 1,
    progress_callback: Optional[Callable[..., None]] = None,
    api_call_limit: int = 50,
    course_type: str = "",
    confirm_callback: Optional[Callable] = None,
    auto_confirm: bool = False,
) -> int:
    """
    通用批量更新执行器（update/all_data 共用）。

    fallback_products 用于"本地扫描为空"时的回退清单。
    progress_callback: 产品完成后回调，签名同 _execute_plans。
    api_call_limit: 触发确认的 API 调用次数阈值，默认 50。
    course_type: 课程类型（"basic"/"premium"），影响确认提示文本。
    confirm_callback: 调用方提供的确认函数，接收 EstimateResult 返回 True/False。
    auto_confirm: True 时跳过所有确认提示（CLI --yes）。
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
            dry_run=command_ctx.dry_run,
            log_dir=report_dir_path(command_ctx.data_root),
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
            dry_run=command_ctx.dry_run,
            log_dir=report_dir_path(command_ctx.data_root),
        )
        return decide_exit_code(
            report=report,
            has_error=True,
            no_executable_products=True,
        )

    # 非 dry_run 时：预取 API 日期 → 预估调用量 → 超阈值时请求确认
    if not command_ctx.dry_run:
        headers_pre, hid_pre = build_headers_or_raise(command_ctx)
        api_date_cache = _prefetch_api_dates(
            products=[normalize_product_name(p.name) for p in plans],
            command_ctx=command_ctx,
            hid=hid_pre,
            headers=headers_pre,
        )
        estimate = _estimate_sync_workload(
            plans, api_date_cache, command_ctx.data_root, api_call_limit, course_type
        )
        if estimate.needs_confirm:
            confirmed = True  # 默认继续
            if confirm_callback:
                confirmed = confirm_callback(estimate)
            elif not auto_confirm:
                # CLI 交互模式：打印预估表，等用户输入
                _print_estimate(estimate)
                confirmed = input("继续同步？(y/n) ").strip().lower() == "y"
            if not confirmed:
                _append_result(
                    report,
                    product="",
                    status="cancelled",
                    reason_code="user_cancelled",
                    error="用户取消同步",
                )
                # 返回 -1 表示用户主动取消，与业务失败（1）和成功（0）区分
                return -1

    # 丢弃 _execute_plans 内部计时（第三返回值），以外层 t_run_start 为准，含前置阶段耗时
    if command_ctx.dry_run:
        total, has_error, _ = _execute_plans(
            plans,
            command_ctx,
            report,
            requested_date_time="",
            conn=None,
            force_update=force_update,
            catch_up_to_latest=True,
            max_workers=max_workers,
            progress_callback=progress_callback,
        )
        # dry_run 模式下 _maybe_run_coin_preprocess 会直接跳过，不发 postprocessing 通知
        preprocess_has_error = _maybe_run_coin_preprocess(
            command_ctx=command_ctx,
            report=report,
            conn=None,
            progress_callback=None,
        )
        has_error = has_error or preprocess_has_error
    else:
        with open_status_db(command_ctx.data_root) as conn:
            try:
                total, has_error, _ = _execute_plans(
                    plans,
                    command_ctx,
                    report,
                    requested_date_time="",
                    conn=conn,
                    force_update=force_update,
                    catch_up_to_latest=True,
                    max_workers=max_workers,
                    progress_callback=progress_callback,
                )
                # 细粒度进度由 _maybe_run_coin_preprocess 内部通过 preprocess_cb 发出
                preprocess_has_error = _maybe_run_coin_preprocess(
                    command_ctx=command_ctx,
                    report=report,
                    conn=conn,
                    progress_callback=progress_callback,
                )
                has_error = has_error or preprocess_has_error
            finally:
                export_status_json(conn, status_json_path(command_ctx.data_root))

    # 使用 run_update_with_settings 自己的 t_run_start（第 1121 行），
    # 而非 _execute_plans 内部的计时，以包含 discover/catalog/plan 前置阶段耗时。
    return _finalize_and_write_report(
        report, total, has_error, t_run_start, report_path,
        dry_run=command_ctx.dry_run,
        log_dir=report_dir_path(command_ctx.data_root),
    )
