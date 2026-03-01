"""职责：封装 HTTP 请求策略、接口调用和下载逻辑。"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import unquote, urlparse

import requests

from .constants import REQUEST_POLICIES
from .models import FatalRequestError, log_debug, normalize_product_name
from .status_store import normalize_data_date


def resolve_request_policy(request_profile: str, request_policies: Dict[str, Dict[str, int]]) -> Dict[str, int]:
    """按请求类型返回请求策略，未知类型回退 default。"""

    profile = (request_profile or "default").strip().lower()
    if profile in request_policies:
        return request_policies[profile]
    return request_policies["default"]

HTTP_ATTEMPTS_BY_PRODUCT: Dict[str, int] = defaultdict(int)

HTTP_FAILURES_BY_PRODUCT: Dict[str, int] = defaultdict(int)

def _reset_http_metrics() -> None:
    """每次命令执行前重置 HTTP 统计，避免跨运行串数据。"""

    HTTP_ATTEMPTS_BY_PRODUCT.clear()
    HTTP_FAILURES_BY_PRODUCT.clear()

def _record_http_attempt(product: str) -> None:
    """记录一次 HTTP 尝试。product 为空时忽略（兼容旧调用）。"""

    name = normalize_product_name(product) if product else ""
    if not name:
        return
    HTTP_ATTEMPTS_BY_PRODUCT[name] += 1

def _record_http_failure(product: str) -> None:
    """记录一次 HTTP 失败尝试（含非 200 与请求异常）。"""

    name = normalize_product_name(product) if product else ""
    if not name:
        return
    HTTP_FAILURES_BY_PRODUCT[name] += 1

def _http_metrics_for_product(product: str) -> Tuple[int, int]:
    """读取指定产品当前累计的 HTTP 指标。"""

    name = normalize_product_name(product) if product else ""
    if not name:
        return 0, 0
    return int(HTTP_ATTEMPTS_BY_PRODUCT.get(name, 0)), int(HTTP_FAILURES_BY_PRODUCT.get(name, 0))

def request_data(
    method: str,
    url: str,
    headers: Dict[str, str],
    *,
    product: str = "",
    request_profile: str = "default",
    **kwargs,
) -> requests.Response:
    """
    统一 HTTP 请求入口（带重试）。

    设计思路：
    - 网络波动/服务器偶发 5xx：重试
    - 参数错误/权限不足（4xx）：立即报错，不重试
    - 按请求类型（latest / download_link / file_download）区分策略
    """

    status_messages = {
        404: "资源不存在（该产品该日期无可下载数据）",
        403: "无下载权限，请检查下载次数和 api-key",
        401: "超出当日下载次数",
        400: "下载时间超出限制",
        500: "服务器内部错误，请稍后重试",
    }

    policy = resolve_request_policy(request_profile, REQUEST_POLICIES)
    max_attempts = int(policy["max_attempts"])
    timeout_seconds = int(policy["timeout_seconds"])
    backoff_cap_seconds = int(policy["backoff_cap_seconds"])
    for attempt in range(1, max_attempts + 1):
        _record_http_attempt(product)
        log_debug(
            f"HTTP {method} attempt={attempt}/{max_attempts} url={url.split('?')[0]}",
            event="HTTP",
            profile=request_profile,
            timeout=timeout_seconds,
            product=normalize_product_name(product) if product else "",
        )
        request_kwargs = dict(kwargs)
        request_kwargs.setdefault("timeout", timeout_seconds)
        try:
            response = requests.request(method=method, url=url, headers=headers, **request_kwargs)
        except requests.RequestException as exc:
            _record_http_failure(product)
            if attempt >= max_attempts:
                hint = ""
                err_text = str(exc)
                if "Failed to resolve" in err_text or "NameResolutionError" in err_text:
                    hint = "（DNS 解析失败：请检查网络、DNS 或代理设置）"
                raise RuntimeError(f"网络请求失败: {exc}{hint}") from exc
            time.sleep(min(2 ** (attempt - 1), backoff_cap_seconds))
            continue

        if response.status_code == 200:
            return response

        _record_http_failure(product)
        message = status_messages.get(response.status_code, f"未知错误（HTTP {response.status_code}）")
        if response.status_code in {400, 401, 403, 404}:
            raise FatalRequestError(message, status_code=response.status_code, request_url=url.split("?")[0])
        if attempt >= max_attempts:
            raise RuntimeError(message)
        time.sleep(min(2 ** (attempt - 1), backoff_cap_seconds))

    raise RuntimeError("请求失败：超过最大重试次数。")

def parse_latest_time_candidates(raw_text: str) -> List[str]:
    """解析 latest 接口返回文本，输出去重升序日期列表（YYYY-MM-DD）。"""

    candidates = [x.strip() for x in re.split(r"[,\s]+", raw_text) if x.strip()]
    normalized = [normalize_data_date(x) for x in candidates]
    valid = sorted({x for x in normalized if x})
    if not valid:
        raise RuntimeError("接口未返回可用的 date_time。")
    return valid

def get_latest_times(api_base: str, product: str, hid: str, headers: Dict[str, str]) -> List[str]:
    """调用 latest 接口获取指定产品可用时间列表（归一化后升序）。"""

    url = f"{api_base}/fetch/{product}-daily/latest?uuid={hid}"
    res = request_data(
        "GET",
        url=url,
        headers=headers,
        product=product,
        request_profile="latest",
    )
    return parse_latest_time_candidates(res.text)

def get_latest_time(api_base: str, product: str, hid: str, headers: Dict[str, str]) -> str:
    """调用 latest 接口获取指定产品最新时间。"""

    return get_latest_times(api_base=api_base, product=product, hid=hid, headers=headers)[-1]

def get_download_link(api_base: str, product: str, date_time: str, hid: str, headers: Dict[str, str]) -> str:
    """根据产品和时间获取真实下载链接。"""

    url = f"{api_base}/get-download-link/{product}-daily/{date_time}?uuid={hid}"
    res = request_data(
        "GET",
        url=url,
        headers=headers,
        product=product,
        request_profile="download_link",
    )
    download_link = res.text.strip()
    if not download_link:
        raise RuntimeError(f"{product} {date_time} 未返回下载链接。")
    return download_link

def build_file_name(file_url: str, product: str, date_time: str) -> str:
    """
    从下载链接提取文件名；提取失败时使用兜底名。
    """

    parsed = urlparse(file_url)
    name = Path(unquote(parsed.path)).name
    if name:
        return name
    return f"{product}_{date_time}.zip"

def save_file(file_url: str, file_path: Path, headers: Dict[str, str], product: str = "") -> None:
    """流式下载文件到本地，避免占用过多内存。"""

    file_path.parent.mkdir(parents=True, exist_ok=True)
    res = request_data(
        "GET",
        url=file_url,
        headers=headers,
        stream=True,
        product=product,
        request_profile="file_download",
    )
    with file_path.open("wb") as f:
        for chunk in res.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

