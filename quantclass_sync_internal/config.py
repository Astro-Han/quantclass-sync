"""职责：处理用户配置、凭证解析和产品发现。"""

from __future__ import annotations

import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator, List, Optional, Sequence, Tuple

from .constants import (
    DISCOVERY_IGNORED_PRODUCTS,
    RUN_MODES,
    STRATEGY_MERGE_KNOWN,
    STRATEGY_MIRROR_UNKNOWN,
)
from .models import (
    DiscoveredProduct,
    ProductPlan,
    RULES,
    TextFileSnapshot,
    _deduplicate,
    UserConfig,
    normalize_product_name,
    split_products,
)


def resolve_path_from_config(raw_path: Path, *, config_file: Path) -> Path:
    """把配置里的路径统一解析为绝对路径（相对路径按配置文件目录解析）。"""

    expanded = raw_path.expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (config_file.parent / expanded).resolve()


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


def validate_run_mode(mode: str) -> str:
    """校验运行模式并返回标准化值。"""

    normalized = (mode or "local").strip().lower()
    if normalized not in RUN_MODES:
        raise ValueError("mode 仅支持 local 或 catalog")
    return normalized

@contextmanager
def atomic_temp_path(
    target: Path,
    tag: str = "atomic",
    create_mode: Optional[int] = None,
) -> Generator[Path, None, None]:
    """
    原子写入上下文管理器：yield 临时路径，退出时用 os.replace 替换目标文件。

    用法示例：
        with atomic_temp_path(dest_path) as tmp:
            tmp.write_text(content, encoding="utf-8")

    create_mode：若指定，在 yield 前用 os.open 预先创建临时文件并设置权限模式。
                 例如写入密钥文件时需要 0o600。
                 若为 None，调用方自行写入 tmp 文件（不预先创建）。
    """

    tmp = target.parent / f".{target.name}.tmp-{tag}-{os.getpid()}-{time.time_ns()}"
    try:
        # 若指定权限模式，预先用 os.open 创建文件，确保权限在写入前就生效
        if create_mode is not None:
            # os.open 的 mode 受进程 umask 影响（实际权限 = mode & ~umask）。
            # 调用者如需精确权限应在写入后自行 os.chmod。
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, create_mode)
            os.close(fd)
        yield tmp
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_text_atomic(
    path: Path,
    content: str,
    encoding: str = "utf-8",
    create_mode: int = 0o666,
    final_mode: int | None = None,
) -> None:
    """
    原子写入文本文件。

    原子写入（要么完整写成功、要么不改变旧文件）可以避免配置/密钥写半截。
    内部调用 atomic_temp_path 上下文管理器实现。
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    with atomic_temp_path(path, tag="text", create_mode=create_mode) as tmp:
        tmp.write_text(content, encoding=encoding)
    # 成功替换后（os.replace 已完成），如需最终权限模式则单独 chmod
    if final_mode is not None:
        os.chmod(path, final_mode)

def snapshot_text_file(path: Path) -> TextFileSnapshot:
    """保存文件当前状态。"""

    if not path.exists():
        return TextFileSnapshot(exists=False)
    mode: Optional[int] = None
    try:
        mode = path.stat().st_mode & 0o777
    except Exception:
        mode = None
    content = path.read_text(encoding="utf-8", errors="ignore")
    return TextFileSnapshot(exists=True, content=content, mode=mode)

def restore_text_file_snapshot(path: Path, snapshot: TextFileSnapshot) -> None:
    """恢复文件到快照状态。"""

    if snapshot.exists:
        _write_text_atomic(path, snapshot.content)
        if snapshot.mode is not None:
            os.chmod(path, snapshot.mode)
        return
    if path.exists():
        path.unlink()

def save_user_config_atomic(path: Path, config: UserConfig) -> None:
    """保存用户配置（原子写入）。"""

    payload = config.model_dump(mode="json")
    _write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

def load_user_config_or_raise(path: Path) -> UserConfig:
    """读取用户配置，失败时给出可操作提示。"""

    if not path.exists():
        raise RuntimeError(f"未找到用户配置文件：{path}；请先执行 setup。")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"用户配置文件读取失败：{path}；请检查 JSON 格式或重新执行 setup。原始错误：{exc}") from exc

    try:
        config = UserConfig(**raw)
    except Exception as exc:
        raise RuntimeError(f"用户配置内容无效：{path}；请重新执行 setup。原始错误：{exc}") from exc
    return config

def save_user_secrets_atomic(path: Path, api_key: str, hid: str) -> None:
    """保存用户密钥文件（原子写入）。"""

    body = f"QUANTCLASS_API_KEY={api_key.strip()}\nQUANTCLASS_HID={hid.strip()}\n"
    _write_text_atomic(path, body, create_mode=0o600, final_mode=0o600)

def save_setup_artifacts_atomic(
    config_path: Path,
    config: UserConfig,
    secrets_path: Path,
    api_key: str,
    hid: str,
) -> None:
    """
    setup 双文件写入（带回滚）。

    任一文件写失败时，把配置和密钥都恢复到写入前状态，避免"半成功"。
    """

    # 先拍快照：后续任何写入失败时，都可以恢复"调用 setup 前"的文件状态。
    config_snapshot = snapshot_text_file(config_path)
    secrets_snapshot = snapshot_text_file(secrets_path)
    try:
        save_user_secrets_atomic(secrets_path, api_key=api_key, hid=hid)
        save_user_config_atomic(config_path, config)
    except Exception as exc:
        rollback_errors: List[str] = []
        for path, snapshot in ((secrets_path, secrets_snapshot), (config_path, config_snapshot)):
            try:
                restore_text_file_snapshot(path, snapshot)
            except Exception as rollback_exc:
                rollback_errors.append(f"{path}: {rollback_exc}")
        if rollback_errors:
            detail = "；".join(rollback_errors)
            raise RuntimeError(f"setup 文件写入失败且回滚不完整：{detail}") from exc
        raise

def load_user_secrets_or_raise(path: Path) -> Tuple[str, str]:
    """读取用户密钥并校验完整性。"""

    api_key, hid = load_secrets_from_file(path)
    if not api_key:
        raise RuntimeError(f"密钥文件缺少 API Key：{path}；请重新执行 setup。")
    if not hid:
        raise RuntimeError(f"密钥文件缺少 HID：{path}；请重新执行 setup。")
    return api_key, hid

def is_product_identifier(raw: str) -> bool:
    """
    判断文本是否像产品英文名。

    规则：
    - 只允许小写字母/数字/连字符
    - 至少包含一个字母（避免把日期误判成产品名）
    """

    s = raw.strip().lower()
    if not s:
        return False
    if not re.fullmatch(r"[a-z0-9-]+", s):
        return False
    return any(ch.isalpha() for ch in s)

def load_products_from_catalog(path: Path) -> List[str]:
    """
    从 catalog.txt 读取产品列表。

    当前规范：
    1) 每行一个产品英文名（product_id）
    2) 允许空行和 # 注释行
    """

    if not path.exists():
        raise RuntimeError(f"产品清单文件不存在: {path}")

    products: List[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    for lineno, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if not is_product_identifier(s):
            raise RuntimeError(
                f"产品清单格式错误：{path}:{lineno} -> `{s}`；"
                '请使用"每行一个产品英文名"的写法。'
            )
        products.append(normalize_product_name(s.lower()))

    result = _deduplicate(products)
    if not result:
        raise RuntimeError(f"产品清单为空：{path}；请至少配置一个产品英文名。")
    return result

def _dir_has_data_files(path: Path) -> bool:
    """
    判断目录内是否存在数据文件（.csv/.ts）。

    递归查找任意一个命中即返回，用 .suffix.lower() 保持大小写不敏感，
    避免把空目录误认为"已有产品"。
    """

    _DATA_SUFFIXES = {".csv", ".ts"}
    for candidate in path.rglob("*"):
        if candidate.is_file() and candidate.suffix.lower() in _DATA_SUFFIXES:
            return True
    return False

def discover_local_products(data_root: Path, catalog_products: Sequence[str]) -> List[DiscoveredProduct]:
    """
    扫描 data_root 一级目录，识别本地已有产品。

    定义：
    - 目录下递归存在 .csv/.ts，才算"本地已有产品"。
    - 是否有效（valid）由 catalog 产品集合判定。
    """

    catalog_set = {normalize_product_name(x) for x in catalog_products}
    discovered: List[DiscoveredProduct] = []

    if not data_root.exists() or not data_root.is_dir():
        return discovered

    for item in sorted(data_root.iterdir(), key=lambda x: x.name):
        if not item.is_dir():
            continue
        product_name = normalize_product_name(item.name)
        if product_name in DISCOVERY_IGNORED_PRODUCTS:
            continue
        if not _dir_has_data_files(item):
            continue
        discovered.append(
            DiscoveredProduct(
                name=product_name,
                source="local",
                valid=product_name in catalog_set,
            )
        )
    return discovered

def resolve_products_by_mode(
    mode: str,
    raw_products: Sequence[str],
    catalog_products: Sequence[str],
    discovered_local: Sequence[DiscoveredProduct],
) -> Tuple[List[str], List[str], List[str]]:
    """
    解析最终产品清单。

    返回三部分：
    1) planned_products：实际要执行的产品
    2) unknown_local_products：本地存在但不在 catalog 的目录
    3) invalid_explicit_products：用户显式指定但不在 catalog 的产品
    """

    mode = (mode or "local").strip().lower()
    if mode not in RUN_MODES:
        mode = "local"

    catalog_norm = [normalize_product_name(x) for x in catalog_products]
    catalog_set = set(catalog_norm)

    explicit = split_products(raw_products)
    explicit_valid = [x for x in explicit if x in catalog_set]
    invalid_explicit = [x for x in explicit if x not in catalog_set]

    unknown_local = [x.name for x in discovered_local if not x.valid]
    local_valid = [x.name for x in discovered_local if x.valid]

    if explicit:
        selected = explicit_valid
    elif mode == "catalog":
        selected = list(catalog_norm)
    else:
        selected = list(local_valid)

    result = _deduplicate(selected)

    return result, unknown_local, invalid_explicit

def build_product_plan(products: Sequence[str]) -> List[ProductPlan]:
    """
    为产品生成执行计划。

    规则：
    - 命中 RULES：merge_known（增量合并）
    - 未命中 RULES：mirror_unknown（镜像写入）
    """

    plans: List[ProductPlan] = []
    for product in products:
        strategy = STRATEGY_MERGE_KNOWN if product in RULES else STRATEGY_MIRROR_UNKNOWN
        plans.append(ProductPlan(name=product, strategy=strategy))
    return plans

def load_secrets_from_file(path: Path) -> Tuple[str, str]:
    """
    从本地文件读取 api_key / hid（若不存在则返回空字符串）。
    """

    if not path.exists():
        return "", ""

    text = path.read_text(encoding="utf-8-sig", errors="ignore")
    pairs: Dict[str, str] = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" in s:
            k, v = s.split("=", 1)
        elif ":" in s:
            k, v = s.split(":", 1)
        else:
            continue
        key = k.strip().lower().replace("-", "_")
        value = v.strip().strip("\"'")
        if value:
            pairs[key] = value

    api_key_candidates = ["quantclass_api_key", "api_key"]
    hid_candidates = ["quantclass_hid", "hid"]

    api_key = next((pairs[k] for k in api_key_candidates if k in pairs), "")
    hid = next((pairs[k] for k in hid_candidates if k in pairs), "")
    return api_key, hid

def _resolve_credentials_components(
    cli_api_key: str,
    cli_hid: str,
    secrets_file: Path,
) -> Tuple[str, str, str, str, str, str, str, str]:
    """
    内部函数：读取 CLI / File / ENV 三层凭证，返回各层原始值及合并结果。

    返回顺序：(cli_api, cli_hid_val, file_api, file_hid, env_api, env_hid, api_key, hid)
    """

    cli_api = cli_api_key.strip()
    cli_hid_val = cli_hid.strip()
    file_api, file_hid = load_secrets_from_file(secrets_file)
    env_api = os.environ.get("QUANTCLASS_API_KEY", "").strip()
    env_hid = os.environ.get("QUANTCLASS_HID", "").strip()

    # 按优先级合并：CLI > 文件 > 环境变量
    api_key = cli_api or file_api or env_api
    hid = cli_hid_val or file_hid or env_hid

    return cli_api, cli_hid_val, file_api, file_hid, env_api, env_hid, api_key, hid


def resolve_credentials(cli_api_key: str, cli_hid: str, secrets_file: Path) -> Tuple[str, str]:
    """
    凭证优先级（高 -> 低，兼容命令）：
    1) 命令行参数
    2) setup 写入的 secrets 文件
    3) 环境变量
    """

    *_, api_key, hid = _resolve_credentials_components(cli_api_key, cli_hid, secrets_file)
    return api_key, hid

def resolve_credentials_for_update(cli_api_key: str, cli_hid: str, secrets_file: Path) -> Tuple[str, str, str]:
    """
    update 专用凭证优先级（高 -> 低）：
    1) 命令行参数
    2) setup 写入的 secrets 文件
    3) 环境变量
    """

    cli_api, cli_hid_val, file_api, file_hid, env_api, env_hid, api_key, hid = (
        _resolve_credentials_components(cli_api_key, cli_hid, secrets_file)
    )

    # 判断 api_key 和 hid 各自来源，生成 credential_source 标识
    api_source = "cli" if cli_api else ("setup_secrets" if file_api else ("env" if env_api else "missing"))
    hid_source = "cli" if cli_hid_val else ("setup_secrets" if file_hid else ("env" if env_hid else "missing"))
    if api_source == hid_source:
        credential_source = api_source
    else:
        # 来源不同时记录 mixed 格式，方便日志诊断
        credential_source = f"mixed(api={api_source},hid={hid_source})"
    return api_key, hid, credential_source
