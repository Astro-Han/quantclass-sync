"""职责：管理状态库、时间戳和运行期路径。"""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from .config import atomic_temp_path
from .constants import (
    DEFAULT_REPORT_RETENTION_DAYS,
    LEGACY_STATUS_DB_REL,
    LEGACY_STATUS_JSON_REL,
    META_REPORT_DIR_REL,
    META_STATUS_DB_REL,
    META_STATUS_JSON_REL,
    SYNC_META_DIRNAME,
    TIMESTAMP_FILE_NAME,
)
from .models import ProductStatus, RuntimePaths, log_info, utc_now_iso

_RUN_SCOPE_PATTERN = re.compile(r"^\d{8}-\d{6}(?:[-_].+)?$")


def _status_db_has_rows(path: Path) -> bool:
    """判断状态库是否可用（存在 product_status 且至少 1 行）。"""

    if not path.exists():
        return False
    if path.stat().st_size <= 0:
        return False

    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='product_status' LIMIT 1"
        ).fetchone()
        if not table_exists:
            return False
        row_exists = conn.execute("SELECT 1 FROM product_status LIMIT 1").fetchone()
        return row_exists is not None
    except Exception:
        return False
    finally:
        if conn is not None:
            conn.close()


def _status_json_has_rows(path: Path) -> bool:
    """判断状态 JSON 是否可用（可解析且非空对象）。"""

    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return isinstance(payload, dict) and bool(payload)


def _has_valid_runtime_state(status_db: Path, status_json: Path) -> bool:
    return _status_db_has_rows(status_db) or _status_json_has_rows(status_json)

def resolve_runtime_paths(data_root: Path) -> RuntimePaths:
    """
    解析运行期状态/日志路径。

    规则：
    - 默认使用新路径：<data_root>/.quantclass_sync/*
    - 若检测到旧路径已有状态数据，且新路径尚无状态数据，则回退旧路径读取（避免迁移期分裂）
    - 运行报告写到 data_root/.quantclass_sync/log/，按数据目录隔离
    """

    data_root = data_root.resolve()
    metadata_root = data_root / SYNC_META_DIRNAME
    report_dir = (data_root / META_REPORT_DIR_REL).resolve()

    new_status_db = data_root / META_STATUS_DB_REL
    new_status_json = data_root / META_STATUS_JSON_REL
    new_has_valid_state = _has_valid_runtime_state(new_status_db, new_status_json)

    legacy_status_db = data_root / LEGACY_STATUS_DB_REL
    legacy_status_json = data_root / LEGACY_STATUS_JSON_REL
    legacy_has_valid_state = _has_valid_runtime_state(legacy_status_db, legacy_status_json)

    # 迁移保护：旧路径有状态且新路径还没初始化时，优先读旧路径，避免同一批数据写到两套状态库。
    if legacy_has_valid_state and not new_has_valid_state:
        return RuntimePaths(
            metadata_root=metadata_root,
            status_db=legacy_status_db,
            status_json=legacy_status_json,
            report_dir=report_dir,
            source="legacy",
        )

    return RuntimePaths(
        metadata_root=metadata_root,
        status_db=new_status_db,
        status_json=new_status_json,
        report_dir=report_dir,
        source="metadata",
    )

def status_db_path(data_root: Path) -> Path:
    """返回状态数据库路径。"""

    return resolve_runtime_paths(data_root).status_db

def status_json_path(data_root: Path) -> Path:
    """返回 products-status.json 路径。"""

    return resolve_runtime_paths(data_root).status_json

def report_dir_path(data_root: Path) -> Path:
    """返回运行报告目录（data_root/.quantclass_sync/log/）。"""

    return resolve_runtime_paths(data_root).report_dir

def normalize_data_date(raw: str) -> Optional[str]:
    """把输入日期统一归一成 YYYY-MM-DD。"""

    text = (raw or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return None
    if re.fullmatch(r"\d{8}", text):
        normalized = f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        try:
            return datetime.strptime(normalized, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None

def read_local_timestamp_date(data_root: Path, product: str) -> Optional[str]:
    """读取本地 timestamp.txt 第一列日期。"""

    path = data_root / product / TIMESTAMP_FILE_NAME
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8-sig", errors="ignore").strip()
    except OSError:
        return None
    if not text:
        return None
    first = text.split(",", 1)[0].strip()
    return normalize_data_date(first)

def write_local_timestamp(data_root: Path, product: str, data_date: str) -> None:
    """回写本地 timestamp.txt（格式：数据日期,本地写入时间）。"""

    normalized = normalize_data_date(data_date)
    if not normalized:
        log_info(
            "timestamp 写入失败：日期格式无效。",
            event="TIMESTAMP",
            product=product,
            data_date=data_date,
        )
        raise ValueError(f"非法数据日期：{data_date}")
    path = data_root / product / TIMESTAMP_FILE_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    local_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with atomic_temp_path(path, tag="ts") as tmp:
        tmp.write_text(f"{normalized},{local_now}\n", encoding="utf-8")

def should_skip_by_timestamp(local_date: Optional[str], api_latest_date: Optional[str]) -> bool:
    """判断本地是否已是最新版本。"""

    if not local_date or not api_latest_date:
        return False
    return local_date >= api_latest_date

def cleanup_work_cache_aggressive(work_dir: Path, run_id: str = "") -> None:
    """激进清理工作缓存目录（默认保留 run 作用域目录，避免并发互删）。"""

    if not work_dir.exists():
        return

    scoped_run_id = (run_id or "").strip()
    if scoped_run_id:
        run_scope = work_dir / scoped_run_id
        if run_scope.exists():
            shutil.rmtree(run_scope, ignore_errors=True)
        work_dir.mkdir(parents=True, exist_ok=True)
        return

    children = list(work_dir.iterdir())
    has_run_scoped_dirs = any(
        child.is_dir()
        and not child.is_symlink()
        and _RUN_SCOPE_PATTERN.fullmatch(child.name)
        for child in children
    )

    for child in children:
        if child.is_symlink():
            try:
                child.unlink()
            except FileNotFoundError:
                pass
            continue
        if child.is_dir():
            # 目录中出现 run_id 分层时，默认不删 run 作用域目录，防止并发任务互相清缓存。
            if has_run_scoped_dirs and _RUN_SCOPE_PATTERN.fullmatch(child.name):
                continue
            shutil.rmtree(child, ignore_errors=True)
            continue
        try:
            child.unlink()
        except FileNotFoundError:
            pass
    work_dir.mkdir(parents=True, exist_ok=True)

def cleanup_report_logs(report_dir: Path, retention_days: int = DEFAULT_REPORT_RETENTION_DAYS) -> None:
    """清理过期 run_report 日志文件。"""

    if retention_days <= 0:
        return
    if not report_dir.exists():
        return
    cutoff_ts = time.time() - retention_days * 24 * 3600
    for path in report_dir.glob("run_report_*.json"):
        try:
            if path.stat().st_mtime < cutoff_ts:
                path.unlink()
        except FileNotFoundError:
            continue

def ensure_status_table(conn: sqlite3.Connection) -> None:
    """确保状态表存在（product_status）。"""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS product_status (
            name TEXT PRIMARY KEY,
            display_name TEXT,
            last_update_time TEXT,
            next_update_time TEXT,
            data_time TEXT,
            data_content_time TEXT,
            is_auto_update INTEGER DEFAULT 0,
            can_auto_update INTEGER DEFAULT 1,
            add_time TEXT,
            is_listed INTEGER DEFAULT 1,
            ts TEXT
        )
        """
    )
    conn.commit()

def connect_status_db(data_root: Path, read_only: bool = False) -> sqlite3.Connection:
    """连接状态库（sqlite3：Python 内置轻量数据库）。"""

    db_path = status_db_path(data_root)
    if read_only:
        # dry-run 只能读现有状态，不允许隐式建库建表。
        if not db_path.exists():
            raise RuntimeError(f"状态库不存在（只读模式无法初始化）: {db_path}")
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    db_path.parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: 并发下载时工作线程在 _lock 保护下使用主线程创建的连接
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    ensure_status_table(conn)
    return conn


@contextmanager
def open_status_db(data_root: Path, read_only: bool = False) -> Iterator[sqlite3.Connection]:
    """上下文化连接状态库，确保连接总是被关闭。"""

    conn = connect_status_db(data_root, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()

def load_product_status(conn: sqlite3.Connection, product: str) -> Optional[ProductStatus]:
    """读取单产品状态。"""

    row = conn.execute("SELECT * FROM product_status WHERE name = ?", (product,)).fetchone()
    if not row:
        return None
    return ProductStatus(**dict(row))

def upsert_product_status(
    conn: sqlite3.Connection,
    status: ProductStatus,
    commit_immediately: bool = True,
) -> None:
    """写入或更新单产品状态。"""

    payload = status.model_dump()
    if not payload.get("add_time"):
        payload["add_time"] = utc_now_iso()
    payload["ts"] = utc_now_iso()

    conn.execute(
        """
        INSERT INTO product_status (
            name, display_name, last_update_time, next_update_time,
            data_time, data_content_time, is_auto_update, can_auto_update,
            add_time, is_listed, ts
        ) VALUES (
            :name, :display_name, :last_update_time, :next_update_time,
            :data_time, :data_content_time, :is_auto_update, :can_auto_update,
            :add_time, :is_listed, :ts
        )
        ON CONFLICT(name) DO UPDATE SET
            display_name=excluded.display_name,
            last_update_time=excluded.last_update_time,
            next_update_time=excluded.next_update_time,
            data_time=excluded.data_time,
            data_content_time=excluded.data_content_time,
            is_auto_update=excluded.is_auto_update,
            can_auto_update=excluded.can_auto_update,
            add_time=COALESCE(product_status.add_time, excluded.add_time),
            is_listed=excluded.is_listed,
            ts=excluded.ts
        """,
        payload,
    )
    if commit_immediately:
        conn.commit()

def list_product_status(conn: sqlite3.Connection) -> List[ProductStatus]:
    """读取全部产品状态。"""

    rows = conn.execute("SELECT * FROM product_status ORDER BY name").fetchall()
    return [ProductStatus(**dict(row)) for row in rows]

def export_status_json(conn: sqlite3.Connection, output_path: Path) -> None:
    """导出官方兼容 products-status.json。"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, object] = {}
    for item in list_product_status(conn):
        payload[item.name] = item.to_json_record()
    with atomic_temp_path(output_path, tag="status-json") as tmp:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
