"""职责：提供压缩包安全解压能力。"""

from __future__ import annotations

import re
import shutil
import stat
import tarfile
import zipfile
from pathlib import Path

# 可选依赖：如果压缩包是 .7z，需要 py7zr 才能解压。
try:
    import py7zr  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    py7zr = None

# 可选依赖：如果压缩包是 .rar，需要 rarfile 才能解压。
try:
    import rarfile  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    rarfile = None

def _ensure_within(base: Path, target: Path) -> None:
    """
    安全解压检查。

    防止压缩包成员路径通过 ../../ 逃逸到目标目录之外。
    """

    try:
        target.resolve().relative_to(base.resolve())
    except Exception as exc:
        raise RuntimeError(f"解压路径越界: {target}") from exc

def safe_extract_zip(path: Path, save_path: Path) -> None:
    """安全解压 zip。"""

    with zipfile.ZipFile(path) as zf:
        for member in zf.infolist():
            member_name = _normalize_member_name(member.filename)
            target = save_path / member_name
            _ensure_within(save_path, target)
            if _is_zip_symlink(member):
                raise RuntimeError(f"zip 包含不安全的链接文件，已拒绝: {member.filename}")
        zf.extractall(save_path)
    _scan_extracted_dangerous_nodes(save_path)

def safe_extract_tar(path: Path, save_path: Path) -> None:
    """安全解压 tar。"""

    with tarfile.open(path) as tf:
        for member in tf.getmembers():
            if member.isfifo():
                raise RuntimeError(f"tar 包含不安全的特殊文件类型，已拒绝: {member.name}")
            if member.isdev():
                raise RuntimeError(f"tar 包含不安全的特殊文件类型，已拒绝: {member.name}")

            member_name = _normalize_member_name(member.name)
            target = save_path / member_name
            _ensure_within(save_path, target)

            # tar 里的软链接/硬链接可能指向目标目录外，需额外校验 linkname。
            if member.issym() or member.islnk():
                link_name = _normalize_member_name(getattr(member, "linkname", ""))
                if not link_name:
                    raise RuntimeError(f"tar 链接目标为空: {member.name}")
                if link_name.startswith("/") or re.match(r"^[a-zA-Z]:[\\/]", link_name):
                    raise RuntimeError(f"tar 链接目标为绝对路径，已拒绝: {member.name} -> {link_name}")
                link_target = save_path / Path(member_name).parent / link_name
                _ensure_within(save_path, link_target)
        tf.extractall(save_path)

def _normalize_member_name(name: str) -> str:
    """把压缩包成员名统一成 POSIX 风格，避免反斜杠绕过路径检查。"""

    return name.replace("\\", "/")

def _is_zip_symlink(member: zipfile.ZipInfo) -> bool:
    """
    判断 zip 成员是否是符号链接。

    zip 在 Unix 下会把文件模式写在 external_attr 的高 16 位。
    """

    mode = (member.external_attr >> 16) & 0xFFFF
    return stat.S_ISLNK(mode)

def _scan_extracted_dangerous_nodes(save_path: Path) -> None:
    """
    解压后统一扫描危险节点类型。

    防止解压库在不同格式下生成符号链接、设备节点或 FIFO。
    """

    for node in save_path.rglob("*"):
        try:
            mode = node.lstat().st_mode
        except FileNotFoundError:
            continue

        if stat.S_ISLNK(mode):
            kind = "symlink"
        elif stat.S_ISBLK(mode):
            kind = "block"
        elif stat.S_ISCHR(mode):
            kind = "char"
        elif stat.S_ISFIFO(mode):
            kind = "fifo"
        else:
            continue

        raise RuntimeError(f"解压结果包含不安全的特殊文件类型，已拒绝: {node} ({kind})")

def safe_extract_rar(path: Path, save_path: Path) -> None:
    """
    安全解压 rar。

    先逐成员检查路径，再执行解压，避免路径越界写入。
    """

    if rarfile is None:
        raise RuntimeError("当前环境未安装 rarfile，无法解压 .rar 文件。")

    with rarfile.RarFile(path) as rf:
        members = rf.infolist()
        for member in members:
            member_name = _normalize_member_name(getattr(member, "filename", ""))
            if not member_name:
                continue
            _ensure_within(save_path, save_path / member_name)
        for member in members:
            rf.extract(member, path=save_path)
    _scan_extracted_dangerous_nodes(save_path)

def safe_extract_7z(path: Path, save_path: Path) -> None:
    """
    安全解压 7z。

    先读取成员名做路径检查，再执行解压。
    """

    if py7zr is None:
        raise RuntimeError("当前环境未安装 py7zr，无法解压 .7z 文件。")

    with py7zr.SevenZipFile(path, "r") as sf:
        member_names = sf.getnames()

    for member_name in member_names:
        normalized = _normalize_member_name(member_name)
        if not normalized:
            continue
        _ensure_within(save_path, save_path / normalized)

    with py7zr.SevenZipFile(path, "r") as sf:
        sf.extractall(path=save_path)
    _scan_extracted_dangerous_nodes(save_path)

def extract_archive(path: Path, save_path: Path) -> None:
    """
    处理下载文件到可遍历目录。

    支持两类输入：
    1) 压缩包：zip/tar/rar/7z
    2) 直出文件：csv/ts（直接复制）
    """

    lower_name = path.name.lower()
    save_path.mkdir(parents=True, exist_ok=True)

    if lower_name.endswith((".csv", ".ts")):
        shutil.copy2(path, save_path / path.name)
        return

    if lower_name.endswith(".zip"):
        safe_extract_zip(path, save_path)
        return

    if lower_name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        safe_extract_tar(path, save_path)
        return

    if lower_name.endswith(".rar"):
        safe_extract_rar(path, save_path)
        return

    if lower_name.endswith(".7z"):
        safe_extract_7z(path, save_path)
        return

    if zipfile.is_zipfile(path):
        safe_extract_zip(path, save_path)
        return
    if tarfile.is_tarfile(path):
        safe_extract_tar(path, save_path)
        return

    raise RuntimeError(f"不支持的压缩格式: {path.name}")
