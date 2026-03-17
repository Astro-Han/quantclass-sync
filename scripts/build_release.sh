#!/usr/bin/env bash
# 打包发布 zip，白名单逐文件添加
# 用法：bash scripts/build_release.sh v1.0

set -euo pipefail

# --- 参数检查 ---
if [[ $# -lt 1 ]]; then
    echo "错误：缺少版本号参数" >&2
    echo "用法：bash scripts/build_release.sh v1.0" >&2
    exit 1
fi

VERSION="$1"
OUTPUT="release/quantclass-sync-tool-${VERSION}.zip"

# --- 白名单 ---
FILES=(
    "README.md"
    "catalog.txt"
    "requirements.txt"
    "quantclass_sync.py"
    "coin_preprocess_builtin.py"
    "QuantClass Sync.command"
    "quantclass_sync_internal/__init__.py"
    "quantclass_sync_internal/cli.py"
    "quantclass_sync_internal/config.py"
    "quantclass_sync_internal/models.py"
    "quantclass_sync_internal/constants.py"
    "quantclass_sync_internal/orchestrator.py"
    "quantclass_sync_internal/file_sync.py"
    "quantclass_sync_internal/csv_engine.py"
    "quantclass_sync_internal/http_client.py"
    "quantclass_sync_internal/status_store.py"
    "quantclass_sync_internal/reporting.py"
    "quantclass_sync_internal/data_query.py"
    "quantclass_sync_internal/archive.py"
    "quantclass_sync_internal/gui/__init__.py"
    "quantclass_sync_internal/gui/api.py"
    "quantclass_sync_internal/gui/assets/index.html"
    "quantclass_sync_internal/gui/assets/style.css"
    "quantclass_sync_internal/gui/assets/app.js"
    "quantclass_sync_internal/gui/assets/alpine.min.js"
    "coin_preprocess_internal/__init__.py"
    "coin_preprocess_internal/runner.py"
    "coin_preprocess_internal/csv_source.py"
    "coin_preprocess_internal/symbol_mapper.py"
    "coin_preprocess_internal/pivot.py"
    "coin_preprocess_internal/constants.py"
    "scripts/repair_coin_cap.py"
)

# --- 检查所有文件是否存在 ---
echo "检查白名单文件..."
MISSING=0
for f in "${FILES[@]}"; do
    if ! test -e "$f"; then
        echo "  缺失：$f" >&2
        MISSING=1
    fi
done
if [[ $MISSING -ne 0 ]]; then
    echo "错误：存在缺失文件，打包中止" >&2
    exit 1
fi
echo "全部 ${#FILES[@]} 个文件均存在"

# --- 准备输出目录，清理旧包 ---
mkdir -p release
if [[ -f "$OUTPUT" ]]; then
    rm "$OUTPUT"
    echo "已删除旧包：$OUTPUT"
fi

# --- 逐文件追加到 zip（路径含空格用数组元素引用） ---
echo "开始打包..."
for f in "${FILES[@]}"; do
    zip -r "$OUTPUT" "$f"
done

echo ""
echo "打包完成：$OUTPUT"
echo ""
echo "--- 包内文件列表 ---"
zipinfo -1 "$OUTPUT"
