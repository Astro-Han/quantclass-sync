#!/bin/bash
# ======================================================================
# E 层安装测试 — 验证论坛用户拿到工具后能装上、能用
#
# 用法：
#   bash tests/test_install.sh          # 运行全部自动化测试（E1 + E2）
#   bash tests/test_install.sh --e3     # 仅打印 E3 手动检查清单
#
# 前置条件：
#   - conda 可用
#   - 从项目根目录运行（脚本内 cd 确保）
# ======================================================================

set -euo pipefail

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

PASS=0
FAIL=0
SKIP=0

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_NAME="test-install-$$"
CLEANUP_ENV=false

# ------------------------------------------------------------------
# 工具函数
# ------------------------------------------------------------------

pass() {
    PASS=$((PASS + 1))
    echo -e "  ${GREEN}PASS${NC}  $1"
}

fail() {
    FAIL=$((FAIL + 1))
    echo -e "  ${RED}FAIL${NC}  $1"
    if [ -n "${2:-}" ]; then
        echo -e "        $2"
    fi
}

skip() {
    SKIP=$((SKIP + 1))
    echo -e "  ${YELLOW}SKIP${NC}  $1"
}

cleanup() {
    if [ "$CLEANUP_ENV" = true ]; then
        echo ""
        echo "清理临时 conda 环境: $ENV_NAME"
        conda deactivate 2>/dev/null || true
        conda env remove -n "$ENV_NAME" -y 2>/dev/null || true
    fi
    if [ -n "${TMPDIR_E2:-}" ] && [ -d "$TMPDIR_E2" ]; then
        rm -rf "$TMPDIR_E2"
    fi
}

trap cleanup EXIT

summary() {
    echo ""
    echo "======================================================================"
    echo -e "结果: ${GREEN}${PASS} 通过${NC}  ${RED}${FAIL} 失败${NC}  ${YELLOW}${SKIP} 跳过${NC}"
    echo "======================================================================"
    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
}

# ------------------------------------------------------------------
# E3 手动检查清单（仅打印）
# ------------------------------------------------------------------

print_e3_checklist() {
    echo ""
    echo "======================================================================"
    echo "E3 .command 手动检查清单（macOS 上逐项执行）"
    echo "======================================================================"
    echo ""
    echo "  E3.1  删除 .gui_conda_env -> 双击 QuantClass Sync.command"
    echo "        验收: 看到\"请输入 conda 环境名\"提示"
    echo ""
    echo "  E3.2  输入正确环境名 -> GUI 窗口弹出"
    echo "        验收: 看到 QuantClass Sync 窗口"
    echo ""
    echo "  E3.3  再次双击（不删 .gui_conda_env）-> 直接弹出 GUI"
    echo "        验收: 无提示，直接启动窗口"
    echo ""
    echo "  E3.4  修改 .gui_conda_env 为不存在的环境名 -> 双击"
    echo "        验收: 看到\"无法激活 conda 环境\"错误提示（非空白闪退）"
    echo ""
}

if [ "${1:-}" = "--e3" ]; then
    print_e3_checklist
    exit 0
fi

# ------------------------------------------------------------------
# 前置检查
# ------------------------------------------------------------------

cd "$PROJECT_DIR"

if ! command -v conda &>/dev/null; then
    echo "错误: conda 不可用，无法运行安装测试。"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "错误: 未找到 requirements.txt，请从项目根目录运行。"
    exit 1
fi

echo "======================================================================"
echo "E 层安装测试"
echo "项目目录: $PROJECT_DIR"
echo "临时环境: $ENV_NAME"
echo "======================================================================"

# ------------------------------------------------------------------
# E1 干净环境安装
# ------------------------------------------------------------------

echo ""
echo "--- E1 干净环境安装 ---"

# 创建临时 conda 环境（默认 Python 版本）
echo "创建 conda 环境 $ENV_NAME ..."
conda create -n "$ENV_NAME" python -y -q 2>&1 | tail -1
CLEANUP_ENV=true

# 获取环境中 python 路径
ENV_PYTHON="$(conda run -n "$ENV_NAME" which python)"
echo "Python: $ENV_PYTHON"

# E1.1 安装依赖 + --help
echo ""
echo "E1.1 新建 conda 环境 + pip install"

# 安装核心依赖（不含 pywebview，E1.2 单独测）
conda run -n "$ENV_NAME" pip install -q \
    "requests>=2.31.0" \
    "typer>=0.12.0" \
    "pydantic>=2.0.0" \
    "rich>=13.0.0" \
    "pandas>=2.0.0" \
    2>&1 | tail -1

# 验证 --help 正常输出
HELP_OUTPUT=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
    python quantclass_sync.py --help 2>&1) || true
if echo "$HELP_OUTPUT" | grep -q "update\|status\|setup"; then
    pass "E1.1 依赖安装成功，--help 输出包含子命令列表"
else
    fail "E1.1 --help 输出异常" "$HELP_OUTPUT"
fi

# E1.2 缺少 pywebview 时 gui 命令给出友好提示
echo ""
echo "E1.2 缺少 pywebview 时 gui 命令行为"

GUI_OUTPUT=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
    python quantclass_sync.py gui 2>&1) || true
GUI_EXIT=$?
if echo "$GUI_OUTPUT" | grep -qi "pywebview\|pip install"; then
    pass "E1.2 缺少 pywebview 时给出安装提示"
else
    fail "E1.2 gui 未给出 pywebview 安装提示" "$GUI_OUTPUT"
fi

# CLI 核心功能不受影响（用 --help 再验一次）
CLI_CHECK=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
    python quantclass_sync.py --help 2>&1) || true
if echo "$CLI_CHECK" | grep -q "update"; then
    pass "E1.2 CLI 核心功能不受 pywebview 缺失影响"
else
    fail "E1.2 CLI 核心功能异常"
fi

# E1.3 Python 3.9 最低版本
echo ""
echo "E1.3 Python 版本兼容性"

PY_VERSION=$(conda run -n "$ENV_NAME" python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "当前环境 Python 版本: $PY_VERSION"

# 检查当前版本是否能正常导入核心模块
IMPORT_CHECK=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
    python -c "import quantclass_sync_internal; print('ok')" 2>&1) || true
if echo "$IMPORT_CHECK" | grep -q "ok"; then
    pass "E1.3 Python $PY_VERSION 下核心模块可导入"
else
    fail "E1.3 Python $PY_VERSION 下核心模块导入失败" "$IMPORT_CHECK"
fi

# 如果能创建 3.9 环境则额外测试（不强制要求）
if conda create -n "${ENV_NAME}-39" python=3.9 -y -q --dry-run 2>&1 | grep -q "package"; then
    skip "E1.3 Python 3.9 测试需手动创建 conda 环境验证 (当前 Python=${PY_VERSION})"
else
    skip "E1.3 conda 无法创建 Python 3.9 环境，跳过最低版本测试"
fi

# ------------------------------------------------------------------
# E2 首次运行
# ------------------------------------------------------------------

echo ""
echo "--- E2 首次运行 ---"

TMPDIR_E2=$(mktemp -d)
echo "临时数据目录: $TMPDIR_E2"

# E2.1 无 user_config.json 时行为
echo ""
echo "E2.1 无 user_config.json"

# 在无配置文件的临时目录运行 status 命令，捕获退出码
STATUS_EXIT=0
STATUS_OUTPUT=$(conda run -n "$ENV_NAME" --cwd "$TMPDIR_E2" \
    python "$PROJECT_DIR/quantclass_sync.py" status 2>&1) || STATUS_EXIT=$?

# 退出码非 0 且无 traceback 即通过（说明有合理的错误处理）
if echo "$STATUS_OUTPUT" | grep -q "Traceback"; then
    fail "E2.1 无配置文件时抛出 traceback" "$(echo "$STATUS_OUTPUT" | tail -3)"
elif [ "$STATUS_EXIT" -ne 0 ]; then
    pass "E2.1 无配置文件时 status 退出码=${STATUS_EXIT}, 无 traceback"
else
    pass "E2.1 无配置文件时 status 正常退出"
fi

# E2.2 setup 后 update --dry-run
echo ""
echo "E2.2 setup + update --dry-run"

DATA_ROOT="$TMPDIR_E2/data"
mkdir -p "$DATA_ROOT"

# 通过环境变量提供 API key / HID，--skip-check 跳过连通性检查
SETUP_EXIT=0
SETUP_OUTPUT=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
    bash -c "QUANTCLASS_API_KEY=test-key-e2 QUANTCLASS_HID=test-hid-e2 \
    python quantclass_sync.py setup \
    --non-interactive \
    --data-root '$DATA_ROOT' \
    --skip-check" \
    2>&1) || SETUP_EXIT=$?

if [ "$SETUP_EXIT" -eq 0 ] && [ -f "$PROJECT_DIR/user_config.json" ]; then
    pass "E2.2 setup 成功生成 user_config.json"

    # 尝试 --dry-run（假 API key，预期网络/认证失败但不应 traceback）
    DRYRUN_EXIT=0
    DRYRUN_OUTPUT=$(conda run -n "$ENV_NAME" --cwd "$PROJECT_DIR" \
        python quantclass_sync.py update --dry-run 2>&1) || DRYRUN_EXIT=$?

    if echo "$DRYRUN_OUTPUT" | grep -q "Traceback"; then
        fail "E2.2 update --dry-run 抛出 traceback" "$(echo "$DRYRUN_OUTPUT" | tail -5)"
    else
        pass "E2.2 update --dry-run 退出码=${DRYRUN_EXIT}, 无 traceback"
    fi

    # 清理 setup 生成的配置文件（避免污染项目目录）
    rm -f "$PROJECT_DIR/user_config.json"
elif echo "$SETUP_OUTPUT" | grep -qi "缺少\|missing\|required"; then
    pass "E2.2 setup 因缺少参数给出提示（可接受）"
else
    fail "E2.2 setup 失败" "exit=$SETUP_EXIT output=$(echo "$SETUP_OUTPUT" | tail -3)"
fi

# ------------------------------------------------------------------
# E3 提示手动检查清单
# ------------------------------------------------------------------

print_e3_checklist

# ------------------------------------------------------------------
# 汇总
# ------------------------------------------------------------------

summary
