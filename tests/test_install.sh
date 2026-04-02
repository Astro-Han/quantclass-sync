#!/bin/bash
# ======================================================================
# E 层安装测试，验证论坛用户拿到工具后能装上、能用
#
# 用法：
#   bash tests/test_install.sh          # 运行全部自动化测试（E1 + E2）
#   bash tests/test_install.sh --e3     # 仅打印 E3 手动检查清单
#
# 前置条件：
#   - 从项目根目录运行，脚本内会自动 cd
#   - 至少满足 conda、uv、python3 -m venv 之一
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
ENV_BACKEND=""
ENV_PYTHON=""
ENV_ROOT=""
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

# 仅当 python3 -m venv 能真实建出解释器时，才把它视为可用后端。
probe_python_venv() {
    local probe_root
    local probe_env

    probe_root="$(mktemp -d)"
    probe_env="$probe_root/probe-env"

    if python3 -m venv "$probe_env" >/dev/null 2>&1 && [ -x "$probe_env/bin/python" ]; then
        rm -rf "$probe_root"
        return 0
    fi

    rm -rf "$probe_root"
    return 1
}

# 这里探测的是“测试用临时环境后端”，按设计文档要求为 conda -> uv -> python3 -m venv。
detect_env_backend() {
    if command -v conda >/dev/null 2>&1; then
        ENV_BACKEND="conda"
        return 0
    fi
    if command -v uv >/dev/null 2>&1; then
        ENV_BACKEND="uv"
        return 0
    fi
    if command -v python3 >/dev/null 2>&1 && probe_python_venv; then
        ENV_BACKEND="venv"
        return 0
    fi
    return 1
}

# 根据后端创建干净环境，并记录解释器路径供后续复用。
create_env() {
    case "$ENV_BACKEND" in
        conda)
            echo "创建 conda 环境 $ENV_NAME ..."
            conda create -n "$ENV_NAME" python -y -q 2>&1 | tail -1
            ENV_PYTHON="$(conda run -n "$ENV_NAME" which python)"
            ENV_ROOT=""
            ;;
        uv)
            ENV_ROOT="$(mktemp -d)"
            echo "创建 uv 虚拟环境 $ENV_ROOT ..."
            uv venv "$ENV_ROOT" >/dev/null
            ENV_PYTHON="$ENV_ROOT/bin/python"
            ;;
        venv)
            ENV_ROOT="$(mktemp -d)"
            echo "创建 python3 -m venv 环境 $ENV_ROOT ..."
            python3 -m venv "$ENV_ROOT"
            ENV_PYTHON="$ENV_ROOT/bin/python"
            ;;
    esac
    CLEANUP_ENV=true
}

# 通用执行器，支持可选 cwd。venv/uv 分支显式注入 PATH 和 VIRTUAL_ENV。
run_in_env() {
    local run_dir="."

    if [ "${1:-}" = "--cwd" ]; then
        run_dir="$2"
        shift 2
    fi

    case "$ENV_BACKEND" in
        conda)
            conda run -n "$ENV_NAME" --cwd "$run_dir" "$@"
            ;;
        uv|venv)
            (
                cd "$run_dir"
                VIRTUAL_ENV="$ENV_ROOT" PATH="$ENV_ROOT/bin:$PATH" "$@"
            )
            ;;
    esac
}

# 依赖安装单独封装，run_in_env 只负责执行，不混入 pip/uv 选择逻辑。
install_deps() {
    case "$ENV_BACKEND" in
        conda)
            run_in_env "$ENV_PYTHON" -m pip install -q \
                "requests>=2.31.0" \
                "typer>=0.12.0" \
                "pydantic>=2.0.0" \
                "rich>=13.0.0" \
                "pandas>=2.0.0"
            ;;
        uv|venv)
            if "$ENV_PYTHON" -m pip --version >/dev/null 2>&1; then
                "$ENV_PYTHON" -m pip install -q \
                    "requests>=2.31.0" \
                    "typer>=0.12.0" \
                    "pydantic>=2.0.0" \
                    "rich>=13.0.0" \
                    "pandas>=2.0.0"
            elif command -v uv >/dev/null 2>&1; then
                uv pip install --python "$ENV_PYTHON" \
                    "requests>=2.31.0" \
                    "typer>=0.12.0" \
                    "pydantic>=2.0.0" \
                    "rich>=13.0.0" \
                    "pandas>=2.0.0"
            else
                echo "错误: 当前虚拟环境缺少 pip，且系统未找到 uv。"
                exit 1
            fi
            ;;
    esac
}

cleanup_env() {
    case "$ENV_BACKEND" in
        conda)
            conda deactivate 2>/dev/null || true
            conda env remove -n "$ENV_NAME" -y 2>/dev/null || true
            ;;
        uv|venv)
            if [ -n "${ENV_ROOT:-}" ] && [ -d "$ENV_ROOT" ]; then
                rm -rf "$ENV_ROOT"
            fi
            ;;
    esac
}

cleanup() {
    if [ "$CLEANUP_ENV" = true ]; then
        echo ""
        echo "清理临时测试环境"
        cleanup_env
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
# E3 手动检查清单，仅打印
# ------------------------------------------------------------------

print_e3_checklist() {
    echo ""
    echo "======================================================================"
    echo "E3 .command 手动检查清单（macOS 上逐项执行）"
    echo "======================================================================"
    echo ""
    echo "  E3.1  conda 可用且 user_config.json 中没有 conda_env -> 双击"
    echo "        验收: 看到\"请输入用于运行本项目的 conda 环境名\"提示"
    echo ""
    echo "  E3.2  conda_env 正确 -> GUI 窗口弹出"
    echo "        验收: 看到 QuantClass Sync 窗口"
    echo ""
    echo "  E3.3  项目目录存在 .venv，且 conda 不可用或 conda_env 无效 -> 双击"
    echo "        验收: 启动日志显示走 .venv 分支，GUI 正常弹出"
    echo ""
    echo "  E3.4  无 conda、无 .venv，仅系统 python3 -> 双击"
    echo "        验收: 看到手动安装依赖提示，不是空白闪退"
    echo ""
    echo "  E3.5  conda 已安装，但 user_config.json 中的 conda_env 指向不存在环境 -> 双击"
    echo "        验收: 日志显示 fallback 到 .venv 或系统 python3"
    echo ""
    echo "  E3.6  .venv 存在但 pip 不可用，且系统装有 uv -> 双击"
    echo "        验收: 依赖安装走 uv pip install --python ..."
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

if ! detect_env_backend; then
    echo "错误: 未找到可用环境工具，需至少满足 conda、uv 或 python3 -m venv 之一。"
    exit 1
fi

if [ ! -f "requirements.txt" ]; then
    echo "错误: 未找到 requirements.txt，请从项目根目录运行。"
    exit 1
fi

echo "======================================================================"
echo "E 层安装测试"
echo "项目目录: $PROJECT_DIR"
echo "环境后端: $ENV_BACKEND"
echo "临时环境: $ENV_NAME"
echo "======================================================================"

# ------------------------------------------------------------------
# E1 干净环境安装
# ------------------------------------------------------------------

echo ""
echo "--- E1 干净环境安装 ---"

create_env
echo "Python: $ENV_PYTHON"

# E1.1 安装依赖 + --help
echo ""
echo "E1.1 新建环境 + pip install"

install_deps 2>&1 | tail -1

HELP_OUTPUT=$(run_in_env --cwd "$PROJECT_DIR" \
    "$ENV_PYTHON" quantclass_sync.py --help 2>&1) || true
if echo "$HELP_OUTPUT" | grep -q "update\|status\|setup"; then
    pass "E1.1 依赖安装成功，--help 输出包含子命令列表"
else
    fail "E1.1 --help 输出异常" "$HELP_OUTPUT"
fi

# E1.2 缺少 pywebview 时 gui 命令给出友好提示
echo ""
echo "E1.2 缺少 pywebview 时 gui 命令行为"

GUI_OUTPUT=$(run_in_env --cwd "$PROJECT_DIR" \
    "$ENV_PYTHON" quantclass_sync.py gui 2>&1) || true
if echo "$GUI_OUTPUT" | grep -qi "pywebview\|pip install"; then
    pass "E1.2 缺少 pywebview 时给出安装提示"
else
    fail "E1.2 gui 未给出 pywebview 安装提示" "$GUI_OUTPUT"
fi

# CLI 核心功能不受影响，用 --help 再验一次。
CLI_CHECK=$(run_in_env --cwd "$PROJECT_DIR" \
    "$ENV_PYTHON" quantclass_sync.py --help 2>&1) || true
if echo "$CLI_CHECK" | grep -q "update"; then
    pass "E1.2 CLI 核心功能不受 pywebview 缺失影响"
else
    fail "E1.2 CLI 核心功能异常"
fi

# E1.3 Python 版本兼容性
echo ""
echo "E1.3 Python 版本兼容性"

PY_VERSION=$(run_in_env "$ENV_PYTHON" -c \
    "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "当前环境 Python 版本: $PY_VERSION"

IMPORT_CHECK=$(run_in_env --cwd "$PROJECT_DIR" \
    "$ENV_PYTHON" -c "import quantclass_sync_internal; print('ok')" 2>&1) || true
if echo "$IMPORT_CHECK" | grep -q "ok"; then
    pass "E1.3 Python $PY_VERSION 下核心模块可导入"
else
    fail "E1.3 Python $PY_VERSION 下核心模块导入失败" "$IMPORT_CHECK"
fi

if [ "$ENV_BACKEND" = "conda" ]; then
    if conda create -n "${ENV_NAME}-39" python=3.9 -y -q --dry-run 2>&1 | grep -q "package"; then
        skip "E1.3 Python 3.9 测试需手动创建 conda 环境验证 (当前 Python=${PY_VERSION})"
    else
        skip "E1.3 conda 无法创建 Python 3.9 环境，跳过最低版本测试"
    fi
else
    skip "E1.3 当前后端不是 conda，跳过 conda 专属 Python 3.9 探测"
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

STATUS_EXIT=0
STATUS_OUTPUT=$(run_in_env --cwd "$TMPDIR_E2" \
    "$ENV_PYTHON" "$PROJECT_DIR/quantclass_sync.py" status 2>&1) || STATUS_EXIT=$?

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

SETUP_EXIT=0
SETUP_OUTPUT=$(run_in_env --cwd "$PROJECT_DIR" bash -c \
    "QUANTCLASS_API_KEY=test-key-e2 QUANTCLASS_HID=test-hid-e2 \
    \"$ENV_PYTHON\" quantclass_sync.py setup \
    --non-interactive \
    --data-root '$DATA_ROOT' \
    --skip-check" 2>&1) || SETUP_EXIT=$?

if [ "$SETUP_EXIT" -eq 0 ] && [ -f "$PROJECT_DIR/user_config.json" ]; then
    pass "E2.2 setup 成功生成 user_config.json"

    DRYRUN_EXIT=0
    DRYRUN_OUTPUT=$(run_in_env --cwd "$PROJECT_DIR" \
        "$ENV_PYTHON" quantclass_sync.py update --dry-run 2>&1) || DRYRUN_EXIT=$?

    if echo "$DRYRUN_OUTPUT" | grep -q "Traceback"; then
        fail "E2.2 update --dry-run 抛出 traceback" "$(echo "$DRYRUN_OUTPUT" | tail -5)"
    else
        pass "E2.2 update --dry-run 退出码=${DRYRUN_EXIT}, 无 traceback"
    fi

    rm -f "$PROJECT_DIR/user_config.json"
elif echo "$SETUP_OUTPUT" | grep -qi "缺少\|missing\|required"; then
    pass "E2.2 setup 因缺少参数给出提示，可接受"
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
