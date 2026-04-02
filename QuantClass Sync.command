#!/bin/bash
# QuantClass Sync GUI 启动脚本（macOS 双击运行）
# conda 环境名存储在 user_config.json 的 conda_env 字段

cd "$(dirname "$0")" || exit 1

PROJECT_DIR="$(pwd)"
CONFIG_FILE="user_config.json"
PYTHON=""
ENV_TYPE=""
CONDA_ENV=""

# 从 user_config.json 读取 conda_env，继续保留现有的全局 python3 依赖。
_read_conda_env() {
    python3 -c "
import json, sys
try:
    data = json.load(open('$CONFIG_FILE'))
    env = data.get('conda_env', '')
    print(env if env else '', end='')
except Exception:
    print('', end='')
" 2>/dev/null
}

# 将 conda_env 写回 user_config.json，其他字段保持不动。
_write_conda_env() {
    python3 -c "
import json, sys
env = '$1'
try:
    data = json.load(open('$CONFIG_FILE'))
except Exception:
    data = {}
data['conda_env'] = env
with open('$CONFIG_FILE', 'w') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
    f.write('\n')
" 2>/dev/null
}

# 统一暂停并退出，避免各分支散落重复交互代码。
_pause_and_exit() {
    local exit_code="$1"
    echo ""
    echo "按任意键退出..."
    read -n 1
    exit "$exit_code"
}

# fallback 日志统一走这里，保证用户能看懂脚本为什么跳到下一级。
_log_fallback() {
    echo "$1"
    echo ""
}

# 尝试加载 conda shell，成功返回 0，失败返回 1。
_load_conda_shell() {
    if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
        source "$HOME/miniconda3/etc/profile.d/conda.sh"
        return 0
    fi
    if [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
        source "$HOME/anaconda3/etc/profile.d/conda.sh"
        return 0
    fi
    if [ -f "$HOME/miniforge3/etc/profile.d/conda.sh" ]; then
        source "$HOME/miniforge3/etc/profile.d/conda.sh"
        return 0
    fi
    if [ -f "/opt/homebrew/Caskroom/miniconda/base/etc/profile.d/conda.sh" ]; then
        source "/opt/homebrew/Caskroom/miniconda/base/etc/profile.d/conda.sh"
        return 0
    fi
    if command -v conda >/dev/null 2>&1; then
        eval "$(conda shell.bash hook)"
        return 0
    fi
    return 1
}

# 只有 conda 工具真实可用时，才允许首次询问 conda 环境名。
_ensure_conda_env_name() {
    CONDA_ENV="$(_read_conda_env)"
    if [ -n "$CONDA_ENV" ]; then
        return 0
    fi

    echo "=== QuantClass Sync 首次启动 ==="
    echo ""
    echo "请输入用于运行本项目的 conda 环境名（如 base、quant 等）："
    read -r CONDA_ENV
    if [ -z "$CONDA_ENV" ]; then
        echo "错误：环境名不能为空。"
        _pause_and_exit 1
    fi
    _write_conda_env "$CONDA_ENV"
    echo "已保存环境名: $CONDA_ENV"
    echo ""
}

# conda 分支必须满足“能加载 conda + 有环境名 + activate 成功”。
_try_activate_conda() {
    if ! _load_conda_shell; then
        _log_fallback "未找到 conda，尝试项目 .venv..."
        return 1
    fi

    _ensure_conda_env_name
    if ! conda activate "$CONDA_ENV"; then
        _log_fallback "conda 环境 '$CONDA_ENV' 无法激活，尝试项目 .venv..."
        return 1
    fi

    PYTHON="$(command -v python)"
    ENV_TYPE="conda"
    return 0
}

# `.venv` 只认项目根目录固定路径。
_try_activate_project_venv() {
    local venv_python="$PROJECT_DIR/.venv/bin/python"

    if [ ! -x "$venv_python" ]; then
        _log_fallback "未找到项目 .venv，尝试系统 python3..."
        return 1
    fi

    # activate 让 PATH 与 VIRTUAL_ENV 保持一致，缺失时仍可直接用解释器路径运行。
    if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
        source "$PROJECT_DIR/.venv/bin/activate"
    fi

    PYTHON="$venv_python"
    ENV_TYPE="venv"
    return 0
}

# 最后兜底系统 python3。
_try_use_system_python() {
    if ! command -v python3 >/dev/null 2>&1; then
        return 1
    fi

    PYTHON="$(command -v python3)"
    ENV_TYPE="system"
    return 0
}

_ensure_requirements_file() {
    if [ -f "$PROJECT_DIR/requirements.txt" ]; then
        return 0
    fi

    echo "错误：未找到 requirements.txt，请重新下载完整安装包。"
    _pause_and_exit 1
}

# 核心依赖检查统一走当前选中的解释器。
_deps_ready() {
    "$PYTHON" -c "from quantclass_sync_internal import cli; import webview; import pandas" 2>/dev/null
}

# 不同环境给出不同提示，避免论坛用户去猜该执行哪条命令。
_print_dependency_help() {
    case "$ENV_TYPE" in
        conda)
            echo "错误：依赖安装失败，请手动执行："
            echo "  $PYTHON -m pip install -r requirements.txt"
            ;;
        venv)
            if "$PYTHON" -m pip --version >/dev/null 2>&1; then
                echo "错误：依赖安装失败，请手动执行："
                echo "  $PYTHON -m pip install -r requirements.txt"
            elif command -v uv >/dev/null 2>&1; then
                echo "错误：当前 .venv 没有 pip，请手动执行："
                echo "  uv pip install --python \"$PYTHON\" -r requirements.txt"
            else
                echo "错误：当前 .venv 没有 pip，且系统未找到 uv。"
                echo "请先安装 pip 或 uv 后重试。"
            fi
            ;;
        system)
            echo "检测到系统 python3，但缺少运行依赖。"
            if "$PYTHON" -m pip --version >/dev/null 2>&1; then
                echo "请先手动执行："
                echo "  python3 -m pip install -r requirements.txt"
            else
                echo "请先执行："
                echo "  python3 -m ensurepip"
                echo "然后再执行："
                echo "  python3 -m pip install -r requirements.txt"
            fi
            echo "安装完成后，请重新双击 QuantClass Sync.command。"
            ;;
    esac
}

# conda / venv 自动补依赖，system 仅提示，不自动安装。
_install_requirements_if_needed() {
    if _deps_ready; then
        return 0
    fi

    case "$ENV_TYPE" in
        conda)
            echo "检测到 conda 环境缺少依赖，正在安装..."
            "$PYTHON" -m pip install -r requirements.txt
            ;;
        venv)
            echo "检测到项目 .venv 缺少依赖，正在安装..."
            if "$PYTHON" -m pip --version >/dev/null 2>&1; then
                "$PYTHON" -m pip install -r requirements.txt
            elif command -v uv >/dev/null 2>&1; then
                uv pip install --python "$PYTHON" -r requirements.txt
            else
                _print_dependency_help
                _pause_and_exit 1
            fi
            ;;
        system)
            _print_dependency_help
            _pause_and_exit 1
            ;;
    esac

    if ! _deps_ready; then
        _print_dependency_help
        _pause_and_exit 1
    fi
}

if ! _try_activate_conda && ! _try_activate_project_venv && ! _try_use_system_python; then
    echo "错误：未找到可用 Python 环境。"
    echo "建议安装以下任一环境："
    echo "  1. conda，并在 user_config.json 中填写 conda_env"
    echo "  2. 项目根目录 .venv，可用 uv 创建：uv venv .venv"
    echo "  3. 系统 python3，并确保命令行可访问"
    _pause_and_exit 1
fi

echo "已选择环境类型: $ENV_TYPE"
echo "Python 路径: $PYTHON"
echo ""

_ensure_requirements_file
_install_requirements_if_needed

echo "启动 QuantClass Sync GUI..."
echo ""

"$PYTHON" quantclass_sync.py gui
EXIT_CODE=$?
if [ "$EXIT_CODE" -ne 0 ]; then
    echo ""
    echo "程序异常退出，按任意键关闭..."
    read -n 1
    exit "$EXIT_CODE"
fi

# 仅正常退出时自动关闭本终端窗口（按名称匹配，不误关其他窗口）。
osascript -e 'tell application "Terminal" to close (every window whose name contains "QuantClass Sync")' &>/dev/null &
