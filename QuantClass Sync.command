#!/bin/bash
# QuantClass Sync GUI 启动脚本（macOS 双击运行）
# conda 环境名存储在 user_config.json 的 conda_env 字段

cd "$(dirname "$0")" || exit 1

CONFIG_FILE="user_config.json"

# 从 user_config.json 读取 conda_env（用 python 解析 JSON，避免依赖 jq）
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

# 将 conda_env 写入 user_config.json（保留其他字段）
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

# 读取或首次设置 conda 环境名
CONDA_ENV=$(_read_conda_env)
if [ -z "$CONDA_ENV" ]; then
    echo "=== QuantClass Sync 首次启动 ==="
    echo ""
    echo "请输入用于运行本项目的 conda 环境名（如 base、quant 等）："
    read -r CONDA_ENV
    if [ -z "$CONDA_ENV" ]; then
        echo "错误：环境名不能为空。"
        echo "按任意键退出..."
        read -n 1
        exit 1
    fi
    _write_conda_env "$CONDA_ENV"
    echo "已保存环境名: $CONDA_ENV"
    echo ""
fi

# 激活 conda 环境
# 尝试常见的 conda 初始化路径
if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniconda3/etc/profile.d/conda.sh"
elif [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
    source "$HOME/anaconda3/etc/profile.d/conda.sh"
elif [ -f "$HOME/miniforge3/etc/profile.d/conda.sh" ]; then
    source "$HOME/miniforge3/etc/profile.d/conda.sh"
elif [ -f "/opt/homebrew/Caskroom/miniconda/base/etc/profile.d/conda.sh" ]; then
    source "/opt/homebrew/Caskroom/miniconda/base/etc/profile.d/conda.sh"
elif command -v conda &> /dev/null; then
    eval "$(conda shell.bash hook)"
else
    echo "错误：未找到 conda。请确认已安装 conda 并可通过终端访问。"
    echo ""
    echo "按任意键退出..."
    read -n 1
    exit 1
fi

if ! conda activate "$CONDA_ENV"; then
    echo "错误：无法激活 conda 环境 '$CONDA_ENV'。"
    echo "请检查环境名是否正确，或编辑 $CONFIG_FILE 中的 conda_env 字段。"
    echo ""
    echo "按任意键退出..."
    read -n 1
    exit 1
fi

echo "已激活环境: $CONDA_ENV"
echo "Python 路径: $(which python)"
echo ""

# 检查 requirements.txt 是否存在
if [ ! -f "requirements.txt" ]; then
    echo "错误：未找到 requirements.txt，请重新下载完整安装包。"
    echo ""
    echo "按任意键退出..."
    read -n 1
    exit 1
fi

# 依赖检测：验证核心包、GUI 包、数据处理包
if ! python -c "from quantclass_sync_internal import cli; import webview; import pandas" 2>/dev/null; then
    echo "首次运行，正在安装依赖..."
    pip install -r requirements.txt
    echo ""
    # 再次检测（不静默 stderr，方便诊断安装后仍失败的原因）
    if ! python -c "from quantclass_sync_internal import cli; import webview; import pandas"; then
        echo "错误：依赖安装失败，请检查网络连接或手动执行："
        echo "  pip install -r requirements.txt"
        echo ""
        echo "按任意键退出..."
        read -n 1
        exit 1
    fi
    echo "依赖安装完成。"
    echo ""
fi

echo "启动 QuantClass Sync GUI..."
echo ""

python quantclass_sync.py gui
if [ $? -ne 0 ]; then
    echo ""
    echo "程序异常退出，按任意键关闭..."
    read -n 1
fi

# GUI 正常退出后，自动关闭本终端窗口（按名称匹配，不误关其他窗口）
osascript -e 'tell application "Terminal" to close (every window whose name contains "QuantClass Sync")' &>/dev/null &
