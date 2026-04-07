#!/bin/bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

HARNESS_DIR="$TMP_ROOT/harness"
FAKE_HOME="$TMP_ROOT/home"
FAKE_BIN="$TMP_ROOT/bin"

mkdir -p "$HARNESS_DIR" "$FAKE_HOME/workspace/quant/.venv/bin" "$FAKE_BIN"

cp "$PROJECT_DIR/QuantClass Sync.command" "$HARNESS_DIR/QuantClass Sync.command"
cp "$PROJECT_DIR/requirements.txt" "$HARNESS_DIR/requirements.txt"
chmod +x "$HARNESS_DIR/QuantClass Sync.command"

cat > "$FAKE_HOME/workspace/quant/.venv/bin/python" <<'EOF'
#!/bin/bash
set -euo pipefail

if [ "${1:-}" = "-c" ]; then
    code="${2:-}"
    if [[ "$code" == *"from quantclass_sync_internal import cli"* ]]; then
        exit 0
    fi
    exit 0
fi

if [ "${1:-}" = "quantclass_sync.py" ] && [ "${2:-}" = "gui" ]; then
    exit 0
fi

echo "unexpected shared python invocation: $*" >&2
exit 1
EOF
chmod +x "$FAKE_HOME/workspace/quant/.venv/bin/python"

cat > "$FAKE_BIN/python3" <<'EOF'
#!/bin/bash
set -euo pipefail

if [ "${1:-}" = "-c" ]; then
    exit 0
fi

echo "system python3 should not be selected" >&2
exit 99
EOF
chmod +x "$FAKE_BIN/python3"

OUTPUT="$(
    HOME="$FAKE_HOME" \
    PATH="$FAKE_BIN:/usr/bin:/bin" \
    bash "$HARNESS_DIR/QuantClass Sync.command" </dev/null 2>&1 || true
)"

echo "$OUTPUT" | grep -q "已选择环境类型: workspace_venv"
echo "$OUTPUT" | grep -q "Python 路径: $FAKE_HOME/workspace/quant/.venv/bin/python"
