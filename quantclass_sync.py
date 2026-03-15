"""QuantClass 数据同步工具入口。"""
import sys
from quantclass_sync_internal.cli import app


def main() -> int:
    """兼容入口：处理 Ctrl+C，返回标准退出码。"""
    try:
        app()
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
