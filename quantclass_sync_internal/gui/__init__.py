"""职责: pywebview 窗口启动逻辑。"""

from __future__ import annotations

from pathlib import Path

# assets 目录路径（index.html 和静态资源所在位置）
ASSETS_DIR = Path(__file__).parent / "assets"


def launch_gui() -> None:
    """启动 GUI 窗口，被 cli.py 的 cmd_gui 命令调用。"""
    try:
        import webview
    except ImportError:
        print(
            "GUI 需要 pywebview 依赖。\n"
            "安装方法: pip install pywebview\n"
            "安装后重新运行: python quantclass_sync.py gui"
        )
        raise SystemExit(1)

    from .api import SyncApi

    api = SyncApi()
    # 创建窗口，挂载 SyncApi 实例供 JS 调用
    # 保留 window 引用：部分 pywebview 后端要求窗口对象在 start() 时仍存活
    window = webview.create_window(  # noqa: F841
        title="QuantClass Sync",
        url=str(ASSETS_DIR / "index.html"),
        js_api=api,
        width=900,
        height=600,
        min_size=(700, 450),
    )
    # 启动 pywebview 主循环（阻塞直到窗口关闭）
    webview.start(debug=False)

    # 窗口已关闭，强制退出以防 pywebview 后台线程残留导致进程不退出
    import os as _os
    _os._exit(0)
