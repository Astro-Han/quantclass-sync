"""职责: pywebview 窗口启动逻辑。"""

from __future__ import annotations

import sys
from pathlib import Path

# assets 目录路径（index.html 和静态资源所在位置）
ASSETS_DIR = Path(__file__).parent / "assets"


def _apply_macos_chrome(window) -> None:
    """macOS：网页铺满整个窗口，标题栏只保留红绿灯。"""

    import AppKit
    import webview
    from PyObjCTools import AppHelper

    # 旧版 pywebview 没有 before_show 事件，保留系统默认窗口
    if not hasattr(getattr(window, "events", None), "before_show"):
        return
    # 标题栏区域的点击会落到网页上，窗口拖动交给 .pywebview-drag-region；
    # 只认直接按在拖拽区域本身，避免点工具栏按钮时带动窗口
    webview.settings["DRAG_REGION_DIRECT_TARGET_ONLY"] = True

    def style_window() -> None:
        ns_window = window.native
        ns_window.setStyleMask_(ns_window.styleMask() | AppKit.NSWindowStyleMaskFullSizeContentView)
        ns_window.setTitlebarAppearsTransparent_(True)
        ns_window.setTitleVisibility_(AppKit.NSWindowTitleHidden)
        # 空的统一工具栏只用来把红绿灯下移到 52px 工具栏带的中线，与页面工具栏对齐
        ns_window.setToolbar_(AppKit.NSToolbar.alloc().initWithIdentifier_("main"))
        ns_window.setToolbarStyle_(AppKit.NSWindowToolbarStyleUnified)
        ns_window.setTitlebarSeparatorStyle_(AppKit.NSTitlebarSeparatorStyleNone)
        # pywebview 给标题栏容器涂了窗口底色，会盖住网页里的工具栏
        ns_window.contentView().superview().subviews().lastObject().setBackgroundColor_(AppKit.NSColor.clearColor())
        # 页面渲染前先露出随系统深浅色变化的窗口底色，避免深色模式启动时白屏一闪
        ns_window.setBackgroundColor_(AppKit.NSColor.windowBackgroundColor())
        ns_window.contentView().setValue_forKey_(False, "drawsBackground")

    window.events.before_show += lambda: AppHelper.callAfter(style_window)


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
        width=1080,
        height=700,
        min_size=(860, 560),
    )
    if sys.platform == "darwin":
        _apply_macos_chrome(window)
    # 启动 pywebview 主循环（阻塞直到窗口关闭）
    webview.start(debug=False)

    # 窗口已关闭，强制退出以防 pywebview 后台线程残留导致进程不退出
    import os as _os
    _os._exit(0)
