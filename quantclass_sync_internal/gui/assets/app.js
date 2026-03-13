// Alpine.js 数据组件
// 等待 alpine:init 事件注册组件，再等待 pywebview:ready 后初始化数据
document.addEventListener('alpine:init', () => {
    Alpine.data('app', () => ({
        // ===== 全局状态 =====
        tab: 'overview',   // 当前 Tab: 'overview' | 'sync'
        loading: true,     // 总览是否正在加载
        products: [],      // 产品列表，每项包含 name/color/local_date/behind_days/last_result
        summary: { green: 0, yellow: 0, red: 0, gray: 0 }, // 四色计数
        dataRoot: '',      // 数据目录路径
        lastRun: null,     // 上次同步时间字符串
        configExists: false,
        overviewError: '',  // 总览加载错误信息

        // ===== 同步状态 =====
        syncStatus: 'idle',    // 'idle' | 'syncing' | 'done' | 'error'
        currentProduct: '',    // 当前正在处理的产品名
        completed: 0,          // 已完成产品数
        total: 0,              // 总产品数
        elapsedSeconds: 0,     // 已用秒数
        errorMessage: '',      // 错误信息（error 状态时）
        runSummary: null,      // 完成后的摘要对象
        pollTimer: null,       // setInterval 句柄

        // ===== 初始化 =====
        // Alpine.js 会在组件挂载时调用 init()
        // 优先检查 pywebview 是否已就绪，否则监听事件
        init() {
            if (window.pywebview) {
                // 延迟到下一轮事件循环，确保 Alpine 组件已完全挂载
                setTimeout(() => this.loadOverview(), 0);
            } else {
                window.addEventListener('pywebview:ready', () => {
                    this.loadOverview();
                });
            }
        },

        // ===== 总览数据加载 =====
        // 调用 Python 端 get_overview()，返回产品列表和统计摘要
        async loadOverview() {
            this.loading = true;
            this.overviewError = '';
            try {
                const data = await window.pywebview.api.get_overview();
                if (data.ok === false) {
                    // Python 端返回错误（配置缺失等）
                    this.overviewError = data.error || '数据加载失败';
                    this.products = [];
                    this.summary = { green: 0, yellow: 0, red: 0, gray: 0 };
                } else {
                    this.products = data.products || [];
                    this.summary = data.summary || { green: 0, yellow: 0, red: 0, gray: 0 };
                    this.dataRoot = data.data_root || '';
                    this.lastRun = data.last_run;
                    this.configExists = true;
                }
            } catch (e) {
                console.error('loadOverview failed:', e);
                this.overviewError = String(e);
            }
            this.loading = false;
        },

        // ===== Tab 切换 =====
        // 切换到总览时自动刷新数据（同步进行中不刷新，避免干扰）
        switchTab(name) {
            this.tab = name;
            if (name === 'overview' && this.syncStatus !== 'syncing') {
                this.loadOverview();
            }
        },

        // ===== 开始同步 =====
        // 调用 Python 端 start_sync()，成功后切换到 syncing 状态并启动轮询
        async startSync() {
            if (this.syncStatus === 'syncing') return;
            // 立即标记为 syncing，防止双击穿透（后端也有锁保护作为最终屏障）
            this.syncStatus = 'syncing';
            this.completed = 0;
            this.total = 0;
            this.currentProduct = '';
            this.elapsedSeconds = 0;
            this.errorMessage = '';
            this.runSummary = null;
            try {
                const result = await window.pywebview.api.start_sync();
                if (result.started) {
                    this.startPolling();
                } else {
                    // Python 端拒绝启动（如已有任务在跑）
                    this.errorMessage = result.message || '无法启动同步';
                    this.syncStatus = 'error';
                }
            } catch (e) {
                console.error('startSync failed:', e);
                this.errorMessage = String(e);
                this.syncStatus = 'error';
            }
        },

        // ===== 进度轮询 =====
        // 每秒轮询一次 get_sync_progress()，根据 status 更新 UI
        startPolling() {
            if (this.pollTimer) clearInterval(this.pollTimer);
            this.pollTimer = setInterval(async () => {
                try {
                    const p = await window.pywebview.api.get_sync_progress();
                    this.currentProduct = p.current_product || '';
                    this.completed = p.completed || 0;
                    this.total = p.total || 0;
                    this.elapsedSeconds = p.elapsed_seconds || 0;

                    if (p.status === 'done') {
                        this.syncStatus = 'done';
                        this.runSummary = p.run_summary;
                        this.stopPolling();
                    } else if (p.status === 'error') {
                        this.syncStatus = 'error';
                        this.errorMessage = p.error_message || '同步失败';
                        this.stopPolling();
                    }
                    // status === 'syncing' 时继续轮询，无需额外处理
                } catch (e) {
                    console.error('poll failed:', e);
                    // 网络/窗口异常时不切换状态，等待下次轮询
                }
            }, 1000);
        },

        stopPolling() {
            if (this.pollTimer) {
                clearInterval(this.pollTimer);
                this.pollTimer = null;
            }
        },

        // ===== 重置为 idle =====
        // "再次同步" 和 "重试" 按钮共用
        resetSync() {
            this.stopPolling();
            this.syncStatus = 'idle';
            this.currentProduct = '';
            this.completed = 0;
            this.total = 0;
            this.elapsedSeconds = 0;
            this.errorMessage = '';
            this.runSummary = null;
        },

        // ===== 格式化工具函数 =====

        // 进度百分比，total=0 时返回 0 避免除零
        progressPercent() {
            if (this.total <= 0) return 0;
            return Math.round((this.completed / this.total) * 100);
        },

        // 预估剩余时间：用已用时/已完成数推算剩余
        estimatedRemaining() {
            if (this.completed <= 0 || this.elapsedSeconds <= 0) return '--';
            const rate = this.elapsedSeconds / this.completed;
            const remaining = rate * (this.total - this.completed);
            if (remaining <= 0) return '--';
            if (remaining < 60) return Math.round(remaining) + ' 秒';
            return Math.round(remaining / 60) + ' 分钟';
        },

        // 将秒数格式化为可读时长
        formatDuration(seconds) {
            if (!seconds && seconds !== 0) return '--';
            if (seconds < 60) return Math.round(seconds) + ' 秒';
            const m = Math.floor(seconds / 60);
            const s = Math.round(seconds % 60);
            return m + ' 分 ' + s + ' 秒';
        },
    }));
});
