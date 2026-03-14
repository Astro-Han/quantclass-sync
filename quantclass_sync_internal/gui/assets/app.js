// Alpine.js 数据组件
// 等待 alpine:init 事件注册组件，再等待 pywebviewready 后初始化数据
document.addEventListener('alpine:init', () => {
    Alpine.data('app', () => ({
        // ===== 全局状态 =====
        currentView: 'main',  // 视图切换: 'setup' | 'main'
        tab: 'overview',   // 当前 Tab: 'overview' | 'sync' | 'history'
        loading: true,     // 总览是否正在加载
        products: [],      // 产品列表，每项包含 name/color/local_date/behind_days/last_result
        summary: { green: 0, yellow: 0, red: 0, gray: 0 }, // 四色计数
        dataRoot: '',      // 数据目录路径
        lastRun: null,     // 上次同步时间字符串
        overviewError: '',  // 总览加载错误信息

        // ===== Setup 向导状态 =====
        setupDataRoot: '',     // 向导表单：数据目录
        setupApiKey: '',       // 向导表单：API Key
        setupHid: '',          // 向导表单：HID
        setupLoading: false,   // 向导提交中
        setupError: '',        // 向导错误信息
        setupWarning: '',      // 向导警告（保存成功但验证失败）

        // ===== 筛选状态 =====
        searchText: '',        // 搜索文本（按产品名模糊匹配）
        filterColor: 'all',   // 筛选颜色: 'all' | 'green' | 'yellow' | 'red' | 'gray'

        // ===== 历史状态 =====
        historyList: [],       // 历史运行列表
        historyDetail: null,   // 当前查看的运行详情（null 时显示列表）
        historyLoading: false, // 历史页加载中
        historyError: '',      // 历史页错误信息
        historyLoaded: false,  // 历史列表是否已加载过（避免重复请求）

        // ===== 健康检查状态 =====
        healthReport: null,        // 健康报告结果对象（null 表示未检查过）
        healthLoading: false,      // 是否正在检查中
        healthError: '',           // 检查失败时的错误信息

        // ===== 同步状态 =====
        syncStatus: 'idle',    // 'idle' | 'syncing' | 'done' | 'error'
        currentProduct: '',    // 最近完成的产品名
        completed: 0,          // 已完成产品数
        total: 0,              // 总产品数
        elapsedSeconds: 0,     // 已用秒数
        errorMessage: '',      // 错误信息（error 状态时）
        runSummary: null,      // 完成后的摘要对象
        pollTimer: null,       // setTimeout 句柄

        // ===== 初始化 =====
        // 先调 get_config() 判断视图：config_exists → main，否则 → setup
        init() {
            const doInit = async () => {
                try {
                    const cfg = await window.pywebview.api.get_config();
                    this.currentView = cfg.config_exists ? 'main' : 'setup';
                    if (this.currentView === 'main') {
                        this.loadOverview();
                    }
                } catch (e) {
                    console.error('init get_config failed:', e);
                    this.currentView = 'setup';
                }
            };
            if (window.pywebview && window.pywebview.api) {
                setTimeout(() => doInit(), 0);
            } else {
                window.addEventListener('pywebviewready', () => doInit());
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
                    // Python 端返回错误（配置缺失等），清空所有派生状态避免残留旧值
                    this.overviewError = data.error || '数据加载失败';
                    this.products = [];
                    this.summary = { green: 0, yellow: 0, red: 0, gray: 0 };
                    this.dataRoot = '';
                    this.lastRun = null;
                } else {
                    this.products = data.products || [];
                    this.summary = data.summary || { green: 0, yellow: 0, red: 0, gray: 0 };
                    this.dataRoot = data.data_root || '';
                    this.lastRun = data.last_run;
                }
            } catch (e) {
                console.error('loadOverview failed:', e);
                this.overviewError = String(e);
                this.products = [];
                this.summary = { green: 0, yellow: 0, red: 0, gray: 0 };
                this.dataRoot = '';
                this.lastRun = null;
            }
            this.loading = false;
        },

        // ===== 筛选方法 =====

        // 按 searchText 和 filterColor 过滤并排序产品列表
        // 注：table x-for 和 empty-state x-show 各调一次，共两次，产品数 <100 无性能问题
        filteredProducts() {
            const order = { red: 0, yellow: 1, green: 2, gray: 3 };
            return this.products
                .filter(p => {
                    if (this.filterColor !== 'all' && p.color !== this.filterColor) return false;
                    if (this.searchText && !p.name.toLowerCase().includes(this.searchText.toLowerCase())) return false;
                    return true;
                })
                .sort((a, b) => (order[a.color] ?? 4) - (order[b.color] ?? 4));
        },

        // 点击统计卡片切换筛选（再点一次恢复 all）
        toggleFilter(color) {
            this.filterColor = this.filterColor === color ? 'all' : color;
        },

        // ===== Tab 切换 =====
        // 切换到总览时自动刷新数据（同步进行中不刷新，避免干扰）
        switchTab(name) {
            this.tab = name;
            if (name === 'overview' && this.syncStatus !== 'syncing') {
                this.loadOverview();
            }
            if (name === 'history' && !this.historyLoaded && !this.historyLoading) {
                this.loadHistory();
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
        // 用 setTimeout 递归代替 setInterval，避免 async 回调堆积
        // （如果一次轮询耗时超过 1 秒，setInterval 会堆积回调）
        startPolling() {
            this.stopPolling();
            const poll = async () => {
                try {
                    const p = await window.pywebview.api.get_sync_progress();
                    this.currentProduct = p.current_product || '';
                    this.completed = p.completed || 0;
                    this.total = p.total || 0;
                    this.elapsedSeconds = p.elapsed_seconds || 0;

                    if (p.status === 'done') {
                        this.syncStatus = 'done';
                        this.runSummary = p.run_summary;
                        this.historyLoaded = false; // 有新运行，下次切历史页时刷新
                        this.pollTimer = null;
                        return; // 终态，不再调度下次轮询
                    } else if (p.status === 'error') {
                        this.syncStatus = 'error';
                        this.errorMessage = p.error_message || '同步失败';
                        this.runSummary = p.run_summary;  // 部分失败时也携带摘要
                        this.historyLoaded = false; // 有新运行，下次切历史页时刷新
                        this.pollTimer = null;
                        return; // 终态，不再调度下次轮询
                    }
                } catch (e) {
                    console.error('poll failed:', e);
                    // 网络/窗口异常时不切换状态，继续下次轮询
                }
                // 上一次完成后才调度下一次，间隔 1 秒
                this.pollTimer = setTimeout(poll, 1000);
            };
            this.pollTimer = setTimeout(poll, 1000);
        },

        stopPolling() {
            if (this.pollTimer) {
                clearTimeout(this.pollTimer);
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

        // 将秒数格式化为可读时长（自动升级到小时级别）
        formatDuration(seconds) {
            if (!seconds && seconds !== 0) return '--';
            if (seconds < 60) return Math.round(seconds) + ' 秒';
            if (seconds < 3600) {
                const m = Math.floor(seconds / 60);
                const s = Math.round(seconds % 60);
                return m + ' 分 ' + s + ' 秒';
            }
            const h = Math.floor(seconds / 3600);
            const m = Math.floor((seconds % 3600) / 60);
            return h + ' 小时 ' + m + ' 分';
        },

        // ===== 历史页方法 =====

        // 加载历史运行列表
        async loadHistory() {
            this.historyLoading = true;
            // 刷新列表时关闭详情视图，确保用户看到最新列表
            this.historyDetail = null;
            this.historyError = '';
            try {
                const data = await window.pywebview.api.get_history();
                if (data.ok === false) {
                    this.historyError = data.error || '历史记录加载失败';
                    this.historyList = [];
                    this.historyLoaded = false; // 失败后允许重试
                } else {
                    this.historyList = data.runs || [];
                    this.historyLoaded = true;
                }
            } catch (e) {
                console.error('loadHistory failed:', e);
                this.historyError = String(e);
                this.historyList = [];
                this.historyLoaded = false; // 异常后允许重试
            }
            this.historyLoading = false;
        },

        // 查看指定运行的产品明细（防重入：避免连续快速点击产生并发请求）
        async viewDetail(reportFile) {
            if (this.historyLoading) return;
            this.historyLoading = true;
            this.historyError = '';
            try {
                const data = await window.pywebview.api.get_run_detail(reportFile);
                if (data.ok === false) {
                    // 加载失败，留在列表视图并展示错误
                    this.historyError = data.error || '报告详情加载失败';
                    this.historyDetail = null;
                } else {
                    this.historyDetail = data;
                }
            } catch (e) {
                console.error('viewDetail failed:', e);
                this.historyError = String(e);
                this.historyDetail = null;
            }
            this.historyLoading = false;
        },

        // ===== 健康检查 =====

        // 调用 Python 端 get_health_report()，扫描数据目录检测三类问题
        async checkHealth() {
            if (this.healthLoading) return;
            this.healthLoading = true;
            this.healthError = '';
            try {
                const data = await window.pywebview.api.get_health_report();
                if (data.ok === false) {
                    this.healthError = data.error || '健康检查失败';
                } else {
                    this.healthReport = data.health;
                }
            } catch (e) {
                console.error('checkHealth failed:', e);
                this.healthError = String(e);
            }
            this.healthLoading = false;
        },

        // 返回历史列表（如有新同步记录待刷新，自动重新加载）
        backToList() {
            this.historyError = '';
            if (!this.historyLoaded) {
                this.loadHistory(); // 内部会清除 historyDetail
            } else {
                this.historyDetail = null;
            }
        },

        // ===== Setup 向导方法 =====

        // 表单验证：三个字段均非空
        setupValid() {
            return this.setupDataRoot.trim() && this.setupApiKey.trim() && this.setupHid.trim();
        },

        // 提交 setup 表单
        async submitSetup() {
            if (!this.setupValid() || this.setupLoading) return;
            this.setupLoading = true;
            this.setupError = '';
            this.setupWarning = '';
            try {
                const result = await window.pywebview.api.run_setup(
                    this.setupDataRoot.trim(),
                    this.setupApiKey.trim(),
                    this.setupHid.trim(),
                    false
                );
                if (!result.ok && result.error_code === 'dir_not_found') {
                    // 目录不存在，弹确认创建
                    if (confirm('该目录不存在，是否创建？\n' + result.resolved_path)) {
                        const result2 = await window.pywebview.api.run_setup(
                            this.setupDataRoot.trim(),
                            this.setupApiKey.trim(),
                            this.setupHid.trim(),
                            true
                        );
                        this._handleSetupResult(result2);
                    }
                    // 用户取消 → 留在向导页
                } else {
                    this._handleSetupResult(result);
                }
            } catch (e) {
                console.error('submitSetup failed:', e);
                this.setupError = String(e);
            }
            this.setupLoading = false;
        },

        // 处理 run_setup 返回结果
        _handleSetupResult(result) {
            if (!result.ok) {
                this.setupError = result.error || '配置保存失败';
                return;
            }
            if (result.warning) {
                // 保存成功但验证失败，展示警告让用户选择
                this.setupWarning = result.warning;
                return;
            }
            // 保存+验证均成功，跳转主界面
            this.currentView = 'main';
            this.loadOverview();
        },

        // 验证警告后选择"仍然继续"
        continueAnyway() {
            this.currentView = 'main';
            this.loadOverview();
        },

        // 验证警告后选择"重新填写"（保留 data_root，清空凭证）
        retrySetup() {
            this.setupWarning = '';
            this.setupApiKey = '';
            this.setupHid = '';
        },

        // 从主界面进入"重新配置"（预填 data_root）
        async goToSetup() {
            try {
                const cfg = await window.pywebview.api.get_config();
                if (cfg.config_exists && cfg.data_root) {
                    this.setupDataRoot = cfg.data_root;
                }
            } catch (e) {
                // 忽略，用户可手动填写
            }
            this.setupApiKey = '';
            this.setupHid = '';
            this.setupError = '';
            this.setupWarning = '';
            this.currentView = 'setup';
        },
    }));
});
