// Alpine.js 数据组件
// 等待 alpine:init 事件注册组件，再等待 pywebviewready 后初始化数据
document.addEventListener('alpine:init', () => {
    Alpine.data('app', () => ({
        // ===== 全局状态 =====
        currentView: 'main',  // 视图切换: 'setup' | 'main'
        tab: 'overview',      // 当前 Tab: 'overview' | 'sync' | 'history'
        loading: true,        // 总览是否正在加载
        products: [],         // 产品列表，每项包含 name/color/local_date/behind_days/last_result/last_error
        summary: { green: 0, yellow: 0, red: 0, gray: 0 }, // 四色计数
        dataRoot: '',         // 数据目录路径
        lastRun: null,        // 上次同步信息对象
        overviewError: '',    // 总览加载错误信息
        canOpenDir: false,    // 数据目录是否可以点击打开（后端支持时为 true）

        // ===== Setup 向导状态 =====
        setupDataRoot: '',         // 向导表单：数据目录
        setupApiKey: '',           // 向导表单：API Key
        setupHid: '',              // 向导表单：HID
        setupCourseType: 'basic',  // 向导表单：课程类型（basic / premium）
        setupLoading: false,       // 向导提交中
        setupError: '',            // 向导错误信息
        setupWarning: '',          // 向导警告（保存成功但验证失败）
        setupConfirmDir: '',       // 待确认创建的目录路径（非空时显示确认 UI）

        // ===== 筛选状态 =====
        searchText: '',        // 搜索文本（按产品名模糊匹配）
        filterColor: 'all',    // 筛选颜色: 'all' | 'green' | 'yellow' | 'red' | 'gray'

        // ===== 历史状态 =====
        historyList: [],       // 历史运行列表
        historyDetail: null,   // 当前查看的运行详情（null 时显示列表）
        historyLoading: false, // 历史页加载中
        historyError: '',      // 历史页错误信息
        historyLoaded: false,  // 历史列表是否已加载过（避免重复请求）

        // ===== 检查更新状态 =====
        checkUpdateLoading: false,
        checkUpdateConfirmVisible: false,
        checkUpdateResult: null,  // {message, isError} 或 null

        // ===== 健康检查状态 =====
        healthState: 'idle',     // idle | confirming | checking | done | error
        healthProgress: null,    // {current, total, product}
        healthResult: null,      // check_data_health 完整结果
        healthError: '',
        healthPollTimer: null,
        repairMessage: '',

        // ===== 同步状态 =====
        syncStatus: 'idle',   // 'idle' | 'syncing' | 'done' | 'error'
        currentProduct: '',   // 最近完成/正在处理的产品名
        completed: 0,         // 已完成产品数
        total: 0,             // 总产品数
        elapsedSeconds: 0,    // 已用秒数
        errorMessage: '',     // 错误信息（error 状态时）
        runSummary: null,     // 完成后的摘要对象
        pollTimer: null,      // setTimeout 句柄
        syncProducts: [],     // 同步过程中已处理产品列表（每项含 name/status/elapsed/files_count/error）
        allProducts: [],      // 全部待同步产品名（用于计算等待中列表）
        showWaiting: false,   // 等待中产品列表是否展开
        postprocessing: false, // 后处理阶段标志
        postprocessDetail: '', // 后处理描述（用户可读）
        estimateData: null,   // API 调用量预估数据（confirm_needed 时填充，用于展示确认卡片）

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
        // 同时读取 get_config() 获取 can_open_dir
        async loadOverview() {
            this.loading = true;
            this.overviewError = '';
            try {
                // 并行拉取 overview 和 config，减少等待时间
                const [data, cfg] = await Promise.all([
                    window.pywebview.api.get_overview(),
                    window.pywebview.api.get_config().catch(() => ({}))
                ]);
                // 读取 can_open_dir 标志（后端可能不返回，默认 false）
                this.canOpenDir = !!(cfg && cfg.can_open_dir);

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

        // ===== 打开数据目录 =====
        // 调用后端打开系统文件管理器，需 can_open_dir = true 时才展示入口
        async openDataDir() {
            if (!this.canOpenDir || !this.dataRoot) return;
            try {
                await window.pywebview.api.open_data_dir();
            } catch (e) {
                console.error('openDataDir failed:', e);
            }
        },

        // ===== 筛选方法 =====

        // 按 searchText 和 filterColor 过滤并排序产品列表
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
        switchTab(name) {
            this.tab = name;
            if (name === 'overview' && this.syncStatus !== 'syncing'
                && !(this.checkUpdateResult && !this.checkUpdateResult.isError)) {
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
            this.syncProducts = [];
            this.allProducts = [];
            this.showWaiting = false;
            this.postprocessing = false;
            this.postprocessDetail = '';
            this.estimateData = null;
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

        // ===== 重试失败产品 =====
        // 调用 start_sync(true) 只重跑上次失败的产品
        async retryFailed() {
            if (this.syncStatus === 'syncing') return;
            this.syncStatus = 'syncing';
            this.completed = 0;
            this.total = 0;
            this.currentProduct = '';
            this.elapsedSeconds = 0;
            this.errorMessage = '';
            this.runSummary = null;
            this.syncProducts = [];
            this.allProducts = [];
            this.showWaiting = false;
            this.postprocessing = false;
            this.postprocessDetail = '';
            this.estimateData = null;
            try {
                // true 表示仅重试失败产品
                const result = await window.pywebview.api.start_sync(true);
                if (result.started) {
                    this.startPolling();
                } else {
                    this.errorMessage = result.message || '无法启动同步';
                    this.syncStatus = 'error';
                }
            } catch (e) {
                console.error('retryFailed failed:', e);
                this.errorMessage = String(e);
                this.syncStatus = 'error';
            }
        },

        // ===== 等待中产品列表（计算属性） =====
        // allProducts 中去掉已在 syncProducts 里出现的，剩余即"等待中"
        waitingProducts() {
            const doneNames = new Set(this.syncProducts.map(p => p.name));
            return this.allProducts.filter(name => !doneNames.has(name));
        },

        // ===== 进度轮询 =====
        // 用 setTimeout 递归代替 setInterval，避免 async 回调堆积
        startPolling() {
            this.stopPolling();
            const poll = async () => {
                try {
                    const p = await window.pywebview.api.get_sync_progress();
                    this.currentProduct = p.current_product || '';
                    this.completed = p.completed || 0;
                    this.total = p.total || 0;
                    this.elapsedSeconds = p.elapsed_seconds || 0;

                    // 更新产品列表和全部产品名
                    if (p.products && Array.isArray(p.products)) {
                        this.syncProducts = p.products;
                    }
                    if (p.all_products && Array.isArray(p.all_products)) {
                        this.allProducts = p.all_products;
                    }

                    // confirm_needed：后台线程等待用户确认，展示确认卡片
                    if (p.status === 'confirm_needed' && p.estimate) {
                        this.estimateData = p.estimate;
                        this.postprocessing = false;
                        // 不切换 syncStatus，继续轮询等待用户点击确认/取消
                    } else if (p.status === 'postprocessing') {
                        this.postprocessing = true;
                        this.postprocessDetail = p.postprocess_detail || '';
                    } else if (p.status === 'done') {
                        this.syncStatus = 'done';
                        this.postprocessing = false;
                        this.postprocessDetail = '';
                        this.estimateData = null;
                        this.runSummary = p.run_summary;
                        this.historyLoaded = false; // 有新运行，下次切历史页时刷新
                        this.checkUpdateResult = null; // 同步后清除检查更新结果
                        this.pollTimer = null;
                        return; // 终态，不再调度下次轮询
                    } else if (p.status === 'error') {
                        this.syncStatus = 'error';
                        this.postprocessing = false;
                        this.postprocessDetail = '';
                        this.estimateData = null;
                        this.errorMessage = p.error_message || '同步失败';
                        this.runSummary = p.run_summary;  // 部分失败时也携带摘要
                        this.historyLoaded = false;
                        this.checkUpdateResult = null;
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
            this.syncProducts = [];
            this.allProducts = [];
            this.estimateData = null;
        },

        // ===== API 调用量确认 =====

        // 用户点击"继续同步"：通知后台线程继续，关闭确认卡片
        async confirmSync() {
            try {
                await window.pywebview.api.confirm_sync();
            } catch (e) {
                console.error('confirmSync failed:', e);
            }
            this.estimateData = null;
        },

        // 用户点击"取消"：通知后台线程取消，await 完成后再切状态（避免请求期间状态已变）
        async cancelSync() {
            try {
                await window.pywebview.api.cancel_sync();
            } catch (e) {
                console.error('cancelSync failed:', e);
            } finally {
                this.estimateData = null;
                this.syncStatus = 'idle';
                this.stopPolling();
            }
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

        // 格式化文件数和耗时（同步产品列表 meta 行）
        // 返回如 "1.2s · 826 文件" 或 "0.5s"
        formatProductMeta(elapsed, filesCount) {
            let parts = [];
            if (elapsed != null) parts.push(Math.round(elapsed * 10) / 10 + 's');
            if (filesCount != null && filesCount > 0) parts.push(filesCount + ' 文件');
            return parts.join(' · ');
        },

        // 将 ISO 时间字符串转换为本地时区的 YYYY-MM-DD HH:mm 格式
        // 后端返回的时间为 UTC，new Date() 会自动转为本地时区
        formatLocalTime(isoStr) {
            if (!isoStr) return '';
            try {
                const d = new Date(isoStr);
                const pad = n => String(n).padStart(2, '0');
                return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
                    + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
            } catch (e) {
                return isoStr;
            }
        },

        // ===== 历史页方法 =====

        async loadHistory() {
            this.historyLoading = true;
            this.historyDetail = null;
            this.historyError = '';
            try {
                const data = await window.pywebview.api.get_history();
                if (data.ok === false) {
                    this.historyError = data.error || '历史记录加载失败';
                    this.historyList = [];
                    this.historyLoaded = false;
                } else {
                    this.historyList = data.runs || [];
                    this.historyLoaded = true;
                }
            } catch (e) {
                console.error('loadHistory failed:', e);
                this.historyError = String(e);
                this.historyList = [];
                this.historyLoaded = false;
            }
            this.historyLoading = false;
        },

        // 查看指定运行的产品明细（防重入）
        async viewDetail(reportFile) {
            if (this.historyLoading) return;
            this.historyLoading = true;
            this.historyError = '';
            try {
                const data = await window.pywebview.api.get_run_detail(reportFile);
                if (data.ok === false) {
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

        // 历史详情按失败→跳过→成功排序
        sortedDetailProducts() {
            if (!this.historyDetail || !this.historyDetail.products) return [];
            const order = { error: 0, skipped: 1, ok: 2 };
            return [...this.historyDetail.products].sort(
                (a, b) => (order[a.status] ?? 3) - (order[b.status] ?? 3)
            );
        },

        // ===== 检查更新 =====

        async doCheckUpdates() {
            this.checkUpdateConfirmVisible = false;
            if (this.checkUpdateLoading) return;
            this.checkUpdateLoading = true;
            this.checkUpdateResult = null;
            try {
                const res = await window.pywebview.api.check_updates();
                if (res.ok === false) {
                    this.checkUpdateResult = { message: res.error, isError: true };
                } else {
                    this.products = res.products;
                    this.summary = res.summary;
                    const msg = '成功查询 ' + res.checked + ' 个产品' +
                        (res.failed > 0 ? '，' + res.failed + ' 个查询失败' : '');
                    this.checkUpdateResult = { message: msg, isError: false };
                }
            } catch (e) {
                console.error('checkUpdates failed:', e);
                this.checkUpdateResult = { message: String(e), isError: true };
            }
            this.checkUpdateLoading = false;
        },

        // ===== 健康检查（异步：确认→进度→结果） =====

        showHealthConfirm() { this.healthState = 'confirming'; },
        cancelHealthCheck() { this.healthState = 'idle'; },
        async startHealthCheck() {
            this.repairMessage = '';
            const res = await window.pywebview.api.start_health_check();
            if (!res.ok) { this.healthError = res.error; this.healthState = 'error'; return; }
            this.healthState = 'checking';
            this.healthProgress = {current: 0, total: 0, product: ''};
            this.pollHealthProgress();
        },
        pollHealthProgress() {
            this.healthPollTimer = setTimeout(async () => {
                const prog = await window.pywebview.api.get_health_progress();
                this.healthProgress = prog;
                if (prog.checking) { this.pollHealthProgress(); return; }
                const result = await window.pywebview.api.get_health_result();
                if (result && result.ok) {
                    this.healthResult = result.health;
                    this.healthState = 'done';
                } else {
                    this.healthError = result ? result.error : '检查失败';
                    this.healthState = 'error';
                }
            }, 1000);
        },
        async repairIssues() {
            const res = await window.pywebview.api.repair_health_issues();
            if (res.ok) {
                const r = res.repair;
                this.repairMessage = '已修复 ' + r.repaired.length + ' 个问题' +
                    (r.failed.length ? '，' + r.failed.length + ' 个失败' : '') +
                    '。可重新检查验证结果。';
            } else {
                this.repairMessage = '修复失败: ' + (res.error || '未知错误');
            }
        },
        closeHealthResult() { this.healthState = 'idle'; this.healthResult = null; this.repairMessage = ''; },
        healthIssuesByCategory() {
            if (!this.healthResult) return {};
            const groups = {};
            for (const i of this.healthResult.issues) {
                (groups[i.category] = groups[i.category] || []).push(i);
            }
            return groups;
        },
        repairableCount() {
            if (!this.healthResult) return 0;
            return this.healthResult.issues.filter(i => i.repairable).length;
        },
        categoryLabel(cat) {
            return {file_integrity:'文件完整性',content_integrity:'内容完整性',temporal_integrity:'时间完整性',coverage_integrity:'覆盖完整性',format_integrity:'格式完整性'}[cat] || cat;
        },
        repairLabel(issue) {
            if (issue.repairable) return '可修复';
            if (issue.repair_action === 'needs_resync') return '需重新同步';
            return '需调查';
        },
        issueTypeLabel(type) {
            return {
                missing_data: '数据缺失', orphan_temp: '临时文件', csv_unreadable: '不可读',
                tail_corruption: '尾部残行', infra_db_corrupt: '数据库损坏', infra_json_corrupt: 'JSON损坏',
                duplicate_rows: '重复行', null_key_fields: '字段空值',
                date_exceeds_timestamp: '日期超前', timestamp_data_gap: '日期落后',
                missing_trading_days: '缺失交易日', file_count_drop: '文件数下降',
                column_inconsistency: '列名不一致',
            }[type] || type;
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

        setupValid() {
            return this.setupDataRoot.trim() && this.setupApiKey.trim() && this.setupHid.trim();
        },

        async submitSetup() {
            if (!this.setupValid() || this.setupLoading) return;
            this.setupLoading = true;
            this.setupError = '';
            this.setupWarning = '';
            this.setupConfirmDir = '';
            try {
                const result = await window.pywebview.api.run_setup(
                    this.setupDataRoot.trim(),
                    this.setupApiKey.trim(),
                    this.setupHid.trim(),
                    false,
                    this.setupCourseType
                );
                if (!result.ok && result.error_code === 'dir_not_found') {
                    this.setupConfirmDir = result.resolved_path;
                } else {
                    this._handleSetupResult(result);
                }
            } catch (e) {
                console.error('submitSetup failed:', e);
                this.setupError = String(e);
            }
            this.setupLoading = false;
        },

        async confirmCreateDir() {
            if (!this.setupValid()) return;
            this.setupLoading = true;
            this.setupConfirmDir = '';
            try {
                const result = await window.pywebview.api.run_setup(
                    this.setupDataRoot.trim(),
                    this.setupApiKey.trim(),
                    this.setupHid.trim(),
                    true,
                    this.setupCourseType
                );
                this._handleSetupResult(result);
            } catch (e) {
                console.error('confirmCreateDir failed:', e);
                this.setupError = String(e);
            }
            this.setupLoading = false;
        },

        cancelCreateDir() {
            this.setupConfirmDir = '';
        },

        _handleSetupResult(result) {
            if (!result.ok) {
                this.setupError = result.error || '配置保存失败';
                return;
            }
            if (result.warning) {
                this.setupWarning = result.warning;
                return;
            }
            this.currentView = 'main';
            this.loadOverview();
        },

        continueAnyway() {
            this.currentView = 'main';
            this.loadOverview();
        },

        retrySetup() {
            this.setupWarning = '';
            this.setupApiKey = '';
            this.setupHid = '';
        },

        async goToSetup() {
            try {
                const cfg = await window.pywebview.api.get_config();
                this.setupDataRoot = (cfg.data_root || '');
            } catch (e) {
                this.setupDataRoot = '';
            }
            this.setupApiKey = '';
            this.setupHid = '';
            this.setupError = '';
            this.setupWarning = '';
            this.setupConfirmDir = '';
            this.currentView = 'setup';
        },
    }));
});
