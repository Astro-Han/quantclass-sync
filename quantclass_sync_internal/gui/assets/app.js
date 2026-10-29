function deriveSyncStartedAtMs(syncStartedAtMs, backendElapsedSeconds, nowMs) {
    if (syncStartedAtMs != null) return syncStartedAtMs;
    const elapsedSeconds = Math.max(0, Number(backendElapsedSeconds) || 0);
    if (elapsedSeconds > 0) return nowMs - elapsedSeconds * 1000;
    return nowMs;
}

function computeDisplayedElapsedSeconds(syncStartedAtMs, backendElapsedSeconds, nowMs) {
    const elapsedSeconds = Math.max(0, Number(backendElapsedSeconds) || 0);
    if (syncStartedAtMs == null) return elapsedSeconds;
    return Math.max(elapsedSeconds, Math.max(0, (nowMs - syncStartedAtMs) / 1000));
}

// 分类展示顺序与侧栏图标；catalog 未标注分类的产品归入"其他"
const CATEGORIES = [
    { name: 'A股行情', icon: 'i-cat-quote', tile: 'var(--tile-quote)' },
    { name: 'A股市场', icon: 'i-cat-market', tile: 'var(--tile-market)' },
    { name: '财务', icon: 'i-cat-finance', tile: 'var(--tile-finance)' },
    { name: '因子', icon: 'i-cat-factor', tile: 'var(--tile-factor)' },
    { name: '宏观', icon: 'i-cat-macro', tile: 'var(--tile-macro)' },
    { name: '美股', icon: 'i-cat-us', tile: 'var(--tile-us)' },
    { name: '加密货币', icon: 'i-cat-crypto', tile: 'var(--tile-crypto)' },
    { name: '其他', icon: 'i-cat-all', tile: 'var(--tile-all)' },
];

const PAGE_TITLES = { overview: '总览', sync: '同步', history: '历史记录', health: '数据健康' };
const WEEKDAYS = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];

// Alpine.js 数据组件
// 等待 alpine:init 事件注册组件，再等待 pywebviewready 后初始化数据
document.addEventListener('alpine:init', () => {
    Alpine.data('app', () => ({
        // ===== 全局状态 =====
        currentView: 'main',  // 视图切换: 'setup' | 'main'
        page: 'overview',     // 当前页面: 'overview' | 'sync' | 'history' | 'health'
        loading: true,        // 总览是否正在加载
        products: [],         // 产品列表，每项包含 name/label/category/color/local_date/behind_days/last_result/last_error
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
        setupFromMain: false,      // 从主界面进入（可取消返回）

        // ===== 筛选状态 =====
        searchText: '',        // 搜索文本（按中文名或产品 ID 模糊匹配）
        segment: 'local',      // 'attention' | 'fresh' | 'local' | 'missing'
        category: '',          // 侧栏选中的分类，空字符串表示全部
        expanded: {},          // 展开完整错误信息的产品
        showMissing: false,    // "全部已下载"视图下是否展开未下载产品

        // ===== 历史状态 =====
        historyList: [],       // 历史运行列表
        historyDetail: null,   // 当前查看的运行详情
        historySelected: '',   // 当前选中的 report_file
        historyLoading: false, // 历史列表加载中
        detailLoading: false,  // 详情加载中
        historyError: '',      // 历史页错误信息
        historyLoaded: false,  // 历史列表是否已加载过（避免重复请求）

        // ===== 检查更新状态 =====
        checkUpdateLoading: false,
        checkUpdateConfirmVisible: false,
        checkUpdateResult: null,  // {message, isError} 或 null

        // ===== 健康检查状态 =====
        healthState: 'idle',     // idle | checking | done | error
        healthProgress: null,    // {current, total, product}
        healthResult: null,      // check_data_health 完整结果
        healthError: '',
        healthPollTimer: null,
        repairMessage: '',
        repairResult: null,   // 修复结果（含 repaired/failed 列表）
        repairing: false,     // 修复中标志

        // ===== 同步状态 =====
        syncStatus: 'idle',   // 'idle' | 'syncing' | 'done' | 'error'
        currentProduct: '',   // 最近完成/正在处理的产品名
        completed: 0,         // 已完成产品数
        total: 0,             // 总产品数
        elapsedSeconds: 0,    // 已用秒数
        displayElapsedSeconds: 0, // 顶部展示用耗时，可在前端平滑递增
        pausedDisplayElapsedSeconds: null, // confirm_needed 时冻结的展示耗时
        syncStartedAtMs: null, // 前端本地记录的同步起点，用于平滑显示总耗时
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
                if (window.pywebview.platform === 'cocoa') {
                    document.documentElement.classList.add('native');
                }
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
        // 调用后端打开系统文件管理器，需 can_open_dir = true 时才可点击
        async openDataDir() {
            if (!this.canOpenDir || !this.dataRoot) return;
            try {
                await window.pywebview.api.open_data_dir();
            } catch (e) {
                console.error('openDataDir failed:', e);
            }
        },

        // ===== 导航 =====
        go(page) {
            this.page = page;
            if (page === 'overview' && this.syncStatus !== 'syncing'
                && !(this.checkUpdateResult && !this.checkUpdateResult.isError)) {
                this.loadOverview();
            }
            if (page === 'history' && !this.historyLoaded && !this.historyLoading) {
                this.loadHistory();
            }
        },

        pageTitle() {
            return PAGE_TITLES[this.page] || '';
        },

        // 侧栏分类：切到总览并只看该分类，再点一次取消
        pickCategory(name) {
            this.category = this.category === name && this.page === 'overview' ? '' : name;
            if (this.page !== 'overview') this.go('overview');
        },

        dataDirName() {
            const parts = this.dataRoot.split('/').filter(Boolean);
            return parts.length ? parts[parts.length - 1] : '数据目录';
        },

        shortPath(path) {
            return (path || '').replace(/^\/Users\/[^/]+/, '~');
        },

        // ===== 产品展示 =====

        localProducts() {
            return this.products.filter(p => p.color !== 'gray');
        },

        attentionCount() {
            return this.summary.yellow + this.summary.red;
        },

        // 侧栏分类列表：只列出存在产品的分类，计数为已下载数量
        categories() {
            return CATEGORIES
                .map(c => ({
                    ...c,
                    total: this.products.filter(p => this.categoryOf(p) === c.name).length,
                    local: this.products.filter(p => this.categoryOf(p) === c.name && p.color !== 'gray').length,
                }))
                .filter(c => c.total > 0);
        },

        categoryOf(p) {
            return CATEGORIES.some(c => c.name === p.category) ? p.category : '其他';
        },

        labelOf(name) {
            const p = this.products.find(x => x.name === name);
            return p && p.label ? p.label : name;
        },

        segmentCount(seg) {
            return this.products.filter(p => this.inSegment(p, seg) && this.inCategory(p)).length;
        },

        inSegment(p, seg) {
            if (seg === 'attention') return p.color === 'yellow' || p.color === 'red';
            if (seg === 'fresh') return p.color === 'green';
            if (seg === 'missing') return p.color === 'gray';
            return p.color !== 'gray';
        },

        inCategory(p) {
            return !this.category || this.categoryOf(p) === this.category;
        },

        matchesSearch(p) {
            const q = this.searchText.trim().toLowerCase();
            return !q || p.name.toLowerCase().includes(q) || (p.label || '').toLowerCase().includes(q);
        },

        // 当前筛选下按分类分组的产品，组内需关注的排在前面
        groupedProducts(seg) {
            const order = { red: 0, yellow: 1, green: 2, gray: 3 };
            const items = this.products
                .filter(p => this.inSegment(p, seg || this.segment) && this.inCategory(p) && this.matchesSearch(p))
                .sort((a, b) => (order[a.color] ?? 4) - (order[b.color] ?? 4)
                    || (a.label || a.name).localeCompare(b.label || b.name, 'zh-CN'));
            return CATEGORIES
                .map(c => ({ ...c, items: items.filter(p => this.categoryOf(p) === c.name) }))
                .filter(g => g.items.length > 0);
        },

        missingProducts() {
            return this.products.filter(p => p.color === 'gray' && this.inCategory(p) && this.matchesSearch(p));
        },

        // 产品状态：图标、徽标文字和色调
        statusOf(p) {
            if (p.color === 'gray') return { cls: 'idle', icon: 'i-download', tone: '', text: '未下载' };
            if (p.last_result === 'error') return { cls: 'bad', icon: 'i-exclaim', tone: 'tone-bad', text: '同步失败' };
            if (p.color === 'green') return { cls: 'ok', icon: 'i-check', tone: 'tone-ok', text: '最新' };
            const lag = p.behind_days != null ? '落后 ' + p.behind_days + ' 天' : '落后';
            return p.color === 'red'
                ? { cls: 'bad', icon: 'i-exclaim', tone: 'tone-bad', text: lag }
                : { cls: 'warn', icon: 'i-exclaim', tone: 'tone-warn', text: lag };
        },

        // "产品 X 下载失败；可能原因：…；原始错误：超出当日下载次数" → "下载失败：超出当日下载次数"
        errorSummary(text) {
            if (!text) return '';
            const s = text.replace(/^产品 \S+ /, '');
            const raw = /原始错误：(.+)$/.exec(s);
            const head = s.split('；')[0];
            return raw ? head + '：' + raw[1] : head;
        },

        toggleExpanded(name) {
            this.expanded = { ...this.expanded, [name]: !this.expanded[name] };
        },

        overviewHeadline() {
            const local = this.localProducts().length;
            if (local === 0) return '还没有已下载的产品';
            const n = this.attentionCount();
            return n === 0 ? '所有产品都是最新的' : n + ' 个产品需要关注';
        },

        overviewTone() {
            if (this.localProducts().length === 0) return 'tone-accent';
            if (this.summary.red > 0) return 'tone-bad';
            return this.summary.yellow > 0 ? 'tone-warn' : 'tone-ok';
        },

        overviewSub() {
            const parts = ['已下载 ' + this.localProducts().length + ' 个产品'];
            if (this.lastRun) {
                parts.push('上次同步 ' + this.formatRelative(this.lastRun.started_at));
            } else {
                parts.push('尚未同步');
            }
            return parts.join(' · ');
        },

        // ===== 开始同步 =====
        // 调用 Python 端 start_sync()，成功后切换到 syncing 状态并启动轮询
        async startSync() {
            if (this.syncStatus === 'syncing') return;
            this.page = 'sync';
            this._resetSyncState();
            // 立即标记为 syncing，防止双击穿透（后端也有锁保护作为最终屏障）
            this.syncStatus = 'syncing';
            try {
                const result = await window.pywebview.api.start_sync();
                this._afterStart(result);
            } catch (e) {
                console.error('startSync failed:', e);
                this._startFailed(String(e));
            }
        },

        // ===== 重试失败产品 =====
        // 调用 start_sync(true) 只重跑上次失败的产品
        async retryFailed() {
            if (this.syncStatus === 'syncing') return;
            this._resetSyncState();
            this.syncStatus = 'syncing';
            try {
                // true 表示仅重试失败产品
                const result = await window.pywebview.api.start_sync(true);
                this._afterStart(result);
            } catch (e) {
                console.error('retryFailed failed:', e);
                this._startFailed(String(e));
            }
        },

        _resetSyncState() {
            this.completed = 0;
            this.total = 0;
            this.currentProduct = '';
            this.elapsedSeconds = 0;
            this.displayElapsedSeconds = 0;
            this.pausedDisplayElapsedSeconds = null;
            this.syncStartedAtMs = null;
            this.errorMessage = '';
            this.runSummary = null;
            this.syncProducts = [];
            this.allProducts = [];
            this.showWaiting = false;
            this.postprocessing = false;
            this.postprocessDetail = '';
            this.estimateData = null;
        },

        _afterStart(result) {
            if (result.started) {
                this.syncStartedAtMs = Date.now();
                this.startPolling();
            } else {
                // Python 端拒绝启动（如已有任务在跑）
                this._startFailed(result.message || '无法启动同步');
            }
        },

        _startFailed(message) {
            this.errorMessage = message;
            this.syncStatus = 'error';
            this.syncStartedAtMs = null;
        },

        // ===== 等待中产品列表（计算属性） =====
        // allProducts 中去掉已在 syncProducts 里出现的，剩余即"等待中"
        waitingProducts() {
            const doneNames = new Set(this.syncProducts.map(p => p.name));
            return this.allProducts.filter(name => !doneNames.has(name));
        },

        // 结果列表：失败→跳过→成功
        sortedSyncProducts() {
            const order = { error: 0, skipped: 1, ok: 2 };
            return [...this.syncProducts].sort((a, b) => (order[a.status] ?? 3) - (order[b.status] ?? 3));
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
                    this.syncElapsedFromProgress(p);

                    // 更新产品列表和全部产品名
                    if (p.products && Array.isArray(p.products)) {
                        this.syncProducts = p.products;
                    }
                    if (p.all_products && Array.isArray(p.all_products)) {
                        this.allProducts = p.all_products;
                    }

                    // confirm_needed：后台线程等待用户确认，展示确认面板
                    if (p.status === 'confirm_needed' && p.estimate) {
                        this.estimateData = p.estimate;
                        this.postprocessing = false;
                        this.pollTimer = null;
                        return; // 等待用户确认，不再继续轮询
                    } else if (p.status === 'postprocessing') {
                        this.postprocessing = true;
                        this.postprocessDetail = p.postprocess_detail || '';
                    } else if (p.status === 'done') {
                        this.syncStatus = 'done';
                        this.postprocessing = false;
                        this.postprocessDetail = '';
                        this.estimateData = null;
                        this.syncStartedAtMs = null;
                        this.runSummary = p.run_summary;
                        this.historyLoaded = false; // 有新运行，下次切历史页时刷新
                        this.checkUpdateResult = null; // 同步后清除检查更新结果
                        this.pollTimer = null;
                        this.loadOverview();
                        return; // 终态，不再调度下次轮询
                    } else if (p.status === 'error') {
                        this.syncStatus = 'error';
                        this.postprocessing = false;
                        this.postprocessDetail = '';
                        this.estimateData = null;
                        this.syncStartedAtMs = null;
                        this.errorMessage = p.error_message || '同步失败';
                        this.runSummary = p.run_summary;  // 部分失败时也携带摘要
                        this.historyLoaded = false;
                        this.checkUpdateResult = null;
                        this.pollTimer = null;
                        this.loadOverview();
                        return; // 终态，不再调度下次轮询
                    } else if (p.status === 'idle') {
                        this.syncStatus = 'idle';
                        this.postprocessing = false;
                        this.postprocessDetail = '';
                        this.estimateData = null;
                        this.syncStartedAtMs = null;
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
        resetSync() {
            this.stopPolling();
            this._resetSyncState();
            this.syncStatus = 'idle';
        },

        // ===== API 调用量确认 =====

        // 用户点击"继续同步"：通知后台线程继续，关闭确认面板
        async confirmSync() {
            try {
                await window.pywebview.api.confirm_sync();
                this.estimateData = null;
                this.pausedDisplayElapsedSeconds = null;
                this.syncStartedAtMs = Date.now() - this.displayElapsedSeconds * 1000;
                this.startPolling();
            } catch (e) {
                console.error('confirmSync failed:', e);
            }
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
                this.pausedDisplayElapsedSeconds = null;
                this.syncStartedAtMs = null;
                this.stopPolling();
            }
        },

        syncElapsedFromProgress(progress) {
            const backendElapsedSeconds = progress && progress.elapsed_seconds != null
                ? progress.elapsed_seconds
                : 0;
            const status = progress && progress.status ? progress.status : '';
            this.elapsedSeconds = backendElapsedSeconds;
            if (status === 'confirm_needed') {
                if (this.pausedDisplayElapsedSeconds == null) {
                    this.pausedDisplayElapsedSeconds = this.displayElapsedSeconds > 0
                        ? this.displayElapsedSeconds
                        : backendElapsedSeconds;
                }
                this.displayElapsedSeconds = this.pausedDisplayElapsedSeconds;
                this.syncStartedAtMs = null;
                return;
            }
            this.pausedDisplayElapsedSeconds = null;
            if (status === 'done' || status === 'error' || status === 'idle') {
                this.displayElapsedSeconds = backendElapsedSeconds;
                return;
            }
            const nowMs = Date.now();
            this.syncStartedAtMs = deriveSyncStartedAtMs(
                this.syncStartedAtMs,
                backendElapsedSeconds,
                nowMs,
            );
            this.displayElapsedSeconds = computeDisplayedElapsedSeconds(
                this.syncStartedAtMs,
                backendElapsedSeconds,
                nowMs,
            );
        },

        // ===== 格式化工具函数 =====

        // 进度百分比，total=0 时返回 0 避免除零
        progressPercent() {
            if (this.total <= 0) return 0;
            return Math.round((this.completed / this.total) * 100);
        },

        // 进度环的描边偏移（半径 22.5）
        ringOffset() {
            const circumference = 2 * Math.PI * 22.5;
            return circumference * (1 - this.progressPercent() / 100);
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
        // 返回如 "1.2 秒 · 826 个文件" 或 "0.5 秒"
        formatProductMeta(elapsed, filesCount) {
            let parts = [];
            if (elapsed != null) parts.push(Math.round(elapsed * 10) / 10 + ' 秒');
            if (filesCount != null && filesCount > 0) parts.push(filesCount + ' 个文件');
            return parts.join(' · ');
        },

        // 后端时间为 UTC ISO 字符串，new Date() 自动转为本地时区
        formatClock(isoStr) {
            const d = new Date(isoStr);
            if (isNaN(d)) return '';
            return String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
        },

        // "今天" / "昨天" / "10月25日 周日"
        formatDay(isoStr) {
            const d = new Date(isoStr);
            if (isNaN(d)) return '';
            const today = new Date();
            const startOf = x => new Date(x.getFullYear(), x.getMonth(), x.getDate()).getTime();
            const diff = Math.round((startOf(today) - startOf(d)) / 86400000);
            if (diff === 0) return '今天';
            if (diff === 1) return '昨天';
            const label = (d.getMonth() + 1) + '月' + d.getDate() + '日 ' + WEEKDAYS[d.getDay()];
            return d.getFullYear() === today.getFullYear() ? label : d.getFullYear() + '年' + label;
        },

        formatRelative(isoStr) {
            const day = this.formatDay(isoStr);
            return day ? day.replace(/ 周.$/, '') + ' ' + this.formatClock(isoStr) : '';
        },

        // 本地数据日期（YYYY-MM-DD）→ "10月23日"
        formatDataDate(dateStr) {
            const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateStr || '');
            if (!m) return '';
            const label = Number(m[2]) + '月' + Number(m[3]) + '日';
            return Number(m[1]) === new Date().getFullYear() ? label : m[1] + '年' + label;
        },

        // ===== 历史页方法 =====

        async loadHistory() {
            this.historyLoading = true;
            this.historyDetail = null;
            this.historySelected = '';
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
            if (this.historyList.length > 0) this.viewDetail(this.historyList[0].report_file);
        },

        // 历史列表按日期分组
        historyByDay() {
            const groups = [];
            for (const run of this.historyList) {
                const day = this.formatDay(run.started_at);
                const last = groups[groups.length - 1];
                if (last && last.day === day) last.runs.push(run);
                else groups.push({ day, runs: [run] });
            }
            return groups;
        },

        // 查看指定运行的产品明细（防重入）
        async viewDetail(reportFile) {
            if (this.detailLoading) return;
            this.historySelected = reportFile;
            this.detailLoading = true;
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
            this.detailLoading = false;
        },

        // 历史详情按失败→跳过→成功排序
        sortedDetailProducts() {
            if (!this.historyDetail || !this.historyDetail.products) return [];
            const order = { error: 0, skipped: 1, ok: 2 };
            return [...this.historyDetail.products].sort(
                (a, b) => (order[a.status] ?? 3) - (order[b.status] ?? 3)
            );
        },

        resultStatus(status) {
            if (status === 'ok') return { cls: 'ok', icon: 'i-check' };
            if (status === 'error') return { cls: 'bad', icon: 'i-xmark' };
            return { cls: 'idle', icon: 'i-minus' };
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
                    const msg = '已查询 ' + res.checked + ' 个产品的最新日期' +
                        (res.failed > 0 ? '，' + res.failed + ' 个查询失败' : '');
                    this.checkUpdateResult = { message: msg, isError: false };
                }
            } catch (e) {
                console.error('checkUpdates failed:', e);
                this.checkUpdateResult = { message: String(e), isError: true };
            }
            this.checkUpdateLoading = false;
        },

        // ===== 健康检查（开始→进度→结果） =====

        async startHealthCheck() {
            this.repairMessage = '';
            this.repairResult = null;
            this.repairing = false;
            this.healthResult = null;
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
        healthPercent() {
            const p = this.healthProgress;
            return p && p.total ? Math.round(p.current / p.total * 100) : 0;
        },
        async repairIssues() {
            this.repairing = true;
            try {
                const res = await window.pywebview.api.repair_health_issues();
                if (res.ok) {
                    const r = res.repair;
                    // 用 (type, product, file, detail) 四元组从本地 issues 中移除已修复的
                    const repairedSet = new Set(r.repaired.map(i => `${i.type}|${i.product}|${i.file}|${i.detail}`));
                    this.healthResult.issues = this.healthResult.issues.filter(
                        i => !repairedSet.has(`${i.type}|${i.product}|${i.file}|${i.detail}`)
                    );
                    // 重算 summary 的 issue 计数（保留 scanned_*/elapsed_seconds 不变）
                    const issues = this.healthResult.issues;
                    const bySeverity = {};
                    const byRepair = {auto_repairable: 0, manual_only: 0, needs_investigation: 0};
                    for (const i of issues) {
                        bySeverity[i.severity] = (bySeverity[i.severity] || 0) + 1;
                        if (i.repair_action && byRepair.hasOwnProperty(i.repair_action)) {
                            byRepair[i.repair_action]++;
                        }
                    }
                    this.healthResult.summary.total = issues.length;
                    this.healthResult.summary.by_severity = bySeverity;
                    this.healthResult.summary.by_repair = byRepair;
                    this.repairResult = r;
                    this.repairMessage = '';  // 清除旧的文字提示
                } else {
                    this.repairMessage = '修复失败：' + (res.error || '未知错误');
                }
            } finally {
                this.repairing = false;
            }
        },
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
        healthMeta() {
            const s = this.healthResult.summary;
            return '扫描 ' + s.scanned_products + ' 个产品、' + s.scanned_files + ' 个文件，用时 ' + this.formatDuration(s.elapsed_seconds);
        },
        categoryLabel(cat) {
            return {file_integrity:'文件完整性',content_integrity:'内容完整性',temporal_integrity:'时间完整性',coverage_integrity:'覆盖完整性',format_integrity:'格式完整性'}[cat] || cat;
        },
        repairLabel(issue) {
            if (issue.repairable) return '可自动修复';
            if (issue.repair_action === 'needs_resync') return '需重新同步';
            return '需人工排查';
        },
        issueTypeLabel(type) {
            return {
                missing_data: '数据缺失', orphan_temp: '残留临时文件', csv_unreadable: '文件不可读',
                tail_corruption: '尾部残行', infra_db_corrupt: '状态库损坏', infra_json_corrupt: 'JSON 损坏',
                duplicate_rows: '重复行', null_key_fields: '关键字段为空',
                date_exceeds_timestamp: '日期超前', timestamp_data_gap: '日期落后',
                missing_trading_days: '缺失交易日', file_count_drop: '文件数下降',
                column_inconsistency: '列名不一致',
            }[type] || type;
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
            this.continueAnyway();
        },

        continueAnyway() {
            this.currentView = 'main';
            this.page = 'overview';
            this.loadOverview();
        },

        retrySetup() {
            this.setupWarning = '';
            this.setupApiKey = '';
            this.setupHid = '';
        },

        cancelSetup() {
            this.currentView = 'main';
        },

        async goToSetup() {
            if (this.syncStatus === 'syncing') return;
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
            this.setupFromMain = true;
            this.currentView = 'setup';
        },
    }));
});
