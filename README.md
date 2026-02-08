# QuantClass 数据同步工具（setup + update 傻瓜版）

当前版本：`v0.5.0`

这个工具现在主打两条命令：
1. `setup`：第一次配置（只做一次）
2. `update`：日常更新（每天常用）

如果你是第一次使用，只看下面 3 步就够了。

---

## 1. 3 步上手（零基础）

### 第一步：安装依赖（只需一次）

【会写入】影响范围：当前 Python 环境（安装依赖包）

```bash
python3 -m pip install -r requirements.txt
```

【会写入】影响范围：当前 Python 环境（仅处理 `.7z/.rar` 压缩包时需要）

```bash
python3 -m pip install -r requirements-archive.txt
```

### 第二步：运行 setup（交互向导）

【会写入】影响范围：
- `user_config.json`
- `user_secrets.env`

```bash
python3 quantclass_daily_sync.py setup
```

你会被询问 4 项信息：
1. `data_root`（数据目录）
2. `API Key`
3. `HID`
4. 产品策略（自动扫描本地目录 / 固定产品清单）

### 第三步：先演练，再正式更新

【会写入】影响范围：
- 只写运行报告，不写业务数据

```bash
python3 quantclass_daily_sync.py update --dry-run
```

【会写入】影响范围：
- 业务数据目录
- 状态库和状态 JSON
- 运行报告

```bash
python3 quantclass_daily_sync.py update
```

说明：工具默认开启详细日志（`verbose`），可实时看到进度；如需安静模式可加 `--no-verbose`。

---

## 2. 你只需要记住的命令

### 2.1 setup（首次配置）

```bash
python3 quantclass_daily_sync.py setup
```

默认行为说明（重要）：
1. `setup` 会先做连通性检查（可用 `--skip-check` 跳过）。
2. 连通性检查失败时，不会保存 `user_config.json` 和 `user_secrets.env`。

可选：非交互模式（自动化场景）

```bash
python3 quantclass_daily_sync.py setup --non-interactive --data-root /your/data/root --api-key YOUR_API_KEY --hid YOUR_HID --product-mode local_scan
```

### 2.2 update（日常更新）

```bash
python3 quantclass_daily_sync.py update
```

常用可选项：
1. `--dry-run`：只演练，不写业务数据
2. `--force`：跳过时间戳门控（门控：先判断是否有新数据再决定是否下载）
3. `--products`：临时指定产品（可重复传参或逗号分隔）
4. `--no-verbose`：关闭详细日志（默认已开启）

凭证优先级（高 -> 低）：
1. 命令行参数（`--api-key/--hid`）
2. `setup` 写入的 `user_secrets.env`
3. 环境变量（`QUANTCLASS_API_KEY/QUANTCLASS_HID`）

示例：只更新两个产品

```bash
python3 quantclass_daily_sync.py update --products stock-trading-data --products stock-main-index-data --verbose
```

---

## 3. 配置文件与输出目录

### 3.1 setup 写入的文件

1. `user_config.json`：用户配置（数据目录、产品策略等）
2. `user_secrets.env`：API Key/HID

这两个文件都已默认加入 `.gitignore`，不会被 Git 提交。

### 3.2 状态与日志目录

默认使用新路径（推荐）：
1. `<data_root>/.quantclass_sync/status/FuelBinStat.db`
2. `<data_root>/.quantclass_sync/status/products-status.json`
3. `<data_root>/.quantclass_sync/log/quantclass/run_report_*.json`

兼容逻辑：
- 若检测到旧路径已有状态数据，且新路径还没有状态数据，会自动回退旧路径读取，避免迁移期状态分裂。

### 3.3 缓存目录

1. 工作缓存：`<project_root>/../.cache/quantclass/`
2. 每次命令结束后（成功或失败）都会自动清理，防止持续膨胀。

---

## 4. 旧命令兼容（给老用户）

以下命令仍可继续使用（兼容保留）：
1. `init`
2. `one_data`
3. `all_data`

兼容命令在未显式传 `--data-root/--secrets-file` 时，也会自动读取 `user_config.json`。

示例：

```bash
python3 quantclass_daily_sync.py init
python3 quantclass_daily_sync.py one_data stock-trading-data --verbose
python3 quantclass_daily_sync.py all_data --mode local --verbose
```

推荐新项目直接用：`setup + update`。

---

## 5. 常见问题（高频）

### Q1：`update` 报“未找到用户配置文件”

先执行：

```bash
python3 quantclass_daily_sync.py setup
```

### Q2：为什么没有更新任何数据？

常见原因：
1. 本地已经是最新（`reason_code=up_to_date`）
2. 你跑的是 `--dry-run`
3. 本地目录没有可更新产品，且未配置默认产品列表

### Q3：怎么查看最新运行报告？

【只读】影响范围：仅读取日志

```bash
report=$(ls -t <data_root>/.quantclass_sync/log/quantclass/run_report_* | head -n 1)
echo "$report"
sed -n '1,220p' "$report"
```

---

## 6. 安全建议

1. 不要把 `user_secrets.env` 提交到 Git。
2. 不要把 API Key/HID 发到群聊或工单截图。
3. 生产更新建议先跑一次 `update --dry-run`。
