# QuantClass 数据同步工具（setup + update 傻瓜版）

当前版本：`v0.7.5`

## Changelog / 更新记录：

* **v0.7.5**
  * 修复 `stock-notices-title` 排序一致性问题：纳入已知规则并启用稳定排序键（含并列字段）。
  * 写入链路新增排序校验与自动修复统计，运行报告可直接看到 `sorted_checked/sorted_violation/sorted_auto_repaired`。
  * 新增 `repair_sort` 命令，可批量扫描并修复历史 CSV 的排序异常（支持 `--dry-run` 演练）。
  * 新增排序相关回归测试，覆盖“发现异常、自动修复、dry-run 不落盘”关键场景。
* **v0.7.4**
  * 修复下载错误语义：HTTP 404 不再显示“参数错误”，统一改为“资源不存在（该产品该日期无可下载数据）”。
  * `FatalRequestError` 新增结构化字段 `status_code` 与 `request_url`，便于日志与排障定位。
  * 新增 `reason_code=no_data_for_date`，下载阶段可明确区分“该日期无数据”与“网络/权限类错误”。
  * 调整 `update` 回补执行流：回补模式遇到“无数据日”记为 `skipped` 并继续后续日期，不再整批提前停止。
  * `stop-on-error` 语义收敛为显式开关：仅在传入 `--stop-on-error` 时才全局提前停止。
  * 新增回归测试：覆盖 HTTP 404 映射、回补跳过无数据日、`stop-on-error` 全局停止行为。
* **v0.7.3**
  * 修复 `update` 回补链路的 probe 触发策略：仅在 `latest` 候选日期无法完整覆盖缺口区间时才做逐日探测，降低 API 放大量。
  * 修复稀疏 `latest` 多日期场景下的漏补风险（例如 `2026-02-07,2026-02-11` 时可继续探测补齐中间日期）。
  * 新增/调整回补回归测试，覆盖“连续多日期不探测、稀疏多日期需探测、大缺口稀疏需探测”。
  * 清理迁移文档：移除 `MIGRATION_COMPAT.md`（迁移已完成，兼容入口已下线）。
* **v0.7.2**
  * `update` 默认行为升级为“从本地 `timestamp.txt` 自动回补到 API 最新日”，支持单次命令补齐多日缺口。
  * 回补链路新增“latest 候选日期 + 逐日探测兜底”策略，兼容接口只返回单日期的场景。
  * 回补过程中命中真实错误（网络/解压/落库）时立即停止并返回非 0，避免误判为成功。
* **v0.7.1**
  * 修复币圈增量预处理中的 `PerformanceWarning`：pivot patch 从逐列写入改为批量拼接，避免 DataFrame 内存碎片化。
  * 新增性能回归测试，覆盖“多 symbol 变更场景下不再触发 `PerformanceWarning`”。
  * 保持 `.pkl` 产物结构与字段语义兼容，同时优化 `_patch_market_pivot` 执行性能。
* **v0.7.0**
  * 币圈预处理提速：改为“无 sidecar 增量 patch”主路径，仅在 spot/swap 有有效增量时执行。
  * 预处理触发源去掉 `coin-cap`，避免无关产品更新导致重计算。
  * 增量异常时自动回退全量重建，仍保持“失败不覆盖旧数据”的安全语义。
* **v0.6.7**
  * 币圈合成后处理启用“严格完整性模式”：spot/swap 两侧目录和有效 symbol 必须同时满足，缺任一侧即失败并返回非 0。
  * 预处理写盘改为“全量成功后原子替换”，任一环节异常都不会覆盖旧版 pkl。
  * 本地产品发现阶段忽略 `coin-binance-spot-swap-preprocess-pkl-1h`，避免运行报告出现同一产品双状态。
* **v0.6.6**
  * 币圈合成后处理进一步收敛：统一使用仓库内置预处理实现，不再依赖或支持外部自定义命令配置。
* **v0.6.5**
  * 币圈合成后处理改为“开箱即用”：未配置 `QUANTCLASS_PREPROCESS_CMD` 时，自动使用包内置预处理实现。
  * `QUANTCLASS_PREPROCESS_CMD` 由必填改为可选覆盖项，便于自定义替换合成脚本。
* **v0.6.4**
  * 新增默认入口行为：已有配置时直接运行 `python3 quantclass_sync.py` 会自动执行 `update`。
  * 新增币圈合成后处理 Hook：检测到 `coin-binance-spot-swap-preprocess-pkl-1h` 且本轮源产品有更新时，自动触发合成命令。
* **v0.6.3**
  * 移除旧脚本入口 `quantclass_daily_sync.py`，统一使用 `quantclass_sync.py` 作为唯一主入口。
  * 更新迁移文档与 README，明确目录/脚本重命名兼容入口已清理完成。
* **v0.6.2**
  * 修复 CSV 同步时的编码保留策略，避免更新过程中把已有本地数据文件改成不兼容编码（含 BOM 场景）。
  * 增加编码相关回归测试，降低同类问题复发概率。

这个工具现在主打两条命令：
1. `setup`：第一次配置（只做一次）
2. `update`：日常更新（每天常用）

如果你是第一次使用，只看下面 3 步就够了。  
补充：
1. 首次直接运行脚本（不带子命令）会自动进入 `setup` 向导。
2. 已配置后直接运行脚本（不带子命令）会默认执行 `update`。

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
python3 quantclass_sync.py setup
```

你会被询问 3 项信息：
1. `data_root`（数据目录）
2. `API Key`
3. `HID`

说明：交互模式默认使用 `local_scan`，且默认产品列表为空。  
如果你要改成固定产品模式，可后续执行：

```bash
python3 quantclass_sync.py setup --non-interactive --product-mode explicit_list --products stock-trading-data --products stock-main-index-data
```

### 第三步：先演练，再正式更新

【会写入】影响范围：
- 只写运行报告，不写业务数据

```bash
python3 quantclass_sync.py --dry-run
```

【会写入】影响范围：
- 业务数据目录
- 状态库和状态 JSON
- 运行报告

```bash
python3 quantclass_sync.py
```

说明：工具默认开启详细日志（`verbose`），可实时看到进度；如需安静模式可加 `--no-verbose`。

---

## 2. 你只需要记住的命令

### 2.1 setup（首次配置）

```bash
python3 quantclass_sync.py setup
```

默认行为说明（重要）：
1. `setup` 会先做连通性检查（可用 `--skip-check` 跳过）。
2. 连通性检查失败时，不会保存 `user_config.json` 和 `user_secrets.env`。

可选：非交互模式（自动化场景）

```bash
python3 quantclass_sync.py setup --non-interactive --data-root /your/data/root --api-key YOUR_API_KEY --hid YOUR_HID --product-mode local_scan
```

### 2.2 update（日常更新）

```bash
python3 quantclass_sync.py
```

上面是默认推荐写法（不带子命令自动执行 `update`）。
如果你更喜欢显式写法，也可继续使用：

```bash
python3 quantclass_sync.py update
```

默认行为（重要）：
1. 若本地 `timestamp.txt` 落后，会自动按日期从旧到新补到 API 最新日（单次命令完成回补）。
2. 若本地已是最新，会命中时间戳门控并跳过（`reason_code=up_to_date`）。
3. 回补过程中遇到真实错误（网络/解压/落库）会立即停止该产品并返回非 0。

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
python3 quantclass_sync.py update --products stock-trading-data --products stock-main-index-data --verbose
```

### 2.3 可选币圈合成后处理（自动触发）

触发条件（同时满足）：
1. `data_root` 下存在目录 `coin-binance-spot-swap-preprocess-pkl-1h`
2. 本轮 `coin-binance-candle-csv-1h` / `coin-binance-swap-candle-csv-1h` 至少一个成功更新且存在有效增量

默认行为：
1. 统一使用仓库内置预处理逻辑（默认、推荐、分发最省心）。
2. 无需额外设置环境变量。
3. 默认严格完整性：spot/swap 两侧都要有有效输入，才会写入新产物。
4. 写盘采用原子替换：全部文件都写成功才会替换正式文件。
5. 预处理默认优先走增量 patch（基于 timestamp + mtime），仅在必要时回退全量。

开箱即用示例（无需任何环境变量）：

```bash
python3 quantclass_sync.py
```

失败语义：
1. 若内置预处理依赖缺失或处理异常，会把本次 `update` 标记为失败（非 0 退出码）
2. 若仅一侧源数据缺失/为空，会直接失败，且保留旧 pkl 不覆盖
3. 若增量 patch 失败，会自动尝试一次全量回退；回退也失败才会最终报错
4. `--dry-run` 下不会执行合成命令，只会在报告里记录跳过

---

## 3. 配置文件与输出目录

### 3.1 setup 写入的文件

1. `user_config.json`：用户配置（数据目录、产品策略等）
2. `user_secrets.env`：API Key/HID

这两个文件都已默认加入 `.gitignore`，不会被 Git 提交。

### 3.2 状态与日志目录

默认路径：
1. `<data_root>/.quantclass_sync/status/FuelBinStat.db`
2. `<data_root>/.quantclass_sync/status/products-status.json`
3. `<script_dir>/log/run_report_*.json`

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
python3 quantclass_sync.py init
python3 quantclass_sync.py one_data stock-trading-data --verbose
python3 quantclass_sync.py all_data --mode local --verbose
```

推荐新项目直接用：`setup + update`。

迁移状态（目录/脚本重命名，更新于 `2026-02-09`）：
1. 旧目录 `.../quant/data/scripts` 兼容入口已移除。
2. 旧脚本名 `quantclass_daily_sync.py` 兼容入口已移除。
3. 当前统一使用新目录 `.../quant/data/quantclass-sync` 与主脚本 `quantclass_sync.py`。

---

## 5. 常见问题（高频）

### Q1：`update` 报“未找到用户配置文件”

先执行：

```bash
python3 quantclass_sync.py setup
```

### Q2：为什么没有更新任何数据？

常见原因：
1. 本地已经是最新（`reason_code=up_to_date`）
2. 你跑的是 `--dry-run`
3. 本地目录没有可更新产品，且未配置默认产品列表

### Q3：怎么查看最新运行报告？

【只读】影响范围：仅读取日志

```bash
report=$(ls -t <script_dir>/log/run_report_* | head -n 1)
echo "$report"
sed -n '1,220p' "$report"
```

---

## 6. 安全建议

1. 不要把 `user_secrets.env` 提交到 Git。
2. 不要把 API Key/HID 发到群聊或工单截图。
3. 生产更新建议先跑一次 `update --dry-run`。
