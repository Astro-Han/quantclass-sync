# QuantClass 数据同步工具（零基础可用）

脚本路径：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`
当前版本：`v0.5.0`（发布日期：`2026-02-08`）

这个工具用来做 2 件事：
1. 日常批量更新本地数据（最常用）
2. 单产品更新（快速定位问题）

如果你只记一条：**先 `--dry-run` 演练，再正式执行。**

---

## 1. 这个项目解决什么问题

你本地有很多数据产品目录（例如 `stock-trading-data`、`stock-main-index-data`）。
手动逐个更新非常耗时，也容易漏更新。

本脚本会自动完成：
1. 识别要更新的产品
2. 拉取最新日期并下载
3. 解压和合并到本地目录
4. 写入运行报告，方便复盘

---

## 2. 新手先看：5 分钟上手

### 第一步：安装依赖（只需一次）

【会写入】影响范围：当前 Python 环境（安装依赖包）

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements.txt
```

【会写入】影响范围：当前 Python 环境（仅处理 `.7z/.rar` 时需要）

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements-archive.txt
```

### 第二步：准备凭证（API Key + HID）

脚本支持 3 种来源，优先级从高到低：
1. 命令行参数 `--api-key`、`--hid`
2. 环境变量 `QUANTCLASS_API_KEY`、`QUANTCLASS_HID`
3. 本地文件 `xbx_apiKey.md`

推荐你使用本地文件（最简单、最不容易输错）：
- 默认文件：`/Users/yuhan/workspace/quant/data/scripts/xbx_apiKey.md`
- 文件内容示例（把值替换成你自己的）：

```text
xbx_api_key=YOUR_API_KEY
xbx_id=YOUR_HID
```

### 第三步：初始化状态（第一次建议执行）

【会写入】影响范围：
- `/Users/yuhan/workspace/quant/data/xbx_data/code/data/FuelBinStat.db`
- `/Users/yuhan/workspace/quant/data/xbx_data/code/data/products-status.json`

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py init
```

### 第四步：先演练（不写业务数据）

【会写入】影响范围：
- 会写运行报告
- 不写业务数据目录
- 不写状态库和状态 JSON

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --dry-run --verbose all_data --mode local
```

### 第五步：正式执行日常更新

【会写入】影响范围：
- `/Users/yuhan/workspace/quant/data/xbx_data/<product>/...`
- 状态文件与运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose all_data --mode local
```

---

## 3. 命令速查（复制即可）

> 注意：`--verbose`、`--dry-run`、`--secrets-file` 这些是**全局参数**，要放在子命令前面。
>
> 正确：`... quantclass_daily_sync.py --verbose all_data --mode local`
>
> 错误：`... quantclass_daily_sync.py all_data --mode local --verbose`

### 3.0 参数简化说明（本次改动）

现在默认只展示核心全局参数（更适合零基础）：
1. `--data-root`
2. `--secrets-file`
3. `--dry-run`
4. `--verbose`

以下高级参数仍可用，但默认隐藏（兼容保留）：
1. `--api-key`
2. `--hid`
3. `--report-file`
4. `--stop-on-error`

### 3.1 更新单个产品

【会写入】影响范围：对应产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose one_data stock-trading-data
```

### 3.2 单产品按指定日期更新

【会写入】影响范围：对应产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose one_data stock-trading-data --date-time 2026-02-06
```

### 3.3 批量更新本地已有产品（推荐日常命令）

【会写入】影响范围：本地已有产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose all_data --mode local
```

### 3.4 指定产品批量更新

【会写入】影响范围：指定产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose all_data --mode local --products stock-trading-data --products stock-main-index-data
```

### 3.5 忽略时间戳门控，强制更新

【会写入】影响范围：目标产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose all_data --mode local --force
```

### 3.6 说明：工具目标已简化为“只做更新”

`full_data_link` / `full_data` 已移除。  
当前版本只保留：
1. `init`
2. `one_data`
3. `all_data`

---

## 4. 这版新增的关键策略（你最关心）

### 4.1 `timestamp.txt` 门控（默认开启）

门控（先判断要不要更新）规则：

| 场景 | 行为 |
|---|---|
| `--force` | 一定更新 |
| `one_data --date-time ...` | 按你指定日期更新 |
| 本地日期 `>=` API latest | 跳过（`reason_code=up_to_date`） |
| 本地 `timestamp.txt` 缺失/格式坏/解析失败 | 回退为更新（防止漏更） |

`timestamp.txt` 格式：

```text
数据日期,本地写入时间
```

例如：

```text
2026-02-07,2026-02-08 15:09:34
```

### 4.2 `.cache` 激进清理（默认开启）

- 工作缓存目录：`/Users/yuhan/workspace/quant/data/.cache/quantclass/`
- 每次命令结束（成功或失败）都会自动清理缓存，防止长期膨胀。
- 运行日志不放在 `.cache`，而是放在：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/log/quantclass/`
- 日志默认保留 365 天，超过自动清理。

---

## 5. 目录与文件说明

### 5.1 业务数据

- `/Users/yuhan/workspace/quant/data/xbx_data/<product>/...`

### 5.2 状态文件

- SQLite 状态库：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/FuelBinStat.db`
- 状态 JSON：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/products-status.json`

### 5.3 运行日志（重点看这里）

- 目录：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/log/quantclass/`
- 文件名示例：`run_report_20260208-150649_all_data.json`

### 5.4 产品时间戳

- 文件：`/Users/yuhan/workspace/quant/data/xbx_data/<product>/timestamp.txt`
- 用途：门控判断“本地是否已最新”。

---

## 6. 如何判断这次执行是否成功

看运行报告里的 3 个字段：
1. `success_total`
2. `failed_total`
3. `skipped_total`

看单产品结果里的 3 个字段：
1. `status`：`ok / error / skipped`
2. `reason_code`：为什么成功、失败或跳过
3. `mode`：这次走的是 `network` 更新还是 `gate` 门控跳过

常见 `reason_code`：
1. `ok`：成功
2. `up_to_date`：已是最新，跳过
3. `unknown_local_product`：本地目录不在产品清单中
4. `network_error`：网络/权限/接口问题
5. `extract_error`：解压失败
6. `merge_error`：合并失败

---

## 7. 常见问题（零基础高频）

### Q1：为什么报 `No such option: --verbose`？

因为 `--verbose` 是全局参数，必须写在子命令前。

【只读】影响范围：仅打印帮助，不写数据

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose all_data --help
```

### Q2：为什么“看起来没新数据”却更新了？

最常见 3 个原因：
1. 本地 `timestamp.txt` 缺失（首次门控前常见）
2. `timestamp.txt` 格式异常（第一列不是日期）
3. 你用了 `--force`

### Q3：如何快速排查某个产品？

先单产品跑，范围最小，日志最清晰。

【会写入】影响范围：该产品目录、状态文件、运行报告

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose one_data stock-trading-data
```

### Q4：怎么查看“最新一份”运行报告？

【只读】影响范围：仅读取日志文件

```bash
report=$(ls -t /Users/yuhan/workspace/quant/data/xbx_data/code/data/log/quantclass/run_report_* | head -n 1)
echo "$report"
sed -n '1,220p' "$report"
```

---

## 8. 安全与隐私

1. 不要把 API Key / HID 提交到 Git。
2. 不要在公开截图里暴露凭证。
3. 推荐优先使用本地 `xbx_apiKey.md` 或环境变量管理凭证。
4. 生产环境优先使用最小命令范围（先 `one_data` 再 `all_data`）。

---

## 9. 维护与扩展

- 产品清单：`/Users/yuhan/workspace/quant/data/scripts/catalog.txt`
- 主脚本：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`
- 依赖清单：`/Users/yuhan/workspace/quant/data/scripts/requirements.txt`

`catalog.txt` 规范（已简化）：
1. 每行一个产品英文名（`product_id`）
2. 允许空行和 `#` 注释行
3. 不再支持“中文名 + 英文名 + 日期”的多列写法（会报格式错误）

示例：

```text
# 每行一个产品英文名
stock-trading-data
stock-main-index-data
stock-trading-data-pro
```

如果要新增产品规则或调整合并策略，建议先 `--dry-run` 验证，再做正式更新。

---

## 10. README 结构依据（联网检索）

本 README 的结构与写法参考了以下最佳实践：
1. GitHub 官方关于 README 的建议（说明项目做什么、为什么有用、如何开始、到哪里求助、谁维护）。
2. GitHub 官方 Markdown 规范（清晰标题、代码块、链接、列表，提升可扫描性）。
3. Open Source Guides 对 README 的问题清单（What/Why/Getting Started/Help）。
4. Plain Language（平实语言）原则（短句、主动语态、术语首次解释）。

参考链接：
- [GitHub Docs: About the repository README file](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes)
- [GitHub Docs: Basic writing and formatting syntax](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax)
- [Open Source Guides: Starting an Open Source Project (Writing a README)](https://opensource.guide/starting-a-project/#writing-a-readme)
- [PlainLanguage.gov](https://www.plainlanguage.gov/)
