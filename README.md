# QuantClass 同步脚本使用说明

本文档面向零代码基础用户，目标是：你能安全地运行脚本、看懂输出、快速定位问题。

## 1. 脚本位置

- `/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`

## 2. 你需要知道的三个模式

- `network` 模式（默认）：联网从 QuantClass 拉取最新数据。
- `cache-only` 模式：不联网，只使用本地缓存目录补跑。
- `dry-run` 演练模式：只计算结果，不写入数据文件。

## 3. 最常用命令

### 3.1 正常日更（默认联网）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py \
  --products stock-trading-data-pro stock-main-index-data stock-fin-data-xbx \
  --data-root /Users/yuhan/workspace/quant/data/xbx_data \
  --work-dir /Users/yuhan/workspace/quant/data/.cache/quantclass
```

### 3.2 离线补跑（使用本地缓存）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py \
  --products stock-trading-data-pro stock-main-index-data stock-fin-data-xbx \
  --cache-only \
  --data-root /Users/yuhan/workspace/quant/data/xbx_data \
  --work-dir /Users/yuhan/workspace/quant/data/.cache/quantclass
```

### 3.3 安全演练（不写数据）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py \
  --products stock-trading-data-pro stock-main-index-data stock-fin-data-xbx \
  --dry-run \
  --data-root /Users/yuhan/workspace/quant/data/xbx_data \
  --work-dir /Users/yuhan/workspace/quant/data/.cache/quantclass
```

## 4. 日志格式与级别

### 4.1 日志格式（新增）

- `text`（默认）：兼容你现在的终端输出格式，最适合人工查看。
- `json`：结构化日志（每行都是 JSON），最适合后续做检索、统计、告警。

常用命令：

```bash
# 默认文本日志（兼容旧命令）
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py
```

```bash
# JSON 结构化日志（推荐接入可观测性系统时使用）
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --log-format json
```

`json` 模式下会包含固定英文机器字段（如 `event`、`level`、`run_id`），
同时 `message` 使用中文，并带 `lang=zh-CN` 字段，兼顾“机器可读 + 人类易读”。

### 4.2 日志级别（简化为 3 级）

- `ERROR`：只有错误。
- `INFO`：默认，显示关键进度和结果。
- `DEBUG`：最详细，用于排查问题。

常用开关：

```bash
# 详细日志
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --verbose
```

```bash
# 安静模式（仅错误和摘要）
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --quiet
```

## 5. 凭证优先级（固定规则）

脚本会按下面顺序找 `api_key` 和 `hid`：

1. 命令行参数：`--api-key --hid`
2. 环境变量：`QUANTCLASS_API_KEY QUANTCLASS_HID`
3. 本地文件：`--secrets-file`（默认 `xbx_data/xbx_apiKey.md`）

你可以只维护 `xbx_data/xbx_apiKey.md`，脚本会自动兜底读取。

## 6. 报告输出（同一份报告，两种输出）

- `--report-file`：写到 JSON 文件（适合留档）
- `--report-json`：终端直接打印 JSON（适合临时查看）

示例：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py \
  --products stock-main-index-data \
  --cache-only \
  --report-file /Users/yuhan/workspace/quant/data/.cache/quantclass/last_run_report.json \
  --report-json
```

## 7. 如何看最终输出

每个产品会输出一行：

- `created`：新建文件数
- `updated`：更新文件数
- `unchanged`：无变化文件数
- `skipped`：跳过文件数
- `rows_added`：新增行数（按主键去重后）
- `elapsed`：该产品耗时

最后 `SUMMARY` 是全局汇总。

## 8. 常见问题

### 8.1 报错 DNS 解析失败

说明当前机器无法解析域名（网络或 DNS 问题）。  
可先用 `--cache-only` 运行，等网络恢复后再用默认模式。

### 8.2 为什么重复跑不会无限增加 rows

脚本按主键去重（幂等），同一条数据重复导入不会重复写入。

### 8.3 为什么要保留旧命令兼容

为了降低迁移风险：你以前能跑的命令仍然能跑，新功能都是可选参数。
