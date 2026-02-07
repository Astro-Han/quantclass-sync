# QuantClass 同步脚本说明（官方命令兼容 + 本地安全增强）

脚本路径：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`  
目标：命令更简单，同时兼容官方高频命令：`init / one_data / all_data / full_data_link / full_data`。

## 1. 先安装依赖

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements.txt
```

可选解压依赖（按需）：

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements-archive.txt
```

## 2. 命令总览

### 2.1 初始化状态（不下载）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py init
```

### 2.2 更新单产品（增量）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py one_data stock-trading-data
```

指定日期（可选）：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py one_data stock-trading-data --date-time 2026-02-06
```

### 2.3 批量更新

默认本地存量模式（只更新本地已有合法产品）：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data
```

按 catalog 全量模式：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --mode catalog
```

显式指定产品（可重复传参）：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --products stock-trading-data --products stock-main-index-data
```

### 2.4 拉取全量下载链接

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py full_data_link stock-trading-data stock-trading-data
```

### 2.5 全量恢复（先备份再覆盖）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py full_data stock-trading-data
```

## 3. 全局参数（所有子命令可用）

1. `--data-root`：数据根目录（默认 `/Users/yuhan/workspace/quant/data/xbx_data`）
2. `--api-key`：QuantClass API Key
3. `--hid`：QuantClass HID
4. `--secrets-file`：本地密钥文件路径（默认 `xbx_apiKey.md`）
5. `--dry-run`：演练模式（不写业务数据、不写状态库、不写状态 JSON）
6. `--report-file`：运行报告 JSON 输出路径
7. `--stop-on-error`：批量更新时遇错即停
8. `--verbose`：输出调试日志

## 4. 数据与状态文件

1. 业务数据目录：`/Users/yuhan/workspace/quant/data/xbx_data/<product>/...`
2. 状态数据库：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/FuelBinStat.db`
3. 状态导出：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/products-status.json`
4. 运行报告：`/Users/yuhan/workspace/quant/data/.cache/quantclass/run_report_*.json`
5. 全量压缩缓存：`/Users/yuhan/workspace/quant/data/.cache/quantclass/zip/`
6. 全量备份目录：`/Users/yuhan/workspace/quant/data/.cache/quantclass/full_backup/<run_id>/`

## 5. 落库策略（默认安全优先）

1. `merge_known`：命中已知规则，执行增量合并（只合并变化行）。
2. `unknown_header_merge`：未知产品但“同名目标文件且表头一致”，执行轻量合并（整行去重）。
3. `mirror_unknown`：其他未知情况，镜像写入（按原路径复制，不做字段推断）。

## 6. 事件码与 reason_code

事件码：
- `CMD_START`
- `CMD_DONE`
- `DISCOVER_DONE`
- `PRODUCT_PLAN`
- `DOWNLOAD_OK`
- `EXTRACT_OK`
- `SYNC_OK`
- `SYNC_FAIL`
- `RUN_SUMMARY`

`reason_code`：
- `ok`
- `unknown_local_product`
- `no_local_products`
- `network_error`
- `extract_error`
- `merge_error`
- `mirror_fallback`
- `unknown_header_merge`
- `full_data_link_missing`
- `full_data_expired`

## 7. 面向零基础的阅读顺序

1. 先看 `@app.command("all_data")`：理解“怎么挑产品”
2. 再看 `process_product()`：理解“单产品下载 -> 解压 -> 落库”
3. 再看 `sync_known_product()` 和 `sync_unknown_product()`：理解“已知规则 vs 未知规则”
4. 最后看 `cmd_full_data()`：理解“先备份后覆盖”的全量恢复

## 8. 中文规范（强制）

1. 新增注释、错误提示、日志说明统一中文。
2. 首次出现术语要带括注解释。
3. 面向用户的错误信息采用三段式：
- 出了什么问题
- 可能原因
- 下一步建议

## 9. 参考

1. QuantClass API 页面：[https://www.quantclass.cn/data/api](https://www.quantclass.cn/data/api)
2. 产品清单：`/Users/yuhan/workspace/quant/data/scripts/catalog.txt`
3. 主脚本：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`
