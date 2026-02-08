# QuantClass 数据同步工具说明（零基础友好版）

脚本路径：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`  
当前目标：用尽量少的命令，完成日常更新、单产品更新、全量恢复。

## 1. 你最关心的核心功能

1. 一键批量更新你本地已有的数据产品：`all_data --mode local`
2. 只更新一个产品：`one_data <product>`
3. 全量恢复单产品（先备份后覆盖）：`full_data <product>`
4. 演练模式（dry-run）：走完整流程但不写业务数据和状态文件
5. 自动落库策略：已知产品增量合并，未知产品安全镜像或轻量合并

## 2. 当前开发进度（截至 2026-02-08）

已完成：
1. 命令体系切换为子命令（`init/one_data/all_data/full_data_link/full_data`）
2. 本地状态管理（`FuelBinStat.db` + `products-status.json`）
3. 本地存量驱动更新（`all_data --mode local`）
4. 已知产品规则合并 + 未知产品轻量合并/镜像兜底
5. 全量恢复“先备份再覆盖”
6. 安全解压（含 `.zip/.tar/.rar/.7z` 路径越界防护）
7. `dry-run` 安全语义修复（不写业务数据、不写状态库、不写状态 JSON）
8. 中文日志、结构化 JSON 报告、统一错误码

当前版本适合：
1. 日常增量更新
2. 已有产品的稳定维护
3. 出现数据异常时的单产品全量恢复

## 3. 三步上手（第一次使用）

### 第一步：安装依赖

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements.txt
```

可选解压依赖（只在你需要处理 `.7z/.rar` 时安装）：

```bash
python3 -m pip install -r /Users/yuhan/workspace/quant/data/scripts/requirements-archive.txt
```

### 第二步：准备凭证

支持三种方式：
1. 命令行传入 `--api-key` 和 `--hid`
2. 环境变量
3. 本地文件 `xbx_apiKey.md`（默认路径：`/Users/yuhan/workspace/quant/data/scripts/xbx_apiKey.md`）

### 第三步：先演练，再正式执行

演练（推荐先跑）：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py --dry-run all_data --mode local
```

正式执行：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --mode local
```

## 4. 常用命令（按用户视角）

### 4.1 初始化状态（不下载）

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py init
```

### 4.2 更新单产品

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py one_data stock-trading-data
```

指定日期：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py one_data stock-trading-data --date-time 2026-02-06
```

### 4.3 批量更新

默认本地存量模式（推荐）：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --mode local
```

catalog 全量轮询模式：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --mode catalog
```

只跑指定产品：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py all_data --products stock-trading-data --products stock-main-index-data
```

### 4.4 全量链路（单产品）

先拿全量下载链接：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py full_data_link stock-trading-data stock-trading-data
```

再执行全量恢复：

```bash
python3 /Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py full_data stock-trading-data
```

## 5. 运行参数（全子命令可用）

1. `--data-root`：数据根目录（默认 `/Users/yuhan/workspace/quant/data/xbx_data`）
2. `--api-key`：QuantClass API Key
3. `--hid`：QuantClass HID
4. `--secrets-file`：本地密钥文件路径
5. `--dry-run`：演练模式（不写业务数据、不写状态库、不写状态 JSON）
6. `--report-file`：运行报告输出路径（JSON）
7. `--stop-on-error`：批量任务遇错即停
8. `--verbose`：输出调试日志

## 6. 数据目录与状态文件

1. 业务数据目录：`/Users/yuhan/workspace/quant/data/xbx_data/<product>/...`
2. 状态数据库：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/FuelBinStat.db`
3. 状态导出文件：`/Users/yuhan/workspace/quant/data/xbx_data/code/data/products-status.json`
4. 运行报告目录：`/Users/yuhan/workspace/quant/data/.cache/quantclass/`
5. 全量缓存目录：`/Users/yuhan/workspace/quant/data/.cache/quantclass/zip/`
6. 全量备份目录：`/Users/yuhan/workspace/quant/data/.cache/quantclass/full_backup/<run_id>/`

## 7. 落库策略（默认安全优先）

1. `merge_known`：命中已知规则，做增量合并
2. `unknown_header_merge`：未知产品但“同名目标文件且表头一致”，做轻量合并
3. `mirror_unknown`：其余未知情况走镜像写入

## 8. 报告口径（你怎么看结果）

报告文件中每个产品都有：
1. `status`：`ok/error/skipped`
2. `strategy`：`merge_known/unknown_header_merge/mirror_unknown/skip`
3. `reason_code`：失败或分流原因

常见 `reason_code`：
1. `ok`：执行成功
2. `unknown_local_product`：本地目录不在 catalog 中，已跳过
3. `invalid_explicit_product`：显式指定的产品不在 catalog 中，已跳过
4. `network_error`：网络或权限问题
5. `extract_error`：解压失败
6. `merge_error`：合并阶段失败
7. `full_data_link_missing`：缺少全量链接
8. `full_data_expired`：全量链接过期

## 9. 零基础排障建议

1. 看起来“卡住”时，先等 1-3 分钟，很多时候在做大文件合并
2. 先用 `--dry-run` 验证流程，再跑正式更新
3. 单产品异常时，优先用 `one_data <product>` 缩小范围
4. 数据明显异常时，先 `full_data_link` 再 `full_data`

## 10. 参考

1. QuantClass API 页面：[https://www.quantclass.cn/data/api](https://www.quantclass.cn/data/api)
2. 产品清单：`/Users/yuhan/workspace/quant/data/scripts/catalog.txt`
3. 主脚本：`/Users/yuhan/workspace/quant/data/scripts/quantclass_daily_sync.py`
