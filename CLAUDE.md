# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目简介

QuantClass 数据同步工具（当前 v0.7.5），从 QuantClass API 自动下载、解析、合并金融数据（股票/币圈）。核心特性：本地落后多日时单次命令自动回补所有缺口。

## 常用命令

```bash
# 安装依赖
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-archive.txt  # 可选，处理 .7z/.rar

# 运行全部测试
python3 -m unittest discover -s tests -p 'test_*.py' -v

# 运行单个测试文件
python3 -m unittest tests.test_update_catchup -v

# 运行单个测试用例
python3 -m unittest tests.test_update_catchup.TestCatchupDateResolution.test_multi_date_ascending -v

# 演练模式（不写业务数据）
python3 quantclass_sync.py --dry-run

# 日常更新（不带子命令默认执行 update）
python3 quantclass_sync.py
```

## 架构概览

单脚本架构，`quantclass_sync.py`（~3940 行）分 4 层，代码内第 68-89 行有"新手阅读路线图"注释：

```
命令入口层（Typer CLI）
  global_options / cmd_setup / cmd_update / cmd_one_data / cmd_all_data / cmd_repair_sort
      ↓
编排层（流程控制）
  run_update_with_settings → _resolve_requested_dates_for_plan → _execute_plans → process_product
      ↓
文件同步层（CSV 合并）
  sync_known_product（已知产品增量合并） / sync_unknown_product（未知产品镜像写入）
      ↓
基础能力层
  request_data（HTTP+重试） / extract_archive（解压） / StatusDb（SQLite 状态库） / RunReport
```

辅助模块 `coin_preprocess_builtin.py`（~1165 行）：币圈 spot/swap 合成预处理，产出 `.pkl` 文件。

## 关键概念

- **门控**：比对本地 `timestamp.txt` 与 API 最新日期，决定是否下载
- **回补（catch-up）**：本地落后时构建日期队列逐日补齐；优先用 API `latest` 候选日期，不完整时才逐日探测
- **已知产品 vs 未知产品**：`KNOWN_DATASETS` 中的产品做增量合并（拆分+去重+排序），其余做镜像写入
- **reason_code**：结构化错误分类枚举（`up_to_date` / `no_data_for_date` / `download_failed` 等）
- **原子写入**：状态库和 timestamp 同步更新；预处理产物全量成功后原子替换

## 核心数据模型

| 类 | 用途 |
|---|---|
| `UserConfig` | 用户配置（data_root、product_mode） |
| `CommandContext` | 运行时上下文（凭证、路径、参数） |
| `ProductPlan` | 单产品执行计划（产品名、策略、日期列表） |
| `FatalRequestError` | HTTP 4xx 结构化异常（status_code、request_url） |
| `ProductSyncError` | 产品同步错误（reason_code） |
| `SyncStats` | 文件操作统计（created/updated/unchanged + 排序校验统计） |
| `RunReport` | 运行报告 |

## 测试

- `tests/` 目录，`unittest` 框架，大量使用 `unittest.mock.patch` 模拟 API 和文件操作
- 测试文件与功能对应：`test_update_catchup`（回补）、`test_http_error_mapping`（HTTP 错误）、`test_sort_repair`（排序修复）、`test_encoding_strategy`（编码保留）、`test_coin_preprocess_*`（币圈预处理）

## 状态和配置文件

- `user_config.json` / `user_secrets.env` — 用户配置和凭证（.gitignore 保护，不要提交）
- `<data_root>/.quantclass_sync/status/` — SQLite 状态库 + JSON 导出
- `<data_root>/<product>/timestamp.txt` — 每个产品最后同步日期
- `log/run_report_*.json` — 运行报告

## 编码规范

- 中文注释和日志；HTTP 错误码有中文映射（404→资源不存在、403→无下载权限等）
- 新增已知产品规则时需更新 `AGGREGATE_SPLIT_COLS` 和 `RULES` 字典（含排序键配置）
- v0.7.5+ 写入链路内置排序校验，异常时自动修复并记录统计
