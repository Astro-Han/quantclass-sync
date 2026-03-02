# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目简介

QuantClass 数据同步工具，从 QuantClass API 自动下载、解析、合并金融数据（股票/币圈）。核心特性：本地落后多日时单次命令自动回补所有缺口。

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

当前为“兼容入口 + 内部模块”架构：

```
兼容入口层
  quantclass_sync.py（显式 re-export + CLI 启动）
      ↓
内部模块层（quantclass_sync_internal）
  cli.py（命令） / orchestrator.py（编排） / file_sync.py+csv_engine.py（文件同步）
  / http_client.py（HTTP） / reporting.py（报告） / status_store.py（状态）等
      ↓
预处理模块层（coin_preprocess_internal）
  runner.py / csv_source.py / symbol_mapper.py / pivot.py
```

`coin_preprocess_builtin.py` 保留兼容导出，实际实现迁移到 `coin_preprocess_internal/`。

## 关键概念

- **门控**：比对本地 `timestamp.txt` 与 API 最新日期，决定是否下载
- **回补（catch-up）**：本地落后时构建日期队列逐日补齐；优先用 API `latest` 候选日期，不完整时才逐日探测
- **已知产品 vs 未知产品**：`KNOWN_DATASETS` 中的产品做增量合并（拆分+去重+排序），其余做镜像写入
- **reason_code**：结构化错误分类（`up_to_date` / `no_data_for_date` / `mirror_fallback` / `mirror_unknown` 等）
  - `mirror_fallback` — 已知产品有文件没命中规则，需关注
  - `mirror_unknown` — 未知产品按预期走镜像，正常
  - `.ts` 文件镜像不影响产品级 reason_code
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
- 测试文件与功能对应：`test_update_catchup`（回补）、`test_http_error_mapping`（HTTP 错误）、`test_sort_repair`（排序修复）、`test_encoding_strategy`（编码保留）、`test_coin_preprocess_*`（币圈预处理）、`test_mirror_reason_codes`（mirror reason_code 语义）
- 测试使用 `tempfile.TemporaryDirectory()`（unittest 风格，不用 pytest 的 tmp_path）

## 状态和配置文件

- `user_config.json` / `user_secrets.env` — 用户配置和凭证（.gitignore 保护，不要提交）
- `<data_root>/.quantclass_sync/status/` — SQLite 状态库 + JSON 导出
- `<data_root>/<product>/timestamp.txt` — 每个产品最后同步日期
- `log/run_report_*.json` — 运行报告

## 编码规范

- 中文注释和日志；HTTP 错误码有中文映射（404→资源不存在、403→无下载权限等）
- 新增已知产品规则时需更新 `AGGREGATE_SPLIT_COLS` 和 `RULES` 字典（含排序键配置）
- 已完成主链路模块化重构，保持 CLI 兼容
- **禁止弯引号**：Python 字符串只能用 ASCII 直引号（`'` `"`），弯引号只允许出现在中文注释里
- 每次改动代码后，运行测试验证：`python3 -m unittest discover -s tests -p 'test_*.py' -v`

## 项目文件结构

- `docs/` — 本地临时文档（已 gitignore，不进仓库）
  - `docs/plans/` — 设计和执行计划（给 Codex 用的 PLAN.md 等）
  - `docs/release/` — 论坛发布草稿
  - `docs/reviews/` — 代码审查笔记
