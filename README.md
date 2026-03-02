# QuantClass 数据同步工具

自动从 QuantClass 下载股票和币圈数据，帮你把本地数据保持最新。
好几天没跑也没关系，一条命令就能把缺的全补上。

当前版本：**v0.8.2**

---

## 三步开始用

### 1. 装依赖

```bash
python3 -m pip install -r requirements.txt
```

如果你的数据里有 `.7z` 或 `.rar` 压缩包，还需要装一个额外依赖：

```bash
python3 -m pip install -r requirements-archive.txt
```

### 2. 配置

```bash
python3 quantclass_sync.py setup
```

跟着提示填三样东西就行：数据存哪（`data_root`）、`API Key`、`HID`。

> 小提示：第一次直接跑 `python3 quantclass_sync.py` 也会自动引导你配置。

### 3. 更新数据

```bash
# 先试跑一次，看看会做什么（不会真的写数据）
python3 quantclass_sync.py --dry-run

# 没问题就正式跑
python3 quantclass_sync.py
```

搞定。后面每天只要重复第 3 步就行。

---

## 命令说明

### setup — 配置

```bash
python3 quantclass_sync.py setup
```

运行后会检查一下网络能不能通（不想检查可以加 `--skip-check`）。
如果连不上，配置不会保存，防止你存了个错的凭证进去。

配好的东西存在 `user_config.json` 和 `user_secrets.env` 里，已经加了 gitignore，不会被误提交。

**非交互模式**（写脚本自动化时用）：

```bash
python3 quantclass_sync.py setup --non-interactive \
  --data-root /your/data/root \
  --api-key YOUR_API_KEY \
  --hid YOUR_HID \
  --product-mode local_scan
```

**指定固定产品列表**（不想自动扫描时用）：

```bash
python3 quantclass_sync.py setup --non-interactive \
  --product-mode explicit_list \
  --products stock-trading-data \
  --products stock-main-index-data
```

### update — 更新数据

```bash
python3 quantclass_sync.py           # 直接跑就行，默认就是 update
python3 quantclass_sync.py update    # 写全也行，效果一样
```

它会自己判断：
- 本地落后了？从上次同步的日期开始，一天天补到最新
- 本地已经是最新？直接跳过，不浪费时间
- 中途出错了（网络断了、文件写坏了）？立刻停下来告诉你

**常用选项：**

| 选项 | 干什么用 |
|------|----------|
| `--dry-run` | 模拟跑一遍，不真的写数据 |
| `--force` | 不管本地是不是最新，强制重新下载 |
| `--products` | 只更新指定的产品（可以写多个） |
| `--no-verbose` | 不想看那么多日志就加这个 |

**例子** — 只更新两个产品：

```bash
python3 quantclass_sync.py --products stock-trading-data --products stock-main-index-data
```

**凭证从哪读？** 按优先级：命令行参数 > `user_secrets.env` > 环境变量。

### 币圈合成（自动的，不用管）

如果你的数据目录里有 `coin-binance-spot-swap-preprocess-pkl-1h`，并且这次跑的时候 spot 或 swap 有新数据进来，工具会自动帮你跑一遍合成预处理。

不需要任何额外配置，几个细节：
- spot 和 swap 两边都得有数据才会写结果，缺一边就不动
- 写文件是原子操作——要么全成功，要么保留旧的不动
- 默认走增量更新，实在不行会自动退回全量重算
- `--dry-run` 时不会跑，只会在报告里记一笔

### repair_sort — 修历史排序问题

发现历史 CSV 排序不对？用这个命令扫一遍并自动修：

```bash
python3 quantclass_sync.py repair_sort            # 扫描 + 修复
python3 quantclass_sync.py repair_sort --dry-run   # 只看不动
```

### repair_coin_cap — 一次性修 coin-cap 历史文件

`coin-cap` 现在按已知规则同步（按 `symbol` 拆分，主键为 `candle_begin_time + symbol`）。
如果你本地还留有旧版日期文件（如 `2026-02-28.csv`），可以用这个脚本做一次清理：

```bash
python3 scripts/repair_coin_cap.py --dry-run
python3 scripts/repair_coin_cap.py
```

说明：
- 默认会先备份到 `<data_root>/coin-cap.backup-<timestamp>/`
- `--no-backup` 可关闭备份
- 会清理 `coin-cap` 目录下日期命名的遗留 CSV，并对 symbol 文件做去重+排序

---

## 文件在哪

### 配置文件（gitignore 保护，不会提交）

| 文件 | 内容 |
|------|------|
| `user_config.json` | 数据目录、产品策略 |
| `user_secrets.env` | API Key 和 HID |

### 运行产生的文件

| 位置 | 内容 |
|------|------|
| `<data_root>/.quantclass_sync/status/` | 同步状态（SQLite + JSON） |
| `<data_root>/<product>/timestamp.txt` | 每个产品上次同步到哪天 |
| `log/run_report_*.json` | 每次运行的报告 |

想看最近一次运行报告：

```bash
ls -t log/run_report_* | head -1 | xargs cat
```

### 缓存

临时缓存在 `<project_root>/../.cache/quantclass/`，跑完自动清理，不用管。

---

## 常见问题

**报"未找到用户配置文件"怎么办？**

先跑一次 `python3 quantclass_sync.py setup`。

**跑了但是没更新任何数据？**

三个常见原因：本地已经是最新了；加了 `--dry-run`；数据目录下没有对应产品的文件夹。

**怎么看运行报告？**

```bash
ls -t log/run_report_* | head -1 | xargs cat
```

---

## 注意安全

- `user_secrets.env` 不要提交到 Git（默认已 gitignore）
- API Key 和 HID 不要发到群里或截图
- 正式跑之前，先 `--dry-run` 一下总没错

---

## 旧命令还能用吗

`init`、`one_data`、`all_data` 这些旧命令还保留着，能跑，但建议换成 `setup + update`。

旧的脚本名 `quantclass_daily_sync.py` 和旧目录已经删了。

---

## 更新记录

<details>
<summary>v0.8.2</summary>

- 安全和稳定性加固：状态写入、权限回滚、解压校验等多处修复
- HTTP 改用连接池，批量同步更快
- 新增 37 个回归测试（总计 199 个）

</details>

<details>
<summary>v0.8.1</summary>

- coin-cap 从未知镜像升级为已知规则合并（按 `symbol` 拆分，主键为 `candle_begin_time + symbol`）
- 加入按文件日期过滤，避免跨天/脏日期行混入当日结果
- 遇到日期列缺失或过滤后无有效行时，统一按 `merge_error` 处理，避免错误推进 `timestamp.txt`
- 新增 `scripts/repair_coin_cap.py`，可一键清理历史日期命名文件并对 symbol 文件去重排序
- 补充 coin-cap 相关回归测试，覆盖过滤异常与失败语义路径

</details>

<details>
<summary>v0.8.0</summary>

- 核心代码拆分成 `quantclass_sync_internal/` 模块包
- 币圈预处理拆分成 `coin_preprocess_internal/`
- 镜像产品的告警分类更精确了（区分 `mirror_fallback` 和 `mirror_unknown`）
- 补了一批自动化测试

</details>

<details>
<summary>v0.7.5</summary>

- 修了 `stock-notices-title` 排序不一致的问题
- 写数据时会自动检查排序，发现问题自动修
- 加了 `repair_sort` 命令，能批量修历史数据的排序

</details>

<details>
<summary>v0.7.4</summary>

- 404 错误不再显示"参数错误"，改成了更准确的"该日期无数据"
- 回补时碰到没数据的日期会跳过继续，不再一停全停

</details>

<details>
<summary>v0.7.3</summary>

- 回补时更聪明了，不必要的 API 请求少了很多
- 修了日期不连续时可能漏补的问题

</details>

<details>
<summary>v0.7.2</summary>

- `update` 支持自动回补了，落后几天跑一次就能全补上

</details>

<details>
<summary>v0.7.1 及更早</summary>

- v0.7.1：修了币圈预处理的性能警告
- v0.7.0：币圈预处理大幅提速
- v0.6.7：写文件改成原子操作，失败不会覆盖旧数据
- v0.6.6：内置预处理，不再依赖外部脚本
- v0.6.5：币圈合成开箱即用
- v0.6.4：直接跑脚本默认执行 update
- v0.6.3：清理旧入口，统一用 `quantclass_sync.py`
- v0.6.2：修了更新时编码被改掉的问题

</details>
