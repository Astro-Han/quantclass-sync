# QuantClass 数据同步工具

[QuantClass（邢不行量化课）](https://www.quantclass.cn/) 的数据增量同步工具。自动下载股票和币圈数据，保持本地数据最新。好几天没跑也没关系，一条命令把缺的全补上。

当前版本：**v1.1** | macOS 工具，Windows 用户推荐使用 QuantClass 官方客户端

---

## 开始用

### 方式一：图形界面（推荐）

双击 `QuantClass Sync.command`，首次运行会自动安装依赖并弹出配置向导。

填三样东西：数据目录（先从官网下载数据）、API Key、HID，点"开始使用"就行了。

以后每次双击直接打开总览页，点"同步"更新数据。

### 方式二：命令行

```bash
# 1. 装依赖
python3 -m pip install -r requirements.txt

# 2. 配置（填数据目录、API Key、HID）
python3 quantclass_sync.py setup

# 3. 更新数据
python3 quantclass_sync.py
```

搞定。后面每天只要重复第 3 步就行。

如果你的数据里有 `.7z` 或 `.rar` 压缩包，还需要装一个额外依赖：

```bash
python3 -m pip install -r requirements-archive.txt
```

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
| `--workers N` | 并发下载线程数（1-8，默认 1） |

**例子** — 只更新两个产品：

```bash
python3 quantclass_sync.py --products stock-trading-data --products stock-main-index-data
```

**凭证从哪读？** 按优先级：命令行参数 > `user_secrets.env` > 环境变量。

### status — 查看状态

```bash
python3 quantclass_sync.py status
```

一屏看全局：哪些产品已同步、落后几天、上次运行结果。离线可用，不调 API。

### gui — 图形界面

```bash
python3 quantclass_sync.py gui
```

打开图形界面窗口，包含总览、同步、历史三个页面。关闭窗口后命令退出。

macOS 用户也可以直接双击 `QuantClass Sync.command` 启动 GUI，不需要打开终端。首次运行会自动安装依赖并弹出配置向导，全程不需要输入命令。

### 币圈合成（自动的，不用管）

如果你的数据目录里有 `coin-binance-spot-swap-preprocess-pkl-1h`，并且这次跑的时候 spot 或 swap 有新数据进来，工具会自动帮你跑一遍合成预处理。

不需要任何额外配置，几个细节：
- spot 和 swap 两边都得有数据才会写结果，缺一边就不动
- 写文件是原子操作 - 要么全成功，要么保留旧的不动
- 默认走增量更新，实在不行会自动退回全量重算
- `--dry-run` 时不会跑，只会在报告里记一笔

<details>
<summary><b>修复命令</b>（一般用不到）</summary>

#### repair_sort — 修历史排序问题

发现历史 CSV 排序不对？用这个命令扫一遍并自动修：

```bash
python3 quantclass_sync.py repair_sort            # 扫描 + 修复
python3 quantclass_sync.py repair_sort --dry-run   # 只看不动
```

#### repair_coin_cap — 一次性修 coin-cap 历史文件

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

</details>

---

## 常见问题

**报"未找到用户配置文件"怎么办？**

用 GUI 的话，直接双击 `.command` 启动，会自动弹出配置向导。用命令行的话，跑一次 `python3 quantclass_sync.py setup`。

**跑了但是没更新任何数据？**

三个常见原因：本地已经是最新了；加了 `--dry-run`；数据目录下没有对应产品的文件夹。

---

## 注意安全

- `user_secrets.env` 不要提交到 Git（默认已 gitignore）
- API Key 和 HID 不要发到群里或截图
- 正式跑之前，先 `--dry-run` 一下总没错

---

完整更新记录见 [GitHub Releases](https://github.com/Astro-Han/quantclass-sync/releases)。
