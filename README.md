# QuantClass 数据同步工具

[QuantClass（邢不行量化课）](https://www.quantclass.cn/) 的数据增量同步工具，通过 API 下载股票和币圈数据。好几天没跑也没关系，一条命令把缺的全补上。

当前版本：**v1.6.1** | macOS 工具，Windows 用户推荐使用 QuantClass 官方客户端

## API 要停了，建议切到官方客户端

根据[官方公告](https://bbs.quantclass.cn/thread/87232)，股票和 B 圈的数据 API 将于 **2026 年 12 月 31 日**停用。本工具依赖这些接口，到时候同步功能就用不了了，已经在用的同学可以提前切换。官方建议改用客户端或回测网页版维护数据更新。

**10 月 31 日补充：Mac 可以直接用官方客户端同步数据了～** 我自己原来的 9 项股票数据已经接上，自动更新也跑起来了，不需要另外找台 Windows 电脑中转。

官方客户端下载：[quantclass-client-pro Releases](https://github.com/qtcls-admin/quantclass-client-pro/releases/latest)

已经有本地数据的，可以这样切换：

1. 安装官方客户端并登录，在数据模块选择原来的**数据根目录**（各产品文件夹的上一级），订阅原来使用的产品。
2. 先用一个小产品试着更新，确认路径和数据日期，再接入其他产品、开启自动更新。我这边大部分能直接接着更新，这次验证的是 9 项股票数据，其他产品建议先试一下。
3. 本地落后太久的产品，可能需要重新下载全量数据。**全量恢复会覆盖对应产品目录**，操作前看清路径和提示。
4. 确认官方客户端能正常更新后，把旧同步工具和原来的定时同步任务停掉，避免两套程序同时写同一份数据。

我自己已经切到官方客户端了，下面保留原工具的使用说明。

---

## 原工具用法（API 停用前）

下载解压，双击 `QuantClass Sync.command`。首次运行自动装依赖、弹出配置向导，填三样东西就能用：数据目录、API Key、HID。

配好之后四个页面：
- **总览** -- 所有产品按分类显示中文名和数据状态，可搜索；点"检查更新"查询最新日期
- **同步** -- 点一下开始更新，实时看每个产品的进度和结果
- **历史记录** -- 每次运行的结果都有记录，失败产品一目了然
- **数据健康** -- 全面检查数据质量，可修复的问题一键修复

<details>
<summary><b>命令行用法</b>（熟悉终端的用户）</summary>

```bash
# 1. 装依赖
python3 -m pip install -r requirements.txt

# 2. 配置
python3 quantclass_sync.py setup

# 3. 更新数据
python3 quantclass_sync.py
```

后面每天只要重复第 3 步。

**命令速查：**

| 命令 | 说明 |
|------|------|
| `python3 quantclass_sync.py` | 更新数据（默认命令） |
| `python3 quantclass_sync.py status` | 查看同步状态（离线可用） |
| `python3 quantclass_sync.py gui` | 打开图形界面 |
| `python3 quantclass_sync.py audit` | 数据质量检查（`--fix` 自动修复） |
| `python3 quantclass_sync.py setup` | 重新配置 |

**update 常用选项：**

| 选项 | 说明 |
|------|------|
| `--dry-run` | 模拟运行：列出要补的日期，不下载、不写数据、不消耗下载次数 |
| `--force` | 强制重新下载 |
| `--products NAME` | 只更新指定产品（可多次使用） |
| `--workers N` | 并发线程数（1-8，默认 4） |

</details>

---

## 注意

- 币圈用户：spot/swap 有新数据时会自动触发合成预处理，不需要额外操作
- `user_secrets.env` 不要提交到 Git（默认已 gitignore）
- API Key 和 HID 不要发到群里或截图

---

完整更新记录见 [GitHub Releases](https://github.com/Astro-Han/quantclass-sync/releases)。
