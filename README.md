# QuantClass 数据同步工具

[QuantClass（邢不行量化课）](https://www.quantclass.cn/) 的数据增量同步工具。自动下载股票和币圈数据，保持本地数据最新。好几天没跑也没关系，一条命令把缺的全补上。

当前版本：**v1.5.0** | macOS 工具，Windows 用户推荐使用 QuantClass 官方客户端

---

## 开始用

下载解压，双击 `QuantClass Sync.command`。首次运行自动装依赖、弹出配置向导，填三样东西就能用：数据目录、API Key、HID。

配好之后三个页面：
- **总览** -- 所有产品的数据状态一目了然，点"检查更新"查询最新日期，点"数据健康"全面检查数据质量
- **同步** -- 点一下开始更新，实时看每个产品的进度和结果
- **历史** -- 每次运行的结果都有记录，失败产品一目了然

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
| `--dry-run` | 模拟运行，不写数据 |
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
