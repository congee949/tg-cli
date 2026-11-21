# tg-cli archive fork

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

这个 fork 用于把指定 Telegram 群长期保存在本地。它在 [jackwener/tg-cli](https://github.com/jackwener/tg-cli) 的基础上增加三项能力：

- `tg sync-many` 按群分批增量同步，返回每个群的写入数量、失败状态和限流等待时间。
- `tg sync-media` 按起始时间下载照片和文档，中断后从游标继续。
- [本地归档工具](examples/archive/) 合并两个 SQLite 来源，并在 macOS APFS 上为已有媒体建立独立副本。

原作者的通用搜索、导出、监听和调度说明仍保留在上游仓库。这里只记录本 fork 的安装和使用方式。

## 安装

```bash
uv tool install git+https://github.com/congee949/tg-cli.git
```

从源码运行：

```bash
git clone https://github.com/congee949/tg-cli.git
cd tg-cli
uv sync --group dev
uv run tg status
```

首次运行会要求登录你的 Telegram 账号。建议在 [my.telegram.org](https://my.telegram.org) 创建自己的应用，并设置 `TG_API_ID` 和 `TG_API_HASH`。

## 使用

先查看账号和已加入的会话：

```bash
tg status --yaml
tg chats --yaml
```

同步指定群。`--limit` 是每轮最多处理的消息数，积压较多时重复运行同一命令即可继续：

```bash
tg sync-many --request="GroupName=50" --json
```

从指定时间开始归档媒体。`--since` 必须带时区，`--output` 使用独立目录：

```bash
tg sync-media \
  --chat="GroupName" \
  --since="2026-01-01T00:00:00Z" \
  --output="./archive" \
  --limit=50
```

重复运行同一命令会继续未完成的下载，并抓取新消息。单个文件上限为 64 MiB，普通下载失败最多尝试 3 次。命令输出 JSON 摘要。

搜索已保存的文字：

```bash
tg search "keyword" --chat "GroupName" --yaml
```

## 本地对账

媒体命令会在输出目录创建 `messages.db` 和 `media/<marked-id>/`。对账命令另外读取 tg-cli 的文字缓存：

```bash
export TGA_HOME="$PWD/archive"
python3 -B examples/archive/archive_reconcile.py \
  --chat-id "$CHAT_ID" \
  --first-msg-id "$FIRST_MSG_ID" \
  --tg-db "$TG_DB" \
  --anti-db "$TGA_HOME/messages.db"
```

在同一台 Mac 的同一个 APFS 卷上创建媒体副本：

```bash
python3 -B examples/archive/archive_media_copy.py \
  --chat-id "$CHAT_ID" \
  --first-msg-id "$FIRST_MSG_ID" \
  --init --limit 32
```

参数、目录结构和增量行为见 [examples/archive/README.md](examples/archive/README.md)。机器可读的同步回执见 [SCHEMA.md](SCHEMA.md)。

## 上游命令

`refresh`、`search`、`export`、`listen` 等原有命令继续可用。完整说明、调度示例和账号安全建议见上游 [README](https://github.com/jackwener/tg-cli/blob/v0.6.0/README.md)。本 fork 当前基于上游 `v0.6.0`。
