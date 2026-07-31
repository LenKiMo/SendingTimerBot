# SendingTimeBot 📬

> 零第三方依赖的 Telegram 定时消息机器人 —— 仅使用 Python 3 标准库

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose%20ready-2496ED.svg?logo=docker&logoColor=white)](docker-compose.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com)

一个不依赖任何第三方库（无需 `pip install` 任何包）的 Telegram 定时消息机器人：配置目标频道与发送间隔后，所有提交给它的消息将按固定节奏自动发送到指定频道。

## 目录

- [功能特性](#-功能特性)
- [快速开始](#-快速开始)
- [使用说明](#-使用说明)
- [调度算法](#-调度算法)
- [配置项](#-配置项)
- [部署指南](#-部署指南)
- [项目结构](#-项目结构)
- [测试](#-测试)
- [常见问题](#-常见问题)
- [许可证](#-许可证)

## ✨ 功能特性

- **零依赖**：仅用 Python 3 标准库（`urllib` / `threading` / `json`），没有 `requirements.txt`
- **严格定时调度**：消息按指定间隔（分钟/秒/时/天）逐条发送，首条 = 收到时刻 + 间隔，后续严格 FIFO 累加
- **批量入队**：多行文本、上传的 `.txt` 等文本文件均按行解析，一次可入队数百条消息
- **循环发送**：`/setloop` 将内容按次数 / 时长 / 结束时间循环定时发送（受条数上限约束）
- **状态持久化**：队列原子写入 `state.json`，停容器 / 重启进程不丢消息
- **断点续传**：长轮询 offset 持久化，重启后不重复消费更新
- **容器化**：`docker compose` 一条命令上线下线，随时恢复
- **健壮性**：429 限流 / 5xx / 网络错误自动指数退避重试；发送失败跳过并通知；用户白名单
- **防滥用**：群组目标需管理员验证（非管理员的 `/set` 需群管理员 `/approve` 审批）；私人用户目标默认禁止（需显式开启，且 Telegram 平台要求用户先与 bot 交互）

## 🚀 快速开始

### 方式一：Docker（推荐）

前置：已安装 Docker 与 Docker Compose。

```bash
# 1. 填写配置
cp .env.example .env          # Windows: copy .env.example .env
#    编辑 .env，填入 BOT_TOKEN=（找 @BotFather 用 /newbot 创建后获取）

# 2. 启动
docker compose up -d --build

# 3. 查看日志
docker compose logs -f

# 4. 下线（队列保留在 ./data，随时可再次上线）
docker compose down
```

### 方式二：本地运行

前置：Python 3.8+。

```bash
python bot.py        # Windows 下也可用 py bot.py
```

- 启动自检通过后输出 `已连接 Telegram: @xxx`，Ctrl+C 停止
- `.env` 需位于当前目录

### 目标权限与验证（三种方式均需）

**目标类型与防滥用策略**（/set 时自动识别目标类型）：

| 目标类型 | 验证方式 |
| --- | --- |
| 频道（@用户名 或 ID） | 直接生效；bot 需为频道管理员（Telegram 强制） |
| 群组 | **操作者本人是群管理员 → 直接生效**；否则需群管理员在**群内**发送 `/approve` 审批（60 分钟内有效） |
| 私人用户 | **默认禁止**（防滥用）。需 `.env` 设置 `ALLOW_PRIVATE_TARGETS=true`，且目标用户必须先与 bot 交互（Telegram 平台禁止 bot 主动联系未交互过的用户） |

群组审批流程：

```
操作者（非管理员） /set 群组 5
    ↓
bot 在群内发布通知：「请群管理员在群内发送 /approve 确认」
    ↓
群管理员在群内发送 /approve（bot 校验管理员身份，Telegram 验证）
    ↓
定时目标启用，开始排队发送
```

## 🎮 使用说明

| 命令 | 作用 |
| --- | --- |
| `/start` | 欢迎与快速开始指引 |
| `/set <目标> <间隔>` | 设置转发目标并开始接收。间隔：纯数字 = 分钟，支持后缀 `s/m/h/d`（`30s`、`5`、`2h`）。换目标时清空旧队列。目标类型与验证策略见上文 |
| `/setloop <间隔> <限制>` | 配置循环发送：随后提交一条内容（文本 / txt，每行一条），按限制循环定时发送到同一目标。限制见下文 |
| `/approve` | 群管理员在目标**群内**发送，批准定时消息配置请求（也可 `/approve <群组>` 从任意会话发送，同样校验管理员身份） |
| `/cancel` | 停止接收新消息；**已排队的消息继续按时发送** |
| `/status` | 查看目标、间隔、接收状态、待发数量、下一条发送时间、循环信息、待审批请求 |
| `/help` | 完整功能菜单 |

**发送规则**

- 私聊文本消息：多行时**每行视为一条消息**（一次粘贴 20 个链接 = 排队 20 条）
- 上传 `.txt` 等文本文件：**每行视为一条消息**（默认上限 5MB / 5000 行）
- 空行自动忽略；发送失败（如频道权限问题）会跳过该条并私信通知
- 群聊中的普通消息不会被入队（仅私聊）

### 🔁 循环发送（/setloop）

在已设置目标（`/set`）后，把一段内容按规律循环定时发送：

```
/setloop <间隔> <限制>
```

| 限制参数 | 含义 | 示例 |
| --- | --- | --- |
| 纯数字 `N` / `times:N` / `次数:N` | 循环 N 次（内容每完整一遍计 1 次） | `/setloop 5 times:10` |
| `duration:D` / `时长:D` | 循环时长（D 用间隔格式） | `/setloop 5 duration:2h` |
| `until:T` / `直到:T` | 循环至结束时间（`HH:MM` 或 `YYYY-MM-DD HH:MM`，本地时区） | `/setloop 5 until:2026-08-01 12:00` |

示例：`/setloop 5 times:10` 后发送包含 3 个链接的 txt → 3 条内容循环 10 遍 = 30 条，每 5 分钟一条。

- 循环配置后，**下一条**文本 / 文件即循环内容（每行一条，一次性消费，不会被转发）
- 循环消息发往 `/set` 设置的同一目标；`/cancel` 停止接收新消息，已排队的循环消息继续按时发送
- 循环序列展开受 `MAX_LOOP_ITEMS`（默认 5000 条）与队列上限约束，防滥用

## ⏰ 调度算法

与需求语义一一对应：

| 场景 | 定时规则 |
| --- | --- |
| 目标频道无定时消息 | 首条 = **收到时刻 + 间隔** |
| 目标频道已有定时消息 | 下一条 = **最后一条定时消息的定时时刻 + 间隔**（严格 FIFO） |
| 队列发空后收到新消息 | 沿用最后一条的定时时刻续排，保持既定节奏 |

**示例时间线**（`/set @pics 5`，10:00 配置）：

```
10:00  收到 20 个链接 → 排队 20 条
10:05  第 1 条发送
10:10  第 2 条发送
10:15  第 3 条发送
...    （每 5 分钟一条，共 100 分钟）
```

即使发送发生延迟（如网络抖动），后续消息仍按原定时时刻的节奏发送，不重新计锚点。

## ⚙️ 配置项

`.env` 文件（UTF-8），优先级：**系统环境变量 > `.env` 文件 > 默认值**。

| 键 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `BOT_TOKEN` | ✅ | — | BotFather 获取的 token |
| `ALLOWED_USER_IDS` | — | 空（任何人） | 允许使用命令的用户 ID 白名单，逗号分隔 |
| `CHECK_INTERVAL_S` | — | 0.5 | 调度线程检查间隔（秒） |
| `MAX_QUEUE` | — | 10000 | 队列上限（条） |
| `MAX_FILE_BYTES` | — | 5242880 | 上传文件大小上限（字节） |
| `MAX_FILE_LINES` | — | 5000 | 单个文件最多解析行数 |
| `STATE_PATH` | — | data/state.json | 状态文件路径 |
| `API_BASE_URL` | — | https://api.telegram.org | API 地址（测试/特殊场景可覆盖） |
| `MAX_RETRIES` | — | 3 | API 失败重试次数（429/5xx/网络错误指数退避） |
| `ALLOW_PRIVATE_TARGETS` | — | false | 是否允许向私人用户定时发送（默认禁止，防滥用） |
| `PENDING_APPROVAL_SECONDS` | — | 3600 | 群组审批请求有效期（秒） |
| `MAX_LOOP_ITEMS` | — | 5000 | 循环发送单次展开的消息总数上限（防滥用） |
| `TZ` | — | — | 定时时间的显示时区（如 `Asia/Shanghai`） |

## 🐳 部署指南

基于 Docker Compose 的最小灵活部署方案：`restart: unless-stopped` 保证崩溃或宿主机重启后自动拉起；`./data` 挂载卷保证队列持久化。

```bash
# 上线
docker compose up -d --build

# 查看实时日志
docker compose logs -f

# 下线（不影响已排队消息，数据保留在 ./data）
docker compose down

# 随时重新上线（已排队消息继续按计划发送）
docker compose up -d

# 更新代码后重新部署
git pull
docker compose up -d --build
```

**数据与持久化**

- 队列状态与轮询进度实时写入 `./data/state.json`（原子替换写入，防损坏）
- 容器销毁、重建、升级均不丢数据；如需彻底清空，删除 `./data` 目录后重启即可

**本地部署**（非容器）见[快速开始](#-快速开始)方式二，配置项完全相同。

## 🗂 项目结构

```
SendingTimeBot/
├── bot.py              主程序：长轮询 + 命令处理 + 调度线程
├── queue.py            定时发送队列（线程安全 + JSON 持久化）
├── telegram_api.py     极简 Telegram API 客户端（urllib）
├── config.py           .env 配置加载（纯标准库）
├── test_bot.py         自动化验证（无需真实 token）
├── Dockerfile          零依赖镜像（无需 pip install）
├── docker-compose.yml  一键部署编排
├── LICENSE             MIT 许可证
├── .env.example        配置模板
└── data/               运行时状态（自动生成，勿手动编辑）
```

## 🧪 测试

无需真实 token，可离线运行：

```bash
python test_bot.py
```

覆盖：调度语义单元测试（假时钟逐条验证需求中的定时规则）、间隔/循环限制解析与循环序列展开测试、
群组审批状态生命周期测试，以及端到端测试（本地伪 Telegram API 服务器驱动真实 bot 主循环）：
频道 `/set` → 排队 → 按间隔发送 → `/cancel`；群组管理员直接启用 / 非管理员 `/approve` 审批 / 审批被拒；
私人目标默认禁止与开关启用；`/setloop` 循环发送全流程与上限截断。

## ❓ 常见问题

- **409 冲突**：同一 token 已有另一个实例在轮询。停止旧实例（`docker compose down` 或旧进程）再启动。
- **403 Forbidden / 400 chat not found**：bot 不是频道管理员、无发送权限，或频道 ID / @用户名 有误。检查目标设置后重新 `/set`。
- **向群组设置未生效**：非群管理员的 `/set` 会进入待审批，需群管理员在群内发送 `/approve`（60 分钟内有效，可配置）。操作者本人是管理员则直接生效。
- **无法向私人用户设置**：默认禁止（防滥用），需在 `.env` 设置 `ALLOW_PRIVATE_TARGETS=true` 并重启；同时 Telegram 平台本身禁止 bot 主动联系未交互过的用户（必须先让目标用户与 bot 对话）。
- **时区显示不对**：在 `.env` 中设置 `TZ`（如 `Asia/Shanghai`）后，用 `docker compose up -d` 重建容器（`restart` 不会重新读取 .env），查看启动日志确认「本地时区」行。
- **需要代理**：设置系统环境变量 `HTTP_PROXY` / `HTTPS_PROXY`（urllib 原生支持），重启即可。
- **投递保证**：消息先持久化再确认，重启不会丢失；极端崩溃窗口下可能重复投递（至少一次语义）。

## 📄 许可证

[MIT License](LICENSE)
