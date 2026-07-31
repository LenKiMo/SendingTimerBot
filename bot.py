"""SendingTimeBot —— 零第三方依赖的 Telegram 定时消息机器人。

用法：python bot.py [--env .env]

流程：
1. 读取配置（.env）→ 自检 getMe → 启动调度线程 → 开始长轮询
2. /set 目标 间隔 → 之后所有私聊文本（多行按行拆分）与上传的文本文件（按行拆分）
   进入定时队列，按间隔向目标逐条发送（频道/群组，群组需管理员验证）
3. /setloop 间隔 限制 → 收集循环内容（文本/txt，每行一条），按次数/时长/结束时间
   展开为定时序列发往同一目标
4. /cancel 停止接收新消息；已排队消息（含循环）继续按时发送
5. 队列、循环信息与轮询 offset 实时持久化到 state.json，重启不丢不重
"""

import argparse
import datetime as dt
import logging
import threading
import time

from config import Config
from queue import SendingQueue
from telegram_api import ApiError, TelegramClient

log = logging.getLogger("bot")

INTERVAL_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

START_TEXT = (
    "👋 你好！我是 SendingTimeBot，定时消息机器人。\n\n"
    "我能把发给我的消息按指定间隔定时发送到频道/群组，也支持循环发送。\n\n"
    "快速开始：\n"
    "1️⃣ /set <目标> <间隔> 设置目标（如 /set @pics 5）\n"
    "2️⃣ 之后发给我的文本 / txt 文件将按间隔逐条定时发送\n"
    "3️⃣ /setloop 可配置循环发送；/status 查看状态；/cancel 停止接收\n\n"
    "发送 /help 查看完整功能菜单。"
)

HELP_TEXT = (
    "📖 SendingTimeBot 功能菜单\n\n"
    "▶️ 定时转发（转发模式）\n"
    "/set <目标> <间隔> — 设置转发目标并开始接收\n"
    "    目标：频道（@用户名 / ID）直接生效，需 bot 为频道管理员\n"
    "          群组：操作者是管理员则直接生效，否则需群内 /approve 审批\n"
    "          私人：默认禁止（需 .env 开启 ALLOW_PRIVATE_TARGETS）\n"
    "    间隔：纯数字 = 分钟，支持 s/m/h/d（如 30s、5、2h）\n"
    "    之后所有私聊文本（每行一条）与 txt 文件（每行一条）排队发送\n\n"
    "🔁 循环发送（循环模式）\n"
    "/setloop <间隔> <限制> — 配置循环，随后发送内容（文本 / txt，每行一条）\n"
    "    限制：times:N  循环 N 次（内容每完整一遍计 1 次）\n"
    "          duration:D  循环时长（如 duration:2h）\n"
    "          until:T  循环至结束时间（如 until:2026-08-01 12:00）\n"
    "    示例：/setloop 5 times:10 → 内容循环 10 遍，共 10×N 条\n"
    "    需先 /set 设置目标；循环消息发往同一目标；单次上限 5000 条\n\n"
    "🛡 群组审批\n"
    "/approve — 群管理员在目标群内发送，批准定时配置请求\n\n"
    "🗂 状态与控制\n"
    "/status — 查看目标 / 队列 / 循环 / 待审批状态\n"
    "/cancel — 停止接收新消息（已排队消息继续按时发送）\n"
    "/start — 欢迎与快速开始\n"
    "/help — 本菜单"
)


def parse_interval(raw):
    """解析间隔："5" = 5 分钟；支持后缀 s/m/h/d，如 30s、2h。返回秒数（最小 1 秒）。"""
    text = str(raw).strip().lower()
    if not text:
        raise ValueError("间隔不能为空")
    if text[-1] in INTERVAL_UNITS:
        seconds = float(text[:-1]) * INTERVAL_UNITS[text[-1]]
    else:
        seconds = float(text) * 60
    if seconds < 1:
        raise ValueError("间隔不能小于 1 秒")
    return seconds


def format_time(ts):
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "—"


def format_interval(seconds):
    seconds = int(seconds)
    if seconds % 3600 == 0:
        return "%d 小时" % (seconds // 3600)
    if seconds % 60 == 0:
        return "%d 分钟" % (seconds // 60)
    return "%d 秒" % seconds


def _parse_until_time(value):
    """解析循环结束时间：HH:MM（当天/次日）或 YYYY-MM-DD HH:MM[:SS]（本地时区）。返回 epoch 秒。"""
    now = time.time()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%H:%M"):
        try:
            parsed = dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
        if fmt == "%H:%M":
            today = dt.datetime.fromtimestamp(now)
            parsed = dt.datetime(today.year, today.month, today.day, parsed.hour, parsed.minute)
            if parsed.timestamp() <= now:
                parsed = parsed + dt.timedelta(days=1)
        elif parsed.timestamp() <= now:
            raise ValueError("结束时间已过")
        return parsed.timestamp()
    raise ValueError("无法解析结束时间（支持 HH:MM 或 YYYY-MM-DD HH:MM）")


def parse_loop_limit(raw):
    """解析 /setloop 的循环限制参数。

    - 纯数字 N       → times:N（内容每完整一遍计 1 次循环）
    - times:N / 次数:N → 同上
    - duration:D / 时长:D → 循环时长（D 用间隔格式，如 2h、90m）
    - until:T / 直到:T  → 循环到指定结束时间（HH:MM 或 YYYY-MM-DD HH:MM）
    返回 (类型, 数值)。类型：times / duration / until。
    """
    text = str(raw).strip()
    if not text:
        raise ValueError("缺少循环限制参数")
    key, _, value = text.partition(":")
    if value:
        key = key.strip().lower()
        value = value.strip()
        if key in ("times", "次数", "循环"):
            try:
                n = int(value)
            except ValueError:
                raise ValueError("循环次数必须是整数（如 times:10）")
            if n < 1:
                raise ValueError("循环次数必须大于 0")
            return "times", n
        if key in ("duration", "时长"):
            return "duration", parse_interval(value)
        if key in ("until", "直到"):
            return "until", _parse_until_time(value)
        raise ValueError("无法识别限制参数（支持 times:N / duration:D / until:T）")
    try:
        n = int(text)
    except ValueError:
        raise ValueError("限制参数格式错误（支持 times:N / duration:D / until:T）")
    if n < 1:
        raise ValueError("循环次数必须大于 0")
    return "times", n


def build_loop_sequence(items, interval_s, limit_type, limit_value, start_time, cap):
    """展开循环发送序列（防滥用：受 cap 总条数上限约束）。

    - times:N    → 内容完整循环 N 遍，共 len(items)×N 条（可被 cap 截断为整轮）
    - duration:D → 从 start_time 起 D 秒内逐条排布（可半轮）
    - until:T    → 逐条排布直到 T（含边界，可半轮）
    返回 (序列, 完整轮数, 是否被上限截断)。
    """
    if not items:
        return [], 0, False
    sequence = []
    truncated = False
    if limit_type == "times":
        rounds = min(int(limit_value), cap // len(items))
        total = len(items) * rounds
        if total > cap or rounds < int(limit_value):
            truncated = True
        if rounds <= 0:
            return [], 0, truncated
        for _ in range(rounds):
            sequence.extend(items)
        return sequence, rounds, truncated
    # duration / until：逐条排布
    end_time = start_time + limit_value if limit_type == "duration" else limit_value
    k = 0
    while start_time + (k + 1) * interval_s <= end_time:
        if len(sequence) >= cap:
            truncated = True
            break
        sequence.append(items[k % len(items)])
        k += 1
    rounds = (k + len(items) - 1) // len(items) if k else 0
    return sequence, rounds, truncated


class Bot:
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = TelegramClient(cfg.bot_token, api_base=cfg.api_base, max_retries=cfg.max_retries)
        self.queue = SendingQueue(cfg.state_path, max_pending=cfg.max_queue)
        self._stop = threading.Event()
        self._owner_chat = None  # 用于失败通知的会话
        self._last_notify_at = 0.0  # 失败通知节流
        self._loop_collect = None  # /setloop 内容收集状态（一次性）

    # ------------------------------------------------------------------ 运行
    def run(self):
        me = self.api.get_me()
        log.info("已连接 Telegram: @%s (%s)", me.get("username"), me.get("first_name"))
        threading.Thread(target=self._scheduler_loop, name="scheduler", daemon=True).start()
        offset = self.queue.get_offset()
        log.info("开始长轮询（offset=%s，目标=%s）", offset, self.queue.snapshot()["target"])
        while not self._stop.is_set():
            try:
                updates = self.api.get_updates(offset=offset)
            except ApiError as exc:
                if exc.code == 409:
                    log.error("409 冲突：已有另一个实例在轮询同一 token，请先停止旧实例再启动。")
                    self._stop.set()
                    break
                log.error("getUpdates 失败：%s", exc)
                time.sleep(3)
                continue
            except Exception as exc:  # 网络错误
                log.error("getUpdates 网络错误：%s", exc)
                time.sleep(3)
                continue
            for update in updates:
                update_id = update.get("update_id")
                if update_id is None:
                    continue
                offset = update_id + 1
                try:
                    self._process_update(update)
                except Exception:
                    log.exception("处理 update %s 时出错", update_id)
                # 处理成功后才推进 offset（至少一次投递；极端崩溃时可能重复，但不丢失）
                self.queue.set_offset(offset)

    # ------------------------------------------------------------------ 调度线程
    def _scheduler_loop(self):
        while not self._stop.is_set():
            try:
                item = self.queue.peek_next_due()
                if item:
                    self._send_one(item)
                else:
                    self._stop.wait(self.cfg.check_interval_s)
            except Exception:
                log.exception("调度线程异常")
                self._stop.wait(1.0)

    def _send_one(self, item):
        text, send_at = item
        target = self.queue.snapshot()["target"]
        if not target:
            self.queue.discard_next()
            return
        try:
            self.api.send_message(target, text)
            self.queue.confirm_sent()
            log.info("已发送到 %s（计划 %s）：%s", target, format_time(send_at), text[:40])
        except ApiError as exc:
            log.error("发送到 %s 失败（%s）：%s", target, exc.code, exc.description)
            self.queue.discard_next()  # 永久错误（无权限/频道不存在等），跳过该条
            self._notify_owner("❌ 发送失败，已跳过该条消息：%s\n原文：%s" % (exc.description, text[:100]))
        except Exception as exc:
            log.warning("发送异常（稍后重试）：%s", exc)

    def _notify_owner(self, text):
        now = time.time()
        if self._owner_chat and now - self._last_notify_at > 10:
            self._last_notify_at = now
            try:
                self.api.send_message(self._owner_chat, text)
            except Exception:
                log.warning("通知发送失败：%s", text)

    # ------------------------------------------------------------------ 更新处理
    def _process_update(self, update):
        msg = update.get("message") or {}
        if not msg:
            return
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        chat_type = chat.get("type")
        text = msg.get("text") or ""
        user_id = (msg.get("from") or {}).get("id")

        if text.startswith("/"):
            self._handle_command(chat_id, chat_type, text, user_id)
            return

        # 非命令消息：只处理私聊（避免群聊内容误入队列）
        if chat_type != "private":
            return
        self._owner_chat = chat_id
        # /setloop 收集状态：下一条文本/文件作为循环内容（不转发）
        if self._loop_collect:
            self._collect_loop_content(chat_id, msg)
            return
        if not self.queue.is_active():
            self.api.send_message(chat_id, "⏸ 当前未开启定时发送。使用 /set <频道ID或@用户名> <间隔> 开始。")
            return
        if msg.get("document"):
            self._queue_document(chat_id, msg["document"])
        elif text:
            self._queue_text(chat_id, text)

    # ------------------------------------------------------------------ 命令
    def _handle_command(self, chat_id, chat_type, text, user_id):
        if self.cfg.allowed_user_ids and user_id not in self.cfg.allowed_user_ids:
            self.api.send_message(chat_id, "❌ 无权使用本机器人。")
            return
        self._owner_chat = chat_id
        cmd = text.split(maxsplit=1)[0].lower().split("@")[0]  # 兼容 /set@BotName 形式
        parts = text.split(maxsplit=2)
        if cmd == "/start":
            self.api.send_message(chat_id, START_TEXT)
        elif cmd in ("/help",):
            self.api.send_message(chat_id, HELP_TEXT)
        elif cmd == "/set":
            self._loop_collect = None  # 换目标时取消循环收集
            self._cmd_set(chat_id, parts, user_id)
        elif cmd == "/setloop":
            self._cmd_setloop(chat_id, parts)
        elif cmd == "/approve":
            self._cmd_approve(chat_id, chat_type, parts, user_id)
        elif cmd == "/cancel":
            self._loop_collect = None
            self.queue.cancel()
            snap = self.queue.snapshot()
            self.api.send_message(
                chat_id,
                "⏹ 已停止接收新消息。已排队的 %d 条消息将继续按时发送。\n使用 /set 可重新开启。"
                % snap["pending_count"],
            )
        elif cmd == "/status":
            self._cmd_status(chat_id)
        else:
            self.api.send_message(chat_id, "未知命令。发送 /help 查看帮助。")

    def _cmd_setloop(self, chat_id, parts):
        """配置循环发送：/setloop <间隔> <限制>，随后收集一条内容（文本/txt，每行一条）。"""
        if len(parts) < 3:
            self.api.send_message(
                chat_id,
                "⚠️ 用法：/setloop <间隔> <限制>\n"
                "限制：times:N（循环 N 次）/ duration:D（时长，如 duration:2h）/ until:T（结束时间，如 until:2026-08-01 12:00）\n"
                "示例：/setloop 5 times:10",
            )
            return
        if not self.queue.snapshot()["target"]:
            self.api.send_message(chat_id, "⚠️ 请先使用 /set <目标> <间隔> 设置转发目标，再配置循环发送。")
            return
        try:
            interval_s = parse_interval(parts[1])
        except ValueError as exc:
            self.api.send_message(chat_id, "⚠️ 间隔无效：%s" % exc)
            return
        try:
            limit_type, limit_value = parse_loop_limit(parts[2])
        except ValueError as exc:
            self.api.send_message(chat_id, "⚠️ 限制参数无效：%s\n示例：/setloop 5 times:10" % exc)
            return
        self._loop_collect = {
            "interval_s": interval_s,
            "limit_type": limit_type,
            "limit_value": limit_value,
            "limit_desc": self._describe_limit(limit_type, limit_value),
        }
        log.info("循环配置就绪：间隔 %s 秒，%s（请求者 %s）", interval_s, self._loop_collect["limit_desc"], chat_id)
        self.api.send_message(
            chat_id,
            "🔁 循环配置就绪：间隔 %s，%s。\n请发送循环内容（文本或 txt 文件，每行一条）。\n发送 /cancel 可取消本次配置。"
            % (format_interval(interval_s), self._loop_collect["limit_desc"]),
        )

    @staticmethod
    def _describe_limit(limit_type, limit_value):
        if limit_type == "times":
            return "循环 %d 次" % limit_value
        if limit_type == "duration":
            return "循环 %s" % format_interval(limit_value)
        return "循环至 %s" % format_time(limit_value)

    def _collect_loop_content(self, chat_id, msg):
        """收集 /setloop 的循环内容并展开为定时序列（一次性消费，防滥用受 MAX_LOOP_ITEMS 约束）。"""
        collect = self._loop_collect
        self._loop_collect = None  # 无论成败，收集状态只消费一次
        if msg.get("document"):
            lines = self._extract_document_lines(chat_id, msg["document"])
            if lines is None:  # 文件过大/下载失败，已回复原因
                return
        else:
            lines = [line.strip() for line in (msg.get("text") or "").splitlines() if line.strip()]
        if not lines:
            self.api.send_message(chat_id, "⚠️ 内容为空，请发送文本或 txt 文件（每行一条）。重新发送 /setloop 再试。")
            return
        if len(lines) > self.cfg.max_file_lines:
            lines = lines[: self.cfg.max_file_lines]
            self.api.send_message(chat_id, "⚠️ 行数超过上限 %d，仅取前 %d 行。" % (self.cfg.max_file_lines, self.cfg.max_file_lines))
        cap = min(self.cfg.max_loop_items, self.cfg.max_queue)
        sequence, rounds, truncated = build_loop_sequence(
            lines, collect["interval_s"], collect["limit_type"], collect["limit_value"], time.time(), cap
        )
        if not sequence:
            self.api.send_message(
                chat_id, "⚠️ 无法生成循环序列（结束时间早于首条发送时间）。请重新 /setloop 配置。"
            )
            return
        loop_desc = "%d 条内容 × %d 轮 = %d 条" % (len(lines), rounds, len(sequence))
        added, first_send_at, full = self.queue.replace_pending(
            sequence,
            collect["interval_s"],
            loop_info={"desc": loop_desc, "total": len(sequence), "interval_s": collect["interval_s"]},
        )
        if added == 0:
            self.api.send_message(chat_id, "⚠️ 队列未激活或已满，循环未生效。")
            return
        snap = self.queue.snapshot()
        reply = "🔁 循环已启动：%s（间隔 %s，%s）。" % (
            loop_desc,
            format_interval(collect["interval_s"]),
            collect["limit_desc"],
        )
        if first_send_at:
            remain = max(0, int(first_send_at - time.time()))
            reply += "\n下一条：%s（约 %s 后）" % (format_time(first_send_at), format_interval(remain))
        if truncated or full:
            reply += "\n⚠️ 已达条数上限，仅生成前 %d 条。" % len(sequence)
        self.api.send_message(chat_id, reply)

    def _cmd_set(self, chat_id, parts, user_id):
        if len(parts) < 3:
            self.api.send_message(
                chat_id,
                "⚠️ 用法：/set <频道/群组/用户> <间隔>\n例如：/set @pics 5（每 5 分钟一条）\n间隔支持后缀 s/m/h/d：30s、5、2h",
            )
            return
        target = parts[1].strip()
        if not (target.startswith("@") or target.lstrip("-").isdigit()):
            self.api.send_message(chat_id, "⚠️ 目标必须是 @用户名 或 数字ID（频道/群组/用户均可）。")
            return
        try:
            interval_s = parse_interval(parts[2])
        except ValueError as exc:
            self.api.send_message(chat_id, "⚠️ 间隔无效：%s\n示例：/set @pics 5（分钟），或 30s / 2h" % exc)
            return

        # 解析目标类型，按防滥用策略分流
        try:
            chat = self.api.get_chat(target)
        except ApiError as exc:
            self.api.send_message(
                chat_id,
                "⚠️ 无法解析目标 %s：%s\n（若目标是私人用户，请先让该用户与 bot 交互；"
                "若是群组/频道，请确认 bot 已被加入且 ID 无误）" % (target, exc.description),
            )
            return
        chat_type = chat.get("type")
        resolved_id = chat["id"]
        log.info("目标 %s 解析为 %s（id=%s），请求者 %s", target, chat_type, resolved_id, user_id)

        if chat_type == "channel":
            self._activate(target, interval_s, chat_id)
        elif chat_type == "private":
            self._set_private(chat_id, target, interval_s)
        else:  # group / supergroup
            self._set_group(chat_id, target, interval_s, resolved_id, user_id)

    def _activate(self, target, interval_s, reply_chat_id):
        """直接启用定时目标（频道 / 已通过验证的群组 / 已开启开关的私人）。"""
        self.queue.set_target(target, interval_s)
        log.info("已启用目标 %s，间隔 %s 秒", target, interval_s)
        self.api.send_message(
            reply_chat_id,
            "✅ 已设置：目标 %s，间隔 %s。\n之后发给我的消息（多行文本按行拆分、上传的文本文件按行拆分）将排队发送。\n查看 /status，停止 /cancel。"
            % (target, format_interval(interval_s)),
        )

    def _set_private(self, reply_chat_id, target, interval_s):
        """私人用户目标：默认禁止（防滥用），需 .env 显式开启。"""
        if not self.cfg.allow_private_targets:
            log.info("拒绝向私人用户 %s 设置定时发送（默认禁止）", target)
            self.api.send_message(
                reply_chat_id,
                "⛔ 已禁止向私人用户定时发送消息（防滥用）。\n如确需启用，请在 .env 中设置 ALLOW_PRIVATE_TARGETS=true 后重启。",
            )
            return
        # getChat 已成功 → 该用户已与 bot 建立会话（Telegram 平台要求用户先交互）
        self._activate(target, interval_s, reply_chat_id)

    def _set_group(self, reply_chat_id, target, interval_s, resolved_id, user_id):
        """群组目标：操作者是群管理员 → 直接启用；否则请求群管理员 /approve 审批。"""
        try:
            admins = self.api.get_chat_administrators(resolved_id)
        except ApiError as exc:
            self.api.send_message(
                reply_chat_id,
                "⚠️ 无法校验群管理员（%s）。\n请先将 bot 加入目标群组，再由群管理员在群内发送 /approve。" % exc.description,
            )
            return
        admin_ids = [a.get("user", {}).get("id") for a in admins]
        if user_id in admin_ids:
            log.info("操作者 %s 是目标群 %s 的管理员，直接启用", user_id, target)
            self._activate(target, interval_s, reply_chat_id)
            return
        # 非管理员 → 登记待审批并通知群内管理员
        self.queue.request_approval(target, resolved_id, interval_s, user_id)
        notice = "📌 收到定时消息配置请求：目标本群，间隔 %s。\n请群管理员在群内发送 /approve 确认（%d 分钟内有效），拒绝则无需操作。" % (
            format_interval(interval_s),
            self.cfg.pending_approval_seconds // 60,
        )
        try:
            self.api.send_message(resolved_id, notice)
            self.api.send_message(
                reply_chat_id,
                "⏳ 你不是该群管理员，已向群内发送审批请求。\n管理员在群内发送 /approve 后即生效（%d 分钟内有效）。" % (self.cfg.pending_approval_seconds // 60),
            )
        except ApiError as exc:
            self.queue.clear_pending_approval()
            self.api.send_message(
                reply_chat_id,
                "⚠️ 无法在群内发送审批通知（%s）。\n请先将 bot 加入目标群组，再由管理员在群内发送 /approve。" % exc.description,
            )

    def _cmd_approve(self, chat_id, chat_type, parts, user_id):
        """群管理员审批：/approve（群内）或 /approve <群组>（任意会话）。"""
        pending = self.queue.get_pending_approval()
        if not pending:
            self.api.send_message(chat_id, "当前没有待审批的群组请求。")
            return
        # 判定要批准的目标
        explicit = parts[1].strip() if len(parts) > 1 else None
        if explicit:
            try:
                target_id = self.api.get_chat(explicit)["id"]
            except ApiError as exc:
                self.api.send_message(chat_id, "⚠️ 无法解析目标：%s" % exc.description)
                return
        elif chat_type in ("group", "supergroup"):
            target_id = chat_id
        else:
            self.api.send_message(
                chat_id, "⚠️ 请在目标群组内发送 /approve，或使用 /approve <群组ID或@用户名>。"
            )
            return
        if str(target_id) != str(pending["chat_id"]):
            self.api.send_message(chat_id, "⚠️ 目标与待审批请求不匹配（待审批目标：%s）。" % pending["target"])
            return
        # 有效期检查
        if time.time() - pending["requested_at"] > self.cfg.pending_approval_seconds:
            self.queue.clear_pending_approval()
            self.api.send_message(chat_id, "⚠️ 该审批请求已过期，请重新 /set 发起。")
            return
        # 管理员身份校验（Telegram 验证）
        try:
            admins = self.api.get_chat_administrators(target_id)
        except ApiError as exc:
            self.api.send_message(
                chat_id, "⚠️ 无法校验群管理员（%s）。请确认 bot 是目标群成员。" % exc.description
            )
            return
        if user_id not in [a.get("user", {}).get("id") for a in admins]:
            log.warning("用户 %s 尝试审批群组 %s 被拒绝（非管理员）", user_id, pending["target"])
            self.api.send_message(chat_id, "⛔ 只有目标群的管理员才能批准该请求。")
            return
        self.queue.set_target(pending["target"], pending["interval_s"])  # 内部会清除待审批
        log.info("群组目标 %s 已由管理员 %s 批准", pending["target"], user_id)
        self.api.send_message(
            chat_id,
            "✅ 已通过群管理员验证，定时目标已启用：%s（间隔 %s）。" % (pending["target"], format_interval(pending["interval_s"])),
        )

    def _cmd_status(self, chat_id):
        snap = self.queue.snapshot()
        pending_approval = snap["pending_approval"]
        if not snap["target"] and not pending_approval:
            self.api.send_message(chat_id, "📋 尚未设置目标。使用 /set <频道/群组/用户> <间隔> 开始。")
            return
        lines = ["📋 当前状态"]
        if snap["target"]:
            lines += [
                "目标：%s" % snap["target"],
                "间隔：%s" % format_interval(snap["interval_s"]),
                "接收：%s" % ("✅ 开启" if snap["active"] else "⏹ 已停止"),
                "待发：%d 条" % snap["pending_count"],
                "下一条：%s" % format_time(snap["next_send_at"]),
            ]
        if snap["loop_info"]:
            loop_interval = snap["loop_info"].get("interval_s") or snap["interval_s"]
            lines.append("循环：%s（间隔 %s）" % (snap["loop_info"]["desc"], format_interval(loop_interval)))
        if pending_approval:
            remain = max(0, int(self.cfg.pending_approval_seconds - (time.time() - pending_approval["requested_at"])))
            lines.append("待审批：%s（%s 后过期，需群管理员 /approve）" % (pending_approval["target"], format_interval(remain)))
        self.api.send_message(chat_id, "\n".join(lines))

    # ------------------------------------------------------------------ 入队
    def _queue_text(self, chat_id, text):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return
        self._queue_and_reply(chat_id, lines)

    def _queue_document(self, chat_id, document):
        lines = self._extract_document_lines(chat_id, document)
        if lines is None:
            return
        if len(lines) > self.cfg.max_file_lines:
            lines = lines[: self.cfg.max_file_lines]
            self.api.send_message(chat_id, "⚠️ 行数超过上限 %d，仅取前 %d 行。" % (self.cfg.max_file_lines, self.cfg.max_file_lines))
        if not lines:
            self.api.send_message(chat_id, "⚠️ 文件中没有可发送的内容。")
            return
        self._queue_and_reply(chat_id, lines)

    def _extract_document_lines(self, chat_id, document):
        """下载并解析文本文件为行列表；失败返回 None（已向用户回复原因）。"""
        file_size = document.get("file_size") or 0
        if file_size > self.cfg.max_file_bytes:
            self.api.send_message(
                chat_id,
                "⚠️ 文件过大（%.1f MB），上限 %.1f MB。"
                % (file_size / 1048576.0, self.cfg.max_file_bytes / 1048576.0),
            )
            return None
        try:
            file_info = self.api.get_file(document["file_id"])
            content = self.api.download_file(file_info["file_path"])
        except Exception as exc:
            log.error("下载文件失败：%s", exc)
            self.api.send_message(chat_id, "❌ 文件下载失败：%s" % exc)
            return None
        return [line.strip() for line in content.decode("utf-8", errors="replace").splitlines() if line.strip()]

    def _queue_and_reply(self, chat_id, lines):
        added, first_send_at, full = self.queue.add_messages(lines)
        if full and added == 0:
            self.api.send_message(chat_id, "⚠️ 队列已满，本次未加入。")
            return
        snap = self.queue.snapshot()
        reply = "📥 已排队 %d 条 → %s（间隔 %s）。" % (added, snap["target"], format_interval(snap["interval_s"]))
        if first_send_at:
            remain = max(0, int(first_send_at - time.time()))
            reply += "\n下一条：%s（约 %s 后）" % (format_time(first_send_at), format_interval(remain))
        reply += "\n当前共 %d 条待发。" % snap["pending_count"]
        if full:
            reply += "\n⚠️ 队列已满，其余内容未加入。"
        self.api.send_message(chat_id, reply)


def main():
    parser = argparse.ArgumentParser(description="SendingTimeBot：零依赖 Telegram 定时消息机器人")
    parser.add_argument("--env", default=".env", help=".env 文件路径（默认 .env）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    cfg = Config(args.env)
    if not cfg.bot_token:
        parser.error("未找到 BOT_TOKEN：请在 %s 中填写（参照 .env.example）" % args.env)

    # 启动时打印生效时区，便于第一时间确认 TZ 是否配置成功
    log.info("本地时区：%s（当前本地时间 %s）", time.strftime("%Z %z"), time.strftime("%Y-%m-%d %H:%M:%S"))

    bot = Bot(cfg)
    try:
        bot.run()
    except KeyboardInterrupt:
        log.info("收到退出信号，正在停止……")
        bot._stop.set()


if __name__ == "__main__":
    main()
