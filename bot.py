"""SendingTimeBot —— 零第三方依赖的 Telegram 定时消息机器人。

用法：python bot.py [--env .env]

流程：
1. 读取配置（.env）→ 自检 getMe → 启动调度线程 → 开始长轮询
2. /set 目标 间隔 → 建立该目标的定时队列（多目标并行；新目标新建队列并设为
   当前，已存在目标切回继续），之后所有私聊文本（多行按行拆分）与上传的文本
   文件（按行拆分）进入当前队列，按间隔定时发送（群组需管理员验证）
3. /setloop 间隔 限制 → 收集循环内容（文本/txt，每行一条），按次数/时长/结束
   时间展开为定时序列发往当前队列的目标
4. /reset 清空指定队列重来；/cancel 停止当前队列接收；已排队消息继续按时发送
5. 每用户队列状态持久化到 data/state_<user_id>.json，轮询 offset 存 meta.json，
   重启不丢不重；旧版单队列状态自动迁移
"""

import argparse
import datetime as dt
import logging
import re
import threading
import time

from config import Config
from queue import LEGACY_USER, QueueStore
from telegram_api import ApiError, TelegramClient

log = logging.getLogger("bot")

INTERVAL_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

START_TEXT = (
    "👋 你好！我是 SendingTimeBot，定时消息机器人。\n\n"
    "我能把发给我的消息按指定间隔定时发送到频道/群组，支持多目标并行，也支持循环发送。\n\n"
    "快速开始：\n"
    "1. /set <目标> <间隔> 设置目标（如 /set @pics 5，可设置多个目标并行发送）\n"
    "2. 之后发给我的文本 / txt 文件将按间隔逐条定时发送到当前目标\n"
    "3. /setloop 可配置循环发送；/status 查看队列；/cancel 停止接收\n\n"
    "发送 /help 查看完整功能菜单。"
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

    items 为循环内容条目：字符串按文本条目处理，或 {kind, payload, caption} 字典
    （支持文本与多媒体混合）。多媒体条目循环时仅重复 file_id 引用，无额外传输。

    - times:N    → 内容完整循环 N 遍，共 len(items)×N 条（可被 cap 截断为整轮）
    - duration:D → 从 start_time 起 D 秒内逐条排布（可半轮）
    - until:T    → 逐条排布直到 T（含边界，可半轮）
    返回 (条目序列, 完整轮数, 是否被上限截断)。
    """
    if not items:
        return [], 0, False
    items = [item if isinstance(item, dict) else {"kind": "text", "payload": item} for item in items]
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


def _chat_display(chat, raw_target):
    """根据 getChat 返回的信息生成目标的可读名称（标题优先，其次 @用户名，最后原样）。"""
    if chat.get("title"):
        return "%s（%s）" % (chat["title"], raw_target)
    if chat.get("username"):
        return "@%s" % chat["username"]
    if chat.get("first_name"):
        name = chat["first_name"]
        if chat.get("last_name"):
            name += " " + chat["last_name"]
        return "%s（%s）" % (name, raw_target)
    return raw_target


class Bot:
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = TelegramClient(cfg.bot_token, api_base=cfg.api_base, max_retries=cfg.max_retries)
        self.store = QueueStore(
            cfg.data_dir,
            max_queues_per_user=cfg.max_queues_per_user,
            max_pending=cfg.max_queue,
        )
        self._stop = threading.Event()
        # /setloop 内容收集状态：user_id -> {interval_s, limit_type, limit_value, limit_desc, items, chat_id, last_activity}
        self._loop_collect = {}
        self._last_notify_at = {}  # user_id -> 失败通知节流时间戳
        self._list_page = {}  # user_id -> /list 上次查看页码（内存态）

    def _help_text(self):
        """完整帮助菜单（动态生成：私人目标文案按当前实例配置分情况显示，不暴露部署细节）。"""
        if self.cfg.allow_private_targets:
            private_line = "· 私人：本实例已开启向私人用户发送（接收者须先与 bot 交互）"
        else:
            private_line = "· 私人：本实例未启用向私人用户发送（如有需要请自行部署实例）"
        return (
            "📖 SendingTimeBot 功能菜单\n\n"
            "▶️ 定时转发\n"
            "/set <目标> <间隔> — 设置目标并开始接收\n"
            "· 频道（@用户名 或 数字ID）：直接生效，需 bot 为频道管理员\n"
            "· 群组：操作者是管理员则直接生效，否则需群内 /approve 审批\n"
            + private_line
            + "\n· 间隔：纯数字 = 分钟，支持 s/m/h/d（如 30s、5、2h）\n"
            "· 新目标 = 新建并行队列；已存在目标 = 切回继续（可选改间隔）\n"
            "· 之后所有私聊文本（每行一条）与 txt 文件（每行一条）进入当前队列\n\n"
            "🔁 循环发送\n"
            "/setloop <间隔> <限制> — 配置循环，随后逐条发送内容收集\n"
            "· 文本消息：每行视为一项；图片/视频/GIF/音频/语音/贴纸/文件：每条一项\n"
            "· 发送 /done 完成收集并开始循环（%d 秒无新内容自动开始）\n"
            "· times:N — 循环 N 次（内容每完整一遍计 1 次）\n"
            "· duration:D — 循环时长（如 duration:2h）\n"
            "· until:T — 循环至结束时间（如 until:2026-08-01 12:00）\n"
            "· 示例：/setloop 5 times:10 → 内容循环 10 遍\n"
            "· 循环发往当前队列的目标；单次上限 5000 条\n\n" % int(self.cfg.loop_collect_timeout)
            + "⏰ 单条定时消息\n"
            "/later <时间> <内容> — 单条消息定时发送到当前目标\n"
            "· 时间：相对时长（10m、2h）或绝对时间（16:30、2026-08-01 12:00）\n"
            "· 由 Telegram 服务器定时发送：客户端可见、可在客户端删除\n"
            "· 局限：每聊天最多 100 条定时消息；bot 无法取消已排定的定时消息\n\n"
            "🛡 群组审批\n"
            "/approve — 群管理员在目标群内发送，批准定时配置请求\n\n"
            "🗂 状态与控制\n"
            "/status — 列出我的全部队列与当前队列\n"
            "/list [页码] — 查看当前队列待发列表（每页 20 条，全局编号，页码写在回复中）\n"
            "/drop <序号> — 删除某条\n"
            "/move <序号> <位置> — 调整顺序（1 = 置顶，最大序号 = 置底）\n"
            "/edit <序号> <新内容> — 修改文本条目内容（媒体条目不支持编辑）\n"
            "/reset [目标] — 清空队列待发并重新起锚（默认当前队列；不影响其它队列）\n"
            "/cancel — 停止当前队列接收新消息（已排队消息继续按时发送）\n"
            "/start — 欢迎与快速开始\n"
            "/help — 本菜单\n\n"
            "ℹ️ 说明\n"
            "· 支持多队列并行：不同目标各自独立排队、按各自间隔发送\n"
            "· 同一时间仅一个「当前队列」接收新消息（由最近一次 /set 决定）\n"
            "· 支持多媒体消息（图片/视频/GIF/音频/语音/贴纸/文件）入队转发，与文本混合排队\n"
            "· /later 与 /set 的区别：/set 是 bot 侧队列（可 /cancel、/reset、循环、多目标并行）；"
            "/later 是 Telegram 原生定时（客户端可见、可手动管理，bot 无法取消）\n"
            "· 接收者必须先与 bot 交互（发送过 /start 或消息），否则 bot 无法向对方发送消息（Telegram 平台限制）"
        )

    # ------------------------------------------------------------------ 运行
    def run(self):
        me = self.api.get_me()
        log.info("已连接 Telegram: @%s (%s)", me.get("username"), me.get("first_name"))
        threading.Thread(target=self._scheduler_loop, name="scheduler", daemon=True).start()
        offset = self.store.get_offset()
        log.info("开始长轮询（offset=%s）", offset)
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
                self.store.set_offset(offset)

    # ------------------------------------------------------------------ 调度线程
    def _scheduler_loop(self):
        last_gc = time.time()
        while not self._stop.is_set():
            try:
                item = self.store.peek_next_due()
                if item:
                    self._send_one(item)
                else:
                    self._stop.wait(self.cfg.check_interval_s)
                # 循环内容收集超时自动开始
                for user_id in list(self._loop_collect.keys()):
                    collect = self._loop_collect.get(user_id)
                    if collect and time.time() - collect.get("last_activity", 0) > self.cfg.loop_collect_timeout:
                        log.info("循环收集超时自动开始（用户 %s，%d 项）", user_id, len(collect["items"]))
                        try:
                            self._finalize_loop_collect(user_id)
                        except Exception:
                            log.exception("自动完成循环收集失败")
                # 周期性自动清理空闲队列（每小时一次）
                if time.time() - last_gc > 3600:
                    self.store.gc_idle()
                    last_gc = time.time()
            except Exception:
                log.exception("调度线程异常")
                self._stop.wait(1.0)

    def _send_one(self, item):
        payload, send_at = item["payload"], item["send_at"]
        kind, caption = item["kind"], item.get("caption")
        target, user_id = item["target"], item["user_id"]
        try:
            if kind == "text":
                self.api.send_message(target, payload)
            else:
                self._send_media(kind, target, payload, caption)
            self.store.confirm_sent(user_id, target)
            log.info("已发送到 %s（计划 %s，%s）：%s", target, format_time(send_at), kind, payload[:40])
        except ApiError as exc:
            log.error("发送到 %s 失败（%s）：%s", target, exc.code, exc.description)
            self.store.discard_next(user_id, target)  # 永久错误（无权限/频道不存在等），跳过该条
            self._notify_owner(user_id, "❌ 发送失败，已跳过该条消息：%s\n原文：%s" % (exc.description, payload[:100]))
        except Exception as exc:
            log.warning("发送异常（稍后重试）：%s", exc)

    def _send_media(self, kind, target, file_id, caption=None):
        """按类型分发媒体发送（caption 截断到 1024 字符，Telegram 平台限制）。"""
        caption = (caption or "")[:1024]
        if kind == "photo":
            self.api.send_photo(target, file_id, caption)
        elif kind == "video":
            self.api.send_video(target, file_id, caption)
        elif kind == "animation":
            self.api.send_animation(target, file_id, caption)
        elif kind == "audio":
            self.api.send_audio(target, file_id, caption)
        elif kind == "voice":
            self.api.send_voice(target, file_id, caption)
        elif kind == "sticker":
            self.api.send_sticker(target, file_id)
        elif kind == "document":
            self.api.send_document(target, file_id, caption)
        else:
            raise ValueError("未知条目类型：%s" % kind)

    def _notify_owner(self, user_id, text):
        if user_id in (None, LEGACY_USER):
            return
        now = time.time()
        if now - self._last_notify_at.get(user_id, 0.0) > 10:
            self._last_notify_at[user_id] = now
            try:
                self.api.send_message(user_id, text)  # 私聊 chat_id == user_id
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
        if chat_type != "private" or not user_id:
            return
        if self.store.claim_legacy(user_id):
            log.info("用户 %s 接管旧版队列", user_id)
        # /setloop 收集状态：下一条文本/文件/媒体追加为循环内容（不转发）
        if user_id in self._loop_collect:
            self._append_loop_items(user_id, chat_id, msg)
            return
        if not self.store.user_active(user_id):
            self.api.send_message(chat_id, "⏸ 当前队列未开启定时发送。使用 /set <目标> <间隔> 开始或切换到其它队列。")
            return
        # 文本类文件 → 按行拆分；其它文件与多媒体 → 作为媒体条目排队
        if msg.get("document") and self._is_text_document(msg["document"]):
            self._queue_document(user_id, chat_id, msg["document"])
            return
        media_item = self._collect_media_item(msg)
        if media_item:
            self._queue_media(user_id, chat_id, media_item)
        elif text:
            self._queue_text(user_id, chat_id, text)

    @staticmethod
    def _is_text_document(document):
        """判断上传文件是否为文本类（按行拆分）；否则作为文件条目转发。"""
        name = (document.get("file_name") or "").lower()
        mime = (document.get("mime_type") or "")
        if mime.startswith("text/"):
            return True
        return any(name.endswith(ext) for ext in (".txt", ".csv", ".md", ".log", ".json", ".tsv", ".yaml", ".yml"))

    def _collect_media_item(self, msg):
        """从消息中提取媒体条目（kind/payload/caption）；纯文本返回 None。"""
        caption = msg.get("caption") or ""
        if msg.get("photo"):
            return {"kind": "photo", "payload": msg["photo"][-1]["file_id"], "caption": caption}
        if msg.get("video"):
            return {"kind": "video", "payload": msg["video"]["file_id"], "caption": caption}
        if msg.get("animation"):
            return {"kind": "animation", "payload": msg["animation"]["file_id"], "caption": caption}
        if msg.get("audio"):
            return {"kind": "audio", "payload": msg["audio"]["file_id"], "caption": caption}
        if msg.get("voice"):
            return {"kind": "voice", "payload": msg["voice"]["file_id"], "caption": caption}
        if msg.get("sticker"):
            return {"kind": "sticker", "payload": msg["sticker"]["file_id"], "caption": ""}
        if msg.get("document"):
            return {"kind": "document", "payload": msg["document"]["file_id"], "caption": caption}
        return None

    # ------------------------------------------------------------------ 命令
    def _handle_command(self, chat_id, chat_type, text, user_id):
        if self.cfg.allowed_user_ids and user_id not in self.cfg.allowed_user_ids:
            self.api.send_message(chat_id, "❌ 无权使用本机器人。")
            return
        if user_id:
            self.store.claim_legacy(user_id)
        cmd = text.split(maxsplit=1)[0].lower().split("@")[0]  # 兼容 /set@BotName 形式
        parts = text.split(maxsplit=2)
        # 循环内容收集中：/done 完成、/cancel 取消（下方统一处理）；其它命令先取消收集再执行
        if user_id in self._loop_collect and cmd not in ("/done", "/cancel", "/setloop", "/help", "/start", "/status"):
            self._loop_collect.pop(user_id, None)
            self.api.send_message(chat_id, "ℹ️ 已取消本次循环收集（内容未保存）。")
        if cmd == "/start":
            self.api.send_message(chat_id, START_TEXT)
        elif cmd in ("/help",):
            self.api.send_message(chat_id, self._help_text())
        elif cmd == "/done":
            self._finalize_loop_collect(user_id, chat_id)
        elif cmd == "/set":
            self._loop_collect.pop(user_id, None)  # 换目标时取消循环收集
            self._cmd_set(user_id, chat_id, parts)
        elif cmd == "/setloop":
            self._cmd_setloop(user_id, chat_id, parts)
        elif cmd == "/later":
            self._cmd_later(user_id, chat_id, parts)
        elif cmd == "/list":
            self._cmd_list(user_id, chat_id, parts)
        elif cmd == "/drop":
            self._cmd_drop(user_id, chat_id, parts)
        elif cmd == "/move":
            self._cmd_move(user_id, chat_id, parts)
        elif cmd == "/edit":
            self._cmd_edit(user_id, chat_id, parts)
        elif cmd == "/reset":
            self._cmd_reset(user_id, chat_id, parts)
        elif cmd == "/approve":
            self._cmd_approve(chat_id, chat_type, parts, user_id)
        elif cmd == "/cancel":
            self._loop_collect.pop(user_id, None)
            self._cmd_cancel(user_id, chat_id)
        elif cmd == "/status":
            self._cmd_status(user_id, chat_id)
        else:
            self.api.send_message(chat_id, "未知命令。发送 /help 查看帮助。")

    # ------------------------------------------------------------------ /set
    def _cmd_set(self, user_id, chat_id, parts):
        if len(parts) < 3:
            self.api.send_message(
                chat_id,
                "⚠️ 用法：/set <频道/群组/用户> <间隔>\n"
                "示例：/set @pics 5（每 5 分钟一条）\n"
                "间隔支持后缀 s/m/h/d：30s、5、2h",
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
                "⚠️ 无法解析目标 %s：%s\n"
                "若目标是私人用户，请先让该用户与 bot 交互；"
                "若是群组/频道，请确认 bot 已被加入且 ID 无误。" % (target, exc.description),
            )
            return
        chat_type = chat.get("type")
        resolved_id = chat["id"]
        display = _chat_display(chat, target)
        log.info("目标 %s 解析为 %s（id=%s），请求者 %s", target, chat_type, resolved_id, user_id)

        if chat_type == "channel":
            self._activate_target(user_id, chat_id, target, interval_s, display)
        elif chat_type == "private":
            self._set_private(user_id, chat_id, target, interval_s, display)
        else:  # group / supergroup
            self._set_group(user_id, chat_id, target, interval_s, resolved_id, display)

    def _activate_target(self, user_id, reply_chat_id, target, interval_s, display=None):
        """直接启用定时目标（频道 / 已通过验证的群组 / 已开启开关的私人）。"""
        result, _record = self.store.set_queue(user_id, target, interval_s, display=display)
        if result == "full":
            self.api.send_message(
                reply_chat_id,
                "⚠️ 已达到每用户队列上限（%d 个目标）。请先用 /reset 清理不需要的队列。" % self.cfg.max_queues_per_user,
            )
            return
        if result == "created":
            log.info("新建队列：目标 %s，间隔 %s 秒（请求者 %s）", target, interval_s, user_id)
            self.api.send_message(
                reply_chat_id,
                "✅ 已创建队列：目标 %s，间隔 %s。\n"
                "后续消息将进入该队列（现有队列继续并行发送）。\n"
                "查看 /status，停止 /cancel。" % (display or target, format_interval(interval_s)),
            )
        else:
            log.info("切回队列：目标 %s，间隔 %s 秒（请求者 %s）", target, interval_s, user_id)
            self.api.send_message(
                reply_chat_id,
                "↩️ 已切回队列：目标 %s，间隔 %s。\n"
                "后续消息将进入该队列（原节奏不受影响）。" % (display or target, format_interval(interval_s)),
            )

    def _set_private(self, user_id, reply_chat_id, target, interval_s, display=None):
        """私人用户目标：默认禁止（防滥用），需部署者在配置中显式开启。"""
        if not self.cfg.allow_private_targets:
            log.info("拒绝向私人用户 %s 设置定时发送（默认禁止）", target)
            self.api.send_message(
                reply_chat_id,
                "⛔ 本实例已禁止向私人用户定时发送。\n"
                "如有需要请自行部署实例。注意：接收者必须先与 bot 交互（发送过 /start 或消息），"
                "否则 bot 无法向对方发送消息（Telegram 平台限制）。",
            )
            return
        # getChat 已成功 → 该用户已与 bot 建立会话（Telegram 平台要求用户先交互）
        self._activate_target(user_id, reply_chat_id, target, interval_s, display=display)

    def _set_group(self, user_id, reply_chat_id, target, interval_s, resolved_id, display=None):
        """群组目标：操作者是群管理员 → 直接启用；否则请求群管理员 /approve 审批。"""
        try:
            admins = self.api.get_chat_administrators(resolved_id)
        except ApiError as exc:
            self.api.send_message(
                reply_chat_id,
                "⚠️ 无法校验群管理员（%s）。\n"
                "请先将 bot 加入目标群组，再由群管理员在群内发送 /approve。" % exc.description,
            )
            return
        admin_ids = [a.get("user", {}).get("id") for a in admins]
        if user_id in admin_ids:
            log.info("操作者 %s 是目标群 %s 的管理员，直接启用", user_id, target)
            self._activate_target(user_id, reply_chat_id, target, interval_s, display=display)
            return
        # 非管理员 → 登记待审批并通知群内管理员
        if not self.store.request_approval(user_id, target, resolved_id, interval_s, user_id, display=display):
            self.api.send_message(
                reply_chat_id,
                "⚠️ 已达到每用户队列上限（%d 个目标）。请先用 /reset 清理不需要的队列。" % self.cfg.max_queues_per_user,
            )
            return
        notice = (
            "📌 收到定时消息配置请求：目标本群（%s），间隔 %s。\n"
            "请群管理员在群内发送 /approve 确认（%d 分钟内有效），拒绝则无需操作。"
            % (display or target, format_interval(interval_s), self.cfg.pending_approval_seconds // 60)
        )
        try:
            self.api.send_message(resolved_id, notice)
            self.api.send_message(
                reply_chat_id,
                "⏳ 你不是该群管理员，已向群内发送审批请求。\n"
                "管理员在群内发送 /approve 后即生效（%d 分钟内有效）。" % (self.cfg.pending_approval_seconds // 60),
            )
        except ApiError as exc:
            self.store.clear_approval(user_id, target)
            self.api.send_message(
                reply_chat_id,
                "⚠️ 无法在群内发送审批通知（%s）。\n"
                "请先将 bot 加入目标群组，再由管理员在群内发送 /approve。" % exc.description,
            )

    def _cmd_approve(self, chat_id, chat_type, parts, user_id):
        """群管理员审批：/approve（群内）或 /approve <群组>（任意会话）。"""
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
        found = self.store.get_pending_approval_by_chat(target_id)
        if not found:
            # 显式指定目标但无匹配待审批：若有其它待审批目标则提示不匹配，否则提示无请求
            any_pending = self.store.get_any_pending_approval() if explicit else None
            if any_pending:
                self.api.send_message(
                    chat_id,
                    "⚠️ 目标与待审批请求不匹配（当前待审批目标：%s）。" % (any_pending[2].get("display") or any_pending[1]),
                )
                return
            self.api.send_message(chat_id, "当前没有待审批的群组请求。")
            return
        owner_user_id, target, pending = found
        # 有效期检查
        if time.time() - pending["requested_at"] > self.cfg.pending_approval_seconds:
            self.store.clear_approval(owner_user_id, target)
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
            log.warning("用户 %s 尝试审批群组 %s 被拒绝（非管理员）", user_id, target)
            self.api.send_message(chat_id, "⛔ 只有目标群的管理员才能批准该请求。")
            return
        self.store.activate_approved(owner_user_id, target)
        display = pending.get("display") or target
        log.info("群组目标 %s 已由管理员 %s 批准（归属用户 %s）", target, user_id, owner_user_id)
        self.api.send_message(
            chat_id,
            "✅ 已通过群管理员验证，定时目标已启用：%s（间隔 %s）。"
            % (display, format_interval(pending["interval_s"])),
        )

    # ------------------------------------------------------------------ /setloop
    def _current_queue(self, user_id):
        """返回该用户当前队列的快照（dict）或 None。"""
        snap = self.store.snapshot_user(user_id)
        if not snap["current"]:
            return None
        return next((q for q in snap["queues"] if q["target"] == snap["current"]), None)

    def _cmd_list(self, user_id, chat_id, parts):
        """查看当前队列待发列表（全局编号分页，页码写在回复中，无需记忆）。"""
        queue = self._current_queue(user_id)
        if queue is None:
            self.api.send_message(chat_id, "📋 你还没有任何队列。使用 /set <目标> <间隔> 开始。")
            return
        page_arg = parts[1].strip() if len(parts) > 1 else ""
        if page_arg.isdigit():
            page = int(page_arg)
        else:
            page = self._list_page.get(user_id, 1)
        result = self.store.get_pending_page(user_id, queue["target"], page, page_size=self.cfg.list_page_size)
        if result is None:
            self.api.send_message(chat_id, "⚠️ 队列不存在。用 /status 查看你的队列。")
            return
        self._list_page[user_id] = result["page"]
        name = queue["display"] or queue["target"]
        lines = ["📋 队列：%s｜共 %d 条｜第 %d/%d 页" % (name, result["total"], result["page"], result["pages"])]
        if not result["items"]:
            lines.append("（队列为空）")
        for item in result["items"]:
            lines.append("%d. %s" % (item["index"], item["preview"]))
        lines.append("")
        next_page = result["page"] + 1 if result["page"] < result["pages"] else 1
        lines.append("输入 /list %d 查看下一页，或 /list 页码 直达（列表随发送实时更新，以本页为准）" % next_page)
        lines.append("操作：/drop 序号｜/move 序号 位置｜/edit 序号 新内容")
        self.api.send_message(chat_id, "\n".join(lines))

    def _cmd_drop(self, user_id, chat_id, parts):
        """删除当前队列第 index 条。"""
        if len(parts) < 2 or not parts[1].isdigit():
            self.api.send_message(chat_id, "⚠️ 用法：/drop <序号>\n示例：/drop 3（删除第 3 条，序号见 /list）")
            return
        queue = self._current_queue(user_id)
        if queue is None:
            self.api.send_message(chat_id, "📋 你还没有任何队列。")
            return
        index = int(parts[1])
        result = self.store.remove_at(user_id, queue["target"], index)
        if result is None:
            self.api.send_message(chat_id, "⚠️ 序号超出范围（当前共 %d 条）。用 /list 查看最新序号。" % queue["pending_count"])
            return
        preview, remaining = result
        log.info("用户 %s 删除队列 %s 第 %d 条", user_id, queue["target"], index)
        self.api.send_message(
            chat_id, "🗑 已删除第 %d 条：%s\n该队列剩 %d 条待发。" % (index, preview, remaining)
        )

    def _cmd_move(self, user_id, chat_id, parts):
        """把第 from 条移动到第 to 位置（1 = 置顶，最大序号 = 置底）。"""
        if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
            self.api.send_message(chat_id, "⚠️ 用法：/move <序号> <位置>\n示例：/move 5 1（第 5 条置顶）、/move 3 12（移到第 12 位）")
            return
        queue = self._current_queue(user_id)
        if queue is None:
            self.api.send_message(chat_id, "📋 你还没有任何队列。")
            return
        remaining = self.store.move_item(user_id, queue["target"], int(parts[1]), int(parts[2]))
        if remaining is None:
            self.api.send_message(chat_id, "⚠️ 序号超出范围（当前共 %d 条）。用 /list 查看最新序号。" % queue["pending_count"])
            return
        log.info("用户 %s 移动队列 %s：%s → %s", user_id, queue["target"], parts[1], parts[2])
        self.api.send_message(chat_id, "↕️ 已调整顺序（当前共 %d 条）。用 /list 确认。" % remaining)

    def _cmd_edit(self, user_id, chat_id, parts):
        """修改第 index 条文本内容（媒体条目不支持编辑）。"""
        if len(parts) < 3 or not parts[1].isdigit():
            self.api.send_message(chat_id, "⚠️ 用法：/edit <序号> <新内容>\n示例：/edit 3 https://新链接")
            return
        queue = self._current_queue(user_id)
        if queue is None:
            self.api.send_message(chat_id, "📋 你还没有任何队列。")
            return
        index = int(parts[1])
        result = self.store.edit_item(user_id, queue["target"], index, parts[2])
        if result is None:
            self.api.send_message(
                chat_id,
                "⚠️ 无法编辑（序号越界、内容为空，或该条为媒体消息——媒体仅支持 /drop 与 /move）。用 /list 查看。",
            )
            return
        old_preview, new_preview = result
        log.info("用户 %s 编辑队列 %s 第 %d 条", user_id, queue["target"], index)
        self.api.send_message(chat_id, "✏️ 已修改第 %d 条：\n%s\n→ %s" % (index, old_preview, new_preview))

    def _cmd_setloop(self, user_id, chat_id, parts):
        """配置循环发送：/setloop <间隔> <限制>，随后逐条收集内容（文本/媒体混合）。"""
        if len(parts) < 3:
            self.api.send_message(
                chat_id,
                "⚠️ 用法：/setloop <间隔> <限制>\n"
                "限制：times:N（循环 N 次）/ duration:D（时长，如 duration:2h）/ until:T（结束时间，如 until:2026-08-01 12:00）\n"
                "示例：/setloop 5 times:10",
            )
            return
        if not self.store.user_active(user_id):
            self.api.send_message(chat_id, "⚠️ 请先使用 /set <目标> <间隔> 设置并激活目标队列，再配置循环发送。")
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
        self._loop_collect[user_id] = {
            "interval_s": interval_s,
            "limit_type": limit_type,
            "limit_value": limit_value,
            "limit_desc": self._describe_limit(limit_type, limit_value),
            "items": [],
            "chat_id": chat_id,
            "last_activity": time.time(),
        }
        log.info("循环配置就绪：间隔 %s 秒，%s（用户 %s）", interval_s, self._loop_collect[user_id]["limit_desc"], user_id)
        self.api.send_message(
            chat_id,
            "🔁 循环配置就绪：间隔 %s，%s。\n"
            "请逐条发送循环内容：文本消息每行一项，图片/视频/GIF/音频/语音/贴纸/文件每条一项。\n"
            "发送 /done 完成收集并开始循环（%d 秒无新内容自动开始）；/cancel 取消。"
            % (format_interval(interval_s), self._loop_collect[user_id]["limit_desc"], self.cfg.loop_collect_timeout),
        )

    @staticmethod
    def _describe_limit(limit_type, limit_value):
        if limit_type == "times":
            return "循环 %d 次" % limit_value
        if limit_type == "duration":
            return "循环 %s" % format_interval(limit_value)
        return "循环至 %s" % format_time(limit_value)

    def _append_loop_items(self, user_id, chat_id, msg):
        """循环内容收集：把本条消息追加为循环条目（文本逐行 / 媒体逐条），防滥用受总条数上限约束。"""
        collect = self._loop_collect.get(user_id)
        if not collect:
            return
        collect["chat_id"] = chat_id
        collect["last_activity"] = time.time()
        if msg.get("document") and self._is_text_document(msg["document"]):
            lines = self._extract_document_lines(chat_id, msg["document"])
            if lines is None:  # 文件过大/下载失败，已回复原因
                return
            new_items = [{"kind": "text", "payload": line} for line in lines]
        elif msg.get("text"):
            new_items = [{"kind": "text", "payload": line} for line in msg["text"].splitlines() if line.strip()]
        else:
            media_item = self._collect_media_item(msg)
            new_items = [media_item] if media_item else []
        if not new_items:
            self.api.send_message(chat_id, "⚠️ 未识别到内容（支持文本 / 图片 / 视频 / GIF / 音频 / 语音 / 贴纸 / 文件）。")
            return
        cap = min(self.cfg.max_loop_items, self.cfg.max_queue)
        room = cap - len(collect["items"])
        if room <= 0:
            self._finalize_loop_collect(user_id)  # 已满，直接开始
            return
        if len(new_items) > room:
            new_items = new_items[:room]
            self.api.send_message(chat_id, "⚠️ 内容已达上限（%d 项），截取后开始循环。" % cap)
            collect["items"].extend(new_items)
            self._finalize_loop_collect(user_id)
            return
        collect["items"].extend(new_items)
        text_n = sum(1 for it in collect["items"] if it["kind"] == "text")
        media_n = len(collect["items"]) - text_n
        self.api.send_message(
            chat_id,
            "✅ 已收集 %d 项（文本 %d / 媒体 %d）。\n"
            "继续发送或 /done 开始循环（%d 秒无新内容自动开始）；/cancel 取消。"
            % (len(collect["items"]), text_n, media_n, self.cfg.loop_collect_timeout),
        )

    def _finalize_loop_collect(self, user_id, chat_id=None):
        """完成收集并展开循环序列（/done、内容已满、超时触发）。"""
        collect = self._loop_collect.pop(user_id, None)
        if not collect:
            return
        chat_id = chat_id or collect.get("chat_id")
        items = collect["items"]
        if not items:
            self.api.send_message(chat_id, "⚠️ 循环内容为空：已取消。请重新 /setloop 配置。")
            return
        cap = min(self.cfg.max_loop_items, self.cfg.max_queue)
        sequence, rounds, truncated = build_loop_sequence(
            items, collect["interval_s"], collect["limit_type"], collect["limit_value"], time.time(), cap
        )
        if not sequence:
            self.api.send_message(
                chat_id, "⚠️ 无法生成循环序列（结束时间早于首条发送时间）。请重新 /setloop 配置。"
            )
            return
        text_n = sum(1 for it in items if it["kind"] == "text")
        media_n = len(items) - text_n
        loop_desc = "%d 项内容 × %d 轮 = %d 条" % (len(items), rounds, len(sequence))
        if media_n:
            loop_desc += "（含 %d 条媒体）" % media_n
        added, first_send_at, full = self.store.replace_items(
            user_id,
            sequence,
            collect["interval_s"],
            loop_info={"desc": loop_desc, "total": len(sequence), "interval_s": collect["interval_s"]},
        )
        if added == 0:
            self.api.send_message(chat_id, "⚠️ 队列未激活或已满，循环未生效。")
            return
        snap = self.store.snapshot_user(user_id)
        current_snap = next((q for q in snap["queues"] if q["target"] == snap["current"]), None)
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
        if current_snap:
            reply += "\n目标：%s" % (current_snap["display"] or current_snap["target"])
        self.api.send_message(chat_id, reply)

    def _cmd_later(self, user_id, chat_id, parts):
        """单条定时消息：/later <时间> <内容> — 插入 Telegram 原生定时队列（客户端可见可管理）。"""
        if len(parts) < 2:
            self.api.send_message(
                chat_id,
                "⚠️ 用法：/later <时间> <内容>\n"
                "时间：相对时长（10m、2h、30）或绝对时间（16:30、2026-08-01 12:00）\n"
                "示例：/later 10m 今晚的公告\n"
                "说明：由 Telegram 服务器定时发送，客户端可见、可在客户端删除；bot 无法取消。",
            )
            return
        snap = self.store.snapshot_user(user_id)
        current_snap = next((q for q in snap["queues"] if q["target"] == snap["current"]), None)
        if current_snap is None:
            self.api.send_message(chat_id, "⚠️ 请先使用 /set <目标> <间隔> 设置目标队列，再使用 /later。")
            return
        now = time.time()
        rest = " ".join(parts[1:]).strip()  # 时间与内容可能含空格（如 2026-08-01 12:00）
        ts = None
        content = None
        # 相对时长（5s / 10m / 2h / 30）+ 内容
        match = re.match(r"^(\d+(?:\.\d+)?[smhd]?)\s+(.+)$", rest, re.S)
        if match:
            try:
                ts = now + parse_interval(match.group(1))
                content = match.group(2).strip()
            except ValueError:
                pass
        # 绝对时间（16:30 / 2026-08-01 12:00[:SS]）+ 内容（含空格日期不受影响）
        if ts is None:
            match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?|\d{1,2}:\d{2})\s+(.+)$", rest, re.S)
            if match:
                try:
                    ts = _parse_until_time(match.group(1))
                    content = match.group(2).strip()
                except ValueError as exc:
                    self.api.send_message(chat_id, "⚠️ 时间无效：%s" % exc)
                    return
        if ts is None or not content:
            self.api.send_message(
                chat_id,
                "⚠️ 无法解析时间与内容。用法：/later <时间> <内容>\n"
                "时间：相对时长（10m、2h、30）或绝对时间（16:30、2026-08-01 12:00）\n"
                "示例：/later 10m 今晚的公告",
            )
            return
        if ts - now < 10:
            self.api.send_message(chat_id, "⚠️ 定时发送需至少 10 秒后（Telegram 平台限制）。")
            return
        if ts - now > 366 * 86400:
            self.api.send_message(chat_id, "⚠️ 定时发送最远支持 366 天（Telegram 平台限制）。")
            return
        target = current_snap["target"]
        try:
            self.api.send_message(target, content, schedule_date=ts)
        except ApiError as exc:
            self.api.send_message(chat_id, "⚠️ 定时发送失败：%s" % exc.description)
            return
        display = current_snap["display"] or target
        log.info("用户 %s 定时消息 → %s（%s）", user_id, target, format_time(ts))
        self.api.send_message(
            chat_id,
            "⏰ 已定时：%s 将于 %s 发送。\n"
            "该消息由 Telegram 服务器定时发送（客户端可见）；如需取消请在客户端操作，bot 无法取消已排定的定时消息。"
            % (display, format_time(ts)),
        )

    # ------------------------------------------------------------------ /reset / /cancel / /status
    def _cmd_reset(self, user_id, chat_id, parts):
        """清空指定队列（默认当前队列）的待发消息并重新起锚。只影响目标队列，不干扰其它队列。"""
        target = parts[1].strip() if len(parts) > 1 else ""
        if not target:
            snap = self.store.snapshot_user(user_id)
            target = snap["current"] if snap["count"] else None
            if not target:
                self.api.send_message(chat_id, "⚠️ 没有可重置的队列。用 /status 查看你的队列，或用 /reset <目标> 指定。")
                return
        record = self.store.reset_queue(user_id, target)
        if record is None:
            self.api.send_message(chat_id, "⚠️ 没有找到目标 %s 的队列。用 /status 查看你的队列。" % target)
            return
        log.info("用户 %s 重置队列 %s", user_id, target)
        self.api.send_message(
            chat_id,
            "🧹 已清空队列 %s 的待发消息并重新起锚（间隔 %s 保留）。\n"
            "之后发送的消息将重新从收到时刻 + 间隔起算。其它队列不受影响。"
            % (record["display"] or target, format_interval(record["interval_s"])),
        )

    def _cmd_cancel(self, user_id, chat_id):
        """停止当前队列接收新消息；已排队消息继续按时发送。"""
        record = self.store.cancel_current(user_id)
        if record is None:
            self.api.send_message(chat_id, "⏹ 当前没有可停止的队列。使用 /set <目标> <间隔> 开始。")
            return
        display = record["display"] or record["target"]
        self.api.send_message(
            chat_id,
            "⏹ 已停止队列 %s 接收新消息。已排队的 %d 条消息将继续按时发送。\n使用 /set 可重新开启。" % (
                display, len(record["pending"]),
            ),
        )

    def _cmd_status(self, user_id, chat_id):
        snap = self.store.snapshot_user(user_id)
        if snap["count"] == 0:
            self.api.send_message(chat_id, "📋 你还没有任何队列。使用 /set <频道/群组/用户> <间隔> 开始。")
            return
        lines = ["📋 我的队列（共 %d 个）" % snap["count"]]
        for index, queue in enumerate(snap["queues"], start=1):
            name = queue["display"] or queue["target"]
            marker = "（当前）" if queue["target"] == snap["current"] else ""
            lines.append("%d. %s%s" % (index, name, marker))
            parts = []
            parts.append("间隔 %s" % format_interval(queue["interval_s"]))
            parts.append("✅ 接收中" if queue["active"] else "⏹ 已停止")
            parts.append("待发 %d 条" % queue["pending_count"])
            if queue["next_send_at"]:
                parts.append("下一条 %s" % format_time(queue["next_send_at"]))
            lines.append("   " + "｜".join(parts))
            if queue["loop_info"]:
                loop_interval = queue["loop_info"].get("interval_s") or queue["interval_s"]
                lines.append("   循环：%s（间隔 %s）" % (queue["loop_info"]["desc"], format_interval(loop_interval)))
            if queue["pending_approval"]:
                remain = max(0, int(self.cfg.pending_approval_seconds - (time.time() - queue["pending_approval"]["requested_at"])))
                lines.append("   待审批：%s 后过期，需群管理员 /approve" % format_interval(remain))
        self.api.send_message(chat_id, "\n".join(lines))

    # ------------------------------------------------------------------ 入队
    def _queue_text(self, user_id, chat_id, text):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return
        self._queue_items(user_id, chat_id, [{"kind": "text", "payload": line} for line in lines])

    def _queue_media(self, user_id, chat_id, media_item):
        self._queue_items(user_id, chat_id, [media_item])

    def _queue_document(self, user_id, chat_id, document):
        lines = self._extract_document_lines(chat_id, document)
        if lines is None:
            return
        if len(lines) > self.cfg.max_file_lines:
            lines = lines[: self.cfg.max_file_lines]
            self.api.send_message(chat_id, "⚠️ 行数超过上限 %d，仅取前 %d 行。" % (self.cfg.max_file_lines, self.cfg.max_file_lines))
        if not lines:
            self.api.send_message(chat_id, "⚠️ 文件中没有可发送的内容。")
            return
        self._queue_items(user_id, chat_id, [{"kind": "text", "payload": line} for line in lines])

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

    def _queue_items(self, user_id, chat_id, items):
        added, first_send_at, full, _target = self.store.add_items(user_id, items)
        if full and added == 0:
            self.api.send_message(chat_id, "⚠️ 队列已满，本次未加入。")
            return
        snap = self.store.snapshot_user(user_id)
        current_snap = next((q for q in snap["queues"] if q["target"] == snap["current"]), None)
        if current_snap is None:
            return
        name = current_snap["display"] or current_snap["target"]
        media_count = sum(1 for it in items if it["kind"] != "text")
        reply = "📥 已排队 %d 条 → %s（间隔 %s）。" % (added, name, format_interval(current_snap["interval_s"]))
        if media_count:
            reply += "\n（含 %d 条图片/视频等媒体消息）" % media_count
        if first_send_at:
            remain = max(0, int(first_send_at - time.time()))
            reply += "\n下一条：%s（约 %s 后）" % (format_time(first_send_at), format_interval(remain))
        reply += "\n该队列当前共 %d 条待发。" % current_snap["pending_count"]
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
