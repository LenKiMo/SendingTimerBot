"""多队列定时发送存储 —— 纯标准库实现，线程安全 + JSON 文件持久化。

架构（多队列并存，多目标并行发送）：
- 每名用户一个独立队列集合，存于 data/state_<user_id>.json
- 集合内以目标为键维护多个并行队列，各自持有独立的定时锚点与节奏
- 「当前队列」指针决定新消息进入哪个队列：
  · /set 新目标 → 新建队列并设为当前（旧队列不受影响，继续并行发送）
  · /set 已存在目标 → 切回该队列（不打断节奏），可选更新间隔
- /cancel 停止当前队列接收；/reset 清空某队列重来
- 群组审批（pending_approval）、循环发送（loop_info）均为队列级状态
- 长轮询 offset 存于 data/meta.json（bot 全局）
- 历史单队列状态（state.json）自动迁移为「遗留队列」，由首位交互的管理员接管

调度语义（与需求一致，逐队列生效）：
- 队列无定时消息时：锚点 = 收到时刻，首条 = 收到时刻 + 间隔
- 队列已有定时消息时：锚点 = 最后一条定时消息的定时时刻，逐条累加
- 队列发空后收到新消息：以收到时刻为基准重新起算（避免旧锚点过期导致瞬间突发发送）
"""

import json
import logging
import os
import threading
import time

log = logging.getLogger("queue")

LEGACY_USER = "__legacy__"

MEDIA_LABELS = {
    "photo": "图片",
    "video": "视频",
    "animation": "GIF",
    "audio": "音频",
    "voice": "语音",
    "sticker": "贴纸",
    "document": "文件",
}


class QueueStore:
    """多用户多队列存储：线程安全 + 原子文件持久化。"""

    def __init__(self, data_dir="data", max_queues_per_user=10, max_pending=10000,
                 idle_seconds=86400, now_fn=time.time):
        self._data_dir = data_dir
        self._max_queues_per_user = max_queues_per_user
        self._max_pending = max_pending
        self._idle_seconds = idle_seconds
        self._now = now_fn
        self._lock = threading.RLock()
        self._users = {}  # user_id(str) -> {"current": target|None, "queues": {target: record}}
        self._meta_path = os.path.join(data_dir, "meta.json")
        self._offset = self._load_meta()
        self._load_all_users()
        self._migrate_legacy()
        self.gc_idle()  # 启动时清理一次

    def _load_all_users(self):
        """启动时预载已有用户状态（data/state_<user_id>.json），供调度与 /status 读取。"""
        try:
            for name in os.listdir(self._data_dir):
                if name.startswith("state_") and name.endswith(".json"):
                    self._user(name[len("state_"):-len(".json")])
        except OSError:
            pass

    # ================================================================== 持久化
    def _user_path(self, user_id):
        return os.path.join(self._data_dir, "state_%s.json" % user_id)

    def _load_meta(self):
        try:
            with open(self._meta_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            offset = data.get("offset")
            return int(offset) if isinstance(offset, int) else None
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

    def _save_meta(self):
        os.makedirs(self._data_dir, exist_ok=True)
        tmp = self._meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"offset": self._offset}, fh, ensure_ascii=False)
        os.replace(tmp, self._meta_path)

    def _clean_record(self, data):
        """校验并规范化一条队列记录。"""
        record = {
            "target": None,
            "display": None,
            "interval_s": None,
            "active": False,
            "pending": [],
            "last_send_at": None,
            "pending_approval": None,
            "loop_info": None,
            "idle_since": None,
        }
        if not isinstance(data, dict):
            return record
        record["target"] = data.get("target")
        # 兼容早期字段名 target_display（两种历史格式均可读取）
        record["display"] = data.get("display") or data.get("target_display")
        interval = data.get("interval_s")
        record["interval_s"] = float(interval) if isinstance(interval, (int, float)) and interval > 0 else None
        record["active"] = bool(data.get("active", False))
        for item in data.get("pending") or []:
            if not isinstance(item, dict) or not isinstance(item.get("send_at"), (int, float)):
                continue
            kind = item.get("kind") or "text"
            payload = item.get("payload")
            if payload is None:
                payload = item.get("text") if kind == "text" else item.get("file_id")
            if payload:
                record["pending"].append({
                    "send_at": float(item["send_at"]),
                    "kind": kind,
                    "payload": str(payload),
                    "caption": item.get("caption"),
                })
        last = data.get("last_send_at")
        record["last_send_at"] = float(last) if isinstance(last, (int, float)) else None
        record["pending_approval"] = self._clean_pending_approval(data.get("pending_approval"))
        record["loop_info"] = self._clean_loop_info(data.get("loop_info"))
        idle_since = data.get("idle_since")
        record["idle_since"] = float(idle_since) if isinstance(idle_since, (int, float)) else None
        return record

    @staticmethod
    def _clean_pending_approval(data):
        if not isinstance(data, dict):
            return None
        interval = data.get("interval_s")
        requested_at = data.get("requested_at")
        interval_s = float(interval) if isinstance(interval, (int, float)) and interval > 0 else None
        requested_at = float(requested_at) if isinstance(requested_at, (int, float)) else None
        if data.get("target") and data.get("chat_id") is not None and interval_s and requested_at is not None:
            return {
                "target": str(data["target"]),
                "chat_id": data["chat_id"],
                "interval_s": interval_s,
                "requested_by": data.get("requested_by"),
                "requested_at": requested_at,
                "display": data.get("display"),
            }
        return None

    @staticmethod
    def _clean_loop_info(data):
        if isinstance(data, dict) and data.get("desc"):
            interval = data.get("interval_s")
            return {
                "desc": str(data["desc"]),
                "total": int(data.get("total", 0)),
                "interval_s": float(interval) if isinstance(interval, (int, float)) else None,
            }
        return None

    def _clean_user(self, data):
        """校验并规范化一个用户集合。"""
        user = {"current": None, "queues": {}}
        if not isinstance(data, dict):
            return user
        queues = data.get("queues") or {}
        if not isinstance(queues, dict):
            queues = {}
        for key, value in queues.items():
            record = self._clean_record(value)
            if record["target"] and record["interval_s"] is not None:
                user["queues"][str(key)] = record
        if not user["queues"]:
            return user
        current = data.get("current")
        if current in user["queues"]:
            user["current"] = current
        else:
            user["current"] = next(iter(user["queues"]))
        return user

    def _user(self, user_id):
        """懒加载用户集合（内存缓存 + 文件读取）。user_id 统一按字符串键。"""
        user_id = str(user_id)
        user = self._users.get(user_id)
        if user is None:
            user = self._clean_user(self._load_file(self._user_path(user_id)))
            self._users[user_id] = user
        return user

    def _load_file(self, path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("状态文件 %s 读取失败（%s），以空状态启动", path, exc)
            return {}

    def _save_user(self, user_id, user):
        """原子写入用户状态文件。遗留队列不落盘（原文件保留至接管）。"""
        if user_id == LEGACY_USER:
            return
        os.makedirs(self._data_dir, exist_ok=True)
        tmp = self._user_path(user_id) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(user, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self._user_path(user_id))

    # ================================================================== 旧版迁移
    def _migrate_legacy(self):
        """旧版单队列 state.json → 遗留队列（由首位交互的管理员接管）。"""
        legacy_path = os.path.join(self._data_dir, "state.json")
        if not os.path.isfile(legacy_path):
            return
        data = self._load_file(legacy_path)
        record = self._clean_record(data)
        if not record["target"]:
            return
        self._users[LEGACY_USER] = {"current": record["target"], "queues": {record["target"]: record}}
        if self._offset is None and isinstance(data.get("offset"), int):
            self._offset = data["offset"]
            self._save_meta()
        log.warning("检测到旧版单队列状态：目标 %s，待发 %d 条，已迁移为遗留队列（将由首位交互的管理员接管）",
                    record["target"], len(record["pending"]))

    def claim_legacy(self, user_id):
        """首位交互的管理员接管遗留队列；返回是否发生接管。"""
        with self._lock:
            legacy = self._users.pop(LEGACY_USER, None)
            if not legacy or not legacy["current"]:
                return False
            user = self._user(user_id)
            target = legacy["current"]
            record = legacy["queues"].get(target)
            if not record:
                return False
            if target not in user["queues"]:
                user["queues"][target] = record
                user["current"] = target
                self._save_user(user_id, user)
                try:
                    os.remove(os.path.join(self._data_dir, "state.json"))
                except OSError:
                    pass
                log.info("旧版队列已由用户 %s 接管（目标 %s，待发 %d 条）", user_id, target, len(record["pending"]))
                return True
        return False

    # ================================================================== 配置与指针
    def set_queue(self, user_id, target, interval_s, display=None):
        """/set：新建队列（设为当前）或切回已存在队列（可选更新间隔）。不打断旧队列。

        返回 (结果, 记录)。结果：created / switched / full（超出每用户队列上限）。
        """
        with self._lock:
            user = self._user(user_id)
            queues = user["queues"]
            if target in queues:
                record = queues[target]
                record["display"] = display
                record["interval_s"] = float(interval_s)
                record["idle_since"] = None  # 切回 = 活跃使用
                result = "switched"
            else:
                if len(queues) >= self._max_queues_per_user:
                    return "full", None
                record = {
                    "target": target,
                    "display": display,
                    "interval_s": float(interval_s),
                    "active": True,
                    "pending": [],
                    "last_send_at": None,
                    "pending_approval": None,
                    "loop_info": None,
                    "idle_since": None,
                }
                queues[target] = record
                result = "created"
            user["current"] = target
            self._save_user(user_id, user)
            return result, record

    def cancel_current(self, user_id):
        """/cancel：停止当前队列接收新消息；已排队消息继续按时发送。"""
        with self._lock:
            user = self._user(user_id)
            target = user.get("current")
            if target and target in user["queues"]:
                record = user["queues"][target]
                record["active"] = False
                record["pending_approval"] = None
                # 无待发即开始计入空闲（供自动清理）
                record["idle_since"] = self._now() if not record["pending"] else None
                self._save_user(user_id, user)
                return record
        return None

    def reset_queue(self, user_id, target):
        """/reset：清空指定队列的待发消息并重新起锚（目标与间隔保留）。"""
        with self._lock:
            user = self._user(user_id)
            record = user["queues"].get(target)
            if record is None:
                return None
            record["pending"] = []
            record["last_send_at"] = None
            record["loop_info"] = None
            record["idle_since"] = None  # 重置 = 活跃使用
            self._save_user(user_id, user)
            return record

    # ================================================================== 入队
    def add_messages(self, user_id, texts):
        """把多条文本排入该用户当前队列（文本条目，每行一条）。"""
        items = [{"kind": "text", "payload": text} for text in texts]
        return self.add_items(user_id, items)

    def add_items(self, user_id, items):
        """把任意类型条目（文本/媒体）排入该用户当前队列。

        items：[{kind, payload, caption}]，kind ∈ text/photo/video/animation/audio/voice/sticker/document。
        返回 (加入条数, 首条时刻, 是否队列满, 当前目标)。
        """
        added = 0
        first_send_at = None
        full = False
        current_target = None
        with self._lock:
            user = self._user(user_id)
            target = user.get("current")
            record = user["queues"].get(target) if target else None
            if record is None or not record["active"] or record["interval_s"] is None:
                return 0, None, False, None
            current_target = target
            anchor = self._next_anchor_locked(record)
            for item in items:
                kind = item.get("kind", "text")
                payload = (item.get("payload") or "").strip() if kind == "text" else (item.get("payload") or "")
                if not payload:
                    continue
                if len(record["pending"]) >= self._max_pending:
                    full = True
                    break
                anchor = anchor + record["interval_s"]
                record["pending"].append({
                    "send_at": anchor,
                    "kind": kind,
                    "payload": payload,
                    "caption": item.get("caption"),
                })
                if first_send_at is None:
                    first_send_at = anchor
                added += 1
            if added:
                record["idle_since"] = None  # 有活动即重置空闲计时
                self._save_user(user_id, user)
        return added, first_send_at, full, current_target

    def _next_anchor_locked(self, record):
        """确定队列下一条消息的基准时刻。

        语义（与需求逐条对应）：
        - 队列有待发消息 → 以最后一条定时消息的定时时刻为锚，逐条累加间隔
        - 队列无待发消息 → 以收到时刻为锚（重新起算），避免旧锚点过期
          导致消息被瞬间突发发送（如 bot 停摆数小时后收到新消息）
        """
        if record["pending"]:
            return record["pending"][-1]["send_at"]  # 有定时消息 → 以最后一条为锚
        return self._now()  # 无定时消息 → 以收到时刻为锚

    def replace_pending(self, user_id, texts, interval_s, loop_info=None):
        """/setloop：清空当前队列并整体排入循环序列（纯文本，锚点沿用最后一条定时时刻）。"""
        items = [{"kind": "text", "payload": text} for text in texts]
        return self.replace_items(user_id, items, interval_s, loop_info)

    def replace_items(self, user_id, items, interval_s, loop_info=None):
        """/setloop：清空当前队列并整体排入循环条目序列（文本/媒体混合，锚点沿用最后一条定时时刻）。"""
        added = 0
        first_send_at = None
        full = False
        with self._lock:
            user = self._user(user_id)
            target = user.get("current")
            record = user["queues"].get(target) if target else None
            if record is None or not record["active"] or interval_s is None or interval_s <= 0:
                return 0, None, False
            record["pending"] = []
            record["loop_info"] = None
            anchor = self._next_anchor_locked(record)
            for item in items:
                kind = item.get("kind", "text")
                payload = (item.get("payload") or "").strip() if kind == "text" else (item.get("payload") or "")
                if not payload:
                    continue
                if len(record["pending"]) >= self._max_pending:
                    full = True
                    break
                anchor = anchor + float(interval_s)
                record["pending"].append({
                    "send_at": anchor,
                    "kind": kind,
                    "payload": payload,
                    "caption": item.get("caption"),
                })
                if first_send_at is None:
                    first_send_at = anchor
                added += 1
            if added:
                record["loop_info"] = loop_info
                record["idle_since"] = None
                self._save_user(user_id, user)
        return added, first_send_at, full

    # ================================================================== 出队（仅调度线程调用）
    def peek_next_due(self, now=None):
        """全局取最早到点的一条 (dict)，保证多队列公平（永不饿死）：send_at 最小者优先。"""
        now = self._now() if now is None else now
        best = None  # (send_at, user_id, target, kind, payload, caption)
        with self._lock:
            for user_id, user in self._users.items():
                for target, record in user["queues"].items():
                    if not record["pending"]:
                        continue
                    item = record["pending"][0]
                    if item["send_at"] <= now and (best is None or item["send_at"] < best[0]):
                        best = (item["send_at"], user_id, target, item["kind"], item["payload"], item["caption"])
        if best is None:
            return None
        return {
            "send_at": best[0],
            "user_id": best[1],
            "target": best[2],
            "kind": best[3],
            "payload": best[4],
            "caption": best[5],
        }

    def confirm_sent(self, user_id, target):
        """发送成功后：移除该队列队首，锚点更新为该条的定时时刻；队列发完清除循环信息。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if record and record["pending"]:
                sent = record["pending"].pop(0)
                record["last_send_at"] = sent["send_at"]
                if not record["pending"]:
                    record["loop_info"] = None
                    if not record["active"]:
                        record["idle_since"] = self._now()  # 发完且已停止 → 开始空闲计时
                self._save_user(user_id, user)

    def discard_next(self, user_id, target):
        """发送永久失败时：丢弃该队列队首，避免死循环卡死队列。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if record and record["pending"]:
                record["pending"].pop(0)
                if not record["pending"]:
                    record["loop_info"] = None
                    if not record["active"]:
                        record["idle_since"] = self._now()
                self._save_user(user_id, user)

    # ================================================================== 群组审批（防滥用）
    def request_approval(self, user_id, target, chat_id, interval_s, requested_by, display=None):
        """非管理员的 /set：登记审批请求（队列以未激活状态登记，审批通过后启用）。"""
        with self._lock:
            user = self._user(user_id)
            queues = user["queues"]
            if target not in queues:
                if len(queues) >= self._max_queues_per_user:
                    return False
                queues[target] = {
                    "target": target,
                    "display": display,
                    "interval_s": float(interval_s),
                    "active": False,
                    "pending": [],
                    "last_send_at": None,
                    "pending_approval": None,
                    "loop_info": None,
                }
            record = queues[target]
            record["display"] = display
            record["interval_s"] = float(interval_s)
            record["active"] = False
            record["idle_since"] = None  # 审批流程中不视为空闲
            record["pending_approval"] = {
                "target": target,
                "chat_id": chat_id,
                "interval_s": float(interval_s),
                "requested_by": requested_by,
                "requested_at": self._now(),
                "display": display,
            }
            self._save_user(user_id, user)
            return True

    def get_pending_approval_by_chat(self, chat_id):
        """按群聊 ID 查找待审批请求，返回 (user_id, target, 请求副本) 或 None。"""
        with self._lock:
            for user_id, user in self._users.items():
                for target, record in user["queues"].items():
                    pending = record.get("pending_approval")
                    if pending and str(pending.get("chat_id")) == str(chat_id):
                        return user_id, target, dict(pending)
        return None

    def get_any_pending_approval(self):
        """返回任意一个待审批请求 (user_id, target, 请求副本)，用于提示目标不匹配；无则 None。"""
        with self._lock:
            for user_id, user in self._users.items():
                for target, record in user["queues"].items():
                    pending = record.get("pending_approval")
                    if pending:
                        return user_id, target, dict(pending)
        return None

    def activate_approved(self, user_id, target):
        """审批通过：启用队列并设为当前。"""
        with self._lock:
            user = self._user(user_id)
            record = user["queues"].get(target)
            if record is None:
                return None
            record["active"] = True
            record["pending_approval"] = None
            record["idle_since"] = None
            user["current"] = target
            self._save_user(user_id, user)
            return record

    def clear_approval(self, user_id, target):
        """审批过期/无法通知等场景：清除待审批；未激活且无待发的空队列一并移除。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if not record:
                return
            record["pending_approval"] = None
            if not record["active"] and not record["pending"]:
                del user["queues"][target]
                if user.get("current") == target:
                    user["current"] = None
            self._save_user(user_id, user)

    # ================================================================== 队列管理（/list /drop /move /edit）
    def get_pending_page(self, user_id, target, page, page_size=20):
        """返回当前队列待发列表页。返回 {"items", "total", "page", "pages"} 或 None（队列不存在）。"""
        with self._lock:
            user = self._user(user_id)
            record = user["queues"].get(target)
            if record is None:
                return None
            total = len(record["pending"])
            pages = max(1, (total + page_size - 1) // page_size)
            page = max(1, min(int(page), pages))
            start = (page - 1) * page_size
            items = []
            for i, item in enumerate(record["pending"][start:start + page_size], start=start + 1):
                items.append({"index": i, "kind": item["kind"], "preview": self._item_preview(item)})
            return {"items": items, "total": total, "page": page, "pages": pages}

    @staticmethod
    def _item_preview(item):
        """条目的列表预览（文本截断 / 媒体标签 + 说明）。"""
        if item["kind"] == "text":
            text = item["payload"]
            return text if len(text) <= 60 else text[:60] + "…"
        label = MEDIA_LABELS.get(item["kind"], item["kind"])
        caption = (item.get("caption") or "").strip()
        if caption:
            caption = caption if len(caption) <= 40 else caption[:40] + "…"
            return "[%s] %s" % (label, caption)
        return "[%s]" % label

    @staticmethod
    def _reanchor_locked(record):
        """删除/移动后压缩队列：保持首条定时时刻，按间隔重建后续定时链。"""
        if not record["pending"] or record["interval_s"] is None:
            return
        head = record["pending"][0]["send_at"]
        for i, item in enumerate(record["pending"][1:], start=1):
            item["send_at"] = head + i * record["interval_s"]

    def remove_at(self, user_id, target, index):
        """删除第 index 条（1-based）。返回 (预览, 剩余条数) 或 None（越界/队列不存在）。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if not record or not 1 <= index <= len(record["pending"]):
                return None
            item = record["pending"].pop(index - 1)
            if not record["pending"]:
                record["last_send_at"] = None  # 手动删空 = 重新起锚
            else:
                self._reanchor_locked(record)
            self._save_user(user_id, user)
            return self._item_preview(item), len(record["pending"])

    def move_item(self, user_id, target, from_index, to_index):
        """把第 from_index 条移动到第 to_index 条之前（均 1-based）。返回剩余条数或 None。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if not record or not 1 <= from_index <= len(record["pending"]):
                return None
            if not 1 <= to_index <= len(record["pending"]):
                return None
            item = record["pending"].pop(from_index - 1)
            record["pending"].insert(to_index - 1, item)
            self._reanchor_locked(record)
            self._save_user(user_id, user)
            return len(record["pending"])

    def edit_item(self, user_id, target, index, new_text):
        """修改第 index 条文本内容（仅 text 类型条目）。返回 (旧预览, 新预览) 或 None。"""
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if not record or not 1 <= index <= len(record["pending"]):
                return None
            item = record["pending"][index - 1]
            if item["kind"] != "text":
                return None
            new_text = new_text.strip()
            if not new_text:
                return None
            old_preview = self._item_preview(item)
            item["payload"] = new_text
            self._save_user(user_id, user)
            return old_preview, self._item_preview(item)

    # ================================================================== 查询
    def update_display(self, user_id, target, display):
        """补全队列记录的显示名（由 bot 在记录缺失显示名时调用）。"""
        if not display:
            return
        with self._lock:
            user = self._users.get(str(user_id))
            record = user["queues"].get(target) if user else None
            if not record:
                return
            record["display"] = display
            if record.get("pending_approval"):
                record["pending_approval"]["display"] = display
            self._save_user(user_id, user)

    def user_active(self, user_id):
        """该用户当前队列是否处于接收状态。"""
        with self._lock:
            user = self._user(user_id)
            if not user or not user.get("current"):
                return False
            record = user["queues"].get(user["current"])
            return bool(record and record["active"])

    def snapshot_user(self, user_id):
        """返回该用户的队列集合快照（供 /status）。"""
        with self._lock:
            user = self._user(user_id)
            if not user or not user["queues"]:
                return {"current": None, "count": 0, "queues": []}
            snapshots = []
            for target, record in user["queues"].items():
                pending = record["pending"]
                snapshots.append({
                    "target": target,
                    "display": record["display"],
                    "interval_s": record["interval_s"],
                    "active": record["active"],
                    "pending_count": len(pending),
                    "next_send_at": pending[0]["send_at"] if pending else None,
                    "last_send_at": record["last_send_at"],
                    "pending_approval": dict(record["pending_approval"]) if record["pending_approval"] else None,
                    "loop_info": dict(record["loop_info"]) if record["loop_info"] else None,
                })
            return {"current": user["current"], "count": len(snapshots), "queues": snapshots}

    # ================================================================== 自动清理（GC）
    def gc_idle(self, now=None, idle_seconds=None):
        """清理长时间空闲的队列与用户状态，保持服务端干净、减少使用痕迹保留。

        规则：队列处于「已停止接收（active=False）且无待发」状态持续超过
        idle_seconds（默认 24 小时）→ 移除该队列记录；用户所有队列被清理后
        删除其状态文件。审批流程中、有待发消息的队列不受影响。
        返回清理的队列数。
        """
        idle_seconds = self._idle_seconds if idle_seconds is None else idle_seconds
        now = self._now() if now is None else now
        cleaned = 0
        with self._lock:
            for user_id in list(self._users.keys()):
                if user_id == LEGACY_USER:
                    continue
                user = self._users[user_id]
                removed_any = False
                for target in list(user["queues"].keys()):
                    record = user["queues"][target]
                    idle_since = record.get("idle_since")
                    if (
                        not record["active"]
                        and not record["pending"]
                        and idle_since is not None
                        and (now - idle_since) >= idle_seconds
                    ):
                        del user["queues"][target]
                        if user.get("current") == target:
                            user["current"] = None
                        removed_any = True
                        cleaned += 1
                        log.info("GC：移除空闲队列 %s（用户 %s）", target, user_id)
                if removed_any:
                    if user["queues"]:
                        self._save_user(user_id, user)
                    else:
                        self._delete_user_file(user_id)
                        self._users.pop(user_id, None)
                        log.info("GC：用户 %s 无队列，已删除状态文件", user_id)
        return cleaned

    def _delete_user_file(self, user_id):
        try:
            os.remove(self._user_path(user_id))
        except OSError:
            pass

    # ================================================================== 轮询 offset（全局）
    def get_offset(self):
        with self._lock:
            return self._offset

    def set_offset(self, offset):
        with self._lock:
            self._offset = int(offset)
            self._save_meta()
