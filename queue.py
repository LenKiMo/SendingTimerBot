"""定时发送队列 —— 纯标准库实现，线程安全 + JSON 文件持久化。

调度语义（与需求逐条对应）：
- 目标频道还没有定时消息时：锚点 = 消息收到时刻，首条消息的发送时刻 = 收到时刻 + 定时间隔
  （例：/set @pics 5 后 10:00 收到链接，第一条于 10:05 发送）
- 目标频道已有定时消息时：锚点 = 最后一条定时消息的发送时刻，新消息逐条累加间隔
- 队列发空后：锚点 = 最后一条已发送消息的"定时时刻"（而非实际发送时刻），
  即使发送发生延迟，后续新消息仍保持既定节奏
- /cancel 只停止接收新消息，已排队的消息继续按计划发送
- /set 换目标时清空旧队列，新目标从收到时刻重新起算

可靠性：
- 每次变更立即原子写入 state.json（临时文件 + os.replace），重启/停容器不丢队列
- 长轮询 offset 同样持久化，重启后不会重复消费更新
"""

import json
import logging
import os
import threading
import time

log = logging.getLogger("queue")


class SendingQueue:
    """线程安全的定时消息队列。"""

    def __init__(self, state_path, max_pending=10000, now_fn=time.time):
        self._state_path = state_path
        self._max_pending = max_pending
        self._now = now_fn
        self._lock = threading.Lock()
        self._state = self._clean_state(self._load())

    # ------------------------------------------------------------------ 持久化
    def _load(self):
        try:
            with open(self._state_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return {}
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("状态文件读取失败（%s），将以空队列启动", exc)
            return {}

    @staticmethod
    def _clean_state(data):
        """校验并规范化加载的状态，防御损坏/手改文件。"""
        state = {
            "target": None,
            "interval_s": None,
            "active": False,
            "pending": [],
            "last_send_at": None,
            "offset": None,
            "pending_approval": None,
            "loop_info": None,
        }
        if not isinstance(data, dict):
            return state
        state["target"] = data.get("target")
        interval = data.get("interval_s")
        state["interval_s"] = float(interval) if isinstance(interval, (int, float)) and interval > 0 else None
        state["active"] = bool(data.get("active", False))
        for item in data.get("pending") or []:
            if (
                isinstance(item, dict)
                and item.get("text") is not None
                and isinstance(item.get("send_at"), (int, float))
            ):
                state["pending"].append({"text": str(item["text"]), "send_at": float(item["send_at"])})
        last = data.get("last_send_at")
        state["last_send_at"] = float(last) if isinstance(last, (int, float)) else None
        offset = data.get("offset")
        state["offset"] = int(offset) if isinstance(offset, int) else None
        pending_approval = data.get("pending_approval")
        if isinstance(pending_approval, dict):
            interval = pending_approval.get("interval_s")
            requested_at = pending_approval.get("requested_at")
            interval_s = float(interval) if isinstance(interval, (int, float)) and interval > 0 else None
            requested_at = float(requested_at) if isinstance(requested_at, (int, float)) else None
            if (
                pending_approval.get("target")
                and pending_approval.get("chat_id") is not None
                and interval_s
                and requested_at is not None
            ):
                state["pending_approval"] = {
                    "target": str(pending_approval["target"]),
                    "chat_id": pending_approval["chat_id"],
                    "interval_s": interval_s,
                    "requested_by": pending_approval.get("requested_by"),
                    "requested_at": requested_at,
                }
        loop_info = data.get("loop_info")
        if isinstance(loop_info, dict) and loop_info.get("desc"):
            state["loop_info"] = {
                "desc": str(loop_info["desc"]),
                "total": int(loop_info.get("total", 0)),
                "interval_s": float(loop_info["interval_s"]) if isinstance(loop_info.get("interval_s"), (int, float)) else None,
            }
        return state

    def _save_locked(self):
        directory = os.path.dirname(self._state_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp = self._state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._state, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, self._state_path)  # 原子替换，避免写一半损坏

    # ------------------------------------------------------------------ 配置
    def set_target(self, target, interval_s):
        """设置目标频道与间隔并开始接收。换目标时清空旧队列、循环与待审批请求，从零开始。"""
        with self._lock:
            self._state["target"] = target
            self._state["interval_s"] = float(interval_s)
            self._state["active"] = True
            self._state["pending"] = []
            self._state["last_send_at"] = None
            self._state["pending_approval"] = None
            self._state["loop_info"] = None
            self._save_locked()

    def cancel(self):
        """停止接收新消息；已排队的消息继续按计划发送。同时清除待审批请求与循环配置。"""
        with self._lock:
            self._state["active"] = False
            self._state["pending_approval"] = None
            self._state["loop_info"] = None
            self._save_locked()

    # ------------------------------------------------------------------ 循环发送（/setloop）
    def replace_pending(self, texts, interval_s, loop_info=None):
        """清空现有队列并整体排入循环序列，锚点沿用最后一条定时时刻（保持节奏）。

        用 /setloop 指定的 interval_s 排布发送时刻（不改动 /set 的默认间隔）。
        loop_info 用于 /status 展示循环配置；队列发完后自动清除。
        返回 (加入条数, 首条发送时刻, 是否因队列满被截断)。
        """
        added = 0
        first_send_at = None
        full = False
        with self._lock:
            if not self._state["active"] or interval_s is None or interval_s <= 0:
                return 0, None, False
            self._state["pending"] = []
            self._state["loop_info"] = None
            anchor = self._next_anchor_locked()
            for text in texts:
                text = text.strip()
                if not text:
                    continue
                if len(self._state["pending"]) >= self._max_pending:
                    full = True
                    break
                anchor = anchor + float(interval_s)
                self._state["pending"].append({"text": text, "send_at": anchor})
                if first_send_at is None:
                    first_send_at = anchor
                added += 1
            if added:
                self._state["loop_info"] = loop_info
                self._save_locked()
        return added, first_send_at, full

    # ------------------------------------------------------------------ 群组审批（防滥用）
    def request_approval(self, target, chat_id, interval_s, requested_by):
        """登记群组审批请求：非管理员的 /set 需目标群管理员 /approve 后才能生效。"""
        with self._lock:
            self._state["pending_approval"] = {
                "target": target,
                "chat_id": chat_id,
                "interval_s": float(interval_s),
                "requested_by": requested_by,
                "requested_at": self._now(),
            }
            self._save_locked()

    def get_pending_approval(self):
        """返回待审批请求副本；无则返回 None。"""
        with self._lock:
            if not self._state["pending_approval"]:
                return None
            return dict(self._state["pending_approval"])

    def clear_pending_approval(self):
        """清除待审批请求（过期/被拒绝/无法通知时）。"""
        with self._lock:
            self._state["pending_approval"] = None
            self._save_locked()

    # ------------------------------------------------------------------ 入队
    def add_messages(self, texts):
        """把多条文本排队（每条间隔递增）。返回 (加入条数, 首条发送时刻, 是否因队列满被拒)。

        - 频道无定时消息 → 首条 = 收到时刻 + 间隔
        - 频道已有定时消息 → 从最后一条定时时刻起逐条累加间隔
        """
        added = 0
        first_send_at = None
        with self._lock:
            if not self._state["active"] or self._state["interval_s"] is None:
                return 0, None, False
            anchor = self._next_anchor_locked()
            for text in texts:
                text = text.strip()
                if not text:
                    continue
                if len(self._state["pending"]) >= self._max_pending:
                    if added:
                        self._save_locked()
                    return added, first_send_at, True
                anchor = anchor + self._state["interval_s"]
                self._state["pending"].append({"text": text, "send_at": anchor})
                if first_send_at is None:
                    first_send_at = anchor
                added += 1
            if added:
                self._save_locked()
        return added, first_send_at, False

    def _next_anchor_locked(self):
        """确定下一条消息的基准时刻。"""
        if self._state["pending"]:
            return self._state["pending"][-1]["send_at"]  # 已有定时消息 → 以最后一条为锚
        if self._state["last_send_at"] is not None:
            return self._state["last_send_at"]  # 队列已空但发送过 → 保持节奏
        return self._now()  # 频道无定时消息 → 以收到时刻为锚

    # ------------------------------------------------------------------ 出队（仅调度线程调用）
    def peek_next_due(self, now=None):
        """查看第一条到点待发的消息 (text, send_at)；未到点返回 None。只读不弹出。"""
        now = self._now() if now is None else now
        with self._lock:
            for item in self._state["pending"]:
                if item["send_at"] <= now:
                    return item["text"], item["send_at"]
            return None

    def confirm_sent(self):
        """发送成功后：移除队首，锚点更新为该条的定时时刻。队列发完时清除循环信息。"""
        with self._lock:
            if self._state["pending"]:
                sent = self._state["pending"].pop(0)
                self._state["last_send_at"] = sent["send_at"]
                if not self._state["pending"]:
                    self._state["loop_info"] = None
                self._save_locked()

    def discard_next(self):
        """发送永久失败时：丢弃队首，避免死循环卡死队列。队列发完时清除循环信息。"""
        with self._lock:
            if self._state["pending"]:
                self._state["pending"].pop(0)
                if not self._state["pending"]:
                    self._state["loop_info"] = None
                self._save_locked()

    # ------------------------------------------------------------------ 其它
    def set_offset(self, offset):
        """持久化长轮询 offset（重启不重复消费更新）。"""
        with self._lock:
            self._state["offset"] = int(offset)
            self._save_locked()

    def get_offset(self):
        with self._lock:
            return self._state["offset"]

    def is_active(self):
        with self._lock:
            return bool(self._state["active"])

    def snapshot(self):
        """返回当前状态快照（供 /status 与调度线程使用）。"""
        with self._lock:
            pending = list(self._state["pending"])
            pending_approval = (
                dict(self._state["pending_approval"]) if self._state["pending_approval"] else None
            )
            loop_info = dict(self._state["loop_info"]) if self._state["loop_info"] else None
        return {
            "target": self._state["target"],
            "interval_s": self._state["interval_s"],
            "active": self._state["active"],
            "pending_count": len(pending),
            "next_send_at": pending[0]["send_at"] if pending else None,
            "last_send_at": self._state["last_send_at"],
            "pending_approval": pending_approval,
            "loop_info": loop_info,
        }
