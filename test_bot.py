"""SendingTimeBot 自动化验证（无需真实 token，可离线运行）。

覆盖：
1. 多队列存储单元测试（注入假时钟）：调度语义、并行队列、当前指针、
   每用户隔离、持久化、审批生命周期、旧版迁移、offset、公平调度、上限、
   自动清理（GC）、多媒体条目、队列管理（list/drop/move/edit）、分页
2. 间隔/循环限制解析与循环序列展开测试（含多媒体条目）
3. 端到端测试：本地伪 Telegram API 服务器 + 真实 bot 主循环：
   - 频道 /set → 排队 → 按间隔发送 → /cancel（回归）
   - 多队列并行、多用户并行
   - 群组管理员验证 / 审批流程（群内 / 显式目标 / 过期 / 不匹配）/ 审批拒绝
   - 私人目标默认禁止与开关启用；白名单拒绝；群聊消息忽略
   - 发送失败跳过与通知；409 冲突停止；队列满拒绝；未知命令
   - `/setloop` 循环发送全流程（文本/媒体/超时/上限/打断）
   - `/later` 原生定时（相对/绝对时间与边界）
   - 队列管理命令（/list 多页、/drop、/move、/edit）
   - `/pace` 已排队消息间隔重排（整批/区间/队尾开区间，参数错误不产生副作用）
   - 命令菜单：启动 setMyCommands 注册（/ 按钮与输入联想）
   - 多媒体转发与 txt 文件按行解析
   - 相册（媒体组）：同批合并为一个相册、分片跨批补拉合并、单张退化单发、超 10 张分批

运行：python test_bot.py
"""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

# 确保可导入同目录模块（兼容从其它目录运行）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot import Bot, build_loop_sequence, parse_interval, parse_loop_limit
from config import Config
from queue import QueueStore


# ======================================================================
# 多队列存储单元测试（假时钟，不依赖真实时间）
# ======================================================================
class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, delta):
        self.t += delta


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.clock = FakeClock(1000.0)  # 收到消息时刻 = 1000
        self.store = QueueStore(self.dir.name, now_fn=self.clock)

    def tearDown(self):
        self.dir.cleanup()

    def test_first_message_anchored_to_receipt_time(self):
        # 需求：目标无定时消息 → 首条 = 收到时刻 + 间隔
        self.store.set_queue(222, "@pics", 300)
        added, first_at, full, target = self.store.add_messages(222, ["链接1"])
        self.assertEqual(added, 1)
        self.assertFalse(full)
        self.assertEqual(target, "@pics")
        self.assertEqual(first_at, 1300.0)  # 1000 + 5*60

    def test_chain_spaced_by_interval(self):
        # 需求：第二条 = 第一条定时时刻 + 间隔，以此类推（FIFO）
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c"])
        item = self.store.peek_next_due(now=2000)
        self.assertEqual((item["payload"], item["send_at"]), ("a", 1300.0))
        self.store.confirm_sent(222, "@pics")
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "b")
        self.store.confirm_sent(222, "@pics")
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "c")

    def test_new_messages_after_drain_anchor_to_receipt_time(self):
        # 需求：队列无定时消息（已发空）→ 新消息以收到时刻为基准重新起算，
        # 避免旧锚点过期导致瞬间突发发送
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a"])
        self.store.confirm_sent(222, "@pics")  # 队列发空（last_send_at = 1300）
        self.clock.advance(10000)  # 11000 时才收到新消息（模拟 bot 停摆）
        added, first_at, _, _ = self.store.add_messages(222, ["d"])
        self.assertEqual(added, 1)
        self.assertEqual(first_at, 11300.0)  # 收到时刻 11000 + 300，而非沿用旧锚 1300+300

    def test_parallel_queues_independent(self):
        # 多队列：/set 新目标不影响旧队列；切回后沿用原节奏
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a"])
        self.store.set_queue(222, "-100999", 60)  # 新建 B，设为当前
        added, first_at, _, target = self.store.add_messages(222, ["b"])
        self.assertEqual(target, "-100999")
        self.assertEqual(first_at, 1060.0)
        result, _ = self.store.set_queue(222, "@pics", 300)  # 切回 A
        self.assertEqual(result, "switched")
        added, first_at, _, target = self.store.add_messages(222, ["c"])
        self.assertEqual(target, "@pics")
        self.assertEqual(first_at, 1600.0)  # A 锚点沿用 1300 + 300
        snap = self.store.snapshot_user(222)
        self.assertEqual(snap["count"], 2)
        self.assertEqual(snap["queues"][0]["pending_count"], 2)  # a, c
        self.assertEqual(snap["queues"][1]["pending_count"], 1)  # b

    def test_switch_updates_interval(self):
        # 切回已存在队列时可更新间隔
        self.store.set_queue(222, "@pics", 300)
        self.store.set_queue(222, "@pics", 60)
        added, first_at, _, _ = self.store.add_messages(222, ["x"])
        self.assertEqual(first_at, 1060.0)

    def test_reset_queue(self):
        # /reset：清空待发并重新起锚
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b"])
        record = self.store.reset_queue(222, "@pics")
        self.assertEqual(record["pending"], [])
        self.assertEqual(self.store.snapshot_user(222)["queues"][0]["pending_count"], 0)
        added, first_at, _, _ = self.store.add_messages(222, ["c"])
        self.assertEqual(first_at, 1300.0)  # 锚点重起：收到时刻 + 间隔

    def test_cancel_current_only(self):
        # /cancel 只停当前队列；其它队列不受影响
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a"])
        self.store.set_queue(222, "-100999", 60)
        self.store.cancel_current(222)
        self.assertFalse(self.store.user_active(222))
        added, _, _, _ = self.store.add_messages(222, ["b"])
        self.assertEqual(added, 0)
        self.store.set_queue(222, "@pics", 300)  # 切回 A
        self.assertTrue(self.store.user_active(222))
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "a")

    def test_per_user_isolation(self):
        # 每用户独立队列集合
        self.store.set_queue(222, "@pics", 300)
        self.store.set_queue(333, "@team", 60)
        self.store.add_messages(222, ["a"])
        self.store.add_messages(333, ["b"])
        self.assertEqual(self.store.snapshot_user(222)["count"], 1)
        self.assertEqual(self.store.snapshot_user(333)["queues"][0]["pending_count"], 1)

    def test_persistence_across_restart(self):
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b"])
        s2 = QueueStore(self.dir.name, now_fn=self.clock)  # 模拟重启
        snap = s2.snapshot_user(222)
        self.assertEqual(snap["count"], 1)
        self.assertEqual(snap["queues"][0]["pending_count"], 2)
        self.assertEqual(s2.peek_next_due(now=2000)["payload"], "a")

    def test_approval_lifecycle(self):
        # 防滥用：审批请求 → 查找 → 通过启用；过期清除空队列
        self.assertTrue(self.store.request_approval(222, "-100555", -100555, 300, 222, display="内部群（-100555）"))
        self.assertFalse(self.store.user_active(222))
        found = self.store.get_pending_approval_by_chat(-100555)
        self.assertEqual(found[0], "222")  # user_id 统一为字符串键
        self.assertEqual(found[1], "-100555")
        self.store.activate_approved(222, "-100555")
        self.assertTrue(self.store.user_active(222))
        self.assertIsNone(self.store.get_pending_approval_by_chat(-100555))
        # 过期清除：未激活且无待发的空队列被移除
        self.assertTrue(self.store.request_approval(222, "-100555", -100555, 300, 222))
        self.store.clear_approval(222, "-100555")
        self.assertEqual(self.store.snapshot_user(222)["count"], 0)

    def test_legacy_migration_and_claim(self):
        # 旧版单队列 state.json → 遗留队列 → 首位交互用户接管
        legacy = {
            "target": "@pics",
            "interval_s": 300,
            "active": True,
            "pending": [{"text": "old", "send_at": 1300.0}],
            "offset": 42,
        }
        with open(os.path.join(self.dir.name, "state.json"), "w", encoding="utf-8") as fh:
            json.dump(legacy, fh)
        store = QueueStore(self.dir.name, now_fn=self.clock)
        self.assertEqual(store.get_offset(), 42)  # offset 一并迁移
        self.assertEqual(store.peek_next_due(now=2000)["payload"], "old")  # 遗留队列可出队
        self.assertTrue(store.claim_legacy(222))
        snap = store.snapshot_user(222)
        self.assertEqual(snap["count"], 1)
        self.assertEqual(snap["current"], "@pics")
        self.assertFalse(os.path.exists(os.path.join(self.dir.name, "state.json")))
        self.assertFalse(store.claim_legacy(222))  # 只能接管一次

    def test_legacy_display_migration(self):
        # 旧版单队列 state.json 的 target_display 字段 → 迁移后保留显示名
        legacy = {
            "target": "@pics",
            "target_display": "图片频道（@pics）",
            "interval_s": 300,
            "active": True,
            "pending": [{"text": "old", "send_at": 1300.0}],
        }
        with open(os.path.join(self.dir.name, "state.json"), "w", encoding="utf-8") as fh:
            json.dump(legacy, fh)
        store = QueueStore(self.dir.name, now_fn=self.clock)
        self.assertTrue(store.claim_legacy(222))
        self.assertEqual(store.snapshot_user(222)["queues"][0]["display"], "图片频道（@pics）")

    def test_update_display_persists(self):
        # 惰性补全：update_display 更新并持久化显示名
        self.store.set_queue(222, "@pics", 300)
        self.store.update_display(222, "@pics", "图片频道（@pics）")
        self.assertEqual(self.store.snapshot_user(222)["queues"][0]["display"], "图片频道（@pics）")
        s2 = QueueStore(self.dir.name, now_fn=self.clock)  # 重启后仍在
        self.assertEqual(s2.snapshot_user(222)["queues"][0]["display"], "图片频道（@pics）")

    def test_offset_meta_persistence(self):
        self.store.set_offset(123)
        s2 = QueueStore(self.dir.name, now_fn=self.clock)
        self.assertEqual(s2.get_offset(), 123)

    def test_peek_fairness_global_min(self):
        # 调度公平：全局最早到点者优先，队列间不饿死
        self.store.set_queue(222, "@pics", 300)
        self.store.set_queue(333, "@team", 60)
        self.store.add_messages(222, ["a"])  # a@1300
        self.store.add_messages(333, ["b"])  # b@1060
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "b")
        self.store.confirm_sent(333, "@team")
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "a")

    def test_max_queues_per_user(self):
        store = QueueStore(self.dir.name, max_queues_per_user=2, now_fn=self.clock)
        store.set_queue(222, "@pics", 300)
        store.set_queue(222, "-100999", 60)
        result, _ = store.set_queue(222, "-100555", 60)
        self.assertEqual(result, "full")

    def test_replace_pending_loop(self):
        # /setloop：清空当前队列、按循环间隔排布、发完清除循环信息
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["x"])
        added, first_at, full = self.store.replace_pending(
            222, ["a", "b"], 60, loop_info={"desc": "2 条内容 × 1 轮 = 2 条", "total": 2, "interval_s": 60}
        )
        self.assertEqual(added, 2)
        self.assertFalse(full)
        self.assertEqual(first_at, 1060.0)
        snap = self.store.snapshot_user(222)["queues"][0]
        self.assertEqual(snap["pending_count"], 2)
        self.assertEqual(snap["loop_info"]["desc"], "2 条内容 × 1 轮 = 2 条")
        self.store.confirm_sent(222, "@pics")
        self.store.confirm_sent(222, "@pics")
        self.assertIsNone(self.store.snapshot_user(222)["queues"][0]["loop_info"])

    def test_blank_lines_ignored(self):
        self.store.set_queue(222, "@pics", 300)
        added, _, _, _ = self.store.add_messages(222, ["a", "", "   ", "b"])
        self.assertEqual(added, 2)

    def test_gc_removes_idle_queue_and_file(self):
        # 自动清理：停止接收且发空超时 → 移除队列记录并删除用户状态文件
        self.store.set_queue(222, "@pics", 300)
        self.store.cancel_current(222)  # 无待发 → 开始空闲计时
        self.clock.advance(90000)  # 25 小时后
        cleaned = self.store.gc_idle()
        self.assertEqual(cleaned, 1)
        self.assertEqual(self.store.snapshot_user(222)["count"], 0)
        self.assertFalse(os.path.exists(os.path.join(self.dir.name, "state_222.json")))

    def test_gc_keeps_active_and_queued_queues(self):
        # 有待发消息 / 仍在接收的队列不受清理影响
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a"])  # 有待发
        self.store.cancel_current(222)  # 停止接收，但队列未发空
        self.store.set_queue(222, "-100999", 60)  # 新队列，接收中
        self.clock.advance(90000)
        cleaned = self.store.gc_idle()
        self.assertEqual(cleaned, 0)
        self.assertEqual(self.store.snapshot_user(222)["count"], 2)

    def test_gc_skips_pending_approval(self):
        # 审批流程中的队列不视为空闲
        self.store.request_approval(222, "-100555", -100555, 300, 222)
        self.clock.advance(90000)
        cleaned = self.store.gc_idle()
        self.assertEqual(cleaned, 0)

    def test_gc_on_startup(self):
        # 启动时扫描并清理过期状态文件
        record = {
            "target": "@pics",
            "interval_s": 300,
            "active": False,
            "pending": [],
            "last_send_at": None,
            "pending_approval": None,
            "loop_info": None,
            "idle_since": -100000.0,  # 早已超过 24 小时空闲
        }
        user = {"current": "@pics", "queues": {"@pics": record}}
        with open(os.path.join(self.dir.name, "state_222.json"), "w", encoding="utf-8") as fh:
            json.dump(user, fh)
        store = QueueStore(self.dir.name, now_fn=self.clock)
        self.assertEqual(store.snapshot_user(222)["count"], 0)
        self.assertFalse(os.path.exists(os.path.join(self.dir.name, "state_222.json")))

    def test_activity_resets_idle_timer(self):
        # 重新活跃（切回/入队）会重置空闲计时
        self.store.set_queue(222, "@pics", 300)
        self.store.cancel_current(222)
        self.clock.advance(86000)  # 接近 24h
        self.store.set_queue(222, "@pics", 300)  # 切回 = 活跃
        self.store.cancel_current(222)
        self.clock.advance(86000)
        cleaned = self.store.gc_idle()
        self.assertEqual(cleaned, 0)  # 距上次空闲计时不足 24h
        self.clock.advance(1000)
        self.assertEqual(self.store.gc_idle(), 1)

    def test_media_items_scheduled_and_persisted(self):
        # 多媒体条目：kind/payload/caption 入队、出队、持久化
        self.store.set_queue(222, "@pics", 300)
        added, first_at, full, _ = self.store.add_items(222, [
            {"kind": "photo", "payload": "FILE_A", "caption": "猫猫图"},
            {"kind": "animation", "payload": "FILE_B", "caption": ""},
            {"kind": "text", "payload": "链接1"},
        ])
        self.assertEqual(added, 3)
        self.assertEqual(first_at, 1300.0)
        item = self.store.peek_next_due(now=2000)
        self.assertEqual(item["kind"], "photo")
        self.assertEqual(item["payload"], "FILE_A")
        self.assertEqual(item["caption"], "猫猫图")
        # 持久化
        s2 = QueueStore(self.dir.name, now_fn=self.clock)
        item = s2.peek_next_due(now=2000)
        self.assertEqual(item["kind"], "photo")
        self.assertEqual(item["payload"], "FILE_A")
        s2.confirm_sent(222, "@pics")
        self.assertEqual(s2.peek_next_due(now=2000)["kind"], "animation")

    def test_album_item_scheduled_and_persisted(self):
        # 相册条目：payload 是多张媒体的列表（不能被字符串化），入队/出队/持久化
        self.store.set_queue(222, "@pics", 300)
        refs = [{"kind": "photo", "file_id": "A"}, {"kind": "photo", "file_id": "B"}]
        added, first_at, full, _ = self.store.add_items(222, [
            {"kind": "album", "payload": refs, "caption": "两图"},
            {"kind": "text", "payload": "链接1"},
        ])
        self.assertEqual(added, 2)
        self.assertEqual(first_at, 1300.0)
        item = self.store.peek_next_due(now=2000)
        self.assertEqual(item["kind"], "album")
        self.assertEqual(item["payload"], refs)
        self.assertEqual(item["caption"], "两图")
        # 持久化：重启后仍是列表，顺序不变（相册在前）
        s2 = QueueStore(self.dir.name, now_fn=self.clock)
        item = s2.peek_next_due(now=2000)
        self.assertEqual(item["kind"], "album")
        self.assertEqual(item["payload"], refs)
        page = s2.get_pending_page(222, "@pics", 1)
        self.assertEqual(page["items"][0]["preview"], "[相册×2（图片）] 两图")
        s2.confirm_sent(222, "@pics")
        self.assertEqual(s2.peek_next_due(now=2000)["payload"], "链接1")

    def test_album_item_rejects_invalid_payload(self):
        # 不足 2 项 / 结构非法的相册负载不入队，避免产生非法的「1 项媒体组」
        self.store.set_queue(222, "@pics", 300)
        added, _, _, _ = self.store.add_items(222, [
            {"kind": "album", "payload": [{"kind": "photo", "file_id": "A"}]},
            {"kind": "album", "payload": "A,B"},
            {"kind": "album", "payload": [{"kind": "photo"}, {"file_id": "B"}]},
            {"kind": "text", "payload": "ok"},
        ])
        self.assertEqual(added, 1)
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "ok")

    def test_album_in_loop_items(self):
        # /setloop 循环序列同样支持相册条目
        self.store.set_queue(222, "@pics", 300)
        refs = [{"kind": "photo", "file_id": "A"}, {"kind": "video", "file_id": "B"}]
        added, _, full = self.store.replace_items(222, [
            {"kind": "album", "payload": refs},
            {"kind": "text", "payload": "L1"},
        ], 60)
        self.assertEqual((added, full), (2, False))
        first = self.store.peek_next_due(now=2000)
        self.assertEqual(first["kind"], "album")
        self.assertEqual(first["payload"], refs)
        self.assertEqual(self.store.get_pending_page(222, "@pics", 1)["items"][0]["preview"],
                         "[相册×2（图片/视频）]")

    def test_remove_at_reanchors(self):
        # /drop：删除后按间隔压缩定时链
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c"])  # 1300/1600/1900
        preview, remaining = self.store.remove_at(222, "@pics", 2)
        self.assertEqual(preview, "b")
        self.assertEqual(remaining, 2)
        item = self.store.peek_next_due(now=2000)
        self.assertEqual(item["payload"], "a")
        self.assertEqual(item["send_at"], 1300.0)
        self.store.confirm_sent(222, "@pics")
        item = self.store.peek_next_due(now=2000)
        self.assertEqual(item["payload"], "c")
        self.assertEqual(item["send_at"], 1600.0)  # 压缩：c 提前到 1300+300

    def test_remove_all_resets_anchor(self):
        # 手动删空 = 重新起锚
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b"])
        self.store.remove_at(222, "@pics", 1)
        self.store.remove_at(222, "@pics", 1)
        self.store.add_messages(222, ["c"])
        self.assertEqual(self.store.peek_next_due(now=2000)["send_at"], 1300.0)

    def test_move_item(self):
        # /move：调整顺序并按新顺序压缩定时链
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c"])  # 1300/1600/1900
        remaining = self.store.move_item(222, "@pics", 1, 3)  # a 移到第 3 位
        self.assertEqual(remaining, 3)
        order = []
        for _ in range(3):
            item = self.store.peek_next_due(now=2500)  # 重锚后末条 2200，需足够大的 now
            order.append(item["payload"])
            self.store.confirm_sent(222, "@pics")
        self.assertEqual(order, ["b", "c", "a"])

    def test_edit_text_item(self):
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["旧链接"])
        old, new = self.store.edit_item(222, "@pics", 1, "新链接")
        self.assertIn("旧链接", old)
        self.assertIn("新链接", new)
        self.assertEqual(self.store.peek_next_due(now=2000)["payload"], "新链接")

    def test_edit_rejects_media(self):
        self.store.set_queue(222, "@pics", 300)
        self.store.add_items(222, [{"kind": "photo", "payload": "FILE_A", "caption": ""}])
        self.assertIsNone(self.store.edit_item(222, "@pics", 1, "x"))

    def test_pending_page_paging(self):
        # /list：分页数学（25 条，每页 20 → 2 页；越界页夹取）
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["m%d" % i for i in range(25)])
        page1 = self.store.get_pending_page(222, "@pics", 1, page_size=20)
        self.assertEqual(page1["total"], 25)
        self.assertEqual(page1["pages"], 2)
        self.assertEqual(len(page1["items"]), 20)
        self.assertEqual(page1["items"][0]["index"], 1)
        self.assertEqual(page1["items"][-1]["index"], 20)
        page2 = self.store.get_pending_page(222, "@pics", 2, page_size=20)
        self.assertEqual(len(page2["items"]), 5)
        self.assertEqual(page2["items"][0]["index"], 21)
        page99 = self.store.get_pending_page(222, "@pics", 99, page_size=20)
        self.assertEqual(page99["page"], 2)  # 越界夹取到末页

    # ------------------------------------------------------------------ /pace 重排已排队消息间隔
    def _slots(self):
        user = self.store._users[str(222)]
        return [it["send_at"] for it in user["queues"]["@pics"]["pending"]]

    def test_reinterval_whole_queue_uniform(self):
        # /pace 整批：队首保持原时刻，其余按新间隔顺延；队列基准间隔同步
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c", "d", "e"])  # 1300,1600,1900,2200,2500
        result = self.store.reinterval(222, "@pics", 1, 5, 600)
        self.assertEqual(result, (5, 1300.0, 3700.0))
        self.assertEqual(self._slots(), [1300.0, 1900.0, 2500.0, 3100.0, 3700.0])
        # 基准间隔同步 → 新入队消息锚在队尾 + 600
        added, first_at, _, _ = self.store.add_messages(222, ["f"])
        self.assertEqual(first_at, 4300.0)
        snap = self.store.snapshot_user(222)
        self.assertEqual(snap["queues"][0]["interval_s"], 600)

    def test_reinterval_range_anchors_on_predecessor(self):
        # /pace X-Y 段内（含段首与前一条的间隔）均按新间隔，段外保持原时刻
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c", "d", "e", "f"])  # 1300..2800
        # 第 3-4 条 → 间隔 120 秒：第 3 条 = 第 2 条(1600) + 120
        result = self.store.reinterval(222, "@pics", 3, 4, 120)
        self.assertEqual(result, (2, 1720.0, 1840.0))
        self.assertEqual(self._slots(), [1300.0, 1600.0, 1720.0, 1840.0, 2500.0, 2800.0])
        # 段外时间未被触碰；基准间隔保持原值（仅整批才同步）
        self.assertEqual(self.store.snapshot_user(222)["queues"][0]["interval_s"], 300)

    def test_reinterval_range_from_head_keeps_head_slot(self):
        # /pace 1-Y：队首原时刻不动，其后按新间隔
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c"])  # 1300,1600,1900
        result = self.store.reinterval(222, "@pics", 1, 2, 30)
        self.assertEqual(result, (2, 1300.0, 1330.0))
        self.assertEqual(self._slots(), [1300.0, 1330.0, 1900.0])

    def test_reinterval_tail_open_range(self):
        # /pace X（单写）= 第 X 条到队尾
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c", "d"])  # 1300,1600,1900,2200
        result = self.store.reinterval(222, "@pics", 3, 4, 600)
        self.assertEqual(self._slots(), [1300.0, 1600.0, 2200.0, 2800.0])

    def test_reinterval_whole_single_item(self):
        # 单条整批：时刻不变，仅同步基准间隔
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a"])
        result = self.store.reinterval(222, "@pics", 1, 1, 60)
        self.assertEqual(result, (1, 1300.0, 1300.0))
        self.assertEqual(self.store.snapshot_user(222)["queues"][0]["interval_s"], 60)

    def test_reinterval_invalid_args(self):
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c"])
        self.assertIsNone(self.store.reinterval(222, "@pics", 2, 1, 60))   # lo > hi
        self.assertIsNone(self.store.reinterval(222, "@pics", 0, 2, 60))   # lo < 1
        self.assertIsNone(self.store.reinterval(222, "@pics", 2, 99, 60))  # hi 越界
        self.assertIsNone(self.store.reinterval(222, "@nope", 1, 3, 60))   # 队列不存在
        self.assertIsNone(self.store.reinterval(222, "@pics", 1, 3, 0))    # 间隔非法
        # 无效调用不产生副作用
        self.assertEqual(self._slots(), [1300.0, 1600.0, 1900.0])

    def test_reinterval_persists_across_reload(self):
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c", "d", "e"])
        self.store.reinterval(222, "@pics", 2, 4, 600)
        store2 = QueueStore(self.dir.name, now_fn=self.clock)
        user = store2._users[str(222)]
        self.assertEqual(
            [it["send_at"] for it in user["queues"]["@pics"]["pending"]],
            [1300.0, 1900.0, 2500.0, 3100.0, 2500.0],
        )

    def test_reinterval_affects_peek_ordering_and_drop_reanchor(self):
        # 重排后调度按新时刻出队；之后的 /drop 按「当前基准间隔（已同步为 600）」压缩重排
        self.store.set_queue(222, "@pics", 300)
        self.store.add_messages(222, ["a", "b", "c", "d"])  # 1300,1600,1900,2200
        self.store.reinterval(222, "@pics", 1, 4, 600)      # 整批 → 1300,1900,2500,3100
        self.assertEqual(self.store.peek_next_due(now=2000)["send_at"], 1300.0)
        self.store.confirm_sent(222, "@pics")  # a 发出
        self.assertEqual(self.store.peek_next_due(now=2000)["send_at"], 1900.0)
        # 删除第 2 条（c@2500）→ 以基准间隔 600 重排：d 从 3100 提前为 2500（若仍用旧 300 则为 2200）
        self.store.remove_at(222, "@pics", 2)
        self.assertEqual(self._slots(), [1900.0, 2500.0])


class IntervalTests(unittest.TestCase):
    def test_minutes_default(self):
        self.assertEqual(parse_interval("5"), 300)

    def test_suffixes(self):
        self.assertEqual(parse_interval("30s"), 30)
        self.assertEqual(parse_interval("2h"), 7200)
        self.assertEqual(parse_interval("1d"), 86400)
        self.assertEqual(parse_interval("5m"), 300)

    def test_invalid(self):
        for bad in ("", "abc", "0", "0.1s", "-5"):
            with self.assertRaises(ValueError):
                parse_interval(bad)


class LoopLimitTests(unittest.TestCase):
    def test_plain_number_is_times(self):
        self.assertEqual(parse_loop_limit("10"), ("times", 10))

    def test_times_forms(self):
        for raw in ("times:5", "次数:5"):
            self.assertEqual(parse_loop_limit(raw), ("times", 5))

    def test_duration(self):
        self.assertEqual(parse_loop_limit("duration:2h"), ("duration", 7200))
        self.assertEqual(parse_loop_limit("时长:90m"), ("duration", 5400))

    def test_until(self):
        limit_type, value = parse_loop_limit("until:2099-01-01 12:00")
        self.assertEqual(limit_type, "until")
        self.assertGreater(value, time.time())

    def test_invalid(self):
        for bad in ("", "abc", "times:x", "times:0", "duration:", "until:不是时间"):
            with self.assertRaises(ValueError):
                parse_loop_limit(bad)


class LoopSequenceTests(unittest.TestCase):
    @staticmethod
    def _payloads(seq):
        return [i["payload"] for i in seq]

    def test_times_expansion(self):
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 60, "times", 3, 1000.0, 10000)
        self.assertEqual(self._payloads(seq), ["a", "b", "a", "b", "a", "b"])
        self.assertEqual(rounds, 3)
        self.assertFalse(truncated)

    def test_media_items_expansion(self):
        # 多媒体条目循环：重复引用 file_id，无需额外传输
        seq, rounds, truncated = build_loop_sequence(
            [{"kind": "photo", "payload": "F1", "caption": "图"}, "链接"], 60, "times", 2, 1000.0, 10000
        )
        self.assertEqual(rounds, 2)
        self.assertFalse(truncated)
        self.assertEqual(self._payloads(seq), ["F1", "链接", "F1", "链接"])
        self.assertEqual(seq[0]["kind"], "photo")
        self.assertEqual(seq[2]["caption"], "图")
        self.assertEqual(seq[1]["kind"], "text")

    def test_duration_half_round(self):
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 2, "duration", 5, 1000.0, 10000)
        self.assertEqual(self._payloads(seq), ["a", "b"])
        self.assertEqual(rounds, 1)
        self.assertFalse(truncated)

    def test_until_equals_duration(self):
        seq, _, _ = build_loop_sequence(["a", "b"], 2, "until", 1005.0, 1000.0, 10000)
        self.assertEqual(self._payloads(seq), ["a", "b"])

    def test_cap_truncates_to_whole_rounds(self):
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 60, "times", 5, 1000.0, 3)
        self.assertEqual(self._payloads(seq), ["a", "b"])
        self.assertEqual(rounds, 1)
        self.assertTrue(truncated)

    def test_cap_smaller_than_one_round(self):
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 60, "times", 5, 1000.0, 1)
        self.assertEqual(seq, [])
        self.assertTrue(truncated)

    def test_empty_items(self):
        seq, rounds, truncated = build_loop_sequence([], 60, "times", 5, 1000.0, 10000)
        self.assertEqual(seq, [])
        self.assertEqual(rounds, 0)

    def test_duration_too_short(self):
        seq, _, _ = build_loop_sequence(["a"], 10, "duration", 5, 1000.0, 10000)
        self.assertEqual(seq, [])


class ConfigTests(unittest.TestCase):
    def test_env_file_parsing(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, ".env")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("# 注释行\nBOT_TOKEN=abc:123\nALLOWED_USER_IDS=1, 2\nMAX_QUEUE=99\n")
            cfg = Config(path)
            self.assertEqual(cfg.bot_token, "abc:123")
            self.assertEqual(cfg.allowed_user_ids, [1, 2])
            self.assertEqual(cfg.max_queue, 99)

    def test_anti_abuse_defaults(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = Config(os.path.join(d, "不存在.env"))
            self.assertFalse(cfg.allow_private_targets)
            self.assertEqual(cfg.pending_approval_seconds, 3600)
            self.assertEqual(cfg.max_queues_per_user, 10)

    def test_env_var_overrides_file(self):
        os.environ["BOT_TOKEN"] = "FROM_ENV"
        try:
            cfg = Config("不存在的文件.env")
            self.assertEqual(cfg.bot_token, "FROM_ENV")
        finally:
            os.environ.pop("BOT_TOKEN", None)


# ======================================================================
# 端到端测试：伪 Telegram API 服务器 + 真实 bot 主循环
# ======================================================================
KNOWN_CHATS = {
    "@pics": {"id": -1001234567890, "type": "channel", "username": "pics", "title": "图片频道"},
    "-1001234567890": {"id": -1001234567890, "type": "channel", "username": "pics", "title": "图片频道"},
    "@team": {"id": -100999, "type": "supergroup", "username": "team", "title": "Team Test"},
    "-100999": {"id": -100999, "type": "supergroup", "username": "team", "title": "Team Test"},
    "-100555": {"id": -100555, "type": "supergroup", "title": "内部群"},
    "-100444": {"id": -100444, "type": "channel", "title": "故障频道"},
    "@other": {"id": -100777, "type": "channel", "username": "other", "title": "另一个频道"},
    "-100777": {"id": -100777, "type": "channel", "username": "other", "title": "另一个频道"},
    "42": {"id": 42, "type": "private", "first_name": "小王"},
}
TARGET_CHAT_IDS = {"@pics", "@team", "@other", "-1001234567890", "-100999", "-100555"}
GROUP_ADMINS = {
    -100999: [{"user": {"id": 222, "first_name": "Admin222"}}],
    -100555: [{"user": {"id": 999, "first_name": "Admin999"}}],
}


class FakeTelegramServer:
    """最小化伪 Telegram Bot API 服务器（内存中记录发送情况）。

    支持：
    - scripted_updates：更新批次，或 ("sleep", 秒数) 延迟批次
    - files：file_id -> bytes（getFile + 文件下载）
    - failing_sends：chat_id(str) -> (错误码, 描述)，sendMessage 对该目标恒失败
    - conflict：getUpdates 恒返回 409（模拟双实例冲突）
    """

    def __init__(self, scripted_updates, files=None, failing_sends=None, conflict=False):
        self.scripted_updates = list(scripted_updates)
        self.files = files or {}
        self.failing_sends = failing_sends or {}
        self.conflict = conflict
        self.sent = []  # 发往目标的记录
        self.replies = []  # 发给用户的回复记录
        self.commands = None  # setMyCommands 最近一次注册参数（命令菜单测试用）
        self.lock = threading.Lock()
        self.server = HTTPServer(("127.0.0.1", 0), self._make_handler())
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    params = json.loads(raw.decode("utf-8"))
                except Exception:
                    params = {}
                method = urlparse(self.path).path.rstrip("/").split("/")[-1]
                result = outer._dispatch(method, params)
                if isinstance(result, tuple) and result and result[0] == "__error__":
                    body = json.dumps({"ok": False, "error_code": result[1], "description": result[2]}).encode("utf-8")
                    self.send_response(result[1])
                else:
                    body = json.dumps({"ok": True, "result": result}).encode("utf-8")
                    self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                # 文件下载：/file/bot<token>/docs/<file_id>
                file_id = urlparse(self.path).path.rstrip("/").rsplit("/", 1)[-1]
                content = outer.files.get(file_id)
                if content is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            def log_message(self, *args):
                pass

        return Handler

    def _dispatch(self, method, params):
        if method == "getMe":
            return {"id": 111111, "username": "testbot", "first_name": "Test"}
        if method == "getUpdates":
            if self.conflict:
                return ("__error__", 409, "Conflict: terminated by other getUpdates request")
            delay = 0.05
            with self.lock:
                if self.scripted_updates:
                    batch = self.scripted_updates.pop(0)
                    if isinstance(batch, tuple) and batch and batch[0] == "sleep":
                        delay = batch[1]
                    else:
                        return batch
            time.sleep(delay if isinstance(delay, (int, float)) else 0.05)
            return []
        if method == "getChat":
            chat = KNOWN_CHATS.get(str(params.get("chat_id")))
            if chat:
                return chat
            return ("__error__", 400, "Bad Request: chat not found")
        if method == "getChatAdministrators":
            admins = GROUP_ADMINS.get(int(params.get("chat_id", 0)))
            if admins is None:
                return ("__error__", 400, "Bad Request: bot is not a member of the chat")
            return admins
        if method == "getFile":
            file_id = params.get("file_id")
            if file_id in self.files:
                return {"file_id": file_id, "file_size": len(self.files[file_id]), "file_path": "docs/" + file_id}
            return ("__error__", 400, "Bad Request: wrong file identifier")
        if method == "setMyCommands":
            with self.lock:
                self.commands = {"commands": params.get("commands"), "scope": params.get("scope")}
            return True
        if method == "sendMessage":
            failure = self.failing_sends.get(str(params.get("chat_id")))
            if failure:
                return ("__error__", failure[0], failure[1])
            record = {
                "chat_id": params.get("chat_id"),
                "text": params.get("text"),
                "at": time.time(),
                "schedule_date": params.get("schedule_date"),
            }
            return self._record_send(record)
        if method == "sendMediaGroup":
            try:
                media = json.loads(params.get("media") or "[]")
            except Exception:
                media = []
            record = {
                "chat_id": params.get("chat_id"),
                "method": method,
                "media": media,
                "file_id": ",".join(str(m.get("media")) for m in media),
                "caption": (media[0].get("caption") if media else "") or "",
                "at": time.time(),
            }
            return self._record_send(record)
        if method in ("sendPhoto", "sendVideo", "sendAnimation", "sendAudio", "sendVoice", "sendSticker", "sendDocument"):
            payload_key = {"sendPhoto": "photo", "sendVideo": "video", "sendAnimation": "animation",
                           "sendAudio": "audio", "sendVoice": "voice", "sendSticker": "sticker",
                           "sendDocument": "document"}[method]
            record = {
                "chat_id": params.get("chat_id"),
                "method": method,
                "file_id": params.get(payload_key),
                "caption": params.get("caption") or "",
                "at": time.time(),
            }
            return self._record_send(record)
        return ("__error__", 400, "Bad Request: unknown method %s" % method)

    def _record_send(self, record):
        with self.lock:
            if str(record["chat_id"]) in TARGET_CHAT_IDS:
                self.sent.append(record)
            else:
                self.replies.append(record)
        return {"message_id": len(self.sent) + len(self.replies)}

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=5)


def make_update(update_id, chat_id, chat_type, user_id, text=None, photo=None, animation=None,
                video=None, document=None, caption=None, mime_type="application/octet-stream",
                file_name="data.bin", media_group_id=None):
    msg = {
        "message_id": update_id,
        "from": {"id": user_id, "first_name": "Tester"},
        "chat": {"id": chat_id, "type": chat_type},
    }
    if text is not None:
        msg["text"] = text
    if caption:
        msg["caption"] = caption
    if photo:
        msg["photo"] = [{"file_id": photo + "_S", "width": 100, "height": 100},
                        {"file_id": photo, "width": 800, "height": 800}]
    if animation:
        msg["animation"] = {"file_id": animation}
    if video:
        msg["video"] = {"file_id": video}
    if document:
        msg["document"] = {"file_id": document, "file_name": file_name, "mime_type": mime_type}
    if media_group_id:
        msg["media_group_id"] = media_group_id
    return {"update_id": update_id, "message": msg}


_ENV_KEYS = ("BOT_TOKEN", "API_BASE_URL", "CHECK_INTERVAL_S", "DATA_DIR", "ALLOWED_USER_IDS", "NO_PROXY",
             "ALLOW_PRIVATE_TARGETS", "LOOP_COLLECT_TIMEOUT", "MAX_QUEUE", "PENDING_APPROVAL_SECONDS",
             "ALBUM_GRACE_S")


def _record_text(r):
    """把伪服务器的发送记录转为文本表示（文本消息原文 / 媒体记录标签）。"""
    if "text" in r:
        return r["text"]
    if r.get("method") == "sendMediaGroup":
        return "[sendMediaGroup:%d]" % len(r.get("media") or [])
    return "[%s:%s]" % (r.get("method", "media"), r.get("file_id", ""))


class BotHarness:
    """端到端测试脚手架：伪服务器 + 环境变量 + 真实 bot 主循环。"""

    def __init__(self, updates, temp_dir, allow_private=False, extra_env=None, files=None, failing_sends=None, conflict=False, data_dir=None):
        for key in _ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "127.0.0.1,localhost"
        self.fake = FakeTelegramServer(updates, files=files, failing_sends=failing_sends, conflict=conflict)
        os.environ["BOT_TOKEN"] = "TEST:TOKEN"
        os.environ["API_BASE_URL"] = "http://127.0.0.1:%d" % self.fake.port
        os.environ["CHECK_INTERVAL_S"] = "0.05"
        os.environ["DATA_DIR"] = data_dir or temp_dir
        os.environ["ALLOWED_USER_IDS"] = ""
        if allow_private:
            os.environ["ALLOW_PRIVATE_TARGETS"] = "true"
        if extra_env:
            os.environ.update(extra_env)
        self.bot = Bot(Config(".env"))
        self._thread = threading.Thread(target=self.bot.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.bot._stop.set()
        self._thread.join(timeout=5)  # 等待 bot 线程退出，避免清理临时目录时仍有写盘
        self.fake.stop()

    def wait_for(self, predicate, timeout=15):
        """轮询直到 predicate(sent_texts, reply_texts) 为真（不持有锁调用回调）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.fake.lock:
                sent = [_record_text(r) for r in self.fake.sent]
                replies = [_record_text(r) for r in self.fake.replies]
            if predicate(sent, replies):
                return True
            time.sleep(0.1)
        return False

    @property
    def sent(self):
        with self.fake.lock:
            return list(self.fake.sent)

    @property
    def replies(self):
        with self.fake.lock:
            return list(self.fake.replies)

    def reply_texts(self):
        return [_record_text(r) for r in self.replies]

    def sent_to(self, chat_id):
        return [_record_text(r) for r in self.sent if str(r["chat_id"]) == str(chat_id)]


class EndToEndTests(unittest.TestCase):
    def _run(self, updates, allow_private=False, extra_env=None, files=None, failing_sends=None, conflict=False, data_dir=None):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        harness = BotHarness(updates, tmp.name, allow_private=allow_private, extra_env=extra_env,
                             files=files, failing_sends=failing_sends, conflict=conflict, data_dir=data_dir)
        self.addCleanup(harness.stop)
        for key in _ENV_KEYS:
            self.addCleanup(os.environ.pop, key, None)
        return harness

    def test_display_healing_for_legacy_records(self):
        # 历史/迁移记录缺失显示名 → /status 时惰性 getChat 补全并持久化
        record = {
            "target": "@pics", "display": None, "interval_s": 300, "active": True,
            "pending": [], "last_send_at": None, "pending_approval": None,
            "loop_info": None, "idle_since": None,
        }
        user = {"current": "@pics", "queues": {"@pics": record}}
        data_dir = tempfile.TemporaryDirectory()
        self.addCleanup(data_dir.cleanup)
        with open(os.path.join(data_dir.name, "state_222.json"), "w", encoding="utf-8") as fh:
            json.dump(user, fh)
        updates = [[make_update(1, 777, "private", 222, "/status")]]
        harness = self._run(updates, data_dir=data_dir.name)
        self.assertTrue(harness.wait_for(lambda s, r: any("图片频道（@pics）" in t for t in r)))
        # 补全已持久化
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["display"], "图片频道（@pics）")

    def test_format_interval_large_seconds(self):
        # 大秒数显示为「小时 分 秒」组合（13003 秒 → 3 小时 36 分 43 秒）
        from bot import format_interval
        self.assertEqual(format_interval(13003), "3 小时 36 分 43 秒")
        self.assertEqual(format_interval(0), "0 秒")
        self.assertEqual(format_interval(60), "1 分")
        self.assertEqual(format_interval(3600), "1 小时")

    def test_full_flow_channel(self):
        # 回归：频道 /set → 排队 → 按间隔发送 → /cancel 停止接收
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "第一条")],
            [make_update(3, 777, "private", 222, "a\nb\nc")],
            [make_update(4, 777, "private", 222, "/cancel")],
            [make_update(5, 777, "private", 222, "不应发送")],
        ]
        harness = self._run(updates)
        started = time.time()
        self.assertTrue(harness.wait_for(lambda s, r: len(s) >= 4), "应发出 4 条消息")

        sent = harness.sent
        self.assertEqual([x["text"] for x in sent], ["第一条", "a", "b", "c"])
        self.assertTrue(all(x["chat_id"] == "@pics" for x in sent), "应全部发往 @pics")
        times = [x["at"] for x in sent]
        self.assertGreaterEqual(times[0] - started, 0.7, "首条应在接收后约 1 秒发出")
        for i in range(1, len(times)):
            self.assertGreaterEqual(times[i] - times[i - 1], 0.7, "相邻消息应间隔约 1 秒")

        # /cancel 后不再接收新消息
        time.sleep(2.0)
        self.assertNotIn("不应发送", [x["text"] for x in harness.sent])

        joined = "\n".join(harness.reply_texts())
        self.assertIn("已排队", joined)
        self.assertIn("图片频道（@pics）", joined)

    def test_multi_queue_parallel(self):
        # 多队列：两个目标并行发送，互不影响
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "pa")],
            [make_update(3, 777, "private", 222, "/set @team 1s")],
            [make_update(4, 777, "private", 222, "tb")],
            [make_update(5, 777, "private", 222, "/status")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "pa" in s and "tb" in s), "两个队列都应发出消息")
        self.assertIn("pa", harness.sent_to("@pics"))
        self.assertIn("tb", harness.sent_to("@team"))
        self.assertNotIn("tb", harness.sent_to("@pics"))
        snap = harness.bot.store.snapshot_user(222)
        self.assertEqual(snap["count"], 2)
        # /status 列出两个队列并标记当前（最后一次 /set 的 @team 为当前）
        self.assertTrue(harness.wait_for(lambda s, r: any("我的队列（共 2 个）" in t for t in r)))
        joined = "\n".join(harness.reply_texts())
        self.assertIn("图片频道（@pics）", joined)
        self.assertIn("Team Test（@team）（当前）", joined)

    def test_reset_command(self):
        # /reset：清空待发并重新起锚
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "a")],
            [make_update(3, 777, "private", 222, "b")],
            [make_update(4, 777, "private", 222, "/reset @pics")],
            [make_update(5, 777, "private", 222, "/status")],
            [make_update(6, 777, "private", 222, "c")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已清空队列" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: "c" in s), "重置后新消息应重新入队发送")
        self.assertNotIn("a", [x["text"] for x in harness.sent])
        self.assertNotIn("b", [x["text"] for x in harness.sent])
        self.assertTrue(any("待发 0 条" in t for t in harness.reply_texts()))

    def test_reset_without_arg_resets_current_queue(self):
        # /reset 无参 = 重置当前队列（且不影响其它队列）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "pa")],
            [make_update(3, 777, "private", 222, "/set @team 1s")],
            [make_update(4, 777, "private", 222, "tb")],
            [make_update(5, 777, "private", 222, "/reset")],
            [make_update(6, 777, "private", 222, "tc")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已清空队列 Team Test" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: "tc" in s))
        # 仅重置了当前队列 @team：@pics 的 pa 仍然按原节奏发出
        self.assertIn("pa", harness.sent_to("@pics"))
        self.assertNotIn("tb", harness.sent_to("@team"))
        self.assertIn("tc", harness.sent_to("@team"))

    def test_later_schedules_single_message(self):
        # /later：单条消息经 Telegram 原生定时发送（schedule_date），不进入 bot 队列
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/later 30s 今晚的公告")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已定时" in t for t in r)))
        sent = [x for x in harness.sent if x["text"] == "今晚的公告"]
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["chat_id"], "@pics")
        self.assertIsNotNone(sent[0]["schedule_date"], "应携带 schedule_date 走 Telegram 原生定时")
        self.assertGreaterEqual(sent[0]["schedule_date"], time.time() + 20, "定时时间应在未来")
        # 不进入 bot 侧队列
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["pending_count"], 0)
        joined = "\n".join(harness.reply_texts())
        self.assertIn("客户端", joined)  # 说明客户端可见、bot 无法取消

    def test_later_requires_target(self):
        # /later 需先设置目标
        updates = [[make_update(1, 777, "private", 222, "/later 5s hi")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("请先使用 /set" in t for t in r)))

    def test_media_forwarding(self):
        # 多媒体：图片（带说明）/GIF/非文本文件入队并按类型发送；txt 文件按行拆分后以文本发送
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, photo="FILE_PHOTO_1", caption="猫猫图")],
            [make_update(3, 777, "private", 222, animation="FILE_GIF_1")],
            [make_update(4, 777, "private", 222, document="FILE_ZIP_1", file_name="data.zip", mime_type="application/zip")],
            [make_update(5, 777, "private", 222, document="FILE_TXT_1", file_name="links.txt", mime_type="text/plain")],
        ]
        harness = self._run(updates, files={"FILE_TXT_1": b"F1\nF2\nF3\n"})
        # 图片 + GIF + 文件转发（3 条媒体）
        self.assertTrue(harness.wait_for(lambda s, r: any("sendPhoto:" in x for x in s)))
        self.assertTrue(harness.wait_for(lambda s, r: any("sendAnimation:" in x for x in s)))
        self.assertTrue(harness.wait_for(lambda s, r: any("sendDocument:" in x for x in s)))
        photos = [x for x in harness.sent if x.get("method") == "sendPhoto"]
        self.assertEqual(len(photos), 1)
        self.assertEqual(photos[0]["file_id"], "FILE_PHOTO_1")  # 最大尺寸
        self.assertEqual(photos[0]["caption"], "猫猫图")
        gifs = [x for x in harness.sent if x.get("method") == "sendAnimation"]
        self.assertEqual(gifs[0]["file_id"], "FILE_GIF_1")
        docs = [x for x in harness.sent if x.get("method") == "sendDocument"]
        self.assertEqual(docs[0]["file_id"], "FILE_ZIP_1")  # 非文本文件 → 文件转发
        # txt 文件经 getFile+下载按行拆分 → 以文本逐条发送
        self.assertTrue(harness.wait_for(lambda s, r: "F1" in s and "F2" in s and "F3" in s))
        sent_texts = harness.sent_to("@pics")
        for line in ("F1", "F2", "F3"):
            self.assertIn(line, sent_texts)
        joined = "\n".join(harness.reply_texts())
        self.assertIn("已排队", joined)

    def test_send_failure_skips_and_notifies(self):
        # 发送失败：永久错误 → 跳过该条并通知用户，队列继续
        updates = [
            [make_update(1, 777, "private", 222, "/set -100444 1s")],
            [make_update(2, 777, "private", 222, "x1")],
            [make_update(3, 777, "private", 222, "x2")],
        ]
        harness = self._run(updates, failing_sends={"-100444": (403, "Forbidden: bot is not a member of the channel chat")})
        self.assertTrue(harness.wait_for(lambda s, r: any("发送失败" in t for t in r)), "应通知用户发送失败")
        # 两条失败消息都被丢弃，队列清空
        deadline = time.time() + 10
        while time.time() < deadline:
            snap = harness.bot.store.snapshot_user(222)
            if snap["queues"] and snap["queues"][0]["pending_count"] == 0:
                break
            time.sleep(0.1)
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["pending_count"], 0)
        # 没有任何消息成功发往故障频道
        self.assertEqual(harness.sent_to("-100444"), [])

    def test_whitelist_rejects_others(self):
        # 白名单：非白名单用户被拒绝
        updates = [[make_update(1, 777, "private", 222, "/set @pics 1s")]]
        harness = self._run(updates, extra_env={"ALLOWED_USER_IDS": "999"})
        self.assertTrue(harness.wait_for(lambda s, r: any("无权使用" in t for t in r)))
        self.assertEqual(harness.bot.store.snapshot_user(222)["count"], 0)

    def test_group_messages_ignored(self):
        # 群聊中的普通消息不进入任何队列
        updates = [[make_update(1, -100999, "supergroup", 222, "群聊内容")]]
        harness = self._run(updates)
        time.sleep(1.5)
        self.assertEqual(harness.replies, [])
        self.assertEqual(harness.bot.store.snapshot_user(222)["count"], 0)

    def test_unknown_command(self):
        updates = [[make_update(1, 777, "private", 222, "/foobar")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("未知命令" in t for t in r)))

    def test_queue_capacity_rejects(self):
        # 队列满：超出 MAX_QUEUE 的消息被拒绝
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
            [make_update(2, 777, "private", 222, "m1")],
            [make_update(3, 777, "private", 222, "m2")],
            [make_update(4, 777, "private", 222, "m3")],
        ]
        harness = self._run(updates, extra_env={"MAX_QUEUE": "2"})
        self.assertTrue(harness.wait_for(lambda s, r: any("队列已满" in t for t in r)))
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["pending_count"], 2)

    def test_later_edges(self):
        # /later 边界：少于 10 秒拒绝、超过 366 天拒绝、绝对时间（含空格日期）可用
        later_dt = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time() + 3600))
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/later 5s 太早了")],
            [make_update(3, 777, "private", 222, "/later 400d 太远了")],
            [make_update(4, 777, "private", 222, "/later %s 远期公告" % later_dt)],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("至少 10 秒后" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("最远支持 366 天" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("已定时" in t for t in r)))
        sent = [x for x in harness.sent if x["text"] == "远期公告"]
        self.assertEqual(len(sent), 1)
        self.assertGreater(sent[0]["schedule_date"], time.time() + 3500)  # 一小时后
        self.assertLess(sent[0]["schedule_date"], time.time() + 3700)

    def test_approve_explicit_target_from_private(self):
        # /approve <群组> 显式目标：任意会话由管理员批准
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            [make_update(2, 888, "private", 999, "/approve -100555")],
            [make_update(3, 777, "private", 222, "g1")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已通过群管理员验证" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: "g1" in s), "显式目标审批后应能发送")
        self.assertTrue(harness.bot.store.user_active(222))

    def test_approve_expired(self):
        # 审批过期：PENDING_APPROVAL_SECONDS 后 /approve 被拒
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            ("sleep", 1.5),
            [make_update(2, -100555, "supergroup", 999, "/approve")],
        ]
        harness = self._run(updates, extra_env={"PENDING_APPROVAL_SECONDS": "1"})
        self.assertTrue(harness.wait_for(lambda s, r: any("已过期" in t for t in s + r)))
        self.assertFalse(harness.bot.store.user_active(222))

    def test_approve_target_mismatch(self):
        # 审批目标不匹配被拒
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            [make_update(2, 888, "private", 999, "/approve @team")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("不匹配" in t for t in r)))
        self.assertFalse(harness.bot.store.user_active(222))

    def test_multi_user_parallel(self):
        # 两名用户并行：各自队列独立发送（333 目标为频道，无需群组审批）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 888, "private", 333, "/set @other 1s")],
            [make_update(3, 777, "private", 222, "a222")],
            [make_update(4, 888, "private", 333, "b333")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "a222" in s and "b333" in s), "两个用户的队列都应发出")
        self.assertIn("a222", harness.sent_to("@pics"))
        self.assertIn("b333", harness.sent_to("@other"))
        self.assertNotIn("b333", harness.sent_to("@pics"))
        self.assertEqual(harness.bot.store.snapshot_user(222)["count"], 1)
        self.assertEqual(harness.bot.store.snapshot_user(333)["count"], 1)

    def test_conflict_409_stops_bot(self):
        # 409 冲突：另一实例轮询同一 token → 停止
        harness = self._run([], conflict=True)
        deadline = time.time() + 10
        while time.time() < deadline and not harness.bot._stop.is_set():
            time.sleep(0.1)
        self.assertTrue(harness.bot._stop.is_set(), "409 冲突后应停止轮询")

    def test_list_multi_page_command(self):
        # /list 多页：页码参数直达
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
        ] + [
            [make_update(i + 2, 777, "private", 222, "m%d" % i)] for i in range(25)
        ] + [
            [make_update(28, 777, "private", 222, "/list 2")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("第 2/2 页" in t for t in r)))
        list_reply = next(t for t in harness.reply_texts() if "第 2/2 页" in t)
        self.assertIn("21. m20", list_reply)
        self.assertIn("25. m24", list_reply)

    def test_setloop_interrupted_by_other_command(self):
        # 收集中被管理命令打断：取消收集并提示
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, "T1")],
            [make_update(4, 777, "private", 222, "/list")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已取消本次循环收集" in t for t in r)))
        self.assertNotIn("循环已启动", "\n".join(harness.reply_texts()))
        self.assertNotIn("T1", harness.sent_to("@pics"))

    def test_list_drop_move_edit(self):
        # 队列管理：/list 分页编号 + /drop /move /edit（用 1h 间隔避免发送干扰）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
            [make_update(2, 777, "private", 222, "A")],
            [make_update(3, 777, "private", 222, "B")],
            [make_update(4, 777, "private", 222, "C")],
            [make_update(5, 777, "private", 222, "D")],
            [make_update(6, 777, "private", 222, "/list")],
            [make_update(7, 777, "private", 222, "/drop 2")],
            [make_update(8, 777, "private", 222, "/move 3 1")],
            [make_update(9, 777, "private", 222, "/edit 1 E")],
            [make_update(10, 777, "private", 222, "/list")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("共 4 条" in t for t in r)), "初始列表应显示 4 条")
        self.assertTrue(harness.wait_for(lambda s, r: any("已删除第 2 条" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("已调整顺序" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("已修改第 1 条" in t for t in r)))
        # 等待第二次 /list 回复（共 2 次带页脚的列表）
        self.assertTrue(harness.wait_for(lambda s, r: sum(1 for t in r if "第 1/1 页" in t) >= 2))
        # 最终列表内容与顺序：[E(原D→编辑), A, C]（drop 2 删 B；move 3 1 将 D 置顶；edit 1 改 D 为 E）
        list_replies = [t for t in harness.reply_texts() if "第 1/1 页" in t]
        self.assertEqual(len(list_replies), 2)
        list_reply = list_replies[-1]  # 最后一次 /list
        self.assertIn("1. E", list_reply)
        self.assertIn("2. A", list_reply)
        self.assertIn("3. C", list_reply)
        # 序号为全局编号（含 /list 页脚提示）
        self.assertIn("/list 1", list_reply)  # 单页时下一页回到第 1 页

    def test_drop_out_of_range(self):
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
            [make_update(2, 777, "private", 222, "A")],
            [make_update(3, 777, "private", 222, "/drop 9")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("序号超出范围" in t for t in r)))

    def test_group_admin_direct_activation(self):
        # 防滥用：操作者本人是群管理员 → 直接启用；回复展示群组真实名称
        updates = [
            [make_update(1, 777, "private", 222, "/set @team 1s")],
            [make_update(2, 777, "private", 222, "hi")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "hi" in s), "应能向群组定时发送")
        self.assertTrue(any("Team Test（@team）" in t for t in harness.reply_texts()))

    def test_group_requires_admin_approval(self):
        # 防滥用：非管理员的 /set → 群内通知 → 管理员 /approve → 生效
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            [make_update(2, -100555, "supergroup", 999, "/approve")],
            [make_update(3, 777, "private", 222, "g1")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "g1" in s), "审批通过后应能排队发送")
        texts = [x["text"] for x in harness.sent] + harness.reply_texts()
        self.assertTrue(any("📌 收到定时消息配置请求" in t for t in texts), "应先在群内发送审批通知")
        self.assertTrue(any("已通过群管理员验证" in t for t in texts), "审批后应回复启用确认")
        self.assertIn("g1", harness.sent_to("-100555"))

    def test_approve_denied_for_non_admin(self):
        # 防滥用：非管理员发送 /approve 被拒绝
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            [make_update(2, -100555, "supergroup", 222, "/approve")],
        ]
        harness = self._run(updates)
        self.assertTrue(
            harness.wait_for(lambda s, r: any("只有目标群的管理员" in t for t in s + r)),
            "非管理员审批应被拒绝",
        )
        self.assertFalse(any("已通过群管理员验证" in t for t in harness.reply_texts()))
        self.assertFalse(harness.bot.store.user_active(222))

    def test_private_target_default_forbidden(self):
        # 防滥用：私人用户目标默认禁止
        updates = [[make_update(1, 777, "private", 222, "/set 42 1s")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("禁止向私人用户" in t for t in r)))
        self.assertFalse(harness.bot.store.user_active(222))

    def test_private_target_allowed_when_enabled(self):
        # 防滥用：ALLOW_PRIVATE_TARGETS=true 且用户已与 bot 交互 → 允许
        updates = [
            [make_update(1, 777, "private", 222, "/set 42 1s")],
            [make_update(2, 777, "private", 222, "p1")],
        ]
        harness = self._run(updates, allow_private=True)
        self.assertTrue(harness.wait_for(lambda s, r: any("p1" in t for t in r)))
        self.assertTrue(harness.bot.store.user_active(222))
        snap = harness.bot.store.snapshot_user(222)
        self.assertEqual(snap["queues"][0]["target"], "42")

    def test_unknown_private_user_refused(self):
        # 防滥用：未与 bot 交互过的用户（getChat 失败）被拒绝
        updates = [[make_update(1, 777, "private", 222, "/set 424242 1s")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("无法解析目标" in t for t in r)))
        self.assertEqual(harness.bot.store.snapshot_user(222)["count"], 0)

    def test_help_reflects_private_policy(self):
        # /help 按实例配置分情况显示私人目标文案，不暴露 .env / 变量细节
        updates = [[make_update(1, 777, "private", 222, "/help")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("本实例未启用向私人用户发送" in t for t in r)))
        joined = "\n".join(harness.reply_texts())
        self.assertNotIn(".env", joined)
        self.assertNotIn("ALLOW_PRIVATE_TARGETS", joined)
        self.assertIn("先与 bot 交互", joined)
        self.assertIn("/reset", joined)
        self.assertIn("/later", joined)
        self.assertIn("客户端", joined)

    def test_help_shows_private_enabled_when_configured(self):
        updates = [[make_update(1, 777, "private", 222, "/help")]]
        harness = self._run(updates, allow_private=True)
        self.assertTrue(harness.wait_for(lambda s, r: any("本实例已开启向私人用户发送" in t for t in r)))

    def test_loop_flow(self):
        # /set → /setloop → 逐条收集内容 → /done → 循环序列按间隔发送（L1,L2 × 2 轮）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, "L1\nL2")],
            [make_update(4, 777, "private", 222, "/done")],
            [make_update(5, 777, "private", 222, "/status")],
        ]
        harness = self._run(updates)
        self.assertTrue(
            harness.wait_for(lambda s, r: s.count("L1") == 2 and s.count("L2") == 2),
            "循环应发送 L1,L2,L1,L2",
        )
        sent = [x["text"] for x in harness.sent]
        self.assertEqual([t for t in sent if t in ("L1", "L2")], ["L1", "L2", "L1", "L2"])
        self.assertNotIn("L1\nL2", sent)  # 收集内容本身不转发
        joined = "\n".join(harness.reply_texts())
        self.assertIn("循环已启动", joined)
        self.assertIn("2 项内容 × 2 轮 = 4 条", joined)
        self.assertIn("循环：2 项内容", joined)  # /status 展示循环信息

    def test_loop_media_flow(self):
        # /setloop 多媒体：图片 + 文本混合收集，循环按 [图片, L1] × 2 发送
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, photo="FILE_LOOP_1", caption="轮播图")],
            [make_update(4, 777, "private", 222, "L1")],
            [make_update(5, 777, "private", 222, "/done")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("sendPhoto:" in x for x in s)))
        self.assertTrue(harness.wait_for(lambda s, r: s.count("L1") == 2))
        self.assertTrue(harness.wait_for(lambda s, r: sum(1 for x in s if "sendPhoto:" in x) == 2))
        # 顺序：图片, L1, 图片, L1
        order = []
        for rec in harness.sent:
            if "text" in rec:
                order.append(rec["text"])
            else:
                order.append("IMG:" + rec["file_id"])
        self.assertEqual(order[:4], ["IMG:FILE_LOOP_1", "L1", "IMG:FILE_LOOP_1", "L1"])
        joined = "\n".join(harness.reply_texts())
        self.assertIn("含 1 条媒体", joined)  # 循环描述注明媒体数量
        self.assertIn("已收集 2 项", joined)

    # ---------------------------------------------------------------- 相册（媒体组）
    def test_album_media_group_sent_as_one_album(self):
        # 同 media_group_id 的多条 update = 一条相册：必须合成 1 条队列项 → 1 次 sendMediaGroup
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, photo="ALB_1", caption="两图推文", media_group_id="G1"),
             make_update(3, 777, "private", 222, photo="ALB_2", media_group_id="G1")],
        ]
        harness = self._run(updates, extra_env={"ALBUM_GRACE_S": "0.2"})
        self.assertTrue(harness.wait_for(lambda s, r: any("sendMediaGroup" in x for x in s)))
        groups = [x for x in harness.sent if x.get("method") == "sendMediaGroup"]
        self.assertEqual(len(groups), 1, "两条分片必须合并成一个相册，不能拆成两次发送")
        self.assertEqual([m["media"] for m in groups[0]["media"]], ["ALB_1", "ALB_2"])
        self.assertEqual([m["type"] for m in groups[0]["media"]], ["photo", "photo"])
        self.assertEqual(groups[0]["media"][0].get("caption"), "两图推文")
        self.assertIsNone(groups[0]["media"][1].get("caption"), "说明只挂第一条")
        self.assertFalse([x for x in harness.sent if x.get("method") == "sendPhoto"], "不应再逐张单发")
        replies = harness.reply_texts()
        self.assertTrue(any("已排队 1 条" in t for t in replies))
        self.assertTrue(any("相册×2" in t for t in replies))

    def test_album_shards_split_across_batches(self):
        # 分片跨批（Telegram 偶发）：等一小会儿补拉一次后仍应合并成一个相册
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, photo="SPLIT_1", caption="分片", media_group_id="G2")],
            [make_update(3, 777, "private", 222, photo="SPLIT_2", media_group_id="G2")],
        ]
        harness = self._run(updates, extra_env={"ALBUM_GRACE_S": "0.3"})
        self.assertTrue(harness.wait_for(lambda s, r: any("sendMediaGroup" in x for x in s)))
        groups = [x for x in harness.sent if x.get("method") == "sendMediaGroup"]
        self.assertEqual(len(groups), 1)
        self.assertEqual([m["media"] for m in groups[0]["media"]], ["SPLIT_1", "SPLIT_2"])
        self.assertEqual(groups[0]["media"][0].get("caption"), "分片")

    def test_single_shard_group_degrades_to_single_send(self):
        # 只有一张分片 → 退化为单发（不产生非法的 1 项媒体组）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, photo="ONE_1", caption="单张", media_group_id="G3")],
        ]
        harness = self._run(updates, extra_env={"ALBUM_GRACE_S": "0.2"})
        self.assertTrue(harness.wait_for(lambda s, r: any("sendPhoto" in x for x in s)))
        self.assertFalse([x for x in harness.sent if x.get("method") == "sendMediaGroup"])
        self.assertEqual([x for x in harness.sent if x.get("method") == "sendPhoto"][0]["caption"], "单张")

    def test_album_over_ten_items_splits_into_multiple_groups(self):
        # 单项超过 10 张时拆成多个媒体组（Telegram 单组上限 10）
        batch = [make_update(2 + i, 777, "private", 222, photo="BIG_%d" % i, media_group_id="G4")
                 for i in range(12)]
        updates = [[make_update(1, 777, "private", 222, "/set @pics 1s")], batch]
        harness = self._run(updates, extra_env={"ALBUM_GRACE_S": "0.2"})
        self.assertTrue(harness.wait_for(
            lambda s, r: sum(1 for x in s if "sendMediaGroup" in x) == 2, timeout=25))
        groups = [x for x in harness.sent if x.get("method") == "sendMediaGroup"]
        self.assertEqual([len(g["media"]) for g in groups], [10, 2])
        self.assertFalse([x for x in harness.sent if x.get("method") == "sendPhoto"])
        self.assertTrue(any("相册×12" in t for t in harness.reply_texts()))

    def test_loop_timeout_auto_start(self):
        # 收集超时自动开始（LOOP_COLLECT_TIMEOUT=0.5s）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, "T1")],
        ]
        harness = self._run(updates, extra_env={"LOOP_COLLECT_TIMEOUT": "0.5"})
        self.assertTrue(harness.wait_for(lambda s, r: any("循环已启动" in t for t in r)), "超时应自动开始循环")
        self.assertTrue(harness.wait_for(lambda s, r: s.count("T1") == 2))

    def test_loop_done_empty_aborts(self):
        # /done 但内容为空 → 取消并提示
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, "/done")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("循环内容为空" in t for t in r)))

    def test_setloop_requires_target(self):
        # 防滥用：未设置目标时拒绝配置循环
        updates = [[make_update(1, 777, "private", 222, "/setloop 1s times:2")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("请先使用 /set" in t for t in r)))

    def test_setloop_requires_limit(self):
        # 防滥用：必须显式给出循环限制（次数/时长/结束时间），不允许无限循环
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("用法：/setloop" in t for t in r)))
        self.assertIsNone(harness.bot._loop_collect.get(222))

    def test_loop_content_capped(self):
        # 防滥用：超大循环被上限截断并提示
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:999999")],
            [make_update(3, 777, "private", 222, "A\nB\nC")],
            [make_update(4, 777, "private", 222, "/done")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已达条数上限" in t for t in r)))
        snap = harness.bot.store.snapshot_user(222)
        total = sum(q["pending_count"] for q in snap["queues"])
        self.assertLessEqual(total, 5000)
        self.assertTrue(any(q["loop_info"] for q in snap["queues"]))


    def test_command_menu_registered_on_startup(self):
        # 启动时 setMyCommands 注册命令菜单（/ 按钮与输入联想）
        harness = self._run([])
        deadline = time.time() + 10
        commands = None
        while time.time() < deadline:
            with harness.fake.lock:
                registered = harness.fake.commands
            if registered:
                commands = registered.get("commands")
                break
            time.sleep(0.1)
        self.assertIsNotNone(commands, "启动时应调用 setMyCommands 注册命令菜单")
        names = [c["command"] for c in commands]
        self.assertGreaterEqual(len(names), 10)
        for want in ("start", "set", "setloop", "pace", "later", "list", "drop",
                     "move", "edit", "reset", "cancel", "status", "approve", "help"):
            self.assertIn(want, names)
        for cmd in commands:
            self.assertRegex(cmd["command"], r"^[a-z0-9_]{1,32}$")
            self.assertTrue(cmd.get("description"), "每个命令都应有描述：%s" % cmd.get("command"))

    def test_pace_command_reschedules_queued_partial(self):
        # /pace 2s 2-3：段首以第 1 条原时刻为锚，段内相邻间隔改为 2 秒；段外与基准间隔不受影响
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
            [make_update(2, 777, "private", 222, "a")],
            [make_update(3, 777, "private", 222, "b")],
            [make_update(4, 777, "private", 222, "c")],
            [make_update(5, 777, "private", 222, "/pace 2s 2-3")],
        ]
        harness = self._run(updates)
        # 入队回复展示的是「本批（新入队）消息」的预计发送时刻，不再是容易误读的 下一条：
        self.assertTrue(harness.wait_for(lambda s, r: any("本批消息预计发送" in t for t in r)))
        joined = "\n".join(harness.reply_texts())
        self.assertNotIn("下一条：", joined)

        self.assertTrue(harness.wait_for(lambda s, r: any("已重排第 2-3 条" in t for t in r)))
        self.assertTrue(any("第 2-3 条（共 2 条）" in t for t in harness.reply_texts()))
        self.assertTrue(any("区间之外的消息保持原定时时刻不变" in t for t in harness.reply_texts()))
        pending = harness.bot.store._users[str(222)]["queues"]["@pics"]["pending"]
        slots = [it["send_at"] for it in pending]
        self.assertEqual(len(slots), 3)
        self.assertAlmostEqual(slots[1] - slots[0], 2.0, delta=0.01)
        self.assertAlmostEqual(slots[2] - slots[1], 2.0, delta=0.01)
        # 分段重排不动队列基准间隔（供后续新入队使用）
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["interval_s"], 3600.0)

    def test_pace_command_reschedules_queued_whole(self):
        # /pace 30 整批：队首原时刻不变，其余按新间隔（1800s）顺延，基准间隔同步
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1h")],
            [make_update(2, 777, "private", 222, "a")],
            [make_update(3, 777, "private", 222, "b")],
            [make_update(4, 777, "private", 222, "c")],
            [make_update(5, 777, "private", 222, "/pace 30")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已重排整批 3 条" in t for t in r)))
        pending = harness.bot.store._users[str(222)]["queues"]["@pics"]["pending"]
        slots = [it["send_at"] for it in pending]
        self.assertEqual(len(slots), 3)
        self.assertAlmostEqual(slots[1] - slots[0], 1800.0, delta=0.01)
        self.assertAlmostEqual(slots[2] - slots[1], 1800.0, delta=0.01)
        self.assertEqual(harness.bot.store.snapshot_user(222)["queues"][0]["interval_s"], 1800.0)
        joined = "\n".join(harness.reply_texts())
        self.assertIn("后续新入队消息也将按新间隔排队", joined)

    def test_pace_usage_and_errors(self):
        # /pace 参数缺失 / 范围非法 / 间隔非法 → 提示且不动队列
        updates = [
            [make_update(1, 777, "private", 222, "/pace")],
            [make_update(2, 777, "private", 222, "/set @pics 1h")],
            [make_update(3, 777, "private", 222, "a")],
            [make_update(4, 777, "private", 222, "b")],
            [make_update(5, 777, "private", 222, "/pace 5 0-2")],
            [make_update(6, 777, "private", 222, "/pace abc")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("用法：/pace" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("范围无效" in t for t in r)))
        self.assertTrue(harness.wait_for(lambda s, r: any("间隔无效" in t for t in r)))
        pending = harness.bot.store._users[str(222)]["queues"]["@pics"]["pending"]
        slots = [it["send_at"] for it in pending]
        self.assertEqual(len(slots), 2)
        self.assertAlmostEqual(slots[1] - slots[0], 3600.0, delta=0.01)  # 未被改动


if __name__ == "__main__":
    unittest.main(verbosity=2)
