"""SendingTimeBot 自动化验证（无需真实 token，可离线运行）。

覆盖：
1. 调度语义单元测试（注入假时钟，验证需求中的定时规则逐条成立）
2. 间隔解析 / .env 配置解析测试
3. 群组审批状态（防滥用）生命周期测试
4. 端到端测试：本地伪 Telegram API 服务器 + 真实 bot 主循环：
   - 频道：/set → 排队 → 按间隔发送 → /cancel（回归）
   - 群组：管理员直接启用 / 非管理员需 /approve 审批 / 非管理员审批被拒
   - 私人：默认禁止 / 开关开启后可用 / 未交互用户被拒

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
from queue import SendingQueue


# ======================================================================
# 调度语义单元测试（假时钟，不依赖真实时间）
# ======================================================================
class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, delta):
        self.t += delta


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.clock = FakeClock(1000.0)  # 收到消息时刻 = 1000
        self.q = SendingQueue(os.path.join(self.dir.name, "state.json"), now_fn=self.clock)

    def tearDown(self):
        self.dir.cleanup()

    def test_first_message_anchored_to_receipt_time(self):
        # 需求：目标频道无定时消息 → 以收到时间为基准的下一个间隔期
        # 例：/set @pics 5 后 10:00 收到 → 首条 10:05 发送
        self.q.set_target("@pics", 300)
        added, first_at, full = self.q.add_messages(["链接1"])
        self.assertEqual(added, 1)
        self.assertFalse(full)
        self.assertEqual(first_at, 1300.0)  # 1000 + 5*60

    def test_chain_spaced_by_interval(self):
        # 需求：第二条 = 第一条定时时刻 + 间隔，以此类推（FIFO）
        self.q.set_target("@pics", 300)
        added, first_at, _ = self.q.add_messages(["a", "b", "c"])
        self.assertEqual(added, 3)
        self.assertEqual(first_at, 1300.0)
        self.assertEqual(self.q.snapshot()["pending_count"], 3)
        self.assertEqual(self.q.peek_next_due(now=2000), ("a", 1300.0))
        self.q.confirm_sent()
        self.assertEqual(self.q.peek_next_due(now=2000), ("b", 1600.0))
        self.q.confirm_sent()
        self.assertEqual(self.q.peek_next_due(now=2000), ("c", 1900.0))

    def test_new_messages_after_drain_keep_cadence(self):
        # 需求：频道已有定时消息 → 以最后一条定时消息的下一个间隔期为定时时间
        # 即使队列早已发空、且收到新消息的时间很晚，仍保持原节奏
        self.q.set_target("@pics", 300)
        self.q.add_messages(["a"])
        self.q.confirm_sent()  # 最后一条定时消息 = 1300
        self.clock.advance(10000)  # 11000 时才收到新消息
        added, first_at, _ = self.q.add_messages(["d"])
        self.assertEqual(added, 1)
        self.assertEqual(first_at, 1600.0)  # 1300 + 300，而非 11000 + 300

    def test_cancel_stops_intake_keeps_queue(self):
        # 需求：/cancel 后不再接收新消息，已排队消息继续
        self.q.set_target("@pics", 300)
        self.q.add_messages(["a"])
        self.q.cancel()
        added, _, _ = self.q.add_messages(["b"])
        self.assertEqual(added, 0)
        self.assertEqual(self.q.snapshot()["pending_count"], 1)
        # 已排队消息仍可到点取出
        self.assertEqual(self.q.peek_next_due(now=1300.0), ("a", 1300.0))

    def test_set_new_target_clears_old_queue(self):
        # /set 换目标：旧队列清空，新目标从收到时刻重新起算
        self.q.set_target("@pics", 300)
        self.q.add_messages(["a"])
        self.q.set_target("-1001234567890", 60)
        self.assertEqual(self.q.snapshot()["pending_count"], 0)
        added, first_at, _ = self.q.add_messages(["x"])
        self.assertEqual(added, 1)
        self.assertEqual(first_at, 1060.0)  # 1000 + 60

    def test_persistence_across_restart(self):
        # 重启（停容器/重启进程）后队列不丢失
        path = os.path.join(self.dir.name, "state.json")
        q1 = SendingQueue(path, now_fn=self.clock)
        q1.set_target("@pics", 300)
        q1.add_messages(["a", "b"])
        q2 = SendingQueue(path, now_fn=self.clock)  # 模拟重启
        snap = q2.snapshot()
        self.assertEqual(snap["target"], "@pics")
        self.assertEqual(snap["pending_count"], 2)
        self.assertEqual(q2.peek_next_due(now=2000), ("a", 1300.0))

    def test_pending_approval_lifecycle(self):
        # 防滥用：待审批请求的登记、持久化、被 /set / /cancel 清除
        path = os.path.join(self.dir.name, "state.json")
        self.q.request_approval("@team", -100999, 300, 222)
        pending = self.q.get_pending_approval()
        self.assertEqual(pending["target"], "@team")
        self.assertEqual(pending["chat_id"], -100999)
        self.assertEqual(pending["interval_s"], 300.0)
        self.assertEqual(pending["requested_by"], 222)
        # 重启持久化
        q2 = SendingQueue(path, now_fn=self.clock)
        self.assertEqual(q2.get_pending_approval()["chat_id"], -100999)
        # 新的 /set 清除待审批
        self.q.set_target("@pics", 60)
        self.assertIsNone(self.q.get_pending_approval())
        # /cancel 清除待审批
        self.q.request_approval("@team", -100999, 300, 222)
        self.q.cancel()
        self.assertIsNone(self.q.get_pending_approval())

    def test_blank_lines_ignored(self):
        self.q.set_target("@pics", 300)
        added, _, _ = self.q.add_messages(["a", "", "   ", "b"])
        self.assertEqual(added, 2)

    def test_replace_pending_clears_old_queue_and_sets_loop_info(self):
        # /setloop：清空旧队列，按循环间隔排布，loop_info 持久化，发完自动清除
        path = os.path.join(self.dir.name, "state.json")
        self.q.set_target("@pics", 300)
        self.q.add_messages(["x"])  # 旧队列
        added, first_at, full = self.q.replace_pending(
            ["a", "b"], 60, loop_info={"desc": "2 条内容 × 1 轮 = 2 条", "total": 2, "interval_s": 60}
        )
        self.assertEqual(added, 2)
        self.assertFalse(full)
        self.assertEqual(first_at, 1060.0)  # 锚点 = 收到时刻 1000 + 60
        snap = self.q.snapshot()
        self.assertEqual(snap["pending_count"], 2)
        self.assertEqual(snap["loop_info"]["desc"], "2 条内容 × 1 轮 = 2 条")
        # 重启持久化
        q2 = SendingQueue(path, now_fn=self.clock)
        self.assertEqual(q2.snapshot()["loop_info"]["desc"], "2 条内容 × 1 轮 = 2 条")
        self.assertEqual(q2.snapshot()["pending_count"], 2)
        # 发完后循环信息自动清除
        self.q.confirm_sent()
        self.q.confirm_sent()
        self.assertIsNone(self.q.snapshot()["loop_info"])

    def test_replace_pending_uses_loop_interval(self):
        # /setloop 的间隔可以不同于 /set 的间隔
        self.q.set_target("@pics", 300)
        added, first_at, _ = self.q.replace_pending(["a"], 30, loop_info={"desc": "x", "total": 1, "interval_s": 30})
        self.assertEqual(added, 1)
        self.assertEqual(first_at, 1030.0)

    def test_replace_pending_refused_when_inactive(self):
        self.q.set_target("@pics", 300)
        self.q.cancel()
        added, _, _ = self.q.replace_pending(["a"], 30)
        self.assertEqual(added, 0)

    def test_queue_capacity(self):
        self.q = SendingQueue(os.path.join(self.dir.name, "state.json"), max_pending=2, now_fn=self.clock)
        self.q.set_target("@pics", 300)
        added, _, full = self.q.add_messages(["a", "b", "c"])
        self.assertEqual(added, 2)
        self.assertTrue(full)


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
    def test_times_expansion(self):
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 60, "times", 3, 1000.0, 10000)
        self.assertEqual(seq, ["a", "b", "a", "b", "a", "b"])
        self.assertEqual(rounds, 3)
        self.assertFalse(truncated)

    def test_duration_half_round(self):
        # 发送时刻 1002/1004 ≤ 1005，1006 超出 → 半轮 [a, b]
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 2, "duration", 5, 1000.0, 10000)
        self.assertEqual(seq, ["a", "b"])
        self.assertEqual(rounds, 1)
        self.assertFalse(truncated)

    def test_until_equals_duration(self):
        seq, _, _ = build_loop_sequence(["a", "b"], 2, "until", 1005.0, 1000.0, 10000)
        self.assertEqual(seq, ["a", "b"])

    def test_cap_truncates_to_whole_rounds(self):
        # 上限截断到完整轮：cap=3、内容 2 条 → 仅保留 1 轮（2 条）
        seq, rounds, truncated = build_loop_sequence(["a", "b"], 60, "times", 5, 1000.0, 3)
        self.assertEqual(seq, ["a", "b"])
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
# 伪服务器已知目标（getChat 可解析）与管理表（getChatAdministrators 返回）
KNOWN_CHATS = {
    "@pics": {"id": -1001234567890, "type": "channel", "username": "pics"},
    "-1001234567890": {"id": -1001234567890, "type": "channel", "username": "pics"},
    "@team": {"id": -100999, "type": "supergroup", "username": "team"},
    "-100999": {"id": -100999, "type": "supergroup", "username": "team"},
    "-100555": {"id": -100555, "type": "supergroup"},
    "42": {"id": 42, "type": "private"},
}
# 发往这些 chat_id 的 sendMessage 记为"发往目标"，其余记为"回复用户"
TARGET_CHAT_IDS = {"@pics", "@team", "-1001234567890", "-100999", "-100555"}
GROUP_ADMINS = {
    -100999: [{"user": {"id": 222, "first_name": "Admin222"}}],
    -100555: [{"user": {"id": 999, "first_name": "Admin999"}}],
}


class FakeTelegramServer:
    """最小化伪 Telegram Bot API 服务器（内存中记录发送情况）。"""

    def __init__(self, scripted_updates):
        self.scripted_updates = list(scripted_updates)
        self.sent = []  # 发往目标的记录
        self.replies = []  # 发给用户的回复记录
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

            def log_message(self, *args):
                pass

        return Handler

    def _dispatch(self, method, params):
        if method == "getMe":
            return {"id": 111111, "username": "testbot", "first_name": "Test"}
        if method == "getUpdates":
            with self.lock:
                if self.scripted_updates:
                    return self.scripted_updates.pop(0)
            time.sleep(0.05)
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
        if method == "sendMessage":
            record = {"chat_id": params.get("chat_id"), "text": params.get("text"), "at": time.time()}
            with self.lock:
                if str(record["chat_id"]) in TARGET_CHAT_IDS:
                    self.sent.append(record)
                else:
                    self.replies.append(record)
            return {"message_id": len(self.sent) + len(self.replies)}
        return ("__error__", 400, "Bad Request: unknown method %s" % method)

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=5)


def make_update(update_id, chat_id, chat_type, user_id, text):
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": user_id, "first_name": "Tester"},
            "chat": {"id": chat_id, "type": chat_type},
            "text": text,
        },
    }


_ENV_KEYS = ("BOT_TOKEN", "API_BASE_URL", "CHECK_INTERVAL_S", "STATE_PATH", "ALLOWED_USER_IDS", "NO_PROXY", "ALLOW_PRIVATE_TARGETS")


class BotHarness:
    """端到端测试脚手架：伪服务器 + 环境变量 + 真实 bot 主循环。"""

    def __init__(self, updates, temp_dir, allow_private=False):
        for key in _ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["NO_PROXY"] = "127.0.0.1,localhost"
        self.fake = FakeTelegramServer(updates)
        os.environ["BOT_TOKEN"] = "TEST:TOKEN"
        os.environ["API_BASE_URL"] = "http://127.0.0.1:%d" % self.fake.port
        os.environ["CHECK_INTERVAL_S"] = "0.05"
        os.environ["STATE_PATH"] = os.path.join(temp_dir, "state.json")
        os.environ["ALLOWED_USER_IDS"] = ""
        if allow_private:
            os.environ["ALLOW_PRIVATE_TARGETS"] = "true"
        self.bot = Bot(Config(".env"))
        self._thread = threading.Thread(target=self.bot.run, daemon=True)
        self._thread.start()

    def stop(self):
        self.bot._stop.set()
        self.fake.stop()

    def wait_for(self, predicate, timeout=15):
        """轮询直到 predicate(sent_texts, reply_texts) 为真（不持有锁调用回调）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.fake.lock:
                sent = [r["text"] for r in self.fake.sent]
                replies = [r["text"] for r in self.fake.replies]
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

    def sent_texts(self):
        return [s["text"] for s in self.sent]

    def reply_texts(self):
        return [r["text"] for r in self.replies]

    def all_texts(self):
        return self.sent_texts() + self.reply_texts()


class EndToEndTests(unittest.TestCase):
    def _run(self, updates, allow_private=False):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        harness = BotHarness(updates, tmp.name, allow_private=allow_private)
        self.addCleanup(harness.stop)
        for key in _ENV_KEYS:
            self.addCleanup(os.environ.pop, key, None)
        return harness

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
        self.assertEqual([s["text"] for s in sent], ["第一条", "a", "b", "c"])
        self.assertTrue(all(s["chat_id"] == "@pics" for s in sent), "应全部发往 @pics")
        times = [s["at"] for s in sent]
        self.assertGreaterEqual(times[0] - started, 0.7, "首条应在接收后约 1 秒发出")
        for i in range(1, len(times)):
            self.assertGreaterEqual(times[i] - times[i - 1], 0.7, "相邻消息应间隔约 1 秒")

        # /cancel 后不再接收新消息
        time.sleep(2.0)
        self.assertNotIn("不应发送", harness.sent_texts())

        joined = "\n".join(harness.reply_texts())
        self.assertIn("已排队", joined)
        self.assertIn("@pics", joined)

    def test_group_admin_direct_activation(self):
        # 防滥用：操作者本人是群管理员 → 直接启用
        updates = [
            [make_update(1, 777, "private", 222, "/set @team 1s")],
            [make_update(2, 777, "private", 222, "hi")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "hi" in s), "应能向群组定时发送")
        self.assertTrue(any("已设置" in t for t in harness.reply_texts()))
        self.assertTrue(any("已设置" in t and "@team" in t for t in harness.reply_texts()))

    def test_group_requires_admin_approval(self):
        # 防滥用：非管理员的 /set → 群内通知 → 管理员 /approve → 生效
        updates = [
            [make_update(1, 777, "private", 222, "/set -100555 1s")],
            [make_update(2, -100555, "supergroup", 999, "/approve")],
            [make_update(3, 777, "private", 222, "g1")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: "g1" in s), "审批通过后应能排队发送")
        texts = harness.all_texts()
        self.assertTrue(any("📌 收到定时消息配置请求" in t for t in texts), "应先在群内发送审批通知")
        self.assertTrue(any("已通过群管理员验证" in t for t in texts), "审批后应回复启用确认")
        # 审批前 /set 未直接生效（消息只在审批后才发出）
        self.assertIn("g1", harness.sent_texts())

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
        self.assertFalse(any("已通过群管理员验证" in t for t in harness.all_texts()))
        self.assertFalse(harness.bot.queue.is_active())

    def test_private_target_default_forbidden(self):
        # 防滥用：私人用户目标默认禁止
        updates = [[make_update(1, 777, "private", 222, "/set 42 1s")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("禁止向私人用户" in t for t in r)))
        self.assertFalse(harness.bot.queue.is_active())

    def test_private_target_allowed_when_enabled(self):
        # 防滥用：ALLOW_PRIVATE_TARGETS=true 且用户已与 bot 交互 → 允许
        updates = [
            [make_update(1, 777, "private", 222, "/set 42 1s")],
            [make_update(2, 777, "private", 222, "p1")],
        ]
        harness = self._run(updates, allow_private=True)
        self.assertTrue(harness.wait_for(lambda s, r: any("p1" in t for t in r)))
        self.assertTrue(harness.bot.queue.is_active())
        self.assertEqual(harness.bot.queue.snapshot()["target"], "42")

    def test_unknown_private_user_refused(self):
        # 防滥用：未与 bot 交互过的用户（getChat 失败）被拒绝
        updates = [[make_update(1, 777, "private", 222, "/set 424242 1s")]]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("无法解析目标" in t for t in r)))
        self.assertFalse(harness.bot.queue.is_active())

    def test_loop_flow(self):
        # /set → /setloop → 提交内容 → 循环序列按间隔发送（L1,L2 × 2 轮）
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:2")],
            [make_update(3, 777, "private", 222, "L1\nL2")],
            [make_update(4, 777, "private", 222, "/status")],
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
        self.assertIn("2 条内容 × 2 轮 = 4 条", joined)
        # /status 展示循环信息
        self.assertIn("循环：2 条内容", joined)

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
        self.assertIsNone(harness.bot._loop_collect)

    def test_loop_content_capped(self):
        # 防滥用：超大循环被上限截断并提示
        updates = [
            [make_update(1, 777, "private", 222, "/set @pics 1s")],
            [make_update(2, 777, "private", 222, "/setloop 1s times:999999")],
            [make_update(3, 777, "private", 222, "A\nB\nC")],
        ]
        harness = self._run(updates)
        self.assertTrue(harness.wait_for(lambda s, r: any("已达条数上限" in t for t in r)))
        # 默认 MAX_LOOP_ITEMS=5000 → 5000 // 3 = 1666 轮
        snap = harness.bot.queue.snapshot()
        self.assertLessEqual(snap["pending_count"], 5000)
        self.assertTrue(snap["loop_info"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
