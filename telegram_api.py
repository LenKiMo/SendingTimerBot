"""极简 Telegram Bot API 客户端 —— 仅使用 Python 标准库（urllib），零第三方依赖。

只实现本项目需要的接口：
- getMe         启动自检
- getUpdates    长轮询接收更新
- sendMessage   发送文本消息
- getFile / 下载 解析用户上传的文本文件

行为约定：
- 429 限流与 5xx 服务端错误：按 2^n 秒指数退避重试（429 优先使用 retry_after）
- 4xx 永久错误（如 403 无权限、400 频道不存在）：直接抛出 ApiError，不重试
- 网络异常（URLError/超时）：按 2^n 秒退避重试，最终失败抛 RuntimeError
- 支持系统 HTTP_PROXY / HTTPS_PROXY 环境变量（urllib 原生支持）
"""

import json
import logging
import time
import urllib.error
import urllib.request

log = logging.getLogger("telegram")


class ApiError(Exception):
    """Telegram API 返回的业务错误（4xx 永久错误、重试耗尽后的 429/5xx）。"""

    def __init__(self, code, description, method=""):
        super().__init__("Telegram API %s: %s" % (code, description))
        self.code = code
        self.description = description
        self.method = method

    @property
    def retryable(self):
        """是否属于可重试错误。"""
        return self.code == 429 or 500 <= self.code < 600


class TelegramClient:
    def __init__(self, token, api_base="https://api.telegram.org", timeout=60, max_retries=3):
        if not token:
            raise ValueError("token 不能为空")
        self.token = token
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._headers = {"Content-Type": "application/json", "User-Agent": "SendingTimeBot"}

    # ------------------------------------------------------------------ 基础
    def _url(self, method):
        return "%s/bot%s/%s" % (self.api_base, self.token, method)

    def _file_url(self, file_path):
        return "%s/file/bot%s/%s" % (self.api_base, self.token, file_path)

    def _retry_wait(self, err, data, attempt):
        """返回需要等待的秒数；返回 None 表示不再重试。"""
        if not err.retryable or attempt >= self.max_retries:
            return None
        if err.code == 429:
            try:
                return int(data["parameters"]["retry_after"])
            except Exception:
                pass
        return 2 ** attempt

    def call(self, method, params=None):
        """POST JSON 到 API 并返回 result 字段。"""
        payload = json.dumps(params or {}).encode("utf-8")
        request = urllib.request.Request(self._url(method), data=payload, headers=self._headers, method="POST")
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    body = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                try:
                    data = json.loads(raw)
                except Exception:
                    data = {}
                err = ApiError(data.get("error_code", exc.code), data.get("description", raw), method)
                wait = self._retry_wait(err, data, attempt)
                if wait is not None:
                    log.warning("%s 限流/服务端错误（%s），%.1f 秒后重试", method, err.description, wait)
                    time.sleep(wait)
                    continue
                raise err
            except (urllib.error.URLError, OSError) as exc:
                if attempt < self.max_retries:
                    log.warning("%s 网络错误（%s），%s 秒后重试", method, exc, 2 ** attempt)
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError("%s 网络请求最终失败：%s" % (method, exc)) from exc

            if not body.get("ok"):
                err = ApiError(body.get("error_code", -1), body.get("description", "未知错误"), method)
                wait = self._retry_wait(err, body, attempt)
                if wait is not None:
                    time.sleep(wait)
                    continue
                raise err
            return body.get("result")
        return None  # 不可达

    # ------------------------------------------------------------------ API
    def get_me(self):
        """启动自检：返回 bot 自身信息（dict）。"""
        return self.call("getMe")

    def get_updates(self, offset=None, timeout=30):
        """长轮询获取更新。offset 用于断点续传（已持久化，重启不重复消费）。"""
        params = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset
        result = self.call("getUpdates", params)
        return result or []

    def send_message(self, chat_id, text):
        """向频道/会话发送文本消息。"""
        return self.call("sendMessage", {"chat_id": chat_id, "text": text})

    def get_file(self, file_id):
        """获取文件信息（含 file_path，用于下载）。"""
        return self.call("getFile", {"file_id": file_id})

    def get_chat(self, chat_id):
        """解析目标（@用户名 或 ID），返回 chat 信息（含 type 字段）。

        type 取值：channel（频道）/ group、supergroup（群组）/ private（私人用户）。
        失败抛 ApiError（目标不存在、bot 无访问权限、私人用户未与 bot 建立会话等）。
        """
        return self.call("getChat", {"chat_id": chat_id})

    def get_chat_administrators(self, chat_id):
        """获取群组管理员列表（每个元素含 user.id），用于防滥用验证。

        要求 bot 是目标群成员；失败抛 ApiError。
        """
        return self.call("getChatAdministrators", {"chat_id": chat_id}) or []

    def download_file(self, file_path):
        """下载文件内容，返回 bytes。"""
        request = urllib.request.Request(self._file_url(file_path), headers={"User-Agent": "SendingTimeBot"})
        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return response.read()
            except (urllib.error.URLError, OSError) as exc:
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError("文件下载失败：%s" % exc) from exc
        return b""
