"""配置加载 —— 纯标准库实现，不依赖 python-dotenv。

优先级：进程环境变量 > .env 文件 > 内置默认值。
所有文件均以 UTF-8 编码读写。
"""

import os


def load_env_file(path):
    """解析 .env 文件（UTF-8）。格式：每行 KEY=VALUE，# 开头为注释，支持引号包裹的值。"""
    result = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                if key:
                    result[key] = value
    except FileNotFoundError:
        pass
    return result


def _pick(env, key, default=""):
    """环境变量优先，其次 .env 文件，最后默认值。"""
    value = os.environ.get(key, "").strip()
    if value:
        return value
    return env.get(key, default).strip()


def _int(env, key, default):
    try:
        return int(_pick(env, key, str(default)))
    except ValueError:
        print("[config] 警告：%s 不是有效整数，使用默认值 %s" % (key, default))
        return default


def _float(env, key, default):
    try:
        return float(_pick(env, key, str(default)))
    except ValueError:
        print("[config] 警告：%s 不是有效数字，使用默认值 %s" % (key, default))
        return default


def _bool(env, key, default):
    value = _pick(env, key, str(default)).strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    print("[config] 警告：%s 不是有效布尔值，使用默认值 %s" % (key, default))
    return default


def _apply_timezone(env):
    """把 TZ 应用到当前进程，并重置 C 库时区规则。

    无论 TZ 来自 .env 文件还是容器环境变量（docker compose env_file 注入），
    都必须调用 tzset()：glibc 的 localtime_r 不会自动读取 TZ 环境变量，
    进程内第一次时间调用后时区会被缓存，跳过 tzset 会导致仍显示 UTC。
    """
    tz = (os.environ.get("TZ") or env.get("TZ") or "").strip()
    if not tz:
        return
    os.environ["TZ"] = tz
    try:
        import time as _time

        _time.tzset()
    except AttributeError:
        pass  # Windows 无 tzset，CRT 大多会直接读取 TZ 环境变量


class Config:
    """程序配置。所有字段均可通过 .env 或系统环境变量覆盖。"""

    def __init__(self, env_file=".env"):
        env = load_env_file(env_file)
        _apply_timezone(env)
        self.env_file = env_file

        # 必填：Telegram Bot Token（@BotFather 获取）
        self.bot_token = _pick(env, "BOT_TOKEN")

        # 可选：API 地址（默认官方；测试/代理场景可覆盖）
        self.api_base = _pick(env, "API_BASE_URL", "https://api.telegram.org")

        # 可选：允许使用命令的用户 ID 白名单（逗号分隔；留空 = 任何人可用）
        ids = []
        for item in _pick(env, "ALLOWED_USER_IDS").split(","):
            item = item.strip()
            if item:
                try:
                    ids.append(int(item))
                except ValueError:
                    print("[config] 警告：忽略无效用户 ID %r" % item)
        self.allowed_user_ids = ids

        # 调度线程检查间隔（秒）
        self.check_interval_s = _float(env, "CHECK_INTERVAL_S", 0.5)

        # 队列上限（条）
        self.max_queue = _int(env, "MAX_QUEUE", 10000)

        # 上传文件大小上限（字节，默认 5MB）
        self.max_file_bytes = _int(env, "MAX_FILE_BYTES", 5 * 1024 * 1024)

        # 单个文件最多解析行数
        self.max_file_lines = _int(env, "MAX_FILE_LINES", 5000)

        # 状态文件路径（Docker 下建议放在挂载卷中）
        self.state_path = _pick(env, "STATE_PATH", "data/state.json")

        # API 请求失败重试次数
        self.max_retries = _int(env, "MAX_RETRIES", 3)

        # 防滥用：是否允许向私人用户定时发送（默认禁止，需显式开启）
        self.allow_private_targets = _bool(env, "ALLOW_PRIVATE_TARGETS", False)

        # 防滥用：群组审批请求的有效期（秒，默认 60 分钟）
        self.pending_approval_seconds = _int(env, "PENDING_APPROVAL_SECONDS", 3600)

        # 防滥用：循环发送单次展开的消息总数上限
        self.max_loop_items = _int(env, "MAX_LOOP_ITEMS", 5000)
