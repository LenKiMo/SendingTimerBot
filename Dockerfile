# SendingTimeBot —— 零第三方依赖，仅需 Python 标准库（无需 requirements.txt）
FROM python:3.12-slim

WORKDIR /app

# 本程序不依赖任何第三方包，直接拷贝源码即可
COPY bot.py config.py queue.py telegram_api.py ./

# 状态文件目录：docker compose 会挂载 ./data 到此，重启/重建容器不丢队列
RUN mkdir -p /app/data
VOLUME ["/app/data"]

# -u 无缓冲输出，便于 docker logs 实时查看日志
CMD ["python", "-u", "bot.py"]
