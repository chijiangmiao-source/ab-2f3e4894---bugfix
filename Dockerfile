FROM python:3.11-slim

# 系统仅使用 Python 标准库（http.server/sqlite3/hmac/threading），无第三方依赖。
WORKDIR /app

COPY app/ ./app/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

ENV DUTY_DB_PATH=/data/duty.db \
    DUTY_HTTP_HOST=0.0.0.0 \
    DUTY_HTTP_PORT=8080 \
    PYTHONUNBUFFERED=1

EXPOSE 8080
VOLUME ["/data"]

# 默认运行值班服务；verify 服务在 compose 中覆盖 command。
CMD ["python", "-m", "app.main"]
