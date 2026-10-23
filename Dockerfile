# wzyp-view — 链动小铺店铺数据洞察
FROM python:3.13-slim

WORKDIR /app

# 时区 + 基础工具
ENV TZ=Asia/Shanghai \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WZYP_HOST=0.0.0.0

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 依赖（分层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 应用代码
COPY server.py agent.py db.py config.py run.py local_store.py platform16688.py platformwzyp.py ./
COPY crawler16688.py crawl_store.py public_http.py ./
COPY static/ ./static/

# 数据目录（聊天记忆 / settings.json 运行时可写）
RUN mkdir -p /app/data
COPY data/wzyp_seeds.json /app/data/wzyp_seeds.json
VOLUME ["/app/data"]

EXPOSE 8770

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8770/api/health',timeout=4)" || exit 1

CMD ["python", "run.py"]
