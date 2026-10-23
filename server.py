"""wzyp-view 后端：FastAPI 静态 + API + SSE"""
import json
import os
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import config as cfg
import db
import platform16688
from crawler16688 import CrawlManager
from crawl_store import JobConflict
from config import BASE_DIR

Source = Literal["wzyp", "16688"]
crawler = CrawlManager()


@asynccontextmanager
async def lifespan(app):
    crawler.recover_interrupted()
    try:
        yield
    finally:
        crawler.shutdown()


def get_agent():
    # 数据浏览与采集无需启动 LLM 依赖。
    import agent
    return agent


def memory_id(thread_id, source):
    return f"16688:{thread_id}" if source == "16688" else thread_id

app = FastAPI(title="wzyp-view", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


# ---------------- 静态页 ----------------

@app.get("/")
def index():
    return FileResponse(os.path.join(BASE_DIR, "static", "index.html"))


# ---------------- 数据 API ----------------

@app.get("/api/stats")
def api_stats(source: Source = "wzyp"):
    with db.use_source(source):
        return {"source": source, "stats": db.shop_stats(), "type_dist": db.type_dist(),
                "price_buckets": db.price_buckets(), "top_keywords": db.top_keywords(24)}


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/sources")
def sources():
    return {"sources": [{"id": "wzyp", "name": "链动小铺", "storage": "MySQL"},
                        {"id": "16688", "name": "16688 交易平台", "storage": "SQLite"}],
            "example_url": platform16688.EXAMPLE_URL}


class SyncRequest(BaseModel):
    url: str = Field(min_length=1, max_length=500)


@app.post("/api/sources/16688/sync")
def sync_16688(body: SyncRequest):
    try:
        return platform16688.sync_shop(body.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except platform16688.PlatformError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


class CrawlRequest(BaseModel):
    mode: Literal["discover", "refresh"] = "discover"
    urls: list[str] = Field(default_factory=list, max_length=1000)


@app.post("/api/sources/16688/crawl")
def start_crawl(body: CrawlRequest):
    try:
        return crawler.start(body.mode, body.urls)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/sources/16688/crawl")
def latest_crawl():
    crawler.recover_interrupted()
    return {"job": crawler.store.report()}


@app.get("/api/sources/16688/crawl/{job_id}")
def crawl_progress(job_id: str):
    crawler.recover_interrupted()
    report = crawler.store.report(job_id)
    if report is None:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    return report


@app.post("/api/sources/16688/crawl/{job_id}/cancel")
def cancel_crawl(job_id: str):
    try:
        return crawler.cancel(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="采集任务不存在") from exc


@app.post("/api/sources/16688/crawl/{job_id}/resume")
def resume_crawl(job_id: str):
    try:
        return crawler.resume(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="采集任务不存在") from exc
    except JobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


class FilterSpec(BaseModel):
    field: str
    op: str  # gt/gte/lt/lte/eq/ne/like/in
    value: object = None


class ShopsQuery(BaseModel):
    kw: str | None = None
    filters: list[FilterSpec] | None = None
    fetch_status: str | None = None   # done / fetching / empty
    sort: str = "goods_count"
    order: str = "desc"
    page: int = Field(default=1, ge=1)
    pageSize: int = Field(default=20, ge=1, le=500)


@app.post("/api/shops")
def api_shops(q: ShopsQuery, source: Source = "wzyp"):
    d = q.model_dump()
    filters = d.pop("filters", None)
    fetch_status = d.pop("fetch_status", None)
    page = d.pop("page"); size = d.pop("pageSize")
    d.pop("sort"); d.pop("order")
    with db.use_source(source):
        return db.list_shops(kw=d.get("kw"), filters=filters, fetch_status=fetch_status,
                             sort=q.sort, order=q.order, offset=(page - 1) * size, limit=size)


class GoodsQuery(BaseModel):
    shop_token: str | None = None
    goods_type: str | None = None
    kw: str | None = None
    price_min: float | None = None
    price_max: float | None = None
    filters: list[FilterSpec] | None = None
    sort: str = "price"
    order: str = "asc"
    page: int = Field(default=1, ge=1)
    pageSize: int = Field(default=20, ge=1, le=500)


@app.post("/api/goods")
def api_goods(q: GoodsQuery, source: Source = "wzyp"):
    d = q.model_dump()
    filters = d.pop("filters", None)
    page = d.pop("page"); size = d.pop("pageSize")
    d.pop("sort"); d.pop("order")
    with db.use_source(source):
        return db.list_goods(**d, filters=filters, sort=q.sort, order=q.order,
                             offset=(page - 1) * size, limit=size)


@app.get("/api/goods/{goods_key}")
def api_goods_detail(goods_key: str, source: Source = "wzyp"):
    with db.use_source(source):
        return db.goods_detail(goods_key) or {}


# ---------------- 设置 API（LLM + DB + 导入导出）----------------

@app.get("/api/llm/settings")
def get_settings():
    s = cfg.get_llm_config()
    return {"baseurl": s["baseurl"], "model": s["model"],
            "apikey": (s.get("apikey") or "")[:8] + "..." if s.get("apikey") else "",
            "has_key": bool(s.get("apikey"))}


class LLMSettings(BaseModel):
    baseurl: str = ""
    model: str = ""
    apikey: str = ""


@app.post("/api/llm/settings")
def save_settings(body: LLMSettings):
    current = cfg.get_llm_config()
    cfg.save_settings(llm={
        "baseurl": body.baseurl or current["baseurl"],
        "model": body.model or current["model"],
        "apikey": body.apikey if body.apikey and not body.apikey.endswith("...") else current.get("apikey", "")})
    return {"ok": True}


@app.get("/api/llm/test")
def test_llm():
    """LLM 连通性测试"""
    try:
        llm = get_agent().build_llm()
        r = llm.invoke("回复:OK")
        return {"ok": True, "reply": str(r.content)[:50]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


# ---- 数据库配置 ----

@app.get("/api/db/settings")
def get_db_settings():
    c = cfg.get_db_config()
    return {"host": c["host"], "port": c["port"], "user": c["user"],
            "database": c["database"],
            "password": (c.get("password") or "")[:4] + "..." if c.get("password") else "",
            "has_password": bool(c.get("password"))}


class DBSettings(BaseModel):
    host: str = ""
    port: int = 0
    user: str = ""
    password: str = ""
    database: str = ""


@app.post("/api/db/settings")
def save_db_settings(body: DBSettings):
    current = cfg.get_db_config()
    new_cfg = {
        "host": body.host or current["host"],
        "port": body.port or current["port"],
        "user": body.user or current["user"],
        "database": body.database or current["database"],
        "password": body.password if body.password and not body.password.endswith("...") else current.get("password", ""),
    }
    # 先测试新配置可用再保存
    try:
        import pymysql as _pym
        conn = _pym.connect(charset="utf8mb4", connect_timeout=8,
                            read_timeout=10, **{**new_cfg, "password": new_cfg["password"]})
        conn.close()
    except Exception as e:
        return {"ok": False, "error": f"连接失败: {str(e)[:150]}"}
    cfg.save_settings(db=new_cfg)
    db.rebuild_pool()
    return {"ok": True}


@app.get("/api/db/test")
def test_db():
    """DB 连通性测试"""
    try:
        rows = db.query("SELECT COUNT(*) AS shops FROM wzyp_shops")
        return {"ok": True, "shops": rows[0]["shops"]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


# ---- 配置导入导出 ----

@app.get("/api/config/export")
def config_export():
    """导出全部配置为 JSON（含明文密钥，仅本机使用）"""
    import json as _json
    data = cfg.load_settings()
    # settings.json 的原始内容即完整配置
    try:
        with open(cfg.SETTINGS_FILE, encoding="utf-8") as f:
            raw = _json.load(f)
        if "llm" in raw or "db" in raw:
            data = raw
    except Exception:
        pass
    return data


class ConfigImport(BaseModel):
    config: dict


@app.post("/api/config/import")
def config_import(body: ConfigImport):
    """导入配置 JSON（结构同导出）"""
    c = body.config or {}
    llm = c.get("llm") or {}
    dbc = c.get("db") or {}
    # 兼容旧平铺格式
    if not llm and any(k in c for k in ("baseurl", "model", "apikey")):
        llm = {k: c[k] for k in ("baseurl", "model", "apikey") if k in c}
    if llm:
        cfg.save_settings(llm=llm)
    if dbc:
        try:
            import pymysql as _pym
            conn = _pym.connect(charset="utf8mb4", connect_timeout=8,
                                host=dbc["host"], port=int(dbc.get("port", 3306)),
                                user=dbc["user"], password=dbc.get("password", ""),
                                database=dbc["database"])
            conn.close()
        except Exception as e:
            return {"ok": False, "error": f"DB 配置无效: {str(e)[:150]}"}
        cfg.save_settings(db=dbc)
        db.rebuild_pool()
    return {"ok": True}


# ---------------- 聊天（SSE 流式）----------------

class ChatBody(BaseModel):
    thread_id: str = "default"
    message: str


@app.post("/api/chat")
def chat(body: ChatBody, source: Source = "wzyp"):
    def gen():
        for evt, payload in get_agent().run_agent_stream(memory_id(body.thread_id, source), body.message, source=source):
            data = json.dumps({"event": evt, "data": payload}, ensure_ascii=False)
            yield f"data: {data}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/chat/history")
def chat_history(thread_id: str = "default", source: Source = "wzyp"):
    return {"messages": get_agent().memory_store.load(memory_id(thread_id, source))}


@app.post("/api/chat/clear")
def chat_clear(thread_id: str = "default", source: Source = "wzyp"):
    get_agent().memory_store.save(memory_id(thread_id, source), [])
    return {"ok": True}


@app.post("/api/chat/delete")
def chat_delete(body: dict, source: Source = "wzyp"):
    """删除单个会话（记忆 + 会话本身）"""
    tid = (body or {}).get("thread_id", "")
    if not tid:
        return {"ok": False, "error": "thread_id required"}
    deleted = get_agent().memory_store.delete(memory_id(tid, source))
    return {"ok": True, "deleted": deleted}
