"""发现任务、独立货铺和统一目录的 API；旧接口由 server 保持兼容。"""
import sqlite3
import threading
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

import catalog
import db
from crawl_store import JobConflict
from crawler16688 import CrawlManager
from discovery import DiscoveryManager, database_path as discovery_path
from platform_independent import PlatformError
from public_http import CrawlBlockedError
from sources import SOURCES, DIRECT_SOURCES, INDEPENDENT, Source, DirectSource, adapter_for, canonical_source
from yike import keyword_value
from yike_crawler import YikeManager
import local_store

class CatalogQuery(BaseModel):
    sources: list[Source] = Field(default_factory=lambda: list(SOURCES), min_length=1, max_length=len(SOURCES) + 1)
    shop_token: str | None = None
    shop_source: Source | None = None
    shop_kw: str | None = Field(default=None, max_length=200)
    goods_type: str | None = None
    kw: str | None = Field(default=None, max_length=1000)
    keywords: list[str] = Field(default_factory=list, max_length=50)
    price_min: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    price_max: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    filters: list[dict] | None = Field(default=None, max_length=50)
    currency: str | None = Field(default="CNY", pattern=r"^(?:[A-Z]{3})?$")
    delivery_method: Literal["auto", "manual", "unknown"] | None = None
    stock_status: Literal["in_stock", "out_of_stock", "unknown"] | None = None
    exclude_zero: bool = False
    sort: str = "price"
    order: Literal["asc", "desc"] = "asc"
    page: int = Field(default=1, ge=1)
    pageSize: int = Field(default=20, ge=1, le=500)

    @model_validator(mode="after")
    def price_range(self):
        if self.price_min is not None and self.price_max is not None and self.price_min > self.price_max:
            raise ValueError("最低价不能大于最高价")
        return self


class DiscoveryBody(BaseModel):
    urls: list[str] = Field(default_factory=list, max_length=1000)
    sources: list[DirectSource] = Field(default_factory=lambda: list(DIRECT_SOURCES), min_length=1, max_length=len(DIRECT_SOURCES) + 1)
    include_indexes: bool = True
    max_depth: int = Field(default=2, ge=0, le=8)
    page_limit: int = Field(default=200, ge=1, le=2000)
    parallel_sources: int = Field(default=4, ge=1, le=8)
    probe_limit: int = Field(default=200, ge=0, le=2000)


class SourceResumeBody(BaseModel):
    retry_mode: Literal["transient", "all"] = "transient"


class ResumeBody(SourceResumeBody):
    extra_pages: int = Field(default=0, ge=0, le=2000)
    extra_probes: int = Field(default=0, ge=0, le=2000)
    max_depth: int | None = Field(default=None, ge=0, le=8)


class CrawlBody(BaseModel):
    mode: Literal["discover", "refresh", "targets"] = "discover"
    urls: list[str] = Field(default_factory=list, max_length=1000)


class YikeCrawlBody(BaseModel):
    keyword: str = Field(description="手动采集的单个关键词，去除首尾空白后须为 1–200 字符")

    @model_validator(mode="after")
    def validate_keyword(self):
        self.keyword = keyword_value(self.keyword)
        return self


class SyncBody(BaseModel):
    url: str = Field(min_length=1, max_length=2048)


class Runtime:
    def __init__(self, legacy):
        self.legacy = legacy
        self.platform_managers = {}
        self.discovery_managers = {}
        self.lock = threading.RLock()

    def manager(self, source):
        source = canonical_source(source)
        if source not in INDEPENDENT | {"yike", "catfk"}:
            return self.legacy(source)
        key = (source, local_store.database_path(source))
        with self.lock:
            if key not in self.platform_managers:
                self.platform_managers[key] = YikeManager(path=key[1]) if source == "yike" else CrawlManager(path=key[1], platform_adapter=adapter_for(source))
            return self.platform_managers[key]

    def discovery(self):
        path = discovery_path()
        with self.lock:
            if path not in self.discovery_managers:
                self.discovery_managers[path] = DiscoveryManager(self.manager, path=path)
            return self.discovery_managers[path]

    def recover(self):
        for source in INDEPENDENT | {"yike", "catfk"}:
            self.manager(source).recover_interrupted()
        self.discovery().recover_interrupted()

    def shutdown(self):
        for manager in self.discovery_managers.values():
            manager.shutdown()
        for manager in self.platform_managers.values():
            manager.shutdown()


def call(function, *args, **kwargs):
    try:
        return function(*args, **kwargs)
    except JobConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, "任务或商品不存在") from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except (PlatformError, CrawlBlockedError) as exc:
        raise HTTPException(502, str(exc)) from exc
    except sqlite3.DatabaseError as exc:
        raise HTTPException(503, "本地采集库暂时不可读取，请稍后重试；原数据保持不变") from exc


def build_router(legacy):
    runtime, router = Runtime(legacy), APIRouter()

    @router.post("/api/catalog/search")
    def search(body: CatalogQuery):
        return call(catalog.search, body.model_dump())

    @router.get('/api/discovery/shops')
    def shop_previews(job_id: str = Query(min_length=1,max_length=100), source: Source | None = None,
                      page: int = Query(1,ge=1), pageSize: int = Query(20,ge=1,le=100)):
        from shop_preview import PreviewStore
        return call(PreviewStore().list,job_id,canonical_source(source) if source else None,page,pageSize)

    @router.get('/api/sources/summary')
    def source_summary():
        rows=[]
        for source in SOURCES:
            try:
                with db.use_source(source):
                    scope=' WHERE status=1' if source=='yike' else ''
                    counts=db.query('SELECT (SELECT COUNT(*) FROM wzyp_shops) AS shop_count,'
                                    '(SELECT COUNT(*) FROM wzyp_goods'+scope+') AS goods_count',one=True)
                rows.append({'id':source,**counts})
            except sqlite3.DatabaseError:
                rows.append({'id':source,'shop_count':None,'goods_count':None,'error':'快照统计暂不可用'})
        return {'sources':rows}

    @router.get("/api/catalog/goods/{source}/{goods_key}")
    def detail(source: Source, goods_key: str):
        result = call(catalog.detail, source, goods_key)
        if result is None:
            raise HTTPException(404, "该商品当前不在完整快照中，可能已下架")
        return result

    @router.post("/api/discovery/jobs")
    def start(body: DiscoveryBody):
        return call(runtime.discovery().start, **body.model_dump())

    @router.get("/api/discovery/jobs")
    def jobs(limit: int = Query(default=20, ge=1, le=100)):
        manager = runtime.discovery()
        manager.recover_interrupted()
        return {"jobs": manager.store.jobs(limit)}

    @router.get("/api/discovery/jobs/{job_id}")
    def report(job_id: str):
        manager = runtime.discovery()
        manager.recover_interrupted()
        result = manager.store.report(job_id)
        if result is None:
            raise HTTPException(404, "发现任务不存在")
        return result

    @router.get("/api/discovery/jobs/{job_id}/candidates")
    def candidates(job_id: str, page: int = Query(1, ge=1), pageSize: int = Query(50, ge=1, le=200), state: str | None = None):
        report(job_id)
        return runtime.discovery().store.candidates(job_id, page, pageSize, state)

    @router.post("/api/discovery/jobs/{job_id}/candidates/{candidate_id}/retry")
    def retry_candidate(job_id: str, candidate_id: int):
        return call(runtime.discovery().retry_candidate, job_id, candidate_id)

    @router.post("/api/discovery/jobs/{job_id}/cancel")
    def cancel(job_id: str):
        return call(runtime.discovery().cancel, job_id)

    @router.post("/api/discovery/jobs/{job_id}/resume")
    def resume(job_id: str, body: ResumeBody | None = None):
        body = body or ResumeBody()
        return call(runtime.discovery().resume, job_id, **body.model_dump())

    @router.get("/api/sources/{source}/jobs")
    def source_history(source: Source, limit: int = Query(20, ge=1, le=100)):
        manager = runtime.manager(source)
        manager.recover_interrupted()
        return {"jobs": manager.store.history(limit)}

    @router.post("/api/sources/yike/crawl")
    def yike_start(body: YikeCrawlBody):
        return call(runtime.manager("yike").start, body.keyword)

    @router.post("/api/sources/{source}/crawl")
    def source_start(source: DirectSource, body: CrawlBody):
        return call(runtime.manager(source).start, body.mode, body.urls)

    @router.get("/api/sources/{source}/crawl")
    def source_latest(source: Source):
        manager = runtime.manager(source)
        manager.recover_interrupted()
        return {"job": manager.store.report()}

    @router.get("/api/sources/{source}/crawl/{job_id}")
    def source_report(source: Source, job_id: str):
        manager = runtime.manager(source)
        manager.recover_interrupted()
        result = manager.store.report(job_id)
        if result is None:
            raise HTTPException(404, "平台采集任务不存在")
        return result

    @router.post("/api/sources/{source}/crawl/{job_id}/cancel")
    def source_cancel(source: Source, job_id: str):
        return call(runtime.manager(source).cancel, job_id)

    @router.post("/api/sources/{source}/crawl/{job_id}/resume")
    def source_resume(source: Source, job_id: str, body: SourceResumeBody | None = None):
        return call(runtime.manager(source).resume, job_id, retry_mode=(body or SourceResumeBody()).retry_mode)

    @router.post("/api/sources/{source}/sync")
    def source_sync(source: DirectSource, body: SyncBody):
        return call(runtime.manager(source).platform.sync_shop, body.url)

    return router, runtime
