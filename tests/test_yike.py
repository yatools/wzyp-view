"""一可匿名读取、按关键词增量采集、断点和原站优先的离线回归。"""
import copy
import json
import math
import os
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api_multisource
import catalog
import db
import local_store
from crawl_store import JobConflict
from public_http import CrawlBlockedError, CrawlCancelled, CurlResponse, PublicSession, RateLimiter
from sources import DIRECT_SOURCES
import yike
from yike_crawler import YikeManager


def product(guid=1, **changes):
    row = dict(guid=str(guid), key=f"key{guid}", title=f"GPT 商品 {guid}", shopGuid="10", shopKey="store",
               shopName="合成货铺", shopUrl="https://wzyp.cn/shop/store", linkUrl=f"https://wzyp.cn/item/key{guid}",
               price=10 + guid, stock=None, stockStatus="有货", goodsType="卡密", categoryName="测试分类",
               updateTime="2026-10-24 01:14:53", active=1, archived=False)
    return {**row, **changes}


def response(value, status=200):
    raw = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return CurlResponse(status, raw.encode(), {}, yike.BASE_URL)


class Web:
    def __init__(self):
        self.products = {"GPT": [product()]}
        self.details = {"1": {**product(), "desc": "<p>公开说明</p>"}}
        self.groups = {}
        self.calls = []
        self.detail_fail = set()
        self.bad_group_page = None
        self.blocked = False
        self.list_error = False
        self.pause_guid = None
        self.paused = threading.Event()
        self.closed = 0

    def add(self, *items):
        for row in items:
            self.details[row["guid"]] = {**row, "desc": "<p>完整说明</p>"}

    def session(self, stop):
        web = self
        class Session:
            anonymous = False
            def close(self): web.closed += 1
            def check_cancel(self):
                if stop.is_set(): raise CrawlCancelled()
            def get(self, url, params=None, headers=None, **kwargs):
                self.check_cancel()
                if url == yike.BASE_URL:
                    self.anonymous = True
                    return response("<html><script>const SITE_TOKEN='public-fixture-token';</script></html>")
                if url != yike.BASE_URL + "api.php": raise AssertionError("不得请求第三方商品链接")
                if not self.anonymous or headers.get("X-Site-Token") != "public-fixture-token" or headers.get("Referer") != yike.BASE_URL:
                    return response({"error": "访问被拒绝"}, 403)
                route = params["route"]
                web.calls.append(copy.deepcopy(params))
                if web.blocked: return response({"error": "访问被拒绝 token=public-fixture-token"})
                if route == "goods":
                    if web.list_error: return response({"error": "查询错误"})
                    rows = web.products.get(params["keyword"], [])
                    return response(rows[params["offset"]:params["offset"] + params["limit"]])
                if route == "goods/group":
                    rows, page, size = web.groups[params["key"]], params["page"], 2
                    items = rows[(page - 1) * size:page * size]
                    if page == web.bad_group_page: items = []
                    return response(dict(groupKey=params["key"], page=page, pageSize=size,
                                         total=len(rows), totalPages=max(1, math.ceil(len(rows) / size)), items=items))
                if route == "goods/detail":
                    guid = params["id"]
                    if guid == web.pause_guid:
                        web.paused.set()
                        if stop.wait(8): raise CrawlCancelled()
                    if guid in web.detail_fail: return response("unavailable", 503)
                    return response(web.details[guid])
                raise AssertionError("访问了未核验接口")
        return Session()


class YikeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        mapping = {"WZYP_LIVE_DB": "wzyp", "WZYP_16688_DB": "16688", "WZYP_DUJIAO_NEXT_DB": "next",
                   "WZYP_DUJIAOKA_DB": "classic", "WZYP_CATFK_DB": "catfk", "WZYP_YIKE_DB": "yike", "WZYP_DISCOVERY_DB": "discovery"}
        self.env = patch.dict(os.environ, {key: str(self.root / (name + ".sqlite3")) for key, name in mapping.items()})
        self.env.start()
        self.web = Web()
        self.manager = YikeManager(session_factory=self.web.session)

    def tearDown(self):
        self.manager.shutdown()
        if self.manager.thread: self.manager.thread.join(5)
        self.env.stop()
        self.tmp.cleanup()

    def finish(self, job):
        self.manager.thread.join(8)
        self.assertFalse(self.manager.thread.is_alive(), "任务未在预期时间退出")
        return self.manager.store.report(job["job_id"])

    def run_job(self, keyword="GPT"):
        return self.finish(self.manager.start(keyword))

    def search(self, **changes):
        return catalog.search({"sources": ["yike"], "currency": "CNY", **changes})

    def seed_direct(self, key="key1", sku="", source="wzyp-live"):
        shop = dict(token="store", nickname="原站货铺", goods_count=1, link="https://wzyp.cn/shop/store", synced_at="2026-10-24T08:00:00+08:00")
        goods = dict(goods_key=key, shop_token="store", goods_type="card", name="原站商品", price=100,
                     status=1, link="https://wzyp.cn/item/" + key, sku_label=sku, synced_at=shop["synced_at"])
        local_store.save_snapshot(shop, [goods], [], path=local_store.database_path(source))

    def test_anonymous_session_and_all_pages_expand_group_without_minimum_price(self):
        rows = [product(i) for i in range(1, 32)]
        self.web.add(*rows)
        self.web.products["GPT"] = [{**rows[0], "grouped": True, "groupKey": "s:fixture", "price": .01}, *rows[1:]]
        self.web.groups["s:fixture"] = rows[:3]
        result = self.run_job(" GPT ")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["keyword"], "GPT")
        self.assertEqual((result["list_pages"], result["group_pages"], result["matched_groups"]), (2, 2, 1))
        self.assertEqual(result["collected_goods"], 31)
        self.assertEqual(self.search()["total"], 31)
        detail = catalog.detail("yike", "1")
        self.assertEqual(detail["price"], 11)
        self.assertEqual(detail["origin_source"], "wzyp-live")
        self.assertEqual(detail["origin_goods_key"], "key1")
        self.assertEqual(detail["source_updated_at"], "2026-10-24 01:14:53")
        self.assertIn("goods/1/key1", detail["source_url"])
        self.assertNotEqual(detail["synced_at"], detail["source_updated_at"])
        self.assertEqual(self.web.closed, 1)
        self.assertEqual(sum(x["route"] == "goods/detail" for x in self.web.calls), 31)

    def test_keywords_upsert_without_replacing_shop_or_deleting_absent_results(self):
        self.run_job()
        second = product(2)
        self.web.add(second)
        self.web.products["second"] = [second]
        self.assertEqual(self.run_job("second")["status"], "completed")
        self.assertEqual(self.run_job("missing")["status"], "completed")
        self.assertEqual(self.search()["total"], 2)
        self.assertEqual(self.manager.store.rows("SELECT goods_count FROM wzyp_shops")[0]["goods_count"], 2)
        self.web.details["1"]["price"] = 999
        self.web.detail_fail.add("1")
        self.assertEqual(self.run_job()["status"], "failed")
        self.assertEqual(catalog.detail("yike", "1")["price"], 11)
        self.assertEqual(self.search()["total"], 2)

    def test_failed_details_resume_and_completed_records_are_not_reloaded(self):
        self.web.products["GPT"] = [product(), product(2)]
        self.web.add(*self.web.products["GPT"])
        self.web.detail_fail.add("2")
        result = self.run_job()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["collected_goods"], 1)
        self.web.detail_fail.clear()
        before = len(self.web.calls)
        result = self.finish(self.manager.resume(result["job_id"]))
        self.assertEqual(result["status"], "completed")
        self.assertEqual([x["id"] for x in self.web.calls[before:] if x["route"] == "goods/detail"], ["2"])
        self.assertTrue(any(x["route"] == "goods" and x["offset"] == 0 for x in self.web.calls[before:]))

    def test_changed_successful_quote_is_refreshed_during_resume(self):
        self.web.products["GPT"] = [product(), product(2)]
        self.web.add(*self.web.products["GPT"])
        self.web.detail_fail.add("2")
        result = self.run_job()
        self.web.products["GPT"][0]["price"] = 42
        self.web.details["1"]["price"] = 42
        self.web.detail_fail.clear()
        self.finish(self.manager.resume(result["job_id"]))
        self.assertEqual(catalog.detail("yike", "1")["price"], 42)

    def test_missing_group_page_cannot_complete_and_can_resume(self):
        items = [product(i) for i in (1, 2, 3)]
        self.web.add(*items)
        self.web.products["GPT"] = [{**items[0], "grouped": True, "groupKey": "s:fixture"}]
        self.web.groups["s:fixture"] = items
        self.web.bad_group_page = 2
        result = self.run_job()
        self.assertEqual(result["status"], "partial")
        self.assertIn("缺页", result["discovery_error"])
        self.assertEqual(self.search()["total"], 2)
        self.web.bad_group_page = None
        self.assertEqual(self.finish(self.manager.resume(result["job_id"]))["status"], "completed")
        self.assertEqual(self.search()["total"], 3)

    def test_cancel_conflict_and_resume_preserve_complete_items(self):
        self.web.products["GPT"] = [product(), product(2)]
        self.web.add(*self.web.products["GPT"])
        self.web.pause_guid = "2"
        job = self.manager.start("GPT")
        self.assertTrue(self.web.paused.wait(5))
        with self.assertRaises(JobConflict): self.manager.start("different")
        self.manager.cancel(job["job_id"])
        result = self.finish(job)
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(self.search()["total"], 1)
        self.web.pause_guid = None
        self.assertEqual(self.finish(self.manager.resume(job["job_id"]))["status"], "completed")

    def test_recover_dead_owner_requeues_working_item(self):
        job_id = self.manager.store.create("GPT")
        self.manager.store.enqueue(job_id, [product()])
        self.manager.store.claim(job_id)
        with patch("yike_crawler.process_alive", return_value=False): self.manager.recover_interrupted()
        self.assertEqual(self.manager.store.report(job_id)["status"], "interrupted")
        self.assertEqual(self.finish(self.manager.resume(job_id))["status"], "completed")

    def test_rejected_api_interrupts_and_malformed_list_does_not_complete(self):
        self.web.blocked = True
        result = self.run_job()
        self.assertEqual(result["status"], "interrupted")
        self.assertNotIn("public-fixture-token", json.dumps(result))
        self.web.blocked, self.web.list_error = False, True
        self.assertEqual(self.run_job()["status"], "failed")

    def test_missing_description_and_wrong_identity_preserve_old_snapshot(self):
        self.run_job()
        self.web.details["1"] = product(price=999)
        self.assertEqual(self.run_job()["status"], "failed")
        self.web.details["1"] = {**product(2, price=999), "desc": "错误编号"}
        self.assertEqual(self.run_job()["status"], "failed")
        self.assertEqual(catalog.detail("yike", "1")["price"], 11)

    def test_explicit_archival_hides_quote_but_keeps_detail_and_unrelated_goods(self):
        self.web.products["GPT"] = [product(), product(2)]
        self.web.add(*self.web.products["GPT"])
        self.run_job()
        self.web.details["1"]["archived"] = True
        self.run_job()
        self.assertEqual(self.search()["total"], 1)
        self.assertEqual(catalog.detail("yike", "1")["status"], 0)
        with db.use_source("yike"):
            self.assertEqual(db.shop_stats()["goods_count"], 1)
            self.assertEqual(db.list_goods()["total"], 1)

    def test_unknown_values_are_not_zero_and_unsupported_delivery_stays_unknown(self):
        self.web.details["1"].update(price=None, stock=-1, stockStatus="未公开")
        self.run_job()
        row = self.search()["rows"][0]
        self.assertIsNone(row["price"])
        self.assertIsNone(row["stock_quantity"])
        self.assertEqual(row["stock_status"], "unknown")
        self.assertEqual(row["delivery_method"], "unknown")

    def test_explicit_archived_list_retracts_known_quote_even_if_detail_is_gone(self):
        self.run_job()
        self.web.products["GPT"][0]["archived"] = True
        self.web.detail_fail.add("1")
        self.assertEqual(self.run_job()["status"], "failed")
        self.assertEqual(self.search()["total"], 0)
        self.assertEqual(catalog.detail("yike", "1")["status"], 0)
        self.assertIn("公开说明", catalog.detail("yike", "1")["description"])

    def test_repeated_list_page_reports_partial_instead_of_looping_or_completing(self):
        self.web.products["GPT"] = [product()] * 60
        result = self.run_job()
        self.assertEqual(result["status"], "partial")
        self.assertIn("分页重复", result["discovery_error"])

    def test_duplicate_group_pages_do_not_report_complete(self):
        self.web.products["GPT"] = [{**product(), "grouped": True, "groupKey": "s:fixture"}]
        self.web.groups["s:fixture"] = [product(), product(2), product()]
        self.web.add(product(2))
        result = self.run_job()
        self.assertEqual(result["status"], "partial")
        self.assertIn("分页重复", result["discovery_error"])

    def test_missing_aggregate_flag_is_not_treated_as_complete_single_quote(self):
        self.web.products["GPT"] = [{**product(), "groupKey": "s:fixture", "productCount": 9}]
        result = self.run_job()
        self.assertEqual(result["status"], "failed")
        self.assertIn("聚合标记", result["discovery_error"])

    def test_native_preference_precedes_filters_pagination_and_counts(self):
        items = [product(), product(2), product(3, linkUrl="https://www.16688.com.cn/goods/key1"),
                 product(4, linkUrl="https://unknown.example/buy/1"), product(5, linkUrl="https://wzyp.cn/item/key1", skuLabel="年卡")]
        self.web.products["GPT"] = items
        self.web.add(*items)
        self.run_job()
        self.seed_direct()
        sources = ["yike", "wzyp-live"]
        result = self.search(sources=sources, pageSize=2)
        self.assertEqual(result["total"], 5)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(sum(x["count"] for x in result["source_counts"]), 5)
        all_rows = self.search(sources=sources, pageSize=100)["rows"]
        self.assertFalse(any(x["source"] == "yike" and x["goods_key"] == "1" for x in all_rows))
        self.assertTrue(any(x["source"] == "yike" and x["goods_key"] == "5" for x in all_rows))
        self.assertEqual(self.search(sources=sources, price_max=11)["total"], 0)
        self.assertEqual(self.search(sources=sources, keywords=["GPT 商品 1"])["total"], 0)
        self.assertEqual(self.search()["total"], 5)

    def test_known_native_sku_does_not_hide_unspecified_aggregate_sku(self):
        self.run_job()
        self.seed_direct(sku="月卡")
        self.assertEqual(self.search(sources=["yike", "wzyp-live"])["total"], 2)

    def test_ai_readonly_access_excludes_jobs_and_keeps_source_metadata(self):
        self.run_job()
        with db.use_source("yike"):
            self.assertIn("source_updated_at", db.schema_text())
            self.assertEqual(db.goods_detail("1")["source_updated_at"], "2026-10-24 01:14:53")
            self.assertIn("error", db.run_readonly_sql("SELECT * FROM yike_jobs"))
            self.assertIn("error", db.run_readonly_sql("DELETE FROM wzyp_goods"))
            self.assertEqual(db.list_goods()["total"], 1)

    def test_api_keyword_validation_status_history_and_discovery_separation(self):
        router, runtime = api_multisource.build_router(lambda source: None)
        runtime.platform_managers[("yike", local_store.database_path("yike"))] = self.manager
        app = FastAPI()
        app.include_router(router)
        with TestClient(app) as client:
            for body in ({}, {"keyword": "  "}, {"keyword": "a" * 201}, {"keyword": None}):
                self.assertEqual(client.post("/api/sources/yike/crawl", json=body).status_code, 422)
            result = client.post("/api/sources/yike/crawl", json={"keyword": " GPT "})
            self.assertEqual(result.status_code, 200)
            job = self.finish(result.json())
            self.assertEqual(job["keyword"], "GPT")
            self.assertEqual(client.get("/api/sources/yike/crawl").json()["job"]["job_id"], job["job_id"])
            self.assertEqual(client.get(f"/api/sources/yike/crawl/{job['job_id']}").status_code, 200)
            self.assertEqual(client.get("/api/sources/yike/jobs").json()["jobs"][0]["keyword"], "GPT")
            self.assertEqual(client.get("/api/sources/yike/crawl/missing").status_code, 404)
            self.assertEqual(client.post("/api/sources/yike/crawl/missing/cancel", json={}).status_code, 404)
            self.assertEqual(client.post("/api/sources/yike/crawl/missing/resume", json={}).status_code, 404)
            self.assertEqual(client.post("/api/discovery/jobs", json={"sources": ["yike"]}).status_code, 422)
            self.assertEqual(client.post("/api/sources/yike/sync", json={"url": "https://vs.yiketools.com/"}).status_code, 422)
            self.assertEqual(client.post("/api/catalog/search", json={"sources": ["yike"]}).json()["total"], 1)
            self.assertEqual(client.get("/api/catalog/goods/yike/1").json()["source_updated_at"], "2026-10-24 01:14:53")
        self.assertEqual(set(api_multisource.DiscoveryBody().sources), set(DIRECT_SOURCES))
        self.assertIn("yike", api_multisource.CatalogQuery().sources)


class AnonymousCookieTests(unittest.TestCase):
    def test_real_public_session_keeps_cookie_and_attaches_public_token(self):
        requests_seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                if self.path == "/":
                    self.send_response(200)
                    self.send_header("Set-Cookie", "anonymous=fixture; HttpOnly; Path=/")
                    self.end_headers()
                    self.wfile.write(b"<script>const SITE_TOKEN='anonymous-token';</script>")
                else:
                    valid = self.headers.get("Cookie") == "anonymous=fixture" and self.headers.get("X-Site-Token") == "anonymous-token"
                    requests_seen.append(valid)
                    self.send_response(200 if valid else 403)
                    self.end_headers()
                    self.wfile.write(b"[]" if valid else b'{"error":"denied"}')
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            origin = f"http://127.0.0.1:{server.server_port}/"
            with patch("yike.BASE_URL", origin):
                with yike.YikeClient(threading.Event(), lambda stop: PublicSession(stop=stop, limiter=RateLimiter(rate=0))) as client:
                    self.assertEqual(client.products("GPT", 0), [])
            self.assertEqual(requests_seen, [True])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


if __name__ == "__main__":
    unittest.main()
