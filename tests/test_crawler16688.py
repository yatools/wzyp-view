import gzip
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from collections import Counter
from unittest.mock import patch

import requests
from fastapi.testclient import TestClient

import db
import local_store
import platform16688 as platform
import server
from crawl_store import CrawlStore, JobConflict
from crawler16688 import CrawlManager, INDEX_MANIFEST, INDEX_ORIGIN, discover_public
from public_http import CrawlCancelled, PublicSession, RateLimiter, retry_delay
from test_16688 import sample_goods

VERSION = "a" * 24
GENERATED = "2026-10-23T03:00:00+00:00"


def response(data, status=200, compressed=False):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(data).encode()
    if compressed:
        result._content = gzip.compress(result._content)
    result._content_consumed = True
    return result


def manifest(version=VERSION, count=2, pages=2):
    return {"schema": 1, "version": version, "generated_at": GENERATED,
            "offers": {"count": count, "pages": [f"/data/v/{version}/offers-{i}.json.gz" for i in range(pages)]}}


def page(offers, version=VERSION):
    return {"version": version, "generated_at": GENERATED, "offers": offers}


def offer(shop, goods):
    return {"sources": {"base_url": f"http://16688.com.cn/shop/{shop}/?tracking=1"},
            "purchase_url": f"https://www.16688.com.cn/goods/{goods}?tracking=1", "price": 99999}


class IndexSession:
    def __init__(self, routes):
        self.routes = {url: value[:] if isinstance(value, list) else value for url, value in routes.items()}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        value = self.routes[url]
        if isinstance(value, list):
            return value.pop(0) if len(value) > 1 else value[0]
        return value


def index_routes():
    return {INDEX_MANIFEST: response(manifest()),
            INDEX_ORIGIN + manifest()["offers"]["pages"][0]: response(page([offer("ALPHA", "G1")]), compressed=True),
            INDEX_ORIGIN + manifest()["offers"]["pages"][1]: response(page([offer("BETA", "G2")]), compressed=True)}


class DiscoveryTests(unittest.TestCase):
    def test_reads_all_pages_normalizes_urls_and_only_uses_links(self):
        progress = []
        session = IndexSession(index_routes())
        urls = discover_public(session, lambda version, pages: progress.append(pages))
        self.assertEqual(urls, sorted([platform.BASE_URL + path for path in
                                     ["/shop/ALPHA", "/goods/G1", "/shop/BETA", "/goods/G2"]]))
        self.assertEqual(progress, [0, 1, 2])
        self.assertEqual(len(session.calls), 3)

    def test_version_change_restarts_from_new_manifest_without_mixing_urls(self):
        next_version = "b" * 24
        old = manifest(count=1, pages=1)
        new = manifest(next_version, count=1, pages=1)
        routes = {INDEX_MANIFEST: [response(old), response(new)],
                  INDEX_ORIGIN + old["offers"]["pages"][0]: response(page([offer("OLD", "G0")], next_version)),
                  INDEX_ORIGIN + new["offers"]["pages"][0]: response(page([offer("NEW", "G9")], next_version))}
        urls = discover_public(IndexSession(routes))
        self.assertTrue(all("OLD" not in url and "G0" not in url for url in urls))
        self.assertIn(platform.BASE_URL + "/shop/NEW", urls)

    def test_missing_page_and_incomplete_counts_fail_without_partial_results(self):
        for status, count in [(404, 2), (200, 10)]:
            routes = index_routes()
            routes[INDEX_MANIFEST] = response(manifest(count=count))
            if status == 404:
                routes[INDEX_ORIGIN + manifest()["offers"]["pages"][1]] = response({}, 404)
            with self.subTest(status=status), self.assertRaises(platform.PlatformError):
                discover_public(IndexSession(routes))

    def test_rejects_foreign_page_urls_and_malformed_manifests(self):
        for bad in [None, {"schema": 2}, {**manifest(), "offers": ["invalid"]},
                    {**manifest(), "offers": {"count": 1, "pages": ["https://localhost/private"]}}]:
            session = IndexSession({INDEX_MANIFEST: response(bad)})
            with self.subTest(bad=bad), self.assertRaises(platform.PlatformError):
                discover_public(session)
            self.assertEqual(set(session.calls), {INDEX_MANIFEST})

    def test_descriptions_only_follow_valid_16688_links(self):
        html = ('<a href="/shop/ALPHA?x=1">A</a><a href="http://16688.com.cn/goods/G1#x">B</a>'
                '<a href="https://evil.test/shop/a">C</a><a href="javascript:alert(1)">D</a>'
                '<a href="/merchant/login">E</a><a href="/shop/ALPHA/">A again</a>')
        self.assertEqual(platform.description_links(html), [platform.BASE_URL + "/goods/G1", platform.BASE_URL + "/shop/ALPHA"])


class NetworkModel:
    def __init__(self):
        self.aliases = {"ALPHA": "S1", "BETA": "S2", "GAMMA": "S3", "S1": "S1", "S2": "S2", "S3": "S3"}
        self.items = {"S1": [sample_goods("G1", 10)], "S2": [sample_goods("G2", 20)], "S3": [sample_goods("G3", 30)]}
        self.calls = Counter()
        self.failed = set()
        self.blocked = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()

    def factory(self, stop):
        model = self

        class Session:
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def check_cancel(self):
                if stop.is_set():
                    raise CrawlCancelled()

            def post(self, url, json=None, **kwargs):
                self.check_cancel()
                endpoint = url.split("/shopApi/")[-1]
                token = model.aliases.get(json.get("shop_no"))
                with model.lock:
                    model.calls[endpoint, token or json.get("goods_no")] += 1
                if endpoint == "goods/detail":
                    for shop_no, items in model.items.items():
                        for item in items:
                            if item["goods_no"] == json.get("goods_no"):
                                return response({"code": 1, "data": {**item, "shop_no": shop_no}})
                    return response({"code": 0, "msg": "商品已失效"})
                if not token:
                    return response({"code": 0, "msg": "店铺不存在"})
                if endpoint == "shop/detail":
                    alias = {"S1": "ALPHA", "S2": "BETA", "S3": "GAMMA"}[token]
                    return response({"code": 1, "data": {"shop_no": token, "shop_alias": alias,
                                    "name": "测试店铺 " + token, "merchant": {"sales_count_text": "12人付款"}}})
                if endpoint == "goods/list":
                    if token in model.failed:
                        raise requests.Timeout()
                    if token == model.blocked:
                        model.entered.set()
                        while not model.release.wait(0.01):
                            self.check_cancel()
                    self.check_cancel()
                    return response({"code": 1, "data": {"list": model.items[token]}})
                if endpoint == "goods_category/list":
                    return response({"code": 1, "data": []})
                raise AssertionError(endpoint)

        return Session()


class CrawlerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.temp.name, "crawl.sqlite3")
        self.env = patch.dict(os.environ, {"WZYP_16688_DB": self.path,"WZYP_DISCOVERY_DB": os.path.join(self.temp.name,"discovery.sqlite3")})
        self.env.start()
        self.network = NetworkModel()
        self.discoveries = 0

        def discover(session, progress):
            self.discoveries += 1
            progress(VERSION, 2)
            return discover_public(IndexSession(index_routes()))

        self.manager = CrawlManager(self.path, self.network.factory, discover, workflow_version=1) # legacy checkpoint coverage

    def tearDown(self):
        self.network.release.set()
        self.manager.shutdown()
        if self.manager.thread:
            self.manager.thread.join(5)
        self.env.stop()
        self.temp.cleanup()

    def finished(self, job_id):
        self.manager.thread.join(5)
        self.assertFalse(self.manager.thread.is_alive(), "后台任务未在测试时间内结束")
        return self.manager.store.report(job_id)

    def until(self, predicate):
        deadline = time.monotonic() + 3
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("未等到预期采集状态")
            threading.Event().wait(0.01)

    def test_discovery_collects_multiple_shops_expands_links_and_deduplicates_aliases(self):
        self.network.items["S1"][0]["description"] = '<a href="/shop/GAMMA">其他店铺</a>'
        self.network.items["S3"][0]["description"] = '<a href="/shop/ALPHA">循环链接</a>'
        started = self.manager.start(urls=[platform.BASE_URL + "/shop/S1"])
        result = self.finished(started["job_id"])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["success_shops"], 3)
        self.assertEqual(result["collected_goods"], 3)
        self.assertEqual(result["pending_links"], 0)
        for shop in ("S1", "S2", "S3"):
            self.assertEqual(self.network.calls["goods/list", shop], 1)
        with db.use_source("16688"):
            self.assertEqual(db.shop_stats()["goods_count"], 3)
            self.assertEqual(db.list_goods(price_max=15)["total"], 1)
            self.assertEqual(db.goods_detail("G1")["price"], 10)  # 第三方索引价格不入库。
            self.assertIn("error", db.run_readonly_sql("SELECT * FROM crawl_jobs"))
            self.assertIn("error", db.run_readonly_sql("SELECT * FROM crawl_items"))

    def test_one_failed_shop_does_not_stop_other_shops_and_resume_only_retries_failure(self):
        self.network.failed.add("S2")
        result = self.finished(self.manager.start()["job_id"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual((result["success_shops"], result["failed_shops"]), (1, 1))
        self.assertTrue(result["failures"])
        self.network.failed.clear()
        self.manager.resume(result["job_id"])
        resumed = self.finished(result["job_id"])
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["success_shops"], 2)
        self.assertEqual(self.network.calls["goods/list", "S1"], 1)
        self.assertEqual(self.discoveries, 1)

    def test_cancel_and_resume_preserve_completed_shops(self):
        self.network.blocked = "S2"
        result = self.manager.start()
        self.assertTrue(self.network.entered.wait(3))
        self.until(lambda: self.manager.store.report(result["job_id"])["success_shops"] >= 1)
        self.manager.cancel(result["job_id"])
        cancelled = self.finished(result["job_id"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertGreater(cancelled["pending_links"], 0)
        self.network.release.set()
        self.manager.resume(result["job_id"])
        resumed = self.finished(result["job_id"])
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(self.network.calls["goods/list", "S1"], 1)

    def test_cancel_during_discovery_restarts_discovery_on_resume(self):
        entered, release = threading.Event(), threading.Event()
        original = self.manager.discover

        def discovery(session, progress):
            entered.set()
            while not release.wait(0.01):
                session.check_cancel()
            return original(session, progress)

        self.manager.discover = discovery
        result = self.manager.start()
        self.assertTrue(entered.wait(3))
        self.manager.cancel(result["job_id"])
        cancelled = self.finished(result["job_id"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertFalse(cancelled["discovery_done"])
        self.assertEqual(cancelled["success_shops"], 0)
        release.set()
        self.manager.resume(result["job_id"])
        self.assertEqual(self.finished(result["job_id"])["success_shops"], 2)

    def test_shutdown_marks_interrupted_and_can_resume_with_new_manager(self):
        self.network.blocked = "S2"
        result = self.manager.start()
        self.assertTrue(self.network.entered.wait(3))
        self.manager.shutdown()
        self.assertEqual(self.finished(result["job_id"])["status"], "interrupted")
        self.network.release.set()
        self.manager = CrawlManager(self.path, self.network.factory)
        self.manager.resume(result["job_id"])
        self.assertEqual(self.finished(result["job_id"])["status"], "completed")

    def test_recovery_reclaims_dead_process_but_not_a_live_owner(self):
        job_id, _ = self.manager.store.create("refresh", [])
        with patch("crawl_store.process_alive", return_value=True):
            self.manager.recover_interrupted()
            self.assertEqual(self.manager.store.report(job_id)["status"], "running")
        with patch("crawl_store.process_alive", return_value=False):
            self.manager.recover_interrupted()
        self.assertEqual(self.manager.store.report(job_id)["status"], "interrupted")
        self.manager.resume(job_id)
        self.assertEqual(self.finished(job_id)["status"], "completed")

    def test_second_manager_returns_existing_job_and_cannot_start_parallel_job(self):
        self.network.blocked = "S2"
        result = self.manager.start()
        self.assertTrue(self.network.entered.wait(3))
        other = CrawlManager(self.path, self.network.factory)
        existing = other.start("refresh")
        self.assertTrue(existing["reused"])
        self.assertEqual(existing["job_id"], result["job_id"])
        self.assertIsNone(other.thread)
        self.manager.cancel(result["job_id"])
        self.finished(result["job_id"])

    def test_refresh_uses_known_shops_without_contacting_index(self):
        self.finished(self.manager.start()["job_id"])
        self.manager.discover = lambda *args: self.fail("刷新不应读取公开索引")
        result = self.finished(self.manager.start("refresh")["job_id"])
        self.assertEqual(result["success_shops"], 2)
        self.assertEqual(result["status"], "completed")

    def test_discovery_failure_still_refreshes_known_shops_and_is_visible(self):
        self.finished(self.manager.start()["job_id"])

        def fail(*args):
            raise platform.PlatformError("索引失效")

        self.manager.discover = fail
        result = self.finished(self.manager.start()["job_id"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["success_shops"], 2)
        self.assertEqual(result["discovery_error"], "索引失效")

    def test_invalid_product_is_reported_without_aborting_valid_shops(self):
        result = self.finished(self.manager.start(urls=[platform.BASE_URL + "/goods/Gmissing"])["job_id"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["success_shops"], 2)
        self.assertEqual(result["failed_links"], 1)

    def test_ai_tool_graph_aggregates_across_collected_shops(self):
        import agent
        from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
        from langchain_core.messages import AIMessage

        class ToolModel(FakeMessagesListChatModel):
            def bind_tools(self, tools):
                return self

        self.finished(self.manager.start()["job_id"])
        model = ToolModel(responses=[
            AIMessage(content="", tool_calls=[{"name": "data_schema", "args": {}, "id": "schema"}]),
            AIMessage(content="", tool_calls=[{"name": "sql_query", "args": {"query":
                "SELECT s.token,COUNT(g.goods_key) AS n FROM wzyp_shops s "
                "JOIN wzyp_goods g ON g.shop_token=s.token GROUP BY s.token ORDER BY s.token"}, "id": "sql"}]),
            AIMessage(content="S1 和 S2 各有 1 件公开商品。"),
        ])
        with patch.object(agent, "build_llm", return_value=model), \
             patch.object(agent, "suggest_followups", return_value=[]), \
             patch.object(agent, "memory_store", agent.MemoryStore(os.path.join(self.temp.name, "memory.json"))):
            output = list(agent.run_agent_stream("multi-store", "比较两个店铺", source="16688"))
            self.assertNotIn("error", [event for event, _ in output])
            previews = [data["preview"] for event, data in output if event == "tool" and data["tool"] == "sql_query"]
            self.assertEqual(len(previews), 1)
            self.assertIn("S1", previews[0])
            self.assertIn("S2", previews[0])

    def test_api_start_progress_cancel_resume_validation_and_lifespan(self):
        self.network.blocked = "S2"
        with patch.object(server, "crawler", self.manager), TestClient(server.app) as client:
            self.assertIsNone(client.get("/api/sources/16688/crawl").json()["job"])
            self.assertEqual(client.post("/api/sources/16688/crawl", json={"mode": "wrong"}).status_code, 422)
            self.assertEqual(client.post("/api/sources/16688/crawl", json={"urls": ["https://localhost/shop/a"]}).status_code, 400)
            job = client.post("/api/sources/16688/crawl", json={}).json()
            self.assertTrue(self.network.entered.wait(3))
            job_id = job["job_id"]
            self.assertEqual(client.get(f"/api/sources/16688/crawl/{job_id}").status_code, 200)
            self.assertEqual(client.post(f"/api/sources/16688/crawl/{job_id}/cancel").status_code, 200)
            self.assertEqual(self.finished(job_id)["status"], "cancelled")
            self.network.release.set()
            self.assertEqual(client.post(f"/api/sources/16688/crawl/{job_id}/resume").status_code, 200)
            self.assertEqual(self.finished(job_id)["status"], "completed")
            self.assertEqual(client.post(f"/api/sources/16688/crawl/{job_id}/resume").status_code, 409)
            for suffix in ["", "/cancel", "/resume"]:
                operation = client.get if not suffix else client.post
                self.assertEqual(operation("/api/sources/16688/crawl/missing" + suffix).status_code, 404)
            self.assertEqual(client.post("/api/shops?source=16688", json={}).json()["total"], 2)


class MigrationTests(unittest.TestCase):
    def test_adding_job_tables_preserves_old_snapshot_and_keeps_them_out_of_ai(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "old.sqlite3")
            conn = sqlite3.connect(path)
            conn.executescript(local_store.SCHEMA.split("CREATE TABLE IF NOT EXISTS crawl_jobs")[0])
            conn.execute("INSERT INTO wzyp_shops(token,nickname,goods_count,link,synced_at) "
                         "VALUES ('SOLD','旧店铺',0,'https://www.16688.com.cn/shop/SOLD','2026-10-01')")
            conn.commit()
            conn.close()
            store = CrawlStore(path)
            self.assertEqual(store.rows("SELECT nickname FROM wzyp_shops")[0]["nickname"], "旧店铺")
            self.assertEqual(store.rows("SELECT COUNT(*) AS n FROM crawl_jobs")[0]["n"], 0)
            with patch.dict(os.environ, {"WZYP_16688_DB": path}), db.use_source("16688"):
                self.assertIn("error", db.run_readonly_sql("SELECT * FROM crawl_jobs"))
                self.assertEqual(db.shop_stats()["shop_count"], 1)


class FakeClockStop:
    def __init__(self, clock):
        self.clock = clock
        self.cancelled = False

    def is_set(self):
        return self.cancelled

    def wait(self, delay):
        self.clock[0] += delay
        return self.cancelled


class HttpTests(unittest.TestCase):
    def test_shared_limiter_spaces_all_requests_and_honors_cancellation(self):
        clock = [0.0]
        stop = FakeClockStop(clock)
        limiter = RateLimiter(2, clock=lambda: clock[0])
        times = []
        for _ in range(4):
            limiter.wait(stop)
            times.append(clock[0])
        self.assertEqual(times, [0, 0.5, 1.0, 1.5])
        stop.cancelled = True
        with self.assertRaises(CrawlCancelled):
            limiter.wait(stop)

    def test_transient_http_failure_retries_and_honors_retry_after(self):
        clock = [0.0]
        stop = FakeClockStop(clock)
        session = PublicSession(stop=stop, limiter=RateLimiter(2, clock=lambda: clock[0]))
        limited = response({}, 429)
        limited.headers["Retry-After"] = "4"
        with patch.object(requests.Session, "request", side_effect=[limited, response({}, 503), response({"ok": True})]) as request:
            self.assertEqual(session.get(INDEX_MANIFEST).status_code, 200)
            self.assertEqual(request.call_count, 3)
            self.assertGreaterEqual(clock[0], 6)
            self.assertFalse(request.call_args.kwargs["allow_redirects"])

    def test_network_timeout_attempts_are_bounded_and_404_is_not_retried(self):
        clock = [0.0]
        session = PublicSession(stop=FakeClockStop(clock), limiter=RateLimiter(0, clock=lambda: clock[0]))
        with patch.object(requests.Session, "request", side_effect=requests.Timeout()) as request:
            with self.assertRaises(requests.Timeout):
                session.get(INDEX_MANIFEST)
            self.assertEqual(request.call_count, 3)
        with patch.object(requests.Session, "request", return_value=response({}, 404)) as request:
            self.assertEqual(session.get(INDEX_MANIFEST).status_code, 404)
            self.assertEqual(request.call_count, 1)

    def test_retry_after_is_bounded(self):
        limited = response({}, 429)
        for text, expected in [("300", 60), ("0", 0), ("bad", 2)]:
            limited.headers["Retry-After"] = text
            self.assertEqual(retry_delay(limited, 1), expected)


if __name__ == "__main__":
    unittest.main()
