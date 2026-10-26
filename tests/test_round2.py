"""Effective collection, distinct-origin budgets and selective continuation."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import sqlite3
import threading
import time
from unittest.mock import patch

from fastapi.testclient import TestClient

from crawl_failure import CollectionError, business, details, normalize
from crawl_store import JobConflict
from crawler16688 import CrawlManager
from discovery import DiscoveryManager, DiscoveryStore, SCHEMA
from hosted_shop import HostedShopPlatform
import local_store
from public_http import CrawlCancelled
from sources import catfk
import server
from test_multisource import TemporaryCatalog, FakeNative, response, next_product
from test_stability import wait_until


class RoundTwoTests(TemporaryCatalog):
    """Regression coverage for persisted version 1 workflows; new defaults are tested in test_staged_discovery."""
    def setUp(self):
        super().setUp()
        self.native = {s: FakeNative(s, local_store.database_path(s), self.web) for s in ("16688", "catfk")}
        self.next.session_factory = self.classic.session_factory = self.web.session
        self.managers = {s: CrawlManager(path=a.database_path(), platform_adapter=a,workflow_version=1)
                         for s, a in {**self.native, "dujiao-next": self.next, "dujiaoka": self.classic}.items()}
        self.detections = []
        def detect(url, stop):
            self.detections.append(url)
            return "dujiao-next" if "next.example" in url else None
        self.discovery = DiscoveryManager(self.managers, page_session_factory=self.web.session,workflow_version=1, detector=detect)
        self.gates = []

    def tearDown(self):
        for gate in self.gates:
            gate.set()
        self.discovery.shutdown()
        if self.discovery.thread:
            self.discovery.thread.join(8)
        for manager in self.managers.values():
            manager.shutdown()
            if manager.thread:
                manager.thread.join(8)
        super().tearDown()

    def finish(self, job, manager=None):
        store = (manager or self.discovery).store
        wait_until(lambda: store.report(job["job_id"])["status"] not in ("running", "cancelling"), 15)
        return store.report(job["job_id"])

    def test_standalone_known_and_local_entries_do_not_wait_for_index_or_other_platform(self):
        index_gate, platform_gate = threading.Event(), threading.Event()
        self.gates += [index_gate, platform_gate]
        self.native["16688"].gate = platform_gate
        old = self.managers["16688"].start("targets", ["https://www.16688.com.cn/shop/OLD"])
        previous = self.discovery.store.create(["https://catfk.com/shop/C1", "https://catfk.com/shop/C1#same"], ["catfk"], False)
        self.discovery.store.update_job(previous, status="cancelled")
        manager = self.managers["catfk"]
        manager.seed_urls = catfk.seed_urls
        def slow_index(session, progress):
            while not index_gate.wait(.02):
                session.check_cancel()
            return ["https://catfk.com/shop/C2"]
        manager.discover = slow_index
        job = manager.start("discover")
        wait_until(lambda: manager.store.report(job["job_id"])["success_shops"] == 1)
        self.assertEqual(self.managers["16688"].store.report(old["job_id"])["status"], "running")
        self.assertFalse(manager.store.report(job["job_id"])["discovery_done"])
        index_gate.set()
        self.assertEqual(self.finish(job, manager)["success_shops"], 2)
        self.assertEqual(self.native["catfk"].calls.count("C1"), 1)

    def test_standalone_index_failure_does_not_lose_success_and_retries_index_only(self):
        manager = self.managers["catfk"]
        manager.discover = lambda *args: (_ for _ in ()).throw(CollectionError("索引暂不可用", "network"))
        job = manager.start("discover", ["https://catfk.com/shop/C1"])
        result = self.finish(job, manager)
        self.assertEqual((result["status"], result["success_shops"]), ("partial", 1))
        self.assertIn("索引", result["discovery_error"])
        manager.discover = lambda *args: ["https://catfk.com/shop/C2"]
        result = self.finish(manager.resume(job["job_id"]), manager)
        self.assertEqual((result["status"], result["success_shops"]), ("completed", 2))
        self.assertEqual(self.native["catfk"].calls, ["C1", "C2"])

    def test_zero_probe_budget_still_collects_known_platform(self):
        job = self.discovery.start(["https://new.example/", "https://catfk.com/shop/C1"],
                                   sources=["catfk"], include_indexes=False, probe_limit=0)
        result = self.finish(job)
        self.assertEqual(result["status"], "partial")
        self.assertEqual((result["success_shops"], result["probes_used"], result["pages"]), (1, 0, 0))
        self.assertEqual(result["counts"]["probe_deferred"], 1)
        self.assertEqual(self.detections, [])

    def test_probe_budget_is_atomic_and_charged_once_per_origin(self):
        job_id = self.discovery.store.create([], ["catfk"], False, probe_limit=3)
        with ThreadPoolExecutor(8) as pool:
            reserved = list(pool.map(lambda i: self.discovery.store.reserve_probe(job_id, f"https://host{i}.example"), range(18)))
        self.assertEqual(sum(reserved), 3)
        with self.discovery.store.connection() as conn:
            origin = conn.execute("SELECT origin FROM discovery_origins WHERE job_id=? LIMIT 1", (job_id,)).fetchone()[0]
        self.assertTrue(self.discovery.store.reserve_probe(job_id, origin))
        self.assertEqual(self.discovery.store.report(job_id)["probes_used"], 3)
        self.discovery.store.update_job(job_id, status="cancelled")

    def test_probe_exhaustion_does_not_probe_more_hosts_and_resume_reuses_verdict(self):
        self.web.pages["https://directory.example/"] = '<a href="/more">同站目录</a><a href="https://next.example/">货铺</a>'
        self.web.pages["https://directory.example/more"] = '<a href="https://catfk.com/shop/C1">云猫</a>'
        self.web.products["https://next.example"].append(next_product(2))
        job = self.discovery.start(["https://directory.example/"], sources=["catfk", "dujiao-next"],
                                   include_indexes=False, probe_limit=1, page_limit=1)
        first = self.finish(job)
        self.assertEqual(first["probes_used"], 1)
        self.assertEqual(self.detections, ["https://directory.example/"])
        # A new manager proves this is persistent, rather than an in-memory cache.
        self.discovery = DiscoveryManager(self.managers, page_session_factory=self.web.session,workflow_version=1,
                                         detector=lambda url, stop: self.detections.append(url) or "dujiao-next")
        resumed = self.discovery.resume(job["job_id"], extra_probes=1, extra_pages=1)
        final = self.finish(resumed)
        self.assertEqual((final["status"], final["probes_used"], final["success_shops"], final["collected_goods"]),
                         ("completed", 2, 2, 5))
        self.assertEqual(len(self.detections), 2)
        self.assertEqual(final["sources"], ["catfk", "dujiao-next"])

    def test_known_platform_pagination_completes_after_probe_budget_is_used(self):
        self.web.products["https://next.example"].append(next_product(2))
        job = self.discovery.start(["https://next.example/", "https://unknown.example/"],
                                   sources=["dujiao-next"], include_indexes=False, probe_limit=1, page_limit=1)
        result = self.finish(job)
        self.assertEqual((result["probes_used"], result["success_shops"], result["collected_goods"]), (1, 1, 4))
        self.assertEqual(len(self.detections), 1)

    def test_homepage_read_for_detection_is_reused_for_directory_links(self):
        self.discovery.detector = None
        self.web.pages["https://directory.example/"] = '<a href="https://catfk.com/shop/C1">云猫</a>'
        job = self.discovery.start(["https://directory.example/"], sources=["catfk"], include_indexes=False)
        result = self.finish(job)
        self.assertEqual(result["success_shops"], 1)
        self.assertEqual(sum(call[1] == "https://directory.example/" for call in self.web.calls), 1)

    def test_missing_directory_page_keeps_origin_verdict_for_other_links(self):
        self.web.pages["https://directory.example/working"] = '<a href="https://catfk.com/shop/C1">云猫</a>'
        job = self.discovery.start(["https://directory.example/missing", "https://directory.example/working"],
                                   sources=["catfk"], include_indexes=False)
        result = self.finish(job)
        self.assertEqual(result["success_shops"], 1)
        self.assertEqual(len(self.detections), 1)
        self.assertEqual(result["failures"][0]["failure"]["category"], "not_found")

    def test_failed_origin_detection_runs_once_per_round_and_can_retry_on_resume(self):
        calls = []
        def detect(url, stop):
            calls.append(url)
            raise CollectionError("核验暂时超时", "network")
        self.discovery.detector = detect
        job = self.discovery.start(["https://unavailable.example/one", "https://unavailable.example/two"],
                                   sources=["catfk"], include_indexes=False)
        self.finish(job)
        self.assertEqual(len(calls), 1)
        self.finish(self.discovery.resume(job["job_id"]))
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.discovery.store.report(job["job_id"])["probes_used"], 1)

    def test_confirmed_independent_snapshot_skips_detection_uncertain_history_does_not(self):
        original = self.managers["dujiao-next"].start("targets", ["https://next.example/"])
        self.finish(original, self.managers["dujiao-next"])
        self.discovery.index_readers = {"16688": lambda *args: []}
        job = self.discovery.start([], sources=["dujiao-next"], probe_limit=0)
        self.assertEqual(self.finish(job)["success_shops"], 1)
        self.assertEqual(self.detections, [])
        # Imported records alone do not prove the adapter was verified.
        with self.managers["dujiao-next"].store.transaction() as conn:
            conn.execute("DELETE FROM crawl_visits")
        job = self.discovery.start([], sources=["dujiao-next"], probe_limit=0)
        self.assertEqual(self.finish(job)["counts"]["probe_deferred"], 1)

    def test_business_failure_does_not_pause_platform_and_resume_is_selective(self):
        original = self.native["catfk"].sync_shop
        attempts, failing = [], [True]
        def collect(url, **kwargs):
            attempts.append(url)
            if url.endswith("/MISSING") and failing[0]:
                raise CollectionError("云猫：商品不存在", failure=business("商品不存在"))
            return original(url, **kwargs)
        self.native["catfk"].sync_shop = collect
        job = self.discovery.start(["https://catfk.com/shop/MISSING", "https://catfk.com/shop/OK"],
                                   sources=["catfk"], include_indexes=False)
        result = self.finish(job)
        progress = result["source_progress"]["catfk"]
        self.assertEqual((result["success_shops"], progress["skipped"], progress["failed"]), (1, 0, 1))
        self.assertEqual(progress["failure"]["category"], "not_found")
        self.assertIn("/OK", progress["last_event"]["url"])
        self.finish(self.discovery.resume(job["job_id"]))
        self.assertEqual(len(attempts), 2)
        failing[0] = False
        result = self.finish(self.discovery.resume(job["job_id"], retry_mode="all"))
        self.assertEqual((result["status"], result["success_shops"]), ("completed", 2))
        self.assertEqual(len(attempts), 3)

    def test_platform_resume_retries_transient_only_then_all(self):
        manager = self.managers["catfk"]
        original, failing, attempts = self.native["catfk"].sync_shop, [True], []
        def collect(url, **kwargs):
            attempts.append(url)
            if failing[0]:
                raise CollectionError("商户已封禁" if url.endswith("/BANNED") else "网络暂时失败",
                                      "merchant_banned" if url.endswith("/BANNED") else "network")
            return original(url, **kwargs)
        self.native["catfk"].sync_shop = collect
        job = manager.start("targets", ["https://catfk.com/shop/BANNED", "https://catfk.com/shop/TEMP"])
        self.finish(job, manager)
        failing[0] = False
        result = self.finish(manager.resume(job["job_id"]), manager)
        self.assertEqual((result["success_shops"], result["permanent_failures"]), (1, 1))
        self.assertEqual(len(attempts), 3)
        result = self.finish(manager.resume(job["job_id"], retry_mode="all"), manager)
        self.assertEqual((result["status"], result["success_shops"]), ("completed", 2))
        self.assertEqual(len(attempts), 4)

    def test_single_retry_creates_a_new_target_job_and_keeps_original_failure(self):
        self.native["catfk"].sync_shop = lambda *args, **kwargs: (_ for _ in ()).throw(CollectionError("店铺已打烊", "shop_closed"))
        job = self.discovery.start(["https://catfk.com/shop/C1"], sources=["catfk"], include_indexes=False)
        self.finish(job)
        before = self.discovery.store.candidates(job["job_id"])["rows"][0]
        self.native["catfk"].sync_shop = self.native["catfk"].__class__.sync_shop.__get__(self.native["catfk"])
        client = TestClient(server.app)
        with patch.object(server.multisource_runtime, "discovery", return_value=self.discovery):
            response = client.post(f'/api/discovery/jobs/{job["job_id"]}/candidates/{before["id"]}/retry', json={})
        self.assertEqual(response.status_code, 200, response.text)
        retry = response.json()["job"]
        self.assertEqual(retry["mode"], "targets")
        self.assertNotEqual(retry["job_id"], before["child_job_id"])
        self.assertEqual(self.finish(retry, self.managers["catfk"])["success_shops"], 1)
        after = self.discovery.store.candidates(job["job_id"])["rows"][0]
        for key in ("state", "reason", "failure", "child_job_id"):
            self.assertEqual(after[key], before[key])
        self.assertEqual(after["retries"][0]["child_job_id"], retry["job_id"])

    def test_single_retry_respects_source_lock_and_unknown_only_prefills(self):
        job_id = self.discovery.store.create(["https://catfk.com/shop/C1", "https://unknown.example/"], ["catfk"], False)
        self.discovery.store.update_job(job_id, status="failed")
        items = self.discovery.store.candidates(job_id)["rows"]
        gate = threading.Event()
        self.gates.append(gate)
        self.native["catfk"].gate = gate
        external = self.managers["catfk"].start("targets", ["https://catfk.com/shop/OTHER"])
        client = TestClient(server.app)
        with patch.object(server.multisource_runtime, "discovery", return_value=self.discovery):
            self.assertEqual(client.post(f'/api/discovery/jobs/{job_id}/candidates/{items[0]["id"]}/retry', json={}).status_code, 409)
            response = client.post(f'/api/discovery/jobs/{job_id}/candidates/{items[1]["id"]}/retry', json={})
        self.assertEqual(response.json()["action"], "rediscover")
        self.assertEqual(response.json()["prefill"]["urls"], ["https://unknown.example/"])
        self.assertEqual(len(self.managers["catfk"].store.history()), 1)
        self.assertEqual(self.managers["catfk"].store.report(external["job_id"])["status"], "running")

    def test_cancel_drains_standalone_index_before_becoming_resumable(self):
        started, release = threading.Event(), threading.Event()
        self.gates.append(release)
        manager = self.managers["catfk"]
        def index(session, progress):
            started.set()
            release.wait(8)
            return ["https://catfk.com/shop/LATE"]
        manager.discover = index
        job = manager.start("discover", ["https://catfk.com/shop/C1"])
        self.assertTrue(started.wait(3))
        manager.cancel(job["job_id"])
        time.sleep(.15)
        self.assertEqual(manager.store.report(job["job_id"])["status"], "cancelling")
        release.set()
        result = self.finish(job, manager)
        self.assertEqual(result["status"], "cancelled")
        self.assertFalse(manager.store.rows("SELECT 1 FROM crawl_items WHERE job_id=? AND target='LATE'", (job["job_id"],)))

    def test_discovery_cancellation_drains_its_slow_owned_child(self):
        started, release = threading.Event(), threading.Event()
        self.gates.append(release)
        original = self.native["catfk"].sync_shop
        def slow_sync(url, **kwargs):
            started.set()
            release.wait(8)
            return original(url, **kwargs)
        self.native["catfk"].sync_shop = slow_sync
        job = self.discovery.start(["https://catfk.com/shop/C1"], sources=["catfk"], include_indexes=False)
        self.assertTrue(started.wait(3))
        self.discovery.cancel(job["job_id"])
        time.sleep(.2)
        self.assertEqual(self.discovery.store.report(job["job_id"])["status"], "cancelling")
        with self.assertRaises(JobConflict):
            self.discovery.resume(job["job_id"])
        release.set()
        self.assertEqual(self.finish(job)["status"], "cancelled")
        manager = self.managers["catfk"]
        self.assertFalse(manager.thread.is_alive())
        # A request to cancel that historical job must leave the newer job alone.
        newer = manager.start("targets", ["https://catfk.com/shop/C2"])
        old_id = self.discovery.store.candidates(job["job_id"])["rows"][0]["child_job_id"]
        manager.cancel(old_id)
        self.assertEqual(self.finish(newer, manager)["status"], "completed")

    def test_api_budgets_modes_and_legacy_body_compatibility(self):
        client = TestClient(server.app)
        with patch.object(server.multisource_runtime, "discovery", return_value=self.discovery):
            for invalid in (-1, 2001):
                self.assertEqual(client.post('/api/discovery/jobs', json={"probe_limit": invalid}).status_code, 422)
            job = self.discovery.start(["https://unknown.example/"], sources=["catfk"], include_indexes=False, probe_limit=0)
            self.finish(job)
            url = f'/api/discovery/jobs/{job["job_id"]}/resume'
            self.assertEqual(client.post(url, json={"retry_mode": "everything"}).status_code, 422)
            self.assertEqual(client.post(url, json={"extra_pages": 0, "extra_probes": 0}).status_code, 200)
            result = self.finish(job)
            self.assertEqual((result["probe_limit"], result["page_limit"]), (0, 200))
        with patch.object(server.crawler, "resume", return_value={"ok": True}) as resume:
            self.assertEqual(client.post('/api/sources/16688/crawl/test/resume').status_code, 200)
            resume.assert_called_with("test", retry_mode="transient")
            self.assertEqual(client.post('/api/sources/16688/crawl/test/resume', json={"retry_mode": "all"}).status_code, 200)
            resume.assert_called_with("test", retry_mode="all")

    def test_legacy_failures_are_conservative_and_closed_or_missing_are_permanent(self):
        self.assertTrue(normalize(message="未知旧错误")["retryable"])
        self.assertTrue(normalize(message="商品不存在、已下架或详情格式异常")["retryable"])
        for text in ("16688：商品不存在", "16688：该商户已封禁", "店铺已打烊", "云猫寄售：商品未上架，如有疑问请联系商家"):
            self.assertFalse(normalize(message=text)["retryable"])
        self.assertFalse(normalize({"category":"unknown","retryable":True}, "云猫寄售：商品未上架，如有疑问请联系商家")["retryable"])
        self.assertTrue(details("snapshot_changed")["retryable"])
        self.assertFalse(details("not_found")["pause_origin"])

    def test_migration_reuses_only_confirmed_legacy_origin_verdicts(self):
        path = str(self.root / "legacy.sqlite3")
        with closing(sqlite3.connect(path)) as conn, conn:
            conn.executescript(SCHEMA)
            conn.execute("INSERT INTO discovery_jobs(job_id,status,sources,include_indexes,max_depth,page_limit,created_at,updated_at,owner_pid,heartbeat) "
                         "VALUES ('old','partial','[\"dujiao-next\"]',0,2,200,'t','t',0,0)")
            for url, state, source in (("https://verified.example/", "collected", "dujiao-next"),
                                       ("https://directory.example/", "directory", None),
                                       ("https://uncertain.example/", "failed", None)):
                conn.execute("INSERT INTO discovery_candidates(job_id,url,depth,state,source,updated_at) VALUES ('old',?,0,?,?,'t')", (url, state, source))
        store = DiscoveryStore(path)
        self.assertEqual(store.origin_cache("old"), {"https://verified.example": "dujiao-next", "https://directory.example": None})
        report = store.report("old")
        self.assertEqual((report["probe_limit"], report["probes_used"], report["sources"]), (200, 0, ["dujiao-next"]))
        self.assertEqual(sum(report["counts"].values()), 3)
