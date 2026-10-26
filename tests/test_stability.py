"""Source identity, fair concurrent scheduling and bounded automatic recovery."""
import asyncio
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

import requests
from fastapi.testclient import TestClient

import browser_transport
import catalog
from crawler16688 import CrawlManager
from discovery import DiscoveryManager
from hosted_http import HostedSession, TransportScope
import local_store
import platformwzyp
from public_http import CrawlBlockedError, CrawlCancelled, CurlResponse, RateLimiter, wait_for_request
from sources import catfk, native_source
import server
from test_multisource import TemporaryCatalog, FakeNative, IndependentPlatform
from test_wzyp_and_llm import FakeWzypSession, make_open_shop


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.02)
    raise AssertionError("离线任务未在期限内达到预期状态")


class HostedShopTests(TemporaryCatalog):
    def session(self, count=21):
        rows = [{"goods_key": f"G{i}", "name": f"公开商品{i}", "price": 10} for i in range(count)]
        pages = {page + 1: {"total": count, "list": rows[start:start+20]}
                 for page, start in enumerate(range(0, count, 20))}
        return FakeWzypSession({"SAME": make_open_shop("SAME", goods_pages={"card": pages},
                                                       categories={"card": [{"id": 1, "name": "会员"}]})})

    def test_parallel_hosted_sources_never_exchange_domain_or_identity(self):
        def sync(adapter):
            session = self.session()
            original = session.post
            def post(url, **kwargs):
                self.assertTrue(url.startswith(adapter.BASE_URL + "/shopApi/"))
                return original(url, **kwargs)
            session.post = post
            return adapter.sync_shop(adapter.BASE_URL + "/shop/SAME", session=session)
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(sync, [platformwzyp, catfk]))
        self.assertEqual([result["goods_count"] for result in results], [21, 21])
        result = catalog.search({"sources": ["wzyp-live", "catfk"], "pageSize": 100})
        self.assertEqual(result["total"], 42)
        self.assertEqual({row["source"] for row in result["rows"]}, {"wzyp-live", "catfk"})
        self.assertTrue(all(row["link"].startswith("https://catfk.com/item/") for row in result["rows"] if row["source"] == "catfk"))
        with self.assertRaises(AttributeError):
            catfk.BASE_URL = "https://wzyp.cn"

    def test_missing_cloud_page_preserves_old_shop_and_details(self):
        catfk.sync_shop("https://catfk.com/shop/SAME", session=self.session(1))
        session = self.session(21)
        del session.shops_data["SAME"]["goods_pages"]["card"][2]
        with self.assertRaises(catfk.PlatformError):
            catfk.sync_shop("https://catfk.com/shop/SAME", session=session)
        rows = local_store.readonly_query("SELECT goods_key FROM wzyp_goods", path=catfk.database_path())
        self.assertEqual(rows, [{"goods_key": "G0"}])
        self.assertEqual(len(local_store.readonly_query("SELECT * FROM wzyp_goods_detail", path=catfk.database_path())), 1)

    def test_aliases_registry_cloud_routes_and_ai_use_the_correct_source(self):
        for host in ("wzyp.cn", "shop.wzyp.cn", "pay.ldxp.cn", "www.ldxp.cn"):
            self.assertEqual(native_source(f"https://{host}/shop/SAME"), "wzyp-live")
        self.assertEqual(native_source("https://catfk.com/item/G0"), "catfk")
        self.assertIsNone(native_source("https://catfk.com.evil.example/shop/SAME"))
        catfk.sync_shop("https://catfk.com/shop/SAME", session=self.session(1))
        client = TestClient(server.app)
        source = next(item for item in client.get("/api/sources").json()["sources"] if item["id"] == "catfk")
        self.assertEqual(source["family"], "hosted")
        self.assertTrue(source["discovery"])
        self.assertEqual(client.get("/api/catalog/goods/catfk/G0").status_code, 200)
        self.assertEqual(client.get("/api/stats?source=catfk").json()["stats"]["goods_count"], 1)
        from agent import _source_note_for
        self.assertIn("云猫寄售", _source_note_for("catfk"))
        from yike import origin_identity
        self.assertEqual(origin_identity("https://catfk.com/item/G0"), ("catfk", "G0"))


class ConcurrentDiscoveryTests(TemporaryCatalog):
    """Regression coverage for persisted version 1 workflows; new defaults are tested in test_staged_discovery."""
    def setUp(self):
        super().setUp()
        self.native = {source: FakeNative(source, local_store.database_path(source), self.web)
                       for source in ("16688", "wzyp-live", "catfk")}
        self.next.session_factory = self.web.session
        self.managers = {source: CrawlManager(path=adapter.database_path(), platform_adapter=adapter)
                         for source, adapter in {**self.native, "dujiao-next": self.next}.items()}
        self.discovery = DiscoveryManager(self.managers, path=str(self.root/"discovery.sqlite3"), workflow_version=1,
            page_session_factory=self.web.session,
            detector=lambda url, stop: "dujiao-next" if "next.example" in url else None)
        self.gates = []

    def tearDown(self):
        for gate in self.gates:
            gate.set()
        self.discovery.shutdown()
        if self.discovery.thread:
            self.discovery.thread.join(5)
        for manager in self.managers.values():
            manager.shutdown()
        super().tearDown()

    def gate(self, source):
        event = threading.Event()
        self.gates.append(event)
        self.native[source].gate = event
        return event

    def finish(self, job):
        wait_until(lambda: self.discovery.store.report(job["job_id"])["status"] not in ("running", "cancelling"))
        return self.discovery.store.report(job["job_id"])

    def test_slow_16688_does_not_hold_cloud_or_dujiao(self):
        gate = self.gate("16688")
        job = self.discovery.start(["https://www.16688.com.cn/shop/S1", "https://catfk.com/shop/C1",
                                    "https://next.example/"], sources=["16688", "catfk", "dujiao-next"], include_indexes=False)
        wait_until(lambda: self.discovery.store.report(job["job_id"])["success_shops"] == 2)
        progress = self.discovery.store.report(job["job_id"])["source_progress"]
        self.assertEqual(progress["16688"]["running"], 1)
        self.assertEqual(progress["catfk"]["success_shops"], 1)
        gate.set()
        self.assertEqual(self.finish(job)["success_shops"], 3)

    def test_busy_external_platform_and_cancellation_leave_external_job_running(self):
        self.gate("16688")
        external = self.managers["16688"].start("targets", ["https://www.16688.com.cn/shop/EXTERNAL"])
        job = self.discovery.start(["https://www.16688.com.cn/shop/NEW", "https://catfk.com/shop/C1"],
                                   sources=["16688", "catfk"], include_indexes=False)
        wait_until(lambda: self.discovery.store.report(job["job_id"])["source_progress"]["catfk"]["success_shops"] == 1)
        self.discovery.cancel(job["job_id"])
        self.assertEqual(self.finish(job)["status"], "cancelled")
        self.assertEqual(self.managers["16688"].store.report(external["job_id"])["status"], "running")

    def test_round_robin_prevents_large_first_platform_starvation(self):
        order = []
        for source, manager in self.managers.items():
            original = manager.start
            def start(*args, source=source, original=original, **kwargs):
                result = original(*args, **kwargs)
                order.append(source)
                return result
            manager.start = start
        job = self.discovery.start([f"https://www.16688.com.cn/shop/S{i}" for i in range(6)] + ["https://catfk.com/shop/C1"],
                                   sources=["16688", "catfk"], include_indexes=False, parallel_sources=1)
        self.assertEqual(self.finish(job)["success_shops"], 7)
        self.assertEqual(order[:2], ["16688", "catfk"])

    def test_concurrency_cap_and_two_slow_probes_do_not_hold_known_platform(self):
        probe_gate = threading.Event()
        self.gates.append(probe_gate)
        def detect(url, stop):
            while not probe_gate.wait(.02):
                if stop.is_set():
                    raise CrawlCancelled()
            return None
        self.discovery.detector = detect
        job = self.discovery.start(["https://unknown-a.example/", "https://unknown-b.example/",
                                    "https://catfk.com/shop/C1"], sources=["catfk"], include_indexes=False)
        wait_until(lambda: self.discovery.store.report(job["job_id"])["success_shops"] == 1)
        self.discovery.cancel(job["job_id"])
        self.finish(job)

    def test_multiple_platforms_obey_requested_cap(self):
        for source in self.native:
            self.gate(source)
        job = self.discovery.start(["https://www.16688.com.cn/shop/S1", "https://catfk.com/shop/C1", "https://wzyp.cn/shop/W1"],
            sources=["16688", "catfk", "wzyp-live"], include_indexes=False, parallel_sources=2)
        wait_until(lambda: sum(x["running"] for x in self.discovery.store.report(job["job_id"])["source_progress"].values()) == 2)
        time.sleep(.15)
        self.assertEqual(sum(x["running"] for x in self.discovery.store.report(job["job_id"])["source_progress"].values()), 2)
        for gate in self.gates:
            gate.set()
        self.assertEqual(self.finish(job)["success_shops"], 3)

    def test_slow_index_does_not_hold_manual_urls_and_atomic_claims_do_not_duplicate(self):
        gate = threading.Event()
        self.gates.append(gate)
        def index(session, progress):
            while not gate.wait(.02):
                session.check_cancel()
            return []
        self.discovery.index_readers = {"16688": index}
        job = self.discovery.start(["https://catfk.com/shop/C1"], sources=["catfk"], include_indexes=True)
        wait_until(lambda: self.discovery.store.report(job["job_id"])["success_shops"] == 1)
        gate.set()
        self.assertEqual(self.finish(job)["status"], "completed")
        job_id = self.discovery.store.create([f"https://catfk.com/shop/A{i}" for i in range(12)], ["catfk"], False)
        with ThreadPoolExecutor(4) as pool:
            rows = list(pool.map(lambda _: self.discovery.store.claim_candidate(job_id), range(12)))
        self.assertEqual(len({row["id"] for row in rows}), 12)
        self.discovery.store.update_job(job_id, status="cancelled")

    def test_page_budget_is_reserved_atomically(self):
        job_id = self.discovery.store.create([], ["catfk"], False, page_limit=1)
        with ThreadPoolExecutor(4) as pool:
            reserved = list(pool.map(lambda _: self.discovery.store.reserve_page(job_id), range(8)))
        self.assertEqual(sum(reserved), 1)
        self.assertEqual(self.discovery.store.report(job_id)["pages"], 1)
        self.discovery.store.update_job(job_id, status="cancelled")

    def test_blocked_platform_is_skipped_once_and_resume_keeps_completed_other_platform(self):
        failing, calls = [True], []
        original = self.native["catfk"].sync_shop
        def sync(url, **kwargs):
            calls.append(url)
            if failing[0]:
                raise CrawlBlockedError("ESA 访问阻断", {"status_code": 403, "protection": "esa", "stage": "browser", "attempts": 3})
            return original(url, **kwargs)
        self.native["catfk"].sync_shop = sync
        job = self.discovery.start(["https://catfk.com/shop/C1", "https://catfk.com/shop/C2", "https://www.16688.com.cn/shop/S1"],
                                   sources=["catfk", "16688"], include_indexes=False)
        result = self.finish(job)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["source_progress"]["catfk"]["skipped"], 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["source_progress"]["catfk"]["failure"]["protection"], "esa")
        failing[0] = False
        self.discovery.resume(job["job_id"])
        result = self.finish(job)
        self.assertEqual(result["success_shops"], 3)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(self.native["16688"].calls, ["S1"])

    def test_cancel_only_becomes_resumable_after_probes_are_drained(self):
        started, stopping, drained = threading.Event(), threading.Event(), threading.Event()
        self.gates.append(drained)
        def detect(url, stop):
            started.set()
            stop.wait(3)
            stopping.set()
            drained.wait(3)
            raise CrawlCancelled()
        self.discovery.detector = detect
        job = self.discovery.start(["https://slow.example/"], sources=["catfk"], include_indexes=False)
        self.assertTrue(started.wait(3))
        self.discovery.cancel(job["job_id"])
        self.assertTrue(stopping.wait(3))
        self.assertEqual(self.discovery.store.report(job["job_id"])["status"], "cancelling")
        drained.set()
        self.assertEqual(self.finish(job)["status"], "cancelled")
        self.discovery.detector = lambda url, stop: None
        self.web.pages["https://slow.example/"] = '<a href="https://catfk.com/shop/C1">公开店铺</a>'
        self.discovery.resume(job["job_id"])
        self.assertEqual(self.finish(job)["success_shops"], 1)


class Wire:
    def __init__(self, responses, stop=None):
        self.responses = iter(responses)
        self.stop = stop or threading.Event()
        self.pinned = {}
        self.headers = {}
        self.bytes_read = 0
        self.calls = 0
        self.resolver = lambda host: "8.8.8.8"

    def _transfer(self, *args, **kwargs):
        self.calls += 1
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value

    def check_cancel(self):
        if self.stop.is_set():
            raise CrawlCancelled()

    def close(self):
        pass


class StreamResponse(CurlResponse):
    def iter_content(self):
        yield self.content


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.scope = TransportScope()
        self.rate = patch("hosted_http.wait_for_request")
        self.rate.start()
        self.limiter = patch("hosted_http.origin_limiter", return_value=Mock())
        self.limiter.start()

    def tearDown(self):
        self.scope.close()
        self.limiter.stop()
        self.rate.stop()

    def session(self, plain, compatible=None, browser=None, scope=None):
        wire = Wire(plain)
        compat = Mock()
        compat.request.side_effect = compatible or [StreamResponse(403, b"blocked")]
        browser = browser or Mock()
        session = HostedSession("https://catfk.com", scope=scope or self.scope,
            plain_factory=lambda **kw: wire, compatible_factory=lambda: compat, browser_factory=lambda: browser)
        return session, wire, compat, browser

    def read(self, session):
        return session.post("https://catfk.com/shopApi/Shop/info", json={"token": "S1"})

    def test_esa_recovery_keeps_diagnostics_and_stops_after_one_browser_attempt(self):
        blocked = StreamResponse(403, b"Denied by http_bot_simple RequestID: public-request", {"Server": "ESA", "Set-Cookie": "secret"})
        browser = Mock()
        browser.request.side_effect = ImportError("missing browser")
        session, wire, compat, browser = self.session([blocked], [blocked], browser)
        for _ in range(2):
            with self.assertRaises(CrawlBlockedError) as error:
                self.read(session)
        failure = error.exception.failure
        self.assertEqual(failure["status_code"], 403)
        self.assertEqual(failure["protection"], "esa")
        self.assertEqual(failure["request_id"], "public-request")
        self.assertEqual(failure["reason_code"], "browser_unavailable")
        self.assertNotIn("secret", json.dumps(failure))
        self.assertEqual(wire.calls, 1)
        self.assertEqual(browser.request.call_count, 1)

    def test_520_is_unknown_origin_failure_with_a_finite_total_budget(self):
        empty = StreamResponse(520, b"")
        browser = Mock()
        browser.request.return_value = empty
        session, wire, compat, browser = self.session([empty]*3, [empty], browser)
        with self.assertRaises(CrawlBlockedError) as error:
            self.read(session)
        self.assertEqual(wire.calls, 3)
        self.assertEqual(compat.request.call_count, 1)
        self.assertEqual(browser.request.call_count, 1)
        self.assertEqual(error.exception.failure["attempts"], 5)
        self.assertEqual(error.exception.failure["reason_code"], "origin_unavailable")
        self.assertNotIn("风控", str(error.exception))

    def test_429_and_server_outage_never_escalate_to_browser(self):
        for status in (429, 503):
            with self.subTest(status=status):
                scope = TransportScope()
                session, wire, compat, browser = self.session([StreamResponse(status, b"", {"Retry-After": "0"})]*3, scope=scope)
                with self.assertRaises(CrawlBlockedError):
                    self.read(session)
                self.assertEqual(wire.calls, 3)
                compat.request.assert_not_called()
                browser.request.assert_not_called()
                scope.close()

    def test_compatible_session_is_reused_across_shop_sessions(self):
        good = StreamResponse(200, b'{"code":1,"data":{}}')
        session, wire, compat, browser = self.session([StreamResponse(403, b"blocked")], [good, good])
        self.assertEqual(self.read(session).status_code, 200)
        other = HostedSession("https://catfk.com", scope=self.scope, compatible_factory=lambda: self.fail("会话不应重复创建"))
        self.assertEqual(self.read(other).status_code, 200)
        self.assertEqual(compat.request.call_count, 2)
        browser.request.assert_not_called()

    def test_missing_browser_timeout_cancellation_and_read_endpoint_limits(self):
        session, wire, compat, browser = self.session([StreamResponse(403, b"blocked")])
        browser.request.side_effect = RuntimeError("timeout")
        with self.assertRaises(CrawlBlockedError) as error:
            self.read(session)
        self.assertEqual(error.exception.failure["reason_code"], "browser_failed")
        with self.assertRaises(ValueError):
            session.post("https://catfk.com/shopApi/Pay/order", json={})
        with self.assertRaises(ValueError):
            session.get("https://127.0.0.1/")
        with self.assertRaises(ValueError):
            session.get("https://wzyp.cn/shop/S1")
        session.stop.set()
        with self.assertRaises(CrawlCancelled):
            self.read(session)

    def test_origin_cooldown_does_not_reserve_the_global_clock(self):
        clock = [0.0]
        total = RateLimiter(4, clock=lambda: clock[0])
        cold = RateLimiter(1, clock=lambda: clock[0])
        cold.defer(30)
        class Stop:
            cancelled = False
            def is_set(self): return self.cancelled
            def wait(self, duration): self.cancelled = True
        with self.assertRaises(CrawlCancelled):
            wait_for_request("https://a.example/", Stop(), cold, total)
        self.assertEqual(total.next_request, 0)
        healthy = RateLimiter(1, clock=lambda: clock[0])
        wait_for_request("https://b.example/", threading.Event(), healthy, total)
        self.assertEqual(total.next_request, .25)


class BrowserBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = object.__new__(browser_transport._BrowserService)
        self.service.lock = asyncio.Lock()
        self.service.contexts = OrderedDict()
        self.service.playwright = Mock()
        async def launch(**kwargs):
            browser = Mock()
            browser.close = AsyncMock()
            context = Mock()
            context.route = AsyncMock()
            context.route_web_socket = AsyncMock()
            context.new_page = AsyncMock()
            browser.new_context = AsyncMock(return_value=context)
            return browser
        self.service.playwright.chromium.launch = AsyncMock(side_effect=launch)

    async def test_pinned_browser_blocks_private_redirects_other_hosts_and_writes(self):
        entry = await self.service.context("run", "https://catfk.com", "8.8.8.8", "anon", threading.Event(), True)
        args = self.service.playwright.chromium.launch.call_args.kwargs["args"]
        self.assertTrue(any("MAP catfk.com 8.8.8.8" in arg for arg in args))
        self.assertEqual(entry["browser"].new_context.call_args.kwargs["service_workers"], "block")
        route_handler = entry["context"].route.call_args.args[1]
        for url, method in (("https://127.0.0.1/", "GET"), ("https://other.example/", "GET"),
                            ("https://catfk.com/shopApi/Pay/order", "POST"), ("http://catfk.com/", "GET")):
            route = Mock()
            route.request.url, route.request.method, route.request.resource_type = url, method, "fetch"
            route.abort, route.continue_ = AsyncMock(), AsyncMock()
            await route_handler(route)
            route.abort.assert_awaited_once()
            route.continue_.assert_not_awaited()
        await self.service.drop("run", "https://catfk.com")

    async def test_two_context_limit_and_failed_origin_does_not_close_other_origin(self):
        first = await self.service.context("run", "https://catfk.com", "8.8.8.8", "a", threading.Event(), True)
        first["busy"] = False
        other = await self.service.context("run", "https://wzyp.cn", "8.8.4.4", "b", threading.Event(), True)
        other["busy"] = False
        self.assertEqual(len(self.service.contexts), 2)
        third = await self.service.context("different", "https://www.16688.com.cn", "1.1.1.1", "c", threading.Event(), True)
        self.assertEqual(len(self.service.contexts), 2)
        first["browser"].close.assert_awaited_once()
        await self.service.drop("different", "https://www.16688.com.cn")
        other["browser"].close.assert_not_awaited()
        await self.service.release("run")


if __name__ == "__main__":
    unittest.main()
