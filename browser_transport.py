"""A single Playwright event-loop thread; at most two anonymous pinned contexts."""
import asyncio
from concurrent.futures import TimeoutError as FutureTimeout
from collections import OrderedDict
import ipaddress
import threading
import time
from urllib.parse import urlsplit
import uuid

from public_http import CrawlCancelled, CurlResponse, wait_for_request
from shop_http import normalize_url, origin_of, _public_ip

_service = None
_service_lock = threading.Lock()


class _NetworkStop:
    def __init__(self, entry):
        self.entry = entry

    def is_set(self):
        return self.entry["closed"].is_set() or self.entry["stop"].is_set()

    def wait(self, seconds):
        self.entry["closed"].wait(min(seconds, 0.1))
        return self.is_set()


class BrowserWorker:
    """A run-scoped lease on the shared browser thread. Nothing is saved to disk."""
    def __init__(self):
        global _service
        self.key = uuid.uuid4().hex
        self.closed = False
        with _service_lock:
            if _service is None:
                _service = _BrowserService()
            self.service = _service

    def request(self, origin, entry_url, method, url, payload, stop, address, visitor, first):
        if self.closed:
            raise RuntimeError("浏览器会话已关闭")
        if not _public_ip(ipaddress.ip_address(address)):
            raise ValueError("浏览器目标不是公网地址")
        deadline = time.monotonic() + (45 if first else 25)
        future = asyncio.run_coroutine_threadsafe(self.service.fetch(
            self.key, origin, entry_url, method, url, payload, stop, address, visitor, first, deadline),
            self.service.loop)
        while True:
            if stop.is_set() or self.closed:
                future.cancel()
                raise CrawlCancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                future.cancel()
                raise RuntimeError("浏览器自动访问超时")
            try:
                return future.result(timeout=min(remaining, 0.1))
            except FutureTimeout:
                continue
            except (CrawlCancelled, ImportError):
                raise
            except Exception as exc:
                if "Executable doesn't exist" in str(exc) or "playwright install" in str(exc):
                    raise ImportError("浏览器组件未安装，请运行 setup.ps1") from exc
                raise RuntimeError("浏览器自动访问失败或组件不可用") from exc

    def close(self):
        if not self.closed:
            self.closed = True
            future = asyncio.run_coroutine_threadsafe(self.service.release(self.key), self.service.loop)
            try:
                future.result(timeout=5)
            except FutureTimeout:
                pass  # Release stays queued; shutdown_browser also drains all contexts.


class _BrowserService:
    def __init__(self):
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True, name="public-browser")
        self.thread.start()
        self.ready.wait(5)

    def run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.slots = asyncio.Semaphore(2)
        self.lock = asyncio.Lock()
        self.contexts = OrderedDict()
        self.playwright = None
        self.ready.set()
        self.loop.run_forever()

    async def release(self, key):
        async with self.lock:
            for identity in [identity for identity in self.contexts if identity[0] == key]:
                entry = self.contexts.pop(identity)
                await self.close_entry(entry)

    @staticmethod
    async def close_entry(entry):
        entry["closed"].set()
        await entry["browser"].close()

    async def drop(self, key, origin):
        async with self.lock:
            entry = self.contexts.pop((key, origin), None)
            if entry:
                await self.close_entry(entry)

    async def context(self, key, origin, address, visitor, stop, first):
        from hosted_http import READ_PATHS
        identity = (key, origin)
        async with self.lock:
            if identity in self.contexts:
                entry = self.contexts[identity]
                entry["stop"] = stop
                entry["busy"] = True
                self.contexts.move_to_end(identity)
                return entry
            if not first:
                raise RuntimeError("本轮浏览器会话已失效，不重复建立验证会话")
            if len(self.contexts) >= 2:
                idle = next((item for item, value in self.contexts.items() if not value["busy"]), None)
                if idle is None:
                    raise RuntimeError("浏览器上下文已满")
                await self.close_entry(self.contexts.pop(idle))
            if self.playwright is None:
                from playwright.async_api import async_playwright
                self.playwright = await async_playwright().start()
            host = urlsplit(origin).hostname
            target = f"[{address}]" if ":" in address else address
            browser = await self.playwright.chromium.launch(headless=True, args=[
                f"--host-resolver-rules=MAP {host} {target}",
                "--no-proxy-server", "--disable-quic", "--dns-prefetch-disable",
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
            ])
            try:
                context = await browser.new_context(service_workers="block", accept_downloads=False,
                                                     extra_http_headers={"Visitorid": visitor})
                entry = {"browser": browser, "context": context, "stop": stop, "busy": True,
                         "closed": threading.Event()}

                async def route_request(route):
                    if entry["closed"].is_set():
                        return
                    request = route.request
                    try:
                        normalized = normalize_url(request.url)
                        valid = origin_of(normalized) == origin
                    except ValueError:
                        valid = False
                    path = urlsplit(request.url).path
                    forbidden = any(part in {"admin", "auth", "login", "merchant", "order", "pay", "cart", "user"}
                                    for part in path.lower().split("/") if part)
                    allowed_method = request.method in {"GET", "HEAD"} or (
                        request.method == "POST" and path in READ_PATHS[origin])
                    if (not valid or forbidden or not allowed_method or entry["stop"].is_set()
                            or request.resource_type in {"image", "font", "media"}):
                        await route.abort()
                        return
                    try:
                        await asyncio.to_thread(wait_for_request, origin, _NetworkStop(entry))
                        if not entry["closed"].is_set():
                            await route.continue_()
                    except CrawlCancelled:
                        if not entry["closed"].is_set():
                            await route.abort()
                    except Exception:
                        if not entry["closed"].is_set():
                            raise

                async def block_socket(socket):
                    await socket.close()

                await context.route("**/*", route_request)
                await context.route_web_socket("**/*", block_socket)
                entry["page"] = await context.new_page()
            except BaseException:
                await browser.close()
                raise
            self.contexts[identity] = entry
            return entry

    async def fetch(self, key, origin, entry_url, method, url, payload, stop, address, visitor, first, deadline):
        async with self.slots:
            entry = None
            try:
                if time.monotonic() >= deadline or stop.is_set():
                    raise CrawlCancelled()
                if origin_of(normalize_url(entry_url)) != origin or origin_of(normalize_url(url)) != origin:
                    raise ValueError("浏览器请求越过源站边界")
                entry = await self.context(key, origin, address, visitor, stop, first)
                page = entry["page"]
                if first:
                    await page.goto(entry_url, wait_until="domcontentloaded", timeout=max(1, int((deadline-time.monotonic())*1000)))
                    # JS-only challenges may finish naturally. Never wait for a human CAPTCHA.
                    await page.wait_for_function("""() => !/Just a moment|Attention Required|安全验证|人机验证/i.test(document.title)
                        && !/verify that you are a real person/i.test(document.body?.innerText || '')""",
                        timeout=max(1, int((deadline-time.monotonic())*1000)))
                remaining = max(1, int((deadline - time.monotonic()) * 1000))
                result = await page.evaluate("""async ({url, method, payload, timeout}) => {
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(), timeout);
                    try {
                        const response = await fetch(url, {method, credentials: 'same-origin', redirect: 'manual',
                            headers: {'Accept': 'application/json', 'Content-Type': 'application/json',
                                'Visitorid': localStorage.getItem('visitorId') || ''},
                            body: method === 'POST' ? JSON.stringify(payload) : undefined,
                            signal: controller.signal});
                        const reader = response.body?.getReader();
                        let size = 0, text = ''; const decoder = new TextDecoder();
                        if (reader) while (true) {
                            const {done, value} = await reader.read(); if (done) break;
                            size += value.length;
                            if (size > 4194304) { controller.abort(); throw Error('response limit'); }
                            text += decoder.decode(value, {stream:true});
                        }
                        text += decoder.decode();
                        return {status:response.status, headers:Object.fromEntries(response.headers), text};
                    } finally { clearTimeout(timer); }
                }""", {"url": url, "method": method, "payload": payload, "timeout": remaining})
                return CurlResponse(result["status"], result["text"].encode("utf-8"), result["headers"], url)
            except BaseException:
                if entry is not None:
                    await self.drop(key, origin)
                raise
            finally:
                if entry is not None:
                    entry["busy"] = False


def shutdown_browser():
    global _service
    with _service_lock:
        service, _service = _service, None
    if service is not None:
        async def close():
            for key in list({identity[0] for identity in service.contexts}):
                await service.release(key)
            if service.playwright is not None:
                await service.playwright.stop()
        future = asyncio.run_coroutine_threadsafe(close(), service.loop)
        try:
            future.result(timeout=5)
        finally:
            service.loop.call_soon_threadsafe(service.loop.stop)
            service.thread.join(timeout=2)
