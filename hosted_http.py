"""Bounded, anonymous recovery for the registered hosted shop domains only."""
from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import ipaddress
import threading
from urllib.parse import urlsplit
import uuid

import requests

from public_http import (CrawlBlockedError, CrawlCancelled, CurlResponse, GLOBAL_LIMITER,
                         detect_block_reason, failure_details, origin_limiter, retry_delay, wait_for_request)
from shop_http import SafeSession, MAX_RESPONSE, normalize_url, origin_of, _public_ip

READ_PATHS = {
    "https://wzyp.cn": frozenset({"/shopApi/Shop/info", "/shopApi/Shop/categoryList",
                                 "/shopApi/Shop/goodsList", "/shopApi/Shop/goodsInfo", "/shopApi/system/config"}),
    "https://catfk.com": frozenset({"/shopApi/Shop/info", "/shopApi/Shop/categoryList",
                                   "/shopApi/Shop/goodsList", "/shopApi/Shop/goodsInfo", "/shopApi/system/config"}),
    "https://www.16688.com.cn": frozenset({"/shopApi/shop/detail", "/shopApi/goods/detail",
                                           "/shopApi/goods/list", "/shopApi/goods_category/list"}),
}
TRANSIENT = {429, 500, 502, 503, 504, 520, 521, 522, 524}


@dataclass
class OriginState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    stage: str = "http"
    plain: object = None
    compatible: object = None
    browser_attempted: bool = False
    failure: object = None
    visitor: str = field(default_factory=lambda: uuid.uuid4().hex[:9])


class TransportScope:
    """Owned by a whole crawl/discovery run, rather than by an individual shop."""
    def __init__(self):
        self.lock = threading.Lock()
        self.origins = {}
        self.browser = None

    def entry(self, origin):
        with self.lock:
            return self.origins.setdefault(origin, OriginState())

    def browser_worker(self):
        with self.lock:
            if self.browser is None:
                from browser_transport import BrowserWorker
                self.browser = BrowserWorker()
            return self.browser

    def close(self):
        # Managers drain their workers before releasing the run's sessions.
        if self.browser is not None:
            self.browser.close()
        for entry in self.origins.values():
            for client in (entry.plain, entry.compatible):
                if client is not None:
                    client.close()
            entry.plain = entry.compatible = None
        self.origins.clear()
        self.browser = None


def scope_for(stop):
    return getattr(stop, "transport_scope", None)


class HostedSession:
    def __init__(self, origin, stop=None, scope=None, plain_factory=SafeSession,
                 compatible_factory=None, browser_factory=None):
        self.origin = origin_of(origin)
        if self.origin not in READ_PATHS:
            raise ValueError("浏览器恢复仅支持已登记的集中平台")
        self.stop = stop if stop is not None else threading.Event()
        inherited = scope or scope_for(self.stop)
        self.owned_scope = inherited is None
        self.scope = inherited or TransportScope()
        self.state = self.scope.entry(self.origin)
        self.plain_factory = plain_factory
        self.compatible_factory = compatible_factory
        self.browser_factory = browser_factory
        self.entry_url = self.origin + "/"
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.owned_scope:
            self.scope.close()

    def check_cancel(self):
        if self.stop.is_set():
            raise CrawlCancelled()

    def set_entry_url(self, url):
        url = normalize_url(url)
        if origin_of(url) != self.origin:
            raise ValueError("店铺入口与已登记源站不一致")
        self.entry_url = url

    @contextmanager
    def _locked(self):
        while not self.state.lock.acquire(timeout=0.1):
            self.check_cancel()
        try:
            self.check_cancel()
            yield
        finally:
            self.state.lock.release()

    def _compatible(self, method, url, payload):
        state = self.state
        if state.compatible is None:
            if self.compatible_factory:
                state.compatible = self.compatible_factory()
            else:
                from curl_cffi import CurlOpt
                from curl_cffi.requests import Session
                host = urlsplit(url).hostname
                address = state.plain.pinned[host]
                address = f"[{address}]" if ":" in address else address
                # Requests and streaming responses are serialized by the origin lock.
                # A shared handle can consequently also be closed by the coordinator.
                state.compatible = Session(impersonate="chrome", trust_env=False, use_thread_local_curl=False,
                    curl_options={CurlOpt.RESOLVE: [f"{host}:443:{address}"], CurlOpt.PROXY: b""})
        headers = {key: value for key, value in state.plain.headers.items() if key.lower() != "user-agent"}
        try:
            response = state.compatible.request(method, url, json=payload, headers=headers,
                                                timeout=25, allow_redirects=False, stream=True)
        except Exception as exc:
            raise requests.ConnectionError("兼容请求未能连接") from exc
        body = bytearray()
        try:
            for chunk in response.iter_content():
                self.check_cancel()
                body.extend(chunk)
                if len(body) > MAX_RESPONSE:
                    raise requests.RequestException("源站响应超过 4 MiB")
            state.plain.bytes_read += len(body)
            return CurlResponse(response.status_code, bytes(body), dict(response.headers), url)
        except CrawlCancelled:
            raise
        except Exception as exc:
            raise requests.ConnectionError("兼容响应读取失败") from exc
        finally:
            response.close()

    def _browser(self, method, url, payload):
        state = self.state
        worker = self.browser_factory() if self.browser_factory else self.scope.browser_worker()
        first = not state.browser_attempted
        state.browser_attempted = True
        host = urlsplit(url).hostname
        return worker.request(self.origin, self.entry_url, method, url, payload, self.stop,
                              state.plain.pinned[host], state.visitor, first=first)

    def request(self, method, url, **kwargs):
        url = normalize_url(url)
        method = method.upper()
        if origin_of(url) != self.origin or method not in {"GET", "POST"}:
            raise ValueError("请求不属于已登记的公开源站")
        if method == "POST" and urlsplit(url).path not in READ_PATHS[self.origin]:
            raise ValueError("仅允许已核实的公开只读商品接口")
        payload = kwargs.get("json")
        if kwargs.get("data") is not None:
            raise ValueError("公开接口只接受 JSON 查询参数")
        with self._locked():
            state = self.state
            if state.failure is not None:
                raise state.failure
            if state.plain is None:
                state.plain = self.plain_factory(stop=self.stop)
                state.plain.headers.update({"Origin": self.origin, "Referer": self.entry_url,
                                            "Visitorid": state.visitor})
            wire = state.plain
            wire.stop = self.stop
            wire.headers.update(self.headers)
            host = urlsplit(url).hostname
            if host not in wire.pinned:
                wire.pinned[host] = wire.resolver(host)
            if not _public_ip(ipaddress.ip_address(wire.pinned[host])):
                raise ValueError("已拒绝非公网源站地址")
            response, attempts, error, code = None, 0, None, "origin_unavailable"
            while True:
                stage = state.stage
                retries = 3 if stage == "http" else 1
                try:
                    for attempt in range(retries):
                        self.check_cancel()
                        wire.check_cancel()
                        # Browser navigation and API fetches apply the same limits in their route handler.
                        if stage != "browser":
                            wait_for_request(url, self.stop, global_limiter=GLOBAL_LIMITER)
                        attempts += 1
                        if stage == "http":
                            try:
                                response = wire._transfer(url, wire.pinned[host], 25, method, json_data=payload)
                            except (requests.ConnectionError, requests.Timeout):
                                if attempt == retries - 1:
                                    raise
                                origin_limiter(url).defer(retry_delay(None, attempt))
                                continue
                            wire.bytes_read += len(response.content)
                        elif stage == "compatible":
                            response = self._compatible(method, url, payload)
                        else:
                            response = self._browser(method, url, payload)
                        self.check_cancel()
                        wire.check_cancel()
                        if len(response.content) > MAX_RESPONSE:
                            raise requests.RequestException("源站响应超过 4 MiB")
                        blocked = detect_block_reason(response.status_code, response.text,
                                                      expect_json=kwargs.get("expect_json", method == "POST"))
                        if blocked:
                            error, code = blocked, "access_denied"
                            break
                        if response.status_code not in TRANSIENT:
                            return response
                        error = f"HTTP {response.status_code} 源站暂不可用，已保留快照"
                        if attempt < retries - 1:
                            origin_limiter(url).defer(retry_delay(response, attempt))
                    # A 429/server outage is not a reason to change browser identity.
                    if response is not None and response.status_code in TRANSIENT - {520}:
                        break
                except CrawlCancelled:
                    raise
                except (ImportError, requests.RequestException, RuntimeError, OSError) as exc:
                    label = {"http": "普通请求", "compatible": "兼容请求", "browser": "浏览器访问"}[stage]
                    error = f"{label}失败：{type(exc).__name__}"
                    if stage == "browser":
                        code = "browser_unavailable" if isinstance(exc, ImportError) else "browser_failed"
                if stage == "http":
                    state.stage = "compatible"
                elif stage == "compatible" and not state.browser_attempted:
                    state.stage = "browser"
                else:
                    break
            failure = failure_details(response, state.stage, attempts, code)
            state.failure = CrawlBlockedError((error or "源站自动恢复失败") + "；本轮跳过此源站，其他平台继续", failure)
            raise state.failure

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self.request("POST", url, **kwargs)
