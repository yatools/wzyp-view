"""无登录态的独立站 HTTPS 读取：公网 DNS 校验、IP 固定、限速与可中断传输。"""
import ipaddress
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

from public_http import (CrawlBlockedError, CrawlCancelled, CurlResponse, GLOBAL_LIMITER,
                         RateLimiter, _parse_curl_headers, _resolve_curl_binary,
                         detect_block_reason, retry_delay, failure_details, wait_for_request)

MAX_RESPONSE = 4 * 1024 * 1024
MAX_SOURCE_BYTES = 128 * 1024 * 1024
_limiters = {}
_lock = threading.Lock()


def normalize_url(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ValueError("链接为空或过长")
    value = value.strip()
    if any(ord(c) < 32 for c in value) or "\\" in value:
        raise ValueError("链接包含无效字符")
    parsed = urlsplit(value)
    try:
        port = parsed.port
        host = (parsed.hostname or "").encode("idna").decode("ascii").lower().rstrip(".")
    except (ValueError, UnicodeError) as exc:
        raise ValueError("链接主机无效") from exc
    if parsed.scheme.lower() != "https" or not host or port not in (None, 443) or parsed.username is not None or parsed.password is not None:
        raise ValueError("仅接受无账号密码的公开 HTTPS 链接（443 端口）")
    if host in {"localhost", "localhost.localdomain"} or "." not in host or host.endswith((".local", ".localhost", ".internal")):
        raise ValueError("不允许本机或内网地址")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and not _public_ip(address):
        raise ValueError("不允许本机或内网地址")
    netloc = f"[{host}]" if ":" in host else host
    query = urlencode([(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                       if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}], doseq=True)
    return urlunsplit(("https", netloc, parsed.path or "/", query, ""))


def origin_of(url):
    parsed = urlsplit(normalize_url(url))
    return f"{parsed.scheme}://{parsed.netloc}"


def _public_ip(address):
    address = getattr(address, "ipv4_mapped", None) or address
    return address.is_global and not address.is_multicast and not address.is_reserved


def resolve_public(host):
    records = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    addresses = sorted({record[4][0] for record in records})
    if not addresses or any(not _public_ip(ipaddress.ip_address(address)) for address in addresses):
        raise ValueError("站点 DNS 包含非公网地址，已拒绝请求")
    return addresses[0]


class SafeSession:
    def __init__(self, stop=None, resolver=None, runner=None):
        self.stop = stop or threading.Event()
        self.resolver = resolver or resolve_public
        self.runner = runner
        self.headers = {"User-Agent": "wzyp-view/public-catalog-reader", "Accept": "application/json, text/html;q=0.9"}
        self.started = time.monotonic()
        self.time_budget = 1800
        self.bytes_read = 0
        self.pinned = {}
        self._cookie_dir = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self._cookie_dir is not None:
            self._cookie_dir.cleanup()
            self._cookie_dir = None

    def check_cancel(self):
        if self.stop.is_set():
            raise CrawlCancelled()
        if time.monotonic() - self.started > self.time_budget or self.bytes_read > MAX_SOURCE_BYTES:
            raise requests.RequestException("单次源站采集超出时间或字节预算，未发布不完整快照")

    def allow_catalog_size(self, total):
        """公开总数已核验后，为低频读取的大目录分配有上限的时间。"""
        if isinstance(total, bool) or not isinstance(total, int) or not 0 <= total <= 20000:
            raise ValueError("无效的公开商品总数")
        self.time_budget = min(6 * 3600, max(1800, 120 + total * 10))

    def _transfer(self, url, address, timeout, method="GET", data=None, json_data=None):
        if self.runner:
            return self.runner(url, address=address, timeout=timeout, method=method, data=data)
        host = urlsplit(url).hostname
        ip = f"[{address}]" if ":" in address else address
        with tempfile.TemporaryDirectory(prefix="catalog-https-") as tmp:
            body, headers = Path(tmp) / "body", Path(tmp) / "headers"
            command = [_resolve_curl_binary(), "-sS", "--compressed", "--noproxy", "*",
                       "--proto", "=https", "--resolve", f"{host}:443:{ip}",
                       "--connect-timeout", "8", "--max-time", str(timeout),
                       "--max-filesize", str(MAX_RESPONSE), "--dump-header", str(headers),
                       "--output", str(body), "--write-out", "%{http_code}"]
            # 只保存源站自己发放的匿名会话，按 origin 隔离，绝不读取浏览器登录态。
            if self._cookie_dir is None:
                self._cookie_dir = tempfile.TemporaryDirectory(prefix="catalog-anonymous-")
            import hashlib
            cookie = str(Path(self._cookie_dir.name) / hashlib.sha256(origin_of(url).encode()).hexdigest())
            command.extend(["--cookie", cookie, "--cookie-jar", cookie])
            if method == "POST":
                if json_data is not None:
                    import json
                    command.extend(["-X", "POST", "-H", "Content-Type: application/json", "--data-binary", json.dumps(json_data, ensure_ascii=False)])
                else:
                    command.extend(["-X", "POST", "--data", urlencode(data or {})])
            for key, value in self.headers.items():
                command.extend(["-H", f"{key}: {value}"])
            command.append(url)
            options = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}
            with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **options) as process:
                try:
                    while True:
                        self.check_cancel()
                        if ((body.exists() and body.stat().st_size > MAX_RESPONSE)
                                or (headers.exists() and headers.stat().st_size > MAX_RESPONSE)):
                            raise requests.RequestException("源站响应超过 4 MiB")
                        try:
                            stdout, stderr = process.communicate(timeout=0.15)
                            break
                        except subprocess.TimeoutExpired:
                            continue
                except BaseException:
                    process.kill()
                    process.communicate()
                    raise
                if process.returncode:
                    raise requests.ConnectionError(f"公开 HTTPS 请求失败（curl {process.returncode}）：{stderr.decode('utf-8', errors='replace')[:180]}")
            content = body.read_bytes()
            if len(content) > MAX_RESPONSE:
                raise requests.RequestException("源站响应超过 4 MiB")
            return CurlResponse(int(stdout or b"0"), content,
                                _parse_curl_headers(headers.read_bytes()), url=url)

    def get(self, url, **kwargs):
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        if urlsplit(url).path not in {"/api/productlist", "/api/proudctinfo"}:
            raise ValueError("仅允许已核实的公开只读商品查询接口")
        return self._request("POST", url, **kwargs)

    def _request(self, method, url, **kwargs):
        params = kwargs.get("params")
        if params:
            url += ("&" if "?" in url else "?") + urlencode(params)
        url = normalize_url(url)
        for redirect in range(4):
            origin = origin_of(url)
            host = urlsplit(url).hostname
            if host not in self.pinned:
                self.pinned[host] = self.resolver(host)
            if not _public_ip(ipaddress.ip_address(self.pinned[host])):
                raise ValueError("DNS 返回非公网地址")
            with _lock:
                limiter = _limiters.setdefault(origin, RateLimiter(rate=1))
            response = None
            for attempt in range(3):
                self.check_cancel()
                wait_for_request(url, self.stop, limiter=limiter, global_limiter=GLOBAL_LIMITER)
                try:
                    response = self._transfer(url, self.pinned[host], 30, method, kwargs.get("data"))
                    self.bytes_read += len(response.content)
                    self.check_cancel()
                    if len(response.content) > MAX_RESPONSE:
                        raise requests.RequestException("源站响应超过 4 MiB")
                    reason = detect_block_reason(response.status_code, "")
                    # 普通货铺的下单表单可能自带 geetest/captcha 插件，不等于整页挑战。
                    if not reason and re.search(r"<title[^>]*>[^<]*(?:Just a moment|Attention Required|访问验证|安全验证|人机验证)|cf-chl-bypass|verify that you are a real person", response.text, re.I):
                        reason = "源站返回人机验证页面，已停止此站点并保留快照"
                    if reason:
                        raise CrawlBlockedError(reason, failure_details(response, attempts=attempt + 1))
                    if response.status_code not in {429, 500, 502, 503, 504, 520, 521, 522, 524}:
                        break
                except (requests.ConnectionError, requests.Timeout):
                    if attempt == 2:
                        raise CrawlBlockedError("源站连接失败，本轮暂停此源站，已保留快照",
                                                failure_details(stage="http", attempts=3, reason_code="origin_unavailable"))
                if attempt < 2:
                    limiter.defer(retry_delay(response, attempt))
            if response is None:
                raise requests.ConnectionError("源站未返回响应")
            if response.status_code in {429, 500, 502, 503, 504, 520, 521, 522, 524}:
                raise CrawlBlockedError(f"HTTP {response.status_code} 源站暂不可用，已保留快照",
                                        failure_details(response, attempts=3, reason_code="origin_unavailable"))
            if response.status_code in {301, 302, 303, 307, 308}:
                if kwargs.get("allow_redirects", True) is False:
                    return response
                if redirect == 3 or not response.headers.get("Location"):
                    raise requests.RequestException("源站跳转过多或缺少目标")
                target = normalize_url(urljoin(url, response.headers["Location"]))
                if method == "POST":
                    raise requests.RequestException("公开商品 POST 接口不允许跳转")
                url = target
                continue
            return response
        raise requests.RequestException("源站跳转异常")
