"""公开采集请求：进程内共享限速、有限重试和可中断等待。"""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time

import requests


class CrawlCancelled(Exception):
    """停止采集；不作为店铺读取失败处理。"""


class CrawlBlockedError(RuntimeError):
    """遇到 403 或人机验证时中断任务并保留断点。"""


class RateLimiter:
    def __init__(self, rate=2, clock=time.monotonic):
        self.interval = 1 / rate if rate else 0
        self.clock = clock
        self.next_request = 0
        self.lock = threading.Lock()

    def wait(self, stop):
        while not stop.is_set():
            with self.lock:
                delay = self.next_request - self.clock()
                if delay <= 0:
                    self.next_request = self.clock() + self.interval
                    return
            stop.wait(min(delay, 0.2))
        raise CrawlCancelled()

    def defer(self, seconds):
        with self.lock:
            self.next_request = max(self.next_request, self.clock() + seconds)


GLOBAL_LIMITER = RateLimiter()


def retry_delay(response, attempt):
    value = response.headers.get("Retry-After", "") if response is not None else ""
    try:
        return min(max(float(value), 0), 60)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            return min(max((date - datetime.now(timezone.utc)).total_seconds(), 0), 60)
        except (ValueError, TypeError, OverflowError):
            return 2 ** attempt


class PublicSession(requests.Session):
    def __init__(self, stop=None, limiter=None):
        super().__init__()
        self.stop = stop or threading.Event()
        self.limiter = limiter or GLOBAL_LIMITER
        self.headers.update({"User-Agent": "wzyp-view/16688-public-reader", "Accept": "application/json"})

    def check_cancel(self):
        if self.stop.is_set():
            raise CrawlCancelled()

    def request(self, method, url, **kwargs):
        kwargs.setdefault("timeout", (8, 30))
        kwargs["allow_redirects"] = False
        for attempt in range(3):
            self.check_cancel()
            self.limiter.wait(self.stop)
            response = None
            try:
                response = super().request(method, url, **kwargs)
                self.check_cancel()
                if response.status_code not in {429, 500, 502, 503, 504, 520, 521, 522, 524} or attempt == 2:
                    return response
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
            delay = retry_delay(response, attempt)
            if response is not None:
                response.close()
            self.limiter.defer(delay)
        raise AssertionError("unreachable")


_CAPTCHA_PATTERNS = re.compile(
    r"(人机验证|滑动验证|安全验证|行为验证|请完成验证|访问被拒绝|Please slide to verify|verify that you are a real person|cf-chl-bypass|Just a moment\.\.\.|geetest|captchaStart)",
    re.IGNORECASE,
)


def detect_block_reason(status_code: int, body_text: str, expect_json: bool = False) -> str | None:
    """检查响应是否触发 HTTP 403/444 或人机验证挑战"""
    if status_code == 403:
        return "HTTP 403 访问被拒绝，可能触发风控或人机验证"
    if status_code == 444:
        return "HTTP 444 访问被源站防火墙阻断，可能触发风控或访问频率限制"
    text = (body_text or "").strip()
    if not text:
        return None
    if expect_json:
        try:
            payload = json.loads(text)
            if isinstance(payload, dict):
                msg = str(payload.get("msg") or payload.get("message") or "")
                code = payload.get("code")
                if code in (403, -403) or _CAPTCHA_PATTERNS.search(msg):
                    return f"接口要求人机验证或拒绝访问: {msg or code}"
                return None
        except Exception:
            if _CAPTCHA_PATTERNS.search(text[:2000]):
                return "响应非 JSON 且包含人机验证/风控页面特征"
    elif _CAPTCHA_PATTERNS.search(text[:2000]) and "<html" in text[:500].lower():
        if "/package/shop/assets/" not in text:
            return "页面返回人机验证挑战"
    return None


class CurlResponse:
    def __init__(self, status_code: int, content: bytes, headers: dict | None = None, url: str = ""):
        self.status_code = int(status_code)
        self.content = content if isinstance(content, (bytes, bytearray)) else str(content).encode("utf-8")
        self.headers = headers or {}
        self.url = url

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.text)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def _resolve_curl_binary() -> str:
    if os.name == "nt":
        win_curl = shutil.which("curl.exe")
        if win_curl:
            return win_curl
    return shutil.which("curl") or "curl"


class CurlSession:
    """使用标准 curl 客户端发起无登录态公开请求，共享进程限速与重试。"""

    def __init__(self, stop=None, limiter=None, runner=None, timeout=20):
        self.stop = stop or threading.Event()
        self.limiter = limiter or GLOBAL_LIMITER
        self.runner = runner
        self.timeout = timeout
        self.curl_bin = _resolve_curl_binary()
        self._cookie_file = None
        if runner is None:
            fd, path = tempfile.mkstemp(prefix="wzyp_curl_cookie_", suffix=".txt")
            os.close(fd)
            self._cookie_file = path
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://wzyp.cn",
            "Referer": "https://wzyp.cn/",
        }

    def check_cancel(self):
        if self.stop.is_set():
            raise CrawlCancelled()

    def close(self):
        if self._cookie_file:
            try:
                os.remove(self._cookie_file)
            except OSError:
                pass
            self._cookie_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _exec_curl(self, method: str, url: str, headers: dict, body_bytes: bytes | None, timeout: int, follow_redirects: bool = True) -> CurlResponse:
        if self.runner is not None:
            return self.runner(method, url, headers=headers, data=body_bytes, timeout=timeout, follow_redirects=follow_redirects)

        marker = b"\n__WZYP_CURL_META__:"
        cmd = [
            self.curl_bin,
            "-sS",
            "--compressed",
            "--max-time",
            str(int(timeout)),
            "-X",
            method.upper(),
        ]
        if self._cookie_file:
            cmd.extend(["-c", self._cookie_file, "-b", self._cookie_file])
        if follow_redirects:
            cmd.append("-L")
        for k, v in headers.items():
            if v is not None:
                cmd.extend(["-H", f"{k}: {v}"])
        if body_bytes is not None:
            cmd.extend(["--data-binary", "@-"])
        cmd.extend(["-w", "\n__WZYP_CURL_META__:%{http_code}"])
        cmd.append(url)

        proc = subprocess.run(
            cmd,
            input=body_bytes,
            capture_output=True,
            timeout=timeout + 5,
        )
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            raise requests.ConnectionError(f"curl exit {proc.returncode}: {stderr or url}")

        stdout = proc.stdout
        idx = stdout.rfind(marker)
        if idx == -1:
            raise requests.ConnectionError(f"curl missing status footer for {url}")
        raw_body = stdout[:idx]
        status_str = stdout[idx + len(marker):].decode("ascii", errors="ignore").strip()
        status_code = int(status_str) if status_str.isdigit() else 0
        return CurlResponse(status_code=status_code, content=raw_body, headers={}, url=url)

    def request(self, method: str, url: str, **kwargs) -> CurlResponse:
        timeout = kwargs.get("timeout", self.timeout)
        if isinstance(timeout, tuple):
            timeout = max(timeout)
        follow_redirects = kwargs.get("allow_redirects", True)
        expect_json = bool(kwargs.get("expect_json", False))

        req_headers = dict(self.headers)
        if kwargs.get("headers"):
            req_headers.update(kwargs["headers"])

        body_bytes = None
        if "json" in kwargs and kwargs["json"] is not None:
            body_bytes = json.dumps(kwargs["json"], ensure_ascii=False).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")
        elif "data" in kwargs and kwargs["data"] is not None:
            raw_data = kwargs["data"]
            body_bytes = raw_data if isinstance(raw_data, bytes) else str(raw_data).encode("utf-8")

        for attempt in range(3):
            self.check_cancel()
            self.limiter.wait(self.stop)
            response = None
            try:
                response = self._exec_curl(
                    method,
                    url,
                    headers=req_headers,
                    body_bytes=body_bytes,
                    timeout=timeout,
                    follow_redirects=follow_redirects,
                )
                self.check_cancel()
                block_reason = detect_block_reason(response.status_code, response.text, expect_json=expect_json)
                if block_reason:
                    raise CrawlBlockedError(block_reason)
                if response.status_code not in {429, 500, 502, 503, 504, 520, 521, 522, 524} or attempt == 2:
                    if attempt == 2 and response.status_code == 520 and not response.text.strip():
                        raise CrawlBlockedError("HTTP 520 源站防火墙连接阻断或风控限制（连续 3 次空响应），已中断任务并保留断点")
                    return response
            except CrawlBlockedError:
                raise
            except (requests.ConnectionError, requests.Timeout, subprocess.TimeoutExpired) as exc:
                if attempt == 2:
                    raise requests.ConnectionError(str(exc)) from exc
            delay = retry_delay(response, attempt)
            if response is not None:
                response.close()
            self.limiter.defer(delay)
        raise AssertionError("unreachable")

    def get(self, url: str, **kwargs) -> CurlResponse:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> CurlResponse:
        return self.request("POST", url, **kwargs)
