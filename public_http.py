"""公开采集请求：进程内共享限速、有限重试和可中断等待。"""
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests


class CrawlCancelled(Exception):
    """停止采集；不作为店铺读取失败处理。"""


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
                if response.status_code not in {429, 500, 502, 503, 504} or attempt == 2:
                    return response
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 2:
                    raise
            delay = retry_delay(response, attempt)
            if response is not None:
                response.close()
            self.limiter.defer(delay)
        raise AssertionError("unreachable")
