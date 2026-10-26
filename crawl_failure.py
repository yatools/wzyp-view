"""Stable failure semantics shared by transports, adapters and resumable queues."""
import json

import requests


PERMANENT = {"not_found", "merchant_banned", "shop_closed", "invalid_link", "unsupported_template"}
PAUSE_ORIGIN = {"access_blocked", "origin_unavailable"}


def details(category="unknown", *, retryable=None, pause_origin=None, **values):
    return {**values, "category": category,
            "retryable": category not in PERMANENT if retryable is None else retryable,
            "pause_origin": category in PAUSE_ORIGIN if pause_origin is None else pause_origin}


def business(message):
    """Only explicit business responses qualify as permanent failures."""
    text = str(message or "")
    if "或" not in text and "异常" not in text:
        for words, category in ((("已封禁", "商户封禁", "商户被禁"), "merchant_banned"),
                                (("已打烊", "店铺打烊", "店铺已关闭"), "shop_closed"),
                                (("不存在", "已下架", "未上架", "已删除", "not found"), "not_found")):
            if any(word in text.lower() for word in words):
                return details(category)
    return details()


def http_failure(response, **values):
    status = getattr(response, "status_code", None)
    category = ("rate_limit" if status == 429 else "access_blocked" if status in (403, 444)
                else "not_found" if status in (404, 410) else "server_error" if status and status >= 500
                else "unknown")
    return details(category, status_code=status, **values)


def normalize(failure=None, message=None):
    if isinstance(failure, str):
        try:
            failure = json.loads(failure)
        except (ValueError, TypeError):
            failure = None
    if not isinstance(failure, dict) or not failure:
        return business(message)
    failure = dict(failure)
    category = failure.pop("category", None)
    if category == "unknown" and business(message)["category"] != "unknown":
        category = business(message)["category"]
        failure.pop("retryable", None)
        failure.pop("pause_origin", None)
    if category is None:
        code = failure.get("reason_code")
        category = ("origin_unavailable" if code in {"origin_unavailable", "browser_failed", "browser_unavailable"}
                    else "access_blocked" if code == "access_denied" or failure.get("status_code") in (403, 444)
                    else "unknown")
    return details(category, **failure)


def from_exception(exc):
    if getattr(exc, "failure", None):
        return normalize(exc.failure, str(exc))
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return http_failure(exc.response)
    if isinstance(exc, (requests.exceptions.JSONDecodeError, json.JSONDecodeError)):
        return details("parse_error")
    if isinstance(exc, (requests.RequestException, OSError)):
        return details("network")
    return normalize(message=str(exc))


def validate_retry_mode(mode):
    if mode not in {"transient", "all"}:
        raise ValueError("retry_mode 应为 transient 或 all")
    return mode


def should_retry(failure, message, mode="transient"):
    return mode == "all" or normalize(failure, message)["retryable"]


class CollectionError(Exception):
    def __init__(self, message, category="parse_error", *, failure=None):
        super().__init__(message)
        self.failure = normalize(failure) if failure is not None else details(category)
