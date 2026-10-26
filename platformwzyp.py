"""读取链动小铺 (wzyp.cn) 的公开店铺/商品接口，不涉及下单或登录态。"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import io
import json
import math
import os
import re
import struct
from urllib.parse import urljoin, urlparse
import zipfile
import zlib

import requests

from config import BASE_DIR
import local_store
from public_http import CrawlBlockedError, CrawlCancelled, CurlSession

SOURCE_NAME = "wzyp-live"
SOURCE_KEY = "wzyp"
BASE_URL = "https://wzyp.cn"
EXAMPLE_URL = f"{BASE_URL}/shop/0000"
DISCOVERY_ORIGIN = "wzyp-view-release-v1.0.0"
RELEASE_TAG_URL = "https://github.com/Samge0/wzyp-view/releases/tag/v1.0.0"
RELEASE_ASSET_URL = "https://github.com/Samge0/wzyp-view/releases/download/v1.0.0/wzyp-data.zip"
SEED_FILE = os.path.join(BASE_DIR, "data", "wzyp_seeds.json")

GOODS_TYPES = ("card", "article", "resource", "equity")
PAGE_SIZE = 20
ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,80}\Z")
ALLOWED_HOSTS = {"wzyp.cn", "www.wzyp.cn", "shop.wzyp.cn", "ldxp.cn", "www.ldxp.cn", "pay.ldxp.cn"}
PLAIN_URL_RE = re.compile(
    r"https?://(?:www\.|pay\.|shop\.)?(?:wzyp|ldxp)\.cn/(?:shop|item|goods)/[A-Za-z0-9_-]{1,80}",
    re.IGNORECASE,
)


from hosted_shop import HostedShopPlatform, PlatformError, _ListChangedError

_PLATFORM = HostedShopPlatform(SOURCE_NAME, SOURCE_KEY, BASE_URL, frozenset(ALLOWED_HOSTS), "链动小铺")

parse_target = _PLATFORM.parse_target
normalize_target = _PLATFORM.normalize_target
extract_links = _PLATFORM.extract_links
_identifier = _PLATFORM._identifier
_number = _PLATFORM._number
_int_count = _PLATFORM._int_count
_format_create_time = _PLATFORM._format_create_time
_check_shop_open = _PLATFORM._check_shop_open

def database_path():
    return _PLATFORM.database_path()

def create_session(stop=None):
    return _PLATFORM.session_factory(stop)

session_factory = create_session
index_session_factory = lambda stop=None: CurlSession(stop=stop)
description_links = extract_links

def load_seed_file(seed_path=None) -> list[str]:
    path = seed_path or SEED_FILE
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    raw_links = data.get("links", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    dedup = {}
    for item in raw_links:
        try:
            norm = normalize_target(item)
            key = (norm["kind"], norm["target"])
            dedup[key] = norm["url"]
        except ValueError:
            continue
    return list(dedup.values())


def _extract_links_from_release_bytes(raw_bytes: bytes) -> list[str]:
    """从 Release ZIP（支持首段 Range 分片或完整 ZIP）或 SQL/文本中提取店铺链接"""
    sql_text = ""
    if raw_bytes.startswith(b"PK\x03\x04") and len(raw_bytes) >= 30:
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                if "wzyp_shops.sql" in zf.namelist():
                    sql_text = zf.read("wzyp_shops.sql").decode("utf-8", errors="replace")
        except Exception:
            sig, ver, flags, comp, mtime, mdate, crc, csize, usize, fn_len, ex_len = struct.unpack(
                "<4sHHHHHIIIHH", raw_bytes[:30]
            )
            data_start = 30 + fn_len + ex_len
            if comp == 8 and len(raw_bytes) >= data_start + csize and csize > 0:
                sql_text = zlib.decompress(raw_bytes[data_start : data_start + csize], -zlib.MAX_WBITS).decode(
                    "utf-8", errors="replace"
                )
            elif comp == 0 and len(raw_bytes) >= data_start + usize and usize > 0:
                sql_text = raw_bytes[data_start : data_start + usize].decode("utf-8", errors="replace")
    else:
        sql_text = raw_bytes.decode("utf-8", errors="replace")

    dedup = {}
    for token in re.findall(r"\n\('([A-Za-z0-9_-]{1,80})',", sql_text):
        norm = normalize_target(f"{BASE_URL}/shop/{token}")
        dedup[(norm["kind"], norm["target"])] = norm["url"]
    for url in extract_links(sql_text):
        norm = normalize_target(url)
        if norm["kind"] == "shop":
            dedup[(norm["kind"], norm["target"])] = norm["url"]
    return list(dedup.values())


def seed_urls():
    return load_seed_file()


def discover_public(session=None, progress=None, seed_path=None):
    """优先从原项目 Release v1.0.0 在线提取 250 家种子店铺链接，失败时回退本地种子文件。"""
    own_session = session is None
    session = session or create_session()
    try:
        if progress:
            progress("v1.0.0", 0)
        links = []
        try:
            if hasattr(session, "check_cancel"):
                session.check_cancel()
            resp = session.get(
                RELEASE_ASSET_URL,
                headers={"Range": "bytes=0-131071", "Accept": "*/*"},
                timeout=20,
                allow_redirects=True,
            )
            if resp.status_code in (200, 206) and resp.content:
                links = _extract_links_from_release_bytes(resp.content)
        except CrawlCancelled:
            raise
        except Exception:
            links = []

        if not links:
            links = load_seed_file(seed_path)
        if not links:
            raise PlatformError("链动小铺种子清单读取失败，未发现可用店铺链接")
        if progress:
            progress("v1.0.0", 1)
        return links
    finally:
        if own_session and hasattr(session, "close"):
            session.close()


fetch_public = _PLATFORM.fetch_public
resolve_target = _PLATFORM.resolve_target
probe_shop = _PLATFORM.probe_shop
_collect_shop_once = _PLATFORM._collect_shop_once
sync_shop = _PLATFORM.sync_shop


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="链动小铺 (wzyp.cn) 公开商品采集")
    parser.add_argument("target", nargs="?", default="crawl", help="crawl、refresh 或 wzyp.cn 商品/店铺链接")
    parser.add_argument("--resume", metavar="JOB_ID", help="继续中断、取消或部分失败的任务")
    parser.add_argument("--url", action="append", default=[], help="为批量采集补充公开链接，可重复")
    arguments = parser.parse_args()
    if arguments.target in {"crawl", "refresh"} or arguments.resume:
        import sys
        from crawler16688 import run_cli
        raise SystemExit(
            run_cli(
                "refresh" if arguments.target == "refresh" else "discover",
                arguments.url,
                arguments.resume,
                platform_adapter=sys.modules[__name__],
            )
        )
    try:
        result = sync_shop(arguments.target)
    except (ValueError, PlatformError, CrawlBlockedError) as exc:
        parser.exit(1, f"同步失败：{exc}\n")
    print(
        json.dumps(
            {
                "shop": result["shop"]["nickname"],
                "token": result["shop"]["token"],
                "goods_count": result["goods_count"],
                "detail_count": result["detail_count"],
                "synced_at": result["synced_at"],
            },
            ensure_ascii=False,
        )
    )
