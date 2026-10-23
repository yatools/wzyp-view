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


class PlatformError(Exception):
    pass


class _ListChangedError(PlatformError):
    """分页或详情读取期间商品上下架导致数量不一致，触发整店重读一次。"""


def database_path():
    return local_store.wzyp_database_path()


def create_session(stop=None):
    return CurlSession(stop=stop)


session_factory = create_session


def parse_target(url):
    raw = str(url or "").strip()
    parsed = urlparse(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("链接端口无效") from exc
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme not in {"http", "https"}
        or host not in ALLOWED_HOSTS
        or parsed.username
        or parsed.password
        or port not in {None, 80, 443}
    ):
        raise ValueError("请输入 wzyp.cn / ldxp.cn 的店铺或商品链接")
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) < 2 or parts[0] not in {"shop", "item", "goods"} or not ID_PATTERN.fullmatch(parts[1]):
        raise ValueError("链接格式应为 /shop/店铺token 或 /item/商品编号")
    kind = "shop" if parts[0] == "shop" else "goods"
    return kind, parts[1]


def normalize_target(url):
    kind, target = parse_target(url)
    path_prefix = "shop" if kind == "shop" else "item"
    return {"kind": kind, "target": target, "url": f"{BASE_URL}/{path_prefix}/{target}"}


def extract_links(*texts):
    """从店铺公告、商品描述等文本中提取公开店铺/商品链接（兼容 HTML 与纯文本）。"""
    links = set()

    class _AnchorParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag != "a":
                return
            for name, value in attrs:
                if name == "href" and value:
                    try:
                        links.add(normalize_target(urljoin(BASE_URL, value.strip()))["url"])
                    except ValueError:
                        pass

    for text in texts:
        content = str(text or "")
        if not content:
            continue
        try:
            parser = _AnchorParser()
            parser.feed(content)
        except Exception:
            pass
        for match in PLAIN_URL_RE.findall(content):
            try:
                links.add(normalize_target(match)["url"])
            except ValueError:
                pass
    return sorted(links)


description_links = extract_links


def _identifier(value):
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
        raise PlatformError("平台返回了无效的商品或店铺编号")
    return value


def _number(value):
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number < 0:
            raise InvalidOperation
        result = float(number)
        if not math.isfinite(result):
            raise InvalidOperation
        return result
    except (ValueError, InvalidOperation) as exc:
        raise PlatformError("平台返回了无效的价格或数量") from exc


def _int_count(value):
    if value is None or value == "" or isinstance(value, bool):
        return None
    num = _number(value)
    if num is None:
        return None
    if num != int(num):
        raise PlatformError("平台返回了非整数计数")
    return int(num)


def _format_create_time(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        ts = int(float(value))
        if ts <= 0:
            return None
        return datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip() or None


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


def fetch_public(session, endpoint, payload):
    try:
        response = session.post(
            f"{BASE_URL}/shopApi/{endpoint.lstrip('/')}",
            json=payload,
            timeout=(8, 25),
            allow_redirects=False,
            expect_json=True,
        )
        if response.status_code == 403:
            raise CrawlBlockedError("HTTP 403 访问被拒绝，可能触发风控或人机验证")
        if response.status_code >= 400:
            raise PlatformError(f"链动小铺接口 HTTP {response.status_code}")
        if len(response.content) > 8 * 1024 * 1024:
            raise PlatformError("平台响应体积异常，请稍后重试")
        body = response.json()
    except (CrawlCancelled, CrawlBlockedError):
        raise
    except (requests.RequestException, ValueError, OSError) as exc:
        raise PlatformError("链动小铺公开接口访问失败，请检查网络后重试") from exc

    if not isinstance(body, dict):
        raise PlatformError("链动小铺响应格式无效")
    if body.get("code") in (403, -403):
        raise CrawlBlockedError(f"链动小铺触发访问限制：{str(body.get('msg') or '403')[:160]}")
    if body.get("code") != 1 or "data" not in body or body["data"] is None:
        message = str(body.get("msg") or "响应格式无效")[:160]
        raise PlatformError(f"链动小铺：{message}")
    return body["data"]


def _check_shop_open(shop: dict) -> str:
    if not isinstance(shop, dict):
        raise PlatformError("店铺不存在或店铺信息异常")
    token = _identifier(shop.get("token") or shop.get("shop_no"))
    custom_status = shop.get("custom_status")
    if custom_status == 0 or str(custom_status) == "0":
        reason = str(shop.get("custom_status_msg") or "").strip()
        raise PlatformError(f"店铺已打烊：{reason}" if reason else "店铺已打烊")
    nickname = str(shop.get("nickname") or "").strip()
    if not nickname:
        raise PlatformError("店铺名称缺失或店铺信息异常")
    shop["token"] = token
    shop["shop_no"] = token
    return token


def resolve_target(url, session):
    kind, target = parse_target(url)
    seed = None
    if kind == "goods":
        seed = fetch_public(session, "Shop/goodsInfo", {"goods_key": target})
        if not isinstance(seed, dict) or seed.get("goods_key") != target:
            raise PlatformError("商品不存在、已下架或详情格式异常")
        user_info = seed.get("user") if isinstance(seed.get("user"), dict) else {}
        shop_token = _identifier(seed.get("shop_token") or user_info.get("token"))
        target = shop_token
    shop = fetch_public(session, "Shop/info", {"token": target})
    token = _check_shop_open(shop)
    if seed:
        user_info = seed.get("user") if isinstance(seed.get("user"), dict) else {}
        seed_shop = seed.get("shop_token") or user_info.get("token")
        if seed_shop != token:
            raise PlatformError("商品所属店铺与平台返回的店铺不一致")
    return shop, seed


def _collect_shop_once(url, session, resolved=None):
    shop, seed = resolved or resolve_target(url, session)
    shop_token = _check_shop_open(shop)
    expected_total = _int_count(shop.get("goods_count"))

    category_names_by_type: dict[str, dict] = {}
    listed_by_type: dict[str, list[dict]] = {}

    for goods_type in GOODS_TYPES:
        if hasattr(session, "check_cancel"):
            session.check_cancel()
        categories = fetch_public(session, "Shop/categoryList", {"token": shop_token, "goods_type": goods_type})
        if not isinstance(categories, list) or any(not isinstance(c, dict) for c in categories):
            raise PlatformError("平台商品分类格式异常")
        category_names_by_type[goods_type] = {
            c.get("id"): str(c.get("name") or "") for c in categories if c.get("id") is not None
        }

        expected_type_count = _int_count(shop.get(f"{goods_type}_count"))
        page = 1
        type_rows = []
        api_type_total = None
        while True:
            if hasattr(session, "check_cancel"):
                session.check_cancel()
            # 全店采集不传 category_id，避免 category_id=-1 返回空列表而误判空店
            listing = fetch_public(
                session,
                "Shop/goodsList",
                {"token": shop_token, "goods_type": goods_type, "current": page, "pageSize": PAGE_SIZE},
            )
            if not isinstance(listing, dict) or not isinstance(listing.get("list"), list):
                raise PlatformError("平台商品列表格式异常，未更新本地快照")
            page_rows = listing["list"]
            page_total = _int_count(listing.get("total"))
            if api_type_total is None:
                api_type_total = page_total
            elif page_total is not None and page_total != api_type_total:
                raise _ListChangedError(f"{goods_type} 分页期间商品总数变化 ({api_type_total} -> {page_total})")

            type_rows.extend(page_rows)
            if len(type_rows) > 5000:
                raise PlatformError("单店单类商品超过 5000 件，未更新本地快照")
            if not page_rows or len(page_rows) < PAGE_SIZE:
                break
            if api_type_total is not None and len(type_rows) >= api_type_total:
                break
            page += 1
            if page > 250:
                raise PlatformError("商品分页超过上限，未更新本地快照")

        if api_type_total is not None and len(type_rows) != api_type_total:
            raise _ListChangedError(f"{goods_type} 分页商品数量({len(type_rows)})与列表总数({api_type_total})不一致")
        if expected_type_count is not None and len(type_rows) != expected_type_count:
            raise _ListChangedError(f"{goods_type} 列表商品数({len(type_rows)})与店铺统计({expected_type_count})不一致")
        listed_by_type[goods_type] = type_rows

    total_listed = sum(len(rows) for rows in listed_by_type.values())
    if expected_total is not None and total_listed != expected_total:
        raise _ListChangedError(f"全店商品列表总数({total_listed})与店铺统计({expected_total})不一致")

    if seed:
        seed_key = seed.get("goods_key")
        if not any(
            isinstance(r, dict) and r.get("goods_key") == seed_key
            for rows in listed_by_type.values()
            for r in rows
        ):
            raise _ListChangedError("入口商品未出现在完整店铺列表中")

    now = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
    goods, details, seen = [], [], set()
    discovered = set()

    # 提取店铺公告与外链按钮中的公开链动小铺链接
    discovered.update(extract_links(shop.get("description")))
    for btn in shop.get("link_buttons") or []:
        if isinstance(btn, dict) and btn.get("link"):
            discovered.update(extract_links(btn.get("link")))

    for goods_type in GOODS_TYPES:
        cat_map = category_names_by_type[goods_type]
        for item in listed_by_type[goods_type]:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not item["name"].strip():
                raise PlatformError("平台返回了无效商品，未更新本地快照")
            key = _identifier(item.get("goods_key"))
            if key in seen:
                raise _ListChangedError(f"商品列表出现重复编号 {key}")
            seen.add(key)

            if hasattr(session, "check_cancel"):
                session.check_cancel()
            try:
                detail_data = fetch_public(session, "Shop/goodsInfo", {"goods_key": key})
            except PlatformError as exc:
                if any(w in str(exc) for w in ("不存在", "下架", "失效", "删除")):
                    raise _ListChangedError(f"商品 {key} 在采集详情时已下架或不存在") from exc
                raise

            if not isinstance(detail_data, dict) or _identifier(detail_data.get("goods_key") or key) != key:
                raise PlatformError(f"商品 {key} 详情格式异常，未更新本地快照")

            actual_type = str(detail_data.get("goods_type") or item.get("goods_type") or goods_type)
            if actual_type not in GOODS_TYPES:
                actual_type = goods_type

            cat_obj = detail_data.get("category") if isinstance(detail_data.get("category"), dict) else (
                item.get("category") if isinstance(item.get("category"), dict) else {}
            )
            cat_id = cat_obj.get("id")
            category_name = str(cat_obj.get("name") or cat_map.get(cat_id) or "")

            price = _number(detail_data.get("price") if detail_data.get("price") is not None else item.get("price"))
            real_price = _number(detail_data.get("real_price") if detail_data.get("real_price") is not None else price)
            market_price = _number(
                detail_data.get("market_price") if detail_data.get("market_price") is not None else item.get("market_price")
            )
            description = str(
                detail_data.get("description")
                if detail_data.get("description") is not None
                else (item.get("description") or "")
            )
            image = str(
                detail_data.get("image") if detail_data.get("image") is not None else (item.get("image") or "")
            )
            status_val = _int_count(detail_data.get("status") if detail_data.get("status") is not None else item.get("status"))
            if status_val is None:
                status_val = 1

            item_link = f"{BASE_URL}/item/{key}"
            common = {
                "goods_key": key,
                "name": str(detail_data.get("name") or item["name"]).strip(),
                "price": price,
                "market_price": market_price,
                "goods_type": actual_type,
                "link": item_link,
                "sales_count_text": None,
                "stock_quantity": None,
                "stock_status": None,
                "category_name": category_name,
                "synced_at": now,
            }
            goods.append({**common, "shop_token": shop_token, "status": status_val})
            details.append({
                **common,
                "shop_token": shop_token,
                "real_price": real_price,
                "image": image,
                "description": description,
            })
            discovered.update(extract_links(description))

    self_shop_url = f"{BASE_URL}/shop/{shop_token}"
    discovered.discard(self_shop_url)

    normalized = {
        "token": shop_token,
        "nickname": str(shop.get("nickname") or shop_token).strip(),
        "login_province": str(shop.get("login_province") or ""),
        "contact_qq": str(shop.get("contact_qq") or ""),
        "contact_mobile": str(shop.get("contact_mobile") or ""),
        "contact_wechat": str(shop.get("contact_wechat") or ""),
        "sell_count": _int_count(shop.get("sell_count")) or 0,
        "sales_count_text": None,
        "deposit_money": _number(shop.get("deposit_money")) or 0.0,
        "goods_count": len(goods),
        "card_count": sum(1 for g in goods if g["goods_type"] == "card"),
        "article_count": sum(1 for g in goods if g["goods_type"] == "article"),
        "resource_count": sum(1 for g in goods if g["goods_type"] == "resource"),
        "equity_count": sum(1 for g in goods if g["goods_type"] == "equity"),
        "auto_count": 0,
        "manual_count": 0,
        "unknown_count": 0,
        "auth_status": _int_count(shop.get("auth_status")),
        "shop_newbie": _int_count(shop.get("shop_newbie")),
        "create_time": _format_create_time(shop.get("create_time")),
        "link": self_shop_url,
        "description": str(shop.get("description") or ""),
        "avatar": str(shop.get("avatar") or ""),
        "fetch_done": 1,
        "synced_at": now,
    }
    return normalized, goods, details, sorted(discovered), now


def sync_shop(url, session=None, resolved=None, store_path=None):
    parse_target(url)
    own_session = session is None
    session = session or create_session()
    target_store = store_path or database_path()
    try:
        last_change_error = None
        for attempt in range(2):
            try:
                normalized, goods, details, discovered_urls, now = _collect_shop_once(
                    url,
                    session=session,
                    resolved=resolved if attempt == 0 else None,
                )
                if hasattr(session, "check_cancel"):
                    session.check_cancel()
                local_store.save_snapshot(normalized, goods, details, path=target_store)
                return {
                    "ok": True,
                    "source": SOURCE_NAME,
                    "shop": normalized,
                    "goods_count": len(goods),
                    "detail_count": len(details),
                    "synced_at": now,
                    "discovered_urls": discovered_urls,
                }
            except _ListChangedError as exc:
                last_change_error = exc
                if attempt == 1:
                    raise PlatformError(f"商品下架或列表变化，重读后仍不完整：{exc}") from exc
        raise PlatformError(str(last_change_error or "同步失败"))
    finally:
        if own_session and hasattr(session, "close"):
            session.close()


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
