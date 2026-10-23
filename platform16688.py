"""读取 16688 的公开店铺/商品接口，不涉及下单或商家后台。"""
import re
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests

import local_store
from public_http import PublicSession

BASE_URL = "https://www.16688.com.cn"
EXAMPLE_URL = f"{BASE_URL}/goods/G74884541"
ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,80}\Z")


class PlatformError(Exception):
    pass


def parse_target(url):
    parsed = urlparse(str(url).strip())
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("链接端口无效") from exc
    if (parsed.scheme not in {"http", "https"}
            or parsed.hostname not in {"www.16688.com.cn", "16688.com.cn"}
            or parsed.username or parsed.password or port not in {None, 80, 443}):
        raise ValueError("请输入 16688.com.cn 的商品或店铺链接")
    parts = parsed.path.strip("/").split("/")
    if len(parts) != 2 or parts[0] not in {"goods", "shop"} or not ID_PATTERN.fullmatch(parts[1]):
        raise ValueError("链接格式应为 /goods/商品编号 或 /shop/店铺编号")
    return parts[0], parts[1]


def normalize_target(url):
    kind, target = parse_target(url)
    return {"kind": kind, "target": target, "url": f"{BASE_URL}/{kind}/{target}"}


def description_links(description):
    links = set()

    class Links(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag != "a":
                return
            for name, value in attrs:
                if name == "href" and value:
                    try:
                        links.add(normalize_target(urljoin(BASE_URL, value))["url"])
                    except ValueError:
                        pass

    Links().feed(str(description or ""))
    return sorted(links)


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


def sales_lower_bound(text):
    """只转换平台公开的付款数量；保留原文，万+ 仅代表下限。"""
    match = re.search(r"(\d+(?:\.\d+)?)\s*(万|亿)?", str(text or "").replace(",", ""))
    if not match:
        return None
    multiplier = {None: 1, "万": 10000, "亿": 100000000}[match[2]]
    return int(Decimal(match[1]) * multiplier)


def stock_quantity(value):
    # 原站手动发货商品使用 -1 表示没有公布可用库存，不能当成负库存或缺货。
    if str(value).strip() == "-1":
        return None
    number = _number(value)
    if number is not None and number != int(number):
        raise PlatformError("平台返回了非整数库存")
    return int(number) if number is not None else None


def fetch_public(session, endpoint, payload):
    try:
        response = session.post(f"{BASE_URL}/shopApi/{endpoint}", json=payload,
                                timeout=(8, 30), allow_redirects=False)
        response.raise_for_status()
        if response.is_redirect or len(response.content) > 8 * 1024 * 1024:
            raise PlatformError("平台响应跳转或体积异常，请稍后重试")
        body = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise PlatformError("16688 公开接口访问失败，请检查网络后重试") from exc
    if not isinstance(body, dict) or body.get("code") != 1 or "data" not in body:
        message = body.get("msg", "响应格式无效") if isinstance(body, dict) else "响应格式无效"
        raise PlatformError(f"16688：{str(message)[:160]}")
    return body["data"]


def resolve_target(url, session):
    kind, target = parse_target(url)
    seed = None
    if kind == "goods":
        seed = fetch_public(session, "goods/detail", {"goods_no": target})
        if not isinstance(seed, dict) or seed.get("goods_no") != target:
            raise PlatformError("商品不存在、已下架或详情格式异常")
        target = _identifier(seed.get("shop_no"))
    shop = fetch_public(session, "shop/detail", {"shop_no": target})
    if not isinstance(shop, dict) or not shop.get("name"):
        raise PlatformError("店铺不存在或店铺信息异常")
    _identifier(shop.get("shop_no"))
    if seed and shop["shop_no"] != seed.get("shop_no"):
        raise PlatformError("商品所属店铺与平台返回的店铺不一致")
    return shop, seed


def sync_shop(url, session=None, resolved=None, store_path=None):
    parse_target(url)
    own_session = session is None
    session = session or PublicSession()
    try:
        session.headers.update({"User-Agent": "wzyp-view/16688-public-reader", "Accept": "application/json"})
        shop, seed = resolved or resolve_target(url, session)
        shop_no = _identifier(shop.get("shop_no"))
        listing = fetch_public(session, "goods/list", {
            "shop_no": shop_no, "goods_category_no": "", "keywords": "", "sort": "default"})
        if not isinstance(listing, dict) or not isinstance(listing.get("list"), list):
            raise PlatformError("平台商品列表格式异常，未更新本地快照")
        rows = listing["list"]
        if len(rows) > 5000:
            raise PlatformError("单店商品超过 5000 件，未更新本地快照")
        # 当前公开接口一次返回整店。若未来改为分页，避免把部分结果写成完整快照。
        if listing.get("has_more") or (listing.get("total") is not None and _number(listing["total"]) != len(rows)):
            raise PlatformError("平台列表已变为分页返回，未更新本地快照")
        if seed and not any(isinstance(row, dict) and row.get("goods_no") == seed["goods_no"] for row in rows):
            raise PlatformError("入口商品未出现在完整店铺列表中，请稍后重试")
        categories = fetch_public(session, "goods_category/list", {"shop_no": shop_no})
        if not isinstance(categories, list) or any(not isinstance(c, dict) for c in categories):
            raise PlatformError("平台商品分类格式异常")
        category_names = {c.get("goods_category_no"): str(c.get("name") or "") for c in categories}
        goods, details, seen, links = [], [], set(), set()
        now = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        for item in rows:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not item["name"].strip():
                raise PlatformError("平台返回了无效商品，未更新本地快照")
            key = _identifier(item.get("goods_no"))
            if key in seen:
                raise PlatformError("平台商品列表出现重复编号，未更新本地快照")
            seen.add(key)
            # 平台前端定义：1、2 为自动发货，3 为人工发货。
            goods_type = {1: "auto", 2: "auto", 3: "manual", "1": "auto", "2": "auto", "3": "manual"}.get(item.get("delivery_method"), "unknown")
            stock = stock_quantity(item.get("stock_available_quantity"))
            common = {
                "goods_key": key, "name": item["name"], "price": _number(item.get("price")),
                "market_price": None, "goods_type": goods_type,
                "link": f"{BASE_URL}/goods/{key}",
                "sales_count_text": str(item.get("sales_count_text") or ""),
                "stock_quantity": stock,
                "stock_status": str(item.get("stock_available_status") or ""),
                "category_name": category_names.get(item.get("goods_category_no"), ""), "synced_at": now,
            }
            goods.append({**common, "shop_token": shop_no, "status": 1})
            details.append({**common, "real_price": None, "image": str(item.get("image") or ""),
                            "description": str(item.get("description") or "")})
            links.update(description_links(item.get("description")))
        merchant = shop.get("merchant") or {}
        if not isinstance(merchant, dict):
            raise PlatformError("平台商户信息格式异常")
        sales_text = str(merchant.get("sales_count_text") or "")
        normalized = {
            "token": shop_no, "nickname": str(shop["name"]),
            "login_province": str(merchant.get("region") or ""),
            "contact_qq": str(shop.get("contact_qq") or ""), "contact_wechat": str(shop.get("contact_wechat") or ""),
            "sell_count": sales_lower_bound(sales_text), "sales_count_text": sales_text,
            "deposit_money": _number(merchant.get("deposit_money")), "goods_count": len(goods),
            "auto_count": sum(g["goods_type"] == "auto" for g in goods),
            "manual_count": sum(g["goods_type"] == "manual" for g in goods),
            "unknown_count": sum(g["goods_type"] == "unknown" for g in goods),
            "auth_status": merchant.get("auth_type"), "shop_newbie": None, "create_time": None,
            "link": f"{BASE_URL}/shop/{_identifier(shop.get('shop_alias') or shop_no)}",
            "description": str(shop.get("announcement") or ""), "avatar": str(shop.get("logo") or ""),
            "fetch_done": 1, "synced_at": now,
        }
        if hasattr(session, "check_cancel"):
            session.check_cancel()
        local_store.save_snapshot(normalized, goods, details, path=store_path)
        return {"ok": True, "source": "16688", "shop": normalized, "goods_count": len(goods),
                "detail_count": len(details), "synced_at": now, "discovered_urls": sorted(links)}
    finally:
        if own_session:
            session.close()


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="16688 平台公开商品采集")
    parser.add_argument("target", nargs="?", default="crawl", help="crawl、refresh 或 16688 商品/店铺链接")
    parser.add_argument("--resume", metavar="JOB_ID", help="继续中断、取消或部分失败的任务")
    parser.add_argument("--url", action="append", default=[], help="为批量采集补充公开链接，可重复")
    arguments = parser.parse_args()
    if arguments.target in {"crawl", "refresh"} or arguments.resume:
        from crawler16688 import run_cli
        raise SystemExit(run_cli("refresh" if arguments.target == "refresh" else "discover",
                                 arguments.url, arguments.resume))
    try:
        result = sync_shop(arguments.target)
    except (ValueError, PlatformError) as exc:
        parser.exit(1, f"同步失败：{exc}\n")
    print(json.dumps({"shop": result["shop"]["nickname"], "goods_count": result["goods_count"],
                      "detail_count": result["detail_count"], "synced_at": result["synced_at"]}, ensure_ascii=False))
