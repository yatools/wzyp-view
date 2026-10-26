"""Immutable adapters for hosted public shops sharing the Shop API protocol."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
import math
import re
from urllib.parse import urljoin, urlparse

import requests
import local_store
from public_http import CrawlBlockedError, CrawlCancelled, failure_details
from crawl_failure import CollectionError as PlatformError, business, from_exception, http_failure

GOODS_TYPES = ("card", "article", "resource", "equity")
PAGE_SIZE = 20
ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,80}\Z")

class _ListChangedError(PlatformError):
    def __init__(self, message):
        super().__init__(message, "snapshot_changed")

@dataclass(frozen=True)
class HostedShopPlatform:
    SOURCE_NAME: str
    SOURCE_KEY: str
    BASE_URL: str
    ALLOWED_HOSTS: frozenset
    display_name: str
    PlatformError = PlatformError

    @property
    def EXAMPLE_URL(self):
        return self.BASE_URL + "/shop/0000"

    def database_path(self):
        return local_store.database_path(self.SOURCE_NAME)

    def session_factory(self, stop=None):
        from hosted_http import HostedSession
        return HostedSession(self.BASE_URL, stop=stop)

    def seed_urls(self):
        if self.SOURCE_NAME != "catfk":
            return []
        from pathlib import Path
        import sqlite3
        from discovery import database_path
        links = set()
        path = Path(database_path()).resolve()
        if path.exists():
            connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            try:
                for row in connection.execute("SELECT DISTINCT url FROM discovery_candidates WHERE url LIKE 'https://catfk.com/%'"):
                    try:
                        links.add(self.normalize_target(row[0])["url"])
                    except ValueError:
                        continue
            finally:
                connection.close()
        return sorted(links)

    def discover_public(self, session=None, progress=None):
        if self.SOURCE_NAME != "catfk":
            return []
        from crawler16688 import discover_public_links
        links = set()
        own_session = session is None
        session = session or self.index_session_factory()
        try:
            try:
                urls = discover_public_links(session, progress)
            except CrawlCancelled:
                raise
            except Exception as exc:
                raise PlatformError("云猫发现索引暂不可用，已有入口继续采集；可稍后重试索引", failure=from_exception(exc)) from exc
            for url in urls:
                try:
                    links.add(self.normalize_target(url)["url"])
                except ValueError:
                    continue
            return sorted(links)
        finally:
            if own_session:
                session.close()

    @staticmethod
    def index_session_factory(stop=None):
        from public_http import PublicSession
        return PublicSession(stop=stop)

    def parse_target(self, url):
        raw = str(url or "").strip()
        parsed = urlparse(raw)
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("链接端口无效") from exc
        host = (parsed.hostname or "").lower()
        if (
            parsed.scheme not in {"http", "https"}
            or host not in self.ALLOWED_HOSTS
            or parsed.username
            or parsed.password
            or port not in {None, 80, 443}
        ):
            raise ValueError(f"请输入{self.display_name}的店铺或商品链接")
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if len(parts) < 2 or parts[0] not in {"shop", "item", "goods"} or not ID_PATTERN.fullmatch(parts[1]):
            raise ValueError("链接格式应为 /shop/店铺token 或 /item/商品编号")
        kind = "shop" if parts[0] == "shop" else "goods"
        return kind, parts[1]


    def normalize_target(self, url):
        kind, target = self.parse_target(url)
        path_prefix = "shop" if kind == "shop" else "item"
        return {"kind": kind, "target": target, "url": f"{self.BASE_URL}/{path_prefix}/{target}"}


    def extract_links(self, *texts):
        """从店铺公告、商品描述等文本中提取公开店铺/商品链接（兼容 HTML 与纯文本）。"""
        links = set()

        class _AnchorParser(HTMLParser):
            def handle_starttag(parser_self, tag, attrs):
                if tag != "a":
                    return
                for name, value in attrs:
                    if name == "href" and value:
                        try:
                            links.add(self.normalize_target(urljoin(self.BASE_URL, value.strip()))["url"])
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
            hosts = "|".join(re.escape(host) for host in self.ALLOWED_HOSTS)
            for match in re.findall(rf"https?://(?:{hosts})/(?:shop|item|goods)/[A-Za-z0-9_-]{{1,80}}", content, re.I):
                try:
                    links.add(self.normalize_target(match)["url"])
                except ValueError:
                    pass
        return sorted(links)


    description_links = extract_links


    def _identifier(self, value):
        if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
            raise PlatformError("平台返回了无效的商品或店铺编号")
        return value


    def _number(self, value):
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


    def _int_count(self, value):
        if value is None or value == "" or isinstance(value, bool):
            return None
        num = self._number(value)
        if num is None:
            return None
        if num != int(num):
            raise PlatformError("平台返回了非整数计数")
        return int(num)


    def _format_create_time(self, value):
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
            ts = int(float(value))
            if ts <= 0:
                return None
            return datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        return str(value).strip() or None


    def fetch_public(self, session, endpoint, payload):
        try:
            response = session.post(
                f"{self.BASE_URL}/shopApi/{endpoint.lstrip('/')}",
                json=payload,
                timeout=(8, 25),
                allow_redirects=False,
                expect_json=True,
            )
            if response.status_code == 403:
                raise CrawlBlockedError("HTTP 403 访问被拒绝，可能触发风控或人机验证", failure_details(response))
            if response.status_code >= 400:
                raise PlatformError(f"{self.display_name}接口 HTTP {response.status_code}", failure=http_failure(response))
            if len(response.content) > 8 * 1024 * 1024:
                raise PlatformError("平台响应体积异常，请稍后重试")
            body = response.json()
        except (CrawlCancelled, CrawlBlockedError):
            raise
        except (requests.RequestException, ValueError, OSError) as exc:
            raise PlatformError(f"{self.display_name}公开接口访问失败，请检查网络后重试", failure=from_exception(exc)) from exc

        if not isinstance(body, dict):
            raise PlatformError(f"{self.display_name}响应格式无效")
        if body.get("code") in (403, -403):
            raise CrawlBlockedError(f"{self.display_name}触发访问限制：{str(body.get('msg') or '403')[:160]}", failure_details(response))
        if body.get("code") != 1 or "data" not in body or body["data"] is None:
            message = str(body.get("msg") or "响应格式无效")[:160]
            raise PlatformError(f"{self.display_name}：{message}", failure=business(message))
        return body["data"]


    def _check_shop_open(self, shop: dict) -> str:
        if not isinstance(shop, dict):
            raise PlatformError("店铺不存在或店铺信息异常")
        token = self._identifier(shop.get("token") or shop.get("shop_no"))
        custom_status = shop.get("custom_status")
        if custom_status == 0 or str(custom_status) == "0":
            reason = str(shop.get("custom_status_msg") or "").strip()
            raise PlatformError(f"店铺已打烊：{reason}" if reason else "店铺已打烊", "shop_closed")
        nickname = str(shop.get("nickname") or "").strip()
        if not nickname:
            raise PlatformError("店铺名称缺失或店铺信息异常")
        shop["token"] = token
        shop["shop_no"] = token
        return token


    def resolve_target(self, url, session):
        if hasattr(session, "set_entry_url"):
            session.set_entry_url(self.normalize_target(url)["url"])
        kind, target = self.parse_target(url)
        seed = None
        if kind == "goods":
            seed = self.fetch_public(session, "Shop/goodsInfo", {"goods_key": target})
            if not isinstance(seed, dict) or seed.get("goods_key") != target:
                raise PlatformError("商品不存在、已下架或详情格式异常")
            user_info = seed.get("user") if isinstance(seed.get("user"), dict) else {}
            shop_token = self._identifier(seed.get("shop_token") or user_info.get("token"))
            target = shop_token
        shop = self.fetch_public(session, "Shop/info", {"token": target})
        token = self._check_shop_open(shop)
        if seed:
            user_info = seed.get("user") if isinstance(seed.get("user"), dict) else {}
            seed_shop = seed.get("shop_token") or user_info.get("token")
            if seed_shop != token:
                raise PlatformError("商品所属店铺与平台返回的店铺不一致")
        return shop, seed


    def probe_shop(self, url, session, resolved=None):
        from shop_preview import probe_hosted
        return probe_hosted(self, url, session, resolved)

    def _collect_shop_once(self, url, session, resolved=None):
        shop, seed = resolved or self.resolve_target(url, session)
        shop_token = self._check_shop_open(shop)
        expected_total = self._int_count(shop.get("goods_count"))

        category_names_by_type: dict[str, dict] = {}
        listed_by_type: dict[str, list[dict]] = {}

        for goods_type in GOODS_TYPES:
            if hasattr(session, "check_cancel"):
                session.check_cancel()
            categories = self.fetch_public(session, "Shop/categoryList", {"token": shop_token, "goods_type": goods_type})
            if not isinstance(categories, list) or any(not isinstance(c, dict) for c in categories):
                raise PlatformError("平台商品分类格式异常")
            category_names_by_type[goods_type] = {
                c.get("id"): str(c.get("name") or "") for c in categories if c.get("id") is not None
            }

            expected_type_count = self._int_count(shop.get(f"{goods_type}_count"))
            page = 1
            type_rows = []
            api_type_total = None
            while True:
                if hasattr(session, "check_cancel"):
                    session.check_cancel()
                # 全店采集不传 category_id，避免 category_id=-1 返回空列表而误判空店
                listing = self.fetch_public(
                    session,
                    "Shop/goodsList",
                    {"token": shop_token, "goods_type": goods_type, "current": page, "pageSize": PAGE_SIZE},
                )
                if not isinstance(listing, dict) or not isinstance(listing.get("list"), list):
                    raise PlatformError("平台商品列表格式异常，未更新本地快照")
                page_rows = listing["list"]
                page_total = self._int_count(listing.get("total"))
                if page_total is None:
                    raise PlatformError("平台商品列表缺少有效总数，未更新本地快照")
                if api_type_total is None:
                    api_type_total = page_total
                elif page_total != api_type_total:
                    raise _ListChangedError(f"{goods_type} 分页期间商品总数变化 ({api_type_total} -> {page_total})")

                type_rows.extend(page_rows)
                if len(type_rows) > 5000:
                    raise PlatformError("单店单类商品超过 5000 件，未更新本地快照")
                if not page_rows or len(page_rows) < PAGE_SIZE:
                    break
                if len(type_rows) >= api_type_total:
                    break
                page += 1
                if page > 250:
                    raise PlatformError("商品分页超过上限，未更新本地快照")

            if len(type_rows) != api_type_total:
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
        discovered.update(self.extract_links(shop.get("description")))
        for btn in shop.get("link_buttons") or []:
            if isinstance(btn, dict) and btn.get("link"):
                discovered.update(self.extract_links(btn.get("link")))

        for goods_type in GOODS_TYPES:
            cat_map = category_names_by_type[goods_type]
            for item in listed_by_type[goods_type]:
                if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not item["name"].strip():
                    raise PlatformError("平台返回了无效商品，未更新本地快照")
                key = self._identifier(item.get("goods_key"))
                if key in seen:
                    raise _ListChangedError(f"商品列表出现重复编号 {key}")
                seen.add(key)

                if hasattr(session, "check_cancel"):
                    session.check_cancel()
                try:
                    detail_data = self.fetch_public(session, "Shop/goodsInfo", {"goods_key": key})
                except PlatformError as exc:
                    if exc.failure["category"] == "not_found":
                        raise _ListChangedError(f"商品 {key} 在采集详情时已下架或不存在") from exc
                    raise

                if not isinstance(detail_data, dict) or self._identifier(detail_data.get("goods_key") or key) != key:
                    raise PlatformError(f"商品 {key} 详情格式异常，未更新本地快照")

                actual_type = str(detail_data.get("goods_type") or item.get("goods_type") or goods_type)
                if actual_type not in GOODS_TYPES:
                    actual_type = goods_type

                cat_obj = detail_data.get("category") if isinstance(detail_data.get("category"), dict) else (
                    item.get("category") if isinstance(item.get("category"), dict) else {}
                )
                cat_id = cat_obj.get("id")
                category_name = str(cat_obj.get("name") or cat_map.get(cat_id) or "")

                price = self._number(detail_data.get("price") if detail_data.get("price") is not None else item.get("price"))
                real_price = self._number(detail_data.get("real_price") if detail_data.get("real_price") is not None else price)
                market_price = self._number(
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
                status_val = self._int_count(detail_data.get("status") if detail_data.get("status") is not None else item.get("status"))
                if status_val is None:
                    status_val = 1

                item_link = f"{self.BASE_URL}/item/{key}"
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
                discovered.update(self.extract_links(description))

        self_shop_url = f"{self.BASE_URL}/shop/{shop_token}"
        discovered.discard(self_shop_url)

        normalized = {
            "token": shop_token,
            "nickname": str(shop.get("nickname") or shop_token).strip(),
            "login_province": str(shop.get("login_province") or ""),
            "contact_qq": str(shop.get("contact_qq") or ""),
            "contact_mobile": str(shop.get("contact_mobile") or ""),
            "contact_wechat": str(shop.get("contact_wechat") or ""),
            "sell_count": self._int_count(shop.get("sell_count")) or 0,
            "sales_count_text": None,
            "deposit_money": self._number(shop.get("deposit_money")) or 0.0,
            "goods_count": len(goods),
            "card_count": sum(1 for g in goods if g["goods_type"] == "card"),
            "article_count": sum(1 for g in goods if g["goods_type"] == "article"),
            "resource_count": sum(1 for g in goods if g["goods_type"] == "resource"),
            "equity_count": sum(1 for g in goods if g["goods_type"] == "equity"),
            "auto_count": 0,
            "manual_count": 0,
            "unknown_count": 0,
            "auth_status": self._int_count(shop.get("auth_status")),
            "shop_newbie": self._int_count(shop.get("shop_newbie")),
            "create_time": self._format_create_time(shop.get("create_time")),
            "link": self_shop_url,
            "description": str(shop.get("description") or ""),
            "avatar": str(shop.get("avatar") or ""),
            "fetch_done": 1,
            "synced_at": now,
        }
        return normalized, goods, details, sorted(discovered), now


    def sync_shop(self, url, session=None, resolved=None, store_path=None):
        self.parse_target(url)
        own_session = session is None
        session = session or self.session_factory()
        target_store = store_path or self.database_path()
        try:
            last_change_error = None
            for attempt in range(2):
                try:
                    normalized, goods, details, discovered_urls, now = self._collect_shop_once(
                        url,
                        session=session,
                        resolved=resolved if attempt == 0 else None,
                    )
                    if hasattr(session, "check_cancel"):
                        session.check_cancel()
                    local_store.save_snapshot(normalized, goods, details, path=target_store)
                    return {
                        "ok": True,
                        "source": self.SOURCE_NAME,
                        "shop": normalized,
                        "goods_count": len(goods),
                        "detail_count": len(details),
                        "synced_at": now,
                        "discovered_urls": discovered_urls,
                    }
                except _ListChangedError as exc:
                    last_change_error = exc
                    if attempt == 1:
                        raise PlatformError(f"商品下架或列表变化，重读后仍不完整：{exc}", "snapshot_changed") from exc
            raise PlatformError(str(last_change_error or "同步失败"))
        finally:
            if own_session and hasattr(session, "close"):
                session.close()
