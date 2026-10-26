"""Dujiao-Next 公开 API 与经典独角数卡公开页面适配。"""
from datetime import datetime, timezone
import hashlib
import math
import re
from urllib.parse import quote, urljoin, urlsplit

from bs4 import BeautifulSoup
import requests

import local_store
from link_discovery import extract_links
from public_http import CrawlBlockedError, CrawlCancelled
from shop_http import SafeSession, normalize_url, origin_of
from crawl_failure import CollectionError as PlatformError, from_exception, http_failure


def text_value(value):
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("zh-CN", "zh_CN", "zh", "zh-TW", "en-US", "en"):
            if isinstance(value.get(key), str) and value[key].strip():
                return value[key].strip()
        return next((v.strip() for v in value.values() if isinstance(v, str) and v.strip()), "")
    return ""


def number(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise PlatformError("公开价格格式异常")
    if isinstance(value, dict):
        value = value.get("amount", value.get("value"))
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise PlatformError("公开价格格式异常") from exc
    if not math.isfinite(result) or result < 0:
        raise PlatformError("公开价格不是有效非负数")
    return result


def identifier(value):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9]\d{0,19}", str(value)):
        raise PlatformError("商品或 SKU 编号不符合公开接口契约")
    return str(value)


def config_value(data, *keys):
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = text_value(data.get(key))
        if value:
            return value
    for key in ("site", "shop", "site_config", "site_info", "settings", "brand"):
        value = config_value(data.get(key), *keys)
        if value:
            return value
    return ""


def _api(session, origin, path, params=None):
    try:
        response = session.get(origin + "/api/v1/public/" + path, params=params or {})
        if response.status_code != 200:
            raise PlatformError(f"公开接口 {path} 返回 HTTP {response.status_code}", failure=http_failure(response))
        body = response.json()
        if (not isinstance(body, dict) or "data" not in body or body.get("success") is False
                or body.get("code", 0) not in (0, 1, 200, "0", "1", "200")):
            raise PlatformError(f"公开接口 {path} 响应契约不匹配")
        return body["data"], body
    except (requests.RequestException, ValueError, TypeError) as exc:
        raise PlatformError(f"公开接口 {path} 无法读取，已保留旧快照", failure=from_exception(exc)) from exc


def _listing_page(data, envelope):
    rows = data if isinstance(data, list) else data.get("items", data.get("list")) if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise PlatformError("商品列表结构异常")
    pagination = envelope.get("pagination") or (data.get("pagination") if isinstance(data, dict) else None) or {}
    if not isinstance(pagination, dict):
        raise PlatformError("商品分页结构异常")
    total = pagination.get("total", data.get("total") if isinstance(data, dict) else None)
    if isinstance(total, bool) or not isinstance(total, int) or total < 0 or total > 20000:
        raise PlatformError("商品列表缺少有效总数，不能确认完整快照")
    return rows, total


def _quantity(item, delivery):
    if item.get("stock_quantity_hidden") or item.get("stock_display_mode") in {"hidden", "range", "status"}:
        return None
    value = item.get("auto_stock_available") if delivery == "auto" else None
    if value is None:
        value = item.get("stock_quantity", item.get("stock_available_quantity"))
    if value is None and delivery == "manual" and "manual_stock_total" in item and "manual_stock_sold" in item:
        value = item["manual_stock_total"] - item["manual_stock_sold"]
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _stock(item, quantity):
    raw = str(item.get("stock_status") or "").lower()
    if item.get("is_sold_out") is True or raw in {"out", "sold_out", "out_of_stock"}:
        return "out_of_stock"
    if raw in {"in_stock", "available", "normal", "high", "low"}:
        return "in_stock"
    if quantity is not None:
        return "in_stock" if quantity > 0 else "out_of_stock"
    return "unknown"


class IndependentPlatform:
    MULTI_ORIGIN = True
    BASE_URL = ""
    PlatformError = PlatformError

    def __init__(self, source):
        self.SOURCE_KEY = source

    def database_path(self):
        return local_store.database_path(self.SOURCE_KEY)

    @staticmethod
    def session_factory(stop=None):
        return SafeSession(stop=stop)

    def normalize_target(self, url):
        origin = origin_of(url)
        return {"kind": "shop", "target": hashlib.sha256(origin.encode()).hexdigest(), "url": origin + "/"}

    def parse_target(self, url):
        target = self.normalize_target(url)
        return target["kind"], target["target"]

    @staticmethod
    def shop_url(shop_no, item_url):
        return origin_of(item_url) + "/"

    @staticmethod
    def discover_public(session, progress=None):
        if progress:
            progress("manual-and-recursive", 0)
        return []

    def probe_shop(self, url, session, resolved=None):
        from shop_preview import probe_independent
        return probe_independent(self, url, session, resolved)

    def resolve_target(self, url, session, preview_only=False):
        origin = origin_of(url)
        if self.SOURCE_KEY == "dujiao-next":
            data, _ = _api(session, origin, "config")
            if not isinstance(data, dict):
                raise PlatformError("不是受支持的 Dujiao-Next 配置接口", "unsupported_template")
            # 必须同时验证商品契约；目录页、普通 JSON 不能冒充货铺。
            preview, envelope = _api(session, origin, "products", {"page": 1, "page_size": 100})
            _listing_page(preview, envelope)
            payload = {"config": data, "preview": (preview, envelope)}
            name = config_value(data, "site_name", "site_title", "shop_name", "name", "title") or urlsplit(origin).hostname
        else:
            response = session.get(origin + "/")
            if response.status_code != 200:
                raise PlatformError(f"货铺首页返回 HTTP {response.status_code}", failure=http_failure(response))
            origin = origin_of(getattr(response, "url", "") or origin)
            soup = BeautifulSoup(response.text, "html.parser")
            marker = soup.select_one(".am-container, .am-g, .layui-container, .layui-tab, .hyper-wrapper, .product-list, .goods-list, [data-dujiaoka], .good-trade")
            known = re.search(r"dujiaoka|独角数卡|/assets/(?:amazeui|layui|choice|hyper|luna)", response.text, re.I)
            links = self._classic_links(soup, origin)
            directory_rows = self._catalog_cards(soup, origin)
            choice = bool(soup.select_one("select#productlist[name='pid']") and "/api/typelist" in response.text and "/api/proudctinfo" in response.text)
            empty = soup.select_one("[data-empty-catalog], .goods-empty, .product-empty") or (soup.select_one(".hyper-wrapper") if not soup.select_one(".home-card") else None)
            if not (known or marker) or (not links and not empty and not choice and not directory_rows):
                raise PlatformError("未识别到经典独角数卡 AmazeUI/LayUI/Choice 完整目录；不支持的模板或验证页", "unsupported_template")
            visited = {origin + "/"}
            current = soup
            while not choice and not preview_only:
                next_link = current.select_one("a[rel='next'], .pagination .next a[href]")
                if not next_link:
                    break
                next_url = normalize_url(urljoin(origin + "/", next_link.get("href", "")))
                if origin_of(next_url) != origin or next_url in visited or len(visited) >= 1000:
                    raise PlatformError("货铺目录分页跳转异常，保留旧快照")
                visited.add(next_url)
                page = session.get(next_url)
                if page.status_code != 200:
                    raise PlatformError("货铺目录分页读取失败，保留旧快照")
                current = BeautifulSoup(page.text, "html.parser")
                more = self._classic_links(current, origin)
                extra = self._catalog_cards(current, origin)
                if not more and not extra:
                    raise PlatformError("货铺目录分页无法核验，保留旧快照")
                if more and not (set(more) - set(links)) and not extra:
                    raise PlatformError("货铺目录分页重复，保留旧快照")
                links.update(more)
                directory_rows.update(extra)
            name_node = soup.select_one("meta[property='og:site_name']")
            name = (name_node.get("content") if name_node else None) or (soup.title.get_text(strip=True) if soup.title else urlsplit(origin).hostname)
            payload = {"html": response.text, "links": links, "choice": choice, "directory_rows": list(directory_rows.values())}
        return {"shop_no": hashlib.sha256(origin.encode()).hexdigest(), "name": name,
                "origin": origin}, payload

    @staticmethod
    def _classic_links(soup, origin):
        result = {}
        for anchor in soup.select("a[href]"):
            url = urljoin(origin + "/", anchor.get("href", ""))
            parsed = urlsplit(url)
            try:
                same_origin = origin_of(url) == origin
            except ValueError:
                continue
            if not same_origin:
                continue
            match = re.fullmatch(r"/buy/(\d+)/?", parsed.path)
            if match:
                result[match[1]] = origin + "/buy/" + match[1]
        return result

    @staticmethod
    def _catalog_cards(soup, origin):
        """2.x 的缺货卡片不公开购买链接，只保留可核验目录字段，不猜测编号。"""
        result = {}
        for card in soup.select(".hyper-wrapper .home-card"):
            if re.fullmatch(r"/buy/\d+/?", urlsplit(card.get("href", "")).path):
                continue
            title, price = card.select_one(".name"), card.select_one(".price")
            if title is None or price is None:
                raise PlatformError("目录卡片缺少名称或标价，不能确认完整快照")
            name = title.get_text(" ", strip=True)
            match = re.fullmatch(r"\s*[¥￥]\s*([\d,]+(?:\.\d+)?)\s*", price.get_text(" ", strip=True))
            sold_out = card.select_one(".ribbon-two-danger") or re.search(r"缺货|售罄|sold out|out of stock", card.get_text(" ", strip=True), re.I)
            if not name or not match or not sold_out:
                raise PlatformError("无法核验无直达链接的目录条目")
            image_node = card.select_one("img")
            image = urljoin(origin, image_node.get("data-src") or image_node.get("src") or "") if image_node else ""
            identity = hashlib.sha256((name + "\n" + image).encode()).hexdigest()
            result[identity] = {"native_id": "catalog:" + identity, "name": name,
                "sku_label": "缺货目录条目，原站未公开商品直达链接", "catalog_entry": 1,
                "price": number(match[1].replace(",", "")), "currency": "CNY", "description": "",
                "image": image, "goods_type": "unknown", "delivery_method": "unknown",
                "stock_quantity": 0, "stock_status": "out_of_stock", "category_name": "", "link": origin + "/"}
        return result

    def _next_rows(self, origin, payload, session):
        categories, _ = _api(session, origin, "categories")
        if not isinstance(categories, (list, dict)):
            raise PlatformError("公开分类响应异常")
        rows, total = _listing_page(*payload["preview"])
        if hasattr(session, "allow_catalog_size"):
            session.allow_catalog_size(total)
        listing, seen = list(rows), set()
        page = 1
        while len(listing) < total:
            session.check_cancel() if hasattr(session, "check_cancel") else None
            page += 1
            data, envelope = _api(session, origin, "products", {"page": page, "page_size": 100})
            rows, expected = _listing_page(data, envelope)
            if expected != total or not rows:
                raise PlatformError("商品分页数量变化或缺页，已保留旧快照", "snapshot_changed")
            listing.extend(rows)
        if len(listing) != total:
            raise PlatformError("商品分页数量不完整", "snapshot_changed")
        currency = config_value(payload["config"], "currency", "currency_code", "site_currency").upper()
        currency = currency if re.fullmatch(r"[A-Z]{3}", currency) else ""
        for item in listing:
            if not isinstance(item, dict) or not item.get("slug") or item.get("id") is None:
                raise PlatformError("商品列表缺少稳定标识")
            identity = identifier(item["id"])
            if identity in seen:
                raise PlatformError("商品分页重复，已保留旧快照", "snapshot_changed")
            seen.add(identity)
            try:
                detail, _ = _api(session, origin, "products/" + quote(str(item["slug"]), safe=""))
            except PlatformError as exc:
                if exc.failure["category"] == "not_found":
                    raise PlatformError("商品在读取完整快照期间下架，已保留旧快照", "snapshot_changed") from exc
                raise
            if not isinstance(detail, dict) or str(detail.get("id")) != identity or detail.get("slug") != item["slug"]:
                raise PlatformError("商品详情标识与列表不一致", "snapshot_changed")
            name = text_value(detail.get("title") or detail.get("name"))
            if not name:
                raise PlatformError("商品详情缺少名称")
            description = text_value(detail.get("content")) or text_value(detail.get("description"))
            category = text_value((detail.get("category") or {}).get("name") or (detail.get("category") or {}).get("title"))
            fulfillment = str(detail.get("fulfillment_type") or "").lower()
            delivery = {"auto": "auto", "automatic": "auto", "manual": "manual"}.get(fulfillment, "unknown")
            skus = detail.get("skus")
            if skus is not None and not isinstance(skus, list):
                raise PlatformError("SKU 列表格式异常")
            variants = skus or [detail]
            seen_skus = set()
            for sku in variants:
                if not isinstance(sku, dict):
                    raise PlatformError("SKU 格式异常")
                if sku.get("is_active") is False:
                    continue
                if skus and sku.get("id") is None:
                    raise PlatformError("SKU 缺少稳定编号")
                sku_id = identifier(sku.get("id")) if skus else "single"
                if sku_id in seen_skus:
                    raise PlatformError("SKU 编号重复")
                seen_skus.add(sku_id)
                specs = sku.get("spec_values") or {}
                if isinstance(specs, dict) and any(k in specs for k in ("zh-CN", "zh_CN", "zh", "en-US", "en")):
                    label = text_value(specs)
                else:
                    label = " / ".join(f"{k}: {text_value(v) or str(v)}" for k, v in specs.items()) if isinstance(specs, dict) else text_value(specs)
                label = label or str(sku.get("sku_code") or "")
                quantity = _quantity(sku, delivery)
                images = detail.get("images") or []
                image = images[0] if isinstance(images, list) and images else ""
                yield {"native_id": identity + ":" + sku_id, "name": name, "sku_label": label,
                       "price": number(sku.get("price_amount", sku.get("price"))), "currency": currency,
                       "description": description, "image": urljoin(origin, image) if isinstance(image, str) else "",
                       "goods_type": delivery, "delivery_method": delivery,
                       "stock_quantity": quantity, "stock_status": _stock(sku, quantity),
                       "category_name": category,
                       "link": origin + "/products/" + quote(str(detail["slug"]), safe="")}

    def _classic_rows(self, origin, payload, session):
        if payload.get("choice"):
            yield from self._choice_rows(origin, payload, session)
            return
        yield from payload.get("directory_rows", [])
        for native_id, url in sorted(payload["links"].items()):
            response = session.get(url)
            if response.status_code != 200:
                raise PlatformError(f"商品 {native_id} 详情读取失败，保留旧快照")
            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.select_one("#goodsname, #goods-name, .goods-name, .product-title, h1[itemprop='name'], .layui-card-header > span[style], .buy-shop #buy-form .form-group h3")
            price = soup.select_one("#price, .product-price, .actual-price, [itemprop='price'], .goods-price, #goods-price, .buy-price, .goods-msg .price-num")
            identity = soup.select_one("input[name='pid'], input[name='goods_id'], input[name='gid'], input[name='id']")
            # 经典 Bootstrap 变体保留 gid 与独角数卡署名，价格直接写在 h6。
            # 仅接受一个有明确货币和价格标签的节点，不从描述中的数字猜价格。
            bootstrap = bool(soup.select_one("input[name='gid']") and
                             re.search(r"dujiaoka|独角数卡", response.text, re.I))
            if bootstrap:
                title = title or soup.select_one("h3.card-title")
                if price is None:
                    labeled_prices = [node for node in soup.select(".card-body h6")
                        if re.fullmatch(r"\s*(?:价格|售价)\s*[:：]\s*[¥￥$€£]\s*[\d,]+(?:\.\d+)?\s*", node.get_text(" ", strip=True))]
                    if len(labeled_prices) == 1:
                        price = labeled_prices[0]
            if not title or not price or not identity or str(identity.get("value")) != native_id:
                raise PlatformError(f"商品 {native_id} 模板不支持或缺少关键字段", "unsupported_template")
            price_text = price.get("content") or price.get_text(" ", strip=True)
            if bootstrap:
                price_text = re.sub(r"^\s*(?:价格|售价)\s*[:：]\s*", "", price_text)
            match = re.fullmatch(r"\s*(?:[¥￥$€£]|CNY|USD|EUR|GBP)?\s*([\d,]+(?:\.\d+)?)\s*(?:元)?\s*", price_text)
            if not match:
                raise PlatformError(f"商品 {native_id} 标价不明确")
            currency_node = soup.select_one("[itemprop='priceCurrency'], [data-currency]")
            currency = (currency_node.get("content") or currency_node.get("data-currency") or "") if currency_node else ""
            around_price = price.parent.get_text(" ", strip=True)
            if not currency:
                currency = "CNY" if re.search(r"[¥￥]|人民币|元", around_price) else "USD" if "$" in price_text else ""
            quantity_node = soup.select_one("input[name='order_number'][max], input[name='buy_amount'][max], [data-stock]")
            stock_text = (quantity_node.get("max") or quantity_node.get("data-stock") or "") if quantity_node else ""
            quantity = int(stock_text) if re.fullmatch(r"\d+", stock_text) else None
            body_text = soup.get_text(" ", strip=True)
            if quantity is None:
                stock_match = re.search(r"(?:库存|In Stock)\s*[（(:：]?\s*(\d+)", body_text, re.I)
                quantity = int(stock_match[1]) if stock_match else None
            content = soup.select_one("#description, #goods-description, .goods-description, .product-description, .detail-content, .description, #goods-content, .trade-gooddesc, .goods-info, .buy-product, .intro")
            # 常见模板的购买说明位于独立 tab，不读取订单表单或输入内容。
            if content is None:
                content = soup.select_one(".layui-tab-content, .am-tabs-bd")
            if content is None and bootstrap:
                for header in soup.select(".card-header"):
                    if header.get_text(" ", strip=True) == "商品描述":
                        content = header.find_next_sibling("div", class_="card-body")
                        break
            image = soup.select_one("meta[property='og:image'], .trade-goodimg img, img[itemprop='image']")
            clean_title = BeautifulSoup(str(title), "html.parser")
            for badge in clean_title.select(".badge, .small-tips, svg"):
                badge.decompose()
            delivery = "auto" if re.search(r"自动发货|自助发货|automatic delivery", body_text, re.I) else "manual" if re.search(r"手动发货|人工处理|manual", body_text, re.I) else "unknown"
            yield {"native_id": native_id, "name": clean_title.get_text(" ", strip=True), "sku_label": "",
                   "price": number(match[1].replace(",", "")), "currency": currency.upper(),
                   "description": str(content) if content else "", "image": urljoin(origin, (image.get("content") or image.get("src") or "")) if image else "",
                   "goods_type": delivery, "delivery_method": delivery,
                   "stock_quantity": quantity, "stock_status": "unknown" if quantity is None else "in_stock" if quantity > 0 else "out_of_stock",
                   "category_name": "", "link": url}

    @staticmethod
    def _choice_rows(origin, payload, session):
        soup = BeautifulSoup(payload["html"], "html.parser")
        token_node = soup.select_one("input[name='_token']")
        csrf = token_node.get("value", "") if token_node else ""
        def read(path, data=None):
            response = session.get(origin + path) if data is None else session.post(origin + path, data={**data, "_token": csrf})
            try:
                body = response.json()
                if response.status_code != 200 or not isinstance(body, dict) or str(body.get("code")) != "1" or not isinstance(body.get("data"), dict):
                    raise ValueError("公开查询契约不匹配")
                return body["data"]
            except (ValueError, TypeError) as exc:
                raise PlatformError("Choice 公开商品目录不完整，已保留旧快照") from exc
        groups = read("/api/typelist").get("typelist")
        if not isinstance(groups, list):
            raise PlatformError("Choice 分类结构异常")
        seen = set()
        for group in groups:
            if not isinstance(group, dict) or group.get("id") is None:
                raise PlatformError("Choice 分类缺少编号")
            if group.get("password"):
                continue
            products = read("/api/productlist", {"tid": group["id"]}).get("products")
            if not isinstance(products, list):
                raise PlatformError("Choice 商品目录结构异常")
            for entry in products:
                if not isinstance(entry, dict) or entry.get("id") is None:
                    raise PlatformError("Choice 商品缺少编号")
                if entry.get("password"):
                    continue
                native_id = identifier(entry["id"])
                if native_id in seen:
                    raise PlatformError("Choice 商品编号重复")
                seen.add(native_id)
                item = read("/api/proudctinfo", {"pid": entry["id"]})
                name = text_value(item.get("pd_name") or item.get("name"))
                if not name or str(item.get("id", native_id)) != native_id:
                    raise PlatformError("Choice 商品详情标识或名称异常")
                stock = item.get("in_stock")
                quantity = int(stock) if isinstance(stock, (str, int)) and re.fullmatch(r"\d+", str(stock)) else None
                delivery = {1: "auto", "1": "auto", 2: "manual", "2": "manual"}.get(item.get("pd_type"), "unknown")
                yield {"native_id": native_id, "name": name, "sku_label": "", "price": number(item.get("actual_price")),
                       "currency": "CNY", "description": text_value(item.get("pd_info") or item.get("description") or item.get("pd_desc")),
                       "image": urljoin(origin, str(item.get("pd_picture") or "")),
                       "goods_type": delivery, "delivery_method": delivery, "stock_quantity": quantity,
                       "stock_status": "unknown" if quantity is None else "in_stock" if quantity > 0 else "out_of_stock",
                       "category_name": text_value(group.get("name")), "link": origin + "/buy/" + quote(native_id, safe="")}

    def sync_shop(self, url, session=None, resolved=None, store_path=None):
        own = session is None
        session = session or self.session_factory()
        try:
            shop, payload = resolved or self.resolve_target(url, session)
            origin, token = shop["origin"], shop["shop_no"]
            rows = self._next_rows(origin, payload, session) if self.SOURCE_KEY == "dujiao-next" else self._classic_rows(origin, payload, session)
            goods, details, seen, links = [], [], set(), set()
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for row in rows:
                row = dict(row)
                native_id = row.pop("native_id")
                key = token + ":" + native_id
                if key in seen or not row["name"]:
                    raise PlatformError("商品标识重复或名称缺失")
                seen.add(key)
                description, image = row.pop("description"), row.pop("image")
                common = {**row, "catalog_entry": row.get("catalog_entry", 0), "goods_key": key,
                          "shop_token": token, "market_price": None, "synced_at": now}
                goods.append({**common, "status": 1})
                details.append({**common, "description": description, "image": image, "real_price": None})
                links.update(extract_links(description, row["link"]))
            normalized = {"token": token, "nickname": shop["name"], "link": origin + "/",
                          "description": "", "goods_count": len(goods), "fetch_done": 1, "synced_at": now,
                          "auto_count": sum(g["goods_type"] == "auto" for g in goods),
                          "manual_count": sum(g["goods_type"] == "manual" for g in goods),
                          "unknown_count": sum(g["goods_type"] == "unknown" for g in goods)}
            links.update(extract_links(payload.get("html"), origin + "/"))
            announcement = (payload.get("config") or {}).get("announcement")
            if isinstance(announcement, dict):
                normalized["description"] = text_value(announcement.get("content") or announcement.get("description"))
            if hasattr(session, "check_cancel"):
                session.check_cancel()
            local_store.save_snapshot(normalized, goods, details, path=store_path or self.database_path(),
                                      extra_links=[(link, origin + "/") for link in links])
            return {"ok": True, "source": self.SOURCE_KEY, "shop": normalized, "goods_count": len(goods),
                    "detail_count": len(details), "synced_at": now,
                    "discovered_urls": sorted(link for link in links if origin_of(link) == origin)}
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            raise PlatformError(f"公开货铺采集失败：{str(exc)[:200]}；已保留旧快照", failure=from_exception(exc)) from exc
        finally:
            if own:
                session.close()


dujiao_next = IndependentPlatform("dujiao-next")
dujiaoka = IndependentPlatform("dujiaoka")
