"""一可比价网公开查询：匿名会话、关键词分页和独立店铺报价。"""
import html
import json
import math
import re
from urllib.parse import quote, urljoin, urlsplit

import platform16688
import platformwzyp
from public_http import PublicSession, CrawlBlockedError, detect_block_reason, failure_details
from crawl_failure import CollectionError, http_failure

BASE_URL = "https://vs.yiketools.com/"
PAGE_SIZE = 30
MAX_RESPONSE = 4 * 1024 * 1024


class YikeError(CollectionError, ValueError):
    pass


def keyword_value(value):
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 200:
        raise ValueError("请输入 1–200 字符的采集关键词")
    return value.strip()


def identity(value):
    value = str(value) if isinstance(value, (int, str)) and not isinstance(value, bool) else ""
    if not re.fullmatch(r"[1-9][0-9]{0,31}", value):
        raise YikeError("一可商品或店铺 GUID 无效")
    return value


def public_link(value):
    if not isinstance(value, str):
        return ""
    try:
        parsed = urlsplit(value)
        if parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username and not parsed.password and parsed.port in {None, 80, 443}:
            return value
    except ValueError:
        pass
    return ""


def origin_identity(link):
    from sources import catfk
    for source, adapter in (("wzyp-live", platformwzyp), ("16688", platform16688), ("catfk", catfk)):
        try:
            target = adapter.normalize_target(link)
            if target["kind"] == "goods":
                return source, target["target"]
        except ValueError:
            pass
    return None, None


def number(value, integer=False):
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        result = float(value)
        if not math.isfinite(result) or result < 0 or (integer and not result.is_integer()):
            return None
        return int(result) if integer else result
    except (ValueError, TypeError, OverflowError):
        return None


def archived(item):
    return item.get("archived") in (True, 1, "1", "true") or item.get("active") in (False, 0, "0")


def source_url(item):
    key = item.get("key")
    if not isinstance(key, str) or not key or len(key) > 200:
        raise YikeError("一可商品缺少公开链接标识")
    return BASE_URL + f"goods/{identity(item.get('guid'))}/{quote(key, safe='')}"


def normalize_detail(item, expected_guid, now):
    if not isinstance(item, dict) or identity(item.get("guid")) != expected_guid:
        raise YikeError("一可商品详情编号与列表不一致")
    if "desc" not in item and "detailText" not in item:
        raise YikeError("一可商品详情不完整，保留旧记录")
    name, shop_name = item.get("title"), item.get("shopName")
    if not isinstance(name, str) or not name.strip() or not isinstance(shop_name, str) or not shop_name.strip():
        raise YikeError("一可商品或店铺名称缺失")
    token = identity(item.get("shopGuid"))
    link, shop_link = public_link(item.get("linkUrl")), public_link(item.get("shopUrl"))
    if not link or not shop_link:
        raise YikeError("一可商品缺少有效的原站商品或店铺链接")
    description = item.get("desc")
    if description is not None and not isinstance(description, str):
        raise YikeError("一可商品说明格式异常")
    if not description:
        text = item.get("detailText") or ""
        if not isinstance(text, str):
            raise YikeError("一可商品说明格式异常")
        description = "<p>" + html.escape(text) + "</p>" if text else ""
    quantity = number(item.get("stock"), integer=True)
    raw_stock = str(item.get("stockStatus") or "")
    if quantity is not None:
        stock_status = "in_stock" if quantity > 0 else "out_of_stock"
    elif any(x in raw_stock for x in ("缺货", "无货", "售罄", "售完")):
        stock_status = "out_of_stock"
    elif any(x in raw_stock for x in ("有货", "库存充足", "库存一般", "库存少量")):
        stock_status = "in_stock"
    else:
        stock_status = "unknown"
    origin_source, origin_key = origin_identity(link)
    # 一可页面将报价明确标为人民币；未公开的发货方式不从“卡密”推断。
    common = {"goods_key": expected_guid, "shop_token": token, "name": name,
              "price": number(item.get("price")), "market_price": number(item.get("marketPrice")) or None,
              "goods_type": str(item.get("goodsType") or "unknown"), "link": link,
              "stock_quantity": quantity, "stock_status": stock_status,
              "category_name": str(item.get("categoryName") or ""), "currency": "CNY",
              "sku_label": str(item.get("skuLabel") or ""), "delivery_method": "unknown",
              "source_url": source_url(item), "source_updated_at": str(item.get("updateTime") or ""),
              "origin_source": origin_source, "origin_goods_key": origin_key, "synced_at": now}
    shop = {"token": token, "nickname": shop_name, "link": shop_link, "synced_at": now}
    goods = {**common, "status": 0 if archived(item) else 1}
    detail = {**common, "description": description,
              "image": public_link(urljoin(BASE_URL, str(item.get("image") or ""))) if item.get("image") else None}
    return shop, goods, detail


class YikeClient:
    def __init__(self, stop, session_factory=None):
        self.stop = stop
        self.session = (session_factory or (lambda stop: PublicSession(stop=stop)))(stop)
        self.token = None

    def __enter__(self):
        try:
            text = self._read("", html_page=True)
            match = re.search(r"\bSITE_TOKEN\s*=\s*['\"]([A-Za-z0-9._~+/=-]{1,2048})['\"]", text)
            if not match:
                raise YikeError("一可公开页面查询令牌缺失或格式变化")
            self.token = match[1]
            return self
        except BaseException:
            self.session.close()
            raise

    def __exit__(self, *args):
        self.session.close()

    def _read(self, path, params=None, html_page=False):
        self.session.check_cancel()
        headers = {"Accept": "text/html" if html_page else "application/json"}
        if not html_page:
            headers.update({"X-Site-Token": self.token, "Referer": BASE_URL})
        with self.session.get(BASE_URL + path, params=params, headers=headers, stream=True) as response:
            if response.status_code in {403, 444, 429}:
                raise CrawlBlockedError(f"一可公开查询被拒绝或限流（HTTP {response.status_code}），可稍后继续采集",
                                        failure_details(response))
            if hasattr(response, "iter_content"):
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    self.session.check_cancel()
                    size += len(chunk)
                    if size > MAX_RESPONSE:
                        raise YikeError("一可响应超过大小限制")
                    chunks.append(chunk)
                raw = b"".join(chunks)
            else:
                raw = response.content
            if len(raw) > MAX_RESPONSE:
                raise YikeError("一可响应超过大小限制")
            text = raw.decode("utf-8-sig", errors="replace")
            reason = detect_block_reason(response.status_code, text, expect_json=not html_page)
            if reason:
                raise CrawlBlockedError("一可公开接口拒绝访问或要求验证，可稍后继续采集")
            if response.status_code != 200:
                raise YikeError(f"一可公开查询返回 HTTP {response.status_code}", failure=http_failure(response))
            if html_page:
                return text
            try:
                value = json.loads(text)
            except (ValueError, RecursionError) as exc:
                raise YikeError("一可公开接口未返回有效 JSON") from exc
            if isinstance(value, dict) and value.get("error"):
                # 不记录服务端错误原文，避免意外把会话令牌写入任务历史。
                raise YikeError("一可公开接口返回错误，保留旧记录")
            return value

    def query(self, route, **params):
        return self._read("api.php", {"route": route, **params})

    def products(self, keyword, offset):
        rows = self.query("goods", keyword=keyword, limit=PAGE_SIZE, offset=offset, sortBy="price_in_stock")
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE or not all(isinstance(x, dict) for x in rows):
            raise YikeError("一可关键词列表格式或分页大小异常")
        return rows

    def group(self, key, page):
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z0-9:_-]{1,200}", key):
            raise YikeError("一可同源组标识无效")
        result = self.query("goods/group", key=key, page=page, sortBy="price_asc")
        if not isinstance(result, dict) or result.get("groupKey") != key or result.get("page") != page:
            raise YikeError("一可同源组分页或标识不一致")
        total, size, pages = result.get("total"), result.get("pageSize"), result.get("totalPages")
        items = result.get("items")
        if any(type(x) is not int for x in (total, size, pages)) or total < 0 or not 1 <= size <= 500 or pages != max(1, math.ceil(total / size)):
            raise YikeError("一可同源组分页数量异常")
        if not isinstance(items, list) or not all(isinstance(x, dict) for x in items) or len(items) != min(size, max(0, total - (page - 1) * size)):
            raise YikeError("一可同源组缺页，保留已有记录")
        return result

    def detail(self, guid):
        return self.query("goods/detail", id=identity(guid))
