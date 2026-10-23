"""离线验证公开接口适配、事务快照和两个数据源的隔离。"""
import copy
import json as jsonlib
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import requests
from fastapi.testclient import TestClient

import db
import local_store
import platform16688 as platform
import server


def sample_goods(key="G1", price=108.99, delivery=1):
    return {"goods_no": key, "name": f"测试商品 {key}", "price": price,
            "description": "<p>公开商品描述</p>", "image": "https://static.16688.com.cn/test.png",
            "sales_count_text": "1.2万+付款", "stock_available_quantity": 19,
            "stock_available_status": "normal", "delivery_method": delivery,
            "goods_category_no": "C1"}


class FakeSession:
    def __init__(self, items=None, shop_no="S1", total=None):
        self.headers = {}
        self.calls = []
        self.items = [sample_goods(), sample_goods("G2", 20, 3)] if items is None else items
        self.shop = {"shop_no": shop_no, "shop_alias": "TEST", "name": "测试店铺",
                     "announcement": "公开公告", "merchant": {"sales_count_text": "1.3万+付款",
                     "deposit_money": 5000, "region": "中国广东", "auth_type": 1}}
        self.total = total
        self.failure = None

    def post(self, url, json=None, **kwargs):
        self.calls.append((url, json, kwargs))
        if self.failure:
            raise self.failure
        endpoint = url.split("/shopApi/")[-1]
        if endpoint == "goods/detail":
            data = {**self.items[0], "shop_no": self.shop["shop_no"]}
        elif endpoint == "shop/detail":
            data = self.shop
        elif endpoint == "goods/list":
            data = {"list": self.items}
            if self.total is not None:
                data["total"] = self.total
        elif endpoint == "goods_category/list":
            data = [{"goods_category_no": "C1", "name": "测试分类"}]
        else:
            raise AssertionError(endpoint)
        response = requests.Response()
        response.status_code = 200
        response._content = jsonlib.dumps({"code": 1, "data": data}).encode()
        return response


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"WZYP_16688_DB": os.path.join(self.temp.name, "test.sqlite3")})
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def sync(self, session=None, url="https://www.16688.com.cn/goods/G1"):
        return platform.sync_shop(url, session or FakeSession())

    def test_product_link_imports_whole_shop_and_preserves_public_values(self):
        session = FakeSession()
        result = self.sync(session)
        self.assertEqual(result["goods_count"], 2)
        self.assertEqual(result["shop"]["sell_count"], 13000)
        self.assertIsNone(result["shop"]["create_time"])
        self.assertTrue(all(call[0].startswith(platform.BASE_URL + "/shopApi/") for call in session.calls))
        self.assertTrue(all(call[2]["allow_redirects"] is False for call in session.calls))
        with db.use_source("16688"):
            detail = db.goods_detail("G1")
            self.assertEqual(detail["sales_count_text"], "1.2万+付款")
            self.assertEqual(detail["category_name"], "测试分类")
            self.assertEqual(detail["stock_quantity"], 19)
            self.assertIsNone(detail["real_price"])
            self.assertIsNone(detail["market_price"])
            self.assertEqual(detail["goods_type"], "auto")
            self.assertEqual(detail["link"], platform.BASE_URL + "/goods/G1")

    def test_shop_alias_and_missing_optional_fields(self):
        item = {"goods_no": "G1", "name": "缺少可选字段"}
        result = self.sync(FakeSession([item]), "https://16688.com.cn/shop/TEST/")
        self.assertEqual(result["shop"]["unknown_count"], 1)
        with db.use_source("16688"):
            self.assertIsNone(db.goods_detail("G1")["stock_quantity"])
            self.assertIsNone(db.goods_detail("G1")["price"])

    def test_delivery_enumerations_match_platform_frontend(self):
        items = [sample_goods(f"G{i}", delivery=method) for i, method in enumerate([1, 2, 3, "1", "2", "3", 99])]
        self.sync(FakeSession(items), "https://www.16688.com.cn/shop/TEST")
        with db.use_source("16688"):
            self.assertEqual([db.goods_detail(f"G{i}")["goods_type"] for i in range(7)],
                             ["auto", "auto", "manual", "auto", "auto", "manual", "unknown"])

    def test_unpublished_manual_inventory_does_not_fail_whole_shop(self):
        manual = sample_goods("G2", 20, 3)
        manual["stock_available_quantity"] = -1
        self.sync(FakeSession([sample_goods(), manual]))
        with db.use_source("16688"):
            self.assertIsNone(db.goods_detail("G2")["stock_quantity"])
            self.assertEqual(db.goods_detail("G2")["goods_type"], "manual")
            self.assertEqual(db.shop_stats()["goods_count"], 2)

    def test_existing_query_features_on_sqlite(self):
        self.sync()
        with db.use_source("16688"):
            stats = db.shop_stats()
            self.assertEqual((stats["shop_count"], stats["goods_count"], stats["detail_count"]), (1, 2, 2))
            self.assertEqual(len(db.type_dist()), 2)
            self.assertEqual(sum(row["count"] for row in db.price_buckets()), 2)
            self.assertTrue(db.top_keywords())
            filtered = db.list_goods(goods_type="auto", price_min=100, price_max=110, kw="G1")
            self.assertEqual(filtered["total"], 1)
            self.assertEqual(db.list_goods(offset=1, limit=1)["rows"][0]["goods_key"], "G1")
            self.assertEqual(db.list_goods(filters=[{"field": "price", "op": "in", "value": []}])["total"], 0)
            self.assertEqual(db.list_shops(filters=[{"field": "avg_price", "op": "gt", "value": 60}])["total"], 1)
            self.assertEqual(db.list_shops(filters=[{"field": "avg_price", "op": "lt", "value": 60}])["total"], 0)
            self.assertEqual(db.list_shops(fetch_status="done")["total"], 1)
            self.assertEqual(db.list_shops(fetch_status="fetching")["total"], 0)
            self.assertEqual(db.list_shops(sort="auto_count")["rows"][0]["auto_count"], 1)

    def test_refresh_removes_stale_goods_and_preserves_other_shops(self):
        self.sync()
        other = FakeSession([sample_goods("G3")], shop_no="S2")
        self.sync(other, "https://www.16688.com.cn/shop/S2")
        self.sync(FakeSession([sample_goods("G1", 88)]))
        with db.use_source("16688"):
            self.assertIsNone(db.goods_detail("G2"))
            self.assertEqual(db.goods_detail("G1")["price"], 88)
            self.assertIsNotNone(db.goods_detail("G3"))
            self.assertEqual(db.shop_stats()["goods_count"], 2)

    def test_failed_fetch_keeps_previous_snapshot(self):
        self.sync()
        session = FakeSession()
        session.failure = requests.Timeout()
        with self.assertRaises(platform.PlatformError):
            self.sync(session)
        with db.use_source("16688"):
            self.assertEqual(db.shop_stats()["goods_count"], 2)

    def test_partial_or_malformed_response_cannot_replace_snapshot(self):
        self.sync()
        for session in [FakeSession(total=10), FakeSession([sample_goods(), sample_goods()]),
                        FakeSession([sample_goods(), {"goods_no": "G2"}]),
                        FakeSession([sample_goods(price="NaN")])]:
            with self.subTest(session=session), self.assertRaises(platform.PlatformError):
                self.sync(session)
        with db.use_source("16688"):
            self.assertEqual(db.shop_stats()["goods_count"], 2)

    def test_transaction_rolls_back_if_storage_write_fails(self):
        self.sync()
        result = self.sync(FakeSession([sample_goods("G1", 88)]))
        shop = copy.deepcopy(result["shop"])
        shop["nickname"] = "不应保存"
        with self.assertRaises(Exception):
            local_store.save_snapshot(shop, [{"goods_key": "bad"}], [])
        with db.use_source("16688"):
            self.assertEqual(db.list_shops()["rows"][0]["nickname"], "测试店铺")
            self.assertEqual(db.goods_detail("G1")["price"], 88)

    def test_readonly_sql_and_schema_use_selected_source(self):
        self.sync()
        with db.use_source("16688"):
            self.assertEqual(db.run_readonly_sql("SELECT COUNT(*) AS n FROM wzyp_goods")["rows"][0]["n"], 2)
            self.assertEqual(db.run_readonly_sql("SELECT * FROM wzyp_goods LIMIT 99999", limit=1)["count"], 1)
            for sql in ["DELETE FROM wzyp_goods", "SELECT 1; DELETE FROM wzyp_goods",
                        "SELECT * FROM sqlite_master", "SELECT load_extension('x')"]:
                self.assertIn("error", db.run_readonly_sql(sql))
            schema = db.schema_text()
            self.assertIn("SQLite", schema)
            self.assertIn("销量", schema)
            self.assertIn("stock_quantity", schema)
        self.assertEqual(db.active_source(), "wzyp")

    def test_parallel_source_contexts_do_not_leak(self):
        def check(source):
            with db.use_source(source):
                return db.active_source()
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(check, ["16688", "wzyp"] * 10)), ["16688", "wzyp"] * 10)
        self.assertEqual(db.active_source(), "wzyp")

    def test_api_routes_validation_and_source_isolation(self):
        self.sync()
        client = TestClient(server.app)
        with patch.object(db, "_build_pool", side_effect=AssertionError("不应连接 MySQL")):
            self.assertEqual(client.get("/api/health").status_code, 200)
            self.assertEqual(client.get("/api/sources").status_code, 200)
            self.assertEqual(client.get("/api/stats?source=16688").json()["stats"]["goods_count"], 2)
            self.assertEqual(client.post("/api/shops?source=16688", json={}).json()["total"], 1)
            self.assertEqual(client.post("/api/goods?source=16688", json={}).json()["total"], 2)
            self.assertEqual(client.get("/api/goods/G1?source=16688").json()["goods_key"], "G1")
            self.assertEqual(client.get("/api/stats?source=other").status_code, 422)
            self.assertEqual(client.post("/api/goods?source=16688", json={"page": 0}).status_code, 422)
            self.assertEqual(client.post("/api/shops?source=16688", json={"pageSize": 10000}).status_code, 422)
            self.assertEqual(client.post("/api/sources/16688/sync", json={"url": "https://localhost/shop/x"}).status_code, 400)
        with patch.object(platform, "sync_shop", side_effect=platform.PlatformError("读取失败")):
            response = client.post("/api/sources/16688/sync", json={"url": platform.EXAMPLE_URL})
            self.assertEqual(response.status_code, 502)
            self.assertEqual(response.json()["detail"], "读取失败")

    def test_legacy_api_default_still_uses_wzyp(self):
        client = TestClient(server.app)
        def check(**kwargs):
            self.assertEqual(db.active_source(), "wzyp")
            return {"total": 0, "rows": []}
        with patch.object(db, "list_goods", side_effect=check):
            self.assertEqual(client.post("/api/goods", json={}).status_code, 200)
        self.assertEqual(server.memory_id("t1", "wzyp"), "t1")
        self.assertEqual(server.memory_id("t1", "16688"), "16688:t1")

    def test_real_agent_graph_queries_selected_source_and_separates_history(self):
        import agent
        from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
        from langchain_core.messages import AIMessage

        class ToolModel(FakeMessagesListChatModel):
            def bind_tools(self, tools):
                return self

        self.sync()
        model = ToolModel(responses=[
            AIMessage(content="", tool_calls=[{"name": "data_schema", "args": {}, "id": "schema"}]),
            AIMessage(content="", tool_calls=[{"name": "sql_query", "args": {
                "query": "SELECT COUNT(*) AS 商品数 FROM wzyp_goods"}, "id": "sql"}]),
            AIMessage(content="当前店铺有 2 件商品。"),
        ])
        with patch.object(agent, "build_llm", return_value=model), \
             patch.object(agent, "suggest_followups", return_value=[]), \
             patch.object(agent, "memory_store", agent.MemoryStore(os.path.join(self.temp.name, "memory.json"))), \
             patch.object(db, "_build_pool", side_effect=AssertionError("AI 不应连接 MySQL")):
            response = TestClient(server.app).post("/api/chat?source=16688", json={"thread_id": "same", "message": "有多少商品"})
            self.assertEqual(response.status_code, 200)
            self.assertNotIn('"event": "error"', response.text)
            self.assertIn('"event": "done"', response.text)
            self.assertIn("当前店铺有 2 件商品", response.text)
            self.assertIn("sql_query", response.text)
            self.assertEqual(len(agent.memory_store.load("16688:same")), 2)
            self.assertEqual(agent.memory_store.load("same"), [])
            client = TestClient(server.app)
            self.assertEqual(len(client.get("/api/chat/history?source=16688&thread_id=same").json()["messages"]), 2)
            self.assertEqual(client.get("/api/chat/history?source=wzyp&thread_id=same").json()["messages"], [])
        self.assertEqual(db.active_source(), "wzyp")


class InputTests(unittest.TestCase):
    def test_rejects_external_hosts_credentials_and_invalid_paths(self):
        for url in ["https://example.com/shop/a", "https://16688.com.cn.evil.com/shop/a",
                    "https://user:pass@16688.com.cn/shop/a", "file:///shop/a",
                    "https://16688.com.cn:8000/shop/a", "https://16688.com.cn/goods/../../admin",
                    "https://16688.com.cn/shop/a%2Fb", "https://16688.com.cn/shop/"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                platform.parse_target(url)

    def test_sales_text_lower_bounds_and_unavailable_values(self):
        for text, expected in [("1.3万+付款", 13000), ("735人付款", 735), ("1,234人付款", 1234),
                               ("未公开", None), (None, None), ("0人付款", 0)]:
            with self.subTest(text=text):
                self.assertEqual(platform.sales_lower_bound(text), expected)


if __name__ == "__main__":
    unittest.main()
