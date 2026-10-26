"""商品名多关键词交集、价格分页和数据源兼容性。所有快照均在临时库内。"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import local_store
import server


class GoodsKeywordsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = patch.dict(os.environ, {
            "WZYP_LIVE_DB": str(self.root / "wzyp.sqlite3"),
            "WZYP_16688_DB": str(self.root / "16688.sqlite3"),
        })
        self.env.start()
        self.seed("wzyp-live", "S1", "甲店", [
            ("A1", "GPT Plus 月卡", 120, "card", 1),
            ("A2", "ChatgPt pLuS 年卡", 240, "card", 1),
            ("A3", "GPT 专用", 300, "card", 1),
            ("A4", "Plus 专用", 400, "card", 1),
            ("A5", "GPT Plus 试用", 0, "card", 1),
            ("A6", "GPT Plus 折扣", 120, "equity", 0),
            ("A7", "GPT Plus 未公开价格", None, "card", 1),
        ])
        self.seed("wzyp-live", "S2", "乙店", [
            ("B1", "GPT Plus 续费", 360, "card", 1),
            ("B2", "GPT Plus 共享月卡", 12, "card", 1),
        ])
        self.seed("wzyp-live", "S3", "拆分关键词店", [
            ("C1", "GPT 专用", 30, "card", 1),
            ("C2", "Plus 专用", 40, "card", 1),
        ])
        self.seed("wzyp-live", "S4", "特殊字符店", [
            ("L1", "Deal 100%_OFF", 10, "card", 1),
            ("L2", "Deal 100XOFF", 10, "card", 1),
            ("L3", "100% literal", 10, "card", 1),
            ("L4", "_ underscore", 10, "card", 1),
            ("L5", "literal ' OR 1=1 --", 10, "card", 1),
            ("L6", "path\\item", 10, "card", 1),
        ])
        self.seed("16688", "P1", "16688 店", [
            ("P1", "GPT PLUS 16688", 9, "auto", 1),
        ])
        self.client = TestClient(server.app)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def seed(self, source, token, nickname, items):
        synced_at = "2026-10-23T18:00:00+08:00"
        shop = {"token": token, "nickname": nickname, "goods_count": len(items),
                "link": f"https://example.test/shop/{token}", "synced_at": synced_at}
        goods = [{"goods_key": key, "shop_token": token, "goods_type": kind, "name": name,
                  "price": price, "status": status, "link": f"https://example.test/item/{key}",
                  "synced_at": synced_at} for key, name, price, kind, status in items]
        local_store.save_snapshot(shop, goods, [], path=local_store.database_path(source))

    def search(self, body, suffix="?source=wzyp-live"):
        response = self.client.post("/api/goods" + suffix, json=body)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_all_keywords_match_each_product_and_keep_shop_names(self):
        result = self.search({"keywords": ["gpt", "plus"], "order": "desc"})
        self.assertEqual(result["total"], 7)
        self.assertEqual([row["goods_key"] for row in result["rows"]],
                         ["B1", "A2", "A1", "A6", "B2", "A5", "A7"])
        self.assertEqual({row["shop_name"] for row in result["rows"]}, {"甲店", "乙店"})
        self.assertEqual(sum(row["shop_token"] == "S1" for row in result["rows"]), 5)
        self.assertEqual(self.search({"keywords": ["gpt", "plus"], "shop_token": "S3"})["total"], 0)

    def test_whitespace_empty_and_duplicate_keywords_do_not_change_results(self):
        expected = self.search({"keywords": ["gpt", "plus"]})
        actual = self.search({"keywords": [" gPt ", " PLUS", "", "  ", "gpt", "plus "]})
        self.assertEqual(actual, expected)
        self.assertEqual(self.search({"keywords": ["  ", ""]}), self.search({}))

    def test_adding_a_keyword_narrows_the_same_product_and_handles_no_match(self):
        result = self.search({"keywords": ["gpt", "plus", "年卡"]})
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["goods_key"], "A2")
        self.assertEqual(self.search({"keywords": ["gpt", "plus", "不存在的词"]}),
                         {"total": 0, "rows": []})

    def test_special_characters_are_literal_and_parameterized(self):
        for words, expected in [(["%", "_"], ["L1"]), (["' OR 1=1 --"], ["L5"]),
                                (["\\"], ["L6"])]:
            with self.subTest(words=words):
                result = self.search({"keywords": words})
                self.assertEqual([row["goods_key"] for row in result["rows"]], expected)

    def test_keywords_combine_with_existing_filters_and_exclude_zero(self):
        result = self.search({"keywords": ["gpt", "plus"], "shop_token": "S1",
                              "goods_type": "card", "price_min": 100, "price_max": 200,
                              "filters": [{"field": "status", "op": "eq", "value": 1},
                                          {"field": "name", "op": "like", "value": "月"}]})
        self.assertEqual([row["goods_key"] for row in result["rows"]], ["A1"])
        paid = self.search({"keywords": ["gpt", "plus"],
                            "filters": [{"field": "price", "op": "gt", "value": 0}]})
        self.assertEqual(paid["total"], 5)
        self.assertTrue(all(row["price"] > 0 for row in paid["rows"]))

    def test_price_sort_and_counts_are_global_and_stable_across_pages(self):
        for order, expected in [("desc", ["B1", "A2", "A1", "A6", "B2", "A5"]),
                                ("asc", ["A5", "B2", "A1", "A6", "A2", "B1"])]:
            with self.subTest(order=order):
                body = {"keywords": ["gpt", "plus"], "order": order, "pageSize": 2,
                        "filters": [{"field": "price", "op": "gte", "value": 0}]}
                pages = [self.search({**body, "page": page}) for page in range(1, 4)]
                self.assertTrue(all(page["total"] == 6 for page in pages))
                keys = [row["goods_key"] for page in pages for row in page["rows"]]
                self.assertEqual(keys, expected)
                self.assertEqual(self.search({**body, "page": 4}), {"total": 6, "rows": []})

    def test_legacy_keyword_and_source_aliases_remain_compatible_and_isolated(self):
        expected = self.search({"keywords": ["gpt", "plus"]})
        for suffix in ("", "?source=wzyp", "?source=wzyp-live"):
            with self.subTest(suffix=suffix):
                self.assertEqual(self.search({"keywords": ["gpt", "plus"]}, suffix), expected)
        legacy = self.search({"kw": "GPT%"})
        self.assertEqual(self.search({"kw": "GPT%", "keywords": []}), legacy)
        combined = self.search({"kw": "GPT%", "keywords": ["plus", "年卡"]})
        self.assertEqual([row["goods_key"] for row in combined["rows"]], ["A2"])
        other = self.search({"keywords": ["gpt", "plus"]}, "?source=16688")
        self.assertEqual(other["total"], 1)
        self.assertEqual(other["rows"][0]["shop_name"], "16688 店")

    def test_invalid_keyword_types_are_rejected_by_the_api(self):
        for keywords in ("gpt", [None], [42], [["gpt"]]):
            with self.subTest(keywords=keywords):
                response = self.client.post("/api/goods", json={"keywords": keywords})
                self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
