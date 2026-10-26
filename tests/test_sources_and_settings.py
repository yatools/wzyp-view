"""统一链动入口、旧参数兼容、配置导入及 AI 查询的回归测试。"""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import agent
import config
import db
import platformwzyp
import server
from test_wzyp_and_llm import FakeWzypSession, make_open_shop


class UnifiedSourcesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.env = patch.dict(os.environ, {
            "WZYP_LIVE_DB": str(self.path / "wzyp.sqlite3"),
            "WZYP_16688_DB": str(self.path / "16688.sqlite3"),
        })
        self.env.start()
        session = FakeWzypSession({"S1": make_open_shop("S1", goods_pages={
            "card": {1: {"total": 1, "list": [{"goods_key": "G1", "name": "实时商品", "price": 10}]}}
        })})
        platformwzyp.sync_shop("https://wzyp.cn/shop/S1", session=session)
        self.client = TestClient(server.app)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_default_and_both_wzyp_names_read_the_same_snapshot_without_mysql(self):
        with patch.dict("sys.modules", {"pymysql": None, "dbutils": None}):
            expected_stats = self.client.get("/api/stats?source=wzyp-live").json()["stats"]
            expected_shops = self.client.post("/api/shops?source=wzyp-live", json={}).json()
            expected_goods = self.client.post("/api/goods?source=wzyp-live", json={}).json()
            expected_detail = self.client.get("/api/goods/G1?source=wzyp-live").json()
            self.assertEqual(expected_stats["goods_count"], 1)
            for suffix in ("", "?source=wzyp", "?source=wzyp-live"):
                with self.subTest(suffix=suffix):
                    self.assertEqual(self.client.get("/api/stats" + suffix).json()["stats"], expected_stats)
                    self.assertEqual(self.client.post("/api/shops" + suffix, json={}).json(), expected_shops)
                    self.assertEqual(self.client.post("/api/goods" + suffix, json={}).json(), expected_goods)
                    self.assertEqual(self.client.get("/api/goods/G1" + suffix).json(), expected_detail)
            self.assertEqual(self.client.get("/api/stats?source=16688").json()["stats"]["goods_count"], 0)

    def test_alias_uses_sqlite_schema_and_readonly_guards(self):
        for source in ("wzyp", "wzyp-live"):
            with self.subTest(source=source), db.use_source(source):
                self.assertIn("SQLite", db.schema_text())
                self.assertNotIn("MySQL", db.schema_text())
                self.assertEqual(db.run_readonly_sql("SELECT COUNT(*) AS n FROM wzyp_goods")["rows"], [{"n": 1}])
                self.assertIn("error", db.run_readonly_sql("SELECT * FROM crawl_jobs"))
                self.assertIn("error", db.run_readonly_sql("DELETE FROM wzyp_goods"))
            self.assertIn("SQLite", agent._source_note_for(source))
            self.assertNotIn("MySQL", agent._source_note_for(source))

    def test_ai_default_and_alias_query_live_data_and_keep_history_namespaces(self):
        from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
        from langchain_core.messages import AIMessage

        class ToolModel(FakeMessagesListChatModel):
            def bind_tools(self, tools):
                return self

        memory = agent.MemoryStore(str(self.path / "chat.json"))
        for suffix, namespace in (("", ""), ("?source=wzyp", ""), ("?source=wzyp-live", "wzyp-live:")):
            with self.subTest(suffix=suffix):
                model = ToolModel(responses=[
                    AIMessage(content="", tool_calls=[{"name": "data_schema", "args": {}, "id": "schema"}]),
                    AIMessage(content="", tool_calls=[{"name": "sql_query", "args": {
                        "query": "SELECT COUNT(*) AS n FROM wzyp_goods"}, "id": "sql"}]),
                    AIMessage(content="当前有 1 件商品。"),
                ])
                thread_id = "unified-" + str(len(suffix))
                with patch.object(agent, "build_llm", return_value=model), \
                     patch.object(agent, "suggest_followups", return_value=[]), \
                     patch.object(agent, "memory_store", memory), \
                     patch.dict("sys.modules", {"pymysql": None, "dbutils": None}):
                    response = self.client.post("/api/chat" + suffix, json={"thread_id": thread_id, "message": "有多少商品"})
                    self.assertEqual(response.status_code, 200)
                    self.assertNotIn('"event": "error"', response.text)
                    self.assertIn('"event": "done"', response.text)
                    messages = memory.load(namespace + thread_id)
                    self.assertEqual(len(messages), 2)
                    self.assertTrue(any('"n": 1' in tool["preview"] for tool in messages[-1]["tools"]))
                    self.assertEqual(self.client.get("/api/chat/history" + (suffix or "?") +
                                     ("&" if suffix else "") + "thread_id=" + thread_id).json()["messages"], messages)


class LegacySettingsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings_file = Path(self.temp.name) / "settings.json"
        self.settings_patch = patch.object(config, "SETTINGS_FILE", str(self.settings_file))
        self.settings_patch.start()
        self.env = patch.dict(os.environ, {})
        self.env.start()
        for name in ("WZYP_LLM_BASEURL", "WZYP_LLM_MODEL", "WZYP_LLM_APIKEY"):
            os.environ.pop(name, None)
        self.client = TestClient(server.app)

    def tearDown(self):
        self.env.stop()
        self.settings_patch.stop()
        self.temp.cleanup()

    def test_old_database_configuration_routes_are_gone(self):
        with patch.dict("sys.modules", {"pymysql": None, "dbutils": None}), \
             patch.object(config, "save_settings", side_effect=AssertionError("停用接口不能写配置")):
            responses = [self.client.get("/api/db/settings"), self.client.get("/api/db/test"),
                         self.client.post("/api/db/settings", json={"host": "unreachable.invalid", "port": "bad"})]
        for response in responses:
            self.assertEqual(response.status_code, 410)
            self.assertIn("无需配置 MySQL", response.json()["detail"])
        self.assertFalse(self.settings_file.exists())

    def test_loading_and_exporting_old_config_ignores_database_fields(self):
        original = {"llm": {"model": "saved-model"}, "db": {"host": "unreachable.invalid", "port": "bad"}}
        self.settings_file.write_text(json.dumps(original), encoding="utf-8")
        with patch.dict(os.environ, {"WZYP_DB_PORT": "bad"}), \
             patch.dict("sys.modules", {"pymysql": None, "dbutils": None}):
            loaded = config.load_settings()
            self.assertEqual(set(loaded), {"llm"})
            self.assertEqual(loaded["llm"]["model"], "saved-model")
            self.assertEqual(self.client.get("/api/config/export").json(), loaded)
        self.assertEqual(json.loads(self.settings_file.read_text(encoding="utf-8")), original)

    def test_importing_old_nested_or_flat_config_saves_only_llm(self):
        llm = {"baseurl": "http://127.0.0.1:8045/v1", "model": "imported-model", "apikey": "test-secret"}
        for imported in ({"llm": llm, "db": {"port": "bad"}}, {**llm, "db": {"port": "bad"}}):
            with self.subTest(imported=imported), patch.dict("sys.modules", {"pymysql": None, "dbutils": None}):
                response = self.client.post("/api/config/import", json={"config": imported})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {"ok": True})
                self.assertEqual(json.loads(self.settings_file.read_text(encoding="utf-8")), {"llm": llm})
                self.assertEqual(self.client.get("/api/config/export").json(), {"llm": llm})


if __name__ == "__main__":
    unittest.main()
