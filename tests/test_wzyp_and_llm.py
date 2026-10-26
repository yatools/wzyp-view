from __future__ import annotations

from contextlib import contextmanager
import gc
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

import agent
import db
import local_store
import platformwzyp
import public_http
import server
from crawler16688 import CrawlManager


@contextmanager
def local_http_server(handler):
    httpd = HTTPServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    try:
        yield httpd
    finally:
        httpd.shutdown()
        worker.join(timeout=5)
        httpd.server_close()


class FakeResponse:
    def __init__(self, body: dict, status_code: int = 200):
        self._body = body
        self.status_code = status_code
        self.content = json.dumps(body, ensure_ascii=False).encode("utf-8")

    def json(self) -> dict:
        return self._body


class FakeWzypSession:
    def __init__(self, shops_data: dict[str, dict], details_data: dict[str, dict] | None = None):
        self.shops_data = shops_data
        self.details_data = details_data or {}
        self.goods_list_payloads: list[dict] = []
        self.closed = False

    def post(self, url: str, json: dict | None = None, **kwargs) -> FakeResponse:
        payload = json or {}
        if url.endswith("/Shop/info"):
            token = payload["token"]
            entry = self.shops_data.get(token)
            if callable(entry):
                return FakeResponse(entry("info", payload))
            if not entry:
                return FakeResponse({"code": 0, "msg": "店铺不存在"})
            return FakeResponse(entry["info"])
        if url.endswith("/Shop/categoryList"):
            token = payload["token"]
            gt = payload["goods_type"]
            entry = self.shops_data[token]
            if callable(entry):
                return FakeResponse(entry("categoryList", payload))
            cats = entry.get("categories", {}).get(gt, [])
            return FakeResponse({"code": 1, "data": cats})
        if url.endswith("/Shop/goodsList"):
            self.goods_list_payloads.append(dict(payload))
            token = payload["token"]
            gt = payload["goods_type"]
            page = payload["current"]
            entry = self.shops_data[token]
            if callable(entry):
                return FakeResponse(entry("goodsList", payload))
            pages = entry.get("goods_pages", {}).get(gt, {})
            if callable(pages):
                return FakeResponse({"code": 1, "data": pages(page, payload)})
            page_data = pages.get(page, {"total": 0, "list": []})
            return FakeResponse({"code": 1, "data": page_data})
        if url.endswith("/Shop/goodsInfo"):
            key = payload["goods_key"]
            detail = self.details_data.get(key)
            if callable(detail):
                return FakeResponse(detail(payload))
            if detail is None:
                return FakeResponse(
                    {
                        "code": 1,
                        "data": {
                            "goods_key": key,
                            "name": f"商品_{key}",
                            "description": f"<p>详情 {key}</p>",
                            "image": f"https://img.wzyp.cn/{key}.png",
                            "price": 15.0,
                            "real_price": 14.5,
                            "market_price": 20.0,
                            "status": 1,
                        },
                    }
                )
            return FakeResponse(detail)
        raise AssertionError(f"Unexpected URL: {url}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self) -> None:
        self.closed = True


def make_open_shop(
    token: str,
    nickname: str = "测试小铺",
    description: str = "",
    categories: dict | None = None,
    goods_pages: dict | None = None,
) -> dict:
    return {
        "info": {
            "code": 1,
            "data": {
                "token": token,
                "nickname": nickname,
                "avatar": "https://img.wzyp.cn/avatar.png",
                "custom_status": 1,
                "custom_status_msg": "",
                "sell_count": 128,
                "deposit_money": 500.0,
                "description": description,
                "contact_wechat": "wx_test",
                "contact_qq": "10001",
                "contact_mobile": "",
                "create_time": 1756000000,
            },
        },
        "categories": categories or {},
        "goods_pages": goods_pages or {},
    }


class LlmProxyAndErrorTests(unittest.TestCase):
    def setUp(self) -> None:
        agent.invalidate_settings()

    def tearDown(self) -> None:
        agent.close_http_clients()

    def test_loopback_detection_covers_localhost_ipv4_range_and_ipv6(self) -> None:
        self.assertTrue(agent.is_loopback_url("http://localhost:8045/v1"))
        self.assertTrue(agent.is_loopback_url("http://127.0.0.1:8045/v1"))
        self.assertTrue(agent.is_loopback_url("http://127.25.30.1:11434/v1"))
        self.assertTrue(agent.is_loopback_url("http://[::1]:8045/v1"))
        self.assertFalse(agent.is_loopback_url("https://api.openai.com/v1"))
        self.assertFalse(agent.is_loopback_url("http://192.168.1.10:8045/v1"))

    def test_local_llm_bypasses_503_proxy_while_remote_follows_proxy(self) -> None:
        proxy_hits: list[str] = []
        local_hits: list[str] = []

        class Proxy503Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                proxy_hits.append(self.path)
                body = b'{"error":{"message":"proxy 503 unavailable"}}'
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_CONNECT(self):
                proxy_hits.append(self.path)
                self.send_response(503)
                self.end_headers()

            def log_message(self, format, *args):
                return

        class LocalLlmHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                req_raw = self.rfile.read(length) if length else b"{}"
                req_json = json.loads(req_raw.decode("utf-8"))
                local_hits.append(self.path)
                if req_json.get("stream"):
                    chunk = {
                        "id": "chatcmpl-local",
                        "object": "chat.completion.chunk",
                        "created": 1756000000,
                        "model": "test-local-model",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {"role": "assistant", "content": "OK"},
                                "finish_reason": "stop",
                            }
                        ],
                    }
                    sse = f"data: {json.dumps(chunk)}\n\ndata: [DONE]\n\n".encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Content-Length", str(len(sse)))
                    self.end_headers()
                    self.wfile.write(sse)
                else:
                    body = {
                        "id": "chatcmpl-local",
                        "object": "chat.completion",
                        "created": 1756000000,
                        "model": "test-local-model",
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": "OK"},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6},
                    }
                    raw = json.dumps(body).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

            def log_message(self, format, *args):
                return

        proxy_server = HTTPServer(("127.0.0.1", 0), Proxy503Handler)
        local_server = HTTPServer(("127.0.0.1", 0), LocalLlmHandler)
        proxy_thread = threading.Thread(target=proxy_server.serve_forever, daemon=True)
        local_thread = threading.Thread(target=local_server.serve_forever, daemon=True)
        proxy_thread.start()
        local_thread.start()

        try:
            proxy_url = f"http://127.0.0.1:{proxy_server.server_port}"
            local_base_url = f"http://127.0.0.1:{local_server.server_port}/v1"

            # 1. Local loopback request connects directly and ignores the 503 proxy
            with patch.dict(
                os.environ,
                {
                    "HTTP_PROXY": proxy_url,
                    "HTTPS_PROXY": proxy_url,
                    "ALL_PROXY": proxy_url,
                    "OPENAI_PROXY": proxy_url,
                    "WZYP_LLM_BASEURL": local_base_url,
                    "WZYP_LLM_MODEL": "test-local-model",
                    "WZYP_LLM_APIKEY": "sk-test-secret-9999",
                },
                clear=False,
            ):
                agent.invalidate_settings()
                llm_local = agent.build_llm()
                reply = llm_local.invoke("请只回复OK")
                self.assertEqual(str(reply.content).strip(), "OK")
                self.assertEqual(len(local_hits), 1)
                self.assertEqual(len(proxy_hits), 0)

            # 2. Remote URL uses trust_env=True and hits the configured 503 proxy
            with patch.dict(
                os.environ,
                {
                    "HTTP_PROXY": proxy_url,
                    "HTTPS_PROXY": proxy_url,
                    "ALL_PROXY": proxy_url,
                    "OPENAI_PROXY": proxy_url,
                    "WZYP_LLM_BASEURL": "http://remote-llm.example.invalid:8080/v1",
                    "WZYP_LLM_MODEL": "remote-model",
                    "WZYP_LLM_APIKEY": "sk-remote-secret-8888",
                },
                clear=False,
            ):
                agent.invalidate_settings()
                llm_remote = agent.build_llm()
                with self.assertRaises(Exception) as ctx:
                    llm_remote.invoke("请只回复OK")
                self.assertGreaterEqual(len(proxy_hits), 1)
                info = agent.classify_llm_error(ctx.exception, apikey="sk-remote-secret-8888")
                self.assertEqual(info["status_code"], 503)
                self.assertEqual(info["error_type"], "service_503")
                self.assertNotIn("sk-remote-secret-8888", info["error"])
        finally:
            proxy_server.shutdown()
            local_server.shutdown()
            proxy_server.server_close()
            local_server.server_close()

    def test_settings_changes_keep_active_streams_and_new_models_usable(self) -> None:
        for change in ("save_unchanged", "change_baseurl"):
            with self.subTest(change=change):
                release_second = threading.Event()
                paths = []

                class StreamingHandler(BaseHTTPRequestHandler):
                    def do_POST(self):
                        self.rfile.read(int(self.headers.get("Content-Length", "0")))
                        paths.append(self.path)
                        self.send_response(200)
                        self.send_header("Content-Type", "text/event-stream")
                        self.end_headers()
                        for text in ("first", "second"):
                            if text == "second":
                                release_second.wait(timeout=5)
                            chunk = {
                                "id": "chatcmpl-review",
                                "object": "chat.completion.chunk",
                                "created": 1,
                                "model": "test",
                                "choices": [{"index": 0, "delta": {"content": text},
                                             "finish_reason": "stop" if text == "second" else None}],
                            }
                            try:
                                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                                self.wfile.flush()
                            except OSError:
                                return
                        self.wfile.write(b"data: [DONE]\n\n")

                    def log_message(self, *args):
                        pass

                with local_http_server(StreamingHandler) as httpd:
                    baseurl = f"http://127.0.0.1:{httpd.server_port}"
                    settings = {"baseurl": baseurl + "/v1", "model": "test", "apikey": "EMPTY"}
                    with patch.object(agent, "get_llm_settings", return_value=settings):
                        old_llm = agent.build_llm()
                        stream = old_llm.stream("test")
                        try:
                            first = next(stream)
                            if change == "save_unchanged":
                                agent.invalidate_settings()
                            else:
                                settings["baseurl"] = baseurl + "/new/v1"
                            new_llm = agent.build_llm()
                            self.assertIsNot(old_llm.http_client, new_llm.http_client)
                            release_second.set()
                            text = first.content + "".join(part.content for part in stream)
                            self.assertEqual(text, "firstsecond")
                            self.assertEqual(new_llm.invoke("test").content, "firstsecond")
                            expected_path = "/v1/chat/completions" if change == "save_unchanged" else "/new/v1/chat/completions"
                            self.assertEqual(paths, ["/v1/chat/completions", expected_path])
                        finally:
                            release_second.set()
                            stream.close()

    def test_retired_clients_close_after_all_models_release_them(self) -> None:
        settings = {"baseurl": "http://127.0.0.1:8045/v1", "model": "test", "apikey": "EMPTY"}
        with patch.object(agent, "get_llm_settings", return_value=settings):
            first = agent.build_llm()
            second = agent.build_llm()
        sync_client, async_client = first.http_client, first.http_async_client
        self.assertIs(sync_client, second.http_client)
        agent.invalidate_settings()
        del first
        gc.collect()
        self.assertFalse(sync_client.is_closed)
        self.assertFalse(async_client.is_closed)
        del second
        gc.collect()
        self.assertTrue(sync_client.is_closed)
        self.assertTrue(async_client.is_closed)

    def test_error_classification_and_key_redaction(self) -> None:
        secret = "sk-super-secret-token-value"
        err_401 = RuntimeError(f"Error code: 401 - Invalid Authorization Bearer {secret}")
        res_401 = agent.classify_llm_error(err_401, apikey=secret)
        self.assertEqual(res_401["error_type"], "auth_error")
        self.assertEqual(res_401["status_code"], 401)
        self.assertNotIn(secret, res_401["error"])

        err_timeout = TimeoutError(f"Request timed out with key {secret}")
        res_timeout = agent.classify_llm_error(err_timeout, apikey=secret)
        self.assertEqual(res_timeout["error_type"], "timeout")
        self.assertIsNone(res_timeout["status_code"])
        self.assertNotIn(secret, res_timeout["error"])


class WzypPlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_16688 = str(Path(self.temp_dir.name) / "16688.sqlite3")
        self.db_wzyp = str(Path(self.temp_dir.name) / "wzyp.sqlite3")
        self.env_patch = patch.dict(
            os.environ,
            {
                "WZYP_16688_DB": self.db_16688,
                **{key: str(Path(self.temp_dir.name)/(key+".sqlite3")) for key in ["WZYP_CATFK_DB","WZYP_DUJIAO_NEXT_DB","WZYP_DUJIAOKA_DB","WZYP_YIKE_DB","WZYP_DISCOVERY_DB"]},
                "WZYP_LIVE_DB": self.db_wzyp,
                "WZYP_SQLITE_DB": self.db_wzyp,
            },
            clear=False,
        )
        self.env_patch.start()

    def tearDown(self) -> None:
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def test_four_category_pagination_omits_category_id_and_maps_fields(self) -> None:
        token = "9B5624X"
        card_page_1 = [
            {
                "goods_key": f"CARD{i:02d}X",
                "name": f"卡密商品{i}",
                "goods_type": "card",
                "price": 15.0 + i,
                "market_price": 20.0 + i,
                "category": {"id": 11, "name": "激活码"},
                "status": 1,
            }
            for i in range(1, 21)
        ]
        card_page_2 = [
            {
                "goods_key": "CARD21X",
                "name": "卡密商品21",
                "goods_type": "card",
                "price": 18.0,
                "market_price": 22.0,
                "category": {"id": 11, "name": ""},
                "status": 1,
            }
        ]
        shops_data = {
            token: make_open_shop(
                token,
                nickname="四类分页小铺",
                categories={
                    "card": [{"id": 11, "name": "激活码"}],
                    "article": [{"id": 22, "name": "教程文档"}],
                    "resource": [{"id": 33, "name": "网盘资源"}],
                    "equity": [{"id": 44, "name": "会员权益"}],
                },
                goods_pages={
                    "card": {
                        1: {"total": 21, "list": card_page_1},
                        2: {"total": 21, "list": card_page_2},
                    },
                    "article": {
                        1: {
                            "total": 1,
                            "list": [
                                {
                                    "goods_key": "ART01X",
                                    "name": "文章商品1",
                                    "goods_type": "article",
                                    "price": 9.9,
                                    "market_price": 0,
                                    "category": {"id": 22, "name": "教程文档"},
                                }
                            ],
                        }
                    },
                    "resource": {
                        1: {
                            "total": 1,
                            "list": [
                                {
                                    "goods_key": "RES01X",
                                    "name": "资源商品1",
                                    "goods_type": "resource",
                                    "price": 29.9,
                                    "market_price": 39.9,
                                    "category": {"id": 33, "name": "网盘资源"},
                                }
                            ],
                        }
                    },
                    "equity": {
                        1: {
                            "total": 1,
                            "list": [
                                {
                                    "goods_key": "EQ01X",
                                    "name": "权益商品1",
                                    "goods_type": "equity",
                                    "price": 5.0,
                                    "market_price": 8.0,
                                    "category": {"id": 44, "name": "会员权益"},
                                }
                            ],
                        }
                    },
                },
            )
        }
        session = FakeWzypSession(shops_data)
        summary = platformwzyp.sync_shop(
            f"https://wzyp.cn/shop/{token}",
            session=session,
            store_path=self.db_wzyp,
        )
        self.assertEqual(summary["shop"]["token"], token)
        self.assertEqual(summary["goods_count"], 24)
        for req_payload in session.goods_list_payloads:
            self.assertNotIn("category_id", req_payload)

        with db.use_source("wzyp-live"):
            types = {r["goods_type"]: r["cnt"] for r in db.query("SELECT goods_type, COUNT(*) AS cnt FROM wzyp_goods GROUP BY goods_type")}
            self.assertEqual(types, {"card": 21, "article": 1, "resource": 1, "equity": 1})
            row = db.query("SELECT * FROM wzyp_goods WHERE goods_key = 'CARD21X'")[0]
            self.assertEqual(row["category_name"], "激活码")
            self.assertIsNone(row["stock_quantity"])
            self.assertIsNone(row["stock_status"])

    def test_missing_or_invalid_page_totals_preserve_existing_snapshot(self) -> None:
        token = "TOTAL01X"
        initial_page = {"total": 1, "list": [{"goods_key": "G1", "name": "原商品", "price": 10}]}
        shops = {token: make_open_shop(token, goods_pages={"card": {1: initial_page}})}
        url = f"https://wzyp.cn/shop/{token}"
        platformwzyp.sync_shop(url, session=FakeWzypSession(shops), store_path=self.db_wzyp)
        before = local_store.readonly_query("SELECT * FROM wzyp_goods", path=self.db_wzyp)
        details_before = local_store.readonly_query("SELECT * FROM wzyp_goods_detail", path=self.db_wzyp)
        broken_pages = [{"list": []}] + [
            {"list": [], "total": value} for value in (None, "", True, -1, "invalid")
        ]
        for page in broken_pages:
            with self.subTest(page=page):
                shops[token]["goods_pages"]["card"] = {1: page}
                with self.assertRaises(platformwzyp.PlatformError):
                    platformwzyp.sync_shop(url, session=FakeWzypSession(shops), store_path=self.db_wzyp)
                self.assertEqual(local_store.readonly_query("SELECT * FROM wzyp_goods", path=self.db_wzyp), before)
                self.assertEqual(local_store.readonly_query("SELECT * FROM wzyp_goods_detail", path=self.db_wzyp), details_before)

    def test_missing_total_on_later_page_preserves_existing_snapshot(self) -> None:
        token = "TOTAL02X"
        initial_page = {"total": 1, "list": [{"goods_key": "G1", "name": "原商品", "price": 10}]}
        shops = {token: make_open_shop(token, goods_pages={"card": {1: initial_page}})}
        url = f"https://wzyp.cn/shop/{token}"
        platformwzyp.sync_shop(url, session=FakeWzypSession(shops), store_path=self.db_wzyp)
        before = local_store.readonly_query("SELECT * FROM wzyp_goods", path=self.db_wzyp)
        items = [{"goods_key": f"NEW{i}", "name": f"新商品{i}", "price": 20} for i in range(21)]
        shops[token]["goods_pages"]["card"] = {
            1: {"total": 21, "list": items[:20]},
            2: {"list": items[20:]},
        }
        with self.assertRaises(platformwzyp.PlatformError):
            platformwzyp.sync_shop(url, session=FakeWzypSession(shops), store_path=self.db_wzyp)
        self.assertEqual(local_store.readonly_query("SELECT * FROM wzyp_goods", path=self.db_wzyp), before)

    def test_count_change_retries_once_and_preserves_snapshot_on_persistent_mismatch(self) -> None:
        token = "0C0164X"
        attempts = {"card": 0}

        def dynamic_card_pages(page: int, payload: dict) -> dict:
            attempts["card"] += 1
            if attempts["card"] == 1:
                # First attempt says total=2 but only returns 1 item
                return {
                    "total": 2,
                    "list": [{"goods_key": "G1", "name": "商品1", "goods_type": "card", "price": 10.0}],
                }
            return {
                "total": 1,
                "list": [{"goods_key": "G1", "name": "商品1", "goods_type": "card", "price": 10.0}],
            }

        shops_data = {
            token: make_open_shop(
                token,
                nickname="重试小铺",
                goods_pages={"card": dynamic_card_pages},
            )
        }
        summary = platformwzyp.sync_shop(
            f"https://wzyp.cn/shop/{token}",
            session=FakeWzypSession(shops_data),
            store_path=self.db_wzyp,
        )
        self.assertEqual(summary["goods_count"], 1)
        self.assertEqual(attempts["card"], 2)

        # Now test persistent count mismatch -> keeps existing snapshot
        def broken_card_pages(page: int, payload: dict) -> dict:
            return {
                "total": 3,
                "list": [{"goods_key": "G1", "name": "商品1", "goods_type": "card", "price": 10.0}],
            }

        shops_data[token]["goods_pages"]["card"] = broken_card_pages
        with self.assertRaises(platformwzyp.PlatformError):
            platformwzyp.sync_shop(
                f"https://wzyp.cn/shop/{token}",
                session=FakeWzypSession(shops_data),
                store_path=self.db_wzyp,
            )
        with db.use_source("wzyp-live"):
            self.assertEqual(db.query("SELECT COUNT(*) AS c FROM wzyp_goods")[0]["c"], 1)

    def test_empty_shop_closed_shop_and_detail_failure_behavior(self) -> None:
        token = "EMPTY01X"
        # 1. Seed shop with 1 item first
        seed_shops = {
            token: make_open_shop(
                token,
                nickname="测试空店与打烊",
                goods_pages={
                    "card": {
                        1: {
                            "total": 1,
                            "list": [{"goods_key": "SEED1", "name": "初始商品", "goods_type": "card", "price": 5.0}],
                        }
                    }
                },
            )
        }
        platformwzyp.sync_shop(
            f"https://wzyp.cn/shop/{token}",
            session=FakeWzypSession(seed_shops),
            store_path=self.db_wzyp,
        )

        # 2. Detail failure preserves previous snapshot
        with self.assertRaises(platformwzyp.PlatformError):
            platformwzyp.sync_shop(
                f"https://wzyp.cn/shop/{token}",
                session=FakeWzypSession(seed_shops, details_data={"SEED1": {"code": 0, "msg": "详情服务异常"}}),
                store_path=self.db_wzyp,
            )
        with db.use_source("wzyp-live"):
            self.assertEqual(db.query("SELECT COUNT(*) AS c FROM wzyp_goods")[0]["c"], 1)

        # 3. Closed shop preserves previous snapshot
        closed_shops = {
            token: {
                "info": {
                    "code": 1,
                    "data": {
                        "token": token,
                        "nickname": "测试空店与打烊",
                        "custom_status": 0,
                        "custom_status_msg": "店主休息中",
                    },
                }
            }
        }
        with self.assertRaises(platformwzyp.PlatformError):
            platformwzyp.sync_shop(
                f"https://wzyp.cn/shop/{token}",
                session=FakeWzypSession(closed_shops),
                store_path=self.db_wzyp,
            )
        with db.use_source("wzyp-live"):
            self.assertEqual(db.query("SELECT COUNT(*) AS c FROM wzyp_goods")[0]["c"], 1)

        # 4. Open empty shop updates snapshot to 0 goods and fetch_done=1
        empty_shops = {token: make_open_shop(token, nickname="测试空店与打烊", goods_pages={})}
        summary = platformwzyp.sync_shop(
            f"https://wzyp.cn/shop/{token}",
            session=FakeWzypSession(empty_shops),
            store_path=self.db_wzyp,
        )
        self.assertEqual(summary["goods_count"], 0)
        with db.use_source("wzyp-live"):
            self.assertEqual(db.query("SELECT COUNT(*) AS c FROM wzyp_goods")[0]["c"], 0)
            shop_row = db.query("SELECT fetch_done, goods_count FROM wzyp_shops WHERE token = %s", (token,))[0]
            self.assertEqual(shop_row["fetch_done"], 1)
            self.assertEqual(shop_row["goods_count"], 0)

    def test_link_extraction_and_token_deduplication(self) -> None:
        html_and_text = """
        欢迎访问分店 <a href="https://wzyp.cn/shop/9B5624X?from=notice">主店</a>
        以及备用链接 https://www.wzyp.cn/shop/0C0164X，
        重复链接 https://wzyp.cn/shop/9B5624X 和外部链接 https://example.com/shop/BAD1234
        """
        links = platformwzyp.extract_links(html_and_text)
        self.assertEqual(
            links,
            sorted([
                "https://wzyp.cn/shop/9B5624X",
                "https://wzyp.cn/shop/0C0164X",
            ]),
        )

    def test_403_or_captcha_interrupts_job_and_preserves_pending_checkpoint(self) -> None:
        t1 = "SHOP001X"
        t2 = "SHOP002X"
        blocked = {"active": True}
        t1_done = threading.Event()

        shops_data = {
            t1: make_open_shop(
                t1,
                nickname="首店成功",
                goods_pages={
                    "card": {
                        1: {
                            "total": 1,
                            "list": [{"goods_key": "G100", "name": "首店商品", "goods_type": "card", "price": 5.0}],
                        }
                    }
                },
            ),
            t2: make_open_shop(
                t2,
                nickname="恢复后成功店铺",
                goods_pages={
                    "card": {
                        1: {
                            "total": 1,
                            "list": [{"goods_key": "G200", "name": "恢复商品", "goods_type": "card", "price": 6.0}],
                        }
                    }
                },
            ),
        }

        class BlockableSession(FakeWzypSession):
            def post(self, url: str, json: dict | None = None, **kwargs) -> FakeResponse:
                payload = json or {}
                if payload.get("token") == t2 and blocked["active"]:
                    t1_done.wait(timeout=2.0)
                    raise public_http.CrawlBlockedError("触发 403 人机验证")
                resp = super().post(url, json=json, **kwargs)
                if payload.get("goods_key") == "G100":
                    t1_done.set()
                return resp

        manager = CrawlManager(
            path=self.db_wzyp,
            session_factory=lambda stop=None: BlockableSession(shops_data),
            workflow_version=1,  # Existing interrupted checkpoints keep the original pipeline.
            discover=lambda session, progress=None: [f"https://wzyp.cn/shop/{t1}", f"https://wzyp.cn/shop/{t2}"],
            platform_adapter=platformwzyp,
        )
        try:
            job = manager.start(mode="discover")
            manager.thread.join(5.0)
            finished = manager.store.report(job["job_id"])
            self.assertEqual(finished["status"], "interrupted")
            self.assertEqual(finished["success_shops"], 1)
            self.assertGreaterEqual(finished["pending_links"], 1)

            # Unblock and resume: t1 is skipped (already done), t2 is retried and completes
            blocked["active"] = False
            resumed = manager.resume(job["job_id"])
            manager.thread.join(5.0)
            final_state = manager.store.report(resumed["job_id"])
            self.assertEqual(final_state["status"], "completed")
            self.assertEqual(final_state["success_shops"], 2)
            self.assertEqual(final_state["collected_goods"], 2)
        finally:
            manager.shutdown()

    def test_api_routes_and_source_isolation_across_two_sources(self) -> None:
        t_wzyp = "WZYP999X"
        platformwzyp.sync_shop(
            f"https://wzyp.cn/shop/{t_wzyp}",
            session=FakeWzypSession(
                {
                    t_wzyp: make_open_shop(
                        t_wzyp,
                        nickname="链动专属店铺",
                        goods_pages={
                            "equity": {
                                1: {
                                    "total": 1,
                                    "list": [{"goods_key": "WGOOD1", "name": "链动权益卡", "goods_type": "equity", "price": 12.0}],
                                }
                            }
                        },
                    )
                }
            ),
            store_path=self.db_wzyp,
        )

        with TestClient(server.app) as client:
            sources = client.get("/api/sources").json()["sources"]
            source_ids = [s["id"] for s in sources]
            self.assertEqual(source_ids, ["wzyp-live", "16688", "dujiao-next", "dujiaoka", "catfk", "yike"])
            self.assertEqual([s["name"] for s in sources], ["链动小铺", "16688 交易平台", "Dujiao-Next", "独角数卡", "云猫寄售", "一可比价网"])
            self.assertFalse(sources[-1]["discovery"])
            self.assertTrue(sources[-1]["keyword_crawl"])

            stats_wzyp_live = client.get("/api/stats?source=wzyp-live").json()
            self.assertEqual(stats_wzyp_live["stats"]["shop_count"], 1)
            self.assertEqual(stats_wzyp_live["stats"]["goods_count"], 1)

            stats_16688 = client.get("/api/stats?source=16688").json()
            self.assertEqual(stats_16688["stats"]["shop_count"], 0)

            goods_resp = client.post("/api/goods?source=wzyp-live", json={"page": 1, "pageSize": 10}).json()
            self.assertEqual(goods_resp["total"], 1)
            self.assertEqual(goods_resp["rows"][0]["goods_key"], "WGOOD1")


class CurlResponseTests(unittest.TestCase):
    def test_real_curl_preserves_retry_after_and_response_body(self) -> None:
        body = '{"message":"稍后重试"}'.encode("utf-8")

        class RetryHandler(BaseHTTPRequestHandler):
            hits = 0

            def do_GET(self):
                type(self).hits += 1
                self.send_response(429 if type(self).hits == 1 else 200)
                self.send_header("retry-after", "45")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        limiter = Mock()
        with local_http_server(RetryHandler) as httpd, patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}):
            with public_http.CurlSession(limiter=limiter) as session:
                response = session.get(f"http://127.0.0.1:{httpd.server_port}", expect_json=True)
        self.assertEqual(RetryHandler.hits, 2)
        limiter.defer.assert_called_once_with(45)
        self.assertEqual(response.headers.get("Retry-After"), "45")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, body)

    def test_redirect_headers_do_not_override_final_response_headers(self) -> None:
        body = b'{"ok":true}'

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", "/final")
                    self.send_header("Retry-After", "60")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.send_header("X-Final", "yes")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

            def log_message(self, *args):
                pass

        with local_http_server(RedirectHandler) as httpd, patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}):
            with public_http.CurlSession(limiter=Mock()) as session:
                response = session.get(f"http://127.0.0.1:{httpd.server_port}/redirect", allow_redirects=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("x-final"), "yes")
        self.assertNotIn("Retry-After", response.headers)
        self.assertEqual(response.content, body)


if __name__ == "__main__":
    unittest.main()
