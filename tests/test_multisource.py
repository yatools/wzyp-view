"""四来源、整店完整性、跨站发现与请求边界的离线回归。"""
import copy
import json
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
import requests
from unittest.mock import patch
from urllib.parse import urlsplit

from fastapi.testclient import TestClient

import catalog
from crawler16688 import CrawlManager
from discovery import DiscoveryManager, DiscoveryStore
import local_store
from platform_independent import IndependentPlatform, PlatformError
from public_http import CrawlBlockedError, CrawlCancelled, CurlResponse, RateLimiter
import server
from shop_http import SafeSession, normalize_url, resolve_public


def response(value, url, status=200, content_type=None):
    raw = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return CurlResponse(status, raw.encode(), {"Content-Type": content_type or ("text/html" if isinstance(value, str) else "application/json")}, url)


def next_product(identity=1, title="GPT Plus", description="<p>公开说明</p>", skus=None):
    return {"id": identity, "slug": f"product-{identity}", "title": {"zh-CN": title},
            "content": {"zh-CN": description}, "price_amount": "99.00", "fulfillment_type": "auto",
            "category": {"name": {"zh-CN": "会员"}},
            "skus": skus if skus is not None else [
                {"id": 11, "is_active": True, "sku_code": "MONTH", "spec_values": {"周期": "月卡"}, "price_amount": "99", "auto_stock_available": 3, "stock_status": "in_stock"},
                {"id": 12, "is_active": True, "spec_values": {"周期": "年卡"}, "price_amount": "999", "stock_quantity_hidden": True, "auto_stock_available": 88, "stock_status": "in_stock"},
                {"id": 13, "is_active": False, "price_amount": "1"},
            ]}


class Web:
    def __init__(self):
        self.products = {"https://next.example": [next_product()]}
        self.pages = {}
        self.calls = []
        self.break_page = False
        self.break_detail = False
        self.override_total = None
        self.blocked = set()
        self.delay = None
        self.choice = False

    def session(self, stop=None):
        web = self
        class Session:
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): self.close()
            def close(self): pass
            def check_cancel(self):
                if stop and stop.is_set(): raise CrawlCancelled()
            def get(self, url, **kwargs):
                self.check_cancel()
                web.calls.append(("GET", url, kwargs))
                origin = "https://" + urlsplit(url).netloc
                if origin in web.blocked: raise CrawlBlockedError("HTTP 403 访问阻断")
                if web.delay:
                    web.delay(stop)
                if url in web.pages: return response(web.pages[url], url)
                if origin not in web.products: return response("not found", url, 404)
                if url.endswith("/config"): return response({"code":0,"data":{"site_name":"公开测试货铺","currency":"CNY"}},url)
                if url.endswith("/categories"): return response({"code":0,"data":[]},url)
                if url.endswith("/products"):
                    page = kwargs.get("params",{}).get("page",1)
                    products = web.products[origin]
                    rows = products[page-1:page] if not (page>1 and web.break_page) else []
                    return response({"code":0,"data":copy.deepcopy(rows),"pagination":{"total":len(products) if web.override_total is None else web.override_total}},url)
                if "/products/" in url:
                    if web.break_detail: return response("unavailable",url,503)
                    slug=url.rsplit("/",1)[-1]
                    item=next((x for x in web.products[origin] if x["slug"]==slug),None)
                    return response({"code":0,"data":copy.deepcopy(item)},url)
                return response("not found",url,404)
            def post(self,url,**kwargs):
                web.calls.append(("POST",url,kwargs))
                if url.endswith("/api/productlist"):
                    return response({"code":1,"data":{"products":[{"id":7,"name":"Choice 商品","password":""},{"id":8,"password":"hidden"}]}},url)
                if url.endswith("/api/proudctinfo"):
                    return response({"code":1,"data":{"id":7,"pd_name":"Choice 商品","actual_price":"25.5","in_stock":0,"pd_type":2,"pd_info":"<p>公开说明</p>"}},url)
                raise AssertionError("不得访问其他 POST 接口")
        return Session()


class TemporaryCatalog(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ,{key:str(self.root/(name+".sqlite3")) for key,name in [
            ("WZYP_LIVE_DB","wzyp"),("WZYP_16688_DB","16688"),("WZYP_DUJIAO_NEXT_DB","next"),
            ("WZYP_DUJIAOKA_DB","classic"),("WZYP_CATFK_DB","catfk"),("WZYP_YIKE_DB","yike"),("WZYP_DISCOVERY_DB","discovery")]})
        self.env.start()
        self.web = Web()
        self.next = IndependentPlatform("dujiao-next")
        self.classic = IndependentPlatform("dujiaoka")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def sync(self, source=None, url="https://next.example/"):
        adapter = source or self.next
        return adapter.sync_shop(url,session=self.web.session(),store_path=adapter.database_path())

    def read(self, source="dujiao-next"):
        with catalog.connection([source]) as (conn, union):
            return [dict(r) for r in conn.execute("SELECT * FROM ("+union+")")] if union else []

    def seed(self, source, token, items, nickname="测试店铺"):
        rows=[]
        for item in items:
            key,name,price,*extra=item
            rows.append({"goods_key":key,"shop_token":token,"goods_type":"auto","name":name,"price":price,
                         "status":1,"link":f"https://{token.lower()}.example/buy/{key}","synced_at":"2026-10-23T12:00:00+08:00",
                         "currency":extra[0] if extra else "CNY"})
        shop={"token":token,"nickname":nickname,"goods_count":len(rows),"link":f"https://{token.lower()}.example/","synced_at":"2026-10-23T12:00:00+08:00"}
        local_store.save_snapshot(shop,rows,[],path=local_store.database_path(source))


class IndependentShopTests(TemporaryCatalog):
    def test_active_skus_are_separate_and_hidden_stock_is_not_exposed(self):
        result=self.sync()
        self.assertEqual(result["goods_count"],2)
        rows=sorted(self.read(),key=lambda x:x["price"])
        self.assertEqual([r["price"] for r in rows],[99,999])
        self.assertEqual(rows[0]["stock_quantity"],3)
        self.assertIsNone(rows[1]["stock_quantity"])
        self.assertNotEqual(rows[0]["goods_key"],rows[1]["goods_key"])
        self.assertIn("周期: 年卡",rows[1]["sku_label"])

    def test_origin_scopes_same_product_and_sku_numbers(self):
        self.sync()
        self.web.products["https://second.example"]=[next_product()]
        self.sync(url="https://second.example/products/product-1")
        self.assertEqual(len(self.read()),4)
        self.assertEqual(len({r["goods_key"] for r in self.read()}),4)

    def test_missing_page_preserves_previous_snapshot_and_links(self):
        self.web.products["https://next.example"][0]["content"]={"zh-CN":'<a href="https://other.example/">货铺</a>'}
        self.sync()
        old=self.read()
        self.web.products["https://next.example"].append(next_product(2))
        self.web.break_page=True
        with self.assertRaises(PlatformError):self.sync()
        self.assertEqual(self.read(),old)
        conn=sqlite3.connect(self.next.database_path())
        try:
            self.assertEqual(conn.execute("SELECT url FROM shop_links").fetchone()[0],"https://other.example/")
        finally:
            conn.close()

    def test_changed_total_duplicate_product_and_missing_total_are_rejected(self):
        self.sync();old=self.read()
        self.web.products["https://next.example"].append(next_product())
        with self.assertRaises(PlatformError):self.sync()
        self.assertEqual(self.read(),old)
        self.web.override_total=True
        with self.assertRaises(PlatformError):self.sync()

    def test_detail_failure_and_nonfinite_price_do_not_publish_partial_shop(self):
        self.sync();old=self.read();self.web.break_detail=True
        with self.assertRaises(PlatformError):self.sync()
        self.web.break_detail=False
        self.web.products["https://next.example"][0]["skus"][0]["price_amount"]="NaN"
        with self.assertRaises(PlatformError):self.sync()
        self.assertEqual(self.read(),old)

    def test_confirmed_empty_shop_removes_old_goods_only_after_valid_contract(self):
        self.sync();self.web.products["https://next.example"]=[]
        self.assertEqual(self.sync()["goods_count"],0)
        self.assertEqual(self.read(),[])

    def test_unknown_currency_stays_unknown_instead_of_assuming_cny(self):
        session=self.web.session();original=session.get
        def get(url,**kwargs):
            if url.endswith('/config'):return response({"data":{"site_name":"No currency"}},url)
            return original(url,**kwargs)
        session.get=get
        self.next.sync_shop("https://next.example",session=session)
        self.assertEqual({r['currency'] for r in self.read()},{''})

    def test_amazeui_and_layui_templates_parse_price_and_stock(self):
        for theme in ("amazeui","layui"):
            with self.subTest(theme=theme):
                origin=f"https://{theme}.example"
                self.web.pages[origin+'/']=f'<html><title>{theme}</title><div class="am-container layui-container"><a href="/buy/1">公开商品</a><a href="mailto:help@example.com">联系</a></div></html>'
                title='<span id="goodsname">经典商品</span>' if theme=='amazeui' else '<div class="layui-card-header"><span style="font-size:20px">经典商品</span></div>'
                price='<span class="trade-price">¥<span id="price">12.50</span></span>' if theme=='amazeui' else '<span class="product-price">¥ 12.50</span>'
                self.web.pages[origin+'/buy/1']=title+price+'<input name="pid" value="1"><input name="order_number" max="0"><div class="product-description">自动发货 <a href="https://next.example/">相关货铺</a></div>'
                result=self.sync(self.classic,origin)
                self.assertEqual(result['goods_count'],1)
        rows=self.read('dujiaoka')
        self.assertEqual(len(rows),2)
        self.assertTrue(all(r['price']==12.5 and r['stock_status']=='out_of_stock' and r['currency']=='CNY' for r in rows))

    def test_choice_uses_only_verified_public_read_endpoints_and_skips_locked_products(self):
        origin='https://choice.example'
        self.web.pages[origin+'/']='<html><title>Choice</title><div class="layui-container"><select id="productlist" name="pid"></select><input name="_token" value="anonymous"></div><script>"/api/typelist";"/api/proudctinfo";</script></html>'
        self.web.pages[origin+'/api/typelist']=json.dumps({'code':1,'data':{'typelist':[{'id':1,'name':'公开分类','password':''},{'id':2,'name':'加密分类','password':'hidden'}]}})
        result=self.sync(self.classic,origin)
        self.assertEqual(result['goods_count'],1)
        self.assertEqual(self.read('dujiaoka')[0]['price'],25.5)
        posts=[call for call in self.web.calls if call[0]=='POST']
        self.assertEqual(len(posts),2)
        self.assertFalse(any(call[2].get('data',{}).get('password') for call in posts))

    def test_hyper_and_luna_2x_fields_and_unlinked_sold_out_catalog_entry(self):
        origin='https://hyper.example'
        self.web.pages[origin+'/']='<link href="/assets/hyper/app.css"><div class="hyper-wrapper"><a class="home-card" href="/buy/1"><p class="name">在售商品</p><div class="price">¥20</div></a><a class="home-card ribbon-box" href="javascript:void(0)"><span class="ribbon-two-danger">缺货</span><p class="name">缺货商品</p><div class="price">¥15</div></a></div>'
        self.web.pages[origin+'/buy/1']='<div class="buy-shop"><form id="buy-form"><div class="form-group"><h3>在售商品</h3></div><input name="gid" value="1"><span class="buy-price">¥20</span><span>自动发货 库存(7)</span><input name="by_amount" data-bts-max="999"></form></div><div class="buy-product">公开说明</div>'
        self.sync(self.classic,origin)
        rows=self.read('dujiaoka')
        self.assertEqual(len(rows),2)
        entry=next(r for r in rows if r['catalog_entry'])
        self.assertEqual(entry['link'],origin+'/')
        self.assertEqual(entry['stock_quantity'],0)
        regular=next(r for r in rows if not r['catalog_entry'])
        self.assertEqual(regular['stock_quantity'],7)
        luna='https://luna.example'
        self.web.pages[luna+'/']='<div class="layui-container"><a href="/buy/2">月卡</a></div>'
        self.web.pages[luna+'/buy/2']='<form><input name="gid" value="2"><div class="goods-msg"><div class="goods-name"><span>月卡<span class="small-tips">库存(3)</span></span></div><div class="price">￥<span class="price-num">50</span></div></div></form><div class="intro">商品说明</div>'
        self.sync(self.classic,luna)
        row=next(r for r in self.read('dujiaoka') if r['price']==50)
        self.assertEqual(row['name'],'月卡')
        self.assertEqual(row['stock_quantity'],3)

    def test_classic_missing_second_catalog_page_preserves_previous_snapshot(self):
        origin='https://classic.example'
        self.web.pages[origin+'/']='<div class="am-container"><a href="/buy/1">商品</a></div>'
        self.web.pages[origin+'/buy/1']='<span id="goodsname">商品</span><span class="trade-price">¥<span id="price">10</span></span><input name="pid" value="1">'
        self.sync(self.classic,origin);old=self.read('dujiaoka')
        self.web.pages[origin+'/']+='<a rel="next" href="/?page=2">下一页</a>'
        with self.assertRaises(PlatformError):self.sync(self.classic,origin)
        self.assertEqual(self.read('dujiaoka'),old)

    def test_classic_bootstrap_variant_uses_explicit_price_and_gid(self):
        origin = 'https://bootstrap.example'
        self.web.pages[origin+'/'] = '<div class="goods-list"><a href="/buy/1">商品</a></div><footer>Powered by 独角数卡</footer>'
        self.web.pages[origin+'/buy/1'] = '<div class="card-body"><h3 class="card-title">公开商品</h3><h6>人工处理</h6><h6>库存：7</h6><div class="col-12"><h6>价格：￥ 12.50</h6></div><input name="gid" value="1"></div><div class="card-header"><h5>商品描述</h5></div><div class="card-body"><p>原始说明</p></div><footer>Powered by 独角数卡</footer>'
        self.sync(self.classic, origin)
        row = self.read('dujiaoka')[0]
        self.assertEqual((row['name'], row['price'], row['currency'], row['stock_quantity'], row['delivery_method']),
                         ('公开商品', 12.5, 'CNY', 7, 'manual'))
        self.assertIn('原始说明', catalog.detail('dujiaoka', row['goods_key'])['description'])
        self.web.pages[origin+'/buy/1'] = self.web.pages[origin+'/buy/1'].replace('value="1"', 'value="2"')
        with self.assertRaises(PlatformError):
            self.sync(self.classic, origin)
        self.assertEqual(self.read('dujiaoka'), [row])

    def test_unknown_classic_template_preserves_store(self):
        self.seed('dujiaoka','OLD',[('1','原商品',10)])
        self.web.pages['https://unknown.example/']='<h1>访问验证</h1>'
        with self.assertRaises(PlatformError):self.sync(self.classic,'https://unknown.example/')
        self.assertEqual(len(self.read('dujiaoka')),1)

    def test_cancel_before_commit_does_not_replace_snapshot(self):
        self.sync();old=self.read()
        session=self.web.session()
        session.check_cancel=lambda: (_ for _ in ()).throw(CrawlCancelled())
        with self.assertRaises(CrawlCancelled):self.next.sync_shop('https://next.example',session=session)
        self.assertEqual(self.read(),old)


class CatalogTests(TemporaryCatalog):
    def setUp(self):
        super().setUp()
        self.seed('wzyp-live','S1',[('A','GPT Plus 月卡',100),('B','GPT 其他',5),('Z','GPT Plus 免费',0)])
        self.seed('16688','S1',[('A','GPT PLUS 共享',20),('P','100%_Plus',None)])
        self.seed('dujiao-next','S1',[('A','GPT Plus 年卡',300),('U','GPT Plus USD',5,'USD')])
        self.client=TestClient(server.app)

    def test_global_sort_and_pages_use_stable_source_scoped_identity(self):
        query={'keywords':['gpt','plus'],'currency':'CNY','exclude_zero':True,'pageSize':2}
        first=self.client.post('/api/catalog/search',json=query).json()
        second=self.client.post('/api/catalog/search',json={**query,'page':2}).json()
        self.assertEqual(first['total'],3)
        self.assertEqual([r['price'] for r in first['rows']+second['rows']],[20,100,300])
        self.assertEqual(len({(r['source'],r['goods_key']) for r in first['rows']+second['rows']}),3)

    def test_literal_keywords_and_unknown_prices(self):
        data=self.client.post('/api/catalog/search',json={'keywords':['100%_']}).json()
        self.assertEqual(data['total'],1)
        self.assertIsNone(data['rows'][0]['price'])
        self.assertEqual(data['rows'][0]['stock_status'],'unknown')

    def test_alias_dedup_currency_scope_and_no_exchange_conversion(self):
        data=self.client.post('/api/catalog/search',json={'sources':['wzyp','wzyp-live']}).json()
        self.assertEqual(data['total'],3)
        usd=self.client.post('/api/catalog/search',json={'currency':'USD'}).json()
        self.assertEqual(usd['total'],1)
        self.assertEqual(usd['rows'][0]['price'],5)
        all_currency=self.client.post('/api/catalog/search',json={'currency':None}).json()
        self.assertEqual(all_currency['currencies'],['CNY','USD'])

    def test_shop_and_source_filters_and_invalid_query_are_validated(self):
        query={'shop_token':'S1','shop_source':'16688'}
        self.assertEqual(self.client.post('/api/catalog/search',json=query).json()['total'],2)
        for query in ({'sources':[]},{'sources':['other']},{'price_min':20,'price_max':1},{'keywords':[5]},{'page':0}):
            self.assertEqual(self.client.post('/api/catalog/search',json=query).status_code,422)

    def test_catalog_reads_do_not_call_write_capable_connection(self):
        with patch.object(local_store,'connect',side_effect=AssertionError('聚合查询不可迁移或写数据库')):
            self.assertGreater(catalog.search({'currency':'CNY'})['total'],0)
            self.assertEqual(catalog.detail('wzyp','A')['price'],100)
        self.assertEqual(self.client.get('/api/catalog/goods/wzyp-live/missing').status_code,404)

    def test_detail_keeps_the_same_delivery_and_stock_meaning_as_search(self):
        detail=self.client.get('/api/catalog/goods/16688/A').json()
        self.assertEqual(detail['delivery_method'],'auto')
        self.assertEqual(detail['stock_status'],'unknown')

    def test_ai_cannot_access_link_outbox_or_discovery_tables(self):
        import db
        with db.use_source('dujiao-next'):
            for table in ('shop_links','crawl_jobs','discovery_candidates'):
                self.assertIn('error',db.run_readonly_sql(f'SELECT * FROM {table}'))


class FakeNative:
    PlatformError=PlatformError
    def __init__(self,source,path,web,gate=None):
        from sources import adapter_for
        self.actual=adapter_for(source);self.SOURCE_KEY=source;self.BASE_URL=self.actual.BASE_URL
        self.path=path;self.web=web;self.gate=gate;self.calls=[]
        self.normalize_target=self.actual.normalize_target
        self.session_factory=web.session
    def database_path(self):return self.path
    def resolve_target(self,url,session):
        norm=self.normalize_target(url);return {'shop_no':norm['target']},None
    def sync_shop(self,url,session=None,resolved=None,store_path=None):
        if self.gate:
            while not self.gate.wait(.02):session.check_cancel()
        token=resolved[0]['shop_no'];self.calls.append(token)
        shop={'token':token,'nickname':self.SOURCE_KEY,'goods_count':1,'link':f'{self.BASE_URL}/shop/{token}','synced_at':'2026-10-23T00:00:00+08:00'}
        item={'goods_key':token+'-goods','shop_token':token,'name':'公开商品','goods_type':'auto','price':10,'status':1,'link':url,'synced_at':shop['synced_at']}
        local_store.save_snapshot(shop,[item],[],path=store_path)
        return {'shop':shop,'goods_count':1,'discovered_urls':[]}


class DiscoveryTests(TemporaryCatalog):
    """Regression coverage for persisted version 1 workflows; new defaults are tested in test_staged_discovery."""
    def setUp(self):
        super().setUp()
        self.next.session_factory=self.web.session;self.classic.session_factory=self.web.session
        self.native={source:FakeNative(source,local_store.database_path(source),self.web) for source in ('wzyp-live','16688','catfk')}
        self.managers={source:CrawlManager(path=adapter.database_path(),platform_adapter=adapter) for source,adapter in {**self.native,'dujiao-next':self.next,'dujiaoka':self.classic}.items()}
        self.discovery=DiscoveryManager(self.managers,path=str(self.root/'discovery.sqlite3'),page_session_factory=self.web.session,workflow_version=1,
            detector=lambda url,stop:'dujiao-next' if urlsplit(url).hostname in ('next.example','third.example') else 'dujiaoka' if urlsplit(url).hostname=='classic.example' else None)

    def tearDown(self):
        self.discovery.shutdown()
        for manager in self.managers.values():manager.shutdown()
        super().tearDown()

    def finish(self,job_id):
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            job=self.discovery.store.report(job_id)
            if job['status'] not in ('running','cancelling'):return job
            time.sleep(.02)
        self.fail('发现任务未在离线测试期限内结束')

    def test_mixed_directory_collects_four_sources_and_keeps_provenance(self):
        self.web.pages['https://directory.example/']='<a href="https://wzyp.cn/shop/W1">W</a><a href="https://www.16688.com.cn/shop/S1">S</a><a href="https://next.example/products/product-1">N</a><a href="https://classic.example/">C</a><a href="/">循环</a>'
        self.web.pages['https://classic.example/']='<div class="am-container"><a href="/buy/7">商品</a></div>'
        self.web.pages['https://classic.example/buy/7']='<span id="goodsname">传统商品</span><span class="trade-price">¥<span id="price">8</span></span><input name="pid" value="7">'
        job=self.discovery.start(['https://directory.example/'],include_indexes=False)
        result=self.finish(job['job_id'])
        self.assertEqual(result['status'],'completed',result)
        self.assertEqual(result['success_shops'],4)
        self.assertEqual(result['collected_goods'],5)
        candidates=self.discovery.store.candidates(job['job_id'])['rows']
        self.assertTrue(any('https://directory.example/' in c['parents'] for c in candidates if c['source']=='dujiao-next'))

    def test_repeated_shop_links_do_not_create_duplicate_snapshots(self):
        job=self.discovery.start(['https://next.example/','https://next.example/products/product-1','https://next.example/#fragment'],include_indexes=False)
        result=self.finish(job['job_id'])
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(result['counts'].get('duplicate'),1)
        details=[call for call in self.web.calls if call[1].endswith('/products/product-1')]
        self.assertEqual(len(details),1)

    def test_unvisited_origin_precedes_more_pages_at_same_depth(self):
        store = self.discovery.store
        job_id = store.create(['https://directory.example/',
                               'https://directory.example/products/1',
                               'https://next.example/'], ['dujiao-next'], False)
        first = store.next_candidate(job_id)
        store.update_candidate(first['id'], 'directory')
        self.assertEqual(store.next_candidate(job_id)['url'], 'https://next.example/')
        # 仍优先保留跨站深度，不能用新域名的深层链接越过本层队列。
        store.enqueue(job_id, ['https://deeper.example/'], 'https://next.example/', 1)
        store.update_candidate(store.next_candidate(job_id)['id'], 'collected')
        self.assertEqual(store.next_candidate(job_id)['url'],
                         'https://directory.example/products/1')

    def test_cross_origin_depth_checkpoint_can_resume_without_recrawling_completed_shop(self):
        self.web.products['https://next.example'][0]['content']={'zh-CN':'<a href="https://third.example/">下一个货铺</a>'}
        self.web.products['https://third.example']=[next_product()]
        job=self.discovery.start(['https://next.example/'],include_indexes=False,max_depth=0)
        result=self.finish(job['job_id'])
        self.assertEqual(result['status'],'partial')
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(result['counts'].get('deferred'),1)
        self.discovery.resume(job['job_id'],max_depth=1)
        result=self.finish(job['job_id'])
        self.assertEqual(result['success_shops'],2)
        self.assertEqual(result['status'],'completed',result)

    def test_directory_budget_does_not_truncate_product_pagination(self):
        self.web.pages['https://directory.example/']='<a href="/more">下一页目录</a><a href="https://next.example/">货铺</a>'
        self.web.pages['https://directory.example/more']='<a href="https://next.example/">货铺</a>'
        self.web.products['https://next.example'].append(next_product(2))
        job=self.discovery.start(['https://directory.example/'],include_indexes=False,page_limit=1)
        result=self.finish(job['job_id'])
        self.assertEqual(result['collected_goods'],4)
        self.assertEqual(result['counts'].get('deferred'),1)
        self.discovery.resume(job['job_id'],extra_pages=1)
        self.assertEqual(self.finish(job['job_id'])['status'],'completed')

    def test_cancel_while_waiting_does_not_cancel_existing_platform_job(self):
        gate=threading.Event();self.native['16688'].gate=gate
        external=self.managers['16688'].start('targets',['https://www.16688.com.cn/shop/EXTERNAL'])
        try:
            job=self.discovery.start(['https://www.16688.com.cn/shop/NEW'],include_indexes=False)
            deadline=time.monotonic()+3
            while time.monotonic()<deadline:
                states=self.discovery.store.report(job['job_id'])['counts']
                if states.get('waiting'):break
                time.sleep(.02)
            self.discovery.cancel(job['job_id'])
            self.assertEqual(self.finish(job['job_id'])['status'],'cancelled')
            self.assertEqual(self.managers['16688'].store.report(external['job_id'])['status'],'running')
        finally:
            gate.set();self.managers['16688'].thread.join(timeout=3)

    def test_dead_owner_is_recovered_and_candidate_state_is_retained(self):
        store=DiscoveryStore(str(self.root/'recovery.sqlite3'))
        job_id=store.create(['https://next.example/'],['dujiao-next'],False)
        item=store.next_candidate(job_id)
        store.update_candidate(item['id'],'collecting',source='dujiao-next',child_job_id='remember-child')
        with store.connection() as conn:conn.execute('UPDATE discovery_jobs SET owner_pid=0,heartbeat=0')
        store.recover()
        self.assertEqual(store.report(job_id)['status'],'interrupted')
        self.assertEqual(store.next_candidate(job_id)['child_job_id'],'remember-child')

    def test_completed_child_is_recovered_if_parent_registration_was_interrupted(self):
        job_id=self.discovery.store.create(['https://next.example/'],['dujiao-next'],False)
        item=self.discovery.store.next_candidate(job_id)
        ownership=job_id+':'+str(item['id'])
        child=self.managers['dujiao-next'].start('targets',[item['url']],reuse=False,discovery_owner=ownership)
        self.managers['dujiao-next'].thread.join(timeout=3)
        self.assertEqual(self.managers['dujiao-next'].store.report(child['job_id'])['status'],'completed')
        self.discovery.store.update_job(job_id,status='interrupted',initialized=1)
        calls=len(self.web.calls)
        self.discovery.resume(job_id)
        result=self.finish(job_id)
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(len(self.web.calls),calls,'恢复不应重新请求已完成店铺')
        row=self.discovery.store.candidates(job_id)['rows'][0]
        self.assertEqual(row['child_job_id'],child['job_id'])
        self.assertEqual(row['state'],'collected')

    def test_failed_initial_enqueue_rolls_back_task_creation(self):
        with patch.object(self.discovery.store,'_enqueue',side_effect=RuntimeError('写入中断')):
            with self.assertRaises(RuntimeError):self.discovery.store.create(['https://next.example/'],['dujiao-next'],False)
        self.assertEqual(self.discovery.store.jobs(),[])

    def test_blocked_site_does_not_abort_other_independent_shop(self):
        self.web.blocked.add('https://next.example')
        self.web.products['https://third.example']=[next_product()]
        job=self.discovery.start(['https://next.example/','https://third.example/'],include_indexes=False)
        result=self.finish(job['job_id'])
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(result['status'],'partial')

    def test_discovery_api_validation_status_candidates_and_source_routes(self):
        client=TestClient(server.app)
        with patch.object(server.multisource_runtime,'discovery',return_value=self.discovery),patch.object(server.multisource_runtime,'manager',side_effect=lambda source:self.managers[source]):
            self.assertEqual(client.post('/api/discovery/jobs',json={'sources':[]}).status_code,422)
            self.assertEqual(client.get('/api/discovery/jobs/missing').status_code,404)
            result=client.post('/api/discovery/jobs',json={'urls':['https://next.example/'],'sources':['dujiao-next'],'include_indexes':False})
            self.assertEqual(result.status_code,200,result.text)
            job_id=result.json()['job_id'];self.finish(job_id)
            self.assertEqual(client.get('/api/discovery/jobs/'+job_id).json()['success_shops'],1)
            self.assertEqual(client.get('/api/discovery/jobs/'+job_id+'/candidates').json()['total'],1)
            self.assertEqual(client.get('/api/discovery/jobs/'+job_id+'/candidates?pageSize=0').status_code,422)
            self.assertEqual(len(client.get('/api/discovery/jobs').json()['jobs']),1)
            self.assertEqual(client.post('/api/discovery/jobs/'+job_id+'/resume',json={}).status_code,409)
            self.assertEqual(client.get('/api/sources/dujiao-next/jobs').json()['jobs'][0]['status'],'completed')
            self.assertEqual(client.get('/api/sources/dujiao-next/crawl').status_code,200)
            sync=client.post('/api/sources/dujiao-next/sync',json={'url':'https://next.example/'})
            self.assertEqual(sync.status_code,200,sync.text)

    def test_index_failure_is_retried_on_resume_and_known_data_is_preserved(self):
        failing=[True]
        def reader(session,progress):
            if failing[0]:raise PlatformError('索引临时失败')
            return ['https://wzyp.cn/shop/W1']
        self.discovery.index_readers={'wzyp-live':reader,'16688':lambda *args:[]}
        job=self.discovery.start([],sources=['wzyp-live'],include_indexes=True)
        self.assertEqual(self.finish(job['job_id'])['status'],'failed')
        failing[0]=False
        self.discovery.resume(job['job_id'])
        result=self.finish(job['job_id'])
        self.assertEqual(result['status'],'completed',result)
        self.assertEqual(result['success_shops'],1)

    def test_public_index_keeps_non_16688_links_only_for_cross_platform_discovery(self):
        from crawler16688 import discover_public,discover_public_links
        version='a'*24;generated='2026-10-23T00:00:00+08:00'
        manifest={'schema':1,'version':version,'generated_at':generated,'offers':{'pages':[f'/data/v/{version}/offers-0.json.gz'],'count':2}}
        document={'version':version,'generated_at':generated,'offers':[{'purchase_url':'https://www.16688.com.cn/shop/S1'},{'purchase_url':'https://next.example/products/product-1'}]}
        with patch('crawler16688.read_index_json',side_effect=[manifest,document]):
            self.assertEqual(len(discover_public(None)),1)
        with patch('crawler16688.read_index_json',side_effect=[manifest,document]):
            self.assertEqual(len(discover_public_links(None)),2)

    def test_selecting_only_new_platforms_still_reads_shared_index(self):
        reads=[]
        def reader(session,progress):reads.append(True);return ['https://next.example/']
        self.discovery.index_readers={'16688':reader}
        job=self.discovery.start([],sources=['dujiao-next'],include_indexes=True)
        self.assertEqual(self.finish(job['job_id'])['success_shops'],1)
        self.assertEqual(reads,[True])


class SafeHTTPTests(unittest.TestCase):
    def test_verified_large_catalog_can_pass_thirty_minutes_but_not_six_hours(self):
        with SafeSession() as session:
            session.started = time.monotonic() - 1801
            with self.assertRaises(requests.RequestException):
                session.check_cancel()
            session.allow_catalog_size(1873)
            session.check_cancel()
            session.allow_catalog_size(20000)
            session.started = time.monotonic() - 6 * 3600 - 1
            with self.assertRaises(requests.RequestException):
                session.check_cancel()
            for total in (-1, True, 20001):
                with self.subTest(total=total), self.assertRaises(ValueError):
                    session.allow_catalog_size(total)

    def test_storefront_captcha_plugin_is_not_mistaken_for_full_page_challenge(self):
        with patch('shop_http.GLOBAL_LIMITER',RateLimiter(0)),patch('shop_http._limiters',{'https://store.example':RateLimiter(0)}):
            with SafeSession(resolver=lambda host:'8.8.8.8',runner=lambda url,**kwargs:response('<title>公开货铺</title><script src="geetest.js"></script>',url)) as session:
                self.assertEqual(session.get('https://store.example/').status_code,200)

    def test_rejects_private_hosts_credentials_ports_and_non_https(self):
        for url in ('http://store.example/','https://localhost/','https://127.0.0.1/','https://169.254.169.254/',
                    'https://[::1]/','https://user:secret@store.example/','https://store.example:8443/','https://store.example\\@127.0.0.1/'):
            with self.subTest(url=url),self.assertRaises(ValueError):normalize_url(url)

    def test_dns_mixed_public_private_answer_is_rejected(self):
        records=[(socket.AF_INET,socket.SOCK_STREAM,6,'',('8.8.8.8',443)),(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443))]
        with patch('shop_http.socket.getaddrinfo',return_value=records),self.assertRaises(ValueError):resolve_public('store.example')

    def test_redirect_to_private_destination_never_transfers(self):
        calls=[]
        def runner(url,**kwargs):
            calls.append(url)
            return CurlResponse(302,b'',{'Location':'https://127.0.0.1/secret'},url)
        with patch('shop_http.GLOBAL_LIMITER',RateLimiter(0)),patch('shop_http._limiters',{}):
            with SafeSession(resolver=lambda host:'8.8.8.8',runner=runner) as session,self.assertRaises(ValueError):session.get('https://store.example/')
        self.assertEqual(calls,['https://store.example/'])

    def test_dns_is_pinned_across_requests_and_post_allowlist_is_enforced(self):
        resolver_calls=[]
        def resolver(host):resolver_calls.append(host);return '8.8.8.8'
        with patch('shop_http.GLOBAL_LIMITER',RateLimiter(0)),patch('shop_http._limiters',{'https://store.example':RateLimiter(0)}):
            with SafeSession(resolver=resolver,runner=lambda url,**kwargs:response({},url)) as session:
                session.get('https://store.example/a');session.get('https://store.example/b')
                with self.assertRaises(ValueError):session.post('https://store.example/order/create',data={})
        self.assertEqual(resolver_calls,['store.example'])

    def test_429_retry_and_cancellation_are_bounded(self):
        responses=[CurlResponse(429,b'',{'Retry-After':'0'}),CurlResponse(200,b'{}')]
        with patch('shop_http.GLOBAL_LIMITER',RateLimiter(0)),patch('shop_http._limiters',{'https://store.example':RateLimiter(0)}):
            with SafeSession(resolver=lambda host:'8.8.8.8',runner=lambda url,**kwargs:responses.pop(0)) as session:
                self.assertEqual(session.get('https://store.example/').status_code,200)
                session.stop.set()
                with self.assertRaises(CrawlCancelled):session.get('https://store.example/')


if __name__=='__main__':unittest.main()
