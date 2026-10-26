"""Default two-stage workflows: directory-only discovery, barriers and durable recovery."""
import json
from pathlib import Path
import sqlite3
import threading
import time
from unittest.mock import patch

from fastapi.testclient import TestClient

from crawler16688 import CrawlManager
from discovery import DiscoveryManager, DiscoveryStore
import local_store
import platformwzyp
from platform_independent import IndependentPlatform
from shop_preview import PreviewStore, ShopPreview
from sources import catfk
import server
from test_multisource import TemporaryCatalog, FakeNative, next_product
from test_stability import wait_until
from test_wzyp_and_llm import FakeWzypSession, make_open_shop


class PreviewNative(FakeNative):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.probes=[]
        self.probe_gate=None

    def probe_shop(self,url,session,resolved=None):
        key=self.normalize_target(url)['target']
        self.probes.append(key)
        while self.probe_gate and not self.probe_gate.wait(.01):
            session.check_cancel()
        state='out_of_stock' if key=='OUT' else 'unknown'
        products=[] if key=='EMPTY' else [{'goods_key':key+'-goods','name':'公开商品','stock_status':state}]
        return ShopPreview(key,key,self.BASE_URL+'/shop/'+key,products,[],len(products),True)


class StagedDiscoveryTests(TemporaryCatalog):
    def setUp(self):
        super().setUp()
        self.adapters={s:PreviewNative(s,local_store.database_path(s),self.web) for s in ('16688','catfk')}
        self.managers={s:CrawlManager(path=a.database_path(),platform_adapter=a) for s,a in self.adapters.items()}
        self.discovery=DiscoveryManager(self.managers,page_session_factory=self.web.session,detector=lambda u,s:None)
        self.gates=[]

    def tearDown(self):
        for gate in self.gates:gate.set()
        self.discovery.shutdown()
        if self.discovery.thread:self.discovery.thread.join(5)
        for manager in self.managers.values():
            manager.shutdown()
            if manager.thread:manager.thread.join(5)
        super().tearDown()

    def finish(self,job,manager=None):
        manager=manager or self.discovery
        wait_until(lambda:manager.store.report(job['job_id'])['status'] not in ('running','cancelling'),10)
        return manager.store.report(job['job_id'])

    def test_default_discovery_waits_for_all_shop_directories_before_any_goods(self):
        gate=threading.Event();self.gates.append(gate);self.adapters['16688'].probe_gate=gate
        job=self.discovery.start(['https://www.16688.com.cn/shop/A','https://catfk.com/shop/B'],
                                 sources=['16688','catfk'],include_indexes=False)
        wait_until(lambda:self.discovery.store.previews.summary(job['job_id'])['discovered_shops']==1)
        self.assertEqual(self.discovery.store.report(job['job_id'])['phase'],'shop_discovery')
        self.assertEqual(self.adapters['catfk'].calls,[])
        self.assertEqual(self.adapters['16688'].calls,[])
        gate.set();result=self.finish(job)
        self.assertEqual((result['phase'],result['success_shops'],result['collected_goods']),('goods_collection',2,2))
        self.assertEqual(result['pending_shops'],0)

    def test_stale_heartbeat_does_not_recover_a_live_discovery_worker(self):
        gate=threading.Event();self.gates.append(gate);self.adapters['catfk'].probe_gate=gate
        job=self.discovery.start(['https://catfk.com/shop/A'],sources=['catfk'],include_indexes=False)
        wait_until(lambda:bool(self.adapters['catfk'].probes))
        with self.discovery.store.connection(write=True) as conn:
            conn.execute('UPDATE discovery_jobs SET heartbeat=0 WHERE job_id=?',(job['job_id'],))
        self.discovery.recover_interrupted()
        self.assertEqual(self.discovery.store.report(job['job_id'])['status'],'running')
        self.discovery.cancel(job['job_id']);gate.set();self.finish(job)

    def test_live_worker_cannot_be_restarted_even_if_persisted_state_was_wrong(self):
        from crawl_store import JobConflict
        gate=threading.Event();self.gates.append(gate);self.adapters['catfk'].probe_gate=gate
        job=self.discovery.start(['https://catfk.com/shop/A'],sources=['catfk'],include_indexes=False)
        wait_until(lambda:bool(self.adapters['catfk'].probes))
        with self.discovery.store.connection(write=True) as conn:
            conn.execute("UPDATE discovery_jobs SET status='interrupted',heartbeat=0 WHERE job_id=?",(job['job_id'],))
        with self.assertRaises(JobConflict):self.discovery.resume(job['job_id'])
        with self.assertRaises(JobConflict):self.discovery.start(['https://catfk.com/shop/B'],sources=['catfk'],include_indexes=False)
        self.discovery.cancel(job['job_id']);gate.set();self.finish(job)

    def test_live_platform_worker_is_not_interrupted_by_stale_heartbeat(self):
        gate=threading.Event();self.gates.append(gate);self.adapters['catfk'].gate=gate
        manager=self.managers['catfk'];job=manager.start('targets',['https://catfk.com/shop/A'])
        wait_until(lambda:bool(manager.store.rows("SELECT 1 FROM crawl_visits WHERE job_id=? AND status='working'",(job['job_id'],))))
        with manager.store.transaction() as conn:
            conn.execute('UPDATE crawl_jobs SET heartbeat=0 WHERE job_id=?',(job['job_id'],))
        manager.recover_interrupted()
        self.assertEqual(manager.store.report(job['job_id'])['status'],'running')
        manager.cancel(job['job_id']);gate.set();self.finish(job,manager)

    def test_live_platform_worker_rejects_resume_after_external_state_change(self):
        from crawl_store import JobConflict
        gate=threading.Event();self.gates.append(gate);self.adapters['catfk'].gate=gate
        manager=self.managers['catfk'];job=manager.start('targets',['https://catfk.com/shop/A'])
        wait_until(lambda:bool(manager.store.rows("SELECT 1 FROM crawl_visits WHERE job_id=? AND status='working'",(job['job_id'],))))
        with manager.store.transaction() as conn:
            conn.execute("UPDATE crawl_jobs SET status='interrupted',heartbeat=0 WHERE job_id=?",(job['job_id'],))
        with self.assertRaises(JobConflict):manager.resume(job['job_id'])
        with self.assertRaises(JobConflict):manager.start('targets',['https://catfk.com/shop/B'])
        manager.cancel(job['job_id']);gate.set();self.finish(job,manager)

    def test_unknown_stock_is_retained_but_empty_and_confirmed_sold_out_are_skipped(self):
        job=self.discovery.start(['https://catfk.com/shop/'+x for x in ('UNKNOWN','EMPTY','OUT')],
                                 sources=['catfk'],include_indexes=False)
        result=self.finish(job)
        self.assertEqual((result['status'],result['discovered_shops'],result['success_shops']),('completed',3,1))
        self.assertEqual(self.adapters['catfk'].calls,['UNKNOWN'])
        self.assertEqual(result['unverified_stock_shops'],0)
        self.assertEqual(self.discovery.store.previews.get(job['job_id'],'catfk','UNKNOWN')['stock_status'],'unknown')
        self.assertEqual(set(result['counts']),{'collected','empty','no_stock'})

    def test_cancel_first_stage_never_collects_and_resume_keeps_saved_directories(self):
        gate=threading.Event();self.gates.append(gate);self.adapters['16688'].probe_gate=gate
        job=self.discovery.start(['https://catfk.com/shop/A','https://www.16688.com.cn/shop/B'],
                                 sources=['catfk','16688'],include_indexes=False)
        wait_until(lambda:self.discovery.store.previews.summary(job['job_id'])['discovered_shops']==1)
        self.discovery.cancel(job['job_id']);result=self.finish(job)
        self.assertEqual((result['status'],result['phase'],result['collected_goods']),('cancelled','shop_discovery',0))
        self.assertEqual(self.adapters['catfk'].calls,[])
        gate.set();resumed=self.discovery.resume(job['job_id']);self.assertEqual(self.finish(resumed)['success_shops'],2)
        self.assertEqual(self.adapters['catfk'].probes,['A'])

    def test_cancel_goods_stage_resumes_without_repeating_shop_search(self):
        gate=threading.Event();self.gates.append(gate);self.adapters['catfk'].gate=gate
        job=self.discovery.start(['https://catfk.com/shop/A'],sources=['catfk'],include_indexes=False)
        wait_until(lambda:self.discovery.store.report(job['job_id'])['counts'].get('collecting')==1)
        self.discovery.cancel(job['job_id']);self.assertEqual(self.finish(job)['phase'],'goods_collection')
        probes=self.adapters['catfk'].probes[:]
        gate.set();self.assertEqual(self.finish(self.discovery.resume(job['job_id']))['success_shops'],1)
        self.assertEqual(self.adapters['catfk'].probes,probes)

    def test_single_platform_discover_also_has_a_barrier_and_persistent_previews(self):
        gate=threading.Event();entered=threading.Event();self.gates.append(gate)
        def index(session,progress):
            entered.set()
            gate.wait(5)
            return []
        manager=self.managers['catfk'];manager.discover=index;manager.seed_urls=lambda:[]
        job=manager.start('discover',['https://catfk.com/shop/A'])
        self.assertTrue(entered.wait(2))
        wait_until(lambda:PreviewStore().summary(job['job_id'])['discovered_shops']==1)
        self.assertEqual(manager.store.report(job['job_id'])['success_shops'],0)
        self.assertEqual(self.adapters['catfk'].calls,[])
        gate.set();result=self.finish(job,manager)
        self.assertEqual((result['workflow_version'],result['success_shops'],result['pending_shops']),(2,1,0))

    def test_partial_discovery_still_collects_successful_directories(self):
        self.web.pages['https://directory.example/']='<a href="https://catfk.com/shop/A">店铺</a><a href="https://missing.example/">失败</a>'
        job=self.discovery.start(['https://directory.example/'],sources=['catfk'],include_indexes=False)
        result=self.finish(job)
        self.assertEqual((result['status'],result['success_shops']),('partial',1))

    def test_platform_resume_retries_failed_probe_before_collecting_it(self):
        adapter=self.adapters['catfk'];probe=adapter.probe_shop;fail=[True]
        def flaky(url,session,resolved=None):
            if url.endswith('/B') and fail[0]:raise adapter.PlatformError('temporary directory failure')
            return probe(url,session,resolved)
        adapter.probe_shop=flaky
        manager=self.managers['catfk'];manager.discover=lambda *args:[]
        job=manager.start('discover',['https://catfk.com/shop/A','https://catfk.com/shop/B'])
        self.assertEqual(self.finish(job,manager)['status'],'partial')
        self.assertEqual(adapter.calls,['A'])
        fail[0]=False
        result=self.finish(manager.resume(job['job_id']),manager)
        self.assertEqual((result['status'],result['success_shops']),('completed',2))
        self.assertEqual(adapter.calls,['A','B'])
        self.assertIn('B',adapter.probes)

    def test_restarted_coordinator_keeps_qualified_shops_and_recovers_unfinished_probes(self):
        store=self.discovery.store
        job_id=store.create(['https://catfk.com/shop/A','https://catfk.com/shop/B'],['catfk'],False,workflow_version=2)
        rows=store.candidates(job_id)['rows']
        preview=ShopPreview('A','A','https://catfk.com/shop/A',[{'name':'公开商品','stock_status':'unknown'}],[],1,True)
        store.previews.save(job_id,'catfk',preview)
        store.update_candidate(rows[0]['id'],'qualified',source='catfk',shop_key='A')
        store.update_candidate(rows[1]['id'],'probing_shop',source='catfk')
        with store.connection() as conn:
            conn.execute('UPDATE discovery_jobs SET initialized=1,owner_pid=2147483647,heartbeat=0 WHERE job_id=?',(job_id,))
        replacement=DiscoveryManager(self.managers,page_session_factory=self.web.session)
        self.discovery=replacement
        replacement.recover_interrupted()
        self.assertEqual(store.report(job_id)['status'],'interrupted')
        result=self.finish(replacement.resume(job_id))
        self.assertEqual(result['success_shops'],2)
        self.assertEqual(self.adapters['catfk'].probes,['B'])

    def test_platform_recovers_crash_between_preview_save_and_queue_checkpoint(self):
        manager=self.managers['catfk'];store=manager.store
        key,_=store.create('discover',['https://catfk.com/shop/A'],workflow_version=2)
        store.discovery_finished(key,[])
        item=store.claim_item(key)
        PreviewStore().save(key,'catfk',ShopPreview('A','A','https://catfk.com/shop/A',[{'name':'商品'}],[],1,True))
        store.remember(item,{'shop_no':'A'})
        with store.transaction() as conn:
            conn.execute('UPDATE crawl_jobs SET owner_pid=2147483647,heartbeat=0 WHERE job_id=?',(key,))
        replacement=CrawlManager(path=store.path,platform_adapter=self.adapters['catfk'],discover=lambda *args:[])
        self.managers['catfk']=replacement
        replacement.recover_interrupted()
        result=self.finish(replacement.resume(key),replacement)
        self.assertEqual((result['status'],result['success_shops'],result['pending_shops']),('completed',1,0))
        self.assertEqual(self.adapters['catfk'].calls,['A'])
        self.assertEqual(self.adapters['catfk'].probes,[])

    def test_goods_description_links_are_saved_for_next_round(self):
        original=self.adapters['catfk'].sync_shop
        def sync(*args,**kwargs):
            result=original(*args,**kwargs)
            result['discovered_urls']=['https://catfk.com/shop/NEXT']
            return result
        self.adapters['catfk'].sync_shop=sync
        self.managers['catfk'].discover=lambda *args:[]
        job=self.managers['catfk'].start('discover',['https://catfk.com/shop/A'])
        result=self.finish(job,self.managers['catfk'])
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(self.adapters['catfk'].calls,['A'])
        self.assertIn('https://catfk.com/shop/NEXT',self.managers['catfk'].store.known_urls(True))

    def test_preview_api_paginates_and_does_not_affect_goods_stats(self):
        store=PreviewStore();store.save('fixture','catfk',ShopPreview('A','目录店铺','https://catfk.com/shop/A',
            [{'name':'公开商品','stock_status':'unknown'}],[],1,True))
        client=TestClient(server.app)
        data=client.get('/api/discovery/shops',params={'job_id':'fixture','source':'catfk','pageSize':1}).json()
        self.assertEqual(data['total'],1)
        self.assertEqual(data['rows'][0]['preview']['products'][0]['name'],'公开商品')
        counts=client.get('/api/sources/summary').json()['sources']
        self.assertEqual(next(x for x in counts if x['id']=='catfk')['goods_count'],0)
        self.assertEqual(client.get('/api/discovery/shops',params={'job_id':'fixture','page':0}).status_code,422)


    def test_saved_first_party_product_links_skip_repeated_goods_entry_requests(self):
        adapter=self.adapters['catfk'];original=adapter.probe_shop
        def probe(url,session,resolved=None):
            result=original(url,session,resolved)
            result.products=[{'name':'公开商品','goods_key':'G1','link':'https://catfk.com/item/G1','stock_status':'unknown'}]
            return result
        adapter.probe_shop=probe
        job=self.discovery.start(['https://catfk.com/shop/A','https://catfk.com/item/G1'],sources=['catfk'],include_indexes=False)
        result=self.finish(job)
        self.assertEqual(result['success_shops'],1)
        self.assertEqual(adapter.probes,['A'])
        self.assertEqual(adapter.calls,['A'])

    def test_heartbeat_thread_survives_a_transient_busy_error(self):
        gate=threading.Event();self.gates.append(gate);attempts=[]
        def index(*args):
            gate.wait(5)
            return []
        self.discovery.index_readers={'16688':index}
        original=self.discovery.store.heartbeat
        def heartbeat(job):
            attempts.append(time.monotonic())
            if len(attempts)==1:raise sqlite3.OperationalError('database is locked')
            return original(job)
        with patch.object(self.discovery.store,'heartbeat',heartbeat):
            job=self.discovery.start(['https://catfk.com/shop/A'],sources=['catfk'],include_indexes=True)
            end=time.monotonic()+4
            while len(attempts)<2 and time.monotonic()<end:time.sleep(.02)
            self.assertGreaterEqual(len(attempts),2)
            self.assertFalse(self.discovery.stop.is_set())
            gate.set()
            self.assertEqual(self.finish(job)['success_shops'],1)


class PreviewAdapterTests(TemporaryCatalog):
    def test_16688_directory_does_not_expand_linked_product_details(self):
        import platform16688
        from test_crawler16688 import NetworkModel
        network=NetworkModel()
        with network.factory(threading.Event()) as session:
            preview=platform16688.probe_shop(platform16688.BASE_URL+'/shop/ALPHA',session).normalized()
        self.assertEqual(preview['eligibility'],'eligible')
        self.assertEqual(preview['reported_total'],1)
        self.assertEqual(sum(n for key,n in network.calls.items() if key[0]=='goods/detail'),0)

    def test_choice_reads_public_category_lists_without_product_details(self):
        origin='https://choice.example'
        self.web.pages[origin+'/']='<title>Choice</title><div class="layui-container"><select id="productlist" name="pid"></select><input name="_token" value="anonymous"></div><script>"/api/typelist";"/api/proudctinfo";</script>'
        self.web.pages[origin+'/api/typelist']=json.dumps({'code':1,'data':{'typelist':[{'id':1,'name':'公开分类','password':''},{'id':2,'name':'加密分类','password':'hidden'}]}})
        with self.web.session() as session:
            preview=self.classic.probe_shop(origin+'/',session).normalized()
        self.assertEqual(preview['eligibility'],'eligible')
        self.assertEqual([x['name'] for x in preview['categories']],['公开分类'])
        self.assertFalse(any(x[1].endswith('/api/proudctinfo') for x in self.web.calls))

    def test_hosted_sources_only_read_first_pages_and_never_goods_details(self):
        rows=[{'goods_key':f'G{i}','name':f'商品{i}','price':i} for i in range(20)]
        shop=make_open_shop('A',goods_pages={'card':{1:{'total':25,'list':rows}}})
        for adapter in (platformwzyp,catfk):
            session=FakeWzypSession({'A':shop})
            post=session.post
            def limited(url,**kwargs):
                self.assertFalse(url.endswith('goodsInfo'))
                self.assertNotEqual((kwargs.get('json') or {}).get('current'),2)
                return post(url,**kwargs)
            session.post=limited
            preview=adapter.probe_shop(adapter.BASE_URL+'/shop/A',session).normalized()
            self.assertEqual((len(preview['products']),preview['reported_total'],preview['complete']),(20,25,False))
            self.assertEqual(preview['eligibility'],'eligible')

    def test_next_reuses_initial_directory_and_does_not_read_product_detail(self):
        with self.web.session() as session:
            preview=self.next.probe_shop('https://next.example/',session).normalized()
        self.assertEqual(preview['eligibility'],'eligible')
        self.assertFalse(any(call[1].endswith('/products/product-1') for call in self.web.calls))

    def test_classic_reads_homepage_without_following_pagination_or_goods_links(self):
        self.web.pages['https://classic.example/']='<div class="am-container"><a href="/buy/7">公开商品</a><a rel="next" href="/?page=2">下一页</a></div>'
        with self.web.session() as session:
            preview=self.classic.probe_shop('https://classic.example/',session).normalized()
        self.assertEqual((preview['eligibility'],preview['complete']),('eligible',False))
        self.assertEqual(len(self.web.calls),1)

    def test_preview_limit_does_not_invent_category_counts_or_stock(self):
        preview=ShopPreview('A','店铺','https://fixture.example/',[{'name':str(i)} for i in range(150)],
                            [{'name':'公开分类','count':None}],150,True).normalized()
        self.assertEqual(len(preview['products']),100)
        self.assertFalse(preview['complete'])
        self.assertEqual(preview['stock_status'],'unknown')
        self.assertIsNone(preview['categories'][0]['count'])

    def test_category_only_count_can_retain_a_shop_without_claiming_stock(self):
        preview=ShopPreview('A','店铺','https://fixture.example/',[],[{'name':'会员','count':3}],None,False).normalized()
        self.assertEqual((preview['eligibility'],preview['stock_status']),('eligible','unknown'))
        self.assertIsNone(preview['reported_total'])

    def test_schema_initializes_once_and_reinitializes_replaced_files(self):
        path=self.root/'identity.sqlite3'
        with patch.object(local_store,'_ensure_incremental_schema',wraps=local_store._ensure_incremental_schema) as migrate:
            local_store.connect(str(path)).close();local_store.connect(str(path)).close()
            self.assertEqual(migrate.call_count,1)
            path.unlink()
            local_store.connect(str(path)).close()
            self.assertEqual(migrate.call_count,2)

    def test_old_stored_discovery_defaults_to_original_workflow(self):
        store=DiscoveryStore(str(self.root/'legacy.sqlite3'))
        key=store.create([],['catfk'],False)
        self.assertEqual(store.report(key)['workflow_version'],1)
