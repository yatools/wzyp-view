"""Real SQLite contention, large discovery queues and safe reuse of first-party identities."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading
import time
from unittest.mock import patch

from discovery import DiscoveryStore
from shop_preview import ShopPreview
from test_multisource import TemporaryCatalog


class DatabaseContentionTests(TemporaryCatalog):
    def setUp(self):
        super().setUp()
        self.store=DiscoveryStore(str(self.root/'contention.sqlite3'))
        self.job=self.store.create(['https://catfk.com/shop/A'],['catfk'],False)
        self.store.previews.summary(self.job)
        self.connect=sqlite3.connect

    def fast_connect(self,*args,**kwargs):
        kwargs['timeout']=.001
        return self.connect(*args,**kwargs)

    def test_short_external_writer_lock_is_retried_without_losing_heartbeat(self):
        blocker=self.connect(self.store.path)
        try:
            blocker.execute('BEGIN IMMEDIATE')
            with patch.object(sqlite3,'connect',self.fast_connect),ThreadPoolExecutor(1) as pool:
                future=pool.submit(self.store.heartbeat,self.job)
                time.sleep(.06)
                self.assertFalse(future.done())
                blocker.rollback()
                self.assertFalse(future.result(2))
        finally:blocker.close()

    def test_bulk_enqueue_heartbeats_previews_and_queries_can_run_together(self):
        self.store.enqueue(self.job,[f'https://catfk.com/shop/S{i}' for i in range(2400)])
        def enqueue():
            self.store.enqueue(self.job,[f'https://catfk.com/shop/N{i}' for i in range(2000)],'index')
        def heartbeat():
            for _ in range(60):self.assertFalse(self.store.heartbeat(self.job))
        def previews():
            for i in range(60):self.store.previews.save(self.job,'catfk',ShopPreview(str(i),str(i),'https://catfk.com/shop/'+str(i)))
        def claim():
            return [self.store.claim_candidate(self.job)['id'] for _ in range(60)]
        def read():
            for _ in range(20):self.assertIsNotNone(self.store.report(self.job))
        with patch.object(sqlite3,'connect',self.fast_connect),ThreadPoolExecutor(6) as pool:
            futures=[pool.submit(fn) for fn in (enqueue,heartbeat,previews,claim,claim,read)]
            results=[future.result(20) for future in futures]
        self.assertEqual(len(set(results[3]+results[4])),120)
        self.assertEqual(self.store.candidates(self.job,size=1)['total'],4401)

    def test_grouped_queue_keeps_depth_and_fresh_origin_priority(self):
        store=self.store
        store.enqueue(self.job,['https://seen.example/first','https://seen.example/second','https://new.example/'])
        rows=store.candidates(self.job)['rows']
        first=next(row for row in rows if row['url']=='https://seen.example/first')
        store.update_candidate(first['id'],'directory')
        store.update_candidate(rows[0]['id'],'collected')
        self.assertEqual(store.next_candidate(self.job)['url'],'https://new.example/')
        self.assertEqual(store.claim_candidate(self.job,excluded={'https://new.example'})['url'],'https://seen.example/second')

    def test_duplicate_cleanup_requires_an_existing_owner(self):
        row=self.store.candidates(self.job)['rows'][0]
        self.assertEqual(self.store.skip_saved_urls(self.job,'catfk','A',[row['url']]),0)
        self.assertEqual(self.store.candidate(self.job,row['id'])['state'],'pending')
        self.store.update_candidate(row['id'],'qualified',source='catfk',shop_key='A')
        self.store.enqueue(self.job,['https://catfk.com/item/G1'])
        self.assertEqual(self.store.skip_saved_urls(self.job,'catfk','A',['https://catfk.com/item/G1']),1)
        self.assertEqual(self.store.candidate(self.job,row['id'])['state'],'qualified')
