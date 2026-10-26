"""公开索引发现 + 原站多店采集。后台任务可取消、持久化和续跑。"""
import gzip
import io
import json
import re
import sqlite3
from sqlite_init import is_busy
from task_lifecycle import run_worker
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime

import requests

import platform16688 as platform
from crawl_store import ACTIVE, CrawlStore
from public_http import CrawlBlockedError, CrawlCancelled, PublicSession
from crawl_failure import from_exception

INDEX_ORIGIN = "https://pricemapai.com"
INDEX_MANIFEST = f"{INDEX_ORIGIN}/data/latest.json"
MAX_DOCUMENT = 16 * 1024 * 1024


def read_index_json(session, url):
    try:
        response = session.get(url, timeout=(8, 30), allow_redirects=False)
        response.raise_for_status()
        if response.is_redirect or len(response.content) > MAX_DOCUMENT:
            raise platform.PlatformError("公开索引响应跳转或体积异常")
        raw = response.content
        if raw.startswith(b"\x1f\x8b"):
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as compressed:
                raw = compressed.read(MAX_DOCUMENT + 1)
        if len(raw) > MAX_DOCUMENT:
            raise platform.PlatformError("公开索引解压后超过大小限制")
        return json.loads(raw)
    except (requests.RequestException, ValueError, OSError, EOFError) as exc:
        raise platform.PlatformError("公开索引暂时不可读取，已保留现有快照") from exc


def discover_public(session, progress=None, all_platforms=False):
    """版本不一致时从新清单重试一次；完整校验前不提交发现的链接。"""
    last_error = None
    for _ in range(2):
        try:
            manifest = read_index_json(session, INDEX_MANIFEST)
            if not isinstance(manifest, dict) or manifest.get("schema") != 1:
                raise platform.PlatformError("公开索引清单格式异常")
            version = manifest.get("version", "")
            if not isinstance(version, str) or not re.fullmatch(r"[a-f0-9]{24}", version):
                raise platform.PlatformError("公开索引版本格式异常")
            generated_at = manifest.get("generated_at", "")
            datetime.fromisoformat(generated_at)
            offers = manifest.get("offers") or {}
            if not isinstance(offers, dict):
                raise platform.PlatformError("公开索引分页格式异常")
            pages, count = offers.get("pages"), offers.get("count")
            if (not isinstance(count, int) or isinstance(count, bool) or not 0 <= count <= 100000
                    or not isinstance(pages, list) or not 1 <= len(pages) <= 1000
                    or pages != [f"/data/v/{version}/offers-{i}.json.gz" for i in range(len(pages))]):
                raise platform.PlatformError("公开索引分页格式异常")
            urls, seen = set(), 0
            if progress:
                progress(version, 0)
            for index, page in enumerate(pages):
                if hasattr(session, "check_cancel"):
                    session.check_cancel()
                document = read_index_json(session, INDEX_ORIGIN + page)
                if (not isinstance(document, dict) or document.get("version") != version
                        or document.get("generated_at") != generated_at or not isinstance(document.get("offers"), list)):
                    raise platform.PlatformError("公开索引分页版本不一致")
                seen += len(document["offers"])
                for offer in document["offers"]:
                    if not isinstance(offer, dict):
                        raise platform.PlatformError("公开索引商品格式异常")
                    source = offer.get("sources") or {}
                    base_url = source.get("base_url") if isinstance(source, dict) else None
                    for url in (base_url, offer.get("purchase_url")):
                        if isinstance(url, str):
                            try:
                                if all_platforms:
                                    from shop_http import normalize_url
                                    urls.add(normalize_url(url))
                                else:
                                    urls.add(platform.normalize_target(url)["url"])
                            except ValueError:
                                pass
                if progress:
                    progress(version, index + 1)
            if seen != count:
                raise platform.PlatformError("公开索引分页数量不完整")
            return sorted(urls)
        except (platform.PlatformError, ValueError, TypeError) as exc:
            last_error = str(exc)
    raise platform.PlatformError(last_error or "公开索引发现失败")


def discover_public_links(session, progress=None):
    """共享清单完整性校验，跨平台任务只提取链接，不导入第三方报价。"""
    return discover_public(session, progress, all_platforms=True)


class CrawlManager:
    def __init__(self, path=None, session_factory=None, discover=None, platform_adapter=None, workflow_version=2):
        self.platform = platform_adapter or platform
        self.workflow_version = workflow_version
        self.store = CrawlStore(path, platform_adapter=self.platform)
        if session_factory is not None:
            self.session_factory = session_factory
        elif hasattr(self.platform, "session_factory"):
            self.session_factory = self.platform.session_factory
        else:
            self.session_factory = lambda stop: PublicSession(stop=stop)
        self.index_session_factory = (self.session_factory if session_factory is not None else
                                      getattr(self.platform, "index_session_factory", self.session_factory))
        if discover is not None:
            self.discover = discover
        elif hasattr(self.platform, "discover_public"):
            self.discover = self.platform.discover_public
        else:
            self.discover = discover_public
        self.seed_urls = getattr(self.platform, "seed_urls", lambda: []) if discover is None else lambda: []
        self.lock = threading.Lock()
        self.thread = None
        self.job_id = None
        self.stop = None
        self.shutting_down = None
        self.block_error = None
        self.block_failure = None

    def recover_interrupted(self):
        self.store.recover()

    def _launch(self, job_id, transport_scope=None):
        self.job_id = job_id
        self.stop = threading.Event()
        from hosted_http import TransportScope
        self.stop.transport_scope = transport_scope or TransportScope()
        self.stop.owns_transport_scope = transport_scope is None
        self.shutting_down = threading.Event()
        self.block_error = None
        self.block_failure = None
        prefix = getattr(self.platform, "SOURCE_KEY", "16688")
        self.thread = threading.Thread(target=run_worker,args=('crawl',self.store.path,job_id,self._run,(job_id,self.stop,self.shutting_down)),
                                       name=f"{prefix}-crawl", daemon=True)
        try:
            self.thread.start()
        except Exception:
            self.store.finish_job(job_id, "failed", "无法启动采集线程")
            raise

    def start(self, mode="discover", urls=None, reuse=True, discovery_owner=None, transport_scope=None):
        if mode not in {"discover", "refresh", "targets"}:
            raise ValueError("mode 应为 discover、refresh 或 targets")
        urls = [self.platform.normalize_target(url)["url"] for url in (urls or [])]
        with self.lock:
            self.recover_interrupted()
            job_id, reused = self.store.create(mode, urls, reuse=reuse, discovery_owner=discovery_owner,
                                               workflow_version=self.workflow_version)
            if not reused:
                self._launch(job_id, transport_scope)
        return {**self.store.report(job_id), "reused": reused}

    def resume(self, job_id, transport_scope=None, retry_mode="transient"):
        with self.lock:
            self.recover_interrupted()
            reused = self.store.resume(job_id, retry_mode)
            if not reused:
                self._launch(job_id, transport_scope)
        return {**self.store.report(job_id), "reused": reused}

    def cancel(self, job_id, interrupt=False):
        with self.lock:
            self.store.cancel(job_id)
            if job_id == self.job_id and self.stop:
                if interrupt:
                    self.shutting_down.set()
                self.stop.set()
        return self.store.report(job_id)

    def wait_for_job(self, job_id):
        with self.lock:
            thread = self.thread if self.job_id == job_id else None
        if thread:
            thread.join()

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self.shutting_down.set()
            self.stop.set()
            self.thread.join(timeout=2)

    def shop_entry_url(self,key,url):
        if not key:return url
        if hasattr(self.platform,'shop_url'):return self.platform.shop_url(key,url)
        return f'{self.platform.BASE_URL}/shop/{key}'

    def _probe_process(self, item, stop):
        from shop_preview import PreviewStore
        from sources import canonical_source
        source=canonical_source(getattr(self.platform,'SOURCE_NAME',self.platform.SOURCE_KEY))
        previews=PreviewStore()
        try:
            if stop.is_set(): raise CrawlCancelled()
            key=self.store.lookup_shop(item['kind'],item['target'])
            previous=previews.get(item['job_id'],source,key) if key else None
            if previous:
                self.store.finish_preview(item,key,previous['eligibility']=='eligible' and previous['collection_state']!='collected')
                return
            with self.session_factory(stop) as session:
                preview=self.platform.probe_shop(self.shop_entry_url(key,item['url']),session)
            if stop.is_set(): raise CrawlCancelled()
            data=preview.normalized()
            previews.save(item['job_id'],source,data)
            self.store.remember(item,{'shop_no':data['shop_key']},goods=data['products'])
            self.store.finish_preview(item,data['shop_key'],data['eligibility']=='eligible')
        except CrawlCancelled:
            self.store.finish_item(item,'pending')
        except Exception as exc:
            self._record_failure(item,None,exc,stop)

    def _process(self, item, stop):
        shop_no, claimed = None, False
        try:
            if stop.is_set():
                raise CrawlCancelled()
            with self.session_factory(stop) as session:
                shop_no = self.store.lookup_shop(item["kind"], item["target"])
                if shop_no:
                    claimed = self.store.claim_shop(item["job_id"], shop_no)
                    if not claimed:
                        self.store.finish_item(item, "skipped", shop_no)
                        return
                url = self.shop_entry_url(shop_no,item['url'])
                resolved = self.platform.resolve_target(url, session)
                shop = resolved[0]
                if shop_no and shop_no != shop["shop_no"]:
                    raise self.platform.PlatformError("店铺编号与已核验入口不一致")
                shop_no = shop["shop_no"]
                self.store.remember(item, shop)
                if not claimed:
                    claimed = self.store.claim_shop(item["job_id"], shop_no)
                    if not claimed:
                        self.store.finish_item(item, "skipped", shop_no)
                        return
                result = self.platform.sync_shop(url, session=session, resolved=resolved, store_path=self.store.path)
                self.store.finish_item(item, "done", shop_no, result=result)
                if self.store.rows('SELECT workflow_version FROM crawl_jobs WHERE job_id=?',(item['job_id'],))[0]['workflow_version']==2:
                    from shop_preview import PreviewStore
                    from sources import canonical_source
                    source=canonical_source(getattr(self.platform,'SOURCE_NAME',self.platform.SOURCE_KEY))
                    PreviewStore().collection(item['job_id'],source,shop_no,'collected')
        except CrawlBlockedError as exc:
            self._record_failure(item, shop_no if claimed else None, exc, stop)
        except CrawlCancelled:
            self.store.finish_item(item, "pending", shop_no if claimed else None)
        except Exception as exc:
            self._record_failure(item, shop_no if claimed else None, exc, stop)

    def _record_failure(self, item, shop_no, exc, stop):
        failure = from_exception(exc)
        if failure["pause_origin"] and not getattr(self.platform, "MULTI_ORIGIN", False):
            self.block_error, self.block_failure = str(exc)[:400], failure
            if self.shutting_down:
                self.shutting_down.set()
            stop.set()
            self.store.finish_item(item, "pending", shop_no, failure=failure)
        else:
            self.store.finish_item(item, "failed", shop_no, str(exc)[:400], failure=failure)

    def _read_index(self, job_id, stop):
        def progress(version, pages):
            if not stop.is_set():
                self.store.discovery_progress(job_id, version, pages)
        with self.index_session_factory(stop) as session:
            return self.discover(session, progress)

    def _run(self, job_id, stop, shutting_down):
        pulse_done = threading.Event()
        prefix = getattr(self.platform, "SOURCE_KEY", "16688")

        def pulse():
            while not pulse_done.wait(1):
                try:
                    if self.store.heartbeat(job_id):stop.set()
                except sqlite3.OperationalError as error:
                    if not is_busy(error):raise

        heartbeat_thread = threading.Thread(target=pulse, daemon=True, name=f"{prefix}-heartbeat")
        heartbeat_thread.start()
        terminal, error, failure = "failed", None, None
        try:
            job = self.store.report(job_id)
            staged=job.get('workflow_version')==2
            discovering=staged and job['phase']=='shop_discovery'
            if job["mode"] != "targets" and (not staged or discovering):
                self.store.enqueue(job_id, self.store.known_urls(job["mode"] == "discover"), "known")
                if job["mode"] == "discover":
                    self.store.enqueue(job_id, self.seed_urls(), "local-seeds")
                    if staged:
                        from shop_preview import PreviewStore
                        from sources import canonical_source
                        source=canonical_source(getattr(self.platform,'SOURCE_NAME',self.platform.SOURCE_KEY))
                        self.store.enqueue(job_id,PreviewStore().known_urls(source),'saved-directory')
            if job["mode"] in {"refresh", "targets"}:
                self.store.discovery_finished(job_id, [])
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"{prefix}-index") as indexes, \
                    ThreadPoolExecutor(max_workers=2, thread_name_prefix=f"{prefix}-shop") as pool:
                index = (indexes.submit(self._read_index, job_id, stop)
                         if job["mode"] == "discover" and not job["discovery_done"] and (not staged or discovering) else None)
                futures = set()
                while True:
                    if self.store.heartbeat(job_id):
                        stop.set()
                    if index is not None and index.done():
                        try:
                            urls = index.result()
                            if not stop.is_set():
                                self.store.discovery_finished(job_id, urls)
                        except CrawlCancelled:
                            if not stop.is_set():
                                raise
                        except Exception as exc:
                            if not stop.is_set():
                                self.store.discovery_finished(job_id, error=str(exc)[:400])
                        index = None
                    while not stop.is_set() and len(futures) < 2:
                        item = self.store.claim_item(job_id)
                        if item is None:
                            break
                        futures.add(pool.submit(self._probe_process if discovering else self._process, item, stop))
                    if not futures and (index is None or stop.is_set()):
                        if discovering and not stop.is_set():
                            with self.store.transaction() as conn:
                                cancelled=conn.execute('SELECT cancel_requested FROM crawl_jobs WHERE job_id=?',(job_id,)).fetchone()[0]
                                if cancelled:
                                    stop.set()
                                else:
                                    conn.execute("UPDATE crawl_jobs SET phase='goods_collection' WHERE job_id=?",(job_id,))
                                    conn.execute("UPDATE crawl_items SET status='pending' WHERE job_id=? AND status='previewed'",(job_id,))
                            if not stop.is_set():
                                discovering=False
                                continue
                        break
                    if futures:
                        done, futures = wait(futures, timeout=0.2, return_when=FIRST_COMPLETED)
                        for future in done:
                            future.result()
                    else:
                        stop.wait(0.1)
            report = self.store.report(job_id)
            if self.block_error:
                terminal, error, failure = "interrupted", self.block_error, self.block_failure
            elif stop.is_set():
                terminal = "interrupted" if shutting_down.is_set() else "cancelled"
            elif report["failure_count"] or report["discovery_error"]:
                terminal = "partial" if report["success_shops"] else "failed"
            else:
                terminal = "completed"
        except CrawlBlockedError as exc:
            terminal, error, failure = "interrupted", str(exc)[:400], exc.failure
        except CrawlCancelled:
            terminal = "interrupted" if shutting_down.is_set() else "cancelled"
        except Exception as exc:
            terminal, error, failure = "failed", str(exc)[:400], from_exception(exc)
        finally:
            stop.set()
            pulse_done.set()
            heartbeat_thread.join(timeout=2)
            if getattr(stop, "owns_transport_scope", False):
                stop.transport_scope.close()
            self.store.finish_job(job_id, terminal, error, failure=failure)


def run_cli(mode, urls, resume, platform_adapter=None):
    manager = CrawlManager(platform_adapter=platform_adapter)
    try:
        report = manager.resume(resume) if resume else manager.start(mode, urls)
        job_id = report["job_id"]
        previous = None
        while True:
            report = manager.store.report(job_id)
            summary = {key: report[key] for key in ("job_id", "status", "phase", "discovered_links", "resolved_shops",
                                                    "success_shops", "failure_count", "collected_goods", "pending_links")}
            if summary != previous:
                print(json.dumps(summary, ensure_ascii=False), flush=True)
                previous = summary
            if report["status"] not in ACTIVE:
                if report["last_error"]:
                    print(report["last_error"], flush=True)
                return 0 if report["status"] == "completed" else 1
            threading.Event().wait(2)
    except KeyboardInterrupt:
        if manager.job_id:
            manager.cancel(manager.job_id)
            if manager.thread:
                manager.thread.join(timeout=40)
        return 130
    except Exception as exc:
        print(f"采集失败：{exc}", flush=True)
        return 1
