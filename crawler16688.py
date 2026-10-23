"""公开索引发现 + 原站多店采集。后台任务可取消、持久化和续跑。"""
import gzip
import io
import json
import re
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime

import requests

import platform16688 as platform
from crawl_store import ACTIVE, CrawlStore
from public_http import CrawlCancelled, PublicSession

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


def discover_public(session, progress=None):
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


class CrawlManager:
    def __init__(self, path=None, session_factory=None, discover=None):
        self.store = CrawlStore(path)
        self.session_factory = session_factory or (lambda stop: PublicSession(stop=stop))
        self.discover = discover or discover_public
        self.lock = threading.Lock()
        self.thread = None
        self.job_id = None
        self.stop = None
        self.shutting_down = None

    def recover_interrupted(self):
        self.store.recover()

    def _launch(self, job_id):
        self.job_id = job_id
        self.stop = threading.Event()
        self.shutting_down = threading.Event()
        self.thread = threading.Thread(target=self._run, args=(job_id, self.stop, self.shutting_down),
                                       name="16688-crawl", daemon=True)
        try:
            self.thread.start()
        except Exception:
            self.store.finish_job(job_id, "failed", "无法启动采集线程")
            raise

    def start(self, mode="discover", urls=None):
        if mode not in {"discover", "refresh"}:
            raise ValueError("mode 应为 discover 或 refresh")
        urls = [platform.normalize_target(url)["url"] for url in (urls or [])]
        with self.lock:
            self.recover_interrupted()
            job_id, reused = self.store.create(mode, urls)
            if not reused:
                self._launch(job_id)
        return {**self.store.report(job_id), "reused": reused}

    def resume(self, job_id):
        with self.lock:
            self.recover_interrupted()
            reused = self.store.resume(job_id)
            if not reused:
                self._launch(job_id)
        return {**self.store.report(job_id), "reused": reused}

    def cancel(self, job_id):
        self.store.cancel(job_id)
        if job_id == self.job_id and self.stop:
            self.stop.set()
        return self.store.report(job_id)

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self.shutting_down.set()
            self.stop.set()
            self.thread.join(timeout=2)

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
                url = f"{platform.BASE_URL}/shop/{shop_no}" if shop_no else item["url"]
                resolved = platform.resolve_target(url, session)
                shop = resolved[0]
                if shop_no and shop_no != shop["shop_no"]:
                    raise platform.PlatformError("店铺编号与已核验入口不一致")
                shop_no = shop["shop_no"]
                self.store.remember(item, shop)
                if not claimed:
                    claimed = self.store.claim_shop(item["job_id"], shop_no)
                    if not claimed:
                        self.store.finish_item(item, "skipped", shop_no)
                        return
                result = platform.sync_shop(url, session=session, resolved=resolved, store_path=self.store.path)
                self.store.finish_item(item, "done", shop_no, result=result)
        except CrawlCancelled:
            self.store.finish_item(item, "pending", shop_no if claimed else None)
        except Exception as exc:
            self.store.finish_item(item, "failed", shop_no if claimed else None, str(exc)[:400])

    def _run(self, job_id, stop, shutting_down):
        pulse_done = threading.Event()

        def pulse():
            while not pulse_done.wait(1):
                if self.store.heartbeat(job_id):
                    stop.set()

        heartbeat_thread = threading.Thread(target=pulse, daemon=True, name="16688-heartbeat")
        heartbeat_thread.start()
        try:
            job = self.store.report(job_id)
            self.store.enqueue(job_id, self.store.known_urls(job["mode"] == "discover"), "known")
            if job["mode"] == "discover" and not job["discovery_done"]:
                try:
                    with self.session_factory(stop) as session:
                        urls = self.discover(session, lambda version, pages: self.store.discovery_progress(job_id, version, pages))
                    self.store.discovery_finished(job_id, urls)
                except platform.PlatformError as exc:
                    self.store.discovery_finished(job_id, error=str(exc)[:400])
            if job["mode"] == "refresh":
                self.store.discovery_finished(job_id, [])
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="16688-shop") as pool:
                futures = set()
                while True:
                    if self.store.heartbeat(job_id):
                        stop.set()
                    while not stop.is_set() and len(futures) < 2:
                        item = self.store.claim_item(job_id)
                        if item is None:
                            break
                        futures.add(pool.submit(self._process, item, stop))
                    if not futures:
                        break
                    done, futures = wait(futures, timeout=0.2, return_when=FIRST_COMPLETED)
                    for future in done:
                        future.result()
            report = self.store.report(job_id)
            if stop.is_set():
                status = "interrupted" if shutting_down.is_set() else "cancelled"
            elif report["failure_count"] or report["discovery_error"]:
                status = "partial" if report["success_shops"] else "failed"
            else:
                status = "completed"
            self.store.finish_job(job_id, status)
        except CrawlCancelled:
            self.store.finish_job(job_id, "interrupted" if shutting_down.is_set() else "cancelled")
        except Exception as exc:
            self.store.finish_job(job_id, "failed", str(exc)[:400])
        finally:
            pulse_done.set()
            heartbeat_thread.join(timeout=2)


def run_cli(mode, urls, resume):
    manager = CrawlManager()
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
