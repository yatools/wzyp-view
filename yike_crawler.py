"""按关键词持久化一可采集任务；成功商品逐条提交，不替换整店快照。"""
import hashlib
import json
import os
import threading
import time
import uuid

import local_store
from crawl_store import CrawlStore, JobConflict, process_alive, timestamp
from public_http import CrawlBlockedError, CrawlCancelled
from crawl_failure import from_exception, normalize as normalize_failure, should_retry, validate_retry_mode
from yike import YikeClient, YikeError, PAGE_SIZE, archived, identity, keyword_value, normalize_detail, source_url

SCHEMA = """
CREATE TABLE IF NOT EXISTS yike_jobs (
 job_id TEXT PRIMARY KEY, keyword TEXT NOT NULL, status TEXT NOT NULL, phase TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, finished_at TEXT,
 owner_pid INTEGER NOT NULL, heartbeat REAL NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0,
 list_pages INTEGER NOT NULL DEFAULT 0, group_pages INTEGER NOT NULL DEFAULT 0,
 matched_groups INTEGER NOT NULL DEFAULT 0, discovery_error TEXT, error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_yike ON yike_jobs((1))
 WHERE status IN ('running','cancelling');
CREATE TABLE IF NOT EXISTS yike_items (
 job_id TEXT NOT NULL REFERENCES yike_jobs(job_id), guid TEXT NOT NULL, url TEXT NOT NULL,
 payload TEXT NOT NULL, revision TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 shop_token TEXT, error TEXT, updated_at TEXT NOT NULL, PRIMARY KEY(job_id,guid)
);
CREATE INDEX IF NOT EXISTS yike_pending ON yike_items(job_id,status);
"""


class YikeStore(CrawlStore):
    def __init__(self, path=None):
        super().__init__(path or local_store.database_path("yike"))
        with self.transaction() as conn:
            conn.executescript(SCHEMA)
            if "failure_json" not in {row[1] for row in conn.execute("PRAGMA table_info(yike_items)")}:
                conn.execute("ALTER TABLE yike_items ADD COLUMN failure_json TEXT")

    def recover(self):
        with self.transaction() as conn:
            for row in conn.execute("SELECT * FROM yike_jobs WHERE status IN ('running','cancelling')").fetchall():
                if not process_alive(row["owner_pid"]) or time.time() - row["heartbeat"] > 90:
                    conn.execute("UPDATE yike_jobs SET status='interrupted',phase='interrupted',updated_at=?,"
                                 "error='服务已停止，可继续按关键词采集' WHERE job_id=?", (timestamp(), row["job_id"]))

    def create(self, keyword):
        with self.transaction() as conn:
            if conn.execute("SELECT 1 FROM yike_jobs WHERE status IN ('running','cancelling')").fetchone():
                raise JobConflict("已有一可关键词采集任务正在运行")
            job_id, now = uuid.uuid4().hex, timestamp()
            conn.execute("INSERT INTO yike_jobs(job_id,keyword,status,phase,created_at,updated_at,owner_pid,heartbeat) "
                         "VALUES (?,?,'running','discovering',?,?,?,?)", (job_id, keyword, now, now, os.getpid(), time.time()))
            return job_id

    def resume(self, job_id, retry_mode="transient"):
        validate_retry_mode(retry_mode)
        with self.transaction() as conn:
            job = conn.execute("SELECT * FROM yike_jobs WHERE job_id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            active = conn.execute("SELECT job_id FROM yike_jobs WHERE status IN ('running','cancelling')").fetchone()
            if active:
                if active[0] == job_id:
                    return True
                raise JobConflict("已有一可关键词采集任务正在运行")
            if job["status"] == "completed":
                raise JobConflict("任务已完成，请重新提交关键词更新报价")
            for item in conn.execute("SELECT * FROM yike_items WHERE job_id=? AND status!='done'", (job_id,)).fetchall():
                if item["status"] != "failed" or should_retry(item["failure_json"], item["error"], retry_mode):
                    conn.execute("UPDATE yike_items SET status='pending',error=NULL,failure_json=NULL WHERE job_id=? AND guid=?", (job_id, item["guid"]))
            conn.execute("UPDATE yike_jobs SET status='running',phase='discovering',cancel_requested=0,"
                         "owner_pid=?,heartbeat=?,updated_at=?,finished_at=NULL,list_pages=0,group_pages=0,"
                         "matched_groups=0,discovery_error=NULL,error=NULL WHERE job_id=?",
                         (os.getpid(), time.time(), timestamp(), job_id))
            return False

    def update(self, job_id, **values):
        if not values.keys() <= {"status", "phase", "error", "discovery_error", "list_pages", "group_pages", "matched_groups", "finished_at"}:
            raise ValueError("未知任务字段")
        values.update(updated_at=timestamp(), heartbeat=time.time())
        with self.transaction() as conn:
            conn.execute("UPDATE yike_jobs SET " + ",".join(f"{key}=?" for key in values) + " WHERE job_id=?", [*values.values(), job_id])

    def heartbeat(self, job_id):
        with self.transaction() as conn:
            conn.execute("UPDATE yike_jobs SET heartbeat=? WHERE job_id=?", (time.time(), job_id))
            return bool(conn.execute("SELECT cancel_requested FROM yike_jobs WHERE job_id=?", (job_id,)).fetchone()[0])

    def cancel(self, job_id):
        with self.transaction() as conn:
            if not conn.execute("SELECT 1 FROM yike_jobs WHERE job_id=?", (job_id,)).fetchone():
                raise KeyError(job_id)
            conn.execute("UPDATE yike_jobs SET cancel_requested=1,status='cancelling',updated_at=? "
                         "WHERE job_id=? AND status IN ('running','cancelling')", (timestamp(), job_id))

    def enqueue(self, job_id, items):
        # 只对真实单店报价入队，聚合卡片永不成为商品记录。
        rows = []
        for item in items:
            guid, url = identity(item.get("guid")), source_url(item)
            payload = json.dumps(item, ensure_ascii=False, sort_keys=True)
            quote_fields = ("guid", "key", "shopGuid", "shopKey", "title", "shopName", "shopUrl", "linkUrl",
                            "price", "stock", "stockStatus", "goodsType", "categoryName", "updateTime",
                            "active", "archived", "inactiveAt", "skuLabel")
            version = hashlib.sha256(json.dumps({key: item.get(key) for key in quote_fields}, sort_keys=True).encode()).hexdigest()
            rows.append((job_id, guid, url, payload, version, timestamp()))
        with self.transaction() as conn:
            conn.executemany("INSERT INTO yike_items(job_id,guid,url,payload,revision,updated_at) VALUES (?,?,?,?,?,?) "
                             "ON CONFLICT(job_id,guid) DO UPDATE SET payload=excluded.payload,revision=excluded.revision,"
                             "status=CASE WHEN yike_items.status IN ('done','failed') AND yike_items.revision=excluded.revision THEN yike_items.status ELSE 'pending' END,"
                             "error=CASE WHEN yike_items.revision=excluded.revision THEN yike_items.error ELSE NULL END,"
                             "failure_json=CASE WHEN yike_items.revision=excluded.revision THEN yike_items.failure_json ELSE NULL END,updated_at=excluded.updated_at", rows)
            # 列表明确下架也可撤下已有报价；详情失效时仍保留最后一次完整说明和价格。
            affected_shops = set()
            for item in items:
                if archived(item):
                    old = conn.execute("SELECT shop_token FROM wzyp_goods WHERE goods_key=?", (identity(item["guid"]),)).fetchone()
                    if old:
                        affected_shops.add(old[0])
                    conn.execute("UPDATE wzyp_goods SET status=0 WHERE goods_key=?", (identity(item["guid"]),))
            for token in affected_shops:
                conn.execute("UPDATE wzyp_shops SET goods_count=(SELECT COUNT(*) FROM wzyp_goods WHERE shop_token=? AND status=1),"
                             "unknown_count=(SELECT COUNT(*) FROM wzyp_goods WHERE shop_token=? AND status=1) WHERE token=?", (token, token, token))

    def claim(self, job_id):
        with self.transaction() as conn:
            job = conn.execute("SELECT status,cancel_requested FROM yike_jobs WHERE job_id=?", (job_id,)).fetchone()
            if job["status"] != "running" or job["cancel_requested"]:
                return None
            row = conn.execute("SELECT * FROM yike_items WHERE job_id=? AND status='pending' ORDER BY rowid LIMIT 1", (job_id,)).fetchone()
            if row:
                conn.execute("UPDATE yike_items SET status='working' WHERE job_id=? AND guid=?", (job_id, row["guid"]))
            return dict(row) if row else None

    def item_error(self, job_id, guid, error, failure=None):
        with self.transaction() as conn:
            conn.execute("UPDATE yike_items SET status='failed',error=?,failure_json=?,updated_at=? WHERE job_id=? AND guid=?",
                         (error[:400], json.dumps(failure or normalize_failure(message=error)), timestamp(), job_id, guid))

    def save(self, job_id, guid, detail):
        shop, goods, extra = normalize_detail(detail, guid, timestamp())
        with self.transaction() as conn:
            old = conn.execute("SELECT shop_token FROM wzyp_goods WHERE goods_key=?", (guid,)).fetchone()
            for table, key, value in (("wzyp_shops", "token", {**shop, "goods_count": 0}),
                                      ("wzyp_goods", "goods_key", goods), ("wzyp_goods_detail", "goods_key", extra)):
                cols = list(value)
                conn.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)}) "
                             f"ON CONFLICT({key}) DO UPDATE SET " + ",".join(f"{col}=excluded.{col}" for col in cols if col != key), list(value.values()))
            for token in {shop["token"], old[0] if old else shop["token"]}:
                conn.execute("UPDATE wzyp_shops SET goods_count=(SELECT COUNT(*) FROM wzyp_goods WHERE shop_token=? AND status=1),"
                             "unknown_count=(SELECT COUNT(*) FROM wzyp_goods WHERE shop_token=? AND status=1) WHERE token=?", (token, token, token))
            conn.execute("UPDATE yike_items SET status='done',shop_token=?,error=NULL,updated_at=? WHERE job_id=? AND guid=?",
                         (shop["token"], timestamp(), job_id, guid))

    def report(self, job_id=None):
        jobs = self.rows("SELECT * FROM yike_jobs WHERE job_id=?", (job_id,)) if job_id else self.rows(
            "SELECT * FROM yike_jobs ORDER BY CASE WHEN status IN ('running','cancelling') THEN 0 ELSE 1 END,created_at DESC,rowid DESC LIMIT 1")
        if not jobs:
            return None
        job = jobs[0]
        counts = {row["status"]: row["n"] for row in self.rows("SELECT status,COUNT(*) n FROM yike_items WHERE job_id=? GROUP BY status", (job["job_id"],))}
        job.update(source="yike", discovered_links=sum(counts.values()), collected_goods=counts.get("done", 0),
                   pending_links=counts.get("pending", 0) + counts.get("working", 0),
                   failure_count=counts.get("failed", 0) + int(bool(job["discovery_error"])),
                   success_shops=self.rows("SELECT COUNT(DISTINCT shop_token) n FROM yike_items WHERE job_id=? AND status='done'", (job["job_id"],))[0]["n"])
        job["failures"] = self.rows("SELECT url,error,failure_json FROM yike_items WHERE job_id=? AND status='failed' ORDER BY rowid LIMIT 20", (job["job_id"],))
        for item in job["failures"]:
            item["failure"] = normalize_failure(item.pop("failure_json"), item["error"])
        job["last_error"] = job["error"] or job["discovery_error"] or (job["failures"][0]["error"] if job["failures"] else None)
        return job

    def history(self, limit=20):
        return [self.report(row["job_id"]) for row in self.rows("SELECT job_id FROM yike_jobs ORDER BY created_at DESC,rowid DESC LIMIT ?", (limit,))]


class YikeManager:
    def __init__(self, path=None, session_factory=None):
        self.store = YikeStore(path)
        self.session_factory = session_factory
        self.lock = threading.Lock()
        self.thread = self.stop = self.shutting_down = None
        self.job_id = None

    def recover_interrupted(self):
        self.store.recover()

    def start(self, keyword):
        keyword = keyword_value(keyword)
        with self.lock:
            self.store.recover()
            job_id = self.store.create(keyword)
            self._launch(job_id)
        return self.store.report(job_id)

    def resume(self, job_id, retry_mode="transient"):
        with self.lock:
            self.store.recover()
            reused = self.store.resume(job_id, retry_mode)
            if not reused:
                self._launch(job_id)
        return {**self.store.report(job_id), "reused": reused}

    def cancel(self, job_id):
        with self.lock:
            self.store.cancel(job_id)
            if job_id == self.job_id and self.stop:
                self.stop.set()
        return self.store.report(job_id)

    def _launch(self, job_id):
        self.job_id = job_id
        self.stop, self.shutting_down = threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self._run, args=(job_id, self.stop, self.shutting_down), name="yike-keyword-crawl", daemon=True)
        try:
            self.thread.start()
        except Exception:
            self.store.update(job_id, status="failed", phase="finished", error="无法启动一可采集线程")
            raise

    def shutdown(self):
        if self.thread and self.thread.is_alive():
            self.shutting_down.set()
            self.stop.set()
            self.thread.join(timeout=2)

    def _discover(self, client, job):
        offset, groups, signatures, group_pages = 0, set(), set(), 0
        while offset < PAGE_SIZE * 10000:
            rows = client.products(job["keyword"], offset)
            signature = tuple((row.get("groupKey"), str(row.get("guid"))) for row in rows)
            if rows and signature in signatures:
                raise YikeError("一可关键词分页重复，需继续采集重新核验")
            signatures.add(signature)
            for row in rows:
                if "grouped" in row and type(row["grouped"]) is not bool:
                    raise YikeError("一可聚合标记格式异常")
                if row.get("grouped") is not True and (str(row.get("groupKey") or "").startswith("s:") or row.get("productCount", 1) != 1):
                    raise YikeError("一可聚合标记不完整，保留待续采任务")
                if row.get("grouped") is not True:
                    self.store.enqueue(job["job_id"], [row])
                    continue
                key = row.get("groupKey")
                if key in groups:
                    continue
                page, group_ids, expected_total = 1, set(), None
                while True:
                    result = client.group(key, page)
                    if expected_total is not None and expected_total != result["total"]:
                        raise YikeError("一可同源组分页期间数量变化，需继续采集重新核验")
                    expected_total = result["total"]
                    ids = [identity(item.get("guid")) for item in result["items"]]
                    if len(set(ids)) != len(ids) or group_ids.intersection(ids):
                        raise YikeError("一可同源组分页重复，需继续采集重新核验")
                    group_ids.update(ids)
                    self.store.enqueue(job["job_id"], result["items"])
                    group_pages += 1
                    self.store.update(job["job_id"], group_pages=group_pages)
                    if page >= result["totalPages"]:
                        break
                    if page >= 10000:
                        raise YikeError("一可同源组超过分页预算，保留待续采任务")
                    page += 1
                groups.add(key)
                self.store.update(job["job_id"], matched_groups=len(groups))
            self.store.update(job["job_id"], list_pages=offset // PAGE_SIZE + 1)
            if len(rows) < PAGE_SIZE:
                return
            offset += len(rows)
        raise YikeError("一可关键词超过分页预算，保留待续采任务")

    def _run(self, job_id, stop, shutting_down):
        pulse_done = threading.Event()

        def pulse():
            while not pulse_done.wait(1):
                if self.store.heartbeat(job_id):
                    stop.set()

        heartbeat = threading.Thread(target=pulse, name="yike-heartbeat", daemon=True)
        heartbeat.start()
        status, error = "failed", None
        try:
            with YikeClient(stop, self.session_factory) as client:
                job = self.store.report(job_id)
                try:
                    self._discover(client, job)
                except (CrawlCancelled, CrawlBlockedError):
                    raise
                except Exception as exc:
                    self.store.update(job_id, discovery_error=str(exc)[:400])
                self.store.update(job_id, phase="collecting")
                while not stop.is_set():
                    item = self.store.claim(job_id)
                    if item is None:
                        break
                    try:
                        detail = client.detail(item["guid"])
                        client.session.check_cancel()
                        self.store.save(job_id, item["guid"], detail)
                    except (CrawlCancelled, CrawlBlockedError):
                        raise
                    except Exception as exc:
                        self.store.item_error(job_id, item["guid"], str(exc), from_exception(exc))
                if stop.is_set() or self.store.heartbeat(job_id):
                    raise CrawlCancelled()
                report = self.store.report(job_id)
                status = ("partial" if report["collected_goods"] else "failed") if report["failure_count"] else "completed"
        except CrawlCancelled:
            status = "interrupted" if shutting_down.is_set() else "cancelled"
        except CrawlBlockedError as exc:
            status, error = "interrupted", str(exc)[:400]
        except Exception as exc:
            error = str(exc)[:400]
        finally:
            pulse_done.set()
            heartbeat.join(timeout=2)
            self.store.update(job_id, status=status, phase="finished", error=error, finished_at=timestamp())
