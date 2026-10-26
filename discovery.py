"""跨平台发现任务：持久化候选与来源链，协调已有平台任务而不抢占它们。"""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from urllib.parse import urlsplit
import uuid

from config import BASE_DIR
from sqlite_init import initialize,write_transaction,is_busy,RetryConnection
from shop_preview import PreviewStore, SCHEMA as PREVIEW_SCHEMA
from task_lifecycle import worker_alive,run_worker,active_job
from crawl_store import ACTIVE, JobConflict, process_alive, timestamp
from link_discovery import extract_links
from public_http import CrawlBlockedError, CrawlCancelled
from shop_http import SafeSession, normalize_url, origin_of
from sources import DIRECT_SOURCES as SOURCES, adapter_for, canonical_source, native_source
from crawl_failure import (details as failure_details, from_exception, normalize as normalize_failure,
                           should_retry, validate_retry_mode, http_failure, CollectionError)

class _ProbeSession:
    """Reuse a homepage read during template detection, only within this probe."""
    def __init__(self, session):
        self.session, self.homepages, self.resolved = session, {}, {}

    def __getattr__(self, name):
        return getattr(self.session, name)

    def get(self, url, **kwargs):
        key=(url,json.dumps(kwargs,sort_keys=True,default=str))
        if key in self.homepages:
            return self.homepages[key]
        response = self.session.get(url, **kwargs)
        self.homepages[key] = response
        return response

SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_jobs (
 job_id TEXT PRIMARY KEY, status TEXT NOT NULL, sources TEXT NOT NULL,
 include_indexes INTEGER NOT NULL, initialized INTEGER NOT NULL DEFAULT 0,
 max_depth INTEGER NOT NULL, page_limit INTEGER NOT NULL, pages INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, finished_at TEXT,
 owner_pid INTEGER NOT NULL, heartbeat REAL NOT NULL, cancel_requested INTEGER NOT NULL DEFAULT 0,
 index_errors TEXT NOT NULL DEFAULT '[]', error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_discovery ON discovery_jobs((1)) WHERE status IN ('running','cancelling');
CREATE TABLE IF NOT EXISTS discovery_candidates (
 id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES discovery_jobs(job_id),
 url TEXT NOT NULL, depth INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
 source TEXT, shop_key TEXT, child_job_id TEXT, reason TEXT, updated_at TEXT NOT NULL,
 origin TEXT,
 UNIQUE(job_id,url)
);
CREATE INDEX IF NOT EXISTS candidate_queue ON discovery_candidates(job_id,state,depth,id);
CREATE INDEX IF NOT EXISTS candidate_source ON discovery_candidates(job_id,source,state,depth,id);
CREATE TABLE IF NOT EXISTS discovery_edges (
 candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id), parent_url TEXT NOT NULL,
 depth INTEGER NOT NULL, PRIMARY KEY(candidate_id,parent_url)
);
CREATE TABLE IF NOT EXISTS discovery_shops (
 job_id TEXT NOT NULL REFERENCES discovery_jobs(job_id), source TEXT NOT NULL,
 shop_key TEXT NOT NULL, goods_count INTEGER NOT NULL,
 PRIMARY KEY(job_id,source,shop_key)
);
CREATE TABLE IF NOT EXISTS discovery_origins (
 job_id TEXT NOT NULL REFERENCES discovery_jobs(job_id), origin TEXT NOT NULL,
 state TEXT NOT NULL, source TEXT, charged INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL, PRIMARY KEY(job_id,origin)
);
CREATE TABLE IF NOT EXISTS discovery_retries (
 id INTEGER PRIMARY KEY, candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id),
 source TEXT NOT NULL, child_job_id TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(source,child_job_id)
);
"""


def database_path():
    return os.environ.get("WZYP_DISCOVERY_DB") or os.path.join(BASE_DIR, "data", "discovery.sqlite3")


class DiscoveryStore:
    def __init__(self, path=None):
        self.path = path or database_path()
        self.previews = PreviewStore(self.path)

    @contextmanager
    def connection(self,write=False):
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=15,factory=RetryConnection)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        def migrate(conn):
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA + PREVIEW_SCHEMA)
            legacy_origins = "probe_limit" not in {row[1] for row in conn.execute("PRAGMA table_info(discovery_jobs)")}
            for table, column, kind in (("discovery_jobs", "parallel_sources", "INTEGER NOT NULL DEFAULT 4"),
                                         ("discovery_jobs", "probe_limit", "INTEGER NOT NULL DEFAULT 200"),
                                         ("discovery_jobs", "probes", "INTEGER NOT NULL DEFAULT 0"),
                                         ("discovery_candidates", "failure_json", "TEXT"),
                                         ("discovery_jobs", "workflow_version", "INTEGER NOT NULL DEFAULT 1"),
                                         ("discovery_jobs", "phase", "TEXT NOT NULL DEFAULT 'shop_discovery'")):
                if column not in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
            if 'origin' not in {row[1] for row in conn.execute('PRAGMA table_info(discovery_candidates)')}:
                conn.execute('ALTER TABLE discovery_candidates ADD COLUMN origin TEXT')
            conn.execute("UPDATE discovery_candidates SET origin=substr(url,1,instr(substr(url,9),'/')+7) "
                         "WHERE origin IS NULL AND url LIKE 'https://%'")
            conn.execute('CREATE INDEX IF NOT EXISTS candidate_origin ON discovery_candidates(job_id,origin,state)')
            conn.execute('CREATE INDEX IF NOT EXISTS candidate_shop ON discovery_candidates(job_id,source,shop_key,state)')
            if legacy_origins:
                # Import only verdicts that the old discovery flow actually verified.
                # Failed records and imported shop rows do not establish a template.
                origin_sql = "substr(url,1,instr(substr(url,9),'/')+7)"
                conn.execute(f"INSERT OR IGNORE INTO discovery_origins(job_id,origin,state,source,charged,updated_at) "
                             f"SELECT job_id,{origin_sql},'identified',MIN(source),0,MAX(updated_at) "
                             "FROM discovery_candidates WHERE source IS NOT NULL AND state IN ('ready','waiting','collecting','collected','duplicate') "
                             f"GROUP BY job_id,{origin_sql} HAVING COUNT(DISTINCT source)=1")
                conn.execute(f"INSERT OR IGNORE INTO discovery_origins(job_id,origin,state,source,charged,updated_at) "
                             f"SELECT job_id,{origin_sql},'directory',NULL,0,MAX(updated_at) "
                             f"FROM discovery_candidates WHERE state='directory' GROUP BY job_id,{origin_sql}")
            conn.commit()
        try:
            initialize(conn,self.path,'discovery',migrate)
        except Exception:
            conn.close()
            raise
        try:
            with write_transaction(conn,self.path) if write else conn:
                yield conn
        finally:
            conn.close()

    def recover(self):
        with self.connection(write=True) as conn:
            for job in conn.execute("SELECT * FROM discovery_jobs WHERE status IN ('running','cancelling')").fetchall():
                if worker_alive('discovery',self.path,job['job_id']):continue
                if not process_alive(job["owner_pid"]) or time.time() - job["heartbeat"] > 90:
                    conn.execute("UPDATE discovery_jobs SET status='interrupted',updated_at=?,error='服务已停止，可继续发现' WHERE job_id=?", (timestamp(), job["job_id"]))
                    state='ready' if job['workflow_version']==2 and job['phase']=='goods_collection' else 'pending'
                    conn.execute("UPDATE discovery_candidates SET state=? WHERE job_id=? AND state IN ('probing','probing_shop','ready','waiting','collecting')", (state,job["job_id"]))

    def enqueue(self, job_id, urls, parent_url="manual", depth=0, source_hint=None):
        entries=iter(urls)
        while batch:=list(islice(entries,200)):
            with self.connection(write=True) as conn:
                self._enqueue(conn, job_id, batch, parent_url, depth, source_hint)

    @staticmethod
    def _enqueue(conn, job_id, urls, parent_url="manual", depth=0, source_hint=None):
        for raw in urls:
            state, reason = "pending", None
            try:
                url = normalize_url(raw)
            except ValueError as exc:
                url, state, reason = str(raw)[:2048], "rejected", str(exc)
            row = conn.execute("SELECT id,depth,state FROM discovery_candidates WHERE job_id=? AND url=?", (job_id, url)).fetchone()
            if row is None:
                source = (native_source(url) or source_hint) if state == "pending" else None
                if state == "pending" and source is None:
                    known = conn.execute("SELECT source FROM discovery_origins WHERE job_id=? AND origin=? AND state='identified'",
                                         (job_id, origin_of(url))).fetchone()
                    source = known[0] if known else None
                cursor = conn.execute("INSERT INTO discovery_candidates(job_id,url,depth,state,reason,source,failure_json,updated_at,origin) VALUES (?,?,?,?,?,?,?,?,?)",
                                      (job_id, url, depth, state, reason, source,
                                       json.dumps(failure_details("invalid_link")) if state == "rejected" else None, timestamp(),
                                       origin_of(url) if state!='rejected' else None))
                item_id = cursor.lastrowid
            else:
                item_id = row["id"]
                if source_hint and state == "pending":
                    conn.execute("UPDATE discovery_candidates SET source=COALESCE(source,?) WHERE id=?", (source_hint, item_id))
                if depth < row["depth"]:
                    conn.execute("UPDATE discovery_candidates SET depth=?,state=CASE WHEN state='deferred' THEN 'pending' ELSE state END WHERE id=?", (depth, item_id))
            conn.execute("INSERT OR IGNORE INTO discovery_edges(candidate_id,parent_url,depth) VALUES (?,?,?)", (item_id, parent_url, depth))

    def create(self, urls, selected_sources, include_indexes=True, max_depth=2, page_limit=200, parallel_sources=4, probe_limit=200, workflow_version=1):
        selected_sources = list(dict.fromkeys(canonical_source(s) for s in selected_sources))
        if not selected_sources:
            raise ValueError("至少选择一个平台")
        if any(source not in SOURCES for source in selected_sources):
            raise ValueError("聚合来源不支持整店发现")
        if isinstance(parallel_sources, bool) or not isinstance(parallel_sources, int) or not 1 <= parallel_sources <= 8:
            raise ValueError("并发平台数须为 1–8")
        if isinstance(probe_limit, bool) or not isinstance(probe_limit, int) or not 0 <= probe_limit <= 2000:
            raise ValueError("源站核验预算须为 0–2000")
        with self.connection(write=True) as conn:
            if active_job('discovery',self.path):
                raise JobConflict('已有发现任务工作线程仍在运行或退出，请稍后重试')
            active = conn.execute("SELECT job_id FROM discovery_jobs WHERE status IN ('running','cancelling')").fetchone()
            if active:
                raise JobConflict("已有跨平台发现任务正在运行，请先完成或取消该任务")
            job_id, now = uuid.uuid4().hex, timestamp()
            conn.execute("INSERT INTO discovery_jobs(job_id,status,sources,include_indexes,max_depth,page_limit,created_at,updated_at,owner_pid,heartbeat) VALUES (?,'running',?,?,?,?,?,?,?,?)",
                         (job_id, json.dumps(selected_sources), int(include_indexes), max_depth, page_limit, now, now, os.getpid(), time.time()))
            self._enqueue(conn, job_id, urls)
            conn.execute("UPDATE discovery_jobs SET parallel_sources=?,probe_limit=? WHERE job_id=?", (parallel_sources, probe_limit, job_id))
            conn.execute('UPDATE discovery_jobs SET workflow_version=? WHERE job_id=?',(workflow_version,job_id))
        return job_id

    def update_job(self, job_id, **values):
        allowed = {"status", "initialized", "pages", "index_errors", "error", "finished_at", "phase"}
        if not set(values) <= allowed:
            raise ValueError("无效任务字段")
        values["updated_at"] = timestamp()
        with self.connection(write=True) as conn:
            conn.execute("UPDATE discovery_jobs SET " + ",".join(f"{k}=?" for k in values) + " WHERE job_id=?", [*values.values(), job_id])

    def update_candidate(self, item_id, state, **values):
        if not set(values) <= {"source", "shop_key", "child_job_id", "reason", "failure_json"}:
            raise ValueError("无效候选字段")
        values.update(state=state, updated_at=timestamp())
        with self.connection(write=True) as conn:
            conn.execute("UPDATE discovery_candidates SET " + ",".join(f"{k}=?" for k in values) + " WHERE id=?", [*values.values(), item_id])

    def next_candidate(self, job_id):
        with self.connection() as conn:
            return self._next_candidate(conn, job_id)

    @staticmethod
    def _next_candidate(conn, job_id, excluded=(), known_only=False):
            # 同层先读取尚未核验的源站，防止一个未知模板的数百个商品页
            # 消耗整个目录预算。保留原始深度、候选身份和来源链。
            clause = (' AND candidate.origin NOT IN ('+','.join('?' for _ in excluded)+')') if excluded else ''
            if known_only:
                clause += " AND candidate.source IS NOT NULL"
            row = conn.execute("""
                WITH heads AS (
                    SELECT MIN(candidate.id) AS id,candidate.depth,candidate.origin
                    FROM discovery_candidates AS candidate
                    WHERE candidate.job_id=? AND candidate.state='pending' """ + clause + """
                    GROUP BY candidate.depth,candidate.origin
                )
                SELECT candidate.* FROM discovery_candidates AS candidate WHERE candidate.id=(
                SELECT head.id FROM heads AS head ORDER BY head.depth,EXISTS (
                    SELECT 1 FROM discovery_candidates AS visited
                    WHERE visited.job_id=? AND visited.origin=head.origin
                    AND visited.state NOT IN ('pending','rejected','deferred')
                ),head.id LIMIT 1)
                """, (job_id, *excluded,job_id)).fetchone()
            return dict(row) if row else None

    def claim_candidate(self, job_id, excluded=(), known_only=False):
        with self.connection(write=True) as conn:
            row = self._next_candidate(conn, job_id, excluded, known_only)
            if row:
                conn.execute("UPDATE discovery_candidates SET state='probing',updated_at=? WHERE id=?", (timestamp(), row["id"]))
            return row

    def ready_candidate(self, job_id, source):
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM discovery_candidates WHERE job_id=? AND source=? AND state IN ('ready','waiting') ORDER BY depth,id LIMIT 1", (job_id, source)).fetchone()
            return dict(row) if row else None

    def skip_saved_urls(self,job_id,source,key,urls):
        entries=iter(dict.fromkeys(urls));total=0
        while batch:=list(islice(entries,200)):
            with self.connection(write=True) as conn:
                owner=conn.execute("SELECT id FROM discovery_candidates WHERE job_id=? AND source=? AND shop_key=? "
                    "AND state IN ('qualified','ready','waiting','collecting','collected','empty','no_stock') LIMIT 1",(job_id,source,key)).fetchone()
                if not owner:return total
                before=conn.total_changes
                conn.executemany("UPDATE discovery_candidates SET state='duplicate',shop_key=?,reason='该店铺目录已保存，跳过已核实归属的重复入口',updated_at=? "
                    "WHERE job_id=? AND source=? AND url=? AND state IN ('pending','ready','waiting') AND id!=?",
                    [(key,timestamp(),job_id,source,url,owner['id']) for url in batch])
                total+=conn.total_changes-before
        return total

    def reserve_page(self, job_id):
        with self.connection(write=True) as conn:
            return conn.execute("UPDATE discovery_jobs SET pages=pages+1 WHERE job_id=? AND pages<page_limit", (job_id,)).rowcount == 1

    def reserve_probe(self, job_id, origin):
        """Charge each origin once, including interrupted or failed first attempts."""
        with self.connection(write=True) as conn:
            row = conn.execute("SELECT charged FROM discovery_origins WHERE job_id=? AND origin=?", (job_id, origin)).fetchone()
            if row:
                return True
            if conn.execute("UPDATE discovery_jobs SET probes=probes+1 WHERE job_id=? AND probes<probe_limit", (job_id,)).rowcount != 1:
                return False
            conn.execute("INSERT INTO discovery_origins VALUES (?,?,'unconfirmed',NULL,1,?)", (job_id, origin, timestamp()))
            return True

    def remember_origin(self, job_id, origin, source=None):
        with self.connection(write=True) as conn:
            conn.execute("INSERT INTO discovery_origins VALUES (?,?,?,?,0,?) ON CONFLICT(job_id,origin) "
                         "DO UPDATE SET state=excluded.state,source=excluded.source,updated_at=excluded.updated_at",
                         (job_id, origin, "identified" if source else "directory", source, timestamp()))
            if source:
                conn.execute("UPDATE discovery_candidates SET source=? WHERE job_id=? AND source IS NULL AND instr(url,?)=1",
                             (source, job_id, origin + "/"))

    def origin_cache(self, job_id):
        with self.connection(write=True) as conn:
            conn.execute("UPDATE discovery_candidates AS candidate SET source=(SELECT known.source FROM discovery_origins known "
                         "WHERE known.job_id=candidate.job_id AND known.origin=substr(candidate.url,1,instr(substr(candidate.url,9),'/')+7) "
                         "AND known.state='identified') WHERE job_id=? AND source IS NULL AND state='pending'", (job_id,))
            return {row["origin"]: row["source"] for row in conn.execute(
                "SELECT origin,source FROM discovery_origins WHERE job_id=? AND state IN ('identified','directory')", (job_id,))}

    def origin_verdict(self, job_id, origin):
        with self.connection() as conn:
            row = conn.execute("SELECT source FROM discovery_origins WHERE job_id=? AND origin=? "
                               "AND state IN ('identified','directory')", (job_id, origin)).fetchone()
            return dict(row) if row else None

    def prior_urls(self, source):
        host = urlsplit(adapter_for(source).BASE_URL).netloc
        with self.connection() as conn:
            rows = conn.execute("SELECT DISTINCT url FROM discovery_candidates WHERE url LIKE ?", ("https://" + host + "/%",)).fetchall()
        return [row[0] for row in rows if native_source(row[0]) == source]

    def heartbeat(self, job_id):
        with self.connection(write=True) as conn:
            conn.execute("UPDATE discovery_jobs SET heartbeat=?,updated_at=? WHERE job_id=?", (time.time(), timestamp(), job_id))
            row = conn.execute("SELECT cancel_requested FROM discovery_jobs WHERE job_id=?", (job_id,)).fetchone()
            return bool(row and row[0])

    def has_shop(self, job_id, source, key):
        with self.connection() as conn:
            return bool(conn.execute("SELECT 1 FROM discovery_shops WHERE job_id=? AND source=? AND shop_key=?", (job_id, source, key)).fetchone())

    def add_shop(self, job_id, source, key, count):
        with self.connection(write=True) as conn:
            conn.execute("INSERT INTO discovery_shops VALUES (?,?,?,?) ON CONFLICT(job_id,source,shop_key) DO UPDATE SET goods_count=excluded.goods_count", (job_id, source, key, count))

    def cancel(self, job_id):
        with self.connection(write=True) as conn:
            if not conn.execute("SELECT 1 FROM discovery_jobs WHERE job_id=?", (job_id,)).fetchone():
                raise KeyError(job_id)
            conn.execute("UPDATE discovery_jobs SET cancel_requested=1,status='cancelling',updated_at=? WHERE job_id=? AND status='running'", (timestamp(), job_id))

    def resume(self, job_id, extra_pages=0, max_depth=None, extra_probes=0, retry_mode="transient"):
        validate_retry_mode(retry_mode)
        for value in (extra_pages, extra_probes):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 2000:
                raise ValueError("追加预算须为 0–2000")
        with self.connection(write=True) as conn:
            row = conn.execute("SELECT * FROM discovery_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                raise KeyError(job_id)
            if worker_alive('discovery',self.path,job_id):
                raise JobConflict('任务工作线程仍在运行或退出，请稍后继续，避免重复运行')
            if conn.execute("SELECT 1 FROM discovery_jobs WHERE status IN ('running','cancelling')").fetchone():
                raise JobConflict("已有发现任务正在运行")
            if row["status"] == "completed":
                raise JobConflict("任务已完成，请创建新发现任务")
            depth = max(row["max_depth"], max_depth if max_depth is not None else row["max_depth"])
            conn.execute("UPDATE discovery_jobs SET status='running',cancel_requested=0,error=NULL,finished_at=NULL,initialized=CASE WHEN index_errors='[]' THEN initialized ELSE 0 END,owner_pid=?,heartbeat=?,max_depth=?,page_limit=page_limit+?,probe_limit=probe_limit+?,updated_at=? WHERE job_id=?", (os.getpid(), time.time(), depth, extra_pages, extra_probes, timestamp(), job_id))
            for item in conn.execute("SELECT * FROM discovery_candidates WHERE job_id=? AND state IN ('failed','blocked','deferred','probe_deferred','probing','probing_shop','ready','waiting','collecting','unsupported')", (job_id,)).fetchall():
                if item["state"] == "unsupported":
                    if retry_mode != "all" or item["source"] and item["source"] not in json.loads(row["sources"]):
                        continue
                    if not item["source"]:
                        conn.execute("UPDATE discovery_origins SET state='unconfirmed',source=NULL WHERE job_id=? AND origin=?", (job_id, origin_of(item["url"])))
                if item["state"] in {"failed", "blocked"} and not should_retry(item["failure_json"], item["reason"], retry_mode):
                    continue
                preview=conn.execute("SELECT eligibility FROM shop_previews WHERE job_id=? AND source=? AND shop_key=?",
                                     (job_id,item['source'],item['shop_key'])).fetchone() if row['workflow_version']==2 else None
                state='ready' if preview and preview['eligibility']=='eligible' else 'pending'
                conn.execute("UPDATE discovery_candidates SET state=?,reason=NULL,failure_json=NULL WHERE id=?", (state,item["id"]))
            # Older queues predate enqueue-time classification. Recover their known
            # native URLs without making them wait behind slow unknown-domain probes.
            for item in conn.execute("SELECT id,url FROM discovery_candidates WHERE job_id=? AND state='pending' AND source IS NULL", (job_id,)).fetchall():
                source = native_source(item["url"])
                if source:
                    conn.execute("UPDATE discovery_candidates SET source=? WHERE id=?", (source, item["id"]))
            if row['workflow_version']==2 and (not row['initialized'] or row['index_errors']!='[]' or
                    conn.execute("SELECT 1 FROM discovery_candidates WHERE job_id=? AND state='pending' LIMIT 1",(job_id,)).fetchone()):
                conn.execute("UPDATE discovery_jobs SET phase='shop_discovery' WHERE job_id=?",(job_id,))

    def candidate(self, job_id, candidate_id):
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM discovery_candidates WHERE job_id=? AND id=?", (job_id, candidate_id)).fetchone()
            return dict(row) if row else None

    def add_retry(self, candidate_id, source, child_id):
        with self.connection(write=True) as conn:
            conn.execute("INSERT OR IGNORE INTO discovery_retries(candidate_id,source,child_job_id,created_at) VALUES (?,?,?,?)",
                         (candidate_id, source, child_id, timestamp()))

    def report(self, job_id):
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM discovery_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                return None
            result = {k: row[k] for k in row.keys() if k not in {"owner_pid", "heartbeat"}}
            result["sources"] = json.loads(result["sources"])
            result["probes_used"] = result["probes"]
            result["index_errors"] = json.loads(result["index_errors"])
            result["counts"] = {r[0]: r[1] for r in conn.execute("SELECT state,COUNT(*) FROM discovery_candidates WHERE job_id=? GROUP BY state", (job_id,))}
            stats = conn.execute("SELECT COUNT(*),COALESCE(SUM(goods_count),0) FROM discovery_shops WHERE job_id=?", (job_id,)).fetchone()
            result.update(success_shops=stats[0], collected_goods=stats[1], discovered_links=sum(result["counts"].values()))
            result["failures"] = [dict(r) for r in conn.execute("SELECT url,state,reason,source,failure_json FROM discovery_candidates WHERE job_id=? AND state IN ('failed','blocked','rejected') AND reason IS NOT NULL ORDER BY updated_at DESC,id DESC LIMIT 10", (job_id,))]
            for item in result["failures"]:
                item["failure"] = normalize_failure(item.pop("failure_json"), item["reason"])
            progress = {source: {"running": 0, "waiting": 0, "skipped": 0, "failed": 0,
                                 "success_shops": 0, "collected_goods": 0, "reason": None, "failure": None,
                                 "last_event": None}
                        for source in result["sources"]}
            for r in conn.execute("SELECT source,state,COUNT(*) n FROM discovery_candidates WHERE job_id=? GROUP BY source,state", (job_id,)):
                if r["source"] in progress:
                    field = {"probing_shop":"running","qualified":"waiting","collecting": "running", "pending": "waiting", "ready": "waiting", "waiting": "waiting", "blocked": "skipped", "failed": "failed"}.get(r["state"])
                    if field:
                        progress[r["source"]][field] += r["n"]
            for r in conn.execute("SELECT source,COUNT(*) n,SUM(goods_count) goods FROM discovery_shops WHERE job_id=? GROUP BY source", (job_id,)):
                if r["source"] in progress:
                    progress[r["source"]].update(success_shops=r["n"], collected_goods=r["goods"])
            for source in progress:
                latest = conn.execute("SELECT reason,failure_json FROM discovery_candidates WHERE job_id=? AND source=? AND state IN ('failed','blocked') AND reason IS NOT NULL ORDER BY updated_at DESC,id DESC LIMIT 1", (job_id, source)).fetchone()
                if latest:
                    progress[source].update(reason=latest["reason"], failure=normalize_failure(latest["failure_json"], latest["reason"]))
                event = conn.execute("SELECT url,updated_at FROM discovery_candidates WHERE job_id=? AND source=? AND state='collected' ORDER BY updated_at DESC,id DESC LIMIT 1", (job_id, source)).fetchone()
                if event:
                    progress[source]["last_event"] = dict(event)
                state = progress[source]
                state["activity"] = ("collecting" if state["running"] else "waiting" if state["waiting"]
                                     else "blocked" if state["skipped"] else "waiting_entry" if result["status"] in ACTIVE
                                     else "finished")
            result["source_progress"] = progress
            if result.get('workflow_version')==2:
                result.update(self.previews.summary(job_id))
            else:
                result['phase']=None
            return result

    def jobs(self, limit=20):
        with self.connection() as conn:
            ids = [r[0] for r in conn.execute("SELECT job_id FROM discovery_jobs ORDER BY created_at DESC,rowid DESC LIMIT ?", (limit,))]
        return [self.report(job_id) for job_id in ids]

    def candidates(self, job_id, page=1, size=50, state=None):
        args = [job_id]
        where = "job_id=?"
        if state:
            where += " AND state=?"
            args.append(state)
        with self.connection() as conn:
            total = conn.execute("SELECT COUNT(*) FROM discovery_candidates WHERE " + where, args).fetchone()[0]
            rows = [dict(r) for r in conn.execute("SELECT * FROM discovery_candidates WHERE " + where + " ORDER BY id LIMIT ? OFFSET ?", [*args, size, (page - 1) * size])]
            for row in rows:
                failure = row.pop("failure_json")
                row["failure"] = (normalize_failure(failure, row["reason"])
                                  if failure or row["state"] in {"failed", "blocked"} else None)
                row["parents"] = [r[0] for r in conn.execute("SELECT parent_url FROM discovery_edges WHERE candidate_id=? ORDER BY parent_url", (row["id"],))]
                row["retries"] = [dict(r) for r in conn.execute("SELECT source,child_job_id,created_at FROM discovery_retries WHERE candidate_id=? ORDER BY id", (row["id"],))]
        return {"total": total, "rows": rows}


class DiscoveryManager:
    def __init__(self, managers, path=None, page_session_factory=None, detector=None, index_readers=None, workflow_version=2):
        self.workflow_version=workflow_version
        self.managers = managers
        self.store = DiscoveryStore(path)
        self.page_session_factory = page_session_factory or SafeSession
        self.detector = detector
        self.index_readers = index_readers
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.shutting_down = False
        self.thread = None
        self.job_id = None

    def _manager(self, source):
        return self.managers(source) if callable(self.managers) else self.managers[source]

    def recover_interrupted(self):
        self.store.recover()

    def start(self, urls=None, sources=None, include_indexes=True, max_depth=2, page_limit=200, parallel_sources=4, probe_limit=200):
        with self.lock:
            self.store.recover()
            job_id = self.store.create(urls or [], list(SOURCES) if sources is None else sources, include_indexes, max_depth, page_limit, parallel_sources, probe_limit, self.workflow_version)
            self._launch(job_id)
        return self.store.report(job_id)

    def _launch(self, job_id):
        self.job_id, self.stop, self.shutting_down = job_id, threading.Event(), False
        self.thread = threading.Thread(target=run_worker,args=('discovery',self.store.path,job_id,self._run,(job_id,self.stop)), daemon=True, name="catalog-discovery")
        try:
            self.thread.start()
        except Exception:
            self.store.update_job(job_id, status="failed", error="无法启动发现线程")
            raise

    def cancel(self, job_id):
        with self.lock:
            self.store.cancel(job_id)
            if job_id == self.job_id:
                self.stop.set()
        return self.store.report(job_id)

    def resume(self, job_id, extra_pages=0, max_depth=None, extra_probes=0, retry_mode="transient"):
        with self.lock:
            self.store.recover()
            self.store.resume(job_id, extra_pages, max_depth, extra_probes, retry_mode)
            self._launch(job_id)
        return self.store.report(job_id)

    def retry_candidate(self, job_id, candidate_id):
        item = self.store.candidate(job_id, candidate_id)
        if item is None:
            raise KeyError(candidate_id)
        source = item["source"] or native_source(item["url"])
        if not source:
            try:
                source = self.store.origin_cache(job_id).get(origin_of(item["url"]))
            except ValueError:
                pass  # Invalid historical entries can be corrected in the prefilled form.
        if not source:
            return {"action": "rediscover", "prefill": {"urls": [item["url"]], "include_indexes": False},
                    "message": "此入口尚未确认平台，可单独重新发现"}
        if source not in SOURCES:
            raise ValueError("该入口不支持单独整店采集")
        if item["state"] in {"probing", "collecting"}:
            raise JobConflict("此入口正在处理，请等待本次处理结束")
        manager = self._manager(source)
        report = manager.start("targets", [item["url"]], reuse=False,
                               discovery_owner=f"retry:{job_id}:{candidate_id}:{uuid.uuid4().hex}")
        self.store.add_retry(candidate_id, source, report["job_id"])
        return {"action": "collect", "source": source, "job": report}

    def shutdown(self):
        self.shutting_down = True
        self.stop.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def _seed_known(self, job, stop):
        if not job["include_indexes"]:
            return
        for source in job["sources"]:
            if stop.is_set():
                raise CrawlCancelled()
            manager = self._manager(source)
            self.store.enqueue(job["job_id"], manager.store.known_urls(True), "known:" + source)
            if job.get('workflow_version')==2:
                self.store.enqueue(job['job_id'],self.store.previews.known_urls(source),'saved-directory:'+source,source_hint=source)
            if getattr(manager.platform, "MULTI_ORIGIN", False):
                # A completed visit proves this adapter actually read a full shop.
                # Imported or uncertain historical rows deliberately remain unknown.
                known = manager.store.rows("SELECT DISTINCT s.link FROM wzyp_shops s JOIN crawl_visits v "
                                           "ON v.shop_no=s.token WHERE v.status='done' AND s.fetch_done=1")
                for row in known:
                    self.store.enqueue(job["job_id"], [row["link"]], "verified:" + source, source_hint=source)
                    self.store.remember_origin(job["job_id"], origin_of(row["link"]), source)
            for edge in manager.store.rows("SELECT url,parent_url FROM shop_links"):
                self.store.enqueue(job["job_id"], [edge["url"]], edge["parent_url"], 0)
            for row in manager.store.rows("SELECT link,description FROM wzyp_goods_detail WHERE description IS NOT NULL AND description!='' UNION ALL SELECT link,description FROM wzyp_shops WHERE description IS NOT NULL AND description!=''"):
                self.store.enqueue(job["job_id"], extract_links(row["description"], row["link"]), row["link"], 0)
        if "catfk" in job["sources"]:
            self.store.enqueue(job["job_id"], self.store.prior_urls("catfk"), "previous:catfk")

    def _read_index(self, source, stop):
        from crawler16688 import discover_public_links
        manager = self._manager(source)
        reader = (self.index_readers or {}).get(source, discover_public_links if source == "16688" else manager.discover)
        with manager.index_session_factory(stop) as session:
            return reader(session, lambda *args: None)

    def _detect(self, url, stop, cache, session=None, preview_only=False):
        native = native_source(url)
        if native:
            return native
        origin = origin_of(url)
        if origin in cache:
            return cache[origin]
        if self.detector:
            return self.detector(url, stop)
        if session is None:
            with self.page_session_factory(stop=stop) as client:
                return self._detect(url, stop, cache, _ProbeSession(client),preview_only)
        for source in ("dujiao-next", "dujiaoka"):
            adapter = adapter_for(source)
            try:
                resolved=adapter.resolve_target(url, session,preview_only=True) if preview_only else adapter.resolve_target(url,session)
                if preview_only: session.resolved[source]=resolved
                return source
            except adapter.PlatformError as exc:
                if from_exception(exc)["category"] in {"network", "rate_limit", "server_error", "snapshot_changed", "origin_unavailable", "access_blocked"}:
                    raise
        return None

    def _enqueue_edges(self, job, edges, depth):
        for edge in edges:
            try:
                step = int(origin_of(edge["url"]) != origin_of(edge["parent_url"]))
            except ValueError:
                step = 1
            self.store.enqueue(job["job_id"], [edge["url"]], edge["parent_url"], depth + step)

    @staticmethod
    def _origin_key(url):
        source = native_source(url)
        return adapter_for(source).BASE_URL if source else origin_of(url)

    def _probe(self, job, item, stop, cache):
        origin = origin_of(item["url"])
        if origin not in cache and not self.store.reserve_probe(job["job_id"], origin):
            return {"state": "probe_deferred", "reason": "新源站核验预算已用完，可追加额度继续"}
        with self.page_session_factory(stop=stop) as client:
            session = _ProbeSession(client)
            source = item.get("source") or self._detect(item["url"], stop, cache, session,job.get('workflow_version')==2)
            if stop.is_set():
                raise CrawlCancelled()
            if origin not in cache:
                # The origin verdict remains useful even if this particular
                # directory URL later returns 404 or exhausts its page budget.
                self.store.remember_origin(job["job_id"], origin, source)
            if source:
                if job.get('workflow_version')==2 and source in job['sources']:
                    preview=self._manager(source).platform.probe_shop(item['url'],session,resolved=session.resolved.get(source))
                    return {'source':source,'verified':True,'preview':preview.normalized()}
                return {"source": source, "verified": True}
            if not self.store.reserve_page(job["job_id"]):
                return {"state": "deferred", "verified": True, "reason": "目录页预算已用完，可追加目录额度继续"}
            response = session.get(item["url"])
        if response.status_code != 200:
            raise CollectionError(f"目录页返回 HTTP {response.status_code}", failure=http_failure(response))
        content_type = response.headers.get("Content-Type", "text/html").lower()
        if not any(kind in content_type for kind in ("html", "text/plain")):
            return {"state": "unsupported", "verified": True, "reason": "既非已支持货铺，也不是公开 HTML/文本目录"}
        parent = getattr(response, "url", "") or item["url"]
        links = [url for url in extract_links(response.text, parent)
                 if not any(part in urlsplit(url).path.lower().split("/") for part in ("admin", "auth", "login", "cart", "order", "pay", "user"))]
        return {"state": "directory" if links else "unsupported", "parent": parent, "links": links,
                "verified": True, "reason": None if links else "未识别到受支持货铺或可扩展链接"}

    def _accept_probe(self, job, item, result, cache):
        source = result.get("source")
        if source or result.get("verified"):
            origin = origin_of(item["url"])
            if origin not in cache or cache[origin] != source:
                self.store.remember_origin(job["job_id"], origin, source)
            cache[origin] = source
        if source:
            enabled = source in job["sources"]
            if enabled and result.get('preview'):
                self._accept_preview(job,item,source,result['preview'])
                return
            self.store.update_candidate(item["id"], "ready" if enabled else "unsupported", source=source,
                                        reason=None if enabled else "该平台未在本任务中启用")
        else:
            parent = result.get("parent", item["url"])
            self._enqueue_edges(job, [{"url": url, "parent_url": parent} for url in result.get("links", [])], item["depth"])
            self.store.update_candidate(item["id"], result["state"], reason=result.get("reason"),
                                        failure_json=json.dumps(failure_details("unsupported_template")) if result["state"] == "unsupported" else None)

    def _scan_shop(self,job,item,source,stop):
        manager=self._manager(source)
        norm=manager.platform.normalize_target(item['url'])
        key=manager.store.lookup_shop(norm['kind'],norm['target'])
        previous=self.store.previews.get(job['job_id'],source,key) if key else None
        if previous: return json.loads(previous['preview_json'])
        with manager.session_factory(stop) as session:
            preview=manager.platform.probe_shop(manager.shop_entry_url(key,item['url']),session)
        data=preview.normalized()
        return data

    def _accept_preview(self,job,item,source,data):
        self.store.previews.save(job['job_id'],source,data)
        manager=self._manager(source)
        norm=manager.platform.normalize_target(item['url'])
        manager.store.remember({'kind':norm['kind'],'target':norm['target']},{'shop_no':data['shop_key']},goods=data['products'])
        with self.store.connection() as conn:
            duplicate=conn.execute("SELECT 1 FROM discovery_candidates WHERE job_id=? AND source=? AND shop_key=? "
                "AND id!=? AND state IN ('qualified','ready','waiting','collecting','collected') LIMIT 1",
                (job['job_id'],source,data['shop_key'],item['id'])).fetchone()
        state='duplicate' if duplicate else 'qualified' if data['eligibility']=='eligible' else data['eligibility']
        reason={'empty':'公开目录没有商品','no_stock':'完整目录中的商品均已明确缺货',
                'duplicate':'本轮已保存该店铺目录'}.get(state)
        if state=='qualified' and data['stock_status']=='unknown': reason='已保存目录，库存待第二阶段核实'
        self.store.update_candidate(item['id'],state,source=source,shop_key=data['shop_key'],reason=reason)
        self.store.skip_saved_urls(job['job_id'],source,data['shop_key'],[data['url'],*(p['link'] for p in data['products'] if p.get('link'))])

    def _reuse_saved_directories(self,job,stop):
        for source in job['sources']:
            if stop.is_set():raise CrawlCancelled()
            previews=self.store.previews.list(job['job_id'],source,size=100000)['rows']
            if not previews:continue
            manager=self._manager(source)
            by_shop={row['shop_key']:[row['url'],*(p['link'] for p in row['preview']['products'] if p.get('link'))] for row in previews}
            for row in manager.store.rows('SELECT shop_token AS shop_key,link FROM wzyp_goods UNION ALL SELECT token AS shop_key,link FROM wzyp_shops'):
                if row['shop_key'] in by_shop:by_shop[row['shop_key']].append(row['link'])
            for row in previews:
                if stop.is_set():raise CrawlCancelled()
                norm=manager.platform.normalize_target(row['url'])
                manager.store.remember({'kind':norm['kind'],'target':norm['target']},{'shop_no':row['shop_key']},goods=row['preview']['products'])
                self.store.skip_saved_urls(job['job_id'],source,row['shop_key'],by_shop[row['shop_key']])

    def _start_child(self, job, candidate, source, scope):
        manager = self._manager(source)
        norm = manager.platform.normalize_target(candidate["url"])
        key = manager.store.lookup_shop(norm["kind"], norm["target"])
        ownership = job["job_id"] + ":" + str(candidate["id"])
        child_id = candidate.get("child_job_id")
        if not child_id:
            owned = manager.store.rows("SELECT job_id FROM crawl_jobs WHERE discovery_owner=? ORDER BY created_at DESC LIMIT 1", (ownership,))
            child_id = owned[0]["job_id"] if owned else None
        if not child_id and key and self.store.has_shop(job["job_id"], source, key):
            self.store.update_candidate(candidate["id"], "duplicate", source=source, shop_key=key, reason="本轮已完整采集该店铺")
            return None
        try:
            report = manager.store.report(child_id) if child_id else None
            if report:
                owner = manager.store.rows("SELECT discovery_owner FROM crawl_jobs WHERE job_id=?", (child_id,))[0]["discovery_owner"]
                if owner != ownership:
                    self.store.update_candidate(candidate["id"], "failed", reason="子任务归属不一致，未操作其他任务")
                    return None
                if report["status"] not in ACTIVE and report["status"] != "completed":
                    # The parent already chose this candidate using its retry mode.
                    report = manager.resume(child_id, transport_scope=scope, retry_mode="all")
            else:
                report = manager.start("targets", [candidate["url"]], reuse=False,
                                       discovery_owner=ownership, transport_scope=scope)
                child_id = report["job_id"]
        except JobConflict:
            self.store.update_candidate(candidate["id"], "waiting", source=source)
            return None
        self.store.update_candidate(candidate["id"], "collecting", source=source, child_job_id=child_id, reason=None)
        if key: self.store.previews.collection(job['job_id'],source,key,'collecting',child_id)
        return {"candidate": candidate, "source": source, "child_id": child_id}

    def _finish_child(self, job, child, report, blocked):
        source, item = child["source"], child["candidate"]
        manager = self._manager(source)
        visits = manager.store.rows("SELECT shop_no,goods_count FROM crawl_visits WHERE job_id=? AND status='done'", (child["child_id"],))
        for visit in visits:
            edges = manager.store.rows("SELECT url,parent_url FROM shop_links WHERE shop_token=?", (visit["shop_no"],))
            if job.get('workflow_version')!=2:
                self._enqueue_edges(job, edges, item["depth"])
            self.store.add_shop(job["job_id"], source, visit["shop_no"], visit["goods_count"])
            if job.get('workflow_version')==2:
                self.store.previews.collection(job['job_id'],source,visit['shop_no'],'collected',child['child_id'])
        norm = manager.platform.normalize_target(item["url"])
        key = manager.store.lookup_shop(norm["kind"], norm["target"])
        success = key and self.store.has_shop(job["job_id"], source, key)
        error = report.get("last_error") or (None if success else "未能完整采集该货铺，已保留旧快照")
        failure = normalize_failure(report.get("failure"), error) if not success else None
        is_blocked = bool(failure and failure["pause_origin"])
        if is_blocked:
            blocked[self._origin_key(item["url"])] = (error, failure)
        self.store.update_candidate(item["id"], "collected" if success else "blocked" if is_blocked else "failed",
                                    source=source, shop_key=key, child_job_id=child["child_id"], reason=error,
                                    failure_json=json.dumps(failure) if failure else None)
        if not success and key:
            self.store.previews.collection(job['job_id'],source,key,'failed',child['child_id'])

    def _run(self, job_id, stop):
        from hosted_http import TransportScope
        scope = TransportScope()
        stop.transport_scope = scope
        pulse_done = threading.Event()
        def pulse():
            while not pulse_done.wait(1):
                try:
                    if self.store.heartbeat(job_id):stop.set()
                except sqlite3.OperationalError as error:
                    if not is_busy(error):raise
        pulse_thread = threading.Thread(target=pulse, daemon=True, name="discovery-heartbeat")
        pulse_thread.start()
        probes_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="shop-probe")
        shops_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix='shop-directory')
        index_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="shop-index")
        probes, indexes, active, cache, blocked, scans = {}, {}, {}, {}, {}, {}
        probe_failures = {}
        cursor = 0
        terminal_status, terminal_error = "failed", None
        try:
            job = self.store.report(job_id)
            staged=job.get('workflow_version')==2
            scanning=staged and job['phase']=='shop_discovery'
            if scanning:self._reuse_saved_directories(job,stop)
            if not job["initialized"] and (not staged or scanning):
                self._seed_known(job, stop)
                if job["include_indexes"]:
                    index_sources = ["16688"] + (["wzyp-live"] if "wzyp-live" in job["sources"] else [])
                    indexes = {index_pool.submit(self._read_index, source, stop): source for source in index_sources}
                self.store.update_job(job_id, index_errors="[]")
                if not indexes:
                    self.store.update_job(job_id, initialized=1)
            cache = self.store.origin_cache(job_id)
            index_errors = []
            while not stop.is_set():
                for source,(item,future) in list(scans.items()):
                    if not future.done(): continue
                    scans.pop(source)
                    try:
                        self._accept_preview(job,item,source,future.result())
                    except CrawlCancelled:
                        self.store.update_candidate(item['id'],'ready',source=source)
                        raise
                    except Exception as exc:
                        failure=from_exception(exc)
                        if failure['pause_origin']: blocked[self._origin_key(item['url'])]=(str(exc)[:400],failure)
                        self.store.update_candidate(item['id'],'blocked' if failure['pause_origin'] else 'failed',source=source,
                            reason=str(exc)[:400],failure_json=json.dumps(failure))
                for future in [future for future in indexes if future.done()]:
                    source = indexes.pop(future)
                    try:
                        self.store.enqueue(job_id, future.result(), "index:" + source)
                    except CrawlCancelled:
                        raise
                    except Exception as exc:
                        index_errors.append({"source": source, "error": str(exc)[:300]})
                    self.store.update_job(job_id, index_errors=json.dumps(index_errors, ensure_ascii=False), initialized=int(not indexes))
                for future in [future for future in probes if future.done()]:
                    item = probes.pop(future)
                    try:
                        self._accept_probe(job, item, future.result(), cache)
                    except CrawlCancelled:
                        raise
                    except CrawlBlockedError as exc:
                        blocked[self._origin_key(item["url"])] = (str(exc), exc.failure)
                        self.store.update_candidate(item["id"], "blocked", reason=str(exc)[:400],
                                                    failure_json=json.dumps(exc.failure) if exc.failure else None)
                    except Exception as exc:
                        failure = from_exception(exc)
                        origin = origin_of(item["url"])
                        verdict = self.store.origin_verdict(job_id, origin)
                        if verdict is not None:
                            cache[origin] = verdict["source"]
                        else:
                            probe_failures[origin] = (str(exc)[:400], failure)
                        if failure["pause_origin"]:
                            blocked[self._origin_key(item["url"])] = (str(exc), failure)
                        self.store.update_candidate(item["id"], "blocked" if failure["pause_origin"] else "failed", reason=str(exc)[:400],
                                                    failure_json=json.dumps(failure))
                for source, child in list(active.items()):
                    report = self._manager(source).store.report(child["child_id"])
                    if report["status"] not in ACTIVE:
                        self._finish_child(job, child, report, blocked)
                        active.pop(source)
                # Native URLs are classified without network requests. Unknown domains get
                # separate probe slots, so a slow directory cannot hold a platform's slot.
                for _ in range(100 if not staged or scanning else 0):
                    if stop.is_set():
                        break
                    excluded = {origin_of(item["url"]) for item in probes.values()}
                    item = self.store.claim_candidate(job_id, excluded, known_only=len(probes) >= 2)
                    if item is None:
                        break
                    if item["depth"] > job["max_depth"]:
                        self.store.update_candidate(item["id"], "deferred", reason="超过跨站扩展深度，可提高深度后继续")
                        continue
                    origin = self._origin_key(item["url"])
                    if origin in blocked:
                        error, failure = blocked[origin]
                        self.store.update_candidate(item["id"], "blocked", reason=error, source=item.get("source") or native_source(item["url"]),
                                                    failure_json=json.dumps(failure) if failure else None)
                        continue
                    source = item.get("source") or native_source(item["url"]) or cache.get(origin_of(item["url"]))
                    if source:
                        self._accept_probe(job, item, {"source": source}, cache)
                    elif origin_of(item["url"]) not in cache and origin_of(item["url"]) in probe_failures:
                        error, failure = probe_failures[origin_of(item["url"])]
                        self.store.update_candidate(item["id"], "failed", reason=error, failure_json=json.dumps(failure))
                    elif origin_of(item["url"]) not in cache and not self.store.reserve_probe(job_id, origin_of(item["url"])):
                        # Reserve in queue order, before submitting parallel work. Exhausted
                        # probes need no worker slot and cannot delay known platform jobs.
                        self.store.update_candidate(item["id"], "probe_deferred", reason="新源站核验预算已用完，可追加额度继续")
                    else:
                        probes[probes_pool.submit(self._probe, job, item, stop, dict(cache))] = item
                # Round-robin, one active child per platform. JobConflict never waits here.
                for _ in range(len(job["sources"])):
                    if len(active)+len(scans) >= job["parallel_sources"] or stop.is_set():
                        break
                    source = job["sources"][cursor % len(job["sources"])]
                    cursor += 1
                    if source in active or source in scans:
                        continue
                    candidate = self.store.ready_candidate(job_id, source)
                    if not candidate:
                        continue
                    origin = self._origin_key(candidate["url"])
                    if origin in blocked:
                        error, failure = blocked[origin]
                        self.store.update_candidate(candidate["id"], "blocked", reason=error,
                                                    failure_json=json.dumps(failure) if failure else None)
                        continue
                    try:
                        if scanning:
                            external=self._manager(source).store.report()
                            if external and external['status'] in ACTIVE:
                                self.store.update_candidate(candidate['id'],'waiting',source=source)
                                continue
                            self.store.update_candidate(candidate['id'],'probing_shop',source=source)
                            scans[source]=(candidate,shops_pool.submit(self._scan_shop,job,candidate,source,stop))
                            continue
                        child = self._start_child(job, candidate, source, scope)
                        if child:
                            active[source] = child
                    except Exception as exc:
                        self.store.update_candidate(candidate["id"], "failed", reason=str(exc)[:400], failure_json=json.dumps(from_exception(exc)))
                with self.store.connection() as conn:
                    waiting = conn.execute("SELECT 1 FROM discovery_candidates WHERE job_id=? AND state IN ('pending','ready','waiting') LIMIT 1", (job_id,)).fetchone()
                if not indexes and not probes and not scans and not active and not waiting:
                    if scanning:
                        with self.store.connection(write=True) as conn:
                            if conn.execute('SELECT cancel_requested FROM discovery_jobs WHERE job_id=?',(job_id,)).fetchone()[0]:
                                stop.set()
                            else:
                                conn.execute("UPDATE discovery_jobs SET phase='goods_collection',updated_at=? WHERE job_id=?",(timestamp(),job_id))
                                conn.execute("UPDATE discovery_candidates SET state='ready' WHERE job_id=? AND state='qualified'",(job_id,))
                        if not stop.is_set():
                            scanning=False
                            continue
                    break
                stop.wait(0.05)
            if stop.is_set():
                raise CrawlCancelled()
            report = self.store.report(job_id)
            incomplete = any(report["counts"].get(state) for state in ("failed", "blocked", "deferred", "probe_deferred")) or report["index_errors"]
            status = ("partial" if report["success_shops"] or report["counts"].get("deferred") or report["counts"].get("probe_deferred") else "failed") if incomplete else "completed"
            terminal_status = status
        except CrawlCancelled:
            terminal_status = "interrupted" if self.shutting_down else "cancelled"
        except Exception as exc:
            terminal_status, terminal_error = "failed", str(exc)[:400]
        finally:
            # Stop only owned children, including when preparation/probing raises unexpectedly.
            stop.set()
            for source, child in active.items():
                manager = self._manager(source)
                child_report = manager.store.report(child["child_id"])
                if not child_report or child_report["status"] not in ACTIVE:
                    continue
                manager.cancel(child["child_id"], interrupt=self.shutting_down)
                manager.wait_for_job(child["child_id"])
            probes_pool.shutdown(wait=True, cancel_futures=True)
            shops_pool.shutdown(wait=True, cancel_futures=True)
            for source,(item,_) in scans.items():
                self.store.update_candidate(item['id'],'ready',source=source)
            index_pool.shutdown(wait=True, cancel_futures=True)
            scope.close()
            pulse_done.set()
            pulse_thread.join(timeout=2)
            # Only expose a terminal state after owned workers have stopped. A resume
            # cannot then race old probes that still write to this same job's queue.
            self.store.update_job(job_id, status=terminal_status, error=terminal_error, finished_at=timestamp())
