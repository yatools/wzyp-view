"""平台采集队列与检查点；这些表不向 AI 的只读查询开放。"""
import os
import json
import time
import uuid
from itertools import islice
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import local_store
from sqlite_init import write_transaction
from task_lifecycle import worker_alive,active_job
import platform16688 as default_platform
from crawl_failure import normalize as normalize_failure, should_retry, validate_retry_mode

ACTIVE = ("running", "cancelling")


def timestamp():
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


def process_alive(pid):
    if pid == os.getpid():
        return True
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return ctypes.get_last_error() == 5  # 无查询权限的进程仍可能活着。
        try:
            code = wintypes.DWORD()
            return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


class JobConflict(Exception):
    pass


class CrawlStore:
    def __init__(self, path=None, platform_adapter=None):
        self.platform = platform_adapter or default_platform
        if path:
            self.path = path
        elif hasattr(self.platform, "database_path"):
            self.path = self.platform.database_path()
        else:
            self.path = local_store.database_path()

    @contextmanager
    def transaction(self):
        conn = local_store.connect(self.path)
        try:
            with write_transaction(conn,self.path):
                yield conn
        finally:
            conn.close()

    def rows(self, sql, args=()):
        conn = local_store.connect(self.path)
        try:
            return [dict(row) for row in conn.execute(sql, args)]
        finally:
            conn.close()

    def recover(self):
        with self.transaction() as conn:
            for row in conn.execute("SELECT * FROM crawl_jobs WHERE status IN ('running','cancelling')").fetchall():
                if worker_alive('crawl',self.path,row['job_id']):continue
                if not process_alive(row["owner_pid"]) or time.time() - row["heartbeat"] > 90:
                    conn.execute("UPDATE crawl_jobs SET status='interrupted', phase=CASE WHEN workflow_version=2 THEN phase ELSE 'interrupted' END, "
                                 "updated_at=?, error='服务已停止，点击继续采集可恢复' WHERE job_id=?",
                                 (timestamp(), row["job_id"]))

    def _enqueue(self, conn, job_id, urls, origin):
        for value in urls:
            try:
                target = self.platform.normalize_target(value)
            except ValueError:
                continue
            conn.execute("INSERT OR IGNORE INTO crawl_items "
                         "(job_id,kind,target,url,origin,updated_at) VALUES (?,?,?,?,?,?)",
                         (job_id, target["kind"], target["target"], target["url"], origin, timestamp()))

    def enqueue(self, job_id, urls, origin):
        entries=iter(urls)
        while batch:=list(islice(entries,200)):
            with self.transaction() as conn:
                self._enqueue(conn, job_id, batch, origin)

    def create(self, mode, urls, reuse=True, discovery_owner=None, workflow_version=1):
        with self.transaction() as conn:
            row = conn.execute("SELECT job_id,status,phase,workflow_version FROM crawl_jobs WHERE status IN ('running','cancelling')").fetchone()
            if row:
                if not reuse:
                    raise JobConflict("已有平台采集任务正在运行，发现任务等待其完成")
                if row["status"] == "running":
                    before=conn.execute('SELECT COALESCE(MAX(id),0) FROM crawl_items').fetchone()[0]
                    self._enqueue(conn, row["job_id"], urls, "manual")
                    if row['workflow_version']==2 and row['phase']=='goods_collection':
                        conn.execute("UPDATE crawl_items SET status='next_round' WHERE job_id=? AND id>?",(row['job_id'],before))
                return row["job_id"], True
            if active_job('crawl',self.path):
                raise JobConflict('已有平台任务工作线程仍在运行或退出，请稍后重试')
            job_id = uuid.uuid4().hex
            now = timestamp()
            conn.execute("INSERT INTO crawl_jobs "
                         "(job_id,mode,status,phase,created_at,updated_at,owner_pid,heartbeat,discovery_owner) "
                         "VALUES (?,?,'running',?,?,?,?,?,?)",
                         (job_id, mode, "discovering" if mode == "discover" else "collecting",
                          now, now, os.getpid(), time.time(), discovery_owner))
            self._enqueue(conn, job_id, urls, "manual")
            if mode == 'discover' and workflow_version == 2:
                conn.execute("UPDATE crawl_jobs SET workflow_version=2,phase='shop_discovery' WHERE job_id=?", (job_id,))
        return job_id, False

    def resume(self, job_id, retry_mode="transient"):
        validate_retry_mode(retry_mode)
        with self.transaction() as conn:
            job = conn.execute("SELECT * FROM crawl_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job:
                raise KeyError(job_id)
            if job['status'] not in ACTIVE and worker_alive('crawl',self.path,job_id):
                raise JobConflict('任务工作线程仍在退出，请稍后继续，避免重复运行')
            active = conn.execute("SELECT job_id FROM crawl_jobs WHERE status IN ('running','cancelling')").fetchone()
            if active:
                if active["job_id"] == job_id:
                    return True
                raise JobConflict("已有平台采集任务正在运行")
            if job["status"] == "completed":
                raise JobConflict("任务已完成，请创建新任务刷新数据")
            items = conn.execute("SELECT * FROM crawl_items WHERE job_id=? AND status IN ('working','failed')", (job_id,)).fetchall()
            for item in items:
                if item["status"] == "failed" and not should_retry(item["failure_json"], item["error"], retry_mode):
                    continue
                conn.execute("UPDATE crawl_items SET status='pending',error=NULL,failure_json=NULL,updated_at=? WHERE id=?",
                             (timestamp(), item["id"]))
                if item["shop_no"]:
                    conn.execute("DELETE FROM crawl_visits WHERE job_id=? AND shop_no=? AND status!='done'", (job_id, item["shop_no"]))
            conn.execute("DELETE FROM crawl_visits WHERE job_id=? AND status='working'", (job_id,))
            probe_pending=bool(conn.execute("SELECT 1 FROM crawl_items WHERE job_id=? AND status='pending' AND shop_no IS NULL LIMIT 1",(job_id,)).fetchone())
            conn.execute("UPDATE crawl_jobs SET status='running',cancel_requested=0,owner_pid=?,heartbeat=?,"
                         "phase=?,updated_at=?,finished_at=NULL,error=NULL,failure_json=NULL WHERE job_id=?",
                         (os.getpid(), time.time(), ('shop_discovery' if job['discovery_error'] or probe_pending else job['phase']) if job['workflow_version']==2 else "collecting" if job["discovery_done"] or job["mode"] == "refresh"
                          else "discovering", timestamp(), job_id))
        return False

    def known_urls(self, include_discovered=False):
        if getattr(self.platform, "MULTI_ORIGIN", False):
            urls = {row["link"] for row in self.rows("SELECT link FROM wzyp_shops")}
        else:
            urls = {f"{self.platform.BASE_URL}/shop/{row['token']}" for row in self.rows("SELECT token FROM wzyp_shops")}
        if include_discovered:
            urls.update(row["url"] for row in self.rows("SELECT DISTINCT url FROM crawl_items"))
        return sorted(urls)

    def history(self, limit=20):
        return [self.report(row["job_id"]) for row in self.rows(
            "SELECT job_id FROM crawl_jobs ORDER BY created_at DESC,rowid DESC LIMIT ?", (limit,))]

    def heartbeat(self, job_id):
        with self.transaction() as conn:
            conn.execute("UPDATE crawl_jobs SET heartbeat=? WHERE job_id=?", (time.time(), job_id))
            return bool(conn.execute("SELECT cancel_requested FROM crawl_jobs WHERE job_id=?", (job_id,)).fetchone()[0])

    def discovery_progress(self, job_id, version, pages):
        with self.transaction() as conn:
            conn.execute("UPDATE crawl_jobs SET discovery_version=?,discovery_pages=?,updated_at=?,heartbeat=? WHERE job_id=?",
                         (version, pages, timestamp(), time.time(), job_id))

    def discovery_finished(self, job_id, urls=None, error=None, origin=None):
        default_origin = getattr(self.platform, "DISCOVERY_ORIGIN", "pricemapai-public-index")
        with self.transaction() as conn:
            if urls is not None:
                self._enqueue(conn, job_id, urls, origin or default_origin)
            conn.execute("UPDATE crawl_jobs SET discovery_done=?,discovery_error=?,phase=CASE WHEN workflow_version=2 THEN phase ELSE 'collecting' END,updated_at=? WHERE job_id=?",
                         (int(error is None), error, timestamp(), job_id))

    def claim_item(self, job_id):
        with self.transaction() as conn:
            job = conn.execute("SELECT status,cancel_requested FROM crawl_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job or job["status"] != "running" or job["cancel_requested"]:
                return None
            row = conn.execute("SELECT * FROM crawl_items WHERE job_id=? AND status='pending' "
                               "ORDER BY CASE kind WHEN 'shop' THEN 0 ELSE 1 END,id LIMIT 1", (job_id,)).fetchone()
            if not row:
                return None
            conn.execute("UPDATE crawl_items SET status='working',updated_at=? WHERE id=?", (timestamp(), row["id"]))
            return dict(row)

    def lookup_shop(self, kind, target):
        rows = self.rows("SELECT shop_no FROM crawl_aliases WHERE kind=? AND target=?", (kind, target))
        if rows:
            return rows[0]["shop_no"]
        if kind == "goods":
            rows = self.rows("SELECT shop_token AS shop_no FROM wzyp_goods WHERE goods_key=?", (target,))
        else:
            rows = self.rows("SELECT token AS shop_no FROM wzyp_shops WHERE token=?", (target,))
        return rows[0]["shop_no"] if rows else None

    def remember(self, item, shop,goods=()):
        aliases = {(item["kind"], item["target"]), ("shop", shop["shop_no"])}
        if shop.get("shop_alias"):
            aliases.add(("shop", shop["shop_alias"]))
        for good in goods:
            try:
                target=self.platform.normalize_target(good.get('link') or '')
                if target['kind']=='goods' and target['target']==str(good.get('goods_key')):
                    aliases.add(('goods',target['target']))
            except ValueError:continue
        with self.transaction() as conn:
            conn.executemany("INSERT INTO crawl_aliases(kind,target,shop_no) VALUES (?,?,?) "
                             "ON CONFLICT(kind,target) DO UPDATE SET shop_no=excluded.shop_no",
                             [(kind, target, shop["shop_no"]) for kind, target in aliases])

    def claim_shop(self, job_id, shop_no):
        with self.transaction() as conn:
            return conn.execute("INSERT OR IGNORE INTO crawl_visits(job_id,shop_no,status) VALUES (?,?,'working')",
                                (job_id, shop_no)).rowcount == 1

    def finish_item(self, item, status, shop_no=None, error=None, result=None, failure=None):
        with self.transaction() as conn:
            conn.execute("UPDATE crawl_items SET status=?,shop_no=?,error=?,failure_json=?,updated_at=? WHERE id=?",
                         (status, shop_no, error, json.dumps(failure) if failure else None, timestamp(), item["id"]))
            if result:
                conn.execute("UPDATE crawl_visits SET status='done',goods_count=?,error=NULL WHERE job_id=? AND shop_no=?",
                             (result["goods_count"], item["job_id"], shop_no))
                job = conn.execute("SELECT mode,workflow_version FROM crawl_jobs WHERE job_id=?", (item["job_id"],)).fetchone()
                mode = job['mode']
                if mode != "targets":
                    before = conn.execute('SELECT COALESCE(MAX(id),0) FROM crawl_items').fetchone()[0]
                    self._enqueue(conn, item["job_id"], result.get("discovered_urls", []), result["shop"]["link"])
                    if job['workflow_version']==2:
                        conn.execute("UPDATE crawl_items SET status='next_round' WHERE job_id=? AND id>?", (item['job_id'],before))
            elif status == "failed" and shop_no:
                conn.execute("UPDATE crawl_visits SET status='failed',error=? WHERE job_id=? AND shop_no=?",
                             (error, item["job_id"], shop_no))
            elif status == "pending" and shop_no:
                conn.execute("DELETE FROM crawl_visits WHERE job_id=? AND shop_no=? AND status!='done'",
                             (item["job_id"], shop_no))
            conn.execute("UPDATE crawl_jobs SET updated_at=? WHERE job_id=?", (timestamp(), item["job_id"]))

    def finish_preview(self,item,key,eligible):
        # The directory lives in another SQLite file. Reconstruct ownership after
        # a crash between saving that preview and checkpointing this queue item.
        with self.transaction() as conn:
            owner=conn.execute("SELECT 1 FROM crawl_items WHERE job_id=? AND shop_no=? AND id!=? "
                               "AND status IN ('previewed','pending','working','done') LIMIT 1",
                               (item['job_id'],key,item['id'])).fetchone()
            state='previewed' if eligible and not owner else 'skipped'
            conn.execute('UPDATE crawl_items SET status=?,shop_no=?,updated_at=? WHERE id=?',
                         (state,key,timestamp(),item['id']))

    def cancel(self, job_id):
        with self.transaction() as conn:
            row = conn.execute("SELECT status FROM crawl_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                raise KeyError(job_id)
            if row["status"] in ACTIVE:
                conn.execute("UPDATE crawl_jobs SET cancel_requested=1,status='cancelling',updated_at=? WHERE job_id=?",
                             (timestamp(), job_id))

    def finish_job(self, job_id, status, error=None, failure=None):
        with self.transaction() as conn:
            conn.execute("UPDATE crawl_jobs SET status=?,phase=CASE WHEN workflow_version=2 THEN phase ELSE ? END,error=?,failure_json=?,updated_at=?,finished_at=? WHERE job_id=?",
                         (status, "interrupted" if status == "interrupted" else "finished", error,
                          json.dumps(failure) if failure else None, timestamp(), timestamp(), job_id))

    def report(self, job_id=None):
        if job_id:
            jobs = self.rows("SELECT * FROM crawl_jobs WHERE job_id=?", (job_id,))
        else:
            jobs = self.rows("SELECT * FROM crawl_jobs ORDER BY CASE WHEN status IN ('running','cancelling') "
                             "THEN 0 ELSE 1 END,updated_at DESC,rowid DESC LIMIT 1")
        if not jobs:
            return None
        job = jobs[0]
        args = (job["job_id"],)
        items = self.rows("SELECT COUNT(*) AS discovered_links, "
                          "SUM(CASE WHEN status IN ('pending','working') THEN 1 ELSE 0 END) AS pending_links, "
                          "SUM(CASE WHEN status IN ('done','skipped','failed') THEN 1 ELSE 0 END) AS processed_links, "
                          "SUM(CASE WHEN status='failed' AND shop_no IS NULL THEN 1 ELSE 0 END) AS failed_links "
                          "FROM crawl_items WHERE job_id=?", args)[0]
        visits = self.rows("SELECT COUNT(*) AS resolved_shops, "
                           "SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS success_shops, "
                           "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_shops, "
                           "SUM(CASE WHEN status='done' THEN goods_count ELSE 0 END) AS collected_goods "
                           "FROM crawl_visits WHERE job_id=?", args)[0]
        job.update({key: value or 0 for key, value in {**items, **visits}.items()})
        job["failure_count"] = job["failed_links"] + job["failed_shops"]
        job["failures"] = self.rows("SELECT url,shop_no,error,failure_json FROM crawl_items "
                                    "WHERE job_id=? AND status='failed' ORDER BY id DESC LIMIT 10", args)
        job["last_error"] = job["error"] or job["discovery_error"] or (job["failures"][0]["error"] if job["failures"] else None)
        job["failure"] = json.loads(job.pop("failure_json") or "null")
        if job["failure"]:
            job["failure"] = normalize_failure(job["failure"], job["error"])
        for item in job["failures"]:
            item["failure"] = normalize_failure(item.pop("failure_json"), item["error"])
        if not job["failure"] and job["failures"]:
            job["failure"] = job["failures"][0]["failure"]
        failed = self.rows("SELECT error,failure_json FROM crawl_items WHERE job_id=? AND status='failed'", args)
        job["retryable_failures"] = sum(should_retry(item["failure_json"], item["error"]) for item in failed)
        job["permanent_failures"] = len(failed) - job["retryable_failures"]
        job.pop("owner_pid")
        job.pop("heartbeat")
        if job.get('workflow_version')==2:
            from shop_preview import PreviewStore
            job.update(PreviewStore().summary(job['job_id']))
        return job
