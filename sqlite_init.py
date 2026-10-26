"""Run idempotent migrations once per database file, without caching connections."""
import os
from contextlib import contextmanager
import sqlite3
import threading
import time

_lock = threading.RLock()
_ready = {}
_writers = {}


def database_lock(path):
    key=os.path.normcase(os.path.realpath(path))
    lock=_writers.get(key)
    if lock is None:
        with _lock:
            lock=_writers.setdefault(key,threading.RLock())
    return lock


def is_busy(error):
    code=getattr(error,'sqlite_errorcode',0)
    return isinstance(error,sqlite3.OperationalError) and (
        code & 255 in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED) or
        'database is locked' in str(error).lower() or 'database table is locked' in str(error).lower())


class RetryConnection(sqlite3.Connection):
    def execute(self,sql,parameters=()):
        # Opening/cleaning up a WAL can briefly contend even for a fresh reader.
        # Never retry a failed statement inside an existing transaction/snapshot.
        deadline=time.monotonic()+2
        while True:
            try:return super().execute(sql,parameters)
            except sqlite3.OperationalError as error:
                if not is_busy(error) or self.in_transaction or time.monotonic()>=deadline:raise
                time.sleep(.01)


@contextmanager
def write_transaction(conn,path):
    """Serialize this process's writers; reserve before any read-modify-write."""
    with database_lock(path):
        deadline=time.monotonic()+30
        while True:
            try:
                conn.execute('BEGIN IMMEDIATE')
                break
            except sqlite3.OperationalError as error:
                if not is_busy(error) or time.monotonic()>=deadline:raise
                time.sleep(.025)
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise


def initialize(conn, path, namespace, migrate):
    path = os.path.realpath(path)
    stat = os.stat(path)
    identity = (stat.st_dev, stat.st_ino, getattr(stat,'st_birthtime_ns',0))
    key = (namespace, path)
    if _ready.get(key)==identity:return
    # Use the same order as ordinary transactions: file lock, then schema guard.
    with database_lock(path),_lock:
        stat = os.stat(path)
        identity = (stat.st_dev, stat.st_ino, getattr(stat,'st_birthtime_ns',0))
        key = (namespace, path)
        if _ready.get(key) != identity:
            migrate(conn)
            conn.commit()
            stat = os.stat(path)
            _ready[key] = (stat.st_dev, stat.st_ino, getattr(stat,'st_birthtime_ns',0))
