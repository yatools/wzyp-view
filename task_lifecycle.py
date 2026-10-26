"""Live local workers are authoritative even when a database heartbeat is delayed."""
import os
import threading

_lock=threading.RLock()
_workers={}


def key(kind,path,job_id):
    return kind,os.path.normcase(os.path.realpath(path)),job_id


def worker_alive(kind,path,job_id):
    with _lock:
        thread=_workers.get(key(kind,path,job_id))
        return thread is not None and thread.is_alive()


def active_job(kind,path):
    prefix=(kind,os.path.normcase(os.path.realpath(path)))
    with _lock:
        return next((identity[2] for identity,thread in _workers.items() if identity[:2]==prefix and thread.is_alive()),None)


def run_worker(kind,path,job_id,function,args):
    identity=key(kind,path,job_id)
    thread=threading.current_thread()
    with _lock:
        existing=_workers.get(identity)
        if existing is not None and existing.is_alive():return
        _workers[identity]=thread
    try:
        function(*args)
    finally:
        with _lock:
            if _workers.get(identity) is thread:_workers.pop(identity,None)
