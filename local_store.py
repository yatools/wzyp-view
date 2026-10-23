"""本地 SQLite 快照存储（支持 16688 与链动小铺 wzyp-live）。沿用查询表名，与原有 MySQL 数据独立保存。"""
from datetime import datetime
import os
import sqlite3

from config import BASE_DIR


SCHEMA = """
CREATE TABLE IF NOT EXISTS wzyp_shops (
    token TEXT PRIMARY KEY, nickname TEXT NOT NULL, login_province TEXT,
    contact_qq TEXT, contact_mobile TEXT, contact_wechat TEXT, sell_count INTEGER,
    sales_count_text TEXT, deposit_money REAL, goods_count INTEGER NOT NULL,
    card_count INTEGER DEFAULT 0, article_count INTEGER DEFAULT 0,
    resource_count INTEGER DEFAULT 0, equity_count INTEGER DEFAULT 0,
    auto_count INTEGER DEFAULT 0, manual_count INTEGER DEFAULT 0,
    unknown_count INTEGER DEFAULT 0, auth_status INTEGER, shop_newbie INTEGER,
    create_time TEXT, link TEXT NOT NULL, description TEXT, avatar TEXT,
    fetch_done INTEGER NOT NULL DEFAULT 1, synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS wzyp_goods (
    id INTEGER PRIMARY KEY, goods_key TEXT NOT NULL UNIQUE,
    shop_token TEXT NOT NULL REFERENCES wzyp_shops(token), goods_type TEXT NOT NULL,
    name TEXT NOT NULL, price REAL, market_price REAL, status INTEGER NOT NULL,
    link TEXT NOT NULL, sales_count_text TEXT, stock_quantity INTEGER,
    stock_status TEXT, category_name TEXT, synced_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS goods_shop ON wzyp_goods(shop_token);
CREATE TABLE IF NOT EXISTS wzyp_goods_detail (
    goods_key TEXT PRIMARY KEY REFERENCES wzyp_goods(goods_key) ON DELETE CASCADE,
    shop_token TEXT,
    name TEXT NOT NULL, price REAL, real_price REAL, market_price REAL,
    goods_type TEXT NOT NULL, image TEXT, description TEXT, link TEXT NOT NULL,
    sales_count_text TEXT, stock_quantity INTEGER, stock_status TEXT,
    category_name TEXT, synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS crawl_jobs (
    job_id TEXT PRIMARY KEY, mode TEXT NOT NULL, status TEXT NOT NULL,
    phase TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    finished_at TEXT, owner_pid INTEGER NOT NULL, heartbeat REAL NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    discovery_done INTEGER NOT NULL DEFAULT 0,
    discovery_version TEXT, discovery_pages INTEGER NOT NULL DEFAULT 0,
    discovery_error TEXT, error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_crawl ON crawl_jobs((1))
    WHERE status IN ('running', 'cancelling');
CREATE TABLE IF NOT EXISTS crawl_items (
    id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES crawl_jobs(job_id),
    kind TEXT NOT NULL, target TEXT NOT NULL, url TEXT NOT NULL, origin TEXT,
    status TEXT NOT NULL DEFAULT 'pending', shop_no TEXT, error TEXT,
    updated_at TEXT NOT NULL, UNIQUE(job_id, kind, target)
);
CREATE INDEX IF NOT EXISTS crawl_pending ON crawl_items(job_id, status, kind, id);
CREATE TABLE IF NOT EXISTS crawl_visits (
    job_id TEXT NOT NULL REFERENCES crawl_jobs(job_id), shop_no TEXT NOT NULL,
    status TEXT NOT NULL, goods_count INTEGER NOT NULL DEFAULT 0, error TEXT,
    PRIMARY KEY(job_id, shop_no)
);
CREATE TABLE IF NOT EXISTS crawl_aliases (
    kind TEXT NOT NULL, target TEXT NOT NULL, shop_no TEXT NOT NULL,
    PRIMARY KEY(kind, target)
);
"""

_INCREMENTAL_COLUMNS = {
    "wzyp_shops": [
        ("contact_mobile", "TEXT"),
        ("sales_count_text", "TEXT"),
        ("auto_count", "INTEGER DEFAULT 0"),
        ("manual_count", "INTEGER DEFAULT 0"),
        ("unknown_count", "INTEGER DEFAULT 0"),
        ("synced_at", "TEXT"),
    ],
    "wzyp_goods": [
        ("sales_count_text", "TEXT"),
        ("stock_quantity", "INTEGER"),
        ("stock_status", "TEXT"),
        ("category_name", "TEXT"),
        ("synced_at", "TEXT"),
    ],
    "wzyp_goods_detail": [
        ("shop_token", "TEXT"),
        ("sales_count_text", "TEXT"),
        ("stock_quantity", "INTEGER"),
        ("stock_status", "TEXT"),
        ("category_name", "TEXT"),
        ("synced_at", "TEXT"),
    ],
}


def database_path(source: str = "16688"):
    if source in ("wzyp-live", "wzyp"):
        return os.environ.get("WZYP_LIVE_DB") or os.environ.get("WZYP_SQLITE_DB") or os.path.join(BASE_DIR, "data", "wzyp.sqlite3")
    return os.environ.get("WZYP_16688_DB") or os.path.join(BASE_DIR, "data", "16688.sqlite3")


def wzyp_database_path():
    return database_path("wzyp-live")


def _ensure_incremental_schema(conn: sqlite3.Connection):
    for table, cols in _INCREMENTAL_COLUMNS.items():
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if not existing:
            continue
        for col_name, col_type in cols:
            if col_name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")


def connect(path=None):
    path = path or database_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    _ensure_incremental_schema(conn)
    conn.create_function("FIELD", -1, lambda value, *items: items.index(value) + 1 if value in items else 0)
    conn.create_function("DATE_FORMAT", 2, _date_format)
    return conn


def _date_format(value, pattern):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).strftime(pattern.replace("%i", "%M"))
    except ValueError:
        return str(value)[:10]


def save_snapshot(shop, goods, details, path=None):
    """完整读取成功后，一次事务更新该店快照；其他店铺保持不变。"""
    conn = connect(path)
    try:
        with conn:
            columns = list(shop)
            updates = ", ".join(f"{c}=excluded.{c}" for c in columns if c != "token")
            conn.execute(
                f"INSERT INTO wzyp_shops ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) "
                f"ON CONFLICT(token) DO UPDATE SET {updates}", list(shop.values()))
            conn.execute("DELETE FROM wzyp_goods WHERE shop_token=?", (shop["token"],))
            for table, rows in (("wzyp_goods", goods), ("wzyp_goods_detail", details)):
                if rows:
                    columns = list(rows[0])
                    conn.executemany(
                        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                        [[row[c] for c in columns] for row in rows])
    finally:
        conn.close()


def readonly_query(sql, args=None, path=None):
    conn = connect(path)
    try:
        # 查询连接禁止写入，AI 也无法修改快照或访问任务表及本机其他数据库。
        allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
                   sqlite3.SQLITE_RECURSIVE}
        tables = {"wzyp_shops", "wzyp_goods", "wzyp_goods_detail"}

        def authorize(action, arg1, arg2, database, trigger):
            if action not in allowed or (action == sqlite3.SQLITE_READ and arg1 not in tables):
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_FUNCTION and (arg2 or "").lower() == "load_extension":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorize)
        # MySQL 参数和 DATE_FORMAT 中的 %% 与现有查询保持兼容。
        if args:
            sql = sql.replace("%s", "?").replace("%%", "%")
        remaining = 1000

        def progress():
            nonlocal remaining
            remaining -= 1
            return int(remaining <= 0)

        conn.set_progress_handler(progress, 10000)
        return [dict(row) for row in conn.execute(sql, args or ())]
    finally:
        conn.close()
