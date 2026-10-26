"""跨来源只读目录聚合。与 AI 的单库 SQL 权限完全分离。"""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import time

import db
import local_store
from sources import SOURCES, canonical_source

SORTABLE = {"price", "name", "shop_name", "goods_type", "synced_at", "source", "stock_quantity"}


@contextmanager
def connection(sources=None):
    selected = list(dict.fromkeys(canonical_source(s) for s in (sources or SOURCES)))
    conn = sqlite3.connect(":memory:", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    parts, attached = [], []
    try:
        for index, source in enumerate(selected):
            path = Path(local_store.database_path(source)).resolve()
            if not path.exists():
                continue
            alias = f"catalog_{index}"
            conn.execute(f"ATTACH DATABASE ? AS {alias}", (path.as_uri() + "?mode=ro",))
            columns = {r[1] for r in conn.execute(f"PRAGMA {alias}.table_info(wzyp_goods)")}
            if not columns:
                continue
            attached.append((source,alias,columns))
            def col(name, fallback="NULL"):
                return f"g.{name}" if name in columns else fallback
            stock = col("stock_quantity")
            raw_status = col("stock_status", "''")
            status = (f"CASE WHEN LOWER({raw_status}) IN ('out','out_of_stock','sold_out') THEN 'out_of_stock' "
                      f"WHEN LOWER({raw_status}) IN ('in_stock','normal','high','low','available') THEN 'in_stock' "
                      f"WHEN {stock}>0 THEN 'in_stock' WHEN {stock}=0 THEN 'out_of_stock' ELSE 'unknown' END")
            delivery = col("delivery_method")
            parts.append(f"""SELECT '{source}' AS source, g.goods_key, g.shop_token,
                s.nickname AS shop_name, s.link AS shop_link, g.name, g.price, g.market_price,
                g.goods_type, g.status, g.link, COALESCE({col('currency', "'CNY'")},'') AS currency,
                {col('catalog_entry', '0')} AS catalog_entry,
                COALESCE({col('sku_label', "''")},'') AS sku_label, {stock} AS stock_quantity,
                {col('source_url')} AS source_url, {col('source_updated_at')} AS source_updated_at,
                {col('origin_source')} AS origin_source, {col('origin_goods_key')} AS origin_goods_key,
                {status} AS stock_status, {col('category_name', "''")} AS category_name,
                COALESCE({delivery},CASE WHEN g.goods_type IN ('auto','manual') THEN g.goods_type ELSE 'unknown' END) AS delivery_method,
                {col('synced_at')} AS synced_at
                FROM {alias}.wzyp_goods g JOIN {alias}.wzyp_shops s ON s.token=g.shop_token
                {"WHERE g.status=1" if source == "yike" else ""}""")
        conn.execute("PRAGMA query_only=ON")
        conn.execute('BEGIN')
        deadline = time.monotonic() + 5
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
        # Look up direct identities through their unique goods_key indexes. A shared
        # wide CTE would materialize every source again for every count/page query.
        for index,(source,alias,columns) in enumerate(attached):
            if source!='yike' or not {'origin_source','origin_goods_key'} <= columns:
                continue
            sku="COALESCE(g.sku_label,'')" if 'sku_label' in columns else "''"
            checks=[]
            for direct,table,fields in attached:
                if direct=='yike': continue
                direct_sku="COALESCE(d.sku_label,'')" if 'sku_label' in fields else "''"
                checks.append(f"NOT EXISTS (SELECT 1 FROM {table}.wzyp_goods d "
                              f"JOIN {table}.wzyp_shops ds ON ds.token=d.shop_token "
                              f"WHERE g.origin_source='{direct}' AND d.goods_key=g.origin_goods_key AND {direct_sku}={sku})")
            if checks: parts[index]+=' AND '+' AND '.join(checks)
        union = " UNION ALL ".join(parts)
        yield conn, union
    finally:
        conn.close()


def search(query):
    q = dict(query)
    with connection(q.get("sources")) as (conn, union):
        if not union:
            return {"total": 0, "rows": [], "currencies": [], "source_counts": []}
        args, clauses = [], []
        for keyword in [q.get("kw") or "", *(q.get("keywords") or [])]:
            keyword = keyword.strip()
            if keyword:
                clauses.append("INSTR(LOWER(name),LOWER(?))>0")
                args.append(keyword)
        for field in ("shop_token", "goods_type", "delivery_method", "stock_status", "currency"):
            if q.get(field) is not None and (q[field] != "" or field == "currency"):
                clauses.append(f"{field}=?")
                args.append(q[field])
        if q.get("shop_source"):
            clauses.append("source=?")
            args.append(canonical_source(q["shop_source"]))
        if q.get("shop_kw"):
            clauses.append("INSTR(LOWER(shop_name),LOWER(?))>0")
            args.append(q["shop_kw"].strip())
        if q.get("price_min") is not None:
            clauses.append("price>=?")
            args.append(q["price_min"])
        if q.get("price_max") is not None:
            clauses.append("price<=?")
            args.append(q["price_max"])
        if q.get("exclude_zero"):
            clauses.append("(price IS NULL OR price>0)")
        if q.get("filters"):
            where, values = db.build_filters(q["filters"], {k: k for k in (
                "goods_key", "name", "shop_token", "shop_name", "goods_type", "price", "market_price", "status")})
            clauses.append(where.replace("%s", "?"))
            args.extend(values)
        where = " AND ".join(clauses) or "1=1"
        base = f"WITH catalog AS ({union}) "
        counts = [dict(r) for r in conn.execute(base + f"SELECT source,COUNT(*) AS count FROM catalog WHERE {where} GROUP BY source", args)]
        total = sum(row['count'] for row in counts)
        sort = q.get("sort") if q.get("sort") in SORTABLE else "price"
        order = "DESC" if q.get("order") == "desc" else "ASC"
        group_currency = "currency ASC," if sort == "price" and not q.get("currency") else ""
        size, page = q.get("pageSize", 20), q.get("page", 1)
        rows = conn.execute(base + f"SELECT * FROM catalog WHERE {where} ORDER BY {group_currency} "
                            f"{sort} IS NULL, {sort} {order},source,shop_token,goods_key LIMIT ? OFFSET ?",
                            [*args, size, (page - 1) * size]).fetchall()
        currencies = [r[0] or "" for r in conn.execute(base + "SELECT DISTINCT currency FROM catalog ORDER BY currency")]
        return {"total": total, "rows": [dict(r) for r in rows], "currencies": currencies, "source_counts": counts}


def detail(source, goods_key):
    source = canonical_source(source)
    path = Path(local_store.database_path(source)).resolve()
    if not path.exists():
        return None
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        row = conn.execute("SELECT g.*,s.nickname AS shop_name,s.link AS shop_link FROM wzyp_goods g "
                           "JOIN wzyp_shops s ON s.token=g.shop_token WHERE g.goods_key=?", (goods_key,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        extra = conn.execute("SELECT description,image FROM wzyp_goods_detail WHERE goods_key=?", (goods_key,)).fetchone()
        if extra:
            result.update(dict(extra))
        result.update(source=source)
        result.setdefault("currency", "CNY")
        result.setdefault("sku_label", "")
        result["delivery_method"] = result.get("delivery_method") or (result["goods_type"] if result["goods_type"] in {"auto", "manual"} else "unknown")
        raw = (result.get("stock_status") or "").lower()
        quantity = result.get("stock_quantity")
        if raw in {"out", "out_of_stock", "sold_out"}:
            result["stock_status"] = "out_of_stock"
        elif raw in {"in_stock", "normal", "high", "low", "available"}:
            result["stock_status"] = "in_stock"
        else:
            result["stock_status"] = "unknown" if quantity is None or quantity < 0 else "in_stock" if quantity else "out_of_stock"
        return result
    finally:
        conn.close()
