"""Small public shop directories and a durable queue, separate from goods snapshots."""
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import quote, urljoin

from config import BASE_DIR
from sqlite_init import initialize,write_transaction,RetryConnection

LIMIT = 100


def count(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = int(value)
        return result if result >= 0 and float(value) == result else None
    except (ValueError, TypeError, OverflowError):
        return None


def stock(item):
    raw = str(item.get('stock_status') or item.get('stock_available_status') or '').lower()
    quantity = count(item.get('stock_quantity', item.get('stock_available_quantity', item.get('in_stock'))))
    if item.get('is_sold_out') is True or raw in {'out', 'out_of_stock', 'sold_out'} or quantity == 0:
        return 'out_of_stock'
    if quantity is not None and quantity > 0 or raw in {'in_stock', 'available', 'normal', 'high', 'low'}:
        return 'in_stock'
    return 'unknown'


@dataclass
class ShopPreview:
    shop_key: str
    name: str
    url: str
    products: list = field(default_factory=list)
    categories: list = field(default_factory=list)
    reported_total: int | None = None
    complete: bool = False
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def normalized(self):
        data = asdict(self)
        products = [x for x in self.products if x.get('name') and x.get('status', 1) not in (0,'0',False)]
        data['complete'] = bool(self.complete and len(products) <= LIMIT)
        data['products'] = products[:LIMIT]
        states = [x.get('stock_status', 'unknown') for x in products]
        total = count(self.reported_total)
        # A non-empty current directory can prove presence without disclosing stock.
        category_presence=any((count(x.get('count')) or 0)>0 for x in self.categories)
        reported_presence=total>0 if total is not None else category_presence
        has_goods = bool(products) or reported_presence and (not self.products or not self.complete)
        if has_goods and not products:data['complete']=False
        all_out = bool(products) and self.complete and all(x == 'out_of_stock' for x in states)
        data['eligibility'] = 'empty' if not has_goods else 'no_stock' if all_out else 'eligible'
        data['stock_status'] = ('in_stock' if 'in_stock' in states else 'out_of_stock' if all_out
                                else 'unknown')
        data['reported_total'] = total
        return data


SCHEMA = """
CREATE TABLE IF NOT EXISTS shop_previews (
 job_id TEXT NOT NULL, source TEXT NOT NULL, shop_key TEXT NOT NULL,
 url TEXT NOT NULL, name TEXT NOT NULL, preview_json TEXT NOT NULL,
 eligibility TEXT NOT NULL, stock_status TEXT NOT NULL, collection_state TEXT NOT NULL DEFAULT 'pending',
 child_job_id TEXT, updated_at TEXT NOT NULL, PRIMARY KEY(job_id,source,shop_key)
);
CREATE INDEX IF NOT EXISTS preview_queue ON shop_previews(job_id,source,eligibility,collection_state);
"""


class PreviewStore:
    def __init__(self, path=None):
        self.path = path or os.environ.get('WZYP_DISCOVERY_DB') or os.path.join(BASE_DIR, 'data', 'discovery.sqlite3')

    @contextmanager
    def connection(self,write=False):
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=15,factory=RetryConnection)
        conn.row_factory = sqlite3.Row
        try:
            def migrate(connection):
                connection.execute('PRAGMA journal_mode=WAL')
                connection.executescript(SCHEMA)
            initialize(conn, self.path, 'previews', migrate)
            with write_transaction(conn,self.path) if write else conn:
                yield conn
        finally:
            conn.close()

    def save(self, job_id, source, preview):
        data = preview.normalized() if isinstance(preview, ShopPreview) else preview
        with self.connection(write=True) as conn:
            cursor = conn.execute('INSERT OR IGNORE INTO shop_previews '
                '(job_id,source,shop_key,url,name,preview_json,eligibility,stock_status,updated_at) VALUES (?,?,?,?,?,?,?,?,?)',
                (job_id, source, data['shop_key'], data['url'], data['name'], json.dumps(data, ensure_ascii=False),
                 data['eligibility'], data['stock_status'], data['checked_at']))
            return cursor.rowcount == 1

    def get(self, job_id, source, key):
        with self.connection() as conn:
            row = conn.execute('SELECT * FROM shop_previews WHERE job_id=? AND source=? AND shop_key=?', (job_id,source,key)).fetchone()
            return dict(row) if row else None

    def collection(self, job_id, source, key, state, child_id=None):
        with self.connection(write=True) as conn:
            conn.execute('UPDATE shop_previews SET collection_state=?,child_job_id=COALESCE(?,child_job_id),updated_at=? '
                         'WHERE job_id=? AND source=? AND shop_key=?',
                         (state,child_id,datetime.now(timezone.utc).isoformat(),job_id,source,key))

    def summary(self, job_id):
        with self.connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS discovered_shops,"
                "SUM(eligibility='eligible' AND collection_state!='collected') AS pending_shops,"
                "SUM(eligibility='eligible' AND stock_status='unknown' AND collection_state!='collected') AS unverified_stock_shops "
                'FROM shop_previews WHERE job_id=?', (job_id,)).fetchone()
            return {key: value or 0 for key,value in dict(row).items()}

    def known_urls(self,source):
        with self.connection() as conn:
            return [row[0] for row in conn.execute('SELECT DISTINCT url FROM shop_previews WHERE source=?',(source,))]

    def list(self, job_id, source=None, page=1, size=20):
        where, args = 'job_id=?', [job_id]
        if source:
            where += ' AND source=?'; args.append(source)
        with self.connection() as conn:
            total = conn.execute('SELECT COUNT(*) FROM shop_previews WHERE '+where,args).fetchone()[0]
            rows = conn.execute('SELECT * FROM shop_previews WHERE '+where+' ORDER BY source,shop_key LIMIT ? OFFSET ?',
                                [*args,size,(page-1)*size]).fetchall()
            return {'total':total,'rows':[{**{k:v for k,v in dict(row).items() if k!='preview_json'},
                                          'preview':json.loads(row['preview_json'])} for row in rows]}


def probe_hosted(adapter, url, session, resolved=None):
    shop, _ = resolved or adapter.resolve_target(url, session)
    key = adapter._check_shop_open(shop)
    products, categories, totals, complete = [], [], [], True
    for kind in ('card','article','resource','equity'):
        groups=adapter.fetch_public(session,'Shop/categoryList',{'token':key,'goods_type':kind})
        if not isinstance(groups,list): raise adapter.PlatformError('公开分类格式异常')
        payload = adapter.fetch_public(session, 'Shop/goodsList', {'token':key,'goods_type':kind,'current':1,'pageSize':20})
        if not isinstance(payload, dict) or not isinstance(payload.get('list'),list):
            raise adapter.PlatformError('公开商品目录格式异常')
        rows, total = payload['list'], count(payload.get('total'))
        group_names={str(g.get('id')):str(g.get('name') or '') for g in groups if isinstance(g,dict)}
        totals.append(total)
        categories.extend({'id':str(g.get('id') or ''),'name':str(g.get('name') or ''),
                           'count':count(g.get('goods_count')),'goods_type':kind} for g in groups if isinstance(g,dict))
        if not groups: categories.append({'id':kind,'name':kind,'count':total})
        complete = complete and total is not None and len(rows) == total
        for item in rows:
            if isinstance(item,dict) and item.get('is_active') is False: continue
            if not isinstance(item,dict) or not str(item.get('name') or '').strip() or not item.get('goods_key'):
                raise adapter.PlatformError('公开商品目录缺少名称或标识')
            products.append({'goods_key':str(item['goods_key']),'name':str(item['name']),
                'link':adapter.BASE_URL+'/item/'+quote(str(item['goods_key']),safe=''),
                'category_name':str(item.get('category_name') or group_names.get(str(item.get('category_id'))) or kind), 'status':item.get('status',1),
                'stock_status':stock(item)})
    return ShopPreview(key, str(shop.get('nickname') or key), adapter.BASE_URL+'/shop/'+key,
                       products,categories,sum(totals) if all(x is not None for x in totals) else None,complete)


def probe_16688(adapter, url, session, resolved=None):
    shop, _ = resolved or adapter.resolve_target(url,session)
    key = str(shop['shop_no'])
    payload = adapter.fetch_public(session,'goods/list',{'shop_no':key,'goods_category_no':'','keywords':'','sort':'default'})
    if not isinstance(payload,dict) or not isinstance(payload.get('list'),list):
        raise adapter.PlatformError('公开商品目录格式异常')
    groups = adapter.fetch_public(session,'goods_category/list',{'shop_no':key})
    if not isinstance(groups,list): raise adapter.PlatformError('公开分类格式异常')
    names = {str(x.get('goods_category_no')):str(x.get('name') or '') for x in groups if isinstance(x,dict)}
    rows = payload['list']; total = count(payload.get('total'))
    complete = not payload.get('has_more') and (total is None or total == len(rows))
    products=[]
    for item in rows:
        if not isinstance(item,dict) or not item.get('goods_no') or not str(item.get('name') or '').strip():
            raise adapter.PlatformError('公开商品目录缺少名称或标识')
        products.append({'goods_key':str(item['goods_no']),'name':str(item['name']),
            'link':adapter.BASE_URL+'/goods/'+quote(str(item['goods_no']),safe=''),
            'category_name':names.get(str(item.get('goods_category_no')),''),
            'stock_status':stock(item),'status':item.get('status',1)})
    categories=[{'id':str(x.get('goods_category_no') or ''),'name':str(x.get('name') or ''),
                 'count':count(x.get('goods_count'))} for x in groups if isinstance(x,dict)]
    return ShopPreview(key,str(shop.get('name') or key),adapter.BASE_URL+'/shop/'+key,products,categories,
                       total if total is not None else len(rows) if complete else None,complete)


def probe_independent(adapter, url, session, resolved=None):
    from bs4 import BeautifulSoup
    from platform_independent import _api, _listing_page, text_value, _quantity, _stock
    shop,payload = resolved or adapter.resolve_target(url,session,preview_only=True)
    origin=shop['origin']; products=[]; categories=[]; total=None; complete=False
    if adapter.SOURCE_KEY=='dujiao-next':
        rows,total=_listing_page(*payload['preview'])
        complete=len(rows)==total
        groups,_=_api(session,origin,'categories')
        if isinstance(groups,dict): groups=groups.get('items',groups.get('list',[]))
        for group in groups if isinstance(groups,list) else []:
            categories.append({'id':str(group.get('id') or ''),'name':text_value(group.get('name') or group.get('title')),
                               'count':count(group.get('product_count',group.get('goods_count')))})
        for item in rows:
            name=text_value(item.get('title') or item.get('name'))
            if item.get('is_active') is False: continue
            if not name or not item.get('slug') or item.get('id') is None:
                raise adapter.PlatformError('公开商品目录缺少名称或标识')
            delivery=str(item.get('fulfillment_type') or '')
            variants=[sku for sku in item.get('skus',[]) if isinstance(sku,dict) and sku.get('is_active') is not False]
            states=[_stock(x,_quantity(x,delivery)) for x in variants] if variants else [_stock(item,_quantity(item,delivery))]
            state='in_stock' if 'in_stock' in states else 'out_of_stock' if all(x=='out_of_stock' for x in states) else 'unknown'
            products.append({'goods_key':str(item['id']),'name':name,'link':origin+'/products/'+quote(str(item['slug']),safe=''),
                             'category_name':text_value((item.get('category') or {}).get('name')),'stock_status':state})
    else:
        soup=BeautifulSoup(payload['html'],'html.parser')
        if payload.get('choice'):
            token=soup.select_one("input[name='_token']"); csrf=token.get('value','') if token else ''
            response=session.get(origin+'/api/typelist'); body=response.json()
            groups=(body.get('data') or {}).get('typelist') if isinstance(body,dict) else None
            if response.status_code!=200 or str(body.get('code'))!='1' or not isinstance(groups,list):
                raise adapter.PlatformError('Choice 公开分类格式异常')
            complete=True
            for group in groups:
                if group.get('password'): continue
                categories.append({'id':str(group.get('id')),'name':str(group.get('name') or ''),'count':count(group.get('product_count'))})
                response=session.post(origin+'/api/productlist',data={'tid':group['id'],'_token':csrf}); body=response.json()
                listing=(body.get('data') or {}) if isinstance(body,dict) else {}
                rows=listing.get('products')
                if response.status_code!=200 or str(body.get('code'))!='1' or not isinstance(rows,list):
                    raise adapter.PlatformError('Choice 公开商品目录格式异常')
                expected=count(listing.get('total'))
                if listing.get('has_more') or expected is not None and expected!=len(rows):complete=False
                for item in rows:
                    if item.get('password'): continue
                    key=item.get('id'); name=text_value(item.get('pd_name') or item.get('name'))
                    if key is None or not name: raise adapter.PlatformError('Choice 商品目录缺少名称或标识')
                    products.append({'goods_key':str(key),'name':name,'link':origin+'/buy/'+quote(str(key),safe=''),
                                     'category_name':str(group.get('name') or ''),'stock_status':stock(item)})
            total=len(products) if complete else None
        else:
            for key,link in payload['links'].items():
                anchors=[a for a in soup.select('a[href]') if urljoin(origin+'/',a.get('href','')).rstrip('/')==link]
                anchor=anchors[0] if anchors else None
                title=anchor.select_one('.name,.goods-name,.product-title,h3,h4') if anchor else None
                name=(title or anchor).get_text(' ',strip=True) if anchor else ''
                if not name and anchor:
                    image=anchor.select_one('img[alt]')
                    name=anchor.get('title') or (image.get('alt') if image else '')
                if not name: continue
                text=anchor.get_text(' ',strip=True)
                quantity=count(anchor.get('data-stock'))
                match=re.search(r'(?:库存|In Stock)\s*[:：]?\s*(\d+)',text,re.I)
                if quantity is None and match: quantity=count(match[1])
                state='out_of_stock' if re.search(r'缺货|售罄|sold out|out of stock',text,re.I) else stock({'stock_quantity':quantity})
                products.append({'goods_key':key,'name':name,'link':link,'category_name':'','stock_status':state})
            products.extend({'goods_key':x['native_id'],'name':x['name'],'link':x['link'],
                             'category_name':x['category_name'],'stock_status':'out_of_stock'} for x in payload.get('directory_rows',[]))
            complete=not bool(soup.select_one("a[rel='next'], .pagination .next a[href]"))
            total=len(payload['links'])+len(payload.get('directory_rows',[])) if complete else None
            if len(products)<len(payload['links'])+len(payload.get('directory_rows',[])):complete=False
    return ShopPreview(shop['shop_no'],str(shop['name']),origin+'/',products,categories,total,complete)
