"""仅供浏览器回归的隔离服务；不用真实数据库或 LLM。"""
import argparse
import asyncio
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

ROOT=Path(__file__).resolve().parents[1]
app=FastAPI()
app.mount('/static',StaticFiles(directory=ROOT/'static'),name='assets')
yike_job = None
discovery_fixture = None
discovery_requests = []


@app.post('/test/discovery/setup')
def setup_discovery():
    global discovery_fixture
    discovery_requests.clear()
    discovery_fixture = dict(job_id='discovery-fixture',status='partial',sources=['catfk'],
        include_indexes=False,max_depth=2,page_limit=20,pages=20,probe_limit=200,probes_used=200,
        discovered_links=3,success_shops=1,collected_goods=4,created_at='2026-10-24T08:00:00+08:00',
        updated_at='2026-10-24T08:01:00+08:00',counts={'failed':1,'probe_deferred':1,'collected':1},index_errors=[],
        source_progress={'catfk':dict(running=0,waiting=0,skipped=0,failed=1,success_shops=1,collected_goods=4,
            activity='finished',reason='店铺已打烊 <img src=x onerror="window.__xss=1">',
            failure={'category':'shop_closed','retryable':False},
            last_event={'url':'https://catfk.com/shop/success'})})
    return {'ok':True}


@app.get('/test/discovery/requests')
def recorded_requests():
    return {'requests':discovery_requests}


@app.post('/api/discovery/jobs/{job_id}/resume')
def resume_discovery(job_id: str, body: dict):
    discovery_requests.append({'action':'resume','body':body})
    return discovery_fixture


@app.get('/api/discovery/jobs/{job_id}/candidates')
def discovery_candidates(job_id: str):
    retried=any(item['action']=='retry-1' for item in discovery_requests)
    return {'total':2,'rows':[
        dict(id=1,url='https://catfk.com/shop/closed',state='failed',source='catfk',depth=0,
             parents=['manual'],reason='店铺已打烊',failure={'category':'shop_closed','retryable':False},
             retries=[{'child_job_id':'retry-fixture'}] if retried else []),
        dict(id=2,url='https://unknown.example/',state='unsupported',source=None,depth=0,
             parents=['manual'],reason='模板未识别',failure={'category':'unsupported_template','retryable':False},retries=[])]}


@app.post('/api/discovery/jobs/{job_id}/candidates/{candidate_id}/retry')
def retry_candidate(job_id: str,candidate_id: int):
    discovery_requests.append({'action':f'retry-{candidate_id}'})
    if candidate_id==2:
        return {'action':'rediscover','prefill':{'urls':['https://unknown.example/'],'include_indexes':False}}
    return {'action':'collect','source':'catfk','job':{'job_id':'retry-fixture','mode':'targets'}}


@app.post('/api/sources/catfk/crawl/{job_id}/resume')
def resume_cloud(job_id: str,body: dict):
    discovery_requests.append({'action':'source-resume','body':body})
    return {'ok':True}


@app.get('/api/sources')
def sources():
    import sys
    if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
    from sources import SOURCES
    return {'sources':[{'id':key,'name':name,'kind':'aggregator' if key=='yike' else 'direct',
                        'family':'hosted' if key in {'wzyp-live','catfk'} else 'other',
                        'discovery':key!='yike','keyword_crawl':key=='yike'} for key,name in SOURCES.items()]}


@app.get('/api/sources/summary')
def source_summary():
    return {'sources':[{'id':x['id'],'shop_count':1,'goods_count':2} for x in sources()['sources']]}


@app.get('/api/discovery/shops')
def preview_shops(job_id:str,source:str|None=None,page:int=1,pageSize:int=20):
    return {'total':1,'rows':[{'source':'catfk','name':'目录预览店铺','url':'https://fixture.example/',
        'eligibility':'eligible','collection_state':'pending','preview':{'reported_total':2,'complete':False,
        'checked_at':'2026-10-26T08:00:00+08:00','categories':[{'name':'会员','count':None}],
        'products':[{'name':'目录预览商品','stock_status':'unknown'}]}}]}


@app.get('/api/discovery/jobs')
def discovery_jobs():
    return {'jobs': [discovery_fixture] if discovery_fixture else []}


@app.get('/api/stats')
def stats(source: str = '16688'):
    return {'stats': {'shop_count': 1, 'goods_count': 2, 'detail_count': 2, 'total_sell': 0,
                      'total_deposit': 0, 'avg_price': 10, 'min_price': 10, 'max_price': 10},
            'type_dist': [], 'price_buckets': [], 'top_keywords': []}


@app.get('/api/llm/settings')
def llm_settings():
    return {'baseurl': '', 'model': '', 'apikey': ''}


@app.get('/api/llm/test')
def llm_test():
    return {'ok': False}


@app.post('/api/llm/settings')
def save_llm(body:dict):
    return {'ok':True}


@app.get('/api/chat/history')
def chat_history():
    return {'messages':[]}


@app.post('/api/chat')
def chat(body:dict):
    async def stream():
        import json
        for word in ['隔离','测试','回复']:
            yield 'data: '+json.dumps({'event':'token','data':word},ensure_ascii=False)+'\r\n\r\n'
            await asyncio.sleep(.01)
        yield 'data: [DONE]\n\n'
    return StreamingResponse(stream(),media_type='text/event-stream')


@app.post('/api/shops')
@app.post('/api/goods')
def legacy_rows(body: dict):
    return {'total': 0, 'rows': []}


@app.get('/api/sources/{source}/crawl')
def latest_job(source: str):
    if source=='catfk' and discovery_fixture:
        return {'job':dict(job_id='cloud-fixture',status='partial',success_shops=1,collected_goods=4,
                            failure_count=1,last_error='店铺已打烊',failures=[])}
    return {'job': yike_job if source == 'yike' else None}


@app.post('/api/sources/yike/crawl')
def keyword_crawl(body: dict):
    global yike_job
    keyword = (body.get('keyword') or '').strip()
    if not 1 <= len(keyword) <= 200:
        raise HTTPException(422, '请输入采集关键词')
    yike_job = dict(job_id='yike-fixture', keyword=keyword, status='running', source='yike', list_pages=2,
                    group_pages=3, matched_groups=1, collected_goods=2, failure_count=0, pending_links=1,
                    success_shops=1, discovered_links=3, created_at='2026-10-24T08:00:00+08:00', failures=[])
    return yike_job


@app.post('/api/sources/yike/crawl/{job_id}/{action}')
def keyword_action(job_id: str, action: str):
    if not yike_job or job_id != yike_job['job_id']:
        raise HTTPException(404, '任务不存在')
    yike_job['status'] = 'cancelled' if action == 'cancel' else 'completed'
    if action != 'cancel':
        yike_job.update(pending_links=0, collected_goods=3)
    return yike_job


@app.get('/api/sources/{source}/jobs')
def job_history(source: str):
    return {'jobs': [yike_job] if source == 'yike' and yike_job else []}


@app.get('/')
def harness():
    global yike_job,discovery_fixture
    yike_job = None
    discovery_fixture = None
    discovery_requests.clear()
    return FileResponse(ROOT/'tests'/'browser_harness.html')


@app.get('/preview')
def preview():
    return FileResponse(ROOT/'static'/'index.html')


@app.post('/api/catalog/search')
async def search(body:dict):
    words=body.get('keywords',[])
    if 'slow' in words:
        await asyncio.sleep(.3)
    return {'total':1,'rows':[{'source':'16688','goods_key':'fixture','name':' '.join(words)+' result',
        'price':10,'currency':'CNY','shop_name':'隔离测试货铺','shop_token':'fixture',
        'delivery_method':'auto','stock_status':'unknown','synced_at':'2026-10-23T00:00:00+08:00'}],
        'currencies':['CNY','USD'],'source_counts':[{'source':'16688','count':1}]}


@app.get('/api/catalog/goods/{source}/{key}')
def detail(source:str,key:str):
    if key=='gone':
        raise HTTPException(404,'商品已下架')
    return {'source':source,'goods_key':key,'name':'隔离测试商品','price':777 if key=='archived' else 10,'currency':'CNY',
            'shop_name':'隔离测试货铺','shop_link':'https://fixture.example/',
            'link':'https://fixture.example/buy/1','delivery_method':'auto','stock_status':'unknown',
            'status':0 if key=='archived' else 1,
            'source_url':'https://vs.yiketools.com/goods/9001/fixture' if source=='yike' else None,
            'source_updated_at':'2026-10-24 01:14:53' if source=='yike' else None,
            'synced_at':'2026-10-24T08:00:00+08:00',
            'description':'<p>公开说明</p><img src=x onerror="window.__xss=1"><script>window.__xss=1</script>'}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=18871)
    args=parser.parse_args()
    uvicorn.run(app,host='127.0.0.1',port=args.port)
