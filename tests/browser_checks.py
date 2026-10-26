"""Run against tests/browser_server.py; isolated HTTP fixtures, never live stores or LLMs."""
import asyncio
import json
from urllib.parse import urlsplit

from playwright.async_api import async_playwright


async def main():
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True)
        context=await browser.new_context(viewport={'width':1440,'height':1000})
        page=await context.new_page()
        errors=[];requests=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        page.on('request',lambda request:requests.append(urlsplit(request.url).path))
        await page.goto('http://127.0.0.1:18871/')
        await page.locator('#outcome[data-failed]').wait_for()
        assert await page.locator('#outcome').get_attribute('data-failed')=='0',await page.locator('#results').inner_text()
        print('PASS existing browser harness: '+await page.locator('#outcome').inner_text(),flush=True)

        await page.clock.install()
        requests.clear()
        await page.goto('http://127.0.0.1:18871/preview')
        await page.locator('#sourceStats-catfk').filter(has_text='1 家货铺').wait_for()
        assert '/api/llm/test' not in requests,'startup invoked the LLM'
        assert '/api/stats' not in requests,'task center requested dashboard calculations'
        assert requests.count('/api/sources/16688/crawl')==1,'duplicate hidden legacy polling'
        def polls():return sum(path=='/api/discovery/jobs' or path.endswith('/crawl') for path in requests)
        before=polls()
        for _ in range(2):
            await page.clock.run_for(15020)
            await page.wait_for_timeout(200)
        idle=polls()-before
        assert idle<=14,f'idle polling made {idle} requests instead of at most 14'
        print(f'PASS idle polling: {idle} status requests / 30s (previous 70), no hidden duplicate or full statistics',flush=True)
        await page.evaluate("Object.defineProperty(document,'hidden',{configurable:true,get:()=>true});document.dispatchEvent(new Event('visibilitychange'))")
        before=polls();await page.clock.run_for(31000);await page.wait_for_timeout(100)
        assert polls()==before,'hidden page continued polling'
        await page.evaluate("Object.defineProperty(document,'hidden',{configurable:true,get:()=>false});document.dispatchEvent(new Event('visibilitychange'))")
        await page.wait_for_timeout(200)
        assert polls()>before,'visible page did not immediately refresh'
        print('PASS visibility pauses and immediately restores polling',flush=True)

        slow=asyncio.Event()
        async def shops(route):
            body=route.request.post_data_json;word=body.get('kw') or 'initial'
            if word=='old':slow.set();await asyncio.sleep(.2)
            row={'token':word,'nickname':word,'goods_count':1,'fetch_done':1}
            try:await route.fulfill(json={'total':1,'rows':[row]})
            except Exception:pass  # Superseded requests are intentionally aborted.
        await page.route('**/api/shops?*',shops)
        await page.locator('nav [data-page=shops]').click()
        await page.locator('#shopKw').fill('old')
        await page.locator('#page-shops button').filter(has_text='查询').click()
        await asyncio.wait_for(slow.wait(),2)
        await page.locator('#shopKw').fill('new')
        await page.locator('#page-shops button').filter(has_text='查询').click()
        await page.locator('#shopTable tbody').filter(has_text='new').wait_for()
        await page.wait_for_timeout(250)
        assert 'old' not in await page.locator('#shopTable tbody').inner_text()
        print('PASS legacy shop list rejects out-of-order responses',flush=True)

        slow.clear()
        async def goods_detail(route):
            key=urlsplit(route.request.url).path.rsplit('/',1)[1]
            if key=='slow':slow.set();await asyncio.sleep(.2)
            try:await route.fulfill(json={'name':key,'goods_key':key,'price':1,'currency':'CNY','description':'<p>商品说明</p>'})
            except Exception:pass
        await page.route('**/api/goods/*?*',goods_detail)
        await page.evaluate("void openDetail('slow')")
        await asyncio.wait_for(slow.wait(),2)
        await page.evaluate("void openDetail('latest')")
        await page.locator('#drawerTitle').filter(has_text='latest').wait_for()
        await page.wait_for_timeout(250)
        assert await page.locator('#drawerTitle').inner_text()=='latest'
        await page.evaluate('closeDrawer()')
        print('PASS legacy details reject stale responses',flush=True)

        job={'job_id':'discovery-fixture','workflow_version':2,'phase':'shop_discovery','status':'failed','error':'database is locked',
             'updated_at':'2026-10-26T08:00:00+08:00','created_at':'2026-10-26T08:00:00+08:00',
             'sources':['catfk'],'counts':{},'source_progress':{},'discovered_links':1,'success_shops':0,
             'collected_goods':0,'discovered_shops':406,'pending_shops':373,'unverified_stock_shops':1,'pages':0,'page_limit':200}
        await page.route('**/api/discovery/jobs',lambda route:route.fulfill(json={'jobs':[job]}))
        await page.locator('nav [data-page=discovery]').click()
        await page.locator('#discoveryHistory').filter(has_text='目录 406 家').wait_for()
        assert '待采 373 家' in await page.locator('#discoveryHistory').inner_text()
        assert '本地数据库写入冲突' in await page.locator('#discoveryHistory').inner_text()
        await page.locator('#discoveryShops').click()
        await page.locator('#shopPreviewDialog .history-item').click()
        assert '目录预览商品' in await page.locator('#shopPreviewDialog').inner_text()
        assert '数量未公开' in await page.locator('#shopPreviewDialog').inner_text()
        assert '库存待核实' in await page.locator('#shopPreviewDialog').inner_text()
        await page.locator('#shopPreviewDialog .icon-button').click()
        print('PASS staged task progress and shop directory preview',flush=True)

        requests.clear()
        await page.locator('nav [data-page=chat]').click()
        await page.locator('#chatInput').fill('隔离测试')
        await page.locator('#sendBtn').click()
        await page.locator('.msg.ai .bubble').filter(has_text='隔离测试回复').wait_for()
        await page.wait_for_function("!document.getElementById('sendBtn').disabled")
        assert requests.count('/api/chat')==1 and '/api/llm/test' not in requests
        assert await page.locator('#llmBadge').inner_text()=='LLM 已连接'
        await page.locator('nav [data-page=settings]').click()
        await page.locator('#page-settings button').filter(has_text='测试连接').click()
        await page.locator('#llmBadge').filter(has_text='LLM 连接失败').wait_for()
        assert requests.count('/api/llm/test')==1
        print('PASS chat sends once, handles CRLF streams, and only explicit connection testing probes the LLM',flush=True)
        assert not errors,'browser errors: '+json.dumps(errors,ensure_ascii=False)
        print('PASS no uncaught browser errors',flush=True)
        await browser.close()


if __name__=='__main__':asyncio.run(main())
