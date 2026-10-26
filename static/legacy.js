/* ================= 通用 ================= */
const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);
const fmt = n => n==null?'-':Number(n).toLocaleString('zh-CN');
const money = n => n==null?'-':'¥'+Number(n).toFixed(2);
const nativeMoney = (n,currency) => n==null?'未公开':(currency==='CNY'?'¥':currency?currency+' ':'币种未公开 ')+Number(n).toFixed(2);
function toast(msg){const t=$('#toast');t.textContent=msg;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2200)}
function readBrowserState(key){try{return localStorage.getItem(key);}catch{return null;}}
function writeBrowserState(key,value){try{localStorage.setItem(key,value);return true;}catch{toast('浏览器未允许保存，本次会话仍可使用');return false;}}
let dataSource=readBrowserState('wzyp_source')||'16688';
if(dataSource==='wzyp'){dataSource='wzyp-live';writeBrowserState('wzyp_source',dataSource)}
let crawlJob=null,crawlTimer=null,crawlPolling=false,crawlActionBusy=false,singleSyncBusy=false;
let crawlLastRefresh=0,crawlLastSnapshot='';
let sourceRegistry=[];
function isCrawlableSource(src=dataSource){return !!sourceRegistry.find(x=>x.id===src)?.discovery}
function isHostedSource(src=dataSource){return sourceRegistry.find(x=>x.id===src)?.family==='hosted'}
function crawlApiBase(src=dataSource){return '/api/sources/'+(src==='wzyp-live'?'wzyp':src)}
function sourceURL(path){const url=new URL(path,location.origin);url.searchParams.set('source',dataSource);return url.pathname+url.search}
const apiClient=import('/static/modules/api.js');
async function api(path,body,options={}){const {request}=await apiClient;return request(sourceURL(path),body,options);}
window.addEventListener('unhandledrejection',e=>{if(e.reason?.name!=='AbortError')toast(e.reason?.message||'请求失败，请检查连接');e.preventDefault()});
function applySourceUI(){
  const is16688=!isHostedSource();
  const crawlable=isCrawlableSource();
  $('#dataSource').value=dataSource;
  $('#sourcePanel').hidden=!crawlable;
  $('#sourceReadNotice').hidden=dataSource!=='yike'||['discovery','catalog','compare'].some(page=>$('#page-'+page).classList.contains('active'));
  if(dataSource==='wzyp-live'){
    $('#sourcePanelTitle').textContent='链动小铺公开商品采集';
    $('#sourcePanelHint').textContent='从公开历史快照中的 250 家店铺入口及公告、商品描述中的公开链接发现店铺。仅展示从原站完整采集成功的数据，不导入历史商品或价格；覆盖范围不代表全站完整数据。';
    $('#crawlUrls').placeholder='可选：每行一个链动小铺店铺（/shop/token）或商品（/item/key）链接，加入下一次采集任务';
    $('#syncUrl').placeholder='输入链动小铺店铺或商品链接，可单独同步整店';
  }else if(dataSource==='catfk'){
    $('#sourcePanelTitle').textContent='云猫寄售公开商品采集';
    $('#sourcePanelHint').textContent='从公开索引、已有入口和补充链接发现云猫店铺，完整采集后更新独立快照。自动恢复失败时跳过该站，其他平台继续。';
    $('#crawlUrls').placeholder='每行一个云猫店铺（/shop/token）或商品（/item/key）链接';
    $('#syncUrl').placeholder='输入云猫店铺或商品 HTTPS 链接';
  }else if(dataSource!=='16688'){
    $('#sourcePanelTitle').textContent=(sourceRegistry.find(x=>x.id===dataSource)?.name||dataSource)+' 公开货铺采集';
    $('#sourcePanelHint').textContent='可补充独立货铺或商品链接，也可在采集任务中心跨平台自动识别。整店成功后更新快照；失败保留原数据。总览和店铺价格统计仅统计人民币商品，其他币种请到商品搜索查看。';
    $('#crawlUrls').placeholder='每行一个公开 HTTPS 货铺或商品链接';
    $('#syncUrl').placeholder='输入货铺或商品 HTTPS 链接，采集该站全部公开商品';
  }else{
    $('#sourcePanelTitle').textContent='16688 平台公开商品采集';
    $('#sourcePanelHint').textContent='已发现公开店铺的商品快照 · 从公开索引和商品链接发现多家店铺，再向 16688 原站采集。覆盖范围不代表全站完整数据。标价未含手续费，“万+付款”为公开销量下限。';
    $('#crawlUrls').placeholder='可选：每行一个 16688 店铺或商品链接，加入下一次平台采集任务';
    $('#syncUrl').placeholder='输入明确的商品或店铺链接，可单独同步整店';
  }
  $('#salesChartTitle').textContent=is16688?'店铺公开销量下限 Top10':'店铺销量 Top10';
  $('#gType').innerHTML='<option value="">全部类型</option>'+(is16688?
    '<option value="auto">自动发货</option><option value="manual">手动发货</option><option value="unknown">未公开</option>':
    '<option value="card">卡密</option><option value="article">文章</option><option value="resource">资源</option><option value="equity">权益</option>');
  if(dataSource==='yike')$('#gType').innerHTML='<option value="">全部类型</option><option value="卡密">卡密</option><option value="unknown">未公开</option>';
  const typeCols=is16688?[['auto_count','自动'],['manual_count','手动'],['unknown_count','未公开']]:[['card_count','卡密'],['equity_count','权益'],['resource_count','资源']];
  $$('#shopHead th').forEach((th,i)=>{if(i>=3&&i<=5){th.dataset.sort=typeCols[i-3][0];th.textContent=typeCols[i-3][1]}});
  $('#shopHead th[data-sort="sell_count"]').textContent=is16688?'公开付款数':'销量';
  $('#shopSort').innerHTML=(is16688?SHOP_SORT_OPTIONS.filter(([v])=>!['card_count','equity_count'].includes(v)).concat([['auto_count','按自动发货数'],['manual_count','按手动发货数']]):SHOP_SORT_OPTIONS).map(([v,l])=>`<option value="${v}">${l}</option>`).join('');
  $('#shopSort').value=shopQ.sort;
  clearTimeout(crawlTimer);
  if(crawlable) scheduleCrawlPoll(0);
}
async function switchSource(value){
  if(sending){$('#dataSource').value=dataSource;toast('请等待当前 AI 回复完成后切换数据源');return}
  shopRequestId++;goodsRequestId++;detailRequestId++;historyRequestId++;goodsInitId++;
  shopController?.abort();goodsController?.abort();detailController?.abort();
  dataSource=value;writeBrowserState('wzyp_source',value);
  shopLoaded=false;goodsLoaded=false;chatInited=false;
  crawlJob=null;crawlLastSnapshot='';$('#syncStatus').textContent='';
  $('#crawlStatus').textContent='尚未开始平台采集。已有店铺快照仍可查询。';renderCrawl(null);
  threadId='t-'+Date.now().toString(36);threads=[];
  shopQ.sort='goods_count';shopQ.order='desc';goodsQ.sort='price';goodsQ.order='desc';
  ['shopKw','gMin','gMax'].forEach(id=>$('#'+id).value='');resetGoodsKeywords();
  $('#shopFetch').value='';$('#gShop').innerHTML='<option value="">全部店铺</option>';$('#gExcludeZero').checked=false;
  $$('.fbuilder .frow').forEach(row=>row.remove());
  $$('.fbuilder').forEach(box=>{box.classList.remove('open');delete box.dataset.rows});
  $('#shopOrder').value='desc';$('#gOrder').value='desc';$('#gSort').value='price';
  closeDrawer();closeConfirm();applySourceUI();
  renderMessages([]);$('#threadList').innerHTML='';window._drillMap=[];
  $('#statGrid').innerHTML='';['typeChart','priceChart','wordCloud','sellChart'].forEach(id=>$('#'+id).innerHTML='');
  $('#shopTable tbody').innerHTML='';$('#goodsTable tbody').innerHTML='';
  updateHeadMarks('#shopHead',shopQ);updateHeadMarks('#goodsHead',goodsQ);
  if($('#page-dashboard').classList.contains('active'))await loadDashboard();
  if($('#page-shops').classList.contains('active')) await loadShops(1);
  if($('#page-goods').classList.contains('active')) await initGoodsPage();
  if($('#page-chat').classList.contains('active')) initChat();
}
async function sync16688(){
  const button=$('#syncButton'), status=$('#syncStatus');
  if(button.disabled)return;
  const targetSource=dataSource;
  singleSyncBusy=true;button.disabled=true;button.textContent='正在同步…';status.textContent='正在识别店铺并读取公开商品，请稍候…';
  try{
    const result=await api(crawlApiBase(targetSource)+'/sync',{url:$('#syncUrl').value.trim()},{timeoutMs:21610000});
    status.textContent=`已同步「${result.shop.nickname}」：${result.goods_count} 件商品、${result.detail_count} 条详情 · ${new Date(result.synced_at).toLocaleString('zh-CN')}`;
    await switchSource(targetSource);toast('店铺数据已更新');
  }catch(e){status.textContent='同步失败：'+e.message;toast(e.message)}
  finally{singleSyncBusy=false;button.textContent='同步单个店铺';renderCrawl(crawlJob)}
}
function crawlActive(){return crawlJob&&['running','cancelling'].includes(crawlJob.status)}
function renderCrawl(job){
  crawlJob=job;
  const active=crawlActive(), busy=active||crawlActionBusy;
  $('#discoverButton').disabled=busy||singleSyncBusy;
  $('#refreshButton').disabled=busy||singleSyncBusy;
  $('#syncButton').disabled=busy||singleSyncBusy;
  $('#cancelCrawlButton').hidden=!active;
  $('#cancelCrawlButton').disabled=crawlActionBusy||job?.status==='cancelling';
  $('#resumeCrawlButton').hidden=!job||!['partial','failed','cancelled','interrupted'].includes(job.status);
  $('#resumeCrawlButton').disabled=busy;
  $('#crawlProgress').hidden=!job;
  if(!job){$('#crawlStatus').textContent='尚未开始平台采集。已有店铺快照仍可查询。';return}
  const statuses={running:'采集中',cancelling:'正在取消，等待当前请求退出',cancelled:'已取消，可继续',interrupted:'已中断，可继续',completed:'本次采集完成',partial:'部分完成，可重试失败项',failed:'采集失败，可重试'};
  const phases={shop_discovery:'第一阶段：搜索店铺目录',goods_collection:'第二阶段：采集整店商品',discovering:'发现公开入口',collecting:'逐店采集',finished:'已结束',interrupted:'已中断'};
  $('#crawlStatus').textContent=`${statuses[job.status]||job.status} · ${phases[job.phase]||job.phase}${job.phase==='discovering'?' · 已读取 '+job.discovery_pages+' 页索引':''} · 最近更新 ${new Date(job.updated_at).toLocaleString('zh-CN')}`;
  const metrics=[['发现链接',job.discovered_links],['核验店铺',job.resolved_shops],['成功店铺',job.success_shops],['失败店铺 / 入口',job.failure_count],['本次商品数',job.collected_goods],['待处理链接',job.pending_links]];
  if(job.workflow_version===2)metrics.push(['发现店铺',job.discovered_shops],['待采店铺',job.pending_shops],['库存待核实',job.unverified_stock_shops]);
  $('#crawlMetrics').innerHTML=metrics.map(([label,value])=>`<div>${label}<b>${fmt(value)}</b></div>`).join('');
  const failures=[job.discovery_error,job.error,...job.failures.map(f=>`${f.url}：${f.error}`)].filter(Boolean);
  $('#crawlErrorBox').hidden=!failures.length;
  $('#crawlFailures').textContent=failures.join('\n');
}
function scheduleCrawlPoll(delay=crawlActive()?3000:15000){clearTimeout(crawlTimer);if(!document.hidden&&!$('#sourcePanel').hidden&&isCrawlableSource())crawlTimer=setTimeout(pollCrawl,delay)}
document.addEventListener('visibilitychange',()=>{if(document.hidden)clearTimeout(crawlTimer);else scheduleCrawlPoll(0)});
async function refreshCrawlViews(){
  if(!isCrawlableSource())return;
  shopLoaded=false;goodsLoaded=false;
  if($('#page-dashboard').classList.contains('active'))await loadDashboard();
  if($('#page-shops').classList.contains('active'))await loadShops(shopPage);
  if($('#page-goods').classList.contains('active'))await initGoodsPage(goodsPage);
}
async function pollCrawl(){
  if(document.hidden||$('#sourcePanel').hidden)return;
  if(crawlPolling){scheduleCrawlPoll();return}
  crawlPolling=true;
  const pollSource=dataSource;
  try{
    const {job}=await api(crawlApiBase(pollSource)+'/crawl');
    if(pollSource!==dataSource||!isCrawlableSource())return;
    renderCrawl(job);
    if(job){
      const snapshot=job.job_id+':'+job.success_shops;
      if(snapshot!==crawlLastSnapshot&&job.success_shops>0&&(!crawlActive()||Date.now()-crawlLastRefresh>10000)){
        crawlLastSnapshot=snapshot;crawlLastRefresh=Date.now();await refreshCrawlViews();
      }
    }
  }catch(e){if(pollSource===dataSource&&isCrawlableSource())$('#crawlStatus').textContent='暂时无法读取进度：'+e.message}
  finally{crawlPolling=false;scheduleCrawlPoll()}
}
async function startCrawl(mode){
  if(crawlActionBusy)return;
  crawlActionBusy=true;renderCrawl(crawlJob);
  try{
    const urls=$('#crawlUrls').value.split(/\r?\n/).map(v=>v.trim()).filter(Boolean);
    renderCrawl(await api(crawlApiBase()+'/crawl',{mode,urls}));
    toast(crawlJob.reused?'已有任务正在采集，已显示当前进度':'已启动平台采集');scheduleCrawlPoll(0);
  }catch(e){toast(e.message)}
  finally{crawlActionBusy=false;renderCrawl(crawlJob)}
}
async function changeCrawl(action){
  if(!crawlJob||crawlActionBusy)return;
  crawlActionBusy=true;renderCrawl(crawlJob);
  try{renderCrawl(await api(`${crawlApiBase()}/crawl/${encodeURIComponent(crawlJob.job_id)}/${action}`,{}));scheduleCrawlPoll(0)}
  catch(e){toast(e.message)}
  finally{crawlActionBusy=false;renderCrawl(crawlJob)}
}
function esc(s){return (s==null?'':String(s)).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}
/* markdown 渲染（marked + XSS 转义） */
function mdRender(s){return window.renderSafeMarkdown?window.renderSafeMarkdown(s):esc(s)}

/* ================= 页面切换 ================= */
$$('nav button').forEach(b=>b.onclick=()=>{
  $$('nav button').forEach(x=>x.classList.remove('active'));b.classList.add('active');
  $$('.page').forEach(p=>p.classList.remove('active'));
  $('#page-'+b.dataset.page).classList.add('active');
  queueMicrotask(()=>scheduleCrawlPoll(0));
  if(b.dataset.page==='dashboard')loadDashboard();
  if(b.dataset.page==='settings')loadSettings();
  if(b.dataset.page==='shops') loadShops(shopPage);
  if(b.dataset.page==='goods') initGoodsPage(goodsPage);
  if(b.dataset.page==='chat' && !chatInited) initChat();
});

/* ================= 总览（全部可点击下钻） ================= */
async function loadDashboard(){
  const requestedSource=dataSource;
  const d = await api('/api/stats');
  if(requestedSource!==dataSource)return;
  const s=d.stats;
  /* 卡片: [label, value, drill] drill=null 表示不可下钻 */
  const cards = [
    ['店铺数', fmt(s.shop_count), {page:'shops'}],
    ['在售商品', fmt(s.goods_count), {page:'goods'}],
    ['商品详情', fmt(s.detail_count), {page:'goods',filters:[{field:'status',op:'eq',value:1}]}],
    [dataSource==='16688'?'公开销量下限':'总销量', fmt(s.total_sell), {page:'shops',sort:'sell_count',order:'desc'}],
    ['总保证金', '¥'+fmt(s.total_deposit), {page:'shops',sort:'deposit_money',order:'desc'}],
    ['商品均价', money(s.avg_price), {page:'goods',filters:[{field:'price',op:'gte',value:0}],sort:'price',order:'desc'}],
    ['最低价', money(s.min_price), {page:'goods',filters:[{field:'price',op:'eq',value:s.min_price}]}],
    ['最高价', money(s.max_price), {page:'goods',filters:[{field:'price',op:'eq',value:s.max_price}],sort:'price',order:'desc'}],
  ];
  $('#statGrid').innerHTML = cards.map(([l,v,drill],i)=>
    `<div class="stat-card ${drill?'clickable':'plain'}" ${drill?`onclick="drill(${i})"`:''} title="${drill?'点击查看明细':''}"><div class="label">${l}</div><div class="value">${v}</div></div>`
  ).join('');
  window._drillMap = cards.map(c=>c[2]);

  /* 类型分布 → 点某类型跳商品页按该类型筛选 */
  barChart('#typeChart', d.type_dist.map(t=>[typeZh(t.goods_type),t.count]), (label)=>{
    const en = dataSource==='yike'?d.type_dist.find(t=>typeZh(t.goods_type)===label)?.goods_type:{'卡密':'card','文章':'article','资源':'resource','权益':'equity','自动发货':'auto','手动发货':'manual','未公开':'unknown'}[label];
    if(en) drillTo({page:'goods', gType:en, sort:'price', order:'asc'});
  });
  /* 价格区间 → 点某区间跳商品页价格筛选 */
  const bucketRange = b => ({
    '<10':[0,9.99],'10-50':[10,50],'50-100':[50,100],
    '100-500':[100,500],'500-2000':[500,2000],'>=2000':[2000,null]
  }[b]);
  barChart('#priceChart', d.price_buckets.map(p=>[p.bucket+'元',p.count]), (label)=>{
    const b = label.replace('元','');
    const [lo,hi] = bucketRange(b) || [null,null];
    drillTo({page:'goods', gMin:lo, gMax:hi, sort:'price', order:'asc'});
  });
  /* 热词 → 点词跳商品页关键词搜索 */
  const maxC=Math.max(...d.top_keywords.map(w=>w.count));
  $('#wordCloud').innerHTML=d.top_keywords.map(w=>{
    const cls=w.count>maxC*.6?'w-lg':w.count>maxC*.3?'w-md':'w-sm';
    return `<span class="${cls}" title="含该词商品 ${w.count} 件，点击查看" onclick="drillTo({page:'goods',gKw:'${w.word.replace(/'/g,"\\'")}'})">${esc(w.word)} <b style="font-size:.8em;opacity:.75">${w.count}</b></span>`;
  }).join('');
  /* 销量Top10 → 点某店跳该店商品 */
  const shops=await api('/api/shops',{sort:'sell_count',order:'desc',page:1,pageSize:10});
  if(requestedSource!==dataSource)return;
  barChart('#sellChart', shops.rows.map(r=>[r.nickname.slice(0,10),r.sell_count]), (label)=>{
    const shop = shops.rows.find(r=>r.nickname.slice(0,10)===label);
    if(shop) drillTo({page:'goods', gShop:shop.token});
  });
}

/* 下钻引擎：带筛选跳到店铺/商品页 */
function drill(i){ const d=window._drillMap[i]; if(d) drillTo(d); }
async function drillTo(opt){
  const page = opt.page || 'shops';
  /* 切页 */
  $$('nav button').forEach(x=>x.classList.remove('active'));
  document.querySelector(`[data-page=${page}]`).classList.add('active');
  $$('.page').forEach(p=>p.classList.remove('active'));
  $('#page-'+page).classList.add('active');
  if(page==='goods' && !goodsLoaded) await initGoodsPage();

  /* 重置目标页全部条件，再应用携带的 */
  if(page==='shops'){
    $('#shopKw').value='';
    $('#shopFetch').value=opt.fetchStatus||'';
    $$('#fbShopRows .frow').forEach(r=>r.remove());
    $('#fbShop').classList.remove('open'); delete $('#fbShop').dataset.rows;
    shopQ.sort=opt.sort||'goods_count'; shopQ.order=opt.order||'desc';
    $('#shopSort').value=shopQ.sort; $('#shopOrder').value=shopQ.order;
    updateHeadMarks('#shopHead',shopQ);
    if(opt.kw) $('#shopKw').value=opt.kw;
    if(opt.filters) applyFiltersToBuilder('shop',opt.filters);
    await loadShops(1);
  }else{
    ['gMin','gMax'].forEach(i=>$('#'+i).value='');resetGoodsKeywords();
    $('#gType').value=''; $('#gShop').value='';
    $('#gExcludeZero').checked = !!opt.excludeZero;
    $$('#fbGoodsRows .frow').forEach(r=>r.remove());
    $('#fbGoods').classList.remove('open'); delete $('#fbGoods').dataset.rows;
    goodsQ.sort=opt.sort||'price'; goodsQ.order=opt.order||'desc';
    $('#gSort').value=goodsQ.sort; $('#gOrder').value=goodsQ.order;
    updateHeadMarks('#goodsHead',goodsQ);
    if(opt.gKw!=null) $('#gKw').value=opt.gKw;
    if(opt.gType!=null) $('#gType').value=opt.gType;
    if(opt.gMin!=null) $('#gMin').value=opt.gMin;
    if(opt.gMax!=null) $('#gMax').value=opt.gMax;
    if(opt.gShop!=null) $('#gShop').value=opt.gShop;
    if(opt.filters) applyFiltersToBuilder('goods',opt.filters);
    await loadGoods(1);
  }
}
/* 把筛选规则渲染进筛选构建器（可视化携带的条件） */
function applyFiltersToBuilder(kind,filters){
  const id = kind==='shop'?'#fbShopRows':'#fbGoodsRows';
  const box = kind==='shop'?'#fbShop':'#fbGoods';
  filters.forEach(f=>addFilterRow(kind));
  const rows=$$(id+' .frow');
  filters.forEach((f,i)=>{
    const r=rows[i];
    if(!r) return;
    r.querySelector('.f-field').value=f.field;
    const opSel=r.querySelector('.f-op'); opSel.value=f.op;
    const inp=r.querySelector('.f-val');
    inp.type = (FIELD_TYPES[f.field]==='text'||FIELD_TYPES[f.field]==='date') ? (FIELD_TYPES[f.field]==='date'?'date':'text') : 'number';
    inp.value=f.value;
  });
  $(box).classList.add('open'); $(box).dataset.rows=1;
}

function typeZh(t){return{card:'卡密',article:'文章',resource:'资源',equity:'权益',auto:'自动发货',manual:'手动发货',unknown:'未公开'}[t]||t}
function barChart(sel,rows,onLabel){
  const max=Math.max(...rows.map(r=>r[1]),1);
  $(sel).innerHTML=rows.map(([l,v],i)=>{
    const click = onLabel ? `onclick="BARCB['${sel}'](${i})"` : '';
    return `<div class="bar-row ${onLabel?'clickable':''}" ${click} title="${onLabel?'点击筛选':''}"><div class="bar-label">${esc(l)}</div><div class="bar-track"><div class="bar-fill" style="width:${Math.max(v/max*100,6)}%">${fmt(v)}</div></div></div>`;
  }).join('');
  if(onLabel){
    window.BARCB = window.BARCB||{};
    window.BARCB[sel] = i => onLabel(rows[i][0], i);
  }
}

/* ================= 筛选构建器 ================= */
const SHOP_FIELDS = [
  ['nickname','店名'],['token','token'],['goods_count','商品数'],['card_count','卡密数'],
  ['equity_count','权益数'],['resource_count','资源数'],['sell_count','销量'],
  ['deposit_money','保证金'],['avg_price','均价'],['min_price','最低价'],['max_price','最高价'],
  ['contact_qq','QQ'],['login_province','省份'],['create_time','开店日期'],
];
const GOODS_FIELDS = [
  ['name','商品名'],['goods_key','goods_key'],['price','价格'],['market_price','市场价'],
  ['goods_type','类型'],['status','状态'],['shop_token','店铺token'],['shop_name','店铺名'],
];
const FIELD_TYPES = {nickname:'text',token:'text',contact_qq:'text',login_province:'text',
  create_time:'date',goods_type:'text',shop_name:'text',goods_key:'text',shop_token:'text'};
const OPS_LIST = [
  ['gt','> 大于'],['gte','≥ 大于等于'],['lte','≤ 小于等于'],['lt','< 小于'],
  ['eq','= 等于'],['ne','≠ 不等于'],['like','包含'],
];
function toggleBuilder(kind){
  const el = kind==='shop' ? $('#fbShop') : $('#fbGoods');
  el.classList.toggle('open');
  if(el.classList.contains('open') && !el.dataset.rows){
    addFilterRow(kind); el.dataset.rows=1;
  }
}
function addFilterRow(kind){
  const fields = kind==='shop'?(dataSource==='16688'?
    SHOP_FIELDS.filter(([field])=>!['card_count','article_count','resource_count','equity_count','shop_newbie','create_time'].includes(field)).concat([['auto_count','自动发货数'],['manual_count','手动发货数'],['unknown_count','未公开类型数']]):SHOP_FIELDS):GOODS_FIELDS;
  const id = kind==='shop'?'#fbShopRows':'#fbGoodsRows';
  const div=document.createElement('div');
  div.className='frow';
  div.innerHTML=`
    <select class="f-field">${fields.map(([v,l])=>`<option value="${v}">${l}</option>`).join('')}</select>
    <select class="f-op">${OPS_LIST.map(([v,l])=>`<option value="${v}">${l}</option>`).join('')}</select>
    <input class="f-val" placeholder="值" style="width:110px">
    <button class="del" onclick="this.parentNode.remove()">✕</button>`;
  div.querySelector('.f-field').onchange=e=>{
    const t=FIELD_TYPES[e.target.value];
    const inp=div.querySelector('.f-val');
    inp.type = t==='date'?'date': t==='text'?'text':'number';
    if(t==='text'||t==='date') div.querySelector('.f-op').value='like'||div.querySelector('.f-op').value;
  };
  $(id).appendChild(div);
}
function collectFilters(kind){
  const rows = kind==='shop' ? $$('#fbShopRows .frow') : $$('#fbGoodsRows .frow');
  const out=[];
  rows.forEach(r=>{
    const f=r.querySelector('.f-field').value, o=r.querySelector('.f-op').value;
    let v=r.querySelector('.f-val').value;
    if(v===''||v==null) return;
    const numeric=!(FIELD_TYPES[f]==='text'||FIELD_TYPES[f]==='date');
    if(o!=='like'&&numeric) v=Number(v);
    if(f==='goods_type'&&o!=='like') v=String(v);
    out.push({field:f,op:o,value:v});
  });
  return out.length?out:null;
}

/* ================= 排序（表头点击 + 下拉） ================= */
const SHOP_SORT_OPTIONS=[['goods_count','按商品数'],['sell_count','按销量'],['deposit_money','按保证金'],
  ['avg_price','按均价'],['min_price','按最低价'],['max_price','按最高价'],['create_time','按开店时间'],
  ['card_count','按卡密数'],['equity_count','按权益数'],['nickname','按店名']];
const GOODS_SORT_OPTIONS=[['price','按价格'],['name','按商品名'],['shop_name','按店铺'],
  ['goods_type','按类型'],['market_price','按市场价'],['status','按状态']];
$('#shopSort').innerHTML=SHOP_SORT_OPTIONS.map(([v,l])=>`<option value="${v}">${l}</option>`).join('');
$('#gSort').innerHTML=GOODS_SORT_OPTIONS.map(([v,l])=>`<option value="${v}">${l}</option>`).join('');

function bindHeadSort(headId, sortSelId, orderSelId, reloadFn, stateObj){
  $$(headId+' th.sortable').forEach(th=>{
    th.onclick=()=>{
      const f=th.dataset.sort;
      if(stateObj.sort===f){ stateObj.order = stateObj.order==='asc'?'desc':'asc'; }
      else { stateObj.sort=f; stateObj.order = f==='nickname'||f==='name'?'asc':'desc'; }
      $(sortSelId).value=stateObj.sort;
      $(orderSelId).value=stateObj.order;
      updateHeadMarks(headId,stateObj);
      reloadFn(1);
    };
  });
}
function updateHeadMarks(headId,stateObj){
  $$(headId+' th.sortable').forEach(th=>{
    th.classList.remove('asc','desc');
    if(th.dataset.sort===stateObj.sort) th.classList.add(stateObj.order);
  });
}
$('#shopSort').onchange=()=>{shopQ.sort=$('#shopSort').value;updateHeadMarks('#shopHead',shopQ);loadShops(1)};
$('#shopOrder').onchange=()=>{shopQ.order=$('#shopOrder').value;updateHeadMarks('#shopHead',shopQ);loadShops(1)};
$('#gSort').onchange=()=>{goodsQ.sort=$('#gSort').value;updateHeadMarks('#goodsHead',goodsQ);loadGoods(1)};
$('#gOrder').onchange=()=>{goodsQ.order=$('#gOrder').value;updateHeadMarks('#goodsHead',goodsQ);loadGoods(1)};

/* ================= 店铺 ================= */
let shopLoaded=false, shopPage=1, shopPageSize=20,shopRequestId=0,shopController,goodsController,detailController,detailRequestId=0,goodsInitId=0;
document.addEventListener('snapshots-changed',event=>{if(event.detail.source===dataSource){shopLoaded=false;goodsLoaded=false;}});
const shopQ={sort:'goods_count',order:'desc'};
bindHeadSort('#shopHead','#shopSort','#shopOrder',loadShops,shopQ);
async function loadShops(page=1){
  const requestedSource=dataSource,requestId=++shopRequestId;
  shopController?.abort();shopController=new AbortController();
  shopPage=page;
  const q={kw:$('#shopKw').value.trim()||null,
    fetch_status:$('#shopFetch').value||null,
    filters:collectFilters('shop'),
    sort:shopQ.sort,order:shopQ.order,page,pageSize:shopPageSize};
  let d;try{d=await api('/api/shops',q,{signal:shopController.signal});}catch(error){if(error.name==='AbortError')return;throw error;}
  if(requestedSource!==dataSource||requestId!==shopRequestId)return;
  $('#shopTable tbody').innerHTML=d.rows.map(r=>{
    const fc=r.fetched_count||0;
    let st;
    if(r.goods_count===0) st='<span class="st-badge st-empty">📦 无商品</span>';
    else if(r.fetch_done===1) st='<span class="st-badge st-done">✅ 已抓完</span>';
    else if(fc===0) st='<span class="st-badge st-fetching">⏳ 待抓取</span>';
    else st=`<span class="st-badge st-fetching">⏳ ${fmt(fc)} 已入库</span>`;
    return `<tr>
    <td><a class="shop-link" onclick="gotoShop('${esc(r.token)}')">${esc(r.nickname)}</a><div style="font-size:11px;color:var(--sub)">${esc(r.token)}</div></td>
    <td>${esc(r.login_province)||'-'}</td>
    <td class="num"><b>${fmt(r.goods_count)}</b><div class="fetch-hint">已入库 ${fmt(fc)}</div></td>
    <td class="num">${fmt(!isHostedSource()?r.auto_count:r.card_count)}</td>
    <td class="num">${fmt(!isHostedSource()?r.manual_count:r.equity_count)}</td>
    <td class="num">${fmt(!isHostedSource()?r.unknown_count:r.resource_count)}</td>
    <td class="num">${dataSource==='16688'?esc(r.sales_count_text)||'未公开':fmt(r.sell_count)}</td>
    <td class="num">${money(r.avg_price)}</td>
    <td class="num" style="font-size:12px">${money(r.min_price)} ~ ${money(r.max_price)}</td>
    <td>${r.create_time||'-'}</td>
    <td style="font-size:12px">${st}</td>
    <td style="font-size:12px">${esc(r.contact_qq)||'-'}</td>
  </tr>`}).join('')||'<tr><td colspan="12" style="text-align:center;color:var(--sub);padding:30px">无数据</td></tr>';
  pager('#shopPager',page,Math.ceil(d.total/shopPageSize),shopPageSize,d.total,loadShops);
  shopLoaded=true;
}
function gotoShop(token){
  $$('nav button').forEach(x=>x.classList.remove('active'));
  document.querySelector('[data-page=goods]').classList.add('active');
  $$('.page').forEach(p=>p.classList.remove('active'));
  $('#page-goods').classList.add('active');
  initGoodsPage().then(()=>{ $('#gShop').value=token; loadGoods(1); });
}

/* ================= 商品 ================= */
let goodsLoaded=false, goodsPage=1, goodsPageSize=20, goodsRequestId=0;
let goodsKeywords=[];
const goodsQ={sort:'price',order:'desc'};
bindHeadSort('#goodsHead','#gSort','#gOrder',loadGoods,goodsQ);
updateHeadMarks('#goodsHead',goodsQ);
function renderGoodsKeywords(){
  $('#gKeywords').innerHTML=goodsKeywords.map((word,index)=>
    `<span class="keyword-tag" role="listitem"><span>${esc(word)}</span><button type="button" aria-label="删除关键词 ${esc(word)}" data-index="${index}">×</button></span>`
  ).join('');
  $$('#gKeywords button').forEach(button=>{
    button.onclick=()=>{goodsKeywords.splice(Number(button.dataset.index),1);renderGoodsKeywords();loadGoods(1)};
  });
}
function commitGoodsKeyword(){
  const word=$('#gKw').value.trim();
  $('#gKw').value='';
  if(!word||goodsKeywords.some(existing=>existing.toLowerCase()===word.toLowerCase()))return false;
  goodsKeywords.push(word);renderGoodsKeywords();return true;
}
function addGoodsKeyword(){commitGoodsKeyword();$('#gKw').focus();return loadGoods(1)}
function resetGoodsKeywords(){goodsKeywords=[];$('#gKw').value='';renderGoodsKeywords()}
function buildGoodsQuery(page=1){
  if(commitGoodsKeyword())page=1;
  const fs=collectFilters('goods')||[];
  if($('#gExcludeZero').checked)fs.push({field:'price',op:'gt',value:0});
  return {shop_token:$('#gShop').value||null,goods_type:$('#gType').value||null,
    keywords:[...goodsKeywords],
    price_min:$('#gMin').value||null,price_max:$('#gMax').value||null,
    filters:fs.length?fs:null,
    sort:goodsQ.sort,order:goodsQ.order,page,pageSize:goodsPageSize};
}
async function initGoodsPage(page=1){
  const requestedSource=dataSource,requestId=++goodsInitId;
  if(!goodsLoaded){
    const selected=$('#gShop').value, rows=[];
    for(let page=1;;page++){
      const shops=await api('/api/shops',{page,pageSize:500,sort:'nickname',order:'asc'});
      if(requestedSource!==dataSource||requestId!==goodsInitId)return;
      rows.push(...shops.rows);
      if(rows.length>=shops.total||!shops.rows.length)break;
    }
    $('#gShop').innerHTML='<option value="">全部店铺</option>'+rows.map(r=>`<option value="${esc(r.token)}">${esc(r.nickname)}</option>`).join('');
    if(rows.some(r=>r.token===selected))$('#gShop').value=selected;
  }
  goodsLoaded=true;
  await loadGoods(page);
}
async function loadGoods(page=1){
  const requestedSource=dataSource;
  const requestId=++goodsRequestId;
  goodsController?.abort();goodsController=new AbortController();
  const q=buildGoodsQuery(page);
  goodsPage=q.page;
  let d;try{d=await api('/api/goods',q,{signal:goodsController.signal});}catch(error){if(error.name==='AbortError')return;throw error;}
  if(requestedSource!==dataSource||requestId!==goodsRequestId)return;
  $('#goodsTable tbody').innerHTML=d.rows.map(r=>`<tr>
    <td style="max-width:340px"><a class="shop-link" onclick="openDetail('${esc(r.goods_key)}')">${esc(r.name)}</a></td>
    <td style="font-size:12px">${esc(r.shop_name)||''}</td>
    <td><span class="chip ${esc(r.goods_type)}">${esc(typeZh(r.goods_type))}</span></td>
    <td class="num"><b>${esc(nativeMoney(r.price,r.currency??'CNY'))}</b></td>
    <td class="num" style="color:var(--sub)">${r.market_price?esc(nativeMoney(r.market_price,r.currency||'CNY')):'-'}</td>
    <td>${r.status==1?'<span style="color:var(--ok)">在售</span>':'<span style="color:var(--warn)">下架</span>'}</td>
    <td><a class="shop-link" style="font-size:12px" href="${esc(r.link)}" target="_blank">原站↗</a></td>
  </tr>`).join('')||'<tr><td colspan="7" style="text-align:center;color:var(--sub);padding:30px">无数据</td></tr>';
  pager('#goodsPager',q.page,Math.ceil(d.total/q.pageSize),q.pageSize,d.total,loadGoods);
  return {query:q,data:d};
}
function resetGoods(){
  resetGoodsKeywords();['gMin','gMax'].forEach(i=>$('#'+i).value='');
  $('#gType').value='';$('#gShop').value='';$('#gExcludeZero').checked=false;
  $$('#fbGoodsRows .frow').forEach(r=>r.remove());
  goodsQ.sort='price';goodsQ.order='desc';$('#gSort').value='price';$('#gOrder').value='desc';
  updateHeadMarks('#goodsHead',goodsQ);return loadGoods(1);
}
async function openDetail(key){
  const requestedSource=dataSource;
  const requestId=++detailRequestId;detailController?.abort();detailController=new AbortController();
  let d;try{d=await api('/api/goods/'+encodeURIComponent(key),undefined,{signal:detailController.signal});}catch(error){if(error.name==='AbortError')return;throw error;}
  if(requestedSource!==dataSource||requestId!==detailRequestId)return;
  $('#drawerTitle').textContent=(d.name||'商品详情').slice(0,40);
  $('#drawerBody').innerHTML=`
    ${d.image?`<img class="cover" src="${esc(d.image)}" onerror="this.remove()">`:''}
    <div class="kv">
      <span class="k">${dataSource==='16688'?'商品标价':'价格'}</span><span><b>${esc(dataSource==='yike'&&d.status===0?'来源已标记下架':nativeMoney(d.price,d.currency??'CNY'))}</b> ${dataSource==='yike'?'（一可聚合快照）':dataSource==='16688'?'（未含手续费）':'（真实价 '+money(d.real_price)+'）'}</span>
      ${dataSource==='yike'?`<span class="k">来源更新时间</span><span>${esc(d.source_updated_at||'未公开')}（来源原文）</span><span class="k">本地采集时间</span><span>${esc(d.synced_at)}</span><span class="k">一可记录</span><span><a href="${esc(d.source_url)}" target="_blank" rel="noopener noreferrer">查看聚合来源 ↗</a></span><span class="k">原站商品</span><span><a href="${esc(d.link)}" target="_blank" rel="noopener noreferrer">查看商品 ↗</a></span>`:''}
      <span class="k">市场价</span><span>${esc(nativeMoney(d.market_price,d.currency??'CNY'))}</span>
      <span class="k">类型</span><span>${esc(typeZh(d.goods_type||''))}</span>
      <span class="k">goods_key</span><span>${esc(d.goods_key)}</span>
      ${dataSource==='16688'?`<span class="k">平台分类</span><span>${esc(d.category_name)||'未公开'}</span>
      <span class="k">公开付款数</span><span>${esc(d.sales_count_text)||'未公开'}</span>
      <span class="k">可用库存</span><span>${fmt(d.stock_quantity)}</span>
      <span class="k">同步时间</span><span>${esc(d.synced_at)}</span>
      <span class="k">原站</span><span><a class="shop-link" href="${esc(d.link)}" target="_blank" rel="noopener noreferrer">查看商品 ↗</a></span>`:isHostedSource()?`<span class="k">平台分类</span><span>${esc(d.category_name)||'未分类'}</span>
      <span class="k">同步时间</span><span>${esc(d.synced_at)||'-'}</span>
      <span class="k">原站</span><span><a class="shop-link" href="${esc(d.link)}" target="_blank" rel="noopener noreferrer">查看商品 ↗</a></span>`:''}
    </div>
    <iframe class="desc-frame" id="descriptionFrame" title="商品描述" sandbox="" referrerpolicy="no-referrer"></iframe>`;
  $('#descriptionFrame').srcdoc='<meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src https: data:; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'"><style>body{font:14px/1.7 sans-serif;overflow-wrap:anywhere}img{max-width:100%;height:auto}</style>'+(d.description||'无描述');
  $('#drawer').classList.add('open');$('#drawerMask').classList.add('open');
}
function closeDrawer(){detailRequestId++;detailController?.abort();$('#drawer').classList.remove('open');$('#drawerMask').classList.remove('open')}
function pager(sel,page,totalPages,size,total,fn){
  $(sel).innerHTML=`<span>共 ${fmt(total)} 条 · 第 ${page}/${Math.max(totalPages,1)} 页 ·
    <select><option value="20"${size==20?' selected':''}>20/页</option><option value="50"${size==50?' selected':''}>50/页</option><option value="100"${size==100?' selected':''}>100/页</option></select></span>
    <button ${page<=1?'disabled':''}>‹ 上一页</button>
    <button ${page>=totalPages?'disabled':''}>下一页 ›</button>`;
  const btns=$(sel).querySelectorAll('button');
  btns[0].onclick=()=>fn(page-1);btns[1].onclick=()=>fn(page+1);
  $(sel).querySelector('select').onchange=e=>{
    if(sel==='#shopPager')shopPageSize=Number(e.target.value);else goodsPageSize=Number(e.target.value);
    fn(1);
  };
}

/* ================= 聊天 ================= */
let chatInited=false, threadId='t-'+Date.now().toString(36), threads=[], sending=false,historyRequestId=0;
function threadsKey(){return 'wzyp_threads_'+dataSource}
function initChat(){
  try{threads=JSON.parse(readBrowserState(threadsKey())||'[]');}catch{threads=[];}
  threads=Array.isArray(threads)?threads.filter(t=>t&&typeof t.id==='string'&&typeof t.title==='string').slice(0,20):[];
  if(!threads.length){threads=[{id:threadId,title:'新对话'}];writeBrowserState(threadsKey(),JSON.stringify(threads))}
  threadId=threads[0].id;
  renderThreads();
  loadHistory();
  chatInited=true;
}
function renderThreads(){
  $('#threadList').innerHTML=threads.map(t=>`<div class="thread ${t.id===threadId?'active':''}" onclick="switchThread('${t.id}')" title="${esc(t.title)}">${esc(t.title)}<button class="t-del" onclick="event.stopPropagation();confirmDeleteThread('${t.id}','${esc(t.title).replace(/'/g,"&#39;")}')" title="删除此对话">🗑</button></div>`).join('');
}
function switchThread(id){if(sending){toast('请等待当前回复完成');return;}threadId=id;renderThreads();loadHistory()}
function newThread(){
  if(sending){toast('请等待当前回复完成');return;}
  threadId='t-'+Date.now().toString(36);
  threads.unshift({id:threadId,title:'新对话'});
  threads=threads.slice(0,20);
  writeBrowserState(threadsKey(),JSON.stringify(threads));
  renderThreads();renderMessages([]);
}
/* ---- 删除会话（带确认弹窗） ---- */
let confirmAction=null;
function confirmDeleteThread(id,title){
  if(sending){toast('请等待当前回复完成');return;}
  $('#confirmTitle').textContent='删除对话';
  $('#confirmBody').innerHTML=`确定要删除对话 <b>「${esc(title)}」</b> 吗？<br>删除后该对话的全部聊天记录将无法恢复。`;
  $('#confirmMask').classList.add('open');
  confirmAction=()=>doDeleteThread(id);
}
function closeConfirm(){
  $('#confirmMask').classList.remove('open');
  confirmAction=null;
}
$('#confirmOk').onclick=()=>{ if(confirmAction) confirmAction(); closeConfirm(); };
$('#confirmMask').onclick=e=>{ if(e.target===$('#confirmMask')) closeConfirm(); };
document.addEventListener('keydown',e=>{ if(e.key==='Escape') closeConfirm(); });
async function doDeleteThread(id){
  try{
    await api('/api/chat/delete',{thread_id:id});
  }catch(e){/* 后端失败也继续本地删除 */}
  threads=threads.filter(t=>t.id!==id);
  writeBrowserState(threadsKey(),JSON.stringify(threads));
  if(threads.length===0){
    threadId='t-'+Date.now().toString(36);
    threads=[{id:threadId,title:'新对话'}];
    writeBrowserState(threadsKey(),JSON.stringify(threads));
    renderThreads();renderMessages([]);
  }else if(id===threadId){
    threadId=threads[0].id;
    renderThreads();loadHistory();
  }else{
    renderThreads();
  }
  toast('对话已删除');
}
async function clearThread(){
  if(sending){toast('请等待当前回复完成');return;}
  await api('/api/chat/clear?thread_id='+threadId,{});
  renderMessages([]);toast('已清空');
}
async function loadHistory(){
  const current=++historyRequestId,source=dataSource,thread=threadId;
  const d=await api('/api/chat/history?thread_id='+encodeURIComponent(thread));
  if(current!==historyRequestId||source!==dataSource||thread!==threadId||sending)return;
  renderMessages(d.messages||[]);
}
function renderMessages(msgs){
  $('#chatEmpty').style.display=msgs.length?'none':'block';
  const html=msgs.map(m=>{
    if(m.role==='human')return `<div class="msg user"><div class="avatar">我</div><div class="bubble">${esc(m.content)}${m.ctx?`<div class="ctx-banner">📋 ${esc(m.ctx)}</div>`:''}</div></div>`;
    if(m.role==='ai')return `<div class="msg ai"><div class="avatar">AI</div><div class="bubble md">${mdRender(m.content)}${m.tools&&m.tools.length?`<div class="tool-line">🔧 ${m.tools.map(t=>esc(t.tool)).join(', ')}</div>`:''}${renderFollowups(m.followups)}</div></div>`;
    return '';
  }).join('');
  $('#chatScroll').innerHTML='<div id="chatEmpty" style="display:none"></div>'+html;
  scrollChat();
}
function renderFollowups(fs){
  if(!fs||!fs.length) return '';
  return `<div class="followups"><span class="fs-label">💡 继续追问：</span>${fs.map((q,i)=>`<button onclick="askFollowup(this)" data-q="${esc(q)}">${esc(q)}</button>`).join('')}</div>`;
}
function askFollowup(btn){ $('#chatInput').value=btn.dataset.q; sendChat(); }
function askQuick(btn){$('#chatInput').value=btn.textContent.trim();sendChat()}
function scrollChat(){const el=$('#chatScroll');el.scrollTop=el.scrollHeight}
async function sendChat(){
  const text=$('#chatInput').value.trim();
  if(!text||sending)return;
  sending=true;$('#sendBtn').disabled=true;$('#chatInput').value='';
  historyRequestId++;
  $('#chatInput').style.height='auto';
  appendMsg('user',text);
  const bubble=appendMsg('ai','');
  bubble.innerHTML='<span class="typing"><i></i><i></i><i></i></span>';
  let acc='',streamIdleTimer,timedOut=false;
  const streamController=new AbortController();
  const armTimeout=()=>{clearTimeout(streamIdleTimer);streamIdleTimer=setTimeout(()=>{timedOut=true;streamController.abort();},130000);};
  try{
    armTimeout();
    const resp=await fetch(sourceURL('/api/chat'),{method:'POST',signal:streamController.signal,headers:{'Content-Type':'application/json'},
      body:JSON.stringify({thread_id:threadId,message:text})});
    if(!resp.ok){let message='请求失败（'+resp.status+'）';try{const data=await resp.json();if(typeof data.detail==='string')message=data.detail;}catch{}throw Error(message);}
    if(!resp.body)throw Error('服务没有返回可读取的回复');
    const reader=resp.body.getReader();const dec=new TextDecoder();
    let buf='';
    while(true){
      const {done,value}=await reader.read();armTimeout();
      buf+=done?dec.decode():dec.decode(value,{stream:true});
      const lines=buf.replace(/\r\n/g,'\n').split('\n\n');buf=lines.pop();
      if(done&&buf.trim()){lines.push(buf);buf='';}
      for(const line of lines){
        if(!line.startsWith('data: '))continue;
        const payload=line.slice(6);
        if(payload==='[DONE]')continue;
        try{
        const {event,data}=JSON.parse(payload);
        if(event==='token'){refreshLlmBadge(true);acc+=data;bubble.innerHTML=`<div class="md-body md">${mdRender(acc)}</div>`;scrollChat()}
        else if(event==='reset'){acc='';bubble.innerHTML='<span class="typing"><i></i><i></i><i></i></span>'}
        else if(event==='tool'){const tl=document.createElement('div');tl.className='tool-line';tl.textContent=`🔧 ${data.tool}`;bubble.appendChild(tl);scrollChat()}
          else if(event==='followups'){bubble.appendChild(buildFollowupsEl(data));scrollChat()}
          else if(event==='error'){refreshLlmBadge(false);acc+=`\n\n⚠️ ${data.message}`;bubble.innerHTML=`<div class="md">${mdRender(acc)}</div>`}
        }catch(e){}
      }
      if(done)break;
    }
  }catch(e){acc+='\n\n⚠️ '+(timedOut?'回复等待超时，请稍后重试。':e.message);bubble.innerHTML=`<div class="md">${mdRender(acc)}</div>`;refreshLlmBadge(false);}finally{clearTimeout(streamIdleTimer);}
  if(!acc)bubble.innerHTML='<div class="md">（无回复，请检查 LLM 设置）</div>';
  const t=threads.find(x=>x.id===threadId);
  if(t&&t.title==='新对话'){t.title=text.slice(0,16);try{writeBrowserState(threadsKey(),JSON.stringify(threads));}catch{toast('浏览器未允许保存会话列表');}renderThreads()}
  sending=false;$('#sendBtn').disabled=false;$('#chatInput').focus();
}
function buildFollowupsEl(fs){
  const div=document.createElement('div');
  div.className='followups';
  div.innerHTML=`<span class="fs-label">💡 继续追问：</span>`+fs.map(q=>`<button onclick="askFollowup(this)" data-q="${esc(q)}">${esc(q)}</button>`).join('');
  return div;
}
function appendMsg(role,text){
  const empty=$('#chatEmpty'); if(empty) empty.style.display='none';
  const div=document.createElement('div');div.className='msg '+role;
  div.innerHTML=`<div class="avatar">${role==='user'?'我':'AI'}</div><div class="bubble ${role==='ai'?'md':''}"></div>`;
  const bubble=div.querySelector('.bubble');
  if(role==='user')bubble.textContent=text;else bubble.innerHTML=mdRender(text);
  $('#chatScroll').appendChild(div);scrollChat();
  return bubble;
}
function refreshLlmBadge(ok=null){const b=$('#llmBadge');b.className=ok===true?'llm-badge':'llm-badge off';b.textContent=ok===true?'LLM 已连接':ok===false?'LLM 连接失败':'LLM 连接未检测';}

/* ================= AI 解读（查询上下文 → 聊天） ================= */
function condText(){
  const fs=collectFilters('shops');
  return `关键词=${$('#shopKw').value||'无'} 筛选=${fs?JSON.stringify(fs):'无'} 排序=${shopQ.sort}(${shopQ.order==='desc'?'降':'升'}序)`;
}
function condTextG(q){
  return `店铺=${q.shop_token||'全部'} 类型=${typeZh(q.goods_type)||'全部'} 商品名同时包含全部关键词=${JSON.stringify(q.keywords)} 价格=${q.price_min??'不限'}~${q.price_max??'不限'} 筛选=${q.filters?JSON.stringify(q.filters):'无'} 排序=${q.sort}(${q.order==='desc'?'降':'升'}序)`;
}
async function aiInterpret(kind){
  if(kind==='shops'){
    const d=await api('/api/shops',{kw:$('#shopKw').value.trim()||null,filters:collectFilters('shop'),
      sort:shopQ.sort,order:shopQ.order,page:shopPage,pageSize:20});
    const ctx={kind:'店铺列表',condition:condText(),total:d.total,
      rows:d.rows.slice(0,20).map(r=>({店铺:r.nickname,token:r.token,商品数:r.goods_count,销量:r.sell_count,均价:r.avg_price,QQ:r.contact_qq}))};
    jumpToChatWith(JSON.stringify(ctx,null,1),'请解读这份店铺查询结果：亮点店铺、销量与商品结构分布、值得关注的信息。');
  }else{
    const result=await loadGoods(goodsPage);
    if(!result)return;
    const {query:q,data:d}=result;
    const ctx={kind:'商品列表',condition:condTextG(q),total:d.total,
      rows:d.rows.slice(0,20).map(r=>({商品:(r.name||'').slice(0,40),店铺:r.shop_name,类型:typeZh(r.goods_type),价格:r.price}))};
    jumpToChatWith(JSON.stringify(ctx,null,1),'请解读这份商品查询结果：价格分布、品类结构、有什么值得注意的商品。');
  }
}
function jumpToChatWith(context,question){
  $$('nav button').forEach(x=>x.classList.remove('active'));
  document.querySelector('[data-page=chat]').classList.add('active');
  $$('.page').forEach(p=>p.classList.remove('active'));
  $('#page-chat').classList.add('active');
  if(!chatInited)initChat();
  const full=`${question}\n\n<查询上下文>\n${context}\n</查询上下文>`;
  $('#chatInput').value=full;
  sendChat();
}

/* ================= 设置 ================= */
async function loadSettings(){
  const s=await api('/api/llm/settings');
  $('#setBaseurl').value=s.baseurl;$('#setModel').value=s.model;
  $('#setApikey').placeholder=s.apikey||'sk-...（本地服务可留空）';
  $('#setApikey').value='';
}
async function saveLlm(){
  await api('/api/llm/settings',{baseurl:$('#setBaseurl').value,model:$('#setModel').value,apikey:$('#setApikey').value});
  toast('LLM 配置已保存');refreshLlmBadge();
}
function formatLlmError(t){
  const labels={
    service_503:'服务不可用 (HTTP 503)',
    auth_failed:'鉴权失败',
    timeout:'请求超时',
    connection_failed:'连接失败',
    http_error:t.status_code?`请求错误 (HTTP ${t.status_code})`:'请求错误'
  };
  const tag=labels[t.error_type]||(t.status_code?`HTTP ${t.status_code}`:'连接异常');
  return `❌ [${tag}] ${t.error||'未知错误'}`;
}
async function testLlm(){
  await api('/api/llm/settings',{baseurl:$('#setBaseurl').value,model:$('#setModel').value,apikey:$('#setApikey').value});
  const t=await api('/api/llm/test',undefined,{timeoutMs:130000});
  const el=$('#testResult');
  el.className='test-result '+(t.ok?'ok':'bad');
  el.textContent=t.ok?'✅ 连接成功：'+t.reply:formatLlmError(t);
  refreshLlmBadge(t.ok);
}
async function exportConfig(){
  const cfgData=await api('/api/config/export');
  const blob=new Blob([JSON.stringify(cfgData,null,2)],{type:'application/json'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;a.download='wzyp-view-config.json';a.click();
  URL.revokeObjectURL(url);
  const el=$('#ioResult');el.className='test-result ok';
  el.textContent='✅ 已导出 wzyp-view-config.json（含明文密钥，注意保管）';
}
async function importConfig(input){
  const file=input.files[0];
  if(!file)return;
  try{
    const text=await file.text();
    const configData=JSON.parse(text);
    const r=await api('/api/config/import',{config:configData});
    const el=$('#ioResult');
    if(r.ok){el.className='test-result ok';el.textContent='✅ 配置导入成功';loadSettings();refreshLlmBadge()}
    else{el.className='test-result bad';el.textContent='❌ 导入失败：'+r.error}
  }catch(e){
    const el=$('#ioResult');el.className='test-result bad';el.textContent='❌ 文件解析失败：'+e.message;
  }
  input.value='';
}

/* ================= 启动 ================= */
$('#chatInput').addEventListener('keydown',e=>{
  if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendChat()}
});
$('#chatInput').addEventListener('input',function(){
  this.style.height='auto';this.style.height=Math.min(this.scrollHeight,120)+'px';
});
$('#shopKw').addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.isComposing)loadShops(1)});
$('#gKw').addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.isComposing){e.preventDefault();addGoodsKeyword()}});
window.sourceRegistryReady=apiClient.then(({request})=>request('/api/sources')).then(async data=>{sourceRegistry=data.sources;
  if(!sourceRegistry.some(x=>x.id===dataSource))dataSource='16688';
  const select=$('#dataSource');select.replaceChildren();
  for(const source of sourceRegistry){const option=document.createElement('option');option.value=source.id;option.textContent=source.name+(source.kind==='aggregator'?' · 聚合快照':'');select.append(option);}
  select.disabled=false;
  applySourceUI();refreshLlmBadge();
  return data;
});
