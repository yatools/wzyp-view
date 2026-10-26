import {request,esc,safeURL,sourceNames,moneyText,dateText,notify,navigate,provenanceFields} from './api.js';
import {contains,toggleCompare} from './compare.js';

let keywords=[], rows=[], page=1, sequence=0, controller, initialized=false, selectedShop=null;
let detailSequence=0,detailController;
const stockLabel={in_stock:'有货',out_of_stock:'缺货',unknown:'未公开'};
const deliveryLabel={auto:'自动发货',manual:'手动发货',unknown:'未公开'};
const $=selector=>document.querySelector(selector);

function renderTags() {
  $('#catalogKeywords').innerHTML=keywords.map((word,i)=>`<span class="keyword-tag">${esc(word)}<button type="button" data-delete-word="${i}" aria-label="删除关键词 ${esc(word)}">×</button></span>`).join('');
  $('#catalogKeywords').querySelectorAll('button').forEach(button=>button.onclick=()=>{keywords.splice(Number(button.dataset.deleteWord),1);renderTags();loadCatalog(1);});
}
function addKeyword() {
  const value=$('#catalogKeywordInput').value.trim();
  if(value && !keywords.some(x=>x.toLowerCase()===value.toLowerCase())) keywords.push(value);
  $('#catalogKeywordInput').value='';renderTags();
}
function buildQuery(nextPage) {
  const sources=[...document.querySelectorAll('[name="catalogSource"]:checked')].map(x=>x.value);
  if(!sources.length) throw new Error('请至少选择一个来源平台。');
  const numeric=id=>$(id).value===''?null:Number($(id).value);
  const currency=$('#catalogCurrency').value;
  return {sources,keywords,shop_kw:selectedShop?null:$('#catalogShop').value.trim()||null,
    shop_token:selectedShop?.shop_token||null,shop_source:selectedShop?.source||null,
    price_min:numeric('#catalogMin'),price_max:numeric('#catalogMax'),
    currency:currency==='*'?null:currency==='?'?'':currency,
    stock_status:$('#catalogStock').value||null,delivery_method:$('#catalogDelivery').value||null,
    exclude_zero:$('#catalogExcludeZero').checked,sort:$('#catalogSort').value,order:$('#catalogOrder').value,
    page:nextPage,pageSize:Number($('#catalogSize').value)};
}
function drawRows() {
  const body=$('#catalogBody'); if(!body) return;
  body.innerHTML=rows.map((row,i)=>`<tr>
    <td><button class="text-button product-name" data-detail="${i}">${esc(row.name)}</button>${row.sku_label?`<div class="cell-sub">${esc(row.sku_label)}</div>`:''}${row.catalog_entry?'<span class="subtle-tag">原站目录条目</span>':''}${row.category_name?`<span class="subtle-tag">${esc(row.category_name)}</span>`:''}</td>
    <td><span class="source-chip">${esc(row.source==='yike'?'一可聚合快照':sourceNames[row.source])}</span><button class="text-button shop-name" data-shop="${i}">${esc(row.shop_name)}</button></td>
    <td class="num"><strong class="price-value">${esc(moneyText(row.price,row.currency))}</strong></td>
    <td><span class="stock-dot ${row.stock_status==='in_stock'?'available':row.stock_status==='out_of_stock'?'unavailable':''}"></span>${esc(stockLabel[row.stock_status]||'未公开')}${row.stock_quantity!=null?`<div class="cell-sub">数量 ${esc(row.stock_quantity)}</div>`:''}</td>
    <td>${esc(deliveryLabel[row.delivery_method]||'未公开')}</td>
    <td class="date-cell">${esc(dateText(row.synced_at))}${row.source==='yike'?`<div class="product-specs">来源：${esc(row.source_updated_at||'未公开')}（原文）</div>`:''}</td>
    <td><button class="btn ${contains(row)?'btn-selected':'btn-ghost'}" data-compare="${i}" aria-pressed="${contains(row)}">${contains(row)?'已加入 ✓':'＋ 对比'}</button></td>
  </tr>`).join('') || '<tr><td colspan="7"><div class="empty-panel compact"><h2>没有符合条件的商品</h2><p>可以减少关键词、放宽筛选，或去采集任务中心发现新货铺。</p><button class="btn btn-ghost" id="catalogEmptyDiscover">发现货铺</button></div></td></tr>';
  body.querySelectorAll('[data-detail]').forEach(b=>b.onclick=()=>openProduct(rows[Number(b.dataset.detail)]));
  body.querySelectorAll('[data-compare]').forEach(b=>b.onclick=()=>{toggleCompare(rows[Number(b.dataset.compare)]);drawRows();});
  body.querySelectorAll('[data-shop]').forEach(b=>b.onclick=()=>{selectedShop=rows[Number(b.dataset.shop)];$('#catalogShop').value=selectedShop.shop_name;loadCatalog(1);});
  body.querySelector('#catalogEmptyDiscover')?.addEventListener('click',()=>navigate('discovery'));
}

export async function loadCatalog(nextPage=1) {
  if(!initialized) return;
  let query;
  try { query=buildQuery(nextPage); } catch(error) { notify(error.message); return; }
  if(query.price_min!=null && query.price_max!=null && query.price_min>query.price_max) {notify('最低价不能大于最高价。');return;}
  controller?.abort(); controller=new AbortController(); const current=++sequence;
  $('#catalogSearchButton').disabled=true;$('#catalogStatus').textContent='正在读取本地商品快照…';$('#catalogError').hidden=true;
  try {
    const data=await request('/api/catalog/search',query,{signal:controller.signal});
    if(current!==sequence) return;
    rows=data.rows;page=nextPage;drawRows();
    $('#catalogStatus').textContent=`共 ${data.total.toLocaleString('zh-CN')} 件商品 · ${data.source_counts.length} 个来源有匹配结果`;
    const currentCurrency=$('#catalogCurrency').value;
    const currencies=[...new Set(['CNY',...data.currencies.filter(Boolean)])];
    $('#catalogCurrency').innerHTML='<option value="*">全部币种（分别排序）</option>'+currencies.map(x=>`<option value="${esc(x)}">${esc(x)}</option>`).join('')+(data.currencies.includes('')?'<option value="?">币种未公开</option>':'');
    $('#catalogCurrency').value=currentCurrency;
    $('#catalogCurrencyNotice').hidden=query.currency!==null;
    const pages=Math.max(1,Math.ceil(data.total/query.pageSize));
    $('#catalogPager').innerHTML=`<span>${page} / ${pages} 页</span><button class="btn btn-ghost" id="catalogPrevious" ${page<=1?'disabled':''}>上一页</button><button class="btn btn-ghost" id="catalogNext" ${page>=pages?'disabled':''}>下一页</button>`;
    $('#catalogPrevious').onclick=()=>loadCatalog(page-1);$('#catalogNext').onclick=()=>loadCatalog(page+1);
  } catch(error) {
    if(error.name==='AbortError'||current!==sequence) return;
    $('#catalogError').hidden=false;$('#catalogError').textContent=error.message;
    $('#catalogStatus').textContent=rows.length?'查询失败，表格保留上一次成功的结果。':'暂时无法读取商品。';
  } finally { if(current===sequence) $('#catalogSearchButton').disabled=false; }
}

export async function openProduct(item) {
  detailController?.abort();detailController=new AbortController();const current=++detailSequence;
  let dialog=$('#catalogDetail');
  if(!dialog) {
    dialog=document.createElement('dialog');dialog.id='catalogDetail';dialog.className='product-dialog';document.body.append(dialog);
    dialog.addEventListener('click',event=>{if(event.target===dialog)dialog.close();});
    dialog.addEventListener('close',()=>{if(dialog.open)return;detailSequence++;detailController?.abort();});
  }
  dialog.innerHTML='<div class="dialog-header"><h2>商品详情</h2><button class="icon-button" id="closeProductDetail" aria-label="关闭商品详情">×</button></div><div class="dialog-content" role="status">正在读取当前快照…</div>';
  $('#closeProductDetail').onclick=()=>dialog.close();
  if(!dialog.open)dialog.showModal();
  const identity=`${item.source}:${item.goods_key}`;dialog.dataset.identity=identity;
  try {
    const data=await request(`/api/catalog/goods/${encodeURIComponent(item.source)}/${encodeURIComponent(item.goods_key)}`,undefined,{signal:detailController.signal});
    if(current!==detailSequence||!dialog.open)return;
    const link=safeURL(data.link), shop=safeURL(data.shop_link);
    dialog.innerHTML=`<div class="dialog-header"><div><p class="eyebrow">${esc(sourceNames[data.source])}</p><h2>${esc(data.name)}</h2></div><button class="icon-button" id="closeProductDetail" aria-label="关闭商品详情">×</button></div><div class="dialog-content"><div class="detail-summary"><strong class="detail-price">${esc(data.source==='yike'&&data.status===0?'来源已标记下架':moneyText(data.price,data.currency))}</strong><button class="btn btn-primary" id="detailAddCompare">${contains(data)?'从对比移除':'加入对比'}</button></div><dl class="detail-fields">${provenanceFields(data)}<dt>货铺</dt><dd>${shop?`<a href="${esc(shop)}" target="_blank" rel="noopener noreferrer">${esc(data.shop_name)} ↗</a>`:esc(data.shop_name)}</dd><dt>原始规格</dt><dd>${esc(data.sku_label||'未单独提供')}</dd><dt>库存</dt><dd>${esc(data.stock_quantity==null?'未公开精确数量':data.stock_quantity)}</dd><dt>同步时间</dt><dd>${esc(dateText(data.synced_at))}</dd><dt>${data.catalog_entry?'原站目录':'原站商品'}</dt><dd>${link?`<a href="${esc(link)}" target="_blank" rel="noopener noreferrer">${data.catalog_entry?'查看原站目录':'打开商品'} ↗</a>`:'未公开'}</dd></dl><h3 class="description-heading">${data.source==='yike'?'一可记录的商品说明':'原站商品说明'}</h3><iframe class="description-frame" id="catalogDescription" title="原站商品说明" sandbox="" referrerpolicy="no-referrer"></iframe></div>`;
    $('#closeProductDetail').onclick=()=>dialog.close();
    $('#detailAddCompare').onclick=()=>{toggleCompare(data);$('#detailAddCompare').textContent=contains(data)?'从对比移除':'加入对比';};
    const clean=window.cleanPublicHTML?window.cleanPublicHTML(data.description||'<p>原站未公开商品说明。</p>'):esc(data.description||'未公开');
    $('#catalogDescription').srcdoc='<meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src https: data:; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'"><style>body{font:14px/1.8 system-ui,sans-serif;color:#334155;overflow-wrap:anywhere}img{max-width:100%;height:auto}a{color:#4863df}</style>'+clean;
  } catch(error) {if(current===detailSequence&&dialog.open&&error.name!=='AbortError')dialog.querySelector('.dialog-content').textContent=error.message;}
}

export function initializeCatalog() {
  const root=$('#page-catalog');
  root.innerHTML=`<div class="workspace-heading"><p class="eyebrow">多来源，本地快照</p><h1>商品搜索</h1><p>查看已成功采集的公开商品与一可关键词聚合快照。混合搜索优先展示原站记录；加入对比后逐项核对规格与来源。</p></div><form class="card catalog-form" id="catalogForm"><div class="keyword-entry"><label for="catalogKeywordInput" class="sr-only">商品关键词</label><input id="catalogKeywordInput" placeholder="输入商品关键词，例如 GPT，按 Enter 添加" autocomplete="off" maxlength="200"><button class="btn btn-ghost" type="button" id="catalogAddKeyword">＋ 添加关键词</button><button class="btn btn-primary" id="catalogSearchButton" type="submit">搜索商品</button></div><div class="keyword-row"><span class="muted">商品名同时包含全部关键词</span><div class="keyword-tags" id="catalogKeywords" role="list" aria-label="搜索关键词"></div></div><div class="source-options">${Object.entries(sourceNames).map(([source,name])=>`<label class="source-option"><input type="checkbox" name="catalogSource" value="${source}" checked> ${esc(name)}</label>`).join('')}</div><div class="catalog-filter-grid"><label>货铺名称<input id="catalogShop" placeholder="全部货铺" autocomplete="off"></label><label>最低标价<input type="number" id="catalogMin" min="0" step="any" placeholder="不限"></label><label>最高标价<input type="number" id="catalogMax" min="0" step="any" placeholder="不限"></label><label>币种<select id="catalogCurrency"><option value="CNY" selected>CNY</option><option value="*">全部币种（分别排序）</option></select></label><label>库存<select id="catalogStock"><option value="">全部库存状态</option><option value="in_stock">有货</option><option value="out_of_stock">缺货</option><option value="unknown">未公开</option></select></label><label>发货方式<select id="catalogDelivery"><option value="">全部发货方式</option><option value="auto">自动发货</option><option value="manual">手动发货</option><option value="unknown">未公开</option></select></label></div><div class="filter-footer"><label class="source-option"><input type="checkbox" id="catalogExcludeZero">排除 0 元标价</label><div class="action-row"><label class="sr-only" for="catalogSort">排序字段</label><select id="catalogSort"><option value="price">按标价</option><option value="name">按商品名</option><option value="shop_name">按货铺名称</option><option value="synced_at">按同步时间</option></select><label class="sr-only" for="catalogOrder">排序方向</label><select id="catalogOrder"><option value="asc">升序 ↑</option><option value="desc">降序 ↓</option></select><button class="btn btn-ghost" type="reset">重置</button></div></div></form><div class="error-banner" id="catalogError" role="alert" hidden></div><div class="notice-banner" id="catalogCurrencyNotice" hidden>不同币种分别排序，价格保持原币种，未进行汇率换算。</div><div class="card result-card"><div class="result-toolbar"><p id="catalogStatus" role="status" aria-live="polite">准备查询…</p><label>每页 <select id="catalogSize"><option>20</option><option>50</option><option>100</option></select> 件</label></div><div class="table-wrap"><table class="catalog-table"><thead><tr><th>商品与规格</th><th>来源 / 货铺</th><th class="num">标价</th><th>库存</th><th>发货方式</th><th>同步时间</th><th>对比</th></tr></thead><tbody id="catalogBody"></tbody></table></div><div class="workspace-pager" id="catalogPager"></div></div>`;
  $('#catalogAddKeyword').onclick=()=>{addKeyword();loadCatalog(1);};
  $('#catalogKeywordInput').onkeydown=event=>{if(event.key==='Enter'&&!event.isComposing){event.preventDefault();addKeyword();loadCatalog(1);}};
  $('#catalogForm').onsubmit=event=>{event.preventDefault();addKeyword();loadCatalog(1);};
  $('#catalogForm').onreset=()=>{keywords=[];selectedShop=null;queueMicrotask(()=>{$('#catalogCurrency').value='CNY';$('#catalogSize').value='20';renderTags();loadCatalog(1);});};
  $('#catalogShop').oninput=()=>{selectedShop=null;};
  root.querySelectorAll('select,input[type="checkbox"]').forEach(el=>el.addEventListener('change',()=>loadCatalog(1)));
  document.addEventListener('compare-changed',drawRows);
  document.addEventListener('show-product',event=>openProduct(event.detail));
  initialized=true;
}
