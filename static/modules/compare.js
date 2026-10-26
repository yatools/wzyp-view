import {request,esc,safeURL,sourceNames,moneyText,dateText,notify,navigate} from './api.js';

const KEY='wzyp_compare_v1';
const LIMIT=8;
const ref = item => `${item.source}\n${item.goods_key}`;
function readStored() {
  try {
    const value=JSON.parse(localStorage.getItem(KEY)||'[]');
    if (!Array.isArray(value)) return [];
    const seen=new Set();
    return value.filter(x=>{
      if(!x || !sourceNames[x.source] || typeof x.goods_key!=='string' || !x.goods_key || seen.has(ref(x))) return false;
      seen.add(ref(x)); return true;
    }).slice(0,LIMIT);
  } catch { return []; }
}
let items=[];
let revision=0;
let refreshSequence=0,refreshController;
function persist() {
  revision++;
  try { localStorage.setItem(KEY,JSON.stringify(items)); } catch { notify('浏览器未允许保存，对比清单本次会话仍可使用。'); }
  updateCount();
  document.dispatchEvent(new CustomEvent('compare-changed'));
}
export function contains(item) { return items.some(x=>ref(x)===ref(item)); }
export function toggleCompare(item) {
  const index=items.findIndex(x=>ref(x)===ref(item));
  if(index>=0) { items.splice(index,1); persist(); return; }
  if(items.length>=LIMIT) { notify('最多对比八件商品，请先移除一件。'); return; }
  const {source,goods_key,shop_name,shop_link,name,sku_label,price,currency,stock_quantity,stock_status,delivery_method,synced_at,link,catalog_entry,source_url,source_updated_at,status}=item;
  items.push({source,goods_key,shop_name,shop_link,name,sku_label,price,currency,stock_quantity,stock_status,delivery_method,synced_at,link,catalog_entry,source_url,source_updated_at,status});
  persist(); notify(`已加入对比（${items.length}/${LIMIT}）`);
}
function updateCount() {
  const el=document.getElementById('compareNavCount'); if(el) el.textContent=items.length;
}
function external(value,label) {
  const url=safeURL(value); return url?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(label)} ↗</a>`:'未公开';
}
const stockText = item => item.status===0?'来源已标记下架':item.stock_quantity!=null ? String(item.stock_quantity) : ({in_stock:'有货，数量未公开',out_of_stock:'缺货',unknown:'未公开'}[item.stock_status]||'未公开');
const deliveryText = item => ({auto:'自动发货',manual:'手动发货',unknown:'未公开'}[item.delivery_method]||'未公开');

export async function renderCompare(refresh=true) {
  refreshController?.abort();refreshController=new AbortController();const currentRefresh=++refreshSequence;
  const root=document.getElementById('page-compare');
  updateCount();
  if(!items.length) {
    root.innerHTML='<div class="workspace-heading"><p class="eyebrow">自己选择，逐项比较</p><h1>对比清单</h1><p>从商品搜索中加入最多八件商品，完整查看标题、规格和来源。</p></div><div class="empty-panel"><span class="empty-symbol">⇄</span><h2>还没有加入商品</h2><p>先搜索你需要的商品，再加入对比。</p><button class="btn btn-primary" id="compareGoSearch">搜索商品</button></div>';
    root.querySelector('#compareGoSearch').onclick=()=>navigate('catalog'); return;
  }
  root.innerHTML=`<div class="workspace-heading heading-row"><div><p class="eyebrow">自己选择，逐项比较</p><h1>对比清单 <small>${items.length}/${LIMIT}</small></h1><p>相似名称不代表相同规格，请结合商品说明判断。不同币种分别查看。</p></div><div class="action-row"><button class="btn btn-ghost" id="compareRefresh">更新快照</button><button class="btn btn-ghost" id="compareClear">清空清单</button></div></div><p class="inline-status" id="compareStatus" role="status">${refresh?'正在核对当前快照…':'已显示当前清单'}</p><div class="compare-scroll"><table class="comparison-table"><thead><tr><th>比较项目</th>${items.map((x,i)=>`<th><div class="compare-product">${esc(x.name)}<button class="icon-button" data-remove="${i}" aria-label="移除 ${esc(x.name)}">×</button></div></th>`).join('')}</tr></thead><tbody>${[
    ['当前标价',x=>x.unavailable?'<span class="text-warn">当前快照不可用</span>':x.status===0?'<span class="text-warn">来源已标记下架</span>':`<strong class="price-value">${esc(moneyText(x.price,x.currency))}</strong>`],
    ['原始规格',x=>esc(x.sku_label||'原站未单独提供，请查看标题与说明')],
    ['来源平台',x=>esc(x.source==='yike'?'一可聚合快照 · 关键词子集':sourceNames[x.source])], ['货铺',x=>external(x.shop_link,x.shop_name||'查看货铺')],
    ['来源更新时间',x=>x.source==='yike'?esc(x.source_updated_at||'未公开')+'（来源原文）':'原站直接采集'],
    ['聚合来源详情',x=>x.source==='yike'?external(x.source_url,'查看一可记录'):'原站直接采集'],
    ['库存',x=>esc(stockText(x))], ['发货方式',x=>esc(deliveryText(x))],
    ['商品说明',x=>`<button class="text-button" data-show-source="${esc(x.source)}" data-show-key="${esc(x.goods_key)}">查看原始说明</button>`],
    ['同步时间',x=>esc(dateText(x.synced_at))], ['原站链接',x=>external(x.link,x.catalog_entry?'打开原站目录':'打开商品')],
  ].map(([label,format])=>`<tr><th>${label}</th>${items.map(x=>`<td>${format(x)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  root.querySelectorAll('[data-remove]').forEach(button=>button.onclick=()=>{items.splice(Number(button.dataset.remove),1);persist();renderCompare(false);});
  root.querySelector('#compareClear').onclick=()=>{items=[];persist();renderCompare(false);};
  root.querySelector('#compareRefresh').onclick=()=>renderCompare(true);
  root.querySelectorAll('[data-show-key]').forEach(button=>button.onclick=()=>document.dispatchEvent(new CustomEvent('show-product',{detail:{source:button.dataset.showSource,goods_key:button.dataset.showKey}})));
  if(!refresh) return;
  const currentRevision=revision;
  const current=items.slice();
  const responses=await Promise.allSettled(current.map(x=>request(`/api/catalog/goods/${encodeURIComponent(x.source)}/${encodeURIComponent(x.goods_key)}`,undefined,{signal:refreshController.signal})));
  if(currentRevision!==revision||currentRefresh!==refreshSequence) return;
  let errors=0;
  items=current.map((item,i)=>{
    if(responses[i].status==='fulfilled') { const {description,image,...safe}=responses[i].value; return {...item,...safe,unavailable:false}; }
    errors++; return {...item,unavailable:true};
  });
  persist();
  await renderCompare(false);
  document.getElementById('compareStatus').textContent=errors?`${errors} 件商品当前无法读取，请核对原站；其余已更新。`:'已核对当前本地快照';
}

export function initializeCompare() { items=readStored(); updateCount(); }
