import {request,esc,safeURL,sourceNames,directSourceNames,sourceBase,statusText,dateText,notify,navigate,phaseText} from './api.js';

let jobs=[],selected=null,candidatePage=1,candidateSequence=0,timer,polling=false,statsAt=0;
const nativeJobs=new Map(),busySources=new Set();
const active=job=>job&&['running','cancelling'].includes(job.status);
const resumable=job=>job&&['failed','partial','cancelled','interrupted'].includes(job.status);
const $=selector=>document.querySelector(selector);

function urlsFrom(value) {
  return value.split(/\r?\n/).flatMap(line=>{
    const trimmed=line.trim();if(!trimmed)return [];
    const found=trimmed.match(/https?:\/\/[^\s<>"']+/g);
    return found?found.map(x=>x.replace(/[。，；）)\]}]+$/,'')):[trimmed];
  });
}
function sourceProgress(job) {
  const activity={collecting:job.phase==='shop_discovery'?'读取目录':'正在采集',waiting:'平台排队中',blocked:'源站受阻',waiting_entry:'等待入口',finished:'本轮结束'};
  return `<div class="source-progress" aria-label="各平台采集进度">${Object.entries(job.source_progress||{}).map(([source,state])=>`<div><strong>${esc(sourceNames[source]||source)}</strong><span>${activity[state.activity]||(state.running?'正在采集':state.waiting?'平台排队中':active(job)?'等待入口':'本轮结束')} · ${state.success_shops} 家成功 · 排队 ${state.waiting} · 受阻 ${state.skipped} · 失败 ${state.failed||0}</span>${state.last_event?`<p class="muted reason-cell">最近收录：${esc(state.last_event.url)}</p>`:''}${state.reason?`<details><summary>最近失败${state.failure?.retryable===false?' · 默认跳过':' · 可续采重试'}</summary><p class="reason-cell">${esc(state.reason)}</p>${state.failure?`<p class="muted">状态码 ${esc(state.failure.status_code??'未返回')} · 请求编号 ${esc(state.failure.request_id||'未提供')}</p>`:''}</details>`:''}</div>`).join('')}</div>`;
}
function updateJob(job) {
  const keep=selected?.job_id===job?.job_id;
  const values=keep?Object.fromEntries(['extraPages','extraProbes','discoveryRetryMode'].map(id=>[id,document.getElementById(id)?.value])):{};
  const optionsOpen=keep&&$('#discoveryCurrent .resume-options')?.open;
  const focused=keep&&['extraPages','extraProbes','discoveryRetryMode'].includes(document.activeElement?.id)?document.activeElement.id:null;
  selected=job;
  if(!job){$('#discoveryCurrent').innerHTML='<div class="current-empty"><span>◎</span><h3>准备发现下一家货铺</h3><p>添加入口链接，或从现有公开索引和种子开始。</p></div>';return;}
  const counts=job.counts||{};
  const pending=['pending','probing','probing_shop','qualified','ready','waiting','collecting'].reduce((n,key)=>n+(counts[key]||0),0);
  $('#discoveryCurrent').innerHTML=`<div class="current-title"><span class="status-pill ${job.status}">${esc(statusText(job.status))}</span><span class="muted">${esc(dateText(job.updated_at))}</span></div><div class="discovery-numbers"><div><strong>${job.discovered_links}</strong><span>发现链接</span></div><div><strong>${job.success_shops}</strong><span>成功货铺</span></div><div><strong>${job.collected_goods}</strong><span>收录商品</span></div><div><strong>${pending}</strong><span>待处理链接</span></div></div><div class="job-summary">${phaseText(job.phase)?`<strong>${esc(phaseText(job.phase))}</strong><span>发现店铺 ${job.discovered_shops??0} · 待采 ${job.pending_shops??0} · 库存待核实 ${job.unverified_stock_shops??0}</span>`:""}<span>源站核验 ${job.probes_used??job.probes??0} / ${job.probe_limit??200}</span><span>目录页 ${job.pages} / ${job.page_limit}</span><span>失败 ${counts.failed||0} · 跳过 ${counts.blocked||0}</span><span>重复 ${counts.duplicate||0}</span><span>等待继续 ${(counts.deferred||0)+(counts.probe_deferred||0)}</span></div>${sourceProgress(job)}${job.error?`<p class="text-warn">${esc(taskErrorText(job.error))}</p>`:''}${job.index_errors?.length?`<p class="text-warn">${job.index_errors.map(x=>`${esc(sourceNames[x.source])} 索引：${esc(x.error)}`).join('<br>')}</p>`:''}<div class="action-row"><button class="btn btn-ghost" id="discoveryCandidatesToggle">查看链接与来源</button>${job.workflow_version===2?'<button class="btn btn-ghost" id="discoveryShops">查看店铺目录</button>':''}${active(job)?'<button class="btn btn-danger" id="discoveryCancel">取消本次发现</button>':''}${resumable(job)?'<button class="btn btn-primary" id="discoveryResume">继续未完成任务</button>':''}</div>${resumable(job)?`<details class="resume-options"><summary>追加预算或重试全部失败</summary><div class="action-row"><label>追加核验源站 <input id="extraProbes" type="number" min="0" max="2000" value="0"></label><label>追加目录页 <input id="extraPages" type="number" min="0" max="2000" value="0"></label><label>失败重试范围 <select id="discoveryRetryMode"><option value="transient">仅暂时故障</option><option value="all">全部失败（含打烊、封禁等）</option></select></label><button class="btn btn-ghost" id="discoveryExtend">按以上设置继续</button></div><p class="muted">已完成货铺保持不变。普通继续不会增加额度。</p></details>`:''}`;
  $('#discoveryCandidatesToggle').onclick=()=>{$('#candidatePanel').hidden=!$('#candidatePanel').hidden;if(!$('#candidatePanel').hidden)loadCandidates();};
  $('#discoveryShops')?.addEventListener('click',()=>showShopPreviews(job.job_id));
  $('#discoveryCancel')?.addEventListener('click',()=>changeDiscovery('cancel'));
  $('#discoveryResume')?.addEventListener('click',()=>changeDiscovery('resume'));
  $('#discoveryExtend')?.addEventListener('click',()=>changeDiscovery('resume',true));
  for(const [id,value] of Object.entries(values)){const input=document.getElementById(id);if(input&&value!==undefined)input.value=value;}
  if(optionsOpen&&$('#discoveryCurrent .resume-options'))$('#discoveryCurrent .resume-options').open=true;
  if(focused)document.getElementById(focused)?.focus({preventScroll:true});
  $('#discoveryJobId').textContent=job.job_id.slice(0,12);
}
async function changeDiscovery(action,extend=false) {
  if(!selected)return;
  const job=selected;
  const button=$('#discovery'+(action==='cancel'?'Cancel':extend?'Extend':'Resume'));if(button)button.disabled=true;
  try {
    const body=action==='resume'?{extra_pages:extend?Number($('#extraPages').value):0,extra_probes:extend?Number($('#extraProbes').value):0,retry_mode:extend?$('#discoveryRetryMode').value:'transient',max_depth:Math.max(job.max_depth,Number($('#discoveryDepth').value))}:{};
    updateJob(await request(`/api/discovery/jobs/${encodeURIComponent(job.job_id)}/${action}`,body));
    notify(action==='cancel'?'已请求取消，已成功采集的数据会保留。':extend?'已按追加额度与重试设置继续。':'已继续未完成任务；暂时故障会重试，完成和永久失败入口会跳过。');
    await refreshTasks();
  } catch(error){notify(error.message);if(button)button.disabled=false;}
}
async function retryCandidate(id,button) {
  if(!selected)return;
  button.disabled=true;
  try {
    const result=await request(`/api/discovery/jobs/${encodeURIComponent(selected.job_id)}/candidates/${encodeURIComponent(id)}/retry`,{});
    if(result.action==='rediscover') {
      $('#discoveryURLs').value=result.prefill.urls.join('\n');$('#discoveryIndexes').checked=false;
      document.querySelectorAll('[name="discoverySource"]').forEach(el=>{el.checked=selected.sources.includes(el.value);});
      $('#discoveryForm').scrollIntoView({behavior:'smooth',block:'center'});$('#discoveryURLs').focus();
      notify('已填入此入口，点击“发现并采集”即可单独重新发现。');
    } else {
      notify(`已启动${sourceNames[result.source]||result.source}单店重试，原失败记录保留。`);
      await loadCandidates();await refreshTasks();
    }
  } catch(error){notify(error.message);}
  finally{button.disabled=false;}
}
async function loadCandidates() {
  if(!selected)return;
  const id=++candidateSequence, job=selected.job_id;
  const state=$('#candidateState').value;
  try {
    const data=await request(`/api/discovery/jobs/${encodeURIComponent(job)}/candidates?page=${candidatePage}&pageSize=30${state?'&state='+encodeURIComponent(state):''}`);
    if(id!==candidateSequence||selected?.job_id!==job)return;
    $('#candidateRows').innerHTML=data.rows.map(row=>{
      const url=safeURL(row.url);
      return `<tr><td class="link-cell">${url?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(row.url)}</a>`:esc(row.url)}</td><td><span class="status-pill ${row.state}">${esc(statusText(row.state))}</span></td><td>${esc(sourceNames[row.source]||'目录 / 待识别')}</td><td>${row.depth}</td><td class="link-cell">${row.parents.map(parent=>`<div>${esc(parent==='manual'?'手动补充':parent)}</div>`).join('')}</td><td class="reason-cell">${esc(row.reason||'—')}${row.failure?`<p class="muted">${row.failure.retryable?'续采可重试':'默认跳过，可主动重试'}</p>`:''}${row.retries?.length?`<p class="muted">已关联 ${row.retries.length} 次单店重试</p>`:''}</td><td>${['failed','blocked','unsupported','rejected','deferred','probe_deferred'].includes(row.state)?`<button class="btn btn-ghost" data-retry-candidate="${row.id}">${row.source?'仅重试此入口':'重新发现此入口'}</button>`:'—'}</td></tr>`;
    }).join('')||'<tr><td colspan="7" class="muted">暂无符合条件的链接</td></tr>';
    $('#candidateRows').querySelectorAll('[data-retry-candidate]').forEach(button=>button.onclick=()=>retryCandidate(button.dataset.retryCandidate,button));
    const pages=Math.max(1,Math.ceil(data.total/30));
    $('#candidatePager').innerHTML=`<span>共 ${data.total} 条 · ${candidatePage}/${pages} 页</span><button class="btn btn-ghost" id="candidatePrevious" ${candidatePage<=1?'disabled':''}>上一页</button><button class="btn btn-ghost" id="candidateNext" ${candidatePage>=pages?'disabled':''}>下一页</button>`;
    $('#candidatePrevious').onclick=()=>{candidatePage--;loadCandidates();};$('#candidateNext').onclick=()=>{candidatePage++;loadCandidates();};
  } catch(error){if(id===candidateSequence)$('#candidateRows').innerHTML=`<tr><td colspan="7" class="text-warn">${esc(error.message)}</td></tr>`;}
}
function taskErrorText(error){return /database (?:table )?is locked/i.test(error||'')?'本地数据库写入冲突；已保存的店铺目录和断点可继续使用。':error||'';}
function renderHistory() {
  $('#discoveryHistory').innerHTML=jobs.map(job=>`<tr><td><button class="text-button" data-select-job="${esc(job.job_id)}">${esc(job.job_id.slice(0,12))}</button></td><td><span class="status-pill ${job.status}">${esc(statusText(job.status))}</span>${job.error?`<p class="text-warn">${esc(taskErrorText(job.error))}</p>`:""}${job.workflow_version===2?`<p class="muted">${esc(phaseText(job.phase))}</p>`:""}</td><td>${job.workflow_version===2?`<p>目录 ${job.discovered_shops??0} 家 · 待采 ${job.pending_shops??0} 家</p><p>商品采集 ${job.success_shops} 家 / ${job.collected_goods} 件</p>`:`${job.success_shops} 家 / ${job.collected_goods} 件`}</td><td>${esc(dateText(job.created_at))}</td><td>${job.sources.map(x=>esc(sourceNames[x])).join('、')}</td></tr>`).join('')||'<tr><td colspan="5"><div class="history-empty">还没有发现任务，成功快照仍可在商品页查询。</div></td></tr>';
  $('#discoveryHistory').querySelectorAll('[data-select-job]').forEach(button=>button.onclick=()=>{candidatePage=1;updateJob(jobs.find(x=>x.job_id===button.dataset.selectJob));if(!$('#candidatePanel').hidden)loadCandidates();$('#discoveryCurrent').scrollIntoView({behavior:'smooth',block:'nearest'});});
}
function renderSource(source,job,error) {
  if(source==='yike'){renderYike(job,error);return;}
  const status=document.getElementById('sourceStatus-'+source);
  status.innerHTML=error?`<p class="text-warn">${esc(error)}</p>`:job?`<span class="status-pill ${job.status}">${esc(statusText(job.status))}</span><p>${job.success_shops} 家成功 · ${job.collected_goods} 件商品 · ${job.failure_count} 条失败</p>${job.workflow_version===2?`<p>${esc(phaseText(job.phase))}</p><p>已发现 ${job.discovered_shops??0} 家 · 待采 ${job.pending_shops??0} 家 · 库存待核实 ${job.unverified_stock_shops??0} 家</p><button class="text-button" data-source="${source}" data-action="previews">查看店铺目录</button>`:""}${job.discovery_error?`<p class="source-error">索引暂不可用：${esc(job.discovery_error)}</p>`:active(job)&&job.mode==='discover'&&!job.discovery_done?'<p class="muted">索引后台读取中，正在搜索已有店铺目录</p>':''}${job.last_error&&job.last_error!==job.discovery_error?`<p class="source-error">最近失败：${esc(job.last_error)}</p>`:''}<div class="action-row">${active(job)?`<button class="btn btn-ghost" data-source="${source}" data-action="cancel">取消任务</button>`:''}${resumable(job)?`<button class="btn btn-ghost" data-source="${source}" data-action="resume">继续未完成任务</button><button class="btn btn-ghost" data-source="${source}" data-action="resume-all">重试全部失败</button>`:''}</div>`:'<p class="muted">尚无平台采集任务</p>';
  document.querySelectorAll(`[data-source="${source}"][data-action]`).forEach(button=>{
    if(['discover','refresh','sync'].includes(button.dataset.action))button.disabled=active(job)||busySources.has(source);
  });
}
function renderYike(job,error) {
  const status=$('#sourceStatus-yike');
  status.innerHTML=error?`<p class="text-warn">${esc(error)}</p>`:job?`<span class="status-pill ${esc(job.status)}">${esc(statusText(job.status))}</span><p>关键词：${esc(job.keyword)}</p><p>列表 ${job.list_pages} 页 · 同源组 ${job.matched_groups} 个 / ${job.group_pages} 页</p><p>成功 ${job.collected_goods} 件 · 失败 ${job.failure_count} 条 · 待处理 ${job.pending_links} 件</p>${job.discovery_error?`<p class="source-error">索引暂不可用：${esc(job.discovery_error)}</p>`:active(job)&&job.mode==='discover'&&!job.discovery_done?'<p class="muted">索引后台读取中，正在搜索已有店铺目录</p>':''}${job.last_error&&job.last_error!==job.discovery_error?`<p class="source-error">最近失败：${esc(job.last_error)}</p>`:''}<div class="action-row">${active(job)?'<button class="btn btn-ghost" data-source="yike" data-action="cancel">取消任务</button>':''}${resumable(job)?'<button class="btn btn-ghost" data-source="yike" data-action="resume">继续未完成任务</button><button class="btn btn-ghost" data-source="yike" data-action="resume-all">重试全部失败</button>':''}</div>`:'<p class="muted">输入关键词，采集匹配商品及同源组内各店报价。</p>';
  $('#yikeStart').disabled=active(job)||busySources.has('yike');
}
function yikeCard() {
  return `<section class="card source-card"><div class="source-card-heading"><span class="source-icon">一</span><div><h3>一可比价网</h3><p class="muted" id="sourceStats-yike">读取快照…</p></div></div><p class="muted">一可聚合快照 · 按关键词采集的商品子集，混合搜索优先展示原站记录。</p><form id="yikeForm"><label for="sourceInput-yike">采集关键词</label><input id="sourceInput-yike" maxlength="200" required placeholder="例如 GPT Plus" autocomplete="off"><div class="action-row"><button class="btn btn-primary" id="yikeStart" type="submit">按关键词采集</button><button class="text-button" type="button" data-source="yike" data-action="history">历史</button></div></form><div class="source-job" id="sourceStatus-yike"></div></section>`;
}
export async function refreshTasks() {
  if(polling)return;
  polling=true;
  try {
    const results=await Promise.allSettled([
      request('/api/discovery/jobs'),...Object.keys(sourceNames).map(source=>request(sourceBase(source)+'/crawl')),
    ]);
    if(results[0].status==='fulfilled') {
      jobs=results[0].value.jobs;$('#tasksError').hidden=true;
      const existing=selected&&jobs.find(x=>x.job_id===selected.job_id);
      updateJob(existing||selected||jobs[0]||null);renderHistory();
      $('#discoveryStart').disabled=jobs.some(active);
      if(selected&&!$('#candidatePanel').hidden)await loadCandidates();
    } else {$('#tasksError').hidden=false;$('#tasksError').textContent=results[0].reason.message;}
    Object.keys(sourceNames).forEach((source,i)=>{
      const result=results[i+1];
      if(result.status==='fulfilled'){
        const previous=nativeJobs.get(source),job=result.value.job;
        if(job&&`${previous?.job_id}:${previous?.success_shops}:${previous?.collected_goods}`!==`${job.job_id}:${job.success_shops}:${job.collected_goods}`)
          document.dispatchEvent(new CustomEvent('snapshots-changed',{detail:{source}}));
        nativeJobs.set(source,job);renderSource(source,job);
      }
      else renderSource(source,nativeJobs.get(source),result.reason.message);
    });
    if(Date.now()-statsAt>15000) {
      statsAt=Date.now();
      try {
        const stats=await request('/api/sources/summary');
        for(const item of stats.sources){const el=document.getElementById('sourceStats-'+item.id);if(el)el.textContent=item.error?'快照统计暂不可用':`${item.shop_count} 家货铺 · ${item.goods_count} 件商品`;}
      } catch {statsAt=0;}
    }
  } finally {polling=false;schedule();}
}
async function sourceAction(source,action) {
  const value=document.getElementById('sourceInput-'+source).value.trim();
  const job=nativeJobs.get(source);
  busySources.add(source);renderSource(source,job);
  try {
    if(action==='previews'){if(job)await showShopPreviews(job.job_id,source);return;}
    if(action==='history'){await showSourceHistory(source);return;}
    if(['cancel','resume','resume-all'].includes(action)) {
      if(job) await request(`${sourceBase(source)}/crawl/${encodeURIComponent(job.job_id)}/${action==='resume-all'?'resume':action}`,action==='cancel'?{}:{retry_mode:action==='resume-all'?'all':'transient'});
    } else if(action==='sync') {
      if(!value)throw new Error('请先输入一个公开货铺或商品链接。');
      const result=await request(sourceBase(source)+'/sync',{url:value},{timeoutMs:21610000});
      notify(`已采集 ${result.shop.nickname}，共 ${result.goods_count} 件商品。`);
    } else if(source==='yike') {
      if(!value)throw new Error('请输入采集关键词。');
      await request(sourceBase(source)+'/crawl',{keyword:value});
    } else {
      if(['dujiao-next','dujiaoka'].includes(source)&&action==='discover'&&!value)throw new Error('请先添加货铺链接，或使用上方的跨平台发现。');
      await request(sourceBase(source)+'/crawl',{mode:action,urls:value?urlsFrom(value):[]});
    }
    statsAt=0;await refreshTasks();
  } catch(error){notify(error.message);}
  finally{busySources.delete(source);renderSource(source,nativeJobs.get(source));}
}
async function showSourceHistory(source) {
  let dialog=$('#sourceHistoryDialog');
  if(!dialog){dialog=document.createElement('dialog');dialog.id='sourceHistoryDialog';dialog.className='product-dialog history-dialog';document.body.append(dialog);}
  dialog.innerHTML=`<div class="dialog-header"><h2>${esc(sourceNames[source])} 采集历史</h2><button class="icon-button" id="closeSourceHistory" aria-label="关闭采集历史">×</button></div><div class="dialog-content">正在读取…</div>`;
  $('#closeSourceHistory').onclick=()=>dialog.close();if(!dialog.open)dialog.showModal();
  const data=await request(`/api/sources/${encodeURIComponent(source)}/jobs?limit=30`);
  dialog.querySelector('.dialog-content').innerHTML=data.jobs.map(job=>`<details class="history-item"><summary><span class="status-pill ${job.status}">${esc(statusText(job.status))}</span> ${esc(dateText(job.created_at))} · ${job.keyword?"关键词："+esc(job.keyword)+" · ":""}${job.success_shops} 家 / ${job.collected_goods} 件</summary><p class="muted">任务 ${esc(job.job_id)}</p><p>发现 ${job.discovered_links} 条链接 · 失败 ${job.failure_count} 条 · 待处理 ${job.pending_links} 条</p>${job.last_error?`<p class="text-warn">${esc(job.last_error)}</p>`:''}${job.failures.map(x=>`<p class="reason-cell">${esc(x.url)}：${esc(x.error)}</p>`).join('')}</details>`).join('')||'<p class="muted">暂无平台采集历史。</p>';
}
let previewSequence=0,previewController;
async function showShopPreviews(jobId,source=null,page=1) {
  previewController?.abort();previewController=new AbortController();const current=++previewSequence;
  let dialog=$('#shopPreviewDialog');
  if(!dialog){dialog=document.createElement('dialog');dialog.id='shopPreviewDialog';dialog.className='product-dialog history-dialog';document.body.append(dialog);dialog.addEventListener('close',()=>{if(dialog.open)return;previewSequence++;previewController?.abort();});}
  dialog.innerHTML='<div class="dialog-header"><h2>已发现的店铺目录</h2><button class="icon-button" aria-label="关闭店铺目录">×</button></div><div class="dialog-content" role="status">正在读取目录预览…</div>';
  dialog.querySelector('button').onclick=()=>dialog.close();if(!dialog.open)dialog.showModal();
  try {
    const data=await request(`/api/discovery/shops?job_id=${encodeURIComponent(jobId)}&page=${page}&pageSize=20${source?'&source='+encodeURIComponent(source):''}`,undefined,{signal:previewController.signal});
    if(current!==previewSequence)return;
    dialog.querySelector('.dialog-content').innerHTML='<p class="muted">目录预览独立保存，未计入正式商品快照。库存未知的店铺会在第二阶段核实。</p>'+data.rows.map(item=>{
      const p=item.preview,url=safeURL(item.url);
      const label={eligible:'进入商品采集',empty:'公开目录为空',no_stock:'目录商品均缺货'}[item.eligibility]||item.eligibility;
      return `<details class="history-item"><summary>${esc(item.name)} · ${esc(sourceNames[item.source])} · ${esc(label)}</summary><p>${url?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">打开店铺 ↗</a>`:''} · 商品数 ${p.reported_total==null?'未公开':esc(p.reported_total)} · ${p.complete?'完整目录':'部分目录预览'} · ${esc(dateText(p.checked_at))}</p><p>分类：${p.categories.map(c=>`${esc(c.name)}（${c.count==null?'数量未公开':esc(c.count)}）`).join('、')||'未公开'}</p><ul>${p.products.map(g=>`<li>${esc(g.name)} · ${esc({in_stock:'有货',out_of_stock:'缺货',unknown:'库存待核实'}[g.stock_status]||'库存待核实')}</li>`).join('')}</ul></details>`;
    }).join('')+(data.rows.length?'':'<p class="muted">暂无已保存的店铺目录。</p>')+`<div class="workspace-pager"><span>共 ${data.total} 家 · 第 ${page} 页</span><button class="btn btn-ghost" data-prev ${page===1?'disabled':''}>上一页</button><button class="btn btn-ghost" data-next ${page*20>=data.total?'disabled':''}>下一页</button></div>`;
    dialog.querySelector('[data-prev]').onclick=()=>showShopPreviews(jobId,source,page-1);
    dialog.querySelector('[data-next]').onclick=()=>showShopPreviews(jobId,source,page+1);
  } catch(error){if(current===previewSequence&&error.name!=='AbortError')dialog.querySelector('.dialog-content').textContent=error.message;}
}
export function initializeTasks() {
  const root=$('#page-discovery');
  root.innerHTML=`<div class="workspace-hero heading-row"><div><p class="eyebrow">公开货铺 · 本地采集</p><h1>发现更多货铺，收录真实商品</h1><p>先读取首页和分类目录保存店铺，本轮搜索结束后自动采集整店公开商品。</p></div><button class="btn btn-ghost" id="tasksGoSearch">查看已收录商品 →</button></div><div class="error-banner" id="tasksError" role="alert" hidden></div><div class="discovery-grid"><form class="card seed-card" id="discoveryForm"><div class="section-title"><h2>添加发现入口</h2><span class="subtle-tag">搜索店铺目录 → 自动采集商品</span></div><label class="sr-only" for="discoveryURLs">店铺、商品或公开目录链接</label><textarea id="discoveryURLs" rows="5" placeholder="粘贴店铺、商品或公开目录页链接，每行一个。\n可以混合多类来源；也可留空，从现有公开索引开始。"></textarea><fieldset class="source-options"><legend class="sr-only">允许收录的平台</legend>${Object.entries(directSourceNames).map(([source,name])=>`<label class="source-option"><input type="checkbox" name="discoverySource" value="${source}" checked>${esc(name)}</label>`).join('')}</fieldset><label class="source-option indexes-option"><input type="checkbox" id="discoveryIndexes" checked>同时使用现有公开索引、种子与已收录入口</label><div class="seed-footer"><details class="discovery-limits"><summary>扩展范围</summary><div class="action-row"><label>同时处理平台 <input id="discoveryParallel" type="number" min="1" max="8" value="4"></label><label>跨站层数 <input id="discoveryDepth" type="number" min="0" max="8" value="2"></label><label>未知源站核验 <input id="discoveryProbeLimit" type="number" min="0" max="2000" value="200"></label><label>目录页预算 <input id="discoveryPageLimit" type="number" min="1" max="2000" value="200"></label></div><p class="muted">每个未知源站只占一次核验额度；目录页单独计数，第一阶段只保存目录预览，第二阶段再完整读取商品。</p></details><button class="btn btn-primary start-discovery" id="discoveryStart" type="submit">搜索店铺并自动采集</button></div></form><div class="card current-job"><div class="section-title"><h2>本次发现</h2><span class="job-id" id="discoveryJobId"></span></div><div id="discoveryCurrent"></div></div></div><section class="card" id="candidatePanel" hidden><div class="section-title"><h2>链接与来源追溯</h2><label class="sr-only" for="candidateState">链接状态</label><select id="candidateState"><option value="">全部状态</option>${['pending','probing','probing_shop','qualified','empty','no_stock','ready','waiting','collecting','collected','directory','duplicate','deferred','probe_deferred','blocked','failed','unsupported','rejected'].map(state=>`<option value="${state}">${esc(statusText(state))}</option>`).join('')}</select></div><div class="table-wrap"><table class="candidate-table"><thead><tr><th>候选链接</th><th>处理状态</th><th>平台</th><th>深度</th><th>发现自</th><th>原因</th><th>单条处理</th></tr></thead><tbody id="candidateRows"></tbody></table></div><div class="workspace-pager" id="candidatePager"></div></section><div class="section-title source-section-title"><div><h2>平台采集</h2><p class="muted">保留各平台独立采集、刷新、取消和断点续跑。</p></div></div><div class="source-card-grid">${Object.entries(directSourceNames).map(([source,name])=>`<section class="card source-card"><div class="source-card-heading"><span class="source-icon">${source==='wzyp-live'?'链':source==='16688'?'16':source==='dujiao-next'?'N':source==='catfk'?'猫':'独'}</span><div><h3>${esc(name)}</h3><p class="muted" id="sourceStats-${source}">读取快照…</p></div></div><div class="source-job" id="sourceStatus-${source}"></div><div class="action-row"><button class="btn btn-ghost" data-source="${source}" data-action="discover">搜索店铺</button><button class="btn btn-ghost" data-source="${source}" data-action="refresh">刷新货铺</button><button class="text-button" data-source="${source}" data-action="history">历史</button></div><details class="source-extra"><summary>补充链接 / 单店同步</summary><label class="sr-only" for="sourceInput-${source}">${esc(name)} 公开链接</label><textarea id="sourceInput-${source}" rows="2" placeholder="公开店铺或商品 HTTPS 链接"></textarea><button class="btn btn-ghost" data-source="${source}" data-action="sync">同步单个货铺</button></details></section>`).join('')}</div><section class="card"><div class="section-title"><h2>发现历史</h2><button class="text-button" id="refreshDiscoveryHistory">刷新</button></div><div class="table-wrap"><table class="history-table"><thead><tr><th>任务</th><th>状态</th><th>采集结果</th><th>创建时间</th><th>收录范围</th></tr></thead><tbody id="discoveryHistory"></tbody></table></div></section>`;
  root.querySelector('.source-card-grid').insertAdjacentHTML('beforeend',yikeCard());
  $('#yikeForm').onsubmit=event=>{event.preventDefault();sourceAction('yike','keyword');};
  $('#tasksGoSearch').onclick=()=>navigate('catalog');
  $('#candidateState').onchange=()=>{candidatePage=1;loadCandidates();};
  $('#refreshDiscoveryHistory').onclick=()=>refreshTasks();
  root.addEventListener('click',event=>{const button=event.target.closest('[data-source][data-action]');if(button&&!button.disabled)sourceAction(button.dataset.source,button.dataset.action);});
  $('#discoveryForm').onsubmit=async event=>{
    event.preventDefault();
    const sources=[...root.querySelectorAll('[name="discoverySource"]:checked')].map(x=>x.value);
    if(!sources.length){notify('请至少选择一个平台。');return;}
    const urls=urlsFrom($('#discoveryURLs').value),include_indexes=$('#discoveryIndexes').checked;
    if(!urls.length&&!include_indexes){notify('请添加入口链接，或启用公开索引与种子。');return;}
    $('#discoveryStart').disabled=true;
    try {
      const job=await request('/api/discovery/jobs',{urls,sources,include_indexes,max_depth:Number($('#discoveryDepth').value),page_limit:Number($('#discoveryPageLimit').value),parallel_sources:Number($('#discoveryParallel').value),probe_limit:Number($('#discoveryProbeLimit').value)});
      updateJob(job);candidatePage=1;notify('已开始搜索店铺目录，本轮搜索结束后自动采集商品。');await refreshTasks();
    } catch(error){notify(error.message);$('#discoveryStart').disabled=false;}
  };
  updateJob(null);
  document.addEventListener('show-shop-previews',event=>showShopPreviews(event.detail.job_id,event.detail.source));
  document.addEventListener('visibilitychange',()=>{if(document.hidden)clearTimeout(timer);else if($('#page-discovery').classList.contains('active'))refreshTasks();});
  document.querySelectorAll('nav button').forEach(button=>button.addEventListener('click',()=>queueMicrotask(schedule)));
  schedule();
}
function schedule() {
  clearTimeout(timer);
  if(document.hidden||!$('#page-discovery')?.classList.contains('active'))return;
  const delay=jobs.some(active)||[...nativeJobs.values()].some(active)?3000:15000;
  timer=setTimeout(async()=>{
    if(!document.hidden&&$('#page-discovery').classList.contains('active'))await refreshTasks();
    schedule();
  },delay);
}
