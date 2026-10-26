export async function request(path, body, options = {}) {
  const {timeoutMs=15000,signal,...fetchOptions}=options;
  const controller=new AbortController();
  const abort=()=>controller.abort();
  if(signal?.aborted)controller.abort();else signal?.addEventListener('abort',abort,{once:true});
  let timedOut=false;
  const timer=setTimeout(()=>{timedOut=true;controller.abort();},timeoutMs);
  try {
  const response = await fetch(path, {
    ...(body === undefined ? {} : {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}),
    ...fetchOptions,signal:controller.signal,
  });
  let data;
  try { data = await response.json(); } catch(error) { if(error.name==='AbortError')throw error;throw new Error('服务返回了无法读取的内容，请确认服务已更新并重启。'); }
  if (!response.ok) {
    const detail = typeof data.detail === 'string' ? data.detail : Array.isArray(data.detail) ? data.detail.map(x=>x.msg).join('；') : '';
    const error=new Error(detail || `请求失败（${response.status}）`);error.status=response.status;throw error;
  }
  return data;
  } catch(error) {
    if(timedOut){const failure=new Error('请求超时，请稍后重试。');failure.name='TimeoutError';throw failure;}
    throw error;
  } finally {clearTimeout(timer);signal?.removeEventListener('abort',abort);}
}

export const sourceNames = {};
export const directSourceNames = {};
export async function loadSources() {
  const data = await (window.sourceRegistryReady || request('/api/sources'));
  for(const source of data.sources) {
    sourceNames[source.id] = source.name;
    if(source.discovery)directSourceNames[source.id] = source.name;
  }
}
export const sourceBase = source => '/api/sources/' + (source === 'wzyp-live' ? 'wzyp' : source);
export const esc = value => String(value ?? '').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
export function safeURL(value) {
  try { const url = new URL(value); return ['https:','http:'].includes(url.protocol) && !url.username && !url.password ? url.href : ''; } catch { return ''; }
}
export const dateText = value => value ? new Date(value).toLocaleString('zh-CN',{hour12:false}) : '尚未同步';
export function provenanceFields(item) {
  if(item.source!=='yike')return '';
  const url=safeURL(item.source_url);
  return `<dt>数据来源</dt><dd>一可聚合快照 · 按关键词采集的商品子集</dd><dt>来源更新时间</dt><dd>${esc(item.source_updated_at||'未公开')}（来源原文）</dd><dt>一可详情</dt><dd>${url?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">查看来源记录 ↗</a>`:'未公开'}</dd>${item.status===0?'<dt>商品状态</dt><dd class="text-warn">来源已标记下架</dd>':''}`;
}
export function moneyText(value,currency='CNY') {
  if (value == null || !Number.isFinite(Number(value))) return '未公开';
  const number = Number(value).toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:4});
  return currency ? `${currency === 'CNY' ? '¥' : currency + ' '}${number}` : `${number}（币种未公开）`;
}
export function notify(message) {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent=message; el.classList.add('show');
  clearTimeout(notify.timer); notify.timer=setTimeout(()=>el.classList.remove('show'),3200);
}
export function navigate(page) {
  document.querySelector(`nav button[data-page="${page}"]`)?.click();
}
export const statusText = status => ({probing_shop:'读取店铺目录',qualified:'已保存目录，等待商品采集',empty:'目录为空',no_stock:'目录商品均缺货',running:'采集中',cancelling:'正在取消',cancelled:'已取消',interrupted:'已中断',completed:'已完成',partial:'部分完成',failed:'失败',ready:'等待采集',blocked:'本轮已跳过',pending:'待核验',probing:'核验中',waiting:'等待平台任务',collecting:'整店采集中',collected:'已收录',duplicate:'重复店铺',directory:'已读取目录',unsupported:'未支持',rejected:'无效链接',deferred:'等待目录额度 / 深度',probe_deferred:'等待核验额度'}[status] || status);

export const phaseText=phase=>({shop_discovery:'第一阶段 · 搜索店铺目录',goods_collection:'第二阶段 · 采集整店商品'}[phase]||'');
