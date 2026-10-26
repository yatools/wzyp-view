/* 共用的富文本边界：先清理再交给 DOM，不修改清理后的 HTML。 */
(() => {
  const escape = text => String(text ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  window.cleanPublicHTML = html => window.DOMPurify
    ? window.DOMPurify.sanitize(String(html ?? ''),{USE_PROFILES:{html:true},FORBID_TAGS:['form','style'],FORBID_ATTR:['style']})
    : escape(html);
  window.renderSafeMarkdown = text => {
    if(!window.marked) return escape(text);
    try { return window.cleanPublicHTML(window.marked.parse(String(text ?? ''),{breaks:true,gfm:true})); }
    catch { return escape(text); }
  };
})();
