import {initializeTasks,refreshTasks} from './modules/tasks.js';
import {initializeCatalog,loadCatalog} from './modules/catalog.js';
import {initializeCompare,renderCompare} from './modules/compare.js';
import {loadSources,esc} from './modules/api.js';

async function initialize() {
  try { await loadSources(); } catch(error) {
    document.getElementById('page-discovery').innerHTML=`<p class="text-warn">${esc(error.message)}。请刷新页面重试。</p>`;
    return;
  }
  initializeTasks();initializeCatalog();initializeCompare();
  const workspacePages=new Set(['discovery','catalog','compare']);
  document.querySelectorAll('nav button').forEach(button=>button.addEventListener('click',()=>{
    const page=button.dataset.page;
    const yike=document.getElementById('dataSource').value==='yike';
    document.getElementById('sourcePanel').hidden=workspacePages.has(page)||yike;
    document.getElementById('sourceReadNotice').hidden=workspacePages.has(page)||!yike;
    if(page==='discovery')refreshTasks();
    if(page==='catalog')loadCatalog(1);
    if(page==='compare')renderCompare();
  }));
  document.getElementById('sourcePanel').hidden=true;
  document.getElementById('dataSource').addEventListener('change',()=>{
    if([...workspacePages].some(page=>document.getElementById('page-'+page).classList.contains('active')))
      document.getElementById('sourcePanel').hidden=true;
  });
  refreshTasks();
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',initialize,{once:true});else initialize();
