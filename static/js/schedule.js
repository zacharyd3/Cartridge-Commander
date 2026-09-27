'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// SCHEDULES PAGE
// ══════════════════════════════════════════════════════════════════════════════
const DOW_LONG = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
const SCHED_DRAFT_DEFAULT = {label:'', mode:'weekly', dow:'0', dom:'1', hour:'2', min:'0', result:''};
const pad2 = n => String(n).padStart(2,'0');

function schedWhen(s){
  const t = `${pad2(s.hour)}:${pad2(s.minute)}`;
  if(s.mode==='daily') return `Daily at ${t}`;
  if(s.mode==='weekly') return `Every ${DOW_LONG[s.day_of_week]||'?'} at ${t}`;
  return `Monthly on day ${s.day_of_month} at ${t}`;
}

// Excludes: paths inside a selected source that the backup leaves out, so a
// whole folder (e.g. appdata) can be picked with a few subfolders skipped.
const underPath = (p, root) => p.startsWith(root.replace(/\/+$/,'') + '/');
function schedExclusion(p){
  if(G.schedExcludes.includes(p)) return 'excluded';
  if(G.schedExcludes.some(x => underPath(p, x))) return 'inherited';
  if(G.schedPaths.some(s => underPath(p, s))) return 'excludable';
  return null;
}
// Drop excludes whose source was removed — they would exclude nothing.
function pruneSchedExcludes(){
  G.schedExcludes = G.schedExcludes.filter(x => G.schedPaths.some(s => underPath(x, s)));
}
function excludeSchedPath(p){
  G.schedExcludes = G.schedExcludes.filter(x => !underPath(x, p));  // now covered by p
  if(!G.schedExcludes.includes(p)) G.schedExcludes.push(p);
  renderPage();
}
function unexcludeSchedPath(p){ G.schedExcludes = G.schedExcludes.filter(x => x !== p); renderPage(); }

function renderSchedulePage(c){
  pruneSchedExcludes();
  const editing = !!G.schedEditId;
  const editingSched = editing ? (G.schedules||[]).find(x=>x.id===G.schedEditId) : null;
  c.insertAdjacentHTML('beforeend', `
    <div class="page-head">
      <div><h1>Schedules</h1><p class="sub">Recurring backup jobs</p></div>
      <div class="actions"><button class="btn primary" onclick="newSchedule()">${ico('plus',15)}New schedule</button></div>
    </div>
    <section class="panel" id="sched-list-card" aria-label="Scheduled backups"><div class="empty-state">Loading…</div></section>
    <section class="panel${editing?' editing':''}" id="sched-form-card" aria-label="${editing?'Edit schedule':'New schedule'}">
      ${panelHead(`${editing?'Edit schedule':'New schedule'}${editingSched?` <span style="font-weight:400;font-size:13px;color:var(--ink-3)">${esc(editingSched.label||'Backup')}</span>`:''}`, '', `<span class="meta" id="sc-summary" style="color:var(--accent-ink);font-weight:500"></span>`)}
      <div class="grid" style="grid-template-columns:380px minmax(0,1fr);gap:0">
        <div class="panel-body" style="border-right:1px solid var(--line-2)">
          <div class="field"><label for="sc-label">Label</label><input id="sc-label" placeholder="Weekly NAS backup" oninput="updateSchedSummary()"/></div>
          <div class="field"><span class="section-label">Frequency</span>
            <input type="hidden" id="sc-mode" value="weekly"/>
            <div class="seg" role="radiogroup" aria-label="Frequency" id="sc-mode-seg">
              ${['daily','weekly','monthly'].map(m=>`<button type="button" role="radio" data-mode="${m}" onclick="setSchedMode('${m}')">${m[0].toUpperCase()+m.slice(1)}</button>`).join('')}
            </div></div>
          <div class="field" id="sc-dow-row"><label for="sc-dow">Day of week</label>
            <select id="sc-dow" onchange="updateSchedSummary()">${DOW_LONG.map((d,i)=>`<option value="${i}">${d}</option>`).join('')}</select></div>
          <div class="field" id="sc-dom-row" style="display:none"><label for="sc-dom">Day of month</label>
            <input id="sc-dom" type="number" min="1" max="28" value="1" style="width:100px" oninput="updateSchedSummary()"/><span class="hint">1–28</span></div>
          <div class="field"><span class="section-label">Time (24-hour)</span>
            <div style="display:flex;align-items:center;gap:8px">
              <input id="sc-hour" class="mono" type="number" min="0" max="23" value="2" style="width:76px" aria-label="Hour" oninput="updateSchedSummary()"/>
              <span style="font-weight:600;color:var(--muted)">:</span>
              <input id="sc-min" class="mono" type="number" min="0" max="59" value="0" style="width:76px" aria-label="Minute" oninput="updateSchedSummary()"/>
            </div></div>
          <div class="btn-row" style="margin-top:8px">
            ${editing
              ? `<button class="btn primary" onclick="saveSchedule()">${ico('save',15)}Save changes</button><button class="btn" onclick="cancelSchedEdit()">Cancel</button>`
              : `<button class="btn primary" onclick="saveSchedule()">${ico('plus',15)}Add schedule</button>`}
          </div>
          <div id="sc-result" class="result"></div>
        </div>
        <div class="panel-body" style="min-width:0">
          <div class="field"><span class="section-label">Sources <span class="note">· ${G.schedPaths.length} selected</span></span>
            <div class="chips" id="sched-chips">${renderChips(G.schedPaths,'schedPaths')}</div></div>
          <div class="field"><span class="section-label">Excluded <span class="note">· ${G.schedExcludes.length} ${G.schedExcludes.length===1?'folder':'folders'}</span></span>
            <div class="chips" id="sched-excl-chips">${renderSchedExcludeChips()}</div></div>
          <div class="pathbar">
            <button class="btn icon" onclick="schedBrowseUp()" aria-label="Up one level">${ico('up',15)}</button>
            <button class="btn sm" onclick="schedBrowseRoot()">Root</button>
            <span class="path" id="sched-browser-path"></span>
          </div>
          <div class="file-list" id="sched-browser-list" style="max-height:340px"></div>
        </div>
      </div>
    </section>`);
  renderScheduleList();
  ensureSchedBrowser();
}

function setSchedMode(m){ const i=$('sc-mode'); if(i) i.value = m; updateSchedForm(); }

function updateSchedForm(){
  const mode = $('sc-mode')?.value || 'weekly';
  if($('sc-dow-row')) $('sc-dow-row').style.display = mode==='weekly' ? '' : 'none';
  if($('sc-dom-row')) $('sc-dom-row').style.display = mode==='monthly' ? '' : 'none';
  document.querySelectorAll('#sc-mode-seg button').forEach(b => { const on = b.dataset.mode===mode; b.classList.toggle('on', on); b.setAttribute('aria-checked', on); });
  updateSchedSummary();
}
function updateSchedSummary(){
  const n = $('sc-summary'); if(!n) return;
  const s = {mode:$('sc-mode')?.value, hour:parseInt($('sc-hour')?.value||'0'), minute:parseInt($('sc-min')?.value||'0'),
    day_of_week:parseInt($('sc-dow')?.value||'0'), day_of_month:parseInt($('sc-dom')?.value||'1')};
  n.textContent = isNaN(s.hour)||isNaN(s.minute) ? '' : 'Runs ' + schedWhen(s).replace(/^Every /,'every ').replace(/^Daily/,'daily').replace(/^Monthly/,'monthly');
}

async function ensureSchedBrowser(){
  if(!G.schedBrowser) G.schedBrowser = await api(`/api/browse?path=${encodeURIComponent(window.APP_CONFIG.backupRoot)}`);
  renderSchedBrowser();
}
function renderSchedBrowser(){
  const br=G.schedBrowser; if(!br) return;
  setTxt('sched-browser-path', br.current||'');
  const list=$('sched-browser-list'); if(!list) return;
  list.innerHTML = browserRows(br, G.schedPaths, {exclusion: schedExclusion});
  wireBrowser(list, {
    onNav: p => schedBrowse(p),
    onAdd: p => addSchedPath(p),
    onRemove: p => { const i = G.schedPaths.indexOf(p); if(i>=0) removePath('schedPaths', i); },
    onExclude: p => excludeSchedPath(p),
    onUnexclude: p => unexcludeSchedPath(p),
  });
  setHTML('sched-chips', renderChips(G.schedPaths,'schedPaths'));
  setHTML('sched-excl-chips', renderSchedExcludeChips());
}
function renderSchedExcludeChips(){
  return renderChips(G.schedExcludes, 'schedExcludes', {cls:'excl',
    empty: G.schedPaths.length ? 'None — open a selected folder below to exclude items inside it' : 'None'});
}
async function schedBrowse(path){
  G.schedBrowser = await api(`/api/browse?path=${encodeURIComponent(path)}`);
  renderSchedBrowser();
}
function addSchedPath(p){ if(!G.schedPaths.includes(p)) G.schedPaths.push(p); renderPage(); }
function schedBrowseUp(){ if(G.schedBrowser?.parent) schedBrowse(G.schedBrowser.parent); }
function schedBrowseRoot(){ schedBrowse(window.APP_CONFIG.backupRoot); }

async function loadSchedules(){
  const data = await api('/api/schedules');
  if(!data.ok) return;
  G.schedules = data.schedules||[];
  renderScheduleList();
}

function renderScheduleList(){
  const card = $('sched-list-card'); if(!card) return;
  if(!G.schedules){ return; }
  if(!G.schedules.length){ card.innerHTML = '<div class="empty-state">No schedules</div>'; return; }
  const running = !!G.state?.backup_job?.running;
  const lastRun = s => s.last_run ? `Last run ${fmtWhen(s.last_run)}` : 'Not run yet';
  card.innerHTML = `<div class="tbl-wrap"><table class="tbl">
    <thead><tr><th style="width:76px">Enabled</th><th>Job</th><th>Frequency</th><th>Next run</th><th>Sources</th><th style="text-align:right">Actions</th></tr></thead>
    <tbody>${G.schedules.map(s => `<tr class="${s.id===G.schedEditId?'editing':''}">
      <td><label class="toggle"><input type="checkbox" ${s.enabled?'checked':''} aria-label="Enable ${esc(s.label||'schedule')}" onchange="toggleSched('${jsq(s.id)}',this.checked)"/><span class="toggle-track"></span></label></td>
      <td><div style="font-weight:600;font-size:13px">${esc(s.label||'Backup')}</div><div class="sub">${s.enabled?esc(lastRun(s)):'Disabled'}</div></td>
      <td>${esc(schedWhen(s))}</td>
      <td>${s.enabled ? `<div>${s.next_run?esc(new Date(s.next_run*1000).toLocaleString(undefined,{weekday:'short',day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'})):'—'}</div><div class="sub">${fmtNext(s.next_run)}</div>` : '<span class="text-muted">Paused</span>'}</td>
      <td class="path" style="line-height:1.6">${(s.paths||[]).map(esc).join('<br>')}${(s.excludes||[]).map(x=>`<br><span class="excl-path" title="Excluded">− ${esc(x)}</span>`).join('')}</td>
      <td><div class="actions">
        <button class="btn sm" onclick="runSchedNow('${jsq(s.id)}')" ${running?'disabled':''} aria-label="Run ${esc(s.label||'schedule')} now">${ico('play',13)}Run now</button>
        <button class="btn icon" onclick="editSched('${jsq(s.id)}')" aria-label="Edit ${esc(s.label||'schedule')}" title="Edit">${ico('edit',14)}</button>
        <button class="btn icon danger" onclick="deleteSched('${jsq(s.id)}')" aria-label="Delete ${esc(s.label||'schedule')}" title="Delete">${ico('trash',14)}</button>
      </div></td></tr>`).join('')}</tbody></table></div>
    <div class="panel-foot" style="justify-content:flex-start">${ico('info',14)}Times in server local time</div>`;
}

// renderPage() snapshots the form's current DOM values into G.scheduleDraft first,
// so a new draft has to be applied after the re-render.
function renderScheduleWithDraft(draft){
  renderPage();
  G.scheduleDraft = draft;
  applyScheduleDraft();
}
function newSchedule(){
  G.schedEditId = null; G.schedPaths = []; G.schedExcludes = [];
  renderScheduleWithDraft({...SCHED_DRAFT_DEFAULT});
  $('sched-form-card')?.scrollIntoView({behavior:'smooth', block:'start'});
  $('sc-label')?.focus();
}
function editSched(id){
  const s=(G.schedules||[]).find(x=>x.id===id);
  if(!s) return;
  G.schedEditId = id;
  G.schedPaths = [...(s.paths||[])];
  G.schedExcludes = [...(s.excludes||[])];
  renderScheduleWithDraft({
    label: s.label||'', mode: s.mode||'weekly',
    dow: String(s.day_of_week??0), dom: String(s.day_of_month??1),
    hour: String(s.hour??2), min: String(s.minute??0), result: '',
  });
  $('sched-form-card')?.scrollIntoView({behavior:'smooth', block:'start'});
}
function cancelSchedEdit(){
  G.schedEditId = null;
  G.schedPaths = [];
  G.schedExcludes = [];
  renderScheduleWithDraft({...SCHED_DRAFT_DEFAULT});
}

async function saveSchedule(){
  captureScheduleDraft();
  const res = $('sc-result');
  if(!G.schedPaths.length){ res.className='result bad'; res.textContent='At least one source is required'; return; }
  const payload={
    label:$('sc-label')?.value||'Scheduled backup',
    paths:G.schedPaths,
    excludes:G.schedExcludes,
    mode:$('sc-mode')?.value||'weekly',
    hour:parseInt($('sc-hour')?.value||'2'),
    minute:parseInt($('sc-min')?.value||'0'),
    day_of_week:parseInt($('sc-dow')?.value||'0'),
    day_of_month:parseInt($('sc-dom')?.value||'1'),
  };
  const editing = !!G.schedEditId;
  const data = editing
    ? await api(`/api/schedules/${G.schedEditId}`,'PUT',payload)
    : await api('/api/schedules','POST',payload);
  if(data.ok){
    G.schedEditId=null;
    G.schedPaths=[];
    G.schedExcludes=[];
    renderScheduleWithDraft({...SCHED_DRAFT_DEFAULT, result: editing ? 'Schedule updated' : 'Schedule created'});
    const r=$('sc-result'); if(r) r.className='result ok';
    loadSchedules();
  } else {
    res.className='result bad'; res.textContent=data.error;
  }
}
async function toggleSched(id,enabled){ await api(`/api/schedules/${id}`,'PUT',{enabled}); loadSchedules(); }
async function deleteSched(id){
  const s=(G.schedules||[]).find(x=>x.id===id);
  if(!confirm(`Delete schedule "${s?.label||'Backup'}"?`)) return;
  await api(`/api/schedules/${id}`,'DELETE');
  if(G.schedEditId===id) cancelSchedEdit();
  loadSchedules();
}

async function runSchedNow(id){
  const s=(G.schedules||[]).find(x=>x.id===id);
  if(!s) return;
  if(G.state?.backup_job?.running){ alert('A backup is already running.'); return; }
  if(!confirm(`Run "${s.label||'this schedule'}" now?`)) return;
  // Optimistically update backup_job and jump to the Backup page, mirroring startBackup().
  if(G.state) G.state.backup_job = {
    ...G.state.backup_job, running: true, status: 'scanning',
    last_message: 'Starting backup — scanning sources…',
    percent: 0, bytes_written: 0, speed_bps: 0, eta_seconds: null,
  };
  showPage('backup');
  const data=await api('/api/backup/start','POST',{paths:s.paths, excludes:s.excludes||[], mode:s.backup_mode||'full', label:s.label});
  if(!data.ok){
    if(G.state) G.state.backup_job = {...G.state.backup_job, running:false, status:'idle'};
    if((data.error||'').toLowerCase().includes('already running')){
      await pollOnce();
    } else {
      if(G.state) G.state.backup_job = {...G.state.backup_job, status:'failed', last_message:data.error||'Failed to start backup', error:data.error||'Failed to start backup'};
      renderPage();
      await pollOnce();
    }
    return;
  }
  if(data.backup_job && G.state) G.state.backup_job = data.backup_job;
  renderPage();
  rapidPollWhile(() => G.state?.backup_job?.running && BACKUP_PREP_PHASES.has(G.state?.backup_job?.status || 'idle'));
}
