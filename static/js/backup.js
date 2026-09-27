'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// BACKUP PAGE
// ══════════════════════════════════════════════════════════════════════════════
const BACKUP_PREP_PHASES = new Set(['scanning','preparing','selecting_tape','loading_tape','pre_hook','erasing']);
const BACKUP_STEPS = [
  {label:'Scan & pick tape', phases:['scanning','preparing','selecting_tape']},
  {label:'Load tape',       phases:['loading_tape']},
  {label:'Pre-backup hook', phases:['pre_hook','erasing']},
  {label:'Write to tape',   phases:['streaming','cancelling']},
  {label:'Build index',     phases:['indexing']},
  {label:'Verify',          phases:['verifying']},
  {label:'Rewind & unload', phases:['rewinding','unloading','post_hook']},
];

function renderBackupPage(c){
  const bk = G.state?.backup_job||{};
  const running = !!bk.running;

  // In a prep phase with no rapid poll running: kick one off so phase changes land quickly.
  if(running && BACKUP_PREP_PHASES.has(bk.status||'') && !G._rapidPolling){
    G._rapidPolling = true;
    const rapidPoll = async () => {
      await pollOnce();
      const st = G.state?.backup_job?.status||'idle';
      if(G.state?.backup_job?.running && BACKUP_PREP_PHASES.has(st)) setTimeout(rapidPoll, 2000);
      else G._rapidPolling = false;
    };
    setTimeout(rapidPoll, 2000);
  }

  c.insertAdjacentHTML('beforeend', `
    <div class="page-head">
      <div><h1>Backup</h1><p class="sub">Tape auto-selection: ${esc(strategyLabel())} strategy</p></div>
      <div class="actions">
        <button class="btn danger" onclick="stopBackup()" ${running?'':'disabled'} id="btn-stop-backup">${ico('stop',15)}Stop backup</button>
      </div>
    </div>`);

  c.insertAdjacentHTML('beforeend', backupJobPanel(bk));

  const row = el('div','grid g-split');
  const src = el('section','panel');
  src.setAttribute('aria-label','Sources');
  src.innerHTML = `${panelHead('Sources','Server files and folders for the next backup')}
    <div style="padding:12px 20px;border-bottom:1px solid var(--line-2)" class="pathbar">
      <button class="btn icon" onclick="backupBrowseUp()" aria-label="Up one level">${ico('up',15)}</button>
      <button class="btn sm" onclick="backupBrowseRoot()">Root</button>
      <span class="path" id="backup-browser-path">${esc(window.APP_CONFIG.backupRoot)}</span>
    </div>
    <div class="file-list flat" id="backup-browser-list" style="max-height:520px"></div>`;
  row.appendChild(src);
  row.insertAdjacentHTML('beforeend', nextBackupPanel(bk));
  c.appendChild(row);

  const row2 = el('div','grid g-split');
  row2.insertAdjacentHTML('beforeend', `<section class="panel" aria-label="Job log">
    ${panelHead('Job log','',`<span class="meta">Newest first · last 30 entries</span>`)}
    <div class="log-list">${logRows(bk.log||[], {limit:30})}</div></section>`);
  row2.insertAdjacentHTML('beforeend', verifyPanel());
  c.appendChild(row2);

  renderBackupBrowser();
}

function strategyLabel(){ return (G.settings?.tape_fill_strategy||'spread') === 'fill' ? 'Fill' : 'Spread'; }

function backupJobPanel(bk){
  const status = bk.status || 'idle';
  const running = !!bk.running;
  const prep = BACKUP_PREP_PHASES.has(status) || status==='rewinding' || status==='unloading';
  const pct = Math.max(0, Math.min(100, Number(bk.percent||0)));
  const label = BACKUP_PHASE_LABEL[status] || status;
  const tone = running ? 'warn' : status==='completed' ? 'ok' : status==='failed' ? 'bad' : '';
  const logLvl = bk.log_level || G.settings?.default_backup_log_level || 'normal';
  const started = bk.started_at ? `started ${fmtTime(bk.started_at)} · ` : '';

  // Stepper — shown while running or once complete.
  let stepper = '';
  if(running || status==='completed'){
    const cur = BACKUP_STEPS.findIndex(s => s.phases.includes(status));
    stepper = `<ol class="stepper" aria-label="Job phases">` + BACKUP_STEPS.map((s,i) => {
      const state = status==='completed' ? 'done' : i < cur ? 'done' : i === cur ? 'active' : '';
      return `<li class="step ${state}"><span class="row"><span class="mark">${state==='done'?ico('check',13,'stroke-width="3"'):i+1}</span><span class="line"></span></span><span class="lbl">${s.label}</span></li>`;
    }).join('') + `</ol>`;
  }

  const vol = G.state?.summary?.loaded_volume || '—';
  const speedMB = (bk.speed_bps||0) / (1024*1024);
  const cells = `<div class="cells c5 nobottom">
    <div class="cell"><div class="cell-label">Written</div><div class="cell-value lg">${hBytes(bk.bytes_written)}</div></div>
    <div class="cell"><div class="cell-label">Estimated size</div><div class="cell-value lg">${hBytes(bk.bytes_total)}</div></div>
    <div class="cell"><div class="cell-label">Throughput</div><div class="cell-value lg ${running&&bk.speed_bps>0?'c-green':''}">${hBytes(bk.speed_bps)}/s</div></div>
    <div class="cell"><div class="cell-label">Time remaining</div><div class="cell-value lg">${running && !prep ? fmtSec(bk.eta_seconds) : '—'}</div></div>
    <div class="cell"><div class="cell-label">Tape in drive</div><div class="cell-value lg">${esc(vol)}</div></div>
  </div>`;

  const progress = running && prep
    ? `<div class="prog-wrap lg"><span class="prog-bar amber pulse"></span></div>
       <div class="meter-labels top"><span>${esc(label)}</span><span>${esc(bk.last_message||'')}</span></div>`
    : `<div class="prog-wrap lg"><span class="prog-bar ${running?'amber':status==='failed'?'amber':''}" style="width:${pct}%"></span></div>
       <div class="meter-labels top"><span class="mono">${pct.toFixed(1)}%</span><span class="${bk.error?'c-red':''}">${esc(bk.error || bk.last_message || '—')}</span></div>`;

  const samples = G.speedSamples;
  let chart = '';
  if(running && status === 'streaming' || samples.length > 1){
    const vals = samples.map(s=>s.v);
    const maxV = Math.max(...vals, speedMB, 1);
    const {max, ticks} = niceTicks(maxV, 4);
    const avg = vals.length ? vals.reduce((a,v)=>a+v,0)/vals.length : 0;
    const lo = vals.length ? Math.min(...vals) : 0, hi = vals.length ? Math.max(...vals) : 0;
    chart = `<div class="stack divider-top" style="gap:10px">
      <div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap">
        <span style="font-size:13px;font-weight:600">Throughput · this job</span>
        <span class="text-sm text-muted tnum">${vals.length ? `Average ${avg.toFixed(0)} MB/s · peak ${hi.toFixed(0)} · low ${lo.toFixed(0)} · ` : ''}sampled each poll</span>
      </div>
      ${lineChart(samples, {max, ticks, height:170, fmt:v=>`${v.toFixed(0)} MB/s`, aria:'Throughput over the current job in MB/s'})}
    </div>`;
  }

  return `<section class="panel" aria-label="Current job">
    ${panelHead(`Current job ${badge(label, tone, running)}`, '', `<span class="meta">${started}log level ${esc(logLvl)}</span>`)}
    <div class="panel-body" style="gap:22px">${stepper}${cells}<div class="stack" style="gap:8px">${progress}</div>${chart}</div>
  </section>`;
}

function nextBackupPanel(bk){
  const running = !!bk.running;
  const lvl = G.settings?.default_backup_log_level || 'normal';
  const s = G.settings || {};
  const items = G.backupPaths.length
    ? `<ul class="src-list">${G.backupPaths.map((p,i)=>`<li><span title="${esc(p)}">${esc(p)}</span><button class="btn icon ghost" aria-label="Remove ${esc(p)}" onclick="removePath('backupPaths',${i})">${ico('x',14)}</button></li>`).join('')}</ul>`
    : `<div class="chart-empty" style="min-height:80px">No sources selected</div>`;
  const cfg = G.settings ? `<div class="field"><span class="section-label">Applied from system config</span>
      <div class="kv right">
        <span>Verify after backup</span><span>${s.verify_after_backup ? `On · ${s.verify_sample_mb===0?'full tape':hBytes((s.verify_sample_mb||0)*1024*1024)+' sample'}` : 'Off'}</span>
        <span>Erase before backup</span><span>${s.erase_before_backup?'On':'Off'}</span>
        <span>Auto-rewind after backup</span><span>${s.auto_rewind_after_backup?'On':'Off'}</span>
        <span>Pre / post hooks</span><span class="mono" style="font-size:12px">${esc(s.pre_backup_hook||'none')} / ${esc(s.post_backup_hook||'none')}</span>
      </div></div>` : '';
  return `<section class="panel" aria-label="Next backup">
    ${panelHead('Next backup','',`<span class="meta">${plural(G.backupPaths.length,'source')}</span>`)}
    <div class="panel-body" style="gap:18px">
      ${items}
      <div class="field"><span class="section-label">Log level</span>
        <div class="seg" role="radiogroup" aria-label="Log level" id="backup-log-level" data-value="${esc(lvl)}">
          ${['minimal','normal','verbose'].map(v=>`<button type="button" role="radio" aria-checked="${v===lvl}" class="${v===lvl?'on':''}" ${running?'disabled':''} onclick="setBackupLogLevel('${v}')">${v[0].toUpperCase()+v.slice(1)}</button>`).join('')}
        </div></div>
      ${cfg}
      <div class="divider-top">
        <button class="btn primary block" style="height:40px" onclick="startBackup()" ${running||!G.backupPaths.length?'disabled':''} id="btn-start-backup">${ico('play',15)}Start backup</button>
      </div>
    </div>
  </section>`;
}

function setBackupLogLevel(v){
  const seg = $('backup-log-level'); if(!seg) return;
  seg.dataset.value = v;
  seg.querySelectorAll('button').forEach(b => { const on = b.textContent.toLowerCase()===v; b.classList.toggle('on', on); b.setAttribute('aria-checked', on); });
}

function verifyPanel(){
  const vj = G.state?.verify_job||{};
  const tone = vj.running ? 'warn' : (vj.errors>0 || String(vj.status||'').includes('error') || vj.status==='failed') ? 'bad' : vj.status==='completed' ? 'ok' : '';
  const label = vj.status ? vj.status[0].toUpperCase()+vj.status.slice(1).replace(/_/g,' ') : 'Idle';
  return `<section class="panel" aria-label="Verification">
    ${panelHead('Verification','',badge(label, tone, vj.running))}
    <div class="panel-body">
      <div class="cells c2 nobottom">
        <div class="cell"><div class="cell-label">Checked</div><div class="cell-value lg">${vj.bytes_verified?hBytes(vj.bytes_verified):'—'}</div></div>
        <div class="cell"><div class="cell-label">Errors</div><div class="cell-value lg ${vj.errors>0?'c-red':vj.bytes_verified?'c-green':''}">${vj.errors ?? (vj.bytes_verified?0:'—')}</div></div>
      </div>
      ${vj.running ? `<div class="prog-wrap thin"><span class="prog-bar blue pulse"></span></div>` : ''}
      <span class="${vj.error?'c-red':''}" style="font-size:12.5px">${esc(vj.error || vj.last_message || 'No verification has run yet')}</span>
      ${(vj.log||[]).length ? `<div class="log-list boxed">${logRows(vj.log, {limit:50, boxed:true})}</div>` : ''}
      <button class="btn" onclick="startVerify()" ${vj.running?'disabled':''}>${ico('shieldCheck',15)}Verify loaded tape</button>
    </div>
  </section>`;
}

function renderChips(arr, key, {cls='', empty='None selected'}={}){
  if(!arr.length) return `<span class="text-sm text-muted">${esc(empty)}</span>`;
  return arr.map((p,i)=>`<span class="chip${cls?' '+cls:''}" title="${esc(p)}"><span>${esc(p)}</span><button class="chip-x" aria-label="Remove ${esc(p)}" onclick="removePath('${key}',${i})">${ico('x',13)}</button></span>`).join('');
}
function removePath(key, i){ G[key].splice(i,1); renderPage(); }

// Shared server-side folder browser (backup sources + schedule sources).
// ``exclusion(path)`` (optional) lets a browser offer excludes for items inside
// a selected source: return 'excluded', 'inherited' (a parent is excluded),
// 'excludable', or a falsy value for the normal Add/Added button.
function browserRows(br, selected, {exclusion}={}){
  const rows = [];
  // Returns [row class, row click attr, button html] for one entry.
  const rowState = p => {
    if(selected.includes(p)) return [' selected', `data-remove="${esc(p)}"`,
      `<button class="btn xs added" data-remove="${esc(p)}">${ico('check',13)}Added</button>`];
    const ex = exclusion?.(p);
    if(ex==='excluded') return [' excluded', `data-unexclude="${esc(p)}"`,
      `<button class="btn xs excluded" data-unexclude="${esc(p)}" title="Include again">${ico('minus',13)}Excluded</button>`];
    if(ex==='inherited') return [' excluded', '', '<span class="file-meta">Excluded by parent</span>'];
    if(ex==='excludable') return ['', `data-exclude="${esc(p)}"`,
      `<button class="btn xs" data-exclude="${esc(p)}">${ico('minus',13)}Exclude</button>`];
    return ['', `data-add="${esc(p)}"`, `<button class="btn xs" data-add="${esc(p)}">${ico('plus',13)}Add</button>`];
  };
  if(br.parent) rows.push(`<div class="file-row dir" data-nav="${esc(br.parent)}"><span class="file-icon">${ico('up',15)}</span><span class="file-name">.. up one level</span></div>`);
  for(const d of br.directories||[]){
    const [cls, , btn] = rowState(d.path);
    rows.push(`<div class="file-row dir${cls}" data-nav="${esc(d.path)}"><span class="file-icon">${ico('folder',16)}</span><span class="file-name">${esc(d.name)}/</span>
      ${btn}</div>`);
  }
  for(const f of br.files||[]){
    const [cls, click, btn] = rowState(f.path);
    rows.push(`<div class="file-row${cls}" ${click}><span class="file-icon">${ico('file',16)}</span><span class="file-name">${esc(f.name)}</span>
      <span class="file-meta">${hBytes(f.size||0)}</span>
      ${btn}</div>`);
  }
  if(!(br.directories||[]).length && !(br.files||[]).length) rows.push('<div class="empty-state">Empty folder</div>');
  return rows.join('');
}
function wireBrowser(list, {onNav, onAdd, onRemove, onExclude, onUnexclude}){
  const act = el => {
    const d = el.dataset;
    if(d.add != null){ onAdd(d.add); return true; }
    if(d.remove != null){ onRemove(d.remove); return true; }
    if(d.exclude != null && onExclude){ onExclude(d.exclude); return true; }
    if(d.unexclude != null && onUnexclude){ onUnexclude(d.unexclude); return true; }
    return false;
  };
  list.onclick = e => {
    const t = e.target.closest('[data-add],[data-remove],[data-exclude],[data-unexclude],[data-nav]');
    if(!t) return;
    const btn = e.target.closest('button');
    if(btn && act(btn)) return;
    if(t.dataset.nav != null){ onNav(t.dataset.nav); return; }
    act(t);
  };
}

async function ensureBackupBrowser(){
  if(!G.backupBrowser) G.backupBrowser = await api(`/api/browse?path=${encodeURIComponent(window.APP_CONFIG.backupRoot)}`);
}
function renderBackupBrowser(){
  ensureBackupBrowser().then(()=>{
    const br = G.backupBrowser; if(!br) return;
    setTxt('backup-browser-path', br.current||'');
    const list = $('backup-browser-list'); if(!list) return;
    list.innerHTML = browserRows(br, G.backupPaths, {});
    wireBrowser(list, {
      onNav: p => backupBrowse(p),
      onAdd: p => addBackupPath(p),
      onRemove: p => { const i = G.backupPaths.indexOf(p); if(i>=0) removePath('backupPaths', i); },
    });
  });
}
async function backupBrowse(path){
  G.backupBrowser = await api(`/api/browse?path=${encodeURIComponent(path)}`);
  renderBackupBrowser();
}
function addBackupPath(p){ if(!G.backupPaths.includes(p)) G.backupPaths.push(p); renderPage(); }
function backupBrowseUp(){ if(G.backupBrowser?.parent) backupBrowse(G.backupBrowser.parent); }
function backupBrowseRoot(){ backupBrowse(window.APP_CONFIG.backupRoot); }

async function startBackup(){
  if(!G.backupPaths.length){ alert('At least one source is required.'); return; }
  const logLevel = $('backup-log-level')?.dataset.value || G.settings?.default_backup_log_level || 'normal';

  // Optimistically update backup_job state so the page renders something useful before
  // the first real poll comes back — the backend may spend 30–60s loading a tape.
  if(G.state) G.state.backup_job = {
    ...G.state.backup_job, running: true, status: 'scanning',
    last_message: 'Starting backup — scanning sources…',
    percent: 0, bytes_written: 0, speed_bps: 0, eta_seconds: null,
  };
  if(G.page !== 'backup') showPage('backup'); else renderPage();

  const data = await api('/api/backup/start','POST',{paths:G.backupPaths, log_level: logLevel});
  if(!data.ok){
    if(G.state) G.state.backup_job = {...G.state.backup_job, running: false, status: 'idle'};
    if((data.error||'').toLowerCase().includes('already running')){
      await pollOnce();
    } else {
      if(G.state) G.state.backup_job = {...G.state.backup_job, status: 'failed', last_message: data.error || 'Failed to start backup', error: data.error || 'Failed to start backup'};
      renderPage();
      await pollOnce();
    }
    return;
  }
  if(data.backup_job && G.state) G.state.backup_job = data.backup_job;
  G.backupPaths = [];
  renderPage();
  rapidPollWhile(() => G.state?.backup_job?.running && BACKUP_PREP_PHASES.has(G.state?.backup_job?.status || 'idle'));
}

// Poll every 2s while `cond()` holds (max 60 polls) so prep-phase changes land quickly.
function rapidPollWhile(cond){
  let n = 0;
  const tick = async () => {
    await pollOnce();
    if(cond() && n++ < 60) setTimeout(tick, 2000);
  };
  setTimeout(tick, 1500);
}

async function stopBackup(){
  if(!confirm('Cancel backup? tar finishes the current file(s) first, then stops.')) return;
  await api('/api/backup/stop','POST');
  await pollOnce();
}

async function startVerify(){
  const data=await api('/api/verify/start','POST');
  if(!data.ok) alert(data.error);
  await pollOnce();
}
