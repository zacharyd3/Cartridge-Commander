'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// CORE — shared state, formatters, API wrapper, icons, derived data
// ══════════════════════════════════════════════════════════════════════════════
let G = {
  state: null,
  driveInfo: null,
  tapeHistoryMap: {},
  indexes: {},
  indexMeta: [],
  page: 'library',
  backupPaths: [],
  schedPaths: [],
  schedExcludes: [],    // paths inside schedPaths that the schedule leaves out
  schedBrowser: null,
  schedEditId: null,    // id of the schedule loaded into the form for editing
  schedules: null,

  backupBrowser: null,
  restoreBrowser: null,
  tdSlot: null,
  tdVol: '',
  tdInDrive: false,
  tdSlotFull: false,
  tdFiles: [],
  tdFiltered: [],
  tdSelected: new Set(),
  restorePending: { paths:[], vol:'', slot:null },
  unloadTargetSlot: null,
  scheduleDraft: null,
  activeAction: null,   // {msg, status} — shown as banner on the overview page
  _rapidPolling: false, // true while prep-phase rapid polls are running
  _changerRapidPolling: false, // true while a changer op (load/unload/reindex/…) rapid-polls
  fmtSelected: new Set(),  // volume_tags checked for erase in the tape catalog
  fmtCatalogOnly: false,   // "Catalog-only reset" checkbox state
  fmtConfirm: '',          // typed confirmation phrase for hardware erase

  records: null,         // backup records (newest first), fetched lazily
  _recordsAt: 0,
  gfs: null,             // /api/gfs/status payload, fetched lazily
  settings: null,
  speedSamples: [],      // [{t, v}] throughput samples for the running backup
  _speedJob: null,
  mediaTab: 'all',
  mediaFilter: '',
  actionFilter: 'all',
  restoreFilter: '',
};

// ── Utils ────────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmtTs = ts => ts ? new Date(ts*1000).toLocaleString() : '—';
const fmtSec = s => {
  if(s==null||s<0) return '—';
  s=Math.round(s);
  if(s<60) return s+'s';
  if(s<3600) return `${Math.floor(s/60)}m ${s%60}s`;
  return `${Math.floor(s/3600)}h ${Math.floor((s%3600)/60)}m`;
};
const hBytes = v => {
  v=Number(v||0); const u=['B','KB','MB','GB','TB']; let i=0;
  while(v>=1024&&i<u.length-1){v/=1024;i++;} return `${v.toFixed(1)} ${u[i]}`;
};
const fmtBytes = hBytes;
const TB = 1024 ** 4;
const toTB = b => (Number(b||0) / TB);
const fmtTB = (b, d=2) => `${toTB(b).toFixed(d)} TB`;
const getEffectiveLoadedSlot = () => G.state?.drive?.effective_loaded_slot ?? G.state?.drive?.loaded_from_slot ?? G.state?.summary?.loaded_slot ?? null;
const getUnloadTargetSlot = () => G.unloadTargetSlot ?? getEffectiveLoadedSlot();
function setUnloadTargetSlot(slot){
  const n = Number(slot||0);
  G.unloadTargetSlot = n > 0 ? n : null;
  if(G.page === 'library') renderPage();
  const tdActs = $('td-actions');
  if(tdActs && $('tape-drawer')?.classList.contains('open') && G.tdInDrive){
    const btn = [...tdActs.querySelectorAll('button')].find(b => b.dataset.act === 'unload');
    if(btn){
      const tgt = getUnloadTargetSlot();
      btn.innerHTML = `${ico('eject',15)}Unload to slot ${tgt ?? '—'}`;
      btn.disabled = !tgt;
    }
  }
}
const fmtNext = ts => {
  if(!ts) return '—';
  const diff = ts*1000 - Date.now();
  if(diff<0) return 'overdue';
  const d=Math.floor(diff/86400000), h=Math.floor((diff%86400000)/3600000), m=Math.floor((diff%3600000)/60000);
  if(d>0) return `in ${d}d ${h}h`;
  return h>0 ? `in ${h}h ${m}m` : `in ${m}m`;
};
const fmtDate = ts => ts ? new Date(ts*1000).toLocaleDateString(undefined,{day:'2-digit',month:'short',year:'numeric'}) : '—';
const fmtTime = ts => ts ? new Date(ts*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '—';
const fmtWhen = ts => {
  if(!ts) return '—';
  const d = new Date(ts*1000), now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  const y = new Date(now); y.setDate(now.getDate()-1);
  if(sameDay) return `Today ${fmtTime(ts)}`;
  if(d.toDateString() === y.toDateString()) return `Yesterday ${fmtTime(ts)}`;
  return d.toLocaleDateString(undefined,{day:'2-digit',month:'short'}) + ' ' + fmtTime(ts);
};
const fmtAgo = ts => {
  if(!ts) return '';
  const diff = Math.floor((Date.now() - ts*1000)/1000);
  if(diff < 60) return 'just now';
  if(diff < 3600) return `${Math.floor(diff/60)}m ago`;
  if(diff < 86400) return `${Math.floor(diff/3600)}h ago`;
  if(diff < 86400*30) return `${Math.floor(diff/86400)}d ago`;
  return fmtDate(ts);
};
const _API_KEY = window.APP_CONFIG.apiKey;
async function api(path,method='GET',body=null){
  const headers = {'Content-Type':'application/json'};
  if(_API_KEY) headers['X-API-Key'] = _API_KEY;
  try {
    const r = await fetch(path,{method,headers,body:body?JSON.stringify(body):null});
    return await r.json();
  } catch(e) {
    return {ok: false, error: `Network error: ${e.message||e}`};
  }
}
function el(tag, cls, html){
  const e=document.createElement(tag);
  if(cls) e.className=cls;
  if(html!==undefined) e.innerHTML=html;
  return e;
}
function setHTML(id,html){ const e=$(id); if(e) e.innerHTML=html; }
function setTxt(id,txt){ const e=$(id); if(e) e.textContent=txt; }
function escHtml(v){ return String(v??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
const esc = escHtml;
// Escape for use inside a single-quoted JS string in an inline handler.
function jsq(v){ return escHtml(String(v??'').replace(/\\/g,'\\\\').replace(/'/g,"\\'")); }
function is_cleaning_vol(vol){ return /^CLN/i.test(vol||''); }
function plural(n, w, p){ return `${n} ${n===1?w:(p||w+'s')}`; }

// ── Icons (inline stroke SVG) ────────────────────────────────────────────────
const ICONS = {
  grid:'<rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/>',
  upload:'<path d="M12 15V3"/><path d="m7 8 5-5 5 5"/><path d="M20 15v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-4"/>',
  restore:'<path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/>',
  calendar:'<rect x="3" y="4" width="18" height="17" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/>',
  cassette:'<rect x="2" y="5" width="20" height="14" rx="2"/><circle cx="8" cy="12" r="2"/><circle cx="16" cy="12" r="2"/><path d="M10 12h4"/>',
  shield:'<path d="M12 3 4 6v6c0 5 3.5 8 8 9 4.5-1 8-4 8-9V6z"/>',
  shieldCheck:'<path d="M12 3 4 6v6c0 5 3.5 8 8 9 4.5-1 8-4 8-9V6z"/><path d="m9 12 2 2 4-4"/>',
  pulse:'<path d="M22 12h-4l-3 8L9 4l-3 8H2"/>',
  sliders:'<path d="M4 21v-7M4 10V3M12 21v-9M12 8V3M20 21v-5M20 12V3M1 14h6M9 8h6M17 16h6"/>',
  search:'<circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/>',
  check:'<path d="M20 6 9 17l-5-5"/>',
  x:'<path d="M18 6 6 18M6 6l12 12"/>',
  alert:'<path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/>',
  info:'<circle cx="12" cy="12" r="9"/><path d="M12 8h.01M11 12h1v4h1"/>',
  play:'<path d="M6 4l14 8-14 8z"/>',
  stop:'<rect x="6" y="6" width="12" height="12" rx="1"/>',
  pause:'<path d="M8 5v14M16 5v14"/>',
  eject:'<path d="M5 17h14M12 5l7 8H5z"/>',
  load:'<path d="M12 5v10M7 10l5 5 5-5M5 19h14"/>',
  rewind:'<path d="M19 20 9 12l10-8z"/><path d="M5 19V5"/>',
  refresh:'<path d="M21 12a9 9 0 0 1-15.5 6.3L3 16"/><path d="M3 12a9 9 0 0 1 15.5-6.3L21 8"/><path d="M21 3v5h-5M3 21v-5h5"/>',
  folder:'<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
  file:'<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><path d="M14 3v6h6"/>',
  chevR:'<path d="m9 18 6-6-6-6"/>',
  chevD:'<path d="m6 9 6 6 6-6"/>',
  drive:'<path d="M22 12H2M5.5 5h13L22 12v6a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2v-6z"/><path d="M6 16h.01M10 16h.01"/>',
  inbox:'<path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.5 5h13L22 12v6a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2v-6z"/>',
  trash:'<path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6"/>',
  archive:'<rect x="2" y="3" width="20" height="5" rx="1"/><path d="M4 8v11a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8M10 12h4"/>',
  plus:'<path d="M12 5v14M5 12h14"/>',
  edit:'<path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4z"/>',
  brush:'<path d="M9.06 11.9 18 3l3 3-8.94 8.94"/><path d="M7 14c-1.66 0-3 1.34-3 3 0 1.3-1 2-2 2 1 1.5 3 2 4 2 2.2 0 4-1.8 4-4 0-1.66-1.34-3-3-3z"/>',
  list:'<path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/>',
  up:'<path d="M12 19V5M5 12l7-7 7 7"/>',
  send:'<path d="m22 2-7 20-4-9-9-4z"/><path d="M22 2 11 13"/>',
  eye:'<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  scan:'<path d="M3 7V5a2 2 0 0 1 2-2h2M17 3h2a2 2 0 0 1 2 2v2M21 17v2a2 2 0 0 1-2 2h-2M7 21H5a2 2 0 0 1-2-2v-2M7 12h10"/>',
  zap:'<path d="M13 2 3 14h9l-1 8 10-12h-9z"/>',
  menu:'<path d="M4 6h16M4 12h16M4 18h16"/>',
  minus:'<path d="M5 12h14"/>',
  circle:'<circle cx="12" cy="12" r="6"/>',
  save:'<path d="M5 3h11l5 5v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z"/><path d="M7 3v5h8M7 21v-7h10v7"/>',
};
function ico(name, size=16, attrs=''){
  return `<svg class="ico" width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${attrs?' '+attrs:''}>${ICONS[name]||''}</svg>`;
}
function hydrateIcons(root=document){
  root.querySelectorAll('[data-ico]').forEach(n => {
    n.outerHTML = ico(n.dataset.ico, Number(n.dataset.size||16));
  });
}

// ── Small HTML builders ──────────────────────────────────────────────────────
function badge(text, tone='', dot=false){
  return `<span class="badge ${tone}">${dot?'<span class="d"></span>':''}${esc(text)}</span>`;
}
function panelHead(title, sub='', right=''){
  return `<div class="panel-head"><div><h2>${title}</h2>${sub?`<p>${sub}</p>`:''}</div>${right}</div>`;
}
function statusTone(status, running){
  if(running) return 'warn';
  if(status==='completed') return 'ok';
  if(status==='completed_with_errors') return 'warn';
  if(status==='failed'||status==='error') return 'bad';
  return '';
}
function meterColor(pct){ return pct > 90 ? 'var(--bad)' : pct > 70 ? 'var(--warn)' : 'var(--ok)'; }
function logClass(msg){
  const m = String(msg||'');
  const low = m.toLowerCase();
  const isPass = m.startsWith('  ') || m.startsWith('\t');
  const isInfo = m.startsWith('ℹ') || m.startsWith('✓') || m.startsWith('✔');
  const isErr = !isPass && !isInfo && (m.startsWith('✗') || low.includes('error') || low.includes('failed') || low.includes('fail'));
  const isWarn = !isErr && (isPass || low.includes('warning') || low.includes('stderr') || low.includes('skipped'));
  if(isErr) return 'error';
  if(isWarn) return 'warn';
  if(m.startsWith('✓') || m.startsWith('✔')) return 'ok';
  return 'info';
}
function logLevelBadge(cls){
  return cls==='error' ? badge('ERROR','bad') : cls==='warn' ? badge('WARN','warn') : badge('INFO','info');
}
function logRows(entries, {limit=30, boxed=false}={}){
  if(!entries?.length) return '<div class="empty-state">No entries</div>';
  return entries.slice(0,limit).map(e => {
    const cls = logClass(e.message);
    if(boxed) return `<div class="log-entry ${cls}"><span class="log-time">${fmtTime(e.ts)}</span><span class="log-msg">${esc(e.message)}</span></div>`;
    return `<div class="log-entry ${cls}"><span class="log-time">${new Date((e.ts||0)*1000).toLocaleTimeString()}</span><span>${logLevelBadge(cls)}</span><span class="log-msg">${esc(e.message)}</span></div>`;
  }).join('');
}

// ── Lazy data: records, schedules, settings, GFS ─────────────────────────────
async function ensureRecords(force=false){
  if(!force && G.records && Date.now() - G._recordsAt < 60000) return G.records;
  const data = await api('/api/backup_records?limit=1000');
  if(data.ok){ G.records = data.records || []; G._recordsAt = Date.now(); }
  return G.records || [];
}
async function ensureSchedules(force=false){
  if(!force && G.schedules) return G.schedules;
  const data = await api('/api/schedules');
  if(data.ok) G.schedules = data.schedules || [];
  return G.schedules || [];
}
async function ensureSettings(force=false){
  if(!force && G.settings) return G.settings;
  const data = await api('/api/settings');
  if(data.ok) G.settings = data.settings;
  return G.settings;
}
async function ensureGfs(force=false){
  if(!force && G.gfs) return G.gfs;
  const data = await api('/api/gfs/status');
  if(data.ok) G.gfs = data;
  return G.gfs;
}
// After a lazy fetch lands, repaint only if the user is still on a page that shows it.
function refreshIfOn(pages){ if(pages.includes(G.page)) renderPage(); }

// Retention stream a record belongs to — mirrors records.gfs_stream_key().
function recordStream(r){
  const label = String(r.label || '').trim();
  if(label && r.started_at != null){
    const auto = `${r.volume_tag || 'nolabel'}_${parseInt(r.started_at,10)}`;
    if(label === auto) return '';
  }
  return label;
}
const streamName = s => s || 'Ad-hoc';

function dayKey(ts){ const d = new Date(ts*1000); return `${d.getFullYear()}-${d.getMonth()+1}-${d.getDate()}`; }

// Bytes written per day for the last `days` days, split by retention stream.
// The running backup (not yet a record) is added to today's total.
function writtenPerDay(days){
  const recs = G.records || [];
  const today = new Date(); today.setHours(0,0,0,0);
  const start = today.getTime()/1000 - (days-1)*86400;
  const buckets = [];
  for(let i=0;i<days;i++){
    const d = new Date((start + i*86400)*1000);
    buckets.push({key: dayKey(start + i*86400), date: d, total: 0, streams: {}, jobs: 0});
  }
  const idx = Object.fromEntries(buckets.map((b,i)=>[b.key,i]));
  const add = (ts, bytes, stream) => {
    const i = idx[dayKey(ts)]; if(i==null) return;
    const b = buckets[i];
    b.total += bytes; b.jobs += 1;
    b.streams[stream] = (b.streams[stream]||0) + bytes;
  };
  for(const r of recs){
    if(!r.started_at || r.started_at < start) continue;
    add(r.started_at, Number(r.bytes_written||0), streamName(recordStream(r)));
  }
  const bk = G.state?.backup_job;
  if(bk?.running && bk.started_at && bk.bytes_written) add(bk.started_at, Number(bk.bytes_written), 'Running');
  return buckets;
}

function outcomeCounts(days=30){
  const since = Date.now()/1000 - days*86400;
  const c = {completed:0, failed:0, cancelled:0, running:0};
  for(const r of (G.records||[])){
    if((r.started_at||0) < since) continue;
    if(r.status in c) c[r.status]++;
  }
  if(G.state?.backup_job?.running) c.running = 1;
  return c;
}

// LTO native capacity, guessed from an LTO barcode suffix ("…L6") when the catalog has none.
function capacityFromTag(vol){
  const m = /L(\d)$/i.exec(vol||'');
  const caps = {1:.1,2:.2,3:.4,4:.8,5:1.5,6:2.5,7:6,8:12,9:18};
  return m && caps[m[1]] ? caps[m[1]] * TB : null;
}

// Total native capacity of the data cartridges in the magazines (drive tape included,
// attributed to the magazine it was loaded from). Cartridges with no catalog entry have
// unknown usage and are reported separately.
function computeStorage(){
  const slots = (G.state?.slots||[]).filter(s=>!s.is_import_export);
  const drive = G.state?.drive || {};
  const meta = Object.fromEntries((G.indexMeta||[]).map(m=>[m.volume_tag,m]));
  const loadedSlot = getEffectiveLoadedSlot();
  const tapes = [];
  for(const s of slots){
    if(!s.full || !s.volume_tag || is_cleaning_vol(s.volume_tag)) continue;
    tapes.push({vol:s.volume_tag, magazine:s.magazine||1});
  }
  if(!drive.empty && drive.volume_tag && !is_cleaning_vol(drive.volume_tag)){
    const from = slots.find(s=>s.slot===loadedSlot);
    tapes.push({vol:drive.volume_tag, magazine: from?.magazine || 1, inDrive:true});
  }
  const knownCaps = (G.indexMeta||[]).map(m=>m.space?.capacity_bytes).filter(Boolean);
  const typical = knownCaps.length ? knownCaps.sort((a,b)=>a-b)[Math.floor(knownCaps.length/2)] : null;
  const mags = {};
  const tot = {cap:0, used:0, free:0, unk:0, count:0};
  const gens = new Set();
  for(const t of tapes){
    const m = meta[t.vol];
    const sp = t.inDrive ? (G.driveInfo?.space?.capacity_bytes ? G.driveInfo.space : m?.space) : m?.space;
    const cap = sp?.capacity_bytes || capacityFromTag(t.vol) || typical || 0;
    if(sp?.lto_generation) gens.add(sp.lto_generation);
    const known = !!m && sp?.used_bytes != null;
    const used = known ? Math.min(Number(sp.used_bytes||0), cap) : 0;
    const g = mags[t.magazine] ||= {magazine:t.magazine, cap:0, used:0, free:0, unk:0, count:0};
    for(const o of [g, tot]){
      o.cap += cap; o.count += 1;
      if(known){ o.used += used; o.free += Math.max(cap-used,0); } else o.unk += cap;
    }
  }
  const archived = (G.indexMeta||[]).filter(m => (m.purpose==='archived' || (!m.present && m.archived_at)) && !m.is_cleaning && !m.deleted);
  const archivedCap = archived.reduce((a,m)=>a + (m.space?.capacity_bytes || capacityFromTag(m.volume_tag) || typical || 0), 0);
  const gen = gens.size === 1 ? [...gens][0] : null;
  return {tot, mags: Object.values(mags).sort((a,b)=>a.magazine-b.magazine), gen, archivedCount: archived.length, archivedCap};
}
