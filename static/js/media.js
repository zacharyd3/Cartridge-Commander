'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// TAPE CATALOG (media) PAGE — catalog charts, cartridge table, erase / format
// ══════════════════════════════════════════════════════════════════════════════

// Every cartridge the page knows about: library slots, drive, mail slot, off-site.
const LOC_ORDER = {drive:0, slot:1, mail:2, offsite:3};
// Sort value for the Location column: drive, then slots by number, mail slot, off-site.
const locSortKey = r => `${LOC_ORDER[r.where]}-${String(r.slot||0).padStart(4,'0')}`;

function catalogRows(){
  const meta = Object.fromEntries((G.indexMeta||[]).map(m=>[m.volume_tag,m]));
  const recyclable = new Set(G.gfs?.recyclable || []);
  const drive = G.state?.drive || {};
  const loaded = getEffectiveLoadedSlot();
  const rows = [];
  const seen = new Set();
  const mk = (vol, loc, where, extra={}) => {
    const m = meta[vol] || null;
    const cln = is_cleaning_vol(vol) || m?.is_cleaning || m?.purpose==='cleaning';
    const sp = where==='drive' ? (G.driveInfo?.space?.capacity_bytes ? G.driveInfo.space : m?.space) : m?.space;
    const files = m?.file_count || 0;
    let state;
    if(cln) state = 'cleaning';
    else if(where==='offsite') state = 'archived';
    else if(where==='drive') state = 'drive';
    else if(recyclable.has(vol)) state = 'recyclable';
    else if(files > 0) state = 'indexed';
    else if(m && !(sp?.used_bytes)) state = 'blank';
    else state = 'noidx';
    seen.add(vol);
    // Full under the reserve set on the Retention page: backups no longer select it,
    // so a tape still in the library is ready to pull for archiving.
    const full = !cln && where!=='offsite' && !!m?.fill?.full;
    return {vol, loc, where, meta:m, sp, files, cln, state, full, sessions:(m?.backup_dirnames||[]).length, ...extra};
  };
  for(const s of (G.state?.slots||[])){
    if(!s.full || !s.volume_tag) continue;
    if(s.is_import_export) rows.push(mk(s.volume_tag, `Mail slot ${s.slot}`, 'mail', {slot:s.slot}));
    else rows.push(mk(s.volume_tag, `Slot ${s.slot} · M${s.magazine}-${s.slot_in_magazine}`, 'slot', {slot:s.slot, magazine:s.magazine, slot_in_magazine:s.slot_in_magazine}));
  }
  if(!drive.empty && drive.volume_tag && !seen.has(drive.volume_tag)) rows.push(mk(drive.volume_tag, `Drive · from slot ${loaded ?? '?'}`, 'drive', {slot:loaded}));
  for(const m of (G.indexMeta||[])){
    if(seen.has(m.volume_tag) || m.deleted) continue;
    if(m.purpose==='archived' || (!m.present && m.archived_at)) rows.push(mk(m.volume_tag, `Off-site · last slot ${m.last_seen_slot ?? '?'}`, 'offsite', {slot:m.last_seen_slot}));
  }
  return rows.sort((a,b) => {
    return (LOC_ORDER[a.where]-LOC_ORDER[b.where]) || ((a.slot||0)-(b.slot||0)) || a.vol.localeCompare(b.vol);
  });
}
const CAT_STATE = {
  indexed:['Indexed','ok'], noidx:['Not indexed','warn'], drive:['In drive','info'], archived:['Archived',''],
  blank:['Blank',''], cleaning:['Cleaning','cln'], recyclable:['Recyclable','bad'],
};
const isFormatCandidate = r => r.where==='slot' && !r.cln;

// ── Suggested labels ─────────────────────────────────────────────────────────
// Words naming a backup's schedule or kind rather than its content, dropped from job
// labels so "Weekly Photo Backup" contributes "Photo" to a tape label.
const LABEL_NOISE = /\b(?:hourly|daily|nightly|bi[- ]?weekly|weekly|fortnightly|monthly|quarterly|yearly|annual(?:ly)?|backups?|jobs?|full|incremental|differential)\b/gi;

// What one backup holds: its job label minus the noise words, or, for an ad-hoc backup
// or a label that was only noise ("Nightly backup"), the folders it backed up.
function backupContentNames(r){
  const name = recordStream(r).replace(/_+/g,' ').replace(LABEL_NOISE,' ').replace(/[()[\]{}]/g,' ')
    .replace(/\s+[-–—:|](?:\s+[-–—:|])*\s+/g,' - ').replace(/\s+/g,' ').replace(/^[\s\-–—:|,.&+/]+|[\s\-–—:|,.&+/]+$/g,'');
  if(name) return [name];
  return (r.paths||[]).map(p => String(p).replace(/\/+$/,'').split('/').pop()).filter(Boolean)
    .map(d => d.charAt(0).toUpperCase() + d.slice(1));
}

const isoDay = ts => { const d = new Date(ts*1000); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; };

// Text to write on a cartridge: what the completed backups still on it hold (including
// part of one spanning tapes) and the dates they span,
// e.g. "Photo & Appdata 2026-09-01 – 2026-10-20". Empty when it has no backup records.
function suggestedTapeLabel(vol){
  const recs = (G.records||[]).filter(r => r.status==='completed' && r.started_at && !r.overwritten
      && recordSegments(r).some(s => s.volume_tag===vol))
    .sort((a,b) => a.started_at - b.started_at);
  if(!recs.length) return '';
  const names = new Map();
  for(const r of recs) for(const n of backupContentNames(r)) if(!names.has(n.toLowerCase())) names.set(n.toLowerCase(), n);
  const n = [...names.values()];
  const what = n.length > 3 ? `${n.slice(0,3).join(', ')} +${n.length-3} more` : n.length > 1 ? `${n.slice(0,-1).join(', ')} & ${n[n.length-1]}` : n[0] || 'Backups';
  const first = isoDay(recs[0].started_at), last = isoDay(recs[recs.length-1].started_at);
  return `${what} ${first===last ? first : `${first} – ${last}`}`;
}

// Why a tape counts as full (see drive_history.tape_fill_status).
const fullReason = f => f.hit_eom ? 'ran out of tape on its last write' : `${fmtTB(f.free_bytes)} free, at or below the ${f.reserve_pct}% threshold`;

function labelCopyHTML(text){
  // Wrap between dates, never inside one; the copied text is unchanged.
  const shown = esc(text).replace(/\d{4}-\d{2}-\d{2}/g, '<span class="d">$&</span>');
  return `<span class="label-sug"><span class="t">${shown}</span><button type="button" class="btn xs" data-text="${esc(text)}" onclick="event.stopPropagation();copyText(this.dataset.text,this)">${ico('copy',13)}Copy</button></span>`;
}

function renderMediaPage(c){
  if(!G.state){ c.innerHTML='<div class="empty-state">Loading…</div>'; return; }
  const rows = catalogRows();
  c.insertAdjacentHTML('beforeend', `
    <div class="page-head">
      <div><h1>Tape catalog</h1><p class="sub">All catalogued cartridges: library slots, drive and off-site archive</p></div>
      <div class="actions"><button class="btn" onclick="startInventory('quick')">${ico('zap',15)}Quick scan</button></div>
    </div>`);
  c.insertAdjacentHTML('beforeend', `<div class="grid stretch g-3">${catalogStatePanel(rows)}${fillLevelPanel(rows)}${lifetimePanel()}</div>`);
  const full = rows.filter(r => r.full);
  if(full.length) c.insertAdjacentHTML('beforeend', archivePanelHTML(full));

  const tbl = el('section','panel');
  tbl.setAttribute('aria-label','Cartridges');
  tbl.id = 'media-table-panel';
  c.appendChild(tbl);
  renderMediaTable();

  c.appendChild(formatPanel(rows));
}

function catalogStatePanel(rows){
  const n = k => rows.filter(r => r.state===k || (k==='indexed' && (r.state==='drive' || r.state==='recyclable') && r.files>0)).length;
  const counts = {indexed:n('indexed'), noidx:rows.filter(r=>r.state==='noidx' || (r.state==='drive' && !r.files)).length, archived:n('archived'), blank:n('blank'), cleaning:n('cleaning')};
  const segs = [
    {k:'indexed', label:'Indexed', color:'var(--s1)'},
    {k:'noidx', label:'Not indexed', color:'var(--s2)'},
    {k:'archived', label:'Archived off-site', color:'var(--s3)'},
    {k:'blank', label:'Blank', color:'var(--s4)'},
    {k:'cleaning', label:'Cleaning', color:'var(--s5)'},
  ];
  return `<section class="panel col" aria-label="Catalog by state">
    ${panelHead('Catalog by state', `${plural(rows.length,'cartridge')}`)}
    <div class="panel-body"><div class="chart-row" style="gap:22px">
      ${donutChart(segs.map(s=>({v:counts[s.k], color:s.color, label:s.label})), {center:rows.length, sub:'cartridges', aria:segs.map(s=>`${counts[s.k]} ${s.label}`).join(', ')})}
      ${legendHTML(segs.map(s=>({color:s.color, label:s.label, value:counts[s.k]})))}
    </div></div>
  </section>`;
}

function fillLevelPanel(rows){
  const bands = [['0–25%',0,25],['25–50%',25,50],['50–75%',50,75],['75–90%',75,90],['90–100%',90,101]];
  const pcts = rows.filter(r => !r.cln && r.where!=='offsite' && r.meta && r.sp?.capacity_bytes).map(r => (r.sp.used_bytes||0)/r.sp.capacity_bytes*100);
  const counts = bands.map(([,lo,hi]) => pcts.filter(p => p>=lo && p<hi).length);
  const {max, ticks} = niceTicks(Math.max(...counts, 1), 2);
  const cols = bands.map((b,i) => ({segs:[{v:counts[i], color:'var(--s1)'}], x:b[0], cap:String(counts[i]), tip:{title:`${b[0]} used`, lines:[plural(counts[i],'cartridge')]}}));
  return `<section class="panel col" aria-label="Fill level">
    ${panelHead('Fill level', `${plural(pcts.length,'data cartridge')} in the library by % used`)}
    <div class="panel-body" style="padding:18px 20px 14px">
      ${pcts.length ? columnChart(cols, {max, ticks, height:150, fmtTick:v=>v, aria:'Cartridges by fill level'}) : '<div class="chart-empty">No capacity data</div>'}
    </div>
  </section>`;
}

function lifetimePanel(){
  const rows = Object.entries(G.tapeHistoryMap||{})
    .filter(([v,h]) => !is_cleaning_vol(v) && (h.total_backup_bytes||0) > 0)
    .sort((a,b)=>(b[1].total_backup_bytes||0)-(a[1].total_backup_bytes||0)).slice(0,6)
    .map(([v,h]) => ({label:v, v:toTB(h.total_backup_bytes), text:`${hBytes(h.total_backup_bytes)} · ${plural(h.load_count||0,'load')}`, mono:true}));
  return `<section class="panel col" aria-label="Lifetime data written">
    ${panelHead('Lifetime data written', 'Top 6 cartridges · drive history')}
    <div class="panel-body">${rows.length ? hbarChart(rows, {labelWidth:84, maxWidth:62}) : '<div class="chart-empty">No drive history yet</div>'}</div>
  </section>`;
}

// Full cartridges still in the library, with what to write on each before pulling it.
function archivePanelHTML(rows){
  const pct = rows[0].meta.fill.reserve_pct;
  return `<section class="panel" aria-label="Ready to archive">
    ${panelHead('Ready to archive', `${plural(rows.length,'full cartridge')} · ${pct}% free or less, or out of tape · backups no longer select ${rows.length===1?'it':'them'}`,
      `<button class="btn sm" onclick="showPage('retention')">${ico('sliders',14)}Full threshold</button>`)}
    <div class="tbl-wrap"><table class="tbl" data-sort-key="archive">
      <thead><tr><th data-sort="text">Volume</th><th data-sort="text">Location</th><th class="num" data-sort="num">Free</th><th data-sort="text">Suggested label</th><th style="width:40px"></th></tr></thead>
      <tbody>${rows.map(r => {
        const label = suggestedTapeLabel(r.vol);
        return `<tr class="clickable" onclick="openCatalogVol('${jsq(r.vol)}')">
          <td class="vol">${esc(r.vol)}</td>
          <td data-v="${locSortKey(r)}">${esc(r.loc)}</td>
          <td class="num" data-v="${r.meta.fill.free_bytes}">${fmtTB(r.meta.fill.free_bytes)}<div class="sub">of ${fmtTB(r.meta.fill.capacity_bytes)}</div></td>
          <td data-v="${esc(label)}">${label ? labelCopyHTML(label) : '<span class="text-muted">No backup records</span>'}</td>
          <td>${ico('chevR',16,'style="color:var(--muted)"')}</td>
        </tr>`;
      }).join('')}</tbody>
    </table></div>
  </section>`;
}

function renderMediaTable(){
  const p = $('media-table-panel'); if(!p) return;
  const all = catalogRows();
  const tabs = [
    ['all','All', all.length],
    ['library','In library', all.filter(r=>r.where!=='offsite').length],
    ['full','Full', all.filter(r=>r.full).length],
    ['archived','Archived', all.filter(r=>r.where==='offsite').length],
    ['cleaning','Cleaning', all.filter(r=>r.cln).length],
  ];
  const f = G.mediaFilter.trim().toLowerCase();
  const rows = all.filter(r =>
    (G.mediaTab==='all' || (G.mediaTab==='library' && r.where!=='offsite') || (G.mediaTab==='full' && r.full) || (G.mediaTab==='archived' && r.where==='offsite') || (G.mediaTab==='cleaning' && r.cln))
    && (!f || r.vol.toLowerCase().includes(f)));
  // Drop selections for tapes that are no longer erase candidates.
  const cand = new Set(all.filter(isFormatCandidate).map(r=>r.vol));
  for(const t of [...G.fmtSelected]) if(!cand.has(t)) G.fmtSelected.delete(t);
  const nSel = G.fmtSelected.size;
  const visibleCand = rows.filter(isFormatCandidate);
  const allChecked = visibleCand.length && visibleCand.every(r=>G.fmtSelected.has(r.vol));

  p.innerHTML = `
    <div class="panel-head" style="padding:0 20px">
      <div class="tabs" role="tablist" aria-label="Filter">${tabs.map(([k,l,n]) => `<button class="tab${G.mediaTab===k?' on':''}" role="tab" aria-selected="${G.mediaTab===k}" onclick="G.mediaTab='${k}';renderMediaTable()">${l}<span class="n">${n}</span></button>`).join('')}</div>
      <label class="search" style="max-width:280px;height:34px">${ico('search',15)}<input id="media-filter" type="search" placeholder="Filter by volume tag" aria-label="Filter by volume tag" value="${esc(G.mediaFilter)}" oninput="mediaFilterInput(this.value)"/></label>
    </div>
    ${nSel ? `<div class="select-bar" style="border-radius:0;border-left:none;border-right:none;border-top:none;padding:10px 20px">
      <span class="n">${plural(nSel,'cartridge')} selected for erase</span>
      <div class="btn-row"><button class="btn sm" onclick="fmtSelectNone()">Clear</button><button class="btn sm danger" onclick="$('erase-panel').scrollIntoView({behavior:'smooth'})">Erase selected…</button></div></div>` : ''}
    <div class="tbl-wrap"><table class="tbl" data-sort-key="media">
      <thead><tr>
        <th style="width:44px"><input type="checkbox" aria-label="Select all erasable" ${allChecked?'checked':''} ${visibleCand.length?'':'disabled'} onchange="fmtToggleAll(this.checked)"/></th>
        <th data-sort="text">Volume</th><th data-sort="text">Location</th><th data-sort="text">Status</th><th data-sort="num">Usage</th><th class="num" data-sort="num">Files</th><th class="num" data-sort="num">Sessions</th><th data-sort="num">Last backup</th><th style="width:40px"></th>
      </tr></thead>
      <tbody>${rows.length ? rows.map(mediaRowHTML).join('') : `<tr><td colspan="9"><div class="empty-state">No cartridges</div></td></tr>`}</tbody>
    </table></div>`;
  p.querySelectorAll('tr[data-vol]').forEach(tr => {
    tr.onclick = e => {
      if(e.target.closest('input')) return;
      const r = all.find(x=>x.vol===tr.dataset.vol); if(r) openCatalogRow(r);
    };
  });
}

function mediaFilterInput(v){
  G.mediaFilter = v;
  renderMediaTable();
  const n = $('media-filter');
  if(n){ n.focus(); n.setSelectionRange(v.length, v.length); }
}

function mediaRowHTML(r){
  const [label, tone] = CAT_STATE[r.state];
  const cap = r.sp?.capacity_bytes;
  const used = r.sp?.used_bytes || 0;
  const pct = cap ? Math.max(0, Math.min(100, used/cap*100)) : 0;
  const known = !!r.meta && cap && !r.cln;
  const last = G.tapeHistoryMap[r.vol]?.last_backup;
  const cand = isFormatCandidate(r);
  const sel = G.fmtSelected.has(r.vol);
  const writing = r.where==='drive' && G.state?.backup_job?.running;
  return `<tr class="clickable${sel?' sel':''}" data-vol="${esc(r.vol)}">
    <td><input type="checkbox" aria-label="Select ${esc(r.vol)}" ${sel?'checked':''} ${cand?'':'disabled'} onchange="fmtToggle('${jsq(r.vol)}',this.checked)"/></td>
    <td class="vol">${esc(r.vol)}${linkedTapesMark(r)}</td>
    <td data-v="${locSortKey(r)}">${esc(r.loc)}</td>
    <td data-v="${esc(label)}${r.full?' full':''}"><div class="btn-row" style="gap:6px">${badge(label, tone)}${r.full ? `<span title="${esc(fullReason(r.meta.fill))} · backups no longer select it">${badge('Full','warn',true)}</span>` : ''}</div></td>
    <td data-v="${known ? pct.toFixed(2) : ''}"><div class="tbl-usage">${known ? `<span class="meter"><span style="width:${pct.toFixed(0)}%;background:${meterColor(pct)}"></span></span><span class="v">${toTB(used).toFixed(2)} / ${toTB(cap).toFixed(2)} TB</span>` : '<span class="text-muted">—</span>'}</div></td>
    <td class="num" data-v="${r.files || ''}">${r.files ? r.files.toLocaleString() : '—'}</td>
    <td class="num">${r.sessions || '—'}</td>
    <td data-v="${writing ? Date.now()/1000 : last || ''}">${writing ? 'Writing now' : last ? esc(fmtDate(last)) : '—'}</td>
    <td>${ico('chevR',16,'style="color:var(--muted)"')}</td>
  </tr>`;
}

// Marker for a tape holding part of a backup that continues on other tapes.
function linkedTapesMark(r){
  const others = new Set();
  for(const sess of (r.meta?.sessions||[])){
    if((sess.parts||1) < 2) continue;
    for(const c of (sess.chain||[])) if(c.volume_tag !== r.vol) others.add(c.volume_tag);
  }
  if(!others.size) return '';
  const t = `Holds part of a backup that spans tapes · linked with ${[...others].join(', ')}`;
  return ` <span title="${esc(t)}" aria-label="${esc(t)}" style="color:var(--accent-ink);vertical-align:-2px">${ico('link',13)}</span>`;
}

function openCatalogVol(vol){
  const r = catalogRows().find(x => x.vol===vol);
  if(r) openCatalogRow(r);
}

function openCatalogRow(r){
  if(r.where==='drive'){
    openTapeDrawer({slot:getUnloadTargetSlot(), loaded_from_slot:getEffectiveLoadedSlot(), volume_tag:r.vol, in_drive:true, full:true, has_index:r.files>0});
  } else {
    openTapeDrawer({slot:r.slot||null, volume_tag:r.vol, full:r.where!=='offsite', in_drive:false,
      is_import_export:r.where==='mail', magazine:r.magazine, slot_in_magazine:r.slot_in_magazine,
      has_index:r.files>0, space:r.sp||null, is_archived:r.where==='offsite', is_cleaning:r.cln});
  }
}

function fmtToggle(vol, on){ on ? G.fmtSelected.add(vol) : G.fmtSelected.delete(vol); renderMediaTable(); renderFormatPanel(); }
function fmtToggleAll(on){
  const f = G.mediaFilter.trim().toLowerCase();
  for(const r of catalogRows().filter(isFormatCandidate)){
    if(f && !r.vol.toLowerCase().includes(f)) continue;
    on ? G.fmtSelected.add(r.vol) : G.fmtSelected.delete(r.vol);
  }
  renderMediaTable(); renderFormatPanel();
}
function fmtSelectAll(){ fmtToggleAll(true); }
function fmtSelectNone(){ G.fmtSelected.clear(); renderMediaTable(); renderFormatPanel(); }

// ── Erase / format ───────────────────────────────────────────────────────────
function formatPanel(){
  const p = el('section','panel');
  p.id = 'erase-panel';
  p.setAttribute('aria-label','Erase tapes');
  setTimeout(renderFormatPanel, 0);
  return p;
}

function fmtSelectedTapes(){
  const bySlot = Object.fromEntries(catalogRows().filter(isFormatCandidate).map(r=>[r.vol, r.slot]));
  return [...G.fmtSelected].filter(v => bySlot[v]).map(v => ({slot:bySlot[v], volume_tag:v})).sort((a,b)=>a.slot-b.slot);
}
function fmtPhrase(sel){ return `FORMAT ${sel.length === 1 ? sel[0].volume_tag : `${sel.length} TAPES`}`; }

function renderFormatPanel(){
  const p = $('erase-panel'); if(!p) return;
  const fj = G.state?.format_job || {};
  const sel = fmtSelectedTapes();
  const recyclable = new Set(G.gfs?.recyclable || []);
  const phrase = fmtPhrase(sel);
  const catOnly = G.fmtCatalogOnly;
  const allRecyclable = sel.length && sel.every(t => recyclable.has(t.volume_tag));
  const canStart = sel.length && !fj.running && (catOnly || G.fmtConfirm.trim() === phrase);
  const statusLabel = {idle:'Idle', running:'Running', completed:'Completed', completed_with_errors:'Completed with errors', failed:'Failed', stopped:'Stopped'}[fj.status] || fj.status || 'Idle';

  const job = fj.status && fj.status !== 'idle' ? `
    <div class="stack" style="gap:12px">
      <div style="display:flex;justify-content:space-between;align-items:baseline"><span class="section-label">${fj.running?'Current erase job':'Last erase job'}</span><span class="text-sm text-muted">${esc(fj.last_message||'')}</span></div>
      <div>${badge(statusLabel, statusTone(fj.status, fj.running))}</div>
      ${fj.running ? `<div class="prog-wrap thin"><span class="prog-bar amber pulse"></span></div>` : ''}
      <div class="kv">
        ${fj.current ? `<span>Current</span><span><span class="mono">${esc(fj.current.volume_tag||'')}</span> · slot ${fj.current.slot}</span>` : ''}
        ${(fj.done||[]).length ? `<span style="color:var(--ok-text);font-weight:600">Done</span><span class="mono">${(fj.done||[]).map(t=>esc(t.volume_tag)).join(', ')}</span>` : ''}
        ${(fj.failed||[]).length ? `<span style="color:var(--bad);font-weight:600">Failed</span><span>${(fj.failed||[]).map(t=>`<span class="mono">${esc(t.volume_tag)}</span> — ${esc(t.error)}`).join('; ')}</span>` : ''}
      </div>
      <div class="log-list boxed">${logRows(fj.log||[], {limit:30, boxed:true})}</div>
      ${fj.running ? `<div><button class="btn danger sm" onclick="stopFormat()">${ico('stop',14)}Stop after current tape</button></div>` : ''}
    </div>` : `<div class="chart-empty">No erase jobs this session</div>`;

  p.innerHTML = `
    ${panelHead('Erase tapes','Short erase · new tape header, catalog cleared', badge(statusLabel, statusTone(fj.status, fj.running)))}
    <div class="grid g-split" style="gap:0">
      <div class="panel-body" style="border-right:1px solid var(--line-2)">
        <div class="field"><span class="section-label">Selected for erase</span>
          ${sel.length ? `<div class="chips">${sel.map(t => `<span class="chip neutral"><span><b style="font-weight:600">${esc(t.volume_tag)}</b> <span class="text-muted">slot ${t.slot}</span></span><button class="chip-x" aria-label="Remove ${esc(t.volume_tag)}" onclick="fmtToggle('${jsq(t.volume_tag)}',false)">${ico('x',13)}</button></span>`).join('')}</div>`
            : '<span class="text-sm text-muted">None — select full data cartridges in the table above</span>'}
          ${allRecyclable ? '<span class="text-sm text-muted">All past GFS retention</span>' : ''}
        </div>
        <label class="check-card${catOnly?' selected':''}">
          <input type="checkbox" id="fmt-catalog-only" ${catOnly?'checked':''} onchange="G.fmtCatalogOnly=this.checked;renderFormatPanel()"/>
          <span><span class="t">Catalog-only reset</span><span class="d" style="display:block">Database only: tapes marked blank, indexes removed</span></span>
        </label>
        ${catOnly ? '' : `<div class="callout bad">${ico('alert',18)}
          <div class="stack" style="gap:10px;flex:1">
            <span><strong style="color:var(--bad-ink)">Data on the selected tapes becomes unrecoverable.</strong> Confirmation phrase required.</span>
            <label class="field" style="font-weight:400">Confirmation phrase <span class="mono" style="font-weight:600;color:var(--ink)">${esc(sel.length?phrase:'FORMAT …')}</span>
              <input id="fmt-confirm" class="mono" placeholder="${esc(sel.length?phrase:'')}" value="${esc(G.fmtConfirm)}" oninput="G.fmtConfirm=this.value;fmtUpdateCount()" ${sel.length?'':'disabled'}/>
            </label>
          </div></div>`}
        <div class="btn-row">
          <button class="btn ${catOnly?'primary':'danger-solid'}" id="btn-format-start" onclick="startFormat()" ${canStart?'':'disabled'}>
            ${ico(catOnly?'list':'trash',15)}${catOnly ? `Reset catalog for ${plural(sel.length,'tape')}` : `Erase ${plural(sel.length,'tape')}`}
          </button>
          <span class="result" id="fmt-result"></span>
        </div>
      </div>
      <div class="panel-body">${job}</div>
    </div>`;
}

function fmtUpdateCount(){
  const sel = fmtSelectedTapes();
  const btn = $('btn-format-start');
  if(btn) btn.disabled = !sel.length || !!G.state?.format_job?.running || (!G.fmtCatalogOnly && G.fmtConfirm.trim() !== fmtPhrase(sel));
}

async function stopFormat(){
  const data = await api('/api/format/stop','POST');
  if(!data.ok) alert(data.error||'Could not stop format.');
  await pollOnce();
}

async function startFormat(){
  const selected = fmtSelectedTapes();
  if(!selected.length) return;
  const catalogOnly = !!G.fmtCatalogOnly;
  if(catalogOnly){
    const names = selected.map(t=>`  • ${t.volume_tag} (slot ${t.slot})`).join('\n');
    if(!confirm(`Reset catalog for ${plural(selected.length,'tape')}?\n\n${names}\n\nTapes are marked blank in the database; file indexes and backup records are removed.`)) return;
  } else if(G.fmtConfirm.trim() !== fmtPhrase(selected)){
    setTxt('fmt-result', 'Confirmation phrase does not match');
    return;
  }
  const data = await api('/api/format/start','POST',{tapes: selected, catalog_only: catalogOnly});
  if(!data.ok){ const r=$('fmt-result'); if(r){ r.className='result bad'; r.textContent=data.error||'Format failed to start.'; } return; }
  G.fmtSelected.clear();
  G.fmtConfirm = '';
  await pollOnce();
  renderPage();
}
