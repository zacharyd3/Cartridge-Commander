'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// OVERVIEW (library) PAGE
// ══════════════════════════════════════════════════════════════════════════════
function renderLibraryPage(c){
  if(!G.state){ c.innerHTML='<div class="empty-state">Loading…</div>'; return; }
  const s = G.state.summary||{};
  const idxMap = Object.fromEntries((G.indexMeta||[]).map(x => [x.volume_tag, x]));

  c.insertAdjacentHTML('beforeend', `
    <div class="page-head">
      <div><h1>Library overview</h1>
        <p class="sub">${esc(librarySubtitle(s))}</p></div>
      <div class="actions">
        <button class="btn" onclick="doRefresh()">${ico('refresh',15)}Refresh</button>
        <button class="btn primary" onclick="showPage('backup')">${ico('plus',15)}New backup</button>
      </div>
    </div>`);

  const banner = overviewBanner();
  if(banner) c.insertAdjacentHTML('beforeend', banner);
  c.insertAdjacentHTML('beforeend', overviewKpis(s, idxMap));
  c.insertAdjacentHTML('beforeend', `<div class="grid stretch g-charts">${storagePanel()}${slotUsagePanel()}${outcomesPanel()}</div>`);
  c.insertAdjacentHTML('beforeend', writtenPanel());

  const row = el('div','grid g-drive');
  row.appendChild(drivePanel(idxMap));
  const right = el('div','stack');
  right.insertAdjacentHTML('beforeend', inventoryPanel());
  right.insertAdjacentHTML('beforeend', recentPanel());
  row.appendChild(right);
  c.appendChild(row);

  c.appendChild(slotMapPanel(idxMap));
}

function librarySubtitle(s){
  const mags = s.magazines?.length || 0;
  const parts = [];
  if(s.total_slots != null) parts.push(`${s.total_slots} slots${mags?` in ${plural(mags,'magazine')}`:''}`);
  const mail = (G.state?.slots||[]).some(x=>x.is_import_export);
  if(mail) parts.push('1 mail slot');
  if(s.density) parts.push(s.density);
  return parts.join(' · ') || 'Tape library';
}

// ── Active operation banner ─────────────────────────────────────────────────
function overviewBanner(){
  const bk = G.state?.backup_job || {};
  const activePhases = ['scanning','selecting_tape','loading_tape','pre_hook','erasing','streaming','indexing','verifying','rewinding','unloading','post_hook','cancelling','preparing'];
  const showBk = !G.activeAction && bk.running && activePhases.includes(bk.status||'');
  if(G.activeAction){
    const a = G.activeAction;
    const cls = a.status==='success' ? 'success' : a.status==='error' ? 'error' : 'working';
    const icon = a.status==='success' ? ico('check',20) : a.status==='error' ? ico('alert',20) : ico('refresh',20,'class="ico spin"');
    const elapsed = a.status==='working' && a.started ? `<span class="banner-elapsed" id="action-elapsed">${fmtSec((Date.now()-a.started)/1000)} elapsed</span>` : '';
    return `<section class="action-banner ${cls}" aria-live="polite"><div class="banner-icon">${icon}</div>
      <div class="banner-body"><span class="banner-title">${esc(a.msg)}</span></div>${elapsed}</section>`;
  }
  if(!showBk) return '';
  const bw = bk.bytes_written||0, bt = bk.bytes_total||0;
  const pct = bt > 0 ? Math.min(bw/bt*100,100) : Number(bk.percent||0);
  const streaming = bk.status === 'streaming';
  const vol = G.state?.summary?.loaded_volume;
  const detail = streaming && bw > 0
    ? [vol ? `<span class="mono">${esc(vol)}</span>` : '', `${hBytes(bw)}${bt>0?' of '+hBytes(bt):''}`, `${hBytes(bk.speed_bps||0)}/s`, bk.eta_seconds!=null ? `ETA ${fmtSec(bk.eta_seconds)}` : ''].filter(Boolean).join(' · ')
    : esc(bk.last_message || '');
  return `<section class="action-banner running" aria-label="Active operation">
    <div class="banner-icon">${ico('upload',20)}</div>
    <div class="banner-body">
      <div style="display:flex;align-items:baseline;gap:10px;flex-wrap:wrap"><span class="banner-title">Backup in progress — ${esc((BACKUP_PHASE_LABEL[bk.status]||bk.status||'').toLowerCase())}</span><span class="banner-detail">${detail}</span></div>
      <div class="prog-wrap thin"><span class="prog-bar ${streaming?'amber':'amber pulse'}" style="width:${streaming?pct.toFixed(1):100}%"></span></div>
    </div>
    ${streaming?`<span class="banner-pct">${pct.toFixed(0)}%</span>`:''}
    <button class="btn" onclick="showPage('backup')">View job</button>
  </section>`;
}

// ── KPI tiles ────────────────────────────────────────────────────────────────
function overviewKpis(s, idxMap){
  const slots = (G.state.slots||[]).filter(x=>!x.is_import_export);
  const full = s.full_slots ?? slots.filter(x=>x.full).length;
  const total = s.total_slots ?? slots.length;
  const mail = (G.state.slots||[]).find(x=>x.is_import_export);
  const inLib = new Set(slots.filter(x=>x.full && x.volume_tag).map(x=>x.volume_tag));
  if(!G.state.drive?.empty && G.state.drive?.volume_tag) inLib.add(G.state.drive.volume_tag);
  const indexed = [...inLib].filter(v => !is_cleaning_vol(v) && (idxMap[v]?.file_count||0) > 0).length;
  const notIdx = [...inLib].filter(v => !is_cleaning_vol(v) && !((idxMap[v]?.file_count||0) > 0)).length;
  const archived = (G.indexMeta||[]).filter(m => (m.purpose==='archived' || (!m.present && m.archived_at)) && !m.is_cleaning && !m.deleted).length;

  const last = (G.records||[]).find(r => r.status==='completed');
  const lastHtml = last
    ? `<span class="kpi-value">${esc(fmtWhen(last.finished_at || last.started_at))}</span>
       <span class="kpi-sub ${last.verified===false?'bad':last.verified?'ok':''}">${last.verified===true?ico('check',13):last.verified===false?ico('alert',13):''}${esc(streamName(recordStream(last)))}${last.verified===true?' · verified':last.verified===false?' · verify failed':''}</span>`
    : `<span class="kpi-value">—</span><span class="kpi-sub">${G.records ? 'No completed backups' : 'Loading…'}</span>`;

  const next = (G.schedules||[]).filter(x=>x.enabled && x.next_run).sort((a,b)=>a.next_run-b.next_run)[0];
  const nextHtml = next
    ? `<span class="kpi-value">${esc(new Date(next.next_run*1000).toLocaleDateString(undefined,{weekday:'short'}) + ' ' + fmtTime(next.next_run))}</span>
       <span class="kpi-sub">${esc(next.label||'Backup')} · ${fmtNext(next.next_run)}</span>`
    : `<span class="kpi-value">—</span><span class="kpi-sub">${G.schedules ? 'No enabled schedules' : 'Loading…'}</span>`;

  return `<div class="kpis">
    <div class="kpi"><span class="kpi-label">Library</span>
      <span class="kpi-value ${s.online?'ok':'bad'}"><span class="dot ${s.online?'green':'red'}" style="width:10px;height:10px"></span>${s.online?'Online':'Offline'}</span>
      <span class="kpi-sub">${s.online?'Drive reachable':'Drive unreachable'}${s.density?' · '+esc(s.density):''}</span></div>
    <div class="kpi"><span class="kpi-label">Slots occupied</span>
      <span class="kpi-value">${full}<span class="of">/ ${total}</span></span>
      <span class="kpi-sub">${s.empty_slots ?? (total-full)} empty${mail?` · mail slot ${mail.full?'occupied':'empty'}`:''}</span></div>
    <div class="kpi"><span class="kpi-label">Indexed tapes</span>
      <span class="kpi-value">${indexed}</span>
      <span class="kpi-sub">${notIdx} not indexed · ${archived} archived off-site</span></div>
    <div class="kpi"><span class="kpi-label">Last completed backup</span>${lastHtml}</div>
    <div class="kpi"><span class="kpi-label">Next scheduled run</span>${nextHtml}</div>
  </div>`;
}

// ── Charts ───────────────────────────────────────────────────────────────────
function storagePanel(){
  const st = computeStorage();
  const t = st.tot;
  const pct = v => t.cap ? `${Math.round(v/t.cap*100)}%` : '—';
  const usedPct = t.cap ? Math.round(t.used/t.cap*100) : 0;
  const ratio = st.gen >= 6 ? 2.5 : st.gen ? 2 : null;
  const capLine = [
    plural(t.count, 'data cartridge'),
    st.gen ? `LTO-${st.gen}` : '',
    ratio ? `up to ${toTB(t.cap*ratio).toFixed(1)} TB at ${ratio}:1 compression` : '',
  ].filter(Boolean).join(' · ');
  const mags = st.mags.map(m => {
    const w = v => m.cap ? (v/m.cap*100).toFixed(1)+'%' : '0%';
    return `<div class="mag-bar"><span><span class="n">Magazine ${m.magazine}</span><span class="s">${plural(m.count,'cartridge')} · ${fmtTB(m.used)} used</span></span>
      <span class="stackbar" role="img" aria-label="Magazine ${m.magazine}: ${fmtTB(m.used)} used, ${fmtTB(m.free)} free, ${fmtTB(m.unk)} not indexed">
        ${m.used?`<span style="width:${w(m.used)};background:var(--s1)"></span>`:''}${m.free?`<span style="width:${w(m.free)};background:var(--r150)"></span>`:''}${m.unk?`<span style="width:${w(m.unk)};background:var(--neutral)"></span>`:''}
      </span><span class="t">${toTB(m.cap).toFixed(1)} TB</span></div>`;
  }).join('');
  return `<section class="panel col" aria-label="Total tape storage">
    ${panelHead('Total tape storage','Native capacity of the data cartridges in the magazines',`<button class="link-btn" onclick="showPage('media')">Per-tape detail</button>`)}
    <div class="panel-body" style="gap:20px">
      <div class="chart-row">
        ${donutChart([
          {v:t.used, color:'var(--s1)', label:'Used', text:fmtTB(t.used)},
          {v:t.free, color:'var(--r150)', label:'Free', text:fmtTB(t.free)},
          {v:t.unk, color:'var(--neutral)', label:'Not indexed', text:fmtTB(t.unk)},
        ], {size:168, stroke:20, center:t.cap?`${usedPct}%`:'—', sub:'used', aria:`${fmtTB(t.used)} used, ${fmtTB(t.free)} free, ${fmtTB(t.unk)} not indexed`})}
        <div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:14px">
          <div><div class="hero">${toTB(t.cap).toFixed(1)} TB</div>
            <div class="text-muted" style="font-size:12.5px;margin-top:8px">${esc(capLine)}</div></div>
          ${legendHTML([
            {color:'var(--s1)', label:'Used', value:fmtTB(t.used), pct:pct(t.used)},
            {color:'var(--r150)', label:'Free', value:fmtTB(t.free), pct:pct(t.free)},
            {color:'var(--neutral)', label:'Not indexed', value:fmtTB(t.unk), pct:pct(t.unk)},
          ], {pct:true})}
        </div>
      </div>
      ${mags ? `<div class="stack divider-top" style="gap:12px">${mags}
        ${st.archivedCount?`<span class="text-sm text-muted">Off-site archive: ${fmtTB(st.archivedCap)} on ${plural(st.archivedCount,'cartridge')}</span>`:''}</div>` : ''}
    </div>
  </section>`;
}

function slotUsagePanel(){
  const slots = (G.state.slots||[]).filter(x=>!x.is_import_export);
  const cln = slots.filter(x=>x.full && is_cleaning_vol(x.volume_tag)).length;
  const data = slots.filter(x=>x.full && !is_cleaning_vol(x.volume_tag)).length;
  const empty = slots.length - cln - data;
  return `<section class="panel col" aria-label="Slot usage">
    ${panelHead('Slot usage', `${slots.length} storage slots${G.state.summary?.magazines?.length?` across ${plural(G.state.summary.magazines.length,'magazine')}`:''}`)}
    <div class="panel-body center-col">
      ${donutChart([
        {v:data, color:'var(--s1)', label:'Data cartridges'},
        {v:cln, color:'var(--cln)', label:'Cleaning'},
        {v:empty, color:'var(--neutral-2)', label:'Empty'},
      ], {size:168, stroke:20, center:`${data+cln}<span class="of"> / ${slots.length}</span>`, sub:'occupied', aria:`${data} data, ${cln} cleaning, ${empty} empty`})}
      ${legendHTML([
        {color:'var(--s1)', label:'Data cartridges', value:data},
        {color:'var(--cln)', label:'Cleaning', value:cln},
        {color:'var(--neutral-2)', label:'Empty', value:empty},
      ])}
    </div>
  </section>`;
}

function outcomesPanel(){
  const o = outcomeCounts(30);
  const finished = o.completed + o.failed + o.cancelled;
  const total = finished + o.running;
  const rate = finished ? Math.round(o.completed / finished * 100) : null;
  return `<section class="panel col" aria-label="Backups in the last 30 days">
    ${panelHead('Backups · 30 days', G.records ? `${plural(total,'job')} from backup records` : 'Loading records…')}
    <div class="panel-body center-col">
      ${donutChart([
        {v:o.completed, color:'var(--ok)', label:'Completed'},
        {v:o.failed, color:'var(--bad)', label:'Failed'},
        {v:o.cancelled, color:'var(--faint)', label:'Cancelled'},
        {v:o.running, color:'var(--warn)', label:'Running'},
      ], {size:168, stroke:20, center:rate==null?'—':`${rate}%`, sub:'succeeded', aria:`${o.completed} completed, ${o.failed} failed, ${o.cancelled} cancelled, ${o.running} running`})}
      ${legendHTML([
        {icon:`<span style="color:var(--ok);display:flex">${ico('check',14)}</span>`, label:'Completed', value:o.completed},
        {icon:`<span style="color:var(--bad);display:flex">${ico('x',14)}</span>`, label:'Failed', value:o.failed},
        {icon:`<span style="color:var(--faint);display:flex">${ico('minus',14)}</span>`, label:'Cancelled', value:o.cancelled},
        {icon:`<span style="color:var(--warn);display:flex">${ico('circle',14)}</span>`, label:'Running', value:o.running},
      ])}
    </div>
  </section>`;
}

function writtenPanel(){
  const days = writtenPerDay(14);
  const total = days.reduce((a,d)=>a+d.total,0);
  const maxTB = Math.max(...days.map(d=>toTB(d.total)), 0);
  const {max, ticks} = niceTicks(maxTB || 1, 3);
  const peak = days.reduce((m,d,i)=> d.total > (days[m]?.total||0) ? i : m, 0);
  const cols = days.map((d,i) => {
    const lines = Object.entries(d.streams).sort((a,b)=>b[1]-a[1]).map(([k,v]) => `${esc(k)} ${toTB(v).toFixed(2)} TB`);
    return {
      segs:[{v:toTB(d.total), color:'var(--s1)'}],
      x: d.date.getDate(),
      cap: i===peak && d.total>0 ? `${toTB(d.total).toFixed(2)} TB` : '',
      tip: {title:`${d.date.toLocaleDateString(undefined,{weekday:'short',day:'numeric',month:'short'})} · ${toTB(d.total).toFixed(2)} TB`, lines: lines.length ? lines : ['No backups']},
      aria:`${d.date.toDateString()}: ${toTB(d.total).toFixed(2)} TB`,
    };
  });
  return `<section class="panel" aria-label="Data written, last 14 days">
    ${panelHead('Data written · last 14 days', G.records ? `${toTB(total).toFixed(2)} TB total · ${toTB(total/14).toFixed(2)} TB/day average` : 'Loading records…', `<button class="btn sm" onclick="showPage('log')">Table view</button>`)}
    <div class="panel-body" style="padding:22px 24px 16px">
      ${columnChart(cols, {max, ticks, fmtTick:v=>v?`${v} TB`:'0', height:200, aria:'Data written per day, last 14 days'})}
    </div>
  </section>`;
}

// ── Drive panel ──────────────────────────────────────────────────────────────
function drivePanel(idxMap){
  const di = G.driveInfo || {};
  const hist = di.history || {};
  const idx = di.index || null;
  const card = el('section','panel');
  card.setAttribute('aria-label','Drive');
  const badges = di.empty ? badge('Empty','') : `${badge(di.online?'Online':'Offline', di.online?'ok':'bad', true)} ${badge(di.at_bot?'At BOT':'Mid-tape','')}`;
  card.innerHTML = panelHead(`Drive`, esc(di.density || G.state?.summary?.density || ''), `<div class="btn-row">${badges}</div>`);
  const body = el('div','panel-body');
  card.appendChild(body);

  if(di.empty){
    body.innerHTML = `<div style="font-size:22px;font-weight:600;color:var(--muted)">No tape loaded</div>
      <div class="text-muted">Drive is empty. Cartridges load from the slot map.</div>
      <div class="btn-row"><button class="btn" id="btn-check-drive">${ico('search',15)}Check drive</button></div>`;
    body.querySelector('#btn-check-drive').onclick = async e => { e.currentTarget.disabled=true; await checkDriveNow(); };
    return card;
  }

  const slot = di.effective_loaded_slot ?? di.loaded_from_slot ?? G.state?.summary?.loaded_slot;
  const fromSlot = (G.state?.slots||[]).find(x=>x.slot===slot);
  const sp = di.space || G.driveInfo?.space;
  let capHtml = '';
  if(sp?.capacity_bytes){
    const used = sp.used_bytes||0, cap = sp.capacity_bytes;
    const free = sp.remaining_bytes ?? Math.max(0, cap-used);
    const p = Math.max(0, Math.min(100, used/cap*100));
    capHtml = `<div style="width:320px;max-width:100%;display:flex;flex-direction:column;gap:6px">
      <div class="meter-labels top"><span>Capacity ${hBytes(cap)}</span><span class="mono">${p.toFixed(1)}% used${sp.estimated?' (est.)':''}</span></div>
      <div class="meter"><span style="width:${p.toFixed(1)}%;background:${meterColor(p)}"></span></div>
      <div class="meter-labels"><span>${hBytes(used)} used</span><span>${hBytes(free)} free</span></div></div>`;
  }
  const sinceLoad = di.time_in_drive_seconds;
  const cell = (l,v,cls='') => `<div class="cell"><div class="cell-label">${l}</div><div class="cell-value ${cls}">${v}</div></div>`;
  body.innerHTML = `
    <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:20px;flex-wrap:wrap">
      <div><div class="mono" style="font-size:28px;font-weight:600;letter-spacing:-.01em">${esc(di.volume_tag||'Unknown')}</div>
        <div class="text-muted" style="font-size:12.5px;margin-top:2px">Loaded from slot ${slot ?? '—'}${fromSlot?.magazine?` · Magazine ${fromSlot.magazine}`:''} · unload target slot ${getUnloadTargetSlot() ?? '—'}</div></div>
      ${capHtml}
    </div>
    <div class="cells c4">
      ${cell('Time in drive', sinceLoad!=null?fmtSec(sinceLoad):'—', sinceLoad>3600*8?'c-amber':'')}
      ${cell('Loaded at', di.loaded_at?fmtTime(di.loaded_at):'—')}
      ${cell('Load count', `<span class="mono">${hist.load_count??'—'}</span>`)}
      ${cell('Backup count', `<span class="mono">${hist.backup_count??'—'}</span>`, hist.backup_count?'c-green':'')}
      ${cell('Last backup', hist.last_backup?fmtWhen(hist.last_backup):'Never')}
      ${cell('Last restore', hist.last_restore?fmtWhen(hist.last_restore):'Never')}
      ${cell('Data written', `<span class="mono">${hist.total_backup_bytes?hBytes(hist.total_backup_bytes):'—'}</span>`, hist.total_backup_bytes?'c-blue':'')}
      ${cell('First loaded', hist.first_loaded?fmtDate(hist.first_loaded):'—')}
    </div>
    <div class="strip${idx?'':' neutral'}" id="drive-index-strip"></div>
    <div class="btn-row" id="drive-actions"></div>
    <details class="raw"><summary>Raw <span class="mono">mt status</span></summary><pre>${esc(di.raw_mt_status||'—')}</pre></details>`;

  const strip = body.querySelector('#drive-index-strip');
  if(idx){
    strip.innerHTML = `<span style="display:flex;align-items:center;gap:10px"><span style="color:var(--accent)">${ico('list',16)}</span><span><strong style="font-weight:600">Index available</strong> · ${(idx.file_count||0).toLocaleString()} files · written ${esc(fmtTs(idx.written_at))}</span></span>`;
    const b = el('button','link-btn'); b.textContent='View index';
    b.onclick = () => openRestoreFor(di.volume_tag);
    strip.appendChild(b);
  } else {
    strip.innerHTML = `<span class="text-muted">No index on disk for this tape</span>`;
    const b = el('button','btn sm primary'); b.innerHTML = `${ico('list',14)}Read index`;
    b.onclick = async () => {
      b.disabled = true; b.textContent = 'Reading…';
      const r = await api('/api/tape_index/read','POST');
      if(r.ok) await pollOnce(); else { alert(r.error); b.disabled=false; b.innerHTML=`${ico('list',14)}Read index`; }
    };
    strip.appendChild(b);
  }

  const acts = body.querySelector('#drive-actions');
  const tgt = getUnloadTargetSlot();
  const unload = el('button','btn primary', `${ico('eject',15)}Unload to slot ${tgt ?? '—'}`);
  unload.disabled = !tgt;
  unload.onclick = async () => {
    const targetSlot = getUnloadTargetSlot();
    if(!targetSlot){ alert('An empty unload target slot is required.'); return; }
    G.activeAction = {msg:'Unloading tape to slot '+targetSlot+'…', status:'working', _action:'unload', started: Date.now()};
    G.unloadTargetSlot = null;
    renderPage();
    const r = await api('/api/unload','POST',{slot:targetSlot});
    if(!r.ok){ G.activeAction = {msg: r.error||'Unload failed', status:'error', _action:'unload'}; renderPage(); }
    else startChangerRapidPoll();
    await pollOnce();
  };
  acts.appendChild(unload);
  const rewind = el('button','btn', `${ico('rewind',15)}Rewind`);
  rewind.onclick = async () => { rewind.disabled = true; await api('/api/rewind','POST'); await pollOnce(); };
  acts.appendChild(rewind);
  const check = el('button','btn', `${ico('search',15)}Check drive`);
  check.onclick = async () => { check.disabled = true; await checkDriveNow(); check.disabled = false; };
  acts.appendChild(check);
  if(idx){
    const rb = el('button','btn', `${ico('restore',15)}Restore…`);
    rb.onclick = () => openTapeDrawer({slot, volume_tag:di.volume_tag, in_drive:true, full:true, has_index:true});
    acts.appendChild(rb);
  }
  acts.appendChild(el('span','vsep'));
  const clean = el('button','btn', `${ico('brush',15)}Run cleaning`);
  clean.onclick = async () => { const r = await api('/api/cleaning/run','POST',{}); if(!r.ok) alert(r.error); await pollOnce(); };
  acts.appendChild(clean);
  const mailInfo = (G.state?.slots||[]).find(x=>x.is_import_export) || null;
  const mail = el('button','btn', `${ico('inbox',15)}${mailInfo ? `Mail slot ${mailInfo.slot}${mailInfo.full?' · occupied':''}` : 'No mail slot'}`);
  mail.disabled = !mailInfo;
  mail.onclick = () => { if(mailInfo) openMailSlot(mailInfo, idxMap); };
  acts.appendChild(mail);
  return card;
}

function openMailSlot(mailInfo, idxMap){
  openTapeDrawer({
    slot:mailInfo.slot, volume_tag:mailInfo.volume_tag||'', full:mailInfo.full,
    in_drive:false, is_import_export:true, has_index:!!(mailInfo.volume_tag && (idxMap[mailInfo.volume_tag]?.file_count||0)>0),
    space:(idxMap[mailInfo.volume_tag||'']||null)?.space||null,
  });
}

// ── Inventory & recent activity ─────────────────────────────────────────────
function inventoryPanel(){
  const inv = G.state.inventory_job || {};
  const loadedVol = G.state?.summary?.loaded_volume || '';
  const loadedForSingle = !!(G.state?.summary?.loaded && loadedVol);
  const pct = inv.total_slots>0 ? (inv.scanned/inv.total_slots*100) : (inv.status==='completed'?100:0);
  const hwScan = inv.running && inv.mode==='quick' && inv.scanned===0 && inv.total_slots===0;
  const tone = inv.running||inv.status==='paused' ? 'warn' : inv.status==='completed' ? 'ok' : '';
  const label = {idle:'Idle', running:'Running', paused:'Paused', completed:'Completed', stopped:'Stopped', failed:'Failed'}[inv.status] || (inv.status||'Idle');
  const progress = inv.running
    ? (hwScan
        ? `<div class="prog-wrap thin"><span class="prog-bar prog-indeterminate"></span></div><span class="text-sm text-muted">Picker arm moving — scanning barcodes</span>`
        : `<div class="prog-wrap thin"><span class="prog-bar blue" style="width:${pct.toFixed(0)}%"></span></div><span class="text-sm text-muted">${inv.scanned}/${inv.total_slots} processed${inv.paused?' · paused':''}${inv.eta_seconds!=null?` · ETA ${fmtSec(inv.eta_seconds)}`:''}</span>`)
    : inv.status==='completed' ? `<div class="prog-wrap thin"><span class="prog-bar" style="width:100%"></span></div>` : '';
  return `<section class="panel" aria-label="Inventory">
    ${panelHead('Inventory','',badge(label, tone))}
    <div class="panel-body">
      <div class="kv">
        <span>Mode</span><span>${inv.mode==='quick'?'Quick scan (barcodes)':'Full scan'}</span>
        ${inv.total_slots?`<span>Processed</span><span class="mono">${inv.scanned??0} / ${inv.total_slots} slots</span>`:''}
        <span>Last message</span><span>${esc(inv.last_message||'—')}</span>
      </div>
      ${progress?`<div class="stack" style="gap:6px">${progress}</div>`:''}
      <div class="btn-grid">
        <button class="btn primary" onclick="startInventory('full')" ${inv.running?'disabled':''}>${ico('scan',15)}Full scan</button>
        <button class="btn" onclick="startInventory('quick')" ${inv.running?'disabled':''}>${ico('zap',15)}Quick scan</button>
        <button class="btn" onclick="${inv.paused?'resumeInventory()':'pauseInventory()'}" ${inv.running?'':'disabled'}>${inv.paused?ico('play',15)+'Resume':ico('pause',15)+'Pause'}</button>
        <button class="btn danger" onclick="stopInventory()" ${inv.running?'':'disabled'}>${ico('stop',15)}Stop</button>
        <button class="btn" onclick="updateLoadedTapeInventory('full')" ${(inv.running||!loadedForSingle)?'disabled':''}>${ico('cassette',15)}Update loaded tape</button>
        <button class="btn" onclick="doRefresh()">${ico('refresh',15)}Refresh status</button>
      </div>
    </div>
  </section>`;
}

function recentPanel(){
  const acts = (G.state?.actions||[]).slice(0,6);
  const rows = acts.length ? acts.map(a => `<div class="recent-row">
      <span class="dot ${a.ok===false?'red':'green'}"></span>
      <span class="k">${esc(a.kind)}</span>
      <span class="d" title="${esc(a.detail)}">${esc(a.detail)}</span>
      <span class="t">${fmtTime(a.ts)}</span></div>`).join('') : '<div class="empty-state">No actions yet</div>';
  return `<section class="panel" aria-label="Recent activity">
    ${panelHead('Recent activity','',`<button class="link-btn" onclick="showPage('log')">View all</button>`)}
    <div>${rows}</div>
  </section>`;
}

// ── Slot map ─────────────────────────────────────────────────────────────────
function slotMapPanel(idxMap){
  const inv = G.state.inventory_job || {};
  const drive = G.state.drive || {};
  const knownVols = new Set(Object.keys(idxMap));
  const scanningSlot = inv.running ? inv.current_slot : null;
  const workingSlot = G.activeAction?.status==='working' ? G.tdSlot : null;
  const panel = el('section','panel');
  panel.setAttribute('aria-label','Slot map');
  panel.innerHTML = panelHead('Slot map','Cartridge positions by magazine',`
    <div class="legend-inline">
      <span><i style="background:#fff;border:1px solid #B9C0CA"></i>Data</span>
      <span><i style="background:var(--accent-tint);border:1px dashed var(--accent)"></i>In drive</span>
      <span><i style="background:var(--cln-tint);border:1px solid var(--cln-line)"></i>Cleaning</span>
      <span><i style="background:var(--panel-2);border:1px dashed #B9C0CA"></i>Empty</span>
      <span><i style="background:#fff;border:2px solid var(--accent)"></i>Unload target</span>
    </div>`);
  const body = el('div','panel-body');
  body.style.gap = '20px';
  panel.appendChild(body);

  const magazines = G.state?.summary?.magazines?.length ? G.state.summary.magazines : [{ magazine: 1, slots: (G.state.slots||[]).filter(s=>!s.is_import_export) }];
  const magsWrap = el('div','mags');
  const effLoaded = getEffectiveLoadedSlot();
  for(const mag of magazines){
    const magSlots = mag.slots || [];
    const wrap = el('div','');
    wrap.innerHTML = `<div class="mag-head"><span class="n">Magazine ${mag.magazine}</span><span class="c">${mag.full_slots ?? magSlots.filter(s=>s.full).length} / ${mag.slot_count ?? magSlots.length} full</span></div>`;
    const grid = el('div','lib-grid');
    for(const slot of magSlots){
      const vol = slot.volume_tag || '';
      const isCln = is_cleaning_vol(vol);
      const isLoaded = effLoaded===slot.slot && !drive.empty;
      const meta = idxMap[vol] || null;
      const hasIdx = knownVols.has(vol) && (meta?.file_count||0) > 0;
      const sp = meta?.space || null;
      const isTarget = !slot.full && !isLoaded && G.unloadTargetSlot===slot.slot;
      const lastBk = G.tapeHistoryMap[vol]?.last_backup;
      let kind = 'data', volText = vol || '—', sub = '', subCls = '', bar = '';
      if(isLoaded){ kind='loaded'; volText = drive.volume_tag || vol || 'In drive'; sub = 'In drive'; }
      else if(!slot.full){ kind='empty'; volText='Empty'; sub = isTarget ? 'Unload target' : 'Available'; }
      else if(isCln){ kind='cleaning'; sub='Cleaning cartridge'; }
      else if(sp?.capacity_bytes && meta){
        const p = Math.max(0, Math.min(100, (sp.used_bytes||0)/sp.capacity_bytes*100));
        bar = `<span class="tile-bar"><span style="width:${p.toFixed(0)}%;background:${meterColor(p)}"></span></span>`;
        const free = sp.remaining_bytes ?? Math.max(0, sp.capacity_bytes-(sp.used_bytes||0));
        sub = `${hBytes(free)} free${lastBk?' · '+fmtAgo(lastBk):''}`;
        if(!hasIdx && !(sp.used_bytes)) sub = `Blank · ${hBytes(sp.capacity_bytes)} free`;
      } else { sub = 'Not indexed'; subCls = 'warn'; }
      if(!bar) bar = '<span class="tile-bar"></span>';
      const cls = ['tape-tile', kind, isTarget?'selected':'', slot.slot===scanningSlot?'scanning':'', slot.slot===workingSlot?'working':''].filter(Boolean).join(' ');
      const tile = el('button', cls);
      tile.type = 'button';
      tile.setAttribute('aria-label', `Slot ${slot.slot} ${volText}`);
      tile.title = meta?.backup_dirnames?.length ? `Latest session: ${meta.backup_dirnames[meta.backup_dirnames.length-1]}` : '';
      tile.innerHTML = `<span class="tile-top"><span>Slot ${String(slot.slot).padStart(2,'0')}</span><span>M${slot.magazine}·${slot.slot_in_magazine}</span></span>
        <span class="tile-vol">${esc(volText)}</span>${bar}<span class="tile-sub ${subCls}">${esc(sub)}</span>`;
      tile.onclick = () => {
        if(!slot.full && !drive.empty && !isLoaded){ setUnloadTargetSlot(isTarget ? null : slot.slot); return; }
        if(isLoaded){
          openTapeDrawer({slot:getUnloadTargetSlot(), loaded_from_slot:effLoaded, volume_tag:drive.volume_tag, in_drive:true, full:true, has_index:knownVols.has(drive.volume_tag)});
          return;
        }
        if(!slot.full) return;
        openTapeDrawer({
          slot:slot.slot, volume_tag:vol, full:slot.full, in_drive:false,
          is_import_export:slot.is_import_export, magazine:slot.magazine, slot_in_magazine:slot.slot_in_magazine,
          has_index:hasIdx, space: sp,
        });
      };
      grid.appendChild(tile);
    }
    wrap.appendChild(grid);
    magsWrap.appendChild(wrap);
  }
  body.appendChild(magsWrap);

  // Drive · mail slot · archived
  const extras = el('div','slot-extras');
  const dcol = el('div',''); dcol.style.width = '270px';
  dcol.innerHTML = '<span class="section-label">Drive</span>';
  const dIdx = drive.volume_tag ? idxMap[drive.volume_tag] : null;
  const dSess = dIdx?.backup_dirnames?.length ? dIdx.backup_dirnames[dIdx.backup_dirnames.length-1] : '';
  const dt = el('button', `wide-tile ${drive.empty?'empty':'drive'}`);
  dt.innerHTML = `<span style="color:${drive.empty?'var(--faint)':'var(--accent)'};display:flex">${ico('drive',22)}</span><span><span class="v">${esc(drive.volume_tag||'Empty')}</span>
    <span class="s">${drive.empty ? (drive.online?'Online':'Offline') : `From slot ${effLoaded ?? '?'}${dSess?' · '+esc(dSess):''}`}</span></span>`;
  if(!drive.empty) dt.onclick = () => openTapeDrawer({slot:getUnloadTargetSlot(), loaded_from_slot:effLoaded, volume_tag:drive.volume_tag, in_drive:true, full:true, has_index:knownVols.has(drive.volume_tag)});
  dcol.appendChild(dt);
  extras.appendChild(dcol);

  const mailSlot = (G.state.slots||[]).find(s=>s.is_import_export);
  if(mailSlot){
    const mcol = el('div',''); mcol.style.width = '220px';
    mcol.innerHTML = '<span class="section-label">Mail slot</span>';
    const mt = el('button', `wide-tile ${mailSlot.full?'':'empty'}`);
    mt.innerHTML = `<span style="color:var(--muted);display:flex">${ico('inbox',22)}</span><span><span class="v">${esc(mailSlot.full ? (mailSlot.volume_tag||'Occupied') : 'Slot '+mailSlot.slot)}</span>
      <span class="s">${mailSlot.full ? 'Slot '+mailSlot.slot+' · occupied' : 'Empty · import / export'}</span></span>`;
    mt.onclick = () => openMailSlot(mailSlot, idxMap);
    mcol.appendChild(mt);
    extras.appendChild(mcol);
  }

  const archived = (G.indexMeta||[]).filter(m => (m.purpose === 'archived' || (!m.present && m.archived_at)) && !m.is_cleaning && !m.deleted);
  if(archived.length){
    const acol = el('div',''); acol.style.flex = '1'; acol.style.minWidth = '260px';
    acol.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center"><span class="section-label">Archived off-site <span class="note">· ${archived.length}</span></span><button class="link-btn" onclick="G.mediaTab='archived';showPage('media')">Manage in catalog</button></div>`;
    const ag = el('div','arch-grid');
    for(const m of archived){
      const t = el('button','wide-tile archived');
      t.innerHTML = `<span><span class="v">${esc(m.volume_tag)}</span><span class="s">${m.last_seen_slot?'Last in slot '+m.last_seen_slot:'Slot unknown'}${m.archived_at?' · since '+fmtDate(m.archived_at):''}</span></span>
        ${m.file_count>0?`<span class="r">${m.file_count.toLocaleString()} files</span>`:''}`;
      t.onclick = () => openTapeDrawer({slot:m.last_seen_slot||null, volume_tag:m.volume_tag, full:false, in_drive:false, is_import_export:false, has_index:m.file_count>0, space:m.space||null, is_archived:true});
      ag.appendChild(t);
    }
    acol.appendChild(ag);
    extras.appendChild(acol);
  }
  body.appendChild(extras);
  return panel;
}
