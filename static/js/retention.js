'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// RETENTION PAGE — GFS policy, tape selection strategy, archive folder naming
// ══════════════════════════════════════════════════════════════════════════════
const TIER_COLOR = {daily:'var(--r250)', weekly:'var(--r500)', monthly:'var(--r650)', expired:'var(--neutral-2)'};

function renderRetentionPage(c){
  const s = G.settings || {};
  const d = s.gfs_daily_keep ?? 7, w = s.gfs_weekly_keep ?? 4, m = s.gfs_monthly_keep ?? 6;
  const strat = s.tape_fill_strategy || 'spread';
  c.insertAdjacentHTML('beforeend', `
    <div class="page-head"><div><h1>Retention &amp; policies</h1><p class="sub">Restore-point retention, tape selection and archive folder naming</p></div></div>

    <section class="panel" aria-label="GFS retention policy">
      ${panelHead('Grandfather-Father-Son rotation','Tapes outside all three tiers are recyclable',`<button class="btn primary" onclick="saveGfsPolicy()">${ico('save',15)}Save policy</button>`)}
      <div class="grid g-split" style="gap:0">
        <div class="panel-body" style="gap:20px;border-right:1px solid var(--line-2)">
          <div class="grid" style="grid-template-columns:repeat(3,minmax(0,1fr));gap:12px">
            ${tierCard('daily','Daily','gfs-daily',d,'Most recent completed backups')}
            ${tierCard('weekly','Weekly','gfs-weekly',w,'Oldest backup of each recent ISO week')}
            ${tierCard('monthly','Monthly','gfs-monthly',m,'Oldest backup of each recent month')}
          </div>
          <div class="field"><span class="section-label">Coverage per job stream</span><div id="gfs-coverage">${coverageHTML(d,w,m)}</div></div>
          <div class="callout neutral">${ico('info',16)}<span>Tracked per job label · ad-hoc backups share one pool · <span class="mono">0</span> disables a tier</span></div>
          <div id="gfs-save-result" class="result"></div>
        </div>
        <div class="panel-body" id="gfs-side">${gfsSideHTML()}</div>
      </div>
      <div id="gfs-content">${gfsClassificationHTML()}</div>
    </section>

    <div class="grid g-2">
      <section class="panel" aria-label="Tape selection strategy">
        ${panelHead('Tape selection strategy','',`<button class="btn sm primary" onclick="saveTapeStrategy()">Save</button>`)}
        <div class="panel-body" style="gap:12px">
          <p style="font-size:12.5px;line-height:1.6;color:var(--ink-2)">Applies when the drive is empty. Tapes with enough free space for the whole backup are preferred.</p>
          ${strategyCard('spread','Spread','Round-robin across the library: available, then blank, then least recently used.', strat)}
          ${strategyCard('fill','Fill','Writes to one tape until full, then rolls to the next.', strat)}
          <div id="tape-strategy-result" class="result"></div>
        </div>
      </section>
      <section class="panel" aria-label="Archive folder naming">
        ${panelHead('Archive folder naming','',`<div class="btn-row"><button class="btn sm" onclick="previewRestorePattern()">${ico('eye',14)}Preview</button><button class="btn sm primary" onclick="saveRestoreSubfolderPattern()">Save</button></div>`)}
        <div class="panel-body" style="gap:14px">
          <p style="font-size:12.5px;line-height:1.6;color:var(--ink-2)">Top-level archive folder per backup · also the default restore sub-folder</p>
          <div class="field"><label for="rsp-input">Pattern</label><input id="rsp-input" class="mono" value="${esc(s.restore_subfolder_pattern ?? '{volume}_{date}')}" placeholder="{volume}_{date}" oninput="clearTimeout(window._rspT);window._rspT=setTimeout(previewRestorePattern,400)"/></div>
          <div class="btn-row" style="gap:6px;font-size:12px;color:var(--ink-3)"><span>Tokens</span>${['{volume}','{date}','{datetime}','{label}'].map(t=>`<button class="token" onclick="insertPatternToken('${t}')">${t}</button>`).join('')}</div>
          <div class="field" style="padding:12px 14px;background:var(--panel-2);border:1px solid var(--line-2);border-radius:6px;gap:4px"><span class="text-sm text-muted">Preview</span><span id="rsp-preview" class="mono" style="font-size:12.5px;color:var(--accent-ink)">—</span></div>
          <div id="rsp-result" class="result"></div>
        </div>
      </section>
    </div>`);
  ['gfs-daily','gfs-weekly','gfs-monthly'].forEach(id => $(id)?.addEventListener('input', () => {
    setHTML('gfs-coverage', coverageHTML(+$('gfs-daily').value||0, +$('gfs-weekly').value||0, +$('gfs-monthly').value||0));
  }));
  setTimeout(previewRestorePattern, 0);
}

function tierCard(tier, name, id, val, desc){
  return `<label class="tier-card" for="${id}"><span class="h"><span>${name}</span><i style="background:${TIER_COLOR[tier]}"></i></span>
    <input id="${id}" type="number" min="0" step="1" value="${val}" aria-label="${name} keep"/>
    <span class="d">${desc}</span></label>`;
}

function coverageHTML(d, w, m){
  const seg = (n, color, flex, pad, gapPx) => n > 0
    ? `<div style="flex:${flex};gap:${gapPx}px;padding:0 ${pad}px;${pad?'background:var(--panel-2);':''}">${Array.from({length:Math.min(n,31)},()=>`<span style="background:${color};margin:${pad?Math.round(pad*.8):0}px 0"></span>`).join('')}</div>` : '';
  const bars = seg(d, TIER_COLOR.daily, 1.3, 0, 2) + seg(w, TIER_COLOR.weekly, 1.6, 6, 10) + seg(m, TIER_COLOR.monthly, 3, 8, 16);
  const labels = [d>0?`<span style="flex:1.3">Now → ${plural(d,'backup')}</span>`:'', w>0?`<span style="flex:1.6">→ ${plural(w,'week')}</span>`:'', m>0?`<span style="flex:3;display:flex;justify-content:space-between"><span>→ ${plural(m,'month')}</span><span>older: recyclable</span></span>`:''].join('');
  return bars ? `<div class="coverage">${bars}</div><div class="coverage-labels">${labels}</div>` : '<div class="chart-empty" style="min-height:60px">All tiers disabled</div>';
}

function gfsSideHTML(){
  const g = G.gfs;
  const recs = g?.records || [];
  const counts = {daily:0, weekly:0, monthly:0, expired:0};
  for(const r of recs) if(r.gfs_class in counts) counts[r.gfs_class]++;
  const rec = g?.recyclable || [];
  const bySlot = Object.fromEntries((G.state?.slots||[]).filter(s=>s.volume_tag).map(s=>[s.volume_tag, s.slot]));
  const newest = v => { const r = recs.find(x=>x.volume_tag===v && x.status==='completed'); return r?.started_at; };
  const donut = g ? `<div class="field" style="gap:12px;padding-bottom:16px;border-bottom:1px solid var(--line-2)">
      <span class="section-label">Restore points by tier <span class="note">· all streams</span></span>
      <div class="chart-row" style="gap:20px">
        ${donutChart([
          {v:counts.daily, color:TIER_COLOR.daily, label:'Daily'},
          {v:counts.weekly, color:TIER_COLOR.weekly, label:'Weekly'},
          {v:counts.monthly, color:TIER_COLOR.monthly, label:'Monthly'},
          {v:counts.expired, color:TIER_COLOR.expired, label:'Recyclable'},
        ], {center:recs.length, sub:'records', aria:`${counts.daily} daily, ${counts.weekly} weekly, ${counts.monthly} monthly, ${counts.expired} recyclable`})}
        ${legendHTML([
          {color:TIER_COLOR.daily, label:'Daily', value:counts.daily},
          {color:TIER_COLOR.weekly, label:'Weekly', value:counts.weekly},
          {color:TIER_COLOR.monthly, label:'Monthly', value:counts.monthly},
          {color:TIER_COLOR.expired, label:'Recyclable', value:counts.expired},
        ])}
      </div></div>` : '<div class="chart-empty">Loading…</div>';
  const list = !g ? '' : rec.length
    ? `<div class="callout warn">${ico('alert',16)}<span>${plural(rec.length,'tape')} beyond retention · safe to reuse</span></div>
       <div style="border:1px solid var(--line-2);border-radius:6px">${rec.map(v => `<div style="display:flex;align-items:center;justify-content:space-between;min-height:48px;padding:6px 14px;border-bottom:1px solid var(--line-3)">
         <span><span class="mono" style="font-weight:600;font-size:13px;display:block">${esc(v)}</span><span class="text-sm text-muted">${bySlot[v]?`Slot ${bySlot[v]}`:'Not in library'}${newest(v)?` · newest backup ${fmtDate(newest(v))}`:''}</span></span>
         ${bySlot[v]?`<button class="link-btn" onclick="G.fmtSelected.add('${jsq(v)}');showPage('media')">Erase…</button>`:''}</div>`).join('')}</div>`
    : `<div class="callout ok">${ico('check',16)}<span>No tapes recyclable under the current policy</span></div>`;
  return `${donut}
    <div style="display:flex;align-items:center;justify-content:space-between"><span class="section-label">Recyclable tapes</span>
      <button class="btn xs" onclick="loadGfsStatus()">${ico('refresh',13)}Re-check</button></div>
    ${list}
    ${g?'<span class="text-sm text-muted">Re-evaluated on policy save</span>':''}`;
}

function gfsClassificationHTML(){
  const recs = (G.gfs?.records || []).slice(0,30);
  if(!recs.length) return '';
  return `<div style="border-top:1px solid var(--line-2)">
    <div class="panel-head" style="border-bottom:none"><h2 style="font-size:13px">Recent record classification</h2><span class="meta">Last ${recs.length} records</span></div>
    <div class="tbl-wrap"><table class="tbl"><thead><tr><th>Tier</th><th>Volume</th><th>Stream (job)</th><th>Started</th><th>Status</th></tr></thead><tbody>
    ${recs.map(r => `<tr><td>${badge(r.gfs_class==='expired'?'recyclable':r.gfs_class, 'wide t-'+r.gfs_class)}</td><td class="vol">${esc(r.volume_tag||'—')}</td>
      <td>${esc(r.stream==='(unlabeled)'?'Ad-hoc':r.stream||'—')}</td><td>${esc(fmtWhen(r.started_at))}</td>
      <td><span class="status-dot ${r.status==='completed'?'ok':r.status==='cancelled'?'neutral':'bad'}">${esc(r.status)}</span></td></tr>`).join('')}
    </tbody></table></div></div>`;
}

function strategyCard(val, title, desc, cur){
  return `<label class="check-card${val===cur?' selected':''}"><input type="radio" name="tape-strategy" value="${val}" ${val===cur?'checked':''} onchange="document.querySelectorAll('[name=tape-strategy]').forEach(r=>r.closest('label').classList.toggle('selected',r.checked))"/>
    <span><span class="t">${title}</span><span class="d" style="display:block">${desc}</span></span></label>`;
}

function insertPatternToken(t){
  const inp = $('rsp-input'); if(!inp) return;
  const a = inp.selectionStart ?? inp.value.length, b = inp.selectionEnd ?? a;
  inp.value = inp.value.slice(0,a) + t + inp.value.slice(b);
  inp.focus(); inp.setSelectionRange(a+t.length, a+t.length);
  previewRestorePattern();
}

async function loadGfsStatus(){
  setHTML('gfs-side', '<div class="chart-empty">Analysing…</div>');
  await ensureGfs(true);
  setHTML('gfs-side', gfsSideHTML());
  setHTML('gfs-content', gfsClassificationHTML());
}

async function saveGfsPolicy(){
  const res=$('gfs-save-result'); if(!res) return;
  const daily=parseInt($('gfs-daily')?.value,10);
  const weekly=parseInt($('gfs-weekly')?.value,10);
  const monthly=parseInt($('gfs-monthly')?.value,10);
  res.className='result'; res.textContent='Saving…';
  const data=await api('/api/settings/gfs','POST',{daily,weekly,monthly});
  if(data.ok){
    const p=data.policy||{};
    if(G.settings){ G.settings.gfs_daily_keep=p.daily; G.settings.gfs_weekly_keep=p.weekly; G.settings.gfs_monthly_keep=p.monthly; }
    // Reflect any clamping the server applied.
    if($('gfs-daily')) $('gfs-daily').value=p.daily;
    if($('gfs-weekly')) $('gfs-weekly').value=p.weekly;
    if($('gfs-monthly')) $('gfs-monthly').value=p.monthly;
    setHTML('gfs-coverage', coverageHTML(p.daily, p.weekly, p.monthly));
    res.className='result ok'; res.textContent=`Saved — daily ${p.daily} / weekly ${p.weekly} / monthly ${p.monthly} · ${plural(data.recyclable_count ?? 0,'tape')} recyclable`;
    loadGfsStatus();
  } else {
    res.className='result bad'; res.textContent=data.error||'Save failed';
  }
}

async function saveTapeStrategy(){
  const res=$('tape-strategy-result');
  const v = document.querySelector('[name=tape-strategy]:checked')?.value;
  if(!res||!v) return;
  res.className='result'; res.textContent='Saving…';
  const data=await api('/api/settings/tape_strategy','POST',{strategy:v});
  if(data.ok){
    if(G.settings) G.settings.tape_fill_strategy=data.strategy;
    res.className='result ok'; res.textContent=`Saved — ${data.strategy==='fill'?'filling one tape at a time':'spreading across tapes'}`;
  } else { res.className='result bad'; res.textContent=data.error||'Save failed'; }
}

async function previewRestorePattern(){
  const inp=$('rsp-input'); const prev=$('rsp-preview'); if(!inp||!prev) return;
  const pattern=inp.value.trim();
  if(!pattern){ prev.textContent='→ restore root (no sub-folder)'; return; }
  const vol = G._restoreVol || G.state?.summary?.loaded_volume || 'TAPEVOL';
  const data = await api(`/api/restore/default_dest?volume_tag=${encodeURIComponent(vol)}&pattern=${encodeURIComponent(pattern)}`).catch(()=>({}));
  prev.textContent = data.ok ? '→ ' + data.dest : '→ (preview unavailable)';
}

async function saveRestoreSubfolderPattern(){
  const inp=$('rsp-input'); const res=$('rsp-result'); if(!inp||!res) return;
  const pattern=inp.value.trim();
  res.className='result'; res.textContent='Saving…';
  const data=await api('/api/settings/restore_subfolder','POST',{pattern});
  if(data.ok){
    res.className='result ok'; res.innerHTML=`Saved · example <span class="mono">${esc(data.example)}</span>`;
    if(G.settings) G.settings.restore_subfolder_pattern=data.pattern;
    setTxt('rsp-preview', '→ '+data.example);
  } else { res.className='result bad'; res.textContent=data.error||'Save failed'; }
}
