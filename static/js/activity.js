'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// ACTIVITY PAGE — data-written chart, throughput by job, backup records, action log
// ══════════════════════════════════════════════════════════════════════════════
const STREAM_COLORS = ['var(--s1)','var(--s2)','var(--s3)'];

function renderLogPage(c){
  c.insertAdjacentHTML('beforeend', `
    <div class="page-head">
      <div><h1>Activity</h1><p class="sub">Backup records and library action audit trail${G.settings?` · retention ${G.settings.log_retention_days ?? 30} days / ${(G.settings.log_max_rows ?? 5000).toLocaleString()} rows`:''}</p></div>
      <div class="actions"><button class="btn" onclick="loadBackupRecords()">${ico('refresh',15)}Reload records</button></div>
    </div>
    <div class="grid stretch g-wide">${writtenStackPanel()}${throughputByJobPanel()}</div>
    <section class="panel" aria-label="Backup records" id="records-panel">${recordsPanelHTML()}</section>
    <section class="panel" aria-label="Action log" id="actions-panel">${actionLogHTML()}</section>`);
}

function writtenStackPanel(){
  if(!G.records) return `<section class="panel col">${panelHead('Data written per day · 30 days','Loading records…')}<div class="panel-body"><div class="chart-empty">Loading…</div></div></section>`;
  const days = writtenPerDay(30);
  // Top two streams by bytes get their own colour; the rest fold into "Other".
  const totals = {};
  for(const d of days) for(const [k,v] of Object.entries(d.streams)) totals[k] = (totals[k]||0) + v;
  const named = Object.entries(totals).sort((a,b)=>b[1]-a[1]).map(([k])=>k).filter(k=>k!=='Running').slice(0,2);
  const series = [...named, 'Other'];
  const sumBy = {}; series.forEach(s=>sumBy[s]=0);
  const cols = days.map((d,i) => {
    const vals = {}; series.forEach(s=>vals[s]=0);
    for(const [k,v] of Object.entries(d.streams)) vals[named.includes(k)?k:'Other'] += v;
    series.forEach(s=>sumBy[s]+=vals[s]);
    const lines = series.filter(s=>vals[s]>0).map(s=>`${esc(s)} ${toTB(vals[s]).toFixed(2)} TB`);
    return {
      segs: series.map((s,j)=>({v:toTB(vals[s]), color:STREAM_COLORS[j]})),
      x: i % 7 === 0 ? d.date.toLocaleDateString(undefined,{day:'numeric',month:'short'}) : '',
      tip: {title:`${d.date.toLocaleDateString(undefined,{weekday:'short',day:'numeric',month:'short'})} · ${toTB(d.total).toFixed(2)} TB`, lines: lines.length?lines:['No backups']},
    };
  });
  const total = days.reduce((a,d)=>a+d.total,0);
  const jobs = days.reduce((a,d)=>a+d.jobs,0);
  const {max, ticks} = niceTicks(Math.max(...days.map(d=>toTB(d.total)), 0) || 1, 3);
  const legend = `<div class="legend-inline" style="font-size:12px;color:var(--ink-2)">${series.map((s,j)=>`<span><i style="width:10px;height:10px;background:${STREAM_COLORS[j]}"></i>${esc(s)} <strong class="tnum" style="font-weight:600">${toTB(sumBy[s]).toFixed(2)} TB</strong></span>`).join('')}</div>`;
  return `<section class="panel col" aria-label="Data written per day">
    ${panelHead('Data written per day · 30 days', `${toTB(total).toFixed(2)} TB across ${plural(jobs,'job')} · by retention stream`, legend)}
    <div class="panel-body" style="padding:20px 20px 16px">${columnChart(cols, {max, ticks, fmtTick:v=>v?`${v} TB`:'0', height:190, barWidth:14, aria:'Data written per day, last 30 days, stacked by job'})}</div>
  </section>`;
}

function throughputByJobPanel(){
  const since = Date.now()/1000 - 30*86400;
  const agg = {};
  for(const r of (G.records||[])){
    if(r.status!=='completed' || !(r.speed_bps>0) || (r.started_at||0) < since) continue;
    const k = streamName(recordStream(r));
    (agg[k] ||= {sum:0,n:0}); agg[k].sum += r.speed_bps; agg[k].n++;
  }
  const rows = Object.entries(agg).map(([k,a]) => ({label:k, v:a.sum/a.n/(1024*1024)})).sort((a,b)=>b.v-a.v).slice(0,6)
    .map(r => ({...r, text:`${r.v.toFixed(0)} MB/s`}));
  const {max, ticks} = niceTicks(Math.max(...rows.map(r=>r.v), 1), 2);
  return `<section class="panel col" aria-label="Average throughput by job">
    ${panelHead('Average throughput by job','Completed backups · 30 days · MB/s')}
    <div class="panel-body" style="flex:1">
      ${rows.length ? `${hbarChart(rows, {max, labelWidth:130, maxWidth:80})}
        <div class="hbar-axis" style="margin-top:auto;margin-left:142px;width:calc((100% - 142px) * .8)">${ticks.map(t=>`<span>${t}</span>`).join('')}</div>` : '<div class="chart-empty">No completed backups in 30 days</div>'}
    </div>
  </section>`;
}

function recordsPanelHTML(){
  const recs = G.records;
  const head = panelHead('Backup records','',`<span class="meta">${recs?`Latest ${Math.min(recs.length,50)}`:''}</span>`);
  if(!recs) return head + '<div class="empty-state">Loading…</div>';
  if(!recs.length) return head + '<div class="empty-state">No backup records yet</div>';
  const S = {completed:['Completed','ok'], failed:['Failed','bad'], cancelled:['Cancelled','']};
  return head + `<div class="tbl-wrap"><table class="tbl"><thead><tr>
      <th>Status</th><th>Volume</th><th>Started</th><th>Job</th><th class="num">Written</th><th>Sources · archive folder</th><th>Verified</th></tr></thead><tbody>
    ${recs.slice(0,50).map(r => {
      const [l,t] = S[r.status] || [r.status, ''];
      const ver = r.verified===true ? `<span class="status-dot ok">Verified</span>` : r.verified===false ? `<span class="status-dot bad">Failed</span>` : '<span class="text-muted">—</span>';
      const notes = [
        r.skipped_count ? `<div class="sub" style="color:var(--warn-text)" title="${esc((r.skipped_items||[]).join('\n'))}">${ico('alert',12)} ${plural(r.skipped_count,'unreadable item')} skipped</div>` : '',
        r.error ? `<div class="sub" style="color:var(--bad)">${esc(r.error)}</div>` : '',
        r.verify_errors ? `<div class="sub" style="color:var(--bad)">Verify: ${plural(r.verify_errors,'error')}</div>` : '',
      ].join('');
      return `<tr class="${r.status==='failed'?'fail':''}">
        <td>${badge(l,t)}</td><td class="vol">${esc(r.volume_tag||'—')}</td><td>${esc(fmtWhen(r.started_at))}</td>
        <td><div>${esc(streamName(recordStream(r)))}</div><div class="sub">${esc(r.mode||'full')}</div></td>
        <td class="num">${hBytes(r.bytes_written||0)}</td>
        <td style="max-width:420px"><div class="path" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${esc((r.paths||[]).join('\n'))}">${esc((r.paths||[]).map(p=>p.split('/').pop()||p).join(', '))}</div>
          ${r.backup_dirname?`<div class="mono" style="font-size:11.5px;color:var(--accent-ink)">${esc(r.backup_dirname)}/</div>`:''}${notes}</td>
        <td>${ver}</td></tr>`;
    }).join('')}</tbody></table></div>`;
}

function actionLogHTML(){
  const all = G.state?.actions || [];
  const rows = G.actionFilter==='fail' ? all.filter(a=>a.ok===false) : all;
  return `<div class="panel-head" style="padding:10px 20px"><h2>Action log</h2>
      <div class="pills" role="tablist" aria-label="Filter">
        <button role="tab" aria-selected="${G.actionFilter==='all'}" class="${G.actionFilter==='all'?'on':''}" onclick="G.actionFilter='all';setHTML('actions-panel',actionLogHTML())">All</button>
        <button role="tab" aria-selected="${G.actionFilter==='fail'}" class="${G.actionFilter==='fail'?'on':''}" onclick="G.actionFilter='fail';setHTML('actions-panel',actionLogHTML())">Failures</button>
      </div></div>
    ${rows.length ? `<div class="tbl-wrap"><table class="tbl"><thead><tr><th style="width:190px">Time</th><th style="width:140px">Action</th><th>Detail</th><th style="width:110px">Result</th></tr></thead><tbody>
      ${rows.map(a => `<tr class="${a.ok===false?'fail':''}"><td class="mono" style="font-size:12px;color:var(--ink-3)">${esc(fmtTs(a.ts))}</td><td style="font-weight:600">${esc(a.kind)}</td>
        <td>${esc(a.detail)}</td><td><span class="status-dot ${a.ok===false?'bad':'ok'}">${a.ok===false?'Failed':'OK'}</span></td></tr>`).join('')}
      </tbody></table></div>` : '<div class="empty-state">No actions</div>'}`;
}

async function loadBackupRecords(){
  setHTML('records-panel', panelHead('Backup records') + '<div class="empty-state">Loading…</div>');
  await ensureRecords(true);
  refreshIfOn(['log']);
}
