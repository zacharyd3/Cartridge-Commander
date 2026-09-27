'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// SETTINGS PAGE — notifications, Home Assistant, templates, drive health, config
// ══════════════════════════════════════════════════════════════════════════════
const NOTIFY_EVENTS = [
  ['on_backup_success',  'Backup completed',     'A backup finished successfully'],
  ['on_backup_failure',  'Backup failed',        'A backup stopped with an error'],
  ['on_verify_failure',  'Verification failed',  'Read-back after a backup found errors'],
  ['on_format_complete', 'Format job finished',  'An erase or catalog reset completed'],
  ['on_inventory_done',  'Inventory finished',   'A full or quick scan completed'],
];
const TEMPLATE_KEYS = ['backup_success_title','backup_success_body','backup_failure_title','backup_failure_body','verify_failure_title','verify_failure_body'];
const TEMPLATE_GROUPS = [
  ['Backup success','backup_success','var(--ok)'],
  ['Backup failure','backup_failure','var(--bad)'],
  ['Verify failure','verify_failure','var(--warn)'],
];
let _haServices = [];   // cached list from last successful fetch

function renderSettingsPage(c){
  const s = G.settings || {};
  const tokens = ['{vol}','{written}','{duration}','{speed}','{verified}','{errors}','{error}','{paths}','{skipped}','{time}'];
  c.insertAdjacentHTML('beforeend', `
    <div class="page-head"><div><h1>Settings</h1><p class="sub">Alerts, integrations, drive diagnostics and the running configuration</p></div></div>
    <div class="grid g-settings">
      <nav class="subnav" aria-label="Settings sections">
        <a href="#set-events">Notifications</a><a href="#set-ha">Home Assistant</a><a href="#set-templates">Message templates</a>
        <a href="#set-health">Drive health</a><a href="#set-config">System configuration</a>
      </nav>
      <div class="stack">
        <section class="panel" id="set-events" aria-label="Notification events">
          ${panelHead('When to notify','Alert triggers for enabled channels',`<div class="btn-row"><span class="result" id="notif-events-result"></span><button class="btn sm primary" onclick="saveNotifyEvents()">Save</button></div>`)}
          <div>${NOTIFY_EVENTS.map(([k,l,d]) => `<label class="toggle-row" for="notif-${k}"><span><span class="t" style="display:block">${l}</span><span class="d" style="display:block">${d}</span></span>
            <span class="toggle"><input type="checkbox" id="notif-${k}"/><span class="toggle-track"></span></span></label>`).join('')}</div>
        </section>

        <section class="panel" id="set-ha" aria-label="Home Assistant">
          ${panelHead('Home Assistant','Alerts via a Home Assistant notify service',`<div class="btn-row"><button class="btn sm" onclick="testHaNotify(this)">${ico('send',14)}Send test</button><button class="btn sm primary" onclick="saveHaConfig()">Save</button></div>`)}
          <div class="panel-body">
            <div id="ha-status" class="strip neutral" style="justify-content:flex-start;flex-wrap:wrap;font-size:12.5px">Loading…</div>
            <div class="grid g-2" style="gap:16px">
              <div class="field"><label for="ha-url">Base URL</label><input id="ha-url" class="mono" placeholder="http://homeassistant.local:8123" autocomplete="off" oninput="haUrlChanged()"/></div>
              <div class="field"><label for="ha-token">Long-lived access token <span style="font-weight:400;color:var(--muted)">· HA Profile → Security</span></label><input id="ha-token" type="password" placeholder="•••••••• (saved)" autocomplete="off"/></div>
            </div>
            <div class="field" style="position:relative">
              <label for="ha-service">Notify service</label>
              <div style="display:flex;gap:8px">
                <div style="flex:1;position:relative">
                  <input id="ha-service" class="mono" placeholder="notify" autocomplete="off" oninput="haServiceInput(this.value)" onfocus="haServiceInputFocus()"/>
                  <div id="ha-service-dropdown" class="dropdown" style="display:none" role="listbox"></div>
                </div>
                <button class="btn" id="ha-fetch-btn" onclick="fetchHaServices()">${ico('refresh',14)}Load services</button>
              </div>
              <span id="ha-service-hint" class="hint"><span class="mono">notify</span> = all devices</span>
            </div>
            <label class="check-line"><input type="checkbox" id="ha-enabled"/>Home Assistant notifications enabled</label>
            <div id="ha-result" class="result"></div>
          </div>
        </section>

        <section class="panel" id="set-templates" aria-label="Message templates">
          ${panelHead('Message templates','Title and body per alert type',`<div class="btn-row"><span class="result" id="notif-templates-result"></span><button class="btn sm primary" onclick="saveNotifyTemplates()">Save templates</button></div>`)}
          <div class="panel-body" style="gap:18px">
            <div class="btn-row" style="gap:6px;font-size:12px;color:var(--ink-3)"><span style="margin-right:4px">Tokens</span>${tokens.map(t=>`<span class="token">${t}</span>`).join('')}</div>
            ${TEMPLATE_GROUPS.map(([name,k,color]) => `<div class="tmpl"><span class="h"><i style="background:${color}"></i>${name}</span>
              <div class="r"><span>Title</span><input id="tmpl-${k}_title" class="mono" aria-label="${name} title"/><button class="btn icon lg" onclick="resetTemplate('${k}_title')" aria-label="Reset ${name} title to default" title="Reset to default">${ico('restore',14)}</button></div>
              <div class="r"><span>Body</span><textarea id="tmpl-${k}_body" rows="3" aria-label="${name} body"></textarea><button class="btn icon lg" onclick="resetTemplate('${k}_body')" aria-label="Reset ${name} body to default" title="Reset to default">${ico('restore',14)}</button></div></div>`).join('')}
          </div>
        </section>

        <section class="panel" id="set-health" aria-label="Drive health">
          ${panelHead('Drive health', `SCSI log pages via <span class="mono">sg_logs</span> · <span class="mono">SG_DEVICE=${esc(s.sg_device||'(not set)')}</span>`, `<button class="btn sm" onclick="loadTapeHealth()">${ico('pulse',14)}Fetch health data</button>`)}
          <div class="panel-body" id="health-content"><div class="chart-empty" style="min-height:80px">No health data fetched</div></div>
        </section>

        <section class="panel" id="set-config" aria-label="System configuration">
          ${panelHead('System configuration','Container environment variables')}
          <div class="config-grid" id="config-table"><div class="empty-state">Loading…</div></div>
        </section>
      </div>
    </div>`);
  loadNotifySettings();
}

async function loadSettingsPage(){
  await ensureSettings(true);
  if(G.page === 'settings') renderPage();
}

async function loadNotifySettings(){
  const s=G.settings||{};
  const haUrl=$('ha-url'); if(haUrl) haUrl.value = s.ha_url||'';
  const haSvc=$('ha-service'); if(haSvc) haSvc.value = s.ha_service||'notify';
  const haEn=$('ha-enabled'); if(haEn) haEn.checked = !!s.ha_enabled;
  const haSt=$('ha-status');
  if(haSt){
    if(s.ha_url){
      haSt.innerHTML = `${badge(s.ha_enabled?'Enabled':'Disabled', s.ha_enabled?'ok':'')}
        <span class="mono">${esc(s.ha_url)}</span><span class="text-muted">·</span>
        <span>service <span class="mono">notify.${esc(s.ha_service||'notify')}</span></span><span class="text-muted">·</span>
        ${s.ha_token_set ? '<span style="color:var(--ok-text);font-weight:500">token set</span>' : '<span style="color:var(--bad);font-weight:500">no token</span>'}`;
    } else {
      haSt.innerHTML = `${badge('Not configured','warn')}`;
    }
  }
  const nc = s.notify || {};
  for(const [k] of NOTIFY_EVENTS){ const cb=$(`notif-${k}`); if(cb) cb.checked = !!nc[k]; }

  const td = await api('/api/settings/notify');
  if(td.ok){
    G._notifyDefaults = td.notify?.defaults || {};
    G._notifyTemplates = td.notify?.templates || {};
    for(const k of TEMPLATE_KEYS){ const ta=$(`tmpl-${k}`); if(ta) ta.value = G._notifyTemplates[k] || G._notifyDefaults[k] || ''; }
  }

  const ct=$('config-table'); if(!ct) return;
  const rows=[
    ['Changer', s.changer], ['Tape', s.tape],
    ['Backup root', s.backup_root], ['Restore root', s.restore_root],
    ['Poll seconds', s.poll_seconds], ['Command timeout', `${s.command_timeout}s`],
    ['Verify after backup', s.verify_after_backup ? 'Yes' : 'No'],
    ['Verify sample', s.verify_sample_mb===0 ? 'Full tape' : hBytes((s.verify_sample_mb||0)*1024*1024)],
    ['Pre-backup hook', s.pre_backup_hook || '(none)'], ['Post-backup hook', s.post_backup_hook || '(none)'],
    ['Erase before backup', s.erase_before_backup ? 'Yes' : 'No'], ['Auto-rewind', s.auto_rewind_after_backup ? 'Yes' : 'No'],
    ['Tape catalog DB', s.tape_catalog_db || '(none)'], ['DB size', hBytes(s.tape_catalog_db_size || 0)],
    ['Catalog rows', (s.catalog_rows ?? 0).toLocaleString()], ['Log rows', (s.log_rows ?? 0).toLocaleString()],
    ['Log retention', `${s.log_retention_days ?? 30} days / ${s.log_max_rows ?? 5000} rows`], ['Drive history file', s.drive_history_file || '(none)'],
    ['SG device', s.sg_device || '(not set)'], ['Mail slot enabled', s.has_mail_slot ? 'Yes' : 'No'],
    ['Magazine size', s.magazine_size ?? '—'], ['Auto rewrite on full', s.auto_rewrite_on_full ? 'Yes' : 'No'],
    ['Default backup log level', s.default_backup_log_level || 'normal'], ['Current backup log level', s.current_backup_log_level || 'normal'],
    ['Restore subfolder pattern', s.restore_subfolder_pattern || '(none — restore to root)'], ['Tape block size', `${(s.tape_block_kb ?? 512)} KiB (TL_TAPE_BLOCK_KB)`],
    ['mbuffer size', `${s.mbuf_size ?? '512M'} (TL_MBUF_SIZE)`], ['mbuffer fill %', `${s.mbuf_fill_pct ?? 75}% (TL_MBUF_FILL_PCT)`],
    ['Skip xattrs / ACLs', (s.skip_xattrs ? 'Yes — faster, no xattr data' : 'No — full fidelity') + ' (TL_SKIP_XATTRS)'],
  ];
  if(rows.length % 2) rows.push(['','']);
  ct.innerHTML = rows.map(([k,v]) => `<div class="row"><span>${esc(k)}</span><span>${esc(v)}</span></div>`).join('');
}

function setResult(id, msg, tone=''){ const r=$(id); if(r){ r.className='result '+tone; r.textContent=msg; } }

async function saveNotifyEvents(){
  setResult('notif-events-result','Saving…');
  const body={};
  for(const [k] of NOTIFY_EVENTS){ const cb=$(`notif-${k}`); if(cb) body[k]=cb.checked; }
  const data=await api('/api/settings/notify','POST',body);
  if(data.ok){
    setResult('notif-events-result','Saved','ok');
    if(G.settings) G.settings.notify = data.notify;
    setTimeout(()=>setResult('notif-events-result',''),3000);
  } else setResult('notif-events-result', data.error||'Save failed', 'bad');
}

async function saveNotifyTemplates(){
  setResult('notif-templates-result','Saving…');
  const templates={};
  for(const k of TEMPLATE_KEYS){ const ta=$(`tmpl-${k}`); if(ta) templates[k]=ta.value; }
  const data=await api('/api/settings/notify','POST',{templates});
  if(data.ok){
    setResult('notif-templates-result','Saved','ok');
    G._notifyTemplates = data.notify?.templates || templates;
    setTimeout(()=>setResult('notif-templates-result',''),3000);
  } else setResult('notif-templates-result', data.error||'Save failed', 'bad');
}

function resetTemplate(key){
  const ta=$(`tmpl-${key}`);
  const def=(G._notifyDefaults||{})[key]||'';
  if(ta && def) ta.value=def;
}

// ── HA notify service picker ────────────────────────────────────────────────
function haUrlChanged(){
  _haServices = [];
  const dd = $('ha-service-dropdown'); if(dd) dd.style.display = 'none';
}
function haServiceInputFocus(){ if(_haServices.length) _renderHaDropdown($('ha-service')?.value||''); }
function haServiceInput(val){ if(_haServices.length) _renderHaDropdown(val); }
function _renderHaDropdown(filter){
  const dd = $('ha-service-dropdown');
  const inp = $('ha-service');
  if(!dd || !_haServices.length){ if(dd) dd.style.display='none'; return; }
  const f = filter.toLowerCase();
  const matches = _haServices.filter(s => s.toLowerCase().includes(f));
  if(!matches.length){ dd.style.display='none'; return; }
  dd.innerHTML = matches.map(s => `<button type="button" role="option" onmousedown="event.preventDefault();selectHaService('${jsq(s)}')">${esc(s)}${s==='notify'?'<small>all devices</small>':''}</button>`).join('');
  dd.style.display = '';
  const closeDD = (e) => {
    if(!dd.contains(e.target) && e.target !== inp){
      dd.style.display='none';
      document.removeEventListener('mousedown', closeDD);
    }
  };
  document.addEventListener('mousedown', closeDD);
}
function selectHaService(name){
  const inp = $('ha-service'); if(inp) inp.value = name;
  const dd = $('ha-service-dropdown'); if(dd) dd.style.display = 'none';
}

async function fetchHaServices(){
  const btn = $('ha-fetch-btn');
  const hint = $('ha-service-hint');
  const urlVal   = ($('ha-url')?.value||'').trim();
  const tokenVal = ($('ha-token')?.value||'').trim();
  if(!urlVal){ setResult('ha-result','Base URL required','bad'); return; }
  if(btn){ btn.disabled=true; btn.textContent='Loading…'; }
  if(hint) hint.textContent = 'Fetching services…';
  // Save URL (+token if typed) so the backend can make the request
  const saveBody = { url: urlVal };
  if(tokenVal) saveBody.token = tokenVal;
  await api('/api/settings/ha', 'POST', saveBody);
  const data = await api('/api/settings/ha_services');
  if(btn){ btn.disabled=false; btn.innerHTML=`${ico('refresh',14)}Load services`; }
  if(data.ok && data.services?.length){
    _haServices = data.services;
    if(hint){ hint.style.color='var(--ok-text)'; hint.textContent = `${plural(data.services.length,'notify service')} found`; }
    _renderHaDropdown($('ha-service')?.value||'');
    $('ha-service')?.focus();
  } else if(data.ok && data.services?.length === 0){
    _haServices = [];
    if(hint){ hint.style.color='var(--warn-text)'; hint.textContent = 'No notify services found in Home Assistant'; }
  } else {
    _haServices = [];
    if(hint){ hint.style.color='var(--bad)'; hint.textContent = `Could not fetch: ${data.error||'unknown error'}`; }
  }
}

async function saveHaConfig(){
  setResult('ha-result','Saving…');
  const token=($('ha-token')?.value||'').trim();
  const body={
    url:     ($('ha-url')?.value||'').trim(),
    service: ($('ha-service')?.value||'notify').trim()||'notify',
    enabled: !!$('ha-enabled')?.checked,
  };
  if(token) body.token = token;
  const data = await api('/api/settings/ha','POST', body);
  if(data.ok){
    setResult('ha-result', data.detail || 'Saved', 'ok');
    if(G.settings){
      G.settings.ha_url     = data.ha?.url     ?? body.url;
      G.settings.ha_service = data.ha?.service ?? body.service;
      G.settings.ha_enabled = data.ha?.enabled ?? body.enabled;
      if(token) G.settings.ha_token_set = true;
    }
    await loadNotifySettings();
    if(token) $('ha-token').value='';
    const dd=$('ha-service-dropdown'); if(dd) dd.style.display='none';
  } else setResult('ha-result', data.error||'Save failed', 'bad');
}

async function testHaNotify(btn){
  if(btn){ btn.disabled=true; btn.textContent='Sending…'; }
  const data = await api('/api/settings/test_ha','POST');
  setResult('ha-result', data.ok ? data.detail : data.error, data.ok ? 'ok' : 'bad');
  if(btn){ btn.disabled=false; btn.innerHTML=`${ico('send',14)}Send test`; }
}

async function loadTapeHealth(){
  const div=$('health-content'); if(!div) return;
  div.innerHTML='<div class="chart-empty" style="min-height:80px">Fetching…</div>';
  const data=await api('/api/tape_health');
  if(!data.ok||!data.health){ div.innerHTML='<div class="callout bad">Failed to fetch health data</div>'; return; }
  const h=data.health;
  if(!h.device){ div.innerHTML=`<div class="callout warn">${ico('alert',16)}<span>SG_DEVICE not configured</span></div>`; return; }
  if(h.error){ div.innerHTML=`<div class="callout bad">${ico('alert',16)}<span>${esc(h.error)}</span></div>`; return; }
  const cnt = (v) => v==null ? '<span class="text-muted">—</span>' : `<span class="${v>0?'c-red':'c-green'}">${v}</span>`;
  let html = `<div class="cells c3 nobottom">
    <div class="cell"><div class="cell-label">Cleaning</div><div class="cell-value" style="font-size:16px">${h.cleaning_required?'<span class="c-red">Required</span>':'<span class="c-green">Not required</span>'}</div></div>
    <div class="cell"><div class="cell-label">Write uncorrected errors</div><div class="cell-value lg">${cnt(h.write_uncorrected)}</div></div>
    <div class="cell"><div class="cell-label">Read uncorrected errors</div><div class="cell-value lg">${cnt(h.read_uncorrected)}</div></div>
  </div><div class="text-sm text-muted">Device <span class="mono">${esc(h.device)}</span></div>`;
  const pages = Object.entries(h.pages||{}).filter(([,t])=>t);
  if(pages.length) html += `<div class="detail-list">${pages.map(([pg,txt])=>`<details><summary>${esc(pg)}</summary><pre>${esc(txt)}</pre></details>`).join('')}</div>`;
  div.innerHTML = html;
}
