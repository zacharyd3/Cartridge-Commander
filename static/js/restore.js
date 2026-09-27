'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// RESTORE PAGE + restore destination dialog
// ══════════════════════════════════════════════════════════════════════════════
let _restoreFiles = [];
let _restoreSelected = new Set();
let _restoreTree = {children:{}};
let _restoreCwd = '';      // current directory path in the restore tree browser
let _restoreSessions = [];

function openRestoreFor(vol){
  closeTapeDrawer();
  G._restoreVol = vol;
  showPage('restore');
  loadRestoreIndex(vol);
}

function restoreTapes(){
  return (G.indexMeta || []).filter(x => (x.file_count || 0) > 0 && !x.is_cleaning && (x.purpose||'data') !== 'cleaning' && !x.deleted);
}

function sessionRange(m){
  const sessions = m.backup_dirnames||[];
  const dates = sessions.map(s => (s.match(/(\d{4}-\d{2}-\d{2})/)||[])[1]).filter(Boolean).sort();
  if(sessions.length > 1){
    if(dates.length >= 2) return `${plural(sessions.length,'session')} (${dates[0]} – ${dates[dates.length-1]})`;
    return plural(sessions.length,'session');
  }
  if(sessions.length === 1) return `1 session (${dates[0] || fmtDate(m.written_at)})`;
  return `written ${fmtDate(m.written_at)}`;
}

function tapeLocation(m){
  const drive = G.state?.drive || {};
  if(!drive.empty && drive.volume_tag === m.volume_tag) return {text:'Drive', tone:'info'};
  const slot = (G.state?.slots||[]).find(s => s.volume_tag === m.volume_tag);
  if(slot) return {text: slot.is_import_export ? `Mail ${slot.slot}` : `Slot ${slot.slot}`, tone:''};
  if(m.present && m.last_seen_slot) return {text:`Slot ${m.last_seen_slot}`, tone:''};
  return {text:'Archived', tone:'warn'};
}

function renderRestorePage(c){
  if(!c){ c=$('content'); if(!c) return; c.innerHTML=''; }
  const rst = G.state?.restore_job||{};
  const tone = statusTone(rst.status, rst.running);
  const statusLabel = rst.running ? 'Restore running' : rst.status==='completed' ? 'Last restore completed' : rst.status==='failed' ? 'Last restore failed' : rst.status==='cancelled' ? 'Last restore cancelled' : 'No restore has run yet';
  const icon = rst.running ? ico('refresh',18,'class="ico spin"') : rst.status==='completed' ? ico('check',18) : rst.status==='failed' ? ico('alert',18) : ico('restore',18);

  c.insertAdjacentHTML('beforeend', `
    <div class="page-head"><div><h1>Restore</h1><p class="sub">File-level recovery from catalogued tapes</p></div></div>
    <section class="action-banner ${rst.running?'running':rst.status==='completed'?'success':rst.status==='failed'?'error':''}" aria-label="Restore job">
      <div class="banner-icon">${icon}</div>
      <div class="banner-body" style="gap:4px">
        <span class="banner-title">${statusLabel}${rst.finished_at && !rst.running ? ' · '+esc(fmtTs(rst.finished_at)) : ''}</span>
        <span class="banner-detail ${rst.error?'c-red':''}">${esc(rst.error || rst.last_message || '')}${rst.dest?` · <span class="mono">${esc(rst.volume_tag||'')}</span> → <span class="mono">${esc(rst.dest)}</span>`:''}</span>
        ${rst.running?`<div class="prog-wrap thin" style="margin-top:4px"><span class="prog-bar amber pulse"></span></div>`:''}
      </div>
      ${rst.running?`<button class="btn danger" onclick="stopRestore()">${ico('stop',15)}Stop restore</button>`:''}
      ${(rst.log||[]).length?`<button class="btn sm" onclick="const n=$('restore-log');n.classList.toggle('hidden')">Restore log</button>`:''}
    </section>
    <section class="panel hidden" id="restore-log" aria-label="Restore log">${panelHead('Restore log')}<div class="log-list">${logRows(rst.log||[], {limit:20})}</div></section>`);

  const grid = el('div','grid g-restore');
  const tapes = restoreTapes();
  const listP = el('section','panel');
  listP.setAttribute('aria-label','Catalogued tapes');
  listP.innerHTML = `<div class="panel-head" style="flex-direction:column;align-items:stretch;gap:10px;padding:14px 16px">
      <div style="display:flex;justify-content:space-between;align-items:baseline"><h2>Tapes</h2><span class="meta">${tapes.length} with an index</span></div>
      <label class="search" style="height:34px">${ico('search',14)}<input id="restore-tape-filter" type="search" placeholder="Filter tapes" aria-label="Filter tapes" value="${esc(G.restoreFilter)}" oninput="G.restoreFilter=this.value;renderRestoreTapeList()"/></label>
    </div><div class="tape-list" id="restore-tape-list" role="listbox" aria-label="Tape to restore from"></div>`;
  grid.appendChild(listP);

  const content = el('section','panel');
  content.setAttribute('aria-label','Tape contents');
  content.innerHTML = `
    <div class="panel-head" id="restore-head"><div><h2 class="mono" id="restore-title" style="font-size:17px">No tape selected</h2><p id="restore-file-meta">Select a tape from the list</p></div><span class="meta" id="restore-loc-note"></span></div>
    <div class="panel-body" id="restore-body">
      <div id="restore-session-picker" class="field" style="display:none"></div>
      <div class="btn-row" id="restore-tools" style="display:none">
        <label class="search">${ico('search',15)}<input id="restore-search" type="search" placeholder="Search files" aria-label="Search files" oninput="renderRestoreTree()"/></label>
        <button class="btn" onclick="selectAllRestore()" id="btn-sel-all">Select all</button>
      </div>
      <nav class="breadcrumb" id="restore-breadcrumb" aria-label="Path"></nav>
      <div class="file-list" id="restore-file-list"></div>
      <div class="select-bar" id="restore-select-bar" style="display:none">
        <span class="n" id="restore-sel-summary">No items selected</span>
        <div class="btn-row">
          <button class="btn" onclick="openRestoreDestDrawer(true)">Restore entire tape</button>
          <button class="btn primary" onclick="openRestoreDestDrawer(false)" id="btn-restore-sel" disabled>${ico('restore',15)}Restore selected…</button>
        </div>
      </div>
    </div>`;
  grid.appendChild(content);
  c.appendChild(grid);

  // Open on the tape in the drive (if indexed), else the first catalogued tape.
  let auto = null;
  if(!G._restoreVol && tapes.length){
    const dv = G.state?.drive?.volume_tag;
    auto = (tapes.find(m => m.volume_tag === dv) || tapes[0]).volume_tag;
  }
  renderRestoreTapeList();
  if(auto) loadRestoreIndex(auto);
  // If a tape was already selected (re-render), re-populate
  else if(G._restoreVol) loadRestoreIndex(G._restoreVol, true);
  else $('restore-file-list').innerHTML = '<div class="empty-state">No tape selected</div>';
}

function renderRestoreTapeList(){
  const list = $('restore-tape-list'); if(!list) return;
  const f = G.restoreFilter.trim().toLowerCase();
  const tapes = restoreTapes().filter(m => !f || m.volume_tag.toLowerCase().includes(f));
  list.innerHTML = tapes.length ? tapes.map(m => {
    const loc = tapeLocation(m);
    const on = m.volume_tag === G._restoreVol;
    return `<button type="button" role="option" aria-selected="${on}" class="tape-opt${on?' active':''}" data-vol="${esc(m.volume_tag)}">
      <span class="top"><span class="v">${esc(m.volume_tag)}</span>${badge(loc.text, loc.tone)}</span>
      <span class="m">${(m.file_count||0).toLocaleString()} files · ${esc(sessionRange(m))}</span></button>`;
  }).join('') : '<div class="empty-state">No catalogued tapes</div>';
  list.querySelectorAll('.tape-opt').forEach(b => b.onclick = () => loadRestoreIndex(b.dataset.vol));
}

async function loadRestoreIndex(vol, silent){
  const changed = vol !== G._restoreVol;
  G._restoreVol = vol;
  if(changed || !silent){ _restoreSelected = new Set(); }
  _restoreFiles = []; _restoreTree = {children:{}};
  renderRestoreTapeList();
  const fl=$('restore-file-list'), meta=$('restore-file-meta');
  if(!vol){ if(fl) fl.innerHTML=''; return; }
  if(!G.indexes[vol]){
    if(fl) fl.innerHTML = '<div class="empty-state">Loading index…</div>';
    const data = await api(`/api/tape_index?volume_tag=${encodeURIComponent(vol)}`);
    if(data.ok) G.indexes[vol]=data;
  }
  if(G._restoreVol !== vol) return;  // another tape was picked meanwhile
  const idx = G.indexes[vol];
  setTxt('restore-title', vol);
  if(!idx){ if(meta) meta.textContent='No index found for this tape'; if(fl) fl.innerHTML=''; return; }
  _restoreFiles = idx.files||[];
  _restoreTree = buildFileTree(_restoreFiles);

  // Backup session folders: catalog list first, else top-level dirs in the archive
  const inferred = new Set();
  for(const f of _restoreFiles){ const sl = f.indexOf('/'); if(sl > 0) inferred.add(f.slice(0, sl)); }
  const sessions = (idx.backup_dirnames||[]).length ? idx.backup_dirnames : [...inferred].sort();
  _restoreSessions = sessions;

  let metaTxt = `${_restoreFiles.length.toLocaleString()} files · ${plural(sessions.length||1,'backup session')} · last written ${fmtTs(idx.written_at)}`;
  if(meta) meta.textContent = metaTxt;
  const m = (G.indexMeta||[]).find(x=>x.volume_tag===vol);
  const loc = m ? tapeLocation(m) : null;
  setTxt('restore-loc-note', loc?.text === 'Drive' ? 'In drive' : loc?.text === 'Archived' ? 'Archived — reinsert before restoring' : loc ? `${loc.text} · auto-loaded at restore start` : '');
  $('restore-tools').style.display = '';
  $('restore-select-bar').style.display = '';
  const srch = $('restore-search'); if(srch && changed) srch.value = '';

  if(changed || !silent) _restoreCwd = sessions.length ? sessions[sessions.length-1] + '/' : '';

  const picker = $('restore-session-picker');
  if(picker){
    if(sessions.length > 1){
      picker.style.display = '';
      const latest = sessions.length - 1;
      const cur = (_restoreCwd.split('/')[0]) || sessions[latest];
      picker.innerHTML = `<span class="section-label">Backup session</span><div class="sess-options cards" role="radiogroup" aria-label="Backup session">`
        + [...sessions].reverse().map(s => {
          const i = sessions.indexOf(s);
          return `<button type="button" role="radio" aria-checked="${s===cur}" class="sess-opt${s===cur?' active':''}" data-session="${esc(s)}"><span class="sess-opt-dot"></span><span class="sess-opt-name" title="${esc(s)}">${esc(s)}/</span>${i===latest?'<span class="sess-opt-badge">Latest</span>':''}</button>`;
        }).join('') + `</div>`;
      picker.querySelectorAll('.sess-opt').forEach(btn => btn.onclick = () => navigateRestoreSession(btn.dataset.session));
    } else { picker.style.display = 'none'; picker.innerHTML = ''; }
  }

  renderRestoreTree();
}

function navigateRestoreSession(sessionName){
  document.querySelectorAll('#restore-session-picker .sess-opt').forEach(b => {
    const on = b.dataset.session === sessionName;
    b.classList.toggle('active', on); b.setAttribute('aria-checked', on);
  });
  navigateRestoreIntoDir(sessionName + '/');
}

function navigateRestoreIntoDir(dirPath){
  _restoreCwd = dirPath;
  const srch = $('restore-search'); if(srch) srch.value = '';
  renderRestoreTree();
}

function renderRestoreTree(){
  const fl=$('restore-file-list'); if(!fl) return;
  const filter = ($('restore-search')?.value||'').trim().toLowerCase();
  const bc = $('restore-breadcrumb');
  if(filter){
    const matches = _restoreFiles.filter(x=>x.toLowerCase().includes(filter));
    if(bc) bc.innerHTML = `<span class="dim">${matches.length.toLocaleString()} result${matches.length!==1?'s':''}</span>`;
    let html = `<div class="file-head"><span></span><span>Path</span><span>Type</span></div>`;
    html += matches.slice(0,800).map(fn => restoreRowHTML(fn, fn, fn.endsWith('/'))).join('');
    if(matches.length>800) html += `<div class="empty-state">${(matches.length-800).toLocaleString()} more — refine search</div>`;
    if(!matches.length) html += '<div class="empty-state">No matches</div>';
    fl.innerHTML = html;
    wireRestoreRows(fl);
    updateRestoreSelSummary();
    return;
  }
  const cwd = _restoreCwd || '';
  if(bc){
    const parts = cwd.replace(/\/$/,'').split('/').filter(Boolean);
    bc.innerHTML = parts.map((p,i) => {
      if(i===parts.length-1) return `<span class="cur">${esc(p)}</span>`;
      const to = parts.slice(0,i+1).join('/') + '/';
      return `<a href="#" class="${i===0?'dim':''}" onclick="navigateRestoreIntoDir('${jsq(to)}');return false;">${esc(p)}</a>`;
    }).join('<span class="sep">/</span>');
  }
  const node = getTreeNode(_restoreTree, cwd);
  const entries = Object.values(node.children||{}).sort((a,b)=> a.isDir!==b.isDir ? (a.isDir?-1:1) : a.name.localeCompare(b.name));
  let html = `<div class="file-head"><span></span><span>Name</span><span>Type</span></div>`;
  if(cwd){
    const up = cwd.replace(/\/$/,'').split('/'); up.pop();
    html += `<div class="file-row dir" data-nav="${esc(up.length ? up.join('/')+'/' : '')}"><span class="file-icon">${ico('up',15)}</span><span class="file-name">.. up one level</span></div>`;
  }
  html += entries.map(e => restoreRowHTML(e.name + (e.isDir?'/':''), e.path, e.isDir)).join('');
  if(!entries.length) html += '<div class="empty-state">Empty folder</div>';
  fl.innerHTML = html;
  wireRestoreRows(fl);
  updateRestoreSelSummary();
}

function restoreRowHTML(label, path, isDir){
  const sel = _restoreSelected.has(path);
  return `<div class="file-row${isDir?' dir':''}${sel?' selected':''}" data-path="${esc(path)}" data-dir="${isDir?1:''}">
    <input type="checkbox" class="file-check" aria-label="Select ${esc(label)}" ${sel?'checked':''}/>
    <span class="file-icon">${ico(isDir?'folder':'file',15)}</span>
    <span class="file-name" title="${esc(path)}">${esc(label)}</span><span class="file-type">${isDir?'Folder':'File'}</span></div>`;
}
function wireRestoreRows(fl){
  fl.onclick = e => {
    const r = e.target.closest('.file-row'); if(!r) return;
    if(r.dataset.nav != null){ navigateRestoreIntoDir(r.dataset.nav); return; }
    const path = r.dataset.path; const cb = r.querySelector('input');
    if(e.target === cb || !r.dataset.dir || e.target.closest('.file-icon')){
      if(e.target !== cb) cb.checked = !cb.checked;
      cb.checked ? _restoreSelected.add(path) : _restoreSelected.delete(path);
      r.classList.toggle('selected', cb.checked);
      updateRestoreSelSummary();
    } else {
      navigateRestoreIntoDir(path);
    }
  };
}

function selectAllRestore(){
  const f = ($('restore-search')?.value||'').trim().toLowerCase();
  if(f){ _restoreFiles.filter(x=>x.toLowerCase().includes(f)).slice(0,800).forEach(p=>_restoreSelected.add(p)); }
  else { const node = getTreeNode(_restoreTree, _restoreCwd||''); Object.values(node.children||{}).forEach(e=>_restoreSelected.add(e.path)); }
  renderRestoreTree();
}

function updateRestoreSelSummary(){
  const n = _restoreSelected.size;
  const sess = (_restoreCwd||'').split('/')[0];
  setTxt('restore-sel-summary', n ? `${plural(n,'item')} selected${sess?` in ${sess}`:''}` : 'No items selected');
  const btn=$('btn-restore-sel'); if(btn) btn.disabled = n===0;
}

async function stopRestore(){
  if(!confirm('Stop the running restore? Files extracted so far remain in the destination.')) return;
  const data = await api('/api/restore/stop','POST');
  if(!data.ok) alert(data.error||'Could not stop restore.');
  await pollOnce();
}

// ── Destination dialog ───────────────────────────────────────────────────────
// fromDrawer: the tape drawer already filled G.restorePending (paths + volume).
function openRestoreDestDrawer(restoreAll, fromDrawer=false){
  const vol = fromDrawer ? (G.restorePending?.vol || '') : (G._restoreVol || G.state?.summary?.loaded_volume || '');
  if(!fromDrawer) G.restorePending = {paths:[], vol, slot:null};
  $('restore-vol-input').value = vol;
  setTxt('restore-dest-result', '');

  if(!vol){
    setRestoreResult('No tape selected', 'bad');
    $('restore-dest-drawer').classList.add('open');
    return;
  }
  // Resolve slot: if the tape is already in the drive pass slot:null so the worker skips
  // the load step; if it is in a slot pass that slot so the worker can load it.
  const slotInfo = (G.state?.slots||[]).find(s=>s.volume_tag===vol);
  const drive = G.state?.drive || {};
  const inDrive = !drive.empty && (drive.volume_tag||'') === vol;
  G.restorePending.vol = vol;
  G.restorePending.slot = inDrive ? null : (slotInfo?.slot ?? null);
  setTxt('restore-src-text', inDrive ? 'In drive' : slotInfo ? `Slot ${slotInfo.slot} · loaded into the drive at restore start` : 'Not in the library — reinsert before starting');
  $('restore-src-note').className = 'callout ' + (inDrive || slotInfo ? 'info' : 'warn');

  if(restoreAll){
    G.restorePending.paths = [];
    $('restore-paths-preview').style.display='none';
    setTxt('rd-sub', `Entire tape ${vol}`);
  } else {
    if(!fromDrawer) G.restorePending.paths = [..._restoreSelected];
    const list = $('restore-paths-list');
    $('restore-paths-preview').style.display='';
    setTxt('restore-paths-count', `· ${G.restorePending.paths.length}`);
    setTxt('rd-sub', `Destination for ${plural(G.restorePending.paths.length,'item')}`);
    list.innerHTML = G.restorePending.paths.slice(0,20).map(p => `<div class="file-row${p.endsWith('/')?' dir':''}" style="cursor:default"><span class="file-icon">${ico(p.endsWith('/')?'folder':'file',15)}</span><span class="file-name">${esc(p)}</span></div>`).join('')
      + (G.restorePending.paths.length>20 ? `<div class="empty-state">${G.restorePending.paths.length-20} more</div>` : '');
  }
  $('restore-dest-drawer').classList.add('open');

  const destInput = $('restore-dest-input');
  if(destInput && !destInput._previewHooked){
    destInput._previewHooked = true;
    destInput.addEventListener('input', updateRestorePathPreview);
  }
  // Destination defaults to the bare restore root: every archive already embeds its own
  // top-level folder (the backup folder name pattern), so appending it here would double it.
  const dest = window.APP_CONFIG.restoreRoot;
  destInput.value = dest;
  updateRestorePathPreview();
  ensureRestoreBrowser(dest);
}

function setRestoreResult(msg, tone=''){ const r=$('restore-dest-result'); if(r){ r.className = 'result ' + tone; r.textContent = msg; } }

function closeRestoreDestDrawer(e){
  if(!e||e.target===$('restore-dest-drawer')) $('restore-dest-drawer').classList.remove('open');
}

async function ensureRestoreBrowser(path){
  const target = path || $('restore-dest-input')?.value || window.APP_CONFIG.restoreRoot;
  G.restoreBrowser = await api(`/api/restore/browse?path=${encodeURIComponent(target)}`);
  renderRestoreBrowser();
}
function renderRestoreBrowser(){
  const br = G.restoreBrowser;
  const list = $('restore-browser-list');
  if(!br || !list) return;
  setTxt('restore-browser-path', br.current || '');
  if($('restore-dest-input')){ $('restore-dest-input').value = br.current || $('restore-dest-input').value; updateRestorePathPreview(); }
  let html = '';
  if(br.parent) html += `<div class="file-row dir" data-nav="${esc(br.parent)}"><span class="file-icon">${ico('up',15)}</span><span class="file-name">.. up one level</span></div>`;
  for(const d of br.directories||[]) html += `<div class="file-row dir" data-nav="${esc(d.path)}"><span class="file-icon">${ico('folder',16)}</span><span class="file-name">${esc(d.name)}/</span><button class="btn xs" data-use="${esc(d.path)}">Use</button></div>`;
  if(!(br.directories||[]).length) html += '<div class="empty-state">No subfolders</div>';
  list.innerHTML = html;
  list.onclick = e => {
    const use = e.target.closest('[data-use]');
    if(use){ $('restore-dest-input').value = use.dataset.use; updateRestorePathPreview(); return; }
    const nav = e.target.closest('[data-nav]');
    if(nav) ensureRestoreBrowser(nav.dataset.nav);
  };
}
function restoreBrowseUp(){ if(G.restoreBrowser?.parent) ensureRestoreBrowser(G.restoreBrowser.parent); }
function restoreBrowseRoot(){ ensureRestoreBrowser(window.APP_CONFIG.restoreRoot); }
async function createRestoreFolder(){
  const base = $('restore-dest-input')?.value || G.restoreBrowser?.current || window.APP_CONFIG.restoreRoot;
  const name = prompt('New folder name');
  if(!name) return;
  const safe = name.trim();
  if(!safe) return;
  const target = `${base.replace(/\/$/, '')}/${safe}`;
  const data = await api('/api/restore/start','POST',{volume_tag:'', paths:[], dest:target, slot:null, dry_run:true});
  if(data.ok){
    $('restore-dest-input').value = target;
    await ensureRestoreBrowser(target);
  } else {
    alert(data.error || 'Could not create folder');
  }
}

function updateRestorePathPreview(){
  const wrap = $('restore-path-preview-wrap');
  const list = $('restore-path-preview-list');
  if(!wrap || !list) return;
  const dest = ($('restore-dest-input')?.value || '').replace(/\/+$/, '');
  const paths = G.restorePending?.paths || [];
  if(!dest || paths.length === 0){ wrap.style.display = 'none'; return; }
  wrap.style.display = '';
  const lines = paths.map(p => dest + '/' + p.replace(/^\//, '').replace(/\/$/, ''));
  const show = lines.slice(0, 12);
  const more = lines.length - show.length;
  list.innerHTML = show.map(l => `<div title="${esc(l)}">${esc(l)}</div>`).join('') + (more > 0 ? `<div style="color:var(--muted)">${more} more</div>` : '');
}

async function confirmRestore(){
  const dest = $('restore-dest-input').value.trim();
  const vol  = $('restore-vol-input').value.trim() || G.restorePending.vol;
  if(!dest){ setRestoreResult('Destination path required', 'bad'); return; }
  const payload = { volume_tag: vol, paths: G.restorePending.paths, dest, slot: G.restorePending.slot };
  setRestoreResult('Starting restore…');
  const data = await api('/api/restore/start','POST', payload);
  if(data.ok){
    // Seed restore_job from the response so the page shows "running" immediately.
    if(data.restore_job && G.state) G.state.restore_job = data.restore_job;
    $('restore-dest-drawer').classList.remove('open');
    closeTapeDrawer();
    showPage('restore');
    const prep = new Set(['preparing','rewinding']);
    rapidPollWhile(() => G.state?.restore_job?.running && prep.has(G.state?.restore_job?.status || 'idle'));
  } else {
    setRestoreResult(data.error, 'bad');
  }
}
