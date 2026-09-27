'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// TAPE DETAIL DRAWER + library control actions
// ══════════════════════════════════════════════════════════════════════════════
const SESSION_RAMP = ['var(--r250)','var(--r500)','var(--r650)'];

function actBtn(icon, label, cls, onclick, {hint='', act='', disabled=false, title=''}={}){
  const b = el('button', `btn ${cls||''}`);
  b.type = 'button';
  b.innerHTML = `${ico(icon,15)}<span>${esc(label)}</span>${hint?`<span class="hint">${esc(hint)}</span>`:''}`;
  if(act) b.dataset.act = act;
  if(title) b.title = title;
  b.disabled = disabled;
  b.onclick = onclick;
  return b;
}

function sessionSpaceHTML(vol, sessions, space){
  const cap = space?.capacity_bytes;
  if(!cap) return '';
  const used = space.used_bytes || 0;
  const free = space.remaining_bytes ?? Math.max(0, cap-used);
  const pctUsed = Math.max(0, Math.min(100, used/cap*100));
  // Bytes per backup session on this tape: the catalog's per-tape sessions
  // (a backup spanning tapes only counts its part on this one), else records.
  const bySess = {};
  const tsess = tapeSessions(vol);
  if(tsess.length){
    for(const s of tsess) if(s.dirname) bySess[s.dirname] = (bySess[s.dirname]||0) + Number(s.bytes||0);
  } else for(const r of (G.records||[])){
    if(r.volume_tag !== vol || r.status !== 'completed' || !r.backup_dirname) continue;
    bySess[r.backup_dirname] = (bySess[r.backup_dirname]||0) + Number(r.bytes_written||0);
  }
  const known = sessions.filter(s => bySess[s] > 0);
  const head = `<div class="meter-labels top"><span>${hBytes(cap)}</span><span class="mono">${pctUsed.toFixed(1)}% used${space.estimated?' (est.)':''}</span></div>`;
  if(!known.length || sessions.length < 2){
    return `<div class="field"><span class="section-label">Capacity</span>${head}
      <div class="meter"><span style="width:${pctUsed.toFixed(1)}%;background:${meterColor(pctUsed)}"></span></div>
      <div class="meter-labels"><span>${hBytes(used)} used</span><span>${hBytes(free)} free</span></div></div>`;
  }
  // Newest sessions get the darkest step; older sessions fold into the light end.
  const shown = known.slice(-3);
  const older = known.slice(0, -3).reduce((a,s)=>a+bySess[s],0);
  const segs = [];
  if(older) segs.push({label:`${known.length-3} older`, v:older, color:SESSION_RAMP[0]});
  // Label from the date on (several backups a day get a time suffix to tell them apart).
  const sessLabel = s => { const i = s.search(/\d{4}-\d{2}-\d{2}/); return i >= 0 ? s.slice(i) : s; };
  shown.forEach((s,i) => segs.push({label:sessLabel(s), v:bySess[s], color:SESSION_RAMP[SESSION_RAMP.length - shown.length + i]}));
  const other = Math.max(0, used - segs.reduce((a,s)=>a+s.v,0));
  if(other > cap*0.005) segs.unshift({label:'Other data', v:other, color:'var(--neutral)'});
  const w = v => (v/cap*100).toFixed(2)+'%';
  return `<div class="field"><span class="section-label">Space by session</span>${head}
    <div class="stackbar thin" role="img" aria-label="${esc(segs.map(s=>`${s.label} ${hBytes(s.v)}`).join(', '))}, ${hBytes(free)} free">
      ${segs.map(s=>`<span style="width:${w(s.v)};background:${s.color}" title="${esc(s.label)} · ${hBytes(s.v)}"></span>`).join('')}<span style="flex:1;background:var(--grid)"></span>
    </div>
    <div class="legend" style="font-size:11.5px;row-gap:6px;margin-top:4px">
      ${segs.map(s=>`<span class="sw" style="width:10px;height:10px;background:${s.color}"></span><span class="lbl mono">${esc(s.label)}</span><span class="val">${hBytes(s.v)}</span>`).join('')}
      <span class="sw outline" style="width:10px;height:10px;background:var(--grid)"></span><span class="lbl">Free</span><span class="val">${hBytes(free)}</span>
    </div></div>`;
}

// Every backup on a tape, in the order written (one tape file each), with the
// other tapes of any backup that continues across tapes.
function tapeSessionsHTML(vol){
  const ss = tapeSessions(vol);
  if(!ss.length) return '';
  const rows = ss.map(s => {
    const parts = s.parts || 1;
    const dead = s.broken || s.status === 'failed' || s.status === 'cancelled';
    const flags = [
      parts > 1 ? badge(`Part ${s.part} of ${parts}`, 'info') : '',
      s.broken ? badge('Broken — another part was overwritten', 'bad')
        : s.status === 'failed' ? badge('Incomplete', 'bad')
        : s.status === 'cancelled' ? badge('Cancelled', 'warn') : '',
    ].join('');
    const chain = parts > 1 ? tapeChainHTML(s.chain, vol) : '';
    return `<div class="tape-sess${dead?' dead':''}">
      <div class="top"><span class="fn" title="Tape file number">#${esc(s.file_number ?? '?')}</span><span class="nm" title="${esc(s.dirname||'')}">${esc(s.dirname || '(unnamed)')}/</span><span class="sz">${hBytes(s.bytes||0)}</span></div>
      ${flags || chain ? `<div class="top" style="flex-wrap:wrap">${flags}${chain}</div>` : ''}
    </div>`;
  }).join('');
  return `<div class="field" style="margin-top:16px"><span class="section-label">Backups on this tape · ${ss.length}</span><div class="tape-sessions">${rows}</div></div>`;
}

async function openTapeDrawer(info){
  G.tdSlot=info.slot; G.tdVol=info.volume_tag||''; G.tdInDrive=info.in_drive;
  G.tdSlotFull=info.full; G.tdFiles=[]; G.tdFiltered=[]; G.tdSelected=new Set();
  G.tdCwd=''; G.tdTree={children:{}};
  ensureRecords();

  const vol=G.tdVol;
  const loadedFromSlot = info.loaded_from_slot ?? getEffectiveLoadedSlot();
  const unloadTargetSlot = info.in_drive ? (info.slot ?? getUnloadTargetSlot()) : null;
  const meta = (vol && (G.indexMeta||[]).find(x => x.volume_tag===vol)) || null;
  const knownIdx = (vol && ((G.indexes||{})[vol] || meta)) || null;
  const space = info.in_drive ? (G.driveInfo?.space || info.space || knownIdx?.space || null) : (info.space || knownIdx?.space || null);
  const isCleaning = !!(info.is_cleaning || (info.purpose||'')==='cleaning' || (vol && is_cleaning_vol(vol)));
  const isArchived = !!info.is_archived;

  $('td-title').textContent = vol || `Slot ${info.slot}`;
  $('td-meta').textContent = info.in_drive
    ? (loadedFromSlot ? `In drive · loaded from slot ${loadedFromSlot}` : 'In drive')
    : isArchived ? `Off-site${info.slot?` · last seen in slot ${info.slot}`:''}`
    : info.is_import_export ? `Mail slot ${info.slot}`
    : info.slot ? (info.magazine ? `Magazine ${info.magazine} · Slot ${info.slot} (position ${info.slot_in_magazine})` : `Slot ${info.slot}`) : '';
  $('td-badges').innerHTML = [
    info.in_drive ? badge('In drive','info') : isArchived ? badge('Archived','warn') : info.is_import_export ? badge('Mail slot','warn') : info.full ? badge('In slot','') : badge('Empty',''),
    info.in_drive && unloadTargetSlot ? badge(`Unload target ${unloadTargetSlot}`,'info') : '',
    isCleaning ? badge('Cleaning','cln') : info.has_index ? badge('Indexed','ok') : vol ? badge('Not indexed','warn') : '',
    space?.lto_generation ? badge(`LTO-${space.lto_generation}`,'info') : '',
  ].filter(Boolean).join('');

  const sessionsList = knownIdx?.backup_dirnames || [];
  $('td-space-bar').innerHTML = sessionSpaceHTML(vol, sessionsList, space);
  const h = G.tapeHistoryMap[vol] || {};
  $('td-stats').innerHTML = vol ? `<div class="kv">
      <span>Last backup</span><span>${h.last_backup?esc(fmtTs(h.last_backup)):'Never'}</span>
      <span>Loads</span><span class="mono">${h.load_count ?? '—'}</span>
      <span>Backups</span><span class="mono">${h.backup_count ?? '—'}</span>
      <span>Written</span><span class="mono">${h.total_backup_bytes?hBytes(h.total_backup_bytes):'—'}</span>
      ${meta?.file_count?`<span>Files</span><span class="mono">${meta.file_count.toLocaleString()}</span>`:''}
    </div>${tapeSessionsHTML(vol)}` : '';
  $('td-result').textContent=''; $('td-result').className='result';

  // ── Actions ──────────────────────────────────────────────────────────────
  const acts=$('td-actions'); acts.innerHTML='';
  const life=$('td-lifecycle'); life.innerHTML='';
  const danger=$('td-danger-actions'); danger.innerHTML='';
  const unloadBtn = () => {
    const targetSlot = info.slot ?? getUnloadTargetSlot();
    return actBtn('eject', `Unload to slot ${targetSlot ?? '—'}`, 'primary left', () => {
      const slot = getUnloadTargetSlot();
      if(!slot){ tdResult('An empty unload target slot is required', 'bad'); return; }
      tapeDrawerAction('unload',{slot});
    }, {act:'unload', disabled:!targetSlot});
  };
  if(isCleaning){
    acts.appendChild(actBtn('brush','Perform cleaning','primary left',()=>tapeDrawerAction('clean',{slot:info.slot||null})));
    if(info.in_drive) acts.appendChild(unloadBtn());
    else if(info.full && info.slot) acts.appendChild(actBtn('load','Load cleaning tape','left',()=>tapeDrawerAction('load',{slot:info.slot})));
  } else {
    const mailSlotInfo = (G.state?.slots||[]).find(s=>s.is_import_export) || null;
    if(!info.in_drive && info.full && info.slot && !info.is_import_export){
      acts.appendChild(actBtn('load','Load into drive','primary left',()=>tapeDrawerAction('load',{slot:info.slot})));
      acts.appendChild(actBtn('list','Re-index','left',()=>tapeDrawerAction('reindex',{slot:info.slot, volume_tag:vol}), {hint:'load → read → return', title:'Load this tape, read its file list, then return it to its slot'}));
      if(mailSlotInfo) acts.appendChild(actBtn('inbox', `Move to mail slot${mailSlotInfo.full ? ' (occupied)' : ''}`, 'left', ()=>tapeDrawerAction('mail_export',{slot:info.slot}), {disabled:!!mailSlotInfo.full}));
    }
    if(!info.in_drive && info.full && info.is_import_export){
      const targetSlot = getUnloadTargetSlot();
      acts.appendChild(actBtn('load', `Import to slot ${targetSlot ?? '—'}`, 'primary left', () => {
        const slot = getUnloadTargetSlot();
        if(!slot){ tdResult('An empty target slot is required — set one on the slot map', 'bad'); return; }
        tapeDrawerAction('mail_import',{slot});
      }, {disabled:!targetSlot}));
    }
    if(info.in_drive){
      acts.appendChild(unloadBtn());
      acts.appendChild(actBtn('rewind','Rewind','left',()=>tapeDrawerAction('rewind',{})));
      acts.appendChild(actBtn('list','Re-index','left',()=>tapeDrawerAction('reindex',{volume_tag:vol}), {hint:'rewind → read', title:'Rewind and read the full file list from this tape'}));
      acts.appendChild(actBtn('refresh','Update inventory','left',()=>tapeDrawerAction('update_loaded_inventory',{mode:'full'})));
    }
    if(info.has_index && vol) acts.appendChild(actBtn('restore','Open in Restore','left',()=>openRestoreFor(vol)));
  }
  $('td-actions').parentElement.style.display = acts.children.length ? '' : 'none';

  const hasCatalog = !!(vol && (info.has_index || meta));
  if(vol && hasCatalog && !isCleaning && !isArchived && (info.full || info.in_drive)){
    life.appendChild(actBtn('archive','Mark archived (off-site)','left',()=>archiveTape(vol), {title:'Catalog and file index are preserved.'}));
    life.insertAdjacentHTML('beforeend', '<span class="act-note">Catalog and file index retained</span>');
  } else if(isArchived){
    life.insertAdjacentHTML('beforeend', '<span class="act-note">Archived · Quick scan un-archives on reinsert</span>');
  }
  $('td-lifecycle-wrap').style.display = life.children.length ? '' : 'none';

  if(vol && (hasCatalog || isCleaning)){
    danger.appendChild(actBtn('trash','Remove from catalog','danger left',()=>deleteTapeIndex(vol), {title:'Hides this tape from all views. Tape contents are not touched.'}));
    if(!isCleaning) danger.appendChild(actBtn('alert','Permanently delete…','danger-solid left',()=>permanentDeleteTapeIndex(vol), {title:'Destroys all catalog data. Requires typing the volume tag.'}));
  }
  $('td-danger').style.display = danger.children.length ? '' : 'none';

  // ── Index section ────────────────────────────────────────────────────────
  const idxSec=$('td-index-section');
  idxSec.style.display='none';
  $('td-no-index').style.display='none';
  $('td-file-list').innerHTML='';
  $('td-search').value='';
  updateTdSelSummary();
  $('tape-drawer').classList.add('open');

  if(info.has_index && vol){
    if(!G.indexes[vol]){
      $('td-no-index').style.display=''; $('td-no-index').textContent='Loading index…';
      const data=await api(`/api/tape_index?volume_tag=${encodeURIComponent(vol)}`);
      if(data.ok) G.indexes[vol]=data;
    }
    if(G.tdVol !== vol) return;
    const idx=G.indexes[vol];
    if(idx){
      $('td-no-index').style.display='none';
      G.tdFiles=idx.files||[];
      G.tdFiltered=[...G.tdFiles];
      G.tdTree=buildFileTree(G.tdFiles);
      const sessions = idx.backup_dirnames?.length ? idx.backup_dirnames : (() => {
        const s = new Set();
        for(const f of G.tdFiles){ const sl=f.indexOf('/'); if(sl>0) s.add(f.slice(0,sl)); }
        return [...s].sort();
      })();
      G.tdSessions = sessions;
      // Always start in the most recent session
      const current = sessions.length ? sessions[sessions.length-1] : '';
      G.tdCwd = current ? current + '/' : '';
      if(!knownIdx?.backup_dirnames?.length) $('td-space-bar').innerHTML = sessionSpaceHTML(vol, sessions, space);

      const banner = $('td-session-banner'), single = $('td-session-single'), switchEl = $('td-session-switch');
      if(sessions.length > 1){
        single.style.display = 'none';
        banner.style.display = '';
        const partsOf = Object.fromEntries(tapeSessions(vol).map(x => [x.dirname, x.parts||1]));
        switchEl.innerHTML = [...sessions].reverse().map((s,i) => `<button type="button" role="radio" aria-checked="${s===current}" class="sess-opt${s===current?' active':''}" data-session="${esc(s)}">
            <span class="sess-opt-dot"></span><span class="sess-opt-name">${esc(s)}/</span>${partsOf[s]>1?`<span class="sess-opt-badge span" title="This backup spans ${partsOf[s]} tapes">${ico('link',11)} ${partsOf[s]} tapes</span>`:''}${i===0?'<span class="sess-opt-badge">Latest</span>':''}</button>`).join('');
        switchEl.querySelectorAll('.sess-opt').forEach(btn => btn.onclick = () => switchTapeSession(btn.dataset.session));
        setTxt('td-index-meta', `${(idx.file_count||G.tdFiles.length).toLocaleString()} files · ${sessions.length} sessions`);
      } else {
        banner.style.display = 'none';
        if(sessions.length === 1){
          single.style.display = '';
          setTxt('td-session-name', current + '/');
          setTxt('td-index-meta-single', `${(idx.file_count||G.tdFiles.length).toLocaleString()} files · ${fmtTs(idx.written_at)}`);
        } else single.style.display = 'none';
      }
      $('td-search').placeholder = `Search ${(idx.file_count||G.tdFiles.length).toLocaleString()} files`;
      idxSec.style.display='';
      renderTapeDrawerFiles('');
    } else {
      $('td-no-index').style.display=''; $('td-no-index').textContent='Index could not be loaded';
    }
  } else {
    $('td-no-index').style.display='';
    $('td-no-index').textContent = isCleaning ? 'Cleaning cartridge' : vol ? 'No file index for this cartridge' : 'Empty slot';
  }
}

function closeTapeDrawer(e){
  if(!e||e.target===$('tape-drawer')) $('tape-drawer').classList.remove('open');
}
function tdResult(msg, tone=''){ const r=$('td-result'); if(r){ r.className='result '+tone; r.textContent=msg; } }

// ── File tree helpers (shared with Restore) ─────────────────────────────────
function buildFileTree(files){
  // Nested map: { name, path, isDir, children:{} }
  const root = { children:{} };
  for(const f of files){
    const parts = f.replace(/\/$/,'').split('/');
    let node = root;
    let pathSoFar = '';
    for(let i=0;i<parts.length;i++){
      const p = parts[i];
      if(!p) continue;
      pathSoFar = pathSoFar ? pathSoFar+'/'+p : p;
      const isLast = (i===parts.length-1);
      const isDir = !isLast || f.endsWith('/');
      if(!node.children[p]){
        node.children[p] = { name: p, path: isDir ? pathSoFar+'/' : pathSoFar, isDir, children: {} };
      }
      if(isDir) node = node.children[p];
    }
  }
  return root;
}
function getTreeNode(tree, cwdPath){
  if(!cwdPath) return tree;
  const parts = cwdPath.replace(/\/$/,'').split('/').filter(Boolean);
  let node = tree;
  for(const p of parts){
    if(node.children && node.children[p]) node = node.children[p];
    else return tree;
  }
  return node;
}

function switchTapeSession(sessionName){
  G.tdCwd = sessionName + '/';
  $('td-session-switch')?.querySelectorAll('.sess-opt').forEach(b => {
    const on = b.dataset.session === sessionName;
    b.classList.toggle('active', on); b.setAttribute('aria-checked', on);
  });
  $('td-search').value = '';
  renderTapeDrawerFiles('');
}

function renderTapeDrawerFiles(filter){
  const fl=$('td-file-list');
  const f=(filter||'').trim().toLowerCase();
  const bc=$('td-breadcrumb');
  let html = `<div class="file-head"><span></span><span>Name</span><span>Type</span></div>`;
  if(f){
    const matches = G.tdFiles.filter(x=>x.toLowerCase().includes(f));
    bc.innerHTML = `<span class="dim">${matches.length.toLocaleString()} result${matches.length!==1?'s':''}</span>`;
    G.tdFiltered = matches;
    html += matches.slice(0,500).map(fn => tdRowHTML(fn, fn, fn.endsWith('/'))).join('');
    if(matches.length>500) html += `<div class="empty-state">${(matches.length-500).toLocaleString()} more — refine search</div>`;
    if(!matches.length) html += '<div class="empty-state">No matches</div>';
  } else {
    G.tdFiltered = [];
    const cwd = G.tdCwd||'';
    const node = getTreeNode(G.tdTree, cwd);
    const entries = Object.values(node.children||{}).sort((a,b)=> a.isDir!==b.isDir ? (a.isDir?-1:1) : a.name.localeCompare(b.name));
    const parts = cwd.replace(/\/$/,'').split('/').filter(Boolean);
    const sessions = G.tdSessions || [];
    bc.innerHTML = parts.map((p,i) => {
      const to = parts.slice(0,i+1).join('/') + '/';
      if(i === parts.length-1) return `<span class="cur">${esc(p)}</span>`;
      return `<a href="#" class="${i===0 && sessions.includes(p)?'dim':''}" onclick="navigateTree('${jsq(to)}');return false;">${esc(p)}</a>`;
    }).join('<span class="sep">/</span>');
    if(cwd){
      const up = cwd.replace(/\/$/,'').split('/'); up.pop();
      html += `<div class="file-row dir" data-nav="${esc(up.length ? up.join('/')+'/' : '')}"><span class="file-icon">${ico('up',15)}</span><span class="file-name">.. up one level</span></div>`;
    }
    for(const e of entries){ G.tdFiltered.push(e.path); html += tdRowHTML(e.name + (e.isDir?'/':''), e.path, e.isDir); }
    if(!entries.length) html += `<div class="empty-state">${cwd?'Empty folder':'No files indexed for this tape'}</div>`;
  }
  fl.innerHTML = html;
  fl.onclick = e => {
    const r = e.target.closest('.file-row'); if(!r) return;
    if(r.dataset.nav != null){ navigateTree(r.dataset.nav); return; }
    const cb = r.querySelector('input'), path = r.dataset.path;
    if(e.target === cb || !r.dataset.dir || e.target.closest('.file-icon')){
      if(e.target !== cb) cb.checked = !cb.checked;
      cb.checked ? G.tdSelected.add(path) : G.tdSelected.delete(path);
      r.classList.toggle('selected', cb.checked);
      updateTdSelSummary();
    } else navigateTree(path);
  };
  updateTdSelSummary();
}
function tdRowHTML(label, path, isDir){
  const sel = G.tdSelected.has(path);
  return `<div class="file-row${isDir?' dir':''}${sel?' selected':''}" data-path="${esc(path)}" data-dir="${isDir?1:''}">
    <input type="checkbox" class="file-check" aria-label="Select ${esc(label)}" ${sel?'checked':''}/>
    <span class="file-icon">${ico(isDir?'folder':'file',15)}</span><span class="file-name" title="${esc(path)}">${esc(label)}</span><span class="file-type">${isDir?'Folder':'File'}</span></div>`;
}
function navigateTree(path){ G.tdCwd = path; $('td-search').value=''; renderTapeDrawerFiles(''); }
function filterTapeDrawer(v){ renderTapeDrawerFiles(v); }
function updateTdSelSummary(){
  setTxt('td-sel-summary', G.tdSelected.size ? `${plural(G.tdSelected.size,'item')} selected` : 'No items selected');
  const b = $('td-restore-btn'); if(b) b.disabled = G.tdSelected.size===0;
}
function selectAllTapeFiles(){
  const f=($('td-search').value||'').trim().toLowerCase();
  if(f) G.tdFiles.filter(x=>x.toLowerCase().includes(f)).slice(0,500).forEach(p=>G.tdSelected.add(p));
  else Object.values(getTreeNode(G.tdTree, G.tdCwd||'').children||{}).forEach(e=>G.tdSelected.add(e.path));
  renderTapeDrawerFiles($('td-search').value||'');
}
function restoreSelected(){
  G.restorePending={paths:[...G.tdSelected], vol:G.tdVol, slot:G.tdSlot};
  openRestoreDestDrawer(false, true);
}
function restoreAll(){
  G.restorePending={paths:[], vol:G.tdVol, slot:G.tdSlot};
  openRestoreDestDrawer(true, true);
}

async function tapeDrawerAction(action, body){
  const acts=$('tape-drawer-inner');
  const actionLabels = {
    load:                    'Loading tape into drive (30–60s)',
    unload:                  'Unloading tape to slot (30–60s)',
    rewind:                  'Rewinding tape',
    reindex:                 'Re-indexing tape — load, read file list, unload (several minutes)',
    read_index:              'Reading tape index (several minutes)',
    update_loaded_inventory: 'Updating tape inventory',
    clean:                   'Running cleaning cycle (2+ minutes)',
    mail_export:             'Moving tape to mail slot',
    mail_import:             'Importing tape from mail slot',
  };
  const msg = actionLabels[action] || 'Working';
  tdResult(msg + '…', 'warn');
  G.activeAction = {msg: msg + '…', status:'working', _action: action, started: Date.now()};
  if(G.page==='library') renderPage();
  // Mechanical changer ops run as a background job — poll faster than the default cadence.
  if(action === 'load' || action === 'unload' || action === 'reindex') startChangerRapidPoll();

  const btns = acts ? [...acts.querySelectorAll('.left button, #td-actions button, #td-lifecycle button, #td-danger-actions button')] : [];
  btns.forEach(b => { b.disabled = true; });
  const fireAndPoll = (action === 'load' || action === 'unload' || action === 'reindex');

  let data;
  try {
    if(action==='load')         data=await api('/api/load','POST',body);
    else if(action==='unload')  data=await api('/api/unload','POST',body);
    else if(action==='reindex') data=await api('/api/tape_index/reindex','POST',body);
    else if(action==='rewind')  data=await api('/api/rewind','POST',body);
    else if(action==='read_index'){
      data=await api('/api/tape_index/read','POST',body);
      if(data.ok && G.tdVol) delete G.indexes[G.tdVol];
    }
    else if(action==='update_loaded_inventory'){
      data=await api('/api/tape_index/update_loaded','POST',body);
      if(data.ok && G.tdVol) delete G.indexes[G.tdVol];
    }
    else if(action==='clean')       data=await api('/api/cleaning/run','POST',body);
    else if(action==='mail_export') data=await api('/api/mail_slot/export','POST',body);
    else if(action==='mail_import') data=await api('/api/mail_slot/import','POST',body);
    else data = {ok:false, error:'Unknown action'};
  } catch(e) {
    data = {ok:false, error: String(e)};
  }

  if(!data.ok){
    tdResult(data.error || 'Error', 'bad');
    G.activeAction = {msg: data.error || 'Error', status:'error', _action: action};
    btns.forEach(b => { b.disabled = false; });
    await pollOnce();
    return;
  }
  if(fireAndPoll){
    // The mechanical operation now runs in the background; applyState clears the banner.
    tdResult(msg + '…', 'warn');
  } else {
    tdResult(data.detail || 'Done', 'ok');
    G.activeAction = {msg: data.detail || 'Done', status:'success', _action: action};
  }
  await pollOnce();
}

function startChangerRapidPoll(){
  if(G._changerRapidPolling) return;
  G._changerRapidPolling = true;
  let ticks = 0;
  const rapidPoll = async () => {
    await pollOnce();
    ticks++;
    if(G.state?.changer_job?.running && ticks < 180) setTimeout(rapidPoll, 2000);
    else G._changerRapidPolling = false;
  };
  setTimeout(rapidPoll, 2000);
}

// ── General library controls ────────────────────────────────────────────────
async function startInventory(mode='full'){
  const data=await api('/api/inventory/start','POST',{mode});
  if(!data.ok) alert(data.error);
  await pollOnce();
}
async function pauseInventory(){
  const data=await api('/api/inventory/pause','POST');
  if(!data.ok) alert(data.error);
  await pollOnce();
}
async function resumeInventory(){
  const data=await api('/api/inventory/resume','POST');
  if(!data.ok) alert(data.error);
  await pollOnce();
}
async function stopInventory(){
  if(!confirm('Stop inventory after the current step and safely unload any loaded tape?')) return;
  const data=await api('/api/inventory/stop','POST');
  if(!data.ok) alert(data.error);
  await pollOnce();
}
async function updateLoadedTapeInventory(mode='full'){
  const data=await api('/api/tape_index/update_loaded','POST',{mode});
  if(!data.ok) alert(data.error||'Failed to update loaded tape inventory.');
  await pollOnce();
}
async function checkDriveNow(){
  const data = await api('/api/drive/check','POST');
  if(!data.ok){ alert(data.error || 'Drive check failed.'); return; }
  if(data.drive_info) G.driveInfo = data.drive_info;
  if(data.state) applyState(data.state);
  if(G.page==='library') renderPage();
}

async function deleteTapeIndex(vol){
  if(!vol) return;
  if(!confirm(
    `Remove "${vol}" from the catalog?\n\n` +
    `The file index and file list are deleted. Data on the physical tape is untouched.\n\n` +
    `Permanent delete (all history) is a separate action.`
  )) return;
  const data = await api('/api/tape_index/delete','POST',{volume_tag:vol, permanent:false});
  if(!data.ok){ alert(data.error||'Delete failed'); return; }
  delete G.indexes[vol];
  G.indexMeta = (G.indexMeta||[]).filter(x=>x.volume_tag!==vol);
  closeTapeDrawer();
  await pollOnce();
}

async function permanentDeleteTapeIndex(vol){
  if(!vol) return;
  if(!confirm(
    `PERMANENT DELETE — "${vol}"\n\n` +
    `Destroys all catalog data for this tape:\n` +
    `  • File index and backup records\n  • Space usage history\n  • All metadata\n\n` +
    `The physical tape is not affected. This cannot be undone.`
  )) return;
  const typed = prompt(`Type  DELETE ${vol}  to confirm permanent deletion:`);
  if(!typed || typed.trim() !== `DELETE ${vol}`){
    alert('Confirmation did not match — permanent delete cancelled.');
    return;
  }
  const data = await api('/api/tape_index/delete','POST',{volume_tag: vol, permanent: true, confirm: `DELETE ${vol}`});
  if(!data.ok){ alert(data.error||'Permanent delete failed'); return; }
  delete G.indexes[vol];
  G.indexMeta = (G.indexMeta||[]).filter(x=>x.volume_tag!==vol);
  closeTapeDrawer();
  await pollOnce();
}

async function archiveTape(vol){
  if(!vol) return;
  if(!confirm(
    `Mark "${vol}" as archived (off-site)?\n\n` +
    `Catalog entry and file index are preserved; the tape stays browsable and restorable ` +
    `and appears under Archived. A Quick scan un-archives it when reinserted.`
  )) return;
  const data = await api('/api/tape_index/archive','POST',{volume_tag:vol});
  if(!data.ok){ alert(data.error||'Archive failed'); return; }
  G.indexMeta = (G.indexMeta||[]).map(m => m.volume_tag===vol ? {...m, purpose:'archived', present:false} : m);
  closeTapeDrawer();
  await pollOnce();
}

async function doRefresh(){
  await api('/api/refresh','POST');
  await pollOnce();
}
