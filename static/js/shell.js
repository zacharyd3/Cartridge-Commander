'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// SHELL — page routing, live-refresh decisions, top bar & sidebar status
// ══════════════════════════════════════════════════════════════════════════════
const PAGE_TITLES = {
  library:'Overview', backup:'Backup', restore:'Restore', schedule:'Schedules',
  media:'Tape catalog', retention:'Retention', log:'Activity', settings:'Settings',
};

function captureScheduleDraft(){
  G.scheduleDraft = {
    label: $('sc-label')?.value ?? G.scheduleDraft?.label ?? '',
    mode: $('sc-mode')?.value ?? G.scheduleDraft?.mode ?? 'weekly',
    dow: $('sc-dow')?.value ?? G.scheduleDraft?.dow ?? '0',
    dom: $('sc-dom')?.value ?? G.scheduleDraft?.dom ?? '1',
    hour: $('sc-hour')?.value ?? G.scheduleDraft?.hour ?? '2',
    min: $('sc-min')?.value ?? G.scheduleDraft?.min ?? '0',
    result: $('sc-result')?.textContent ?? G.scheduleDraft?.result ?? '',
  };
}

function applyScheduleDraft(){
  const d = G.scheduleDraft || {};
  if($('sc-label')) $('sc-label').value = d.label ?? '';
  if($('sc-mode')) $('sc-mode').value = d.mode ?? 'weekly';
  if($('sc-dow')) $('sc-dow').value = d.dow ?? '0';
  if($('sc-dom')) $('sc-dom').value = d.dom ?? '1';
  if($('sc-hour')) $('sc-hour').value = d.hour ?? '2';
  if($('sc-min')) $('sc-min').value = d.min ?? '0';
  updateSchedForm();
  if($('sc-result')) $('sc-result').textContent = d.result ?? '';
}

function showPage(name){
  if(G.page==='schedule') captureScheduleDraft();
  G.page = name;
  document.body.classList.remove('nav-open');
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  $('nav-'+name)?.classList.add('active');
  setTxt('crumb-page', PAGE_TITLES[name] || name);
  renderPage();
  $('content').scrollTop = 0; window.scrollTo(0,0);
  if(name==='schedule') loadSchedules();
  if(name==='settings') loadSettingsPage();
  if(name==='library'){
    Promise.all([ensureRecords(), ensureSchedules()]).then(()=>refreshIfOn(['library']));
  }
  if(name==='log') ensureRecords(true).then(()=>refreshIfOn(['log']));
  if(name==='media') Promise.all([ensureGfs(), pollLibraryData()]).then(()=>refreshIfOn(['media']));
  if(name==='retention') Promise.all([ensureSettings(true), ensureGfs(true)]).then(()=>refreshIfOn(['retention']));
  if(name==='backup') ensureSettings().then(()=>refreshIfOn(['backup']));
  if(name==='restore') pollLibraryData().then(()=>refreshIfOn(['restore']));
}

function renderPage(){
  if(G.page==='schedule') captureScheduleDraft();
  const c = $('content');
  // Preserve focus/caret of an input being typed into across the repaint.
  const active = document.activeElement;
  const focusId = active && c.contains(active) && active.id ? active.id : null;
  const caret = focusId && typeof active.selectionStart === 'number' ? active.selectionStart : null;
  const scrollY = window.scrollY;
  c.innerHTML = '';
  if(G.page==='library')        renderLibraryPage(c);
  else if(G.page==='backup')    renderBackupPage(c);
  else if(G.page==='restore')   renderRestorePage(c);
  else if(G.page==='schedule') { renderSchedulePage(c); applyScheduleDraft(); }
  else if(G.page==='media')     renderMediaPage(c);
  else if(G.page==='retention') renderRetentionPage(c);
  else if(G.page==='log')       renderLogPage(c);
  else if(G.page==='settings')  renderSettingsPage(c);
  // Rebuilding innerHTML can reset the scroll position — restore it so a background
  // poll refresh doesn't yank the page out from under someone scrolled further down.
  window.scrollTo(0, scrollY);
  setTimeout(() => { window.scrollTo(0, scrollY); }, 0);
  if(focusId){
    const n = $(focusId);
    if(n){ n.focus(); if(caret!=null && n.setSelectionRange) try{ n.setSelectionRange(caret, caret); }catch(e){} }
  }
}

// Fields the overview actually renders from /api/status — used to decide whether a
// poll needs a repaint at all. Deliberately excludes last_updated (changes every poll)
// so an idle library doesn't flash on every tick.
function _librarySnapshot(state){
  return JSON.stringify({
    slots: state.slots, drive: state.drive, summary: state.summary,
    backup_job: state.backup_job, changer_job: state.changer_job,
    format_job: state.format_job, inventory_job: state.inventory_job,
    actions: (state.actions||[]).slice(0,6),
  });
}

function pageWantsLiveRefresh(page, prevState, nextState){
  if(!prevState) return true;
  if(page==='library') return _librarySnapshot(prevState) !== _librarySnapshot(nextState);
  if(page==='media') return JSON.stringify([prevState.format_job, prevState.slots, prevState.drive]) !== JSON.stringify([nextState.format_job, nextState.slots, nextState.drive]);
  if(page==='log') return JSON.stringify((prevState.actions||[]).slice(0,40)) !== JSON.stringify((nextState.actions||[]).slice(0,40));
  if(page==='settings' || page==='schedule' || page==='retention') return false;
  if(page==='backup'){
    const pick = s => JSON.stringify({backup: s.backup_job||{}, verify: s.verify_job||{}});
    return pick(prevState) !== pick(nextState);
  }
  if(page==='restore'){
    return JSON.stringify(prevState.restore_job||{}) !== JSON.stringify(nextState.restore_job||{});
  }
  return false;
}

const BACKUP_PHASE_LABEL = {
  idle:'Idle', scanning:'Scanning sources', preparing:'Preparing', selecting_tape:'Selecting tape', loading_tape:'Loading tape',
  pre_hook:'Running pre-backup hook', erasing:'Erasing tape', rewinding:'Rewinding',
  streaming:'Writing to tape', indexing:'Building index', verifying:'Verifying',
  cancelling:'Cancelling', unloading:'Unloading tape', post_hook:'Running post-backup hook',
  completed:'Completed', failed:'Failed', cancelled:'Cancelled',
};

function updateTopbar(data){
  const s = data.summary||{};
  const bk = data.backup_job||{};
  const bkRunning = !!bk.running;
  const dot = $('dot-drive');
  if(dot) dot.className = 'dot ' + (s.online ? 'green' : 'red');
  setTxt('lbl-library', data.ok === false && !s.online ? 'Library unreachable' : s.online ? 'Library online' : 'Drive offline');
  const side = $('side-status');
  if(side){
    side.className = 'side-status ' + (s.online ? 'ok' : 'bad');
    side.innerHTML = `<span class="d"></span>${s.online ? 'Online' : 'Offline'}`;
  }

  const sp = data.drive_info?.space || G.driveInfo?.space || null;
  const lbl = $('lbl-loaded');
  if(lbl){
    if(s.loaded_volume){
      let t = `<span class="vol">${esc(s.loaded_volume)}</span>`;
      const parts = [];
      if(s.loaded_slot) parts.push(`slot ${s.loaded_slot}`);
      if(sp?.capacity_bytes != null) parts.push(`${hBytes(sp.used_bytes||0)} / ${hBytes(sp.capacity_bytes)} used`);
      if(sp?.remaining_bytes != null) parts.push(`${hBytes(sp.remaining_bytes)} free${sp.estimated?' est.':''}`);
      lbl.innerHTML = t + (parts.length ? ' ' + esc(parts.join(' · ')) : '');
    } else lbl.textContent = 'No tape loaded';
  }

  // Backup-in-progress chip
  let chip = $('lbl-backup-running');
  if(bkRunning){
    if(!chip){
      chip = el('span','status-chip warn');
      chip.id = 'lbl-backup-running';
      chip.onclick = () => showPage('backup');
      chip.setAttribute('role','button');
      $('chip-drive')?.after(chip);
    }
    const pct = Number(bk.percent||0);
    const phase = BACKUP_PHASE_LABEL[bk.status] || 'Backup running';
    chip.innerHTML = `<span class="dot amber pulse"></span>${esc(phase)}${bk.status==='streaming'?` · ${pct.toFixed(0)}%`:''}`;
  } else if(chip) chip.remove();

  const pill = $('nav-backup-pill');
  if(pill){
    if(bkRunning){ pill.classList.remove('hidden'); pill.textContent = bk.status==='streaming' ? `${Number(bk.percent||0).toFixed(0)}%` : '•••'; }
    else pill.classList.add('hidden');
  }

  const up = $('lbl-updated');
  if(up) up.textContent = data.last_updated ? `Updated ${new Date(data.last_updated*1000).toLocaleTimeString()}` : '';
  const sub = $('brand-sub');
  if(sub){
    const gen = sp?.lto_generation || (s.density && /LTO-?(\d)/i.exec(s.density)?.[1]);
    sub.textContent = `${s.total_slots ?? '—'} slots${gen ? ` · LTO-${gen}` : ''}`;
  }
}

function applyState(data){
  const prevState = G.state;
  G.state = data;
  updateTopbar(data);
  trackBackupSpeed(data.backup_job||{});

  // ── Clear activeAction banner once the operation is reflected in state ───
  if(G.activeAction && prevState){
    const act = G.activeAction._action;
    const cj = data.changer_job || {};

    if(G.activeAction.status === 'working'){
      // For load/unload/reindex: check if changer_job reports done
      if((act === 'load' || act === 'unload' || act === 'reindex') && cj.running === false && cj.started_at){
        if(cj.status === 'completed'){
          G.activeAction = {msg: cj.detail || 'Done', status:'success', _action: act};
          // Invalidate cached index so the drawer refreshes if reopened
          if(act === 'reindex' && G.tdVol) delete G.indexes[G.tdVol];
        } else if(cj.status === 'failed'){
          G.activeAction = {msg: cj.error || 'Failed', status:'error', _action: act};
        }
      } else if((act === 'load' || act === 'unload' || act === 'reindex') && cj.running && cj.detail){
        // Still running — surface the changer's live phase text instead of the
        // initial static message.
        if(G.activeAction.msg !== cj.detail) G.activeAction = {...G.activeAction, msg: cj.detail};
      }
    }

    if(G.activeAction.status === 'success'){
      const driveWasEmpty = prevState.drive?.empty ?? true;
      const driveNowEmpty = data.drive?.empty ?? true;
      let settled = false;
      if(act === 'load')        settled = driveWasEmpty  && !driveNowEmpty;
      else if(act === 'unload') settled = !driveWasEmpty && driveNowEmpty;
      else                      settled = true;  // reindex/rewind/etc: clear after one poll
      if(settled) G.activeAction = null;
    }

    if(G.activeAction && G.activeAction.status === 'error'){
      // Errors clear after one poll so the message is seen and the op can be retried
      G.activeAction = null;
    }
  }

  // A backup that just finished adds a record — refresh the cached list.
  if(prevState?.backup_job?.running && !data.backup_job?.running){
    ensureRecords(true).then(()=>refreshIfOn(['library','log']));
  }

  // Avoid full page redraws on every poll, which cause flashing and wipe out
  // open lists / in-progress browsing state.
  if(!prevState){ renderPage(); return; }
  if(pageWantsLiveRefresh(G.page, prevState, data)) renderPage();
}

// Throughput history isn't stored server-side; sample it from each poll while streaming.
function trackBackupSpeed(bk){
  const jobKey = bk.started_at || null;
  if(jobKey !== G._speedJob){ G._speedJob = jobKey; G.speedSamples = []; }
  if(bk.running && bk.status === 'streaming' && bk.speed_bps > 0){
    const last = G.speedSamples[G.speedSamples.length-1];
    const now = Date.now();
    if(!last || now - last.t > 1500) G.speedSamples.push({t: now, v: bk.speed_bps / (1024*1024)});
    if(G.speedSamples.length > 2000) G.speedSamples.splice(0, G.speedSamples.length - 2000);
  }
}
