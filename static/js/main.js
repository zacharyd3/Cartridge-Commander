'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// POLL LOOP & BOOT
// ══════════════════════════════════════════════════════════════════════════════
const LIBRARY_DATA_PAGES = ['library','media','restore'];

// Heavier catalog details (drive info, index metadata, per-tape history).
async function pollLibraryData(){
  const prevDriveInfo = G.driveInfo, prevIndexMeta = G.indexMeta, prevTapeHistory = G.tapeHistoryMap;
  const [diData, idxData, histData] = await Promise.all([
    api('/api/drive_info'),
    api('/api/tape_index'),
    api('/api/tape_history'),
  ]);
  if(diData.ok) G.driveInfo = diData.drive_info;
  if(idxData.ok) G.indexMeta = idxData.indexes||[];
  if(histData.ok) G.tapeHistoryMap = histData.history||{};
  return JSON.stringify(prevDriveInfo)   !== JSON.stringify(G.driveInfo) ||
         JSON.stringify(prevIndexMeta)   !== JSON.stringify(G.indexMeta) ||
         JSON.stringify(prevTapeHistory) !== JSON.stringify(G.tapeHistoryMap);
}

async function pollOnce(){
  const page = G.page;
  const data = await api('/api/status');
  if(data.drive_info) G.driveInfo = data.drive_info;
  applyState(data);

  // Only fetch the heavier catalog details when a page that shows them (or the tape
  // drawer) is open — avoids extra DOM churn and preserves work on e.g. Schedules.
  const drawerOpen = !!$('tape-drawer')?.classList.contains('open');
  if(LIBRARY_DATA_PAGES.includes(page) || drawerOpen){
    const changed = await pollLibraryData();
    // This data isn't covered by pageWantsLiveRefresh's snapshot — only repaint when it
    // actually changed, so an idle page doesn't flash every poll.
    if(changed && G.page === page && page !== 'restore') renderPage();
  }
}

// Boot
hydrateIcons();
ensureSettings().then(s => {
  if(s?.sg_device) setTxt('side-sg', s.sg_device);
  else setTxt('side-sg', 'not set');
  if(G.page === 'backup') renderPage();
});
Promise.all([ensureRecords(), ensureSchedules()]).then(() => refreshIfOn(['library']));
pollOnce();
setInterval(pollOnce, window.APP_CONFIG.pollingMs);
// Records change slowly; refresh them in the background once a minute.
setInterval(() => ensureRecords().then(() => refreshIfOn(['library'])), 60000);

// Ticks the "elapsed" readout on the active-action banner every second, independent of
// the poll cadence, so long mechanical tape ops visibly keep moving.
setInterval(() => {
  const n = $('action-elapsed');
  if(n && G.activeAction?.status === 'working' && G.activeAction.started){
    n.textContent = fmtSec((Date.now()-G.activeAction.started)/1000) + ' elapsed';
  }
}, 1000);

document.addEventListener('keydown', e => {
  if(e.key !== 'Escape') return;
  if($('restore-dest-drawer')?.classList.contains('open')) closeRestoreDestDrawer();
  else if($('tape-drawer')?.classList.contains('open')) closeTapeDrawer();
  else document.body.classList.remove('nav-open');
});
document.addEventListener('click', e => {
  if(document.body.classList.contains('nav-open') && !e.target.closest('.sidebar') && !e.target.closest('.menu-btn')) document.body.classList.remove('nav-open');
});
