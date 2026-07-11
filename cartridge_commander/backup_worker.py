"""backup_worker module (split from the original monolithic app.py)."""

import os
import re
import time
import threading
import subprocess
import fcntl
from typing import Any, Dict, List, Optional
from .config import AUTO_REWIND_AFTER, AUTO_REWRITE_ON_FULL, BACKUP_LOG_LEVEL_DEFAULT, CHANGER, COMMAND_TIMEOUT, ERASE_BEFORE_BACKUP, INCREMENTAL_DIR, POST_BACKUP_HOOK, PRE_BACKUP_HOOK, TAPE, TAPE_BLOCK_BYTES, VERIFY_AFTER_BACKUP
from . import state as shared_state


def _pick_backup_tape() -> Dict[str, Any]:
    """Choose the best available tape to back up to when no tape is loaded.

    Priority order:
      1. Tape explicitly marked purpose='available' — cleanest choice.
      2. GFS-recyclable tape (oldest completed backup beyond retention).
      3. Tape with no backup record at all (never been used).
      4. Tape with the oldest last_backup timestamp (least recently used).
    Never picks cleaning tapes.  Raises TapeError if nothing is found.
    Returns a dict with 'volume_tag' and 'slot'.
    """
    from .records import gfs_get_recyclable
    from .db import list_all_known_indexes
    from .state import TapeError, is_cleaning_volume_tag
    from .changer import refresh_state
    state = refresh_state()
    # Build map of slot info keyed by volume_tag for tapes physically present
    slot_map: Dict[str, Dict[str, Any]] = {
        str(s.get("volume_tag") or "").strip(): s
        for s in (state.get("slots") or [])
        if s.get("full") and not s.get("is_import_export") and s.get("volume_tag")
           and not is_cleaning_volume_tag(str(s.get("volume_tag") or ""))
    }
    if not slot_map:
        raise TapeError("No non-cleaning tapes found in any library slot.")

    recyclable_set = set(gfs_get_recyclable())
    known = {i["volume_tag"]: i for i in list_all_known_indexes()
             if i.get("volume_tag") and not is_cleaning_volume_tag(i["volume_tag"])}

    # FIX: snapshot drive_history inside the lock once so we can query it
    # without holding the lock across the whole candidate-building loop.
    with shared_state._drive_history_lock:
        drive_hist_snap = dict(shared_state._drive_history)

    # LTO-6 native capacity (2.5 TB).  Used as the fallback when the index
    # has no capacity_bytes entry (e.g. tapes that were never queried via
    # sg_logs).  Adjust via env var LTO_NATIVE_CAPACITY_TB if needed.
    _LTO_NATIVE_BYTES = float(os.getenv("LTO_NATIVE_CAPACITY_TB", "2.5")) * 1e12

    candidates = []
    skipped_full: List[str] = []
    for vol, slot_info in slot_map.items():
        idx     = known.get(vol, {})
        dh      = drive_hist_snap.get(vol, {})
        purpose = str(idx.get("purpose") or "").strip().lower()

        is_recyclable = vol in recyclable_set
        is_available  = purpose in ("available", "recyclable") or is_recyclable
        never_used    = (dh.get("backup_count") or 0) == 0 and not idx.get("written_at")

        # FIX: read last_backup from drive_history, not from the tape index.
        # The index field last_backup_ts was never written before this patch,
        # so all tapes scored 0 and the picker always chose the same tape
        # (the one that sorted first alphabetically after bucket ordering).
        last_bk = dh.get("last_backup") or idx.get("last_backup_ts") or 0

        # FIX: skip tapes whose accumulated logical bytes exceed the tape's
        # native capacity (with 5% headroom).  total_backup_bytes accumulates
        # the uncompressed source size across all backups on this tape.
        # Even with good compression a tape cannot hold more data than its
        # native rating.  Without this check the picker kept appending to
        # KB2785L6 well past the 2.5 TB mark because nothing ever excluded it.
        capacity   = float(idx.get("capacity_bytes") or 0) or _LTO_NATIVE_BYTES
        used_bytes = float(dh.get("total_backup_bytes") or idx.get("used_bytes") or 0)
        is_full    = (used_bytes >= capacity * 0.95) and not is_recyclable

        if is_full:
            skipped_full.append(vol)
            continue

        # Priority bucket: lower = preferred
        if is_available:
            bucket = 0
        elif never_used or not idx:
            bucket = 1
        else:
            bucket = 2

        score = last_bk   # within same bucket, prefer oldest (smallest ts)
        candidates.append({
            "volume_tag": vol,
            "slot":       int(slot_info.get("slot") or 0),
            "bucket":     bucket,
            "score":      score,
            "purpose":    purpose or "unknown",
        })

    if skipped_full:
        import logging
        logging.getLogger(__name__).info(
            "_pick_backup_tape: skipped full tape(s): %s", ", ".join(skipped_full)
        )

    if not candidates:
        if skipped_full:
            raise TapeError(
                f"No writable tape found — {len(skipped_full)} tape(s) are at capacity "
                f"({', '.join(skipped_full)}). "
                "Erase a tape or mark one as 'available' to continue."
            )
        raise TapeError("No suitable backup tape found in the library.")

    candidates.sort(key=lambda x: (x["bucket"], x["score"], x["volume_tag"]))
    return candidates[0]


def _find_return_slot(vol: str, exclude_slot: Optional[int] = None) -> Optional[int]:
    """Find the best slot to unload a tape back to after backup.

    Priority:
      1. The slot we loaded it from (last_seen_slot in catalog).
      2. Any empty non-IE storage slot.
      3. The mail slot if it's empty.
    Returns None if no slot is available (caller should warn and leave tape in drive).
    """
    from .db import load_tape_index
    from .changer import get_mail_slot_info, refresh_state
    state = refresh_state()
    slots = state.get("slots") or []

    # 1. Try last known slot from catalog
    idx = load_tape_index(vol)
    last_slot = (idx or {}).get("last_seen_slot") if idx else None
    if last_slot and last_slot != exclude_slot:
        slot_info = next((s for s in slots if s.get("slot") == int(last_slot)), None)
        if slot_info and not slot_info.get("full"):
            return int(last_slot)

    # 2. Any empty storage slot (not IE)
    empty_slots = [s for s in slots
                   if not s.get("full") and not s.get("is_import_export")
                   and s.get("slot") != exclude_slot]
    if empty_slots:
        return int(empty_slots[0]["slot"])

    # 3. Mail slot if empty
    mail = get_mail_slot_info(state)
    if mail and not mail.get("full"):
        return int(mail["slot"])

    return None


# ---------------------------------------------------------------------------
# Backup worker
# ---------------------------------------------------------------------------

def backup_worker(paths: List[str], backup_mode: str = "full",
                  job_id: str = "", label: str = "", log_level: str = BACKUP_LOG_LEVEL_DEFAULT) -> None:
    from .records import add_backup_record
    from .db import save_tape_index, update_tape_index_metadata
    from .verify_worker import verify_worker
    from .drive_history import _is_tape_full_error, _mt_status_shows_eot, _record_backup_done, _save_last_known_loaded_slot, _switch_to_rewrite_candidate, build_tape_space_info, bytes_written_for_volume, space_meta_from_info
    from .state import TapeError, append_backup_log, backup_log_allows, bytes_human, is_cleaning_volume_tag, log_action, normalize_backup_log_level, now_ts, run_cmd, secs_human, set_backup_state
    from .mqtt import publish_state_to_mqtt
    from .changer import ensure_under_backup_root, estimate_path_size, refresh_state
    from .notify import notify_backup_failure, notify_backup_success
    from .settings import build_backup_dirname
    selected = [ensure_under_backup_root(p) for p in paths]
    rels     = [os.path.relpath(p, "/") for p in selected]
    total_size = sum(estimate_path_size(p) for p in selected)
    start    = time.time()
    shared_state._stop_requested = False
    vol      = (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "")
    if not job_id:
        job_id = f"{vol or 'nolabel'}_{int(start)}"
    record_id = str(int(start * 1000))

    # Build the archive prefix directory name now (uses vol + start time).
    # Vol may change below if auto-load picks a different tape, so we'll
    # recompute it once the final vol is known before building the tar command.
    _backup_dirname: str = ""   # set after vol is finalised

    # Track whether we auto-loaded a tape so we can auto-unload it when done
    _auto_loaded_slot: Optional[int] = None

    if is_cleaning_volume_tag(vol):
        raise TapeError(f"Tape {vol} is a cleaning tape and cannot be written to.")

    log_level = normalize_backup_log_level(log_level)
    set_backup_state(
        running=True, status="preparing", selected_paths=selected,
        bytes_total=total_size, bytes_written=0, percent=0.0,
        speed_bps=0.0, eta_seconds=None,
        started_at=now_ts(), finished_at=None,
        last_message="Preparing…", log=[], error=None, log_level=log_level,
    )
    append_backup_log(f"Backup [{backup_mode}] for {len(selected)} path(s) on {vol or '(no tape)'}.", level="minimal")
    publish_state_to_mqtt(refresh_state())

    bw = 0
    verify_errors = 0
    verified = False

    try:
        # ── Auto-select and load tape if drive is empty ──────────────────────
        refresh_state()
        drive_state = (shared_state._state_cache.get("drive") or {})
        if drive_state.get("empty", True):
            append_backup_log("No tape in drive — selecting tape automatically…", level="minimal")
            set_backup_state(status="selecting_tape", last_message="Selecting tape…")
            publish_state_to_mqtt(refresh_state())
            try:
                chosen = _pick_backup_tape()
                append_backup_log(
                    f"Auto-selected {chosen['volume_tag']} from slot {chosen['slot']} "
                    f"(priority: {chosen['purpose']}).", level="minimal"
                )
                set_backup_state(status="loading_tape",
                                 last_message=f"Loading {chosen['volume_tag']} from slot {chosen['slot']}…")
                publish_state_to_mqtt(refresh_state())
                run_cmd(["mtx", "-f", CHANGER, "load", str(chosen["slot"]), "0"],
                        timeout=max(COMMAND_TIMEOUT, 120))
                _save_last_known_loaded_slot(chosen["slot"])
                _auto_loaded_slot = chosen["slot"]
                time.sleep(3)
                refresh_state()
                vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "") or chosen["volume_tag"]
                append_backup_log(f"Tape {vol} loaded from slot {chosen['slot']}.", level="minimal")
            except TapeError as e:
                raise TapeError(f"Could not auto-select a tape: {e}")

        # ── Pre-backup hook ──────────────────────────────────────────────────
        if PRE_BACKUP_HOOK:
            set_backup_state(status="pre_hook")
            publish_state_to_mqtt(refresh_state())
            if not run_hook(PRE_BACKUP_HOOK, "pre-backup"):
                raise TapeError("Pre-backup hook failed — aborting.")

        # ── Rewind ──────────────────────────────────────────────────────────
        append_backup_log("Rewinding tape before backup.")
        run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

        if ERASE_BEFORE_BACKUP:
            append_backup_log("Erasing tape…")
            set_backup_state(status="erasing")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mt", "-f", TAPE, "erase"], timeout=7200)
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

        # ── Build incremental args ───────────────────────────────────────────
        extra_args, snap_file = incremental_tar_args(selected, job_id, backup_mode)
        if backup_mode != "full":
            append_backup_log(f"Incremental mode '{backup_mode}' — snapshot: {snap_file}", level="normal")

        # ── Compute the archive prefix directory name ────────────────────────
        # Every file in the archive is stored under a unique top-level directory
        # so that restoring it always produces an isolated, identifiable folder.
        # The name follows the same pattern as the restore subfolder setting.
        _backup_dirname = build_backup_dirname(
            volume_tag=vol, start_ts=start, label=label
        )
        # GNU tar --transform rewrites archive member paths without touching the
        # source filesystem.  We prepend the dirname to every archived path.
        # ORDERING: --transform must come AFTER --listed-incremental in the arg
        # list.  With --listed-incremental, tar first evaluates which files to
        # include by comparing source paths against the snapshot (no transform
        # applied), then streams the selected files applying the transform to
        # their names as it writes them.  Placing --transform first on some tar
        # versions causes it to also attempt to match snapshot paths against the
        # transformed names, producing empty archives.
        _transform_expr = f"s|^|{_backup_dirname}/|"
        # extra_args currently holds the --listed-incremental arg (if any);
        # append --transform after it so the ordering is always correct.
        extra_args = extra_args + [f"--transform={_transform_expr}"]
        append_backup_log(
            f"Archive prefix: {_backup_dirname}/ "
            f"(restoring will create {_backup_dirname}/ in the restore root)",
            level="minimal",
        )

        # ── Stream to tape ───────────────────────────────────────────────────
        #
        # Architecture: fully kernel-managed pipeline, Python is NOT in the data path.
        #
        #   tar -C / -cf - --sparse [paths]
        #     └─ stdout ──► mbuffer -m 512M -s 512k -P 75   (smoothing ring buffer)
        #                     └─ stdout ──► dd bs=512k of=/dev/nst0
        #
        # If mbuffer is present: it handles both buffering AND progress stats via stderr.
        #   -P 75  — don't start writing to tape until buffer is 75% full; this gives
        #            the tape drive a large burst to start with and reduces shoe-shining.
        #   -A     — async I/O: separate threads for input and output sides of the buffer,
        #            so a momentary read stall on the filesystem doesn't stall the tape.
        #   -v 1   — emit periodic stats lines to stderr so we can parse MB/s without pv.
        #
        # If mbuffer is absent: fall back to pv | dd (pv provides the byte counter).
        #
        # pv is only used when mbuffer is NOT present — adding pv between tar and mbuffer
        # introduces an extra pipe hop and process for no benefit since mbuffer already
        # reports stats.
        #
        # All inter-process pipe buffers are enlarged to 1 MiB via fcntl F_SETPIPE_SZ.
        # The default 64 KiB kernel pipe buffer can cause tar to block waiting for the
        # next process to drain it, especially during filesystem metadata reads.
        #
        # tar --sparse detects and efficiently archives sparse files (VM disk images,
        # database files with pre-allocated space) without expanding empty regions.
        #
        # No software compression — LTO hardware compression is always faster and
        # produces better ratios than software compression on typical data.

        set_backup_state(status="streaming")
        append_backup_log("Starting tar → tape pipeline.", level="minimal")
        publish_state_to_mqtt(refresh_state())

        _TAPE_BLOCK_BYTES = TAPE_BLOCK_BYTES
        _MBUF_SIZE        = os.getenv("TL_MBUF_SIZE", "512M")  # larger default buffer
        _MBUF_FILL_PCT    = os.getenv("TL_MBUF_FILL_PCT", "75")  # fill % before writing
        _has_mbuffer = subprocess.run(["which", "mbuffer"], capture_output=True).returncode == 0
        _has_pv      = subprocess.run(["which", "pv"],      capture_output=True).returncode == 0

        # Sparse file detection: tar --sparse makes tar detect holes in files and
        # represent them as sparse regions in the archive, saving tape space for
        # VM images, database files, and pre-allocated files.
        # This is always safe — non-sparse files are archived normally.
        _SPARSE_ARGS = ["--sparse"]

        # Optional: skip extended attributes / ACLs (faster on NFS/Samba mounts with
        # many small files, but loses xattr data — off by default).
        _SKIP_XATTRS = os.getenv("TL_SKIP_XATTRS", "false").lower() == "true"
        _XATTR_ARGS  = ["--no-acls", "--no-xattrs", "--no-selinux"] if _SKIP_XATTRS else []

        # Temp file for tar's verbose file list (avoids the stderr-pipe deadlock).
        # Written to /tmp, not the backup array — negligible size.
        import tempfile
        _tar_log_fd, _tar_log_path = tempfile.mkstemp(prefix="tl2000_tar_", suffix=".log")
        os.close(_tar_log_fd)

        # tar: write stdout into the pipeline; verbose file list goes to a temp log file
        tar_cmd = (["tar", "-C", "/", "-cvf", "-"]
                   + _SPARSE_ARGS + _XATTR_ARGS + extra_args + rels)

        # dd: final writer — large block size, write directly to tape device.
        dd_cmd = ["dd", f"bs={_TAPE_BLOCK_BYTES}", f"of={TAPE}", "iflag=fullblock", "status=progress"]

        def _try_set_pipe_size(fd, size: int = 1048576) -> None:
            """Increase a pipe's kernel buffer to reduce blocking between stages.
            F_SETPIPE_SZ = 1031, F_GETPIPE_SZ = 1032 (Linux-specific).
            Silently ignored if unsupported (older kernels, non-Linux)."""
            try:
                fcntl.fcntl(fd, 1031, size)
            except Exception:
                pass

        if _has_mbuffer:
            # mbuffer replaces pv — it buffers AND reports stats
            # -s: block size (must match tape block size)
            # -m: total ring buffer size
            # -P: start writing when buffer reaches this % full (reduces shoe-shining)
            # -v 1: emit one stats line per second to stderr
            # -q: suppress the summary at exit (we log it ourselves)
            mbuf_cmd = [
                "mbuffer",
                "-s", str(_TAPE_BLOCK_BYTES),
                "-m", _MBUF_SIZE,
                "-P", str(_MBUF_FILL_PCT),
                "-v", "1",
                "-q",
            ]
            _pipeline_tools = ["mbuffer", "dd"]
        else:
            mbuf_cmd = None
            # Only use pv when mbuffer is absent
            _pipeline_tools = (["pv", "dd"] if _has_pv else ["dd"])

        append_backup_log(
            f"Pipeline: tar --sparse | {' | '.join(_pipeline_tools)} → {TAPE}  "
            f"(block={_TAPE_BLOCK_BYTES//1024}KiB"
            f"{', buf=' + _MBUF_SIZE + ' fill=' + str(_MBUF_FILL_PCT) + '%' if _has_mbuffer else ''}"
            f"{', skip_xattrs' if _SKIP_XATTRS else ''}"
            f")",
            level="minimal",
        )

        # ── Spawn processes ──────────────────────────────────────────────────
        _tar_log_fh = open(_tar_log_path, "wb")

        tar_proc = subprocess.Popen(
            tar_cmd,
            stdout=subprocess.PIPE,
            stderr=_tar_log_fh,
            close_fds=True,
        )
        shared_state._tar_proc = tar_proc

        prev_stdout = tar_proc.stdout
        # Enlarge tar→next pipe buffer
        _try_set_pipe_size(prev_stdout.fileno())

        pv_proc   = None
        mbuf_proc = None

        if _has_mbuffer:
            # tar → mbuffer → dd  (pv not used)
            mbuf_proc = subprocess.Popen(
                mbuf_cmd,
                stdin=prev_stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            prev_stdout.close()
            prev_stdout = mbuf_proc.stdout
            _try_set_pipe_size(prev_stdout.fileno())
        elif _has_pv:
            # tar → pv → dd  (mbuffer not available)
            pv_proc = subprocess.Popen(
                ["pv", "-n", "-F", "%b", "-i", "2"],
                stdin=prev_stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            prev_stdout.close()
            prev_stdout = pv_proc.stdout
            _try_set_pipe_size(prev_stdout.fileno())

        dd_proc = subprocess.Popen(
            dd_cmd,
            stdin=prev_stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        prev_stdout.close()

        # ── Background thread: drain progress stderr ─────────────────────────
        # When mbuffer is present: parse mbuffer's "-v 1" stats from its stderr.
        #   Format: "mbuffer: in @ X.XX MB/s, out @ X.XX MB/s, buffer X.X% full"
        #   We also need dd's byte count, so we drain dd stderr separately.
        # When only pv is present: parse pv's byte count from its stderr.
        # When neither: parse dd's byte count from its stderr.
        #
        # All reads use os.read() on raw fds — no Python IO buffering delay.
        _pv_bw_ref    = [0]     # bytes written (updated by drain thread)
        _dd_speed_ref = [0.0]   # speed in bytes/s
        _pv_stderr_lines = []

        # If mbuffer is present, we also need a dedicated dd stderr drain thread
        # (dd's progress lines give us the definitive byte count written to tape).
        _dd_stderr_extra: List[str] = []

        def _drain_dd_stderr_extra():
            """Drain dd stderr when mbuffer is handling the main progress drain."""
            fd = dd_proc.stderr.fileno() if dd_proc.stderr else None
            if fd is None:
                return
            buf = b""
            while True:
                try:
                    chunk = os.read(fd, 4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf or b"\r" in buf:
                        sep = b"\n" if b"\n" in buf else b"\r"
                        line_b, buf = buf.split(sep, 1)
                        line = line_b.decode(errors="ignore").strip()
                        if not line:
                            continue
                        _dd_stderr_extra.append(line)
                        # Parse byte count for progress tracking
                        m = re.match(r'(\d+)\s+bytes.*copied', line)
                        if m:
                            _pv_bw_ref[0] = int(m.group(1))
                        sm = re.search(r'([\d.]+)\s*(B|kB|MB|GB)/s', line)
                        if sm:
                            val = float(sm.group(1))
                            mult = {'B':1,'kB':1e3,'MB':1e6,'GB':1e9}.get(sm.group(2),1)
                            _dd_speed_ref[0] = val * mult
                except OSError:
                    break

        def _drain_progress_stderr():
            """Read progress data from mbuffer stderr, pv stderr, or dd stderr."""
            if mbuf_proc and mbuf_proc.stderr:
                # mbuffer -v 1 emits: "mbuffer: in @ X.XX MB/s, out @ X.XX MB/s, X.X% full"
                src_proc  = mbuf_proc
                src_label = "mbuffer"
            elif pv_proc and pv_proc.stderr:
                src_proc  = pv_proc
                src_label = "pv"
            else:
                src_proc  = dd_proc
                src_label = "dd"

            src_fd = src_proc.stderr.fileno() if src_proc and src_proc.stderr else None
            if src_fd is None:
                return

            buf = b""
            while True:
                try:
                    chunk = os.read(src_fd, 4096)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf or b"\r" in buf:
                        sep = b"\n" if b"\n" in buf else b"\r"
                        line_b, buf = buf.split(sep, 1)
                        line = line_b.decode(errors="ignore").strip()
                        if not line:
                            continue
                        if src_label == "mbuffer":
                            # "mbuffer: in @ 125.40 MB/s, out @ 124.80 MB/s, buffer 78.2% full"
                            # Extract outbound speed (what tape is seeing)
                            out_m = re.search(r'out\s*@\s*([\d.]+)\s*(B|kB|MB|GB)/s', line)
                            if out_m:
                                val = float(out_m.group(1))
                                mult = {'B':1,'kB':1e3,'MB':1e6,'GB':1e9}.get(out_m.group(2),1)
                                _dd_speed_ref[0] = val * mult
                            # Byte count comes from dd stderr drain, not mbuffer
                        elif src_label == "pv":
                            digits = line.replace(",", "").split()[0]
                            if digits.isdigit():
                                _pv_bw_ref[0] = int(digits)
                        else:
                            # dd
                            m = re.match(r'(\d+)\s+bytes.*copied', line)
                            if m:
                                _pv_bw_ref[0] = int(m.group(1))
                            sm = re.search(r'([\d.]+)\s*(B|kB|MB|GB)/s', line)
                            if sm:
                                val = float(sm.group(1))
                                mult = {'B':1,'kB':1e3,'MB':1e6,'GB':1e9}.get(sm.group(2),1)
                                _dd_speed_ref[0] = val * mult
                        _pv_stderr_lines.append(line)
                        if len(_pv_stderr_lines) > 200:
                            del _pv_stderr_lines[:-200]
                except OSError:
                    break

        pv_drain = threading.Thread(target=_drain_progress_stderr, daemon=True)
        pv_drain.start()

        # When mbuffer handles the main drain, dd stderr needs its own thread
        # to provide the authoritative byte count written to tape.
        _dd_extra_drain = None
        if mbuf_proc:
            _dd_extra_drain = threading.Thread(target=_drain_dd_stderr_extra, daemon=True)
            _dd_extra_drain.start()

        # ── Background thread: collect tar's verbose file list from log file ─
        _tar_entry_count = [0]
        _tar_last_entry  = [""]
        _tar_log_reader_stop = threading.Event()
        def _tail_tar_log():
            try:
                with open(_tar_log_path, "rb") as fh:
                    buf = b""
                    while not _tar_log_reader_stop.is_set():
                        chunk = fh.read(65536)
                        if chunk:
                            buf += chunk
                            while b"\n" in buf:
                                line_b, buf = buf.split(b"\n", 1)
                                line = line_b.decode(errors="ignore").strip()
                                if not line:
                                    continue
                                _tar_entry_count[0] += 1
                                _tar_last_entry[0] = line
                                if backup_log_allows("verbose"):
                                    append_backup_log(f"Archived: {line}", level="verbose")
                                elif backup_log_allows("normal") and _tar_entry_count[0] % 500 == 0:
                                    append_backup_log(
                                        f"Archived {_tar_entry_count[0]:,} entries… last: {line[-100:]}",
                                        level="normal",
                                    )
                        else:
                            time.sleep(0.2)
            except Exception:
                pass

        tar_log_thread = threading.Thread(target=_tail_tar_log, daemon=True)
        tar_log_thread.start()

        # ── Progress polling loop — waits for dd to finish ───────────────────
        cancel_requested = False
        stderr_lines = []   # keep for compat with rc-check block below
        _last_bw = 0
        _last_bw_ts = time.time()
        _rolling_speed = 0.0

        try:
            while dd_proc.poll() is None:
                time.sleep(2)

                if shared_state._stop_requested and not cancel_requested:
                    append_backup_log("Cancel requested — terminating pipeline.", level="minimal")
                    for proc in [tar_proc, pv_proc, mbuf_proc, dd_proc]:
                        if proc and proc.poll() is None:
                            try:
                                proc.terminate()
                            except Exception:
                                pass
                    cancel_requested = True
                    set_backup_state(status="cancelling",
                                     last_message="Cancelling backup…",
                                     eta_seconds=None)
                    publish_state_to_mqtt(refresh_state())
                    break

                # Byte count from drain thread (pv or dd progress parsing)
                bw = _pv_bw_ref[0]
                now_t = time.time()
                elapsed = max(now_t - start, 0.001)

                # Rolling speed over the last interval (more accurate than total average)
                interval = max(now_t - _last_bw_ts, 0.001)
                interval_bytes = bw - _last_bw
                if interval_bytes > 0:
                    _rolling_speed = interval_bytes / interval
                elif _dd_speed_ref[0] > 0:
                    # Fall back to dd's own speed report when pv isn't available
                    _rolling_speed = _dd_speed_ref[0]
                _last_bw = bw
                _last_bw_ts = now_t

                speed = _rolling_speed if _rolling_speed > 0 else (bw / elapsed if bw > 0 else 0.0)
                pct   = min(bw / total_size * 100.0, 100.0) if total_size > 0 and bw > 0 else 0.0
                eta   = int((total_size - bw) / speed) if speed > 0 and total_size > bw > 0 else None
                entries = _tar_entry_count[0]
                set_backup_state(
                    bytes_written=bw, percent=pct,
                    speed_bps=speed, eta_seconds=eta,
                    status="streaming",
                    last_message=(
                        f"{bytes_human(bw)} / {bytes_human(total_size)} "
                        f"— {bytes_human(speed)}/s "
                        f"— {entries:,} files "
                        f"— ETA {secs_human(eta)}"
                    ),
                )
                publish_state_to_mqtt(refresh_state())

        finally:
            # Stop the tar log tailer
            _tar_log_reader_stop.set()
            tar_log_thread.join(timeout=5)
            _tar_log_fh.close()
            # Do NOT delete _tar_log_path here — the index step reads it to build
            # the file list without re-reading the whole tape. It will be cleaned up
            # after indexing (or in the outer except/finally).
            shared_state._tar_proc = None

        # ── Wait for all pipeline stages to finish ───────────────────────────
        # Join drain threads first — they own stderr fds
        pv_drain.join(timeout=15)
        if _dd_extra_drain:
            _dd_extra_drain.join(timeout=15)

        tar_rc = tar_proc.wait(timeout=120)
        if pv_proc:
            pv_proc.wait(timeout=30)
        if mbuf_proc:
            # mbuffer stderr was being drained by pv_drain thread — don't read it again.
            # Just wait for it to exit and check the return code.
            mbuf_rc = mbuf_proc.wait(timeout=60)
            if mbuf_rc not in (0, -15) and not cancel_requested:
                mbuf_last = "\n".join(_pv_stderr_lines[-5:])
                append_backup_log(f"mbuffer exited {mbuf_rc}: {mbuf_last[-200:]}", level="minimal")

        # dd stderr:
        #   - mbuffer present: _dd_extra_drain read dd stderr → use _dd_stderr_extra
        #   - pv present: pv_drain was on pv stderr → dd stderr still readable
        #   - neither: pv_drain was on dd stderr → use _pv_stderr_lines
        dd_err_out = ""
        if mbuf_proc:
            dd_err_out = "\n".join(_dd_stderr_extra[-10:])
        elif pv_proc:
            try:
                dd_err_out = (dd_proc.stderr.read() or b"").decode(errors="ignore").strip()
            except Exception:
                pass
        else:
            dd_err_out = "\n".join(_pv_stderr_lines[-10:])

        dd_rc = dd_proc.wait(timeout=60)

        # Final byte count:
        #   - mbuffer+dd: _dd_stderr_extra has dd's byte count (most accurate)
        #   - pv or dd only: _pv_bw_ref has it
        if mbuf_proc and _pv_bw_ref[0] > 0:
            bw = _pv_bw_ref[0]  # updated by _drain_dd_stderr_extra
        else:
            bw = _pv_bw_ref[0] if _pv_bw_ref[0] > 0 else total_size

        # tar stderr was written to _tar_log_path; the finally block may have deleted it
        # so we grab what the tail thread already captured rather than re-opening the file
        stderr_lines = list(_pv_stderr_lines) if not pv_proc else []  # dd lines if no pv

        rc = tar_rc   # primary exit code for error check below

        # tar stderr was captured to _tar_log_path (not to stderr_lines which holds dd/pv progress).
        # Read the last portion of the tar log file for genuine tar error messages.
        _tar_error_lines = []
        try:
            if os.path.exists(_tar_log_path):
                with open(_tar_log_path, "rb") as _tlf:
                    _tlf.seek(0, 2)
                    _tail_size = min(_tlf.tell(), 8192)
                    _tlf.seek(-_tail_size, 2)
                    _tar_error_lines = [
                        l.decode(errors="ignore").strip()
                        for l in _tlf.read().splitlines()
                        if l.strip()
                    ][-20:]
        except Exception:
            pass

        # Check dd for tape-full — dd exits non-zero with ENOSPC when tape is full.
        # Some drives/kernels instead report a bare "Input/output error" for the
        # same condition (hit more often on bigger multi-folder backups that run
        # past where a smaller single-folder backup used to stop) — confirm via
        # `mt status` EOD/EOT flags before treating that ambiguous case as full.
        if dd_rc not in (0, -15) and not cancel_requested:
            tape_full = _is_tape_full_error(Exception(dd_err_out))
            if not tape_full and "input/output error" in dd_err_out.lower():
                tape_full = _mt_status_shows_eot()
                if tape_full:
                    append_backup_log(
                        "dd reported a bare I/O error; mt status confirms EOD/EOT — treating as tape-full.",
                        level="normal",
                    )
            if AUTO_REWRITE_ON_FULL and tape_full:
                append_backup_log(f"Tape full detected (dd rc={dd_rc}): {dd_err_out[:200]}", level="minimal")
                append_backup_log("Switching to oldest available/recyclable tape and restarting.", level="minimal")
                _switch_to_rewrite_candidate(vol)
                return backup_worker(selected, backup_mode=backup_mode, job_id=job_id, label=label, log_level=log_level)
            elif tape_full:
                append_backup_log(f"Tape full detected (dd rc={dd_rc}): {dd_err_out[:200]}", level="minimal")
                raise TapeError("Tape is full. Load a new/recyclable tape and start the backup again.")
            elif dd_err_out:
                append_backup_log(f"dd error (rc={dd_rc}): {dd_err_out[:300]}", level="minimal")
                raise TapeError(f"dd write to tape failed (rc={dd_rc}): {dd_err_out[:200]}")

        if cancel_requested:
            elapsed_total = max(time.time() - start, 0.001)
            set_backup_state(
                running=False, status="cancelled", finished_at=now_ts(),
                bytes_written=bw, percent=min((bw / total_size * 100.0), 100.0) if total_size > 0 else 0.0,
                speed_bps=bw / elapsed_total if elapsed_total > 0 else 0.0, eta_seconds=None,
                error=None, last_message="Backup cancelled by user.",
            )
            append_backup_log("Backup cancelled by user.", level="minimal")
            log_action("backup", True, f"Cancelled for {', '.join(selected)}", {"bytes_written": bw})
            add_backup_record({
                "id": record_id,
                "label": label or job_id,
                "volume_tag": vol,
                "paths": selected,
                "mode": backup_mode,
                "status": "cancelled",
                "started_at": int(start),
                "finished_at": now_ts(),
                "bytes_written": bw,
                "log_level": log_level,
                "backup_dirname": _backup_dirname,
            })
            if POST_BACKUP_HOOK:
                run_hook(POST_BACKUP_HOOK, "post-backup (after cancel)")
            publish_state_to_mqtt(refresh_state())
            return
        # tar exit codes: 0 = success, 1 = warnings (files changed/skipped), 2+ = fatal.
        # rc==1 is normal for live filesystems — treat as success.
        if rc not in (0, 1):
            # Use real tar output from log file, not dd progress lines
            if _tar_error_lines:
                append_backup_log(f"tar stderr: {chr(10).join(_tar_error_lines[-20:])}", level="minimal")
            err_msg = "\n".join(_tar_error_lines[-10:]).strip() or f"tar failed (rc={rc})"
            raise TapeError(err_msg)
        elif rc == 1 and _tar_error_lines:
            # Log warnings but continue
            append_backup_log(f"tar completed with warnings (rc=1): {_tar_error_lines[-1]}", level="normal")

        append_backup_log(f"Tar complete. Wrote {bytes_human(bw)}.", level="minimal")
        # Do NOT re-fetch vol from state_cache here — by this point the state cache may
        # have been refreshed and the tape may already be returning to its slot, causing
        # vol to come back empty and the index/verify steps to be skipped entirely.
        # vol was set earlier when the tape was loaded and is still valid.

        # ── Index ────────────────────────────────────────────────────────────
        # Build the file index from the tar verbose log captured during streaming.
        # This avoids re-reading the entire tape (which would timeout on large backups
        # and leave dd holding /dev/nst0 busy for subsequent rewind/verify steps).
        if vol:
            append_backup_log("Building tape index from backup log…")
            set_backup_state(status="indexing")
            publish_state_to_mqtt(refresh_state())
            try:
                fl = []
                _log_path_for_index = locals().get("_tar_log_path", "")
                if _log_path_for_index and os.path.exists(_log_path_for_index):
                    with open(_log_path_for_index, "rb") as _lf:
                        fl = [
                            line.decode(errors="ignore").strip()
                            for line in _lf.read().splitlines()
                            if line.strip()
                        ]
                    # With --listed-incremental, tar's own diagnostics (e.g.
                    # "tar: mnt/foo: Directory is new") are written to stderr
                    # alongside the verbose member list, since stdout is the
                    # archive stream. Both land in the same log file, so strip
                    # tar's diagnostic lines here — otherwise they get indexed
                    # as bogus "tar: ..." entries in the restore browser.
                    fl = [p for p in fl if not p.startswith("tar: ")]
                    # tar's verbose create log (captured on stderr, since stdout is the
                    # archive stream) reports each member's SOURCE path — i.e. before
                    # --transform is applied. The archive itself stores every member
                    # under f"{_backup_dirname}/...", so re-derive the real in-archive
                    # paths here; otherwise the saved index doesn't match what's on
                    # tape and selective restores fail with "Not found in archive".
                    fl = [f"{_backup_dirname}/{p}" for p in fl]
                    # Clean up now that we've read it
                    try:
                        os.unlink(_log_path_for_index)
                    except Exception:
                        pass
                else:
                    append_backup_log("Warning: tar log not available — skipping index build.", level="normal")

                if not fl:
                    append_backup_log("Warning: tar log was empty — index not saved.", level="normal")
                else:
                    prior_bw = bytes_written_for_volume(vol)
                    total_used = prior_bw + bw
                    drive_snap = shared_state._state_cache.get("drive", {})
                    space_meta = space_meta_from_info(build_tape_space_info(
                        vol, drive=drive_snap,
                        idx={"volume_tag": vol, "used_bytes": total_used, "space_estimated": False},
                        loaded=True,
                    ))
                    space_meta["used_bytes"]      = total_used
                    space_meta["space_estimated"] = 0
                    # FIX: stamp last_backup_ts so _pick_backup_tape() can use it
                    # to score tapes by recency.  Previously this field was never
                    # written, so all tapes looked equally fresh and the picker
                    # always fell back to alphabetical order — i.e. the same tape.
                    space_meta["last_backup_ts"]  = now_ts()
                    save_tape_index(vol, fl, now_ts(), meta={"present": True, "backup_dirname": _backup_dirname, **space_meta})
                    append_backup_log(
                        f"Index saved: {len(fl)} entries for {vol} "
                        f"({bytes_human(total_used)} used on tape).", level="normal"
                    )
            except Exception as e:
                append_backup_log(f"Warning: index failed: {e}", level="normal")
                # Clean up tar log on error too
                try:
                    _lp = locals().get("_tar_log_path", "")
                    if _lp and os.path.exists(_lp):
                        os.unlink(_lp)
                except Exception:
                    pass

        # ── Verify ───────────────────────────────────────────────────────────
        if VERIFY_AFTER_BACKUP and vol:
            append_backup_log("Starting post-backup verification…")
            set_backup_state(status="verifying")
            publish_state_to_mqtt(refresh_state())
            try:
                verify_worker(vol, backup_record_id=record_id)
                with shared_state._verify_lock:
                    verify_errors = shared_state._verify_job.get("errors", 0)
                verified = True
                if verify_errors > 0:
                    append_backup_log(f"⚠ Verify found {verify_errors} error(s).", level="normal")
                else:
                    append_backup_log("✓ Verification passed.", level="normal")
            except Exception as verify_exc:
                # Verification failure must NOT mark the whole backup as failed —
                # the data was written successfully.  Log the issue and continue.
                append_backup_log(f"⚠ Verification step encountered an error: {verify_exc}", level="minimal")
                verify_errors = 1
                verified = False

        # ── Rewind after ─────────────────────────────────────────────────────
        if AUTO_REWIND_AFTER:
            set_backup_state(status="rewinding")
            append_backup_log("Rewinding after backup.")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

        # ── Auto-unload tape back to its slot ────────────────────────────────
        # Return the tape to the slot it came from.  We do NOT exclude _auto_loaded_slot
        # here — that is the tape's home slot and we want to return it there.
        _return_slot = _find_return_slot(vol)
        if _return_slot:
            append_backup_log(f"Returning tape {vol} to slot {_return_slot}…", level="minimal")
            set_backup_state(status="unloading", last_message=f"Unloading tape to slot {_return_slot}…")
            publish_state_to_mqtt(refresh_state())
            try:
                run_cmd(["mtx", "-f", CHANGER, "unload", str(_return_slot), "0"],
                        timeout=max(COMMAND_TIMEOUT, 120))
                _save_last_known_loaded_slot(None)
                update_tape_index_metadata(vol, present=True,
                                           last_seen_slot=_return_slot,
                                           last_seen_at=now_ts())
                append_backup_log(f"Tape returned to slot {_return_slot}.", level="minimal")
                # Clear _auto_loaded_slot so the finally block knows the unload
                # was already handled and does not fire a second time.
                _auto_loaded_slot = None
            except Exception as ue:
                append_backup_log(f"Warning: could not unload tape: {ue}", level="minimal")
        else:
            append_backup_log("Warning: no empty slot found to return tape to — leaving in drive.", level="minimal")
            # Drive still has tape — clear _auto_loaded_slot so finally doesn't
            # try to unload it again to a potentially wrong slot.
            _auto_loaded_slot = None

        # ── Post-backup hook ─────────────────────────────────────────────────
        if POST_BACKUP_HOOK:
            set_backup_state(status="post_hook")
            publish_state_to_mqtt(refresh_state())
            run_hook(POST_BACKUP_HOOK, "post-backup")

        elapsed_total = max(time.time() - start, 0.001)
        set_backup_state(
            running=False, status="completed", bytes_written=bw, percent=100.0,
            speed_bps=bw / elapsed_total, eta_seconds=0,
            finished_at=now_ts(), last_message="Backup completed successfully.", error=None,
        )
        append_backup_log("Backup completed successfully.")
        log_action("backup", True, f"Completed for {', '.join(selected)}", {"bytes_written": bw})
        _record_backup_done(vol, bw)

        # ── Backup record ────────────────────────────────────────────────────
        add_backup_record({
            "id": record_id,
            "label": label or job_id,
            "volume_tag": vol,
            "paths": selected,
            "mode": backup_mode,
            "status": "completed",
            "started_at": int(start),
            "finished_at": now_ts(),
            "bytes_written": bw,
            "speed_bps": bw / elapsed_total,
            "verified": verified,
            "verify_errors": verify_errors,
            "log_level": log_level,
            "backup_dirname": _backup_dirname,
        })

        # ── Notify ───────────────────────────────────────────────────────────
        notify_backup_success(vol, selected, bw, elapsed_total, verified, verify_errors)

    except Exception as e:
        elapsed_total = max(time.time() - start, 0.001)
        set_backup_state(
            running=False, status="failed", finished_at=now_ts(),
            error=str(e), last_message=f"Backup failed: {e}", eta_seconds=None,
        )
        append_backup_log(f"Backup failed: {e}")
        log_action("backup", False, str(e))
        add_backup_record({
            "id": record_id,
            "label": label or job_id,
            "volume_tag": vol,
            "paths": selected,
            "mode": backup_mode,
            "status": "failed",
            "error": str(e),
            "started_at": int(start),
            "finished_at": now_ts(),
            "bytes_written": bw,
            "log_level": log_level,
            "backup_dirname": _backup_dirname,
        })
        # Try post-hook even on failure
        if POST_BACKUP_HOOK:
            run_hook(POST_BACKUP_HOOK, "post-backup (after failure)")
        notify_backup_failure(vol, selected, str(e))
    finally:
        shared_state._tar_proc = None
        # Only auto-unload in the finally block if _auto_loaded_slot is still set.
        # The success path clears it after its own unload, so this only fires on
        # genuine failures or cancellations where the tape was never returned.
        if _auto_loaded_slot is not None:
            try:
                # Refresh state so we get the current drive status, not a stale cache
                cur_drive = refresh_state().get("drive") or {}
                if not cur_drive.get("empty", True):
                    _return_slot = _find_return_slot(vol) or _auto_loaded_slot
                    append_backup_log(
                        f"Auto-unloading tape {vol} to slot {_return_slot} after failure.",
                        level="minimal")
                    run_cmd(["mtx", "-f", CHANGER, "unload", str(_return_slot), "0"],
                            timeout=max(COMMAND_TIMEOUT, 120))
                    _save_last_known_loaded_slot(None)
            except Exception:
                pass
        publish_state_to_mqtt(refresh_state())

# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------

def _snapshot_path(job_id: str) -> str:
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", job_id)
    return os.path.join(INCREMENTAL_DIR, f"{safe}.snapshot")


def incremental_tar_args(paths: List[str], job_id: str,
                          mode: str = "full") -> tuple:
    """
    Build extra tar arguments for incremental/differential backups.
    mode: 'full' | 'incremental' | 'differential'
    Returns (extra_tar_args, snapshot_file_used_or_None)
    """
    snap = _snapshot_path(job_id)
    if mode == "full":
        # Reset snapshot — next run will be incremental against this full
        if os.path.exists(snap):
            os.rename(snap, snap + ".prev")
        return ["--listed-incremental=" + snap], snap
    elif mode == "incremental":
        if not os.path.exists(snap):
            # No prior snapshot → fall back to full
            return ["--listed-incremental=" + snap], snap
        return ["--listed-incremental=" + snap], snap
    elif mode == "differential":
        # Copy snapshot so it doesn't advance (always diff against last full)
        snap_diff = snap + ".diff_tmp"
        if os.path.exists(snap):
            import shutil
            shutil.copy2(snap, snap_diff)
        return ["--listed-incremental=" + snap_diff], snap_diff
    return [], None


# ---------------------------------------------------------------------------
# Pre/post backup hooks
# ---------------------------------------------------------------------------

def run_hook(script: str, label: str) -> bool:
    """Run a shell script hook. Returns True if it succeeded."""
    from .state import append_backup_log, log_action
    if not script:
        return True
    append_backup_log(f"Running {label} hook: {script}", level="normal")
    try:
        proc = subprocess.run(
            script, shell=True, capture_output=True, text=True, timeout=300
        )
        if proc.stdout:
            append_backup_log(f"{label} stdout: {proc.stdout.strip()[:500]}", level="verbose")
        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout or "Hook failed").strip()[:300]
            append_backup_log(f"{label} hook FAILED (rc={proc.returncode}): {msg}", level="normal")
            log_action("hook", False, f"{label}: {msg}")
            return False
        append_backup_log(f"{label} hook OK.", level="normal")
        return True
    except Exception as e:
        append_backup_log(f"{label} hook exception: {e}", level="normal")
        log_action("hook", False, str(e))
        return False


# ---------------------------------------------------------------------------
# Verification worker
# ---------------------------------------------------------------------------

