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


def _pick_backup_tape(required_bytes: int = 0, exclude: Optional[List[str]] = None,
                      allow_span: bool = False, check_total: bool = True) -> Dict[str, Any]:
    """Choose the best available tape to back up to when no tape is loaded.

    Selection is capacity-aware: when ``required_bytes`` is given, tapes that
    can hold the whole pending backup are always preferred over tapes that
    can't, so a backup is never needlessly split.  Among tapes that fit,
    ordering depends on the configured fill strategy:

      * ``spread`` (default) — prefer, in order: a tape marked
        available/recyclable, then a blank/never-used tape, then the
        least-recently-used written tape.  Round-robins across the library.
      * ``fill`` — prefer a partially-used tape (most-full first) so one tape
        is filled before moving on, then available/recyclable, then blank.
        Handy for pulling a full tape for offsite storage.

    Backups are appended after what is already on a tape; a tape marked
    available/recyclable, or with nothing on it, is written from the start
    instead (``overwrite`` in the result).

    Never picks cleaning tapes or anything in ``exclude``.  If no tape can hold
    the whole backup: with ``allow_span`` the backup will continue on further
    tapes, so this returns the best tape to start on (after checking, when
    ``check_total``, that the library has enough free space in total);
    otherwise it raises TapeError.  Returns a dict with 'volume_tag', 'slot',
    'remaining' and 'overwrite'.
    """
    from .records import gfs_get_recyclable
    from .db import list_all_known_indexes
    from .state import TapeError, bytes_human, is_cleaning_volume_tag
    from .changer import refresh_state
    from .settings import get_tape_fill_strategy
    from .tape_layout import get_sessions, tape_has_data
    strategy = get_tape_fill_strategy()
    excluded = set(exclude or [])
    state = refresh_state()
    # Build map of slot info keyed by volume_tag for tapes physically present
    slot_map: Dict[str, Dict[str, Any]] = {
        str(s.get("volume_tag") or "").strip(): s
        for s in (state.get("slots") or [])
        if s.get("full") and not s.get("is_import_export") and s.get("volume_tag")
           and not is_cleaning_volume_tag(str(s.get("volume_tag") or ""))
           and str(s.get("volume_tag") or "").strip() not in excluded
    }
    if not slot_map:
        raise TapeError("No other non-cleaning tapes found in the library slots." if excluded
                        else "No non-cleaning tapes found in any library slot.")

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
        # Written from the start: its contents are expendable or there are none.
        overwrite     = is_available or not tape_has_data(vol)

        # FIX: read last_backup from drive_history, not from the tape index.
        # The index field last_backup_ts was never written before this patch,
        # so all tapes scored 0 and the picker always chose the same tape
        # (the one that sorted first alphabetically after bucket ordering).
        last_bk = dh.get("last_backup") or idx.get("last_backup_ts") or 0

        # Skip tapes whose data already fills the tape's native capacity
        # (with 5% headroom), or that hit end-of-media on their last write.
        # Used space comes from the catalog, which tracks what is actually on
        # the tape now; drive_history's total_backup_bytes counts every byte
        # ever written, including backups since overwritten.
        capacity   = float(idx.get("capacity_bytes") or 0) or _LTO_NATIVE_BYTES
        used_bytes = float(idx.get("used_bytes") or 0)
        sessions   = get_sessions(vol)
        hit_eom    = bool(sessions and sessions[-1].get("ended_at_eom"))
        is_full    = (used_bytes >= capacity * 0.95 or hit_eom) and not is_available

        if is_full:
            skipped_full.append(vol)
            continue

        # Usable free space, using the same 5% headroom as the full check.  An
        # available/recyclable tape is treated as empty because its contents
        # will be overwritten.
        usable = capacity * 0.95
        remaining = usable if overwrite else max(usable - used_bytes, 0.0)
        # Whether this tape can hold the whole pending backup; a tape that
        # can't would split it across tapes, so it is a last resort.
        fits = required_bytes <= 0 or remaining >= float(required_bytes)

        # Priority bucket: lower = preferred
        if is_available:
            bucket = 0
        elif never_used or not idx:
            bucket = 1
        else:
            bucket = 2

        candidates.append({
            "volume_tag": vol,
            "slot":       int(slot_info.get("slot") or 0),
            "bucket":     bucket,
            "remaining":  remaining,
            "last_bk":    last_bk,
            "fits":       fits,
            "overwrite":  overwrite,
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

    # Fail fast: if we know the backup size and no single tape can hold it all,
    # stop before writing anything rather than half-filling a tape — unless the
    # backup may span tapes and the library has room for it across tapes.
    if required_bytes > 0 and not any(c["fits"] for c in candidates):
        total_free = sum(c["remaining"] for c in candidates)
        largest = max(c["remaining"] for c in candidates)
        if not allow_span:
            raise TapeError(
                f"Backup needs {bytes_human(int(required_bytes))} but no single tape has that "
                f"much free space (largest free: {bytes_human(int(largest))}), and spanning "
                "backups across tapes is off (Retention page) or mbuffer is not installed. "
                "Enable spanning, erase/free a tape, mark one as 'available', or reduce the selection."
            )
        if check_total and total_free < required_bytes:
            raise TapeError(
                f"Backup needs {bytes_human(int(required_bytes))} but the library only has "
                f"{bytes_human(int(total_free))} free across {len(candidates)} writable tape(s). "
                "Erase/free tapes, mark some as 'available', or reduce the selection."
            )

    def _sort_key(c: Dict[str, Any]):
        fits_rank = 0 if c["fits"] else 1   # tapes that fit the whole backup win
        if strategy == "fill":
            # Concentrate on a tape until full: partially-used tapes first
            # (most-full = least remaining first), then available/recyclable,
            # then blank.
            fill_group = {2: 0, 0: 1, 1: 2}[c["bucket"]]
            sub = c["remaining"] if c["bucket"] == 2 else c["last_bk"]
            return (fits_rank, fill_group, sub, c["volume_tag"])
        # spread (default): available → blank → used, oldest-used first
        return (fits_rank, c["bucket"], c["last_bk"], c["volume_tag"])

    candidates.sort(key=_sort_key)
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
# Tape spanning
# ---------------------------------------------------------------------------

_MB_UNITS = {"": 1, "k": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}
# mbuffer status line: "in @ 95.2 MiB/s, out @ 94.8 MiB/s, 1234 GiB total, buffer  78% full"
_MB_STATUS_RE = re.compile(r"out @\s*([\d.]+)\s*([kMGT]?)i?B/s,\s*([\d.]+)\s*([kMGT]?)i?B total")
# mbuffer -v 4 at end-of-media: cumulative blocks written before the swap
_MB_EOV_RE = re.compile(r"end of volume - last block on volume:\s*(\d+)")
# mbuffer at exit: "summary: 6835 kiByte in  1.2sec - average of 5553 kiB/s"
_MB_SUMMARY_RE = re.compile(r"summary:\s*([\d.]+)\s*([kMGT]?)i?B")


def _position_for_write(vol: str, overwrite: bool, log) -> int:
    """Position the loaded tape for a new backup and return its tape file
    number: file 0 when the tape is being rewritten, else end-of-data."""
    from .state import run_cmd
    from .tape_layout import forget_sessions_from, seek_end_of_data
    if overwrite:
        run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
        forget_sessions_from(vol, 0, reason=f"{vol} rewritten from the start by a new backup")
        return 0
    log(f"Spacing {vol} to end-of-data to append…")
    fn = seek_end_of_data()
    forget_sessions_from(vol, fn, reason=f"{vol} appended at tape file {fn}")
    log(f"Appending to {vol} as tape file {fn}.")
    return fn


class _TapeSpan:
    """The tapes one backup is written to, and the mid-backup tape swap.

    mbuffer writes the archive straight to the tape device.  When the drive
    signals end-of-media, mbuffer closes the device (writing the closing
    filemark) and runs its ``-A`` command.  That command drops a request file
    in a private temp dir and waits for an answer; the watcher thread here
    does the swap — unload the full tape, pick and load the next, space it to
    where the backup continues — and answers, after which mbuffer reopens the
    device and carries on.  The swap lives in Python so it shares tape
    selection, loading and logging with the rest of the backup.

    ``segments`` lists where the stream went: [{volume_tag, file_number,
    start (byte offset in the stream), bytes}].
    """

    def __init__(self, vol: str, file_number: int, total_size: int, allow_span: bool, log):
        import tempfile
        self.segments: List[Dict[str, Any]] = [
            {"volume_tag": vol, "file_number": file_number, "start": 0, "bytes": 0}]
        self.total_size = total_size
        self.allow_span = allow_span
        self.log = log
        self.dir = tempfile.mkdtemp(prefix="tl2000_span_")
        self.req = os.path.join(self.dir, "request")
        self.ack = os.path.join(self.dir, "ack")
        # Run by mbuffer through /bin/sh at end-of-media.  Exits non-zero (and
        # so fails the write) if the answer is "no" or the backup has gone.
        self.script = os.path.join(self.dir, "swap")
        with open(self.script, "w") as fh:
            fh.write(f"touch '{self.req}'\n"
                     f"while [ ! -e '{self.ack}' ]; do [ -d '{self.dir}' ] || exit 1; sleep 1; done\n"
                     f"rc=$(cat '{self.ack}'); rm -f '{self.ack}'; exit ${{rc:-1}}\n")
        self.eov_blocks: List[int] = []
        self.out_total = 0          # approximate bytes out, from mbuffer status
        self.swapping = False
        self.swapped = False        # a continuation tape was loaded by us
        self.error: Optional[str] = None
        self.full_without_span = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)

    def autoload_cmd(self) -> str:
        # Kept short: mbuffer logs the command into a 256-byte buffer at -v 4
        # and aborts (assertion) if the line does not fit.
        return f"sh {self.script}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)

    def feed_mbuffer_line(self, line: str) -> None:
        m = _MB_EOV_RE.search(line)
        if m:
            self.eov_blocks.append(int(m.group(1)))
            return
        m = _MB_STATUS_RE.search(line)
        if m:
            self.out_total = int(float(m.group(3)) * _MB_UNITS.get(m.group(4), 1))
            return
        m = _MB_SUMMARY_RE.search(line)
        if m:
            self.out_total = int(float(m.group(1)) * _MB_UNITS.get(m.group(2), 1))

    def speed_from_line(self, line: str) -> Optional[float]:
        m = _MB_STATUS_RE.search(line)
        return float(m.group(1)) * _MB_UNITS.get(m.group(2), 1) if m else None

    @property
    def written_bytes(self) -> int:
        """Best count of bytes mbuffer got onto tape so far."""
        eov = self.eov_blocks[-1] * TAPE_BLOCK_BYTES if self.eov_blocks else 0
        return max(self.out_total, eov)

    @property
    def volumes(self) -> List[str]:
        return [s["volume_tag"] for s in self.segments]

    @property
    def current(self) -> Dict[str, Any]:
        return self.segments[-1]

    def finish(self, total_bytes: int) -> None:
        """Close the last segment once the stream length is known.  A segment
        that ended at end-of-media already has its exact size from mbuffer's
        block count (and a failed swap leaves mbuffer's own total too high)."""
        last = self.segments[-1]
        if last.get("ended_at_eom"):
            return
        last["bytes"] = max(int(total_bytes) - int(last["start"]), 0)

    def record_segments(self) -> List[Dict[str, Any]]:
        return [{"volume_tag": s["volume_tag"], "file_number": s["file_number"], "bytes": s["bytes"],
                 **({"ended_at_eom": True} if s.get("ended_at_eom") else {})}
                for s in self.segments]

    def _answer(self, rc: int) -> None:
        tmp = self.ack + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(str(rc))
        os.replace(tmp, self.ack)

    def _watch(self) -> None:
        while not self._stop.is_set():
            if os.path.exists(self.req):
                try:
                    os.unlink(self.req)
                except OSError:
                    pass
                rc = self._swap()
                try:
                    self._answer(rc)
                except OSError:
                    pass
            time.sleep(0.5)

    def _swap(self) -> int:
        from .changer import refresh_state
        from .db import update_tape_index_metadata
        from .drive_history import _save_last_known_loaded_slot
        from .mqtt import publish_state_to_mqtt
        from .state import TapeError, bytes_human, run_cmd, set_backup_state
        from .tape_layout import loaded_volume
        self.swapping = True
        try:
            # mbuffer logs the exact block count at end-of-media just before it
            # runs the swap command; give the log reader a moment to see it.
            deadline = time.time() + 10
            while len(self.eov_blocks) < len(self.segments) and time.time() < deadline:
                time.sleep(0.2)
            if len(self.eov_blocks) >= len(self.segments):
                boundary = self.eov_blocks[len(self.segments) - 1] * TAPE_BLOCK_BYTES
            else:
                boundary = self.out_total
            cur = self.segments[-1]
            cur["bytes"] = max(boundary - cur["start"], 0)
            cur["ended_at_eom"] = True
            vol = cur["volume_tag"]
            if not self.allow_span:
                self.full_without_span = True
                raise TapeError(f"Tape {vol} is full and spanning backups across tapes is disabled.")
            if shared_state._stop_requested:
                raise TapeError("Backup cancelled during tape change.")
            part = len(self.segments) + 1
            self.log(f"End of tape on {vol} after {bytes_human(cur['bytes'])} — "
                     f"continuing on another tape (part {part}).")
            set_backup_state(status="changing_tape", last_message=f"{vol} full — changing to the next tape…")
            publish_state_to_mqtt(refresh_state())

            need = max(self.total_size - boundary, 0)
            chosen = _pick_backup_tape(required_bytes=need, exclude=self.volumes,
                                       allow_span=True, check_total=False)
            ret = _find_return_slot(vol)
            if not ret:
                raise TapeError(f"No free slot to return full tape {vol} to.")
            self.log(f"Returning {vol} to slot {ret}…")
            run_cmd(["mtx", "-f", CHANGER, "unload", str(ret), "0"], timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(None)
            update_tape_index_metadata(vol, present=True, last_seen_slot=ret, last_seen_at=int(time.time()))
            time.sleep(2)
            self.log(f"Loading {chosen['volume_tag']} from slot {chosen['slot']} "
                     f"({'rewrite' if chosen['overwrite'] else 'append'}, priority: {chosen['purpose']})…")
            run_cmd(["mtx", "-f", CHANGER, "load", str(chosen["slot"]), "0"], timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(chosen["slot"])
            self.swapped = True
            time.sleep(3)
            now_vol = loaded_volume()
            nxt = chosen["volume_tag"] if now_vol in ("", "?") else now_vol
            if nxt != chosen["volume_tag"]:
                raise TapeError(f"Loaded slot {chosen['slot']} expecting {chosen['volume_tag']} "
                                f"but the drive reports {nxt}.")
            fn = _position_for_write(nxt, chosen["overwrite"], self.log)
            self.segments.append({"volume_tag": nxt, "file_number": fn, "start": boundary, "bytes": 0})
            set_backup_state(status="streaming", tapes=self.volumes, current_tape=nxt,
                             last_message=f"Continuing on {nxt} (part {len(self.segments)})…")
            publish_state_to_mqtt(refresh_state())
            self.log(f"Continuing backup on {nxt}, tape file {fn} (part {len(self.segments)}).")
            return 0
        except Exception as e:
            self.error = str(e)
            self.log(f"Tape change failed: {e}")
            return 1
        finally:
            self.swapping = False


# ---------------------------------------------------------------------------
# Backup worker
# ---------------------------------------------------------------------------

# tar diagnostics that mean "this one file/folder couldn't be read and was left
# out" — e.g. Unraid appdata owned by another container's UID, or a file that
# vanished mid-backup.  With --ignore-failed-read tar reports these as warnings
# and keeps going; we collect them so the user can see what was skipped.
_TAR_SKIP_MARKERS = (
    "Cannot open", "Cannot stat", "Cannot read", "Cannot savedir",
    "Cannot readlink", "Read error", "Permission denied",
)
_SKIPPED_ITEMS_KEEP = 200


def _scan_tar_log_for_skips(log_path: str) -> Dict[str, Any]:
    """Scan tar's stderr log for files/folders tar could not read.

    Returns {"count": int, "items": [first N "path: reason" strings],
    "fatal": bool}.  ``fatal`` is True when tar hit an unrecoverable error
    (the archive itself is broken), which must still fail the backup.
    """
    count, items, fatal = 0, [], False
    try:
        with open(log_path, "rb") as fh:
            for raw in fh:
                if not raw.startswith(b"tar: "):
                    continue
                line = raw.decode(errors="ignore").strip()[5:]
                if "Error is not recoverable" in line:
                    fatal = True
                    continue
                if not any(m in line for m in _TAR_SKIP_MARKERS):
                    continue
                count += 1
                if len(items) < _SKIPPED_ITEMS_KEEP:
                    items.append(line.replace("Warning: ", "", 1))
    except OSError:
        pass
    return {"count": count, "items": items, "fatal": fatal}


class _ScanCancelled(Exception):
    """Raised from the source-size scan when the user cancels the backup."""


def start_backup_thread(paths: List[str], backup_mode: str = "full", label: str = "",
                        log_level: str = BACKUP_LOG_LEVEL_DEFAULT,
                        excludes: Optional[List[str]] = None) -> bool:
    """Claim the backup job and run backup_worker in the background.

    ``excludes`` are paths inside ``paths`` to leave out of the archive.

    The claim happens synchronously, so the job already reads as running when
    this returns; False means another backup is active and nothing started.
    """
    if not shared_state.claim_backup_job(paths, log_level=log_level):
        return False
    threading.Thread(
        target=backup_worker,
        args=(paths,),
        kwargs={"backup_mode": backup_mode, "label": label, "log_level": log_level,
                "excludes": list(excludes or [])},
        daemon=True,
    ).start()
    return True


def backup_worker(paths: List[str], backup_mode: str = "full",
                  job_id: str = "", label: str = "", log_level: str = BACKUP_LOG_LEVEL_DEFAULT,
                  excludes: Optional[List[str]] = None) -> None:
    from .records import add_backup_record
    from .db import update_tape_index_metadata
    from .verify_worker import verify_worker
    from .drive_history import _is_tape_full_error, _mt_status_shows_eot, _record_backup_done, _save_last_known_loaded_slot, _switch_to_rewrite_candidate, build_tape_space_info
    from .tape_layout import add_backup_sessions, loaded_volume, tape_has_data, unique_dirname
    from .state import TapeError, append_backup_log, backup_log_allows, bytes_human, is_cleaning_volume_tag, log_action, log_exit_codes, log_pipeline, log_traceback, normalize_backup_log_level, now_ts, run_cmd, secs_human, set_backup_state
    from .mqtt import publish_state_to_mqtt
    from .changer import ensure_under_backup_root, estimate_path_size, normalize_excludes, refresh_state
    from .notify import notify_backup_failure, notify_backup_success
    from .settings import build_backup_dirname, get_allow_tape_spanning
    # The caller has already claimed the job (claim_backup_job), so it reads as
    # running/"scanning" while the sources are sized -- which can take minutes
    # on large or remote shares. Anything that fails before the main try block
    # below must release the claim, or the job would read as running forever.
    log_level = normalize_backup_log_level(log_level)
    try:
        selected = [ensure_under_backup_root(p) for p in paths]
        rels     = [os.path.relpath(p, "/") for p in selected]
        excluded = normalize_excludes(excludes or [], selected)
        set_backup_state(status="scanning", selected_paths=selected, log_level=log_level,
                         last_message=f"Scanning {len(selected)} source(s)…")
        append_backup_log(f"Scanning {len(selected)} source(s) to estimate backup size…", level="minimal")
        if excluded:
            append_backup_log(f"Excluding {len(excluded)} path(s): {', '.join(excluded)}", level="minimal")
        publish_state_to_mqtt(refresh_state())

        scanned = 0
        last_report = 0.0
        def _scan_progress(so_far: int) -> None:
            nonlocal last_report
            if shared_state._stop_requested:
                raise _ScanCancelled()
            now = time.monotonic()
            if now - last_report >= 1.0:
                last_report = now
                set_backup_state(bytes_total=scanned + so_far,
                                 last_message=f"Scanning sources… {bytes_human(scanned + so_far)} found")

        _seen_inodes: set = set()   # hardlinks count once across all sources, as in tar
        for p in selected:
            _breakdown: Dict[str, int] = {}
            _src_bytes = estimate_path_size(p, progress=_scan_progress, exclude=excluded,
                                            seen=_seen_inodes, breakdown=_breakdown)
            scanned += _src_bytes
            # Largest entries first, so an unexpectedly large total shows where it comes from.
            _top = sorted(_breakdown.items(), key=lambda kv: kv[1], reverse=True)[:10]
            _detail = ", ".join(f"{os.path.basename(k)} {bytes_human(v)}" for k, v in _top if v > 0)
            _more = len(_breakdown) - len(_top)
            append_backup_log(f"  {p}: {bytes_human(_src_bytes)}"
                              + (f" — largest: {_detail}" if _detail else "")
                              + (f" (+{_more} more)" if _more > 0 else ""), level="minimal")
            set_backup_state(bytes_total=scanned)
            if shared_state._stop_requested:
                raise _ScanCancelled()
        total_size = scanned
        append_backup_log(f"Scan complete — {bytes_human(total_size)} to back up.", level="minimal")

        vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "")
        if is_cleaning_volume_tag(vol):
            raise TapeError(f"Tape {vol} is a cleaning tape and cannot be written to.")
    except _ScanCancelled:
        set_backup_state(running=False, status="cancelled", finished_at=now_ts(),
                         eta_seconds=None, last_message="Backup cancelled while scanning sources.")
        append_backup_log("Backup cancelled while scanning sources.")
        log_action("backup", False, "Cancelled while scanning sources.")
        publish_state_to_mqtt(refresh_state())
        return
    except Exception as e:
        set_backup_state(running=False, status="failed", finished_at=now_ts(),
                         error=str(e), last_message=f"Backup failed: {e}", eta_seconds=None)
        append_backup_log(f"Backup failed: {e}")
        log_action("backup", False, str(e))
        log_traceback("backup", e)
        publish_state_to_mqtt(refresh_state())
        return

    start    = time.time()
    if not job_id:
        job_id = f"{vol or 'nolabel'}_{int(start)}"
    record_id = str(int(start * 1000))

    # Build the archive prefix directory name now (uses vol + start time).
    # Vol may change below if auto-load picks a different tape, so we'll
    # recompute it once the final vol is known before building the tar command.
    _backup_dirname: str = ""   # set after vol is finalised

    # Track whether we auto-loaded a tape so we can auto-unload it when done
    _auto_loaded_slot: Optional[int] = None

    # Where the stream went (tapes and tape files); set once writing starts.
    span: Optional[_TapeSpan] = None

    # mbuffer writes straight to the tape and can swap tapes at end-of-media;
    # without it the fallback pipeline stops at the end of the first tape.
    _has_mbuffer = subprocess.run(["which", "mbuffer"], capture_output=True).returncode == 0
    _has_pv      = subprocess.run(["which", "pv"],      capture_output=True).returncode == 0
    allow_span   = get_allow_tape_spanning() and _has_mbuffer
    if get_allow_tape_spanning() and not _has_mbuffer:
        append_backup_log("mbuffer not installed — backups cannot continue onto another tape.", level="normal")

    set_backup_state(status="preparing", bytes_total=total_size, last_message="Preparing…",
                     tapes=[], current_tape="")
    append_backup_log(f"Backup [{backup_mode}] for {len(selected)} path(s) on {vol or '(no tape)'}.", level="minimal")
    publish_state_to_mqtt(refresh_state())

    bw = 0
    verify_errors = 0
    verified = False

    def _log(msg: str) -> None:
        append_backup_log(msg, level="minimal")

    try:
        # ── Auto-select and load tape if drive is empty ──────────────────────
        refresh_state()
        drive_state = (shared_state._state_cache.get("drive") or {})
        overwrite_first = False
        if drive_state.get("empty", True):
            append_backup_log("No tape in drive — selecting tape automatically…", level="minimal")
            set_backup_state(status="selecting_tape", last_message="Selecting tape…")
            publish_state_to_mqtt(refresh_state())
            try:
                chosen = _pick_backup_tape(required_bytes=total_size, allow_span=allow_span)
                append_backup_log(
                    f"Auto-selected {chosen['volume_tag']} from slot {chosen['slot']} "
                    f"(priority: {chosen['purpose']}, {'rewrite' if chosen['overwrite'] else 'append'}).",
                    level="minimal"
                )
                if total_size > chosen["remaining"] > 0:
                    append_backup_log(
                        f"Backup ({bytes_human(total_size)}) is larger than the free space on "
                        f"{chosen['volume_tag']} (~{bytes_human(int(chosen['remaining']))}) — "
                        "it will continue on further tapes.", level="minimal")
                set_backup_state(status="loading_tape",
                                 last_message=f"Loading {chosen['volume_tag']} from slot {chosen['slot']}…")
                publish_state_to_mqtt(refresh_state())
                run_cmd(["mtx", "-f", CHANGER, "load", str(chosen["slot"]), "0"],
                        timeout=max(COMMAND_TIMEOUT, 120))
                _save_last_known_loaded_slot(chosen["slot"])
                _auto_loaded_slot = chosen["slot"]
                overwrite_first = bool(chosen["overwrite"])
                time.sleep(3)
                refresh_state()
                vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "") or chosen["volume_tag"]
                append_backup_log(f"Tape {vol} loaded from slot {chosen['slot']}.", level="minimal")
            except TapeError as e:
                raise TapeError(f"Could not auto-select a tape: {e}")

        # ── Capacity guard for a manually pre-loaded tape ────────────────────
        # Auto-selected tapes were already checked by _pick_backup_tape; this
        # covers the case where the operator loaded a tape by hand.  A tape
        # that will be erased, is marked available/recyclable, or has nothing
        # on it is written from the start; anything else is appended to.
        if _auto_loaded_slot is None and vol and not is_cleaning_volume_tag(vol):
            from .db import load_tape_index
            from .records import gfs_get_recyclable as _gfs_recyclable
            _purpose = str((load_tape_index(vol) or {}).get("purpose") or "").strip().lower()
            overwrite_first = (ERASE_BEFORE_BACKUP or _purpose in ("available", "recyclable")
                               or vol in set(_gfs_recyclable()) or not tape_has_data(vol))
            _sinfo = build_tape_space_info(vol, idx=load_tape_index(vol) or {})
            _cap = float(_sinfo.get("capacity_bytes") or 0)
            if _cap and total_size > 0:
                _used = 0.0 if overwrite_first else float(_sinfo.get("used_bytes") or 0)
                _remaining = max(_cap * 0.95 - _used, 0.0)
                if total_size > _remaining:
                    if not allow_span:
                        raise TapeError(
                            f"Backup needs {bytes_human(int(total_size))} but loaded tape {vol} only has "
                            f"{bytes_human(int(_remaining))} free. Enable 'Span backups across tapes' on the "
                            "Retention page, load a larger/empty tape, or reduce the selection."
                        )
                    # The rest goes to other tapes in the library; make sure
                    # there is room for it before writing anything.
                    _pick_backup_tape(required_bytes=int(total_size - _remaining), exclude=[vol],
                                      allow_span=True)
                    append_backup_log(
                        f"Backup ({bytes_human(total_size)}) is larger than the free space on {vol} "
                        f"(~{bytes_human(int(_remaining))}) — it will continue on further tapes.",
                        level="minimal")

        # ── Pre-backup hook ──────────────────────────────────────────────────
        if PRE_BACKUP_HOOK:
            set_backup_state(status="pre_hook")
            publish_state_to_mqtt(refresh_state())
            if not run_hook(PRE_BACKUP_HOOK, "pre-backup"):
                raise TapeError("Pre-backup hook failed — aborting.")

        # ── Position the tape ────────────────────────────────────────────────
        # Backups are appended after what is already on the tape (each backup
        # is one tape file), unless the tape is being rewritten from the start.
        if ERASE_BEFORE_BACKUP:
            append_backup_log("Rewinding tape before backup.")
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
            append_backup_log("Erasing tape…")
            set_backup_state(status="erasing")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mt", "-f", TAPE, "erase"], timeout=7200)
            overwrite_first = True
        set_backup_state(status="positioning", last_message=f"Positioning {vol}…")
        publish_state_to_mqtt(refresh_state())
        _first_file = _position_for_write(vol, overwrite_first, _log)
        if overwrite_first:
            append_backup_log(f"Writing {vol} from the start (tape file 0).", level="minimal")

        # ── Build incremental args ───────────────────────────────────────────
        extra_args, snap_file = incremental_tar_args(selected, job_id, backup_mode)
        if backup_mode != "full":
            append_backup_log(f"Incremental mode '{backup_mode}' — snapshot: {snap_file}", level="normal")

        # ── Compute the archive prefix directory name ────────────────────────
        # Every file in the archive is stored under a unique top-level directory
        # so that restoring it always produces an isolated, identifiable folder.
        # The name follows the same pattern as the restore subfolder setting,
        # made unique across the library: restore finds a backup by this name,
        # and a tape now holds several backups.
        _backup_dirname = unique_dirname(
            build_backup_dirname(volume_tag=vol, start_ts=start, label=label), start
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
        #     └─ stdout ──► mbuffer -m 512M -s 512k -P 75 -o /dev/nst0 -A <swap>
        #
        # If mbuffer is present it smooths the stream AND writes the tape itself:
        #   -P 75  — don't start writing to tape until buffer is 75% full; this gives
        #            the tape drive a large burst to start with and reduces shoe-shining.
        #   -o     — write straight to the tape device (512 KiB blocks, like dd did).
        #   -A     — at end-of-media, close the tape (writing its filemark) and run
        #            the swap command, then continue on the next tape: this is how
        #            one backup spans several tapes (see _TapeSpan).
        #   -v 4   — status lines (speed, bytes out) and end-of-volume block counts
        #            on stderr, parsed for progress and per-tape sizes.
        #
        # If mbuffer is absent: fall back to pv | dd, or plain dd (no spanning).
        #
        # All inter-process pipe buffers are enlarged to 1 MiB via fcntl F_SETPIPE_SZ.
        # The default 64 KiB kernel pipe buffer can cause tar to block waiting for the
        # next process to drain it, especially during filesystem metadata reads.
        #
        # tar --sparse detects and efficiently archives sparse files (VM disk images,
        # database files with pre-allocated space) without expanding empty regions.
        # tar --totals reports the exact archive length at the end.
        #
        # No software compression — LTO hardware compression is always faster and
        # produces better ratios than software compression on typical data.

        set_backup_state(status="streaming", tapes=[vol], current_tape=vol)
        append_backup_log("Starting tar → tape pipeline.", level="minimal")
        publish_state_to_mqtt(refresh_state())

        _TAPE_BLOCK_BYTES = TAPE_BLOCK_BYTES
        _MBUF_SIZE        = os.getenv("TL_MBUF_SIZE", "512M")  # larger default buffer
        _MBUF_FILL_PCT    = os.getenv("TL_MBUF_FILL_PCT", "75")  # fill % before writing

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
        # --ignore-failed-read: a file or folder tar can't read (permission
        # denied, vanished mid-backup, …) is skipped with a warning instead of
        # making tar exit 2 and failing the whole backup.  Skipped items are
        # reported once tar finishes.
        # Excludes: --anchored matches each pattern from the start of the member
        # name (so mnt/user/appdata/plex never also drops some other .../plex),
        # and --no-wildcards takes folder names containing * ? [ literally.
        # tar skips an excluded directory without descending into it.
        _EXCLUDE_ARGS = (["--anchored", "--no-wildcards"]
                         + [f"--exclude={os.path.relpath(x, '/')}" for x in excluded]
                         if excluded else [])
        tar_cmd = (["tar", "-C", "/", "-cvf", "-", "--ignore-failed-read", "--totals"]
                   + _SPARSE_ARGS + _XATTR_ARGS + extra_args + _EXCLUDE_ARGS + rels)

        span = _TapeSpan(vol, _first_file, total_size, allow_span, _log)

        def _try_set_pipe_size(fd, size: int = 1048576) -> None:
            """Increase a pipe's kernel buffer to reduce blocking between stages.
            F_SETPIPE_SZ = 1031, F_GETPIPE_SZ = 1032 (Linux-specific).
            Silently ignored if unsupported (older kernels, non-Linux)."""
            try:
                fcntl.fcntl(fd, 1031, size)
            except Exception:
                pass

        dd_cmd = None
        if _has_mbuffer:
            # -s: block size (must match tape block size)
            # -m: total ring buffer size
            # -P: start writing when buffer reaches this % full (reduces shoe-shining)
            # -f: allow overwriting an existing output (only matters off /dev)
            # --no-direct: plain write()s to the tape device, as dd did
            mbuf_cmd = [
                "mbuffer",
                "-s", str(_TAPE_BLOCK_BYTES),
                "-m", _MBUF_SIZE,
                "-P", str(_MBUF_FILL_PCT),
                "-v", "4",
                "-f", "--no-direct",
                "-o", TAPE,
                "-A", span.autoload_cmd(),
            ]
            _pipeline_tools = ["mbuffer"]
        else:
            mbuf_cmd = None
            # dd: final writer — large block size, write directly to tape device.
            dd_cmd = ["dd", f"bs={_TAPE_BLOCK_BYTES}", f"of={TAPE}", "iflag=fullblock", "status=progress"]
            _pipeline_tools = (["pv", "dd"] if _has_pv else ["dd"])

        append_backup_log(
            f"Pipeline: tar --sparse | {' | '.join(_pipeline_tools)} → {TAPE}  "
            f"(block={_TAPE_BLOCK_BYTES//1024}KiB"
            f"{', buf=' + _MBUF_SIZE + ' fill=' + str(_MBUF_FILL_PCT) + '%' if _has_mbuffer else ''}"
            f"{', spanning' if allow_span else ''}"
            f"{', skip_xattrs' if _SKIP_XATTRS else ''}"
            f")",
            level="minimal",
        )

        # ── Spawn processes ──────────────────────────────────────────────────
        log_pipeline(
            "backup", tar_cmd,
            mbuf_cmd if _has_mbuffer else (["pv", "-n", "-F", "%b", "-i", "2"] if _has_pv else None),
            dd_cmd,
        )
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
        dd_proc   = None

        if _has_mbuffer:
            # tar → mbuffer → tape
            span.start()
            mbuf_proc = subprocess.Popen(
                mbuf_cmd,
                stdin=prev_stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                close_fds=True,
            )
            prev_stdout.close()
        else:
            if _has_pv:
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
        writer_proc = mbuf_proc or dd_proc

        # ── Background thread: drain progress stderr ─────────────────────────
        # mbuffer: status lines give speed and bytes out; end-of-volume lines
        #   give the exact per-tape split (fed to span).
        # pv: byte count.  dd: byte count and speed.
        #
        # All reads use os.read() on raw fds — no Python IO buffering delay.
        _pv_bw_ref    = [0]     # bytes written (updated by drain thread)
        _dd_speed_ref = [0.0]   # speed in bytes/s
        _pv_stderr_lines = []

        def _drain_progress_stderr():
            """Read progress data from mbuffer stderr, pv stderr, or dd stderr."""
            if mbuf_proc:
                src_proc, src_label = mbuf_proc, "mbuffer"
            elif pv_proc:
                src_proc, src_label = pv_proc, "pv"
            else:
                src_proc, src_label = dd_proc, "dd"

            src_fd = src_proc.stderr.fileno() if src_proc and src_proc.stderr else None
            if src_fd is None:
                return

            buf = b""
            while True:
                try:
                    chunk = os.read(src_fd, 4096)
                    if not chunk:
                        break
                    *lines, buf = re.split(rb"[\r\n]", buf + chunk)
                    for line_b in lines:
                        line = line_b.decode(errors="ignore").strip()
                        if not line:
                            continue
                        if src_label == "mbuffer":
                            span.feed_mbuffer_line(line)
                            speed = span.speed_from_line(line)
                            if speed is not None:
                                _dd_speed_ref[0] = speed
                                _pv_bw_ref[0] = span.out_total
                                continue    # status line: not worth keeping
                            if backup_log_allows("verbose") or "error" in line.lower() or "end of volume" in line:
                                append_backup_log(line, level="verbose")
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
                                if line.startswith("tar: ") or line.startswith(_TAR_TOTALS_PREFIX):
                                    # tar diagnostic, not an archived member.
                                    # Surface skipped items live; everything
                                    # else (e.g. "Directory is new") is noise.
                                    if (backup_log_allows("normal")
                                            and any(m in line for m in _TAR_SKIP_MARKERS)):
                                        append_backup_log(line, level="normal")
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

        # ── Progress polling loop — waits for the tape writer to finish ─────
        cancel_requested = False
        _last_bw = 0
        _last_bw_ts = time.time()
        _rolling_speed = 0.0

        try:
            while writer_proc.poll() is None:
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

                if span.swapping:
                    continue    # the swap reports its own progress

                # Byte count from drain thread (mbuffer, pv or dd progress parsing)
                bw = _pv_bw_ref[0]
                now_t = time.time()
                elapsed = max(now_t - start, 0.001)

                # Rolling speed over the last interval (more accurate than total average)
                interval = max(now_t - _last_bw_ts, 0.001)
                interval_bytes = bw - _last_bw
                if interval_bytes > 0:
                    _rolling_speed = interval_bytes / interval
                elif _dd_speed_ref[0] > 0:
                    # Fall back to the writer's own speed report
                    _rolling_speed = _dd_speed_ref[0]
                _last_bw = bw
                _last_bw_ts = now_t

                speed = _rolling_speed if _rolling_speed > 0 else (bw / elapsed if bw > 0 else 0.0)
                pct   = min(bw / total_size * 100.0, 100.0) if total_size > 0 and bw > 0 else 0.0
                eta   = int((total_size - bw) / speed) if speed > 0 and total_size > bw > 0 else None
                entries = _tar_entry_count[0]
                _tape_note = (f" — tape {len(span.segments)}: {span.current['volume_tag']}"
                              if len(span.segments) > 1 else "")
                set_backup_state(
                    bytes_written=bw, percent=pct,
                    speed_bps=speed, eta_seconds=eta,
                    status="streaming",
                    last_message=(
                        f"{bytes_human(bw)} / {bytes_human(total_size)} "
                        f"— {bytes_human(speed)}/s "
                        f"— {entries:,} files "
                        f"— ETA {secs_human(eta)}{_tape_note}"
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
        # Join the drain thread first — it owns the writer's stderr fd
        pv_drain.join(timeout=15)

        tar_rc = tar_proc.wait(timeout=120)
        if pv_proc:
            pv_proc.wait(timeout=30)
        writer_rc = writer_proc.wait(timeout=60)
        span.close()

        # dd stderr:
        #   - pv present: pv_drain was on pv stderr → dd stderr still readable
        #   - no pv: pv_drain was on dd stderr → use _pv_stderr_lines
        #   - mbuffer: its non-status lines are in _pv_stderr_lines
        writer_err_out = ""
        if dd_proc and pv_proc:
            try:
                writer_err_out = (dd_proc.stderr.read() or b"").decode(errors="ignore").strip()
            except Exception:
                pass
        else:
            writer_err_out = "\n".join(_pv_stderr_lines[-10:])

        log_exit_codes("backup", tar=tar_rc, **({"mbuffer": writer_rc} if mbuf_proc else {}),
                       **({"pv": pv_proc.returncode} if pv_proc else {}),
                       **({"dd": writer_rc} if dd_proc else {}))

        # Final byte count.  When the whole archive reached the tape, tar
        # --totals is its exact length.  Otherwise (write failed, cancelled)
        # count what the tape writer says it wrote: tar may have pushed more
        # into the pipe than ever reached the tape.
        _tar_total = _read_tar_totals(_tar_log_path)
        _writer_bytes = span.written_bytes if mbuf_proc else _pv_bw_ref[0]
        if writer_rc == 0 and not cancel_requested and _tar_total:
            bw = _tar_total
        elif _writer_bytes > 0:
            bw = _writer_bytes
        else:
            bw = total_size if (writer_rc == 0 and not cancel_requested) else 0
        span.finish(bw)
        vol = span.current["volume_tag"]   # the tape in the drive now
        if span.swapped:
            _auto_loaded_slot = _auto_loaded_slot or -1   # make sure it goes back on failure

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
                        if l.strip() and not l.startswith(_TAR_TOTALS_PREFIX.encode())
                    ][-20:]
        except Exception:
            pass

        # Did the tape writer fail?  Checked before tar: a dead writer makes tar
        # fail with a broken pipe, which would hide the real cause.
        if writer_rc not in (0, -15) and not cancel_requested:
            if mbuf_proc:
                tape_full = span.full_without_span
                if span.error and not tape_full:
                    raise TapeError(f"Could not continue on another tape: {span.error}")
            else:
                # dd exits non-zero with ENOSPC when the tape is full.  Some
                # drives/kernels instead report a bare "Input/output error" for
                # the same condition (hit more often on bigger multi-folder
                # backups that run past where a smaller single-folder backup
                # used to stop) — confirm via `mt status` EOD/EOT flags before
                # treating that ambiguous case as full.
                tape_full = _is_tape_full_error(Exception(writer_err_out))
                if not tape_full and "input/output error" in writer_err_out.lower():
                    tape_full = _mt_status_shows_eot()
                    if tape_full:
                        append_backup_log(
                            "dd reported a bare I/O error; mt status confirms EOD/EOT — treating as tape-full.",
                            level="normal",
                        )
            if AUTO_REWRITE_ON_FULL and tape_full:
                append_backup_log(f"Tape full detected (rc={writer_rc}): {writer_err_out[:200]}", level="minimal")
                append_backup_log("Switching to oldest available/recyclable tape and restarting.", level="minimal")
                _record_partial(span, _backup_dirname, record_id, "failed")
                _switch_to_rewrite_candidate(vol)
                return backup_worker(selected, backup_mode=backup_mode, job_id=job_id, label=label, log_level=log_level)
            elif tape_full:
                append_backup_log(f"Tape full detected (rc={writer_rc}): {writer_err_out[:200]}", level="minimal")
                raise TapeError("Tape is full. Enable 'Span backups across tapes', or load a new/recyclable "
                                "tape and start the backup again.")
            elif writer_err_out:
                append_backup_log(f"Tape write error (rc={writer_rc}): {writer_err_out[:300]}", level="minimal")
                raise TapeError(f"Write to tape failed (rc={writer_rc}): {writer_err_out[:200]}")
            else:
                raise TapeError(f"Write to tape failed (rc={writer_rc}).")

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
            _record_partial(span, _backup_dirname, record_id, "cancelled")
            add_backup_record({
                "id": record_id,
                "label": label or job_id,
                "volume_tag": span.segments[0]["volume_tag"],
                "volumes": span.volumes,
                "segments": span.record_segments(),
                "paths": selected,
                "excludes": excluded,
                "mode": backup_mode,
                "status": "cancelled",
                "started_at": int(start),
                "finished_at": now_ts(),
                "bytes_written": bw,
                "log_level": log_level,
                "backup_dirname": _backup_dirname,
            })
            span = None
            if POST_BACKUP_HOOK:
                run_hook(POST_BACKUP_HOOK, "post-backup (after cancel)")
            publish_state_to_mqtt(refresh_state())
            return
        # Collect everything tar had to skip (unreadable files/folders).
        _skips = _scan_tar_log_for_skips(_tar_log_path)
        skipped_count = _skips["count"]
        skipped_items = _skips["items"]

        # tar exit codes: 0 = success, 1 = warnings (files changed/skipped), 2+ = fatal.
        # rc==1 is normal for live filesystems — treat as success.
        # rc==2 caused only by unreadable files (which --ignore-failed-read should
        # already downgrade, but not every read failure is covered on every tar
        # version) is also a success: the writer finished cleanly, so the archive
        # is complete apart from the skipped items.
        if rc == 2 and skipped_count and not _skips["fatal"]:
            append_backup_log(
                "tar exited 2 only because some items could not be read — continuing.",
                level="normal",
            )
        elif rc not in (0, 1):
            # Use real tar output from log file, not dd progress lines
            if _tar_error_lines:
                append_backup_log(f"tar stderr: {chr(10).join(_tar_error_lines[-20:])}", level="minimal")
            err_msg = "\n".join(_tar_error_lines[-10:]).strip() or f"tar failed (rc={rc})"
            raise TapeError(err_msg)
        elif rc == 1 and _tar_error_lines:
            # Log warnings but continue
            append_backup_log(f"tar completed with warnings (rc=1): {_tar_error_lines[-1]}", level="normal")

        if skipped_count:
            append_backup_log(
                f"⚠ Skipped {skipped_count:,} item(s) that could not be read "
                "(e.g. permission denied) — the rest of the backup continued:",
                level="minimal",
            )
            for _item in skipped_items[:50]:
                append_backup_log(f"  skipped: {_item}", level="minimal")
            if skipped_count > 50:
                append_backup_log(f"  …and {skipped_count - 50:,} more.", level="minimal")

        append_backup_log(f"Tar complete. Wrote {bytes_human(bw)}.", level="minimal")
        segments = span.record_segments()
        if len(segments) > 1:
            append_backup_log(
                f"Backup spans {len(segments)} tapes: "
                + ", ".join(f"{s['volume_tag']} (file {s['file_number']}, {bytes_human(s['bytes'])})"
                            for s in segments) + ".",
                level="minimal")
        # Do NOT re-fetch vol from state_cache here — by this point the state cache may
        # have been refreshed and the tape may already be returning to its slot, causing
        # vol to come back empty and the index/verify steps to be skipped entirely.
        # vol was set earlier when the tape was loaded and is still valid.

        # ── Index ────────────────────────────────────────────────────────────
        # Build the file index from the tar verbose log captured during streaming.
        # This avoids re-reading the entire tape (which would timeout on large backups
        # and leave dd holding /dev/nst0 busy for subsequent rewind/verify steps).
        # The backup is recorded on every tape it is on, with its position.
        append_backup_log("Building tape index from backup log…")
        set_backup_state(status="indexing")
        publish_state_to_mqtt(refresh_state())
        fl = []
        try:
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
                # tar's diagnostic lines (and the --totals summary) here —
                # otherwise they get indexed as bogus entries in the restore browser.
                fl = [p for p in fl if not p.startswith("tar: ") and not p.startswith(_TAR_TOTALS_PREFIX)]
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
                append_backup_log("Warning: tar log was empty — file index not saved.", level="normal")
        except Exception as e:
            append_backup_log(f"Warning: index failed: {e}", level="normal")
            # Clean up tar log on error too
            try:
                _lp = locals().get("_tar_log_path", "")
                if _lp and os.path.exists(_lp):
                    os.unlink(_lp)
            except Exception:
                pass
        try:
            add_backup_sessions(segments, _backup_dirname, record_id, "completed",
                                files=fl or None, written_at=now_ts())
            for _seg in segments:
                update_tape_index_metadata(_seg["volume_tag"], present=True)
            if fl:
                append_backup_log(
                    f"Index saved: {len(fl)} entries on {', '.join(span.volumes)}.", level="normal")
        except Exception as e:
            append_backup_log(f"Warning: saving tape catalog failed: {e}", level="minimal")
        _chain = [{"volume_tag": s["volume_tag"], "file_number": s["file_number"]} for s in segments]
        span = None     # recorded; nothing left to clean up on failure

        # ── Verify ───────────────────────────────────────────────────────────
        if VERIFY_AFTER_BACKUP and vol:
            append_backup_log("Starting post-backup verification…")
            set_backup_state(status="verifying")
            publish_state_to_mqtt(refresh_state())
            try:
                verify_worker(_chain[0]["volume_tag"], backup_record_id=record_id, chain=_chain)
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

        # Verifying a multi-tape backup reloads its tapes; what is in the drive now?
        _now_loaded = loaded_volume()
        if _now_loaded != "?":
            vol = _now_loaded

        # ── Rewind after ─────────────────────────────────────────────────────
        if AUTO_REWIND_AFTER and vol:
            set_backup_state(status="rewinding")
            append_backup_log("Rewinding after backup.")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

        # ── Auto-unload tape back to its slot ────────────────────────────────
        # Return the tape to the slot it came from.  We do NOT exclude _auto_loaded_slot
        # here — that is the tape's home slot and we want to return it there.
        _return_slot = _find_return_slot(vol) if vol else None
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
            if vol:
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
        _done_msg = (f"Backup completed — {skipped_count:,} unreadable item(s) skipped."
                     if skipped_count else "Backup completed successfully.")
        if len(segments) > 1:
            _done_msg += f" Spans {len(segments)} tapes: {' → '.join(s['volume_tag'] for s in segments)}."
        set_backup_state(
            running=False, status="completed", bytes_written=bw, percent=100.0,
            speed_bps=bw / elapsed_total, eta_seconds=0,
            finished_at=now_ts(), last_message=_done_msg, error=None,
        )
        append_backup_log(_done_msg)
        log_action("backup", True, f"Completed for {', '.join(selected)}",
                   {"bytes_written": bw, "tapes": [s["volume_tag"] for s in segments]})
        for _seg in segments:
            _record_backup_done(_seg["volume_tag"], int(_seg["bytes"]))

        # ── Backup record ────────────────────────────────────────────────────
        add_backup_record({
            "id": record_id,
            "label": label or job_id,
            "volume_tag": segments[0]["volume_tag"],
            "volumes": [s["volume_tag"] for s in segments],
            "segments": segments,
            "paths": selected,
            "excludes": excluded,
            "mode": backup_mode,
            "status": "completed",
            "started_at": int(start),
            "finished_at": now_ts(),
            "bytes_written": bw,
            "speed_bps": bw / elapsed_total,
            "verified": verified,
            "verify_errors": verify_errors,
            "skipped_count": skipped_count,
            "skipped_items": skipped_items,
            "log_level": log_level,
            "backup_dirname": _backup_dirname,
        })

        # ── Notify ───────────────────────────────────────────────────────────
        notify_backup_success(" → ".join(s["volume_tag"] for s in segments), selected, bw,
                              elapsed_total, verified, verify_errors, skipped=skipped_count)

    except Exception as e:
        elapsed_total = max(time.time() - start, 0.001)
        set_backup_state(
            running=False, status="failed", finished_at=now_ts(),
            error=str(e), last_message=f"Backup failed: {e}", eta_seconds=None,
        )
        append_backup_log(f"Backup failed: {e}")
        log_action("backup", False, str(e))
        log_traceback("backup", e)
        _segs = None
        if span is not None:
            try:
                span.close()
                if not span.segments[-1]["bytes"]:
                    span.finish(span.written_bytes or bw)
                vol = span.current["volume_tag"]
                _record_partial(span, _backup_dirname, record_id, "failed")
                _segs = span.record_segments()
                if span.swapped:
                    _auto_loaded_slot = _auto_loaded_slot or -1
            except Exception as rec_err:
                append_backup_log(f"Warning: could not record the partial backup: {rec_err}")
        add_backup_record({
            "id": record_id,
            "label": label or job_id,
            "volume_tag": (_segs[0]["volume_tag"] if _segs else vol),
            **({"volumes": [s["volume_tag"] for s in _segs], "segments": _segs} if _segs else {}),
            "paths": selected,
            "excludes": excluded,
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
                    cur_vol = loaded_volume() or vol
                    _return_slot = _find_return_slot(cur_vol) or (_auto_loaded_slot if _auto_loaded_slot > 0 else None)
                    if _return_slot:
                        append_backup_log(
                            f"Auto-unloading tape {cur_vol} to slot {_return_slot} after failure.",
                            level="minimal")
                        run_cmd(["mtx", "-f", CHANGER, "unload", str(_return_slot), "0"],
                                timeout=max(COMMAND_TIMEOUT, 120))
                        _save_last_known_loaded_slot(None)
            except Exception:
                pass
        publish_state_to_mqtt(refresh_state())


_TAR_TOTALS_PREFIX = "Total bytes written:"


def _read_tar_totals(log_path: str) -> int:
    """Exact archive length from tar's --totals line, or 0 if absent."""
    try:
        with open(log_path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(size - 8192, 0))
            tail = fh.read().decode(errors="ignore")
    except OSError:
        return 0
    m = None
    for m in re.finditer(r"^Total bytes written:\s*(\d+)", tail, re.M):
        pass
    return int(m.group(1)) if m else 0


def _record_partial(span: Optional["_TapeSpan"], dirname: str, record_id: str, status: str) -> None:
    """Record the tape files a failed or cancelled backup left behind: they
    still take space, and later backups are appended after them."""
    from .tape_layout import add_backup_sessions
    if span is None:
        return
    segs = span.record_segments()
    if len(segs) == 1 and not segs[0]["bytes"]:
        return      # nothing reached the tape
    add_backup_sessions(segs, dirname or f"(partial-{record_id})", record_id, status, files=None)


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

