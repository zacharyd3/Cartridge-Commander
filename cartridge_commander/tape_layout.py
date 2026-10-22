"""tape_layout module — where each backup lives on tape.

Backups are appended to a tape one after another.  Each backup is one tape
file: a tar stream closed by a filemark, so the Nth backup on a tape is tape
file N (counting from 0).  A backup too big for the space left on its tape
continues onto another tape — mbuffer asks for a new tape at end-of-media and
the backup worker swaps it — so one backup can be a *chain* of segments:

    [{"volume_tag": "AB0001L6", "file_number": 3},   # tar stream starts here
     {"volume_tag": "AB0002L6", "file_number": 0}]   # ...and continues here

Reading the segments back in order and concatenating them gives the original
tar stream, which is how restore and verify read a spanned backup.

The catalog keeps a per-tape list of "sessions", one per tape file, in the
``sessions_json`` column:

    {"dirname":     top-level archive folder of the backup,
     "file_number": tape file this segment is on this tape,
     "part", "parts": which segment of the chain this is (1-based) / how many,
     "chain":       every segment of the backup, in order (see above),
     "record_id":   the backup record, when known,
     "bytes":       bytes of this segment on this tape,
     "status":      completed | failed | cancelled | legacy | found,
     "broken":      True once another segment of the chain was overwritten}

Writing at file N of a tape destroys everything from file N onwards, so every
write goes through ``forget_sessions_from`` first: the sessions it destroys
are dropped, their records are marked ``overwritten``, and any other segments
of those chains are marked ``broken``.
"""

import json
import os
import re
import subprocess
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from .config import CHANGER, COMMAND_TIMEOUT, TAPE, TAPE_BLOCK_BYTES
from . import state as shared_state
from .logsetup import get_logger

_log = get_logger("tape")

# Serialises catalog read-modify-write of a tape's session list.
_layout_lock = threading.RLock()


# ---------------------------------------------------------------------------
# Catalog access
# ---------------------------------------------------------------------------

def _load_row(vol: str) -> Optional[Dict[str, Any]]:
    from .db import tape_catalog_conn, _json_list
    with tape_catalog_conn() as conn:
        row = conn.execute(
            "SELECT volume_tag, files_json, backup_dirnames, sessions_json, used_bytes, capacity_bytes "
            "FROM tape_catalog WHERE volume_tag = ? AND is_deleted = 0", (vol,)
        ).fetchone()
    if not row:
        return None
    return {
        "volume_tag": row["volume_tag"],
        "files": _json_list(row["files_json"]),
        "backup_dirnames": _json_list(row["backup_dirnames"]),
        "sessions": _json_list(row["sessions_json"]),
        "used_bytes": row["used_bytes"],
        "capacity_bytes": row["capacity_bytes"],
    }


def get_sessions(vol: str) -> List[Dict[str, Any]]:
    """Sessions recorded for ``vol``, ordered by tape file."""
    from .db import tape_catalog_conn, _json_list
    if not vol:
        return []
    with tape_catalog_conn() as conn:
        row = conn.execute(
            "SELECT sessions_json FROM tape_catalog WHERE volume_tag = ? AND is_deleted = 0", (vol,)
        ).fetchone()
    sessions = _json_list(row["sessions_json"]) if row else []
    return sorted(sessions, key=lambda s: int(s.get("file_number") or 0))


def _space_fields(vol: str, used: int, capacity: Optional[int]) -> Dict[str, Any]:
    from .drive_history import build_tape_space_info
    info = build_tape_space_info(vol, idx={"volume_tag": vol, "used_bytes": used,
                                           "capacity_bytes": capacity})
    cap = info.get("capacity_bytes")
    remaining = max(int(cap) - used, 0) if cap else None
    return {
        "used_bytes": used,
        "remaining_bytes": remaining,
        "remaining_pct": (max(0.0, min(100.0, remaining / cap * 100.0)) if cap and remaining is not None else None),
        "lto_generation": info.get("lto_generation"),
        "capacity_bytes": cap,
    }


def _write_layout(vol: str, sessions: List[Dict[str, Any]], files: Optional[List[str]] = None,
                  dirnames: Optional[List[str]] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """Persist a tape's sessions (and optionally its file list / dirnames),
    recomputing used space from the sessions."""
    from .db import tape_catalog_conn, update_tape_index_metadata
    from .state import now_ts
    sessions = sorted(sessions, key=lambda s: int(s.get("file_number") or 0))
    row = _load_row(vol)
    if row is None:
        update_tape_index_metadata(vol, present=True)
        row = _load_row(vol) or {"capacity_bytes": None}
    used = sum(int(s.get("bytes") or 0) for s in sessions)
    space = _space_fields(vol, used, row.get("capacity_bytes"))
    sets = {
        "sessions_json": json.dumps(sessions),
        "used_bytes": space["used_bytes"],
        "remaining_bytes": space["remaining_bytes"],
        "remaining_pct": space["remaining_pct"],
        "space_estimated": 0,
        "updated_at": now_ts(),
    }
    if space.get("capacity_bytes"):
        sets["capacity_bytes"] = space["capacity_bytes"]
    if space.get("lto_generation"):
        sets["lto_generation"] = space["lto_generation"]
    if files is not None:
        sets["files_json"] = json.dumps(files)
        sets["file_count"] = len(files)
    if dirnames is not None:
        sets["backup_dirnames"] = json.dumps(dirnames)
    sets.update(extra or {})
    cols = ", ".join(f"{k} = ?" for k in sets)
    with tape_catalog_conn() as conn:
        conn.execute(f"UPDATE tape_catalog SET {cols} WHERE volume_tag = ? AND is_deleted = 0",
                     (*sets.values(), vol))
        conn.commit()


def tape_used_bytes(vol: str) -> Optional[int]:
    """Bytes used on ``vol`` according to its sessions, or None when the tape
    has no session list yet (never written by this version)."""
    sessions = get_sessions(vol)
    if not sessions:
        return None
    return sum(int(s.get("bytes") or 0) for s in sessions)


def tape_has_data(vol: str) -> bool:
    row = _load_row(vol)
    if not row:
        return False
    return bool(row["sessions"] or row["files"] or (row["used_bytes"] or 0) > 0)


# ---------------------------------------------------------------------------
# Record helpers
# ---------------------------------------------------------------------------

def record_segments(rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Segments of a backup record; records from before tape spanning are a
    single segment at the start of their tape."""
    segs = rec.get("segments")
    if isinstance(segs, list) and segs:
        return segs
    vol = rec.get("volume_tag") or ""
    return [{"volume_tag": vol, "file_number": 0, "bytes": int(rec.get("bytes_written") or 0)}] if vol else []


def record_volumes(rec: Dict[str, Any]) -> List[str]:
    vols: List[str] = []
    for seg in record_segments(rec):
        v = seg.get("volume_tag")
        if v and v not in vols:
            vols.append(v)
    return vols


def _mark_records(record_ids: Iterable[str], **fields: Any) -> int:
    from .records import _save_backup_records
    ids = {str(r) for r in record_ids if r}
    if not ids:
        return 0
    n = 0
    with shared_state._backup_records_lock:
        for rec in shared_state._backup_records:
            if str(rec.get("id")) in ids:
                rec.update(fields)
                n += 1
    if n:
        _save_backup_records()
    return n


# ---------------------------------------------------------------------------
# Overwrites
# ---------------------------------------------------------------------------

def forget_sessions_from(vol: str, file_number: int, reason: str = "") -> List[Dict[str, Any]]:
    """Record that ``vol`` is about to be written at ``file_number``: every
    session at or after it is destroyed.  Returns the dropped sessions.

    Writing at file 0 also clears any file list left from before sessions
    were tracked (the whole tape is being rewritten).
    """
    from .state import now_ts
    with _layout_lock:
        row = _load_row(vol)
        if row is None:
            return []
        sessions = row["sessions"]
        keep = [s for s in sessions if int(s.get("file_number") or 0) < file_number]
        dropped = [s for s in sessions if int(s.get("file_number") or 0) >= file_number]
        if not dropped and file_number > 0:
            return []
        dropped_dirs = {s.get("dirname") for s in dropped if s.get("dirname")}
        if file_number == 0:
            files: List[str] = []
            dirnames: List[str] = []
        else:
            files = [f for f in row["files"] if f.split("/", 1)[0] not in dropped_dirs]
            dirnames = [d for d in row["backup_dirnames"] if d not in dropped_dirs]
        _write_layout(vol, keep, files=files, dirnames=dirnames)

    # Records whose data was destroyed.  At file 0 that includes any record on
    # this tape from before sessions were tracked.
    ids = {s.get("record_id") for s in dropped if s.get("record_id")}
    if file_number == 0:
        with shared_state._backup_records_lock:
            for rec in shared_state._backup_records:
                if rec.get("overwritten"):
                    continue
                if not rec.get("segments") and rec.get("volume_tag") == vol:
                    ids.add(rec.get("id"))
    _mark_records(ids, overwritten=True, overwritten_at=now_ts(),
                  overwritten_reason=reason or f"{vol} rewritten from tape file {file_number}")

    # Other tapes holding segments of a destroyed chain: those segments still
    # take space but can no longer be restored.
    for s in dropped:
        chain = s.get("chain") or []
        for seg in chain:
            other = seg.get("volume_tag")
            if not other or other == vol:
                continue
            with _layout_lock:
                other_sessions = get_sessions(other)
                changed = False
                for os_ in other_sessions:
                    if os_.get("dirname") == s.get("dirname") and not os_.get("broken"):
                        os_["broken"] = True
                        changed = True
                if changed:
                    _write_layout(other, other_sessions)
    if dropped:
        _log.info("%s: %d session(s) from tape file %d onwards are being overwritten: %s",
                  vol, len(dropped), file_number, ", ".join(sorted(d for d in dropped_dirs if d)))
    return dropped


def forget_tape(vol: str, reason: str = "") -> None:
    """The whole tape was erased (format): drop every session on it."""
    forget_sessions_from(vol, 0, reason=reason or f"{vol} erased")


# ---------------------------------------------------------------------------
# Recording new backups
# ---------------------------------------------------------------------------

def add_backup_sessions(segments: List[Dict[str, Any]], dirname: str, record_id: str,
                        status: str, files: Optional[List[str]] = None,
                        written_at: Optional[int] = None) -> None:
    """Add one backup (all of its segments) to the catalog of each tape it is on.

    ``files`` (the backup's member list) is added to the file index of every
    tape in the chain, so browsing any of them offers the whole backup.
    """
    from .state import now_ts
    chain = [{"volume_tag": s["volume_tag"], "file_number": int(s.get("file_number") or 0)} for s in segments]
    parts = len(segments)
    ts = int(written_at or now_ts())
    for i, seg in enumerate(segments):
        vol = seg["volume_tag"]
        with _layout_lock:
            row = _load_row(vol)
            if row is None:
                from .db import update_tape_index_metadata
                update_tape_index_metadata(vol, present=True)
                row = _load_row(vol) or {"sessions": [], "files": [], "backup_dirnames": []}
            fn = int(seg.get("file_number") or 0)
            sessions = [s for s in row["sessions"] if int(s.get("file_number") or 0) != fn]
            sessions.append({
                "dirname": dirname,
                "file_number": fn,
                "part": i + 1,
                "parts": parts,
                "chain": chain,
                "record_id": record_id,
                "bytes": int(seg.get("bytes") or 0),
                "written_at": ts,
                "status": status,
                **({"ended_at_eom": True} if seg.get("ended_at_eom") else {}),
            })
            new_files = None
            new_dirnames = None
            extra: Dict[str, Any] = {}
            if files:
                prefix = dirname + "/"
                new_files = [f for f in row["files"] if not (f == dirname or f.startswith(prefix))] + list(files)
                new_dirnames = [d for d in row["backup_dirnames"] if d != dirname] + [dirname]
                extra = {"written_at": ts}
            _write_layout(vol, sessions, files=new_files, dirnames=new_dirnames, extra=extra)


def unique_dirname(dirname: str, start_ts: float) -> str:
    """Make a backup's archive folder name unique across the library.

    With backups appended to tapes, two runs on the same day with the default
    ``{volume}_{date}`` pattern would otherwise share a folder name, and
    restore finds a backup by its folder name.
    """
    import datetime
    taken = set()
    from .db import list_all_known_indexes
    for idx in list_all_known_indexes():
        taken.update(idx.get("backup_dirnames") or [])
        taken.update(s.get("dirname") for s in (idx.get("sessions") or []))
    with shared_state._backup_records_lock:
        taken.update(r.get("backup_dirname") for r in shared_state._backup_records
                     if not r.get("overwritten"))
    if dirname not in taken:
        return dirname
    stamp = datetime.datetime.fromtimestamp(start_ts).strftime("%H%M%S")
    cand = f"{dirname}_{stamp}"
    n = 2
    while cand in taken:
        cand = f"{dirname}_{stamp}_{n}"
        n += 1
    return cand


# ---------------------------------------------------------------------------
# Live re-index
# ---------------------------------------------------------------------------

def sessions_from_layout(vol: str, layout: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rebuild a tape's session list from a live read of the tape.

    The tape is the authority on which backups exist and where; what the
    catalog already knew about each one (its chain, record, status) is kept.
    """
    known = {}
    for s in get_sessions(vol):
        if s.get("dirname"):
            known[s["dirname"]] = s
    out: List[Dict[str, Any]] = []
    for entry in layout:
        fn = int(entry.get("file_number") or 0)
        names = entry.get("dirnames") or []
        if not names:
            continue
        for name in names:
            prev = known.get(name)
            if prev:
                sess = dict(prev, file_number=fn)
                # A chain visits each tape at most once.
                sess["chain"] = [
                    dict(seg, file_number=fn) if seg.get("volume_tag") == vol else seg
                    for seg in (prev.get("chain") or [])
                ] or [{"volume_tag": vol, "file_number": fn}]
            else:
                sess = {"dirname": name, "file_number": fn, "part": 1, "parts": 1,
                        "chain": [{"volume_tag": vol, "file_number": fn}],
                        "record_id": None, "status": "found"}
            # Bytes belong to the tape file; with several folders in one file
            # (archives written before per-backup folders) count them once.
            sess["bytes"] = int(entry.get("bytes") or 0) if name == names[0] else 0
            out.append(sess)
    return out


# ---------------------------------------------------------------------------
# Finding backups for restore / verify
# ---------------------------------------------------------------------------

def find_session(dirname: str, prefer_vol: str = "") -> Optional[Dict[str, Any]]:
    """Locate a backup by its archive folder name.  Returns a session dict
    (with ``chain``) or None if nothing in the catalog knows it."""
    from .db import list_all_known_indexes
    if prefer_vol:
        for s in get_sessions(prefer_vol):
            if s.get("dirname") == dirname and s.get("chain"):
                return s
    for idx in list_all_known_indexes():
        for s in idx.get("sessions") or []:
            if s.get("dirname") == dirname and s.get("chain"):
                return s
    with shared_state._backup_records_lock:
        recs = [r for r in shared_state._backup_records
                if r.get("backup_dirname") == dirname and r.get("segments") and not r.get("overwritten")]
    if recs:
        segs = recs[0]["segments"]
        return {"dirname": dirname, "file_number": segs[0].get("file_number", 0),
                "part": 1, "parts": len(segs), "record_id": recs[0].get("id"),
                "chain": [{"volume_tag": s["volume_tag"], "file_number": int(s.get("file_number") or 0)} for s in segs]}
    return None


def chain_label(chain: List[Dict[str, Any]]) -> str:
    return " → ".join(f"{s.get('volume_tag')}#{s.get('file_number', 0)}" for s in chain)


# ---------------------------------------------------------------------------
# Drive positioning
# ---------------------------------------------------------------------------

_FILE_NO_RE = re.compile(r"file\s+number\s*=\s*(-?\d+)", re.I)


def mt_status() -> str:
    proc = subprocess.run(["mt", "-f", TAPE, "status"], capture_output=True, text=True,
                          timeout=max(COMMAND_TIMEOUT, 30))
    return (proc.stdout or "") + (proc.stderr or "")


def current_file_number() -> Optional[int]:
    m = _FILE_NO_RE.search(mt_status())
    if not m:
        return None
    n = int(m.group(1))
    return n if n >= 0 else None


def seek_file(file_number: int) -> None:
    """Position the drive at the start of tape file ``file_number``."""
    from .state import run_cmd
    run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
    if file_number > 0:
        run_cmd(["mt", "-f", TAPE, "fsf", str(int(file_number))], timeout=max(COMMAND_TIMEOUT, 900))


def _count_files_from_bot() -> int:
    """Count tape files by spacing forward one filemark at a time — only used
    when the driver cannot say which file end-of-data is in."""
    from .state import run_cmd
    run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
    n = 0
    while n < 10000:
        try:
            run_cmd(["mt", "-f", TAPE, "fsf", "1"], timeout=max(COMMAND_TIMEOUT, 900))
        except Exception:
            break
        n += 1
    # Leave the drive at end-of-data, after the last filemark.
    seek_file(n)
    return n


def seek_end_of_data() -> int:
    """Position the drive at end-of-data, ready to append; returns the tape
    file number a new write will get.  Only for tapes known to hold data —
    any failure raises rather than risk writing over existing backups."""
    from .state import run_cmd
    run_cmd(["mt", "-f", TAPE, "eod"], timeout=max(COMMAND_TIMEOUT, 900))
    fn = current_file_number()
    if fn is None:
        # Driver lost track of the file number (e.g. fast-mteom); count.
        fn = _count_files_from_bot()
    return fn


# ---------------------------------------------------------------------------
# Loading tapes
# ---------------------------------------------------------------------------

def loaded_volume() -> str:
    from .changer import refresh_state
    st = refresh_state()
    drive = st.get("drive") or {}
    if drive.get("empty", True):
        return ""
    return str((st.get("summary") or {}).get("loaded_volume") or drive.get("volume_tag") or "").strip() or "?"


def ensure_tape_loaded(vol: str, log: Callable[[str], None]) -> None:
    """Make sure ``vol`` is the tape in the drive, swapping tapes if needed."""
    from .changer import refresh_state
    from .drive_history import _save_last_known_loaded_slot
    from .state import TapeError, run_cmd
    from .db import update_tape_index_metadata
    from .backup_worker import _find_return_slot
    current = loaded_volume()
    if current == vol:
        return
    st = shared_state._state_cache
    slot = next((s for s in (st.get("slots") or [])
                 if s.get("full") and str(s.get("volume_tag") or "").strip() == vol), None)
    if not slot:
        raise TapeError(f"Tape {vol} is not in the library — insert it and try again.")
    if current:
        ret = _find_return_slot(current)
        if not ret:
            raise TapeError(f"No free slot to return {current} to before loading {vol}.")
        log(f"Returning {current} to slot {ret}…")
        run_cmd(["mtx", "-f", CHANGER, "unload", str(ret), "0"], timeout=max(COMMAND_TIMEOUT, 120))
        _save_last_known_loaded_slot(None)
        update_tape_index_metadata(current, present=True, last_seen_slot=ret)
        time.sleep(2)
    log(f"Loading {vol} from slot {slot['slot']}…")
    run_cmd(["mtx", "-f", CHANGER, "load", str(slot["slot"]), "0"], timeout=max(COMMAND_TIMEOUT, 120))
    _save_last_known_loaded_slot(int(slot["slot"]))
    time.sleep(3)
    refresh_state()
    now = loaded_volume()
    if now and now not in (vol, "?"):
        raise TapeError(f"Loaded slot {slot['slot']} expecting {vol} but the drive reports {now}.")


# ---------------------------------------------------------------------------
# Reading a chain back
# ---------------------------------------------------------------------------

class ChainReader:
    """Feed the tape files of a backup chain, in order, into one pipe.

    Runs dd once per segment with its stdout on ``write_fd``; the consumer
    (tar) reads the concatenated stream from the other end.  Tapes are swapped
    between segments as needed.  Python never touches the data.
    """

    def __init__(self, chain: List[Dict[str, Any]], write_fd: int,
                 log: Callable[[str], None], should_stop: Callable[[], bool],
                 limit_bytes: Optional[int] = None, proc_holder: Optional[Callable[[Any], None]] = None):
        self.chain = chain
        self.write_fd = write_fd
        self.log = log
        self.should_stop = should_stop
        self.limit_bytes = limit_bytes
        self.proc_holder = proc_holder
        self.bytes_done = 0            # bytes from completed segments
        self.current_bytes = 0         # bytes read so far in the current segment
        self.segment_index = 0
        self.error: Optional[str] = None
        self.consumer_gone = False
        self.dd_stderr: List[str] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    @property
    def bytes_read(self) -> int:
        return self.bytes_done + self.current_bytes

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: Optional[float] = None) -> None:
        self._thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        from .state import log_pipeline
        try:
            for i, seg in enumerate(self.chain):
                if self.should_stop() or self.consumer_gone:
                    break
                if self.limit_bytes is not None and self.bytes_done >= self.limit_bytes:
                    break
                self.segment_index = i
                vol = seg["volume_tag"]
                fn = int(seg.get("file_number") or 0)
                if len(self.chain) > 1:
                    self.log(f"Reading part {i + 1}/{len(self.chain)}: {vol}, tape file {fn}.")
                ensure_tape_loaded(vol, self.log)
                seek_file(fn)
                dd_cmd = ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"]
                if self.limit_bytes is not None:
                    left = max(self.limit_bytes - self.bytes_done, 0)
                    dd_cmd.append(f"count={max(1, (left + TAPE_BLOCK_BYTES - 1) // TAPE_BLOCK_BYTES)}")
                log_pipeline("tape", dd_cmd)
                proc = subprocess.Popen(dd_cmd, stdout=self.write_fd, stderr=subprocess.PIPE)
                if self.proc_holder:
                    self.proc_holder(proc)
                self.current_bytes = 0
                buf = b""
                last_line = ""
                fd = proc.stderr.fileno()
                while True:
                    chunk = os.read(fd, 4096)
                    if not chunk:
                        break
                    *lines, buf = re.split(rb"[\r\n]", buf + chunk)
                    for line_b in lines:
                        line = line_b.decode(errors="ignore").strip()
                        if not line:
                            continue
                        last_line = line
                        m = re.match(r"(\d+)\s+bytes", line)
                        if m:
                            self.current_bytes = int(m.group(1))
                        else:
                            self.dd_stderr.append(line)
                            del self.dd_stderr[:-20]
                rc = proc.wait()
                if self.proc_holder:
                    self.proc_holder(None)
                self.bytes_done += self.current_bytes
                self.current_bytes = 0
                if rc != 0:
                    if "broken pipe" in " ".join(self.dd_stderr).lower() or rc == -13:
                        self.consumer_gone = True
                        break
                    if self.should_stop():
                        break
                    self.error = f"dd read of {vol} tape file {fn} failed (exit {rc}): {last_line[-200:]}"
                    break
        except Exception as e:
            self.error = str(e)
        finally:
            try:
                os.close(self.write_fd)
            except OSError:
                pass


def migrate_legacy_layout() -> None:
    """One-time upgrade of catalogs written before backups were appended.

    Every backup used to rewind and write from the start of its tape, so on
    each tape only the newest backup that wrote anything still exists (at
    tape file 0); older records for that tape were silently overwritten.
    Record that, and give each tape a session list so appends start after
    what is really there.
    """
    from .db import _db_get_json, _db_set_json, list_all_known_indexes
    from .state import now_ts
    if _db_get_json("tape_layout_version", 0) >= 1:
        return
    with shared_state._backup_records_lock:
        recs = list(shared_state._backup_records)
    by_vol: Dict[str, List[Dict[str, Any]]] = {}
    for r in recs:
        if r.get("segments") or not r.get("volume_tag"):
            continue
        by_vol.setdefault(r["volume_tag"], []).append(r)
    overwritten_ids: List[str] = []
    known = {i["volume_tag"]: i for i in list_all_known_indexes()}
    for vol, vrecs in by_vol.items():
        vrecs.sort(key=lambda r: r.get("started_at") or 0)
        wrote = [r for r in vrecs if int(r.get("bytes_written") or 0) > 0]
        survivor = wrote[-1] if wrote else None
        for r in wrote[:-1]:
            overwritten_ids.append(r.get("id"))
        idx = known.get(vol)
        if survivor and idx is not None and not idx.get("sessions"):
            dirname = survivor.get("backup_dirname") or ""
            sess = {"dirname": dirname, "file_number": 0, "part": 1, "parts": 1,
                    "chain": [{"volume_tag": vol, "file_number": 0}],
                    "record_id": survivor.get("id"),
                    "bytes": int(survivor.get("bytes_written") or 0),
                    "written_at": survivor.get("finished_at") or survivor.get("started_at"),
                    "status": survivor.get("status") or "legacy"}
            dirnames = [dirname] if dirname else []
            files = None
            if dirname and (idx.get("backup_dirnames") or []) != dirnames:
                row = _load_row(vol) or {"files": []}
                files = [f for f in row["files"] if f.split("/", 1)[0] == dirname] or row["files"]
            _write_layout(vol, [sess], files=files, dirnames=dirnames if dirname else None)
    if overwritten_ids:
        _mark_records(overwritten_ids, overwritten=True, overwritten_at=now_ts(),
                      overwritten_reason="Overwritten by a later backup to the same tape "
                                         "(backups used to start at the beginning of the tape)")
        _log.info("Tape layout upgrade: %d older backup record(s) marked overwritten", len(overwritten_ids))
    _db_set_json("tape_layout_version", 1)
