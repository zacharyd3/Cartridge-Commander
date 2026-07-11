"""drive_history module (split from the original monolithic app.py)."""

import os
import re
import json
import time
from typing import Any, Dict, List, Optional
from .config import CHANGER, COMMAND_TIMEOUT, TAPE
from . import state as shared_state


def _load_drive_history() -> None:
    from .db import _db_get_json, _db_set_json
    from .state import DRIVE_HISTORY_FILE
    data = _db_get_json("drive_history", None)
    if isinstance(data, dict):
        shared_state._drive_history = data
        return
    os.makedirs(os.path.dirname(DRIVE_HISTORY_FILE), exist_ok=True)
    if not os.path.exists(DRIVE_HISTORY_FILE):
        shared_state._drive_history = {}
        return
    try:
        with open(DRIVE_HISTORY_FILE) as f:
            shared_state._drive_history = json.load(f)
        _db_set_json("drive_history", shared_state._drive_history)
    except Exception:
        shared_state._drive_history = {}


def _save_drive_history() -> None:
    from .db import _db_set_json
    with shared_state._drive_history_lock:
        payload = json.loads(json.dumps(shared_state._drive_history))
    _db_set_json("drive_history", payload)

def _record_tape_loaded(vol: str) -> None:
    """Call when a tape is confirmed loaded into the drive."""
    from .state import now_ts
    if not vol:
        return
    shared_state._drive_loaded_at = now_ts()
    shared_state._drive_loaded_vol = vol
    with shared_state._drive_history_lock:
        entry = shared_state._drive_history.setdefault(vol, {
            "volume_tag": vol,
            "load_count": 0,
            "first_loaded": None,
            "last_loaded": None,
            "last_unloaded": None,
            "last_backup": None,
            "last_restore": None,
            "total_backup_bytes": 0,
            "backup_count": 0,
            "restore_count": 0,
        })
        entry["load_count"] = entry.get("load_count", 0) + 1
        entry["last_loaded"] = shared_state._drive_loaded_at
        if not entry.get("first_loaded"):
            entry["first_loaded"] = shared_state._drive_loaded_at
    _save_drive_history()

def _record_tape_unloaded(vol: str) -> None:
    from .state import now_ts
    if vol:
        with shared_state._drive_history_lock:
            entry = shared_state._drive_history.get(vol, {})
            if entry:
                entry["last_unloaded"] = now_ts()
        _save_drive_history()
    shared_state._drive_loaded_at = None
    shared_state._drive_loaded_vol = ""

def _record_backup_done(vol: str, bytes_written: int) -> None:
    from .state import now_ts
    if not vol:
        return
    with shared_state._drive_history_lock:
        entry = shared_state._drive_history.setdefault(vol, {"volume_tag": vol})
        entry["last_backup"] = now_ts()
        entry["backup_count"] = entry.get("backup_count", 0) + 1
        entry["total_backup_bytes"] = entry.get("total_backup_bytes", 0) + bytes_written
    _save_drive_history()

def _record_restore_done(vol: str) -> None:
    from .state import now_ts
    if not vol:
        return
    with shared_state._drive_history_lock:
        entry = shared_state._drive_history.setdefault(vol, {"volume_tag": vol})
        entry["last_restore"] = now_ts()
        entry["restore_count"] = entry.get("restore_count", 0) + 1
    _save_drive_history()

def _load_last_known_loaded_slot() -> None:
    from .db import _db_get_json, _db_set_json
    from .state import LAST_LOADED_SLOT_FILE
    slot = _db_get_json("last_known_loaded_slot", None)
    try:
        slot = int(slot) if slot is not None else None
    except Exception:
        slot = None
    if slot and slot > 0:
        shared_state._last_known_loaded_slot = slot
        return
    os.makedirs(os.path.dirname(LAST_LOADED_SLOT_FILE), exist_ok=True)
    if not os.path.exists(LAST_LOADED_SLOT_FILE):
        shared_state._last_known_loaded_slot = None
        return
    try:
        with open(LAST_LOADED_SLOT_FILE) as f:
            data = json.load(f)
        slot = int(data.get("slot", 0) or 0)
        shared_state._last_known_loaded_slot = slot if slot > 0 else None
        _db_set_json("last_known_loaded_slot", shared_state._last_known_loaded_slot)
    except Exception:
        shared_state._last_known_loaded_slot = None


def _save_last_known_loaded_slot(slot: Optional[int]) -> None:
    from .db import _db_set_json
    shared_state._last_known_loaded_slot = slot if slot and int(slot) > 0 else None
    _db_set_json("last_known_loaded_slot", shared_state._last_known_loaded_slot)

def get_effective_loaded_slot() -> Optional[int]:
    drive = shared_state._state_cache.get("drive", {}) or {}
    slot = drive.get("loaded_from_slot")
    if slot:
        return int(slot)
    return shared_state._last_known_loaded_slot

def infer_lto_generation(drive: Dict[str, Any], volume_tag: str = "") -> Optional[int]:
    density = str((drive or {}).get("density") or "").upper()
    vol = str(volume_tag or "").upper()

    for gen in range(1, 10):
        if f"LTO-{gen}" in density or f"LTO{gen}" in density:
            return gen

    m = re.search(r"\bLTO[- ]?([1-9])\b", density)
    if m:
        return int(m.group(1))

    m = re.match(r"^L([1-9])", vol)
    if m:
        return int(m.group(1))

    return None


def lto_native_capacity_bytes(gen: Optional[int]) -> Optional[int]:
    caps_tb = {1: 0.10, 2: 0.20, 3: 0.40, 4: 0.80, 5: 1.50, 6: 2.50, 7: 6.00, 8: 12.00, 9: 18.00}
    tb = caps_tb.get(gen)
    if tb is None:
        return None
    return int(tb * 1024 * 1024 * 1024 * 1024)


def bytes_written_for_volume(volume_tag: str) -> int:
    if not volume_tag:
        return 0
    total = 0
    with shared_state._backup_records_lock:
        recs = list(shared_state._backup_records)
    for r in recs:
        if r.get("volume_tag") == volume_tag and r.get("status") == "completed":
            try:
                total += int(r.get("bytes_written") or 0)
            except Exception:
                pass
    return total


def _space_bool(v: Any, default: bool = True) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    try:
        return bool(int(v))
    except Exception:
        return default


def build_tape_space_info(volume_tag: str, drive: Optional[Dict[str, Any]] = None, idx: Optional[Dict[str, Any]] = None, loaded: bool = False) -> Dict[str, Any]:
    drive = drive or {}
    idx = idx or {}
    vol = str(volume_tag or idx.get("volume_tag") or drive.get("volume_tag") or "").strip()
    if not vol:
        return {
            "loaded": bool(loaded),
            "volume_tag": "",
            "lto_generation": None,
            "capacity_bytes": None,
            "used_bytes": 0,
            "remaining_bytes": None,
            "remaining_pct": None,
            "estimated": True,
        }

    gen = idx.get("lto_generation")
    try:
        gen = int(gen) if gen is not None else None
    except Exception:
        gen = None
    if gen is None:
        gen = infer_lto_generation(drive, vol)

    capacity = idx.get("capacity_bytes")
    try:
        capacity = int(capacity) if capacity is not None else None
    except Exception:
        capacity = None
    if capacity is None:
        capacity = lto_native_capacity_bytes(gen)

    used = idx.get("used_bytes")
    try:
        used = int(used) if used is not None else None
    except Exception:
        used = None
    if used is None:
        used = bytes_written_for_volume(vol)

    remaining = idx.get("remaining_bytes")
    try:
        remaining = int(remaining) if remaining is not None else None
    except Exception:
        remaining = None

    remaining_pct = idx.get("remaining_pct")
    try:
        remaining_pct = float(remaining_pct) if remaining_pct is not None else None
    except Exception:
        remaining_pct = None

    if capacity is not None:
        if remaining is None:
            remaining = max(capacity - used, 0)
        if remaining_pct is None and capacity > 0:
            remaining_pct = max(0.0, min(100.0, (remaining / capacity) * 100.0))

    estimated = _space_bool(idx.get("space_estimated"), True)

    return {
        "loaded": bool(loaded),
        "volume_tag": vol,
        "lto_generation": gen,
        "capacity_bytes": capacity,
        "used_bytes": used,
        "remaining_bytes": remaining,
        "remaining_pct": remaining_pct,
        "estimated": estimated,
    }


def space_meta_from_info(info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "lto_generation": info.get("lto_generation"),
        "capacity_bytes": info.get("capacity_bytes"),
        "used_bytes": info.get("used_bytes"),
        "remaining_bytes": info.get("remaining_bytes"),
        "remaining_pct": info.get("remaining_pct"),
        "space_estimated": 1 if info.get("estimated", True) else 0,
    }


def build_loaded_tape_space_info() -> Dict[str, Any]:
    from .db import load_tape_index
    summary = (shared_state._state_cache.get("summary") or {})
    drive = (shared_state._state_cache.get("drive") or {})
    vol = str(summary.get("loaded_volume") or drive.get("volume_tag") or "").strip()

    if not vol or drive.get("empty", True):
        return {
            "loaded": False,
            "volume_tag": "",
            "lto_generation": None,
            "capacity_bytes": None,
            "used_bytes": 0,
            "remaining_bytes": None,
            "remaining_pct": None,
            "estimated": True,
        }

    idx = load_tape_index(vol) or {}
    return build_tape_space_info(vol, drive=drive, idx=idx, loaded=True)


def safe_file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


def _is_tape_full_error(err: Exception) -> bool:
    msg = str(err or "").lower()
    patterns = [
        "no space left on device",
        "end of medium",
        "end-of-medium",
        "write filemark failed",
        "cannot write",
        "write returned zero bytes",
        "device offline",
    ]
    return any(p in msg for p in patterns)


def _mt_status_shows_eot() -> bool:
    """Check `mt status` for EOD/EOT flags.

    Some drives/kernels report a bare "Input/output error" from dd — instead of
    the more explicit ENOSPC-style wording matched by `_is_tape_full_error` —
    when a write hits the physical end of the tape. That's easy to hit on a
    bigger multi-folder backup that a smaller single-folder backup never
    reached. Querying the drive directly distinguishes "actually out of tape"
    from a genuine drive/media fault so we don't misreport the latter as full.
    """
    from .state import run_cmd
    try:
        text = run_cmd(["mt", "-f", TAPE, "status"], timeout=30)
    except Exception:
        return False
    upper = (text or "").upper()
    return "EOD" in upper or "EOT" in upper


def _candidate_rewrite_tapes(current_volume: str = "") -> List[Dict[str, Any]]:
    from .records import gfs_get_recyclable
    from .db import list_all_known_indexes
    from .state import is_cleaning_volume_tag
    from .changer import refresh_state
    state = refresh_state()
    present_slots = {
        str(s.get("volume_tag") or "").strip(): s
        for s in (state.get("slots") or [])
        if s.get("full") and not s.get("is_import_export") and s.get("volume_tag")
    }
    recyclable_set = set(gfs_get_recyclable())
    out = []
    for idx in list_all_known_indexes():
        vol = str(idx.get("volume_tag") or "").strip()
        if not vol or vol == current_volume or is_cleaning_volume_tag(vol):
            continue
        slot = present_slots.get(vol)
        if not slot:
            continue
        purpose = str(idx.get("purpose") or "").strip().lower()
        if purpose not in {"available", "recyclable"} and vol not in recyclable_set:
            continue
        score = idx.get("written_at") or idx.get("updated_at") or idx.get("last_seen_at") or 0
        out.append({
            "volume_tag": vol,
            "slot": int(slot.get("slot") or 0),
            "purpose": "recyclable" if vol in recyclable_set else (purpose or "available"),
            "score": int(score or 0),
        })
    out.sort(key=lambda x: (x["score"], x["volume_tag"]))
    return out


def _switch_to_rewrite_candidate(current_volume: str = "") -> Dict[str, Any]:
    from .db import update_tape_index_metadata
    from .state import TapeError, append_backup_log, run_cmd
    from .changer import refresh_state
    candidates = _candidate_rewrite_tapes(current_volume)
    if not candidates:
        raise TapeError("Tape appears full and no available/recyclable replacement tapes were found.")

    current_slot = get_effective_loaded_slot()
    if current_slot:
        append_backup_log(f"Current tape appears full. Returning it to slot {current_slot}…")
        run_cmd(["mtx", "-f", CHANGER, "unload", str(current_slot), "0"], timeout=max(COMMAND_TIMEOUT, 120))
        _save_last_known_loaded_slot(None)
        time.sleep(2)

    chosen = candidates[0]
    append_backup_log(f"Auto-rewrite selecting {chosen['volume_tag']} from slot {chosen['slot']} ({chosen['purpose']}).")
    run_cmd(["mtx", "-f", CHANGER, "load", str(chosen["slot"]), "0"], timeout=max(COMMAND_TIMEOUT, 120))
    _save_last_known_loaded_slot(chosen["slot"])
    time.sleep(2)
    run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
    append_backup_log(f"Erasing {chosen['volume_tag']} for rewrite…")
    run_cmd(["mt", "-f", TAPE, "erase"], timeout=7200)
    run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
    update_tape_index_metadata(chosen["volume_tag"], present=True, purpose="available", is_cleaning=False)
    refresh_state()
    return chosen


def get_drive_info() -> Dict[str, Any]:
    """Return enriched drive info combining live state + history."""
    from .db import load_tape_index
    from .state import now_ts
    drive = shared_state._state_cache.get("drive", {})
    vol = drive.get("volume_tag", "")
    with shared_state._drive_history_lock:
        hist = dict(shared_state._drive_history.get(vol, {})) if vol else {}

    time_in_drive = None
    if shared_state._drive_loaded_at and not drive.get("empty"):
        time_in_drive = now_ts() - shared_state._drive_loaded_at

    idx_meta = None
    if vol:
        idx = load_tape_index(vol)
        if idx:
            idx_meta = {
                "file_count": idx.get("file_count", 0),
                "written_at": idx.get("written_at"),
                "purpose": idx.get("purpose", "cleaning" if idx.get("is_cleaning") else "data"),
                "is_cleaning": bool(idx.get("is_cleaning")),
                "space": build_tape_space_info(vol, drive=drive, idx=idx, loaded=not drive.get("empty", True)),
            }

    effective_slot = drive.get("loaded_from_slot") or shared_state._last_known_loaded_slot

    return {
        "volume_tag": vol,
        "empty": drive.get("empty", True),
        "online": drive.get("online", False),
        "ready": drive.get("online", False),
        "at_bot": drive.get("at_bot", False),
        "density": drive.get("density", ""),
        "loaded_from_slot": drive.get("loaded_from_slot"),
        "effective_loaded_slot": drive.get("effective_loaded_slot") or drive.get("loaded_from_slot") or shared_state._last_known_loaded_slot,
        "effective_loaded_slot": effective_slot,
        "loaded_at": shared_state._drive_loaded_at,
        "time_in_drive_seconds": time_in_drive,
        "history": hist,
        "index": idx_meta,
        "space": build_tape_space_info(vol, drive=drive, idx=load_tape_index(vol) or {}, loaded=not drive.get("empty", True)) if vol else build_tape_space_info("", loaded=False),
        "raw_mt_status": drive.get("raw_status", ""),
    }

def _check_drive_change() -> None:
    """Detect load/unload events by comparing drive state to last known volume."""
    drive = shared_state._state_cache.get("drive", {})
    current_vol = drive.get("volume_tag", "") if not drive.get("empty") else ""
    if current_vol and current_vol != shared_state._drive_loaded_vol:
        # New tape appeared
        _record_tape_loaded(current_vol)
    elif not current_vol and shared_state._drive_loaded_vol:
        # Tape was removed
        _record_tape_unloaded(shared_state._drive_loaded_vol)

# ---------------------------------------------------------------------------
# MTX / MT parsing
# ---------------------------------------------------------------------------

