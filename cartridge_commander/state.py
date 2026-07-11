"""Shared in-process job state: dicts, locks, and small pure helpers.

Every other module reaches these through ``shared_state.<name>`` (see how
other modules import this module as ``from . import state as shared_state``)
rather than importing the raw names directly, because several of them are
fully *rebound* elsewhere (not just mutated in place) -- a plain
``from .state import _backup_records`` would go stale the moment
``_load_backup_records()`` replaces the list.
"""

import os
import json
import time
import datetime
import threading
import subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from .config import BACKUP_LOG_LEVEL_DEFAULT, BACKUP_ROOT, CHANGER, COMMAND_TIMEOUT, TAPE


_action_lock = threading.Lock()
_action_log: List[Dict[str, Any]] = []

_state_cache: Dict[str, Any] = {
    "ok": False, "changer": CHANGER, "tape": TAPE,
    "backup_root": BACKUP_ROOT, "slots": [], "drive": {},
    "summary": {}, "last_updated": None, "last_error": None,
}

_backup_job: Dict[str, Any] = {
    "running": False, "status": "idle", "selected_paths": [],
    "bytes_total": 0, "bytes_written": 0, "percent": 0.0,
    "speed_bps": 0.0, "eta_seconds": None,
    "started_at": None, "finished_at": None,
    "last_message": "No backup has run yet.", "log": [], "error": None,
    "log_level": BACKUP_LOG_LEVEL_DEFAULT,
}
_backup_lock = threading.Lock()
_tar_proc: Optional[subprocess.Popen] = None
_stop_requested: bool = False

_restore_job: Dict[str, Any] = {
    "running": False, "status": "idle", "volume_tag": "",
    "paths": [], "dest": "", "started_at": None, "finished_at": None,
    "eta_seconds": None,
    "last_message": "No restore has run yet.", "log": [], "error": None,
}
_restore_lock = threading.Lock()

_restore_proc: Optional[subprocess.Popen] = None   # tar process for the active restore
_stop_restore: bool = False                          # set True to cancel running restore

# Format job — erase one or more tapes sequentially
_format_job: Dict[str, Any] = {
    "running": False, "status": "idle",
    "queue": [],          # list of {slot, volume_tag} dicts to format
    "current": None,      # {slot, volume_tag} being formatted now
    "done": [],           # completed items
    "failed": [],         # failed items with error
    "started_at": None, "finished_at": None,
    "last_message": "No format job has run.",
    "log": [], "error": None,
}
_format_lock = threading.Lock()
_stop_format: bool = False

# Changer job — tracks async load/unload/clean so the UI never blocks
_changer_job: Dict[str, Any] = {
    "running": False, "action": "", "status": "idle",
    "detail": "", "error": None, "started_at": None, "finished_at": None,
}
_changer_lock = threading.Lock()

def set_changer_state(**kw):
    with _changer_lock: _changer_job.update(kw)

def snapshot_changer_job():
    return snap(_changer_job, _changer_lock)

_inventory_job: Dict[str, Any] = {
    "running": False, "status": "idle", "mode": "full", "paused": False,
    "current_slot": None, "total_slots": 0, "scanned": 0, "started_at": None,
    "finished_at": None, "eta_seconds": None,
    "last_message": "No inventory scan has run.", "log": [],
}
_inventory_lock = threading.Lock()
_inventory_pause_event = threading.Event()
_inventory_pause_event.set()
_inventory_stop_requested: bool = False

_schedules: List[Dict[str, Any]] = []
_schedules_lock = threading.Lock()

# Backup records (persistent job history)
_backup_records: List[Dict[str, Any]] = []
_backup_records_lock = threading.Lock()

# Verification job state
_verify_job: Dict[str, Any] = {
    "running": False, "status": "idle", "volume_tag": "",
    "started_at": None, "finished_at": None,
    "bytes_verified": 0, "errors": 0, "eta_seconds": None,
    "last_message": "No verification has run.", "log": [], "error": None,
}
_verify_lock = threading.Lock()

# Drive activity history — persisted to disk
DRIVE_HISTORY_FILE = os.getenv("DRIVE_HISTORY_FILE", "/var/lib/tl2000/drive_history.json")
_drive_history: Dict[str, Any] = {}   # keyed by volume_tag
_drive_history_lock = threading.Lock()
# Tracks when the current tape was loaded into the drive
_drive_loaded_at: Optional[int] = None
_drive_loaded_vol: str = ""
LAST_LOADED_SLOT_FILE = os.getenv("LAST_LOADED_SLOT_FILE", "/var/lib/tl2000/last_loaded_slot.json")
_last_known_loaded_slot: Optional[int] = None

# Runtime-overridable restore subfolder pattern (persisted to DB)
def now_ts() -> int:
    return int(time.time())

class TapeError(RuntimeError):
    pass

def is_cleaning_volume_tag(vol: str) -> bool:
    return str(vol or "").strip().upper().startswith("CLN")


VALID_BACKUP_LOG_LEVELS = {"minimal", "normal", "verbose"}

def normalize_backup_log_level(level: Optional[str]) -> str:
    lvl = str(level or BACKUP_LOG_LEVEL_DEFAULT or "normal").strip().lower()
    return lvl if lvl in VALID_BACKUP_LOG_LEVELS else "normal"

def current_backup_log_level() -> str:
    with _backup_lock:
        return normalize_backup_log_level(_backup_job.get("log_level", BACKUP_LOG_LEVEL_DEFAULT))

def backup_log_allows(level: str) -> bool:
    order = {"minimal": 0, "normal": 1, "verbose": 2}
    active = normalize_backup_log_level(current_backup_log_level())
    return order[normalize_backup_log_level(level)] <= order[active]

@dataclass
class Slot:
    slot: int
    is_import_export: bool
    full: bool
    volume_tag: str

@dataclass
class Drive:
    element: int
    empty: bool
    loaded_from_slot: Optional[int]
    volume_tag: str
    online: bool
    at_bot: bool
    raw_status: str
    density: str

def log_action(kind, ok, detail, extra=None):
    from .db import _save_action_log, db_log
    with _action_lock:
        _action_log.insert(0, {"ts": now_ts(), "kind": kind, "ok": ok, "detail": detail, "extra": extra or {}})
        del _action_log[500:]
    _save_action_log()
    db_log("action", "info" if ok else "error", f"{kind}: {detail}")

def _insert_log(job_dict, lock, message, category="app"):
    from .db import db_log
    with lock:
        job_dict["log"].insert(0, {"ts": now_ts(), "message": message})
        del job_dict["log"][200:]
        job_dict["last_message"] = message
    db_log(category, "info", message)

def append_backup_log(msg, level="minimal"):
    if backup_log_allows(level):
        _insert_log(_backup_job, _backup_lock, msg, "backup")
def append_restore_log(msg):   _insert_log(_restore_job, _restore_lock, msg, "restore")
def append_inventory_log(msg): _insert_log(_inventory_job, _inventory_lock, msg, "inventory")
def set_backup_state(**kw):
    with _backup_lock:   _backup_job.update(kw)
def set_restore_state(**kw):
    with _restore_lock:  _restore_job.update(kw)
def set_inventory_state(**kw):
    with _inventory_lock: _inventory_job.update(kw)

def request_inventory_pause():
    with _inventory_lock:
        _inventory_job["paused"] = True
        if _inventory_job.get("running"):
            _inventory_job["status"] = "paused"
            _inventory_job["last_message"] = "Inventory paused."
    _inventory_pause_event.clear()

def request_inventory_resume():
    with _inventory_lock:
        _inventory_job["paused"] = False
        if _inventory_job.get("running") and _inventory_job.get("status") == "paused":
            _inventory_job["status"] = "scanning"
            _inventory_job["last_message"] = "Inventory resumed."
    _inventory_pause_event.set()

def request_inventory_stop():
    global _inventory_stop_requested
    _inventory_stop_requested = True
    _inventory_pause_event.set()
    with _inventory_lock:
        if _inventory_job.get("running"):
            _inventory_job["last_message"] = "Stopping inventory after current step…"

def inventory_should_stop() -> bool:
    global _inventory_stop_requested
    return bool(_inventory_stop_requested)

def inventory_wait_if_paused() -> None:
    from .changer import refresh_state
    from .mqtt import publish_state_to_mqtt
    while True:
        if inventory_should_stop():
            raise TapeError("Inventory stopped by user.")
        with _inventory_lock:
            paused = bool(_inventory_job.get("paused"))
        if not paused:
            _inventory_pause_event.set()
            return
        append_inventory_log("Inventory paused — waiting to resume…")
        set_inventory_state(status="paused", last_message="Inventory paused.")
        publish_state_to_mqtt(refresh_state())
        _inventory_pause_event.wait(timeout=1)

def snap(d, lock):
    with lock: return json.loads(json.dumps(d))
def snapshot_backup_job():    return snap(_backup_job,    _backup_lock)
def snapshot_restore_job():   return snap(_restore_job,   _restore_lock)
def snapshot_inventory_job(): return snap(_inventory_job, _inventory_lock)

def run_cmd(args, timeout=None):
    timeout = timeout or COMMAND_TIMEOUT
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise TapeError((proc.stderr or proc.stdout or "Command failed").strip())
    return proc.stdout.strip()

def bytes_human(v):
    v = float(v)
    for u in ["B","KB","MB","GB","TB"]:
        if v < 1024 or u == "TB": return f"{v:.1f} {u}"
        v /= 1024

def secs_human(s):
    if s is None or s < 0: return "—"
    s = int(s)
    if s < 60:   return f"{s}s"
    if s < 3600: return f"{s//60}m {s%60}s"
    return f"{s//3600}h {(s%3600)//60}m"

def _fmt_ts_short(ts: Optional[int]) -> str:
    """Format a unix timestamp as a short human-readable date, or 'unknown'."""
    if not ts:
        return "unknown date"
    try:
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    except Exception:
        return "unknown date"


def calc_eta_seconds(started_at: Optional[int], completed: int, total: int) -> Optional[int]:
    if not started_at or total <= 0 or completed <= 0 or completed >= total:
        return 0 if total > 0 and completed >= total else None
    elapsed = max(now_ts() - int(started_at), 1)
    avg_per_item = elapsed / float(completed)
    remaining = max(total - completed, 0)
    return int(avg_per_item * remaining)

# ---------------------------------------------------------------------------
# Drive history helpers
# ---------------------------------------------------------------------------

def set_format_state(**kw):
    with _format_lock: _format_job.update(kw)

def snapshot_format_job():
    return snap(_format_job, _format_lock)

def append_format_log(msg: str) -> None:
    from .db import db_log
    with _format_lock:
        _format_job["log"].insert(0, {"ts": now_ts(), "message": msg})
        del _format_job["log"][200:]
        _format_job["last_message"] = msg
    db_log("format", "info", msg)

def set_verify_state(**kw):
    with _verify_lock: _verify_job.update(kw)

def append_verify_log(msg):
    with _verify_lock:
        _verify_job["log"].insert(0, {"ts": now_ts(), "message": msg})
        del _verify_job["log"][100:]
        _verify_job["last_message"] = msg

def snapshot_verify_job():
    return snap(_verify_job, _verify_lock)


