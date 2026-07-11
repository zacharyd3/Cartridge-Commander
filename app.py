import os
import hmac
import fcntl
import re
import json
import time
import signal
import datetime
import calendar
import threading
import subprocess
import sqlite3
import shlex
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request, render_template, send_from_directory

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CHANGER              = os.getenv("TL_CHANGER",            "/dev/sg12")
TAPE                 = os.getenv("TL_TAPE",               "/dev/nst0")
POLL_SECONDS         = int(os.getenv("TL_POLL_SECONDS",   "15"))
COMMAND_TIMEOUT      = int(os.getenv("TL_COMMAND_TIMEOUT","60"))
WEBUI_PASSWORD       = os.getenv("TL_WEBUI_PASSWORD",     "")
BACKUP_ROOT          = os.getenv("BACKUP_ROOT",           "/mnt/user")
BACKUP_CHUNK_SIZE    = int(os.getenv("BACKUP_CHUNK_SIZE", str(1024*1024)))
TAPE_BLOCK_BYTES     = int(os.getenv("TL_TAPE_BLOCK_KB", "512")) * 1024   # physical tape block size, default 512 KiB
AUTO_REWIND_AFTER    = os.getenv("AUTO_REWIND_AFTER_BACKUP","true").lower() == "true"
STARTUP_QUICK_SCAN   = os.getenv("STARTUP_QUICK_SCAN",     "true").lower() == "true"
ERASE_BEFORE_BACKUP  = os.getenv("ERASE_BEFORE_BACKUP",   "false").lower() == "true"
TAPE_INDEX_DIR       = os.getenv("TAPE_INDEX_DIR",        "/var/lib/tl2000/index")
ICON_PATH            = os.getenv("ICON_PATH",             "/var/lib/tl2000/icon.png")
SCHEDULES_FILE       = os.getenv("SCHEDULES_FILE",        "/var/lib/tl2000/schedules.json")
RESTORE_ROOT         = os.getenv("RESTORE_ROOT",          "/mnt/restore")
# Pattern for the default restore sub-folder.  Tokens: {volume} {date} {datetime} {tape}
# e.g. "{volume}_{date}"  →  /mnt/restore/SM9158L6_2026-03-30
# Set to "" to restore directly into RESTORE_ROOT (old behaviour).
RESTORE_SUBFOLDER_PATTERN = os.getenv("RESTORE_SUBFOLDER_PATTERN", "{volume}_{date}")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))
LOG_MAX_ROWS       = int(os.getenv("LOG_MAX_ROWS", "5000"))
AUTO_REWRITE_ON_FULL = os.getenv("AUTO_REWRITE_ON_FULL", "true").lower() == "true"
BACKUP_LOG_LEVEL_DEFAULT = os.getenv("BACKUP_LOG_LEVEL_DEFAULT", "normal").strip().lower()

# Email notifications


# Home Assistant notifications (can also be set at runtime via /api/settings/ha)
HA_NOTIFY_URL        = os.getenv("HA_URL",              "")   # e.g. http://homeassistant.local:8123
HA_NOTIFY_TOKEN      = os.getenv("HA_TOKEN",            "")   # long-lived access token
HA_NOTIFY_SERVICE    = os.getenv("HA_NOTIFY_SERVICE",   "notify")  # e.g. "mobile_app_phone" or "notify"
HA_NOTIFY_ENABLED    = os.getenv("HA_NOTIFY_ENABLED",   "true").lower() == "true"

# GFS retention
GFS_DAILY_KEEP       = int(os.getenv("GFS_DAILY_KEEP",   "7"))
GFS_WEEKLY_KEEP      = int(os.getenv("GFS_WEEKLY_KEEP",  "4"))
GFS_MONTHLY_KEEP     = int(os.getenv("GFS_MONTHLY_KEEP", "6"))

# Incremental backups
INCREMENTAL_DIR      = os.getenv("INCREMENTAL_DIR",      "/var/lib/tl2000/incremental")

# Hooks
PRE_BACKUP_HOOK      = os.getenv("PRE_BACKUP_HOOK",      "")
POST_BACKUP_HOOK     = os.getenv("POST_BACKUP_HOOK",     "")

# Tape health / sg3_utils
SG_DEVICE            = os.getenv("SG_DEVICE",            "")  # e.g. /dev/sg0 (the tape drive, not changer)

# Verification
VERIFY_AFTER_BACKUP  = os.getenv("VERIFY_AFTER_BACKUP",  "true").lower() == "true"
VERIFY_SAMPLE_MB     = int(os.getenv("VERIFY_SAMPLE_MB", "512"))  # 0 = full verify

# Backup records
BACKUP_RECORDS_FILE  = os.getenv("BACKUP_RECORDS_FILE",  "/var/lib/tl2000/backup_records.json")
TAPE_CATALOG_DB     = os.getenv("TAPE_CATALOG_DB", "/var/lib/tl2000/tape_catalog.db")

MQTT_HOST            = os.getenv("MQTT_HOST",  "")
MQTT_PORT            = int(os.getenv("MQTT_PORT","1883"))
MQTT_USER            = os.getenv("MQTT_USER",  "")
MQTT_PASS            = os.getenv("MQTT_PASS",  "")
MQTT_BASE            = os.getenv("MQTT_BASE",  "homelab/tl2000")
HA_DISCOVERY_PREFIX  = os.getenv("HA_DISCOVERY_PREFIX","homeassistant")
HAS_MAIL_SLOT       = os.getenv("TL_HAS_MAIL_SLOT",      "false").lower() == "true"
MAGAZINE_SIZE       = int(os.getenv("TL_MAGAZINE_SIZE",     "12"))
CLEANING_WAIT_SECONDS = int(os.getenv("TL_CLEANING_WAIT_SECONDS", "120"))
EJECT_LEFT_CMD       = os.getenv("TL_EJECT_LEFT_CMD", "").strip()
EJECT_RIGHT_CMD      = os.getenv("TL_EJECT_RIGHT_CMD", "").strip()

DEVICE_INFO = {
    "identifiers": ["odin_tl2000"],
    "name": "Odin TL2000",
    "manufacturer": "IBM",
    "model": "3573-TL / TL2000",
    "sw_version": "0.7.0",
}

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
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
_restore_subfolder_pattern: str = RESTORE_SUBFOLDER_PATTERN
_restore_subfolder_lock = threading.Lock()

def get_restore_subfolder_pattern() -> str:
    with _restore_subfolder_lock:
        return _restore_subfolder_pattern

def set_restore_subfolder_pattern(pattern: str) -> None:
    global _restore_subfolder_pattern
    with _restore_subfolder_lock:
        _restore_subfolder_pattern = str(pattern or "")
    _db_set_json("restore_subfolder_pattern", _restore_subfolder_pattern)

def _load_restore_subfolder_pattern() -> None:
    global _restore_subfolder_pattern
    val = _db_get_json("restore_subfolder_pattern", None)
    if val is not None:
        with _restore_subfolder_lock:
            _restore_subfolder_pattern = str(val)

# ---------------------------------------------------------------------------
# Home Assistant notification runtime config (overrides env vars when saved)
# ---------------------------------------------------------------------------
_ha_config_lock = threading.Lock()
_ha_config: Dict[str, Any] = {
    "url":     HA_NOTIFY_URL,
    "token":   HA_NOTIFY_TOKEN,
    "service": HA_NOTIFY_SERVICE,
    "enabled": HA_NOTIFY_ENABLED,
}

def get_ha_config() -> Dict[str, Any]:
    with _ha_config_lock:
        return dict(_ha_config)

def set_ha_config(url: str, token: str, service: str, enabled: bool) -> None:
    with _ha_config_lock:
        _ha_config["url"]     = url.strip().rstrip("/")
        _ha_config["token"]   = token.strip()
        _ha_config["service"] = service.strip() or "notify"
        _ha_config["enabled"] = bool(enabled)
    _db_set_json("ha_config", dict(_ha_config))

def _load_ha_config() -> None:
    data = _db_get_json("ha_config", None)
    if isinstance(data, dict):
        with _ha_config_lock:
            _ha_config["url"]     = str(data.get("url",     _ha_config["url"]))
            _ha_config["token"]   = str(data.get("token",   _ha_config["token"]))
            _ha_config["service"] = str(data.get("service", _ha_config["service"])) or "notify"
            _ha_config["enabled"] = bool(data.get("enabled", _ha_config["enabled"]))

# ---------------------------------------------------------------------------
# Notification event config (which events trigger alerts + custom templates)
# ---------------------------------------------------------------------------
# Default message templates. Tokens: {vol} {paths} {written} {duration}
# {speed} {verified} {errors} {error} {time}
_NOTIFY_DEFAULT_TEMPLATES: Dict[str, str] = {
    "backup_success_title":   "Backup OK — {vol}",
    "backup_success_body":    "✅ {vol} completed\nWritten: {written} in {duration} ({speed}/s)\nVerified: {verified}",
    "backup_failure_title":   "Backup FAILED — {vol}",
    "backup_failure_body":    "❌ {vol} failed\nError: {error}\nTime: {time}",
    "verify_failure_title":   "Verify FAILED — {vol}",
    "verify_failure_body":    "⚠️ {vol} verify failed\nErrors: {errors}\n{error}",
}

_notify_config_lock = threading.Lock()
_notify_config: Dict[str, Any] = {
    "on_backup_success":  True,
    "on_backup_failure":  True,
    "on_verify_failure":  True,
    "on_format_complete": False,
    "on_inventory_done":  False,
    "templates": dict(_NOTIFY_DEFAULT_TEMPLATES),
}

def get_notify_config() -> Dict[str, Any]:
    with _notify_config_lock:
        cfg = dict(_notify_config)
        cfg["templates"] = dict(_notify_config["templates"])
        return cfg

def set_notify_config(updates: Dict[str, Any]) -> None:
    with _notify_config_lock:
        for k in ("on_backup_success", "on_backup_failure", "on_verify_failure",
                  "on_format_complete", "on_inventory_done"):
            if k in updates:
                _notify_config[k] = bool(updates[k])
        if isinstance(updates.get("templates"), dict):
            for k, v in updates["templates"].items():
                if k in _NOTIFY_DEFAULT_TEMPLATES and isinstance(v, str):
                    _notify_config["templates"][k] = v.strip() or _NOTIFY_DEFAULT_TEMPLATES[k]
    _db_set_json("notify_config", get_notify_config())

def _load_notify_config() -> None:
    data = _db_get_json("notify_config", None)
    if isinstance(data, dict):
        with _notify_config_lock:
            for k in ("on_backup_success", "on_backup_failure", "on_verify_failure",
                      "on_format_complete", "on_inventory_done"):
                if k in data:
                    _notify_config[k] = bool(data[k])
            if isinstance(data.get("templates"), dict):
                for k, v in data["templates"].items():
                    if k in _NOTIFY_DEFAULT_TEMPLATES and isinstance(v, str) and v.strip():
                        _notify_config["templates"][k] = v.strip()

def _render_notify_template(key: str, **tokens: Any) -> str:
    """Render a notification template key with the given token substitutions."""
    cfg = get_notify_config()
    tmpl = cfg["templates"].get(key) or _NOTIFY_DEFAULT_TEMPLATES.get(key, "")
    try:
        return tmpl.format(**tokens)
    except (KeyError, ValueError):
        # Fall back gracefully if template has bad tokens
        return _NOTIFY_DEFAULT_TEMPLATES.get(key, tmpl).format(**tokens)

def build_restore_dest(volume_tag: str = "", pattern: Optional[str] = None) -> str:
    """Expand a subfolder pattern and return the full destination path.

    Supported tokens:
      {volume}   — volume tag (e.g. SM9158L6)
      {tape}     — alias for {volume}
      {date}     — YYYY-MM-DD (today)
      {datetime} — YYYY-MM-DD_HH-MM-SS

    If pattern is empty or resolves to an empty string the bare RESTORE_ROOT is
    returned (preserving the old behaviour).
    """
    if pattern is None:
        pattern = get_restore_subfolder_pattern()
    pattern = str(pattern or "").strip()
    if not pattern:
        return RESTORE_ROOT
    now_dt = datetime.datetime.now()
    expanded = (
        pattern
        .replace("{volume}",   volume_tag or "unknown")
        .replace("{tape}",     volume_tag or "unknown")
        .replace("{date}",     now_dt.strftime("%Y-%m-%d"))
        .replace("{datetime}", now_dt.strftime("%Y-%m-%d_%H-%M-%S"))
    )
    # Sanitise: strip any path separators that could escape RESTORE_ROOT
    expanded = expanded.strip("/").replace("..", "")
    if not expanded:
        return RESTORE_ROOT
    return os.path.join(RESTORE_ROOT, expanded)


def build_backup_dirname(volume_tag: str = "", start_ts: Optional[float] = None,
                         label: str = "", pattern: Optional[str] = None) -> str:
    """Return the directory name that will be used as the top-level prefix inside
    the tar archive for this backup.

    Uses the same pattern as the restore subfolder so that restoring a backup
    automatically drops files into a uniquely-named folder.

    Extra token supported here:
      {label}  — the backup job label (sanitised), falls back to {volume} if empty

    The result is a single path component (no slashes), safe to use as a tar
    --transform prefix.  If the pattern is empty a sensible default is generated
    from the volume tag and timestamp.
    """
    if pattern is None:
        pattern = get_restore_subfolder_pattern()
    pattern = str(pattern or "").strip()

    # Use backup start time, not "now", so the name is stable throughout the job
    dt = datetime.datetime.fromtimestamp(float(start_ts or time.time()))

    safe_label = re.sub(r"[^A-Za-z0-9_\-]", "_", str(label or "").strip())[:40]

    if not pattern:
        # Fallback when pattern is blank: volume_YYYY-MM-DD_HHMM
        base = volume_tag or "backup"
        return f"{base}_{dt.strftime('%Y-%m-%d_%H%M')}"

    expanded = (
        pattern
        .replace("{volume}",   volume_tag or "backup")
        .replace("{tape}",     volume_tag or "backup")
        .replace("{label}",    safe_label or volume_tag or "backup")
        .replace("{date}",     dt.strftime("%Y-%m-%d"))
        .replace("{datetime}", dt.strftime("%Y-%m-%d_%H-%M-%S"))
    )
    # Strip path separators and dots so this is always a single safe directory name
    expanded = re.sub(r"[/\\]", "_", expanded).strip("._").replace("..", "")
    return expanded or f"{volume_tag or 'backup'}_{dt.strftime('%Y-%m-%d_%H%M')}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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

MTX_SLOT_RE  = re.compile(r"^\s*Storage Element\s+(\d+)(\s+IMPORT/EXPORT)?\s*:\s*(Full|Empty)(?:\s*:\s*VolumeTag\s*=\s*(.*?))?\s*$", re.I)
MTX_DRIVE_RE = re.compile(r"^\s*Data Transfer Element\s+(\d+)\s*:\s*(Empty|Full)(?:\s*\(\s*Storage Element\s+(\d+)\s+Loaded\s*\))?(?:\s*:\s*VolumeTag\s*=\s*(.*?))?\s*$", re.I)
DENSITY_RE   = re.compile(r"Density code .*?\((.*?)\)")
STATUS_RE    = re.compile(r"General status bits on \((.*?)\):\s*(.*)")

def log_action(kind, ok, detail, extra=None):
    with _action_lock:
        _action_log.insert(0, {"ts": now_ts(), "kind": kind, "ok": ok, "detail": detail, "extra": extra or {}})
        del _action_log[500:]
    _save_action_log()
    db_log("action", "info" if ok else "error", f"{kind}: {detail}")

def _insert_log(job_dict, lock, message, category="app"):
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
def append_verify_log(msg):    _insert_log(_verify_job, _verify_lock, msg, "verify")

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

def _load_drive_history() -> None:
    global _drive_history
    data = _db_get_json("drive_history", None)
    if isinstance(data, dict):
        _drive_history = data
        return
    os.makedirs(os.path.dirname(DRIVE_HISTORY_FILE), exist_ok=True)
    if not os.path.exists(DRIVE_HISTORY_FILE):
        _drive_history = {}
        return
    try:
        with open(DRIVE_HISTORY_FILE) as f:
            _drive_history = json.load(f)
        _db_set_json("drive_history", _drive_history)
    except Exception:
        _drive_history = {}


def _save_drive_history() -> None:
    with _drive_history_lock:
        payload = json.loads(json.dumps(_drive_history))
    _db_set_json("drive_history", payload)

def _record_tape_loaded(vol: str) -> None:
    """Call when a tape is confirmed loaded into the drive."""
    global _drive_loaded_at, _drive_loaded_vol
    if not vol:
        return
    _drive_loaded_at = now_ts()
    _drive_loaded_vol = vol
    with _drive_history_lock:
        entry = _drive_history.setdefault(vol, {
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
        entry["last_loaded"] = _drive_loaded_at
        if not entry.get("first_loaded"):
            entry["first_loaded"] = _drive_loaded_at
    _save_drive_history()

def _record_tape_unloaded(vol: str) -> None:
    global _drive_loaded_at, _drive_loaded_vol
    if vol:
        with _drive_history_lock:
            entry = _drive_history.get(vol, {})
            if entry:
                entry["last_unloaded"] = now_ts()
        _save_drive_history()
    _drive_loaded_at = None
    _drive_loaded_vol = ""

def _record_backup_done(vol: str, bytes_written: int) -> None:
    if not vol:
        return
    with _drive_history_lock:
        entry = _drive_history.setdefault(vol, {"volume_tag": vol})
        entry["last_backup"] = now_ts()
        entry["backup_count"] = entry.get("backup_count", 0) + 1
        entry["total_backup_bytes"] = entry.get("total_backup_bytes", 0) + bytes_written
    _save_drive_history()

def _record_restore_done(vol: str) -> None:
    if not vol:
        return
    with _drive_history_lock:
        entry = _drive_history.setdefault(vol, {"volume_tag": vol})
        entry["last_restore"] = now_ts()
        entry["restore_count"] = entry.get("restore_count", 0) + 1
    _save_drive_history()

def _load_last_known_loaded_slot() -> None:
    global _last_known_loaded_slot
    slot = _db_get_json("last_known_loaded_slot", None)
    try:
        slot = int(slot) if slot is not None else None
    except Exception:
        slot = None
    if slot and slot > 0:
        _last_known_loaded_slot = slot
        return
    os.makedirs(os.path.dirname(LAST_LOADED_SLOT_FILE), exist_ok=True)
    if not os.path.exists(LAST_LOADED_SLOT_FILE):
        _last_known_loaded_slot = None
        return
    try:
        with open(LAST_LOADED_SLOT_FILE) as f:
            data = json.load(f)
        slot = int(data.get("slot", 0) or 0)
        _last_known_loaded_slot = slot if slot > 0 else None
        _db_set_json("last_known_loaded_slot", _last_known_loaded_slot)
    except Exception:
        _last_known_loaded_slot = None


def _save_last_known_loaded_slot(slot: Optional[int]) -> None:
    global _last_known_loaded_slot
    _last_known_loaded_slot = slot if slot and int(slot) > 0 else None
    _db_set_json("last_known_loaded_slot", _last_known_loaded_slot)

def get_effective_loaded_slot() -> Optional[int]:
    drive = _state_cache.get("drive", {}) or {}
    slot = drive.get("loaded_from_slot")
    if slot:
        return int(slot)
    return _last_known_loaded_slot

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
    with _backup_records_lock:
        recs = list(_backup_records)
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
    summary = (_state_cache.get("summary") or {})
    drive = (_state_cache.get("drive") or {})
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


def db_counts() -> Dict[str, int]:
    out = {"catalog_rows": 0, "log_rows": 0}
    try:
        with tape_catalog_conn() as conn:
            out["catalog_rows"] = conn.execute("SELECT COUNT(*) FROM tape_catalog WHERE is_deleted = 0").fetchone()[0]
            out["log_rows"] = conn.execute("SELECT COUNT(*) FROM app_log").fetchone()[0]
    except Exception:
        pass
    return out


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
    try:
        text = run_cmd(["mt", "-f", TAPE, "status"], timeout=30)
    except Exception:
        return False
    upper = (text or "").upper()
    return "EOD" in upper or "EOT" in upper


def _candidate_rewrite_tapes(current_volume: str = "") -> List[Dict[str, Any]]:
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
    drive = _state_cache.get("drive", {})
    vol = drive.get("volume_tag", "")
    with _drive_history_lock:
        hist = dict(_drive_history.get(vol, {})) if vol else {}

    time_in_drive = None
    if _drive_loaded_at and not drive.get("empty"):
        time_in_drive = now_ts() - _drive_loaded_at

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

    effective_slot = drive.get("loaded_from_slot") or _last_known_loaded_slot

    return {
        "volume_tag": vol,
        "empty": drive.get("empty", True),
        "online": drive.get("online", False),
        "ready": drive.get("online", False),
        "at_bot": drive.get("at_bot", False),
        "density": drive.get("density", ""),
        "loaded_from_slot": drive.get("loaded_from_slot"),
        "effective_loaded_slot": drive.get("effective_loaded_slot") or drive.get("loaded_from_slot") or _last_known_loaded_slot,
        "effective_loaded_slot": effective_slot,
        "loaded_at": _drive_loaded_at,
        "time_in_drive_seconds": time_in_drive,
        "history": hist,
        "index": idx_meta,
        "space": build_tape_space_info(vol, drive=drive, idx=load_tape_index(vol) or {}, loaded=not drive.get("empty", True)) if vol else build_tape_space_info("", loaded=False),
        "raw_mt_status": drive.get("raw_status", ""),
    }

def _check_drive_change() -> None:
    """Detect load/unload events by comparing drive state to last known volume."""
    global _drive_loaded_vol
    drive = _state_cache.get("drive", {})
    current_vol = drive.get("volume_tag", "") if not drive.get("empty") else ""
    if current_vol and current_vol != _drive_loaded_vol:
        # New tape appeared
        _record_tape_loaded(current_vol)
    elif not current_vol and _drive_loaded_vol:
        # Tape was removed
        _record_tape_unloaded(_drive_loaded_vol)

# ---------------------------------------------------------------------------
# MTX / MT parsing
# ---------------------------------------------------------------------------

def normalize_slot(slot_data: Dict[str, Any], has_mail_slot: Optional[bool] = None) -> Dict[str, Any]:
    """Normalize slot metadata for libraries with or without a mail slot and assign magazine info.

    TL_HAS_MAIL_SLOT is only needed to force-enable mail slot support when mtx does
    not report any IE slots in its header (some firmware versions omit the IE count).
    If mtx itself marks a slot IMPORT/EXPORT we always honour that — suppressing it
    caused the mail slot export/import buttons to be permanently disabled even on
    libraries like the Dell TL2000 that correctly report their IE slot.
    """
    slot_num = int(slot_data.get("slot", 0) or 0)
    declared_ie = bool(slot_data.get("is_import_export", False))
    # Always respect what mtx reported. The env var is a force-enable, not a gate.
    effective_ie = declared_ie
    # IE slots don't belong to a magazine
    magazine = ((slot_num - 1) // max(MAGAZINE_SIZE, 1)) + 1 if slot_num > 0 and not effective_ie else None
    slot_in_mag = ((slot_num - 1) % max(MAGAZINE_SIZE, 1)) + 1 if slot_num > 0 and not effective_ie else None
    return {
        **slot_data,
        "declared_import_export": declared_ie,
        "is_import_export": effective_ie,
        "magazine": magazine,
        "slot_in_magazine": slot_in_mag,
    }

def parse_mtx_status(text):
    slots, drive = [], {"element": 0, "empty": True, "loaded_from_slot": None, "volume_tag": ""}
    hm = re.search(r"Storage Changer .*?:(\d+) Drives, (\d+) Slots \(\s*(\d+) Import/Export \)", text, re.I)
    raw_ie_slots = int(hm.group(3)) if hm else 0
    detected_has_mail_slot = bool(raw_ie_slots) or bool(HAS_MAIL_SLOT)
    header = {
        "drives": int(hm.group(1)) if hm else None,
        "slots_total": int(hm.group(2)) if hm else None,
        "ie_slots": raw_ie_slots if detected_has_mail_slot else 0,
        "magazine_size": MAGAZINE_SIZE,
        "has_mail_slot": detected_has_mail_slot,
    }

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        md = MTX_DRIVE_RE.match(line)
        if not md and "Data Transfer Element" in line:
            # Very forgiving fallback for vendor/mtx formatting oddities.
            elem_m = re.search(r"Data Transfer Element\s+(\d+)", line, re.I)
            state_m = re.search(r":\s*(Empty|Full)", line, re.I)
            slot_m = re.search(r"Storage Element\s+(\d+)\s+Loaded", line, re.I)
            vol_m = re.search(r"VolumeTag\s*=\s*(.+?)\s*$", line, re.I)
            if elem_m and state_m:
                md = (
                    elem_m.group(1),
                    state_m.group(1),
                    slot_m.group(1) if slot_m else None,
                    vol_m.group(1) if vol_m else "",
                )

        if md:
            if isinstance(md, tuple):
                elem, state, loaded_slot, vol = md
            else:
                elem, state, loaded_slot, vol = md.group(1), md.group(2), md.group(3), md.group(4)
            drive = {
                "element": int(elem),
                "empty": str(state).strip().lower() == "empty",
                "loaded_from_slot": int(loaded_slot) if loaded_slot else None,
                "volume_tag": (vol or "").strip(),
            }
            continue

        ms = MTX_SLOT_RE.match(line)
        if ms:
            slots.append(normalize_slot(asdict(Slot(
                int(ms.group(1)),
                bool(ms.group(2)),
                str(ms.group(3)).strip().lower() == "full",
                (ms.group(4) or "").strip(),
            )), has_mail_slot=detected_has_mail_slot))

    return {"header": header, "slots": slots, "drive": drive, "raw": text,
            # Recompute has_mail_slot in case any slot was parsed as IE even if header said 0
            "has_mail_slot": any(s.get("is_import_export") for s in slots) or header.get("has_mail_slot", False)}

def parse_mt_status(text, mtx_drive):
    bits, density = [], ""
    md = DENSITY_RE.search(text)
    if md:
        density = md.group(1).strip()
    mb = STATUS_RE.search(text)
    if mb:
        bits = mb.group(2).split()

    upper_text = (text or "").upper()

    # Start with mtx's view
    drive_empty = bool(mtx_drive.get("empty", True))

    # Strong empty indicators from mt
    mt_says_empty = (
        "FILE NUMBER=-1" in upper_text and
        "BLOCK NUMBER=-1" in upper_text
    ) or (" DR_OPEN " in f" {upper_text} " and " BOT " not in f" {upper_text} ")

    # Strong media-present indicators from mt
    mt_says_loaded = (
        " BOT " in f" {upper_text} " or
        "EOT" in upper_text or
        "FILE NUMBER=0" in upper_text or
        ("FILE NUMBER=" in upper_text and "FILE NUMBER=-1" not in upper_text)
    )

    if mt_says_empty:
        drive_empty = True
    elif mt_says_loaded:
        drive_empty = False

    # Empty drives often do not report ONLINE. Do not treat that as an error.
    online = True
    if bits:
        bad_bits = {"ERROR", "WR_PROT_ERR"}
        if any(bit in bits for bit in bad_bits):
            online = False

    # DR_OPEN by itself should not mean "offline" for an empty drive
    if drive_empty:
        online = True

    volume_tag = (mtx_drive.get("volume_tag", "") or "").strip()
    loaded_from_slot = mtx_drive.get("loaded_from_slot")

    # If mt says empty, do not keep stale loaded metadata around
    if drive_empty:
        volume_tag = ""
        loaded_from_slot = None

    return asdict(Drive(
        element=mtx_drive.get("element", 0),
        empty=drive_empty,
        loaded_from_slot=loaded_from_slot,
        volume_tag=volume_tag,
        online=online,
        at_bot=("BOT" in bits) and not drive_empty,
        raw_status=text,
        density=density,
    ))

def collect_state():
    mtx_text = run_cmd(["mtx","-f",CHANGER,"status"])
    mtx_data = parse_mtx_status(mtx_text)
    try:
        mt_text = run_cmd(["mt","-f",TAPE,"status"])
    except TapeError as e:
        mt_text = str(e)
    drive = parse_mt_status(mt_text, mtx_data["drive"])
    slots = mtx_data["slots"]
    ie   = next((s for s in slots if s["is_import_export"]), None)
    cln  = next((s for s in slots if s["volume_tag"].startswith("CLN")), None)
    usable_slots = [s for s in slots if not s["is_import_export"]]
    full = [s for s in usable_slots if s["full"]]
    magazine_count = max((s.get("magazine") or 0 for s in usable_slots), default=0)
    magazines = []
    for mag in range(1, magazine_count + 1):
        mag_slots = [s for s in usable_slots if s.get("magazine") == mag]
        magazines.append({
            "magazine": mag,
            "slot_count": len(mag_slots),
            "full_slots": len([s for s in mag_slots if s.get("full")]),
            "empty_slots": len([s for s in mag_slots if not s.get("full")]),
            "slots": mag_slots,
        })
    effective_loaded_slot = drive["loaded_from_slot"] or _last_known_loaded_slot
    summary = {
        "total_slots": len(usable_slots),
        "full_slots": len(full),
        "empty_slots": len([s for s in usable_slots if not s["full"]]),
        "loaded": not drive["empty"],
        "loaded_slot": effective_loaded_slot,
        "loaded_volume": drive["volume_tag"],
        "import_export_tag": (ie or {}).get("volume_tag","") if ie else "",
        "import_export_slot": (ie or {}).get("slot") if ie else None,
        "import_export_full": (ie or {}).get("full", False) if ie else False,
        "cleaning_slot": (cln or {}).get("slot"),
        "cleaning_tag":  (cln or {}).get("volume_tag",""),
        "density": drive["density"],
        "online": drive["online"],
        "ready": drive["online"],
        "at_bot": drive["at_bot"],
        "has_mail_slot": mtx_data.get("has_mail_slot") or mtx_data.get("header", {}).get("has_mail_slot", HAS_MAIL_SLOT),
        "magazine_size": MAGAZINE_SIZE,
        "magazines": magazines,
    }
    return {
        "ok": True, "changer": CHANGER, "tape": TAPE, "backup_root": BACKUP_ROOT,
        "slots": slots, "drive": {**drive, "effective_loaded_slot": effective_loaded_slot}, "summary": summary,
        "backup_job": snapshot_backup_job(),
        "restore_job": snapshot_restore_job(),
        "inventory_job": snapshot_inventory_job(),
        "verify_job": snapshot_verify_job(),
        "format_job": snapshot_format_job(),
        "drive_info": get_drive_info(),
        "last_updated": now_ts(), "last_error": None,
    }

def refresh_state():
    global _state_cache
    try:
        _state_cache = collect_state()
    except Exception as e:
        _state_cache = {**_state_cache, "ok": False,
                        "backup_job": snapshot_backup_job(),
                        "restore_job": snapshot_restore_job(),
                        "inventory_job": snapshot_inventory_job(),
                        "last_error": str(e), "last_updated": now_ts()}
    _check_drive_change()
    return _state_cache

def get_mail_slot_info(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the import/export (mail) slot dict from a state snapshot, or None.

    Checks both the slot list (is_import_export flag) and the summary block so it
    works regardless of whether HAS_MAIL_SLOT is set in the environment.  The TL2000
    reports its IE slot via mtx, so after the normalize_slot fix this will always
    find it when a tape is present in the mail slot or the slot is empty-but-present.
    """
    # Prefer the slot list — most reliable source
    for s in (state.get("slots") or []):
        if s.get("is_import_export"):
            return s
    # Fallback: summary may have import_export_slot even if slot list is stale
    summary = state.get("summary") or {}
    ie_slot_num = summary.get("import_export_slot")
    if ie_slot_num:
        # Build a minimal dict so callers don't have to special-case
        return {
            "slot": ie_slot_num,
            "full": bool(summary.get("import_export_full", False)),
            "volume_tag": summary.get("import_export_tag", ""),
            "is_import_export": True,
        }
    return None


def ensure_under_backup_root(raw):
    raw = (raw or "").strip()
    if not raw: raise TapeError("Empty path.")
    resolved = os.path.realpath(raw)
    root = os.path.realpath(BACKUP_ROOT)
    if not resolved.startswith(root + os.sep) and resolved != root:
        raise TapeError(f"Path outside backup root: {raw}")
    if not os.path.exists(resolved): raise TapeError(f"Path does not exist: {raw}")
    return resolved

def list_directories(path=None):
    base = ensure_under_backup_root(path or BACKUP_ROOT)
    dirs = []
    files = []
    with os.scandir(base) as it:
        for e in it:
            try:
                if e.is_dir(follow_symlinks=False):
                    dirs.append({"name": e.name, "path": e.path})
                elif e.is_file(follow_symlinks=False):
                    try:
                        size = e.stat(follow_symlinks=False).st_size
                    except OSError:
                        size = 0
                    files.append({"name": e.name, "path": e.path, "size": size})
            except OSError:
                pass
    dirs.sort(key=lambda x: x["name"].lower())
    files.sort(key=lambda x: x["name"].lower())
    parent = None if os.path.realpath(base)==os.path.realpath(BACKUP_ROOT) else os.path.dirname(base)
    return {"root": BACKUP_ROOT, "current": base, "parent": parent, "directories": dirs, "files": files}


def ensure_under_restore_root(raw):
    raw = (raw or "").strip()
    if not raw:
        raise TapeError("Empty restore path.")
    resolved = os.path.realpath(raw)
    root = os.path.realpath(RESTORE_ROOT)
    if not resolved.startswith(root + os.sep) and resolved != root:
        raise TapeError(f"Path outside restore root: {raw}")
    os.makedirs(resolved, exist_ok=True)
    return resolved


def list_restore_directories(path=None):
    base = ensure_under_restore_root(path or RESTORE_ROOT)
    dirs = []
    files = []
    with os.scandir(base) as it:
        for e in it:
            try:
                if e.is_dir(follow_symlinks=False):
                    dirs.append({"name": e.name, "path": e.path})
                elif e.is_file(follow_symlinks=False):
                    try:
                        size = e.stat(follow_symlinks=False).st_size
                    except OSError:
                        size = 0
                    files.append({"name": e.name, "path": e.path, "size": size})
            except OSError:
                pass
    dirs.sort(key=lambda x: x["name"].lower())
    files.sort(key=lambda x: x["name"].lower())
    parent = None if os.path.realpath(base)==os.path.realpath(RESTORE_ROOT) else os.path.dirname(base)
    return {"root": RESTORE_ROOT, "current": base, "parent": parent, "directories": dirs, "files": files}


def get_cleaning_slot() -> Optional[int]:
    for s in (_state_cache.get("slots") or []):
        if s.get("full") and is_cleaning_volume_tag(s.get("volume_tag", "")):
            return int(s.get("slot"))
    return (_state_cache.get("summary") or {}).get("cleaning_slot")


def estimate_path_size(path):
    if os.path.isfile(path):
        try: return os.path.getsize(path)
        except OSError: return 0
    total = 0
    for r, ds, fs in os.walk(path, onerror=lambda e: None, followlinks=False):
        for n in fs:
            try: total += os.path.getsize(os.path.join(r, n))
            except OSError: pass
    return total

# ---------------------------------------------------------------------------
# Tape catalog (SQLite, embedded in the app)
# ---------------------------------------------------------------------------

def tape_catalog_conn():
    os.makedirs(os.path.dirname(TAPE_CATALOG_DB), exist_ok=True)
    conn = sqlite3.connect(TAPE_CATALOG_DB)
    conn.row_factory = sqlite3.Row
    # WAL mode: set on every connection — SQLite persists the journal_mode
    # setting in the DB file so this is effectively a no-op after the first call,
    # but setting it explicitly ensures any new file starts in WAL mode too.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_tape_catalog() -> None:
    with tape_catalog_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tape_catalog (
                volume_tag TEXT PRIMARY KEY,
                written_at INTEGER,
                file_count INTEGER NOT NULL DEFAULT 0,
                files_json TEXT NOT NULL DEFAULT '[]',
                present INTEGER NOT NULL DEFAULT 0,
                last_seen_at INTEGER,
                last_seen_slot INTEGER,
                magazine INTEGER,
                slot_in_magazine INTEGER,
                purpose TEXT,
                is_cleaning INTEGER NOT NULL DEFAULT 0,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(tape_catalog)").fetchall()}
        if "purpose" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN purpose TEXT")
        if "is_cleaning" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN is_cleaning INTEGER NOT NULL DEFAULT 0")
        if "lto_generation" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN lto_generation INTEGER")
        if "capacity_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN capacity_bytes INTEGER")
        if "used_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN used_bytes INTEGER")
        if "remaining_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN remaining_bytes INTEGER")
        if "remaining_pct" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN remaining_pct REAL")
        if "space_estimated" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN space_estimated INTEGER NOT NULL DEFAULT 1")
        if "backup_dirnames" not in cols:
            # JSON array of unique top-level directory names baked into this tape's archive(s).
            # e.g. ["SM9158L6_2026-03-30", "SM9158L6_2026-04-06"]
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN backup_dirnames TEXT NOT NULL DEFAULT '[]'")
        if "archived_at" not in cols:
            # Timestamp when the tape was last marked as not present after a scan
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN archived_at INTEGER")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_kv (
                key TEXT PRIMARY KEY,
                value_json TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                category TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_app_log_ts ON app_log(ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_app_log_cat ON app_log(category, ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tape_catalog_present ON tape_catalog(present, is_deleted)")
        conn.commit()

def db_log(category: str, level: str, message: str) -> None:
    ts = now_ts()
    with tape_catalog_conn() as conn:
        conn.execute(
            "INSERT INTO app_log (ts, category, level, message) VALUES (?, ?, ?, ?)",
            (ts, category, level, message),
        )
        conn.commit()

def _prune_app_log() -> None:
    """Trim app_log by age and row count.  Called periodically from scheduler_loop,
    not on every write, to avoid a full-table-scan on each log entry."""
    ts = now_ts()
    try:
        with tape_catalog_conn() as conn:
            cutoff = ts - (LOG_RETENTION_DAYS * 86400)
            conn.execute("DELETE FROM app_log WHERE ts < ?", (cutoff,))
            conn.execute("""
                DELETE FROM app_log
                WHERE id NOT IN (
                    SELECT id FROM app_log
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                )
            """, (LOG_MAX_ROWS,))
            conn.commit()
    except Exception:
        pass

def get_recent_logs(limit: int = 500, category: Optional[str] = None) -> List[Dict[str, Any]]:
    with tape_catalog_conn() as conn:
        if category:
            rows = conn.execute(
                "SELECT ts, category, level, message FROM app_log WHERE category=? ORDER BY ts DESC, id DESC LIMIT ?",
                (category, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT ts, category, level, message FROM app_log ORDER BY ts DESC, id DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [dict(row) for row in rows]

def _db_set_json(key: str, value: Any) -> None:
    with tape_catalog_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_kv (key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (key, json.dumps(value), now_ts()),
        )
        conn.commit()


def _db_get_json(key: str, default: Any = None) -> Any:
    with tape_catalog_conn() as conn:
        row = conn.execute("SELECT value_json FROM app_kv WHERE key = ?", (key,)).fetchone()
    if not row or row[0] is None:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default


def _save_action_log() -> None:
    with _action_lock:
        payload = json.loads(json.dumps(_action_log[:500]))
    _db_set_json("action_log", payload)


def _load_action_log() -> None:
    global _action_log
    data = _db_get_json("action_log", None)
    if isinstance(data, list):
        cleaned = []
        for item in data:
            if not isinstance(item, dict):
                continue
            cleaned.append({
                "ts": int(item.get("ts", 0) or 0),
                "kind": str(item.get("kind", "app") or "app"),
                "ok": item.get("ok", True),
                "detail": str(item.get("detail", "") or ""),
                "extra": item.get("extra", {}) if isinstance(item.get("extra", {}), dict) else {},
            })
        _action_log = cleaned[:500]
    else:
        _action_log = []

def migrate_legacy_tape_indexes() -> None:
    try:
        os.makedirs(TAPE_INDEX_DIR, exist_ok=True)
    except OSError:
        return
    legacy = [n for n in os.listdir(TAPE_INDEX_DIR) if n.endswith('.json')]
    if not legacy:
        return
    for name in legacy:
        path = os.path.join(TAPE_INDEX_DIR, name)
        try:
            with open(path) as f:
                data = json.load(f)
            vol = str(data.get('volume_tag') or '').strip()
            if not vol:
                continue
            save_tape_index(
                vol,
                data.get('files') or [],
                int(data.get('written_at') or 0),
                meta={
                    'present': bool(data.get('present', False)),
                    'last_seen_at': data.get('last_seen_at'),
                    'last_seen_slot': data.get('last_seen_slot'),
                    'magazine': data.get('magazine'),
                    'slot_in_magazine': data.get('slot_in_magazine'),
                    'purpose': data.get('purpose') or ('cleaning' if is_cleaning_volume_tag(vol) else 'data'),
                    'is_cleaning': data.get('is_cleaning', is_cleaning_volume_tag(vol)),
                },
            )
        except Exception:
            pass

def _catalog_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    try:
        return bool(int(v)) if isinstance(v, (int, bool)) else bool(v)
    except Exception:
        return default

def _row_to_index(row: sqlite3.Row) -> Dict[str, Any]:
    files = []
    try:
        files = json.loads(row['files_json'] or '[]')
    except Exception:
        files = []
    data = {
        'volume_tag': row['volume_tag'],
        'written_at': row['written_at'],
        'file_count': int(row['file_count'] or len(files)),
        'files': files,
        'present': _catalog_bool(row['present']),
        'last_seen_at': row['last_seen_at'],
        'last_seen_slot': row['last_seen_slot'],
        'magazine': row['magazine'],
        'slot_in_magazine': row['slot_in_magazine'],
        'purpose': row['purpose'] or ('cleaning' if _catalog_bool(row['is_cleaning']) else 'data'),
        'is_cleaning': _catalog_bool(row['is_cleaning']),
        'deleted': _catalog_bool(row['is_deleted']),
        'updated_at': row['updated_at'],
        'lto_generation': row['lto_generation'] if 'lto_generation' in row.keys() else None,
        'capacity_bytes': row['capacity_bytes'] if 'capacity_bytes' in row.keys() else None,
        'used_bytes': row['used_bytes'] if 'used_bytes' in row.keys() else None,
        'remaining_bytes': row['remaining_bytes'] if 'remaining_bytes' in row.keys() else None,
        'remaining_pct': row['remaining_pct'] if 'remaining_pct' in row.keys() else None,
        'space_estimated': _catalog_bool(row['space_estimated'], True) if 'space_estimated' in row.keys() else True,
        'backup_dirnames': json.loads(row['backup_dirnames'] or '[]') if 'backup_dirnames' in row.keys() else [],
        'archived_at': row['archived_at'] if 'archived_at' in row.keys() else None,
    }
    data['space'] = build_tape_space_info(data['volume_tag'], idx=data, loaded=False)
    return data

def save_tape_index(vol, files, written_at, meta=None):
    if not vol:
        return
    meta = meta or {}
    files = list(files or [])
    ts_now = now_ts()
    is_cleaning = bool(meta.get('is_cleaning', is_cleaning_volume_tag(vol)))
    purpose = meta.get('purpose') or ('cleaning' if is_cleaning else 'data')

    # Merge the new backup_dirname into the tape's running list of dirnames
    new_dirname = str(meta.get('backup_dirname') or '').strip()
    existing_idx = load_tape_index(vol)
    existing_dirnames = list(existing_idx.get('backup_dirnames') or []) if existing_idx else []
    if new_dirname and new_dirname not in existing_dirnames:
        existing_dirnames.append(new_dirname)
    backup_dirnames_json = json.dumps(existing_dirnames)
    with tape_catalog_conn() as conn:
        conn.execute("""
            INSERT INTO tape_catalog (
                volume_tag, written_at, file_count, files_json,
                present, last_seen_at, last_seen_slot, magazine, slot_in_magazine,
                purpose, is_cleaning, lto_generation, capacity_bytes, used_bytes,
                remaining_bytes, remaining_pct, space_estimated, backup_dirnames,
                is_deleted, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(volume_tag) DO UPDATE SET
                written_at=excluded.written_at,
                -- Only overwrite files_json when the new list is non-empty.
                -- An empty list from a failed tar -tf would otherwise silently
                -- wipe a previously-saved good index.
                file_count=CASE WHEN excluded.file_count > 0
                                THEN excluded.file_count
                                ELSE tape_catalog.file_count END,
                files_json=CASE WHEN excluded.file_count > 0
                                THEN excluded.files_json
                                ELSE tape_catalog.files_json END,
                present=COALESCE(excluded.present, tape_catalog.present),
                last_seen_at=COALESCE(excluded.last_seen_at, tape_catalog.last_seen_at),
                last_seen_slot=COALESCE(excluded.last_seen_slot, tape_catalog.last_seen_slot),
                magazine=COALESCE(excluded.magazine, tape_catalog.magazine),
                slot_in_magazine=COALESCE(excluded.slot_in_magazine, tape_catalog.slot_in_magazine),
                purpose=COALESCE(excluded.purpose, tape_catalog.purpose),
                is_cleaning=excluded.is_cleaning,
                lto_generation=COALESCE(excluded.lto_generation, tape_catalog.lto_generation),
                capacity_bytes=COALESCE(excluded.capacity_bytes, tape_catalog.capacity_bytes),
                used_bytes=COALESCE(excluded.used_bytes, tape_catalog.used_bytes),
                remaining_bytes=COALESCE(excluded.remaining_bytes, tape_catalog.remaining_bytes),
                remaining_pct=COALESCE(excluded.remaining_pct, tape_catalog.remaining_pct),
                space_estimated=COALESCE(excluded.space_estimated, tape_catalog.space_estimated),
                backup_dirnames=excluded.backup_dirnames,
                is_deleted=0,
                updated_at=excluded.updated_at
        """, (
            vol,
            int(written_at or ts_now),
            len(files),
            json.dumps(files),
            1 if meta.get('present') else 0,
            meta.get('last_seen_at') if meta.get('last_seen_at') is not None else ts_now,
            meta.get('last_seen_slot'),
            meta.get('magazine'),
            meta.get('slot_in_magazine'),
            purpose,
            1 if is_cleaning else 0,
            meta.get('lto_generation'),
            meta.get('capacity_bytes'),
            meta.get('used_bytes'),
            meta.get('remaining_bytes'),
            meta.get('remaining_pct'),
            meta.get('space_estimated', 1),
            backup_dirnames_json,
            ts_now,
            ts_now,
        ))
        conn.commit()

def update_tape_index_metadata(vol, **meta):
    if not vol:
        return
    ts_now = now_ts()
    is_cleaning = bool(meta.get('is_cleaning', is_cleaning_volume_tag(vol)))
    purpose = meta.get('purpose') or ('cleaning' if is_cleaning else 'data')
    # When a tape is confirmed present, clear archived_at and restore purpose to 'data'
    # (unless it's being explicitly set to something else).
    being_confirmed_present = bool(meta.get('present'))
    with tape_catalog_conn() as conn:
        conn.execute("""
            INSERT INTO tape_catalog (
                volume_tag, written_at, file_count, files_json,
                present, last_seen_at, last_seen_slot, magazine, slot_in_magazine,
                purpose, is_cleaning, lto_generation, capacity_bytes, used_bytes,
                remaining_bytes, remaining_pct, space_estimated,
                is_deleted, created_at, updated_at
            ) VALUES (?, NULL, 0, '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(volume_tag) DO UPDATE SET
                present=COALESCE(excluded.present, tape_catalog.present),
                last_seen_at=COALESCE(excluded.last_seen_at, tape_catalog.last_seen_at),
                last_seen_slot=COALESCE(excluded.last_seen_slot, tape_catalog.last_seen_slot),
                magazine=COALESCE(excluded.magazine, tape_catalog.magazine),
                slot_in_magazine=COALESCE(excluded.slot_in_magazine, tape_catalog.slot_in_magazine),
                -- When a tape comes back from archived state, restore its purpose to 'data'
                purpose=CASE
                    WHEN excluded.present=1 AND tape_catalog.purpose='archived'
                    THEN COALESCE(excluded.purpose, 'data')
                    ELSE COALESCE(excluded.purpose, tape_catalog.purpose)
                END,
                is_cleaning=excluded.is_cleaning,
                lto_generation=COALESCE(excluded.lto_generation, tape_catalog.lto_generation),
                capacity_bytes=COALESCE(excluded.capacity_bytes, tape_catalog.capacity_bytes),
                used_bytes=COALESCE(excluded.used_bytes, tape_catalog.used_bytes),
                remaining_bytes=COALESCE(excluded.remaining_bytes, tape_catalog.remaining_bytes),
                remaining_pct=COALESCE(excluded.remaining_pct, tape_catalog.remaining_pct),
                space_estimated=COALESCE(excluded.space_estimated, tape_catalog.space_estimated),
                -- Clear archived_at when tape is confirmed present again
                archived_at=CASE WHEN excluded.present=1 THEN NULL ELSE tape_catalog.archived_at END,
                is_deleted=0,
                updated_at=excluded.updated_at
        """, (
            vol,
            1 if meta.get('present') else 0,
            meta.get('last_seen_at') if meta.get('last_seen_at') is not None else ts_now,
            meta.get('last_seen_slot'),
            meta.get('magazine'),
            meta.get('slot_in_magazine'),
            purpose,
            1 if is_cleaning else 0,
            meta.get('lto_generation'),
            meta.get('capacity_bytes'),
            meta.get('used_bytes'),
            meta.get('remaining_bytes'),
            meta.get('remaining_pct'),
            meta.get('space_estimated', 1),
            ts_now,
            ts_now,
        ))
        conn.commit()

def load_tape_index(vol):
    if not vol:
        return None
    with tape_catalog_conn() as conn:
        row = conn.execute(
            "SELECT * FROM tape_catalog WHERE volume_tag = ? AND is_deleted = 0",
            (vol,),
        ).fetchone()
    return _row_to_index(row) if row else None

def mark_tape_archived(vol: str) -> None:
    """Mark a tape as archived — present=0, purpose='archived', archived_at=now.

    Called during inventory when a tape that was previously catalogued is no
    longer seen in any slot or the drive.  The catalog record and file index
    are preserved so the tape can still be browsed and restored if it's
    reinserted later.
    """
    if not vol:
        return
    ts = now_ts()
    with tape_catalog_conn() as conn:
        conn.execute(
            """UPDATE tape_catalog
               SET present = 0, purpose = 'archived', archived_at = ?, updated_at = ?
               WHERE volume_tag = ? AND is_deleted = 0""",
            (ts, ts, vol),
        )
        conn.commit()

def delete_tape_index(vol: str, permanent: bool = False) -> bool:
    """Remove a tape from the catalog.

    permanent=False (default / soft delete):
      Sets is_deleted=1.  The row is hidden from all normal queries but the
      data is retained for recovery.  Use this for routine removal.

    permanent=True (hard delete):
      Physically removes the row.  All history, file index, and space data
      are gone forever.  Only called when the user explicitly confirms a
      permanent wipe via the UI.
    """
    if not vol:
        return False
    with tape_catalog_conn() as conn:
        if permanent:
            cur = conn.execute("DELETE FROM tape_catalog WHERE volume_tag = ?", (vol,))
        else:
            cur = conn.execute(
                "UPDATE tape_catalog SET is_deleted = 1, present = 0, updated_at = ? WHERE volume_tag = ?",
                (now_ts(), vol),
            )
        conn.commit()
        return cur.rowcount > 0

def mark_all_indexes_not_present():
    """Mark every catalog entry as not currently present in the library.

    Preserves last_seen_slot — we want to remember where a tape was last seen
    even after it has been removed from the library.  The slot is only cleared
    when a tape is confirmed present at a *different* slot (i.e. it moved).
    """
    with tape_catalog_conn() as conn:
        conn.execute(
            "UPDATE tape_catalog SET present = 0, updated_at = ? WHERE is_deleted = 0",
            (now_ts(),),
        )
        conn.commit()

def read_tape_index_live() -> List[str]:
    """Read the file list from the tape currently in the drive.

    Uses `dd if=TAPE bs=TAPE_BLOCK_BYTES | tar -t -f -` so that the physical
    block size matches what was used when writing (default 512 KiB via dd).
    Reading with plain `tar -tf /dev/nst0` uses the default 512-byte block
    size, which causes the kernel tape driver to return ENOMEM when it tries
    to read a 512 KiB physical block into a 512-byte buffer.

    Always rewinds before reading.
    """
    try:
        subprocess.run(["mt", "-f", TAPE, "rewind"],
                       capture_output=True, timeout=max(COMMAND_TIMEOUT, 300), check=True)
    except Exception as e:
        raise TapeError(f"Rewind before index read failed: {e}")

    # dd reads physical tape blocks at the correct block size and streams bytes
    # to tar's stdin; tar reads the archive from stdin with no block-size concern.
    # status=progress ensures dd emits its byte counter and any errors to stderr
    # even if tar exits early — critical for diagnosing failures.
    dd_proc = subprocess.Popen(
        ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_proc = subprocess.Popen(
        ["tar", "-t", "-f", "-"],
        stdin=dd_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Allow dd to receive SIGPIPE if tar exits early
    dd_proc.stdout.close()

    # Drain dd stderr concurrently — if we don't, and dd's stderr pipe buffer
    # fills, dd will block and tar will never get EOF on its stdin.
    _dd_stderr_buf: List[bytes] = []
    def _drain_dd_err():
        try:
            for line in dd_proc.stderr:
                _dd_stderr_buf.append(line)
        except Exception:
            pass
    _dd_drain_t = threading.Thread(target=_drain_dd_err, daemon=True)
    _dd_drain_t.start()

    # Scale timeout to tape size: assume worst case 80 MB/s read speed.
    # Minimum 10 min, no upper cap — a 12 TB LTO-8 could take ~42 hours at 80 MB/s
    # but in practice we only call this for verify/reindex, not post-backup indexing.
    try:
        tar_out, tar_err = tar_proc.communicate(timeout=max(600, TAPE_BLOCK_BYTES))
    except subprocess.TimeoutExpired:
        # Kill both processes and release /dev/nst0 before re-raising
        for _p in (tar_proc, dd_proc):
            try:
                _p.kill()
            except Exception:
                pass
        for _p in (tar_proc, dd_proc):
            try:
                _p.wait(timeout=10)
            except Exception:
                pass
        _dd_drain_t.join(timeout=5)
        raise TapeError(
            f"tar -t timed out reading tape index — tape may be too large for the "
            f"configured timeout. Use 'Read Index' from the library after the backup completes."
        )
    dd_proc.wait(timeout=30)
    _dd_drain_t.join(timeout=5)

    dd_stderr = b"".join(_dd_stderr_buf).decode(errors="ignore").strip()
    files = [l for l in (tar_out or b"").decode(errors="ignore").splitlines() if l.strip()]
    err = (tar_err or b"").decode(errors="ignore").strip()

    # Append any dd errors to the tar error message for diagnostics
    if dd_stderr and "error" in dd_stderr.lower():
        err = (err + "\ndd: " + dd_stderr[-300:]).strip()

    # rc=1 from tar means warnings (e.g. socket files skipped) — still usable.
    # rc=2 means fatal error and no output.
    if tar_proc.returncode not in (0, 1) and not files:
        # Distinguish blank/foreign-format tapes from genuine read errors.
        # "does not look like a tar archive" means the tape has data but it's not tar
        # (written by other software, or a partial/corrupt first block).
        # Empty stderr with rc=2 typically means a completely blank tape.
        _BLANK_OR_FOREIGN = (
            "does not look like a tar archive" in err
            or "This does not look like a tar archive" in err
            or "Skipping to next header" in err
            or not err  # blank tape — dd reads nothing, tar gets EOF immediately
        )
        if _BLANK_OR_FOREIGN:
            raise TapeError(f"__blank_or_foreign__: {err[:200] or 'no tar header found'}")
        raise TapeError(f"tar -t failed (rc={tar_proc.returncode}): {err[:300] or 'no output'}")

    return files

def list_all_known_indexes(include_deleted: bool = False):
    query = "SELECT * FROM tape_catalog"
    params = []
    if not include_deleted:
        query += " WHERE is_deleted = 0"
    query += " ORDER BY volume_tag COLLATE NOCASE ASC"
    with tape_catalog_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for row in rows:
        item = _row_to_index(row)
        item.pop('files', None)
        result.append(item)
    return result

# ---------------------------------------------------------------------------
# Restore worker
# ---------------------------------------------------------------------------

def restore_worker(volume_tag: str, tape_paths: List[str], dest: str, slot: Optional[int]) -> None:
    """
    Restore files from tape.
    tape_paths: list of paths as they appear in the tar archive.
                If empty, restore everything.
    dest: local destination directory.
    slot: if set, load this slot first (then unload after).

    Supports cancellation via /api/restore/stop — sets _stop_restore which
    terminates the tar process and marks the job cancelled.
    """
    global _restore_proc, _stop_restore
    if is_cleaning_volume_tag(volume_tag):
        raise TapeError(f"{volume_tag} is a cleaning tape and cannot be restored.")

    dest = ensure_under_restore_root(dest)
    _stop_restore = False

    set_restore_state(
        running=True, status="preparing", volume_tag=volume_tag,
        paths=tape_paths, dest=dest,
        started_at=now_ts(), finished_at=None,
        last_message="Preparing restore…", log=[], error=None,
    )
    append_restore_log(f"Restore started. Volume: {volume_tag}, {len(tape_paths)} path(s) → {dest}")
    publish_state_to_mqtt(refresh_state())

    loaded_slot = None
    try:
        os.makedirs(dest, exist_ok=True)

        # Load tape if requested
        if slot is not None:
            cur = (_state_cache.get("drive") or {})
            if not cur.get("empty"):
                existing = cur.get("loaded_from_slot") or _last_known_loaded_slot
                if existing:
                    append_restore_log(f"Unloading current tape (slot {existing}) first…")
                    run_cmd(["mtx","-f",CHANGER,"unload",str(existing),"0"], timeout=max(COMMAND_TIMEOUT,120))
            append_restore_log(f"Loading slot {slot} into drive…")
            run_cmd(["mtx","-f",CHANGER,"load",str(slot),"0"], timeout=max(COMMAND_TIMEOUT,120))
            _save_last_known_loaded_slot(slot)
            loaded_slot = slot
            time.sleep(3)

        # Rewind
        append_restore_log("Rewinding tape…")
        set_restore_state(status="rewinding")
        publish_state_to_mqtt(refresh_state())
        run_cmd(["mt","-f",TAPE,"rewind"], timeout=max(COMMAND_TIMEOUT,300))

        # Build tar extract command — use dd | tar so block size matches what was written.
        # tar reading directly from the tape device uses the wrong block size (512 B)
        # which causes ENOMEM on drives that wrote at 512 KiB blocks.
        tar_paths = [p.lstrip("/") for p in tape_paths]

        # FIX: was "status=none" which swallowed all dd errors silently.
        # "status=progress" lets us capture and log dd read errors/stats so failures
        # are visible and diagnosable instead of producing an empty/corrupt restore.
        dd_cmd  = ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"]
        tar_cmd = ["tar", "-C", dest, "-xvf", "-"]
        if tar_paths:
            tar_cmd += tar_paths

        append_restore_log(
            f"Extracting {'all files' if not tar_paths else str(len(tar_paths))+' path(s)'} to {dest}…  "
            f"(dd bs={TAPE_BLOCK_BYTES//1024}KiB | tar -x)"
        )
        set_restore_state(status="extracting")
        publish_state_to_mqtt(refresh_state())

        dd_proc = subprocess.Popen(dd_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tar_proc = subprocess.Popen(
            tar_cmd,
            stdin=dd_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # FIX: close our copy of dd's stdout in the parent so that when tar exits
        # and closes its end, dd receives SIGPIPE / sees a broken pipe and exits
        # cleanly instead of hanging forever waiting for a reader.
        dd_proc.stdout.close()
        _restore_proc = tar_proc

        count = 0
        _tar_stderr_lines: List[str] = []
        _dd_stderr_lines: List[str] = []

        def _drain_tar_stderr():
            try:
                for raw in tar_proc.stderr:
                    _tar_stderr_lines.append(raw.decode(errors="ignore").rstrip())
            except Exception:
                pass

        # FIX: drain dd stderr in a background thread so dd never blocks on a full
        # stderr pipe.  Previously dd's stderr was never read, so dd could stall
        # waiting to write progress output, stalling the entire pipeline.
        def _drain_dd_stderr():
            try:
                for raw in dd_proc.stderr:
                    line = raw.decode(errors="ignore").rstrip()
                    _dd_stderr_lines.append(line)
            except Exception:
                pass

        t_err    = threading.Thread(target=_drain_tar_stderr, daemon=True)
        t_dd_err = threading.Thread(target=_drain_dd_stderr,  daemon=True)
        t_err.start()
        t_dd_err.start()

        # Drain stdout (verbose file list) — this is what drives progress.
        # No timeout: a full restore of hundreds of GB can take many hours.
        for raw_line in tar_proc.stdout:
            if _stop_restore:
                append_restore_log("Stop requested — terminating restore.")
                try: tar_proc.terminate()
                except Exception: pass
                try: dd_proc.terminate()
                except Exception: pass
                break
            line = raw_line.decode(errors="ignore").strip()
            if line:
                count += 1
                if count % 200 == 0:
                    append_restore_log(f"Extracted {count:,} files… (last: {line[-80:]})")
                    set_restore_state(last_message=f"Extracting… {count:,} files")
                    publish_state_to_mqtt(refresh_state())

        tar_proc.stdout.close()
        t_err.join(timeout=10)

        rc    = tar_proc.wait()
        dd_rc = dd_proc.wait(timeout=30)
        t_dd_err.join(timeout=5)
        _restore_proc = None

        tar_err_text = "\n".join(_tar_stderr_lines[-10:])

        # FIX: check dd exit code.  dd exits non-zero on read errors (e.g. EIO,
        # ENOMEDIUM).  Previously this was never checked so a completely failed
        # read (0 bytes transferred) looked identical to a successful restore.
        if dd_rc not in (0, None) and not _stop_restore:
            dd_err_text = "\n".join(_dd_stderr_lines[-5:]).strip()
            raise TapeError(
                f"dd read from tape failed (exit {dd_rc}). "
                f"Check that the correct tape is loaded and the drive is ready. "
                f"dd stderr: {dd_err_text[-200:]}"
            )

        # Log dd stats (bytes read, speed) at normal verbosity so we can see
        # whether any data actually came off the tape.
        if _dd_stderr_lines:
            append_restore_log(f"dd: {_dd_stderr_lines[-1]}", )

        if _stop_restore:
            set_restore_state(
                running=False, status="cancelled", finished_at=now_ts(),
                last_message=f"Restore cancelled after {count:,} files.", error=None,
            )
            append_restore_log(f"Restore cancelled by user after {count:,} files.")
            log_action("restore", True, f"Cancelled after {count} files from {volume_tag}")
            return

        if rc not in (0, 1):  # tar rc=1 = warnings (e.g. timestamps)
            detail = tar_err_text.strip()[-300:] or f"tar exited rc={rc}"
            raise TapeError(f"tar exited rc={rc}: {detail}")

        if tar_err_text.strip():
            append_restore_log(f"tar warnings: {tar_err_text.strip()[-200:]}")

        set_restore_state(
            running=False, status="completed", finished_at=now_ts(),
            last_message=f"Restore complete — {count:,} files extracted to {dest}.", error=None,
        )
        append_restore_log(f"Restore complete. {count:,} files extracted.")
        log_action("restore", True, f"Restored {len(tape_paths) or 'all'} path(s) from {volume_tag} → {dest}")
        _record_restore_done(volume_tag)

    except Exception as e:
        _restore_proc = None
        set_restore_state(running=False, status="failed", finished_at=now_ts(),
                          error=str(e), last_message=f"Restore failed: {e}")
        append_restore_log(f"Restore failed: {e}")
        log_action("restore", False, str(e))
    finally:
        _restore_proc = None
        _stop_restore = False
        if loaded_slot is not None:
            try:
                append_restore_log(f"Unloading tape back to slot {loaded_slot}…")
                run_cmd(["mtx","-f",CHANGER,"unload",str(loaded_slot),"0"], timeout=max(COMMAND_TIMEOUT,120))
                _save_last_known_loaded_slot(None)
            except Exception as ue:
                append_restore_log(f"Warning: could not unload: {ue}")
        publish_state_to_mqtt(refresh_state())

# ---------------------------------------------------------------------------
# Format (erase) worker
# ---------------------------------------------------------------------------

def set_format_state(**kw):
    with _format_lock: _format_job.update(kw)

def snapshot_format_job():
    return snap(_format_job, _format_lock)

def append_format_log(msg: str) -> None:
    with _format_lock:
        _format_job["log"].insert(0, {"ts": now_ts(), "message": msg})
        del _format_job["log"][200:]
        _format_job["last_message"] = msg
    db_log("format", "info", msg)

def format_worker(tapes: List[Dict[str, Any]], catalog_only: bool = False) -> None:
    """Erase one or more tapes sequentially.

    catalog_only=True  -> skip all hardware; only reset DB/index entries.
                          The tape still has its old data physically but the
                          library treats it as blank/available immediately.
    catalog_only=False -> full hardware short-erase:
      1. Load tape (unload current drive occupant first if needed).
      2. Run `mt erase -s` — short/quick erase, writes new BOT+EOD marker.
         Takes ~5-30 s on LTO-6.  We always pass -s to prevent the kernel
         defaulting to a long erase (full overwrite, many hours).
      3. Unload back to slot.
      4. Reset catalog entry + backup records.

    A stop flag (_stop_format) is checked between tapes so the user can
    cancel the queue without killing a tape that is mid-erase.
    """
    global _stop_format
    _stop_format = False

    queue = list(tapes)
    done: List[Dict] = []
    failed: List[Dict] = []

    set_format_state(
        running=True, status="preparing",
        queue=[{**t} for t in queue],
        current=None, done=[], failed=[],
        started_at=now_ts(), finished_at=None,
        last_message=f"Format queued for {len(queue)} tape(s)…",
        log=[], error=None,
    )
    append_format_log(f"Format job started — {len(queue)} tape(s) queued.")
    publish_state_to_mqtt(refresh_state())

    for i, tape in enumerate(queue):
        if _stop_format:
            append_format_log("Format stopped by user.")
            break

        slot = int(tape.get("slot") or 0)
        vol  = str(tape.get("volume_tag") or f"SLOT{slot}").strip()

        set_format_state(
            current=tape,
            status="formatting",
            last_message=f"[{i+1}/{len(queue)}] Formatting {vol} (slot {slot})…",
        )
        append_format_log(f"[{i+1}/{len(queue)}] Starting format of {vol} (slot {slot})…")
        publish_state_to_mqtt(refresh_state())

        loaded_this_tape = False
        try:
            if catalog_only:
                # ── Catalog-only / soft format ───────────────────────────────
                # No hardware interaction at all.  Just reset the DB records so
                # the tape appears blank and available to the library.  The tape
                # still has its old physical data but nothing will read it unless
                # someone does a raw restore — for re-purposing this is fine.
                append_format_log(f"  {vol}: catalog-only reset (no hardware erase).")
            else:
                # ── Hardware short-erase path ────────────────────────────────
                cur_drive = refresh_state().get("drive", {})
                drive_vol = cur_drive.get("volume_tag", "")
                drive_slot = cur_drive.get("loaded_from_slot") or _last_known_loaded_slot
                drive_empty = cur_drive.get("empty", True)

                if not drive_empty:
                    if drive_vol == vol or drive_slot == slot:
                        append_format_log(f"  {vol} already in drive — skipping load.")
                        loaded_this_tape = False
                    else:
                        append_format_log(f"  Unloading {drive_vol or '?'} (slot {drive_slot}) first…")
                        run_cmd(["mtx", "-f", CHANGER, "unload", str(drive_slot), "0"],
                                timeout=max(COMMAND_TIMEOUT, 120))
                        _save_last_known_loaded_slot(None)
                        time.sleep(2)
                        append_format_log(f"  Loading {vol} from slot {slot}…")
                        run_cmd(["mtx", "-f", CHANGER, "load", str(slot), "0"],
                                timeout=max(COMMAND_TIMEOUT, 120))
                        _save_last_known_loaded_slot(slot)
                        loaded_this_tape = True
                        time.sleep(3)
                else:
                    append_format_log(f"  Loading {vol} from slot {slot}…")
                    run_cmd(["mtx", "-f", CHANGER, "load", str(slot), "0"],
                            timeout=max(COMMAND_TIMEOUT, 120))
                    _save_last_known_loaded_slot(slot)
                    loaded_this_tape = True
                    time.sleep(3)

                # ── Rewind before erase ──────────────────────────────────────
                append_format_log(f"  Rewinding {vol}…")
                run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

                # ── Short erase ──────────────────────────────────────────────
                # Always pass `-s` to force a short/quick erase.
                # Without -s, some mt-st versions default to a long erase
                # (full block overwrite) which takes hours on LTO and was the
                # cause of the UI freeze.  -s writes a new BOT marker + EOD
                # and returns in seconds.
                append_format_log(f"  Erasing {vol}… (short erase -s, ~5–30 s)")
                set_format_state(last_message=f"[{i+1}/{len(queue)}] Erasing {vol}…")
                publish_state_to_mqtt(refresh_state())
                run_cmd(["mt", "-f", TAPE, "erase", "-s"], timeout=300)
                append_format_log(f"  Erase complete for {vol}.")

                # ── Rewind after erase ───────────────────────────────────────
                run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))

                # ── Unload back to slot ──────────────────────────────────────
                if loaded_this_tape:
                    append_format_log(f"  Returning {vol} to slot {slot}…")
                    run_cmd(["mtx", "-f", CHANGER, "unload", str(slot), "0"],
                            timeout=max(COMMAND_TIMEOUT, 120))
                    _save_last_known_loaded_slot(None)

            # ── Clear catalog entry ──────────────────────────────────────────
            # Reset the file index, space usage, and backup records for this tape.
            # Preserve physical metadata (slot, LTO generation, capacity).
            with tape_catalog_conn() as conn:
                conn.execute("""
                    UPDATE tape_catalog SET
                        written_at = NULL,
                        file_count = 0,
                        files_json = '[]',
                        used_bytes = 0,
                        remaining_bytes = NULL,
                        remaining_pct = NULL,
                        space_estimated = 1,
                        backup_dirnames = '[]',
                        archived_at = NULL,
                        purpose = 'available',
                        present = 1,
                        last_seen_slot = ?,
                        last_seen_at = ?,
                        updated_at = ?
                    WHERE volume_tag = ? AND is_deleted = 0
                """, (slot, now_ts(), now_ts(), vol))
                conn.commit()

            # Remove backup records for this tape so space calculations are clean
            with _backup_records_lock:
                before = len(_backup_records)
                _backup_records[:] = [r for r in _backup_records if r.get("volume_tag") != vol]
                removed = before - len(_backup_records)
            _save_backup_records()
            if removed:
                append_format_log(f"  Cleared {removed} backup record(s) for {vol}.")

            done.append(tape)
            append_format_log(f"✓ {vol} formatted and marked available.")
            log_action("format", True, f"{vol} (slot {slot}) formatted and cleared.")
            set_format_state(done=[{**t} for t in done], failed=[{**t} for t in failed])
            publish_state_to_mqtt(refresh_state())

        except Exception as fmt_err:
            append_format_log(f"✗ Failed to format {vol}: {fmt_err}")
            log_action("format", False, f"{vol}: {fmt_err}")
            failed.append({**tape, "error": str(fmt_err)})
            # Best-effort unload on error (only relevant for hardware path)
            if loaded_this_tape and not catalog_only:
                try:
                    run_cmd(["mtx", "-f", CHANGER, "unload", str(slot), "0"],
                            timeout=max(COMMAND_TIMEOUT, 120))
                    _save_last_known_loaded_slot(None)
                except Exception:
                    pass
            set_format_state(done=[{**t} for t in done], failed=[{**t} for t in failed])
            publish_state_to_mqtt(refresh_state())

    final_status = "completed" if not failed else ("completed_with_errors" if done else "failed")
    final_msg = (
        f"Format complete — {len(done)} succeeded"
        + (f", {len(failed)} failed" if failed else "")
        + ("." if not _stop_format else " (stopped by user).")
    )
    set_format_state(
        running=False, status=final_status,
        current=None,
        done=[{**t} for t in done],
        failed=[{**t} for t in failed],
        finished_at=now_ts(),
        last_message=final_msg,
    )
    append_format_log(final_msg)
    _stop_format = False
    notify_format_complete(
        [t["volume_tag"] for t in done],
        [t["volume_tag"] for t in failed],
    )
    refresh_state()
    publish_state_to_mqtt(refresh_state())
    """Audit and repair the tape catalog after a full inventory scan.

    Checks performed:
      1. Orphaned entries — volume in catalog but not seen in any live slot.
         → Mark present=0, log warning. Do NOT delete; tape may be off-site.
      2. Broken file lists — file_count > 0 but files_json is empty or invalid.
         → Reset file_count=0 so the tape shows up for re-indexing next scan.
      3. Entries with used_bytes > 0 but file_count = 0 — data written but
         no TOC. Cross-reference backup_records to repair used_bytes if possible.
      4. Corrupt files_json — not valid JSON or not a list.
         → Reset to '[]' and file_count=0.
      5. Duplicate volume_tag entries (shouldn't happen with UNIQUE constraint
         but handle legacy data) — keep the one with most files.
    """
    live_vol_set = {str(s.get("volume_tag") or "").strip()
                    for s in live_slots
                    if s.get("volume_tag") and not is_cleaning_volume_tag(str(s.get("volume_tag") or ""))}

    all_indexes = list_all_known_indexes()
    fixes = 0

    for idx in all_indexes:
        vol = str(idx.get("volume_tag") or "").strip()
        if not vol:
            continue

        changed: Dict[str, Any] = {}

        # 1. Orphaned — not seen in any live slot
        if idx.get("present") and vol not in live_vol_set and not is_cleaning_volume_tag(vol):
            changed["present"] = False
            append_inventory_log(f"Audit: {vol} not found in any slot — marking not-present.")

        # 2 & 4. Validate files_json
        raw_files_json = None
        try:
            with tape_catalog_conn() as conn:
                row = conn.execute(
                    "SELECT files_json, file_count FROM tape_catalog WHERE volume_tag=? AND is_deleted=0",
                    (vol,)
                ).fetchone()
            if row:
                raw_files_json = row["files_json"]
                stored_count   = int(row["file_count"] or 0)
        except Exception:
            pass

        if raw_files_json is not None:
            try:
                parsed = json.loads(raw_files_json or "[]")
                if not isinstance(parsed, list):
                    raise ValueError("not a list")
                actual_count = len(parsed)
                # Fix file_count mismatch
                if actual_count != stored_count and actual_count > 0:
                    changed["file_count"] = actual_count
                    append_inventory_log(
                        f"Audit: {vol} file_count mismatch ({stored_count} stored, "
                        f"{actual_count} actual) — correcting."
                    )
                elif actual_count == 0 and stored_count > 0:
                    # files_json is an empty list but file_count claims non-zero
                    changed["file_count"] = 0
                    append_inventory_log(
                        f"Audit: {vol} claims {stored_count} files but files_json is empty — "
                        f"resetting file_count so tape re-indexes on next scan."
                    )
            except (json.JSONDecodeError, ValueError):
                # Corrupt JSON
                changed["files_json"] = "[]"
                changed["file_count"] = 0
                append_inventory_log(f"Audit: {vol} has corrupt files_json — reset to empty.")

        # 3. used_bytes > 0 but file_count = 0 — try to recover used_bytes from backup_records
        file_count_final = changed.get("file_count", idx.get("file_count") or 0)
        if file_count_final == 0 and (idx.get("used_bytes") or 0) == 0:
            # Check backup_records for this volume
            bw_from_records = bytes_written_for_volume(vol)
            if bw_from_records > 0:
                changed["used_bytes"] = bw_from_records
                changed["space_estimated"] = 0
                append_inventory_log(
                    f"Audit: {vol} had no used_bytes — recovered "
                    f"{bytes_human(bw_from_records)} from backup records."
                )

        if changed:
            fixes += 1
            try:
                with tape_catalog_conn() as conn:
                    for col, val in changed.items():
                        if col == "present":
                            conn.execute(
                                "UPDATE tape_catalog SET present=?, updated_at=? WHERE volume_tag=? AND is_deleted=0",
                                (1 if val else 0, now_ts(), vol)
                            )
                        elif col == "file_count":
                            conn.execute(
                                "UPDATE tape_catalog SET file_count=?, updated_at=? WHERE volume_tag=? AND is_deleted=0",
                                (int(val), now_ts(), vol)
                            )
                        elif col == "files_json":
                            conn.execute(
                                "UPDATE tape_catalog SET files_json=?, updated_at=? WHERE volume_tag=? AND is_deleted=0",
                                (str(val), now_ts(), vol)
                            )
                        elif col == "used_bytes":
                            conn.execute(
                                "UPDATE tape_catalog SET used_bytes=?, space_estimated=?, updated_at=? WHERE volume_tag=? AND is_deleted=0",
                                (int(val), changed.get("space_estimated", 1), now_ts(), vol)
                            )
                    conn.commit()
            except Exception as e:
                append_inventory_log(f"Audit: failed to apply fix for {vol}: {e}")

    summary = f"Catalog audit complete: {len(all_indexes)} entries checked, {fixes} fixed."
    append_inventory_log(summary)
    log_action("inventory_audit", True, summary)


# ---------------------------------------------------------------------------
# Inventory worker
# ---------------------------------------------------------------------------

def inventory_worker(mode: str = "full"):
    """
    Inventory worker — two modes:

    QUICK
      1. Run `mtx inventory` so the picker arm physically moves and reads every
         barcode (you'll hear it).
      2. Re-read `mtx status` to get the authoritative post-scan slot map.
      3. Mark *every* catalog entry as not-present (wipe stale locations).
      4. Walk the fresh slot map and mark each confirmed tape present with its
         current slot number.  The tape currently in the drive (if any) is also
         re-confirmed.
      Cleaning tapes are included — their location matters too.
      No tape is loaded/unloaded; the drive is never touched.

    FULL
      Everything quick does, PLUS for each data tape not yet fully indexed:
        • Unload any tape currently in the drive (returned to its correct slot).
        • Load the target tape.
        • Rewind and read the full file list (TOC).
        • Capture live space metrics while the tape is loaded.
        • Unload back to the tape's slot.
      At the end: run the DB audit/repair pass and restore any tape that was
      in the drive before the scan started.
    """
    global _inventory_stop_requested
    loaded_slot: Optional[int] = None   # slot of tape *we* loaded during full scan
    orig_slot:   Optional[int] = None   # slot of tape that was in drive before we started
    scanned_count = 0

    def _safe_unload(slot: Optional[int]) -> None:
        """Best-effort unload; never raises."""
        nonlocal loaded_slot
        if slot is None:
            return
        try:
            run_cmd(["mtx", "-f", CHANGER, "unload", str(slot), "0"],
                    timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(None)
            loaded_slot = None
        except Exception as _ue:
            append_inventory_log(f"Warning: could not unload slot {slot}: {_ue}")

    def _safe_restore_orig() -> None:
        """Re-load the tape that was in the drive before the scan, if any."""
        if orig_slot is None:
            return
        try:
            cur = refresh_state().get("drive", {})
            if not cur.get("empty", True):
                # Something is already in the drive — don't clobber it
                append_inventory_log(
                    f"Drive not empty — skipping restore of original slot {orig_slot}.")
                return
            append_inventory_log(f"Restoring original tape from slot {orig_slot}…")
            run_cmd(["mtx", "-f", CHANGER, "load", str(orig_slot), "0"],
                    timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(orig_slot)
        except Exception as _re:
            append_inventory_log(f"Warning: could not restore slot {orig_slot}: {_re}")

    try:
        _inventory_stop_requested = False
        request_inventory_resume()

        # ── Snapshot hardware state before we do anything ────────────────────
        state      = refresh_state()
        drive_info = state.get("drive", {})
        orig_slot  = drive_info.get("loaded_from_slot") or _last_known_loaded_slot

        # All storage slots (including empty ones — we need to know about gaps)
        all_storage_slots = [s for s in state.get("slots", [])
                              if not s.get("is_import_export")]
        full_slots_initial = [s for s in all_storage_slots if s.get("full")]

        if mode == "quick":
            start_msg = (f"Starting quick scan — will move picker arm and scan all barcodes. "
                         f"Drive {'has tape in slot ' + str(orig_slot) if orig_slot else 'is empty'}.")
        else:
            known_indexed = {i["volume_tag"] for i in list_all_known_indexes()
                             if (i.get("file_count") or 0) > 0}
            # Full scan targets: slots with tapes not yet fully indexed
            full_scan_targets = [s for s in full_slots_initial
                                 if not s.get("volume_tag") or
                                 s["volume_tag"] not in known_indexed]
            # Also scan any tape currently in the drive that isn't indexed yet
            drive_vol = drive_info.get("volume_tag", "")
            if (drive_vol and not is_cleaning_volume_tag(drive_vol)
                    and drive_vol not in known_indexed):
                append_inventory_log(f"Drive tape {drive_vol} also needs indexing — will index in-place.")
            start_msg = (f"Starting full scan — {len(full_scan_targets)} tape(s) to index "
                         f"(skipping {len(full_slots_initial) - len(full_scan_targets)} already-indexed).")

        inv_started_at = now_ts()
        # For quick we don't know the final count until after mtx inventory runs;
        # use the initial full count as a placeholder.
        placeholder_count = len(full_slots_initial)
        set_inventory_state(
            running=True, status="scanning", mode=mode, paused=False,
            total_slots=placeholder_count, scanned=0,
            started_at=inv_started_at, finished_at=None, eta_seconds=None,
            current_slot=None, last_message=start_msg, log=[],
        )
        append_inventory_log(start_msg)
        publish_state_to_mqtt(refresh_state())

        # ════════════════════════════════════════════════════════════════════
        # STEP 1 (both modes): hardware barcode scan via `mtx inventory`
        # This physically moves the picker arm along every slot and updates
        # the library's own barcode table.  Without this, `mtx status` only
        # returns whatever the library last cached — which may be stale.
        # ════════════════════════════════════════════════════════════════════
        append_inventory_log("Moving picker arm — scanning all slot barcodes (mtx inventory)…")
        set_inventory_state(status="scanning", last_message="Hardware barcode scan in progress…")
        publish_state_to_mqtt(refresh_state())

        try:
            run_cmd(["mtx", "-f", CHANGER, "inventory"], timeout=max(COMMAND_TIMEOUT, 600))
            append_inventory_log("Picker arm barcode scan complete.")
        except Exception as inv_err:
            append_inventory_log(
                f"Warning: 'mtx inventory' returned an error ({inv_err}). "
                f"Continuing with mtx status re-read — slot data may be slightly stale.")

        # ── Re-read hardware state after the physical scan ───────────────────
        fresh_state = refresh_state()
        fresh_slots = fresh_state.get("slots", [])
        fresh_drive = fresh_state.get("drive", {})

        fresh_storage = [s for s in fresh_slots if not s.get("is_import_export")]
        full_slots_fresh  = [s for s in fresh_storage if s.get("full")]
        empty_slots_fresh = [s for s in fresh_storage if not s.get("full")]

        append_inventory_log(
            f"Post-scan slot state: {len(full_slots_fresh)} occupied, "
            f"{len(empty_slots_fresh)} empty "
            f"(drive: {'tape ' + (fresh_drive.get('volume_tag') or '?') if not fresh_drive.get('empty') else 'empty'})."
        )

        # ════════════════════════════════════════════════════════════════════
        # STEP 2 (both modes): rebuild catalog from authoritative hardware state
        #
        # Mark EVERYTHING not-present first (single write), then re-confirm
        # each tape we can actually see — slots AND drive.
        # ════════════════════════════════════════════════════════════════════
        append_inventory_log("Resetting presence flags in catalog…")
        mark_all_indexes_not_present()

        # Update total_slots now that we have accurate fresh data
        set_inventory_state(total_slots=len(full_slots_fresh), scanned=0)

        # Re-confirm tape currently in drive (if any) — it is NOT in any slot
        # so it would stay marked absent if we only walk the slot list.
        drive_vol_fresh = fresh_drive.get("volume_tag", "") if not fresh_drive.get("empty") else ""
        drive_slot_fresh = fresh_drive.get("loaded_from_slot") or orig_slot
        if drive_vol_fresh:
            is_cln = is_cleaning_volume_tag(drive_vol_fresh)
            drive_meta = {
                "present": True,
                "last_seen_at": now_ts(),
                # Keep last_seen_slot as wherever it came from — it's in the drive now
                "last_seen_slot": drive_slot_fresh,
                "purpose": "cleaning" if is_cln else "data",
                "is_cleaning": is_cln,
            }
            try:
                update_tape_index_metadata(drive_vol_fresh, **drive_meta)
                append_inventory_log(
                    f"Drive tape {drive_vol_fresh} (from slot {drive_slot_fresh}) re-confirmed present.")
            except Exception as _de:
                append_inventory_log(f"Warning: could not update drive tape {drive_vol_fresh}: {_de}")

        # Walk every slot confirmed occupied by hardware scan
        to_confirm = list(full_slots_fresh)
        scanned_count = 0
        for i, slot in enumerate(to_confirm):
            inventory_wait_if_paused()
            if inventory_should_stop():
                raise TapeError("Inventory stopped by user.")

            sn  = slot["slot"]
            vol = slot.get("volume_tag") or f"SLOT{sn}"
            is_cln = is_cleaning_volume_tag(vol)

            eta_now = calc_eta_seconds(inv_started_at, i, len(to_confirm))
            set_inventory_state(
                current_slot=sn, status="scanning", eta_seconds=eta_now,
                last_message=f"[{i+1}/{len(to_confirm)}] Confirming slot {sn}: {vol}",
            )
            publish_state_to_mqtt(refresh_state())

            slot_meta = {
                "present": True,
                "last_seen_at": now_ts(),
                "last_seen_slot": sn,
                "magazine": slot.get("magazine"),
                "slot_in_magazine": slot.get("slot_in_magazine"),
                "purpose": "cleaning" if is_cln else "data",
                "is_cleaning": is_cln,
            }
            # Carry over any existing space metrics we already know
            slot_meta.update(space_meta_from_info(
                build_tape_space_info(vol, idx=load_tape_index(vol) or {}, loaded=False)
            ))
            try:
                update_tape_index_metadata(vol, **slot_meta)
                append_inventory_log(
                    f"[{i+1}/{len(to_confirm)}] Slot {sn}: {vol} ({'cleaning' if is_cln else 'data'}) — confirmed.")
            except Exception as _ce:
                append_inventory_log(
                    f"[{i+1}/{len(to_confirm)}] Slot {sn}: error updating {vol}: {_ce}")
                log_action("inventory", False, f"Slot {sn}: {_ce}")

            scanned_count += 1
            eta_now = calc_eta_seconds(inv_started_at, scanned_count, len(to_confirm))
            set_inventory_state(scanned=scanned_count, current_slot=None, eta_seconds=eta_now)
            publish_state_to_mqtt(refresh_state())

        # ════════════════════════════════════════════════════════════════════
        # STEP 3 (full mode only): load each un-indexed tape and read its TOC
        # ════════════════════════════════════════════════════════════════════
        if mode == "full":
            # Rebuild target list from the now-accurate fresh slot data
            known_indexed = {i["volume_tag"] for i in list_all_known_indexes()
                             if (i.get("file_count") or 0) > 0}
            full_scan_targets = [s for s in full_slots_fresh
                                 if not s.get("volume_tag") or
                                 s["volume_tag"] not in known_indexed]

            # Also handle tape already in drive that needs indexing
            if (drive_vol_fresh and not is_cleaning_volume_tag(drive_vol_fresh)
                    and drive_vol_fresh not in known_indexed):
                # Synthesise a fake "slot" entry for the drive tape
                _drive_fake_slot = {
                    "slot": drive_slot_fresh or 0,
                    "volume_tag": drive_vol_fresh,
                    "full": True,
                    "is_import_export": False,
                    "magazine": None,
                    "slot_in_magazine": None,
                    "_in_drive": True,   # flag so we skip the load step
                }
                full_scan_targets.insert(0, _drive_fake_slot)

            toc_total = len(full_scan_targets)
            toc_done  = 0
            append_inventory_log(
                f"Full scan: {toc_total} tape(s) need TOC indexing.")
            set_inventory_state(total_slots=toc_total, scanned=0)
            publish_state_to_mqtt(refresh_state())

            for i, slot in enumerate(full_scan_targets):
                inventory_wait_if_paused()
                if inventory_should_stop():
                    raise TapeError("Inventory stopped by user.")

                sn  = slot["slot"]
                vol = slot.get("volume_tag") or f"SLOT{sn}"
                already_in_drive = slot.get("_in_drive", False)
                is_cln = is_cleaning_volume_tag(vol)

                base_meta = {
                    "present": True,
                    "last_seen_at": now_ts(),
                    "last_seen_slot": sn if not already_in_drive else drive_slot_fresh,
                    "magazine": slot.get("magazine"),
                    "slot_in_magazine": slot.get("slot_in_magazine"),
                    "purpose": "cleaning" if is_cln else "data",
                    "is_cleaning": is_cln,
                }

                eta_now = calc_eta_seconds(inv_started_at, i, toc_total)
                set_inventory_state(
                    current_slot=sn, status="scanning", eta_seconds=eta_now,
                    last_message=f"[{i+1}/{toc_total}] Indexing {vol} (slot {sn})",
                )
                publish_state_to_mqtt(refresh_state())

                try:
                    if is_cln:
                        update_tape_index_metadata(vol, **base_meta)
                        append_inventory_log(
                            f"[{i+1}/{toc_total}] Slot {sn}: {vol} is a cleaning tape — skipping TOC.")
                    else:
                        # ── Load tape if not already in drive ────────────────
                        if not already_in_drive:
                            cur_drive = refresh_state().get("drive", {})
                            existing_slot = cur_drive.get("loaded_from_slot") or _last_known_loaded_slot
                            if not cur_drive.get("empty", True) and existing_slot:
                                append_inventory_log(
                                    f"[{i+1}/{toc_total}] Unloading current tape (slot {existing_slot})…")
                                _safe_unload(existing_slot)
                                time.sleep(2)

                            inventory_wait_if_paused()
                            if inventory_should_stop():
                                raise TapeError("Inventory stopped by user.")

                            append_inventory_log(
                                f"[{i+1}/{toc_total}] Loading slot {sn} ({vol})…")
                            run_cmd(["mtx", "-f", CHANGER, "load", str(sn), "0"],
                                    timeout=max(COMMAND_TIMEOUT, 120))
                            _save_last_known_loaded_slot(sn)
                            loaded_slot = sn
                            time.sleep(3)
                        else:
                            append_inventory_log(
                                f"[{i+1}/{toc_total}] {vol} already in drive — indexing in-place.")
                            loaded_slot = sn  # so _safe_unload knows what to do later

                        inventory_wait_if_paused()
                        if inventory_should_stop():
                            raise TapeError("Inventory stopped by user.")

                        # ── Read TOC ─────────────────────────────────────────
                        append_inventory_log(
                            f"[{i+1}/{toc_total}] Reading TOC for {vol}…")
                        try:
                            files = read_tape_index_live()
                        except TapeError as _idx_err:
                            if "__blank_or_foreign__" in str(_idx_err):
                                append_inventory_log(
                                    f"[{i+1}/{toc_total}] Slot {sn} ({vol}): blank or non-tar tape — skipping index.")
                                # Still mark it present at its slot
                                update_tape_index_metadata(vol, **base_meta)
                                if not already_in_drive:
                                    _safe_unload(sn)
                                toc_done += 1
                                set_inventory_state(scanned=toc_done, current_slot=None)
                                publish_state_to_mqtt(refresh_state())
                                continue
                            raise

                        # ── Capture live space info while tape is in drive ───
                        live_space = build_loaded_tape_space_info()
                        base_meta.update(space_meta_from_info(live_space))
                        save_tape_index(vol, files, now_ts(), meta=base_meta)
                        append_inventory_log(
                            f"[{i+1}/{toc_total}] {vol}: {len(files):,} files indexed, "
                            f"space={bytes_human(live_space.get('used_bytes') or 0)} used.")

                        # ── Unload (only if we loaded it; leave drive tapes alone) ──
                        if not already_in_drive:
                            _safe_unload(sn)
                        else:
                            # Drive tape was indexed in-place; don't eject it
                            loaded_slot = None

                except Exception as _slot_err:
                    append_inventory_log(f"[{i+1}/{toc_total}] Error on slot {sn} ({vol}): {_slot_err}")
                    log_action("inventory", False, f"Slot {sn}: {_slot_err}")
                    if loaded_slot and not already_in_drive:
                        _safe_unload(loaded_slot)
                    if inventory_should_stop() and "stopped by user" in str(_slot_err).lower():
                        raise

                toc_done += 1
                eta_now = calc_eta_seconds(inv_started_at, toc_done, toc_total)
                set_inventory_state(scanned=toc_done, current_slot=None, eta_seconds=eta_now)
                publish_state_to_mqtt(refresh_state())

            scanned_count = toc_done

        # ════════════════════════════════════════════════════════════════════
        # STEP 3: mark previously-known tapes that are no longer visible as archived
        #
        # After the hardware barcode scan and slot confirmation, any tape in the
        # catalog that is still present=0 was not seen by the scanner.  Two cases:
        #   a) Tape is in the drive — already handled above (re-confirmed).
        #   b) Tape has been physically removed from the library — mark archived.
        # We only archive tapes that were previously present (had a last_seen_slot).
        # Tapes that were never seen (just added to catalog manually) are left alone.
        # ════════════════════════════════════════════════════════════════════
        if not inventory_should_stop():
            all_catalog = list_all_known_indexes()
            seen_vols = ({s.get("volume_tag") for s in full_slots_fresh if s.get("volume_tag")}
                         | ({drive_vol_fresh} if drive_vol_fresh else set()))
            archived_count = 0
            returned_count = 0
            for idx in all_catalog:
                vol_c = idx.get("volume_tag", "")
                if not vol_c or is_cleaning_volume_tag(vol_c):
                    continue
                was_present = idx.get("present", False)  # state BEFORE this scan (not updated yet by our writes)
                now_present = vol_c in seen_vols
                prev_purpose = str(idx.get("purpose") or "")

                if now_present and prev_purpose == "archived":
                    # Tape has come back — log it
                    returned_count += 1
                    append_inventory_log(
                        f"✓ {vol_c} has returned to the library "
                        f"(was archived since {_fmt_ts_short(idx.get('archived_at'))}).")
                    log_action("inventory", True, f"{vol_c} returned from archived state.")

                elif not now_present and idx.get("last_seen_slot") and prev_purpose not in ("archived",):
                    # Tape was previously known in a slot but is now gone — archive it
                    mark_tape_archived(vol_c)
                    archived_count += 1
                    append_inventory_log(
                        f"⚠ {vol_c} not found in any slot (was in slot "
                        f"{idx.get('last_seen_slot')}) — marked as archived. "
                        f"Catalog and file index preserved.")
                    log_action("inventory", True,
                               f"{vol_c} marked archived (not seen in slot {idx.get('last_seen_slot')}).")

            if archived_count:
                append_inventory_log(
                    f"Inventory: {archived_count} tape(s) marked archived (removed from library).")
            if returned_count:
                append_inventory_log(
                    f"Inventory: {returned_count} tape(s) returned from archived state.")

        # ════════════════════════════════════════════════════════════════════
        # STEP 4: catalog audit/repair (both modes — quick catches moved tapes)
        # ════════════════════════════════════════════════════════════════════
        if not inventory_should_stop():
            append_inventory_log("Running catalog audit and repair…")
            set_inventory_state(status="auditing", last_message="Auditing catalog…")
            publish_state_to_mqtt(refresh_state())
            try:
                _inventory_db_audit(full_slots_fresh)
            except Exception as _ae:
                append_inventory_log(f"Catalog audit error: {_ae}")
                log_action("inventory_audit", False, str(_ae))

        # ── Restore original tape ────────────────────────────────────────────
        if mode == "full":
            _safe_restore_orig()

        final_status = "stopped" if inventory_should_stop() else "completed"
        drive_after = refresh_state().get("drive", {})
        drive_tape_after = drive_after.get("volume_tag", "") if not drive_after.get("empty") else ""
        final_msg = (
            f"Inventory {final_status}. "
            f"{scanned_count} tape(s) processed. "
            f"Drive: {'tape ' + drive_tape_after if drive_tape_after else 'empty'}."
        )
        set_inventory_state(
            running=False, status=final_status, paused=False, finished_at=now_ts(),
            current_slot=None, eta_seconds=0, last_message=final_msg,
        )
        append_inventory_log(final_msg)
        log_action("inventory", True, final_msg)
        notify_inventory_done(scanned_count, 0, 0)

    except Exception as e:
        # Make sure any tape we loaded gets returned before giving up
        if loaded_slot is not None:
            _safe_unload(loaded_slot)
        if mode == "full":
            _safe_restore_orig()

        final_status = "stopped" if "stopped by user" in str(e).lower() else "failed"
        msg = ("Inventory stopped by user." if final_status == "stopped"
               else f"Inventory failed: {e}")
        set_inventory_state(
            running=False, status=final_status, paused=False, finished_at=now_ts(),
            current_slot=None, eta_seconds=0, last_message=msg,
        )
        append_inventory_log(msg)
        if final_status == "failed":
            log_action("inventory", False, str(e))
        else:
            log_action("inventory", True, msg)

    finally:
        _inventory_stop_requested = False
        request_inventory_resume()
        publish_state_to_mqtt(refresh_state())

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

def _load_schedules():
    global _schedules
    data = _db_get_json("schedules", None)
    if isinstance(data, list):
        _schedules = data
        return
    os.makedirs(os.path.dirname(SCHEDULES_FILE), exist_ok=True)
    if not os.path.exists(SCHEDULES_FILE):
        _schedules = []
        return
    try:
        with open(SCHEDULES_FILE) as f:
            _schedules = json.load(f)
        _db_set_json("schedules", _schedules)
    except Exception:
        _schedules = []


def _save_schedules():
    with _schedules_lock:
        payload = json.loads(json.dumps(_schedules))
    _db_set_json("schedules", payload)

def _next_run_ts(s):
    mode, hour, minute = s.get("mode","weekly"), int(s.get("hour",2)), int(s.get("minute",0))
    now = datetime.datetime.now()
    base = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if mode == "daily":
        c = base if base > now else base + datetime.timedelta(days=1)
    elif mode == "weekly":
        dow = int(s.get("day_of_week",0))
        da = (dow - now.weekday()) % 7
        c = base + datetime.timedelta(days=da)
        if c <= now: c += datetime.timedelta(weeks=1)
    elif mode == "monthly":
        dom = int(s.get("day_of_month",1))
        try: c = base.replace(day=dom)
        except ValueError: c = base.replace(day=28)
        if c <= now:
            dim = calendar.monthrange(now.year, now.month)[1]
            c += datetime.timedelta(days=dim)
    else:
        return None
    return int(c.timestamp())

def _update_next_run(s): s["next_run"] = _next_run_ts(s)

def scheduler_loop():
    _last_log_prune = 0
    while True:
        time.sleep(30)
        now = now_ts()

        # Prune app_log once per hour instead of on every write
        if now - _last_log_prune > 3600:
            _prune_app_log()
            _last_log_prune = now

        with _schedules_lock: sched_copy = list(_schedules)
        for s in sched_copy:
            if not s.get("enabled", True): continue
            nr = s.get("next_run")
            if nr and now >= nr:
                paths, label = s.get("paths",[]), s.get("label","?")
                with _backup_lock: busy = _backup_job.get("running")
                if busy:
                    log_action("scheduler", False, f"'{label}' skipped — backup running.")
                else:
                    log_action("scheduler", True, f"'{label}' fired.")
                    threading.Thread(
                        target=backup_worker,
                        args=(paths,),
                        kwargs={"backup_mode": s.get("backup_mode", "full"), "label": label},
                        daemon=True,
                    ).start()
                with _schedules_lock:
                    for x in _schedules:
                        if x.get("id") == s.get("id"):
                            _update_next_run(x); x["last_run"] = now
                _save_schedules()

# ---------------------------------------------------------------------------
# Backup tape auto-selection and auto-unload helpers
# ---------------------------------------------------------------------------

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
    with _drive_history_lock:
        drive_hist_snap = dict(_drive_history)

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
    global _tar_proc, _stop_requested
    selected = [ensure_under_backup_root(p) for p in paths]
    rels     = [os.path.relpath(p, "/") for p in selected]
    total_size = sum(estimate_path_size(p) for p in selected)
    start    = time.time()
    _stop_requested = False
    vol      = (_state_cache.get("summary") or {}).get("loaded_volume", "")
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
        drive_state = (_state_cache.get("drive") or {})
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
                vol = (_state_cache.get("summary") or {}).get("loaded_volume", "") or chosen["volume_tag"]
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
        _tar_proc = tar_proc

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

                if _stop_requested and not cancel_requested:
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
            _tar_proc = None

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
                    drive_snap = _state_cache.get("drive", {})
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
                with _verify_lock:
                    verify_errors = _verify_job.get("errors", 0)
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
        _tar_proc = None
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

def _ha_notify(title: str, message: str) -> None:
    """Send a notification via Home Assistant's notify service REST API.

    Silently does nothing if HA URL/token are unset or HA notifications are
    disabled.  Uses only the stdlib so no extra dependencies are required.
    """
    import urllib.request, urllib.error
    cfg = get_ha_config()
    if not cfg["enabled"] or not cfg["url"] or not cfg["token"]:
        return
    service = cfg["service"] or "notify"
    url = f"{cfg['url']}/api/services/notify/{service}"
    payload = json.dumps({"title": f"[TL2000] {title}", "message": message}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {cfg['token']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        log_action("ha_notify", True, f"Sent: {title}")
    except urllib.error.HTTPError as e:
        body = e.read(200).decode(errors="ignore")
        log_action("ha_notify", False, f"HTTP {e.code} sending '{title}': {body}")
    except Exception as e:
        log_action("ha_notify", False, f"Failed to send '{title}': {e}")


def notify_backup_success(vol: str, paths: List[str], bw: int, elapsed: float,
                           verified: bool, verify_errors: int) -> None:
    if not get_notify_config()["on_backup_success"]:
        return
    ver_str = ("Yes — " + str(verify_errors) + " errors") if verified else "No"
    status = "✅ COMPLETED" + (" + ✅ VERIFIED" if verified and verify_errors == 0
                               else f" + ⚠️ VERIFY ERRORS ({verify_errors})" if verified else "")
    tokens = dict(
        vol=vol, paths=", ".join(paths),
        written=bytes_human(bw), duration=secs_human(int(elapsed)),
        speed=bytes_human(bw / max(elapsed, 1)),
        verified=ver_str, errors=str(verify_errors), error="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_success_title", **tokens)
    body  = _render_notify_template("backup_success_body",  **tokens)
    _ha_notify(title, body)


def notify_backup_failure(vol: str, paths: List[str], error: str) -> None:
    if not get_notify_config()["on_backup_failure"]:
        return
    tokens = dict(
        vol=vol, paths=", ".join(paths), error=error,
        written="", duration="", speed="", verified="", errors="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_failure_title", **tokens)
    body  = _render_notify_template("backup_failure_body",  **tokens)
    _ha_notify(title, body)


def notify_verify_failure(vol: str, errors: int, detail: str) -> None:
    if not get_notify_config()["on_verify_failure"]:
        return
    tokens = dict(
        vol=vol, errors=str(errors), error=detail[:300],
        paths="", written="", duration="", speed="", verified="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("verify_failure_title", **tokens)
    body  = _render_notify_template("verify_failure_body",  **tokens)
    _ha_notify(title, body)


def notify_format_complete(vols: List[str], failed: List[str]) -> None:
    if not get_notify_config()["on_format_complete"]:
        return
    vol = ", ".join(vols) if vols else "(none)"
    err = ", ".join(failed) if failed else ""
    status = "✅ Format complete" if not failed else "⚠️ Format completed with errors"
    title = f"[TL2000] {status}"
    body  = f"{status}\nFormatted: {vol}" + (f"\nFailed: {err}" if err else "")
    _ha_notify(title, body)


def notify_inventory_done(total: int, added: int, changed: int) -> None:
    if not get_notify_config()["on_inventory_done"]:
        return
    title = "[TL2000] Inventory complete"
    body  = f"✅ Inventory done — {total} tapes, {added} added, {changed} changed"
    _ha_notify(title, body)



# ---------------------------------------------------------------------------
# Backup records (persistent job history)
# ---------------------------------------------------------------------------

def _load_backup_records() -> None:
    global _backup_records
    data = _db_get_json("backup_records", None)
    if isinstance(data, list):
        _backup_records = data
        return
    os.makedirs(os.path.dirname(BACKUP_RECORDS_FILE), exist_ok=True)
    if not os.path.exists(BACKUP_RECORDS_FILE):
        _backup_records = []
        return
    try:
        with open(BACKUP_RECORDS_FILE) as f:
            _backup_records = json.load(f)
        _db_set_json("backup_records", _backup_records[-500:])
    except Exception:
        _backup_records = []


def _save_backup_records() -> None:
    with _backup_records_lock:
        payload = list(_backup_records[-500:])
    _db_set_json("backup_records", payload)


def add_backup_record(rec: Dict[str, Any]) -> None:
    with _backup_records_lock:
        _backup_records.insert(0, rec)
    _save_backup_records()


def get_backup_records(vol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with _backup_records_lock:
        recs = list(_backup_records)
    if vol:
        recs = [r for r in recs if r.get("volume_tag") == vol]
    return recs[:limit]


# ---------------------------------------------------------------------------
# Tape health (sg3_utils)
# ---------------------------------------------------------------------------

def get_tape_health() -> Dict[str, Any]:
    """
    Pull LTO drive error counters via sg_logs.
    Returns empty dict if SG_DEVICE is not set or sg_logs is not installed.
    """
    if not SG_DEVICE:
        return {}
    result: Dict[str, Any] = {"device": SG_DEVICE, "error": None, "pages": {}}
    try:
        # Page 0x03 = Read error counters, 0x05 = Write error counters
        # 0x0c = Sequential-access device, 0x2e = TapeAlert
        pages = {
            "write_errors": ("0x02", ["Errors corrected without delay",
                                      "Total (e.g. rewrites or rereads)",
                                      "Total errors corrected",
                                      "Total times correction applied",
                                      "Total bytes processed",
                                      "Total uncorrected errors"]),
            "read_errors":  ("0x03", ["Errors corrected without delay",
                                      "Total (e.g. rewrites or rereads)",
                                      "Total errors corrected",
                                      "Total times correction applied",
                                      "Total bytes processed",
                                      "Total uncorrected errors"]),
            "tape_alert":   ("0x2e", []),
        }
        for page_name, (page_code, _) in pages.items():
            try:
                out = subprocess.run(
                    ["sg_logs", "--page=" + page_code, SG_DEVICE],
                    capture_output=True, text=True, timeout=10
                ).stdout
                result["pages"][page_name] = out.strip()
            except Exception as e:
                result["pages"][page_name] = f"(error: {e})"

        # Also grab load count from sequential-access page 0x0c
        try:
            out = subprocess.run(
                ["sg_logs", "--page=0x0c", SG_DEVICE],
                capture_output=True, text=True, timeout=10
            ).stdout
            result["pages"]["sequential_access"] = out.strip()
            # Parse load count
            lc_m = re.search(r"Cleaning action required\s*[=:]\s*(\d+)", out, re.I)
            cln_m = re.search(r"Cleaning required\s*[=:]\s*(\d+)", out, re.I)
            result["cleaning_required"] = bool(cln_m and int(cln_m.group(1)))
        except Exception:
            pass

        # Parse uncorrected errors for quick summary
        for label, page in [("write_uncorrected", "write_errors"),
                             ("read_uncorrected",  "read_errors")]:
            page_text = result["pages"].get(page, "")
            m = re.search(r"Total uncorrected errors\s*[=:]\s*(\d+)", page_text, re.I)
            result[label] = int(m.group(1)) if m else None

    except Exception as e:
        result["error"] = str(e)
    return result


# ---------------------------------------------------------------------------
# GFS retention
# ---------------------------------------------------------------------------

def gfs_classify(record: Dict[str, Any]) -> str:
    """Classify a backup record for display purposes.

    Uses calendar windows (month / ISO week) rather than weekday checks so
    the classification is stable regardless of which day backups run.
    """
    ts = record.get("started_at")
    if not ts:
        return "expired"
    dt = datetime.datetime.fromtimestamp(ts)

    # Check against the keep sets from gfs_get_recyclable
    recyclable_set = set(gfs_get_recyclable())
    vol = record.get("volume_tag", "")

    if vol and vol not in recyclable_set and record.get("status") == "completed":
        # Determine which bucket is keeping it
        with _backup_records_lock:
            records = sorted(_backup_records, key=lambda r: r.get("started_at", 0))
        completed = [r for r in records if r.get("status") == "completed" and r.get("started_at") and r.get("volume_tag")]

        monthly_rep: Dict[str, str] = {}
        weekly_rep:  Dict[str, str] = {}
        for rec in completed:
            dt2 = datetime.datetime.fromtimestamp(rec["started_at"])
            ym = dt2.strftime("%Y-%m")
            if ym not in monthly_rep:
                monthly_rep[ym] = rec["volume_tag"]
            iso_year, iso_week, _ = dt2.isocalendar()
            yw = f"{iso_year}-{iso_week:02d}"
            if yw not in weekly_rep:
                weekly_rep[yw] = rec["volume_tag"]

        keep_monthly = set(list(monthly_rep.values())[-GFS_MONTHLY_KEEP:])
        keep_weekly  = set(list(weekly_rep.values()) [-GFS_WEEKLY_KEEP:])

        if vol in keep_monthly:
            return "monthly"
        if vol in keep_weekly:
            return "weekly"
        return "daily"

    return "expired" if vol in recyclable_set else "daily"


def gfs_get_recyclable() -> List[str]:
    """Apply GFS retention and return volume_tags safe to reuse.

    Keeps:
      - The oldest completed backup in each of the last GFS_MONTHLY_KEEP calendar months.
      - The oldest completed backup in each of the last GFS_WEEKLY_KEEP ISO weeks
        (that aren't already kept as a monthly).
      - The most recent GFS_DAILY_KEEP completed backups (that aren't already kept).

    Everything older than the above windows, and not in a keep set, is recyclable.
    """
    with _backup_records_lock:
        records = sorted(_backup_records, key=lambda r: r.get("started_at", 0))

    completed = [r for r in records if r.get("status") == "completed" and r.get("started_at") and r.get("volume_tag")]

    # Group by year-month and ISO year-week, keeping oldest per window
    monthly_rep: Dict[str, str] = {}   # "YYYY-MM"   → volume_tag of oldest record
    weekly_rep:  Dict[str, str] = {}   # "YYYY-WW"   → volume_tag of oldest record

    for rec in completed:
        dt = datetime.datetime.fromtimestamp(rec["started_at"])
        vol = rec["volume_tag"]

        ym_key = dt.strftime("%Y-%m")
        if ym_key not in monthly_rep:
            monthly_rep[ym_key] = vol

        iso_year, iso_week, _ = dt.isocalendar()
        yw_key = f"{iso_year}-{iso_week:02d}"
        if yw_key not in weekly_rep:
            weekly_rep[yw_key] = vol

    # Trim to the configured keep counts (most recent N windows)
    keep_monthly: set = set(list(monthly_rep.values())[-GFS_MONTHLY_KEEP:])
    keep_weekly:  set = set(list(weekly_rep.values()) [-GFS_WEEKLY_KEEP:])

    # Daily: the most recent N completed backups overall
    recent_vols = [r["volume_tag"] for r in reversed(completed)]
    keep_daily: set = set(recent_vols[:GFS_DAILY_KEEP])

    keep_all = keep_monthly | keep_weekly | keep_daily

    # Any volume not in keep_all whose most recent completed backup is outside
    # all keep windows is recyclable.  We de-duplicate and preserve insertion order.
    seen: set = set()
    recyclable: List[str] = []
    for rec in completed:
        vol = rec["volume_tag"]
        if vol not in keep_all and vol not in seen:
            seen.add(vol)
            recyclable.append(vol)

    return recyclable


# ---------------------------------------------------------------------------
# Incremental backup support
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

def set_verify_state(**kw):
    with _verify_lock: _verify_job.update(kw)

def append_verify_log(msg):
    with _verify_lock:
        _verify_job["log"].insert(0, {"ts": now_ts(), "message": msg})
        del _verify_job["log"][100:]
        _verify_job["last_message"] = msg

def snapshot_verify_job():
    return snap(_verify_job, _verify_lock)


def verify_worker(vol: str, backup_record_id: Optional[str] = None) -> None:
    """
    Read back the tape and verify integrity.

    Strategy:
      1. Issue a rewind and then poll `mt status` until the drive reports it is
         sitting at file 0, block 0 (BOT).  The status flags string varies by
         driver; we parse the numeric file/block fields instead of looking for a
         "BOT" keyword that not all drivers emit.
      2. Run  dd if=TAPE bs=BLOCK [count=N] | tar -t -f -  as a pure archive-
         readability check.  Both stdout and stderr of each process are drained
         concurrently to prevent pipe-buffer deadlocks.
      3. "Unexpected EOF in archive" is NOT treated as an error when sampling
         (VERIFY_SAMPLE_MB > 0) because dd deliberately truncates the stream
         mid-archive — tar hitting EOF there is expected and correct.
      4. At verbose log level the full stderr from both dd and tar is written to
         the verify log so it is trivial to diagnose any genuine failure.
    """
    set_verify_state(
        running=True, status="preparing", volume_tag=vol,
        started_at=now_ts(), finished_at=None,
        bytes_verified=0, errors=0, eta_seconds=None,
        last_message="Starting verification…", log=[], error=None,
    )
    verbose = backup_log_allows("verbose")
    sampling = VERIFY_SAMPLE_MB > 0
    append_verify_log(
        f"Verification started for {vol}  "
        f"(block={TAPE_BLOCK_BYTES//1024}KiB, "
        f"sample={'full tape' if not sampling else str(VERIFY_SAMPLE_MB)+'MB'}, "
        f"log={'verbose' if verbose else 'normal'})."
    )
    publish_state_to_mqtt(refresh_state())

    errors = 0
    bytes_verified = 0

    def _at_bot(mt_text: str) -> bool:
        """Return True if mt status indicates the tape is at file 0, block 0 (BOT).

        Different kernel drivers report this differently:
          - Some emit a "BOT" flag in the general status bits line.
          - Linux st driver always prints "file number=N, block number=M" —
            file 0, block 0 means we are at the very beginning.
        We check both forms so we don't need to know which driver is in use.
        """
        t = mt_text.lower()
        # Explicit BOT keyword
        if "bot" in t or "beginning of tape" in t:
            return True
        # Parse "file number=N, block number=M"
        fm = re.search(r"file number\s*=\s*(\d+)", t)
        bm = re.search(r"block number\s*=\s*(\d+)", t)
        if fm and bm:
            return int(fm.group(1)) == 0 and int(bm.group(1)) == 0
        return False

    try:
        # ── Step 1: rewind then wait for BOT ────────────────────────────────
        # Issue the rewind immediately — don't wait first.  The old code waited
        # up to 5 minutes hoping the drive would self-report BOT, but LTO-6
        # reports "file number=1, block number=0" after a write (positioned at
        # the end of the last file mark), which never matches "BOT".  Just
        # rewind and then confirm we are at file 0 block 0.
        append_verify_log("Rewinding tape before verification…")
        set_verify_state(status="rewinding")
        publish_state_to_mqtt(refresh_state())

        try:
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
            append_verify_log("Rewind command issued — waiting for BOT…")
        except Exception as _rw_err:
            append_verify_log(f"Warning: rewind returned an error: {_rw_err} — continuing.")

        # Poll mt status until file=0, block=0 (BOT confirmed) or timeout
        _bot_deadline = time.time() + 120   # 2 minutes max after rewind
        _bot_confirmed = False
        while time.time() < _bot_deadline:
            try:
                _mt_out = subprocess.run(
                    ["mt", "-f", TAPE, "status"],
                    capture_output=True, text=True, timeout=15,
                )
                _mt_text = _mt_out.stdout + _mt_out.stderr
                if verbose:
                    append_verify_log(f"mt status: {_mt_text.strip()[:300]}")
                if _at_bot(_mt_text):
                    _bot_confirmed = True
                    append_verify_log("Drive confirmed at BOT (file 0, block 0).")
                    break
            except Exception as _me:
                append_verify_log(f"mt status check error: {_me}")
            time.sleep(3)

        if not _bot_confirmed:
            append_verify_log(
                "Warning: could not confirm BOT within 2 min after rewind. "
                "Proceeding anyway — verify may read from wrong position if drive is still seeking.")

        # Brief settle — some drives need a moment after reaching BOT
        time.sleep(2)

        # ── Step 2: dd | tar -t ─────────────────────────────────────────────
        set_verify_state(status="reading_data")
        limit_bytes = VERIFY_SAMPLE_MB * 1024 * 1024 if sampling else None
        append_verify_log(
            f"Starting read-back: "
            f"{'full tape' if not limit_bytes else bytes_human(limit_bytes)} "
            f"via dd (bs={TAPE_BLOCK_BYTES//1024}KiB) | tar -t"
        )
        if sampling:
            append_verify_log(
                f"NOTE: Sampling mode — dd will stop after {VERIFY_SAMPLE_MB} MB. "
                f"'Unexpected EOF' at the sample boundary is expected and not an error."
            )
        publish_state_to_mqtt(refresh_state())

        dd_cmd = ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"]
        if limit_bytes:
            block_count = max(1, (limit_bytes + TAPE_BLOCK_BYTES - 1) // TAPE_BLOCK_BYTES)
            dd_cmd += [f"count={block_count}"]
        append_verify_log(f"dd command: {' '.join(dd_cmd)}")

        dd_proc  = subprocess.Popen(dd_cmd,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tar_proc = subprocess.Popen(
            ["tar", "-t", "-f", "-"],
            stdin=dd_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        dd_proc.stdout.close()   # let tar own the read end

        verify_started = now_ts()
        tar_files_seen   = 0
        _dd_stderr_lines: List[str] = []
        _tar_stderr_lines: List[str] = []

        def _drain_tar_stdout():
            nonlocal tar_files_seen
            try:
                for _ in tar_proc.stdout:
                    tar_files_seen += 1
            except Exception:
                pass

        def _drain_tar_stderr():
            try:
                for raw in tar_proc.stderr:
                    _tar_stderr_lines.append(raw.decode(errors="ignore").rstrip())
            except Exception:
                pass

        def _drain_dd_stderr():
            try:
                for raw in dd_proc.stderr:
                    _dd_stderr_lines.append(raw.decode(errors="ignore").rstrip())
            except Exception:
                pass

        t_tar_out = threading.Thread(target=_drain_tar_stdout, daemon=True)
        t_tar_err = threading.Thread(target=_drain_tar_stderr, daemon=True)
        t_dd_err  = threading.Thread(target=_drain_dd_stderr,  daemon=True)
        t_tar_out.start(); t_tar_err.start(); t_dd_err.start()

        while tar_proc.poll() is None:
            time.sleep(3)
            _bv = 0
            for _line in reversed(_dd_stderr_lines):
                _m = re.search(r'^(\d+)\s+bytes', _line)
                if _m:
                    _bv = int(_m.group(1))
                    break
            if _bv:
                bytes_verified = _bv
            eta_v = calc_eta_seconds(verify_started, bytes_verified, limit_bytes) if limit_bytes else None
            set_verify_state(
                bytes_verified=bytes_verified,
                last_message=f"Verified {tar_files_seen:,} entries, {bytes_human(bytes_verified)} read…",
                eta_seconds=eta_v,
            )
            publish_state_to_mqtt(refresh_state())

        t_tar_out.join(timeout=15)
        t_tar_err.join(timeout=15)
        # Close tar's pipes now that we've drained them
        try:
            tar_proc.stdout.close()
        except Exception:
            pass
        try:
            tar_proc.stderr.close()
        except Exception:
            pass
        tar_rc = tar_proc.wait(timeout=30)
        dd_proc.wait(timeout=60)
        t_dd_err.join(timeout=15)
        # Explicitly close dd's stderr pipe so the kernel releases the fd
        # immediately.  Without this, the pipe fd can linger just long enough
        # for the subsequent `mt rewind` call to see /dev/nst0 as busy.
        try:
            dd_proc.stderr.close()
        except Exception:
            pass

        # Brief settle — give the st driver a moment to fully release the
        # device after dd exits before we issue the rewind.
        time.sleep(1)

        # Final byte count from dd stderr
        for _line in reversed(_dd_stderr_lines):
            _m = re.search(r'^(\d+)\s+bytes', _line)
            if _m:
                bytes_verified = int(_m.group(1))
                break

        dd_rc = dd_proc.returncode

        # ── Log diagnostics (always show process results; full stderr if verbose or error) ──
        append_verify_log(
            f"Process results: tar rc={tar_rc}, dd rc={dd_rc}, "
            f"files seen={tar_files_seen:,}, bytes read={bytes_human(bytes_verified)}"
        )
        if verbose or tar_rc not in (0, 1):
            for _l in (_tar_stderr_lines[:50] if _tar_stderr_lines else ["(empty)"]):
                append_verify_log(f"  tar stderr: {_l[:300]}")
        if verbose or dd_rc not in (0,):
            for _l in (_dd_stderr_lines[-10:] if _dd_stderr_lines else ["(empty)"]):
                append_verify_log(f"  dd stderr: {_l[:300]}")

        # ── Evaluate result ─────────────────────────────────────────────────
        # Key rule: "Unexpected EOF in archive" when sampling is NOT an error.
        # dd stopped feeding data at the count= limit mid-archive; tar seeing
        # EOF there is the designed behaviour, not a tape defect.
        read_errors = 0
        if tar_rc not in (0, 1):
            tar_err_text = " ".join(_tar_stderr_lines).lower()
            unexpected_eof = "unexpected eof" in tar_err_text or "eof in archive" in tar_err_text

            if sampling and unexpected_eof and dd_rc == 0:
                # dd finished cleanly at its count= limit; EOF is expected
                append_verify_log(
                    f"ℹ tar rc={tar_rc} with 'Unexpected EOF' — this is normal when sampling "
                    f"({bytes_human(limit_bytes)} limit reached mid-archive). Not counted as error."
                )
            else:
                read_errors += 1
                errors += 1
                _tar_summary = "; ".join(_tar_stderr_lines[:5]) or "(no stderr)"
                _dd_errors   = "; ".join(
                    l for l in _dd_stderr_lines
                    if "error" in l.lower() or "failed" in l.lower()
                )[:200]
                append_verify_log(
                    f"✗ tar exited rc={tar_rc} after {tar_files_seen:,} entries "
                    f"({bytes_human(bytes_verified)} from dd). "
                    f"tar: {_tar_summary[:200]}"
                    + (f"  dd: {_dd_errors}" if _dd_errors else "")
                )
        elif _tar_stderr_lines and (verbose or tar_rc == 1):
            for _l in _tar_stderr_lines[:10]:
                append_verify_log(f"ℹ tar warning: {_l[:200]}")

        if dd_rc not in (0,) and not limit_bytes:
            append_verify_log(
                f"ℹ dd exited rc={dd_rc} on unlimited read "
                f"(may simply mean end-of-tape reached)")

        if read_errors == 0:
            append_verify_log(
                f"✓ Archive integrity OK — {tar_files_seen:,} entries readable, "
                f"{bytes_human(bytes_verified)} verified."
            )
        else:
            append_verify_log(
                f"✗ Integrity check failed: {read_errors} error(s). "
                f"Entries read before failure: {tar_files_seen:,}. "
                f"Bytes from tape: {bytes_human(bytes_verified)}."
            )

        # ── Update backup record ────────────────────────────────────────────
        if backup_record_id:
            with _backup_records_lock:
                for rec in _backup_records:
                    if rec.get("id") == backup_record_id:
                        rec["verified"]      = errors == 0
                        rec["verify_errors"] = errors
                        rec["verified_at"]   = now_ts()
                        rec["verify_bytes"]  = bytes_verified
            _save_backup_records()

        status = "completed" if errors == 0 else "completed_with_errors"
        set_verify_state(
            running=False, status=status, finished_at=now_ts(),
            errors=errors, bytes_verified=bytes_verified, eta_seconds=0,
            last_message=f"Verification done: {errors} error(s), {bytes_human(bytes_verified)} checked.",
            error=None if errors == 0 else f"{errors} integrity error(s) found.",
        )
        prefix = "✓" if errors == 0 else "✗"
        append_verify_log(f"{prefix} Verification complete — {errors} total error(s).")
        log_action("verify", errors == 0,
                   f"{vol}: {errors} errors, {bytes_human(bytes_verified)} read")

        if errors > 0:
            notify_verify_failure(vol, errors,
                f"{read_errors} read/parse error(s) after {tar_files_seen:,} entries "
                f"({bytes_human(bytes_verified)} checked)")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        set_verify_state(running=False, status="failed", finished_at=now_ts(), eta_seconds=0,
                         error=str(e), last_message=f"Verify failed: {e}")
        append_verify_log(f"Verification failed with exception: {e}")
        if verbose:
            for _tbl in tb.splitlines()[-10:]:
                append_verify_log(f"  {_tbl}")
        log_action("verify", False, str(e))
        notify_verify_failure(vol, -1, str(e))
    finally:
        publish_state_to_mqtt(refresh_state())


# Health data cache (refreshed on a slower cadence — sg_logs is slow)
_health_cache: Dict[str, Any] = {}
_health_cache_lock = threading.Lock()
_health_last_refresh: int = 0
HEALTH_REFRESH_INTERVAL = int(os.getenv("HEALTH_REFRESH_INTERVAL", "300"))  # 5 min default


def _refresh_health_cache() -> None:
    global _health_last_refresh
    if not SG_DEVICE:
        return
    h = get_tape_health()
    with _health_cache_lock:
        _health_cache.clear()
        _health_cache.update(h)
    _health_last_refresh = now_ts()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
_mqtt_client = None
_mqtt_connected = False

def mqtt_available(): return bool(MQTT_HOST and mqtt is not None)
def mqtt_topic(*parts): return "/".join([MQTT_BASE.strip("/")] + [p.strip("/") for p in parts])
def ha_topic(comp, oid, suffix="config"): return f"{HA_DISCOVERY_PREFIX}/{comp}/odin_tl2000/{oid}/{suffix}"

def mqtt_publish(topic, payload, retain=True):
    if not mqtt_available() or not _mqtt_client or not _mqtt_connected: return
    if not isinstance(payload, str): payload = json.dumps(payload)
    _mqtt_client.publish(topic, payload, retain=retain)

def publish_discovery():  # noqa: C901
    base = {
        "device": DEVICE_INFO,
        "availability_topic": mqtt_topic("availability"),
        "payload_available": "online",
        "payload_not_available": "offline",
    }

    def sensor(oid, name, st, icon, sc=None, vt=None, unit=None, expire=None, cat=None, dc=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st, "icon": icon}
        if sc:   p["state_class"]        = sc
        if vt:   p["value_template"]     = vt
        if unit: p["unit_of_measurement"] = unit
        if expire: p["expire_after"]     = expire
        if cat:  p["entity_category"]    = cat
        if dc:   p["device_class"]       = dc
        mqtt_publish(ha_topic("sensor", oid), p)

    def binary(oid, name, st, dc=None, cat=None, icon=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "payload_on": "ON", "payload_off": "OFF"}
        if dc:   p["device_class"]    = dc
        if cat:  p["entity_category"] = cat
        if icon: p["icon"]            = icon
        mqtt_publish(ha_topic("binary_sensor", oid), p)

    def button(oid, name, ct, pp, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "command_topic": ct,
             "payload_press": pp, "icon": icon}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("button", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def select_entity(oid, name, st, ct, options, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "options": options, "icon": icon}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("select", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def number_entity(oid, name, st, ct, min_v, max_v, step, unit, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "min": min_v, "max": max_v, "step": step,
             "unit_of_measurement": unit, "icon": icon, "mode": "box"}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("number", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def text_entity(oid, name, st, ct, icon, cat=None, pattern=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "icon": icon}
        if cat:     p["entity_category"] = cat
        if pattern: p["pattern"]         = pattern
        mqtt_publish(ha_topic("text", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    # ── Library / drive sensors ──────────────────────────────────────────────
    sensor("full_slots",    "Full Slots",    mqtt_topic("state","full_slots"),
           "mdi:archive",    "measurement", "{{ value | int }}", "tapes")
    sensor("empty_slots",   "Empty Slots",   mqtt_topic("state","empty_slots"),
           "mdi:archive-off","measurement", "{{ value | int }}", "tapes")
    sensor("total_slots",   "Total Slots",   mqtt_topic("state","total_slots"),
           "mdi:archive-plus","measurement","{{ value | int }}", "tapes", cat="diagnostic")
    sensor("loaded_slot",   "Loaded Slot",   mqtt_topic("state","loaded_slot"),
           "mdi:numeric",    "measurement", "{{ value | int(0) }}", "")
    sensor("loaded_volume", "Loaded Volume", mqtt_topic("state","loaded_volume"),
           "mdi:tape-drive")
    sensor("density",       "Tape Density",  mqtt_topic("state","density"),
           "mdi:database",   cat="diagnostic")
    if HAS_MAIL_SLOT:
        sensor("import_export_tag","Mail Slot Tag",mqtt_topic("state","import_export_tag"),
               "mdi:mailbox")
    sensor("cleaning_tag",  "Cleaning Tape", mqtt_topic("state","cleaning_tag"),
           "mdi:broom",      cat="diagnostic")
    sensor("last_action",   "Last Action",   mqtt_topic("state","last_action"),
           "mdi:history",    cat="diagnostic")
    sensor("time_in_drive_mins","Time In Drive",mqtt_topic("state","time_in_drive_mins"),
           "mdi:timer",      "measurement", "{{ value | int(0) }}", "min")
    sensor("tape_load_count","Tape Load Count",mqtt_topic("state","tape_load_count"),
           "mdi:counter",    "total_increasing","{{ value | int(0) }}", "",cat="diagnostic")
    sensor("tape_total_written","Tape Total Written",mqtt_topic("state","tape_total_written"),
           "mdi:archive",    "total_increasing","{{ value | float(0) }}", "B",
           dc="data_size", cat="diagnostic")
    sensor("tape_total_written_hr","Tape Total Written (readable)",mqtt_topic("state","tape_total_written_hr"),
           "mdi:archive",    cat="diagnostic")

    # ── Backup sensors ───────────────────────────────────────────────────────
    sensor("backup_status",      "Backup Status",    mqtt_topic("backup","status"),
           "mdi:backup-restore")
    sensor("backup_percent",     "Backup Progress",  mqtt_topic("backup","percent"),
           "mdi:percent",         "measurement","{{ value | float | round(1) }}","%")
    # data_rate device class: HA auto-converts B/s → KB/s → MB/s → GB/s in the UI
    sensor("backup_speed_bps",   "Backup Speed",     mqtt_topic("backup","speed_bps"),
           "mdi:speedometer",     "measurement","{{ value | float(0) }}", "B/s",
           dc="data_rate")
    # data_size device class: HA auto-converts B → KB → MB → GB → TB in the UI
    sensor("backup_bytes_written","Backup Written",  mqtt_topic("backup","bytes_written"),
           "mdi:counter",         "total_increasing","{{ value | float(0) }}", "B",
           dc="data_size")
    sensor("backup_bytes_total", "Backup Total Size",mqtt_topic("backup","bytes_total"),
           "mdi:database",        "measurement","{{ value | float(0) }}", "B",
           dc="data_size")
    # Human-readable companion topics for dashboards that just want a string
    sensor("backup_written_hr",  "Backup Written (readable)", mqtt_topic("backup","bytes_written_hr"),
           "mdi:counter",         cat="diagnostic")
    sensor("backup_total_hr",    "Backup Total (readable)",   mqtt_topic("backup","bytes_total_hr"),
           "mdi:database",        cat="diagnostic")
    sensor("backup_speed_hr",    "Backup Speed (readable)",   mqtt_topic("backup","speed_hr"),
           "mdi:speedometer",     cat="diagnostic")
    sensor("backup_eta_secs",    "Backup ETA",       mqtt_topic("backup","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int }}", "s")
    sensor("backup_last_msg",    "Backup Message",   mqtt_topic("backup","last_message"),
           "mdi:text-box-outline",expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("backup_last_vol",    "Last Backup Volume",mqtt_topic("backup","last_volume"),
           "mdi:tape-drive")
    sensor("backup_last_ok_ts",  "Last Successful Backup",mqtt_topic("backup","last_ok_ts"),
           "mdi:check-circle",    "measurement","{{ value | int }}", "")
    sensor("backup_last_written","Last Backup Size", mqtt_topic("backup","last_written_hr"),
           "mdi:archive",         cat="diagnostic")
    sensor("backup_mode",        "Backup Mode",      mqtt_topic("backup","mode"),
           "mdi:layers",          cat="diagnostic")

    # ── Verification sensors ─────────────────────────────────────────────────
    sensor("verify_status",      "Verify Status",    mqtt_topic("verify","status"),
           "mdi:shield-check")
    sensor("verify_errors",      "Verify Errors",    mqtt_topic("verify","errors"),
           "mdi:alert-circle",    "measurement","{{ value | int }}", "")
    sensor("verify_bytes",       "Verify Bytes Read",mqtt_topic("verify","bytes_verified"),
           "mdi:eye-check",       "measurement","{{ value | float(0) }}", "B",
           dc="data_size", cat="diagnostic")
    sensor("verify_bytes_hr",    "Verify Read (readable)", mqtt_topic("verify","bytes_verified_hr"),
           "mdi:eye-check",       cat="diagnostic")
    sensor("verify_last_msg",    "Verify Message",   mqtt_topic("verify","last_message"),
           "mdi:text",            expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("verify_eta_secs",    "Verify ETA",       mqtt_topic("verify","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int }}", "s", cat="diagnostic")

    # ── Restore sensors ──────────────────────────────────────────────────────
    sensor("restore_status",     "Restore Status",   mqtt_topic("restore","status"),
           "mdi:restore")
    sensor("restore_last_msg",   "Restore Message",  mqtt_topic("restore","last_message"),
           "mdi:text",            expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("restore_dest",       "Restore Destination",mqtt_topic("restore","dest"),
           "mdi:folder-arrow-down",cat="diagnostic")

    # ── Inventory sensors ────────────────────────────────────────────────────
    sensor("inventory_status",   "Inventory Status", mqtt_topic("inventory","status"),
           "mdi:magnify")
    sensor("inventory_mode",     "Inventory Mode",   mqtt_topic("inventory","mode"),
           "mdi:lightning-bolt", cat="diagnostic")
    sensor("inventory_progress", "Inventory Progress",mqtt_topic("inventory","progress"),
           "mdi:progress-check",  "measurement","{{ value | int(0) }}", "%")
    sensor("inventory_scanned",  "Tapes Scanned",    mqtt_topic("inventory","scanned"),
           "mdi:check-all",       "measurement","{{ value | int(0) }}", "tapes",cat="diagnostic")
    sensor("inventory_eta_secs", "Inventory ETA",   mqtt_topic("inventory","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int(0) }}", "s", cat="diagnostic")

    # ── Tape health sensors ──────────────────────────────────────────────────
    sensor("health_write_errors","Write Uncorrected Errors",mqtt_topic("health","write_uncorrected"),
           "mdi:pencil-off",      "measurement","{{ value | int(-1) }}", "")
    sensor("health_read_errors", "Read Uncorrected Errors", mqtt_topic("health","read_uncorrected"),
           "mdi:eye-off",         "measurement","{{ value | int(-1) }}", "")

    # ── GFS / retention sensors ──────────────────────────────────────────────
    sensor("gfs_recyclable",     "Recyclable Tapes", mqtt_topic("gfs","recyclable_count"),
           "mdi:recycle",         "measurement","{{ value | int(0) }}", "tapes")
    sensor("last_backup_record_status","Last Job Status",mqtt_topic("backup","last_record_status"),
           "mdi:clipboard-check")

    # ── Binary sensors ───────────────────────────────────────────────────────
    binary("drive_online",       "Drive Reachable",  mqtt_topic("state","drive_online"),
           dc="connectivity")
    binary("tape_loaded",        "Tape Loaded",      mqtt_topic("state","tape_loaded"),
           icon="mdi:tape-drive")
    binary("at_bot",             "Tape At BOT",      mqtt_topic("state","at_bot"),
           icon="mdi:rewind", cat="diagnostic")
    binary("backup_running",     "Backup Running",   mqtt_topic("backup","running"),
           dc="running")
    binary("restore_running",    "Restore Running",  mqtt_topic("restore","running"),
           dc="running")
    binary("inventory_running",  "Inventory Running",mqtt_topic("inventory","running"),
           dc="running")
    binary("inventory_paused",   "Inventory Paused", mqtt_topic("inventory","paused"),
           icon="mdi:pause-circle")
    binary("verify_running",     "Verify Running",   mqtt_topic("verify","running"),
           dc="running")
    binary("cleaning_needed",    "Cleaning Needed",  mqtt_topic("health","cleaning_needed"),
           dc="problem", icon="mdi:broom")
    binary("backup_healthy",     "Backup System OK", mqtt_topic("state","backup_healthy"),
           icon="mdi:shield-check")
    binary("last_verify_passed", "Last Verify Passed",mqtt_topic("verify","last_passed"),
           icon="mdi:shield-check")

    # ── Buttons ──────────────────────────────────────────────────────────────
    button("cmd_rewind",     "Rewind Tape",      mqtt_topic("cmd","rewind"),   "rewind","mdi:rewind")
    button("cmd_unload",     "Unload Tape",      mqtt_topic("cmd","unload"),   "unload","mdi:eject")
    button("cmd_refresh",    "Refresh Status",   mqtt_topic("cmd","refresh"),  "refresh","mdi:refresh", cat="diagnostic")
    button("cmd_stop_backup","Stop Backup",      mqtt_topic("cmd","stop_backup"),"stop","mdi:stop")
    button("cmd_read_index", "Read Tape Index",  mqtt_topic("cmd","read_index"),"read","mdi:format-list-bulleted")
    button("cmd_inventory",       "Run Inventory",       mqtt_topic("cmd","inventory"),       "scan","mdi:magnify-scan")
    button("cmd_inventory_quick", "Run Quick Scan",      mqtt_topic("cmd","inventory_quick"), "scan","mdi:barcode-scan")
    button("cmd_inventory_pause", "Pause Inventory",     mqtt_topic("cmd","inventory_pause"), "pause","mdi:pause")
    button("cmd_inventory_resume","Resume Inventory",    mqtt_topic("cmd","inventory_resume"),"play","mdi:play")
    button("cmd_inventory_stop",  "Stop Inventory",      mqtt_topic("cmd","inventory_stop"),  "stop","mdi:stop-circle")
    button("cmd_verify",     "Verify Tape",      mqtt_topic("cmd","verify"),    "verify","mdi:shield-check")
    button("cmd_backup_full","Backup (Full)",    mqtt_topic("cmd","backup_full"),"full","mdi:backup-restore")
    button("cmd_backup_incr","Backup (Incr.)",   mqtt_topic("cmd","backup_incr"),"incr","mdi:delta")
    if HAS_MAIL_SLOT:
        button("cmd_eject_mail", "Eject Mail Slot",  mqtt_topic("cmd","eject_mail"),"eject","mdi:email-arrow-right")

    # ── Selects ──────────────────────────────────────────────────────────────
    select_entity("sel_backup_mode",  "Backup Mode",
                  mqtt_topic("backup","mode_select_state"),
                  mqtt_topic("cmd","set_backup_mode"),
                  ["full","incremental","differential"],
                  "mdi:layers", cat="config")

    # ── Number controls ──────────────────────────────────────────────────────
    number_entity("num_verify_sample_mb","Verify Sample Size",
                  mqtt_topic("config","verify_sample_mb"),
                  mqtt_topic("cmd","set_verify_sample_mb"),
                  0, 4096, 128, "MB", "mdi:eye-check", cat="config")
    number_entity("num_load_slot",    "Load Slot Number",
                  mqtt_topic("config","load_slot"),
                  mqtt_topic("cmd","set_load_slot"),
                  1, max(1, (24 if not HAS_MAIL_SLOT else 48)), 1, "", "mdi:numeric", cat="config")

    # ── Text inputs ──────────────────────────────────────────────────────────
    text_entity("txt_backup_label", "Backup Job Label",
                mqtt_topic("config","backup_label"),
                mqtt_topic("cmd","set_backup_label"),
                "mdi:label", cat="config")

    # Subscribe to load and backup_start (parametric, not simple buttons)
    if _mqtt_client and _mqtt_connected:
        _mqtt_client.subscribe(mqtt_topic("cmd","load"))
        _mqtt_client.subscribe(mqtt_topic("cmd","backup_start"))

    # Publish initial state for selects/numbers/texts
    mqtt_publish(mqtt_topic("backup","mode_select_state"), "full")
    mqtt_publish(mqtt_topic("config","verify_sample_mb"),  str(VERIFY_SAMPLE_MB))
    mqtt_publish(mqtt_topic("config","load_slot"),         "1")
    mqtt_publish(mqtt_topic("config","backup_label"),      "")


# Runtime config state (set by HA controls via MQTT)
_mqtt_runtime_cfg = {
    "backup_mode":      "full",
    "load_slot":        1,
    "backup_label":     "",
    "verify_sample_mb": VERIFY_SAMPLE_MB,
}


def publish_state_to_mqtt(state):  # noqa: C901
    if not mqtt_available(): return
    mqtt_publish(mqtt_topic("availability"), "online")

    s     = state.get("summary", {})
    drive = state.get("drive", {})
    bk    = state.get("backup_job", {})
    inv   = state.get("inventory_job", {})
    rst   = state.get("restore_job", {})
    vj    = state.get("verify_job", {})

    drive_reachable = bool(drive.get("online", s.get("online", False)))
    tape_loaded = bool(not drive.get("empty", not s.get("loaded", False)))
    inventory_running = bool(inv.get("running", False))
    verify_running = bool(vj.get("running", False))

    # ── Library / drive ──────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("state","loaded_volume"),    s.get("loaded_volume",""))
    mqtt_publish(mqtt_topic("state","import_export_tag"),s.get("import_export_tag",""))
    mqtt_publish(mqtt_topic("state","cleaning_tag"),     s.get("cleaning_tag",""))
    mqtt_publish(mqtt_topic("state","density"),          s.get("density",""))
    mqtt_publish(mqtt_topic("state","drive_online"),     "ON" if drive_reachable else "OFF")
    mqtt_publish(mqtt_topic("state","tape_loaded"),      "ON" if tape_loaded else "OFF")
    mqtt_publish(mqtt_topic("state","at_bot"),           "ON" if s.get("at_bot")  else "OFF")
    mqtt_publish(mqtt_topic("state","full_slots"),       str(int(s.get("full_slots",   0))))
    mqtt_publish(mqtt_topic("state","empty_slots"),      str(int(s.get("empty_slots",  0))))
    mqtt_publish(mqtt_topic("state","total_slots"),      str(int(s.get("total_slots",  0))))
    ls = s.get("loaded_slot")
    mqtt_publish(mqtt_topic("state","loaded_slot"),      str(int(ls)) if ls else "0")
    mqtt_publish(mqtt_topic("state","last_action"),
                 _action_log[0]["detail"] if _action_log else "idle")

    # Time in drive (minutes, integer)
    tind = None
    if _drive_loaded_at and not (state.get("drive") or {}).get("empty"):
        tind = (now_ts() - _drive_loaded_at) // 60
    mqtt_publish(mqtt_topic("state","time_in_drive_mins"), str(tind) if tind is not None else "0")

    # Load count and total data written for loaded tape
    # FIX: read all values inside the lock so no other thread can mutate _drive_history
    # between the lock release and the reads that follow.
    vol = s.get("loaded_volume","")
    with _drive_history_lock:
        hist             = _drive_history.get(vol, {}) if vol else {}
        _tape_load_count = int(hist.get("load_count", 0))
        _tape_total_bw   = int(hist.get("total_backup_bytes", 0) or 0)
    mqtt_publish(mqtt_topic("state","tape_load_count"),       str(_tape_load_count))
    mqtt_publish(mqtt_topic("state","tape_total_written"),    str(_tape_total_bw))
    mqtt_publish(mqtt_topic("state","tape_total_written_hr"), bytes_human(_tape_total_bw) if _tape_total_bw else "0 B")

    # Overall system health: empty drive is OK; inventory load/unload churn is OK.
    # Only mark unhealthy for actual command/state errors, a failed last backup, or verify errors.
    last_bk_ok = True
    with _backup_records_lock:
        if _backup_records:
            last_bk_ok = _backup_records[0].get("status") == "completed"

    last_verify_passed = True
    if verify_running:
        last_verify_passed = True
    elif vj.get("status", "idle") not in ("idle", "completed"):
        last_verify_passed = not (vj.get("errors", 0) > 0)
    elif vj.get("status") == "completed":
        last_verify_passed = not (vj.get("errors", 0) > 0)

    system_healthy = True
    if state.get("last_error"):
        system_healthy = False
    elif not drive_reachable:
        system_healthy = False
    elif not last_bk_ok:
        system_healthy = False
    elif not last_verify_passed:
        system_healthy = False

    # Inventory activity should not count as a fault by itself.
    if inventory_running:
        system_healthy = system_healthy and drive_reachable

    mqtt_publish(mqtt_topic("state","backup_healthy"),
                 "ON" if system_healthy else "OFF")

    # ── Backup ───────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("backup","running"),        "ON" if bk.get("running") else "OFF")
    mqtt_publish(mqtt_topic("backup","status"),         bk.get("status","idle"))
    mqtt_publish(mqtt_topic("backup","percent"),        f"{float(bk.get('percent',0)):.2f}")
    _bk_speed = float(bk.get("speed_bps", 0) or 0)
    _bk_written = int(bk.get("bytes_written", 0) or 0)
    _bk_total = int(bk.get("bytes_total", 0) or 0)
    mqtt_publish(mqtt_topic("backup","speed_bps"),      f"{_bk_speed:.1f}")
    mqtt_publish(mqtt_topic("backup","bytes_written"),  str(_bk_written))
    mqtt_publish(mqtt_topic("backup","bytes_total"),    str(_bk_total))
    # Human-readable companions
    mqtt_publish(mqtt_topic("backup","speed_hr"),       bytes_human(_bk_speed) + "/s")
    mqtt_publish(mqtt_topic("backup","bytes_written_hr"), bytes_human(_bk_written))
    mqtt_publish(mqtt_topic("backup","bytes_total_hr"), bytes_human(_bk_total))
    eta = bk.get("eta_seconds")
    mqtt_publish(mqtt_topic("backup","eta_seconds"),    str(int(eta)) if eta is not None else "0")
    mqtt_publish(mqtt_topic("backup","last_message"),   bk.get("last_message",""))
    mqtt_publish(mqtt_topic("backup","mode"),           _mqtt_runtime_cfg.get("backup_mode","full"))

    # Last completed backup info (from records)
    with _backup_records_lock:
        recs = list(_backup_records)
    last_ok = next((r for r in recs if r.get("status")=="completed"), None)
    mqtt_publish(mqtt_topic("backup","last_volume"),    last_ok.get("volume_tag","") if last_ok else "")
    mqtt_publish(mqtt_topic("backup","last_ok_ts"),     str(int(last_ok.get("finished_at",0))) if last_ok else "0")
    mqtt_publish(mqtt_topic("backup","last_written_hr"),
                 bytes_human(int(last_ok.get("bytes_written",0))) if last_ok else "0 B")
    last_rec = recs[0] if recs else None
    mqtt_publish(mqtt_topic("backup","last_record_status"), last_rec.get("status","none") if last_rec else "none")

    # ── Verification ─────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("verify","running"),         "ON" if vj.get("running") else "OFF")
    mqtt_publish(mqtt_topic("verify","status"),          vj.get("status","idle"))
    mqtt_publish(mqtt_topic("verify","errors"),          str(int(vj.get("errors",0))))
    _vj_bytes = int(vj.get("bytes_verified", 0) or 0)
    mqtt_publish(mqtt_topic("verify","bytes_verified"),  str(_vj_bytes))
    mqtt_publish(mqtt_topic("verify","bytes_verified_hr"), bytes_human(_vj_bytes))
    v_eta = vj.get("eta_seconds")
    mqtt_publish(mqtt_topic("verify","eta_seconds"),     str(int(v_eta)) if v_eta is not None else "0")
    mqtt_publish(mqtt_topic("verify","last_message"),    vj.get("last_message",""))
    vst = vj.get("status","idle")
    mqtt_publish(mqtt_topic("verify","last_passed"),
                 "ON" if (vst in ("completed",) and vj.get("errors",0)==0) else
                 ("OFF" if vst in ("completed_with_errors","failed") else "OFF"))

    # ── Restore ──────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("restore","running"),        "ON" if rst.get("running") else "OFF")
    mqtt_publish(mqtt_topic("restore","status"),         rst.get("status","idle"))
    mqtt_publish(mqtt_topic("restore","last_message"),   rst.get("last_message",""))
    mqtt_publish(mqtt_topic("restore","dest"),           rst.get("dest",""))

    # ── Inventory ────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("inventory","running"),      "ON" if inv.get("running") else "OFF")
    mqtt_publish(mqtt_topic("inventory","paused"),       "ON" if inv.get("paused") else "OFF")
    mqtt_publish(mqtt_topic("inventory","status"),       inv.get("status","idle"))
    mqtt_publish(mqtt_topic("inventory","mode"),         inv.get("mode","full"))
    total = inv.get("total_slots",0)
    scanned = inv.get("scanned",0)
    pct = int(scanned/total*100) if total>0 else 0
    mqtt_publish(mqtt_topic("inventory","progress"),     str(pct))
    mqtt_publish(mqtt_topic("inventory","scanned"),      str(scanned))
    i_eta = inv.get("eta_seconds")
    mqtt_publish(mqtt_topic("inventory","eta_seconds"),  str(int(i_eta)) if i_eta is not None else "0")

    # ── Tape health ──────────────────────────────────────────────────────────
    # (we cache health data to avoid calling sg_logs on every poll — updated separately)
    with _health_cache_lock:
        hc = dict(_health_cache)
    mqtt_publish(mqtt_topic("health","write_uncorrected"),
                 str(hc.get("write_uncorrected",-1)) if hc.get("write_uncorrected") is not None else "-1")
    mqtt_publish(mqtt_topic("health","read_uncorrected"),
                 str(hc.get("read_uncorrected",-1)) if hc.get("read_uncorrected") is not None else "-1")
    mqtt_publish(mqtt_topic("health","cleaning_needed"), "ON" if hc.get("cleaning_required") else "OFF")

    # ── GFS ──────────────────────────────────────────────────────────────────
    recyclable = gfs_get_recyclable()
    mqtt_publish(mqtt_topic("gfs","recyclable_count"), str(len(recyclable)))

    # ── Full JSON snapshot (for advanced HA templates) ────────────────────────
    mqtt_publish(mqtt_topic("state","raw_json"), state)


def _handle_mqtt_cmd(topic, payload):  # noqa: C901
    global _stop_requested, _tar_proc
    suffix  = topic.split("/")[-1]
    payload = payload.strip()

    # ── Drive controls ───────────────────────────────────────────────────────
    if suffix == "rewind":
        def _do():
            try: run_cmd(["mt","-f",TAPE,"rewind"]); log_action("mqtt_rewind",True,"Rewound via MQTT")
            except Exception as e: log_action("mqtt_rewind",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "unload":
        def _do():
            slot = get_effective_loaded_slot()
            if slot:
                try: run_cmd(["mtx","-f",CHANGER,"unload",str(slot),"0"]); _save_last_known_loaded_slot(None); log_action("mqtt_unload",True,f"Unloaded {slot}")
                except Exception as e: log_action("mqtt_unload",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "load":
        # payload = slot number, or use _mqtt_runtime_cfg["load_slot"]
        try:
            slot = int(payload) if payload.isdigit() else _mqtt_runtime_cfg.get("load_slot", 1)
            def _do(s=slot):
                try:
                    refresh_state()
                    if not (_state_cache.get("drive") or {}).get("empty"):
                        raise TapeError("Drive already has a tape loaded.")
                    run_cmd(["mtx","-f",CHANGER,"load",str(s),"0"])
                    _save_last_known_loaded_slot(s)
                    log_action("mqtt_load",True,f"Loaded slot {s}")
                except Exception as e:
                    log_action("mqtt_load",False,str(e))
                publish_state_to_mqtt(refresh_state())
            threading.Thread(target=_do, daemon=True).start()
        except Exception as e:
            log_action("mqtt_load",False,str(e))

    elif suffix == "eject_mail":
        def _do():
            slot = (_state_cache.get("summary") or {}).get("cleaning_slot") or 1
            try: run_cmd(["mtx","-f",CHANGER,"transfer","0",str(slot)]); log_action("mqtt_eject_mail",True,"Mail slot ejected")
            except Exception as e: log_action("mqtt_eject_mail",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    # ── Backup controls ──────────────────────────────────────────────────────
    elif suffix in ("backup_full", "backup_incr"):
        mode = "full" if suffix == "backup_full" else "incremental"
        with _schedules_lock: scheds = list(_schedules)
        # Use paths from first enabled schedule, or fall back to BACKUP_ROOT
        paths = next((s.get("paths",[]) for s in scheds if s.get("enabled")), [BACKUP_ROOT])
        label = _mqtt_runtime_cfg.get("backup_label","") or f"HA {mode} backup"
        with _backup_lock: busy = _backup_job.get("running")
        if busy:
            log_action("mqtt_backup",False,"Backup already running")
        else:
            log_action("mqtt_backup",True,f"Starting {mode} backup via HA")
            threading.Thread(target=backup_worker, args=(paths,),
                             kwargs={"backup_mode": mode, "label": label},
                             daemon=True).start()

    elif suffix == "backup_start":
        try:
            d = json.loads(payload)
            paths = d.get("paths",[])
            mode  = d.get("mode","full")
            label = d.get("label","") or _mqtt_runtime_cfg.get("backup_label","")
            if paths:
                with _backup_lock:
                    if not _backup_job.get("running"):
                        threading.Thread(target=backup_worker, args=(paths,),
                                        kwargs={"backup_mode": mode, "label": label},
                                        daemon=True).start()
        except Exception as e: log_action("mqtt_backup_start",False,str(e))

    elif suffix == "stop_backup":
        _stop_requested = True
        if _tar_proc:
            try: _tar_proc.send_signal(signal.SIGTERM)
            except Exception: pass
        log_action("mqtt_stop",True,"Stop backup via HA")
        publish_state_to_mqtt(refresh_state())

    # ── Verification ─────────────────────────────────────────────────────────
    elif suffix == "verify":
        vol = (_state_cache.get("summary") or {}).get("loaded_volume","")
        if vol:
            with _verify_lock:
                if not _verify_job.get("running"):
                    threading.Thread(target=verify_worker, args=(vol,), daemon=True).start()
                    log_action("mqtt_verify",True,f"Verify started for {vol}")
        else:
            log_action("mqtt_verify",False,"No tape loaded")

    # ── Index / inventory ─────────────────────────────────────────────────────
    elif suffix == "read_index":
        def _do():
            vol = (_state_cache.get("summary") or {}).get("loaded_volume","")
            try:
                if is_cleaning_volume_tag(vol):
                    update_tape_index_metadata(vol or "unknown", present=True, purpose="cleaning", is_cleaning=True)
                    raise TapeError(f"{vol} is a cleaning tape; index read skipped.")
                run_cmd(["mt","-f",TAPE,"rewind"], timeout=max(COMMAND_TIMEOUT,300))
                fl = read_tape_index_live()
                save_tape_index(vol or "unknown", fl, now_ts())
                log_action("mqtt_read_index",True,f"{len(fl)} files for {vol}")
            except Exception as e: log_action("mqtt_read_index",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "inventory":
        with _inventory_lock:
            if not _inventory_job.get("running"):
                threading.Thread(target=inventory_worker, kwargs={"mode":"full"}, daemon=True).start()
    elif suffix == "inventory_quick":
        with _inventory_lock:
            if not _inventory_job.get("running"):
                threading.Thread(target=inventory_worker, kwargs={"mode":"quick"}, daemon=True).start()
    elif suffix == "inventory_pause":
        request_inventory_pause()
        publish_state_to_mqtt(refresh_state())
    elif suffix == "inventory_resume":
        request_inventory_resume()
        publish_state_to_mqtt(refresh_state())
    elif suffix == "inventory_stop":
        request_inventory_stop()
        publish_state_to_mqtt(refresh_state())

    # ── Config setters (from HA selects/numbers/texts) ────────────────────────
    elif suffix == "set_backup_mode":
        if payload in ("full","incremental","differential"):
            _mqtt_runtime_cfg["backup_mode"] = payload
            mqtt_publish(mqtt_topic("backup","mode_select_state"), payload)
            mqtt_publish(mqtt_topic("backup","mode"), payload)
            log_action("mqtt_config",True,f"Backup mode set to {payload}")

    elif suffix == "set_load_slot":
        try:
            _mqtt_runtime_cfg["load_slot"] = int(payload)
            mqtt_publish(mqtt_topic("config","load_slot"), str(int(payload)))
        except Exception: pass

    elif suffix == "set_verify_sample_mb":
        try:
            _mqtt_runtime_cfg["verify_sample_mb"] = int(float(payload))
            mqtt_publish(mqtt_topic("config","verify_sample_mb"), str(int(float(payload))))
        except Exception: pass

    elif suffix == "set_backup_label":
        _mqtt_runtime_cfg["backup_label"] = payload
        mqtt_publish(mqtt_topic("config","backup_label"), payload)

    elif suffix == "refresh":
        publish_state_to_mqtt(refresh_state())
        # Also refresh health cache
        threading.Thread(target=_refresh_health_cache, daemon=True).start()

def mqtt_loop():
    global _mqtt_client, _mqtt_connected
    if not mqtt_available(): return
    def on_connect(c,u,f,rc,props=None):
        global _mqtt_connected; _mqtt_connected = (rc==0)
        if _mqtt_connected: publish_discovery(); publish_state_to_mqtt(refresh_state())
    def on_disconnect(c,u,rc,props=None):
        global _mqtt_connected; _mqtt_connected = False
    def on_message(c,u,msg):
        try: _handle_mqtt_cmd(msg.topic,(msg.payload or b"").decode(errors="ignore"))
        except Exception as e: log_action("mqtt_msg",False,str(e))
    _mqtt_client = (mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                    if hasattr(mqtt,"CallbackAPIVersion") else mqtt.Client())
    if MQTT_USER: _mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    _mqtt_client.on_connect = on_connect
    _mqtt_client.on_disconnect = on_disconnect
    _mqtt_client.on_message = on_message
    _mqtt_client.will_set(mqtt_topic("availability"), "offline", retain=True)
    _mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    _mqtt_client.loop_start()
    while True:
        try:
            publish_state_to_mqtt(refresh_state())
            # Refresh health data on its own slower cadence
            if SG_DEVICE and (now_ts() - _health_last_refresh) >= HEALTH_REFRESH_INTERVAL:
                _refresh_health_cache()
        except Exception as e: log_action("mqtt_publish",False,str(e))
        time.sleep(POLL_SECONDS)

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def require_password():
    if not WEBUI_PASSWORD: return None
    if not hmac.compare_digest(request.headers.get("X-API-Key",""), WEBUI_PASSWORD):
        return jsonify({"ok":False,"error":"Unauthorized"}), 401
    return None

def do_action(kind, fn):
    auth = require_password()
    if auth is not None: return auth
    with _action_lock:
        try:
            detail = fn(); log_action(kind,True,detail)
            state = refresh_state(); publish_state_to_mqtt(state)
            return {"ok":True,"detail":detail,"state":state}
        except Exception as e:
            log_action(kind,False,str(e))
            state = refresh_state(); publish_state_to_mqtt(state)
            return {"ok":False,"error":str(e),"state":state}

@app.get("/api/drive_info")
def api_drive_info():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok": True, "drive_info": get_drive_info()})

@app.get("/api/tape_history")
def api_tape_history():
    auth = require_password()
    if auth is not None: return auth
    vol = request.args.get("volume_tag", "").strip()
    with _drive_history_lock:
        if vol:
            return jsonify({"ok": True, "history": _drive_history.get(vol, {})})
        return jsonify({"ok": True, "history": dict(_drive_history)})

@app.get("/api/status")
def api_status():
    auth = require_password()
    if auth is not None: return auth
    state = refresh_state()
    return jsonify({**state, "actions": _action_log[:50], "drive_info": get_drive_info(),
                    "changer_job": snapshot_changer_job()})

@app.get("/api/browse")
def api_browse():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok":True, **list_directories(request.args.get("path", BACKUP_ROOT))})

@app.get("/api/tape_index")
def api_tape_index():
    auth = require_password()
    if auth is not None: return auth
    vol = request.args.get("volume_tag","").strip()
    if vol:
        idx = load_tape_index(vol)
        if idx is None: return jsonify({"ok":False,"error":f"No index for {vol}"}), 404
        return jsonify({"ok":True,**idx})
    return jsonify({"ok":True,"indexes":list_all_known_indexes()})

@app.post("/api/tape_index/read")
def api_tape_index_read():
    auth = require_password()
    if auth is not None: return auth
    vol = (_state_cache.get("summary") or {}).get("loaded_volume","")
    def _do():
        if is_cleaning_volume_tag(vol):
            update_tape_index_metadata(vol or "unknown", present=True, purpose="cleaning", is_cleaning=True)
            raise TapeError(f"{vol or 'unknown'} is a cleaning tape; index read skipped.")
        fl = read_tape_index_live()
        if not fl:
            raise TapeError("tar -tf returned no files — is a tape loaded and does it contain a tar archive?")
        space = build_loaded_tape_space_info()
        save_tape_index(vol or "unknown", fl, now_ts(), meta={**space_meta_from_info(space), 'present': True})
        return f"Index built: {len(fl)} files for '{vol or 'unknown'}'"
    return jsonify(do_action("read_index", _do))

@app.post("/api/tape_index/update_loaded")
def api_tape_index_update_loaded():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    mode = str(p.get("mode", "full")).strip().lower() or "full"
    if mode not in ("full", "quick"):
        return jsonify({"ok":False,"error":"Mode must be 'full' or 'quick'."}), 400

    def _do():
        refresh_state()
        drive = _state_cache.get("drive", {}) or {}
        if drive.get("empty", True):
            raise TapeError("No tape is currently loaded in the drive.")
        if snapshot_inventory_job().get("running"):
            raise TapeError("Inventory is already running.")
        if snapshot_backup_job().get("running"):
            raise TapeError("Backup is currently running.")
        if snapshot_restore_job().get("running"):
            raise TapeError("Restore is currently running.")
        if snapshot_verify_job().get("running"):
            raise TapeError("Verify is currently running.")

        vol = (drive.get("volume_tag") or (_state_cache.get("summary") or {}).get("loaded_volume") or "unknown").strip() or "unknown"
        loaded_slot = get_effective_loaded_slot()
        slot_info = next((s for s in (_state_cache.get("slots") or []) if s.get("slot") == loaded_slot), None)
        meta = {
            "present": True,
            "last_seen_at": now_ts(),
            "last_seen_slot": loaded_slot,
            "magazine": (slot_info or {}).get("magazine"),
            "slot_in_magazine": (slot_info or {}).get("slot_in_magazine"),
            "purpose": "cleaning" if is_cleaning_volume_tag(vol) else "data",
            "is_cleaning": is_cleaning_volume_tag(vol),
        }

        if is_cleaning_volume_tag(vol):
            update_tape_index_metadata(vol, **meta)
            return f"Updated loaded cleaning tape metadata for {vol}."

        if mode == "quick":
            update_tape_index_metadata(vol, **meta)
            return f"Updated loaded tape metadata for {vol}."

        files = read_tape_index_live()
        if not files:
            raise TapeError("tar -tf returned no files — tape may be empty or not a tar archive.")
        save_tape_index(vol, files, now_ts(), meta=meta)
        return f"Updated loaded tape inventory for {vol}: {len(files)} files."

    return jsonify(do_action("update_loaded_inventory", _do))

@app.post("/api/tape_index/delete")
def api_tape_index_delete():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    vol = str(p.get("volume_tag", "")).strip()
    permanent = bool(p.get("permanent", False))
    if not vol:
        return jsonify({"ok": False, "error": "Provide volume_tag."}), 400
    # Safety: require explicit confirmation string for permanent deletes
    if permanent:
        confirm_str = str(p.get("confirm", "")).strip()
        if confirm_str != f"DELETE {vol}":
            return jsonify({
                "ok": False,
                "error": f"Permanent delete requires confirm='DELETE {vol}' in the request body."
            }), 400
    if not delete_tape_index(vol, permanent=permanent):
        return jsonify({"ok": False, "error": f"No catalog entry for {vol}."}), 404
    action = "Permanently deleted" if permanent else "Soft-deleted (hidden)"
    log_action("tape_delete", True, f"{action} catalog entry for {vol}.")
    return jsonify({"ok": True, "detail": f"{action} catalog entry for {vol}."})

@app.post("/api/tape_index/archive")
def api_tape_index_archive():
    """Manually mark a tape as archived (off-site / removed from library)."""
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    vol = str(p.get("volume_tag", "")).strip()
    if not vol:
        return jsonify({"ok": False, "error": "Provide volume_tag."}), 400
    idx = load_tape_index(vol)
    if not idx:
        return jsonify({"ok": False, "error": f"No catalog entry for {vol}."}), 404
    mark_tape_archived(vol)
    log_action("tape_archive", True, f"Manually archived {vol}.")
    return jsonify({"ok": True, "detail": f"{vol} marked as archived. Catalog and file index preserved."})

@app.post("/api/tape_index/reindex")
def api_tape_index_reindex():
    """Re-index a tape that is either already in the drive or in a specific slot.

    If slot is provided and the tape is not in the drive, this endpoint will:
      1. Unload whatever is currently in the drive (if anything).
      2. Load the requested slot.
      3. Read the tape index.
      4. Unload the tape back to the same slot.

    Runs asynchronously — use /api/changer/status or /api/status to poll progress.
    """
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    slot = int(p.get("slot", 0) or 0)
    vol  = str(p.get("volume_tag", "") or "").strip()

    with _changer_lock:
        if _changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409
    with _backup_lock:
        if _backup_job.get("running"):
            return jsonify({"ok": False, "error": "A backup is running — cannot re-index now."}), 409
    with _inventory_lock:
        if _inventory_job.get("running"):
            return jsonify({"ok": False, "error": "Inventory is running — cannot re-index now."}), 409

    def _run():
        loaded_slot_here: Optional[int] = None
        try:
            refresh_state()
            drive = (_state_cache.get("drive") or {})
            drive_vol = (drive.get("volume_tag") or "").strip()
            target_slot = slot

            set_changer_state(running=True, action="reindex", status="preparing",
                              detail=f"Preparing to re-index {vol or f'slot {slot}'}…",
                              error=None, started_at=now_ts(), finished_at=None)
            publish_state_to_mqtt(refresh_state())

            if not drive.get("empty"):
                # Tape already in drive
                if (drive_vol and drive_vol == vol) or (not vol and not target_slot):
                    # The tape we want is already loaded — just read it
                    target_vol = drive_vol or vol
                    set_changer_state(status="reading", detail=f"Reading index for {target_vol}…")
                    publish_state_to_mqtt(refresh_state())
                    try:
                        files = read_tape_index_live()
                    except TapeError as _re:
                        if "__blank_or_foreign__" in str(_re):
                            raise TapeError("Tape is blank or was not written by this software — no tar archive found.")
                        raise
                    if not files:
                        raise TapeError("tar returned no files — is this tape a tar archive?")
                    space = build_loaded_tape_space_info()
                    meta = {**space_meta_from_info(space), "present": True}
                    # Preserve slot info if known
                    loaded_s = get_effective_loaded_slot()
                    if loaded_s:
                        meta["last_seen_slot"] = loaded_s
                        meta["last_seen_at"]   = now_ts()
                    save_tape_index(target_vol, files, now_ts(), meta=meta)
                    detail = f"Re-indexed {target_vol}: {len(files)} files."
                    log_action("reindex", True, detail)
                    set_changer_state(running=False, status="completed", detail=detail, finished_at=now_ts())
                    publish_state_to_mqtt(refresh_state())
                    return
                else:
                    # Different tape in drive — unload it first
                    existing_slot = get_effective_loaded_slot()
                    if existing_slot:
                        set_changer_state(status="unloading",
                                          detail=f"Unloading current tape to slot {existing_slot}…")
                        publish_state_to_mqtt(refresh_state())
                        run_cmd(["mtx", "-f", CHANGER, "unload", str(existing_slot), "0"],
                                timeout=max(COMMAND_TIMEOUT, 120))
                        _save_last_known_loaded_slot(None)
                        time.sleep(2)
                    else:
                        raise TapeError("Drive has a tape but its slot is unknown — unload it manually first.")

            if not target_slot:
                # Try to find the slot from the catalog
                idx = load_tape_index(vol)
                target_slot = int((idx or {}).get("last_seen_slot") or 0)
                if not target_slot:
                    raise TapeError(f"No slot known for {vol} — open the tape in the library and select its slot.")

            # Load the tape
            set_changer_state(status="loading", detail=f"Loading slot {target_slot}…")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mtx", "-f", CHANGER, "load", str(target_slot), "0"],
                    timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(target_slot)
            loaded_slot_here = target_slot
            time.sleep(3)
            refresh_state()

            target_vol = (_state_cache.get("summary") or {}).get("loaded_volume", "") or vol

            # Read the index
            set_changer_state(status="reading", detail=f"Reading index for {target_vol}…")
            publish_state_to_mqtt(refresh_state())
            try:
                files = read_tape_index_live()
            except TapeError as _re:
                if "__blank_or_foreign__" in str(_re):
                    raise TapeError("Tape is blank or was not written by this software — no tar archive found.")
                raise
            if not files:
                raise TapeError("tar returned no files — is this tape a tar archive?")

            space = build_loaded_tape_space_info()
            save_tape_index(target_vol, files, now_ts(), meta={
                **space_meta_from_info(space),
                "present": True,
                "last_seen_slot": target_slot,
                "last_seen_at":   now_ts(),
            })
            detail = f"Re-indexed {target_vol}: {len(files)} files."
            append_inventory_log(detail)

            # Unload back to same slot
            set_changer_state(status="unloading", detail=f"Returning {target_vol} to slot {target_slot}…")
            publish_state_to_mqtt(refresh_state())
            run_cmd(["mtx", "-f", CHANGER, "unload", str(target_slot), "0"],
                    timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(None)
            loaded_slot_here = None

            log_action("reindex", True, detail)
            set_changer_state(running=False, status="completed", detail=detail, finished_at=now_ts())

        except Exception as e:
            if loaded_slot_here:
                try:
                    run_cmd(["mtx", "-f", CHANGER, "unload", str(loaded_slot_here), "0"],
                            timeout=max(COMMAND_TIMEOUT, 120))
                    _save_last_known_loaded_slot(None)
                except Exception:
                    pass
            log_action("reindex", False, str(e))
            set_changer_state(running=False, status="failed", error=str(e),
                              detail=f"Re-index failed: {e}", finished_at=now_ts())
        finally:
            publish_state_to_mqtt(refresh_state())

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "detail": f"Re-index started for {vol or f'slot {slot}'}…",
                    "changer_job": snapshot_changer_job()})

@app.post("/api/load")
def api_load():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    slot = int(p.get("slot", 0))
    if slot <= 0:
        return jsonify({"ok": False, "error": "Provide slot number."}), 400
    with _changer_lock:
        if _changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409

    def _run():
        set_changer_state(running=True, action="load", status="loading",
                          detail=f"Loading slot {slot} into drive…",
                          error=None, started_at=now_ts(), finished_at=None)
        publish_state_to_mqtt(refresh_state())
        try:
            refresh_state()
            drive = (_state_cache.get("drive") or {})
            if not drive.get("empty"):
                loaded_vol = (drive.get("volume_tag") or "").strip()
                loaded_slot = get_effective_loaded_slot()
                if not loaded_vol and not loaded_slot:
                    _save_last_known_loaded_slot(None)
                    refresh_state()
                    drive2 = (_state_cache.get("drive") or {})
                    if not drive2.get("empty"):
                        loaded_vol = (drive2.get("volume_tag") or "unknown").strip() or "unknown"
                        loaded_slot = get_effective_loaded_slot()
                        raise TapeError(f"Drive already has tape {loaded_vol or 'unknown'} loaded{f' from slot {loaded_slot}' if loaded_slot else ''}. Unload it first.")
                else:
                    raise TapeError(f"Drive already has tape {loaded_vol or 'unknown'} loaded{f' from slot {loaded_slot}' if loaded_slot else ''}. Unload it first.")
            slot_info = next((s for s in (_state_cache.get("slots") or []) if s.get("slot") == slot), None)
            if not slot_info:
                raise TapeError(f"Slot {slot} not found.")
            if not slot_info.get("full"):
                raise TapeError(f"Slot {slot} is empty.")
            run_cmd(["mtx", "-f", CHANGER, "load", str(slot), "0"], timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(slot)
            time.sleep(2)
            refresh_state()
            detail = f"Loaded slot {slot}"
            log_action("load", True, detail)
            set_changer_state(running=False, status="completed", detail=detail, finished_at=now_ts())
        except Exception as e:
            log_action("load", False, str(e))
            set_changer_state(running=False, status="failed", error=str(e),
                              detail=f"Load failed: {e}", finished_at=now_ts())
        publish_state_to_mqtt(refresh_state())

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "detail": f"Loading slot {slot}…", "changer_job": snapshot_changer_job()})

@app.post("/api/unload")
def api_unload():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    slot = int(p.get("slot", 0))
    if slot <= 0:
        slot = get_effective_loaded_slot() or 0
    if slot <= 0:
        return jsonify({"ok": False, "error": "No destination slot known for unload."}), 400
    with _changer_lock:
        if _changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409

    def _run():
        set_changer_state(running=True, action="unload", status="unloading",
                          detail=f"Unloading tape to slot {slot}…",
                          error=None, started_at=now_ts(), finished_at=None)
        publish_state_to_mqtt(refresh_state())
        try:
            refresh_state()
            drive = (_state_cache.get("drive") or {})
            if drive.get("empty"):
                raise TapeError("Drive is already empty.")
            slot_info = next((s for s in (_state_cache.get("slots") or []) if s.get("slot") == slot), None)
            if slot_info and slot_info.get("full"):
                raise TapeError(f"Destination slot {slot} is already full.")
            run_cmd(["mtx", "-f", CHANGER, "unload", str(slot), "0"], timeout=max(COMMAND_TIMEOUT, 120))
            _save_last_known_loaded_slot(None)
            time.sleep(2)
            refresh_state()
            detail = f"Unloaded to slot {slot}"
            log_action("unload", True, detail)
            set_changer_state(running=False, status="completed", detail=detail, finished_at=now_ts())
        except Exception as e:
            log_action("unload", False, str(e))
            set_changer_state(running=False, status="failed", error=str(e),
                              detail=f"Unload failed: {e}", finished_at=now_ts())
        publish_state_to_mqtt(refresh_state())

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "detail": f"Unloading tape to slot {slot}…", "changer_job": snapshot_changer_job()})

@app.get("/api/changer/status")
def api_changer_status():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok": True, "changer_job": snapshot_changer_job()})

@app.post("/api/rewind")
def api_rewind():
    def _do(): run_cmd(["mt","-f",TAPE,"rewind"]); return f"Rewound {TAPE}"
    return jsonify(do_action("rewind", _do))

@app.post("/api/backup/start")
def api_backup_start():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    paths = p.get("paths") or []
    if not isinstance(paths, list) or not paths:
        return jsonify({"ok": False, "error": "Pick at least one folder."}), 400
    with _backup_lock:
        if _backup_job.get("running"):
            return jsonify({"ok": False, "error": "Backup already running."}), 409
    validated = [ensure_under_backup_root(x) for x in paths]
    log_level = normalize_backup_log_level(p.get("log_level"))
    threading.Thread(
        target=backup_worker,
        args=(validated,),
        kwargs={"backup_mode": p.get("mode", "full"), "label": p.get("label", ""), "log_level": log_level},
        daemon=True,
    ).start()
    return jsonify({"ok": True, "detail": "Backup started.", "backup_job": snapshot_backup_job()})

@app.post("/api/backup/stop")
def api_backup_stop():
    auth = require_password()
    if auth is not None: return auth
    global _stop_requested, _tar_proc
    with _backup_lock:
        if not _backup_job.get("running"):
            return jsonify({"ok":False,"error":"No backup running."}), 409
    _stop_requested = True
    if _tar_proc:
        try: _tar_proc.send_signal(signal.SIGTERM)
        except Exception: pass
    log_action("stop_backup",True,"Stop requested.")
    return jsonify({"ok":True,"detail":"Cancel requested — tar will stop after the current file(s)."})


@app.post("/api/restore/stop")
def api_restore_stop():
    auth = require_password()
    if auth is not None: return auth
    global _stop_restore, _restore_proc
    with _restore_lock:
        if not _restore_job.get("running"):
            return jsonify({"ok": False, "error": "No restore running."}), 409
    _stop_restore = True
    if _restore_proc:
        try: _restore_proc.send_signal(signal.SIGTERM)
        except Exception: pass
    log_action("stop_restore", True, "Restore stop requested.")
    return jsonify({"ok": True, "detail": "Restore cancel requested."})


@app.post("/api/format/start")
def api_format_start():
    """Erase (format) one or more tapes.

    Request body:
        {
          "tapes": [ {"slot": 5, "volume_tag": "SM9158L6"}, … ],
          "catalog_only": false   // optional; true = DB reset only, no hardware
        }

    catalog_only=false (default): load each tape, run `mt erase -s` (short
        erase, seconds not hours), unload, clear catalog.
    catalog_only=true: skip all hardware — just reset DB/index entries so the
        tape appears blank and available.  Use when the tape is already blank
        or was erased externally.
    """
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    tapes = p.get("tapes", [])
    catalog_only = bool(p.get("catalog_only", False))
    if not tapes:
        return jsonify({"ok": False, "error": "Provide at least one tape in 'tapes'."}), 400

    # Validate each entry
    validated = []
    for t in tapes:
        slot = int(t.get("slot") or 0)
        vol  = str(t.get("volume_tag") or "").strip()
        if not slot:
            return jsonify({"ok": False, "error": "Each tape must have a 'slot'."}), 400
        if is_cleaning_volume_tag(vol):
            return jsonify({"ok": False, "error": f"{vol} is a cleaning tape — cannot format."}), 400
        validated.append({"slot": slot, "volume_tag": vol})

    with _format_lock:
        if _format_job.get("running"):
            return jsonify({"ok": False, "error": "A format job is already running."}), 409
    with _backup_lock:
        if _backup_job.get("running"):
            return jsonify({"ok": False, "error": "A backup is running — cannot format now."}), 409
    with _inventory_lock:
        if _inventory_job.get("running"):
            return jsonify({"ok": False, "error": "An inventory scan is running — cannot format now."}), 409

    mode_label = "catalog-only" if catalog_only else "hardware short-erase"
    threading.Thread(target=format_worker, args=(validated,), kwargs={"catalog_only": catalog_only}, daemon=True).start()
    return jsonify({"ok": True, "detail": f"Format started for {len(validated)} tape(s) ({mode_label}).",
                    "format_job": snapshot_format_job()})

@app.get("/api/format/status")
def api_format_status():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok": True, "format_job": snapshot_format_job()})

@app.post("/api/format/stop")
def api_format_stop():
    auth = require_password()
    if auth is not None: return auth
    global _stop_format
    with _format_lock:
        if not _format_job.get("running"):
            return jsonify({"ok": False, "error": "No format job running."}), 409
    _stop_format = True
    log_action("format_stop", True, "Format stop requested — will stop after current tape.")
    return jsonify({"ok": True, "detail": "Format will stop after the current tape finishes."})


@app.get("/api/restore/browse")
def api_restore_browse():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok":True, **list_restore_directories(request.args.get("path", RESTORE_ROOT))})

@app.post("/api/cleaning/run")
def api_cleaning_run():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    requested_slot = int(p.get("slot", 0) or 0)

    def _do():
        refresh_state()
        drive = (_state_cache.get("drive") or {})
        summary = (_state_cache.get("summary") or {})
        loaded_slot = get_effective_loaded_slot()
        cleaning_slot = requested_slot or get_cleaning_slot()

        if not drive.get("empty"):
            loaded_vol = (drive.get("volume_tag") or "").strip()
            if not is_cleaning_volume_tag(loaded_vol):
                raise TapeError(f"Drive currently has non-cleaning tape {loaded_vol or 'unknown'} loaded.")
            cleaning_slot = loaded_slot or cleaning_slot
            append_inventory_log(f"Cleaning cycle requested for already-loaded cleaning tape {loaded_vol}.")
        else:
            if not cleaning_slot:
                raise TapeError("No cleaning tape slot found.")
            slot_info = next((s for s in (_state_cache.get("slots") or []) if int(s.get("slot",0)) == int(cleaning_slot)), None)
            if not slot_info or not slot_info.get("full"):
                raise TapeError(f"Cleaning slot {cleaning_slot} is empty or not found.")
            if not is_cleaning_volume_tag(slot_info.get("volume_tag", "")):
                raise TapeError(f"Slot {cleaning_slot} does not contain a cleaning tape.")
            run_cmd(["mtx","-f",CHANGER,"load",str(cleaning_slot),"0"], timeout=max(COMMAND_TIMEOUT,120))
            _save_last_known_loaded_slot(cleaning_slot)
            time.sleep(3)
            refresh_state()

        append_inventory_log(f"Cleaning cycle started using slot {cleaning_slot}. Waiting up to {CLEANING_WAIT_SECONDS}s.")
        deadline = time.time() + CLEANING_WAIT_SECONDS
        while time.time() < deadline:
            time.sleep(5)
            refresh_state()
            cur_drive = (_state_cache.get("drive") or {})
            if cur_drive.get("empty"):
                _save_last_known_loaded_slot(None)
                return f"Cleaning tape completed and drive is empty."

        refresh_state()
        cur_drive = (_state_cache.get("drive") or {})
        if not cur_drive.get("empty") and cleaning_slot:
            run_cmd(["mtx","-f",CHANGER,"unload",str(cleaning_slot),"0"], timeout=max(COMMAND_TIMEOUT,120))
            _save_last_known_loaded_slot(None)
            time.sleep(2)
            refresh_state()
        return f"Cleaning cycle finished (timed wait {CLEANING_WAIT_SECONDS}s)."

    return jsonify(do_action("run_cleaning", _do))

@app.post("/api/mail_slot/export")
def api_mail_slot_export():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    slot = int(p.get("slot", 0) or 0)
    if slot <= 0:
        return jsonify({"ok":False, "error":"slot is required."}), 400

    def _do():
        refresh_state()
        state = _state_cache
        mail_slot = get_mail_slot_info(state)
        if not mail_slot:
            raise TapeError("No import/export slot detected on this library.")
        if mail_slot.get("full"):
            raise TapeError(f"Mail slot {mail_slot.get('slot')} already has tape {(mail_slot.get('volume_tag') or '').strip() or 'loaded'}. Import or remove it first.")
        drive = state.get("drive", {}) or {}
        effective_loaded = drive.get("loaded_from_slot") or _last_known_loaded_slot
        if effective_loaded and int(effective_loaded) == slot:
            raise TapeError("That tape is currently in the drive. Unload it before moving it to the mail slot.")
        slot_info = next((s for s in (state.get("slots") or []) if int(s.get("slot", 0) or 0) == slot), None)
        if not slot_info:
            raise TapeError(f"Slot {slot} not found.")
        if slot_info.get("is_import_export"):
            raise TapeError("Selected slot is already the mail slot.")
        if not slot_info.get("full"):
            raise TapeError(f"Slot {slot} is empty.")
        run_cmd(["mtx", "-f", CHANGER, "transfer", str(slot), str(mail_slot.get("slot"))], timeout=max(COMMAND_TIMEOUT, 180))
        time.sleep(2)
        refresh_state()
        return f"Moved tape from slot {slot} to mail slot {mail_slot.get('slot')}."

    return jsonify(do_action("mail_slot_export", _do))

@app.post("/api/mail_slot/import")
def api_mail_slot_import():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    target_slot = int(p.get("slot", 0) or 0)
    if target_slot <= 0:
        return jsonify({"ok":False, "error":"slot is required."}), 400

    def _do():
        refresh_state()
        state = _state_cache
        mail_slot = get_mail_slot_info(state)
        if not mail_slot:
            raise TapeError("No import/export slot detected on this library.")
        if not mail_slot.get("full"):
            raise TapeError("Mail slot is empty.")
        slot_info = next((s for s in (state.get("slots") or []) if int(s.get("slot", 0) or 0) == target_slot), None)
        if not slot_info:
            raise TapeError(f"Slot {target_slot} not found.")
        if slot_info.get("is_import_export"):
            raise TapeError("Choose a storage slot, not the mail slot.")
        if slot_info.get("full"):
            raise TapeError(f"Slot {target_slot} is not empty.")
        run_cmd(["mtx", "-f", CHANGER, "transfer", str(mail_slot.get("slot")), str(target_slot)], timeout=max(COMMAND_TIMEOUT, 180))
        time.sleep(2)
        refresh_state()
        return f"Imported tape from mail slot {mail_slot.get('slot')} to slot {target_slot}."

    return jsonify(do_action("mail_slot_import", _do))

@app.post("/api/restore/start")
def api_restore_start():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    volume_tag = p.get("volume_tag","")
    tape_paths = p.get("paths",[])       # paths as they appear in the tar index
    dest       = p.get("dest", RESTORE_ROOT)
    if is_cleaning_volume_tag(volume_tag):
        return jsonify({"ok":False,"error":"Cleaning tapes cannot be restored."}), 400
    try:
        dest = ensure_under_restore_root(dest)
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 400
    slot       = p.get("slot")           # optional: load this slot first
    if slot is not None:
        slot = int(slot)
    if p.get("dry_run"):
        return jsonify({"ok":True, "detail":f"Restore destination ready: {dest}"})
    with _restore_lock:
        if _restore_job.get("running"):
            return jsonify({"ok":False,"error":"Restore already running."}), 409
    threading.Thread(target=restore_worker, args=(volume_tag, tape_paths, dest, slot), daemon=True).start()
    return jsonify({"ok":True,"detail":"Restore started.","restore_job":snapshot_restore_job()})

@app.get("/api/restore/status")
def api_restore_status():
    return jsonify({"ok":True,"restore_job":snapshot_restore_job()})

@app.post("/api/inventory/start")
def api_inventory_start():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    mode = str(p.get("mode", "full")).strip().lower() or "full"
    if mode not in ("full", "quick"):
        return jsonify({"ok":False,"error":"Mode must be 'full' or 'quick'."}), 400
    with _inventory_lock:
        if _inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory already running."}), 409
    threading.Thread(target=inventory_worker, kwargs={"mode": mode}, daemon=True).start()
    return jsonify({"ok":True,"detail":f"{mode.title()} inventory started."})

@app.post("/api/inventory/pause")
def api_inventory_pause():
    auth = require_password()
    if auth is not None: return auth
    with _inventory_lock:
        if not _inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_pause()
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory paused."})

@app.post("/api/inventory/resume")
def api_inventory_resume():
    auth = require_password()
    if auth is not None: return auth
    with _inventory_lock:
        if not _inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_resume()
    append_inventory_log("Inventory resumed.")
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory resumed."})

@app.post("/api/inventory/stop")
def api_inventory_stop():
    auth = require_password()
    if auth is not None: return auth
    with _inventory_lock:
        if not _inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_stop()
    append_inventory_log("Stop requested — inventory will stop after the current step.")
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory stop requested."})

@app.get("/api/schedules")
def api_schedules_get():
    auth = require_password()
    if auth is not None: return auth
    with _schedules_lock: return jsonify({"ok":True,"schedules":list(_schedules)})

@app.post("/api/schedules")
def api_schedules_create():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    for f in ["paths","mode"]:
        if f not in p: return jsonify({"ok":False,"error":f"Missing: {f}"}), 400
    s = {"id": str(int(time.time()*1000)),
         "label": p.get("label","Scheduled backup"),
         "paths": p["paths"], "mode": p["mode"],
         "hour": int(p.get("hour",2)), "minute": int(p.get("minute",0)),
         "day_of_week": int(p.get("day_of_week",0)),
         "day_of_month": int(p.get("day_of_month",1)),
         "enabled": True, "last_run": None, "next_run": None}
    _update_next_run(s)
    with _schedules_lock: _schedules.append(s)
    _save_schedules()
    return jsonify({"ok":True,"schedule":s})

@app.put("/api/schedules/<sid>")
def api_schedules_update(sid):
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    found = None
    with _schedules_lock:
        for s in _schedules:
            if s["id"] == sid:
                for k in ["label","paths","mode","hour","minute","day_of_week","day_of_month","enabled"]:
                    if k in p: s[k] = p[k]
                _update_next_run(s)
                found = s
                break
    if found is None:
        return jsonify({"ok":False,"error":"Not found."}), 404
    _save_schedules()
    return jsonify({"ok":True,"schedule":found})

@app.delete("/api/schedules/<sid>")
def api_schedules_delete(sid):
    auth = require_password()
    if auth is not None: return auth
    with _schedules_lock:
        before = len(_schedules)
        _schedules[:] = [s for s in _schedules if s["id"] != sid]
        if len(_schedules) == before: return jsonify({"ok":False,"error":"Not found."}), 404
    _save_schedules(); return jsonify({"ok":True})

@app.post("/api/verify/start")
def api_verify_start():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    vol = p.get("volume_tag") or (_state_cache.get("summary") or {}).get("loaded_volume", "")
    if not vol:
        return jsonify({"ok": False, "error": "No tape loaded and no volume_tag given."}), 400
    with _verify_lock:
        if _verify_job.get("running"):
            return jsonify({"ok": False, "error": "Verification already running."}), 409
    threading.Thread(target=verify_worker, args=(vol,), daemon=True).start()
    return jsonify({"ok": True, "detail": f"Verification started for {vol}."})

@app.get("/api/verify/status")
def api_verify_status():
    return jsonify({"ok": True, "verify_job": snapshot_verify_job()})

@app.get("/api/backup_records")
def api_backup_records():
    auth = require_password()
    if auth is not None: return auth
    vol   = request.args.get("volume_tag", "").strip() or None
    limit = int(request.args.get("limit", "100"))
    return jsonify({"ok": True, "records": get_backup_records(vol, limit)})

@app.get("/api/tape_health")
def api_tape_health():
    return jsonify({"ok": True, "health": get_tape_health()})

@app.get("/api/gfs/status")
def api_gfs_status():
    auth = require_password()
    if auth is not None: return auth
    recyclable = gfs_get_recyclable()
    with _backup_records_lock:
        recs = list(_backup_records)
    classified = [{"id": r.get("id"), "volume_tag": r.get("volume_tag"),
                   "started_at": r.get("started_at"), "status": r.get("status"),
                   "gfs_class": gfs_classify(r)} for r in recs[:200]]
    return jsonify({"ok": True, "recyclable": recyclable,
                    "policy": {"daily": GFS_DAILY_KEEP, "weekly": GFS_WEEKLY_KEEP,
                               "monthly": GFS_MONTHLY_KEEP},
                    "records": classified})

@app.get("/api/settings")
def api_settings_get():
    auth = require_password()
    if auth is not None: return auth
    return jsonify({"ok": True, "settings": {
        "verify_after_backup": VERIFY_AFTER_BACKUP,
        "verify_sample_mb": VERIFY_SAMPLE_MB,
        "erase_before_backup": ERASE_BEFORE_BACKUP,
        "auto_rewind_after_backup": AUTO_REWIND_AFTER,
        "pre_backup_hook": PRE_BACKUP_HOOK,
        "post_backup_hook": POST_BACKUP_HOOK,
        "sg_device": SG_DEVICE,
        "gfs_daily_keep": GFS_DAILY_KEEP,
        "gfs_weekly_keep": GFS_WEEKLY_KEEP,
        "gfs_monthly_keep": GFS_MONTHLY_KEEP,
        "restore_root": RESTORE_ROOT,
        "backup_root": BACKUP_ROOT,
        "changer": CHANGER, "tape": TAPE,
        "cleaning_wait_seconds": CLEANING_WAIT_SECONDS,
        "mail_slot_present": bool(get_mail_slot_info(refresh_state() if not _state_cache.get("slots") else _state_cache)),
        "default_backup_log_level": normalize_backup_log_level(BACKUP_LOG_LEVEL_DEFAULT),
        "current_backup_log_level": current_backup_log_level(),
        "restore_subfolder_pattern": get_restore_subfolder_pattern(),
        "ha_url": get_ha_config()["url"],
        "ha_service": get_ha_config()["service"],
        "ha_enabled": get_ha_config()["enabled"],
        "ha_token_set": bool(get_ha_config()["token"]),
        "notify": get_notify_config(),
        "tape_block_kb": TAPE_BLOCK_BYTES // 1024,
        "mbuf_size": os.getenv("TL_MBUF_SIZE", "512M"),
        "mbuf_fill_pct": os.getenv("TL_MBUF_FILL_PCT", "75"),
        "skip_xattrs": os.getenv("TL_SKIP_XATTRS", "false").lower() == "true",
    }})

@app.post("/api/settings/restore_subfolder")
def api_settings_restore_subfolder():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    pattern = str(p.get("pattern", "")).strip()
    set_restore_subfolder_pattern(pattern)
    example = build_restore_dest("EXAMPLE", pattern=pattern)
    return jsonify({"ok": True,
                    "pattern": get_restore_subfolder_pattern(),
                    "example": example,
                    "detail": f"Pattern saved. Example dest: {example}"})

@app.get("/api/restore/default_dest")
def api_restore_default_dest():
    auth = require_password()
    if auth is not None: return auth
    vol = request.args.get("volume_tag", "")
    # Optional pattern override — used by the settings preview button
    pattern = request.args.get("pattern", None)
    return jsonify({"ok": True, "dest": build_restore_dest(vol, pattern=pattern)})

@app.post("/api/settings/notify")
def api_settings_notify_save():
    """Save notification event toggles and message templates."""
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    set_notify_config(p)
    return jsonify({"ok": True, "detail": "Notification settings saved.",
                    "notify": get_notify_config()})

@app.get("/api/settings/notify")
def api_settings_notify_get():
    auth = require_password()
    if auth is not None: return auth
    cfg = get_notify_config()
    cfg["defaults"] = dict(_NOTIFY_DEFAULT_TEMPLATES)
    return jsonify({"ok": True, "notify": cfg})

@app.post("/api/settings/ha")
def api_settings_ha_save():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    existing = get_ha_config()
    # Only overwrite token if the caller actually sent one; blank means "keep existing"
    token = str(p["token"]).strip() if "token" in p and str(p.get("token","")).strip() else existing["token"]
    set_ha_config(
        url     = str(p.get("url",     existing["url"])),
        token   = token,
        service = str(p.get("service", existing["service"])) or "notify",
        enabled = bool(p.get("enabled", existing["enabled"])),
    )
    cfg = get_ha_config()
    return jsonify({"ok": True, "detail": "Home Assistant config saved.", "ha": {
        "url": cfg["url"], "service": cfg["service"], "enabled": cfg["enabled"],
        # never echo the token back
    }})

@app.post("/api/settings/test_ha")
def api_test_ha():
    auth = require_password()
    if auth is not None: return auth
    cfg = get_ha_config()
    if not cfg["url"] or not cfg["token"]:
        return jsonify({"ok": False, "error": "HA URL and token must be configured first."})
    try:
        import urllib.request, urllib.error
        service = cfg["service"] or "notify"
        url = f"{cfg['url']}/api/services/notify/{service}"
        payload = json.dumps({
            "title": "[TL2000] Test Notification",
            "message": f"Test from TL2000 tape library — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        }).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Authorization": f"Bearer {cfg['token']}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return jsonify({"ok": True, "detail": f"Test notification sent via notify.{service}."})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@app.get("/api/settings/ha_services")
def api_ha_services():
    """Fetch available notify services from the connected HA instance."""
    auth = require_password()
    if auth is not None: return auth
    cfg = get_ha_config()
    if not cfg["url"] or not cfg["token"]:
        return jsonify({"ok": False, "error": "HA URL and token must be configured first.", "services": []})
    try:
        import urllib.request, urllib.error
        req = urllib.request.Request(
            f"{cfg['url']}/api/services",
            headers={"Authorization": f"Bearer {cfg['token']}", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        # Find the notify domain and extract its services
        notify_services = []
        for domain_obj in data:
            if domain_obj.get("domain") == "notify":
                svcs = domain_obj.get("services", {})
                # services is a dict of {service_name: {description, fields, ...}}
                notify_services = sorted(svcs.keys()) if isinstance(svcs, dict) else []
                break
        return jsonify({"ok": True, "services": notify_services})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "services": []})

@app.post("/api/refresh")
def api_refresh():
    auth = require_password()
    if auth is not None: return auth
    state = refresh_state(); publish_state_to_mqtt(state)
    return jsonify({"ok":True,"state":state})

@app.post("/api/drive/check")
def api_drive_check():
    auth = require_password()
    if auth is not None: return auth
    state = refresh_state()
    publish_state_to_mqtt(state)
    return jsonify({"ok": True, "state": state, "drive_info": get_drive_info()})

@app.get("/healthz")
def healthz():
    # Deliberately does not touch the changer/tape hardware or require auth --
    # used by the container HEALTHCHECK and must stay fast even mid-backup.
    return jsonify({"ok": True})

@app.get("/icon.png")
@app.get("/favicon.ico")
def serve_icon():
    return send_from_directory(os.path.dirname(ICON_PATH), os.path.basename(ICON_PATH),
                               mimetype="image/png")

@app.get("/")
def index():
    return render_template("index.html", changer=CHANGER, tape=TAPE,
                           polling=POLL_SECONDS, backup_root=BACKUP_ROOT,
                           restore_root=RESTORE_ROOT,
                           webui_password=WEBUI_PASSWORD)



if __name__ == "__main__":
    os.makedirs(TAPE_INDEX_DIR, exist_ok=True)
    init_tape_catalog()
    migrate_legacy_tape_indexes()
    # Compact the DB on startup — reclaims space from deleted rows and soft-deleted
    # catalog entries.  VACUUM cannot run inside a transaction so we open a raw
    # connection.  This is fast (seconds) for a small tape-library DB.
    try:
        _vconn = sqlite3.connect(TAPE_CATALOG_DB)
        _vconn.execute("VACUUM")
        _vconn.close()
    except Exception:
        pass
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    _load_schedules()
    _load_drive_history()
    _load_last_known_loaded_slot()
    _load_restore_subfolder_pattern()
    _load_ha_config()
    _load_notify_config()
    _load_backup_records()
    _load_action_log()
    refresh_state()
    # Warn about tapes that have size data but no file index — these need a
    # "Read Index" pass with the tape loaded to recover the file list.
    try:
        _broken = []
        for _idx in list_all_known_indexes():
            if (_idx.get("used_bytes") or 0) > 0 and (_idx.get("file_count") or 0) == 0:
                _broken.append(_idx["volume_tag"])
        if _broken:
            db_log("app", "info",
                   f"Tapes with usage data but no file index (load each and use 'Read Index' to recover): "
                   f"{', '.join(_broken)}")
    except Exception:
        pass
    if mqtt_available():
        threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    if STARTUP_QUICK_SCAN and CHANGER:
        # Reconcile the catalog against the actual slot contents on boot so tapes that
        # are physically present don't show as "archived" just because the container
        # restarted since the last manual scan.
        threading.Thread(target=inventory_worker, kwargs={"mode": "quick"}, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")), debug=False)