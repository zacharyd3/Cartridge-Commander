"""settings module (split from the original monolithic app.py)."""

import os
import re
import time
import datetime
import threading
from typing import Any, Dict, Optional
from .config import GFS_DAILY_KEEP, GFS_MONTHLY_KEEP, GFS_WEEKLY_KEEP, HA_NOTIFY_ENABLED, HA_NOTIFY_SERVICE, HA_NOTIFY_TOKEN, HA_NOTIFY_URL, RESTORE_ROOT, RESTORE_SUBFOLDER_PATTERN, TAPE_FILL_STRATEGY


_restore_subfolder_pattern: str = RESTORE_SUBFOLDER_PATTERN
_restore_subfolder_lock = threading.Lock()

def get_restore_subfolder_pattern() -> str:
    with _restore_subfolder_lock:
        return _restore_subfolder_pattern

def set_restore_subfolder_pattern(pattern: str) -> None:
    from .db import _db_set_json
    global _restore_subfolder_pattern
    with _restore_subfolder_lock:
        _restore_subfolder_pattern = str(pattern or "")
    _db_set_json("restore_subfolder_pattern", _restore_subfolder_pattern)

def _load_restore_subfolder_pattern() -> None:
    from .db import _db_get_json
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
    from .db import _db_set_json
    with _ha_config_lock:
        _ha_config["url"]     = url.strip().rstrip("/")
        _ha_config["token"]   = token.strip()
        _ha_config["service"] = service.strip() or "notify"
        _ha_config["enabled"] = bool(enabled)
    _db_set_json("ha_config", dict(_ha_config))

def _load_ha_config() -> None:
    from .db import _db_get_json
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
    from .db import _db_set_json
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
    from .db import _db_get_json
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

# ---------------------------------------------------------------------------
# GFS (Grandfather-Father-Son) retention runtime config
# ---------------------------------------------------------------------------
# The three "keep" counts control how many backups survive in each rotation
# tier before a tape becomes recyclable.  They seed from the GFS_*_KEEP env
# vars but, once saved from the UI, the persisted values win.
_gfs_config_lock = threading.Lock()
_gfs_config: Dict[str, int] = {
    "daily":   GFS_DAILY_KEEP,
    "weekly":  GFS_WEEKLY_KEEP,
    "monthly": GFS_MONTHLY_KEEP,
}

# Sanity ceiling — no rotation tier keeps more than this many windows.
_GFS_MAX_KEEP = 3650

def _coerce_keep(value: Any, fallback: int) -> int:
    """Clamp an incoming keep count to a sane non-negative integer."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(0, min(n, _GFS_MAX_KEEP))

def get_gfs_config() -> Dict[str, int]:
    with _gfs_config_lock:
        return dict(_gfs_config)

def set_gfs_config(daily: Any = None, weekly: Any = None, monthly: Any = None) -> Dict[str, int]:
    """Update any subset of the GFS keep counts and persist the result."""
    from .db import _db_set_json
    with _gfs_config_lock:
        if daily is not None:
            _gfs_config["daily"] = _coerce_keep(daily, _gfs_config["daily"])
        if weekly is not None:
            _gfs_config["weekly"] = _coerce_keep(weekly, _gfs_config["weekly"])
        if monthly is not None:
            _gfs_config["monthly"] = _coerce_keep(monthly, _gfs_config["monthly"])
        result = dict(_gfs_config)
    _db_set_json("gfs_config", result)
    return result

def _load_gfs_config() -> None:
    from .db import _db_get_json
    data = _db_get_json("gfs_config", None)
    if isinstance(data, dict):
        with _gfs_config_lock:
            for k in ("daily", "weekly", "monthly"):
                if k in data:
                    _gfs_config[k] = _coerce_keep(data[k], _gfs_config[k])

# ---------------------------------------------------------------------------
# Tape fill strategy runtime config
# ---------------------------------------------------------------------------
# "spread" — round-robin across the library (default, original behaviour).
# "fill"   — concentrate writes on one tape until full, then roll to the next
#            (handy for pulling a full tape for offsite storage).
_VALID_FILL_STRATEGIES = ("spread", "fill")
_tape_strategy_lock = threading.Lock()
_tape_fill_strategy: str = TAPE_FILL_STRATEGY if TAPE_FILL_STRATEGY in _VALID_FILL_STRATEGIES else "spread"

def get_tape_fill_strategy() -> str:
    with _tape_strategy_lock:
        return _tape_fill_strategy

def set_tape_fill_strategy(strategy: str) -> str:
    from .db import _db_set_json
    global _tape_fill_strategy
    s = str(strategy or "").strip().lower()
    if s not in _VALID_FILL_STRATEGIES:
        s = "spread"
    with _tape_strategy_lock:
        _tape_fill_strategy = s
    _db_set_json("tape_fill_strategy", s)
    return s

def _load_tape_fill_strategy() -> None:
    from .db import _db_get_json
    global _tape_fill_strategy
    val = _db_get_json("tape_fill_strategy", None)
    if isinstance(val, str) and val.strip().lower() in _VALID_FILL_STRATEGIES:
        with _tape_strategy_lock:
            _tape_fill_strategy = val.strip().lower()

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

