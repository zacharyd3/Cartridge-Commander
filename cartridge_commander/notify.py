"""notify module (split from the original monolithic app.py)."""

import json
import datetime
import threading
from typing import Any, Dict, List, Set, Tuple


def _ha_notify(title: str, message: str) -> None:
    """Send a notification via Home Assistant's notify service REST API.

    Silently does nothing if HA URL/token are unset or HA notifications are
    disabled.  Uses only the stdlib so no extra dependencies are required.
    """
    from .settings import get_ha_config
    from .state import log_action
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
                           verified: bool, verify_errors: int, skipped: int = 0) -> None:
    from .settings import _render_notify_template, get_notify_config
    from .state import bytes_human, secs_human
    if not get_notify_config()["on_backup_success"]:
        return
    ver_str = ("Yes — " + str(verify_errors) + " errors") if verified else "No"
    status = "✅ COMPLETED" + (" + ✅ VERIFIED" if verified and verify_errors == 0
                               else f" + ⚠️ VERIFY ERRORS ({verify_errors})" if verified else "")
    tokens = dict(
        vol=vol, paths=", ".join(paths),
        written=bytes_human(bw), duration=secs_human(int(elapsed)),
        speed=bytes_human(bw / max(elapsed, 1)),
        verified=ver_str, errors=str(verify_errors), error="", skipped=str(skipped),
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_success_title", **tokens)
    body  = _render_notify_template("backup_success_body",  **tokens)
    if skipped and "{skipped}" not in (get_notify_config()["templates"].get("backup_success_body") or ""):
        body += f"\n⚠️ Skipped {skipped} unreadable item(s) — see backup log."
    _ha_notify(title, body)


def notify_backup_failure(vol: str, paths: List[str], error: str) -> None:
    from .settings import _render_notify_template, get_notify_config
    if not get_notify_config()["on_backup_failure"]:
        return
    tokens = dict(
        vol=vol, paths=", ".join(paths), error=error,
        written="", duration="", speed="", verified="", errors="", skipped="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_failure_title", **tokens)
    body  = _render_notify_template("backup_failure_body",  **tokens)
    _ha_notify(title, body)


def notify_verify_failure(vol: str, errors: int, detail: str) -> None:
    from .settings import _render_notify_template, get_notify_config
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
    from .settings import get_notify_config
    if not get_notify_config()["on_format_complete"]:
        return
    vol = ", ".join(vols) if vols else "(none)"
    err = ", ".join(failed) if failed else ""
    status = "✅ Format complete" if not failed else "⚠️ Format completed with errors"
    title = f"[TL2000] {status}"
    body  = f"{status}\nFormatted: {vol}" + (f"\nFailed: {err}" if err else "")
    _ha_notify(title, body)


def notify_inventory_done(total: int, added: int, changed: int) -> None:
    from .settings import get_notify_config
    if not get_notify_config()["on_inventory_done"]:
        return
    title = "[TL2000] Inventory complete"
    body  = f"✅ Inventory done — {total} tapes, {added} added, {changed} changed"
    _ha_notify(title, body)


# ---------------------------------------------------------------------------
# Tape full
# ---------------------------------------------------------------------------
# A tape is announced once each time it fills: the catalog key below maps each
# tape that has been full to a signature of what was on it then.  A tape that
# is erased, marked available or recycled by GFS and then filled again carries
# new sessions, so a new signature, and is announced again; one that only
# drifts in and out of "full" as settings change (threshold, GFS keep counts)
# keeps its signature and is not.

_FULL_TAPES_KEY = "notified_full_tapes"
_full_tapes_lock = threading.Lock()


def _fill_signature(idx: Dict[str, Any]) -> str:
    """Identifies the data on a tape: its newest session (the backup written
    last), or for a tape catalogued before sessions were tracked, its write time."""
    sessions = idx.get("sessions") or []
    if sessions:
        last = max(sessions, key=lambda s: int(s.get("file_number") or 0))
        return f"{last.get('record_id') or last.get('dirname') or ''}@{int(last.get('file_number') or 0)}"
    return f"legacy@{idx.get('written_at') or 0}"


def _full_tapes_now() -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
    """Every catalogued data tape that tape selection treats as full right now,
    and the volume tags of every tape in the catalog."""
    from .db import list_all_known_indexes
    from .drive_history import tape_fill_status
    from .records import gfs_get_recyclable
    from .state import is_cleaning_volume_tag
    recyclable = set(gfs_get_recyclable())
    out: Dict[str, Dict[str, Any]] = {}
    catalogued: Set[str] = set()
    for idx in list_all_known_indexes():
        vol = str(idx.get("volume_tag") or "").strip()
        catalogued.add(vol)
        if (not vol or idx.get("is_cleaning") or is_cleaning_volume_tag(vol)
                or str(idx.get("purpose") or "").lower() == "cleaning"):
            continue
        fill = tape_fill_status(vol, idx, recyclable=vol in recyclable)
        if fill["full"]:
            out[vol] = {"idx": idx, "fill": fill, "sig": _fill_signature(idx)}
    return out, catalogued


def _tape_location(vol: str, idx: Dict[str, Any]) -> str:
    """Where the tape sits now, e.g. "slot 4", "mail slot 24" or "drive"."""
    from . import state as shared_state
    cache = shared_state._state_cache or {}
    for s in cache.get("slots") or []:
        if s.get("full") and str(s.get("volume_tag") or "").strip() == vol:
            return f"{'mail slot' if s.get('is_import_export') else 'slot'} {s.get('slot')}"
    drive = cache.get("drive") or {}
    if not drive.get("empty", True) and str(drive.get("volume_tag") or "").strip() == vol:
        return "drive"
    return f"slot {idx['last_seen_slot']}" if idx.get("last_seen_slot") else ""


def seed_full_tape_tracking() -> None:
    """Record the tapes already full, without announcing them, the first time
    this runs, so turning the feature on doesn't alert about every old tape."""
    from .db import _db_get_json, _db_set_json
    with _full_tapes_lock:
        if _db_get_json(_FULL_TAPES_KEY, None) is not None:
            return
        full, _ = _full_tapes_now()
        _db_set_json(_FULL_TAPES_KEY, {v: f["sig"] for v, f in full.items()})


def check_full_tapes() -> List[str]:
    """Announce the tapes that have filled since the last check.

    Called after anything that can fill a tape or change what counts as full:
    a backup, an inventory, a new full threshold.  Only tapes still in the
    library are announced; one already pulled for archiving needs no swap.
    Returns the volume tags that newly filled while in the library (whether or
    not the alert is turned on).
    """
    from .db import _db_get_json, _db_set_json
    from .settings import _render_notify_template, get_notify_config
    from .state import bytes_human, log_action
    with _full_tapes_lock:
        known = _db_get_json(_FULL_TAPES_KEY, None)
        known = known if isinstance(known, dict) else {}
        full, catalogued = _full_tapes_now()
        fresh = sorted(v for v, f in full.items() if known.get(v) != f["sig"])
        updated = {v: sig for v, sig in known.items() if v in catalogued}
        updated.update({v: full[v]["sig"] for v in fresh})
        if updated != known:
            _db_set_json(_FULL_TAPES_KEY, updated)

    announce = []
    for vol in fresh:
        idx = full[vol]["idx"]
        if idx.get("present") and str(idx.get("purpose") or "").lower() != "archived":
            announce.append(vol)
    if not announce:
        return []

    lines, slots = [], []
    for vol in announce:
        fill = full[vol]["fill"]
        where = _tape_location(vol, full[vol]["idx"])
        if where:
            slots.append(where.rsplit(" ", 1)[-1])
        why = ("ran out of tape on its last write" if fill["hit_eom"]
               else f"at or below the {fill['reserve_pct']}% threshold")
        line = (f"• {vol}{f' ({where})' if where else ''} — "
                f"{bytes_human(fill['free_bytes'])} free of {bytes_human(fill['capacity_bytes'])}, {why}")
        lines.append(line)
        log_action("tape_full", True, f"{vol} is full — ready to swap out"
                   + (f" ({where})" if where else "") + f": {why}.")

    if not get_notify_config()["on_tape_full"]:
        return announce
    tokens = dict(
        vol=", ".join(announce), slot=", ".join(slots), count=str(len(announce)),
        tapes="\n".join(lines),
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("tape_full_title", **tokens)
    body  = _render_notify_template("tape_full_body",  **tokens)
    _ha_notify(title, body)
    return announce



# ---------------------------------------------------------------------------
# Backup records (persistent job history)
# ---------------------------------------------------------------------------

