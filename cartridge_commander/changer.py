"""changer module (split from the original monolithic app.py)."""

import os
import re
from dataclasses import asdict
from typing import Any, Dict, Optional
from .config import BACKUP_ROOT, CHANGER, HAS_MAIL_SLOT, MAGAZINE_SIZE, RESTORE_ROOT, TAPE
from . import state as shared_state


MTX_SLOT_RE  = re.compile(r"^\s*Storage Element\s+(\d+)(\s+IMPORT/EXPORT)?\s*:\s*(Full|Empty)(?:\s*:\s*VolumeTag\s*=\s*(.*?))?\s*$", re.I)
MTX_DRIVE_RE = re.compile(r"^\s*Data Transfer Element\s+(\d+)\s*:\s*(Empty|Full)(?:\s*\(\s*Storage Element\s+(\d+)\s+Loaded\s*\))?(?:\s*:\s*VolumeTag\s*=\s*(.*?))?\s*$", re.I)
DENSITY_RE   = re.compile(r"Density code .*?\((.*?)\)")
STATUS_RE    = re.compile(r"General status bits on \((.*?)\):\s*(.*)")

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
    from .state import Slot
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
    from .state import Drive
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
    from .drive_history import get_drive_info
    from .state import TapeError, now_ts, run_cmd, snapshot_backup_job, snapshot_format_job, snapshot_inventory_job, snapshot_restore_job, snapshot_verify_job
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
    effective_loaded_slot = drive["loaded_from_slot"] or shared_state._last_known_loaded_slot
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
    from .drive_history import _check_drive_change
    from .state import now_ts, snapshot_backup_job, snapshot_inventory_job, snapshot_restore_job
    try:
        shared_state._state_cache = collect_state()
    except Exception as e:
        shared_state._state_cache = {**shared_state._state_cache, "ok": False,
                        "backup_job": snapshot_backup_job(),
                        "restore_job": snapshot_restore_job(),
                        "inventory_job": snapshot_inventory_job(),
                        "last_error": str(e), "last_updated": now_ts()}
    _check_drive_change()
    return shared_state._state_cache

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
    from .state import TapeError
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
    from .state import TapeError
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
    from .state import is_cleaning_volume_tag
    for s in (shared_state._state_cache.get("slots") or []):
        if s.get("full") and is_cleaning_volume_tag(s.get("volume_tag", "")):
            return int(s.get("slot"))
    return (shared_state._state_cache.get("summary") or {}).get("cleaning_slot")


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

