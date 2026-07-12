"""records module (split from the original monolithic app.py)."""

import os
import re
import json
import datetime
import subprocess
from typing import Any, Dict, List, Optional
from .config import BACKUP_RECORDS_FILE, SG_DEVICE
from .settings import get_gfs_config
from . import state as shared_state


def _load_backup_records() -> None:
    from .db import _db_get_json, _db_set_json
    data = _db_get_json("backup_records", None)
    if isinstance(data, list):
        shared_state._backup_records = data
        return
    os.makedirs(os.path.dirname(BACKUP_RECORDS_FILE), exist_ok=True)
    if not os.path.exists(BACKUP_RECORDS_FILE):
        shared_state._backup_records = []
        return
    try:
        with open(BACKUP_RECORDS_FILE) as f:
            shared_state._backup_records = json.load(f)
        _db_set_json("backup_records", shared_state._backup_records[-500:])
    except Exception:
        shared_state._backup_records = []


def _save_backup_records() -> None:
    from .db import _db_set_json
    with shared_state._backup_records_lock:
        payload = list(shared_state._backup_records[-500:])
    _db_set_json("backup_records", payload)


def add_backup_record(rec: Dict[str, Any]) -> None:
    with shared_state._backup_records_lock:
        shared_state._backup_records.insert(0, rec)
    _save_backup_records()


def get_backup_records(vol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    with shared_state._backup_records_lock:
        recs = list(shared_state._backup_records)
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

def _last_n(seq: List[str], n: int) -> List[str]:
    """Return the last ``n`` items, or an empty list when ``n`` <= 0.

    ``seq[-0:]`` is ``seq[0:]`` (the whole list), so a plain negative slice
    can't express "keep none" — this guards against that.
    """
    return list(seq[-n:]) if n > 0 else []


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
        with shared_state._backup_records_lock:
            records = sorted(shared_state._backup_records, key=lambda r: r.get("started_at", 0))
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

        cfg = get_gfs_config()
        keep_monthly = set(_last_n(list(monthly_rep.values()), cfg["monthly"]))
        keep_weekly  = set(_last_n(list(weekly_rep.values()),  cfg["weekly"]))

        if vol in keep_monthly:
            return "monthly"
        if vol in keep_weekly:
            return "weekly"
        return "daily"

    return "expired" if vol in recyclable_set else "daily"


def gfs_get_recyclable() -> List[str]:
    """Apply GFS retention and return volume_tags safe to reuse.

    The keep counts come from the runtime GFS config (get_gfs_config), which
    seeds from the GFS_*_KEEP env vars but is editable/persisted from the UI.

    Keeps:
      - The oldest completed backup in each of the last ``monthly`` calendar months.
      - The oldest completed backup in each of the last ``weekly`` ISO weeks
        (that aren't already kept as a monthly).
      - The most recent ``daily`` completed backups (that aren't already kept).

    Everything older than the above windows, and not in a keep set, is recyclable.
    """
    with shared_state._backup_records_lock:
        records = sorted(shared_state._backup_records, key=lambda r: r.get("started_at", 0))

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
    cfg = get_gfs_config()
    keep_monthly: set = set(_last_n(list(monthly_rep.values()), cfg["monthly"]))
    keep_weekly:  set = set(_last_n(list(weekly_rep.values()),  cfg["weekly"]))

    # Daily: the most recent N completed backups overall
    recent_vols = [r["volume_tag"] for r in reversed(completed)]
    keep_daily: set = set(recent_vols[:cfg["daily"]])

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

