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


def gfs_stream_key(record: Dict[str, Any]) -> str:
    """Return the retention stream a record belongs to.

    Retention is applied independently per job so that separate schedules
    (e.g. a Monday "appdata" job and a Friday "photos" job) each keep their own
    daily/weekly/monthly history instead of competing for shared slots.

    Scheduled jobs carry a stable label (the schedule's name) which is used as
    the stream key.  Ad-hoc backups started without a label are stored with an
    auto-generated job id of ``{volume_tag}_{started_at}``; those are collapsed
    into one shared "" stream so an occasional manual backup doesn't pin a tape
    forever as the sole member of its own stream.
    """
    label = str(record.get("label") or "").strip()
    started = record.get("started_at")
    if label and started is not None:
        vol = record.get("volume_tag") or ""
        auto_job_id = f"{vol or 'nolabel'}_{int(started)}"
        if label == auto_job_id:
            return ""
    return label


def _gfs_completed_streams() -> Dict[str, List[Dict[str, Any]]]:
    """Completed, tagged backups partitioned by stream key, each chronological."""
    with shared_state._backup_records_lock:
        records = sorted(shared_state._backup_records, key=lambda r: r.get("started_at", 0))
    streams: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        if rec.get("status") == "completed" and rec.get("started_at") and rec.get("volume_tag"):
            streams.setdefault(gfs_stream_key(rec), []).append(rec)
    return streams


def _gfs_stream_keep(stream: List[Dict[str, Any]], cfg: Dict[str, int]):
    """Return (monthly, weekly, daily) kept volume-tag sets for a single stream.

    ``stream`` must be in chronological order.  Each tier keeps the oldest
    backup per calendar window (month / ISO week), and daily keeps the most
    recent N backups in this stream.
    """
    monthly_rep: Dict[str, str] = {}   # "YYYY-MM" → volume_tag of oldest record
    weekly_rep:  Dict[str, str] = {}   # "YYYY-WW" → volume_tag of oldest record
    for rec in stream:
        dt = datetime.datetime.fromtimestamp(rec["started_at"])
        vol = rec["volume_tag"]
        ym_key = dt.strftime("%Y-%m")
        if ym_key not in monthly_rep:
            monthly_rep[ym_key] = vol
        iso_year, iso_week, _ = dt.isocalendar()
        yw_key = f"{iso_year}-{iso_week:02d}"
        if yw_key not in weekly_rep:
            weekly_rep[yw_key] = vol

    keep_monthly = set(_last_n(list(monthly_rep.values()), cfg["monthly"]))
    keep_weekly  = set(_last_n(list(weekly_rep.values()),  cfg["weekly"]))
    recent_vols  = [r["volume_tag"] for r in reversed(stream)]
    keep_daily   = set(recent_vols[:cfg["daily"]])
    return keep_monthly, keep_weekly, keep_daily


def gfs_classify(record: Dict[str, Any]) -> str:
    """Classify a backup record for display purposes.

    Classification is done within the record's own retention stream, using
    calendar windows (month / ISO week) rather than weekday checks so it stays
    stable regardless of which day backups run.
    """
    ts = record.get("started_at")
    if not ts:
        return "expired"

    vol = record.get("volume_tag", "")
    recyclable_set = set(gfs_get_recyclable())

    if vol and vol not in recyclable_set and record.get("status") == "completed":
        cfg = get_gfs_config()
        stream = _gfs_completed_streams().get(gfs_stream_key(record), [])
        keep_monthly, keep_weekly, _keep_daily = _gfs_stream_keep(stream, cfg)
        if vol in keep_monthly:
            return "monthly"
        if vol in keep_weekly:
            return "weekly"
        return "daily"

    return "expired" if vol in recyclable_set else "daily"


def gfs_get_recyclable() -> List[str]:
    """Apply GFS retention and return volume_tags safe to reuse.

    Retention runs independently per job stream (see ``gfs_stream_key``) so
    separate schedules each keep their own history.  The keep counts come from
    the runtime GFS config (get_gfs_config), which seeds from the GFS_*_KEEP env
    vars but is editable/persisted from the UI.  Within each stream it keeps:

      - The oldest completed backup in each of the last ``monthly`` calendar months.
      - The oldest completed backup in each of the last ``weekly`` ISO weeks.
      - The most recent ``daily`` completed backups.

    A tape is recyclable only when it is kept by *no* stream, so a tape shared
    between jobs is retained as long as any one job still needs it.
    """
    cfg = get_gfs_config()
    streams = _gfs_completed_streams()

    keep_all: set = set()
    for stream in streams.values():
        keep_monthly, keep_weekly, keep_daily = _gfs_stream_keep(stream, cfg)
        keep_all |= keep_monthly | keep_weekly | keep_daily

    # Any tag not kept by any stream is recyclable.  De-duplicate while
    # preserving chronological (oldest-first) order across all streams.
    all_completed = sorted(
        (rec for stream in streams.values() for rec in stream),
        key=lambda r: r["started_at"],
    )
    seen: set = set()
    recyclable: List[str] = []
    for rec in all_completed:
        vol = rec["volume_tag"]
        if vol not in keep_all and vol not in seen:
            seen.add(vol)
            recyclable.append(vol)

    return recyclable


# ---------------------------------------------------------------------------
# Incremental backup support
# ---------------------------------------------------------------------------

