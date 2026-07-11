"""routes module (split from the original monolithic app.py)."""

import os
import json
import time
import signal
import datetime
import threading
import hmac
from typing import Optional
from flask import jsonify, request, render_template, send_from_directory
from .flaskapp import app
from .config import AUTO_REWIND_AFTER, BACKUP_LOG_LEVEL_DEFAULT, BACKUP_ROOT, CHANGER, CLEANING_WAIT_SECONDS, COMMAND_TIMEOUT, ERASE_BEFORE_BACKUP, GFS_DAILY_KEEP, GFS_MONTHLY_KEEP, GFS_WEEKLY_KEEP, ICON_PATH, POLL_SECONDS, POST_BACKUP_HOOK, PRE_BACKUP_HOOK, RESTORE_ROOT, SG_DEVICE, TAPE, TAPE_BLOCK_BYTES, VERIFY_AFTER_BACKUP, VERIFY_SAMPLE_MB, WEBUI_PASSWORD
from . import state as shared_state
from .state import TapeError, append_inventory_log, current_backup_log_level, is_cleaning_volume_tag, log_action, normalize_backup_log_level, now_ts, request_inventory_pause, request_inventory_resume, request_inventory_stop, run_cmd, set_changer_state, snapshot_backup_job, snapshot_changer_job, snapshot_format_job, snapshot_inventory_job, snapshot_restore_job, snapshot_verify_job
from .settings import _NOTIFY_DEFAULT_TEMPLATES, build_restore_dest, get_ha_config, get_notify_config, get_restore_subfolder_pattern, set_ha_config, set_notify_config, set_restore_subfolder_pattern
from .changer import ensure_under_backup_root, ensure_under_restore_root, get_cleaning_slot, get_mail_slot_info, list_directories, list_restore_directories, refresh_state
from .db import delete_tape_index, list_all_known_indexes, load_tape_index, mark_tape_archived, read_tape_index_live, save_tape_index, update_tape_index_metadata
from .drive_history import _save_last_known_loaded_slot, build_loaded_tape_space_info, get_drive_info, get_effective_loaded_slot, space_meta_from_info
from .records import get_backup_records, get_tape_health, gfs_classify, gfs_get_recyclable
from .mqtt import publish_state_to_mqtt
from .scheduler import _save_schedules, _update_next_run
from .backup_worker import backup_worker
from .restore_worker import restore_worker
from .format_worker import format_worker
from .inventory_worker import inventory_worker
from .verify_worker import verify_worker


def require_password():
    if not WEBUI_PASSWORD: return None
    if not hmac.compare_digest(request.headers.get("X-API-Key",""), WEBUI_PASSWORD):
        return jsonify({"ok":False,"error":"Unauthorized"}), 401
    return None

def do_action(kind, fn):
    auth = require_password()
    if auth is not None: return auth
    with shared_state._action_lock:
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
    with shared_state._drive_history_lock:
        if vol:
            return jsonify({"ok": True, "history": shared_state._drive_history.get(vol, {})})
        return jsonify({"ok": True, "history": dict(shared_state._drive_history)})

@app.get("/api/status")
def api_status():
    auth = require_password()
    if auth is not None: return auth
    state = refresh_state()
    return jsonify({**state, "actions": shared_state._action_log[:50], "drive_info": get_drive_info(),
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
    vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume","")
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
        drive = shared_state._state_cache.get("drive", {}) or {}
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

        vol = (drive.get("volume_tag") or (shared_state._state_cache.get("summary") or {}).get("loaded_volume") or "unknown").strip() or "unknown"
        loaded_slot = get_effective_loaded_slot()
        slot_info = next((s for s in (shared_state._state_cache.get("slots") or []) if s.get("slot") == loaded_slot), None)
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

    with shared_state._changer_lock:
        if shared_state._changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409
    with shared_state._backup_lock:
        if shared_state._backup_job.get("running"):
            return jsonify({"ok": False, "error": "A backup is running — cannot re-index now."}), 409
    with shared_state._inventory_lock:
        if shared_state._inventory_job.get("running"):
            return jsonify({"ok": False, "error": "Inventory is running — cannot re-index now."}), 409

    def _run():
        loaded_slot_here: Optional[int] = None
        try:
            refresh_state()
            drive = (shared_state._state_cache.get("drive") or {})
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

            target_vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "") or vol

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
    with shared_state._changer_lock:
        if shared_state._changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409

    def _run():
        set_changer_state(running=True, action="load", status="loading",
                          detail=f"Loading slot {slot} into drive…",
                          error=None, started_at=now_ts(), finished_at=None)
        publish_state_to_mqtt(refresh_state())
        try:
            refresh_state()
            drive = (shared_state._state_cache.get("drive") or {})
            if not drive.get("empty"):
                loaded_vol = (drive.get("volume_tag") or "").strip()
                loaded_slot = get_effective_loaded_slot()
                if not loaded_vol and not loaded_slot:
                    _save_last_known_loaded_slot(None)
                    refresh_state()
                    drive2 = (shared_state._state_cache.get("drive") or {})
                    if not drive2.get("empty"):
                        loaded_vol = (drive2.get("volume_tag") or "unknown").strip() or "unknown"
                        loaded_slot = get_effective_loaded_slot()
                        raise TapeError(f"Drive already has tape {loaded_vol or 'unknown'} loaded{f' from slot {loaded_slot}' if loaded_slot else ''}. Unload it first.")
                else:
                    raise TapeError(f"Drive already has tape {loaded_vol or 'unknown'} loaded{f' from slot {loaded_slot}' if loaded_slot else ''}. Unload it first.")
            slot_info = next((s for s in (shared_state._state_cache.get("slots") or []) if s.get("slot") == slot), None)
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
    with shared_state._changer_lock:
        if shared_state._changer_job.get("running"):
            return jsonify({"ok": False, "error": "A changer operation is already in progress."}), 409

    def _run():
        set_changer_state(running=True, action="unload", status="unloading",
                          detail=f"Unloading tape to slot {slot}…",
                          error=None, started_at=now_ts(), finished_at=None)
        publish_state_to_mqtt(refresh_state())
        try:
            refresh_state()
            drive = (shared_state._state_cache.get("drive") or {})
            if drive.get("empty"):
                raise TapeError("Drive is already empty.")
            slot_info = next((s for s in (shared_state._state_cache.get("slots") or []) if s.get("slot") == slot), None)
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
    with shared_state._backup_lock:
        if shared_state._backup_job.get("running"):
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
    with shared_state._backup_lock:
        if not shared_state._backup_job.get("running"):
            return jsonify({"ok":False,"error":"No backup running."}), 409
    shared_state._stop_requested = True
    if shared_state._tar_proc:
        try: shared_state._tar_proc.send_signal(signal.SIGTERM)
        except Exception: pass
    log_action("stop_backup",True,"Stop requested.")
    return jsonify({"ok":True,"detail":"Cancel requested — tar will stop after the current file(s)."})


@app.post("/api/restore/stop")
def api_restore_stop():
    auth = require_password()
    if auth is not None: return auth
    with shared_state._restore_lock:
        if not shared_state._restore_job.get("running"):
            return jsonify({"ok": False, "error": "No restore running."}), 409
    shared_state._stop_restore = True
    if shared_state._restore_proc:
        try: shared_state._restore_proc.send_signal(signal.SIGTERM)
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

    with shared_state._format_lock:
        if shared_state._format_job.get("running"):
            return jsonify({"ok": False, "error": "A format job is already running."}), 409
    with shared_state._backup_lock:
        if shared_state._backup_job.get("running"):
            return jsonify({"ok": False, "error": "A backup is running — cannot format now."}), 409
    with shared_state._inventory_lock:
        if shared_state._inventory_job.get("running"):
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
    with shared_state._format_lock:
        if not shared_state._format_job.get("running"):
            return jsonify({"ok": False, "error": "No format job running."}), 409
    shared_state._stop_format = True
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
        drive = (shared_state._state_cache.get("drive") or {})
        summary = (shared_state._state_cache.get("summary") or {})
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
            slot_info = next((s for s in (shared_state._state_cache.get("slots") or []) if int(s.get("slot",0)) == int(cleaning_slot)), None)
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
            cur_drive = (shared_state._state_cache.get("drive") or {})
            if cur_drive.get("empty"):
                _save_last_known_loaded_slot(None)
                return f"Cleaning tape completed and drive is empty."

        refresh_state()
        cur_drive = (shared_state._state_cache.get("drive") or {})
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
        state = shared_state._state_cache
        mail_slot = get_mail_slot_info(state)
        if not mail_slot:
            raise TapeError("No import/export slot detected on this library.")
        if mail_slot.get("full"):
            raise TapeError(f"Mail slot {mail_slot.get('slot')} already has tape {(mail_slot.get('volume_tag') or '').strip() or 'loaded'}. Import or remove it first.")
        drive = state.get("drive", {}) or {}
        effective_loaded = drive.get("loaded_from_slot") or shared_state._last_known_loaded_slot
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
        state = shared_state._state_cache
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
    with shared_state._restore_lock:
        if shared_state._restore_job.get("running"):
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
    with shared_state._inventory_lock:
        if shared_state._inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory already running."}), 409
    threading.Thread(target=inventory_worker, kwargs={"mode": mode}, daemon=True).start()
    return jsonify({"ok":True,"detail":f"{mode.title()} inventory started."})

@app.post("/api/inventory/pause")
def api_inventory_pause():
    auth = require_password()
    if auth is not None: return auth
    with shared_state._inventory_lock:
        if not shared_state._inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_pause()
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory paused."})

@app.post("/api/inventory/resume")
def api_inventory_resume():
    auth = require_password()
    if auth is not None: return auth
    with shared_state._inventory_lock:
        if not shared_state._inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_resume()
    append_inventory_log("Inventory resumed.")
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory resumed."})

@app.post("/api/inventory/stop")
def api_inventory_stop():
    auth = require_password()
    if auth is not None: return auth
    with shared_state._inventory_lock:
        if not shared_state._inventory_job.get("running"):
            return jsonify({"ok":False,"error":"Inventory is not running."}), 409
    request_inventory_stop()
    append_inventory_log("Stop requested — inventory will stop after the current step.")
    publish_state_to_mqtt(refresh_state())
    return jsonify({"ok":True,"detail":"Inventory stop requested."})

@app.get("/api/schedules")
def api_schedules_get():
    auth = require_password()
    if auth is not None: return auth
    with shared_state._schedules_lock: return jsonify({"ok":True,"schedules":list(shared_state._schedules)})

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
    with shared_state._schedules_lock: shared_state._schedules.append(s)
    _save_schedules()
    return jsonify({"ok":True,"schedule":s})

@app.put("/api/schedules/<sid>")
def api_schedules_update(sid):
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    found = None
    with shared_state._schedules_lock:
        for s in shared_state._schedules:
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
    with shared_state._schedules_lock:
        before = len(shared_state._schedules)
        shared_state._schedules[:] = [s for s in shared_state._schedules if s["id"] != sid]
        if len(shared_state._schedules) == before: return jsonify({"ok":False,"error":"Not found."}), 404
    _save_schedules(); return jsonify({"ok":True})

@app.post("/api/verify/start")
def api_verify_start():
    auth = require_password()
    if auth is not None: return auth
    p = request.get_json(silent=True) or {}
    vol = p.get("volume_tag") or (shared_state._state_cache.get("summary") or {}).get("loaded_volume", "")
    if not vol:
        return jsonify({"ok": False, "error": "No tape loaded and no volume_tag given."}), 400
    with shared_state._verify_lock:
        if shared_state._verify_job.get("running"):
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
    with shared_state._backup_records_lock:
        recs = list(shared_state._backup_records)
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
        "mail_slot_present": bool(get_mail_slot_info(refresh_state() if not shared_state._state_cache.get("slots") else shared_state._state_cache)),
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



