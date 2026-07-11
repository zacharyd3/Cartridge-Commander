"""format_worker module (split from the original monolithic app.py)."""

import time
from typing import Any, Dict, List
from .config import CHANGER, COMMAND_TIMEOUT, TAPE
from . import state as shared_state
from .state import append_format_log, log_action, now_ts, run_cmd, set_format_state
from .changer import refresh_state
from .db import tape_catalog_conn
from .drive_history import _save_last_known_loaded_slot
from .records import _save_backup_records
from .notify import notify_format_complete
from .mqtt import publish_state_to_mqtt


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

    A stop flag (shared_state._stop_format) is checked between tapes so the user can
    cancel the queue without killing a tape that is mid-erase.
    """
    shared_state._stop_format = False

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
        if shared_state._stop_format:
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
                drive_slot = cur_drive.get("loaded_from_slot") or shared_state._last_known_loaded_slot
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
            with shared_state._backup_records_lock:
                before = len(shared_state._backup_records)
                shared_state._backup_records[:] = [r for r in shared_state._backup_records if r.get("volume_tag") != vol]
                removed = before - len(shared_state._backup_records)
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
        + ("." if not shared_state._stop_format else " (stopped by user).")
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
    shared_state._stop_format = False
    notify_format_complete(
        [t["volume_tag"] for t in done],
        [t["volume_tag"] for t in failed],
    )
    refresh_state()
    publish_state_to_mqtt(refresh_state())
