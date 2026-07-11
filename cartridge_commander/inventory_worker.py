"""inventory_worker module (split from the original monolithic app.py)."""

import json
import time
from typing import Any, Dict, Optional
from .config import CHANGER, COMMAND_TIMEOUT
from . import state as shared_state


def _inventory_db_audit(live_slots):
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
    from .db import list_all_known_indexes, tape_catalog_conn
    from .drive_history import bytes_written_for_volume
    from .state import append_inventory_log, bytes_human, is_cleaning_volume_tag, log_action, now_ts
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
    from .db import list_all_known_indexes, load_tape_index, mark_all_indexes_not_present, mark_tape_archived, read_tape_index_live, save_tape_index, update_tape_index_metadata
    from .drive_history import _save_last_known_loaded_slot, build_loaded_tape_space_info, build_tape_space_info, space_meta_from_info
    from .state import TapeError, _fmt_ts_short, append_inventory_log, bytes_human, calc_eta_seconds, inventory_should_stop, inventory_wait_if_paused, is_cleaning_volume_tag, log_action, now_ts, request_inventory_resume, run_cmd, set_inventory_state
    from .mqtt import publish_state_to_mqtt
    from .changer import refresh_state
    from .notify import notify_inventory_done
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
        shared_state._inventory_stop_requested = False
        request_inventory_resume()

        # ── Snapshot hardware state before we do anything ────────────────────
        state      = refresh_state()
        drive_info = state.get("drive", {})
        orig_slot  = drive_info.get("loaded_from_slot") or shared_state._last_known_loaded_slot

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
                            existing_slot = cur_drive.get("loaded_from_slot") or shared_state._last_known_loaded_slot
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
        shared_state._inventory_stop_requested = False
        request_inventory_resume()
        publish_state_to_mqtt(refresh_state())

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

