"""restore_worker module (split from the original monolithic app.py)."""

import os
import time
import threading
import subprocess
from typing import List, Optional
from .config import CHANGER, COMMAND_TIMEOUT, TAPE, TAPE_BLOCK_BYTES
from . import state as shared_state
from .state import TapeError, append_restore_log, is_cleaning_volume_tag, log_action, now_ts, run_cmd, set_restore_state
from .changer import ensure_under_restore_root, refresh_state
from .drive_history import _record_restore_done, _save_last_known_loaded_slot
from .mqtt import publish_state_to_mqtt


def restore_worker(volume_tag: str, tape_paths: List[str], dest: str, slot: Optional[int]) -> None:
    """
    Restore files from tape.
    tape_paths: list of paths as they appear in the tar archive.
                If empty, restore everything.
    dest: local destination directory.
    slot: if set, load this slot first (then unload after).

    Supports cancellation via /api/restore/stop — sets shared_state._stop_restore which
    terminates the tar process and marks the job cancelled.
    """
    if is_cleaning_volume_tag(volume_tag):
        raise TapeError(f"{volume_tag} is a cleaning tape and cannot be restored.")

    dest = ensure_under_restore_root(dest)
    shared_state._stop_restore = False

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
            cur = (shared_state._state_cache.get("drive") or {})
            if not cur.get("empty"):
                existing = cur.get("loaded_from_slot") or shared_state._last_known_loaded_slot
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
        shared_state._restore_proc = tar_proc

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
            if shared_state._stop_restore:
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
        shared_state._restore_proc = None

        tar_err_text = "\n".join(_tar_stderr_lines[-10:])

        # FIX: check dd exit code.  dd exits non-zero on read errors (e.g. EIO,
        # ENOMEDIUM).  Previously this was never checked so a completely failed
        # read (0 bytes transferred) looked identical to a successful restore.
        if dd_rc not in (0, None) and not shared_state._stop_restore:
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

        if shared_state._stop_restore:
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
        shared_state._restore_proc = None
        set_restore_state(running=False, status="failed", finished_at=now_ts(),
                          error=str(e), last_message=f"Restore failed: {e}")
        append_restore_log(f"Restore failed: {e}")
        log_action("restore", False, str(e))
    finally:
        shared_state._restore_proc = None
        shared_state._stop_restore = False
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

