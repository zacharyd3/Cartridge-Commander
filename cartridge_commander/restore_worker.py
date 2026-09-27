"""restore_worker module (split from the original monolithic app.py)."""

import os
import threading
import subprocess
from typing import Any, Dict, List, Optional
from .config import CHANGER, COMMAND_TIMEOUT, TAPE, TAPE_BLOCK_BYTES
from . import state as shared_state
from .state import TapeError, append_restore_log, is_cleaning_volume_tag, log_action, log_exit_codes, log_pipeline, log_traceback, now_ts, run_cmd, set_restore_state
from .changer import ensure_under_restore_root, refresh_state
from .drive_history import _record_restore_done, _save_last_known_loaded_slot
from .mqtt import publish_state_to_mqtt


def _plan_restore(volume_tag: str, tape_paths: List[str]) -> List[Dict[str, Any]]:
    """Work out which tape files to read for a restore.

    Returns [{"name", "chain", "paths"}], one per backup involved.  Paths are
    grouped by their top-level folder — each backup's archive folder — and
    each backup is read from wherever it starts, across as many tapes as it
    spans.  An empty path list means everything on ``volume_tag``.
    """
    from .tape_layout import find_session, get_sessions
    plans: Dict[tuple, Dict[str, Any]] = {}

    def _add(chain: List[Dict[str, Any]], name: str, paths: Optional[List[str]]) -> None:
        key = tuple((c["volume_tag"], int(c.get("file_number") or 0)) for c in chain)
        plan = plans.setdefault(key, {"name": name, "chain": chain, "paths": []})
        if paths is None:
            plan["paths"] = None
        elif plan["paths"] is not None:
            plan["paths"].extend(paths)

    if not tape_paths:
        sessions = get_sessions(volume_tag)
        if not sessions:
            # Written before sessions were tracked: one archive at the start.
            _add([{"volume_tag": volume_tag, "file_number": 0}], volume_tag, None)
        for s in sessions:
            if s.get("broken") or s.get("status") in ("failed", "cancelled"):
                append_restore_log(f"Skipping {s.get('dirname') or 'tape file ' + str(s.get('file_number'))}: "
                                   f"{'part of it was overwritten' if s.get('broken') else 'backup did not complete'}.")
                continue
            chain = s.get("chain") or [{"volume_tag": volume_tag, "file_number": int(s.get("file_number") or 0)}]
            _add(chain, s.get("dirname") or volume_tag, None)
        return list(plans.values())

    by_top: Dict[str, List[str]] = {}
    for p in tape_paths:
        by_top.setdefault(p.split("/", 1)[0], []).append(p)
    for top, paths in by_top.items():
        sess = find_session(top, prefer_vol=volume_tag)
        if sess and sess.get("broken"):
            raise TapeError(f"{top} can no longer be restored: part of it was overwritten.")
        if sess:
            _add(sess["chain"], top, paths)
        else:
            # Unknown to the catalog (e.g. written before positions were
            # tracked): it can only be the archive at the start of the tape.
            _add([{"volume_tag": volume_tag, "file_number": 0}], top, paths)
    return list(plans.values())


def restore_worker(volume_tag: str, tape_paths: List[str], dest: str, slot: Optional[int]) -> None:
    """
    Restore files from tape.
    tape_paths: list of paths as they appear in the tar archive.
                If empty, restore everything on the tape.
    dest: local destination directory.
    slot: kept for API compatibility — tapes are loaded as needed, since a
          backup may start on, or continue onto, other tapes.

    Supports cancellation via /api/restore/stop — sets shared_state._stop_restore which
    terminates the tar process and marks the job cancelled.
    """
    from .tape_layout import ChainReader, chain_label, loaded_volume
    from .backup_worker import _find_return_slot
    from .db import update_tape_index_metadata
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

    initially_loaded = ""
    try:
        os.makedirs(dest, exist_ok=True)
        initially_loaded = loaded_volume()
        plans = _plan_restore(volume_tag, tape_paths)
        if not plans:
            raise TapeError(f"Nothing restorable found on {volume_tag}.")
        tapes_needed = []
        for plan in plans:
            for c in plan["chain"]:
                if c["volume_tag"] not in tapes_needed:
                    tapes_needed.append(c["volume_tag"])
        if len(tapes_needed) > 1 or tapes_needed != [volume_tag]:
            append_restore_log(f"Tapes needed: {', '.join(tapes_needed)}.")

        count = 0
        for n, plan in enumerate(plans, 1):
            if shared_state._stop_restore:
                break
            chain = plan["chain"]
            prefix = f"[{n}/{len(plans)}] " if len(plans) > 1 else ""
            # Build tar extract command — use dd | tar so block size matches what was written.
            # tar reading directly from the tape device uses the wrong block size (512 B)
            # which causes ENOMEM on drives that wrote at 512 KiB blocks.
            tar_paths = [p.lstrip("/") for p in (plan["paths"] or [])]
            tar_cmd = ["tar", "-C", dest, "-xvf", "-"] + tar_paths
            append_restore_log(
                f"{prefix}Extracting {'all of ' + plan['name'] if not tar_paths else str(len(tar_paths))+' path(s)'} "
                f"to {dest}…  (dd bs={TAPE_BLOCK_BYTES//1024}KiB | tar -x)"
                + (f" — backup spans {len(chain)} tapes: {chain_label(chain)}" if len(chain) > 1
                   else f" — {chain[0]['volume_tag']}, tape file {chain[0].get('file_number', 0)}")
            )
            set_restore_state(status="extracting")
            publish_state_to_mqtt(refresh_state())

            log_pipeline("restore", ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"], tar_cmd)
            read_fd, write_fd = os.pipe()
            tar_proc = subprocess.Popen(
                tar_cmd,
                stdin=read_fd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Close our copy of the read end so that when tar exits the dd feeding
            # the pipe gets SIGPIPE and exits instead of hanging.
            os.close(read_fd)
            shared_state._restore_proc = tar_proc
            _dd_holder: List[Any] = [None]
            reader = ChainReader(chain, write_fd, log=append_restore_log,
                                 should_stop=lambda: shared_state._stop_restore,
                                 proc_holder=lambda p: _dd_holder.__setitem__(0, p))
            reader.start()

            _tar_stderr_lines: List[str] = []

            def _drain_tar_stderr():
                try:
                    for raw in tar_proc.stderr:
                        _tar_stderr_lines.append(raw.decode(errors="ignore").rstrip())
                except Exception:
                    pass

            t_err = threading.Thread(target=_drain_tar_stderr, daemon=True)
            t_err.start()

            # Drain stdout (verbose file list) — this is what drives progress.
            # No timeout: a full restore of hundreds of GB can take many hours.
            for raw_line in tar_proc.stdout:
                if shared_state._stop_restore:
                    append_restore_log("Stop requested — terminating restore.")
                    for _p in (tar_proc, _dd_holder[0]):
                        try:
                            if _p is not None:
                                _p.terminate()
                        except Exception:
                            pass
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
            rc = tar_proc.wait()
            if shared_state._stop_restore and _dd_holder[0] is not None:
                try:
                    _dd_holder[0].terminate()
                except Exception:
                    pass
            reader.join(timeout=60)
            log_exit_codes("restore", tar=rc)
            shared_state._restore_proc = None

            tar_err_text = "\n".join(_tar_stderr_lines[-10:])

            if shared_state._stop_restore:
                break

            # A failed read (tape missing, EIO, …) must not look like a restore
            # that simply found nothing.
            if reader.error:
                raise TapeError(
                    f"{reader.error}. Check that the tape is in the library and the drive is ready."
                    + (f" dd stderr: {' | '.join(reader.dd_stderr[-3:])[-200:]}" if reader.dd_stderr else "")
                )
            append_restore_log(f"{prefix}Read {reader.bytes_read:,} bytes from tape.")

            if rc not in (0, 1):  # tar rc=1 = warnings (e.g. timestamps)
                detail = tar_err_text.strip()[-300:] or f"tar exited rc={rc}"
                raise TapeError(f"tar exited rc={rc}: {detail}")

            if tar_err_text.strip():
                append_restore_log(f"tar warnings: {tar_err_text.strip()[-200:]}")

        if shared_state._stop_restore:
            set_restore_state(
                running=False, status="cancelled", finished_at=now_ts(),
                last_message=f"Restore cancelled after {count:,} files.", error=None,
            )
            append_restore_log(f"Restore cancelled by user after {count:,} files.")
            log_action("restore", True, f"Cancelled after {count} files from {volume_tag}")
            return

        set_restore_state(
            running=False, status="completed", finished_at=now_ts(),
            last_message=f"Restore complete — {count:,} files extracted to {dest}.", error=None,
        )
        append_restore_log(f"Restore complete. {count:,} files extracted.")
        log_action("restore", True, f"Restored {len(tape_paths) or 'all'} path(s) from {', '.join(tapes_needed)} → {dest}")
        for _v in tapes_needed:
            _record_restore_done(_v)

    except Exception as e:
        shared_state._restore_proc = None
        set_restore_state(running=False, status="failed", finished_at=now_ts(),
                          error=str(e), last_message=f"Restore failed: {e}")
        append_restore_log(f"Restore failed: {e}")
        log_action("restore", False, str(e))
        log_traceback("restore", e)
    finally:
        shared_state._restore_proc = None
        shared_state._stop_restore = False
        # Put back any tape this restore loaded; leave a tape that was already
        # in the drive where it was.
        try:
            now_loaded = loaded_volume()
            if now_loaded and now_loaded != initially_loaded:
                ret = _find_return_slot(now_loaded)
                if ret:
                    append_restore_log(f"Unloading {now_loaded} back to slot {ret}…")
                    run_cmd(["mtx","-f",CHANGER,"unload",str(ret),"0"], timeout=max(COMMAND_TIMEOUT,120))
                    _save_last_known_loaded_slot(None)
                    update_tape_index_metadata(now_loaded, present=True, last_seen_slot=ret)
        except Exception as ue:
            append_restore_log(f"Warning: could not unload: {ue}")
        publish_state_to_mqtt(refresh_state())

# ---------------------------------------------------------------------------
# Format (erase) worker
# ---------------------------------------------------------------------------

