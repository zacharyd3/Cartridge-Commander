"""verify_worker module (split from the original monolithic app.py)."""

import os
import time
import threading
import subprocess
from typing import Any, Dict, List, Optional
from .config import TAPE, TAPE_BLOCK_BYTES, VERIFY_SAMPLE_MB
from . import state as shared_state


def _chains_to_verify(vol: str, log) -> List[Dict[str, Any]]:
    """Every backup on ``vol`` worth verifying, as {"dirname", "chain"}.

    Backups are appended one after another, so a tape can hold several; one
    that started on another tape is verified from its first tape.  Partial
    (failed/cancelled) and broken backups are skipped: they are known to be
    incomplete.
    """
    from .tape_layout import get_sessions
    sessions = get_sessions(vol)
    if not sessions:
        # Written before sessions were tracked: one archive at the start.
        return [{"dirname": "", "chain": [{"volume_tag": vol, "file_number": 0}]}]
    present = {str(s.get("volume_tag") or "").strip()
               for s in (shared_state._state_cache.get("slots") or []) if s.get("full")}
    present.add(str((shared_state._state_cache.get("summary") or {}).get("loaded_volume") or "").strip())
    out, seen = [], set()
    for s in sessions:
        name = s.get("dirname") or f"file {s.get('file_number')}"
        if name in seen:
            continue
        seen.add(name)
        if s.get("status") in ("failed", "cancelled") or s.get("broken"):
            log(f"Skipping {name}: {'part of it was overwritten' if s.get('broken') else 'backup did not complete'}.")
            continue
        chain = s.get("chain") or [{"volume_tag": vol, "file_number": int(s.get("file_number") or 0)}]
        missing = [c["volume_tag"] for c in chain if c["volume_tag"] not in present]
        if missing:
            log(f"Skipping {name}: needs tape(s) not in the library: {', '.join(missing)}.")
            continue
        out.append({"dirname": s.get("dirname") or "", "chain": chain})
    return out


def verify_worker(vol: str, backup_record_id: Optional[str] = None,
                  chain: Optional[List[Dict[str, Any]]] = None) -> None:
    """
    Read back backups from tape and verify integrity.

    With ``chain`` (as passed after a backup), verifies that one backup, which
    may span several tapes.  Otherwise verifies every backup on ``vol``.

    Strategy:
      1. For each backup, position the drive at its tape file (rewind, then
         space forward over filemarks) — loading other tapes first when the
         backup spans several — and run dd over each segment in order into
         one pipe (tape_layout.ChainReader).
      2. Pipe that stream into  tar -t -f -  as a pure archive-readability
         check.  Both stdout and stderr of each process are drained
         concurrently to prevent pipe-buffer deadlocks.
      3. "Unexpected EOF in archive" is NOT treated as an error when sampling
         (VERIFY_SAMPLE_MB > 0) because dd deliberately truncates the stream
         mid-archive — tar hitting EOF there is expected and correct.
      4. At verbose log level the full stderr from both dd and tar is written to
         the verify log so it is trivial to diagnose any genuine failure.
    """
    from .records import _save_backup_records
    from .state import append_verify_log, backup_log_allows, bytes_human, calc_eta_seconds, log_action, log_pipeline, log_traceback, now_ts, set_verify_state
    from .mqtt import publish_state_to_mqtt
    from .changer import refresh_state
    from .notify import notify_verify_failure
    from .tape_layout import ChainReader, chain_label
    set_verify_state(
        running=True, status="preparing", volume_tag=vol,
        started_at=now_ts(), finished_at=None,
        bytes_verified=0, errors=0, eta_seconds=None,
        last_message="Starting verification…", log=[], error=None,
    )
    verbose = backup_log_allows("verbose")
    sampling = VERIFY_SAMPLE_MB > 0
    append_verify_log(
        f"Verification started for {vol}  "
        f"(block={TAPE_BLOCK_BYTES//1024}KiB, "
        f"sample={'full backup' if not sampling else str(VERIFY_SAMPLE_MB)+'MB per backup'}, "
        f"log={'verbose' if verbose else 'normal'})."
    )
    publish_state_to_mqtt(refresh_state())

    errors = 0
    bytes_verified = 0
    read_errors = 0
    files_total = 0

    try:
        if chain:
            targets = [{"dirname": "", "chain": chain}]
        else:
            refresh_state()
            targets = _chains_to_verify(vol, append_verify_log)
        if not targets:
            append_verify_log("Nothing to verify on this tape.")

        limit_bytes = VERIFY_SAMPLE_MB * 1024 * 1024 if sampling else None
        if sampling:
            append_verify_log(
                f"NOTE: Sampling mode — reading stops after {VERIFY_SAMPLE_MB} MB of each backup. "
                f"'Unexpected EOF' at the sample boundary is expected and not an error."
            )

        for n, target in enumerate(targets, 1):
            tchain = target["chain"]
            name = target["dirname"] or chain_label(tchain)
            prefix = f"[{n}/{len(targets)}] " if len(targets) > 1 else ""
            append_verify_log(
                f"{prefix}Verifying {name}"
                + (f" — spans {len(tchain)} tapes: {chain_label(tchain)}" if len(tchain) > 1
                   else f" (tape file {tchain[0].get('file_number', 0)})"))
            set_verify_state(status="reading_data")
            publish_state_to_mqtt(refresh_state())

            # ── dd (per segment) | tar -t ───────────────────────────────────
            read_fd, write_fd = os.pipe()
            log_pipeline("verify", ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"],
                         ["tar", "-t", "-f", "-"])
            tar_proc = subprocess.Popen(
                ["tar", "-t", "-f", "-"],
                stdin=read_fd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            os.close(read_fd)   # tar owns the read end
            reader = ChainReader(tchain, write_fd, log=append_verify_log,
                                 should_stop=lambda: False, limit_bytes=limit_bytes)
            reader.start()

            verify_started = now_ts()
            tar_files_seen = [0]
            _tar_stderr_lines: List[str] = []

            def _drain_tar_stdout():
                try:
                    for _ in tar_proc.stdout:
                        tar_files_seen[0] += 1
                except Exception:
                    pass

            def _drain_tar_stderr():
                try:
                    for raw in tar_proc.stderr:
                        _tar_stderr_lines.append(raw.decode(errors="ignore").rstrip())
                except Exception:
                    pass

            t_tar_out = threading.Thread(target=_drain_tar_stdout, daemon=True)
            t_tar_err = threading.Thread(target=_drain_tar_stderr, daemon=True)
            t_tar_out.start(); t_tar_err.start()

            base = bytes_verified
            while tar_proc.poll() is None:
                time.sleep(3)
                bytes_verified = base + reader.bytes_read
                eta_v = calc_eta_seconds(verify_started, reader.bytes_read, limit_bytes) if limit_bytes else None
                set_verify_state(
                    bytes_verified=bytes_verified,
                    last_message=f"{prefix}Verified {tar_files_seen[0]:,} entries, {bytes_human(bytes_verified)} read…",
                    eta_seconds=eta_v,
                )
                publish_state_to_mqtt(refresh_state())

            t_tar_out.join(timeout=15)
            t_tar_err.join(timeout=15)
            # Close tar's pipes now that we've drained them
            for _pipe in (tar_proc.stdout, tar_proc.stderr):
                try:
                    _pipe.close()
                except Exception:
                    pass
            tar_rc = tar_proc.wait(timeout=30)
            reader.join(timeout=120)
            bytes_verified = base + reader.bytes_read
            files_total += tar_files_seen[0]

            # Brief settle — give the st driver a moment to fully release the
            # device after dd exits before anything else touches the drive.
            time.sleep(1)

            # ── Log diagnostics (always show process results; full stderr if verbose or error) ──
            append_verify_log(
                f"Process results: tar rc={tar_rc}, "
                f"files seen={tar_files_seen[0]:,}, bytes read={bytes_human(reader.bytes_read)}"
                + (f", read error: {reader.error}" if reader.error else "")
            )
            if verbose or tar_rc not in (0, 1):
                for _l in (_tar_stderr_lines[:50] if _tar_stderr_lines else ["(empty)"]):
                    append_verify_log(f"  tar stderr: {_l[:300]}")
            if verbose or reader.error:
                for _l in (reader.dd_stderr[-10:] if reader.dd_stderr else ["(empty)"]):
                    append_verify_log(f"  dd stderr: {_l[:300]}")

            # ── Evaluate result ─────────────────────────────────────────────
            # Key rule: "Unexpected EOF in archive" when sampling is NOT an error.
            # dd stopped feeding data at the sample limit mid-archive; tar seeing
            # EOF there is the designed behaviour, not a tape defect.
            failed = False
            if reader.error:
                failed = True
            elif tar_rc not in (0, 1):
                tar_err_text = " ".join(_tar_stderr_lines).lower()
                unexpected_eof = "unexpected eof" in tar_err_text or "eof in archive" in tar_err_text
                if sampling and unexpected_eof:
                    append_verify_log(
                        f"ℹ tar rc={tar_rc} with 'Unexpected EOF' — this is normal when sampling "
                        f"({bytes_human(limit_bytes)} limit reached mid-archive). Not counted as error."
                    )
                else:
                    failed = True
            elif _tar_stderr_lines and (verbose or tar_rc == 1):
                for _l in _tar_stderr_lines[:10]:
                    append_verify_log(f"ℹ tar warning: {_l[:200]}")

            if failed:
                read_errors += 1
                errors += 1
                _tar_summary = "; ".join(_tar_stderr_lines[:5]) or "(no stderr)"
                append_verify_log(
                    f"✗ {name}: tar exited rc={tar_rc} after {tar_files_seen[0]:,} entries "
                    f"({bytes_human(reader.bytes_read)} from tape). "
                    f"tar: {_tar_summary[:200]}"
                    + (f"  read: {reader.error[:200]}" if reader.error else "")
                )
            else:
                append_verify_log(
                    f"✓ {name}: archive readable — {tar_files_seen[0]:,} entries, "
                    f"{bytes_human(reader.bytes_read)} verified."
                )

        if read_errors == 0:
            append_verify_log(
                f"✓ Archive integrity OK — {files_total:,} entries readable, "
                f"{bytes_human(bytes_verified)} verified."
            )
        else:
            append_verify_log(
                f"✗ Integrity check failed: {read_errors} error(s). "
                f"Entries read: {files_total:,}. "
                f"Bytes from tape: {bytes_human(bytes_verified)}."
            )

        # ── Update backup record ────────────────────────────────────────────
        if backup_record_id:
            with shared_state._backup_records_lock:
                for rec in shared_state._backup_records:
                    if rec.get("id") == backup_record_id:
                        rec["verified"]      = errors == 0
                        rec["verify_errors"] = errors
                        rec["verified_at"]   = now_ts()
                        rec["verify_bytes"]  = bytes_verified
            _save_backup_records()

        status = "completed" if errors == 0 else "completed_with_errors"
        set_verify_state(
            running=False, status=status, finished_at=now_ts(),
            errors=errors, bytes_verified=bytes_verified, eta_seconds=0,
            last_message=f"Verification done: {errors} error(s), {bytes_human(bytes_verified)} checked.",
            error=None if errors == 0 else f"{errors} integrity error(s) found.",
        )
        prefix = "✓" if errors == 0 else "✗"
        append_verify_log(f"{prefix} Verification complete — {errors} total error(s).")
        log_action("verify", errors == 0,
                   f"{vol}: {errors} errors, {bytes_human(bytes_verified)} read")

        if errors > 0:
            notify_verify_failure(vol, errors,
                f"{read_errors} read/parse error(s) after {files_total:,} entries "
                f"({bytes_human(bytes_verified)} checked)")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        set_verify_state(running=False, status="failed", finished_at=now_ts(), eta_seconds=0,
                         error=str(e), last_message=f"Verify failed: {e}")
        append_verify_log(f"Verification failed with exception: {e}")
        log_traceback("verify", e)
        if verbose:
            for _tbl in tb.splitlines()[-10:]:
                append_verify_log(f"  {_tbl}")
        log_action("verify", False, str(e))
        notify_verify_failure(vol, -1, str(e))
    finally:
        publish_state_to_mqtt(refresh_state())


# Health data cache (refreshed on a slower cadence — sg_logs is slow)
