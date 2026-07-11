"""verify_worker module (split from the original monolithic app.py)."""

import re
import time
import threading
import subprocess
from typing import List, Optional
from .config import COMMAND_TIMEOUT, TAPE, TAPE_BLOCK_BYTES, VERIFY_SAMPLE_MB
from . import state as shared_state


def verify_worker(vol: str, backup_record_id: Optional[str] = None) -> None:
    """
    Read back the tape and verify integrity.

    Strategy:
      1. Issue a rewind and then poll `mt status` until the drive reports it is
         sitting at file 0, block 0 (BOT).  The status flags string varies by
         driver; we parse the numeric file/block fields instead of looking for a
         "BOT" keyword that not all drivers emit.
      2. Run  dd if=TAPE bs=BLOCK [count=N] | tar -t -f -  as a pure archive-
         readability check.  Both stdout and stderr of each process are drained
         concurrently to prevent pipe-buffer deadlocks.
      3. "Unexpected EOF in archive" is NOT treated as an error when sampling
         (VERIFY_SAMPLE_MB > 0) because dd deliberately truncates the stream
         mid-archive — tar hitting EOF there is expected and correct.
      4. At verbose log level the full stderr from both dd and tar is written to
         the verify log so it is trivial to diagnose any genuine failure.
    """
    from .records import _save_backup_records
    from .state import append_verify_log, backup_log_allows, bytes_human, calc_eta_seconds, log_action, now_ts, run_cmd, set_verify_state
    from .mqtt import publish_state_to_mqtt
    from .changer import refresh_state
    from .notify import notify_verify_failure
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
        f"sample={'full tape' if not sampling else str(VERIFY_SAMPLE_MB)+'MB'}, "
        f"log={'verbose' if verbose else 'normal'})."
    )
    publish_state_to_mqtt(refresh_state())

    errors = 0
    bytes_verified = 0

    def _at_bot(mt_text: str) -> bool:
        """Return True if mt status indicates the tape is at file 0, block 0 (BOT).

        Different kernel drivers report this differently:
          - Some emit a "BOT" flag in the general status bits line.
          - Linux st driver always prints "file number=N, block number=M" —
            file 0, block 0 means we are at the very beginning.
        We check both forms so we don't need to know which driver is in use.
        """
        t = mt_text.lower()
        # Explicit BOT keyword
        if "bot" in t or "beginning of tape" in t:
            return True
        # Parse "file number=N, block number=M"
        fm = re.search(r"file number\s*=\s*(\d+)", t)
        bm = re.search(r"block number\s*=\s*(\d+)", t)
        if fm and bm:
            return int(fm.group(1)) == 0 and int(bm.group(1)) == 0
        return False

    try:
        # ── Step 1: rewind then wait for BOT ────────────────────────────────
        # Issue the rewind immediately — don't wait first.  The old code waited
        # up to 5 minutes hoping the drive would self-report BOT, but LTO-6
        # reports "file number=1, block number=0" after a write (positioned at
        # the end of the last file mark), which never matches "BOT".  Just
        # rewind and then confirm we are at file 0 block 0.
        append_verify_log("Rewinding tape before verification…")
        set_verify_state(status="rewinding")
        publish_state_to_mqtt(refresh_state())

        try:
            run_cmd(["mt", "-f", TAPE, "rewind"], timeout=max(COMMAND_TIMEOUT, 300))
            append_verify_log("Rewind command issued — waiting for BOT…")
        except Exception as _rw_err:
            append_verify_log(f"Warning: rewind returned an error: {_rw_err} — continuing.")

        # Poll mt status until file=0, block=0 (BOT confirmed) or timeout
        _bot_deadline = time.time() + 120   # 2 minutes max after rewind
        _bot_confirmed = False
        while time.time() < _bot_deadline:
            try:
                _mt_out = subprocess.run(
                    ["mt", "-f", TAPE, "status"],
                    capture_output=True, text=True, timeout=15,
                )
                _mt_text = _mt_out.stdout + _mt_out.stderr
                if verbose:
                    append_verify_log(f"mt status: {_mt_text.strip()[:300]}")
                if _at_bot(_mt_text):
                    _bot_confirmed = True
                    append_verify_log("Drive confirmed at BOT (file 0, block 0).")
                    break
            except Exception as _me:
                append_verify_log(f"mt status check error: {_me}")
            time.sleep(3)

        if not _bot_confirmed:
            append_verify_log(
                "Warning: could not confirm BOT within 2 min after rewind. "
                "Proceeding anyway — verify may read from wrong position if drive is still seeking.")

        # Brief settle — some drives need a moment after reaching BOT
        time.sleep(2)

        # ── Step 2: dd | tar -t ─────────────────────────────────────────────
        set_verify_state(status="reading_data")
        limit_bytes = VERIFY_SAMPLE_MB * 1024 * 1024 if sampling else None
        append_verify_log(
            f"Starting read-back: "
            f"{'full tape' if not limit_bytes else bytes_human(limit_bytes)} "
            f"via dd (bs={TAPE_BLOCK_BYTES//1024}KiB) | tar -t"
        )
        if sampling:
            append_verify_log(
                f"NOTE: Sampling mode — dd will stop after {VERIFY_SAMPLE_MB} MB. "
                f"'Unexpected EOF' at the sample boundary is expected and not an error."
            )
        publish_state_to_mqtt(refresh_state())

        dd_cmd = ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"]
        if limit_bytes:
            block_count = max(1, (limit_bytes + TAPE_BLOCK_BYTES - 1) // TAPE_BLOCK_BYTES)
            dd_cmd += [f"count={block_count}"]
        append_verify_log(f"dd command: {' '.join(dd_cmd)}")

        dd_proc  = subprocess.Popen(dd_cmd,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tar_proc = subprocess.Popen(
            ["tar", "-t", "-f", "-"],
            stdin=dd_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        dd_proc.stdout.close()   # let tar own the read end

        verify_started = now_ts()
        tar_files_seen   = 0
        _dd_stderr_lines: List[str] = []
        _tar_stderr_lines: List[str] = []

        def _drain_tar_stdout():
            nonlocal tar_files_seen
            try:
                for _ in tar_proc.stdout:
                    tar_files_seen += 1
            except Exception:
                pass

        def _drain_tar_stderr():
            try:
                for raw in tar_proc.stderr:
                    _tar_stderr_lines.append(raw.decode(errors="ignore").rstrip())
            except Exception:
                pass

        def _drain_dd_stderr():
            try:
                for raw in dd_proc.stderr:
                    _dd_stderr_lines.append(raw.decode(errors="ignore").rstrip())
            except Exception:
                pass

        t_tar_out = threading.Thread(target=_drain_tar_stdout, daemon=True)
        t_tar_err = threading.Thread(target=_drain_tar_stderr, daemon=True)
        t_dd_err  = threading.Thread(target=_drain_dd_stderr,  daemon=True)
        t_tar_out.start(); t_tar_err.start(); t_dd_err.start()

        while tar_proc.poll() is None:
            time.sleep(3)
            _bv = 0
            for _line in reversed(_dd_stderr_lines):
                _m = re.search(r'^(\d+)\s+bytes', _line)
                if _m:
                    _bv = int(_m.group(1))
                    break
            if _bv:
                bytes_verified = _bv
            eta_v = calc_eta_seconds(verify_started, bytes_verified, limit_bytes) if limit_bytes else None
            set_verify_state(
                bytes_verified=bytes_verified,
                last_message=f"Verified {tar_files_seen:,} entries, {bytes_human(bytes_verified)} read…",
                eta_seconds=eta_v,
            )
            publish_state_to_mqtt(refresh_state())

        t_tar_out.join(timeout=15)
        t_tar_err.join(timeout=15)
        # Close tar's pipes now that we've drained them
        try:
            tar_proc.stdout.close()
        except Exception:
            pass
        try:
            tar_proc.stderr.close()
        except Exception:
            pass
        tar_rc = tar_proc.wait(timeout=30)
        dd_proc.wait(timeout=60)
        t_dd_err.join(timeout=15)
        # Explicitly close dd's stderr pipe so the kernel releases the fd
        # immediately.  Without this, the pipe fd can linger just long enough
        # for the subsequent `mt rewind` call to see /dev/nst0 as busy.
        try:
            dd_proc.stderr.close()
        except Exception:
            pass

        # Brief settle — give the st driver a moment to fully release the
        # device after dd exits before we issue the rewind.
        time.sleep(1)

        # Final byte count from dd stderr
        for _line in reversed(_dd_stderr_lines):
            _m = re.search(r'^(\d+)\s+bytes', _line)
            if _m:
                bytes_verified = int(_m.group(1))
                break

        dd_rc = dd_proc.returncode

        # ── Log diagnostics (always show process results; full stderr if verbose or error) ──
        append_verify_log(
            f"Process results: tar rc={tar_rc}, dd rc={dd_rc}, "
            f"files seen={tar_files_seen:,}, bytes read={bytes_human(bytes_verified)}"
        )
        if verbose or tar_rc not in (0, 1):
            for _l in (_tar_stderr_lines[:50] if _tar_stderr_lines else ["(empty)"]):
                append_verify_log(f"  tar stderr: {_l[:300]}")
        if verbose or dd_rc not in (0,):
            for _l in (_dd_stderr_lines[-10:] if _dd_stderr_lines else ["(empty)"]):
                append_verify_log(f"  dd stderr: {_l[:300]}")

        # ── Evaluate result ─────────────────────────────────────────────────
        # Key rule: "Unexpected EOF in archive" when sampling is NOT an error.
        # dd stopped feeding data at the count= limit mid-archive; tar seeing
        # EOF there is the designed behaviour, not a tape defect.
        read_errors = 0
        if tar_rc not in (0, 1):
            tar_err_text = " ".join(_tar_stderr_lines).lower()
            unexpected_eof = "unexpected eof" in tar_err_text or "eof in archive" in tar_err_text

            if sampling and unexpected_eof and dd_rc == 0:
                # dd finished cleanly at its count= limit; EOF is expected
                append_verify_log(
                    f"ℹ tar rc={tar_rc} with 'Unexpected EOF' — this is normal when sampling "
                    f"({bytes_human(limit_bytes)} limit reached mid-archive). Not counted as error."
                )
            else:
                read_errors += 1
                errors += 1
                _tar_summary = "; ".join(_tar_stderr_lines[:5]) or "(no stderr)"
                _dd_errors   = "; ".join(
                    l for l in _dd_stderr_lines
                    if "error" in l.lower() or "failed" in l.lower()
                )[:200]
                append_verify_log(
                    f"✗ tar exited rc={tar_rc} after {tar_files_seen:,} entries "
                    f"({bytes_human(bytes_verified)} from dd). "
                    f"tar: {_tar_summary[:200]}"
                    + (f"  dd: {_dd_errors}" if _dd_errors else "")
                )
        elif _tar_stderr_lines and (verbose or tar_rc == 1):
            for _l in _tar_stderr_lines[:10]:
                append_verify_log(f"ℹ tar warning: {_l[:200]}")

        if dd_rc not in (0,) and not limit_bytes:
            append_verify_log(
                f"ℹ dd exited rc={dd_rc} on unlimited read "
                f"(may simply mean end-of-tape reached)")

        if read_errors == 0:
            append_verify_log(
                f"✓ Archive integrity OK — {tar_files_seen:,} entries readable, "
                f"{bytes_human(bytes_verified)} verified."
            )
        else:
            append_verify_log(
                f"✗ Integrity check failed: {read_errors} error(s). "
                f"Entries read before failure: {tar_files_seen:,}. "
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
                f"{read_errors} read/parse error(s) after {tar_files_seen:,} entries "
                f"({bytes_human(bytes_verified)} checked)")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        set_verify_state(running=False, status="failed", finished_at=now_ts(), eta_seconds=0,
                         error=str(e), last_message=f"Verify failed: {e}")
        append_verify_log(f"Verification failed with exception: {e}")
        if verbose:
            for _tbl in tb.splitlines()[-10:]:
                append_verify_log(f"  {_tbl}")
        log_action("verify", False, str(e))
        notify_verify_failure(vol, -1, str(e))
    finally:
        publish_state_to_mqtt(refresh_state())


# Health data cache (refreshed on a slower cadence — sg_logs is slow)
