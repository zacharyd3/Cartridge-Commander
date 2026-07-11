"""db module (split from the original monolithic app.py)."""

import os
import json
import threading
import subprocess
import sqlite3
from typing import Any, Dict, List, Optional
from .config import COMMAND_TIMEOUT, LOG_MAX_ROWS, LOG_RETENTION_DAYS, TAPE, TAPE_BLOCK_BYTES, TAPE_CATALOG_DB, TAPE_INDEX_DIR
from . import state as shared_state


def db_counts() -> Dict[str, int]:
    out = {"catalog_rows": 0, "log_rows": 0}
    try:
        with tape_catalog_conn() as conn:
            out["catalog_rows"] = conn.execute("SELECT COUNT(*) FROM tape_catalog WHERE is_deleted = 0").fetchone()[0]
            out["log_rows"] = conn.execute("SELECT COUNT(*) FROM app_log").fetchone()[0]
    except Exception:
        pass
    return out


def tape_catalog_conn():
    os.makedirs(os.path.dirname(TAPE_CATALOG_DB), exist_ok=True)
    conn = sqlite3.connect(TAPE_CATALOG_DB)
    conn.row_factory = sqlite3.Row
    # WAL mode: set on every connection — SQLite persists the journal_mode
    # setting in the DB file so this is effectively a no-op after the first call,
    # but setting it explicitly ensures any new file starts in WAL mode too.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_tape_catalog() -> None:
    with tape_catalog_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tape_catalog (
                volume_tag TEXT PRIMARY KEY,
                written_at INTEGER,
                file_count INTEGER NOT NULL DEFAULT 0,
                files_json TEXT NOT NULL DEFAULT '[]',
                present INTEGER NOT NULL DEFAULT 0,
                last_seen_at INTEGER,
                last_seen_slot INTEGER,
                magazine INTEGER,
                slot_in_magazine INTEGER,
                purpose TEXT,
                is_cleaning INTEGER NOT NULL DEFAULT 0,
                is_deleted INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        cols = {row[1] for row in conn.execute("PRAGMA table_info(tape_catalog)").fetchall()}
        if "purpose" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN purpose TEXT")
        if "is_cleaning" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN is_cleaning INTEGER NOT NULL DEFAULT 0")
        if "lto_generation" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN lto_generation INTEGER")
        if "capacity_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN capacity_bytes INTEGER")
        if "used_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN used_bytes INTEGER")
        if "remaining_bytes" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN remaining_bytes INTEGER")
        if "remaining_pct" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN remaining_pct REAL")
        if "space_estimated" not in cols:
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN space_estimated INTEGER NOT NULL DEFAULT 1")
        if "backup_dirnames" not in cols:
            # JSON array of unique top-level directory names baked into this tape's archive(s).
            # e.g. ["SM9158L6_2026-03-30", "SM9158L6_2026-04-06"]
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN backup_dirnames TEXT NOT NULL DEFAULT '[]'")
        if "archived_at" not in cols:
            # Timestamp when the tape was last marked as not present after a scan
            conn.execute("ALTER TABLE tape_catalog ADD COLUMN archived_at INTEGER")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_kv (
                key TEXT PRIMARY KEY,
                value_json TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                category TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_app_log_ts ON app_log(ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_app_log_cat ON app_log(category, ts DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tape_catalog_present ON tape_catalog(present, is_deleted)")
        conn.commit()

def db_log(category: str, level: str, message: str) -> None:
    from .state import now_ts
    ts = now_ts()
    with tape_catalog_conn() as conn:
        conn.execute(
            "INSERT INTO app_log (ts, category, level, message) VALUES (?, ?, ?, ?)",
            (ts, category, level, message),
        )
        conn.commit()

def _prune_app_log() -> None:
    """Trim app_log by age and row count.  Called periodically from scheduler_loop,
    not on every write, to avoid a full-table-scan on each log entry."""
    from .state import now_ts
    ts = now_ts()
    try:
        with tape_catalog_conn() as conn:
            cutoff = ts - (LOG_RETENTION_DAYS * 86400)
            conn.execute("DELETE FROM app_log WHERE ts < ?", (cutoff,))
            conn.execute("""
                DELETE FROM app_log
                WHERE id NOT IN (
                    SELECT id FROM app_log
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                )
            """, (LOG_MAX_ROWS,))
            conn.commit()
    except Exception:
        pass

def get_recent_logs(limit: int = 500, category: Optional[str] = None) -> List[Dict[str, Any]]:
    with tape_catalog_conn() as conn:
        if category:
            rows = conn.execute(
                "SELECT ts, category, level, message FROM app_log WHERE category=? ORDER BY ts DESC, id DESC LIMIT ?",
                (category, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT ts, category, level, message FROM app_log ORDER BY ts DESC, id DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [dict(row) for row in rows]

def _db_set_json(key: str, value: Any) -> None:
    from .state import now_ts
    with tape_catalog_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_kv (key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (key, json.dumps(value), now_ts()),
        )
        conn.commit()


def _db_get_json(key: str, default: Any = None) -> Any:
    with tape_catalog_conn() as conn:
        row = conn.execute("SELECT value_json FROM app_kv WHERE key = ?", (key,)).fetchone()
    if not row or row[0] is None:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default


def _save_action_log() -> None:
    with shared_state._action_lock:
        payload = json.loads(json.dumps(shared_state._action_log[:500]))
    _db_set_json("action_log", payload)


def _load_action_log() -> None:
    data = _db_get_json("action_log", None)
    if isinstance(data, list):
        cleaned = []
        for item in data:
            if not isinstance(item, dict):
                continue
            cleaned.append({
                "ts": int(item.get("ts", 0) or 0),
                "kind": str(item.get("kind", "app") or "app"),
                "ok": item.get("ok", True),
                "detail": str(item.get("detail", "") or ""),
                "extra": item.get("extra", {}) if isinstance(item.get("extra", {}), dict) else {},
            })
        shared_state._action_log = cleaned[:500]
    else:
        shared_state._action_log = []

def migrate_legacy_tape_indexes() -> None:
    from .state import is_cleaning_volume_tag
    try:
        os.makedirs(TAPE_INDEX_DIR, exist_ok=True)
    except OSError:
        return
    legacy = [n for n in os.listdir(TAPE_INDEX_DIR) if n.endswith('.json')]
    if not legacy:
        return
    for name in legacy:
        path = os.path.join(TAPE_INDEX_DIR, name)
        try:
            with open(path) as f:
                data = json.load(f)
            vol = str(data.get('volume_tag') or '').strip()
            if not vol:
                continue
            save_tape_index(
                vol,
                data.get('files') or [],
                int(data.get('written_at') or 0),
                meta={
                    'present': bool(data.get('present', False)),
                    'last_seen_at': data.get('last_seen_at'),
                    'last_seen_slot': data.get('last_seen_slot'),
                    'magazine': data.get('magazine'),
                    'slot_in_magazine': data.get('slot_in_magazine'),
                    'purpose': data.get('purpose') or ('cleaning' if is_cleaning_volume_tag(vol) else 'data'),
                    'is_cleaning': data.get('is_cleaning', is_cleaning_volume_tag(vol)),
                },
            )
        except Exception:
            pass

def _catalog_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    try:
        return bool(int(v)) if isinstance(v, (int, bool)) else bool(v)
    except Exception:
        return default

def _row_to_index(row: sqlite3.Row) -> Dict[str, Any]:
    from .drive_history import build_tape_space_info
    files = []
    try:
        files = json.loads(row['files_json'] or '[]')
    except Exception:
        files = []
    data = {
        'volume_tag': row['volume_tag'],
        'written_at': row['written_at'],
        'file_count': int(row['file_count'] or len(files)),
        'files': files,
        'present': _catalog_bool(row['present']),
        'last_seen_at': row['last_seen_at'],
        'last_seen_slot': row['last_seen_slot'],
        'magazine': row['magazine'],
        'slot_in_magazine': row['slot_in_magazine'],
        'purpose': row['purpose'] or ('cleaning' if _catalog_bool(row['is_cleaning']) else 'data'),
        'is_cleaning': _catalog_bool(row['is_cleaning']),
        'deleted': _catalog_bool(row['is_deleted']),
        'updated_at': row['updated_at'],
        'lto_generation': row['lto_generation'] if 'lto_generation' in row.keys() else None,
        'capacity_bytes': row['capacity_bytes'] if 'capacity_bytes' in row.keys() else None,
        'used_bytes': row['used_bytes'] if 'used_bytes' in row.keys() else None,
        'remaining_bytes': row['remaining_bytes'] if 'remaining_bytes' in row.keys() else None,
        'remaining_pct': row['remaining_pct'] if 'remaining_pct' in row.keys() else None,
        'space_estimated': _catalog_bool(row['space_estimated'], True) if 'space_estimated' in row.keys() else True,
        'backup_dirnames': json.loads(row['backup_dirnames'] or '[]') if 'backup_dirnames' in row.keys() else [],
        'archived_at': row['archived_at'] if 'archived_at' in row.keys() else None,
    }
    data['space'] = build_tape_space_info(data['volume_tag'], idx=data, loaded=False)
    return data

def save_tape_index(vol, files, written_at, meta=None):
    from .state import is_cleaning_volume_tag, now_ts
    if not vol:
        return
    meta = meta or {}
    files = list(files or [])
    ts_now = now_ts()
    is_cleaning = bool(meta.get('is_cleaning', is_cleaning_volume_tag(vol)))
    purpose = meta.get('purpose') or ('cleaning' if is_cleaning else 'data')

    # Merge the new backup_dirname into the tape's running list of dirnames
    new_dirname = str(meta.get('backup_dirname') or '').strip()
    existing_idx = load_tape_index(vol)
    existing_dirnames = list(existing_idx.get('backup_dirnames') or []) if existing_idx else []
    if new_dirname and new_dirname not in existing_dirnames:
        existing_dirnames.append(new_dirname)
    backup_dirnames_json = json.dumps(existing_dirnames)
    with tape_catalog_conn() as conn:
        conn.execute("""
            INSERT INTO tape_catalog (
                volume_tag, written_at, file_count, files_json,
                present, last_seen_at, last_seen_slot, magazine, slot_in_magazine,
                purpose, is_cleaning, lto_generation, capacity_bytes, used_bytes,
                remaining_bytes, remaining_pct, space_estimated, backup_dirnames,
                is_deleted, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(volume_tag) DO UPDATE SET
                written_at=excluded.written_at,
                -- Only overwrite files_json when the new list is non-empty.
                -- An empty list from a failed tar -tf would otherwise silently
                -- wipe a previously-saved good index.
                file_count=CASE WHEN excluded.file_count > 0
                                THEN excluded.file_count
                                ELSE tape_catalog.file_count END,
                files_json=CASE WHEN excluded.file_count > 0
                                THEN excluded.files_json
                                ELSE tape_catalog.files_json END,
                present=COALESCE(excluded.present, tape_catalog.present),
                last_seen_at=COALESCE(excluded.last_seen_at, tape_catalog.last_seen_at),
                last_seen_slot=COALESCE(excluded.last_seen_slot, tape_catalog.last_seen_slot),
                magazine=COALESCE(excluded.magazine, tape_catalog.magazine),
                slot_in_magazine=COALESCE(excluded.slot_in_magazine, tape_catalog.slot_in_magazine),
                purpose=COALESCE(excluded.purpose, tape_catalog.purpose),
                is_cleaning=excluded.is_cleaning,
                lto_generation=COALESCE(excluded.lto_generation, tape_catalog.lto_generation),
                capacity_bytes=COALESCE(excluded.capacity_bytes, tape_catalog.capacity_bytes),
                used_bytes=COALESCE(excluded.used_bytes, tape_catalog.used_bytes),
                remaining_bytes=COALESCE(excluded.remaining_bytes, tape_catalog.remaining_bytes),
                remaining_pct=COALESCE(excluded.remaining_pct, tape_catalog.remaining_pct),
                space_estimated=COALESCE(excluded.space_estimated, tape_catalog.space_estimated),
                backup_dirnames=excluded.backup_dirnames,
                is_deleted=0,
                updated_at=excluded.updated_at
        """, (
            vol,
            int(written_at or ts_now),
            len(files),
            json.dumps(files),
            1 if meta.get('present') else 0,
            meta.get('last_seen_at') if meta.get('last_seen_at') is not None else ts_now,
            meta.get('last_seen_slot'),
            meta.get('magazine'),
            meta.get('slot_in_magazine'),
            purpose,
            1 if is_cleaning else 0,
            meta.get('lto_generation'),
            meta.get('capacity_bytes'),
            meta.get('used_bytes'),
            meta.get('remaining_bytes'),
            meta.get('remaining_pct'),
            meta.get('space_estimated', 1),
            backup_dirnames_json,
            ts_now,
            ts_now,
        ))
        conn.commit()

def update_tape_index_metadata(vol, **meta):
    from .state import is_cleaning_volume_tag, now_ts
    if not vol:
        return
    ts_now = now_ts()
    is_cleaning = bool(meta.get('is_cleaning', is_cleaning_volume_tag(vol)))
    purpose = meta.get('purpose') or ('cleaning' if is_cleaning else 'data')
    # When a tape is confirmed present, clear archived_at and restore purpose to 'data'
    # (unless it's being explicitly set to something else).
    being_confirmed_present = bool(meta.get('present'))
    with tape_catalog_conn() as conn:
        conn.execute("""
            INSERT INTO tape_catalog (
                volume_tag, written_at, file_count, files_json,
                present, last_seen_at, last_seen_slot, magazine, slot_in_magazine,
                purpose, is_cleaning, lto_generation, capacity_bytes, used_bytes,
                remaining_bytes, remaining_pct, space_estimated,
                is_deleted, created_at, updated_at
            ) VALUES (?, NULL, 0, '[]', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            ON CONFLICT(volume_tag) DO UPDATE SET
                present=COALESCE(excluded.present, tape_catalog.present),
                last_seen_at=COALESCE(excluded.last_seen_at, tape_catalog.last_seen_at),
                last_seen_slot=COALESCE(excluded.last_seen_slot, tape_catalog.last_seen_slot),
                magazine=COALESCE(excluded.magazine, tape_catalog.magazine),
                slot_in_magazine=COALESCE(excluded.slot_in_magazine, tape_catalog.slot_in_magazine),
                -- When a tape comes back from archived state, restore its purpose to 'data'
                purpose=CASE
                    WHEN excluded.present=1 AND tape_catalog.purpose='archived'
                    THEN COALESCE(excluded.purpose, 'data')
                    ELSE COALESCE(excluded.purpose, tape_catalog.purpose)
                END,
                is_cleaning=excluded.is_cleaning,
                lto_generation=COALESCE(excluded.lto_generation, tape_catalog.lto_generation),
                capacity_bytes=COALESCE(excluded.capacity_bytes, tape_catalog.capacity_bytes),
                used_bytes=COALESCE(excluded.used_bytes, tape_catalog.used_bytes),
                remaining_bytes=COALESCE(excluded.remaining_bytes, tape_catalog.remaining_bytes),
                remaining_pct=COALESCE(excluded.remaining_pct, tape_catalog.remaining_pct),
                space_estimated=COALESCE(excluded.space_estimated, tape_catalog.space_estimated),
                -- Clear archived_at when tape is confirmed present again
                archived_at=CASE WHEN excluded.present=1 THEN NULL ELSE tape_catalog.archived_at END,
                is_deleted=0,
                updated_at=excluded.updated_at
        """, (
            vol,
            1 if meta.get('present') else 0,
            meta.get('last_seen_at') if meta.get('last_seen_at') is not None else ts_now,
            meta.get('last_seen_slot'),
            meta.get('magazine'),
            meta.get('slot_in_magazine'),
            purpose,
            1 if is_cleaning else 0,
            meta.get('lto_generation'),
            meta.get('capacity_bytes'),
            meta.get('used_bytes'),
            meta.get('remaining_bytes'),
            meta.get('remaining_pct'),
            meta.get('space_estimated', 1),
            ts_now,
            ts_now,
        ))
        conn.commit()

def load_tape_index(vol):
    if not vol:
        return None
    with tape_catalog_conn() as conn:
        row = conn.execute(
            "SELECT * FROM tape_catalog WHERE volume_tag = ? AND is_deleted = 0",
            (vol,),
        ).fetchone()
    return _row_to_index(row) if row else None

def mark_tape_archived(vol: str) -> None:
    """Mark a tape as archived — present=0, purpose='archived', archived_at=now.

    Called during inventory when a tape that was previously catalogued is no
    longer seen in any slot or the drive.  The catalog record and file index
    are preserved so the tape can still be browsed and restored if it's
    reinserted later.
    """
    from .state import now_ts
    if not vol:
        return
    ts = now_ts()
    with tape_catalog_conn() as conn:
        conn.execute(
            """UPDATE tape_catalog
               SET present = 0, purpose = 'archived', archived_at = ?, updated_at = ?
               WHERE volume_tag = ? AND is_deleted = 0""",
            (ts, ts, vol),
        )
        conn.commit()

def delete_tape_index(vol: str, permanent: bool = False) -> bool:
    """Remove a tape from the catalog.

    permanent=False (default / soft delete):
      Sets is_deleted=1.  The row is hidden from all normal queries but the
      data is retained for recovery.  Use this for routine removal.

    permanent=True (hard delete):
      Physically removes the row.  All history, file index, and space data
      are gone forever.  Only called when the user explicitly confirms a
      permanent wipe via the UI.
    """
    from .state import now_ts
    if not vol:
        return False
    with tape_catalog_conn() as conn:
        if permanent:
            cur = conn.execute("DELETE FROM tape_catalog WHERE volume_tag = ?", (vol,))
        else:
            cur = conn.execute(
                "UPDATE tape_catalog SET is_deleted = 1, present = 0, updated_at = ? WHERE volume_tag = ?",
                (now_ts(), vol),
            )
        conn.commit()
        return cur.rowcount > 0

def mark_all_indexes_not_present():
    """Mark every catalog entry as not currently present in the library.

    Preserves last_seen_slot — we want to remember where a tape was last seen
    even after it has been removed from the library.  The slot is only cleared
    when a tape is confirmed present at a *different* slot (i.e. it moved).
    """
    from .state import now_ts
    with tape_catalog_conn() as conn:
        conn.execute(
            "UPDATE tape_catalog SET present = 0, updated_at = ? WHERE is_deleted = 0",
            (now_ts(),),
        )
        conn.commit()

def read_tape_index_live() -> List[str]:
    """Read the file list from the tape currently in the drive.

    Uses `dd if=TAPE bs=TAPE_BLOCK_BYTES | tar -t -f -` so that the physical
    block size matches what was used when writing (default 512 KiB via dd).
    Reading with plain `tar -tf /dev/nst0` uses the default 512-byte block
    size, which causes the kernel tape driver to return ENOMEM when it tries
    to read a 512 KiB physical block into a 512-byte buffer.

    Always rewinds before reading.
    """
    from .state import TapeError
    try:
        subprocess.run(["mt", "-f", TAPE, "rewind"],
                       capture_output=True, timeout=max(COMMAND_TIMEOUT, 300), check=True)
    except Exception as e:
        raise TapeError(f"Rewind before index read failed: {e}")

    # dd reads physical tape blocks at the correct block size and streams bytes
    # to tar's stdin; tar reads the archive from stdin with no block-size concern.
    # status=progress ensures dd emits its byte counter and any errors to stderr
    # even if tar exits early — critical for diagnosing failures.
    dd_proc = subprocess.Popen(
        ["dd", f"if={TAPE}", f"bs={TAPE_BLOCK_BYTES}", "status=progress"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_proc = subprocess.Popen(
        ["tar", "-t", "-f", "-"],
        stdin=dd_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Allow dd to receive SIGPIPE if tar exits early
    dd_proc.stdout.close()

    # Drain dd stderr concurrently — if we don't, and dd's stderr pipe buffer
    # fills, dd will block and tar will never get EOF on its stdin.
    _dd_stderr_buf: List[bytes] = []
    def _drain_dd_err():
        try:
            for line in dd_proc.stderr:
                _dd_stderr_buf.append(line)
        except Exception:
            pass
    _dd_drain_t = threading.Thread(target=_drain_dd_err, daemon=True)
    _dd_drain_t.start()

    # Scale timeout to tape size: assume worst case 80 MB/s read speed.
    # Minimum 10 min, no upper cap — a 12 TB LTO-8 could take ~42 hours at 80 MB/s
    # but in practice we only call this for verify/reindex, not post-backup indexing.
    try:
        tar_out, tar_err = tar_proc.communicate(timeout=max(600, TAPE_BLOCK_BYTES))
    except subprocess.TimeoutExpired:
        # Kill both processes and release /dev/nst0 before re-raising
        for _p in (tar_proc, dd_proc):
            try:
                _p.kill()
            except Exception:
                pass
        for _p in (tar_proc, dd_proc):
            try:
                _p.wait(timeout=10)
            except Exception:
                pass
        _dd_drain_t.join(timeout=5)
        raise TapeError(
            f"tar -t timed out reading tape index — tape may be too large for the "
            f"configured timeout. Use 'Read Index' from the library after the backup completes."
        )
    dd_proc.wait(timeout=30)
    _dd_drain_t.join(timeout=5)

    dd_stderr = b"".join(_dd_stderr_buf).decode(errors="ignore").strip()
    files = [l for l in (tar_out or b"").decode(errors="ignore").splitlines() if l.strip()]
    err = (tar_err or b"").decode(errors="ignore").strip()

    # Append any dd errors to the tar error message for diagnostics
    if dd_stderr and "error" in dd_stderr.lower():
        err = (err + "\ndd: " + dd_stderr[-300:]).strip()

    # rc=1 from tar means warnings (e.g. socket files skipped) — still usable.
    # rc=2 means fatal error and no output.
    if tar_proc.returncode not in (0, 1) and not files:
        # Distinguish blank/foreign-format tapes from genuine read errors.
        # "does not look like a tar archive" means the tape has data but it's not tar
        # (written by other software, or a partial/corrupt first block).
        # Empty stderr with rc=2 typically means a completely blank tape.
        _BLANK_OR_FOREIGN = (
            "does not look like a tar archive" in err
            or "This does not look like a tar archive" in err
            or "Skipping to next header" in err
            or not err  # blank tape — dd reads nothing, tar gets EOF immediately
        )
        if _BLANK_OR_FOREIGN:
            raise TapeError(f"__blank_or_foreign__: {err[:200] or 'no tar header found'}")
        raise TapeError(f"tar -t failed (rc={tar_proc.returncode}): {err[:300] or 'no output'}")

    return files

def list_all_known_indexes(include_deleted: bool = False):
    query = "SELECT * FROM tape_catalog"
    params = []
    if not include_deleted:
        query += " WHERE is_deleted = 0"
    query += " ORDER BY volume_tag COLLATE NOCASE ASC"
    with tape_catalog_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    result = []
    for row in rows:
        item = _row_to_index(row)
        item.pop('files', None)
        result.append(item)
    return result

# ---------------------------------------------------------------------------
# Restore worker
# ---------------------------------------------------------------------------

