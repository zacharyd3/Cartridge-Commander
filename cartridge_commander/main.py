"""Startup sequence: load persisted state, then serve the Flask app.

Run via the root-level ``app.py`` launcher (``python app.py``, matching the
Dockerfile's ``CMD``), or directly as ``python -m cartridge_commander.main``.
"""

import os
import threading
import sqlite3
from .flaskapp import app
from .config import CHANGER, INCREMENTAL_DIR, STARTUP_QUICK_SCAN, TAPE_CATALOG_DB, TAPE_INDEX_DIR
from .settings import _load_gfs_config, _load_ha_config, _load_notify_config, _load_restore_subfolder_pattern, _load_tape_fill_strategy
from .changer import refresh_state
from .db import _load_action_log, db_log, init_tape_catalog, list_all_known_indexes, migrate_legacy_tape_indexes
from .drive_history import _load_drive_history, _load_last_known_loaded_slot
from .records import _load_backup_records
from .mqtt import mqtt_available, mqtt_loop
from .scheduler import _load_schedules, scheduler_loop
from .inventory_worker import inventory_worker
from . import routes  # noqa: F401 -- import for its @app.route registration side effects


def run() -> None:
    os.makedirs(TAPE_INDEX_DIR, exist_ok=True)
    init_tape_catalog()
    migrate_legacy_tape_indexes()
    # Compact the DB on startup — reclaims space from deleted rows and soft-deleted
    # catalog entries.  VACUUM cannot run inside a transaction so we open a raw
    # connection.  This is fast (seconds) for a small tape-library DB.
    try:
        _vconn = sqlite3.connect(TAPE_CATALOG_DB)
        _vconn.execute("VACUUM")
        _vconn.close()
    except Exception:
        pass
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    _load_schedules()
    _load_drive_history()
    _load_last_known_loaded_slot()
    _load_restore_subfolder_pattern()
    _load_ha_config()
    _load_notify_config()
    _load_gfs_config()
    _load_tape_fill_strategy()
    _load_backup_records()
    _load_action_log()
    refresh_state()
    # Warn about tapes that have size data but no file index — these need a
    # "Read Index" pass with the tape loaded to recover the file list.
    try:
        _broken = []
        for _idx in list_all_known_indexes():
            if (_idx.get("used_bytes") or 0) > 0 and (_idx.get("file_count") or 0) == 0:
                _broken.append(_idx["volume_tag"])
        if _broken:
            db_log("app", "info",
                   f"Tapes with usage data but no file index (load each and use 'Read Index' to recover): "
                   f"{', '.join(_broken)}")
    except Exception:
        pass
    if mqtt_available():
        threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    if STARTUP_QUICK_SCAN and CHANGER:
        # Reconcile the catalog against the actual slot contents on boot so tapes that
        # are physically present don't show as "archived" just because the container
        # restarted since the last manual scan.
        threading.Thread(target=inventory_worker, kwargs={"mode": "quick"}, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")), debug=False)


if __name__ == "__main__":
    run()