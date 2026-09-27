"""Startup sequence: load persisted state, then serve the Flask app.

Run via the root-level ``app.py`` launcher (``python app.py``, matching the
Dockerfile's ``CMD``), or directly as ``python -m cartridge_commander.main``.
"""

import os
import sys
import datetime
import threading
import sqlite3
from .flaskapp import app
from . import config as cfg
from .config import CHANGER, INCREMENTAL_DIR, STARTUP_QUICK_SCAN, TAPE_CATALOG_DB, TAPE_INDEX_DIR, validate_device_paths
from .logsetup import LOG_HTTP_REQUESTS, LOG_LEVEL, configure_logging, get_logger
from .settings import _load_gfs_config, _load_ha_config, _load_notify_config, _load_restore_subfolder_pattern, _load_tape_fill_strategy, _load_allow_tape_spanning
from .changer import refresh_state
from .db import _load_action_log, db_log, init_tape_catalog, list_all_known_indexes, migrate_legacy_tape_indexes
from .drive_history import _load_drive_history, _load_last_known_loaded_slot
from .records import _load_backup_records
from .mqtt import mqtt_available, mqtt_loop
from .scheduler import _load_schedules, scheduler_loop
from .inventory_worker import inventory_worker
from . import routes  # noqa: F401 -- import for its @app.route registration side effects
from . import state as shared_state

_log = get_logger("app")


def _log_startup_banner() -> None:
    on_off = lambda v: "on" if v else "off"
    _log.info("Cartridge Commander %s starting (pid %s, Python %s).",
              cfg.DEVICE_INFO.get("sw_version", "?"), os.getpid(), sys.version.split()[0])
    _log.info("Devices: changer=%s tape=%s sg_device=%s",
              cfg.CHANGER or "(unset)", cfg.TAPE or "(unset)", cfg.SG_DEVICE or "(unset)")
    _log.info("Paths: backup_root=%s restore_root=%s catalog_db=%s",
              cfg.BACKUP_ROOT, cfg.RESTORE_ROOT, cfg.TAPE_CATALOG_DB)
    _log.info("Tape: block=%s KiB, command_timeout=%ss, mail_slot=%s, magazine_size=%s",
              cfg.TAPE_BLOCK_BYTES // 1024, cfg.COMMAND_TIMEOUT, on_off(cfg.HAS_MAIL_SLOT), cfg.MAGAZINE_SIZE)
    _log.info("Backup: verify_after=%s (sample %s MB), auto_rewind=%s, erase_before=%s, "
              "auto_rewrite_on_full=%s, pre_hook=%s, post_hook=%s",
              on_off(cfg.VERIFY_AFTER_BACKUP), cfg.VERIFY_SAMPLE_MB or "full", on_off(cfg.AUTO_REWIND_AFTER),
              on_off(cfg.ERASE_BEFORE_BACKUP), on_off(cfg.AUTO_REWRITE_ON_FULL),
              cfg.PRE_BACKUP_HOOK or "none", cfg.POST_BACKUP_HOOK or "none")
    _log.info("Web UI: port=%s, password=%s, poll=%ss | MQTT: %s | startup quick scan: %s",
              os.getenv("PORT", "8080"), "set" if cfg.WEBUI_PASSWORD else "none", cfg.POLL_SECONDS,
              f"{cfg.MQTT_HOST}:{cfg.MQTT_PORT}" if cfg.MQTT_HOST else "disabled",
              on_off(cfg.STARTUP_QUICK_SCAN))
    _log.info("Container logging: LOG_LEVEL=%s LOG_HTTP_REQUESTS=%s "
              "(set LOG_LEVEL=DEBUG for every device command and API read).",
              LOG_LEVEL, LOG_HTTP_REQUESTS)


def run() -> None:
    configure_logging()
    _log_startup_banner()
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
    except Exception as e:
        _log.warning("Startup VACUUM of %s failed: %s", TAPE_CATALOG_DB, e)
    # Check the device paths before any worker shells out to mtx/mt. A bad path
    # otherwise surfaces as "0 tapes, drive offline", which reads as a hardware
    # fault instead of a config typo. Advisory only -- we still start serving so
    # the message is visible in the UI log rather than only in `docker logs`.
    for _problem in validate_device_paths():
        try:
            db_log("app", "error", f"Device configuration: {_problem}")
        except Exception:
            _log.error("Device configuration: %s", _problem)
    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    _load_schedules()
    _load_drive_history()
    _load_last_known_loaded_slot()
    _load_restore_subfolder_pattern()
    _load_ha_config()
    _load_notify_config()
    _load_gfs_config()
    _load_tape_fill_strategy()
    _load_allow_tape_spanning()
    _load_backup_records()
    _load_action_log()
    # Catalogs from before backups were appended: record which old backups a
    # later one overwrote, and where the surviving one sits.  Runs once.
    try:
        from .tape_layout import migrate_legacy_layout
        migrate_legacy_layout()
    except Exception as e:
        _log.warning("Tape layout upgrade failed: %s", e)
    with shared_state._schedules_lock:
        _scheds = list(shared_state._schedules)
    _log.info("Loaded %d schedule(s), %d backup record(s).", len(_scheds), len(shared_state._backup_records))
    for _s in _scheds:
        _nr = _s.get("next_run")
        _log.info("  schedule '%s' (%s, %s, %s): next run %s",
                  _s.get("label", "?"), _s.get("mode", "?"), _s.get("backup_mode", "full"),
                  "enabled" if _s.get("enabled", True) else "disabled",
                  datetime.datetime.fromtimestamp(_nr).strftime("%Y-%m-%d %H:%M") if _nr else "not scheduled")
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
    except Exception as e:
        _log.warning("Could not check tape indexes at startup: %s", e)
    if cfg.MQTT_HOST and not mqtt_available():
        _log.warning("MQTT_HOST is set but paho-mqtt is not installed -- MQTT disabled.")
    if mqtt_available():
        threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    if STARTUP_QUICK_SCAN and CHANGER:
        # Reconcile the catalog against the actual slot contents on boot so tapes that
        # are physically present don't show as "archived" just because the container
        # restarted since the last manual scan.
        _log.info("Starting quick inventory scan to reconcile the catalog with the library.")
        threading.Thread(target=inventory_worker, kwargs={"mode": "quick"}, daemon=True).start()
    _log.info("Startup complete; web UI listening on 0.0.0.0:%s.", os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")), debug=False)


if __name__ == "__main__":
    run()