"""Environment-driven configuration constants for Cartridge Commander."""

import os
import stat


CHANGER              = os.getenv("TL_CHANGER",            "/dev/sg12")
TAPE                 = os.getenv("TL_TAPE",               "/dev/nst0")
POLL_SECONDS         = int(os.getenv("TL_POLL_SECONDS",   "15"))
COMMAND_TIMEOUT      = int(os.getenv("TL_COMMAND_TIMEOUT","60"))
WEBUI_PASSWORD       = os.getenv("TL_WEBUI_PASSWORD",     "")
BACKUP_ROOT          = os.getenv("BACKUP_ROOT",           "/mnt/user")
BACKUP_CHUNK_SIZE    = int(os.getenv("BACKUP_CHUNK_SIZE", str(1024*1024)))
TAPE_BLOCK_BYTES     = int(os.getenv("TL_TAPE_BLOCK_KB", "512")) * 1024   # physical tape block size, default 512 KiB
AUTO_REWIND_AFTER    = os.getenv("AUTO_REWIND_AFTER_BACKUP","true").lower() == "true"
STARTUP_QUICK_SCAN   = os.getenv("STARTUP_QUICK_SCAN",     "true").lower() == "true"
ERASE_BEFORE_BACKUP  = os.getenv("ERASE_BEFORE_BACKUP",   "false").lower() == "true"
TAPE_INDEX_DIR       = os.getenv("TAPE_INDEX_DIR",        "/var/lib/tl2000/index")
ICON_PATH            = os.getenv("ICON_PATH",             "/var/lib/tl2000/icon.png")
SCHEDULES_FILE       = os.getenv("SCHEDULES_FILE",        "/var/lib/tl2000/schedules.json")
RESTORE_ROOT         = os.getenv("RESTORE_ROOT",          "/mnt/restore")
# Pattern for the default restore sub-folder.  Tokens: {volume} {date} {datetime} {tape}
# e.g. "{volume}_{date}"  →  /mnt/restore/SM9158L6_2026-03-30
# Set to "" to restore directly into RESTORE_ROOT (old behaviour).
RESTORE_SUBFOLDER_PATTERN = os.getenv("RESTORE_SUBFOLDER_PATTERN", "{volume}_{date}")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))
LOG_MAX_ROWS       = int(os.getenv("LOG_MAX_ROWS", "5000"))
AUTO_REWRITE_ON_FULL = os.getenv("AUTO_REWRITE_ON_FULL", "true").lower() == "true"
BACKUP_LOG_LEVEL_DEFAULT = os.getenv("BACKUP_LOG_LEVEL_DEFAULT", "normal").strip().lower()

# Email notifications


# Home Assistant notifications (can also be set at runtime via /api/settings/ha)
HA_NOTIFY_URL        = os.getenv("HA_URL",              "")   # e.g. http://homeassistant.local:8123
HA_NOTIFY_TOKEN      = os.getenv("HA_TOKEN",            "")   # long-lived access token
HA_NOTIFY_SERVICE    = os.getenv("HA_NOTIFY_SERVICE",   "notify")  # e.g. "mobile_app_phone" or "notify"
HA_NOTIFY_ENABLED    = os.getenv("HA_NOTIFY_ENABLED",   "true").lower() == "true"

# GFS retention
GFS_DAILY_KEEP       = int(os.getenv("GFS_DAILY_KEEP",   "7"))
GFS_WEEKLY_KEEP      = int(os.getenv("GFS_WEEKLY_KEEP",  "4"))
GFS_MONTHLY_KEEP     = int(os.getenv("GFS_MONTHLY_KEEP", "6"))

# Tape selection strategy: "spread" (round-robin across the library, default)
# or "fill" (concentrate writes on one tape until full, then roll to the next).
TAPE_FILL_STRATEGY   = os.getenv("TAPE_FILL_STRATEGY",   "spread").strip().lower()

# Incremental backups
INCREMENTAL_DIR      = os.getenv("INCREMENTAL_DIR",      "/var/lib/tl2000/incremental")

# Hooks
PRE_BACKUP_HOOK      = os.getenv("PRE_BACKUP_HOOK",      "")
POST_BACKUP_HOOK     = os.getenv("POST_BACKUP_HOOK",     "")

# Tape health / sg3_utils
SG_DEVICE            = os.getenv("SG_DEVICE",            "")  # e.g. /dev/sg0 (the tape drive, not changer)

# Verification
VERIFY_AFTER_BACKUP  = os.getenv("VERIFY_AFTER_BACKUP",  "true").lower() == "true"
VERIFY_SAMPLE_MB     = int(os.getenv("VERIFY_SAMPLE_MB", "512"))  # 0 = full verify

# Backup records
BACKUP_RECORDS_FILE  = os.getenv("BACKUP_RECORDS_FILE",  "/var/lib/tl2000/backup_records.json")
TAPE_CATALOG_DB     = os.getenv("TAPE_CATALOG_DB", "/var/lib/tl2000/tape_catalog.db")

MQTT_HOST            = os.getenv("MQTT_HOST",  "")
MQTT_PORT            = int(os.getenv("MQTT_PORT","1883"))
MQTT_USER            = os.getenv("MQTT_USER",  "")
MQTT_PASS            = os.getenv("MQTT_PASS",  "")
MQTT_BASE            = os.getenv("MQTT_BASE",  "homelab/tl2000")
HA_DISCOVERY_PREFIX  = os.getenv("HA_DISCOVERY_PREFIX","homeassistant")
HAS_MAIL_SLOT       = os.getenv("TL_HAS_MAIL_SLOT",      "false").lower() == "true"
MAGAZINE_SIZE       = int(os.getenv("TL_MAGAZINE_SIZE",     "12"))
CLEANING_WAIT_SECONDS = int(os.getenv("TL_CLEANING_WAIT_SECONDS", "120"))
EJECT_LEFT_CMD       = os.getenv("TL_EJECT_LEFT_CMD", "").strip()
EJECT_RIGHT_CMD      = os.getenv("TL_EJECT_RIGHT_CMD", "").strip()

DEVICE_INFO = {
    "identifiers": ["odin_tl2000"],
    "name": "Odin TL2000",
    "manufacturer": "IBM",
    "model": "3573-TL / TL2000",
    "sw_version": "0.7.0",
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_device_paths():
    """Check the configured device paths before anything tries to use them.

    A wrong-but-plausible value here is the single easiest way to break the
    app, and the failure is silent in the worst way: `mtx -f <bad path> status`
    just exits non-zero, `run_cmd` raises, and the UI reports an empty library
    with the drive offline -- which looks like a hardware fault rather than a
    typo. Stray characters from the Docker device syntax are the usual cause
    (``:/dev/sg10`` instead of ``/dev/sg10``), and a stale /dev/sgN number
    after a reboot is the other.

    Returns a list of human-readable problem strings; empty means all good.
    Purely advisory -- the caller decides how loudly to complain, so the web
    UI can still start and show the message rather than the container dying
    with the reason buried in the logs.
    """
    problems = []
    # SG_DEVICE is optional (health polling only); the other two are required.
    for name, value, required in (
        ("TL_CHANGER", CHANGER,   True),
        ("TL_TAPE",    TAPE,      True),
        ("SG_DEVICE",  SG_DEVICE, False),
    ):
        if not value:
            if required:
                problems.append(f"{name} is empty -- set it to the device path.")
            continue

        if not value.startswith("/"):
            # Catches ':/dev/sg10', 'dev/sg10', quotes, and stray arrows -- all
            # of which are relative paths that will never resolve.
            problems.append(
                f"{name}={value!r} is not an absolute path. Enter the bare device "
                f"path with no colons, quotes or arrows (e.g. /dev/sg10); a value "
                f"copied from Docker's host:container device syntax will look "
                f"like this."
            )
            continue

        if not os.path.exists(value):
            problems.append(
                f"{name}={value!r} does not exist in the container. Either the "
                f"device is not mapped in (add a Device entry for it), or "
                f"/dev/sgN renumbering moved it -- run scripts/identify-devices.sh "
                f"on the host to find the current path."
            )
            continue

        try:
            if not stat.S_ISCHR(os.stat(value).st_mode):
                problems.append(
                    f"{name}={value!r} exists but is not a character device. "
                    f"Expected a /dev node; check the Device mapping."
                )
        except OSError as exc:
            problems.append(f"{name}={value!r} could not be inspected: {exc}")

    return problems


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
