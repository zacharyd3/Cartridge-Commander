"""mqtt module (split from the original monolithic app.py)."""

import os
import json
import time
import signal
import threading
from typing import Any, Dict
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None
from .config import BACKUP_ROOT, CHANGER, COMMAND_TIMEOUT, DEVICE_INFO, HAS_MAIL_SLOT, HA_DISCOVERY_PREFIX, MQTT_BASE, MQTT_HOST, MQTT_PASS, MQTT_PORT, MQTT_USER, POLL_SECONDS, SG_DEVICE, TAPE, VERIFY_SAMPLE_MB
from . import state as shared_state


_health_cache: Dict[str, Any] = {}
_health_cache_lock = threading.Lock()
_health_last_refresh: int = 0
HEALTH_REFRESH_INTERVAL = int(os.getenv("HEALTH_REFRESH_INTERVAL", "300"))  # 5 min default


def _refresh_health_cache() -> None:
    from .records import get_tape_health
    from .state import now_ts
    global _health_last_refresh
    if not SG_DEVICE:
        return
    h = get_tape_health()
    with _health_cache_lock:
        _health_cache.clear()
        _health_cache.update(h)
    _health_last_refresh = now_ts()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
_mqtt_client = None
_mqtt_connected = False

def mqtt_available(): return bool(MQTT_HOST and mqtt is not None)
def mqtt_topic(*parts): return "/".join([MQTT_BASE.strip("/")] + [p.strip("/") for p in parts])
def ha_topic(comp, oid, suffix="config"): return f"{HA_DISCOVERY_PREFIX}/{comp}/odin_tl2000/{oid}/{suffix}"

def mqtt_publish(topic, payload, retain=True):
    if not mqtt_available() or not _mqtt_client or not _mqtt_connected: return
    if not isinstance(payload, str): payload = json.dumps(payload)
    _mqtt_client.publish(topic, payload, retain=retain)

def publish_discovery():  # noqa: C901
    base = {
        "device": DEVICE_INFO,
        "availability_topic": mqtt_topic("availability"),
        "payload_available": "online",
        "payload_not_available": "offline",
    }

    def sensor(oid, name, st, icon, sc=None, vt=None, unit=None, expire=None, cat=None, dc=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st, "icon": icon}
        if sc:   p["state_class"]        = sc
        if vt:   p["value_template"]     = vt
        if unit: p["unit_of_measurement"] = unit
        if expire: p["expire_after"]     = expire
        if cat:  p["entity_category"]    = cat
        if dc:   p["device_class"]       = dc
        mqtt_publish(ha_topic("sensor", oid), p)

    def binary(oid, name, st, dc=None, cat=None, icon=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "payload_on": "ON", "payload_off": "OFF"}
        if dc:   p["device_class"]    = dc
        if cat:  p["entity_category"] = cat
        if icon: p["icon"]            = icon
        mqtt_publish(ha_topic("binary_sensor", oid), p)

    def button(oid, name, ct, pp, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "command_topic": ct,
             "payload_press": pp, "icon": icon}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("button", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def select_entity(oid, name, st, ct, options, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "options": options, "icon": icon}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("select", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def number_entity(oid, name, st, ct, min_v, max_v, step, unit, icon, cat=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "min": min_v, "max": max_v, "step": step,
             "unit_of_measurement": unit, "icon": icon, "mode": "box"}
        if cat: p["entity_category"] = cat
        mqtt_publish(ha_topic("number", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    def text_entity(oid, name, st, ct, icon, cat=None, pattern=None):
        p = {**base, "name": name, "unique_id": f"odin_tl2000_{oid}",
             "object_id": f"odin_tl2000_{oid}", "state_topic": st,
             "command_topic": ct, "icon": icon}
        if cat:     p["entity_category"] = cat
        if pattern: p["pattern"]         = pattern
        mqtt_publish(ha_topic("text", oid), p)
        if _mqtt_client and _mqtt_connected:
            _mqtt_client.subscribe(ct)

    # ── Library / drive sensors ──────────────────────────────────────────────
    sensor("full_slots",    "Full Slots",    mqtt_topic("state","full_slots"),
           "mdi:archive",    "measurement", "{{ value | int }}", "tapes")
    sensor("empty_slots",   "Empty Slots",   mqtt_topic("state","empty_slots"),
           "mdi:archive-off","measurement", "{{ value | int }}", "tapes")
    sensor("total_slots",   "Total Slots",   mqtt_topic("state","total_slots"),
           "mdi:archive-plus","measurement","{{ value | int }}", "tapes", cat="diagnostic")
    sensor("loaded_slot",   "Loaded Slot",   mqtt_topic("state","loaded_slot"),
           "mdi:numeric",    "measurement", "{{ value | int(0) }}", "")
    sensor("loaded_volume", "Loaded Volume", mqtt_topic("state","loaded_volume"),
           "mdi:tape-drive")
    sensor("density",       "Tape Density",  mqtt_topic("state","density"),
           "mdi:database",   cat="diagnostic")
    if HAS_MAIL_SLOT:
        sensor("import_export_tag","Mail Slot Tag",mqtt_topic("state","import_export_tag"),
               "mdi:mailbox")
    sensor("cleaning_tag",  "Cleaning Tape", mqtt_topic("state","cleaning_tag"),
           "mdi:broom",      cat="diagnostic")
    sensor("last_action",   "Last Action",   mqtt_topic("state","last_action"),
           "mdi:history",    cat="diagnostic")
    sensor("time_in_drive_mins","Time In Drive",mqtt_topic("state","time_in_drive_mins"),
           "mdi:timer",      "measurement", "{{ value | int(0) }}", "min")
    sensor("tape_load_count","Tape Load Count",mqtt_topic("state","tape_load_count"),
           "mdi:counter",    "total_increasing","{{ value | int(0) }}", "",cat="diagnostic")
    sensor("tape_total_written","Tape Total Written",mqtt_topic("state","tape_total_written"),
           "mdi:archive",    "total_increasing","{{ value | float(0) }}", "B",
           dc="data_size", cat="diagnostic")
    sensor("tape_total_written_hr","Tape Total Written (readable)",mqtt_topic("state","tape_total_written_hr"),
           "mdi:archive",    cat="diagnostic")

    # ── Backup sensors ───────────────────────────────────────────────────────
    sensor("backup_status",      "Backup Status",    mqtt_topic("backup","status"),
           "mdi:backup-restore")
    sensor("backup_percent",     "Backup Progress",  mqtt_topic("backup","percent"),
           "mdi:percent",         "measurement","{{ value | float | round(1) }}","%")
    # data_rate device class: HA auto-converts B/s → KB/s → MB/s → GB/s in the UI
    sensor("backup_speed_bps",   "Backup Speed",     mqtt_topic("backup","speed_bps"),
           "mdi:speedometer",     "measurement","{{ value | float(0) }}", "B/s",
           dc="data_rate")
    # data_size device class: HA auto-converts B → KB → MB → GB → TB in the UI
    sensor("backup_bytes_written","Backup Written",  mqtt_topic("backup","bytes_written"),
           "mdi:counter",         "total_increasing","{{ value | float(0) }}", "B",
           dc="data_size")
    sensor("backup_bytes_total", "Backup Total Size",mqtt_topic("backup","bytes_total"),
           "mdi:database",        "measurement","{{ value | float(0) }}", "B",
           dc="data_size")
    # Human-readable companion topics for dashboards that just want a string
    sensor("backup_written_hr",  "Backup Written (readable)", mqtt_topic("backup","bytes_written_hr"),
           "mdi:counter",         cat="diagnostic")
    sensor("backup_total_hr",    "Backup Total (readable)",   mqtt_topic("backup","bytes_total_hr"),
           "mdi:database",        cat="diagnostic")
    sensor("backup_speed_hr",    "Backup Speed (readable)",   mqtt_topic("backup","speed_hr"),
           "mdi:speedometer",     cat="diagnostic")
    sensor("backup_eta_secs",    "Backup ETA",       mqtt_topic("backup","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int }}", "s")
    sensor("backup_last_msg",    "Backup Message",   mqtt_topic("backup","last_message"),
           "mdi:text-box-outline",expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("backup_last_vol",    "Last Backup Volume",mqtt_topic("backup","last_volume"),
           "mdi:tape-drive")
    sensor("backup_last_ok_ts",  "Last Successful Backup",mqtt_topic("backup","last_ok_ts"),
           "mdi:check-circle",    "measurement","{{ value | int }}", "")
    sensor("backup_last_written","Last Backup Size", mqtt_topic("backup","last_written_hr"),
           "mdi:archive",         cat="diagnostic")
    sensor("backup_mode",        "Backup Mode",      mqtt_topic("backup","mode"),
           "mdi:layers",          cat="diagnostic")

    # ── Verification sensors ─────────────────────────────────────────────────
    sensor("verify_status",      "Verify Status",    mqtt_topic("verify","status"),
           "mdi:shield-check")
    sensor("verify_errors",      "Verify Errors",    mqtt_topic("verify","errors"),
           "mdi:alert-circle",    "measurement","{{ value | int }}", "")
    sensor("verify_bytes",       "Verify Bytes Read",mqtt_topic("verify","bytes_verified"),
           "mdi:eye-check",       "measurement","{{ value | float(0) }}", "B",
           dc="data_size", cat="diagnostic")
    sensor("verify_bytes_hr",    "Verify Read (readable)", mqtt_topic("verify","bytes_verified_hr"),
           "mdi:eye-check",       cat="diagnostic")
    sensor("verify_last_msg",    "Verify Message",   mqtt_topic("verify","last_message"),
           "mdi:text",            expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("verify_eta_secs",    "Verify ETA",       mqtt_topic("verify","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int }}", "s", cat="diagnostic")

    # ── Restore sensors ──────────────────────────────────────────────────────
    sensor("restore_status",     "Restore Status",   mqtt_topic("restore","status"),
           "mdi:restore")
    sensor("restore_last_msg",   "Restore Message",  mqtt_topic("restore","last_message"),
           "mdi:text",            expire=POLL_SECONDS*4, cat="diagnostic")
    sensor("restore_dest",       "Restore Destination",mqtt_topic("restore","dest"),
           "mdi:folder-arrow-down",cat="diagnostic")

    # ── Inventory sensors ────────────────────────────────────────────────────
    sensor("inventory_status",   "Inventory Status", mqtt_topic("inventory","status"),
           "mdi:magnify")
    sensor("inventory_mode",     "Inventory Mode",   mqtt_topic("inventory","mode"),
           "mdi:lightning-bolt", cat="diagnostic")
    sensor("inventory_progress", "Inventory Progress",mqtt_topic("inventory","progress"),
           "mdi:progress-check",  "measurement","{{ value | int(0) }}", "%")
    sensor("inventory_scanned",  "Tapes Scanned",    mqtt_topic("inventory","scanned"),
           "mdi:check-all",       "measurement","{{ value | int(0) }}", "tapes",cat="diagnostic")
    sensor("inventory_eta_secs", "Inventory ETA",   mqtt_topic("inventory","eta_seconds"),
           "mdi:timer-outline",   "measurement","{{ value | int(0) }}", "s", cat="diagnostic")

    # ── Tape health sensors ──────────────────────────────────────────────────
    sensor("health_write_errors","Write Uncorrected Errors",mqtt_topic("health","write_uncorrected"),
           "mdi:pencil-off",      "measurement","{{ value | int(-1) }}", "")
    sensor("health_read_errors", "Read Uncorrected Errors", mqtt_topic("health","read_uncorrected"),
           "mdi:eye-off",         "measurement","{{ value | int(-1) }}", "")

    # ── GFS / retention sensors ──────────────────────────────────────────────
    sensor("gfs_recyclable",     "Recyclable Tapes", mqtt_topic("gfs","recyclable_count"),
           "mdi:recycle",         "measurement","{{ value | int(0) }}", "tapes")
    sensor("last_backup_record_status","Last Job Status",mqtt_topic("backup","last_record_status"),
           "mdi:clipboard-check")

    # ── Binary sensors ───────────────────────────────────────────────────────
    binary("drive_online",       "Drive Reachable",  mqtt_topic("state","drive_online"),
           dc="connectivity")
    binary("tape_loaded",        "Tape Loaded",      mqtt_topic("state","tape_loaded"),
           icon="mdi:tape-drive")
    binary("at_bot",             "Tape At BOT",      mqtt_topic("state","at_bot"),
           icon="mdi:rewind", cat="diagnostic")
    binary("backup_running",     "Backup Running",   mqtt_topic("backup","running"),
           dc="running")
    binary("restore_running",    "Restore Running",  mqtt_topic("restore","running"),
           dc="running")
    binary("inventory_running",  "Inventory Running",mqtt_topic("inventory","running"),
           dc="running")
    binary("inventory_paused",   "Inventory Paused", mqtt_topic("inventory","paused"),
           icon="mdi:pause-circle")
    binary("verify_running",     "Verify Running",   mqtt_topic("verify","running"),
           dc="running")
    binary("cleaning_needed",    "Cleaning Needed",  mqtt_topic("health","cleaning_needed"),
           dc="problem", icon="mdi:broom")
    binary("backup_healthy",     "Backup System OK", mqtt_topic("state","backup_healthy"),
           icon="mdi:shield-check")
    binary("last_verify_passed", "Last Verify Passed",mqtt_topic("verify","last_passed"),
           icon="mdi:shield-check")

    # ── Buttons ──────────────────────────────────────────────────────────────
    button("cmd_rewind",     "Rewind Tape",      mqtt_topic("cmd","rewind"),   "rewind","mdi:rewind")
    button("cmd_unload",     "Unload Tape",      mqtt_topic("cmd","unload"),   "unload","mdi:eject")
    button("cmd_refresh",    "Refresh Status",   mqtt_topic("cmd","refresh"),  "refresh","mdi:refresh", cat="diagnostic")
    button("cmd_stop_backup","Stop Backup",      mqtt_topic("cmd","stop_backup"),"stop","mdi:stop")
    button("cmd_read_index", "Read Tape Index",  mqtt_topic("cmd","read_index"),"read","mdi:format-list-bulleted")
    button("cmd_inventory",       "Run Inventory",       mqtt_topic("cmd","inventory"),       "scan","mdi:magnify-scan")
    button("cmd_inventory_quick", "Run Quick Scan",      mqtt_topic("cmd","inventory_quick"), "scan","mdi:barcode-scan")
    button("cmd_inventory_pause", "Pause Inventory",     mqtt_topic("cmd","inventory_pause"), "pause","mdi:pause")
    button("cmd_inventory_resume","Resume Inventory",    mqtt_topic("cmd","inventory_resume"),"play","mdi:play")
    button("cmd_inventory_stop",  "Stop Inventory",      mqtt_topic("cmd","inventory_stop"),  "stop","mdi:stop-circle")
    button("cmd_verify",     "Verify Tape",      mqtt_topic("cmd","verify"),    "verify","mdi:shield-check")
    button("cmd_backup_full","Backup (Full)",    mqtt_topic("cmd","backup_full"),"full","mdi:backup-restore")
    button("cmd_backup_incr","Backup (Incr.)",   mqtt_topic("cmd","backup_incr"),"incr","mdi:delta")
    if HAS_MAIL_SLOT:
        button("cmd_eject_mail", "Eject Mail Slot",  mqtt_topic("cmd","eject_mail"),"eject","mdi:email-arrow-right")

    # ── Selects ──────────────────────────────────────────────────────────────
    select_entity("sel_backup_mode",  "Backup Mode",
                  mqtt_topic("backup","mode_select_state"),
                  mqtt_topic("cmd","set_backup_mode"),
                  ["full","incremental","differential"],
                  "mdi:layers", cat="config")

    # ── Number controls ──────────────────────────────────────────────────────
    number_entity("num_verify_sample_mb","Verify Sample Size",
                  mqtt_topic("config","verify_sample_mb"),
                  mqtt_topic("cmd","set_verify_sample_mb"),
                  0, 4096, 128, "MB", "mdi:eye-check", cat="config")
    number_entity("num_load_slot",    "Load Slot Number",
                  mqtt_topic("config","load_slot"),
                  mqtt_topic("cmd","set_load_slot"),
                  1, max(1, (24 if not HAS_MAIL_SLOT else 48)), 1, "", "mdi:numeric", cat="config")

    # ── Text inputs ──────────────────────────────────────────────────────────
    text_entity("txt_backup_label", "Backup Job Label",
                mqtt_topic("config","backup_label"),
                mqtt_topic("cmd","set_backup_label"),
                "mdi:label", cat="config")

    # Subscribe to load and backup_start (parametric, not simple buttons)
    if _mqtt_client and _mqtt_connected:
        _mqtt_client.subscribe(mqtt_topic("cmd","load"))
        _mqtt_client.subscribe(mqtt_topic("cmd","backup_start"))

    # Publish initial state for selects/numbers/texts
    mqtt_publish(mqtt_topic("backup","mode_select_state"), "full")
    mqtt_publish(mqtt_topic("config","verify_sample_mb"),  str(VERIFY_SAMPLE_MB))
    mqtt_publish(mqtt_topic("config","load_slot"),         "1")
    mqtt_publish(mqtt_topic("config","backup_label"),      "")


# Runtime config state (set by HA controls via MQTT)
_mqtt_runtime_cfg = {
    "backup_mode":      "full",
    "load_slot":        1,
    "backup_label":     "",
    "verify_sample_mb": VERIFY_SAMPLE_MB,
}


def publish_state_to_mqtt(state):  # noqa: C901
    from .records import gfs_get_recyclable
    from .state import bytes_human, now_ts
    if not mqtt_available(): return
    mqtt_publish(mqtt_topic("availability"), "online")

    s     = state.get("summary", {})
    drive = state.get("drive", {})
    bk    = state.get("backup_job", {})
    inv   = state.get("inventory_job", {})
    rst   = state.get("restore_job", {})
    vj    = state.get("verify_job", {})

    drive_reachable = bool(drive.get("online", s.get("online", False)))
    tape_loaded = bool(not drive.get("empty", not s.get("loaded", False)))
    inventory_running = bool(inv.get("running", False))
    verify_running = bool(vj.get("running", False))

    # ── Library / drive ──────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("state","loaded_volume"),    s.get("loaded_volume",""))
    mqtt_publish(mqtt_topic("state","import_export_tag"),s.get("import_export_tag",""))
    mqtt_publish(mqtt_topic("state","cleaning_tag"),     s.get("cleaning_tag",""))
    mqtt_publish(mqtt_topic("state","density"),          s.get("density",""))
    mqtt_publish(mqtt_topic("state","drive_online"),     "ON" if drive_reachable else "OFF")
    mqtt_publish(mqtt_topic("state","tape_loaded"),      "ON" if tape_loaded else "OFF")
    mqtt_publish(mqtt_topic("state","at_bot"),           "ON" if s.get("at_bot")  else "OFF")
    mqtt_publish(mqtt_topic("state","full_slots"),       str(int(s.get("full_slots",   0))))
    mqtt_publish(mqtt_topic("state","empty_slots"),      str(int(s.get("empty_slots",  0))))
    mqtt_publish(mqtt_topic("state","total_slots"),      str(int(s.get("total_slots",  0))))
    ls = s.get("loaded_slot")
    mqtt_publish(mqtt_topic("state","loaded_slot"),      str(int(ls)) if ls else "0")
    mqtt_publish(mqtt_topic("state","last_action"),
                 shared_state._action_log[0]["detail"] if shared_state._action_log else "idle")

    # Time in drive (minutes, integer)
    tind = None
    if shared_state._drive_loaded_at and not (state.get("drive") or {}).get("empty"):
        tind = (now_ts() - shared_state._drive_loaded_at) // 60
    mqtt_publish(mqtt_topic("state","time_in_drive_mins"), str(tind) if tind is not None else "0")

    # Load count and total data written for loaded tape
    # FIX: read all values inside the lock so no other thread can mutate shared_state._drive_history
    # between the lock release and the reads that follow.
    vol = s.get("loaded_volume","")
    with shared_state._drive_history_lock:
        hist             = shared_state._drive_history.get(vol, {}) if vol else {}
        _tape_load_count = int(hist.get("load_count", 0))
        _tape_total_bw   = int(hist.get("total_backup_bytes", 0) or 0)
    mqtt_publish(mqtt_topic("state","tape_load_count"),       str(_tape_load_count))
    mqtt_publish(mqtt_topic("state","tape_total_written"),    str(_tape_total_bw))
    mqtt_publish(mqtt_topic("state","tape_total_written_hr"), bytes_human(_tape_total_bw) if _tape_total_bw else "0 B")

    # Overall system health: empty drive is OK; inventory load/unload churn is OK.
    # Only mark unhealthy for actual command/state errors, a failed last backup, or verify errors.
    last_bk_ok = True
    with shared_state._backup_records_lock:
        if shared_state._backup_records:
            last_bk_ok = shared_state._backup_records[0].get("status") == "completed"

    last_verify_passed = True
    if verify_running:
        last_verify_passed = True
    elif vj.get("status", "idle") not in ("idle", "completed"):
        last_verify_passed = not (vj.get("errors", 0) > 0)
    elif vj.get("status") == "completed":
        last_verify_passed = not (vj.get("errors", 0) > 0)

    system_healthy = True
    if state.get("last_error"):
        system_healthy = False
    elif not drive_reachable:
        system_healthy = False
    elif not last_bk_ok:
        system_healthy = False
    elif not last_verify_passed:
        system_healthy = False

    # Inventory activity should not count as a fault by itself.
    if inventory_running:
        system_healthy = system_healthy and drive_reachable

    mqtt_publish(mqtt_topic("state","backup_healthy"),
                 "ON" if system_healthy else "OFF")

    # ── Backup ───────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("backup","running"),        "ON" if bk.get("running") else "OFF")
    mqtt_publish(mqtt_topic("backup","status"),         bk.get("status","idle"))
    mqtt_publish(mqtt_topic("backup","percent"),        f"{float(bk.get('percent',0)):.2f}")
    _bk_speed = float(bk.get("speed_bps", 0) or 0)
    _bk_written = int(bk.get("bytes_written", 0) or 0)
    _bk_total = int(bk.get("bytes_total", 0) or 0)
    mqtt_publish(mqtt_topic("backup","speed_bps"),      f"{_bk_speed:.1f}")
    mqtt_publish(mqtt_topic("backup","bytes_written"),  str(_bk_written))
    mqtt_publish(mqtt_topic("backup","bytes_total"),    str(_bk_total))
    # Human-readable companions
    mqtt_publish(mqtt_topic("backup","speed_hr"),       bytes_human(_bk_speed) + "/s")
    mqtt_publish(mqtt_topic("backup","bytes_written_hr"), bytes_human(_bk_written))
    mqtt_publish(mqtt_topic("backup","bytes_total_hr"), bytes_human(_bk_total))
    eta = bk.get("eta_seconds")
    mqtt_publish(mqtt_topic("backup","eta_seconds"),    str(int(eta)) if eta is not None else "0")
    mqtt_publish(mqtt_topic("backup","last_message"),   bk.get("last_message",""))
    mqtt_publish(mqtt_topic("backup","mode"),           _mqtt_runtime_cfg.get("backup_mode","full"))

    # Last completed backup info (from records)
    with shared_state._backup_records_lock:
        recs = list(shared_state._backup_records)
    last_ok = next((r for r in recs if r.get("status")=="completed"), None)
    mqtt_publish(mqtt_topic("backup","last_volume"),    last_ok.get("volume_tag","") if last_ok else "")
    mqtt_publish(mqtt_topic("backup","last_ok_ts"),     str(int(last_ok.get("finished_at",0))) if last_ok else "0")
    mqtt_publish(mqtt_topic("backup","last_written_hr"),
                 bytes_human(int(last_ok.get("bytes_written",0))) if last_ok else "0 B")
    last_rec = recs[0] if recs else None
    mqtt_publish(mqtt_topic("backup","last_record_status"), last_rec.get("status","none") if last_rec else "none")

    # ── Verification ─────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("verify","running"),         "ON" if vj.get("running") else "OFF")
    mqtt_publish(mqtt_topic("verify","status"),          vj.get("status","idle"))
    mqtt_publish(mqtt_topic("verify","errors"),          str(int(vj.get("errors",0))))
    _vj_bytes = int(vj.get("bytes_verified", 0) or 0)
    mqtt_publish(mqtt_topic("verify","bytes_verified"),  str(_vj_bytes))
    mqtt_publish(mqtt_topic("verify","bytes_verified_hr"), bytes_human(_vj_bytes))
    v_eta = vj.get("eta_seconds")
    mqtt_publish(mqtt_topic("verify","eta_seconds"),     str(int(v_eta)) if v_eta is not None else "0")
    mqtt_publish(mqtt_topic("verify","last_message"),    vj.get("last_message",""))
    vst = vj.get("status","idle")
    mqtt_publish(mqtt_topic("verify","last_passed"),
                 "ON" if (vst in ("completed",) and vj.get("errors",0)==0) else
                 ("OFF" if vst in ("completed_with_errors","failed") else "OFF"))

    # ── Restore ──────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("restore","running"),        "ON" if rst.get("running") else "OFF")
    mqtt_publish(mqtt_topic("restore","status"),         rst.get("status","idle"))
    mqtt_publish(mqtt_topic("restore","last_message"),   rst.get("last_message",""))
    mqtt_publish(mqtt_topic("restore","dest"),           rst.get("dest",""))

    # ── Inventory ────────────────────────────────────────────────────────────
    mqtt_publish(mqtt_topic("inventory","running"),      "ON" if inv.get("running") else "OFF")
    mqtt_publish(mqtt_topic("inventory","paused"),       "ON" if inv.get("paused") else "OFF")
    mqtt_publish(mqtt_topic("inventory","status"),       inv.get("status","idle"))
    mqtt_publish(mqtt_topic("inventory","mode"),         inv.get("mode","full"))
    total = inv.get("total_slots",0)
    scanned = inv.get("scanned",0)
    pct = int(scanned/total*100) if total>0 else 0
    mqtt_publish(mqtt_topic("inventory","progress"),     str(pct))
    mqtt_publish(mqtt_topic("inventory","scanned"),      str(scanned))
    i_eta = inv.get("eta_seconds")
    mqtt_publish(mqtt_topic("inventory","eta_seconds"),  str(int(i_eta)) if i_eta is not None else "0")

    # ── Tape health ──────────────────────────────────────────────────────────
    # (we cache health data to avoid calling sg_logs on every poll — updated separately)
    with _health_cache_lock:
        hc = dict(_health_cache)
    mqtt_publish(mqtt_topic("health","write_uncorrected"),
                 str(hc.get("write_uncorrected",-1)) if hc.get("write_uncorrected") is not None else "-1")
    mqtt_publish(mqtt_topic("health","read_uncorrected"),
                 str(hc.get("read_uncorrected",-1)) if hc.get("read_uncorrected") is not None else "-1")
    mqtt_publish(mqtt_topic("health","cleaning_needed"), "ON" if hc.get("cleaning_required") else "OFF")

    # ── GFS ──────────────────────────────────────────────────────────────────
    recyclable = gfs_get_recyclable()
    mqtt_publish(mqtt_topic("gfs","recyclable_count"), str(len(recyclable)))

    # ── Full JSON snapshot (for advanced HA templates) ────────────────────────
    mqtt_publish(mqtt_topic("state","raw_json"), state)


def _handle_mqtt_cmd(topic, payload):  # noqa: C901
    from .db import read_tape_index_live, save_tape_index, update_tape_index_metadata
    from .verify_worker import verify_worker
    from .drive_history import _save_last_known_loaded_slot, get_effective_loaded_slot
    from .state import TapeError, is_cleaning_volume_tag, log_action, now_ts, request_inventory_pause, request_inventory_resume, request_inventory_stop, run_cmd
    from .inventory_worker import inventory_worker
    from .changer import refresh_state
    from .backup_worker import backup_worker
    suffix  = topic.split("/")[-1]
    payload = payload.strip()

    # ── Drive controls ───────────────────────────────────────────────────────
    if suffix == "rewind":
        def _do():
            try: run_cmd(["mt","-f",TAPE,"rewind"]); log_action("mqtt_rewind",True,"Rewound via MQTT")
            except Exception as e: log_action("mqtt_rewind",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "unload":
        def _do():
            slot = get_effective_loaded_slot()
            if slot:
                try: run_cmd(["mtx","-f",CHANGER,"unload",str(slot),"0"]); _save_last_known_loaded_slot(None); log_action("mqtt_unload",True,f"Unloaded {slot}")
                except Exception as e: log_action("mqtt_unload",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "load":
        # payload = slot number, or use _mqtt_runtime_cfg["load_slot"]
        try:
            slot = int(payload) if payload.isdigit() else _mqtt_runtime_cfg.get("load_slot", 1)
            def _do(s=slot):
                try:
                    refresh_state()
                    if not (shared_state._state_cache.get("drive") or {}).get("empty"):
                        raise TapeError("Drive already has a tape loaded.")
                    run_cmd(["mtx","-f",CHANGER,"load",str(s),"0"])
                    _save_last_known_loaded_slot(s)
                    log_action("mqtt_load",True,f"Loaded slot {s}")
                except Exception as e:
                    log_action("mqtt_load",False,str(e))
                publish_state_to_mqtt(refresh_state())
            threading.Thread(target=_do, daemon=True).start()
        except Exception as e:
            log_action("mqtt_load",False,str(e))

    elif suffix == "eject_mail":
        def _do():
            slot = (shared_state._state_cache.get("summary") or {}).get("cleaning_slot") or 1
            try: run_cmd(["mtx","-f",CHANGER,"transfer","0",str(slot)]); log_action("mqtt_eject_mail",True,"Mail slot ejected")
            except Exception as e: log_action("mqtt_eject_mail",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    # ── Backup controls ──────────────────────────────────────────────────────
    elif suffix in ("backup_full", "backup_incr"):
        mode = "full" if suffix == "backup_full" else "incremental"
        with shared_state._schedules_lock: scheds = list(shared_state._schedules)
        # Use paths from first enabled schedule, or fall back to BACKUP_ROOT
        paths = next((s.get("paths",[]) for s in scheds if s.get("enabled")), [BACKUP_ROOT])
        label = _mqtt_runtime_cfg.get("backup_label","") or f"HA {mode} backup"
        with shared_state._backup_lock: busy = shared_state._backup_job.get("running")
        if busy:
            log_action("mqtt_backup",False,"Backup already running")
        else:
            log_action("mqtt_backup",True,f"Starting {mode} backup via HA")
            threading.Thread(target=backup_worker, args=(paths,),
                             kwargs={"backup_mode": mode, "label": label},
                             daemon=True).start()

    elif suffix == "backup_start":
        try:
            d = json.loads(payload)
            paths = d.get("paths",[])
            mode  = d.get("mode","full")
            label = d.get("label","") or _mqtt_runtime_cfg.get("backup_label","")
            if paths:
                with shared_state._backup_lock:
                    if not shared_state._backup_job.get("running"):
                        threading.Thread(target=backup_worker, args=(paths,),
                                        kwargs={"backup_mode": mode, "label": label},
                                        daemon=True).start()
        except Exception as e: log_action("mqtt_backup_start",False,str(e))

    elif suffix == "stop_backup":
        shared_state._stop_requested = True
        if shared_state._tar_proc:
            try: shared_state._tar_proc.send_signal(signal.SIGTERM)
            except Exception: pass
        log_action("mqtt_stop",True,"Stop backup via HA")
        publish_state_to_mqtt(refresh_state())

    # ── Verification ─────────────────────────────────────────────────────────
    elif suffix == "verify":
        vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume","")
        if vol:
            with shared_state._verify_lock:
                if not shared_state._verify_job.get("running"):
                    threading.Thread(target=verify_worker, args=(vol,), daemon=True).start()
                    log_action("mqtt_verify",True,f"Verify started for {vol}")
        else:
            log_action("mqtt_verify",False,"No tape loaded")

    # ── Index / inventory ─────────────────────────────────────────────────────
    elif suffix == "read_index":
        def _do():
            vol = (shared_state._state_cache.get("summary") or {}).get("loaded_volume","")
            try:
                if is_cleaning_volume_tag(vol):
                    update_tape_index_metadata(vol or "unknown", present=True, purpose="cleaning", is_cleaning=True)
                    raise TapeError(f"{vol} is a cleaning tape; index read skipped.")
                run_cmd(["mt","-f",TAPE,"rewind"], timeout=max(COMMAND_TIMEOUT,300))
                fl = read_tape_index_live()
                save_tape_index(vol or "unknown", fl, now_ts())
                log_action("mqtt_read_index",True,f"{len(fl)} files for {vol}")
            except Exception as e: log_action("mqtt_read_index",False,str(e))
            publish_state_to_mqtt(refresh_state())
        threading.Thread(target=_do, daemon=True).start()

    elif suffix == "inventory":
        with shared_state._inventory_lock:
            if not shared_state._inventory_job.get("running"):
                threading.Thread(target=inventory_worker, kwargs={"mode":"full"}, daemon=True).start()
    elif suffix == "inventory_quick":
        with shared_state._inventory_lock:
            if not shared_state._inventory_job.get("running"):
                threading.Thread(target=inventory_worker, kwargs={"mode":"quick"}, daemon=True).start()
    elif suffix == "inventory_pause":
        request_inventory_pause()
        publish_state_to_mqtt(refresh_state())
    elif suffix == "inventory_resume":
        request_inventory_resume()
        publish_state_to_mqtt(refresh_state())
    elif suffix == "inventory_stop":
        request_inventory_stop()
        publish_state_to_mqtt(refresh_state())

    # ── Config setters (from HA selects/numbers/texts) ────────────────────────
    elif suffix == "set_backup_mode":
        if payload in ("full","incremental","differential"):
            _mqtt_runtime_cfg["backup_mode"] = payload
            mqtt_publish(mqtt_topic("backup","mode_select_state"), payload)
            mqtt_publish(mqtt_topic("backup","mode"), payload)
            log_action("mqtt_config",True,f"Backup mode set to {payload}")

    elif suffix == "set_load_slot":
        try:
            _mqtt_runtime_cfg["load_slot"] = int(payload)
            mqtt_publish(mqtt_topic("config","load_slot"), str(int(payload)))
        except Exception: pass

    elif suffix == "set_verify_sample_mb":
        try:
            _mqtt_runtime_cfg["verify_sample_mb"] = int(float(payload))
            mqtt_publish(mqtt_topic("config","verify_sample_mb"), str(int(float(payload))))
        except Exception: pass

    elif suffix == "set_backup_label":
        _mqtt_runtime_cfg["backup_label"] = payload
        mqtt_publish(mqtt_topic("config","backup_label"), payload)

    elif suffix == "refresh":
        publish_state_to_mqtt(refresh_state())
        # Also refresh health cache
        threading.Thread(target=_refresh_health_cache, daemon=True).start()

def mqtt_loop():
    from .state import log_action, now_ts
    from .changer import refresh_state
    global _mqtt_client, _mqtt_connected
    if not mqtt_available(): return
    def on_connect(c,u,f,rc,props=None):
        global _mqtt_connected; _mqtt_connected = (rc==0)
        if _mqtt_connected: publish_discovery(); publish_state_to_mqtt(refresh_state())
    def on_disconnect(c,u,rc,props=None):
        global _mqtt_connected; _mqtt_connected = False
    def on_message(c,u,msg):
        try: _handle_mqtt_cmd(msg.topic,(msg.payload or b"").decode(errors="ignore"))
        except Exception as e: log_action("mqtt_msg",False,str(e))
    _mqtt_client = (mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
                    if hasattr(mqtt,"CallbackAPIVersion") else mqtt.Client())
    if MQTT_USER: _mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    _mqtt_client.on_connect = on_connect
    _mqtt_client.on_disconnect = on_disconnect
    _mqtt_client.on_message = on_message
    _mqtt_client.will_set(mqtt_topic("availability"), "offline", retain=True)
    _mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
    _mqtt_client.loop_start()
    while True:
        try:
            publish_state_to_mqtt(refresh_state())
            # Refresh health data on its own slower cadence
            if SG_DEVICE and (now_ts() - _health_last_refresh) >= HEALTH_REFRESH_INTERVAL:
                _refresh_health_cache()
        except Exception as e: log_action("mqtt_publish",False,str(e))
        time.sleep(POLL_SECONDS)

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

