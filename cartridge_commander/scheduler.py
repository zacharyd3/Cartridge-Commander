"""scheduler module (split from the original monolithic app.py)."""

import os
import json
import time
import datetime
import calendar
import threading
from .config import SCHEDULES_FILE
from . import state as shared_state
from .state import log_action, now_ts
from .db import _db_get_json, _db_set_json, _prune_app_log
from .backup_worker import backup_worker


def _load_schedules():
    data = _db_get_json("schedules", None)
    if isinstance(data, list):
        shared_state._schedules = data
        return
    os.makedirs(os.path.dirname(SCHEDULES_FILE), exist_ok=True)
    if not os.path.exists(SCHEDULES_FILE):
        shared_state._schedules = []
        return
    try:
        with open(SCHEDULES_FILE) as f:
            shared_state._schedules = json.load(f)
        _db_set_json("schedules", shared_state._schedules)
    except Exception:
        shared_state._schedules = []


def _save_schedules():
    with shared_state._schedules_lock:
        payload = json.loads(json.dumps(shared_state._schedules))
    _db_set_json("schedules", payload)

def _next_run_ts(s):
    mode, hour, minute = s.get("mode","weekly"), int(s.get("hour",2)), int(s.get("minute",0))
    now = datetime.datetime.now()
    base = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if mode == "daily":
        c = base if base > now else base + datetime.timedelta(days=1)
    elif mode == "weekly":
        dow = int(s.get("day_of_week",0))
        da = (dow - now.weekday()) % 7
        c = base + datetime.timedelta(days=da)
        if c <= now: c += datetime.timedelta(weeks=1)
    elif mode == "monthly":
        dom = int(s.get("day_of_month",1))
        try: c = base.replace(day=dom)
        except ValueError: c = base.replace(day=28)
        if c <= now:
            dim = calendar.monthrange(now.year, now.month)[1]
            c += datetime.timedelta(days=dim)
    else:
        return None
    return int(c.timestamp())

def _update_next_run(s): s["next_run"] = _next_run_ts(s)

def scheduler_loop():
    _last_log_prune = 0
    while True:
        time.sleep(30)
        now = now_ts()

        # Prune app_log once per hour instead of on every write
        if now - _last_log_prune > 3600:
            _prune_app_log()
            _last_log_prune = now

        with shared_state._schedules_lock: sched_copy = list(shared_state._schedules)
        for s in sched_copy:
            if not s.get("enabled", True): continue
            nr = s.get("next_run")
            if nr and now >= nr:
                paths, label = s.get("paths",[]), s.get("label","?")
                with shared_state._backup_lock: busy = shared_state._backup_job.get("running")
                if busy:
                    log_action("scheduler", False, f"'{label}' skipped — backup running.")
                else:
                    log_action("scheduler", True, f"'{label}' fired.")
                    threading.Thread(
                        target=backup_worker,
                        args=(paths,),
                        kwargs={"backup_mode": s.get("backup_mode", "full"), "label": label},
                        daemon=True,
                    ).start()
                with shared_state._schedules_lock:
                    for x in shared_state._schedules:
                        if x.get("id") == s.get("id"):
                            _update_next_run(x); x["last_run"] = now
                _save_schedules()

# ---------------------------------------------------------------------------
# Backup tape auto-selection and auto-unload helpers
# ---------------------------------------------------------------------------

