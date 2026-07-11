"""notify module (split from the original monolithic app.py)."""

import json
import datetime
from typing import List


def _ha_notify(title: str, message: str) -> None:
    """Send a notification via Home Assistant's notify service REST API.

    Silently does nothing if HA URL/token are unset or HA notifications are
    disabled.  Uses only the stdlib so no extra dependencies are required.
    """
    from .settings import get_ha_config
    from .state import log_action
    import urllib.request, urllib.error
    cfg = get_ha_config()
    if not cfg["enabled"] or not cfg["url"] or not cfg["token"]:
        return
    service = cfg["service"] or "notify"
    url = f"{cfg['url']}/api/services/notify/{service}"
    payload = json.dumps({"title": f"[TL2000] {title}", "message": message}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {cfg['token']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        log_action("ha_notify", True, f"Sent: {title}")
    except urllib.error.HTTPError as e:
        body = e.read(200).decode(errors="ignore")
        log_action("ha_notify", False, f"HTTP {e.code} sending '{title}': {body}")
    except Exception as e:
        log_action("ha_notify", False, f"Failed to send '{title}': {e}")


def notify_backup_success(vol: str, paths: List[str], bw: int, elapsed: float,
                           verified: bool, verify_errors: int) -> None:
    from .settings import _render_notify_template, get_notify_config
    from .state import bytes_human, secs_human
    if not get_notify_config()["on_backup_success"]:
        return
    ver_str = ("Yes — " + str(verify_errors) + " errors") if verified else "No"
    status = "✅ COMPLETED" + (" + ✅ VERIFIED" if verified and verify_errors == 0
                               else f" + ⚠️ VERIFY ERRORS ({verify_errors})" if verified else "")
    tokens = dict(
        vol=vol, paths=", ".join(paths),
        written=bytes_human(bw), duration=secs_human(int(elapsed)),
        speed=bytes_human(bw / max(elapsed, 1)),
        verified=ver_str, errors=str(verify_errors), error="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_success_title", **tokens)
    body  = _render_notify_template("backup_success_body",  **tokens)
    _ha_notify(title, body)


def notify_backup_failure(vol: str, paths: List[str], error: str) -> None:
    from .settings import _render_notify_template, get_notify_config
    if not get_notify_config()["on_backup_failure"]:
        return
    tokens = dict(
        vol=vol, paths=", ".join(paths), error=error,
        written="", duration="", speed="", verified="", errors="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("backup_failure_title", **tokens)
    body  = _render_notify_template("backup_failure_body",  **tokens)
    _ha_notify(title, body)


def notify_verify_failure(vol: str, errors: int, detail: str) -> None:
    from .settings import _render_notify_template, get_notify_config
    if not get_notify_config()["on_verify_failure"]:
        return
    tokens = dict(
        vol=vol, errors=str(errors), error=detail[:300],
        paths="", written="", duration="", speed="", verified="",
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    title = _render_notify_template("verify_failure_title", **tokens)
    body  = _render_notify_template("verify_failure_body",  **tokens)
    _ha_notify(title, body)


def notify_format_complete(vols: List[str], failed: List[str]) -> None:
    from .settings import get_notify_config
    if not get_notify_config()["on_format_complete"]:
        return
    vol = ", ".join(vols) if vols else "(none)"
    err = ", ".join(failed) if failed else ""
    status = "✅ Format complete" if not failed else "⚠️ Format completed with errors"
    title = f"[TL2000] {status}"
    body  = f"{status}\nFormatted: {vol}" + (f"\nFailed: {err}" if err else "")
    _ha_notify(title, body)


def notify_inventory_done(total: int, added: int, changed: int) -> None:
    from .settings import get_notify_config
    if not get_notify_config()["on_inventory_done"]:
        return
    title = "[TL2000] Inventory complete"
    body  = f"✅ Inventory done — {total} tapes, {added} added, {changed} changed"
    _ha_notify(title, body)



# ---------------------------------------------------------------------------
# Backup records (persistent job history)
# ---------------------------------------------------------------------------

