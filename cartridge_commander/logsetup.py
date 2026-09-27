"""Container logging: everything the app does, written to stdout for `docker logs`.

The web UI's log view reads the ``app_log`` table (see ``db.db_log``); this
module is the separate stream that ends up in the container log. ``db_log``
mirrors every entry here, and the lower-level pieces the UI never shows
(shell commands, HTTP actions, MQTT commands, tracebacks) log here directly.

Environment:
  LOG_LEVEL          DEBUG | INFO (default) | WARNING | ERROR
                     DEBUG adds every mtx/mt status poll, read-only API
                     requests, per-file archive lines and thread names.
  LOG_HTTP_REQUESTS  quiet (default) -- skip the UI's /api/status polling,
                                        /healthz, static files and icons
                     all             -- log every request, polling included
"""

import logging
import os
import re
import sys
import threading

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
LOG_HTTP_REQUESTS = os.getenv("LOG_HTTP_REQUESTS", "quiet").strip().lower()

_ROOT_NAME = "cartridge_commander"
_configured = False

# Requests the UI makes on a timer or on every page load. At the default
# setting they are dropped entirely -- they are what buried the useful lines.
_POLL_PATHS = ("/api/status", "/healthz", "/icon.png", "/favicon.ico")
_POLL_PREFIXES = ("/static/",)

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


class _Formatter(logging.Formatter):
    """``2026-09-27 15:24:13 INFO    [backup] message`` -- category is the
    logger name without the package prefix; thread name added at DEBUG."""

    def __init__(self, show_thread: bool):
        super().__init__(datefmt="%Y-%m-%d %H:%M:%S")
        self._show_thread = show_thread

    def format(self, record):
        name = record.name
        if name.startswith(_ROOT_NAME + "."):
            name = name[len(_ROOT_NAME) + 1:]
        thread = f" ({record.threadName})" if self._show_thread else ""
        msg = _ANSI_RE.sub("", record.getMessage())  # werkzeug colours some lines
        line = f"{self.formatTime(record, self.datefmt)} {record.levelname:<7} [{name}]{thread} {msg}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class _WerkzeugFilter(logging.Filter):
    """Drop werkzeug's per-request access lines; ``log_request`` replaces them
    with ones that include the request body and outcome. Werkzeug's other
    messages (startup address, errors) pass through."""

    def filter(self, record):
        msg = record.getMessage()
        return not ('" ' in msg and " HTTP/" in msg)


def get_logger(category: str) -> logging.Logger:
    return logging.getLogger(f"{_ROOT_NAME}.{category}")


def level_from_name(level: str) -> int:
    return _LEVELS.get(str(level or "").strip().lower(), logging.INFO)


def configure_logging() -> None:
    global _configured
    if _configured:
        return
    _configured = True

    level = getattr(logging, LOG_LEVEL, None)
    if not isinstance(level, int):
        level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_Formatter(show_thread=level <= logging.DEBUG))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    # Keep chatty third-party libraries at INFO even when we are at DEBUG.
    for noisy in ("urllib3", "paho"):
        logging.getLogger(noisy).setLevel(max(level, logging.INFO))
    werkzeug = logging.getLogger("werkzeug")
    werkzeug.setLevel(logging.INFO)
    werkzeug.addFilter(_WerkzeugFilter())

    if LOG_LEVEL not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        get_logger("app").warning("Unknown LOG_LEVEL=%r -- using INFO.", LOG_LEVEL)

    # Uncaught exceptions in worker threads would otherwise print a bare
    # traceback with no timestamp, or vanish if stderr is not captured.
    def _thread_excepthook(args):
        if issubclass(args.exc_type, SystemExit):
            return
        get_logger("app").error(
            "Unhandled exception in thread %s",
            args.thread.name if args.thread else "?",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    def _excepthook(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        get_logger("app").critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_tb))

    threading.excepthook = _thread_excepthook
    sys.excepthook = _excepthook


def _is_poll_request(path: str) -> bool:
    return path in _POLL_PATHS or path.startswith(_POLL_PREFIXES)


_REDACT_KEYS = ("pass", "token", "secret", "key", "auth")


def _redact(value, depth=0):
    if depth > 4:
        return value
    if isinstance(value, dict):
        return {
            k: ("***" if any(s in str(k).lower() for s in _REDACT_KEYS) and v else _redact(v, depth + 1))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v, depth + 1) for v in value]
    return value


def _short(text: str, limit: int = 500) -> str:
    text = str(text)
    return text if len(text) <= limit else text[:limit] + f"… (+{len(text) - limit} chars)"


def log_request(request, response, elapsed_s: float) -> None:
    """Log one finished HTTP request.

    State-changing requests (anything but GET/HEAD/OPTIONS) are logged at INFO
    with their JSON body and the API's ok/error result, so a button press in
    the UI shows up as one readable line. Failed requests are WARNING. Plain
    reads are DEBUG, and the UI's polling is skipped unless
    LOG_HTTP_REQUESTS=all.
    """
    log = get_logger("http")
    path = request.path
    status = response.status_code
    method = request.method
    is_read = method in ("GET", "HEAD", "OPTIONS")

    if LOG_HTTP_REQUESTS == "all":
        level = logging.WARNING if status >= 400 else logging.INFO
    elif status >= 400:
        level = logging.WARNING
    elif is_read and _is_poll_request(path):
        return
    elif is_read:
        level = logging.DEBUG
    else:
        level = logging.INFO
    if not log.isEnabledFor(level):
        return

    parts = [f"{request.remote_addr} {method} {request.full_path.rstrip('?')} -> {status} ({elapsed_s * 1000:.0f} ms)"]
    if not is_read:
        body = request.get_json(silent=True)
        if body is not None:
            parts.append(f"body={_short(_redact(body))}")
        elif request.form:
            parts.append(f"form={_short(_redact(request.form.to_dict()))}")
    if response.is_json and not (is_read and status < 400):
        try:
            data = response.get_json(silent=True)
        except Exception:
            data = None
        if isinstance(data, dict):
            if data.get("error"):
                parts.append(f"error={_short(data['error'])}")
                level = max(level, logging.WARNING)
            elif data.get("detail"):
                parts.append(f"result={_short(data['detail'])}")
    log.log(level, " ".join(parts))
