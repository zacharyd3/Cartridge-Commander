"""The Flask application instance.

Split into its own module (rather than living in ``routes.py`` or ``main.py``)
so that every module needing the ``app`` object -- ``routes.py`` for
``@app.route``, ``main.py`` for ``app.run()`` -- can import it without a
circular import.
"""
import os
import time

from flask import Flask, g, request

from .logsetup import log_request

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_PKG_DIR)

app = Flask(
    __name__,
    template_folder=os.path.join(_REPO_ROOT, "templates"),
    static_folder=os.path.join(_REPO_ROOT, "static"),
)


@app.before_request
def _log_request_start():
    g._log_started = time.monotonic()


@app.after_request
def _log_request_done(response):
    # Replaces werkzeug's access line (filtered out in logsetup) with one that
    # carries the request body and the API's ok/error result.
    try:
        log_request(request, response, time.monotonic() - getattr(g, "_log_started", time.monotonic()))
    except Exception:
        pass
    return response
