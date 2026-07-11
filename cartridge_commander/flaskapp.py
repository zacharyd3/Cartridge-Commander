"""The Flask application instance.

Split into its own module (rather than living in ``routes.py`` or ``main.py``)
so that every module needing the ``app`` object -- ``routes.py`` for
``@app.route``, ``main.py`` for ``app.run()`` -- can import it without a
circular import.
"""
import os

from flask import Flask

_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_PKG_DIR)

app = Flask(
    __name__,
    template_folder=os.path.join(_REPO_ROOT, "templates"),
    static_folder=os.path.join(_REPO_ROOT, "static"),
)
