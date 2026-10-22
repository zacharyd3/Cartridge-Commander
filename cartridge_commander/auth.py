"""Google sign-in (OpenID Connect) in front of the web UI and API.

Off unless GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET is set. When on, every
request except the health check, icon, static files and the sign-in pages
needs a session from signing in with one of the AUTH_ALLOWED_EMAILS accounts:

  /login          sign-in page, and where a refused sign-in says why
  /auth/google    starts Google's authorization code flow (state, nonce, PKCE)
  /auth/callback  Google redirects back here; Authlib checks the ID token's
                  signature, issuer, audience, expiry and nonce, then this
                  module checks the allowlist and, with AUTH_REQUIRE_MFA,
                  that Google reported a multi-factor sign-in (``amr``)
  /logout         POST, ends the session

The session is Flask's signed cookie, valid for AUTH_SESSION_HOURS from
sign-in. Requests that change anything also need the session's CSRF token,
which core.js sends as ``X-CSRF-Token``. The shared TL_WEBUI_PASSWORD API key
is not accepted in place of a session, so nothing gets in without Google.

A configuration problem fails closed: the gate stays up and nobody can sign
in until it is fixed.
"""

import datetime
import hmac
import json
import os
import re
import secrets
import time
from urllib.parse import urlencode, urlsplit

from flask import jsonify, redirect, render_template, request, session

from .config import AUTH_ALLOWED_EMAILS, AUTH_REQUIRE_MFA, AUTH_SESSION_HOURS, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, PUBLIC_URL, SECRET_KEY_FILE
from .db import db_log
from .flaskapp import app
from .logsetup import get_logger, level_from_name

_log = get_logger("auth")

SSO_ENABLED = bool(GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET)
ALLOWED_EMAILS = frozenset(e.lower() for e in re.split(r"[\s,;]+", AUTH_ALLOWED_EMAILS) if e)

_GOOGLE_METADATA_URL = "https://accounts.google.com/.well-known/openid-configuration"

# `amr` values that prove more than a password: Google's own "mfa", or a
# sign-in with a security key (hwk) or passkey (swk), each multi-factor itself.
_MFA_AMR = frozenset({"mfa", "hwk", "swk"})

# Reachable without signing in ("healthz" and "serve_icon" are in routes.py).
_PUBLIC_ENDPOINTS = frozenset({"healthz", "serve_icon", "static", "login", "auth_google", "auth_callback"})

# ?error= codes for /login -> (callout tone, message). Codes only, so the page
# never echoes text from the URL.
_LOGIN_MESSAGES = {
    "not_allowed":  ("bad",  "That Google account isn't allowed to use Cartridge Commander."),
    "unverified":   ("bad",  "Google hasn't verified that account's email address."),
    "mfa_required": ("bad",  "Google didn't report a multi-factor sign-in. Turn on 2-Step Verification "
                             "for this account, then sign in again."),
    "mfa_unknown":  ("bad",  "Google didn't say how this account signed in, so a multi-factor sign-in "
                             "can't be confirmed. See the container log."),
    "failed":       ("bad",  "Sign-in didn't complete. Try again."),
    "config":       ("bad",  "Sign-in isn't set up correctly. See the container log."),
    "signed_out":   ("info", "You've been signed out."),
}

_oauth = None    # Authlib registry, set by init_auth() once the config checks out
_problems = []   # configuration problems; any of them means nobody can sign in


def validate_auth_config():
    """Problems that stop Google sign-in from working; empty means all good."""
    problems = []
    if not GOOGLE_CLIENT_ID:
        problems.append("GOOGLE_CLIENT_ID is empty -- set it to the OAuth client ID from Google Cloud Console.")
    if not GOOGLE_CLIENT_SECRET:
        problems.append("GOOGLE_CLIENT_SECRET is empty -- set it to the OAuth client secret from Google Cloud Console.")
    if not ALLOWED_EMAILS:
        problems.append("AUTH_ALLOWED_EMAILS is empty -- list the Google accounts allowed to sign in, comma-separated.")
    if AUTH_SESSION_HOURS < 1:
        problems.append(f"AUTH_SESSION_HOURS={AUTH_SESSION_HOURS} must be at least 1.")
    if not PUBLIC_URL:
        problems.append("PUBLIC_URL is empty -- set it to the address the reverse proxy serves the app on, "
                        "e.g. https://tape.example.com.")
        return problems
    try:
        parts = urlsplit(PUBLIC_URL)
        host = parts.hostname
    except ValueError:
        parts, host = None, None
    if not host or parts.scheme not in ("http", "https") or (
            parts.scheme == "http" and host not in ("localhost", "127.0.0.1")):
        problems.append(f"PUBLIC_URL={PUBLIC_URL!r} must be an https:// address "
                        f"(Google only redirects to plain http:// on localhost).")
    elif parts.path or parts.query or parts.fragment:
        problems.append(f"PUBLIC_URL={PUBLIC_URL!r} must be only the scheme and host, with no path -- "
                        f"the app has to be served at the root of its address.")
    return problems


def _load_secret_key():
    """The key that signs session cookies, kept in SECRET_KEY_FILE so sign-ins
    survive a container restart. Deleting the file and restarting signs
    everyone out."""
    try:
        with open(SECRET_KEY_FILE) as f:
            key = f.read().strip()
        if len(key) >= 32:
            return key
    except FileNotFoundError:
        pass
    except OSError as e:
        _log.warning("Could not read the session key from %s: %s", SECRET_KEY_FILE, e)
    key = secrets.token_hex(32)
    try:
        os.makedirs(os.path.dirname(SECRET_KEY_FILE) or ".", exist_ok=True)
        fd = os.open(SECRET_KEY_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(key)
    except OSError as e:
        _log.warning("Could not save the session key to %s (%s); sign-ins will not survive a restart.",
                     SECRET_KEY_FILE, e)
    return key


def init_auth():
    """Set up sessions and the Google client. Called once from main.run()
    before serving; returns the configuration problems for the caller to log."""
    global _oauth, _problems
    if not SSO_ENABLED:
        return []
    problems = validate_auth_config()
    try:
        from authlib.integrations.flask_client import OAuth
    except ImportError:
        OAuth = None
        problems.append("Authlib is not installed -- rebuild the image so requirements.txt is installed.")
    app.secret_key = _load_secret_key()
    app.config.update(
        SESSION_COOKIE_NAME="cc_session",
        SESSION_COOKIE_HTTPONLY=True,
        # Lax rather than Strict: the cookie has to come back on Google's
        # redirect to /auth/callback, which carries the sign-in state.
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=PUBLIC_URL.startswith("https://"),
        # The UI polls every few seconds; don't re-send the cookie each time.
        SESSION_REFRESH_EACH_REQUEST=False,
        PERMANENT_SESSION_LIFETIME=datetime.timedelta(hours=max(AUTH_SESSION_HOURS, 1)),
    )
    if OAuth is not None and not problems:
        _oauth = OAuth(app)
        _oauth.register(
            name="google",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            server_metadata_url=_GOOGLE_METADATA_URL,
            client_kwargs={"scope": "openid email profile", "code_challenge_method": "S256"},
        )
    _problems = problems
    return problems


def _ready():
    return not _problems and _oauth is not None


def current_user():
    """The signed-in account as {"email", "name"}, or None."""
    email = session.get("user_email")
    if not email:
        return None
    # Re-checking the allowlist means removing an account and restarting the
    # container signs it out straight away.
    if time.time() >= float(session.get("auth_expires") or 0) or email not in ALLOWED_EMAILS:
        session.clear()
        return None
    return {"email": email, "name": session.get("user_name") or ""}


def template_context():
    """Values index.html needs for the signed-in account and the CSRF token."""
    if not SSO_ENABLED:
        return {"user_email": "", "csrf_token": ""}
    user = current_user()
    return {"user_email": user["email"] if user else "", "csrf_token": session.get("csrf_token") or ""}


def _audit(level, message):
    """Record a sign-in event on the Activity page, without letting a failed
    log write break the sign-in itself."""
    try:
        db_log("auth", level, message)
    except Exception:
        _log.log(level_from_name(level), message)


def _login_url(**params):
    query = urlencode({k: v for k, v in params.items() if v})
    return f"{PUBLIC_URL}/login" + (f"?{query}" if query else "")


def _safe_next(target):
    """``target`` if it is a path on this app, else "/" -- where to go after
    signing in must never be another site."""
    target = str(target or "")
    if (not target.startswith("/") or target.startswith("//") or "\\" in target
            # browsers drop tabs and newlines, so "/\t/evil" would become "//evil"
            or any(ord(c) < 0x21 or ord(c) == 0x7F for c in target)
            or target.startswith(("/login", "/logout", "/auth/"))):
        return "/"
    return target


@app.before_request
def _require_sign_in():
    if not SSO_ENABLED or request.endpoint in _PUBLIC_ENDPOINTS:
        return None
    if current_user() is None:
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Sign-in required.", "login_required": True}), 401
        return redirect(_login_url(next=_safe_next(request.full_path.rstrip("?"))))
    if request.method not in ("GET", "HEAD", "OPTIONS"):
        sent = request.headers.get("X-CSRF-Token") or request.form.get("csrf_token") or ""
        expected = session.get("csrf_token") or ""
        if not expected or not hmac.compare_digest(sent.encode(), expected.encode()):
            return jsonify({"ok": False, "error": "Missing or invalid CSRF token -- reload the page."}), 403
    return None


@app.get("/login")
def login():
    if not SSO_ENABLED:
        return redirect("/")
    nxt = _safe_next(request.args.get("next"))
    if current_user() is not None:
        return redirect(PUBLIC_URL + nxt)
    code = "config" if not _ready() else (request.args.get("error") or
                                          ("signed_out" if request.args.get("signed_out") else ""))
    tone, message = _LOGIN_MESSAGES.get(code, (None, None))
    return render_template("login.html", tone=tone, message=message, can_sign_in=_ready(),
                           require_mfa=AUTH_REQUIRE_MFA, session_hours=AUTH_SESSION_HOURS,
                           start_url=f"{PUBLIC_URL}/auth/google?" + urlencode({"next": nxt}))


@app.get("/auth/google")
def auth_google():
    if not SSO_ENABLED:
        return redirect("/")
    if not _ready():
        return redirect(_login_url(error="config"))
    nxt = _safe_next(request.args.get("next"))
    if current_user() is not None:
        return redirect(PUBLIC_URL + nxt)
    # Start from an empty session: Authlib keeps each attempt's state in the
    # cookie, and abandoned attempts would otherwise pile up past its size limit.
    session.clear()
    session["login_next"] = nxt
    extra = {}
    if AUTH_REQUIRE_MFA:
        # Ask for `amr` (how the account signed in). Google fills it in only
        # when "Authentication strength claims" is on for the OAuth client.
        extra["claims"] = json.dumps({"id_token": {"amr": {"essential": True}}})
    try:
        return _oauth.google.authorize_redirect(f"{PUBLIC_URL}/auth/callback", prompt="select_account", **extra)
    except Exception:
        _log.exception("Could not start Google sign-in (is accounts.google.com reachable from the container?).")
        return redirect(_login_url(error="failed"))


@app.get("/auth/callback")
def auth_callback():
    if not SSO_ENABLED:
        return redirect("/")
    if not _ready():
        return redirect(_login_url(error="config"))
    nxt = _safe_next(session.get("login_next"))
    try:
        token = _oauth.google.authorize_access_token()
    except Exception as e:
        # Covers a cancelled consent screen, a stale or replayed state, and an
        # ID token that fails verification.
        _log.warning("Google sign-in failed: %s", e)
        session.clear()
        return redirect(_login_url(error="failed"))

    info = token.get("userinfo") or {}
    email = str(info.get("email") or "").strip().lower()
    amr = info.get("amr") or []
    if isinstance(amr, str):
        amr = [amr]
    if not info:
        denied, why = "failed", "Google returned no ID token."
    elif info.get("email_verified") not in (True, "true"):
        denied, why = "unverified", f"{email or '(no email)'} is not a verified Google address."
    elif email not in ALLOWED_EMAILS:
        denied, why = "not_allowed", f"{email} is not in AUTH_ALLOWED_EMAILS."
    elif AUTH_REQUIRE_MFA and not amr:
        denied, why = "mfa_unknown", (
            f"{email}: Google's ID token has no `amr` claim, so a multi-factor sign-in can't be confirmed. "
            f"Turn on \"Authentication strength claims\" for the OAuth client in Google Cloud Console "
            f"(Google offers it once the app is published and verified), or set AUTH_REQUIRE_MFA=false "
            f"to rely on the account's own 2-Step Verification.")
    elif AUTH_REQUIRE_MFA and not _MFA_AMR.intersection(amr):
        denied, why = "mfa_required", f"{email} signed in without a second factor (amr={amr})."
    else:
        denied, why = None, ""

    session.clear()  # drop the sign-in state and any session from before
    if denied:
        _audit("warning", f"Refused sign-in: {why}")
        return redirect(_login_url(error=denied))
    session.permanent = True
    session["user_email"] = email
    session["user_name"] = str(info.get("name") or "")
    session["auth_expires"] = time.time() + AUTH_SESSION_HOURS * 3600
    session["csrf_token"] = secrets.token_urlsafe(32)
    _audit("info", f"Signed in: {email}" + (f" (amr={','.join(amr)})" if amr else ""))
    return redirect(PUBLIC_URL + nxt)


@app.post("/logout")
def logout():
    if not SSO_ENABLED:
        return redirect("/")
    email = session.get("user_email")
    session.clear()
    if email:
        _audit("info", f"Signed out: {email}")
    return redirect(_login_url(signed_out=1))
