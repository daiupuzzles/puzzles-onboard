"""
Puzzles Client Onboarding — Flask Form

Run locally: doppler run --project puzzles --config prd -- python app.py
Production:  gunicorn app:app (via Dockerfile)

Pages:
  /             — Onboarding form
  /preview      — Dry-run preview + confirm
  /run          — Kick off onboarding (background thread)
  /status/<slug> — Poll run status (JSON, for frontend polling)
  /receipt/<slug> — Final receipt with all URLs
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import threading
import traceback
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path

import jwt
import requests
from flask import Flask, render_template_string, request, redirect, url_for, jsonify, make_response, g
from supabase import create_client as supabase_create_client

# Import the onboarding engine
import sys
sys.path.insert(0, str(Path(__file__).parent))
from onboard_client import (
    OnboardingState, make_slug, preflight, doppler_get,
    step_clockify, step_asana, step_jira, step_drive, step_sheets, step_zoom,
    init_clockify, init_jira, init_google, init_asana,
    _check_clockify, _check_asana, _check_jira, _check_drive, _check_sheets,
    SERVICE_GROUPS,
)

app = Flask(__name__)
_flask_secret = os.environ.get("FLASK_SECRET_KEY", "")
if not _flask_secret:
    logging.getLogger("app").warning("FLASK_SECRET_KEY not set — using random key. Sessions won't survive restarts.")
    _flask_secret = os.urandom(32).hex()
app.secret_key = _flask_secret

# ---------------------------------------------------------------------------
# Auth config (Asana OAuth via Supabase — mirrors web-qa-auditor pattern)
# ---------------------------------------------------------------------------

def _env_or_doppler(key: str) -> str:
    """Get from env var first, fall back to Doppler CLI."""
    val = os.environ.get(key)
    if val:
        return val
    return doppler_get(key)


_SUPABASE_URL = _env_or_doppler("SUPABASE_INHOUSE_URL")
_SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_INHOUSE_ANON_KEY") or doppler_get("SUPABASE_INHOUSE_ANON_KEY")
_SUPABASE_SERVICE_KEY = _env_or_doppler("SUPABASE_INHOUSE_SERVICE_KEY")
_SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET") or os.environ.get("SUPABASE_INHOUSE_JWT_SECRET", "")
_ASANA_CLIENT_ID = os.environ.get("ASANA_CLIENT_ID", "") or doppler_get("ASANA_CLIENT_ID")
_ASANA_CLIENT_SECRET = os.environ.get("ASANA_CLIENT_SECRET", "") or doppler_get("ASANA_CLIENT_SECRET")

_supabase_admin = supabase_create_client(_SUPABASE_URL, _SUPABASE_SERVICE_KEY)

if not _SUPABASE_JWT_SECRET:
    logging.getLogger("app").warning(
        "SUPABASE_JWT_SECRET not set — HS256 JWT validation disabled. "
        "Auth will only work with ES256 (JWKS). Set SUPABASE_JWT_SECRET in Doppler."
    )

# Public paths that don't require auth
_PUBLIC_PATHS = {"/login", "/logout", "/favicon.ico", "/api/auth/session", "/api/auth/clear-session", "/api/asana/store-tokens", "/healthz"}


_jwks_client = None

def _get_jwks_client():
    global _jwks_client
    if _jwks_client is None:
        from jwt import PyJWKClient
        _jwks_client = PyJWKClient(
            f"{_SUPABASE_URL}/auth/v1/.well-known/jwks.json",
            headers={"apikey": _SUPABASE_ANON_KEY},
        )
    return _jwks_client


def _validate_jwt_token(token: str) -> dict | None:
    """Validate a Supabase JWT (HS256 or ES256). Returns {id, email} or None."""
    try:
        header = jwt.get_unverified_header(token)
        alg = header.get("alg", "HS256")
        if alg == "HS256":
            if not _SUPABASE_JWT_SECRET:
                return None
            payload = jwt.decode(token, _SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")
        elif alg == "ES256":
            signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
            payload = jwt.decode(token, signing_key.key, algorithms=["ES256"], audience="authenticated")
        else:
            return None
        return {"id": payload["sub"], "email": payload.get("email")}
    except jwt.PyJWTError:
        return None


def _get_user_from_request() -> dict | None:
    """Get authenticated user from session cookie or Authorization header."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return _validate_jwt_token(auth[7:])
    token = request.cookies.get("sb-session")
    if token:
        return _validate_jwt_token(token)
    return None


@app.before_request
def _auth_gate():
    """Require authentication for all routes except public paths."""
    if request.path in _PUBLIC_PATHS:
        return None
    user = _get_user_from_request()
    if not user:
        if "text/html" in request.headers.get("Accept", ""):
            return redirect("/login")
        return jsonify({"error": "Authentication required"}), 401
    g.user = user
    return None


_refresh_locks: dict[str, threading.Lock] = {}
_refresh_locks_guard = threading.Lock()


def _get_asana_token_for_user(supabase_user_id: str) -> str | None:
    """Get valid Asana OAuth token for user. Auto-refresh if expired.
    Per-user lock prevents double-refresh race (Asana refresh tokens are single-use)."""
    with _refresh_locks_guard:
        if supabase_user_id not in _refresh_locks:
            _refresh_locks[supabase_user_id] = threading.Lock()
        lock = _refresh_locks[supabase_user_id]
    with lock:
        return _get_asana_token_for_user_inner(supabase_user_id)


def _get_asana_token_for_user_inner(supabase_user_id: str) -> str | None:
    try:
        result = _supabase_admin.table("asana_provider_tokens").select("*").eq(
            "supabase_user_id", supabase_user_id
        ).execute()
        if not result.data:
            return None
        row = result.data[0]
        # Check expiry (60s buffer)
        expires_at = datetime.fromisoformat(row["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at > datetime.now(timezone.utc) + timedelta(seconds=60):
            return row["access_token"]
        # Refresh the token
        r = requests.post("https://app.asana.com/-/oauth_token", data={
            "grant_type": "refresh_token",
            "client_id": _ASANA_CLIENT_ID,
            "client_secret": _ASANA_CLIENT_SECRET,
            "refresh_token": row["refresh_token"],
        }, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        new_expires = datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600))
        _supabase_admin.table("asana_provider_tokens").update({
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token", row["refresh_token"]),
            "expires_at": new_expires.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("supabase_user_id", supabase_user_id).execute()
        return data["access_token"]
    except Exception as e:
        log.warning("Failed to get Asana token for user %s: %s", supabase_user_id, e)
        return None

log = logging.getLogger("app")

# Track running jobs (thread-safe)
_running: dict[str, str] = {}  # slug -> "running" | "complete" | "failed"
_running_lock = threading.Lock()

# Track preflight checks (thread-safe)
_preflight: dict[str, dict] = {}  # slug -> {check_name: "pending"|"ok"|"fail"|"skip", ...}
_preflight_lock = threading.Lock()


def _validate_slug(slug: str) -> str:
    """Sanitize slug to prevent path traversal."""
    slug = slug.strip()
    if not re.match(r'^[a-z0-9][a-z0-9\-]*$', slug):
        raise ValueError(f"Invalid slug: {slug}")
    return slug


# ---------------------------------------------------------------------------
# Auth routes (Asana OAuth via Supabase)
# ---------------------------------------------------------------------------

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Login — Puzzles Onboarding</title>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0a0a0a; color: #e0e0e0; min-height: 100vh;
         display: flex; align-items: center; justify-content: center; }
  .card { text-align: center; max-width: 360px; width: 100%; }
  .card h1 { font-size: 1.8rem; margin-bottom: 0.5rem; color: #fff; }
  .card p { color: #888; font-size: 0.9rem; margin-bottom: 2rem; }
  .btn { display: inline-block; padding: 12px 32px; background: #4a9eff; color: #fff;
         border: none; border-radius: 8px; font-size: 0.95rem; font-weight: 600;
         cursor: pointer; transition: background 0.2s; }
  .btn:hover { background: #3a8eef; }
  .btn.hidden { display: none; }
  #status { margin-top: 1rem; font-size: 0.85rem; color: #888; }
  #status.error { color: #ff6b6b; }
  #status.success { color: #51cf66; }
</style>
</head>
<body>
<div class="card">
  <h1>Puzzles Onboarding</h1>
  <p>Sign in with your Asana account to attribute tasks to yourself.</p>
  <button id="login-btn" class="btn" onclick="loginWithAsana()">Login with Asana</button>
  <div id="status"></div>
</div>
<script>
const sb = supabase.createClient({{ supabase_url|tojson }}, {{ supabase_anon_key|tojson }});

async function setSessionCookie(accessToken) {
  try {
    await fetch('/api/auth/session', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ access_token: accessToken }),
    });
  } catch (e) { /* best-effort */ }
}

let _tokenStoreAttempted = false;

sb.auth.onAuthStateChange(async (event, session) => {
  if (session?.provider_token && !_tokenStoreAttempted) {
    _tokenStoreAttempted = true;
    showStatus('Connecting your Asana account...', 'info');
    document.getElementById('login-btn').classList.add('hidden');
    await setSessionCookie(session.access_token);
    const ok = await storeTokensWithRetry(session);
    if (ok) {
      showStatus('Connected! Redirecting...', 'success');
      setTimeout(() => { window.location.href = '/'; }, 500);
    } else {
      await sb.auth.signOut();
      await fetch('/api/auth/clear-session', { method: 'POST' });
      showStatus('Failed to connect Asana. Please try again.', 'error');
      document.getElementById('login-btn').classList.remove('hidden');
    }
    return;
  }
  if (session && !session.provider_token) {
    await setSessionCookie(session.access_token);
    window.location.href = '/';
  }
  if (event === 'TOKEN_REFRESHED' && session) {
    await setSessionCookie(session.access_token);
  }
});

async function storeTokensWithRetry(session) {
  const delays = [1000, 2000, 4000];
  for (let attempt = 0; attempt <= delays.length; attempt++) {
    try {
      const r = await fetch('/api/asana/store-tokens', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + session.access_token,
        },
        body: JSON.stringify({
          provider_token: session.provider_token,
          provider_refresh_token: session.provider_refresh_token,
        }),
      });
      if (r.ok) return true;
    } catch (e) { /* retry */ }
    if (attempt < delays.length) await new Promise(r => setTimeout(r, delays[attempt]));
  }
  return false;
}

function loginWithAsana() {
  document.getElementById('login-btn').classList.add('hidden');
  showStatus('Redirecting to Asana...', 'info');
  sb.auth.signInWithOAuth({
    provider: 'custom:asana',
    options: { redirectTo: window.location.origin + '/login' },
  });
}

function showStatus(msg, type) {
  const el = document.getElementById('status');
  el.textContent = msg;
  el.className = type === 'success' ? 'success' : type === 'error' ? 'error' : '';
}
</script>
</body>
</html>
"""


@app.route("/login")
def login_page():
    return render_template_string(LOGIN_PAGE,
        supabase_url=_SUPABASE_URL,
        supabase_anon_key=_SUPABASE_ANON_KEY,
    )


@app.route("/api/auth/session", methods=["POST"])
def set_session_cookie():
    """Set HttpOnly session cookie from Supabase JWT."""
    body = request.get_json(silent=True) or {}
    token = body.get("access_token", "")
    if not token or not _validate_jwt_token(token):
        return jsonify({"error": "invalid token"}), 401
    resp = make_response(jsonify({"ok": True}))
    resp.set_cookie(
        "sb-session", token,
        httponly=True, samesite="Lax",
        secure=os.environ.get("ENV") != "development",
        max_age=3600, path="/",
    )
    return resp


@app.route("/api/auth/clear-session", methods=["POST"])
def clear_session_cookie():
    """Clear session cookie on logout."""
    resp = make_response(jsonify({"ok": True}))
    resp.delete_cookie(
        "sb-session", path="/", samesite="Lax",
        secure=os.environ.get("ENV") != "development",
    )
    return resp


@app.route("/api/asana/store-tokens", methods=["POST"])
def store_asana_tokens():
    """Store Asana provider tokens after OAuth login."""
    user = _get_user_from_request()
    if not user:
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    provider_token = body.get("provider_token")
    provider_refresh_token = body.get("provider_refresh_token")
    if not provider_token or not provider_refresh_token:
        return jsonify({"error": "provider_token and provider_refresh_token required"}), 400
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    # Best-effort: fetch Asana user info
    asana_user_gid = None
    asana_user_name = None
    try:
        r = requests.get(
            "https://app.asana.com/api/1.0/users/me?opt_fields=gid,name",
            headers={"Authorization": f"Bearer {provider_token}"},
            timeout=5,
        )
        if r.status_code == 200:
            me = r.json().get("data", {})
            asana_user_gid = me.get("gid")
            asana_user_name = me.get("name")
    except Exception:
        pass
    row = {
        "supabase_user_id": user["id"],
        "access_token": provider_token,
        "refresh_token": provider_refresh_token,
        "expires_at": expires_at,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if asana_user_gid:
        row["asana_user_gid"] = asana_user_gid
    if asana_user_name:
        row["asana_user_name"] = asana_user_name
    _supabase_admin.table("asana_provider_tokens").upsert(
        row, on_conflict="supabase_user_id"
    ).execute()
    return jsonify({"ok": True, "asana_user_name": asana_user_name})


@app.route("/logout")
def logout():
    resp = make_response(redirect("/login"))
    resp.delete_cookie(
        "sb-session", path="/", samesite="Lax",
        secure=os.environ.get("ENV") != "development",
    )
    return resp


@app.route("/healthz")
def healthz():
    return "ok", 200


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

LAYOUT = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title }} — Puzzles Onboarding</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
         background: #0a0a0a; color: #e0e0e0; padding: 2rem; max-width: 800px; margin: 0 auto; }
  h1 { color: #fff; margin-bottom: 0.5rem; font-size: 1.5rem; }
  h2 { color: #ccc; font-size: 1.1rem; margin: 1.5rem 0 0.5rem; }
  .subtitle { color: #888; margin-bottom: 2rem; }
  form { display: flex; flex-direction: column; gap: 1rem; }
  label { font-weight: 500; font-size: 0.9rem; color: #bbb; }
  input[type=text], select { background: #1a1a1a; border: 1px solid #333; color: #fff;
         padding: 0.6rem 0.8rem; border-radius: 6px; font-size: 0.95rem; width: 100%; }
  input:focus, select:focus { outline: none; border-color: #4a9eff; }
  .checkbox-group { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.25rem; }
  .checkbox-group label { background: #1a1a1a; border: 1px solid #333; padding: 0.4rem 0.8rem;
         border-radius: 20px; cursor: pointer; font-weight: 400; font-size: 0.85rem; transition: all 0.15s; }
  .checkbox-group input:checked + span { color: #4a9eff; }
  .checkbox-group label:has(input:checked) { border-color: #4a9eff; background: #0d2040; }
  .checkbox-group input { display: none; }
  .row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  button, .btn { background: #4a9eff; color: #fff; border: none; padding: 0.7rem 1.5rem;
         border-radius: 6px; font-size: 0.95rem; cursor: pointer; font-weight: 600;
         text-decoration: none; display: inline-block; text-align: center; transition: background 0.15s; }
  button:hover, .btn:hover { background: #3580d4; }
  .btn-secondary { background: #333; }
  .btn-secondary:hover { background: #444; }
  .btn-danger { background: #d44; }
  .step { padding: 0.6rem 0.8rem; border-radius: 6px; margin: 0.3rem 0;
          display: flex; justify-content: space-between; align-items: center; }
  .step-ok { background: #0d2a0d; border: 1px solid #1a4a1a; }
  .step-fail { background: #2a0d0d; border: 1px solid #4a1a1a; }
  .step-skip { background: #1a1a1a; border: 1px solid #333; color: #888; }
  .step-pending { background: #1a1a1a; border: 1px solid #333; }
  .step-running { background: #1a2a0d; border: 1px solid #3a5a1a; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.7; } }
  .url { color: #4a9eff; text-decoration: none; font-size: 0.85rem; }
  .url:hover { text-decoration: underline; }
  .receipt-box { background: #111; border: 1px solid #333; border-radius: 8px; padding: 1rem; margin: 1rem 0; }
  pre { background: #111; padding: 1rem; border-radius: 6px; overflow-x: auto; font-size: 0.85rem; white-space: pre-wrap; }
  .alert { padding: 0.8rem 1rem; border-radius: 6px; margin: 1rem 0; }
  .alert-warn { background: #2a2000; border: 1px solid #5a4a00; color: #ffcc00; }
  .alert-err { background: #2a0d0d; border: 1px solid #5a1a1a; color: #ff6666; }
  .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #333;
             border-top-color: #4a9eff; border-radius: 50%; animation: spin 0.8s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  .copy-btn { background: #222; border: 1px solid #444; color: #aaa; padding: 0.3rem 0.6rem;
              border-radius: 4px; cursor: pointer; font-size: 0.8rem; }
  .copy-btn:hover { background: #333; color: #fff; }
</style>
</head>
<body>
{% block content %}{% endblock %}
</body>
</html>
"""

FORM_PAGE = LAYOUT.replace("{% block content %}{% endblock %}", """
<h1>New Client Onboarding</h1>
<p class="subtitle">Creates scaffolding across Clockify, Asana, Jira, Drive, Sheets, and Zoom.</p>

<form method="POST" action="/preview">
  <div class="row">
    <div>
      <label>Client Name *</label>
      <input type="text" name="client_name" required placeholder="Acme Corp">
    </div>
    <div>
      <label>Jira Key * (2-10 uppercase)</label>
      <input type="text" name="jira_key" id="jira_key" required placeholder="AC" pattern="[A-Z][A-Z0-9]{1,9}"
             style="text-transform: uppercase;">
      <div id="key-status" style="font-size:0.8rem; margin-top:0.25rem; min-height:1.2em;"></div>
    </div>
  </div>

  <div>
    <label>Service Groups (empty = empty Asana + Jira projects)</label>
    <div class="checkbox-group">
      {% for key, label in groups %}
      <label><input type="checkbox" name="service_groups" value="{{ key }}"><span>{{ label }}</span></label>
      {% endfor %}
    </div>
  </div>

  <div class="row">
    <div>
      <label>Project Type</label>
      <select name="project_type">
        <option value="Ongoing">Ongoing</option>
        <option value="Specific">Specific</option>
      </select>
    </div>
    <div>
      <label>Priority</label>
      <select name="priority">
        <option value="">—</option>
        <option value="Low">Low</option>
        <option value="Medium">Medium</option>
        <option value="High">High</option>
      </select>
    </div>
  </div>

  <div class="row">
    <div>
      <label>Level of Effort (1-10)</label>
      <input type="text" name="loe" placeholder="6">
    </div>
    <div>
      <label>Website URL</label>
      <input type="text" name="website_url" placeholder="https://acmecorp.com">
    </div>
  </div>

  <button type="submit">Preview &rarr;</button>
</form>

<script>
let suggestTimer = null;
const nameInput = document.querySelector('input[name="client_name"]');
const keyInput = document.getElementById('jira_key');
const keyStatus = document.getElementById('key-status');

// Auto-suggest key when client name changes
nameInput.addEventListener('input', () => {
  clearTimeout(suggestTimer);
  suggestTimer = setTimeout(() => {
    const name = nameInput.value.trim();
    if (name.length < 2) return;
    // Only suggest if key field is empty or was auto-filled
    if (keyInput.dataset.manual === 'true') return;
    keyStatus.innerHTML = '<span style="color:#888">Suggesting...</span>';
    fetch('/api/suggest-key?name=' + encodeURIComponent(name))
      .then(r => r.json())
      .then(data => {
        if (data.key) {
          keyInput.value = data.key;
          _setKeyStatus(data.key, data.available);
        }
      });
  }, 500);
});

function _setKeyStatus(key, available) {
  // Safe: uses textContent to prevent XSS from server-reflected values
  keyStatus.textContent = '';
  const span = document.createElement('span');
  if (available === true) {
    span.style.color = '#6f6';
    span.textContent = '\u2713 ' + key + ' is available';
  } else if (available === false) {
    span.style.color = '#f66';
    span.textContent = '\u2717 ' + key + ' is taken';
  } else {
    span.style.color = '#ff0';
    span.textContent = '? Could not verify';
  }
  keyStatus.appendChild(span);
}

// Mark as manual if user types in the key field
keyInput.addEventListener('input', () => {
  keyInput.dataset.manual = 'true';
  // Validate on input
  clearTimeout(suggestTimer);
  const key = keyInput.value.trim().toUpperCase();
  if (key.length < 2) { keyStatus.textContent = ''; return; }
  suggestTimer = setTimeout(() => {
    keyStatus.textContent = 'Checking...';
    keyStatus.style.color = '#888';
    fetch('/api/check-key?key=' + encodeURIComponent(key))
      .then(r => r.json())
      .then(data => {
        _setKeyStatus(data.key, data.available);
      });
  }, 400);
});

// Reset manual flag if name changes and key is cleared
nameInput.addEventListener('input', () => {
  if (!keyInput.value) keyInput.dataset.manual = '';
});
</script>
""")

PREVIEW_PAGE = LAYOUT.replace("{% block content %}{% endblock %}", """
<h1>Preview: {{ client_name }}</h1>
<p class="subtitle">Verifying platform access before onboarding.</p>

<div class="receipt-box">
  <p><strong>Client:</strong> {{ client_name }}</p>
  <p><strong>Jira Key:</strong> {{ jira_key }}</p>
  <p><strong>Slug:</strong> {{ slug }}</p>
  <p><strong>Service Groups:</strong> {{ service_groups_display }}</p>
  <p><strong>Project Type:</strong> {{ project_type }}</p>
</div>

<h2>Pre-flight Checks</h2>
<div class="step" id="check-validation"><span class="check-icon" id="icon-validation"></span> Input validation</div>
<div class="step" id="check-clockify"><span class="check-icon" id="icon-clockify"></span> Clockify</div>
<div class="step" id="check-asana"><span class="check-icon" id="icon-asana"></span> Asana</div>
<div class="step" id="check-jira"><span class="check-icon" id="icon-jira"></span> Jira</div>
<div class="step" id="check-google_drive"><span class="check-icon" id="icon-google_drive"></span> Google Drive</div>
<div class="step" id="check-google_sheets"><span class="check-icon" id="icon-google_sheets"></span> Google Sheets</div>
<div class="step" id="check-zoom"><span class="check-icon" id="icon-zoom"></span> Zoom</div>

<h2>Onboarding Steps</h2>
<div class="step step-pending">1. Clockify — Create client + project</div>
{% if has_service_groups %}
<div class="step step-pending">2. Asana — Create project + populate selected sections</div>
<div class="step step-pending">3. Jira PD — Create project + populate matching issues</div>
{% else %}
<div class="step step-pending">2. Asana — Create empty project</div>
<div class="step step-pending">3. Jira PD — Create empty project</div>
{% endif %}
<div class="step step-pending">4. Google Drive — Copy 25-folder template tree</div>
<div class="step step-pending">5a. Google Sheets — Copy client mastersheet</div>
<div class="step step-pending">5b. Google Sheets — Append to BRAND DISTRIBUTION</div>
<div class="step step-pending">6. Zoom — Team chat channel (if creds available)</div>

<div id="preflight-errors" style="display:none;" class="alert alert-err"></div>
<div id="preflight-warnings" style="display:none;" class="alert alert-warn"></div>

<div style="margin-top: 1.5rem; display: flex; gap: 1rem;">
  <form method="POST" action="/run" id="run-form">
    <input type="hidden" name="client_name" value="{{ client_name }}">
    <input type="hidden" name="jira_key" value="{{ jira_key }}">
    <input type="hidden" name="service_groups" value="{{ service_groups_raw }}">
    <input type="hidden" name="project_type" value="{{ project_type }}">
    <input type="hidden" name="priority" value="{{ priority }}">
    <input type="hidden" name="loe" value="{{ loe }}">
    <input type="hidden" name="website_url" value="{{ website_url }}">
    <button type="submit" id="btn-run" disabled style="opacity:0.5">Run Onboarding &rarr;</button>
  </form>
  <a href="/" class="btn btn-secondary">&larr; Back</a>
</div>

<script>
const slug = {{ slug|tojson }};
const checks = ["validation", "clockify", "asana", "jira", "google_drive", "google_sheets", "zoom"];
let pollCount = 0;
const maxPolls = 60;

function updateChecks() {
  pollCount++;
  if (pollCount > maxPolls) {
    document.getElementById("preflight-errors").innerHTML = "<strong>Preflight timed out.</strong> Refresh to retry.";
    document.getElementById("preflight-errors").style.display = "block";
    return;
  }

  fetch("/preflight-status/" + slug)
    .then(r => r.json())
    .then(data => {
      checks.forEach(c => {
        const icon = document.getElementById("icon-" + c);
        const row = document.getElementById("check-" + c);
        if (!icon || !row) return;
        const st = data[c];
        if (st === "ok") { icon.textContent = "\\u2705"; row.className = "step step-ok"; }
        else if (st === "fail") { icon.textContent = "\\u274C"; row.className = "step step-fail"; }
        else if (st === "skip") { icon.textContent = "\\u23ED\\uFE0F"; row.className = "step step-skip"; }
        else { icon.textContent = "\\u23F3"; }
      });

      if (data.errors && data.errors.length > 0) {
        const el = document.getElementById("preflight-errors");
        el.textContent = "";
        const h = document.createElement("strong");
        h.textContent = "Pre-flight errors:";
        el.appendChild(h);
        data.errors.forEach(e => { const d = document.createElement("div"); d.textContent = e; el.appendChild(d); });
        el.style.display = "block";
      }
      if (data.warnings && data.warnings.length > 0) {
        const el = document.getElementById("preflight-warnings");
        el.textContent = "";
        const h = document.createElement("strong");
        h.textContent = "Warnings:";
        el.appendChild(h);
        data.warnings.forEach(w => { const d = document.createElement("div"); d.textContent = w; el.appendChild(d); });
        el.style.display = "block";
      }

      if (data.overall === "ok") {
        const btn = document.getElementById("btn-run");
        btn.disabled = false;
        btn.style.opacity = "1";
        return;
      } else if (data.overall === "fail") {
        return;
      }
      setTimeout(updateChecks, 1000);
    })
    .catch(() => setTimeout(updateChecks, 2000));
}

// Prevent double-submit
document.getElementById("run-form").addEventListener("submit", function(e) {
  document.getElementById("btn-run").disabled = true;
  document.getElementById("btn-run").textContent = "Starting...";
});

updateChecks();
</script>
""")

RUN_PAGE = LAYOUT.replace("{% block content %}{% endblock %}", """
<h1>Onboarding: {{ client_name }}</h1>
<p class="subtitle">Running... <span class="spinner"></span></p>

<div id="steps">
  {% for step_name, label in step_labels %}
  <div class="step step-pending" id="step-{{ step_name }}">
    <span>{{ label }}</span>
    <span id="status-{{ step_name }}">pending</span>
  </div>
  {% endfor %}
</div>

<div id="done-box" style="display:none; margin-top:1.5rem;">
  <a href="/receipt/{{ slug }}" class="btn">View Receipt &rarr;</a>
</div>

<script>
const slug = {{ slug|tojson }};
const stepNames = {{ step_names_json|safe }};

function poll() {
  fetch("/status/" + slug)
    .then(r => r.json())
    .then(data => {
      const steps = data.steps || {};
      let allDone = true;
      stepNames.forEach(name => {
        const el = document.getElementById("step-" + name);
        const statusEl = document.getElementById("status-" + name);
        const s = (steps[name] || {}).status || "pending";
        statusEl.textContent = s;
        el.className = "step step-" + (s === "complete" ? "ok" : s === "skipped" ? "skip" : s === "failed" ? "fail" : s === "in_progress" ? "running" : "pending");
        if (s !== "complete" && s !== "skipped" && s !== "failed") allDone = false;
      });
      if (data.status === "complete" || data.status === "failed" || allDone) {
        document.querySelector(".subtitle").innerHTML = data.status === "failed"
          ? '<span style="color:#f66">Failed</span>' : '<span style="color:#6f6">Complete!</span>';
        document.getElementById("done-box").style.display = "block";
      } else {
        setTimeout(poll, 2000);
      }
    })
    .catch(() => setTimeout(poll, 3000));
}
setTimeout(poll, 1000);
</script>
""")

RECEIPT_PAGE = LAYOUT.replace("{% block content %}{% endblock %}", """
<h1>Receipt: {{ client_name }}</h1>
<p class="subtitle">Onboarding {{ status }}</p>

<div class="receipt-box">
  {% for step_name, label in step_labels %}
  {% set step = steps.get(step_name, {}) %}
  {% set s = step.get('status', 'pending') %}
  <div class="step {{ 'step-ok' if s == 'complete' else 'step-skip' if s == 'skipped' else 'step-fail' if s == 'failed' else 'step-pending' }}">
    <span>{{ label }}</span>
    <span>
      {% if step.get('project_url') %}<a href="{{ step.project_url }}" target="_blank" class="url">{{ step.project_url|truncate(50) }}</a>
      {% elif step.get('root_folder_url') %}<a href="{{ step.root_folder_url }}" target="_blank" class="url">{{ step.root_folder_url|truncate(50) }}</a>
      {% elif step.get('spreadsheet_url') %}<a href="{{ step.spreadsheet_url }}" target="_blank" class="url">{{ step.spreadsheet_url|truncate(50) }}</a>
      {% else %}{{ s }}{% endif %}
    </span>
  </div>
  {% endfor %}
</div>

<h2>Copy as Markdown</h2>
<pre id="md-receipt">{{ markdown_receipt }}</pre>
<button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('md-receipt').textContent)">Copy</button>

<div style="margin-top: 1.5rem;">
  <a href="/" class="btn btn-secondary">&larr; New Onboarding</a>
</div>
""")

# ---------------------------------------------------------------------------
# Step labels (shared across templates)
# ---------------------------------------------------------------------------

STEP_LABELS = [
    ("preflight", "0. Pre-flight validation"),
    ("clockify", "1. Clockify — Client + Project"),
    ("asana", "2. Asana — Project + Tasks"),
    ("jira", "3. Jira PD — Project + Issues"),
    ("drive", "4. Google Drive — Folder Tree"),
    ("sheets_client", "5a. Sheets — Client Mastersheet"),
    ("sheets_brand_distribution", "5b. Sheets — BRAND DISTRIBUTION"),
    ("zoom", "6. Zoom — Team Chat"),
]

SERVICE_GROUP_LABELS = [
    ("seo", "SEO"),
    ("dev", "Web Development"),
    ("design", "Design / UX/UI"),
    ("social", "Social Media"),
    ("crm", "CRM"),
    ("accounts", "Account Management"),
    ("content", "Content"),
]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def form_page():
    return render_template_string(FORM_PAGE, title="New Client", groups=SERVICE_GROUP_LABELS)


@app.route("/api/suggest-key")
def suggest_key():
    """Auto-suggest a Jira key from client name and check availability."""
    client_name = request.args.get("name", "").strip()
    if not client_name:
        return jsonify({"key": "", "available": False, "error": "No name provided"})

    # Generate candidate keys from client name
    words = client_name.upper().split()
    candidates = []
    if len(words) >= 3:
        candidates.append("".join(w[0] for w in words[:3]))  # ACM for Acme Corp Media
    if len(words) >= 2:
        candidates.append("".join(w[0] for w in words[:2]))   # AC for Acme Corp
    if len(words) == 1:
        w = words[0]
        candidates.append(w[:3])  # ACM for ACME
        candidates.append(w[:2])  # AC for ACME
    # Also try first word initial + second word first 2 chars
    if len(words) >= 2:
        candidates.append(words[0][0] + words[1][:2])

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for c in candidates:
        c = c.upper()
        if c not in seen and len(c) >= 2:
            seen.add(c)
            unique.append(c)

    # Check availability against Jira
    try:
        init_jira()
        from jira_wrapper import find_project_by_key
        for key in unique:
            existing = find_project_by_key(key)
            if not existing:
                return jsonify({"key": key, "available": True, "candidates": unique})
        # All taken — suggest with numeric suffix
        base = unique[0] if unique else client_name[:3].upper()
        for i in range(2, 10):
            key = f"{base}{i}"
            existing = find_project_by_key(key)
            if not existing:
                return jsonify({"key": key, "available": True, "candidates": unique})
        return jsonify({"key": unique[0], "available": False, "candidates": unique, "error": "All candidates taken"})
    except Exception as e:
        # If Jira check fails, return best guess without availability check
        return jsonify({"key": unique[0] if unique else "", "available": None, "error": str(e)})


@app.route("/api/check-key")
def check_key():
    """Check if a specific Jira key is available."""
    key = request.args.get("key", "").strip().upper()
    if not key:
        return jsonify({"key": "", "available": False})
    try:
        init_jira()
        from jira_wrapper import find_project_by_key
        existing = find_project_by_key(key)
        return jsonify({"key": key, "available": existing is None})
    except Exception as e:
        return jsonify({"key": key, "available": None, "error": str(e)})


@app.route("/preview", methods=["POST"])
def preview_page():
    client_name = request.form.get("client_name", "").strip()
    jira_key = request.form.get("jira_key", "").strip().upper()
    service_groups = request.form.getlist("service_groups")
    project_type = request.form.get("project_type", "Ongoing")
    priority = request.form.get("priority", "")
    loe = request.form.get("loe", "")
    website_url = request.form.get("website_url", "")

    slug = make_slug(client_name)

    # Early validation (instant, no API calls)
    errors = []
    if not client_name:
        errors.append("Client name cannot be empty")
    if jira_key and not re.match(r"^[A-Z][A-Z0-9]{1,9}$", jira_key):
        errors.append(f"Jira key '{jira_key}' must be 2-10 uppercase letters/numbers starting with a letter")

    # Initialize preflight status for live polling
    with _preflight_lock:
        _preflight[slug] = {
            "validation": "ok" if not errors else "fail",
            "clockify": "pending",
            "asana": "pending",
            "jira": "pending",
            "google_drive": "pending",
            "google_sheets": "pending",
            "zoom": "pending",
            "overall": "fail" if errors else "running",
            "errors": errors,
            "warnings": [],
        }

    # Run platform checks in background thread (only if validation passed)
    if not errors:
        def _run_preflight():
            from concurrent.futures import ThreadPoolExecutor, as_completed

            state = OnboardingState(slug, client_name, jira_key)

            # Pre-init all wrappers sequentially (they set module-level state)
            init_clockify()
            init_asana()
            init_jira()
            init_google()

            # Parallel group: Clockify, Asana, Jira (separate auth systems)
            parallel_checks = [
                ("clockify", _check_clockify),
                ("asana", _check_asana),
                ("jira", _check_jira),
            ]
            # Sequential group: Google Drive + Sheets (shared httplib2 — not thread-safe)
            google_checks = [
                ("google_drive", _check_drive),
                ("google_sheets", _check_sheets),
            ]
            all_ok = True
            warnings: list[str] = []

            def _run_check(check_name, check_fn):
                check_warnings: list[str] = []
                try:
                    check_fn(state, check_warnings)
                    with _preflight_lock:
                        _preflight[slug][check_name] = "ok"
                    return check_name, True, check_warnings
                except Exception as e:
                    with _preflight_lock:
                        _preflight[slug][check_name] = "fail"
                        _preflight[slug]["errors"].append(f"{check_name}: {e}")
                    return check_name, False, check_warnings

            # Run Clockify/Asana/Jira in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(_run_check, name, fn): name for name, fn in parallel_checks}
                for future in as_completed(futures):
                    name, ok, check_warnings = future.result()
                    if not ok:
                        all_ok = False
                    warnings.extend(check_warnings)

            # Run Google checks sequentially (httplib2 SSL not thread-safe)
            for check_name, check_fn in google_checks:
                name, ok, check_warnings = _run_check(check_name, check_fn)
                if not ok:
                    all_ok = False
                warnings.extend(check_warnings)

            # Zoom (optional, fast — no need for thread pool)
            try:
                doppler_get("ZOOM_CLIENT_ID")
                with _preflight_lock:
                    _preflight[slug]["zoom"] = "ok"
            except Exception:
                with _preflight_lock:
                    _preflight[slug]["zoom"] = "skip"
                    warnings.append("Zoom: no credentials — will be skipped")

            with _preflight_lock:
                _preflight[slug]["overall"] = "ok" if all_ok else "fail"
                _preflight[slug]["warnings"] = list(warnings)

        t = threading.Thread(target=_run_preflight, daemon=True)
        t.start()

    return render_template_string(PREVIEW_PAGE,
        title="Preview",
        client_name=client_name,
        jira_key=jira_key,
        slug=slug,
        service_groups_display=", ".join(service_groups) if service_groups else "NONE (empty projects)",
        has_service_groups=bool(service_groups),
        service_groups_raw=",".join(service_groups),
        project_type=project_type,
        priority=priority,
        loe=loe,
        website_url=website_url,
    )


@app.route("/preflight-status/<slug>")
def preflight_status(slug):
    """Poll preflight check progress."""
    try:
        slug = _validate_slug(slug)
    except ValueError:
        return jsonify({"error": "Invalid slug"}), 400
    with _preflight_lock:
        raw = _preflight.get(slug, {"overall": "unknown"})
        # Return a snapshot copy to avoid race with background thread
        status = dict(raw)
        status["errors"] = list(raw.get("errors", []))
        status["warnings"] = list(raw.get("warnings", []))
    return jsonify(status)


@app.route("/run", methods=["POST"])
def run_page():
    client_name = request.form.get("client_name", "").strip()
    jira_key = request.form.get("jira_key", "").strip().upper()
    service_groups_raw = request.form.get("service_groups", "")
    service_groups = [s for s in service_groups_raw.split(",") if s] or None
    project_type = request.form.get("project_type", "Ongoing")
    priority = request.form.get("priority", "")
    loe = request.form.get("loe", "")
    website_url = request.form.get("website_url", "")

    slug = make_slug(client_name)
    try:
        loe_int = int(loe) if loe else 0
    except ValueError:
        loe_int = 0

    config = {
        "project_type": project_type,
        "priority": priority,
        "loe": loe_int,
        "website_url": website_url,
    }

    # Start onboarding in background thread
    with _running_lock:
        _running[slug] = "running"

    def _run():
        try:
            state = OnboardingState(slug, client_name, jira_key,
                                    service_groups=service_groups, config=config)
            state.save()

            if not preflight(state):
                with _running_lock:
                    _running[slug] = "failed"
                return

            step_clockify(state)
            step_asana(state, service_groups=service_groups)
            step_jira(state, service_groups=service_groups)
            step_drive(state)
            step_sheets(state, config)
            step_zoom(state)

            with _running_lock:
                _running[slug] = "complete"
        except Exception as e:
            log.error("Onboarding failed: %s\n%s", e, traceback.format_exc())
            with _running_lock:
                _running[slug] = "failed"
            try:
                state.set_step("_error", {"status": "failed", "message": str(e)})
            except Exception:
                pass

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    step_names_json = json.dumps([s[0] for s in STEP_LABELS])

    return render_template_string(RUN_PAGE,
        title="Running",
        client_name=client_name,
        slug=slug,
        step_labels=STEP_LABELS,
        step_names_json=step_names_json,
    )


@app.route("/status/<slug>")
def status_endpoint(slug):
    """JSON endpoint polled by the run page every 2 seconds."""
    try:
        slug = _validate_slug(slug)
    except ValueError:
        return jsonify({"status": "error", "steps": {}}), 400

    # JSON state file is authoritative — derive overall from step statuses
    try:
        state = OnboardingState.load(slug)
        steps = state.data.get("steps", {})

        # Check if all expected steps are present and complete/skipped
        expected = {"preflight", "clockify", "asana", "jira", "drive",
                    "sheets_client", "sheets_brand_distribution", "zoom"}
        present = set(steps.keys())
        # Only check statuses of expected steps (ignore internal keys like _error)
        statuses = [steps[k].get("status", "pending") for k in expected if k in present]

        if expected.issubset(present) and all(s in ("complete", "skipped") for s in statuses):
            overall = "complete"
        elif any(s == "failed" for s in statuses):
            overall = "failed"
        else:
            overall = "running"
        return jsonify({"status": overall, "steps": steps})
    except FileNotFoundError:
        with _running_lock:
            status = _running.get(slug, "unknown")
        return jsonify({"status": status, "steps": {}})


@app.route("/receipt/<slug>")
def receipt_page(slug):
    """Final receipt with all URLs + copy-as-markdown."""
    try:
        slug = _validate_slug(slug)
    except ValueError:
        from flask import abort
        abort(400)

    try:
        state = OnboardingState.load(slug)
    except FileNotFoundError:
        from flask import abort
        abort(404)

    steps = state.data.get("steps", {})
    client_name = state.client_name
    status = "complete" if all(
        s.get("status") in ("complete", "skipped") for s in steps.values()
    ) else "incomplete"

    # Build markdown receipt
    lines = [f"## {client_name} — Onboarding Receipt", f"**Date:** {state.data.get('updated_at', '')[:10]}", ""]
    for step_name, label in STEP_LABELS:
        step = steps.get(step_name, {})
        s = step.get("status", "pending")
        icon = {"complete": "✅", "skipped": "⏭️", "failed": "❌"}.get(s, "⏳")
        url = step.get("project_url") or step.get("root_folder_url") or step.get("spreadsheet_url") or ""
        lines.append(f"- {icon} **{label}** — {url if url else s}")
    markdown_receipt = "\n".join(lines)

    return render_template_string(RECEIPT_PAGE,
        title="Receipt",
        client_name=client_name,
        status=status,
        steps=steps,
        step_labels=STEP_LABELS,
        markdown_receipt=markdown_receipt,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    print("\n  Puzzles Client Onboarding")
    print("  http://localhost:5050\n")
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
