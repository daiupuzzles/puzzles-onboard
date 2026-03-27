"""
Google API Base — Multi-account OAuth2 auth and service creation.
Uses refresh tokens stored in Doppler.
"""

import logging
import time
from typing import Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger("google_wrapper.auth")

# ---------------------------------------------------------------------------
# Account registry
# ---------------------------------------------------------------------------

ACCOUNTS = {
    "flowsly.io": {
        "email": "daniel@flowsly.io",
        "refresh_token_key": "GOOGLE_FLOWSLY_IO_REFRESH_TOKEN",
        "client_id_key": "GOOGLE_FLOWSLY_IO_CLIENT_ID",
        "client_secret_key": "GOOGLE_FLOWSLY_IO_CLIENT_SECRET",
    },
    "flowsly.ai": {
        "email": "daniel@flowsly.ai",
        "refresh_token_key": "GOOGLE_FLOWSLY_AI_REFRESH_TOKEN",
        "client_id_key": "GOOGLE_FLOWSLY_AI_CLIENT_ID",
        "client_secret_key": "GOOGLE_FLOWSLY_AI_CLIENT_SECRET",
    },
    "puzzles": {
        "email": "daniel@puzzles.consulting",
        "refresh_token_key": "GOOGLE_PUZZLES_REFRESH_TOKEN",
        "client_id_key": "GOOGLE_PUZZLES_CLIENT_ID",
        "client_secret_key": "GOOGLE_PUZZLES_CLIENT_SECRET",
    },
}

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/contacts.readonly",
    "https://mail.google.com/",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_current_account: Optional[str] = None

# Caches: {account_slug: service_object}
_service_cache: dict = {}  # keys like "flowsly.io:gmail:v1"
_creds_cache: dict = {}    # keys like "flowsly.io"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def _get_secrets():
    """Load shared Google OAuth secrets from Doppler flowsly/prd."""
    from ._secrets import get_secrets
    return get_secrets()


def _build_credentials(account_slug: str) -> Credentials:
    """Build OAuth2 Credentials for an account using its refresh token."""
    if account_slug in _creds_cache:
        creds = _creds_cache[account_slug]
        if creds.valid:
            return creds
        # Token expired — rebuild below

    if account_slug not in ACCOUNTS:
        raise ValueError(
            f"Unknown account '{account_slug}'. "
            f"Valid: {', '.join(ACCOUNTS.keys())}"
        )

    secrets = _get_secrets()
    acct = ACCOUNTS[account_slug]

    client_id = secrets.get(acct["client_id_key"])
    client_secret = secrets.get(acct["client_secret_key"])
    refresh_token = secrets.get(acct["refresh_token_key"])

    if not client_id or not client_secret:
        raise RuntimeError(
            f"{acct['client_id_key']} / {acct['client_secret_key']} not found in Doppler flowsly/prd"
        )
    if not refresh_token:
        raise RuntimeError(
            f"Refresh token '{acct['refresh_token_key']}' not found in Doppler. "
            f"Run authenticate('{account_slug}') first."
        )

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
    )

    _creds_cache[account_slug] = creds
    return creds


# ---------------------------------------------------------------------------
# Service builders
# ---------------------------------------------------------------------------

def _get_service(service_name: str, version: str, account: Optional[str] = None):
    """Build or return cached Google API service."""
    slug = account or get_current_account()
    cache_key = f"{slug}:{service_name}:{version}"

    if cache_key in _service_cache:
        return _service_cache[cache_key]

    creds = _build_credentials(slug)
    service = build(service_name, version, credentials=creds, cache_discovery=False)
    _service_cache[cache_key] = service
    return service


def get_gmail_service(account: Optional[str] = None):
    """Get Gmail API v1 service."""
    return _get_service("gmail", "v1", account)


def get_calendar_service(account: Optional[str] = None):
    """Get Calendar API v3 service."""
    return _get_service("calendar", "v3", account)


def get_people_service(account: Optional[str] = None):
    """Get People API v1 service."""
    return _get_service("people", "v1", account)


def get_drive_service(account: Optional[str] = None):
    """Get Drive API v3 service."""
    return _get_service("drive", "v3", account)


def get_docs_service(account: Optional[str] = None):
    """Get Docs API v1 service."""
    return _get_service("docs", "v1", account)


# ---------------------------------------------------------------------------
# Account context
# ---------------------------------------------------------------------------

def use_google(slug: str) -> str:
    """Set the current Google account context. Returns the slug."""
    global _current_account
    if slug not in ACCOUNTS:
        raise ValueError(
            f"Unknown account '{slug}'. Valid: {', '.join(ACCOUNTS.keys())}"
        )
    _current_account = slug
    logger.debug("Google account context set to '%s'", slug)
    return slug


def get_current_account() -> str:
    """Return current account slug. Raises if not set."""
    if not _current_account:
        raise RuntimeError("No Google account set. Call use_google(slug) first.")
    return _current_account


def get_account_email(account: Optional[str] = None) -> str:
    """Return email address for an account slug."""
    slug = account or get_current_account()
    if slug not in ACCOUNTS:
        raise ValueError(f"Unknown account '{slug}'")
    return ACCOUNTS[slug]["email"]


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

def api_call_with_retry(fn, *args, max_retries: int = 3, **kwargs):
    """Execute a Google API call with exponential backoff on rate limit errors."""
    from googleapiclient.errors import HttpError

    for attempt in range(max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except HttpError as e:
            if e.resp.status in (429, 500, 503) and attempt < max_retries:
                wait = (2 ** attempt) + (time.time() % 1)  # jitter
                logger.warning(
                    "Google API %s (attempt %d/%d), retrying in %.1fs",
                    e.resp.status, attempt + 1, max_retries + 1, wait,
                )
                time.sleep(wait)
                continue
            raise


# ---------------------------------------------------------------------------
# First-time OAuth setup (interactive)
# ---------------------------------------------------------------------------

def authenticate(slug: str):
    """Run interactive OAuth2 flow for first-time account setup.

    Opens browser, gets authorization code, exchanges for tokens,
    and stores the refresh token in Doppler flowsly/prd.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    if slug not in ACCOUNTS:
        raise ValueError(f"Unknown account '{slug}'. Valid: {', '.join(ACCOUNTS.keys())}")

    secrets = _get_secrets()
    acct = ACCOUNTS[slug]
    client_id = secrets.get(acct["client_id_key"])
    client_secret = secrets.get(acct["client_secret_key"])

    if not client_id or not client_secret:
        raise RuntimeError(
            f"{acct['client_id_key']} / {acct['client_secret_key']} not found in Doppler"
        )

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(
        port=8090,
        prompt="consent",
        access_type="offline",
        login_hint=ACCOUNTS[slug]["email"],
    )

    refresh_token = creds.refresh_token
    if not refresh_token:
        raise RuntimeError(
            "No refresh token returned. Revoke app access at "
            "https://myaccount.google.com/permissions and try again."
        )

    token_key = ACCOUNTS[slug]["refresh_token_key"]

    # Store in Doppler
    import subprocess
    result = subprocess.run(
        [
            "doppler", "secrets", "set",
            f'{token_key}={refresh_token}',
            "--project", "flowsly",
            "--config", "prd",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to store token in Doppler: {result.stderr}")

    # Clear caches so next call picks up new token
    _creds_cache.pop(slug, None)
    for key in list(_service_cache.keys()):
        if key.startswith(f"{slug}:"):
            _service_cache.pop(key, None)

    logger.info("Authenticated '%s' (%s). Refresh token stored as %s in Doppler.",
                slug, ACCOUNTS[slug]["email"], token_key)
    print(f"Authenticated {ACCOUNTS[slug]['email']}. Token stored in Doppler as {token_key}.")
