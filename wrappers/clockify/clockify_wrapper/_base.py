"""
Clockify Wrapper — base infrastructure.

Auth, API call, secrets management, workspace context.
Credentials loaded from Doppler flowsly/prd (CLOCKIFY_API_KEY).
"""
from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from ._secrets import get_secrets

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://api.clockify.me/api/v1"
REPORTS_URL = "https://reports.api.clockify.me/v1"

_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubles each attempt

# ---------------------------------------------------------------------------
# Global state — lazy-loaded
# ---------------------------------------------------------------------------
_api_key: Optional[str] = None
_workspace_id: Optional[str] = None
_user_id: Optional[str] = None


def _get_api_key() -> str:
    global _api_key
    if _api_key is None:
        secrets = get_secrets()
        _api_key = secrets.get("CLOCKIFY_API_KEY", "")
        if not _api_key:
            raise RuntimeError("Missing CLOCKIFY_API_KEY in Doppler flowsly/prd")
    return _api_key


# ---------------------------------------------------------------------------
# Core API call with retry
# ---------------------------------------------------------------------------
def _api_call(
    method: str,
    path: str,
    *,
    base_url: Optional[str] = None,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
) -> dict | list:
    """Make authenticated Clockify API call with retry on rate-limit / 5xx.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE).
        path: API path (without base URL), e.g. "workspaces".
        base_url: Override base URL (used for Reports API).
        params: Query parameters.
        json_data: JSON request body.

    Returns:
        Parsed JSON (dict or list). Empty dict for 204 responses.

    Raises:
        RuntimeError: After max retries exhausted.
        requests.HTTPError: On 4xx client errors.
    """
    url = f"{base_url or BASE_URL}/{path.lstrip('/')}"
    headers = {
        "X-Api-Key": _get_api_key(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    last_exc: Exception | None = None
    last_status: int | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(
                method, url, headers=headers, params=params, json=json_data, timeout=30
            )

            # Rate limited — retry with backoff
            if resp.status_code == 429:
                last_status = 429
                wait = _RETRY_BACKOFF * (2**attempt)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = max(wait, int(retry_after))
                    except ValueError:
                        pass  # non-integer Retry-After, use computed backoff
                log.warning(
                    "Rate limited, retrying in %ds (attempt %d/%d)",
                    wait, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            # Server error — retry
            if resp.status_code >= 500:
                last_status = resp.status_code
                wait = _RETRY_BACKOFF * (2**attempt)
                log.warning(
                    "Server error %s, retrying in %ds (attempt %d/%d)",
                    resp.status_code, wait, attempt + 1, _MAX_RETRIES,
                )
                time.sleep(wait)
                continue

            # Client errors — fail immediately
            resp.raise_for_status()

            # No content
            if resp.status_code == 204 or not resp.content:
                return {}

            return resp.json()

        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            wait = _RETRY_BACKOFF * (2**attempt)
            log.warning("Connection error, retrying in %ds: %s", wait, exc)
            time.sleep(wait)

    status_info = f" (last status: {last_status})" if last_status else ""
    raise last_exc or RuntimeError(
        f"Clockify API call failed after {_MAX_RETRIES} retries: {method} {path}{status_info}"
    )


def _api_call_raw(
    method: str,
    path: str,
    *,
    base_url: Optional[str] = None,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
) -> bytes:
    """Like _api_call but returns raw response bytes (for PDF/CSV export).

    Same retry logic as _api_call, but sets Accept: */* and returns
    resp.content instead of parsing JSON.
    """
    url = f"{base_url or BASE_URL}/{path.lstrip('/')}"
    headers = {
        "X-Api-Key": _get_api_key(),
        "Content-Type": "application/json",
        "Accept": "*/*",
    }

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(
                method, url, headers=headers, params=params, json=json_data, timeout=60
            )

            if resp.status_code == 429:
                wait = _RETRY_BACKOFF * (2**attempt)
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = max(wait, int(retry_after))
                log.warning("Rate limited, retrying in %ds (attempt %d/%d)", wait, attempt + 1, _MAX_RETRIES)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = _RETRY_BACKOFF * (2**attempt)
                log.warning("Server error %s, retrying in %ds (attempt %d/%d)", resp.status_code, wait, attempt + 1, _MAX_RETRIES)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.content

        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            wait = _RETRY_BACKOFF * (2**attempt)
            log.warning("Connection error, retrying in %ds: %s", wait, exc)
            time.sleep(wait)

    raise last_exc or RuntimeError(
        f"Clockify API call (raw) failed after {_MAX_RETRIES} retries: {method} {path}"
    )


# ---------------------------------------------------------------------------
# Workspace & user discovery
# ---------------------------------------------------------------------------
def get_workspaces() -> list[dict]:
    """List all workspaces for the authenticated user."""
    return _api_call("GET", "workspaces")


def get_workspace_id() -> str:
    """Get (and cache) the first workspace ID for the authenticated user.

    If a workspace was set via configure(), returns that. Otherwise auto-detects.
    """
    global _workspace_id
    if _workspace_id is None:
        workspaces = get_workspaces()
        if not workspaces:
            raise RuntimeError("No Clockify workspaces found for this API key")
        _workspace_id = workspaces[0]["id"]
        log.info("Clockify workspace: %s (%s)", workspaces[0].get("name"), _workspace_id)
    return _workspace_id


def configure(
    *,
    api_key: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> None:
    """Explicitly set API key and/or workspace ID.

    Use this when working with a non-default Clockify account (e.g., Puzzles
    vs Flowsly). Call before any other wrapper functions.

    Resets cached user ID when api_key changes (different account = different user).

    Args:
        api_key: Clockify API key. Skips Doppler lookup if provided.
        workspace_id: Clockify workspace ID. Skips auto-detection if provided.
    """
    global _api_key, _workspace_id, _user_id
    if api_key is not None:
        _api_key = api_key
        _user_id = None  # different key = different user
        log.info("Clockify API key set explicitly (user cache cleared)")
    if workspace_id is not None:
        _workspace_id = workspace_id
        log.info("Clockify workspace set explicitly: %s", workspace_id)


def get_current_user() -> dict:
    """Get the authenticated user's profile."""
    return _api_call("GET", "user")


def get_user_id() -> str:
    """Get (and cache) the authenticated user's ID."""
    global _user_id
    if _user_id is None:
        user = get_current_user()
        _user_id = user["id"]
    return _user_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clear_cache() -> None:
    """Reset all cached state (useful for testing)."""
    global _api_key, _workspace_id, _user_id
    _api_key = None
    _workspace_id = None
    _user_id = None
