"""
Clockify Wrapper — client operations.

CRUD for Clockify clients (the billing entity that groups projects).
"""
from __future__ import annotations

import logging
from typing import Optional

from ._base import _api_call, get_workspace_id

log = logging.getLogger(__name__)


def list_clients(
    workspace_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    archived: Optional[bool] = None,
    page: int = 1,
    page_size: int = 200,
) -> list[dict]:
    """List clients in the workspace.

    Args:
        workspace_id: Override workspace (default: auto-detected).
        name: Filter by name (partial match, case-insensitive).
        archived: Filter by archived status. None = all.
        page: Page number (1-based).
        page_size: Results per page (max 5000).

    Returns:
        List of client dicts with id, name, email, note, archived, etc.
    """
    ws = workspace_id or get_workspace_id()
    params = {"page": page, "page-size": page_size}
    if name is not None:
        params["name"] = name
    if archived is not None:
        params["archived"] = str(archived).lower()
    return _api_call("GET", f"workspaces/{ws}/clients", params=params)


def get_client(client_id: str, workspace_id: Optional[str] = None) -> dict:
    """Get a single client by ID.

    Args:
        client_id: Clockify client ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Client dict.
    """
    ws = workspace_id or get_workspace_id()
    return _api_call("GET", f"workspaces/{ws}/clients/{client_id}")


def find_client_by_name(
    name: str,
    workspace_id: Optional[str] = None,
    *,
    exact: bool = True,
) -> dict | None:
    """Find a client by name.

    Args:
        name: Client name to search for.
        workspace_id: Override workspace (default: auto-detected).
        exact: If True, match exact name (case-insensitive). If False, partial match.

    Returns:
        Client dict if found, None otherwise.
    """
    clients = list_clients(workspace_id, name=name)
    for c in clients:
        if exact and c["name"].lower() == name.lower():
            return c
        if not exact and name.lower() in c["name"].lower():
            return c
    return None


def create_client(
    name: str,
    workspace_id: Optional[str] = None,
    *,
    email: Optional[str] = None,
    note: Optional[str] = None,
    address: Optional[str] = None,
) -> dict:
    """Create a new client.

    Args:
        name: Client name (required).
        workspace_id: Override workspace (default: auto-detected).
        email: Client email.
        note: Internal note.
        address: Client address.

    Returns:
        Created client dict with id, name, etc.
    """
    ws = workspace_id or get_workspace_id()
    body: dict = {"name": name}
    if email is not None:
        body["email"] = email
    if note is not None:
        body["note"] = note
    if address is not None:
        body["address"] = address

    result = _api_call("POST", f"workspaces/{ws}/clients", json_data=body)
    log.info("Created Clockify client: %s (id=%s)", name, result.get("id"))
    return result


def update_client(
    client_id: str,
    workspace_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    email: Optional[str] = None,
    note: Optional[str] = None,
    address: Optional[str] = None,
    archived: Optional[bool] = None,
) -> dict:
    """Update an existing client.

    Args:
        client_id: Clockify client ID.
        workspace_id: Override workspace (default: auto-detected).
        name: New name.
        email: New email.
        note: New note.
        address: New address.
        archived: Set archive status.

    Returns:
        Updated client dict.
    """
    ws = workspace_id or get_workspace_id()
    body: dict = {}
    if name is not None:
        body["name"] = name
    if email is not None:
        body["email"] = email
    if note is not None:
        body["note"] = note
    if address is not None:
        body["address"] = address
    if archived is not None:
        body["archived"] = archived

    if not body:
        raise ValueError("update_client() requires at least one field to update")

    return _api_call("PUT", f"workspaces/{ws}/clients/{client_id}", json_data=body)


def archive_client(client_id: str, workspace_id: Optional[str] = None) -> dict:
    """Archive a client.

    Clockify's PUT endpoint requires `name` in the body (full update, not patch).
    Fetches the current client first to preserve the name.

    Args:
        client_id: Clockify client ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Updated client dict.
    """
    current = get_client(client_id, workspace_id)
    return update_client(client_id, workspace_id, name=current["name"], archived=True)


def delete_client(client_id: str, workspace_id: Optional[str] = None) -> dict:
    """Delete a client permanently.

    Args:
        client_id: Clockify client ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Empty dict (204 response).
    """
    ws = workspace_id or get_workspace_id()
    result = _api_call("DELETE", f"workspaces/{ws}/clients/{client_id}")
    log.info("Deleted Clockify client: %s", client_id)
    return result
