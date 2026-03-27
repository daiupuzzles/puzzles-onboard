"""
Clockify Wrapper — project operations.

CRUD for Clockify projects. Projects belong to a workspace and optionally
to a client (billing entity).
"""
from __future__ import annotations

import logging
from typing import Optional

from ._base import _api_call, get_workspace_id

log = logging.getLogger(__name__)


def list_projects(
    workspace_id: Optional[str] = None,
    *,
    client_id: Optional[str] = None,
    name: Optional[str] = None,
    archived: Optional[bool] = None,
    billable: Optional[bool] = None,
    page: int = 1,
    page_size: int = 200,
) -> list[dict]:
    """List projects in the workspace.

    Args:
        workspace_id: Override workspace (default: auto-detected).
        client_id: Filter by client ID.
        name: Filter by name (partial match, case-insensitive).
        archived: Filter by archived status. None = all.
        billable: Filter by billable status. None = all.
        page: Page number (1-based).
        page_size: Results per page (max 5000).

    Returns:
        List of project dicts with id, name, clientId, billable, archived, etc.
    """
    ws = workspace_id or get_workspace_id()
    params: dict = {"page": page, "page-size": page_size}
    if client_id is not None:
        params["clients"] = client_id
    if name is not None:
        params["name"] = name
    if archived is not None:
        params["archived"] = str(archived).lower()
    if billable is not None:
        params["billable"] = str(billable).lower()
    return _api_call("GET", f"workspaces/{ws}/projects", params=params)


def get_project(project_id: str, workspace_id: Optional[str] = None) -> dict:
    """Get a single project by ID.

    Args:
        project_id: Clockify project ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Project dict.
    """
    ws = workspace_id or get_workspace_id()
    return _api_call("GET", f"workspaces/{ws}/projects/{project_id}")


def find_project_by_name(
    name: str,
    workspace_id: Optional[str] = None,
    *,
    exact: bool = True,
    client_id: Optional[str] = None,
) -> dict | None:
    """Find a project by name.

    Args:
        name: Project name to search for.
        workspace_id: Override workspace (default: auto-detected).
        exact: If True, match exact name (case-insensitive). If False, partial match.
        client_id: Narrow search to a specific client's projects.

    Returns:
        Project dict if found, None otherwise.
    """
    projects = list_projects(workspace_id, name=name, client_id=client_id)
    for p in projects:
        if exact and p["name"].lower() == name.lower():
            return p
        if not exact and name.lower() in p["name"].lower():
            return p
    return None


def create_project(
    name: str,
    workspace_id: Optional[str] = None,
    *,
    client_id: Optional[str] = None,
    billable: bool = True,
    is_public: bool = True,
    color: Optional[str] = None,
    note: Optional[str] = None,
) -> dict:
    """Create a new project.

    Args:
        name: Project name (required).
        workspace_id: Override workspace (default: auto-detected).
        client_id: Associate with a client.
        billable: Whether time entries are billable (default: True).
        is_public: Whether visible to all workspace members (default: True).
        color: Hex color code (e.g., "#0b83d9").
        note: Project description/notes.

    Returns:
        Created project dict with id, name, clientId, etc.
    """
    ws = workspace_id or get_workspace_id()
    body: dict = {
        "name": name,
        "billable": billable,
        "isPublic": is_public,
    }
    if client_id is not None:
        body["clientId"] = client_id
    if color is not None:
        body["color"] = color
    if note is not None:
        body["note"] = note

    result = _api_call("POST", f"workspaces/{ws}/projects", json_data=body)
    log.info("Created Clockify project: %s (id=%s)", name, result.get("id"))
    return result


def update_project(
    project_id: str,
    workspace_id: Optional[str] = None,
    *,
    name: Optional[str] = None,
    client_id: Optional[str] = None,
    billable: Optional[bool] = None,
    is_public: Optional[bool] = None,
    color: Optional[str] = None,
    note: Optional[str] = None,
    archived: Optional[bool] = None,
) -> dict:
    """Update an existing project.

    Args:
        project_id: Clockify project ID.
        workspace_id: Override workspace (default: auto-detected).
        name: New name.
        client_id: New client association.
        billable: New billable status.
        is_public: New visibility.
        color: New color.
        note: New notes.
        archived: Set archive status.

    Returns:
        Updated project dict.
    """
    ws = workspace_id or get_workspace_id()
    body: dict = {}
    if name is not None:
        body["name"] = name
    if client_id is not None:
        body["clientId"] = client_id
    if billable is not None:
        body["billable"] = billable
    if is_public is not None:
        body["isPublic"] = is_public
    if color is not None:
        body["color"] = color
    if note is not None:
        body["note"] = note
    if archived is not None:
        body["archived"] = archived

    if not body:
        raise ValueError("update_project() requires at least one field to update")

    return _api_call("PUT", f"workspaces/{ws}/projects/{project_id}", json_data=body)


def archive_project(project_id: str, workspace_id: Optional[str] = None) -> dict:
    """Archive a project.

    Args:
        project_id: Clockify project ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Updated project dict.
    """
    return update_project(project_id, workspace_id, archived=True)


def delete_project(project_id: str, workspace_id: Optional[str] = None) -> dict:
    """Delete a project permanently.

    Args:
        project_id: Clockify project ID.
        workspace_id: Override workspace (default: auto-detected).

    Returns:
        Empty dict (204 response).
    """
    ws = workspace_id or get_workspace_id()
    result = _api_call("DELETE", f"workspaces/{ws}/projects/{project_id}")
    log.info("Deleted Clockify project: %s", project_id)
    return result
