"""
Clockify Wrapper — Time tracking API client.

Provides authenticated access to Clockify API with automatic workspace detection.
Credentials loaded from Doppler flowsly/prd (CLOCKIFY_API_KEY) by default.
Use configure() for non-default accounts (e.g., Puzzles).
"""

# Infrastructure
from ._base import (
    configure,
    get_workspace_id,
    get_workspaces,
    get_current_user,
    get_user_id,
    clear_cache,
)

# Clients
from .clients import (
    list_clients,
    get_client,
    find_client_by_name,
    create_client,
    update_client,
    archive_client,
    delete_client,
)

# Projects
from .projects import (
    list_projects,
    get_project,
    find_project_by_name,
    create_project,
    update_project,
    archive_project,
    delete_project,
)

# Reports
from .reports import (
    get_summary_report,
    get_client_summary,
    get_detailed_report,
    export_report_pdf,
)

__all__ = [
    # Infrastructure
    "configure",
    "get_workspace_id",
    "get_workspaces",
    "get_current_user",
    "get_user_id",
    "clear_cache",
    # Clients
    "list_clients",
    "get_client",
    "find_client_by_name",
    "create_client",
    "update_client",
    "archive_client",
    "delete_client",
    # Projects
    "list_projects",
    "get_project",
    "find_project_by_name",
    "create_project",
    "update_project",
    "archive_project",
    "delete_project",
    # Reports
    "get_summary_report",
    "get_client_summary",
    "get_detailed_report",
    "export_report_pdf",
]
