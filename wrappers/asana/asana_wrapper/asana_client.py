"""
Asana SDK Client - Core client with workspace context management.

Provides centralized Asana API client configuration with workspace context.
Credentials are loaded from Doppler based on selected workspace.

Usage:
    from asana_wrapper import use_workspace, get_client

    use_workspace("puzzles")  # Set context once
    client = get_client()     # Get configured client
"""

import asana
from typing import Optional, Dict, Any

from ._secrets import get_secret

# Workspace configurations
_WORKSPACES: Dict[str, Dict[str, str]] = {
    "puzzles": {
        "doppler_project": "puzzles",
        "doppler_key": "ASANA_API_KEY",
        "workspace_gid": None,  # Will be fetched on first use
    }
}

# Global state
_current_workspace: Optional[str] = None
_api_client: Optional[asana.ApiClient] = None
_workspace_gid: Optional[str] = None


def use_workspace(workspace: str = "puzzles") -> None:
    """
    Set the current workspace context.

    Loads credentials from Doppler and initializes the Asana client.
    Must be called before any other operations.

    Args:
        workspace: Workspace name (default: "puzzles")

    Raises:
        ValueError: If workspace is not configured
    """
    global _current_workspace, _api_client, _workspace_gid

    if workspace not in _WORKSPACES:
        available = ", ".join(_WORKSPACES.keys())
        raise ValueError(f"Unknown workspace '{workspace}'. Available: {available}")

    # Skip if already using this workspace
    if _current_workspace == workspace and _api_client is not None:
        return

    # Get credentials from Doppler
    config = _WORKSPACES[workspace]
    api_key = get_secret(config["doppler_key"], project=config["doppler_project"])

    # Initialize Asana client
    configuration = asana.Configuration()
    configuration.access_token = api_key

    _api_client = asana.ApiClient(configuration)
    _current_workspace = workspace
    _workspace_gid = None  # Reset, will be fetched on demand

    print(f"Asana: Using workspace '{workspace}'")


def get_client() -> asana.ApiClient:
    """
    Get the configured Asana API client.

    Returns:
        Configured asana.ApiClient instance

    Raises:
        RuntimeError: If use_workspace() hasn't been called
    """
    if _api_client is None:
        raise RuntimeError("No workspace selected. Call use_workspace() first.")
    return _api_client


def get_workspace_gid() -> str:
    """
    Get the current workspace GID.

    Fetches from API on first call and caches the result.

    Returns:
        Workspace GID string

    Raises:
        RuntimeError: If use_workspace() hasn't been called
        ValueError: If no workspaces found for user
    """
    global _workspace_gid

    if _api_client is None:
        raise RuntimeError("No workspace selected. Call use_workspace() first.")

    if _workspace_gid is not None:
        return _workspace_gid

    # Fetch workspaces from API
    workspaces_api = asana.WorkspacesApi(_api_client)
    workspaces = list(workspaces_api.get_workspaces(opts={"limit": 10}))

    if not workspaces:
        raise ValueError("No workspaces found for this user")

    # Use first workspace (most users have only one)
    _workspace_gid = workspaces[0]["gid"]
    print(f"Asana: Using workspace GID {_workspace_gid} ({workspaces[0].get('name', 'unnamed')})")

    return _workspace_gid


def get_current_workspace() -> Optional[str]:
    """
    Get the current workspace name.

    Returns:
        Current workspace name or None if not set
    """
    return _current_workspace


def close_all() -> None:
    """
    Clean up resources and reset state.

    Call this when done with Asana operations to release resources.
    """
    global _current_workspace, _api_client, _workspace_gid

    _current_workspace = None
    _api_client = None
    _workspace_gid = None

    print("Asana: Closed all connections")


# Convenience function to ensure workspace is set
def _ensure_workspace() -> None:
    """Internal helper to ensure workspace is configured."""
    if _api_client is None:
        use_workspace("puzzles")  # Default to puzzles
