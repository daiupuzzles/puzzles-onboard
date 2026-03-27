"""
Supabase Client Base — Project config, client creation, connection management.
"""

import os
import logging
import threading
from typing import Optional, Any, Dict

logger = logging.getLogger("supabase_wrapper")

# Project configurations
_PROJECTS = {
    "ghl": {
        "env_prefix": "SUPABASE_GHL",
        "doppler_prefix": "SUPABASE_GHL",
    },
    "dillingus": {
        "env_prefix": "SUPABASE_DILLINGUS",
        "doppler_prefix": "SUPABASE_DILLINGUS",
    },
    "co": {
        "env_prefix": "SUPABASE_CO",
        "doppler_prefix": "SUPABASE_CO",
    },
    "puzzles": {
        "env_prefix": "SUPABASE_INHOUSE",
        "doppler_prefix": "SUPABASE_INHOUSE",
    },
}

# Pooler fallback ports: transaction (6543) → session (5432)
TRANSACTION_POOLER_PORT = "6543"
SESSION_POOLER_PORT = "5432"

# n8n webhook fallback for SQL execution when both poolers are down.
N8N_WEBHOOK_BASE = "https://n8n.flowsly.ai/webhook"
N8N_SQL_WEBHOOKS: Dict[str, str] = {
    "ghl": "execute-sql",
    "dillingus": "execute-sql-dillingus",
    "co": "execute-sql-co",
}

# Current project context
_current_project: Optional[str] = None
_clients: Dict[str, Any] = {}
_tls = threading.local()
_port_overrides: Dict[str, str] = {}


def _get_connections() -> Dict[str, Any]:
    """Return the per-thread connection dict."""
    if not hasattr(_tls, "connections"):
        _tls.connections = {}
    return _tls.connections


def _get_config(project: str) -> dict:
    """Get configuration for a Supabase project."""
    if project not in _PROJECTS:
        raise ValueError(f"Unknown project '{project}'. Available: {list(_PROJECTS.keys())}")

    config = _PROJECTS[project]

    # Try Doppler first
    try:
        from supabase_wrapper._secrets import get_secrets
        secrets = get_secrets()

        return {
            "user": secrets.get(f"{config['doppler_prefix']}_DB_USER"),
            "password": secrets.get(f"{config['doppler_prefix']}_DB_PASSWORD"),
            "host": secrets.get(f"{config['doppler_prefix']}_DB_HOST"),
            "port": secrets.get(f"{config['doppler_prefix']}_DB_PORT", "6543"),
            "dbname": secrets.get(f"{config['doppler_prefix']}_DB_NAME", "postgres"),
            "direct_host": secrets.get(f"{config['doppler_prefix']}_DIRECT_HOST"),
            "direct_user": secrets.get(f"{config['doppler_prefix']}_DIRECT_USER", "postgres"),
            "url": secrets.get(f"{config['doppler_prefix']}_URL"),
            "service_key": secrets.get(f"{config['doppler_prefix']}_SERVICE_KEY"),
            "anon_key": secrets.get(f"{config['doppler_prefix']}_ANON_KEY"),
        }
    except Exception:
        # Fall back to environment variables
        prefix = config["env_prefix"]
        return {
            "user": os.getenv(f"{prefix}_USER"),
            "password": os.getenv(f"{prefix}_PASSWORD"),
            "host": os.getenv(f"{prefix}_HOST"),
            "port": os.getenv(f"{prefix}_PORT", "6543"),
            "dbname": os.getenv(f"{prefix}_DBNAME", "postgres"),
            "direct_host": os.getenv(f"{prefix}_DIRECT_HOST"),
            "direct_user": os.getenv(f"{prefix}_DIRECT_USER", "postgres"),
            "url": os.getenv(f"{prefix}_URL"),
            "service_key": os.getenv(f"{prefix}_SERVICE_KEY"),
            "anon_key": os.getenv(f"{prefix}_ANON_KEY"),
        }


def use_project(project: str) -> None:
    """Set the current Supabase project context.

    Args:
        project: Project identifier (e.g., "ghl", "dillingus")
    """
    global _current_project

    if project not in _PROJECTS:
        raise ValueError(f"Unknown project '{project}'. Available: {list(_PROJECTS.keys())}")

    _current_project = project


def get_current_project() -> Optional[str]:
    """Return the current project name, or None if not set."""
    return _current_project


def _resolve_project(project: Optional[str] = None) -> str:
    """Resolve project from argument or current context."""
    project = project or _current_project
    if not project:
        raise RuntimeError("No project specified. Call use_project() first or pass project name.")
    return project


def get_client(project: str = None):
    """Get a Supabase Python client for REST API access.

    This is the preferred method — works through firewalls.

    Args:
        project: Project identifier. Uses current context if not specified.

    Returns:
        Supabase client object
    """
    project = _resolve_project(project)

    if project in _clients:
        return _clients[project]

    try:
        from supabase import create_client
    except ImportError:
        raise ImportError("supabase not installed. Run: pip install supabase")

    config = _get_config(project)
    client = create_client(config["url"], config["service_key"])
    _clients[project] = client
    return client


# Alias for backwards compatibility
get_supabase_client = get_client


def close_all() -> None:
    """Close all connections and clients (current thread only)."""
    _connections = _get_connections()
    for project in list(_connections.keys()):
        try:
            _connections[project].close()
        except Exception:
            pass
        del _connections[project]
    _clients.clear()
    _port_overrides.clear()


# Alias
close_all_connections = close_all
