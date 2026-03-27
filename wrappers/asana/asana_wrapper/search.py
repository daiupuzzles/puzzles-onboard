"""
Asana Search and Typeahead Operations.

Provides quick search/autocomplete functionality for various resource types.

Usage:
    from asana_wrapper import use_workspace, typeahead

    use_workspace("puzzles")
    results = typeahead("bug", resource_type="task")
"""

import asana
from typing import List, Dict, Any, Iterator

from .asana_client import get_client, get_workspace_gid, _ensure_workspace


def typeahead(
    query: str,
    resource_type: str = "task",
    count: int = 10,
    opt_fields: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    Quick typeahead search for resources.

    Fast autocomplete-style search that returns results as you type.
    More suitable for UI autocomplete than full search.

    Args:
        query: Search text (prefix matching)
        resource_type: Type to search ("task", "project", "user", "tag", "portfolio")
        count: Maximum results to return (default: 10, max: 100)
        opt_fields: Additional fields to return

    Returns:
        List of matching resource dictionaries

    Example:
        tasks = typeahead("bug", resource_type="task")
        projects = typeahead("dev", resource_type="project")
        users = typeahead("john", resource_type="user")
    """
    _ensure_workspace()
    typeahead_api = asana.TypeaheadApi(get_client())
    workspace_gid = get_workspace_gid()

    opts = {
        "resource_type": resource_type,
        "query": query,
        "count": min(count, 100),
    }

    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        # Default fields vary by resource type
        if resource_type == "task":
            opts["opt_fields"] = "gid,name,completed,assignee,projects"
        elif resource_type == "project":
            opts["opt_fields"] = "gid,name,archived,color"
        elif resource_type == "user":
            opts["opt_fields"] = "gid,name,email"
        elif resource_type == "tag":
            opts["opt_fields"] = "gid,name,color"
        else:
            opts["opt_fields"] = "gid,name"

    # Returns a list, not an iterator
    return list(typeahead_api.typeahead_for_workspace(workspace_gid, **opts))


def quick_find_task(query: str, count: int = 5) -> List[Dict[str, Any]]:
    """
    Quick find tasks by name prefix.

    Convenience wrapper around typeahead for tasks.

    Args:
        query: Task name to search for
        count: Maximum results (default: 5)

    Returns:
        List of matching task dictionaries

    Example:
        tasks = quick_find_task("review")
    """
    return typeahead(query, resource_type="task", count=count)


def quick_find_project(query: str, count: int = 5) -> List[Dict[str, Any]]:
    """
    Quick find projects by name prefix.

    Convenience wrapper around typeahead for projects.

    Args:
        query: Project name to search for
        count: Maximum results (default: 5)

    Returns:
        List of matching project dictionaries

    Example:
        projects = quick_find_project("dev")
    """
    return typeahead(query, resource_type="project", count=count)


def quick_find_user(query: str, count: int = 5) -> List[Dict[str, Any]]:
    """
    Quick find users by name prefix.

    Convenience wrapper around typeahead for users.

    Args:
        query: User name to search for
        count: Maximum results (default: 5)

    Returns:
        List of matching user dictionaries

    Example:
        users = quick_find_user("john")
    """
    return typeahead(query, resource_type="user", count=count)
