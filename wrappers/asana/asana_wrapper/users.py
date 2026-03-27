"""
Asana User Operations.

Provides functions for querying users in the workspace.

Usage:
    from asana_wrapper import use_workspace, get_me, find_user_by_name

    use_workspace("puzzles")
    me = get_me()
    user = find_user_by_name("John")
"""

import asana
from typing import Optional, List, Dict, Any, Iterator

from .asana_client import get_client, get_workspace_gid, _ensure_workspace


def get_me(opt_fields: List[str] = None) -> Dict[str, Any]:
    """
    Get the current authenticated user.

    Args:
        opt_fields: Additional fields to return

    Returns:
        User dictionary with current user info

    Example:
        me = get_me()
        print(f"Logged in as {me['name']}")
    """
    _ensure_workspace()
    users_api = asana.UsersApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,email,workspaces"

    return users_api.get_user("me", opts=opts)


def get_user(user_gid: str, opt_fields: List[str] = None) -> Dict[str, Any]:
    """
    Get a user by GID.

    Args:
        user_gid: The user's GID
        opt_fields: Additional fields to return

    Returns:
        User dictionary

    Example:
        user = get_user("12345")
        print(user["name"])
    """
    _ensure_workspace()
    users_api = asana.UsersApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,email"

    return users_api.get_user(user_gid, opts=opts)


def get_users(opt_fields: List[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Get all users in the current workspace.

    Args:
        opt_fields: Additional fields to return

    Returns:
        Iterator of user dictionaries

    Example:
        users = list(get_users())
        for user in get_users():
            print(user["name"])
    """
    _ensure_workspace()
    users_api = asana.UsersApi(get_client())
    workspace_gid = get_workspace_gid()

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,email"

    return users_api.get_users_for_workspace(workspace_gid, opts=opts)


def find_user_by_name(name: str, exact: bool = False) -> Optional[Dict[str, Any]]:
    """
    Find a user by name.

    Args:
        name: User name to search for
        exact: Require exact match (default: False, case-insensitive partial match)

    Returns:
        User dictionary or None if not found

    Example:
        user = find_user_by_name("John")
        user = find_user_by_name("john doe", exact=True)
    """
    _ensure_workspace()

    name_lower = name.lower()
    for user in get_users():
        user_name = user.get("name", "")
        if exact:
            if user_name == name:
                return user
        else:
            if name_lower in user_name.lower():
                return user

    return None


def find_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Find a user by email address.

    Args:
        email: Email address to search for (case-insensitive)

    Returns:
        User dictionary or None if not found

    Example:
        user = find_user_by_email("john@company.com")
    """
    _ensure_workspace()

    email_lower = email.lower()
    for user in get_users(opt_fields=["gid", "name", "email"]):
        user_email = user.get("email", "")
        if user_email and user_email.lower() == email_lower:
            return user

    return None
