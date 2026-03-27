"""
Asana Project Operations.

Provides functions for listing and querying projects and sections.

Usage:
    from asana_wrapper import use_workspace, get_projects, find_project_by_name

    use_workspace("puzzles")
    projects = list(get_projects())
    dev_project = find_project_by_name("Development")
"""

import asana
from typing import Optional, List, Dict, Any, Iterator

from .asana_client import get_client, get_workspace_gid, _ensure_workspace


def get_projects(
    team: str = None,
    archived: bool = False,
    opt_fields: List[str] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Get projects in the current workspace.

    Args:
        team: Team GID to filter by (optional)
        archived: Include archived projects (default: False)
        opt_fields: Additional fields to return

    Returns:
        Iterator of project dictionaries

    Example:
        projects = list(get_projects())
        for project in get_projects():
            print(project["name"])
    """
    _ensure_workspace()
    projects_api = asana.ProjectsApi(get_client())
    workspace_gid = get_workspace_gid()

    opts = {"archived": archived}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,archived,color,notes,owner,team,created_at"

    if team:
        return projects_api.get_projects_for_team(team, opts=opts)
    else:
        return projects_api.get_projects_for_workspace(workspace_gid, opts=opts)


def get_project(project_gid: str, opt_fields: List[str] = None) -> Dict[str, Any]:
    """
    Get a single project by GID.

    Args:
        project_gid: The project's GID
        opt_fields: Additional fields to return

    Returns:
        Project dictionary

    Example:
        project = get_project("12345")
        print(project["name"])
    """
    _ensure_workspace()
    projects_api = asana.ProjectsApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,archived,color,notes,owner,team,created_at,due_on,start_on,members,custom_fields"

    return projects_api.get_project(project_gid, opts=opts)


def find_project_by_name(name: str, exact: bool = False) -> Optional[Dict[str, Any]]:
    """
    Find a project by name.

    Args:
        name: Project name to search for
        exact: Require exact match (default: False, case-insensitive partial match)

    Returns:
        Project dictionary or None if not found

    Example:
        project = find_project_by_name("Development")
        project = find_project_by_name("Dev", exact=False)  # Finds "Development"
    """
    _ensure_workspace()

    name_lower = name.lower()
    for project in get_projects():
        project_name = project.get("name", "")
        if exact:
            if project_name == name:
                return project
        else:
            if name_lower in project_name.lower():
                return project

    return None


def get_sections(project_gid: str, opt_fields: List[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Get sections in a project.

    Args:
        project_gid: The project's GID
        opt_fields: Additional fields to return

    Returns:
        Iterator of section dictionaries

    Example:
        sections = list(get_sections("12345"))
        for section in get_sections("12345"):
            print(section["name"])
    """
    _ensure_workspace()
    sections_api = asana.SectionsApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,created_at"

    return sections_api.get_sections_for_project(project_gid, opts=opts)


def get_section(section_gid: str, opt_fields: List[str] = None) -> Dict[str, Any]:
    """
    Get a single section by GID.

    Args:
        section_gid: The section's GID
        opt_fields: Additional fields to return

    Returns:
        Section dictionary

    Example:
        section = get_section("12345")
    """
    _ensure_workspace()
    sections_api = asana.SectionsApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,created_at,project"

    return sections_api.get_section(section_gid, opts=opts)


def find_section_by_name(project_gid: str, name: str, exact: bool = False) -> Optional[Dict[str, Any]]:
    """
    Find a section by name within a project.

    Args:
        project_gid: The project's GID
        name: Section name to search for
        exact: Require exact match (default: False)

    Returns:
        Section dictionary or None if not found

    Example:
        section = find_section_by_name("project123", "In Progress")
    """
    _ensure_workspace()

    name_lower = name.lower()
    for section in get_sections(project_gid):
        section_name = section.get("name", "")
        if exact:
            if section_name == name:
                return section
        else:
            if name_lower in section_name.lower():
                return section

    return None


def get_teams(opt_fields: List[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Get teams in the current workspace.

    Args:
        opt_fields: Additional fields to return

    Returns:
        Iterator of team dictionaries

    Example:
        teams = list(get_teams())
    """
    _ensure_workspace()
    teams_api = asana.TeamsApi(get_client())
    workspace_gid = get_workspace_gid()

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,description"

    return teams_api.get_teams_for_workspace(workspace_gid, opts=opts)
