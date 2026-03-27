"""
Asana Task Operations.

Provides functions for creating, reading, updating, and completing tasks.

Usage:
    from asana_wrapper import use_workspace, get_tasks, create_task

    use_workspace("puzzles")
    tasks = list(get_tasks(assignee="me"))
    new_task = create_task("Review PR", due_on="2024-01-15")
"""

import asana
from typing import Optional, List, Dict, Any, Iterator
from datetime import datetime, date

from .asana_client import get_client, get_workspace_gid, _ensure_workspace


def get_tasks(
    project: str = None,
    assignee: str = "me",
    completed_since: str = "now",
    modified_since: str = None,
    section: str = None,
    opt_fields: List[str] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Get tasks with various filters.

    Args:
        project: Project GID to filter by
        assignee: Assignee GID or "me" (default: "me")
        completed_since: Filter completed tasks. "now" means incomplete only (default)
        modified_since: ISO date string to filter by modification date
        section: Section GID to filter by
        opt_fields: Additional fields to return

    Returns:
        Iterator of task dictionaries

    Example:
        tasks = list(get_tasks(assignee="me"))
        for task in get_tasks(project="12345"):
            print(task["name"])
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    # Build options
    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,completed,due_on,assignee,projects,tags,notes"

    if completed_since:
        opts["completed_since"] = completed_since
    if modified_since:
        opts["modified_since"] = modified_since

    # Choose endpoint based on filters
    if section:
        return tasks_api.get_tasks_for_section(section, opts=opts)
    elif project:
        return tasks_api.get_tasks_for_project(project, opts=opts)
    elif assignee:
        workspace_gid = get_workspace_gid()
        return tasks_api.get_tasks(
            opts={**opts, "assignee": assignee, "workspace": workspace_gid}
        )
    else:
        raise ValueError("Must specify at least one of: project, assignee, or section")


def get_task(task_gid: str, opt_fields: List[str] = None) -> Dict[str, Any]:
    """
    Get a single task by GID.

    Args:
        task_gid: The task's GID
        opt_fields: Additional fields to return

    Returns:
        Task dictionary

    Example:
        task = get_task("12345")
        print(task["name"], task["completed"])
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,completed,due_on,due_at,assignee,projects,tags,notes,parent,subtasks,custom_fields"

    return tasks_api.get_task(task_gid, opts=opts)


def create_task(
    name: str,
    project: str = None,
    assignee: str = "me",
    due_on: str = None,
    due_at: str = None,
    notes: str = None,
    parent: str = None,
    tags: List[str] = None,
    custom_fields: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Create a new task.

    Args:
        name: Task name (required)
        project: Project GID to add task to
        assignee: Assignee GID or "me" (default: "me")
        due_on: Due date as YYYY-MM-DD string
        due_at: Due datetime as ISO string (overrides due_on)
        notes: Task description/notes
        parent: Parent task GID (creates a subtask)
        tags: List of tag GIDs
        custom_fields: Dict of custom field GID -> value

    Returns:
        Created task dictionary

    Example:
        task = create_task("Review PR", due_on="2024-01-15")
        task = create_task("Subtask", parent="12345")
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())
    workspace_gid = get_workspace_gid()

    # Build task data
    data = {
        "name": name,
        "workspace": workspace_gid,
    }

    if assignee:
        data["assignee"] = assignee
    if due_on:
        data["due_on"] = due_on
    if due_at:
        data["due_at"] = due_at
    if notes:
        data["notes"] = notes
    if parent:
        data["parent"] = parent
    if tags:
        data["tags"] = tags
    if custom_fields:
        data["custom_fields"] = custom_fields
    if project:
        data["projects"] = [project]

    body = {"data": data}
    return tasks_api.create_task(body, opts={})


def update_task(task_gid: str, **updates) -> Dict[str, Any]:
    """
    Update a task.

    Args:
        task_gid: The task's GID
        **updates: Fields to update (name, notes, due_on, assignee, completed, etc.)

    Returns:
        Updated task dictionary

    Example:
        update_task("12345", name="New name", due_on="2024-01-20")
        update_task("12345", assignee="67890")
        update_task("12345", completed=True)
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    body = {"data": updates}
    return tasks_api.update_task(body, task_gid, opts={})


def complete_task(task_gid: str) -> Dict[str, Any]:
    """
    Mark a task as complete.

    Args:
        task_gid: The task's GID

    Returns:
        Updated task dictionary

    Example:
        complete_task("12345")
    """
    return update_task(task_gid, completed=True)


def uncomplete_task(task_gid: str) -> Dict[str, Any]:
    """
    Mark a task as incomplete.

    Args:
        task_gid: The task's GID

    Returns:
        Updated task dictionary

    Example:
        uncomplete_task("12345")
    """
    return update_task(task_gid, completed=False)


def delete_task(task_gid: str) -> None:
    """
    Delete a task.

    Args:
        task_gid: The task's GID

    Example:
        delete_task("12345")
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())
    tasks_api.delete_task(task_gid)


def add_task_to_project(
    task_gid: str,
    project_gid: str,
    section: str = None,
    insert_before: str = None,
    insert_after: str = None,
) -> None:
    """
    Add a task to a project.

    Args:
        task_gid: The task's GID
        project_gid: The project's GID
        section: Section GID to add to (optional)
        insert_before: Task GID to insert before (optional)
        insert_after: Task GID to insert after (optional)

    Example:
        add_task_to_project("task123", "project456")
        add_task_to_project("task123", "project456", section="section789")
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    data = {"project": project_gid}
    if section:
        data["section"] = section
    if insert_before:
        data["insert_before"] = insert_before
    if insert_after:
        data["insert_after"] = insert_after

    body = {"data": data}
    tasks_api.add_project_for_task(body, task_gid)


def remove_task_from_project(task_gid: str, project_gid: str) -> None:
    """
    Remove a task from a project.

    Args:
        task_gid: The task's GID
        project_gid: The project's GID

    Example:
        remove_task_from_project("task123", "project456")
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    body = {"data": {"project": project_gid}}
    tasks_api.remove_project_for_task(body, task_gid)


def get_subtasks(task_gid: str, opt_fields: List[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Get subtasks of a task.

    Args:
        task_gid: Parent task's GID
        opt_fields: Additional fields to return

    Returns:
        Iterator of subtask dictionaries

    Example:
        subtasks = list(get_subtasks("12345"))
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,completed,due_on,assignee"

    return tasks_api.get_subtasks_for_task(task_gid, opts=opts)


def set_parent(task_gid: str, parent_gid: str, insert_before: str = None, insert_after: str = None) -> Dict[str, Any]:
    """
    Set or change a task's parent (make it a subtask).

    Args:
        task_gid: The task's GID
        parent_gid: New parent task's GID (or None to unparent)
        insert_before: Sibling task GID to insert before
        insert_after: Sibling task GID to insert after

    Returns:
        Updated task dictionary

    Example:
        set_parent("subtask123", "parent456")
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    data = {"parent": parent_gid}
    if insert_before:
        data["insert_before"] = insert_before
    if insert_after:
        data["insert_after"] = insert_after

    body = {"data": data}
    return tasks_api.set_parent_for_task(body, task_gid, opts={})


def search_tasks(
    query: str,
    workspace_gid: str = None,
    resource_subtype: str = "default_task",
    completed: bool = None,
    is_subtask: bool = None,
    assignee: str = None,
    projects: List[str] = None,
    opt_fields: List[str] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Search for tasks in a workspace.

    Args:
        query: Search text
        workspace_gid: Workspace to search (default: current workspace)
        resource_subtype: Task subtype filter ("default_task", "milestone", "section")
        completed: Filter by completion status
        is_subtask: Filter by subtask status
        assignee: Filter by assignee GID
        projects: Filter by project GIDs
        opt_fields: Additional fields to return

    Returns:
        Iterator of matching task dictionaries

    Example:
        results = list(search_tasks("bug fix"))
        results = list(search_tasks("review", completed=False))
    """
    _ensure_workspace()
    tasks_api = asana.TasksApi(get_client())

    if workspace_gid is None:
        workspace_gid = get_workspace_gid()

    opts = {"text": query}

    if resource_subtype:
        opts["resource_subtype"] = resource_subtype
    if completed is not None:
        opts["completed"] = completed
    if is_subtask is not None:
        opts["is_subtask"] = is_subtask
    if assignee:
        opts["assignee.any"] = assignee
    if projects:
        opts["projects.any"] = ",".join(projects)

    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,name,completed,due_on,assignee,projects"

    return tasks_api.search_tasks_for_workspace(workspace_gid, opts=opts)


def get_stories_for_task(
    task_gid: str,
    opt_fields: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get stories (comments + system events) for a task.

    Args:
        task_gid: The task's GID
        opt_fields: Fields to return

    Returns:
        List of story dicts (comments have type "comment")

    Example:
        stories = get_stories_for_task("12345")
        comments = [s for s in stories if s.get("type") == "comment"]
    """
    _ensure_workspace()
    stories_api = asana.StoriesApi(get_client())

    opts = {}
    if opt_fields:
        opts["opt_fields"] = ",".join(opt_fields)
    else:
        opts["opt_fields"] = "gid,type,text,created_by,created_at,resource_subtype"

    return list(stories_api.get_stories_for_task(task_gid, opts=opts))


def add_comment(task_gid: str, text: str) -> Dict[str, Any]:
    """
    Post a comment on a task.

    Args:
        task_gid: The task's GID
        text: Comment text (plain text)

    Returns:
        Created story dict

    Example:
        add_comment("12345", "Looks good, merging now.")
    """
    _ensure_workspace()
    stories_api = asana.StoriesApi(get_client())

    body = {"data": {"text": text}}
    return stories_api.create_story_for_task(body, task_gid, opts={})
