"""
Asana Wrapper - Natural language task management via Claude Code.

Provides a clean interface to Asana's API with workspace context management.
Designed to be called by Claude Code for natural language task operations.

Setup:
    Requires ASANA_API_KEY in Doppler (puzzles/prd)

Usage:
    from asana_wrapper import use_workspace, get_tasks, create_task

    use_workspace("puzzles")  # Set context once

    # Task operations
    tasks = list(get_tasks(assignee="me"))
    task = create_task("Review PR", due_on="2024-01-15")
    complete_task(task["gid"])

    # Project operations
    projects = list(get_projects())
    dev = find_project_by_name("Development")

    # User operations
    me = get_me()
    user = find_user_by_name("John")

    # Search/typeahead
    results = typeahead("bug", resource_type="task")
    results = search_tasks("review")
"""

# Client management
from .asana_client import (
    use_workspace,
    get_client,
    get_workspace_gid,
    get_current_workspace,
    close_all,
)

# Task operations
from .tasks import (
    get_tasks,
    get_task,
    create_task,
    update_task,
    complete_task,
    uncomplete_task,
    delete_task,
    add_task_to_project,
    remove_task_from_project,
    get_subtasks,
    set_parent,
    search_tasks,
    get_stories_for_task,
    add_comment,
)

# Project operations
from .projects import (
    get_projects,
    get_project,
    find_project_by_name,
    get_sections,
    get_section,
    find_section_by_name,
    get_teams,
)

# User operations
from .users import (
    get_me,
    get_user,
    get_users,
    find_user_by_name,
    find_user_by_email,
)

# Search operations
from .search import (
    typeahead,
    quick_find_task,
    quick_find_project,
    quick_find_user,
)

# Subtask inheritance
from .subtask_inheritance import (
    find_ancestor_with_projects,
    process_subtask_inheritance,
    process_subtasks_batch,
)

__all__ = [
    # Client
    "use_workspace",
    "get_client",
    "get_workspace_gid",
    "get_current_workspace",
    "close_all",
    # Tasks
    "get_tasks",
    "get_task",
    "create_task",
    "update_task",
    "complete_task",
    "uncomplete_task",
    "delete_task",
    "add_task_to_project",
    "remove_task_from_project",
    "get_subtasks",
    "set_parent",
    "search_tasks",
    "get_stories_for_task",
    "add_comment",
    # Projects
    "get_projects",
    "get_project",
    "find_project_by_name",
    "get_sections",
    "get_section",
    "find_section_by_name",
    "get_teams",
    # Users
    "get_me",
    "get_user",
    "get_users",
    "find_user_by_name",
    "find_user_by_email",
    # Search
    "typeahead",
    "quick_find_task",
    "quick_find_project",
    "quick_find_user",
    # Subtask Inheritance
    "find_ancestor_with_projects",
    "process_subtask_inheritance",
    "process_subtasks_batch",
]
