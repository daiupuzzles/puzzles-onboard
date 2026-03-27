"""
Asana Subtask Project Inheritance.

Automatically adds subtasks to their parent's project(s) and assigns
the parent's assignee.

Usage:
    from asana_wrapper import use_workspace, process_subtask_inheritance

    use_workspace("puzzles")
    result = process_subtask_inheritance("subtask_gid", "parent_gid")

Batch:
    from asana_wrapper import process_subtasks_batch

    use_workspace("puzzles")
    results = process_subtasks_batch([
        ["subtask_gid1", "parent_gid1"],
        ["subtask_gid2", "parent_gid2"],
    ])
"""

from typing import Dict, Any, List, Optional, Tuple

from .asana_client import get_client, _ensure_workspace
from .tasks import get_task, add_task_to_project, update_task

# Constants
MAX_PARENT_LEVELS = 4


def find_ancestor_with_projects(
    task_gid: str,
    max_levels: int = MAX_PARENT_LEVELS,
) -> Tuple[Optional[Dict[str, Any]], int]:
    """
    Walk up the parent chain to find an ancestor with projects.

    Args:
        task_gid: Starting task GID (typically the parent of a new subtask)
        max_levels: Maximum levels to traverse (default: 4)

    Returns:
        Tuple of (ancestor_info, levels_checked)
        ancestor_info contains {"projects": [...], "assignee": {...}} or None

    Example:
        ancestor, levels = find_ancestor_with_projects("parent123")
        if ancestor:
            print(f"Found projects at level {levels}")
    """
    _ensure_workspace()

    current_gid = task_gid
    opt_fields = ["projects.gid", "projects.name", "parent.gid", "assignee.gid", "assignee.name"]

    for level in range(max_levels):
        task = get_task(current_gid, opt_fields=opt_fields)

        # Check if this task has projects
        projects = task.get("projects", [])
        if projects:
            return (
                {
                    "gid": current_gid,
                    "projects": projects,
                    "assignee": task.get("assignee"),
                },
                level + 1,
            )

        # Check for parent
        parent = task.get("parent")
        if not parent or not parent.get("gid"):
            # No more parents
            return None, level + 1

        current_gid = parent["gid"]

    return None, max_levels


def process_subtask_inheritance(
    subtask_gid: str,
    parent_gid: str,
    assign_to_parent_assignee: bool = True,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Process a single subtask: add to parent's project(s) and optionally assign.

    This is the main entry point for subtask inheritance processing.
    It walks up the parent chain to find an ancestor with projects,
    then adds the subtask to those projects and optionally assigns
    the ancestor's assignee.

    Args:
        subtask_gid: The subtask's GID
        parent_gid: The direct parent task's GID
        assign_to_parent_assignee: Also assign subtask to parent's assignee (default: True)
        verbose: Print progress messages (default: True)

    Returns:
        Result dictionary with status, actions taken, and any errors

    Example:
        result = process_subtask_inheritance("subtask123", "parent456")
        if result["status"] == "success":
            print(f"Added to: {result['projects_added']}")
    """
    _ensure_workspace()

    if verbose:
        print(f"[{subtask_gid}] Processing subtask (parent: {parent_gid})")

    # Find ancestor with projects
    ancestor, levels = find_ancestor_with_projects(parent_gid)

    if not ancestor:
        if verbose:
            print(f"[{subtask_gid}] SKIP: No ancestor with projects (checked {levels} levels)")
        return {
            "subtask_gid": subtask_gid,
            "status": "skipped",
            "reason": "No ancestor with projects",
            "levels_checked": levels,
        }

    projects = ancestor["projects"]
    assignee = ancestor.get("assignee")

    if verbose:
        project_names = [p.get("name", p["gid"]) for p in projects]
        print(f"[{subtask_gid}] Found ancestor at L{levels} with projects: {project_names}")

    # Track results
    actions = []
    errors = []

    # Add to all projects
    for project in projects:
        project_name = project.get("name", project["gid"])
        try:
            add_task_to_project(subtask_gid, project["gid"])
            actions.append(f"added to '{project_name}'")
        except Exception as e:
            errors.append(f"Failed to add to '{project_name}': {str(e)}")

    # Assign if requested and ancestor has assignee
    assignee_name = None
    if assign_to_parent_assignee and assignee and assignee.get("gid"):
        assignee_name = assignee.get("name", assignee["gid"])
        try:
            update_task(subtask_gid, assignee=assignee["gid"])
            actions.append(f"assigned to {assignee_name}")
        except Exception as e:
            errors.append(f"Failed to assign to {assignee_name}: {str(e)}")

    # Determine status
    if errors:
        status = "partial" if actions else "error"
        if verbose:
            print(f"[{subtask_gid}] {status.upper()}: {len(actions)} actions, {len(errors)} errors")
    else:
        status = "success"
        if verbose:
            print(f"[{subtask_gid}] OK: {', '.join(actions)}")

    return {
        "subtask_gid": subtask_gid,
        "status": status,
        "projects_added": [p.get("name", p["gid"]) for p in projects],
        "assignee_set": assignee_name,
        "actions": actions,
        "errors": errors if errors else None,
        "levels_checked": levels,
    }


def process_subtasks_batch(
    subtasks: List[List[str]],
    assign_to_parent_assignee: bool = True,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Process multiple subtasks.

    Args:
        subtasks: List of [subtask_gid, parent_gid] pairs
        assign_to_parent_assignee: Also assign subtasks to parent's assignee
        verbose: Print progress messages

    Returns:
        Batch result with summary and individual results

    Example:
        results = process_subtasks_batch([
            ["subtask1", "parent1"],
            ["subtask2", "parent2"],
        ])
        print(f"Success: {results['summary']['success']}")
    """
    _ensure_workspace()

    if verbose:
        print(f"BATCH: Processing {len(subtasks)} subtasks")

    if not subtasks:
        if verbose:
            print("BATCH: No subtasks to process")
        return {"status": "ok", "processed": 0, "summary": {}, "results": []}

    results = []
    for subtask_gid, parent_gid in subtasks:
        result = process_subtask_inheritance(
            subtask_gid,
            parent_gid,
            assign_to_parent_assignee=assign_to_parent_assignee,
            verbose=verbose,
        )
        results.append(result)

    # Summary
    success = sum(1 for r in results if r.get("status") == "success")
    partial = sum(1 for r in results if r.get("status") == "partial")
    skipped = sum(1 for r in results if r.get("status") == "skipped")
    error = sum(1 for r in results if r.get("status") == "error")

    if verbose:
        print(f"DONE: {success} success, {partial} partial, {skipped} skipped, {error} error")

    return {
        "status": "ok",
        "processed": len(results),
        "summary": {
            "success": success,
            "partial": partial,
            "skipped": skipped,
            "error": error,
        },
        "results": results,
    }
