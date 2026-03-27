"""
Jira Wrapper - Multi-instance Jira Cloud REST API.

Provides a clean interface to Jira's REST API v3 with instance context management.
Uses `use_jira(slug)` pattern for switching between Jira instances.
Use `configure()` for non-default instances with separate credentials.

Setup:
    Requires in Doppler (flowsly/prd):
    - JIRA_EMAIL: Atlassian account email
    - JIRA_API_TOKEN: API token from id.atlassian.com
    - JIRA_USER_ID: Account ID for JQL queries
    - JIRA_*_BASE_URL: Base URL per instance (e.g., JIRA_COLUMBUS_BASE_URL)

Usage:
    from jira_wrapper import use_jira, get_assigned_issues, search_issues

    use_jira("columbus")  # Set context once

    # Get assigned issues
    issues = get_assigned_issues(max_results=10)

    # Search with JQL
    issues = search_issues('project = KAN AND status = "In Progress"')

    # Get issue details
    issue = get_issue("KAN-123")

    # Add comment
    add_comment("KAN-123", "Working on this now")

    # --- New: Puzzles instance with configure() ---
    from jira_wrapper import configure, create_jpd_project, copy_issues_from_project

    configure(
        email="services@puzzles.consulting",
        api_token="...",
        base_url="https://puzzlesconsulting.atlassian.net",
    )

    project = create_jpd_project("AC", "Acme Corp")  # uses undocumented simplified endpoint
    copy_issues_from_project("PU", "AC")
"""

from ._base import (
    # Instance management
    use_jira,
    get_current_instance,
    get_base_url,
    add_instance,
    configure,
    # Issue operations
    get_assigned_issues,
    get_issue,
    search_issues,
    get_mentions,
    # Issue creation
    create_issue,
    bulk_create_issues,
    copy_issues_from_project,
    # Comment operations
    get_issue_comments,
    add_comment,
    # Project operations
    get_project,
    search_projects,
    find_project_by_key,
    create_project,
    create_jpd_project,
    delete_project,
    get_project_issue_types,
    # Helpers
    text_to_adf,
    adf_to_text,
)

__all__ = [
    # Instance management
    "use_jira",
    "get_current_instance",
    "get_base_url",
    "add_instance",
    "configure",
    # Issue operations
    "get_assigned_issues",
    "get_issue",
    "search_issues",
    "get_mentions",
    # Issue creation
    "create_issue",
    "bulk_create_issues",
    "copy_issues_from_project",
    # Comment operations
    "get_issue_comments",
    "add_comment",
    # Project operations
    "get_project",
    "search_projects",
    "find_project_by_key",
    "create_project",
    "create_jpd_project",
    "delete_project",
    "get_project_issue_types",
    # Helpers
    "text_to_adf",
    "adf_to_text",
]
