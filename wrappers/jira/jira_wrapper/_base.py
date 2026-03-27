"""
Jira Cloud REST API wrapper — multi-instance, basic auth.

Supports multiple Jira Cloud instances accessed with the same Atlassian account.
Uses `use_jira(slug)` pattern for instance switching.

Instance configuration is loaded from Doppler (flowsly/prd):
- JIRA_EMAIL: Atlassian account email
- JIRA_API_TOKEN: API token from id.atlassian.com
- JIRA_USER_ID: Account ID for JQL queries
- JIRA_{SLUG}_BASE_URL: Base URL per instance
"""
from __future__ import annotations

import base64
import logging
import subprocess
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Doppler secrets loader
# ---------------------------------------------------------------------------

_secrets_cache: dict | None = None

_DOPPLER_KEYS = [
    "JIRA_EMAIL",
    "JIRA_API_TOKEN",
    "JIRA_USER_ID",
    "JIRA_COLUMBUS_BASE_URL",
    "JIRA_GOLDEN_BASE_URL",
]


def _load_secrets() -> dict:
    """Load Jira secrets from Doppler (flowsly/prd)."""
    global _secrets_cache
    if _secrets_cache is not None:
        return _secrets_cache

    secrets = {}
    for key in _DOPPLER_KEYS:
        try:
            result = subprocess.run(
                ["doppler", "secrets", "get", key, "--project", "flowsly", "--config", "prd", "--plain"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                secrets[key] = result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            log.warning("Failed to fetch Doppler secret: %s", key)

    _secrets_cache = secrets
    return secrets


# ---------------------------------------------------------------------------
# Instance registry
# ---------------------------------------------------------------------------

_INSTANCES = {
    "columbus": {"url_key": "JIRA_COLUMBUS_BASE_URL", "projects": ["KAN"]},
    "golden": {"url_key": "JIRA_GOLDEN_BASE_URL", "projects": ["KAN"]},
}

_current_instance: Optional[str] = None
_base_url: Optional[str] = None
_auth_header: Optional[str] = None
_user_id: Optional[str] = None

# Rate-limit / retry config
_MAX_RETRIES = 3
_RETRY_BACKOFF = 2  # seconds, doubles each retry
_PAGE_SIZE = 50  # Jira default/max per page


# ---------------------------------------------------------------------------
# Instance management
# ---------------------------------------------------------------------------

def use_jira(slug: str) -> None:
    """Switch Jira instance context.

    Args:
        slug: Instance slug (e.g., "columbus", "golden")

    Raises:
        ValueError: If slug is not in registry
        RuntimeError: If required secrets are missing
    """
    global _current_instance, _base_url, _auth_header, _user_id

    if slug not in _INSTANCES:
        available = ", ".join(_INSTANCES.keys())
        raise ValueError(f"Unknown instance '{slug}'. Available: {available}")

    if _current_instance == slug and _base_url is not None:
        return

    secrets = _load_secrets()

    cfg = _INSTANCES[slug]
    _base_url = secrets.get(cfg["url_key"], "").rstrip("/")
    if not _base_url:
        raise RuntimeError(f"Missing Doppler secret: {cfg['url_key']}")

    email = secrets.get("JIRA_EMAIL", "")
    token = secrets.get("JIRA_API_TOKEN", "")
    _user_id = secrets.get("JIRA_USER_ID", "")

    if not email or not token:
        raise RuntimeError("Missing JIRA_EMAIL or JIRA_API_TOKEN in Doppler")

    encoded = base64.b64encode(f"{email}:{token}".encode()).decode()
    _auth_header = f"Basic {encoded}"
    _current_instance = slug

    log.info("Jira: using instance '%s' (%s)", slug, _base_url)


def get_current_instance() -> Optional[str]:
    """Return current instance slug."""
    return _current_instance


def get_base_url() -> Optional[str]:
    """Return current instance base URL."""
    return _base_url


def add_instance(slug: str, url_key: str, projects: list[str]) -> None:
    """Register a new Jira instance at runtime.

    Args:
        slug: Instance slug for use_jira()
        url_key: Doppler secret key for base URL
        projects: List of project keys for this instance
    """
    _INSTANCES[slug] = {"url_key": url_key, "projects": projects}


def configure(
    *,
    email: Optional[str] = None,
    api_token: Optional[str] = None,
    base_url: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    """Explicitly set Jira credentials without Doppler lookup.

    Use this for non-default Jira instances where credentials are in a
    different Doppler project (e.g., Puzzles in puzzles/prd).

    Args:
        email: Atlassian account email.
        api_token: API token from id.atlassian.com.
        base_url: Jira instance URL (e.g., https://puzzlesconsulting.atlassian.net).
        user_id: Atlassian account ID (optional, needed for assignee queries).
    """
    global _base_url, _auth_header, _user_id, _current_instance

    if bool(email) != bool(api_token):
        raise ValueError("configure() requires both email and api_token, or neither")

    if email and api_token:
        encoded = base64.b64encode(f"{email}:{api_token}".encode()).decode()
        _auth_header = f"Basic {encoded}"
        log.info("Jira auth set explicitly for %s", email)

    if base_url:
        _base_url = base_url.rstrip("/")
        log.info("Jira base URL set: %s", _base_url)

    if user_id is not None:
        _user_id = user_id

    _current_instance = "configured"


def _ensure_instance() -> None:
    """Raise if no instance selected."""
    if _current_instance is None or _base_url is None:
        raise RuntimeError("No Jira instance selected. Call use_jira() first.")


# ---------------------------------------------------------------------------
# Internal API call
# ---------------------------------------------------------------------------

def _api_call(
    method: str,
    path: str,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
) -> dict:
    """Make authenticated Jira REST API v3 call with retry on rate-limit."""
    _ensure_instance()

    url = f"{_base_url}/rest/api/3/{path.lstrip('/')}"
    headers = {
        "Authorization": _auth_header,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    last_error = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.request(
                method, url, headers=headers, params=params, json=json_data, timeout=30
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            last_error = e
            wait = _RETRY_BACKOFF * (2 ** attempt)
            log.warning("Network error, retrying in %ds (attempt %d/%d): %s", wait, attempt + 1, _MAX_RETRIES, e)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = _RETRY_BACKOFF * (2 ** attempt)
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    wait = max(wait, int(retry_after))
                except ValueError:
                    pass  # HTTP-date format — use computed backoff
            log.warning("Rate limited, retrying in %ds (attempt %d/%d)", wait, attempt + 1, _MAX_RETRIES)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()

    if last_error:
        raise RuntimeError(f"Jira API failed after {_MAX_RETRIES} retries: {method} {path}: {last_error}") from last_error
    raise RuntimeError(f"Jira API rate-limited after {_MAX_RETRIES} retries: {method} {path}")


# ---------------------------------------------------------------------------
# ADF helpers (public)
# ---------------------------------------------------------------------------

def text_to_adf(text: str) -> dict:
    """Wrap plain text in Atlassian Document Format."""
    return {
        "version": 1,
        "type": "doc",
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": text}]}
        ],
    }


def adf_to_text(adf: Optional[dict]) -> str:
    """Extract plain text from ADF document."""
    if not adf or not isinstance(adf, dict):
        return ""
    parts = []
    for block in adf.get("content", []):
        for inline in block.get("content", []):
            if inline.get("type") == "text":
                parts.append(inline.get("text", ""))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Issue parsing
# ---------------------------------------------------------------------------

def _parse_issue(raw: dict) -> dict:
    """Parse raw Jira issue into a clean dict."""
    fields = raw.get("fields", {})

    assignee = fields.get("assignee") or {}
    reporter = fields.get("reporter") or {}
    status = fields.get("status") or {}
    priority = fields.get("priority") or {}

    key = raw.get("key", "")
    return {
        "key": key,
        "summary": fields.get("summary", ""),
        "status": status.get("name", ""),
        "assignee": assignee.get("displayName", ""),
        "assignee_account_id": assignee.get("accountId", ""),
        "reporter": reporter.get("displayName", ""),
        "priority": priority.get("name", ""),
        "description": adf_to_text(fields.get("description")),
        "due_date": fields.get("duedate"),
        "updated": fields.get("updated", ""),
        "created": fields.get("created", ""),
        "labels": fields.get("labels", []),
        "comments_count": (fields.get("comment") or {}).get("total", 0),
        "url": f"{_base_url}/browse/{key}",
        "raw_fields": fields,
    }


def _parse_comment(raw: dict) -> dict:
    """Parse raw Jira comment into a clean dict."""
    author = raw.get("author") or {}
    return {
        "id": raw.get("id", ""),
        "author": author.get("displayName", ""),
        "author_account_id": author.get("accountId", ""),
        "body": adf_to_text(raw.get("body")),
        "created": raw.get("created", ""),
        "updated": raw.get("updated", ""),
    }


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

def _paginate_jql(params: dict, max_results: int = 200) -> list[dict]:
    """Fetch all pages from Jira JQL search endpoint.

    Uses POST /rest/api/3/search/jql with nextPageToken pagination.
    The old /search endpoint was deprecated Oct 2025.
    """
    all_issues = []
    next_page_token = None

    # Convert fields from comma-string to list if needed
    fields = params.get("fields", "")
    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(",") if f.strip()]
    if not fields:
        fields = ["*navigable"]

    while len(all_issues) < max_results:
        body = {
            "jql": params.get("jql", ""),
            "fields": fields,
            "maxResults": min(_PAGE_SIZE, max_results - len(all_issues)),
        }
        if next_page_token:
            body["nextPageToken"] = next_page_token

        data = _api_call("POST", "search/jql", json_data=body)
        issues = data.get("issues", [])
        all_issues.extend(issues)

        next_page_token = data.get("nextPageToken")
        if not issues or not next_page_token:
            break

    return all_issues


def _paginate_comments(issue_key: str, params: Optional[dict] = None) -> list[dict]:
    """Fetch all comment pages for a Jira issue."""
    params = dict(params or {})
    all_comments = []
    start_at = 0
    while True:
        params["startAt"] = start_at
        params["maxResults"] = _PAGE_SIZE
        data = _api_call("GET", f"issue/{issue_key}/comment", params=params)
        comments = data.get("comments", [])
        all_comments.extend(comments)
        total = data.get("total", len(comments))
        if not comments or len(all_comments) >= total:
            break
        start_at += len(comments)
    return all_comments


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def get_assigned_issues(
    project_keys: Optional[list] = None,
    updated_since: Optional[str] = None,
    max_results: int = 50,
    account: Optional[str] = None,
) -> list[dict]:
    """Get issues assigned to the current user.

    Args:
        project_keys: Project keys to filter (default: instance's projects)
        updated_since: ISO date string for updated filter
        max_results: Maximum issues to return
        account: Instance slug (switches context if provided)

    Returns:
        List of parsed issue dicts
    """
    if account:
        use_jira(account)
    _ensure_instance()

    keys = project_keys
    if not keys:
        inst = _INSTANCES.get(_current_instance)
        if not inst:
            raise ValueError("No project_keys provided and current instance has no default projects. Pass project_keys explicitly.")
        keys = inst["projects"]
    projects_str = ", ".join(keys)

    jql = f'assignee = "{_user_id}" AND project IN ({projects_str})'
    if updated_since:
        jql += f' AND updated >= "{updated_since}"'
    jql += " ORDER BY updated DESC"

    raw_issues = _paginate_jql({
        "jql": jql,
        "fields": "summary,status,assignee,reporter,priority,description,updated,created,labels,comment,duedate",
    }, max_results=max_results)

    return [_parse_issue(i) for i in raw_issues]


def get_issue(issue_key: str, account: Optional[str] = None) -> dict:
    """Get full issue detail.

    Args:
        issue_key: Issue key (e.g., "CTMT-123")
        account: Instance slug (switches context if provided)

    Returns:
        Parsed issue dict
    """
    if account:
        use_jira(account)
    _ensure_instance()

    raw = _api_call("GET", f"issue/{issue_key}", params={
        "fields": "summary,status,assignee,reporter,priority,description,updated,created,labels,comment,duedate",
    })
    return _parse_issue(raw)


def get_issue_comments(
    issue_key: str,
    since: Optional[str] = None,
    account: Optional[str] = None,
) -> list[dict]:
    """Get comments on an issue.

    Args:
        issue_key: Issue key (e.g., "CTMT-123")
        since: ISO timestamp to filter comments after
        account: Instance slug (switches context if provided)

    Returns:
        List of parsed comment dicts
    """
    if account:
        use_jira(account)
    _ensure_instance()

    raw_comments = _paginate_comments(issue_key, params={"orderBy": "-created"})

    comments = [_parse_comment(c) for c in raw_comments]

    if since:
        if hasattr(since, 'isoformat'):
            since = since.isoformat()
        comments = [c for c in comments if c["created"] >= since]

    return comments


def add_comment(
    issue_key: str,
    body: str,
    account: Optional[str] = None,
) -> dict:
    """Post a plain-text comment (auto-wrapped in ADF).

    Args:
        issue_key: Issue key (e.g., "CTMT-123")
        body: Comment text (plain text, will be wrapped in ADF)
        account: Instance slug (switches context if provided)

    Returns:
        Parsed comment dict
    """
    if account:
        use_jira(account)
    _ensure_instance()

    payload = {"body": text_to_adf(body)}
    raw = _api_call("POST", f"issue/{issue_key}/comment", json_data=payload)
    return _parse_comment(raw)


def search_issues(
    jql: str,
    max_results: int = 50,
    fields: Optional[list] = None,
    account: Optional[str] = None,
) -> list[dict]:
    """Run a JQL search.

    Args:
        jql: JQL query string
        max_results: Maximum issues to return
        fields: List of fields to retrieve
        account: Instance slug (switches context if provided)

    Returns:
        List of parsed issue dicts
    """
    if account:
        use_jira(account)
    _ensure_instance()

    field_str = ",".join(fields) if fields else "summary,status,assignee,reporter,priority,description,updated,created,labels,comment,duedate"

    raw_issues = _paginate_jql({
        "jql": jql,
        "fields": field_str,
    }, max_results=max_results)

    return [_parse_issue(i) for i in raw_issues]


def get_mentions(
    since: Optional[str] = None,
    account: Optional[str] = None,
) -> list[dict]:
    """Get issues/comments mentioning current user.

    Args:
        since: ISO date string to filter by update time
        account: Instance slug (switches context if provided)

    Returns:
        List of parsed issue dicts
    """
    if account:
        use_jira(account)
    _ensure_instance()

    inst = _INSTANCES.get(_current_instance)
    if not inst:
        raise ValueError("get_mentions() requires a pre-configured instance with default projects. Use use_jira() or pass project_keys to search_issues() instead.")
    keys = inst["projects"]
    projects_str = ", ".join(keys)

    jql = f'project IN ({projects_str}) AND text ~ "{_user_id}"'
    if since:
        jql += f' AND updated >= "{since}"'
    jql += " ORDER BY updated DESC"

    raw_issues = _paginate_jql({
        "jql": jql,
        "fields": "summary,status,assignee,reporter,priority,description,updated,created,labels,comment,duedate",
    })

    return [_parse_issue(i) for i in raw_issues]


# ---------------------------------------------------------------------------
# Project operations
# ---------------------------------------------------------------------------

def get_project(
    project_id_or_key: str,
    account: Optional[str] = None,
) -> dict:
    """Get a project by ID or key.

    Args:
        project_id_or_key: Project ID (numeric) or key (e.g., "ACC").
        account: Instance slug (switches context if provided).

    Returns:
        Project dict with id, key, name, projectTypeKey, etc.
    """
    if account:
        use_jira(account)
    _ensure_instance()
    return _api_call("GET", f"project/{project_id_or_key}")


def search_projects(
    query: Optional[str] = None,
    type_key: Optional[str] = None,
    max_results: int = 50,
    account: Optional[str] = None,
) -> list[dict]:
    """Search/list projects.

    Args:
        query: Text filter on project key/name (case-insensitive).
        type_key: Filter by project type (e.g., "product_discovery", "software").
        max_results: Maximum projects to return (max 100).
        account: Instance slug (switches context if provided).

    Returns:
        List of project dicts.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    params: dict = {"maxResults": min(max_results, 100)}
    if query:
        params["query"] = query
    if type_key:
        params["typeKey"] = type_key

    data = _api_call("GET", "project/search", params=params)
    return data.get("values", [])


def find_project_by_key(
    key: str,
    account: Optional[str] = None,
) -> dict | None:
    """Check if a project exists by key.

    Args:
        key: Project key (e.g., "ACC").
        account: Instance slug (switches context if provided).

    Returns:
        Project dict if found, None if not.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    try:
        return _api_call("GET", f"project/{key}")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        raise


def create_project(
    key: str,
    name: str,
    *,
    project_type_key: str = "business",
    project_template_key: Optional[str] = None,
    lead_account_id: Optional[str] = None,
    description: Optional[str] = None,
    assignee_type: str = "UNASSIGNED",
    account: Optional[str] = None,
) -> dict:
    """Create a new Jira project.

    Args:
        key: Unique project key (uppercase letters + numbers, starts with letter).
        name: Project display name.
        project_type_key: "business", "software", "service_desk", or "product_discovery"
            (product_discovery may require Enterprise — will attempt and surface any error).
        project_template_key: Template key (must match project_type_key). If omitted,
            Jira uses the default template for the type.
        lead_account_id: Account ID of project lead. If omitted, the authenticated
            user is used.
        description: Project description.
        assignee_type: "PROJECT_LEAD" or "UNASSIGNED".
        account: Instance slug (switches context if provided).

    Returns:
        Dict with id, key, self (URL).
    """
    if account:
        use_jira(account)
    _ensure_instance()

    body: dict = {
        "key": key,
        "name": name,
        "projectTypeKey": project_type_key,
        "assigneeType": assignee_type,
    }
    if project_template_key:
        body["projectTemplateKey"] = project_template_key
    if lead_account_id:
        body["leadAccountId"] = lead_account_id
    if description:
        body["description"] = description

    result = _api_call("POST", "project", json_data=body)
    log.info("Created Jira project: %s (key=%s, id=%s)", name, key, result.get("id"))
    return result


def create_jpd_project(
    key: str,
    name: str,
    *,
    access_level: str = "PRIVATE",
    account: Optional[str] = None,
) -> dict:
    """Create a Jira Product Discovery (JPD) project.

    Uses the undocumented ``/rest/simplified/latest/project`` endpoint
    (the same one the Jira UI calls). The standard ``/rest/api/3/project``
    endpoint does NOT support ``product_discovery`` project types.

    Args:
        key: Project key (2-10 uppercase alphanumeric, starting with a letter).
        name: Display name.
        access_level: "PRIVATE" (default) or other values (undocumented).
        account: Instance slug (switches context if provided).

    Returns:
        Dict with projectId, projectKey, projectName, returnUrl, simplified.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    url = f"{_base_url}/rest/simplified/latest/project"
    headers = {
        "Authorization": _auth_header,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = {
        "key": key,
        "templateKey": "jira.polaris:discovery",
        "name": name,
        "accessLevel": access_level,
    }

    # Retry with backoff (same pattern as _api_call)
    last_error = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=30)
        except (requests.ConnectionError, requests.Timeout) as e:
            last_error = e
            wait = _RETRY_BACKOFF * (2 ** attempt)
            log.warning("JPD create: network error, retrying in %ds (attempt %d/%d): %s",
                        wait, attempt + 1, _MAX_RETRIES, e)
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = _RETRY_BACKOFF * (2 ** attempt)
            log.warning("JPD create: rate limited, retrying in %ds (attempt %d/%d)",
                        wait, attempt + 1, _MAX_RETRIES)
            time.sleep(wait)
            continue

        resp.raise_for_status()
        result = resp.json()
        break
    else:
        if last_error:
            raise RuntimeError(f"JPD create failed after {_MAX_RETRIES} retries: {last_error}") from last_error
        raise RuntimeError(f"JPD create rate-limited after {_MAX_RETRIES} retries")
    log.info("Created JPD project: %s (key=%s, id=%s)", name, key, result.get("projectId"))
    return result


def delete_project(
    project_id_or_key: str,
    account: Optional[str] = None,
) -> dict:
    """Delete a project permanently.

    Args:
        project_id_or_key: Project ID or key.
        account: Instance slug (switches context if provided).

    Returns:
        Empty dict (204 response).
    """
    if account:
        use_jira(account)
    _ensure_instance()

    result = _api_call("DELETE", f"project/{project_id_or_key}")
    log.info("Deleted Jira project: %s", project_id_or_key)
    return result


# ---------------------------------------------------------------------------
# Issue creation
# ---------------------------------------------------------------------------

def create_issue(
    project_key: str,
    issue_type: str,
    summary: str,
    *,
    description: Optional[str] = None,
    labels: Optional[list[str]] = None,
    priority: Optional[str] = None,
    assignee_account_id: Optional[str] = None,
    parent_key: Optional[str] = None,
    extra_fields: Optional[dict] = None,
    account: Optional[str] = None,
) -> dict:
    """Create a single issue.

    Args:
        project_key: Project key (e.g., "ACC").
        issue_type: Issue type name (e.g., "Task", "Bug", "Idea", "Story").
        summary: Issue title.
        description: Plain text description (auto-wrapped in ADF).
        labels: List of label strings.
        priority: Priority name (e.g., "High", "Medium", "Low").
        assignee_account_id: Account ID of assignee.
        parent_key: Parent issue key for subtasks (e.g., "ACC-1").
        extra_fields: Additional fields dict merged into the fields payload.
        account: Instance slug (switches context if provided).

    Returns:
        Dict with id, key, self.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    fields: dict = {
        "project": {"key": project_key},
        "issuetype": {"name": issue_type},
        "summary": summary,
    }

    if description:
        fields["description"] = text_to_adf(description)
    if labels:
        fields["labels"] = labels
    if priority:
        fields["priority"] = {"name": priority}
    if assignee_account_id:
        fields["assignee"] = {"accountId": assignee_account_id}
    if parent_key:
        fields["parent"] = {"key": parent_key}
    if extra_fields:
        fields.update(extra_fields)

    result = _api_call("POST", "issue", json_data={"fields": fields})
    log.info("Created issue: %s in %s", result.get("key"), project_key)
    return result


def bulk_create_issues(
    issues: list[dict],
    account: Optional[str] = None,
) -> dict:
    """Bulk-create issues (max 50 per call).

    Partial success is possible — always check both `issues` and `errors` in the response.

    Args:
        issues: List of issue payloads, each with a "fields" dict. Same schema
            as the body of create_issue, e.g.:
            [{"fields": {"project": {"key": "ACC"}, "issuetype": {"name": "Idea"}, "summary": "..."}}]
        account: Instance slug (switches context if provided).

    Returns:
        Dict with:
        - issues: list of created issues (id, key, self)
        - errors: list of per-item errors (failedElementNumber, status, elementErrors)

    Raises:
        ValueError: If more than 50 issues are passed.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    if len(issues) > 50:
        raise ValueError(f"Jira bulk create supports max 50 issues per call, got {len(issues)}")

    result = _api_call("POST", "issue/bulk", json_data={"issueUpdates": issues})
    created = result.get("issues", [])
    errors = result.get("errors", [])
    log.info("Bulk created %d issues (%d errors)", len(created), len(errors))
    return result


def copy_issues_from_project(
    source_project_key: str,
    target_project_key: str,
    *,
    jql_filter: Optional[str] = None,
    issue_type_override: Optional[str] = None,
    max_issues: int = 200,
    account: Optional[str] = None,
) -> dict:
    """Copy all issues from one project to another.

    Reads issues from the source project via JQL, then bulk-creates them in the
    target project. Returns a mapping of source_key -> target_key.

    Args:
        source_project_key: Source project key (e.g., "PU").
        target_project_key: Target project key (e.g., "ACC").
        jql_filter: Additional JQL to filter source issues (appended to project filter).
        issue_type_override: Override issue type name for all copied issues.
        max_issues: Maximum issues to copy.
        account: Instance slug (switches context if provided).

    Returns:
        Dict with:
        - copied: dict of source_key -> target_key
        - errors: list of error dicts from bulk create
        - total_source: number of source issues found
    """
    if account:
        use_jira(account)
    _ensure_instance()

    # Fetch source issues
    jql = f"project = {source_project_key} ORDER BY created ASC"
    if jql_filter:
        jql = f"project = {source_project_key} AND ({jql_filter}) ORDER BY created ASC"

    source_issues = _paginate_jql({
        "jql": jql,
        "fields": "summary,issuetype,description,labels,priority",
    }, max_results=max_issues)

    if not source_issues:
        log.info("No issues found in source project %s", source_project_key)
        return {"copied": {}, "errors": [], "total_source": 0}

    log.info("Found %d issues in %s to copy to %s", len(source_issues), source_project_key, target_project_key)

    # Build bulk payloads (batches of 50)
    copied: dict[str, str] = {}
    all_errors: list = []

    for batch_start in range(0, len(source_issues), 50):
        batch = source_issues[batch_start:batch_start + 50]
        payloads = []
        batch_keys = []

        for raw_issue in batch:
            fields = raw_issue.get("fields", {})
            src_key = raw_issue.get("key", "")
            batch_keys.append(src_key)

            issue_type_name = issue_type_override or (fields.get("issuetype") or {}).get("name", "Task")

            new_fields: dict = {
                "project": {"key": target_project_key},
                "issuetype": {"name": issue_type_name},
                "summary": fields.get("summary", "Untitled"),
            }

            desc = fields.get("description")
            if desc:
                new_fields["description"] = desc  # Already in ADF from source

            labels = fields.get("labels")
            if labels:
                new_fields["labels"] = labels

            payloads.append({"fields": new_fields})

        # Bulk create this batch
        result = bulk_create_issues(payloads)
        created_issues = result.get("issues", [])
        errors = result.get("errors", [])

        if errors:
            log.warning("Bulk create batch had %d errors out of %d issues", len(errors), len(batch_keys))

        # Map source -> target keys.
        # Jira bulk create returns `failedElementNumber` as a 0-based index
        # into the submitted `issueUpdates` array. Created issues are returned
        # in order, excluding failed items.
        created_idx = 0
        failed_indices = {e.get("failedElementNumber") for e in errors}
        for i, src_key in enumerate(batch_keys):
            if i not in failed_indices and created_idx < len(created_issues):
                copied[src_key] = created_issues[created_idx].get("key", "")
                created_idx += 1

        all_errors.extend(errors)

    log.info("Copied %d/%d issues from %s to %s (%d errors)",
             len(copied), len(source_issues), source_project_key, target_project_key, len(all_errors))

    return {
        "copied": copied,
        "errors": all_errors,
        "total_source": len(source_issues),
    }


def get_project_issue_types(
    project_id_or_key: str,
    account: Optional[str] = None,
) -> list[dict]:
    """Get valid issue types for a project.

    Args:
        project_id_or_key: Project ID or key.
        account: Instance slug (switches context if provided).

    Returns:
        List of issue type dicts with id, name, description, subtask.
    """
    if account:
        use_jira(account)
    _ensure_instance()

    data = _api_call("GET", f"project/{project_id_or_key}")
    return data.get("issueTypes", [])


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    for slug in _INSTANCES:
        print(f"\n--- Testing instance: {slug} ---")
        use_jira(slug)
        issues = get_assigned_issues(max_results=3)
        print(f"Assigned issues: {len(issues)}")
        for iss in issues:
            print(f"  {iss['key']}: {iss['summary']} [{iss['status']}]")
