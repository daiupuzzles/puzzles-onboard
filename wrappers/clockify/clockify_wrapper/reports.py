"""
Clockify Wrapper — Reports API.

Uses the Reports API (reports.api.clockify.me) for aggregated time data.
The standard time entries endpoint does NOT support client filtering,
so the Reports API is required for client-scoped summaries.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from ._base import REPORTS_URL, _api_call, _api_call_raw, get_workspace_id

log = logging.getLogger(__name__)


def get_summary_report(
    workspace_id: Optional[str] = None,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    client_ids: Optional[list[str]] = None,
    project_ids: Optional[list[str]] = None,
) -> dict:
    """Get a summary report with hours grouped by project.

    Args:
        workspace_id: Override workspace (default: auto-detected).
        start_date: ISO 8601 start (default: 7 days ago).
        end_date: ISO 8601 end (default: now).
        client_ids: Filter to specific Clockify client IDs.
        project_ids: Filter to specific Clockify project IDs.

    Returns:
        Raw Clockify summary report response.
    """
    ws = workspace_id or get_workspace_id()

    now = datetime.now(timezone.utc)
    if not start_date:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")
    if not end_date:
        end_date = now.strftime("%Y-%m-%dT23:59:59")

    body: dict = {
        "dateRangeStart": start_date,
        "dateRangeEnd": end_date,
        "summaryFilter": {
            "groups": ["PROJECT"],
            "sortColumn": "GROUP",
        },
        "exportType": "JSON",
    }

    if client_ids:
        body["clients"] = {"ids": client_ids, "contains": "CONTAINS", "status": "ALL"}
    if project_ids:
        body["projects"] = {"ids": project_ids, "contains": "CONTAINS", "status": "ALL"}

    return _api_call(
        "POST",
        f"workspaces/{ws}/reports/summary",
        base_url=REPORTS_URL,
        json_data=body,
    )


def get_client_summary(
    client_id: str,
    workspace_id: Optional[str] = None,
    *,
    days: int = 7,
) -> dict:
    """Get a parsed time summary for a single client.

    Convenience wrapper around get_summary_report() that returns
    a clean dict suitable for comms hub briefing enrichment.

    Args:
        client_id: Clockify client ID.
        workspace_id: Override workspace.
        days: Number of days to look back (default: 7).

    Returns:
        {
            "total_seconds": int,
            "total_hours": float,
            "projects": [{"name": str, "hours": float, "seconds": int}],
            "period_days": int,
        }
    """
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    end = now.strftime("%Y-%m-%dT23:59:59")

    raw = get_summary_report(
        workspace_id, start_date=start, end_date=end, client_ids=[client_id]
    )

    # Parse the summary response
    total_seconds = 0
    projects = []

    # Clockify summary groups data under "groupOne" (first GROUP level)
    for group in raw.get("groupOne", []):
        project_name = group.get("name", "Unknown")
        duration = group.get("duration", 0)  # seconds
        total_seconds += duration
        projects.append({
            "name": project_name,
            "hours": round(duration / 3600, 2),
            "seconds": duration,
        })

    return {
        "total_seconds": total_seconds,
        "total_hours": round(total_seconds / 3600, 2),
        "projects": projects,
        "period_days": days,
    }


def get_detailed_report(
    workspace_id: Optional[str] = None,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    client_ids: Optional[list[str]] = None,
    page_size: int = 1000,
) -> list[dict]:
    """Get a detailed report with individual time entries.

    Handles pagination automatically — loops until all entries are fetched.

    Args:
        workspace_id: Override workspace (default: auto-detected).
        start_date: ISO 8601 start (default: 7 days ago).
        end_date: ISO 8601 end (default: now).
        client_ids: Filter to specific Clockify client IDs.
        page_size: Entries per page (max 1000).

    Returns:
        List of all time entry dicts with keys like taskName, description,
        clientId, timeInterval (containing duration, start, end), etc.
    """
    ws = workspace_id or get_workspace_id()

    now = datetime.now(timezone.utc)
    if not start_date:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")
    if not end_date:
        end_date = now.strftime("%Y-%m-%dT23:59:59")

    all_entries: list[dict] = []
    page = 1

    while True:
        body: dict = {
            "dateRangeStart": start_date,
            "dateRangeEnd": end_date,
            "detailedFilter": {
                "page": page,
                "pageSize": page_size,
                "options": {"totals": "CALCULATE"},
            },
        }

        if client_ids:
            body["clients"] = {"ids": client_ids, "contains": "CONTAINS", "status": "ALL"}

        resp = _api_call(
            "POST",
            f"workspaces/{ws}/reports/detailed",
            base_url=REPORTS_URL,
            json_data=body,
        )

        entries = resp.get("timeentries", [])
        all_entries.extend(entries)

        # Check if we've fetched all entries
        if not entries or len(entries) < page_size:
            break

        page += 1
        log.debug("Fetching page %d of detailed report", page)

    log.info("Fetched %d detailed time entries", len(all_entries))
    return all_entries


def export_report_pdf(
    workspace_id: Optional[str] = None,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    client_ids: Optional[list[str]] = None,
) -> bytes:
    """Export a detailed report as PDF.

    Args:
        workspace_id: Override workspace (default: auto-detected).
        start_date: ISO 8601 start (default: 7 days ago).
        end_date: ISO 8601 end (default: now).
        client_ids: Filter to specific Clockify client IDs.

    Returns:
        Raw PDF bytes.
    """
    ws = workspace_id or get_workspace_id()

    now = datetime.now(timezone.utc)
    if not start_date:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00")
    if not end_date:
        end_date = now.strftime("%Y-%m-%dT23:59:59")

    body: dict = {
        "dateRangeStart": start_date,
        "dateRangeEnd": end_date,
        "detailedFilter": {
            "page": 1,
            "pageSize": 1000,
            "options": {"totals": "CALCULATE"},
        },
        "exportType": "PDF",
    }

    if client_ids:
        body["clients"] = {"ids": client_ids, "contains": "CONTAINS", "status": "ALL"}

    return _api_call_raw(
        "POST",
        f"workspaces/{ws}/reports/detailed",
        base_url=REPORTS_URL,
        json_data=body,
    )
