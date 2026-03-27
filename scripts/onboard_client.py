#!/usr/bin/env /opt/homebrew/bin/python3.14
"""
Puzzles Consulting — Client Onboarding Automation

Creates scaffolding across 6 platforms for a new client:
1. Clockify: Client + Project
2. Asana: Duplicate Brand Template
3. Jira PD: Create project + copy template issues
4. Google Drive: Copy folder tree template
5. Google Sheets: Copy client mastersheet + append to BRAND DISTRIBUTION
6. Zoom: Team chat channel (optional, gated on creds)

Usage:
    python onboard_client.py "Acme Corp" AC
    python onboard_client.py "Acme Corp" AC --dry-run
    python onboard_client.py "Acme Corp" AC --resume
    python onboard_client.py "Acme Corp" AC --service-groups seo,dev
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("onboard")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CLOCKIFY_WORKSPACE_ID = "6040f154f30cf74c8f545e25"

ASANA_WORKSPACE_GID = "551909527247946"
ASANA_TEMPLATE_GID = "1205571251918318"
ASANA_TEAM_GID = "1177712511213897"  # Client Services

JIRA_TEMPLATE_PROJECT_KEY = "PU"

DRIVE_TEMPLATE_FOLDER_ID = "1RCjDQen6ap_Dkxpy_sNQOGusWERQX5k_"
DRIVE_CLIENTS_FOLDER_ID = "1dbnOly1BTBTOWowk9ujuQubaMEx8g5Vj"

MASTERSHEET_TEMPLATE_ID = "1RFyUxtBQoRZtpux8kc1ytoHmvOA9CVUuG-zItvPei5c"
BRAND_DISTRIBUTION_SHEET_ID = "1g7HSsCrsgni8_6YZ94R8u1JJ7WNt5SqVUc9TXsBJxoo"
BRAND_DISTRIBUTION_TAB = "BRAND DISTRIBUTION"

# Service group mapping: Asana sections + Jira issue prefixes
SERVICE_GROUPS = {
    "seo": {"asana_sections": ["SEO"], "jira_prefixes": ["SEO -"]},
    "dev": {"asana_sections": ["DEV"], "jira_prefixes": ["DEV -"]},
    "design": {"asana_sections": ["DES"], "jira_prefixes": ["UX/UI -", "PM -"]},
    "social": {"asana_sections": ["SOCIAL MEDIA"], "jira_prefixes": []},
    "crm": {"asana_sections": ["CRM"], "jira_prefixes": []},
    "accounts": {"asana_sections": ["ACCOUNTS", "Q2"], "jira_prefixes": ["CLIENT -"]},
    "content": {"asana_sections": [], "jira_prefixes": []},
}

STATE_DIR = Path(__file__).parent.parent / "state"


# ---------------------------------------------------------------------------
# Doppler helper
# ---------------------------------------------------------------------------

def doppler_get(key: str, project: str = "puzzles", config: str = "prd") -> str:
    """Get a single secret from Doppler."""
    result = subprocess.run(
        ["doppler", "secrets", "get", key, "--project", project, "--config", config, "--plain"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Doppler secret {key} not found in {project}/{config}")
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

class OnboardingState:
    """Atomic, per-item state tracking for resumable onboarding.

    Dual persistence: JSON (local, authoritative) + Supabase (audit trail).
    JSON is written atomically after each operation. Supabase sync is
    fire-and-forget — failures are logged but never block the run.
    """

    _supabase_client = None  # lazy-loaded
    _supabase_lock = threading.Lock()

    def __init__(self, client_slug: str, client_name: str, jira_key: str,
                 service_groups: list[str] | None = None, config: dict | None = None):
        self.path = STATE_DIR / f"{client_slug}.json"
        self.data: dict = {
            "version": 1,
            "client_slug": client_slug,
            "client_name": client_name,
            "jira_key": jira_key,
            "service_groups": service_groups or [],
            "config": config or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "steps": {},
        }

    @classmethod
    def load(cls, client_slug: str) -> "OnboardingState":
        """Load state — try JSON first, fall back to Supabase."""
        path = STATE_DIR / f"{client_slug}.json"
        if path.exists():
            with open(path) as f:
                data = json.load(f)
            state = cls.__new__(cls)
            state.path = path
            state.data = data
            return state

        # Fall back to Supabase
        sb = cls._get_supabase()
        if sb:
            try:
                result = sb.table("client_onboarding").select("*").eq("client_slug", client_slug).execute()
                if result.data:
                    row = result.data[0]
                    state = cls.__new__(cls)
                    state.path = path
                    state.data = {
                        "version": 1,
                        "client_slug": row["client_slug"],
                        "client_name": row["client_name"],
                        "jira_key": row["jira_key"],
                        "service_groups": row.get("service_groups") or [],
                        "config": row.get("config") or {},
                        "created_at": row.get("created_at", ""),
                        "updated_at": row.get("updated_at", ""),
                        "steps": row.get("steps") or {},
                    }
                    state.save()  # Write JSON locally for future use
                    log.info("Loaded state from Supabase for '%s'", client_slug)
                    return state
            except Exception as e:
                log.warning("Supabase fallback load failed: %s", e)

        raise FileNotFoundError(f"No state found for '{client_slug}' (checked JSON + Supabase)")

    @classmethod
    def exists(cls, client_slug: str) -> bool:
        if (STATE_DIR / f"{client_slug}.json").exists():
            return True
        sb = cls._get_supabase()
        if sb:
            try:
                result = sb.table("client_onboarding").select("id").eq("client_slug", client_slug).execute()
                return len(result.data) > 0
            except Exception:
                pass
        return False

    @classmethod
    def _get_supabase(cls):
        """Lazy-load Supabase client (thread-safe)."""
        if cls._supabase_client is not None:
            return cls._supabase_client if cls._supabase_client else None
        with cls._supabase_lock:
            if cls._supabase_client is not None:  # double-check after lock
                return cls._supabase_client if cls._supabase_client else None
            try:
                from supabase import create_client
                url = doppler_get("SUPABASE_INHOUSE_URL")
                key = doppler_get("SUPABASE_INHOUSE_SERVICE_KEY")
                cls._supabase_client = create_client(url, key)
            except Exception as e:
                log.warning("Supabase client init failed (will continue without): %s", e)
                cls._supabase_client = False  # sentinel: don't retry
        return cls._supabase_client if cls._supabase_client else None

    def save(self) -> None:
        """Atomic write: JSON first (blocking), then Supabase (fire-and-forget)."""
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()

        # 1. Write JSON atomically
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=STATE_DIR, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(self.data, f, indent=2)
            os.replace(tmp_path, self.path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        # 2. Sync to Supabase (fire-and-forget)
        self._sync_to_supabase()

    def _sync_to_supabase(self) -> None:
        """Upsert current state to Supabase. Never raises."""
        sb = self._get_supabase()
        if not sb:
            return

        # Determine overall status from steps
        steps = self.data.get("steps", {})
        statuses = [s.get("status", "pending") for s in steps.values()]
        if any(s == "failed" for s in statuses):
            overall = "failed"
        elif all(s in ("complete", "skipped") for s in statuses) and statuses:
            overall = "complete"
        elif any(s in ("in_progress", "complete") for s in statuses):
            overall = "running"
        else:
            overall = "pending"

        row = {
            "client_slug": self.data["client_slug"],
            "client_name": self.data["client_name"],
            "jira_key": self.data["jira_key"],
            "status": overall,
            "service_groups": self.data.get("service_groups") or [],
            "config": self.data.get("config") or {},
            "steps": steps,
            "updated_at": self.data["updated_at"],
        }

        if overall == "complete":
            row["completed_at"] = self.data["updated_at"]
            row["receipt"] = self._build_receipt()

        try:
            sb.table("client_onboarding").upsert(row, on_conflict="client_slug").execute()
        except Exception as e:
            log.warning("Supabase sync failed (run continues): %s", e)

    def _build_receipt(self) -> dict:
        """Build a receipt dict with all created resource URLs."""
        steps = self.data.get("steps", {})
        receipt = {}
        for step_name, step_data in steps.items():
            if not isinstance(step_data, dict):
                continue
            receipt[step_name] = {"status": step_data.get("status", "unknown")}
            for key in ("project_url", "root_folder_url", "spreadsheet_url", "client_id", "project_id", "project_gid", "project_key"):
                if key in step_data:
                    receipt[step_name][key] = step_data[key]
        return receipt

    def get_step(self, name: str) -> dict:
        return self.data["steps"].get(name, {})

    def set_step(self, name: str, value: dict) -> None:
        self.data["steps"][name] = value
        self.save()

    def step_status(self, name: str) -> str:
        return self.get_step(name).get("status", "pending")

    @property
    def client_name(self) -> str:
        return self.data["client_name"]

    @property
    def jira_key(self) -> str:
        return self.data["jira_key"]


def make_slug(client_name: str) -> str:
    """Generate a canonical slug from client name + month."""
    slug = re.sub(r"[^a-z0-9]+", "-", client_name.lower()).strip("-")
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    return f"{slug}-{month}"


# ---------------------------------------------------------------------------
# Platform initializers
# ---------------------------------------------------------------------------

def init_clockify() -> None:
    from clockify_wrapper import configure
    configure(
        api_key=doppler_get("PUZZLES_CLOCKIFY_API_KEY"),
        workspace_id=CLOCKIFY_WORKSPACE_ID,
    )


def init_jira() -> None:
    from jira_wrapper import configure as jira_configure
    jira_configure(
        email=doppler_get("JIRA_PUZZLES_EMAIL"),
        api_token=doppler_get("JIRA_PUZZLES_API_TOKEN"),
        base_url=doppler_get("JIRA_PUZZLES_BASE_URL"),
    )


def init_google() -> None:
    from google_wrapper import use_google
    use_google("puzzles")


def init_asana() -> None:
    from asana_wrapper import use_workspace
    use_workspace("puzzles")


# ---------------------------------------------------------------------------
# Pre-flight validation
# ---------------------------------------------------------------------------

def preflight(state: OnboardingState, dry_run: bool = False) -> bool:
    """Validate all credentials and templates before any writes.

    Returns True if all checks pass. Prints errors and returns False otherwise.
    """
    if state.step_status("preflight") == "complete" and not dry_run:
        log.info("Pre-flight already passed, skipping")
        return True

    errors: list[str] = []
    warnings: list[str] = []

    # Input validation
    jira_key = state.jira_key
    if not re.match(r"^[A-Z][A-Z0-9]{1,9}$", jira_key):
        errors.append(f"Jira key '{jira_key}' must be 2-10 uppercase letters/numbers starting with a letter")

    if not state.client_name.strip():
        errors.append("Client name cannot be empty")

    # Platform checks
    checks = [
        ("Clockify", _check_clockify),
        ("Asana", _check_asana),
        ("Jira", _check_jira),
        ("Google Drive", _check_drive),
        ("Google Sheets", _check_sheets),
    ]

    for name, check_fn in checks:
        try:
            check_fn(state, warnings)
            log.info("  [OK] %s", name)
        except Exception as e:
            errors.append(f"{name}: {e}")
            log.error("  [FAIL] %s: %s", name, e)

    # Zoom — optional
    try:
        doppler_get("ZOOM_CLIENT_ID")
        log.info("  [OK] Zoom (creds found)")
    except Exception:
        warnings.append("Zoom: no credentials configured — will be skipped")
        log.info("  [SKIP] Zoom (no creds)")

    for w in warnings:
        log.warning("  WARN: %s", w)

    if errors:
        log.error("Pre-flight FAILED with %d errors:", len(errors))
        for e in errors:
            log.error("  - %s", e)
        return False

    if not dry_run:
        state.set_step("preflight", {"status": "complete"})

    return True


def _check_clockify(state: OnboardingState, warnings: list[str]) -> None:
    init_clockify()
    from clockify_wrapper import get_current_user, find_client_by_name
    get_current_user()  # verifies API key works
    existing = find_client_by_name(state.client_name)
    if existing:
        warnings.append(f"Clockify client '{state.client_name}' already exists (id={existing['id']})")


def _check_asana(state: OnboardingState, warnings: list[str]) -> None:
    init_asana()
    from asana_wrapper.projects import get_project, find_project_by_name
    get_project(ASANA_TEMPLATE_GID)  # verifies template exists
    existing = find_project_by_name(state.client_name)
    if existing:
        warnings.append(f"Asana project '{state.client_name}' already exists (gid={existing['gid']})")


def _check_jira(state: OnboardingState, warnings: list[str]) -> None:
    init_jira()
    from jira_wrapper import get_project, find_project_by_key, search_issues
    get_project(JIRA_TEMPLATE_PROJECT_KEY)  # verifies template project
    issues = search_issues(f"project = {JIRA_TEMPLATE_PROJECT_KEY}", max_results=1)
    if not issues:
        warnings.append("Jira template project PU has no issues to copy")
    existing = find_project_by_key(state.jira_key)
    if existing:
        warnings.append(f"Jira project key '{state.jira_key}' already exists")


def _check_drive(state: OnboardingState, warnings: list[str]) -> None:
    init_google()
    from google_wrapper.drive import get_file_metadata
    get_file_metadata(DRIVE_TEMPLATE_FOLDER_ID)  # verifies template accessible
    get_file_metadata(DRIVE_CLIENTS_FOLDER_ID)  # verifies destination accessible


def _check_sheets(state: OnboardingState, warnings: list[str]) -> None:
    init_google()
    from google_wrapper.drive import get_file_metadata
    get_file_metadata(MASTERSHEET_TEMPLATE_ID)  # verifies template accessible
    get_file_metadata(BRAND_DISTRIBUTION_SHEET_ID)  # verifies main sheet accessible


# ---------------------------------------------------------------------------
# Step 1: Clockify
# ---------------------------------------------------------------------------

def step_clockify(state: OnboardingState) -> None:
    if state.step_status("clockify") == "complete":
        log.info("Clockify: already complete, skipping")
        return

    state.set_step("clockify", {"status": "in_progress"})
    init_clockify()
    from clockify_wrapper import find_client_by_name, create_client, find_project_by_name, create_project

    name = state.client_name

    # Idempotent: check before create
    client = find_client_by_name(name)
    if client:
        log.info("Clockify: client '%s' already exists (id=%s)", name, client["id"])
    else:
        client = create_client(name)
        log.info("Clockify: created client '%s' (id=%s)", name, client["id"])

    project = find_project_by_name(name, client_id=client["id"])
    if project:
        log.info("Clockify: project '%s' already exists (id=%s)", name, project["id"])
    else:
        project = create_project(name, client_id=client["id"], billable=True, is_public=True)
        log.info("Clockify: created project '%s' (id=%s)", name, project["id"])

    state.set_step("clockify", {
        "status": "complete",
        "client_id": client["id"],
        "project_id": project["id"],
    })


# ---------------------------------------------------------------------------
# Step 2: Asana
# ---------------------------------------------------------------------------

def step_asana(state: OnboardingState, service_groups: list[str] | None = None) -> None:
    step = state.get_step("asana")
    if step.get("status") == "complete":
        log.info("Asana: already complete, skipping")
        return

    init_asana()
    from asana_wrapper.projects import find_project_by_name
    from asana_wrapper.tasks import create_task, add_task_to_project
    import asana

    name = state.client_name

    # Check if already exists — but only mark complete if tasks are populated
    # (or if no service groups = empty project is correct)
    existing = find_project_by_name(name, exact=True)
    if existing:
        existing_gid = existing["gid"]
        # If no service groups, empty project is correct → mark complete
        # If service groups, check that project has tasks before marking complete
        wanted_sections: set[str] = set()
        if service_groups:
            for sg in service_groups:
                wanted_sections.update(SERVICE_GROUPS.get(sg, {}).get("asana_sections", []))
        if not wanted_sections:
            log.info("Asana: project '%s' already exists (gid=%s) — no tasks needed", name, existing_gid)
            state.set_step("asana", {
                "status": "complete",
                "project_gid": existing_gid,
                "project_url": f"https://app.asana.com/0/{existing_gid}",
            })
            return
        # Has service groups — verify tasks exist
        from asana_wrapper.tasks import get_tasks as _check_tasks
        existing_tasks = list(_check_tasks(project=existing_gid,
                                           completed_since="1970-01-01T00:00:00Z",
                                           opt_fields=["gid"]))
        if existing_tasks:
            log.info("Asana: project '%s' already exists with %d tasks — complete",
                     name, len(existing_tasks))
            state.set_step("asana", {
                "status": "complete",
                "project_gid": existing_gid,
                "project_url": f"https://app.asana.com/0/{existing_gid}",
                "tasks_created": len(existing_tasks),
            })
            return
        # Project exists but has no tasks — needs population
        log.info("Asana: project '%s' exists but has 0 tasks — will populate", name)
        state.set_step("asana", {"status": "in_progress", "project_gid": existing_gid})

    # Helper to extract GID from SDK response (dict or typed object)
    def _gid(obj):
        if isinstance(obj, dict):
            return obj.get("gid") or obj.get("data", {}).get("gid", "")
        return getattr(obj, "gid", "")

    from asana_wrapper.asana_client import get_client as get_asana_client
    api_client = get_asana_client()
    projects_api = asana.ProjectsApi(api_client)

    # Resume support: if project was already created but tasks weren't finished
    step = state.get_step("asana")  # Re-read in case idempotency check updated it
    project_gid = step.get("project_gid")
    if not project_gid:
        # Create empty project (always — no more template duplication)
        result = projects_api.create_project_for_team(
            {"data": {"name": name, "default_view": "list"}},
            ASANA_TEAM_GID,
            {},
        )
        project_gid = _gid(result)
        log.info("Asana: created project '%s' (gid=%s)", name, project_gid)
        state.set_step("asana", {"status": "in_progress", "project_gid": project_gid})

    # Determine if we need to populate tasks from template map
    wanted_sections: set[str] = set()
    if service_groups:
        for sg in service_groups:
            wanted_sections.update(SERVICE_GROUPS.get(sg, {}).get("asana_sections", []))

    if not wanted_sections:
        # No service groups or no asana_sections for selected groups → empty project
        if not service_groups:
            log.info("Asana: no service groups — empty project")
        else:
            log.info("Asana: selected groups have no asana_sections — empty project")
        state.set_step("asana", {
            "status": "complete",
            "project_gid": project_gid,
            "project_url": f"https://app.asana.com/0/{project_gid}",
            "tasks_created": 0,
        })
        return

    # Resume guard: if project already has tasks, don't re-create
    from asana_wrapper.tasks import get_tasks
    existing_tasks = list(get_tasks(project=project_gid,
                                    completed_since="1970-01-01T00:00:00Z",
                                    opt_fields=["gid", "name"]))
    if existing_tasks:
        log.info("Asana: project already has %d tasks — treating as complete", len(existing_tasks))
        state.set_step("asana", {
            "status": "complete",
            "project_gid": project_gid,
            "project_url": f"https://app.asana.com/0/{project_gid}",
            "tasks_created": len(existing_tasks),
        })
        return

    # Populate from template map
    from template_maps import ASANA_TEMPLATE_MAP

    sections_api = asana.SectionsApi(api_client)
    tasks_created = 0

    for section_name in sorted(wanted_sections):
        task_defs = ASANA_TEMPLATE_MAP.get(section_name, [])
        if not task_defs:
            log.info("Asana: section '%s' has no tasks in template map — skipping", section_name)
            continue

        # Create section in project
        section_result = sections_api.create_section_for_project(
            project_gid,
            {"body": {"data": {"name": section_name}}},
        )
        section_gid = _gid(section_result)
        log.info("Asana: created section '%s' (gid=%s)", section_name, section_gid)

        # Create parent tasks and subtasks
        for task_def in task_defs:
            # Don't pass project= here — add_task_to_project places it in the correct section
            task = create_task(
                task_def["name"],
                assignee=None,
                notes=task_def.get("notes", ""),
            )
            task_gid = _gid(task)
            # Place task in project + section in one call
            add_task_to_project(task_gid, project_gid, section=section_gid)
            tasks_created += 1

            for sub_def in task_def.get("subtasks", []):
                create_task(
                    sub_def["name"],
                    parent=task_gid,
                    assignee=None,
                    notes=sub_def.get("notes", ""),
                )
                tasks_created += 1

        log.info("Asana: populated section '%s' — %d tasks + subtasks",
                 section_name, len(task_defs) + sum(len(t.get("subtasks", [])) for t in task_defs))

    log.info("Asana: total tasks created: %d", tasks_created)

    state.set_step("asana", {
        "status": "complete",
        "project_gid": project_gid,
        "project_url": f"https://app.asana.com/0/{project_gid}",
        "tasks_created": tasks_created,
    })


# ---------------------------------------------------------------------------
# Step 3: Jira
# ---------------------------------------------------------------------------

def step_jira(state: OnboardingState, service_groups: list[str] | None = None) -> None:
    step = state.get_step("jira")
    if step.get("status") == "complete":
        log.info("Jira: already complete, skipping")
        return

    state.set_step("jira", {"status": "in_progress"})
    init_jira()
    from jira_wrapper import find_project_by_key, create_jpd_project

    jira_key = state.jira_key
    name = state.client_name

    # Create JPD project (idempotent)
    existing = find_project_by_key(jira_key)
    if existing:
        log.info("Jira: project '%s' already exists (id=%s)", jira_key, existing.get("id"))
        project_id = str(existing.get("id", ""))
    else:
        # Use undocumented simplified endpoint for JPD projects
        result = create_jpd_project(jira_key, name)
        project_id = str(result.get("projectId", ""))
        log.info("Jira: created JPD project %s (id=%s)", jira_key, project_id)

    # Update state with project info (for live UI)
    state.set_step("jira", {
        "status": "in_progress",
        "project_id": project_id,
        "project_key": jira_key,
    })

    from jira_wrapper._base import _api_call as jira_api
    from jira_wrapper import search_issues

    # Populate issues from static template map
    if not service_groups:
        log.info("Jira: no service groups selected — empty project")
        copy_result = {"copied": {}, "errors": [], "total_source": 0}
    else:
        prefixes = []
        for sg in service_groups:
            prefixes.extend(SERVICE_GROUPS.get(sg, {}).get("jira_prefixes", []))

        if not prefixes:
            log.info("Jira: selected groups have no jira_prefixes — empty project")
            copy_result = {"copied": {}, "errors": [], "total_source": 0}
        else:
            from template_maps import JIRA_TEMPLATE_MAP
            from jira_wrapper import bulk_create_issues, text_to_adf

            # Filter static map by prefix (Python string matching — no JQL)
            filtered = [i for i in JIRA_TEMPLATE_MAP
                        if any(i["summary"].startswith(p) for p in prefixes)]
            log.info("Jira: %d issues match prefixes %s", len(filtered), prefixes)

            copied: dict[str, str] = {}
            all_errors: list = []

            for batch_start in range(0, len(filtered), 50):
                batch = filtered[batch_start:batch_start + 50]
                payloads = []
                for issue_def in batch:
                    fields: dict = {
                        "project": {"key": jira_key},
                        "issuetype": {"name": issue_def.get("issue_type", "Idea")},
                        "summary": issue_def["summary"],
                    }
                    if issue_def.get("description"):
                        fields["description"] = text_to_adf(issue_def["description"])
                    payloads.append({"fields": fields})

                result = bulk_create_issues(payloads)
                created_issues = result.get("issues", [])
                errors = result.get("errors", [])

                # Map created issues by index
                failed_indices = {e.get("failedElementNumber") for e in errors}
                created_idx = 0
                for i, issue_def in enumerate(batch):
                    if i not in failed_indices and created_idx < len(created_issues):
                        copied[issue_def["summary"]] = created_issues[created_idx].get("key", "")
                        created_idx += 1

                all_errors.extend(errors)

            log.info("Jira: created %d/%d issues (%d errors)",
                     len(copied), len(filtered), len(all_errors))
            copy_result = {"copied": copied, "errors": all_errors, "total_source": len(filtered)}

    # Delete any issues NOT created by us (JPD sample ideas)
    # Retry search until we see at least our own issues (Jira indexing lag)
    our_keys = set(copy_result.get("copied", {}).values())
    sample_issues = []
    for attempt in range(5):
        if attempt > 0:
            time.sleep(3)
        all_issues = search_issues(f"project = {jira_key}", max_results=300)
        found_ours = sum(1 for i in all_issues if i["key"] in our_keys)
        sample_issues = [i for i in all_issues if i["key"] not in our_keys]
        log.info("Jira cleanup attempt %d: %d total, %d ours, %d samples",
                 attempt + 1, len(all_issues), found_ours, len(sample_issues))
        # If we can see our created issues, the index is caught up
        if our_keys and found_ours >= len(our_keys) * 0.8:
            break
        # If no service groups (empty project), just look for any issues to delete
        if not our_keys and all_issues:
            break
    deleted = 0
    for issue in sample_issues:
        try:
            jira_api("DELETE", f"issue/{issue['key']}")
            deleted += 1
            log.info("Jira: deleted sample issue %s (%s)", issue["key"], issue.get("summary", ""))
        except Exception as e:
            log.warning("Jira: failed to delete %s: %s", issue["key"], e)
    if sample_issues:
        log.info("Jira: deleted %d/%d sample issues from '%s'", deleted, len(sample_issues), jira_key)

    base_url = doppler_get("JIRA_PUZZLES_BASE_URL")
    state.set_step("jira", {
        "status": "complete",
        "project_id": project_id,
        "project_key": jira_key,
        "project_url": f"{base_url}/jira/discovery/project/{jira_key}",
        "issues_copied": len(copy_result["copied"]),
        "issues_errors": len(copy_result["errors"]),
        "issue_map": copy_result["copied"],
    })


# ---------------------------------------------------------------------------
# Step 4: Google Drive
# ---------------------------------------------------------------------------

def step_drive(state: OnboardingState) -> None:
    step = state.get_step("drive")
    if step.get("status") == "complete":
        log.info("Drive: already complete, skipping")
        return

    init_google()
    from google_wrapper.drive import (
        create_folder, list_folder_contents, get_file_metadata,
        copy_file, search_files, GOOGLE_FOLDER,
    )

    name = state.client_name
    folders_created = step.get("folders_created", {})

    # Create root client folder (idempotent)
    root_id = step.get("root_folder_id")
    if not root_id:
        # Check if folder already exists
        existing = search_files(name_contains=name, mime_type=GOOGLE_FOLDER, folder_id=DRIVE_CLIENTS_FOLDER_ID)
        exact = [f for f in existing if f["name"] == name]
        if exact:
            root_id = exact[0]["id"]
            log.info("Drive: root folder '%s' already exists (id=%s)", name, root_id)
        else:
            result = create_folder(name, parent_folder_id=DRIVE_CLIENTS_FOLDER_ID)
            root_id = result["id"]
            log.info("Drive: created root folder '%s' (id=%s)", name, root_id)

    # Recursive depth-first copy of template tree
    def copy_tree(template_folder_id: str, dest_parent_id: str) -> None:
        children = list_folder_contents(template_folder_id)
        for child in children:
            child_id = child["id"]
            child_name = child["name"]
            child_mime = child.get("mimeType", "")

            # Skip if already created (idempotent)
            if child_id in folders_created:
                continue

            if child_mime == GOOGLE_FOLDER:
                # Check if subfolder already exists in dest
                existing_children = search_files(
                    name_contains=child_name, mime_type=GOOGLE_FOLDER, folder_id=dest_parent_id
                )
                exact_match = [f for f in existing_children if f["name"] == child_name]

                if exact_match:
                    new_id = exact_match[0]["id"]
                    log.info("Drive: subfolder '%s' already exists (id=%s)", child_name, new_id)
                else:
                    result = create_folder(child_name, parent_folder_id=dest_parent_id)
                    new_id = result["id"]
                    log.info("Drive: created subfolder '%s' (id=%s)", child_name, new_id)

                folders_created[child_id] = {"new_id": new_id, "name": child_name}
                # Save after each folder for resumability
                _save_drive_state(state, root_id, folders_created)
                # Recurse into subfolder
                copy_tree(child_id, new_id)
            else:
                # Copy file (doc, sheet, etc.)
                result = copy_file(child_id, new_name=child_name, folder_id=dest_parent_id)
                folders_created[child_id] = {"new_id": result["id"], "name": child_name}
                _save_drive_state(state, root_id, folders_created)
                log.info("Drive: copied file '%s'", child_name)

    copy_tree(DRIVE_TEMPLATE_FOLDER_ID, root_id)

    # Find key folder IDs for later steps
    shared_folder_id = None
    mastersheet_folder_id = None
    for template_id, info in folders_created.items():
        if info["name"] == "06. Shared Folder":
            shared_folder_id = info["new_id"]
        if info["name"] == "Client Mastersheet":
            mastersheet_folder_id = info["new_id"]

    state.set_step("drive", {
        "status": "complete",
        "root_folder_id": root_id,
        "root_folder_url": f"https://drive.google.com/drive/folders/{root_id}",
        "folders_created": folders_created,
        "folders_total": len(folders_created),
        "shared_folder_id": shared_folder_id,
        "mastersheet_folder_id": mastersheet_folder_id,
    })


def _save_drive_state(state: OnboardingState, root_id: str, folders_created: dict) -> None:
    """Save intermediate drive state for resumability."""
    state.set_step("drive", {
        "status": "in_progress",
        "root_folder_id": root_id,
        "root_folder_url": f"https://drive.google.com/drive/folders/{root_id}",
        "folders_created": folders_created,
        "folders_total": len(folders_created),
    })


# ---------------------------------------------------------------------------
# Step 5: Google Sheets
# ---------------------------------------------------------------------------

def step_sheets(state: OnboardingState, config: dict) -> None:
    """Copy client mastersheet + append to BRAND DISTRIBUTION."""
    init_google()

    # 5a: Copy client mastersheet (re-read state fresh each time)
    if state.step_status("sheets_client") != "complete":
        _step_sheets_client(state, config)

    # 5b: Append to BRAND DISTRIBUTION (re-read state fresh after 5a)
    if state.step_status("sheets_brand_distribution") != "complete":
        _step_sheets_brand_distribution(state, config)


def _step_sheets_client(state: OnboardingState, config: dict) -> None:
    # Preserve existing step data (e.g. spreadsheet_id from a prior partial run)
    existing_data = state.get_step("sheets_client")
    state.set_step("sheets_client", {**existing_data, "status": "in_progress"})
    from google_wrapper.drive import copy_file, move_file

    name = state.client_name
    drive_step = state.get_step("drive")
    mastersheet_folder_id = drive_step.get("mastersheet_folder_id")

    if not mastersheet_folder_id:
        log.warning("Sheets: no mastersheet folder found in Drive — skipping client mastersheet")
        state.set_step("sheets_client", {"status": "complete", "skipped": True})
        return

    # Check if we have a sheet from a prior partial run
    existing_sheet_id = state.get_step("sheets_client").get("spreadsheet_id")

    if existing_sheet_id:
        sheet_id = existing_sheet_id
        log.info("Sheets: resuming with existing sheet (id=%s)", sheet_id)
    else:
        # Copy template spreadsheet
        new_sheet = copy_file(MASTERSHEET_TEMPLATE_ID, new_name=f"{name} - Mastersheet")
        sheet_id = new_sheet["id"]
        log.info("Sheets: copied mastersheet template (id=%s)", sheet_id)
        # Save immediately so we don't lose the sheet ID on next failure
        state.set_step("sheets_client", {"status": "in_progress", "spreadsheet_id": sheet_id})

    # Move to client's Drive folder
    move_file(sheet_id, mastersheet_folder_id)
    log.info("Sheets: moved mastersheet to client folder")

    # Auto-populate URLs in the mastersheet
    _populate_mastersheet_urls(sheet_id, state)

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    state.set_step("sheets_client", {
        "status": "complete",
        "spreadsheet_id": sheet_id,
        "spreadsheet_url": sheet_url,
    })


def _populate_mastersheet_urls(sheet_id: str, state: OnboardingState) -> None:
    """Update the client mastersheet with URLs from other steps."""
    drive_step = state.get_step("drive")
    jira_step = state.get_step("jira")

    updates = []

    # Row 4 (index 3): Shared Folder URL (column F = index 5)
    shared_folder_id = drive_step.get("shared_folder_id")
    if shared_folder_id:
        shared_url = f"https://drive.google.com/drive/folders/{shared_folder_id}"
        updates.append({
            "range": "MASTERSHEET!F4",
            "values": [[shared_url]],
        })

    # Row 5 (index 4): Roadmap / Jira PD URL (column F = index 5)
    jira_url = jira_step.get("project_url")
    if jira_url:
        updates.append({
            "range": "MASTERSHEET!F5",
            "values": [[jira_url]],
        })

    if not updates:
        return

    # Use Sheets API to batch update
    from google_wrapper._base import _get_service
    service = _get_service("sheets", "v4")
    body = {"valueInputOption": "RAW", "data": updates}
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id, body=body
    ).execute()
    log.info("Sheets: auto-populated %d URLs in client mastersheet", len(updates))


def _step_sheets_brand_distribution(state: OnboardingState, config: dict) -> None:
    """Append a row to the main BRAND DISTRIBUTION tab."""
    state.set_step("sheets_brand_distribution", {"status": "in_progress"})
    from google_wrapper._base import _get_service

    name = state.client_name
    sheets_client = state.get_step("sheets_client")
    mastersheet_url = sheets_client.get("spreadsheet_url", "")

    # Build row: NO, CLIENT, PROJECT, LOE, PRIORITY, STATUS, URL, MASTERSHEET, ROADMAP, ...
    website_url = config.get("website_url", "").replace('"', '""')
    safe_mastersheet = mastersheet_url.replace('"', '""')
    row = [
        "",  # NO (auto-filled or manual)
        name,
        config.get("project_type", "Ongoing"),
        str(config.get("loe", "")),
        config.get("priority", ""),
        "In Queue",
        f'=HYPERLINK("{website_url}", "Visit")' if website_url else "",
        f'=HYPERLINK("{safe_mastersheet}", "Visit")' if safe_mastersheet else "",
        config.get("roadmap_quarter", ""),
    ]

    # Team assignments (optional)
    team = config.get("team_assignments", {})
    for role in ["account", "pod1", "designer", "pod2", "seo", "pod3", "content", "local", "qa", "pod4", "dev", "pod5", "portal", "organic"]:
        row.append(team.get(role, ""))

    service = _get_service("sheets", "v4")
    service.spreadsheets().values().append(
        spreadsheetId=BRAND_DISTRIBUTION_SHEET_ID,
        range=f"'{BRAND_DISTRIBUTION_TAB}'!B4",
        valueInputOption="USER_ENTERED",
        insertDataOption="OVERWRITE",
        body={"values": [row]},
    ).execute()

    log.info("Sheets: appended row to BRAND DISTRIBUTION for '%s'", name)
    state.set_step("sheets_brand_distribution", {"status": "complete"})


# ---------------------------------------------------------------------------
# Step 6: Zoom (optional)
# ---------------------------------------------------------------------------

def step_zoom(state: OnboardingState) -> None:
    if state.step_status("zoom") == "complete":
        log.info("Zoom: already complete, skipping")
        return

    try:
        doppler_get("ZOOM_CLIENT_ID")
    except Exception:
        log.warning("Zoom: no credentials configured — skipping")
        state.set_step("zoom", {"status": "skipped", "reason": "No credentials configured"})
        return

    # TODO: Implement Zoom Team Chat channel creation when creds available
    log.warning("Zoom: credentials found but channel creation not yet implemented")
    state.set_step("zoom", {"status": "skipped", "reason": "Not yet implemented"})


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def dry_run(state: OnboardingState, service_groups: list[str] | None) -> None:
    """Run pre-flight checks and output a manifest of what would be created."""
    print("\n=== DRY RUN — Onboarding Manifest ===\n")
    print(f"Client:     {state.client_name}")
    print(f"Jira Key:   {state.jira_key}")
    print(f"Slug:       {state.data['client_slug']}")
    if service_groups:
        print(f"Service Groups: {', '.join(service_groups)}")
    else:
        print(f"Service Groups: NONE (empty Asana + Jira projects)")
    print()

    print("Steps that would be executed:")
    print(f"  1. Clockify: Create client + project '{state.client_name}'")
    if service_groups:
        sections = set()
        for sg in service_groups:
            sections.update(SERVICE_GROUPS.get(sg, {}).get("asana_sections", []))
        prefixes = []
        for sg in service_groups:
            prefixes.extend(SERVICE_GROUPS.get(sg, {}).get("jira_prefixes", []))
        print(f"  2. Asana: Create project + populate {', '.join(sections) or '(none)'} sections from template map")
        print(f"  3. Jira PD: Create project '{state.jira_key}' + populate issues matching {prefixes or '(none)'}")
    else:
        print(f"  2. Asana: Create empty project '{state.client_name}' in Client Services")
        print(f"  3. Jira PD: Create empty project '{state.jira_key}'")
    print(f"  4. Google Drive: Copy 25-folder template → '{state.client_name}/' in Clients folder")
    print(f"  5a. Google Sheets: Copy mastersheet template → client's Drive folder")
    print(f"  5b. Google Sheets: Append row to BRAND DISTRIBUTION")
    print(f"  6. Zoom: Create team chat channel (if creds available)")
    print()
    print(f"Estimated runtime: ~60-90 seconds")
    print()

    print("Running pre-flight checks...")
    ok = preflight(state, dry_run=True)
    if ok:
        print("\n[OK] All pre-flight checks passed. Ready to run.")
    else:
        print("\n[FAIL] Pre-flight checks failed. Fix errors above before running.")


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def run_onboarding(
    client_name: str,
    jira_key: str,
    *,
    resume: bool = False,
    service_groups: list[str] | None = None,
    config: dict | None = None,
) -> OnboardingState:
    """Execute the full onboarding sequence."""
    config = config or {}

    # Early validation — before creating state or touching Supabase
    if not client_name.strip():
        log.error("Client name cannot be empty")
        sys.exit(1)
    if not re.match(r"^[A-Z][A-Z0-9]{1,9}$", jira_key):
        log.error("Jira key '%s' must be 2-10 uppercase letters/numbers starting with a letter", jira_key)
        sys.exit(1)

    slug = make_slug(client_name)

    if resume and OnboardingState.exists(slug):
        state = OnboardingState.load(slug)
        # Restore service_groups from state if not provided on CLI
        if service_groups is None:
            service_groups = state.data.get("service_groups") or None
        log.info("Resuming onboarding for '%s' (slug=%s, service_groups=%s)",
                 client_name, slug, service_groups)
    else:
        if OnboardingState.exists(slug) and not resume:
            log.error("State file already exists for slug '%s'. Use --resume to continue, or delete the state file.", slug)
            sys.exit(1)
        state = OnboardingState(slug, client_name, jira_key,
                                service_groups=service_groups, config=config)
        state.save()
        log.info("Starting new onboarding for '%s' (slug=%s)", client_name, slug)

    # Pre-flight
    if not preflight(state):
        log.error("Pre-flight failed. Aborting.")
        sys.exit(1)

    # Execute steps
    step_clockify(state)
    step_asana(state, service_groups=service_groups)
    step_jira(state, service_groups=service_groups)
    step_drive(state)
    step_sheets(state, config)
    step_zoom(state)

    # Print receipt
    print("\n=== Onboarding Complete ===\n")
    print(f"Client: {client_name}")
    print(f"State:  {state.path}\n")

    for step_name in ["clockify", "asana", "jira", "drive", "sheets_client", "sheets_brand_distribution", "zoom"]:
        step = state.get_step(step_name)
        status = step.get("status", "pending")
        icon = {"complete": "[OK]", "skipped": "[SKIP]", "pending": "[--]"}.get(status, "[??]")

        url = step.get("project_url") or step.get("root_folder_url") or step.get("spreadsheet_url") or ""
        print(f"  {icon} {step_name:30s} {url}")

    print()
    return state


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Puzzles Client Onboarding Automation")
    parser.add_argument("client_name", help="Client name (e.g., 'Acme Corp')")
    parser.add_argument("jira_key", help="Jira project key abbreviation (e.g., 'AC')")
    parser.add_argument("--dry-run", action="store_true", help="Preview what would be created without making changes")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--service-groups", type=str, default="",
                        help="Comma-separated service groups to filter tasks (e.g., seo,dev,design)")
    parser.add_argument("--project-type", default="Ongoing", help="Project type: Ongoing or Specific")
    parser.add_argument("--loe", type=int, default=0, help="Level of effort (1-10)")
    parser.add_argument("--priority", default="", help="Priority: Low, Medium, High")
    parser.add_argument("--website-url", default="", help="Client website URL")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    service_groups = [s.strip() for s in args.service_groups.split(",") if s.strip()] or None

    config = {
        "project_type": args.project_type,
        "loe": args.loe,
        "priority": args.priority,
        "website_url": args.website_url,
    }

    if args.dry_run:
        slug = make_slug(args.client_name)
        state = OnboardingState(slug, args.client_name, args.jira_key.upper())
        dry_run(state, service_groups)
    else:
        run_onboarding(
            args.client_name,
            args.jira_key.upper(),
            resume=args.resume,
            service_groups=service_groups,
            config=config,
        )


if __name__ == "__main__":
    main()
