#!/usr/bin/env /opt/homebrew/bin/python3.14
"""
Cleanup test artifacts from client onboarding E2E tests.

Reads the state JSON (or Supabase) to find resource IDs, then deletes
across all platforms in reverse creation order. Idempotent — safe to
run multiple times. 404/not-found errors are treated as success.

Usage:
    python cleanup_test_run.py --slug test-delete-me-2026-03 --jira-key TD
    python cleanup_test_run.py --slug test-delete-me-2026-03 --jira-key TD --dry-run
    python cleanup_test_run.py --sweep          # find & delete all TEST* artifacts
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

log = logging.getLogger("cleanup")

CLOCKIFY_WORKSPACE_ID = "6040f154f30cf74c8f545e25"
BRAND_DISTRIBUTION_SHEET_ID = "1g7HSsCrsgni8_6YZ94R8u1JJ7WNt5SqVUc9TXsBJxoo"
BRAND_DISTRIBUTION_TAB = "BRAND DISTRIBUTION"
DRIVE_CLIENTS_FOLDER_ID = "1dbnOly1BTBTOWowk9ujuQubaMEx8g5Vj"
STATE_DIR = Path(__file__).parent.parent / "state"


def doppler_get(key: str, project: str = "puzzles", config: str = "prd") -> str:
    result = subprocess.run(
        ["doppler", "secrets", "get", key, "--project", project, "--config", config, "--plain"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Doppler secret {key} not found")
    return result.stdout.strip()


def init_clockify():
    from clockify_wrapper import configure
    configure(
        api_key=doppler_get("PUZZLES_CLOCKIFY_API_KEY"),
        workspace_id=CLOCKIFY_WORKSPACE_ID,
    )


def init_jira():
    from jira_wrapper import configure
    configure(
        email=doppler_get("JIRA_PUZZLES_EMAIL"),
        api_token=doppler_get("JIRA_PUZZLES_API_TOKEN"),
        base_url=doppler_get("JIRA_PUZZLES_BASE_URL"),
    )


def init_asana():
    from asana_wrapper import use_workspace
    use_workspace("puzzles")


def init_google():
    from google_wrapper import use_google
    use_google("puzzles")


def get_supabase():
    from supabase import create_client
    url = doppler_get("SUPABASE_INHOUSE_URL")
    key = doppler_get("SUPABASE_INHOUSE_SERVICE_KEY")
    return create_client(url, key)


def load_state(slug: str) -> dict | None:
    """Load state from JSON, fallback to Supabase."""
    path = STATE_DIR / f"{slug}.json"
    if path.exists():
        return json.loads(path.read_text())

    try:
        sb = get_supabase()
        result = sb.table("client_onboarding").select("*").eq("client_slug", slug).execute()
        if result.data:
            row = result.data[0]
            return {
                "client_slug": row["client_slug"],
                "client_name": row["client_name"],
                "jira_key": row["jira_key"],
                "steps": row.get("steps") or {},
            }
    except Exception as e:
        log.warning("Supabase fallback failed: %s", e)

    return None


def cleanup_single(slug: str, jira_key: str, dry_run: bool = False) -> dict:
    """Delete all test artifacts for a single slug. Returns status per platform."""
    results = {}
    state = load_state(slug)
    steps = state.get("steps", {}) if state else {}
    client_name = state.get("client_name", "") if state else ""

    # 1. BRAND DISTRIBUTION row
    try:
        init_google()
        from google_wrapper._base import _get_service
        svc = _get_service("sheets", "v4")
        sheet_data = svc.spreadsheets().values().get(
            spreadsheetId=BRAND_DISTRIBUTION_SHEET_ID,
            range=f"'{BRAND_DISTRIBUTION_TAB}'!A:X",
        ).execute()
        rows = sheet_data.get("values", [])
        row_idx = None
        for i, row in enumerate(rows):
            if client_name and any(client_name in str(cell) for cell in row):
                row_idx = i
                break

        if row_idx is not None:
            if dry_run:
                results["brand_distribution"] = f"WOULD DELETE row {row_idx}"
            else:
                meta = svc.spreadsheets().get(spreadsheetId=BRAND_DISTRIBUTION_SHEET_ID).execute()
                sheet_id = None
                for s in meta.get("sheets", []):
                    if s["properties"]["title"] == BRAND_DISTRIBUTION_TAB:
                        sheet_id = s["properties"]["sheetId"]
                        break
                if sheet_id is not None:
                    svc.spreadsheets().batchUpdate(
                        spreadsheetId=BRAND_DISTRIBUTION_SHEET_ID,
                        body={"requests": [{"deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_idx,
                                "endIndex": row_idx + 1,
                            }
                        }}]}
                    ).execute()
                    results["brand_distribution"] = f"DELETED row {row_idx}"
                else:
                    results["brand_distribution"] = "ERROR: tab not found"
        else:
            results["brand_distribution"] = "NOT_FOUND"
    except Exception as e:
        results["brand_distribution"] = f"ERROR: {e}"

    # 2. Sheets mastersheet
    sheets_step = steps.get("sheets_client", {})
    sheet_id = sheets_step.get("spreadsheet_id")
    if sheet_id:
        try:
            from google_wrapper.drive import trash_file
            if dry_run:
                results["sheets_mastersheet"] = f"WOULD TRASH {sheet_id}"
            else:
                trash_file(sheet_id)
                results["sheets_mastersheet"] = f"DELETED {sheet_id}"
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                results["sheets_mastersheet"] = "NOT_FOUND"
            else:
                results["sheets_mastersheet"] = f"ERROR: {e}"
    else:
        results["sheets_mastersheet"] = "NOT_FOUND (no ID in state)"

    # 3. Drive folder
    drive_step = steps.get("drive", {})
    root_folder_id = drive_step.get("root_folder_id")
    if root_folder_id:
        try:
            from google_wrapper.drive import trash_file
            if dry_run:
                results["drive"] = f"WOULD TRASH folder {root_folder_id}"
            else:
                trash_file(root_folder_id)
                results["drive"] = f"DELETED {root_folder_id}"
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                results["drive"] = "NOT_FOUND"
            else:
                results["drive"] = f"ERROR: {e}"
    else:
        results["drive"] = "NOT_FOUND (no ID in state)"

    # 4. Jira project
    if jira_key:
        try:
            init_jira()
            from jira_wrapper import find_project_by_key, delete_project
            existing = find_project_by_key(jira_key)
            if existing:
                if dry_run:
                    results["jira"] = f"WOULD DELETE project {jira_key}"
                else:
                    delete_project(jira_key)
                    results["jira"] = f"DELETED project {jira_key}"
            else:
                results["jira"] = "NOT_FOUND"
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                results["jira"] = "NOT_FOUND"
            else:
                results["jira"] = f"ERROR: {e}"
    else:
        results["jira"] = "SKIPPED (no key provided)"

    # 5. Asana project
    asana_step = steps.get("asana", {})
    asana_gid = asana_step.get("project_gid")
    if asana_gid:
        try:
            init_asana()
            import asana
            from asana_wrapper.asana_client import get_client
            api = asana.ProjectsApi(get_client())
            if dry_run:
                results["asana"] = f"WOULD DELETE project {asana_gid}"
            else:
                api.delete_project(asana_gid)
                results["asana"] = f"DELETED project {asana_gid}"
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                results["asana"] = "NOT_FOUND"
            else:
                results["asana"] = f"ERROR: {e}"
    else:
        # Try finding by name
        if client_name:
            try:
                init_asana()
                from asana_wrapper.projects import find_project_by_name
                proj = find_project_by_name(client_name)
                if proj:
                    if dry_run:
                        results["asana"] = f"WOULD DELETE project {proj['gid']} (found by name)"
                    else:
                        import asana
                        from asana_wrapper.asana_client import get_client
                        api = asana.ProjectsApi(get_client())
                        api.delete_project(proj["gid"])
                        results["asana"] = f"DELETED project {proj['gid']} (found by name)"
                else:
                    results["asana"] = "NOT_FOUND"
            except Exception as e:
                results["asana"] = f"ERROR: {e}"
        else:
            results["asana"] = "NOT_FOUND (no GID or name)"

    # 6. Clockify project + client
    ck_step = steps.get("clockify", {})
    ck_project_id = ck_step.get("project_id")
    ck_client_id = ck_step.get("client_id")

    if ck_project_id or ck_client_id:
        init_clockify()
        from clockify_wrapper import archive_project as ck_arch_proj, delete_project as ck_del_proj
        from clockify_wrapper import archive_client as ck_arch_client, delete_client as ck_del_client

    if ck_project_id:
        try:
            if dry_run:
                results["clockify_project"] = f"WOULD DELETE {ck_project_id}"
            else:
                try:
                    ck_arch_proj(ck_project_id)
                except Exception:
                    pass  # may already be archived
                ck_del_proj(ck_project_id)
                results["clockify_project"] = f"DELETED {ck_project_id}"
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                results["clockify_project"] = "NOT_FOUND"
            else:
                results["clockify_project"] = f"ERROR: {e}"
    else:
        results["clockify_project"] = "NOT_FOUND (no ID in state)"

    if ck_client_id:
        try:
            if dry_run:
                results["clockify_client"] = f"WOULD ARCHIVE {ck_client_id}"
            else:
                ck_arch_client(ck_client_id)
                results["clockify_client"] = f"ARCHIVED {ck_client_id}"
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                results["clockify_client"] = "NOT_FOUND"
            else:
                results["clockify_client"] = f"ERROR: {e}"
    else:
        # Try finding by name
        if client_name:
            try:
                init_clockify()
                from clockify_wrapper import find_client_by_name, archive_client as ck_arch_client2
                client = find_client_by_name(client_name)
                if client:
                    if dry_run:
                        results["clockify_client"] = f"WOULD ARCHIVE {client['id']} (found by name)"
                    else:
                        ck_arch_client2(client["id"])
                        results["clockify_client"] = f"ARCHIVED {client['id']} (found by name)"
                else:
                    results["clockify_client"] = "NOT_FOUND"
            except Exception as e:
                results["clockify_client"] = f"ERROR: {e}"
        else:
            results["clockify_client"] = "NOT_FOUND (no ID or name)"

    # 7. Supabase row
    try:
        sb = get_supabase()
        check = sb.table("client_onboarding").select("id").eq("client_slug", slug).execute()
        if check.data:
            if dry_run:
                results["supabase"] = f"WOULD DELETE row for {slug}"
            else:
                sb.table("client_onboarding").delete().eq("client_slug", slug).execute()
                results["supabase"] = f"DELETED row for {slug}"
        else:
            results["supabase"] = "NOT_FOUND"
    except Exception as e:
        results["supabase"] = f"ERROR: {e}"

    # 8. Local state file
    state_path = STATE_DIR / f"{slug}.json"
    if state_path.exists():
        if dry_run:
            results["state_file"] = f"WOULD DELETE {state_path}"
        else:
            state_path.unlink()
            results["state_file"] = f"DELETED {state_path}"
    else:
        results["state_file"] = "NOT_FOUND"

    return results


def sweep(dry_run: bool = False) -> dict:
    """Scan all platforms for TEST* resources and clean them up."""
    findings = {}

    # Clockify
    try:
        init_clockify()
        from clockify_wrapper import list_clients
        clients = list_clients(name="TEST")
        if clients:
            findings["clockify_clients"] = [{"id": c["id"], "name": c["name"]} for c in clients]
        else:
            findings["clockify_clients"] = "CLEAN"
    except Exception as e:
        findings["clockify_clients"] = f"ERROR: {e}"

    # Jira — search for projects with keys used in testing
    try:
        init_jira()
        from jira_wrapper import find_project_by_key
        test_keys = ["TD", "TF", "TR", "TDP", "TFC", "TPRE"]
        found = []
        for key in test_keys:
            proj = find_project_by_key(key)
            if proj:
                found.append({"key": key, "id": proj.get("id"), "name": proj.get("name")})
        findings["jira_projects"] = found if found else "CLEAN"
    except Exception as e:
        findings["jira_projects"] = f"ERROR: {e}"

    # Asana
    try:
        init_asana()
        from asana_wrapper.search import quick_find_project
        projects = quick_find_project("TEST")
        if projects:
            findings["asana_projects"] = [{"gid": p["gid"], "name": p["name"]} for p in projects]
        else:
            findings["asana_projects"] = "CLEAN"
    except Exception as e:
        findings["asana_projects"] = f"ERROR: {e}"

    # Drive
    try:
        init_google()
        from google_wrapper.drive import search_files, GOOGLE_FOLDER
        folders = search_files(name_contains="TEST", mime_type=GOOGLE_FOLDER,
                              folder_id=DRIVE_CLIENTS_FOLDER_ID)
        if folders:
            findings["drive_folders"] = [{"id": f["id"], "name": f["name"]} for f in folders]
        else:
            findings["drive_folders"] = "CLEAN"
    except Exception as e:
        findings["drive_folders"] = f"ERROR: {e}"

    # Supabase
    try:
        sb = get_supabase()
        result = sb.table("client_onboarding").select("client_slug,status").like("client_slug", "test-%").execute()
        if result.data:
            findings["supabase_rows"] = result.data
        else:
            findings["supabase_rows"] = "CLEAN"
    except Exception as e:
        findings["supabase_rows"] = f"ERROR: {e}"

    # Local state files
    test_files = list(STATE_DIR.glob("test-*.json"))
    findings["state_files"] = [f.name for f in test_files] if test_files else "CLEAN"

    return findings


def main():
    parser = argparse.ArgumentParser(description="Clean up test artifacts from client onboarding E2E tests")
    parser.add_argument("--slug", help="State slug to clean up (e.g., test-delete-me-2026-03)")
    parser.add_argument("--jira-key", help="Jira project key to delete (e.g., TD)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    parser.add_argument("--sweep", action="store_true", help="Scan all platforms for TEST* artifacts")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.sweep:
        print("\n=== SWEEP: Scanning all platforms for TEST* artifacts ===\n")
        findings = sweep(dry_run=args.dry_run)
        all_clean = True
        for platform, status in findings.items():
            if status == "CLEAN":
                print(f"  {platform}: CLEAN")
            else:
                all_clean = False
                print(f"  {platform}: {json.dumps(status, indent=4) if isinstance(status, (list, dict)) else status}")
        if all_clean:
            print("\nAll platforms clean.")
        else:
            print("\nArtifacts found. Run cleanup for each slug, or delete manually.")
        return

    if not args.slug:
        parser.error("--slug is required (or use --sweep)")

    mode = "DRY RUN" if args.dry_run else "CLEANUP"
    print(f"\n=== {mode}: {args.slug} (jira_key={args.jira_key or 'N/A'}) ===\n")

    results = cleanup_single(args.slug, args.jira_key or "", dry_run=args.dry_run)

    errors = 0
    for platform, status in results.items():
        icon = "+" if "DELETED" in status or "WOULD" in status else ("~" if "NOT_FOUND" in status else "!")
        if "ERROR" in status:
            errors += 1
        print(f"  [{icon}] {platform}: {status}")

    print()
    if errors:
        print(f"{errors} error(s) during cleanup.")
        sys.exit(1)
    else:
        print("Cleanup complete." if not args.dry_run else "Dry run complete — no changes made.")


if __name__ == "__main__":
    main()
