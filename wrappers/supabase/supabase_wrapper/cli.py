"""
CLI entrypoint for supabase-sql — run SQL against remote Supabase projects.

Usage:
    supabase-sql -p co "SELECT * FROM organizations LIMIT 5"
    supabase-sql -p ghl --json "SELECT NOW()"
    supabase-sql -p co --no-fetch "INSERT INTO ..."
    echo "SELECT 1" | supabase-sql -p co
"""

import argparse
import json
import sys

from supabase_wrapper._base import use_project
from supabase_wrapper.sql import execute_sql


def main():
    parser = argparse.ArgumentParser(
        prog="supabase-sql",
        description="Execute SQL against remote Supabase projects via psycopg2 with pooler fallback.",
    )
    parser.add_argument(
        "-p", "--project",
        required=True,
        help="Project identifier (e.g., ghl, dillingus, co)",
    )
    parser.add_argument(
        "-n", "--no-fetch",
        action="store_true",
        help="Don't fetch results (for DDL/DML without RETURNING)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output as JSON (default: table format)",
    )
    parser.add_argument(
        "sql",
        nargs="?",
        default=None,
        help="SQL query (reads from stdin if omitted)",
    )

    args = parser.parse_args()

    # Get SQL from arg or stdin
    sql = args.sql
    if sql is None:
        if sys.stdin.isatty():
            parser.error("No SQL provided. Pass as argument or pipe via stdin.")
        sql = sys.stdin.read().strip()

    if not sql:
        parser.error("Empty SQL query.")

    # Set project context
    use_project(args.project)

    # Execute
    fetch = not args.no_fetch
    try:
        results = execute_sql(sql, project=args.project, fetch=fetch)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not fetch or results is None:
        print("OK")
        return

    if not results:
        print("(0 rows)")
        return

    # Output
    if args.json_output:
        print(json.dumps(results, indent=2, default=str))
    else:
        _print_table(results)


def _print_table(rows):
    """Print results as a simple aligned table."""
    if not rows:
        return

    columns = list(rows[0].keys())

    # Calculate column widths
    widths = {col: len(col) for col in columns}
    str_rows = []
    for row in rows:
        str_row = {}
        for col in columns:
            val = row[col]
            s = "NULL" if val is None else str(val)
            str_row[col] = s
            widths[col] = max(widths[col], len(s))
        str_rows.append(str_row)

    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    print(header)
    print(separator)

    # Rows
    for str_row in str_rows:
        line = " | ".join(str_row[col].ljust(widths[col]) for col in columns)
        print(line)

    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


if __name__ == "__main__":
    main()
