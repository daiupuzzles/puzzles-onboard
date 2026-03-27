"""
Supabase Wrapper - Multi-project database access via REST API and raw SQL.

Setup:
    Requires SUPABASE_* secrets in Doppler (flowsly/prd)

Usage:
    from supabase_wrapper import use_project, select, execute_sql

    use_project("ghl")  # Set context once

    # REST operations
    rows = select("table_name", limit=10)
    insert("table_name", {"col": "val"})

    # Raw SQL
    result = execute_sql("SELECT NOW() as ts")
"""

# Base
from ._base import (
    use_project,
    get_current_project,
    get_client,
    get_supabase_client,
    close_all,
    close_all_connections,
)

# REST operations
from .rest import (
    select,
    insert,
    update,
    upsert,
    delete,
    call_rpc,
)

# SQL operations
from .sql import (
    get_connection,
    get_connection_with_keepalive,
    get_connection_with_retry,
    execute_sql,
    execute,
    execute_one,
)

__all__ = [
    # Base
    "use_project",
    "get_current_project",
    "get_client",
    "get_supabase_client",
    "close_all",
    "close_all_connections",
    # REST
    "select",
    "insert",
    "update",
    "upsert",
    "delete",
    "call_rpc",
    # SQL
    "get_connection",
    "get_connection_with_keepalive",
    "get_connection_with_retry",
    "execute_sql",
    "execute",
    "execute_one",
]
