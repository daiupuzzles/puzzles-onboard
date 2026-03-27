"""
Supabase REST API operations — CRUD via the Supabase Python SDK.

Works through firewalls (no direct DB access needed).
"""

from typing import Any, Dict, List, Optional

from supabase_wrapper._base import get_client


def select(table: str, columns: str = "*", where: dict = None,
           order_by: str = None, limit: int = None, project: str = None) -> List[Dict]:
    """Select rows from a table using REST API.

    Args:
        table: Table name
        columns: Columns to select (default "*")
        where: Dict of column:value filters (eq matching)
        order_by: Column to order by (prefix with - for desc)
        limit: Max rows to return
        project: Project identifier

    Returns:
        List of dicts
    """
    client = get_client(project)
    query = client.table(table).select(columns)

    if where:
        for col, val in where.items():
            query = query.eq(col, val)

    if order_by:
        desc = order_by.startswith("-")
        col = order_by[1:] if desc else order_by
        query = query.order(col, desc=desc)

    if limit:
        query = query.limit(limit)

    result = query.execute()
    return result.data


def insert(table: str, data: dict, project: str = None) -> Optional[Dict]:
    """Insert a row using REST API.

    Args:
        table: Table name
        data: Dict of column:value pairs
        project: Project identifier

    Returns:
        Inserted row as dict
    """
    client = get_client(project)
    result = client.table(table).insert(data).execute()
    return result.data[0] if result.data else None


def update(table: str, data: dict, where: dict, project: str = None) -> List[Dict]:
    """Update rows using REST API.

    Args:
        table: Table name
        data: Dict of column:value pairs to update
        where: Dict of column:value filters
        project: Project identifier

    Returns:
        Updated rows
    """
    client = get_client(project)
    query = client.table(table).update(data)

    for col, val in where.items():
        query = query.eq(col, val)

    result = query.execute()
    return result.data


def upsert(table: str, data: dict, project: str = None) -> Optional[Dict]:
    """Upsert (insert or update) a row using REST API.

    Args:
        table: Table name
        data: Dict of column:value pairs
        project: Project identifier

    Returns:
        Upserted row as dict
    """
    client = get_client(project)
    result = client.table(table).upsert(data).execute()
    return result.data[0] if result.data else None


def delete(table: str, where: dict, project: str = None) -> List[Dict]:
    """Delete rows using REST API.

    Args:
        table: Table name
        where: Dict of column:value filters
        project: Project identifier

    Returns:
        Deleted rows
    """
    client = get_client(project)
    query = client.table(table).delete()

    for col, val in where.items():
        query = query.eq(col, val)

    result = query.execute()
    return result.data


def call_rpc(function_name: str, params: dict = None, project: str = None) -> Any:
    """Call a Supabase RPC function.

    Args:
        function_name: Name of the function
        params: Dict of parameter names to values
        project: Project identifier

    Returns:
        Function result
    """
    client = get_client(project)
    result = client.rpc(function_name, params or {}).execute()
    return result.data
