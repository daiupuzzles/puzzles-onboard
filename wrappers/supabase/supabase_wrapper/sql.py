"""
Supabase raw SQL operations — psycopg2 connections with pooler fallback.

Connection fallback chain:
  1. Direct IPv6 (if DIRECT_HOST set)
  2. Transaction pooler (port 6543)
  3. Session pooler (port 5432)
  4. n8n webhook (if both poolers fail)
"""

import logging
from typing import Any, Dict, List, Optional

from supabase_wrapper._base import (
    _get_config,
    _get_connections,
    _port_overrides,
    _resolve_project,
    TRANSACTION_POOLER_PORT,
    SESSION_POOLER_PORT,
    N8N_WEBHOOK_BASE,
    N8N_SQL_WEBHOOKS,
    logger,
)


def get_connection(project: str = None):
    """Get a direct database connection (psycopg2).

    Tries direct IPv6 first (if DIRECT_HOST set), then transaction pooler
    (port 6543), then session pooler (port 5432). Caches the working
    method for subsequent calls.

    Args:
        project: Project identifier. Uses current context if not specified.

    Returns:
        psycopg2 connection object
    """
    import psycopg2

    project = _resolve_project(project)

    _connections = _get_connections()
    if project in _connections:
        try:
            _connections[project].cursor().execute("SELECT 1")
            return _connections[project]
        except Exception:
            try:
                _connections[project].close()
            except Exception:
                pass
            del _connections[project]

    config = _get_config(project)

    # Try direct connection first (IPv6 — works on Windmill/cloud, not locally)
    direct_host = config.get("direct_host")
    if direct_host and project not in _port_overrides:
        try:
            conn = psycopg2.connect(
                user=config.get("direct_user", "postgres"),
                password=config["password"],
                host=direct_host,
                port=5432,
                dbname=config["dbname"],
                connect_timeout=5
            )
            _port_overrides[project] = "direct"
            _connections[project] = conn
            logger.info("Direct Supabase connection succeeded (%s).", direct_host)
            return conn
        except (psycopg2.OperationalError, OSError) as e:
            logger.debug("Direct connection failed (%s): %s. Falling back to pooler.", direct_host, e)

    # If direct was previously successful, reconnect via direct
    if _port_overrides.get(project) == "direct" and direct_host:
        conn = psycopg2.connect(
            user=config.get("direct_user", "postgres"),
            password=config["password"],
            host=direct_host,
            port=5432,
            dbname=config["dbname"],
            connect_timeout=10
        )
        _connections[project] = conn
        return conn

    primary_port = _port_overrides.get(project, config.get("port", TRANSACTION_POOLER_PORT))
    fallback_port = SESSION_POOLER_PORT if primary_port == TRANSACTION_POOLER_PORT else TRANSACTION_POOLER_PORT

    # Try primary port
    _primary_error = None
    try:
        conn = psycopg2.connect(
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(primary_port),
            dbname=config["dbname"],
            connect_timeout=10
        )
        _connections[project] = conn
        return conn
    except (psycopg2.OperationalError, OSError) as e:
        _primary_error = e
        logger.warning("Supabase pooler port %s failed: %s. Trying fallback port %s...",
                       primary_port, e, fallback_port)

    # Try fallback port
    try:
        conn = psycopg2.connect(
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(fallback_port),
            dbname=config["dbname"],
            connect_timeout=10
        )
        _port_overrides[project] = fallback_port
        _connections[project] = conn
        logger.info("Supabase fallback to port %s succeeded. Cached for this session.", fallback_port)
        return conn
    except (psycopg2.OperationalError, OSError) as fallback_err:
        raise ConnectionError(
            f"Both Supabase poolers failed for project '{project}'. "
            f"Transaction pooler ({primary_port}): {_primary_error} | "
            f"Session pooler ({fallback_port}): {fallback_err}"
        ) from fallback_err


def get_connection_with_keepalive(
    project: str = None,
    keepalives_idle: int = 60,
    keepalives_interval: int = 10,
    keepalives_count: int = 5,
    connect_timeout: int = 15,
):
    """Get a direct database connection with TCP keepalive enabled.

    Use this instead of get_connection() for long-running batch jobs
    where the connection may sit idle between operations.

    Args:
        project: Project identifier. Uses current context if not specified.
        keepalives_idle: Seconds idle before first probe (default 60).
        keepalives_interval: Seconds between probes (default 10).
        keepalives_count: Failed probes before disconnect (default 5).
        connect_timeout: Connection timeout in seconds (default 15).

    Returns:
        psycopg2 connection object with keepalive enabled.
    """
    import psycopg2

    project = _resolve_project(project)
    config = _get_config(project)

    keepalive_opts = dict(
        keepalives=1,
        keepalives_idle=keepalives_idle,
        keepalives_interval=keepalives_interval,
        keepalives_count=keepalives_count,
    )

    primary_port = _port_overrides.get(project, config.get("port", TRANSACTION_POOLER_PORT))
    fallback_port = SESSION_POOLER_PORT if primary_port == TRANSACTION_POOLER_PORT else TRANSACTION_POOLER_PORT

    _primary_err = None
    try:
        return psycopg2.connect(
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(primary_port),
            dbname=config["dbname"],
            connect_timeout=connect_timeout,
            **keepalive_opts,
        )
    except (psycopg2.OperationalError, OSError) as exc:
        _primary_err = exc
        logger.warning("Supabase port %s failed: %s. Trying %s...", primary_port, exc, fallback_port)

    try:
        conn = psycopg2.connect(
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(fallback_port),
            dbname=config["dbname"],
            connect_timeout=connect_timeout,
            **keepalive_opts,
        )
        _port_overrides[project] = fallback_port
        logger.info("Supabase fallback to port %s succeeded (keepalive).", fallback_port)
        return conn
    except (psycopg2.OperationalError, OSError) as fallback_err:
        raise ConnectionError(
            f"Both Supabase poolers failed for project '{project}'. "
            f"Transaction pooler ({primary_port}): {_primary_err} | "
            f"Session pooler ({fallback_port}): {fallback_err}"
        ) from fallback_err


def get_connection_with_retry(
    project: str = None,
    max_attempts: int = 5,
    base_delay: float = 10.0,
    keepalive: bool = True,
    **keepalive_kwargs,
):
    """Get a database connection with exponential backoff retry.

    Retry schedule (defaults): 10s, 20s, 40s, 80s, 160s (~5 min total).

    Args:
        project: Project identifier. Uses current context if not specified.
        max_attempts: Max connection attempts (default 5).
        base_delay: Initial delay in seconds, doubles each attempt (default 10).
        keepalive: If True, use get_connection_with_keepalive (default True).
        **keepalive_kwargs: Passed to get_connection_with_keepalive if keepalive=True.

    Returns:
        psycopg2 connection object.

    Raises:
        ConnectionError: If all attempts fail.
    """
    import psycopg2
    import time as _time

    connect_fn = (
        lambda: get_connection_with_keepalive(project, **keepalive_kwargs)
        if keepalive
        else lambda: get_connection(project)
    )

    for attempt in range(1, max_attempts + 1):
        try:
            return connect_fn()
        except (ConnectionError, psycopg2.OperationalError, OSError) as exc:
            if attempt == max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning(
                "DB connect attempt %d/%d failed: %s — retrying in %.0fs...",
                attempt, max_attempts, exc, delay,
            )
            _time.sleep(delay)


def _render_sql_params(sql: str, params) -> str:
    """Render psycopg2-style %s params into a raw SQL string for webhook fallback."""
    if not params:
        return sql

    def _quote(val):
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, (list, tuple)):
            return "ARRAY[" + ", ".join(_quote(v) for v in val) + "]"
        s = str(val).replace("'", "''")
        return f"'{s}'"

    rendered = [_quote(p) for p in params]

    result = sql
    for r in rendered:
        result = result.replace("%s", r, 1)
    return result


def _execute_via_webhook(sql: str, params=None, project: str = None, fetch: bool = True) -> Optional[List[Dict]]:
    """Execute SQL via n8n webhook (third-priority fallback)."""
    import requests as _requests

    webhook_path = N8N_SQL_WEBHOOKS.get(project)
    if not webhook_path:
        raise ConnectionError(
            f"No n8n webhook configured for project '{project}'. "
            f"Available: {list(N8N_SQL_WEBHOOKS.keys())}"
        )

    rendered_sql = _render_sql_params(sql, params)
    url = f"{N8N_WEBHOOK_BASE}/{webhook_path}"

    logger.info("Webhook fallback: POST %s (project=%s)", url, project)
    resp = _requests.post(url, json={"sql": rendered_sql}, timeout=30)

    if resp.status_code >= 400:
        raise ConnectionError(f"n8n webhook failed ({resp.status_code}): {resp.text[:300]}")

    if not fetch:
        return None

    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def execute_sql(sql: str, params: tuple = None, project: str = None, fetch: bool = True) -> Optional[List[Dict]]:
    """Execute raw SQL query.

    Tries connections in order:
      1. Primary pooler port (transaction, 6543)
      2. Fallback pooler port (session, 5432)
      3. n8n webhook (if both poolers fail)

    Args:
        sql: SQL query string
        params: Query parameters (tuple)
        project: Project identifier
        fetch: Whether to fetch results (False for DDL/DML without RETURNING)

    Returns:
        List of dicts for SELECT queries, None otherwise
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    project = _resolve_project(project)

    for attempt in range(2):
        try:
            conn = get_connection(project)
        except ConnectionError:
            logger.warning("Both poolers unreachable for '%s', trying n8n webhook fallback...", project)
            return _execute_via_webhook(sql, params, project, fetch)

        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cur.execute(sql, params)

            if fetch:
                results = cur.fetchall()
                conn.commit()
                return [dict(row) for row in results]
            else:
                conn.commit()
                return None
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            cur.close()
            if attempt == 0:
                logger.warning("SQL execution failed (connection error): %s. Reconnecting...", e)
                _connections = _get_connections()
                if project in _connections:
                    try:
                        _connections[project].close()
                    except Exception:
                        pass
                    del _connections[project]
                continue
            logger.warning("Pooler connection failed after retry for '%s', trying n8n webhook fallback...", project)
            return _execute_via_webhook(sql, params, project, fetch)
        finally:
            if not cur.closed:
                cur.close()


# Aliases
execute = execute_sql
execute_one = lambda sql, params=None, project=None: (execute_sql(sql, params, project) or [None])[0]
