"""Lakebase helpers — reusable connection patterns.

Importable from any module notebook:

    from Includes.lakebase_helpers import (
        get_oauth_engine, refresh_token_in_engine,
        wait_for_instance, dsn_for_instance,
    )

This is a plain Python module (no Databricks notebook header) so the import
resolves reliably under Workspace Files. The functions wrap SDK calls
(`databricks.sdk.WorkspaceClient.database`) plus SQLAlchemy engine factories
with auto-refreshing OAuth tokens.
"""
from __future__ import annotations
import time
import threading
from typing import Optional

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from databricks.sdk import WorkspaceClient


# ----------------------------------------------------------------------------
# Wait helpers
# ----------------------------------------------------------------------------
def wait_for_instance(
    instance_name: str,
    state: str = "AVAILABLE",
    timeout_seconds: int = 600,
    poll_seconds: int = 10,
    w: Optional[WorkspaceClient] = None,
) -> dict:
    """Block until the Lakebase instance reaches the given state.

    Returns the instance object once available; raises TimeoutError otherwise.
    """
    w = w or WorkspaceClient()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        info = w.database.get_database_instance(name=instance_name)
        cur = info.state.value if info.state else "UNKNOWN"
        if cur == state:
            return info
        if cur == "FAILED":
            raise RuntimeError(f"Instance {instance_name} entered FAILED state")
        time.sleep(poll_seconds)
    raise TimeoutError(f"{instance_name} did not reach {state} within {timeout_seconds}s")


# ----------------------------------------------------------------------------
# DSN building
# ----------------------------------------------------------------------------
def dsn_for_instance(
    instance_name: str,
    database: str = "postgres",
    sslmode: str = "require",
    w: Optional[WorkspaceClient] = None,
) -> str:
    """Return a psycopg-style DSN with a fresh OAuth token."""
    w = w or WorkspaceClient()
    info = w.database.get_database_instance(name=instance_name)
    user = w.current_user.me().user_name
    token = w.database.generate_database_credential(
        request_id=f"helper-{int(time.time())}",
        instance_names=[instance_name],
    ).token
    host = info.read_write_dns
    return (f"postgresql://{user}:{token}@{host}:5432/{database}"
            f"?sslmode={sslmode}")


# ----------------------------------------------------------------------------
# SQLAlchemy engine with auto-refreshing OAuth token
# ----------------------------------------------------------------------------
def get_oauth_engine(
    instance_name: str,
    database: str = "postgres",
    refresh_buffer_seconds: int = 300,  # refresh 5 min before expiry
    w: Optional[WorkspaceClient] = None,
) -> Engine:
    """Return a SQLAlchemy Engine that auto-refreshes its OAuth token.

    Lakebase OAuth tokens expire after 1 hour. For long-running connections,
    we hook into SQLAlchemy's `do_connect` event to refresh the token if it's
    near expiry, ensuring connection-pool checkouts always have a live token.

    Use this for production code paths. For one-shot demos, plain DSN is fine.
    """
    w = w or WorkspaceClient()
    user = w.current_user.me().user_name
    info = w.database.get_database_instance(name=instance_name)
    host = info.read_write_dns

    state = {"token": None, "expires_at": 0.0}
    lock = threading.Lock()

    def _maybe_refresh() -> str:
        with lock:
            if (state["token"] is None
                    or time.time() > state["expires_at"] - refresh_buffer_seconds):
                cred = w.database.generate_database_credential(
                    request_id=f"engine-refresh-{int(time.time())}",
                    instance_names=[instance_name],
                )
                state["token"] = cred.token
                # Tokens are 1-hour by default; pad slightly to be safe.
                state["expires_at"] = time.time() + 3600
            return state["token"]

    # Start with a token so the engine constructs without error
    initial_token = _maybe_refresh()

    engine = create_engine(
        f"postgresql+psycopg://{user}:{initial_token}@{host}:5432/{database}"
        f"?sslmode=require",
        pool_pre_ping=True,
        pool_recycle=1800,  # cycle pooled conns every 30 min — re-uses fresh token
    )

    @event.listens_for(engine, "do_connect")
    def _refresh_password(dialect, conn_rec, cargs, cparams):
        # Replace password with a freshly-checked token at connect time
        cparams["password"] = _maybe_refresh()

    return engine


def refresh_token_in_engine(engine: Engine):
    """Force-clear the connection pool so the next checkout uses a fresh token.

    Useful if you suspect token issues. Calls do_connect on next use.
    """
    engine.dispose()
