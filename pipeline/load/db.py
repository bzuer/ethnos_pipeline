"""Database connection helpers for the load pipeline.

Delegates to pipeline.common for connection/reconnect behavior.
"""

import logging
from typing import Optional

import mariadb

from pipeline.common import (  # noqa: F401 — re-exported
    get_connection,
    read_db_config,
    ensure_connection as _ensure_connection,
)


def ensure_connection(conn: Optional[mariadb.Connection],
                      config_path: Optional[str] = None,
                      **connect_kwargs) -> mariadb.Connection:
    """Return a healthy connection, reconnecting via pipeline.common when needed."""
    return _ensure_connection(conn, config_path=config_path, **connect_kwargs)


def safe_rollback(connection, label: str) -> bool:
    """Try to rollback, closing connection on failure. Returns True on success."""
    if not connection or not connection.open:
        return False
    try:
        connection.rollback()
        return True
    except Exception as e:
        logging.error(f"{label} rollback failed: {e}")
        try:
            connection.close()
        except Exception:
            pass
        return False
