"""Database connection helpers for the load pipeline.

Delegates to pipeline.common.get_connection for the actual connection
(with socket discovery + TCP fallback). Adds load-specific helpers:
ensure_connection (reconnect on stale) and safe_rollback.
"""

import logging
from typing import Optional

import mariadb

from pipeline.common import get_connection, read_db_config  # noqa: F401 — re-exported


def ensure_connection(conn: Optional[mariadb.Connection],
                      config_path: Optional[str] = None) -> mariadb.Connection:
    """Return *conn* if still alive, otherwise reconnect via get_connection."""
    if conn and conn.open:
        ping = getattr(conn, "ping", None)
        if callable(ping):
            try:
                ping()
                return conn
            except mariadb.Error:
                try:
                    conn.close()
                except mariadb.Error:
                    pass
        else:
            cursor = None
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return conn
            except mariadb.Error:
                try:
                    conn.close()
                except mariadb.Error:
                    pass
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except mariadb.Error:
                        pass
    return get_connection(config_path)


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
