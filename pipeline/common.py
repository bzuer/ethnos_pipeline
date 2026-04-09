import configparser
import logging
import os
import re
import time
from typing import Any, Dict, Optional

import mariadb


log = logging.getLogger(__name__)


def normalize_issn(raw) -> Optional[str]:
    """Validate and normalize an ISSN to XXXX-XXXX format. Returns None if invalid."""
    if not raw:
        return None
    compact = re.sub(r"[\s\-]", "", str(raw).strip().upper())
    if not re.fullmatch(r"[0-9X]{8}", compact):
        return None
    return f"{compact[:4]}-{compact[4:]}"


DEFAULT_SOCKET_CANDIDATES = (
    "/run/mysqld/mysqld.sock",
    "/var/run/mysqld/mysqld.sock",
    "/var/lib/mysql/mysql.sock",
    "/tmp/mysql.sock",
)

RUNTIME_DB_KEYS = {
    "connect_timeout",
    "read_timeout",
    "write_timeout",
    "connection_retries",
    "connection_retry_base",
    "connection_retry_max",
    "session_wait_timeout",
    "session_net_read_timeout",
    "session_net_write_timeout",
    "session_innodb_lock_wait_timeout",
    "session_tmp_table_size",
    "session_max_heap_table_size",
    # legacy names (accepted for compatibility)
    "wait_timeout",
    "net_read_timeout",
    "net_write_timeout",
    "innodb_lock_wait_timeout",
    "tmp_table_size",
    "max_heap_table_size",
}

TRANSIENT_CONNECTION_ERRNOS = {
    1040,  # too many connections
    1205,  # lock wait timeout (often transient in busy DBs)
    2002,  # can't connect to local server
    2003,  # can't connect to server
    2006,  # server has gone away
    2013,  # lost connection during query
    2055,  # lost connection / bad handshake
}


def _resolve_config_candidates(config_path: Optional[str]) -> list[str]:
    if config_path:
        return [config_path]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    return [
        os.path.join(script_dir, "config.ini"),
        os.path.join(repo_root, "config.ini"),
    ]


def read_db_config(config_path: Optional[str] = None, section: str = "database") -> Dict[str, Any]:
    parser = configparser.ConfigParser()
    found = False
    for candidate in _resolve_config_candidates(config_path):
        if candidate and parser.read(candidate):
            found = True
            break
    if not found:
        raise FileNotFoundError("Config file not found.")
    if not parser.has_section(section):
        raise KeyError(f"Section [{section}] not found in config.")

    config = dict(parser.items(section))
    if "port" in config:
        config["port"] = int(config["port"])
    if "name" in config:
        config["database"] = config.pop("name")
    return config


def _discover_socket() -> Optional[str]:
    for socket_path in DEFAULT_SOCKET_CANDIDATES:
        if os.path.exists(socket_path):
            return socket_path
    return None


def _coerce_int(raw_value: Any, default: int, minimum: int = 1) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    return max(value, minimum)


def _coerce_float(raw_value: Any, default: float, minimum: float = 0.0) -> float:
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return default
    return max(value, minimum)


def _coerce_optional_int(raw_value: Any, minimum: int = 1) -> Optional[int]:
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    try:
        value = int(text)
    except ValueError:
        return None
    return max(value, minimum)


def _extract_runtime_options(db_config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "connect_timeout": _coerce_int(db_config.get("connect_timeout"), 20, 1),
        "read_timeout": _coerce_int(db_config.get("read_timeout"), 60, 1),
        "write_timeout": _coerce_int(db_config.get("write_timeout"), 60, 1),
        "retries": _coerce_int(db_config.get("connection_retries"), 4, 1),
        "retry_base": _coerce_float(db_config.get("connection_retry_base"), 1.0, 0.1),
        "retry_max": _coerce_float(db_config.get("connection_retry_max"), 8.0, 0.5),
    }


def _extract_session_options(db_config: Dict[str, Any]) -> Dict[str, int]:
    option_map = {
        "session_wait_timeout": "wait_timeout",
        "session_net_read_timeout": "net_read_timeout",
        "session_net_write_timeout": "net_write_timeout",
        "session_innodb_lock_wait_timeout": "innodb_lock_wait_timeout",
        "session_tmp_table_size": "tmp_table_size",
        "session_max_heap_table_size": "max_heap_table_size",
        # legacy aliases
        "wait_timeout": "wait_timeout",
        "net_read_timeout": "net_read_timeout",
        "net_write_timeout": "net_write_timeout",
        "innodb_lock_wait_timeout": "innodb_lock_wait_timeout",
        "tmp_table_size": "tmp_table_size",
        "max_heap_table_size": "max_heap_table_size",
    }
    session_options: Dict[str, int] = {}
    for cfg_key, session_key in option_map.items():
        value = _coerce_optional_int(db_config.get(cfg_key))
        if value is not None:
            session_options[session_key] = value
    return session_options


def _strip_non_connect_keys(db_config: Dict[str, Any]) -> Dict[str, Any]:
    excluded = {"charset", "collation"}
    excluded.update(RUNTIME_DB_KEYS)
    return {k: v for k, v in db_config.items() if k not in excluded}


def _prepare_connection_params(db_config: Dict[str, Any]) -> Dict[str, Any]:
    params = _strip_non_connect_keys(db_config)

    if params.get("unix_socket"):
        return params

    host = str(params.get("host", "")).strip().lower()
    if host in {"", "localhost"}:
        socket_path = _discover_socket()
        if socket_path:
            params["unix_socket"] = socket_path
            params.pop("host", None)
            params.pop("port", None)
            return params
        if host == "localhost":
            params["host"] = "127.0.0.1"
            params.setdefault("port", 3306)
    return params


def _prepare_tcp_fallback_params(db_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fallback = _strip_non_connect_keys(db_config)
    host = str(fallback.get("host", "")).strip().lower()
    if host not in {"", "localhost", "127.0.0.1"}:
        return None
    fallback["host"] = "127.0.0.1"
    fallback.setdefault("port", 3306)
    fallback.pop("unix_socket", None)
    return fallback


def _is_transient_connection_error(exc: Exception) -> bool:
    errno = getattr(exc, "errno", None)
    if errno in TRANSIENT_CONNECTION_ERRNOS:
        return True
    message = str(exc).lower()
    transient_markers = (
        "too many connections",
        "server has gone away",
        "lost connection",
        "can't connect",
        "connection refused",
        "timed out",
        "temporarily unavailable",
    )
    return any(marker in message for marker in transient_markers)


def _is_connection_open(conn: Optional[mariadb.Connection]) -> bool:
    if conn is None:
        return False
    try:
        return bool(conn.open)
    except Exception:
        return False


def _connect_once(
    db_config: Dict[str, Any],
    connect_timeout: int,
    read_timeout: int,
    write_timeout: int,
) -> mariadb.Connection:
    primary = _prepare_connection_params(db_config)
    try:
        return mariadb.connect(
            **primary,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
        )
    except mariadb.Error as primary_error:
        fallback = _prepare_tcp_fallback_params(db_config)
        if fallback is None:
            raise
        try:
            return mariadb.connect(
                **fallback,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
            )
        except mariadb.Error as fallback_error:
            raise fallback_error from primary_error


def _initialize_connection(conn: mariadb.Connection, session_options: Dict[str, int]) -> None:
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_uca1400_ai_ci'")
        if session_options:
            assignments = ", ".join(f"{key}={value}" for key, value in session_options.items())
            cur.execute(f"SET SESSION {assignments}")
    finally:
        cur.close()


def get_connection(
    config_path: Optional[str] = None,
    *,
    connect_timeout: Optional[int] = None,
    read_timeout: Optional[int] = None,
    write_timeout: Optional[int] = None,
    retries: Optional[int] = None,
    retry_base: Optional[float] = None,
    retry_max: Optional[float] = None,
    session_options: Optional[Dict[str, int]] = None,
) -> mariadb.Connection:
    """Create a MariaDB connection with retry/backoff and session initialization.

    Runtime defaults come from [database] config keys when present:
      connect_timeout, read_timeout, write_timeout,
      connection_retries, connection_retry_base, connection_retry_max,
      session_wait_timeout, session_net_read_timeout, session_net_write_timeout,
      session_innodb_lock_wait_timeout, session_tmp_table_size, session_max_heap_table_size.
    """
    db_config = read_db_config(config_path=config_path)
    runtime = _extract_runtime_options(db_config)

    ct = connect_timeout if connect_timeout is not None else runtime["connect_timeout"]
    rt = read_timeout if read_timeout is not None else runtime["read_timeout"]
    wt = write_timeout if write_timeout is not None else runtime["write_timeout"]
    total_retries = retries if retries is not None else runtime["retries"]
    base_sleep = retry_base if retry_base is not None else runtime["retry_base"]
    max_sleep = retry_max if retry_max is not None else runtime["retry_max"]

    total_retries = max(1, int(total_retries))
    merged_session_options = _extract_session_options(db_config)
    if session_options:
        merged_session_options.update(session_options)

    last_error: Optional[Exception] = None
    for attempt in range(1, total_retries + 1):
        try:
            conn = _connect_once(
                db_config=db_config,
                connect_timeout=int(ct),
                read_timeout=int(rt),
                write_timeout=int(wt),
            )
            _initialize_connection(conn, merged_session_options)
            return conn
        except mariadb.Error as exc:
            last_error = exc
            is_transient = _is_transient_connection_error(exc)
            if attempt >= total_retries or not is_transient:
                raise
            sleep_seconds = min(float(base_sleep) * (2 ** (attempt - 1)), float(max_sleep))
            log.warning(
                "DB connect failed (attempt %d/%d, transient=%s): %s; retrying in %.1fs",
                attempt, total_retries, is_transient, exc, sleep_seconds,
            )
            time.sleep(sleep_seconds)

    if last_error:
        raise last_error
    raise RuntimeError("Failed to open DB connection for unknown reason.")


def ensure_connection(
    conn: Optional[mariadb.Connection],
    config_path: Optional[str] = None,
    **connect_kwargs,
) -> mariadb.Connection:
    """Return a healthy open connection, reconnecting when needed."""
    if _is_connection_open(conn):
        cursor = None
        try:
            ping = getattr(conn, "ping", None)
            if callable(ping):
                ping()
            else:
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
    return get_connection(config_path=config_path, **connect_kwargs)
