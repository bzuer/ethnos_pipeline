import configparser
import os
from typing import Any, Dict, Optional

import mariadb


DEFAULT_SOCKET_CANDIDATES = (
    "/run/mysqld/mysqld.sock",
    "/var/run/mysqld/mysqld.sock",
    "/var/lib/mysql/mysql.sock",
    "/tmp/mysql.sock",
)


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
        raise FileNotFoundError("Arquivo de configuração não encontrado.")
    if not parser.has_section(section):
        raise KeyError(f"Seção [{section}] não encontrada no config.")

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


def _prepare_connection_params(db_config: Dict[str, Any]) -> Dict[str, Any]:
    params = {k: v for k, v in db_config.items() if k not in {"charset", "collation"}}

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


def get_connection(config_path: Optional[str] = None) -> mariadb.Connection:
    db_config = read_db_config(config_path=config_path)
    primary = _prepare_connection_params(db_config)

    try:
        conn = mariadb.connect(**primary, connect_timeout=20, read_timeout=60, write_timeout=60)
    except mariadb.Error:
        fallback = {k: v for k, v in db_config.items() if k not in {"charset", "collation", "unix_socket"}}
        host = str(fallback.get("host", "")).strip().lower()
        if host in {"", "localhost", "127.0.0.1"}:
            fallback["host"] = "127.0.0.1"
            fallback.setdefault("port", 3306)
            conn = mariadb.connect(**fallback, connect_timeout=20, read_timeout=60, write_timeout=60)
        else:
            raise

    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_uca1400_ai_ci'")
    cur.close()
    return conn
