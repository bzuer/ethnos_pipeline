"""Configuration management for the extract pipeline."""

import os
import configparser
from typing import Optional


# Module-level config, set once via set_config() from each script's main().
_config: Optional[configparser.ConfigParser] = None


def set_config(config: configparser.ConfigParser) -> None:
    """Store config for use by retry, HTTP, and other modules."""
    global _config
    _config = config


def get_config() -> Optional[configparser.ConfigParser]:
    return _config


def read_config(filename: str = 'config.ini') -> configparser.ConfigParser:
    """Read config.ini, searching script dir then repo root."""
    parser = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    candidates = [
        filename if os.path.isabs(filename) else None,
        os.path.join(script_dir, filename),
        os.path.join(repo_root, filename),
    ]
    for path in candidates:
        if path and parser.read(path):
            return parser
    raise FileNotFoundError(f"Config '{filename}' not found.")
