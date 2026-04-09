"""Unified HTTP client factory for the extract pipeline.

All extract scripts use httpx via this module for consistent connection
pooling, timeouts, User-Agent, and redirect handling.
"""

import httpx

from pipeline.extract.config import get_config


VERSION = "4.0"


def build_user_agent(config=None) -> str:
    """Build a canonical User-Agent string from [api].email."""
    if config is None:
        config = get_config()
    email = "anonymous@example.com"
    if config is not None:
        email = config.get('api', 'email', fallback='anonymous@example.com')
    return f"data-pipeline/{VERSION} (mailto:{email})"


def create_client(config=None, *, workers: int = 1,
                  extra_headers: dict = None, timeout: float = 30.0) -> httpx.Client:
    """Create a configured httpx.Client with appropriate pool limits.

    Args:
        config: ConfigParser instance (uses global if None).
        workers: Number of concurrent workers — pool is sized to workers + 5.
        extra_headers: Additional headers merged with User-Agent (e.g. API keys).
        timeout: Request timeout in seconds.
    """
    pool_size = max(workers + 5, 10)
    limits = httpx.Limits(
        max_keepalive_connections=pool_size,
        max_connections=pool_size + 5,
    )

    headers = {"User-Agent": build_user_agent(config)}
    if extra_headers:
        headers.update(extra_headers)

    return httpx.Client(
        timeout=timeout,
        headers=headers,
        limits=limits,
        follow_redirects=True,
    )
