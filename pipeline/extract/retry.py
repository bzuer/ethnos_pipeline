"""Retry, rate-limiting, and fail-command infrastructure for the extract pipeline."""

import logging
import subprocess
import time
import threading

from pipeline.extract.config import get_config


logger = logging.getLogger(__name__)

MAX_RETRIES = 4

# Global rate-limit gate: when any thread gets a 429, all threads pause.
_rate_limit_lock = threading.Lock()
_rate_limit_until = 0.0  # monotonic timestamp — all threads wait until this


def _wait_for_rate_limit():
    """Block until any active rate-limit cooldown expires."""
    while True:
        with _rate_limit_lock:
            remaining = _rate_limit_until - time.monotonic()
        if remaining <= 0:
            return
        logger.debug(f"Rate-limit gate: waiting {remaining:.1f}s")
        time.sleep(remaining)


def _set_rate_limit(seconds: float):
    """Signal all threads to pause for *seconds*."""
    global _rate_limit_until
    with _rate_limit_lock:
        new_until = time.monotonic() + seconds
        if new_until > _rate_limit_until:
            _rate_limit_until = new_until


# Serialize fail-command execution across threads.
_fail_lock = threading.Lock()
_fail_cooldown = 0.0  # timestamp until which we skip re-running


def run_fail_command(config=None, *, cooldown: float = 30.0) -> None:
    """Run [fail].command from config.ini (e.g. VPN reconnect).

    Thread-safe: only one thread runs the command at a time; others that arrive
    within *cooldown* seconds of the last execution just wait out the remaining
    cooldown instead of re-running.
    """
    global _fail_cooldown

    if config is None:
        config = get_config()
    if config is None:
        logger.warning("No config available — cannot run fail command.")
        return

    cmd = config.get("fail", "command", fallback=None)
    if not cmd:
        logger.warning("No [fail].command configured — cannot recover from persistent errors.")
        return

    with _fail_lock:
        now = time.monotonic()
        if now < _fail_cooldown:
            remaining = _fail_cooldown - now
            logger.info(f"Fail command already ran recently, waiting {remaining:.0f}s cooldown...")
            time.sleep(remaining)
            return

        logger.warning(f"Running fail command: {cmd}")
        try:
            subprocess.run(cmd, shell=True, timeout=60, check=False)
        except Exception as e:
            logger.error(f"Fail command error: {e}")
        _fail_cooldown = time.monotonic() + cooldown
        time.sleep(cooldown)


def retry_request(attempt_fn, *, max_retries: int = MAX_RETRIES, label: str = ""):
    """Retry an HTTP request with exponential backoff and 429 handling.

    *attempt_fn()* should perform one HTTP call and return a tuple ``(result, disposition)``:
      - ``('success', value)``   → return *value* immediately.
      - ``('not_found', value)`` → return *value* immediately (e.g. None).
      - ``('retry', None)``      → back off and retry.
      - ``('retry_429', None)``  → back off, retry, and flag for fail-command.

    When a 429 is received, the global rate-limit gate is set so that all
    concurrent threads pause before their next request.

    Returns the successful *value*, or *None* if all retries are exhausted.
    """
    got_429 = False
    for attempt in range(1, max_retries + 1):
        _wait_for_rate_limit()
        try:
            disposition, value = attempt_fn()
        except Exception as e:
            logger.warning(f"  {label}: error {e} ({attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
            continue

        if disposition == 'success':
            return value
        if disposition == 'not_found':
            return value
        if disposition == 'retry_429':
            got_429 = True
            wait = min(11 * (2 ** attempt), 120)
            logger.warning(f"  {label}: 429 rate-limit, all threads pausing {wait}s ({attempt}/{max_retries})")
            _set_rate_limit(wait)
            time.sleep(wait)
            continue
        # 'retry' or anything else
        logger.warning(f"  {label}: failed ({attempt}/{max_retries})")
        if attempt < max_retries:
            time.sleep(2 ** attempt)

    logger.error(f"  {label}: exhausted after {max_retries} attempts")
    if got_429:
        run_fail_command()
    return None
