"""Venue-specific domain logic for the extract pipeline.

Venue loading (CSV and DB) and the shared batch-processing loop used by both
Scopus and OpenAlex venue extractors.
"""

import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

from pipeline.common import normalize_issn  # noqa: F401 — re-exported for convenience


logger = logging.getLogger(__name__)


def load_venues_from_csv(filepath: str) -> List[Dict[str, str]]:
    """Read a venues CSV file. Returns list of dicts with stripped values."""
    venues = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            venues.append({k.strip(): (v.strip() if v else '') for k, v in row.items()})
    return venues


def load_pending_venues_from_db(config_path: Optional[str] = None) -> List[Dict[str, str]]:
    """Load venues with validation_status = 'PENDING' from the database.

    Returns list of dicts with keys: venue_id, venue_name, issn, eissn
    (same shape as load_venues_from_csv output).
    """
    from pipeline.common import get_connection
    conn = get_connection(config_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, name, issn, eissn FROM venues "
            "WHERE validation_status = 'PENDING' ORDER BY id"
        )
        venues = []
        for row in cursor.fetchall():
            venues.append({
                'venue_id': str(row[0]),
                'venue_name': row[1] or '',
                'issn': row[2] or '',
                'eissn': row[3] or '',
            })
        cursor.close()
        logger.info(f"Loaded {len(venues)} PENDING venues from database.")
        return venues
    finally:
        conn.close()


def process_venues_batched(
    venues: List[Dict[str, str]],
    *,
    process_fn: Callable,
    workers: int = 2,
    batch_size: int = 100,
    pause: float = 1.0,
) -> Dict[str, int]:
    """Process venues in batches with concurrent workers.

    *process_fn(venue, index, total)* is called for each venue and must return
    a status string ('success', 'skipped', 'not_found', 'failed').

    Returns totals dict with counts per status.
    """
    total = len(venues)
    total_batches = (total + batch_size - 1) // batch_size

    totals = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

    for i in range(0, total, batch_size):
        batch = venues[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        batch_stats = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

        logger.info(f"--- Batch {batch_num}/{total_batches} ({len(batch)} venues) ---")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_fn, venue, i + j + 1, total): venue
                for j, venue in enumerate(batch)
            }
            for future in as_completed(futures):
                venue = futures[future]
                try:
                    status = future.result()
                    batch_stats[status] = batch_stats.get(status, 0) + 1
                except Exception as e:
                    logging.error(f"{venue.get('venue_id')} → Error: {e}")
                    batch_stats['failed'] += 1

        for k in totals:
            totals[k] += batch_stats[k]

        logger.info(f"--- Batch {batch_num} done: {batch_stats} | Total so far: {totals} ---")
        if batch_num < total_batches:
            time.sleep(pause)

    return totals
