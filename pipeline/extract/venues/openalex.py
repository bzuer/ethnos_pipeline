
"""
Fetch venue metadata from the OpenAlex Sources API.

Input:  venues_list.csv (columns: venue_id, venue_name, issn, eissn, ...)
Output: venues/openalex/{issn}.json or {openalex_id}.json
"""

import argparse
import sys
import os
import json
import time
import logging
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from pipeline.extract.common import read_config, load_venues_from_csv, normalize_issn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_venues_openalex.log"),
        logging.StreamHandler(),
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

BASE_API_URL = "https://api.openalex.org/sources"
DEFAULT_OUTPUT_DIR = "venues/openalex"
MAX_RETRIES = 3


def _request_get(client, url, params, label):
    """Execute an OpenAlex request with retry/backoff. Returns response JSON or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                wait = 2 ** attempt
                logging.warning(f"  429 rate-limit on {label}, retry in {wait}s ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            logging.warning(f"  HTTP {resp.status_code} on {label} ({attempt}/{MAX_RETRIES})")
        except httpx.HTTPError as e:
            logging.warning(f"  Request error on {label}: {e} ({attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    return None


def process_venue(client, venue, output_dir, force, index, total):
    """Fetch metadata for a single venue via ISSN -> eISSN -> name cascade."""
    venue_id = venue['venue_id']
    name = venue.get('venue_name', '').strip()
    issn = normalize_issn(venue.get('issn', ''))
    eissn = normalize_issn(venue.get('eissn', ''))

    prefix = f"[{index}/{total}] {venue_id} {name}"

    # Pre-check cache: ISSN-based filenames are predictable
    if not force:
        for candidate in (issn, eissn):
            if candidate and os.path.exists(os.path.join(output_dir, f"{candidate}.json")):
                logging.info(f"{prefix} → Cached")
                return 'skipped'

    data = None
    cache_key = None
    matched_by = None

    # Cascade: ISSN -> eISSN -> name
    if issn:
        data = _request_get(client, f"{BASE_API_URL}/issn:{issn}", None, f"issn:{issn}")
        if data:
            cache_key = issn
            matched_by = f"issn={issn}"
    if not data and eissn:
        data = _request_get(client, f"{BASE_API_URL}/issn:{eissn}", None, f"issn:{eissn}")
        if data:
            cache_key = eissn
            matched_by = f"eissn={eissn}"
    if not data and name:
        search_data = _request_get(client, BASE_API_URL, {"search": name}, f"search={name}")
        if search_data:
            results = search_data.get('results', [])
            if results:
                data = results[0]
                cache_key = data.get('id', '').replace('https://openalex.org/', '')
                matched_by = "name"

    if not data or not cache_key:
        logging.info(f"{prefix} → Not found")
        return 'not_found'

    output_file = os.path.join(output_dir, f"{cache_key}.json")

    # Post-fetch cache check (for name-matched files with openalex_id keys)
    if not force and os.path.exists(output_file):
        logging.info(f"{prefix} → Cached")
        return 'skipped'

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"{prefix} → OK ({matched_by})")
    return 'success'


def main():
    parser = argparse.ArgumentParser(
        description="Fetch venue metadata from OpenAlex Sources API."
    )
    parser.add_argument("--input-file", default="venues_list.csv",
                        help="CSV file with venue_id, venue_name, issn, eissn columns.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help="Output directory for JSON cache files.")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini.")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if cache file exists.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max venues to process (0 = unlimited).")
    args = parser.parse_args()

    if not os.path.isfile(args.input_file):
        logging.error(f"Input file does not exist: {args.input_file}")
        sys.exit(1)

    config = read_config(args.config)
    email = config.get('openalex', 'email',
                       fallback=config.get('api', 'email', fallback='anonymous@example.com'))
    workers = config.getint('openalex', 'workers', fallback=2)
    batch_size = config.getint('openalex', 'batch_size', fallback=100)
    pause = config.getfloat('openalex', 'pause_between_batches', fallback=1.0)

    os.makedirs(args.output_dir, exist_ok=True)

    venues = load_venues_from_csv(args.input_file)
    if args.limit > 0:
        venues = venues[:args.limit]

    total = len(venues)
    total_batches = (total + batch_size - 1) // batch_size

    logging.info(f"--- OpenAlex venue collect: {total} venues, {total_batches} batches, {workers} workers → {args.output_dir} ---")

    limits = httpx.Limits(max_keepalive_connections=workers + 5, max_connections=workers + 10)
    headers = {"User-Agent": f"VenueCollector/1.0 (mailto:{email})"}

    totals = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

    with httpx.Client(timeout=30.0, headers=headers, limits=limits, follow_redirects=True) as client:
        for i in range(0, total, batch_size):
            batch = venues[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            batch_stats = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

            logging.info(f"--- Batch {batch_num}/{total_batches} ({len(batch)} venues) ---")

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(process_venue, client, venue, args.output_dir,
                                    args.force, i + j + 1, total): venue
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

            logging.info(f"--- Batch {batch_num} done: {batch_stats} | Total so far: {totals} ---")
            if batch_num < total_batches:
                time.sleep(pause)

    logging.info(f"=== Finished: {totals} ===")


if __name__ == "__main__":
    main()
