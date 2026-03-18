
"""
Fetch venue metadata from the Scopus Serial Title API.

Input:  venues_list.csv (columns: venue_id, venue_name, issn, eissn, ...)
Output: venues/scopus/{venue_id}.json
"""

import argparse
import sys
import os
import json
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from pipeline.extract.common import read_config, load_venues_from_csv, normalize_issn

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_venues_scopus.log"),
        logging.StreamHandler(),
    ]
)

BASE_API_URL = "https://api.elsevier.com/content/serial/title"
DEFAULT_OUTPUT_DIR = "venues/scopus"
MAX_RETRIES = 3


def build_headers(config):
    api_key = config.get('api', 'scopus_api_key', fallback=None)
    if not api_key:
        api_key = config.get('scopus', 'api_key', fallback=None)
    if not api_key:
        logging.error("No Scopus API key found in config (api.scopus_api_key or scopus.api_key)")
        sys.exit(1)
    headers = {
        "X-ELS-APIKey": api_key,
        "Accept": "application/json",
    }
    inst_token = config.get('scopus', 'inst_token', fallback=None)
    if inst_token:
        headers["X-ELS-Insttoken"] = inst_token
    return headers


def _request(session, params, label):
    """Execute a Scopus API request with retry/backoff. Returns JSON dict or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_API_URL, params=params, timeout=30)
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
        except requests.exceptions.RequestException as e:
            logging.warning(f"  Request error on {label}: {e} ({attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    return None


def process_venue(session, venue, output_dir, force, index, total):
    """Fetch metadata for a single venue via ISSN -> eISSN -> name cascade."""
    venue_id = venue['venue_id']
    name = venue.get('venue_name', '').strip()
    output_file = os.path.join(output_dir, f"{venue_id}.json")

    prefix = f"[{index}/{total}] {venue_id} {name}"

    if not force and os.path.exists(output_file):
        logging.info(f"{prefix} → Cached")
        return 'skipped'

    issn = normalize_issn(venue.get('issn', ''))
    eissn = normalize_issn(venue.get('eissn', ''))

    data = None
    matched_by = None

    # Cascade: ISSN -> eISSN -> name
    if issn:
        data = _request(session, {"issn": issn}, f"issn={issn}")
        if data:
            matched_by = f"issn={issn}"
    if not data and eissn:
        data = _request(session, {"issn": eissn}, f"eissn={eissn}")
        if data:
            matched_by = f"eissn={eissn}"
    if not data and name:
        data = _request(session, {"title": name}, f"title={name}")
        if data:
            matched_by = f"title"

    if not data:
        logging.info(f"{prefix} → Not found")
        return 'not_found'

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"{prefix} → OK ({matched_by})")
    return 'success'


def main():
    parser = argparse.ArgumentParser(
        description="Fetch venue metadata from Scopus Serial Title API."
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
    headers = build_headers(config)

    workers = config.getint('scopus', 'workers', fallback=2)
    batch_size = config.getint('scopus', 'batch_size', fallback=100)
    pause = config.getfloat('scopus', 'pause_between_batches', fallback=1.0)

    os.makedirs(args.output_dir, exist_ok=True)

    venues = load_venues_from_csv(args.input_file)
    if args.limit > 0:
        venues = venues[:args.limit]

    total = len(venues)
    total_batches = (total + batch_size - 1) // batch_size

    logging.info(f"--- Scopus venue collect: {total} venues, {total_batches} batches, {workers} workers → {args.output_dir} ---")

    session = requests.Session()
    session.headers.update(headers)

    totals = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

    for i in range(0, total, batch_size):
        batch = venues[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        batch_stats = {'success': 0, 'skipped': 0, 'not_found': 0, 'failed': 0}

        logging.info(f"--- Batch {batch_num}/{total_batches} ({len(batch)} venues) ---")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_venue, session, venue, args.output_dir,
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
