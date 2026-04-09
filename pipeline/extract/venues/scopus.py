
"""
Fetch venue metadata from the Scopus Serial Title API.

Input:  venues_list.csv (columns: venue_id, venue_name, issn, eissn, ...)
Output: venues/scopus/{venue_id}.json
"""

import argparse
import sys
import os
import json
import logging

from pipeline.extract.config import read_config, set_config
from pipeline.extract.retry import retry_request
from pipeline.extract.http import create_client
from pipeline.extract.venues_common import load_venues_from_csv, load_pending_venues_from_db, normalize_issn, process_venues_batched
from pipeline.extract.io_utils import write_not_found_marker, has_not_found_marker, remove_not_found_marker

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


def _has_valid_entries(data):
    """Check if Scopus API response contains actual serial entries."""
    smr = data.get("serial-metadata-response") if isinstance(data, dict) else None
    if not isinstance(smr, dict):
        return False
    if smr.get("error"):
        return False
    entries = smr.get("entry")
    return isinstance(entries, list) and len(entries) > 0


def _request(client, params, label):
    """Execute a Scopus API request with retry/backoff. Returns JSON dict or None."""
    def _attempt():
        resp = client.get(BASE_API_URL, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if _has_valid_entries(data):
                return 'success', data
            return 'not_found', None
        if resp.status_code == 404:
            return 'not_found', None
        if resp.status_code == 429:
            return 'retry_429', None
        return 'retry', None
    return retry_request(_attempt, label=label)


def process_venue(client, venue, output_dir, force, index, total):
    """Fetch metadata for a single venue via ISSN -> eISSN -> name cascade."""
    venue_id = venue['venue_id']
    name = venue.get('venue_name', '').strip()
    output_file = os.path.join(output_dir, f"{venue_id}.json")

    prefix = f"[{index}/{total}] {venue_id} {name}"

    if not force and os.path.exists(output_file):
        logging.info(f"{prefix} → Cached")
        return 'skipped'

    if not force and has_not_found_marker(output_dir, venue_id):
        logging.info(f"{prefix} → Skipped (previously not found)")
        return 'not_found'

    issn = normalize_issn(venue.get('issn', ''))
    eissn = normalize_issn(venue.get('eissn', ''))

    data = None
    matched_by = None

    # Cascade: ISSN -> eISSN -> name
    if issn:
        data = _request(client, {"issn": issn}, f"issn={issn}")
        if data:
            matched_by = f"issn={issn}"
    if not data and eissn:
        data = _request(client, {"issn": eissn}, f"eissn={eissn}")
        if data:
            matched_by = f"eissn={eissn}"
    if not data and name:
        data = _request(client, {"title": name}, f"title={name}")
        if data:
            matched_by = f"title"

    if not data:
        logging.info(f"{prefix} → Not found")
        write_not_found_marker(output_dir, venue_id)
        return 'not_found'

    remove_not_found_marker(output_dir, venue_id)

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

    config = read_config(args.config)
    set_config(config)
    scopus_headers = build_headers(config)

    workers = config.getint('scopus', 'workers', fallback=2)
    batch_size = config.getint('scopus', 'batch_size', fallback=100)
    pause = config.getfloat('scopus', 'pause_between_batches', fallback=1.0)

    os.makedirs(args.output_dir, exist_ok=True)

    if args.force:
        if not os.path.isfile(args.input_file):
            logging.error(f"Input file does not exist: {args.input_file}")
            sys.exit(1)
        venues = load_venues_from_csv(args.input_file)
    else:
        venues = load_pending_venues_from_db(config_path=args.config)
    if args.limit > 0:
        venues = venues[:args.limit]

    logging.info(f"--- Scopus venue collect: {len(venues)} venues, {workers} workers → {args.output_dir} ---")

    with create_client(config, workers=workers, extra_headers=scopus_headers) as client:
        def _process(venue, index, total_count):
            return process_venue(client, venue, args.output_dir, args.force, index, total_count)

        totals = process_venues_batched(
            venues, process_fn=_process,
            workers=workers, batch_size=batch_size, pause=pause,
        )

    logging.info(f"=== Finished: {totals} ===")


if __name__ == "__main__":
    main()
