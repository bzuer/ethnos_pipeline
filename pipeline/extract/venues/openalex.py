
"""
Fetch venue metadata from the OpenAlex Sources API.

Input:  venues_list.csv (columns: venue_id, venue_name, issn, eissn, ...)
Output: venues/openalex/{issn}.json or {openalex_id}.json
"""

import argparse
import sys
import os
import json
import logging
import httpx

from pipeline.extract.config import read_config, set_config
from pipeline.extract.retry import retry_request
from pipeline.extract.http import create_client
from pipeline.extract.venues_common import load_venues_from_csv, load_pending_venues_from_db, normalize_issn, process_venues_batched
from pipeline.extract.io_utils import write_not_found_marker, has_not_found_marker, remove_not_found_marker

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


def _request_get(client, url, params, label):
    """Execute an OpenAlex request with retry/backoff. Returns response JSON or None."""
    def _attempt():
        resp = client.get(url, params=params)
        if resp.status_code == 200:
            return 'success', resp.json()
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
    issn = normalize_issn(venue.get('issn', ''))
    eissn = normalize_issn(venue.get('eissn', ''))

    prefix = f"[{index}/{total}] {venue_id} {name}"

    # Pre-check cache: ISSN-based filenames are predictable
    if not force:
        for candidate in (issn, eissn):
            if candidate and os.path.exists(os.path.join(output_dir, f"{candidate}.json")):
                logging.info(f"{prefix} → Cached")
                return 'skipped'
        if has_not_found_marker(output_dir, venue_id):
            logging.info(f"{prefix} → Skipped (previously not found)")
            return 'not_found'

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
        write_not_found_marker(output_dir, venue_id)
        return 'not_found'

    output_file = os.path.join(output_dir, f"{cache_key}.json")

    # Post-fetch cache check (for name-matched files with openalex_id keys)
    if not force and os.path.exists(output_file):
        logging.info(f"{prefix} → Cached")
        return 'skipped'

    remove_not_found_marker(output_dir, venue_id)

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

    config = read_config(args.config)
    set_config(config)
    email = config.get('openalex', 'email',
                       fallback=config.get('api', 'email', fallback='anonymous@example.com'))
    workers = config.getint('openalex', 'workers', fallback=2)
    batch_size = config.getint('openalex', 'batch_size', fallback=100)
    pause = config.getfloat('openalex', 'pause_between_batches', fallback=1.0)

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

    logging.info(f"--- OpenAlex venue collect: {len(venues)} venues, {workers} workers → {args.output_dir} ---")

    with create_client(config, workers=workers) as client:
        def _process(venue, index, total_count):
            return process_venue(client, venue, args.output_dir, args.force, index, total_count)

        totals = process_venues_batched(
            venues, process_fn=_process,
            workers=workers, batch_size=batch_size, pause=pause,
        )

    logging.info(f"=== Finished: {totals} ===")


if __name__ == "__main__":
    main()
