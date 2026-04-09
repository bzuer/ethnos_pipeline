
"""
Fetch work metadata from the Crossref API for missing DOIs.

Input:  works/doi/missing/ directory with per-ISSN subdirs containing
        per-year .txt files (one DOI per line), produced by missing_dois.py.

Output: works/crossref/{issn}/{sanitized_doi}.json

Primary strategy: bulk ISSN+year fetch via Crossref /works endpoint.
Fallback: individual DOI fetch for unmatched DOIs.
"""

import argparse
import sys
import os
import logging
import httpx

from pipeline.extract.config import read_config, set_config
from pipeline.extract.retry import retry_request
from pipeline.extract.http import create_client
from pipeline.extract.io_utils import sanitize_doi_for_filename, discover_issns, record_not_found_doi
from pipeline.extract.works_common import (
    EXCLUDED_WORK_TYPES, save_work, process_issn_generic,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CACHE_DIR = "works/crossref"
CROSSREF_WORKS_URL = "https://api.crossref.org/works"


# ---------------------------------------------------------------------------
# Crossref envelope wrapper
# ---------------------------------------------------------------------------

def _crossref_envelope(message: dict) -> dict:
    return {
        "status": "ok",
        "message-type": "work",
        "message-version": "1.0.0",
        "message": message,
    }


def _save_crossref_work(message: dict, doi: str, entity_dir: str, tag: str) -> str:
    return save_work(message, doi, entity_dir, tag, envelope_fn=_crossref_envelope)


# ---------------------------------------------------------------------------
# Bulk fetch: all works for an ISSN+year via Crossref /works?filter=
# ---------------------------------------------------------------------------

def fetch_issn_year(client: httpx.Client, issn: str, year: str,
                    rows: int = 1000) -> list:
    """Fetch all works for ISSN+year using cursor-based pagination."""
    all_items = []
    cursor = '*'

    while True:
        params = {
            'filter': f'issn:{issn},from-pub-date:{year},until-pub-date:{year}',
            'rows': rows,
            'cursor': cursor,
        }

        def _attempt():
            resp = client.get(CROSSREF_WORKS_URL, params=params, timeout=60)
            if resp.status_code == 200:
                return 'success', resp.json()
            if resp.status_code == 429:
                return 'retry_429', None
            return 'retry', None

        data = retry_request(_attempt, label=f"issn:{issn} year:{year}")
        if data is None:
            return all_items

        items = data.get('message', {}).get('items', [])
        all_items.extend(items)
        total = data.get('message', {}).get('total-results', '?')
        logging.info(f"  year {year}: {len(items)} works (total: {total})")

        if not items:
            return all_items

        next_cursor = data.get('message', {}).get('next-cursor')
        if not next_cursor or len(items) < rows:
            return all_items
        cursor = next_cursor

    return all_items


# ---------------------------------------------------------------------------
# Individual DOI fetch (fallback)
# ---------------------------------------------------------------------------

_NOT_FOUND = object()


def fetch_single_doi(client: httpx.Client, doi: str, entity_dir: str) -> str:
    """Fetch a single DOI from Crossref and save to cache."""
    safe = sanitize_doi_for_filename(doi)
    cache_path = os.path.join(entity_dir, f"{safe}.json")
    if os.path.exists(cache_path):
        return 'Cached'

    url = f"https://api.crossref.org/works/{doi}"

    def _attempt():
        response = client.get(url, timeout=30)
        if response.status_code == 200:
            return 'success', response.json()
        if response.status_code == 404:
            return 'not_found', _NOT_FOUND
        if response.status_code == 429:
            return 'retry_429', None
        return 'retry', None

    data = retry_request(_attempt, label=doi)
    if data is _NOT_FOUND:
        logging.info(f"{doi} -> not found (404)")
        record_not_found_doi(entity_dir, doi)
        return 'Not Found'
    if data is None:
        logging.error(f"{doi} -> failed")
        return 'Failed'

    # Delegate filtering and saving to the shared path (same as bulk).
    # The API response message IS the work dict; _save_crossref_work wraps it.
    msg = data.get('message', {})
    return _save_crossref_work(msg, doi, entity_dir, 'individual')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch work metadata from Crossref for missing DOIs (bulk by ISSN+year)."
    )
    parser.add_argument("input_dir",
                        help="Directory with per-ISSN subdirs of missing DOIs (works/doi/missing).")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini.")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch even if entity output dir already exists.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max ISSNs to process (0 = unlimited).")
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        logging.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    config = read_config(args.config)
    set_config(config)

    workers = config.getint('crossref', 'workers', fallback=1)
    batch_size = config.getint('crossref', 'batch_size', fallback=0)
    pause = config.getfloat('crossref', 'pause_between_batches', fallback=0)

    os.makedirs(CACHE_DIR, exist_ok=True)
    logging.info(f"Output: {CACHE_DIR}/ | {workers} workers, batch_size={batch_size}, pause={pause}s")
    logging.info(f"Excluded types: {', '.join(sorted(EXCLUDED_WORK_TYPES))}")

    issns = discover_issns(args.input_dir, args.limit)
    logging.info(f"Processing {len(issns)} ISSNs from {args.input_dir}")

    client = create_client(config, workers=workers, timeout=60)
    for i, issn in enumerate(issns, 1):
        process_issn_generic(
            issn, args.input_dir, CACHE_DIR, args.force,
            bulk_fetch_fn=lambda year, _missing, _c=client, _issn=issn:
                fetch_issn_year(_c, _issn, year),
            doi_extractor=lambda item: item.get('DOI', '').lower(),
            save_fn=_save_crossref_work,
            single_fetch_fn=lambda doi, edir, _c=client:
                fetch_single_doi(_c, doi, edir),
            workers=workers,
            batch_size=batch_size,
            pause_between_batches=pause,
        )
        if i % 50 == 0:
            logging.info(f"Progress: {i}/{len(issns)} ISSNs")

    logging.info("All ISSNs processed.")


if __name__ == "__main__":
    main()
