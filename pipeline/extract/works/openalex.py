
"""
Fetch work metadata from the OpenAlex API for missing DOIs.

Input:  works/doi/missing/ directory with per-ISSN subdirs containing
        per-year .txt files (one DOI per line), produced by missing_dois.py.

Output: works/openalex/{issn}/{sanitized_doi}.json

Primary strategy: bulk venue+year fetch (up to 200 works per API call).
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
from pipeline.extract.works_common import save_work, process_issn_generic, EXCLUDED_WORK_TYPES
from pipeline.extract.common import resolve_openalex_source_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_works_openalex.log"),
        logging.StreamHandler(),
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)

BULK_API_URL = "https://api.openalex.org/works"
SINGLE_API_URL = "https://api.openalex.org/works/"
CACHE_DIR = "works/openalex"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_oa_doi(raw: str) -> str:
    """Normalize an OpenAlex DOI field (URL form) to bare lowercase DOI."""
    if not raw:
        return ''
    raw = raw.strip().lower()
    for prefix in ('https://doi.org/', 'http://doi.org/'):
        if raw.startswith(prefix):
            return raw[len(prefix):]
    return raw


def _save_oa_work(work: dict, doi: str, entity_dir: str, tag: str) -> str:
    return save_work(work, doi, entity_dir, tag, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Bulk fetch: all works for a venue+year
# ---------------------------------------------------------------------------

def fetch_venue_year(client: httpx.Client, source_id: str, year: str,
                     per_page: int = 200, page_pause: float = 0.1) -> list:
    """Fetch all works for a venue+year via paginated bulk API call."""
    import time
    all_works = []
    page = 1

    while True:
        params = {
            'filter': f'primary_location.source.id:{source_id},publication_year:{year}',
            'per_page': per_page,
            'page': page,
        }

        def _attempt():
            resp = client.get(BULK_API_URL, params=params)
            if resp.status_code == 200:
                return 'success', resp.json()
            if resp.status_code == 429:
                return 'retry_429', None
            return 'retry', None

        data = retry_request(_attempt, label=f"source:{source_id} year:{year} p{page}")
        if data is None:
            return all_works

        results = data.get('results', [])
        all_works.extend(results)
        meta = data.get('meta', {})
        logging.info(f"  year {year}: {len(results)} works (total: {meta.get('count', '?')})")

        if len(results) < per_page:
            return all_works
        page += 1
        time.sleep(page_pause)

    return all_works


# ---------------------------------------------------------------------------
# Individual DOI fetch (fallback)
# ---------------------------------------------------------------------------

_NOT_FOUND = object()


def fetch_single_doi(client: httpx.Client, doi: str, entity_dir: str) -> str:
    """Fetch a single DOI from OpenAlex and save to cache."""
    safe = sanitize_doi_for_filename(doi)
    output_file = os.path.join(entity_dir, f"{safe}.json")
    if os.path.exists(output_file):
        return 'Cached'

    url = f"{SINGLE_API_URL}https://doi.org/{doi}"

    def _attempt():
        response = client.get(url)
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

    return _save_oa_work(data, doi, entity_dir, tag='')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch work metadata from OpenAlex for missing DOIs (bulk by venue+year)."
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
    email = config.get('api', 'email', fallback='anonymous@example.com')

    workers = config.getint('openalex', 'workers', fallback=1)
    batch_size = config.getint('openalex', 'batch_size', fallback=0)
    pause = config.getfloat('openalex', 'pause_between_batches', fallback=0)

    os.makedirs(CACHE_DIR, exist_ok=True)
    logging.info(f"Output: {CACHE_DIR}/ | {workers} workers, batch_size={batch_size}, pause={pause}s")
    logging.info(f"Excluded types: {', '.join(sorted(EXCLUDED_WORK_TYPES))}")

    issns = discover_issns(args.input_dir, args.limit)
    logging.info(f"Processing {len(issns)} ISSNs from {args.input_dir}")

    # Single DB connection for resolve_openalex_source_id across all ISSNs
    db_conn = None
    try:
        from pipeline.common import get_connection
        db_conn = get_connection(args.config)
    except Exception as e:
        logging.warning(f"Could not open DB connection for source ID resolution: {e}")

    try:
        with create_client(config, workers=workers) as client:
            for i, issn in enumerate(issns, 1):
                source_id_holder = [None]

                def _pre_bulk():
                    nonlocal db_conn
                    if db_conn is not None:
                        from pipeline.common import ensure_connection
                        db_conn = ensure_connection(
                            db_conn,
                            config_path=args.config,
                            read_timeout=300,
                            write_timeout=300,
                            retries=5,
                            retry_base=1.0,
                            retry_max=12.0,
                        )
                    sid = resolve_openalex_source_id(
                        issn,
                        http_client=client,
                        conn=db_conn,
                        config_path=args.config,
                    )
                    source_id_holder[0] = sid
                    if not sid:
                        logging.warning(f"[{issn}] no OpenAlex source ID — all DOIs go to individual fallback")
                        return False
                    return True

                process_issn_generic(
                    issn, args.input_dir, CACHE_DIR, args.force,
                    bulk_fetch_fn=lambda year, _missing, _c=client:
                        fetch_venue_year(_c, source_id_holder[0], year),
                    doi_extractor=lambda w: _normalize_oa_doi(w.get('doi')),
                    save_fn=_save_oa_work,
                    single_fetch_fn=lambda doi, edir, _c=client:
                        fetch_single_doi(_c, doi, edir),
                    pre_bulk_hook=_pre_bulk,
                    workers=workers,
                    batch_size=batch_size,
                    pause_between_batches=pause,
                )
                if i % 50 == 0:
                    logging.info(f"Progress: {i}/{len(issns)} ISSNs")
    finally:
        if db_conn is not None:
            try:
                db_conn.close()
            except Exception:
                pass

    logging.info("All ISSNs processed.")


if __name__ == "__main__":
    main()
