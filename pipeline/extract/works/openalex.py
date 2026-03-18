
"""
Fetch work metadata from the OpenAlex API for a list of DOIs.

Input:  a directory of .txt files (one DOI per line), typically produced by
        missing_dois.py.  Each file is named after a venue/entity.

Output: works/openalex/{entity}/{sanitized_doi}.json
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
from pipeline.extract.common import (
    read_config, load_dois_from_file, sanitize_doi_for_filename,
    parse_entity_from_filename,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("extract_works_openalex.log"),
        logging.StreamHandler(),
    ]
)

BASE_API_URL = "https://api.openalex.org/works/"
CACHE_DIR = "works/openalex"

EXCLUDED_TYPES = {
    'editorial', 'erratum', 'corrigendum', 'note', 'letter',
    'retraction', 'publisher-note', 'book-review', 'errata',
    'acknowledgments', 'journal-issue',
}


def fetch_and_save(client: httpx.Client, doi: str, entity_dir: str) -> str:
    safe = sanitize_doi_for_filename(doi)
    output_file = os.path.join(entity_dir, f"{safe}.json")

    if os.path.exists(output_file):
        return 'skipped'

    url = f"{BASE_API_URL}https://doi.org/{doi}"
    try:
        response = client.get(url)
        if response.status_code == 404:
            logging.warning(f"DOI not found on OpenAlex: {doi}")
            return 'failed'
        if response.status_code != 200:
            logging.error(f"HTTP {response.status_code} for {doi}")
            return 'failed'

        data = response.json()
        if data.get('type') in EXCLUDED_TYPES:
            logging.info(f"Filtered {doi} (type: {data.get('type')})")
            return 'filtered'

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return 'success'

    except Exception as e:
        logging.error(f"Error fetching {doi}: {e}")
        return 'failed'


def process_entity(client: httpx.Client, input_dir: str, filename: str,
                   workers: int, batch_size: int, pause: float):
    entity_name = parse_entity_from_filename(filename)
    if not entity_name:
        logging.warning(f"Cannot parse entity from '{filename}'. Skipping.")
        return

    entity_dir = os.path.join(CACHE_DIR, entity_name)
    os.makedirs(entity_dir, exist_ok=True)

    filepath = os.path.join(input_dir, filename)
    dois = list(load_dois_from_file(filepath))
    if not dois:
        logging.info(f"No DOIs in '{filename}'. Skipping.")
        return

    logging.info(f"--- Entity: {entity_name} | {len(dois)} unique DOIs ---")

    for i in range(0, len(dois), batch_size):
        batch = dois[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(dois) + batch_size - 1) // batch_size

        logging.info(f"Batch {batch_num}/{total_batches} ({len(batch)} DOIs) for {entity_name}")

        results = {'success': 0, 'skipped': 0, 'failed': 0, 'filtered': 0}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(fetch_and_save, client, doi, entity_dir): doi
                for doi in batch
            }
            for future in as_completed(futures):
                status = future.result()
                results[status] = results.get(status, 0) + 1

        logging.info(f"Batch {batch_num} done: {results}")
        if batch_num < total_batches:
            time.sleep(pause)

    logging.info(f"--- Done: {entity_name} ---")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch work metadata from OpenAlex for DOI lists."
    )
    parser.add_argument("input_dir",
                        help="Directory with .txt files of DOIs (one per line).")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini.")
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        logging.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    config = read_config(args.config)
    email = config.get('api', 'email', fallback='anonymous@example.com')
    workers = config.getint('openalex', 'workers', fallback=1)
    batch_size = config.getint('openalex', 'batch_size', fallback=100_000_000)
    pause = config.getfloat('openalex', 'pause_between_batches', fallback=1.0)

    os.makedirs(CACHE_DIR, exist_ok=True)

    limits = httpx.Limits(max_keepalive_connections=workers + 5, max_connections=workers + 10)
    headers = {"User-Agent": f"DataCollector/4.0 (mailto:{email})"}

    with httpx.Client(timeout=30.0, headers=headers, limits=limits, follow_redirects=True) as client:
        for filename in sorted(os.listdir(args.input_dir)):
            if filename.endswith('.txt'):
                process_entity(client, args.input_dir, filename, workers, batch_size, pause)

    logging.info("All entities processed.")


if __name__ == "__main__":
    main()
