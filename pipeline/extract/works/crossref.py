
"""
Fetch work metadata from the Crossref API for a list of DOIs.

Input:  a directory of .txt files (one DOI per line), typically produced by
        missing_dois.py.  Each file is named after a venue/entity.

Output: works/crossref/{entity}/{sanitized_doi}.json
"""

import argparse
import sys
import os
import json
import time
import logging
import requests
import concurrent.futures
from typing import Set, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from pipeline.extract.common import (
    read_config, load_dois_from_file, sanitize_doi_for_filename,
    parse_entity_from_filename,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CACHE_DIR = "works/crossref"

EXCLUDED_TYPES = {
    'editorial', 'erratum', 'corrigendum', 'note', 'letter',
    'retraction', 'publisher-note', 'book-review', 'errata',
    'acknowledgments', 'journal-issue',
}


def fetch_single_doi(doi: str, entity_cache_dir: str, user_agent: str) -> Tuple[str, str]:
    """Fetch a single DOI from Crossref and save to cache."""
    safe = sanitize_doi_for_filename(doi)
    cache_path = os.path.join(entity_cache_dir, f"{safe}.json")

    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            if cached.get('status') == 'filtered' and cached.get('type') not in EXCLUDED_TYPES:
                pass  # re-fetch: was filtered by a rule that no longer applies
            else:
                return doi, "Cached"
        except (json.JSONDecodeError, Exception):
            pass  # corrupt cache — re-fetch

    try:
        url = f"https://api.crossref.org/works/{doi}"
        response = requests.get(url, timeout=30, headers={'User-Agent': user_agent})

        if response.status_code == 200:
            data = response.json()
            pub_type = data.get('message', {}).get('type')
            if pub_type in EXCLUDED_TYPES:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump({"status": "filtered", "doi": doi, "type": pub_type}, f)
                return doi, "Filtered"
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            return doi, "Success"
        elif response.status_code == 404:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump({"status": "not-found", "doi": doi}, f)
            return doi, "Not Found"
        else:
            logging.error(f"HTTP {response.status_code} for {doi}")
            return doi, "Failed"
    except requests.exceptions.RequestException as e:
        logging.error(f"Connection error for {doi}: {e}")
        return doi, "Failed"


def process_entity(input_dir: str, filename: str, config, user_agent: str):
    entity_name = parse_entity_from_filename(filename)
    if not entity_name:
        logging.warning(f"Cannot parse entity from '{filename}'. Skipping.")
        return

    workers = config.getint('crossref', 'workers', fallback=24)
    batch_size = config.getint('crossref', 'batch_size', fallback=1_000_000_000)
    pause = config.getint('crossref', 'pause_between_batches', fallback=5)

    entity_cache_dir = os.path.join(CACHE_DIR, entity_name)
    os.makedirs(entity_cache_dir, exist_ok=True)

    filepath = os.path.join(input_dir, filename)
    dois = load_dois_from_file(filepath)
    if not dois:
        logging.info(f"No DOIs in '{filename}'. Skipping.")
        return

    logging.info(f"--- Entity: {entity_name} | {len(dois)} DOIs ---")

    dois_list = sorted(dois)
    batches = [dois_list[i:i + batch_size] for i in range(0, len(dois_list), batch_size)]

    for i, batch in enumerate(batches, 1):
        logging.info(f"Batch {i}/{len(batches)} for '{entity_name}' ({len(batch)} DOIs)")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(
                lambda d: fetch_single_doi(d, entity_cache_dir, user_agent), batch
            ))
        stats = {}
        for _, status in results:
            stats[status] = stats.get(status, 0) + 1
        logging.info(f"Batch {i} done: {stats}")
        if i < len(batches):
            time.sleep(pause)

    logging.info(f"--- Done: {entity_name} ---")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch work metadata from Crossref for DOI lists."
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
    user_agent = f"DoiEnricher/4.0 (mailto:{email})"

    os.makedirs(CACHE_DIR, exist_ok=True)
    logging.info(f"Excluded types: {', '.join(sorted(EXCLUDED_TYPES))}")

    for filename in sorted(os.listdir(args.input_dir)):
        if filename.endswith('.txt'):
            process_entity(args.input_dir, filename, config, user_agent)

    logging.info("All entities processed.")


if __name__ == "__main__":
    main()
