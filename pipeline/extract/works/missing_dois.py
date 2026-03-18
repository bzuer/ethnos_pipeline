
"""
Identify missing DOIs per venue by comparing Crossref's journal catalog against
the local database.

DOI cache layout (preserves data across re-runs of different year ranges):

    works/doi/cache/
    └── {issn}/
        └── {year}.txt          # one DOI per line

Output:

    works/doi/missing/
    └── issn_{issn}_missing.txt # DOIs present on Crossref but absent from DB
"""

import argparse
import sys
import csv
import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, List, Dict, Any

from habanero import Crossref
from httpx import HTTPError, RequestError

# allow running from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from pipeline.common import get_connection
from pipeline.extract.common import read_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_RETRIES = 4
BACKOFF_FACTOR = 5
USER_AGENT_TEMPLATE = "DoiChecker/4.0 (mailto:{email}; project: data_db enrichment)"
DOI_CACHE_DIR = 'works/doi/cache'
OUTPUT_DIR = "works/doi/missing"


class DoiChecker:
    def __init__(self, config, from_year: int = None, until_year: int = None):
        self.api_email = config.get('api', 'email', fallback='anonymous@example.com')
        user_agent = USER_AGENT_TEMPLATE.format(email=self.api_email)
        self.crossref_client = Crossref(mailto=self.api_email, ua_string=user_agent, timeout=30)
        self.from_year = from_year
        self.until_year = until_year

        os.makedirs(DOI_CACHE_DIR, exist_ok=True)
        logging.info(f"Crossref client initialized. DOI cache at '{DOI_CACHE_DIR}'.")

    # ------------------------------------------------------------------
    # Crossref DOI fetching — results partitioned by publication year
    # ------------------------------------------------------------------

    def _fetch_crossref_dois(self, issn: str) -> Set[str]:
        """Fetch DOIs from Crossref for the given ISSN, partitioned by year in cache."""
        issn_cache_dir = os.path.join(DOI_CACHE_DIR, issn)
        os.makedirs(issn_cache_dir, exist_ok=True)

        # build filter string for Crossref
        filters = {}
        if self.from_year:
            filters['from-pub-date'] = str(self.from_year)
        if self.until_year:
            filters['until-pub-date'] = str(self.until_year)

        # check if we already have cache files covering the requested range
        cached_dois = self._load_cached_dois(issn_cache_dir)
        if cached_dois is not None:
            logging.info(f"Using cached DOIs for ISSN {issn} ({len(cached_dois)} DOIs).")
            return cached_dois

        logging.info(f"Fetching DOIs for ISSN {issn} from Crossref (filters: {filters})...")
        for attempt in range(MAX_RETRIES):
            try:
                kwargs = dict(
                    ids=issn, works=True, limit=1000,
                    select="DOI,published-print,published-online,issued",
                    cursor="*", cursor_max=100000,
                )
                if filters:
                    kwargs['filter'] = filters

                res = self.crossref_client.journals(**kwargs)
                items = [item for page in res for item in page['message']['items']]

                # partition by year
                year_buckets: Dict[str, List[str]] = {}
                for item in items:
                    doi = item.get('DOI', '').lower()
                    if not doi:
                        continue
                    year = self._extract_year(item)
                    year_buckets.setdefault(year, []).append(doi)

                # save each year bucket as a separate file
                all_dois = set()
                for year, dois in year_buckets.items():
                    year_file = os.path.join(issn_cache_dir, f"{year}.txt")
                    with open(year_file, 'w', encoding='utf-8') as f:
                        for doi in sorted(set(dois)):
                            f.write(f"{doi}\n")
                    all_dois.update(dois)

                logging.info(f"Found {len(all_dois)} DOIs on Crossref for {issn} across {len(year_buckets)} year(s).")
                return all_dois

            except (HTTPError, RequestError) as e:
                logging.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} for {issn} failed: {e}")
                if attempt + 1 == MAX_RETRIES:
                    logging.error(f"Max retries reached for {issn}.")
                else:
                    time.sleep(BACKOFF_FACTOR * (2 ** attempt))
        return set()

    def _load_cached_dois(self, issn_cache_dir: str):
        """Load DOIs from year-partitioned cache files. Returns None if cache is empty."""
        year_files = [f for f in os.listdir(issn_cache_dir) if f.endswith('.txt')]
        if not year_files:
            return None

        # if a year range is specified, only load matching years
        dois = set()
        for yf in year_files:
            year_str = yf.replace('.txt', '')
            if self._year_in_range(year_str):
                for doi in self._read_doi_file(os.path.join(issn_cache_dir, yf)):
                    dois.add(doi)
        return dois if dois else None

    def _year_in_range(self, year_str: str) -> bool:
        """Check if a year string falls within the configured range."""
        if year_str == 'unknown':
            return True
        try:
            y = int(year_str)
        except ValueError:
            return True
        if self.from_year and y < self.from_year:
            return False
        if self.until_year and y > self.until_year:
            return False
        return True

    @staticmethod
    def _read_doi_file(filepath: str) -> Set[str]:
        dois = set()
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                d = line.strip()
                if d:
                    dois.add(d)
        return dois

    @staticmethod
    def _extract_year(item: dict) -> str:
        """Extract publication year from a Crossref work item."""
        for key in ('published-print', 'published-online', 'issued'):
            parts = item.get(key, {}).get('date-parts', [[]])
            if parts and parts[0] and parts[0][0]:
                return str(parts[0][0])
        return 'unknown'

    # ------------------------------------------------------------------
    # DB comparison
    # ------------------------------------------------------------------

    def _get_database_dois(self, issn: str) -> Set[str]:
        logging.info(f"Querying database for existing DOIs with ISSN {issn}...")
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT p.doi "
                "FROM publications p "
                "JOIN venues v ON v.id = p.venue_id "
                "WHERE p.doi IS NOT NULL "
                "  AND (v.issn = ? OR v.eissn = ?)",
                (issn, issn),
            )
            results = {row[0].lower() for row in cursor.fetchall() if row[0]}
            logging.info(f"Found {len(results)} existing DOIs for {issn} in database.")
            return results
        except Exception as e:
            logging.error(f"DB query error for {issn}: {e}")
            return set()
        finally:
            if conn:
                conn.close()

    # ------------------------------------------------------------------
    # Main per-ISSN logic
    # ------------------------------------------------------------------

    def process_issn(self, issn: str):
        try:
            output_file = os.path.join(OUTPUT_DIR, f"issn_{issn}_missing.txt")
            if os.path.exists(output_file):
                logging.info(f"Output for {issn} already exists. Skipping.")
                return

            crossref_dois = self._fetch_crossref_dois(issn)
            if not crossref_dois:
                logging.warning(f"No DOIs obtained from Crossref for {issn}.")
                return

            db_dois = self._get_database_dois(issn)
            missing = crossref_dois - db_dois
            logging.info(f"Comparison for {issn}: {len(missing)} missing DOIs.")

            if not missing:
                open(output_file, 'w').close()
                return

            with open(output_file, 'w', encoding='utf-8') as f:
                for doi in sorted(missing):
                    f.write(f"{doi}\n")
            logging.info(f"Missing DOIs for {issn} saved to {output_file}")

        except Exception as e:
            logging.critical(f"Critical failure processing ISSN {issn}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Find DOIs present on Crossref but missing from the local database."
    )
    parser.add_argument("-i", "--input-file", default="venues_list.csv",
                        help="CSV file with an 'issn' column.")
    parser.add_argument("-w", "--workers", type=int, default=None,
                        help="Parallel workers (default: [crossref] workers from config.ini).")
    parser.add_argument("--from-year", type=int, default=None,
                        help="Only consider publications from this year onward.")
    parser.add_argument("--until-year", type=int, default=None,
                        help="Only consider publications up to this year.")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch from Crossref even if cache exists.")
    parser.add_argument("--config", default="config.ini",
                        help="Path to config.ini.")
    args = parser.parse_args()

    try:
        issns: List[str] = []
        with open(args.input_file, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            header = [h.strip().lower() for h in reader.fieldnames]
            if 'issn' not in header:
                raise ValueError(f"CSV '{args.input_file}' must contain an 'issn' column.")
            issn_col = reader.fieldnames[header.index('issn')]
            for row in reader:
                issn = row[issn_col].strip()
                if issn:
                    issns.append(issn)
        logging.info(f"Found {len(issns)} ISSNs in '{args.input_file}'.")
    except FileNotFoundError:
        logging.critical(f"Input file '{args.input_file}' not found.")
        sys.exit(1)

    if not issns:
        logging.warning("No ISSNs to process.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    config = read_config(args.config)

    workers = args.workers if args.workers is not None else config.getint('crossref', 'workers', fallback=2)

    checker = DoiChecker(config, from_year=args.from_year, until_year=args.until_year)

    if args.force:
        for issn in issns:
            # clear year-cache for requested range so it's re-fetched
            issn_dir = os.path.join(DOI_CACHE_DIR, issn)
            if os.path.isdir(issn_dir):
                for yf in os.listdir(issn_dir):
                    if yf.endswith('.txt'):
                        year_str = yf.replace('.txt', '')
                        if checker._year_in_range(year_str):
                            os.remove(os.path.join(issn_dir, yf))
            # clear previous output so comparison is re-run
            output_file = os.path.join(OUTPUT_DIR, f"issn_{issn}_missing.txt")
            if os.path.exists(output_file):
                os.remove(output_file)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        total = len(issns)
        logging.info(f"Starting with {workers} workers...")
        futures = [executor.submit(checker.process_issn, issn) for issn in issns]
        for i, future in enumerate(as_completed(futures)):
            if (i + 1) % 50 == 0 or (i + 1) == total:
                logging.info(f"Progress: {i + 1}/{total} ISSNs processed.")

    logging.info("Batch processing complete.")


if __name__ == "__main__":
    main()
