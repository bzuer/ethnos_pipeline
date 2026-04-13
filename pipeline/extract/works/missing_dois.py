
"""
Identify missing DOIs per venue by comparing Crossref's journal catalog against
the local database.

DOI cache layout (preserves data across re-runs of different year ranges):

    works/doi/cache/
    └── {issn}/
        └── {year}.txt          # one DOI per line

Output (partitioned by year for bulk fetching):

    works/doi/missing/
    └── {issn}/
        └── {year}.txt          # missing DOIs for that year
"""

import argparse
import sys
import csv
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, List, Dict, Optional, Tuple

from habanero import Crossref

from pipeline.common import get_connection, ensure_connection, normalize_issn
from pipeline.extract.config import read_config, set_config
from pipeline.extract.retry import retry_request
from pipeline.extract.works_common import get_missing_dois_by_year, EXCLUDED_WORK_TYPES
from pipeline.extract.io_utils import load_filtered_dois

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DOI_CACHE_DIR = 'works/doi/cache'
OUTPUT_DIR = "works/doi/missing"


def choose_lookup_issn(raw_issn: Optional[str], raw_eissn: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Choose lookup key with ISSN priority; fallback to eISSN only when ISSN is absent/invalid."""
    issn = normalize_issn(raw_issn)
    if issn:
        return issn, 'issn'
    eissn = normalize_issn(raw_eissn)
    if eissn:
        return eissn, 'eissn'
    return None, None


class DoiChecker:
    def __init__(self, config, from_year: int = None, until_year: int = None, config_path: str = None):
        self.config = config
        self.config_path = config_path
        self.api_email = config.get('api', 'email', fallback='anonymous@example.com')
        from pipeline.extract.http import build_user_agent
        user_agent = build_user_agent(config)
        self.crossref_client = Crossref(mailto=self.api_email, ua_string=user_agent, timeout=30)
        self.from_year = from_year
        self.until_year = until_year
        self._db_local = threading.local()

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
            logging.info(f"[{issn}] loaded {len(cached_dois)} DOIs from cache ({issn_cache_dir})")
            return cached_dois

        logging.info(f"[{issn}] no cache, fetching from Crossref API (filters: {filters})...")

        kwargs = dict(
            ids=issn, works=True, limit=1000,
            select="DOI,type,published-print,published-online,issued",
            cursor="*", cursor_max=100000,
        )
        if filters:
            kwargs['filter'] = filters

        def _attempt():
            try:
                res = self.crossref_client.journals(**kwargs)
                items = [item for page in res for item in page['message']['items']]
                return 'success', items
            except Exception as e:
                if "429" in str(e):
                    return 'retry_429', None
                raise

        items = retry_request(_attempt, max_retries=1, label=f"crossref-journals:{issn}")
        if items is None:
            return set()

        # partition by year, filtering excluded types
        year_buckets: Dict[str, List[str]] = {}
        filtered_count = 0
        for item in items:
            doi = item.get('DOI', '').lower()
            if not doi:
                continue
            if item.get('type') in EXCLUDED_WORK_TYPES:
                filtered_count += 1
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

        if filtered_count:
            logging.info(f"[{issn}] filtered {filtered_count} excluded-type DOIs")
        logging.info(f"[{issn}] fetched {len(all_dois)} DOIs across {len(year_buckets)} year(s), cached to {issn_cache_dir}")
        return all_dois

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

    def _get_thread_connection(self):
        conn = getattr(self._db_local, "conn", None)
        conn = ensure_connection(
            conn,
            config_path=self.config_path,
            read_timeout=300,
            write_timeout=300,
            retries=5,
            retry_base=1.0,
            retry_max=12.0,
        )
        self._db_local.conn = conn
        return conn

    def _get_database_dois(self, issn: str, lookup_field: str = 'issn') -> Set[str]:
        cursor = None
        try:
            conn = self._get_thread_connection()
            cursor = conn.cursor()
            if lookup_field == 'eissn':
                cursor.execute(
                    "SELECT p.doi "
                    "FROM publications p "
                    "JOIN venues v ON v.id = p.venue_id "
                    "WHERE p.doi IS NOT NULL "
                    "  AND v.eissn = ?",
                    (issn,),
                )
            else:
                cursor.execute(
                    "SELECT p.doi "
                    "FROM publications p "
                    "JOIN venues v ON v.id = p.venue_id "
                    "WHERE p.doi IS NOT NULL "
                    "  AND v.issn = ?",
                    (issn,),
                )
            results = {row[0].lower() for row in cursor.fetchall() if row[0]}
            return results
        except Exception as e:
            logging.error(f"[{issn}] DB query error: {e}")
            stale = getattr(self._db_local, "conn", None)
            if stale is not None:
                try:
                    stale.close()
                except Exception:
                    pass
                self._db_local.conn = None
            raise
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Main per-ISSN logic
    # ------------------------------------------------------------------

    def process_issn(self, issn: str, lookup_field: str = 'issn'):
        try:
            raw_lookup = issn
            issn = normalize_issn(issn) or ''
            if not issn:
                logging.warning(f"[{raw_lookup}] invalid lookup key, skipping")
                return
            if lookup_field not in ('issn', 'eissn'):
                lookup_field = 'issn'

            issn_output_dir = os.path.join(OUTPUT_DIR, issn)
            if os.path.isdir(issn_output_dir) and os.listdir(issn_output_dir):
                logging.info(f"[{issn}] output already exists, skipping")
                return

            logging.info(f"[{issn}] fetching DOIs from Crossref...")
            crossref_dois = self._fetch_crossref_dois(issn)
            if not crossref_dois:
                logging.warning(f"[{issn}] no DOIs found on Crossref")
                return
            logging.info(f"[{issn}] Crossref: {len(crossref_dois)} DOIs")

            logging.info(f"[{issn}] querying database for existing DOIs via {lookup_field}...")
            db_dois = self._get_database_dois(issn, lookup_field=lookup_field)
            logging.info(f"[{issn}] database: {len(db_dois)} DOIs")

            missing = crossref_dois - db_dois

            # Subtract DOIs already filtered by extract scripts (paratext, erratum, etc.)
            already_filtered = set()
            for source_dir in ('works/crossref', 'works/openalex'):
                already_filtered |= load_filtered_dois(os.path.join(source_dir, issn))
            if already_filtered:
                before = len(missing)
                missing -= already_filtered
                logging.info(f"[{issn}] excluded {before - len(missing)} previously filtered DOIs")

            logging.info(f"[{issn}] missing: {len(missing)} DOIs (Crossref {len(crossref_dois)} - DB {len(db_dois)})")

            if not missing:
                os.makedirs(issn_output_dir, exist_ok=True)
                logging.info(f"[{issn}] nothing missing")
                return

            # partition missing DOIs by year using the cache
            by_year = get_missing_dois_by_year(issn, missing, cache_dir=DOI_CACHE_DIR)
            # rename 'uncached' key to 'unknown' for output and convert sets to sorted lists
            year_buckets = {}
            for year, dois_set in by_year.items():
                key = 'unknown' if year == 'uncached' else year
                year_buckets[key] = sorted(year_buckets.get(key, []) + sorted(dois_set))

            # write per-year files
            os.makedirs(issn_output_dir, exist_ok=True)
            total = 0
            for year, dois in sorted(year_buckets.items()):
                year_file = os.path.join(issn_output_dir, f"{year}.txt")
                with open(year_file, 'w', encoding='utf-8') as f:
                    for doi in dois:
                        f.write(f"{doi}\n")
                total += len(dois)

            logging.info(f"[{issn}] saved {total} missing DOIs across {len(year_buckets)} year(s) -> {issn_output_dir}")

        except Exception as e:
            logging.critical(f"[{issn}] critical failure: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Find DOIs present on Crossref but missing from the local database."
    )
    parser.add_argument("-i", "--input-file", default="venues_list.csv",
                        help="CSV file with an 'issn' column.")
    parser.add_argument("--relevance", type=int, default=None, metavar="N",
                        help="Filter venues by llm_relevance >= N from the database (overrides CSV).")
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

    if args.relevance is not None:
        conn = None
        cursor = None
        try:
            conn = get_connection(args.config)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT issn, eissn FROM venues "
                "WHERE llm_relevance >= ? AND (issn IS NOT NULL OR eissn IS NOT NULL)",
                (args.relevance,),
            )
            issn_tasks: List[Tuple[str, str]] = []
            invalid_rows = 0
            for issn, eissn in cursor.fetchall():
                lookup_issn, lookup_field = choose_lookup_issn(issn, eissn)
                if lookup_issn:
                    issn_tasks.append((lookup_issn, lookup_field))
                else:
                    invalid_rows += 1
            logging.info(f"Found {len(issn_tasks)} ISSN lookup keys from venues with llm_relevance >= {args.relevance}.")
            if invalid_rows:
                logging.warning(f"Skipped {invalid_rows} venues with invalid ISSN/eISSN.")
        except Exception as e:
            logging.critical(f"DB query for --relevance failed: {e}")
            sys.exit(1)
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        try:
            issn_tasks: List[Tuple[str, str]] = []
            invalid_rows = 0
            with open(args.input_file, mode='r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                header = [h.strip().lower() for h in reader.fieldnames]
                if 'issn' not in header:
                    raise ValueError(f"CSV '{args.input_file}' must contain an 'issn' column.")
                issn_col = reader.fieldnames[header.index('issn')]
                eissn_col = reader.fieldnames[header.index('eissn')] if 'eissn' in header else None
                for row in reader:
                    lookup_issn, lookup_field = choose_lookup_issn(
                        row.get(issn_col, ''),
                        row.get(eissn_col, '') if eissn_col else '',
                    )
                    if lookup_issn:
                        issn_tasks.append((lookup_issn, lookup_field))
                    else:
                        invalid_rows += 1
            logging.info(f"Found {len(issn_tasks)} ISSN lookup keys in '{args.input_file}'.")
            if invalid_rows:
                logging.warning(f"Skipped {invalid_rows} rows with invalid ISSN/eISSN.")
        except FileNotFoundError:
            logging.critical(f"Input file '{args.input_file}' not found.")
            sys.exit(1)

    if issn_tasks:
        by_key: Dict[str, str] = {}
        promoted_to_issn = 0
        for issn, lookup_field in issn_tasks:
            previous = by_key.get(issn)
            if previous is None:
                by_key[issn] = lookup_field
                continue
            if previous == 'eissn' and lookup_field == 'issn':
                by_key[issn] = 'issn'
                promoted_to_issn += 1
        deduped_tasks = list(by_key.items())
        if len(deduped_tasks) != len(issn_tasks):
            logging.info(f"Removed {len(issn_tasks) - len(deduped_tasks)} duplicate ISSN lookup keys.")
        if promoted_to_issn:
            logging.info(f"Promoted {promoted_to_issn} lookup key(s) from eISSN to ISSN.")
        issn_tasks = deduped_tasks

    if not issn_tasks:
        logging.warning("No ISSNs to process.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    config = read_config(args.config)
    set_config(config)

    workers = args.workers if args.workers is not None else config.getint('crossref', 'workers', fallback=2)

    checker = DoiChecker(
        config,
        from_year=args.from_year,
        until_year=args.until_year,
        config_path=args.config,
    )

    if args.force:
        import shutil
        for issn, _lookup_field in issn_tasks:
            # clear year-cache for requested range so it's re-fetched
            issn_dir = os.path.join(DOI_CACHE_DIR, issn)
            if os.path.isdir(issn_dir):
                for yf in os.listdir(issn_dir):
                    if yf.endswith('.txt'):
                        year_str = yf.replace('.txt', '')
                        if checker._year_in_range(year_str):
                            os.remove(os.path.join(issn_dir, yf))
            # clear previous output so comparison is re-run
            issn_output_dir = os.path.join(OUTPUT_DIR, issn)
            if os.path.isdir(issn_output_dir):
                shutil.rmtree(issn_output_dir)

    logging.info(f"Processing {len(issn_tasks)} ISSNs with {workers} workers")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        total = len(issn_tasks)
        futures = [
            executor.submit(checker.process_issn, issn, lookup_field)
            for issn, lookup_field in issn_tasks
        ]
        for i, future in enumerate(as_completed(futures)):
            if (i + 1) % 50 == 0 or (i + 1) == total:
                logging.info(f"Progress: {i + 1}/{total} ISSNs processed")

    logging.info("All ISSNs processed.")


if __name__ == "__main__":
    main()
