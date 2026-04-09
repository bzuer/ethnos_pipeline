"""Work-specific domain logic for the extract pipeline.

Work type filtering, title validation, save/cache helpers, DOI-year partitioning,
and the shared ISSN processing orchestrator used by both Crossref and OpenAlex
work extractors.
"""

import json
import os
import logging
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Set

from pipeline.extract.io_utils import (
    sanitize_doi_for_filename, load_filtered_dois, record_filtered_doi,
    load_not_found_dois, record_not_found_doi,
)


logger = logging.getLogger(__name__)


# Canonical set from load/filtering.py — extract filters early to save API calls and disk.
from pipeline.load.filtering import EXCLUDED_WORK_TYPES  # noqa: F401


def has_blank_title(title) -> bool:
    """Return True if the title is blank/empty/missing.

    Handles both Crossref format (list of strings) and OpenAlex format (plain string).
    """
    if title is None:
        return True
    if isinstance(title, list):
        # Crossref: title is a list like ['Some Title']
        if not title:
            return True
        title = title[0] if title else ''
    if not isinstance(title, str):
        return True
    return not title.strip()


def save_work(work_data: dict, doi: str, entity_dir: str, tag: str, *,
              envelope_fn=None, ensure_ascii: bool = True, indent: int = 4) -> str:
    """Filter and save a work dict to disk. Returns status string.

    *envelope_fn*, if provided, wraps *work_data* before writing (e.g. Crossref envelope).
    """
    safe = sanitize_doi_for_filename(doi)
    cache_path = os.path.join(entity_dir, f"{safe}.json")

    if os.path.exists(cache_path):
        return 'Cached'

    work_type = work_data.get('type')
    if work_type in EXCLUDED_WORK_TYPES:
        logger.info(f"{doi} -> filtered ({work_type})")
        record_filtered_doi(entity_dir, doi)
        return 'Filtered'
    if has_blank_title(work_data.get('title')):
        logger.info(f"{doi} -> filtered (blank title)")
        record_filtered_doi(entity_dir, doi)
        return 'Filtered'

    payload = envelope_fn(work_data) if envelope_fn else work_data
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=ensure_ascii, indent=indent)
    logger.info(f"{doi} -> saved {tag} ({work_type})")
    return 'Success'


# ---------------------------------------------------------------------------
# DOI-year partitioning helpers
# ---------------------------------------------------------------------------

def get_missing_dois_by_year(issn: str, missing_dois: Set[str],
                             cache_dir: str = 'works/doi/cache') -> Dict[str, Set[str]]:
    """Cross-reference missing DOIs with year-partitioned cache to determine which years to fetch.

    Returns dict mapping year (str) -> set of missing DOIs for that year.
    DOIs not found in any cache file are collected under the key 'uncached'.
    """
    issn_cache_dir = os.path.join(cache_dir, issn)
    if not os.path.isdir(issn_cache_dir):
        return {'uncached': set(missing_dois)}

    by_year: Dict[str, Set[str]] = {}
    found = set()

    for filename in os.listdir(issn_cache_dir):
        if not filename.endswith('.txt'):
            continue
        year = filename.replace('.txt', '')
        year_path = os.path.join(issn_cache_dir, filename)
        try:
            with open(year_path, 'r', encoding='utf-8') as f:
                year_dois = {line.strip().lower() for line in f if line.strip()}
        except OSError:
            continue
        overlap = missing_dois & year_dois
        if overlap:
            by_year[year] = overlap
            found |= overlap

    uncached = missing_dois - found
    if uncached:
        by_year['uncached'] = uncached

    return by_year


def load_missing_dois_by_year(issn: str,
                              missing_dir: str = 'works/doi/missing') -> Dict[str, Set[str]]:
    """Read per-year missing DOI files from {missing_dir}/{issn}/.

    Returns dict mapping year (str) -> set of missing DOIs for that year.
    """
    issn_dir = os.path.join(missing_dir, issn)
    if not os.path.isdir(issn_dir):
        return {}

    by_year: Dict[str, Set[str]] = {}
    for filename in os.listdir(issn_dir):
        if not filename.endswith('.txt'):
            continue
        year = filename.replace('.txt', '')
        filepath = os.path.join(issn_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                dois = {line.strip().lower() for line in f if line.strip()}
        except OSError:
            continue
        if dois:
            by_year[year] = dois

    return by_year


# ---------------------------------------------------------------------------
# Shared ISSN processing orchestrator
# ---------------------------------------------------------------------------

# Lock protecting fallback_dois mutation from concurrent _process_year_result calls
_fallback_lock = threading.Lock()


def process_issn_generic(
    issn: str,
    missing_dir: str,
    cache_dir: str,
    force: bool,
    *,
    bulk_fetch_fn,       # (year, year_missing_set) -> list[dict]
    doi_extractor,       # dict -> str (normalized lowercase DOI)
    save_fn,             # (work_dict, doi, entity_dir, tag) -> status str
    single_fetch_fn,     # (doi, entity_dir) -> status str
    pre_bulk_hook=None,  # () -> bool; return False to skip bulk entirely
    workers: int = 1,
    batch_size: int = 0,
    pause_between_batches: float = 0,
):
    """Shared ISSN processing loop for work extractors.

    Iterates years, does bulk fetch + index, falls back to individual fetch.
    When *workers* > 1, year fetches and individual fallbacks run concurrently.
    Individual fallbacks are grouped into batches of *batch_size* with a pause
    of *pause_between_batches* seconds between them.
    """
    entity_dir = os.path.join(cache_dir, issn)

    by_year = load_missing_dois_by_year(issn, missing_dir)
    if not by_year:
        logger.info(f"[{issn}] no missing DOIs")
        return

    # Filter out DOIs that are already cached or were previously filtered
    if not force and os.path.isdir(entity_dir):
        filtered_set = load_filtered_dois(entity_dir)
        not_found_set = load_not_found_dois(entity_dir)
        skip_set = filtered_set | not_found_set
        pruned_by_year: Dict[str, Set[str]] = {}
        for year, dois in by_year.items():
            remaining = set()
            for doi in dois:
                safe = sanitize_doi_for_filename(doi)
                if os.path.exists(os.path.join(entity_dir, f"{safe}.json")):
                    continue
                if doi.lower() in skip_set:
                    continue
                remaining.add(doi)
            if remaining:
                pruned_by_year[year] = remaining
        skipped = sum(len(v) for v in by_year.values()) - sum(len(v) for v in pruned_by_year.values())
        if not pruned_by_year:
            logger.info(f"[{issn}] skipped — all DOIs already cached or filtered ({skipped} total)")
            return
        if skipped:
            logger.info(f"[{issn}] {skipped} DOIs already cached/filtered, {sum(len(v) for v in pruned_by_year.values())} remaining")
        by_year = pruned_by_year

    total_missing = sum(len(v) for v in by_year.values())
    os.makedirs(entity_dir, exist_ok=True)

    # Clear stale .filtered and .notfound manifests on --force
    if force:
        for fname in ('.filtered', '.notfound'):
            manifest = os.path.join(entity_dir, fname)
            if os.path.exists(manifest):
                os.remove(manifest)

    logger.info(f"[{issn}] starting — {total_missing} missing DOIs across {len(by_year)} year(s), {workers} workers")

    stats: Counter = Counter()
    fallback_dois: set = set()

    # pre_bulk_hook can signal that bulk is unavailable (e.g. no source_id)
    bulk_available = True
    if pre_bulk_hook is not None:
        bulk_available = pre_bulk_hook()
        if not bulk_available:
            for dois in by_year.values():
                fallback_dois |= dois

    if bulk_available:
        # Classify years: fetchable vs non-fetchable
        years_to_fetch = []
        for year in sorted(by_year.keys()):
            year_missing = by_year[year]

            if not year.isdigit():
                logger.info(f"[{issn}] year '{year}': {len(year_missing)} DOIs -> individual fallback")
                fallback_dois |= year_missing
                continue

            all_cached = all(
                os.path.exists(os.path.join(entity_dir, f"{sanitize_doi_for_filename(d)}.json"))
                for d in year_missing
            )
            if all_cached:
                logger.info(f"[{issn}] year {year}: all {len(year_missing)} DOIs already cached")
                stats['Cached'] += len(year_missing)
                continue

            years_to_fetch.append((year, year_missing))

        # Worker function: fetch one year, return indexed results
        def _fetch_year(year, year_missing):
            logger.info(f"[{issn}] year {year}: fetching bulk ({len(year_missing)} missing DOIs)")
            items = bulk_fetch_fn(year, year_missing)
            doi_index = {}
            for item in items:
                bare = doi_extractor(item)
                if bare:
                    doi_index[bare] = item
            return year, year_missing, doi_index

        def _process_year_result(year, year_missing, doi_index):
            for doi in year_missing:
                work = doi_index.get(doi.lower())
                if work:
                    stats[save_fn(work, doi, entity_dir, 'via bulk')] += 1
                else:
                    with _fallback_lock:
                        fallback_dois.add(doi)

        if workers > 1 and len(years_to_fetch) > 1:
            logger.info(f"[{issn}] bulk: {len(years_to_fetch)} years with {workers} workers")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_fetch_year, year, dois): year
                    for year, dois in years_to_fetch
                }
                for future in as_completed(futures):
                    yr = futures[future]
                    try:
                        year, year_missing, doi_index = future.result()
                        _process_year_result(year, year_missing, doi_index)
                    except Exception as e:
                        logger.error(f"[{issn}] year {yr}: bulk fetch error: {e}")
                        with _fallback_lock:
                            fallback_dois |= by_year.get(yr, set())
        else:
            for year, year_missing in years_to_fetch:
                _, _, doi_index = _fetch_year(year, year_missing)
                _process_year_result(year, year_missing, doi_index)

    # Individual fallbacks — batched with workers
    if fallback_dois:
        sorted_dois = sorted(fallback_dois)
        effective_batch = batch_size if batch_size > 0 else len(sorted_dois)
        total_batches = (len(sorted_dois) + effective_batch - 1) // effective_batch
        logger.info(f"[{issn}] {len(sorted_dois)} DOIs -> individual fallback "
                     f"({workers} workers, {total_batches} batch(es) of {effective_batch})")

        for batch_start in range(0, len(sorted_dois), effective_batch):
            batch = sorted_dois[batch_start:batch_start + effective_batch]

            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(single_fetch_fn, doi, entity_dir): doi
                        for doi in batch
                    }
                    for future in as_completed(futures):
                        try:
                            stats[future.result()] += 1
                        except Exception as e:
                            doi = futures[future]
                            logger.error(f"[{issn}] {doi}: individual fetch error: {e}")
                            stats['Failed'] += 1
            else:
                for doi in batch:
                    stats[single_fetch_fn(doi, entity_dir)] += 1

            if pause_between_batches > 0 and batch_start + effective_batch < len(sorted_dois):
                time.sleep(pause_between_batches)

    logger.info(f"[{issn}] done — {dict(stats)}")
