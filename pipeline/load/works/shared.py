"""Shared helpers for work loaders (Crossref and OpenAlex).

DOI skip/mark functions used identically by both sources.
Fast-mode helpers: filename-based DOI extraction and bulk existence check.
"""

import os
import logging
from typing import Dict, List, Optional, Tuple

import mariadb

from pipeline.load.constants import STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE


def should_skip_existing_doi(
    cursor: mariadb.Cursor,
    doi: Optional[str],
    mode: str,
    doi_presence_cache: Dict[str, bool],
) -> bool:
    """In 'new'/'fast' mode, return True if DOI already exists in publications."""
    if mode not in ("new", "fast") or not doi:
        return False
    if doi in doi_presence_cache:
        return doi_presence_cache[doi]
    cursor.execute("SELECT 1 FROM publications WHERE doi = ? LIMIT 1", (doi,))
    exists = cursor.fetchone() is not None
    doi_presence_cache[doi] = exists
    return exists


def mark_doi_processed(
    doi: Optional[str],
    mode: str,
    status: str,
    doi_presence_cache: Dict[str, bool],
) -> None:
    """Mark a DOI as processed in the presence cache after successful load."""
    if mode in ("new", "fast") and doi and status in (STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE):
        doi_presence_cache[doi] = True


# ---------------------------------------------------------------------------
# Fast-mode helpers
# ---------------------------------------------------------------------------

def doi_from_filename(filepath: str) -> Optional[str]:
    """Extract DOI from a sanitized JSON filename (best-effort reverse of sanitize_doi_for_filename).

    Filename convention: ``10.XXXX_suffix.json`` where ``_`` replaced the
    single ``/`` separating registrant from suffix.  We restore the first
    ``_`` after the ``10.`` prefix back to ``/``.
    """
    stem = os.path.basename(filepath).removesuffix(".json")
    if not stem.startswith("10."):
        return None
    doi = stem.replace("_", "/", 1)
    return doi if "/" in doi else None


def bulk_filter_new_files(
    cursor: mariadb.Cursor,
    json_files: List[str],
    doi_presence_cache: Dict[str, bool],
    batch_size: int = 1000,
) -> Tuple[List[str], int]:
    """Return only files whose DOI is not yet in publications (fast mode).

    1. Derive DOI from each filename (no JSON parsing).
    2. Bulk-check against DB in batches.
    3. Populate doi_presence_cache for downstream mark_doi_processed.

    Returns (new_files, skipped_count).
    """
    file_doi_pairs: List[Tuple[str, str]] = []
    unparseable: List[str] = []

    for fp in json_files:
        doi = doi_from_filename(fp)
        if doi:
            file_doi_pairs.append((fp, doi))
        else:
            unparseable.append(fp)

    existing_dois: set = set()
    all_dois = [d for _, d in file_doi_pairs]

    for i in range(0, len(all_dois), batch_size):
        batch = all_dois[i : i + batch_size]
        placeholders = ",".join(["?"] * len(batch))
        cursor.execute(
            f"SELECT doi FROM publications WHERE doi IN ({placeholders})",
            tuple(batch),
        )
        existing_dois.update(row[0] for row in cursor)

    for doi in existing_dois:
        doi_presence_cache[doi] = True
    for _, doi in file_doi_pairs:
        if doi not in existing_dois:
            doi_presence_cache[doi] = False

    new_files = [fp for fp, doi in file_doi_pairs if doi not in existing_dois]
    # Include unparseable files so they get processed normally (fallback)
    new_files.extend(unparseable)
    skipped = len(json_files) - len(new_files)

    if skipped:
        logging.info(f"FAST skip={skipped} new={len(new_files)} (unparseable={len(unparseable)})")

    return new_files, skipped
