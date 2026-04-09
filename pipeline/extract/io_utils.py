"""File I/O utilities for the extract pipeline.

DOI file loading, filename sanitization, .filtered manifests, .404 markers,
ISSN discovery, and file sorting helpers.
"""

import os
import re
import logging
from typing import Dict, List, Set, Optional


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DOI file I/O
# ---------------------------------------------------------------------------

def load_dois_from_file(filepath: str) -> Set[str]:
    """Load DOIs from a text file (one per line)."""
    dois = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                doi = line.strip()
                if doi:
                    dois.add(doi)
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
    return dois


def sanitize_doi_for_filename(doi: str) -> str:
    return doi.replace('/', '_').replace(':', '_')


def parse_entity_from_filename(filename: str) -> Optional[str]:
    """Extract entity name from filenames like 'venue(Name).txt' or '1_OA_works_(Name).txt'."""
    match = re.search(r'(?:venue|author|OA_works_)\((.+?)\)\.txt$', filename)
    if match:
        entity = match.group(1)
        return re.sub(r'[<>:"/\\|?*]', '_', entity)
    # fallback: issn_XXXX-XXXX_missing.txt
    match = re.search(r'issn_([\dXx-]+)_missing\.txt$', filename)
    if match:
        return match.group(1)
    return None


def sort_files_by_doi_count(input_dir: str, filenames: List[str]) -> List[str]:
    """Sort DOI list files by number of lines (ascending), so smallest files are processed first."""
    def _count_lines(filename: str) -> int:
        filepath = os.path.join(input_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return sum(1 for line in f if line.strip())
        except OSError:
            return 0

    return sorted(filenames, key=_count_lines)


def discover_issns(input_dir: str, limit: int = 0) -> List[str]:
    """List ISSN subdirectories under *input_dir*, optionally truncated to *limit*."""
    issns = sorted(
        d for d in os.listdir(input_dir)
        if os.path.isdir(os.path.join(input_dir, d))
    )
    if limit:
        issns = issns[:limit]
    return issns


# ---------------------------------------------------------------------------
# .filtered manifest (works — append-only, per-ISSN directory)
# ---------------------------------------------------------------------------

def record_filtered_doi(entity_dir: str, doi: str) -> None:
    """Append a DOI to the .filtered manifest in *entity_dir* (thread-safe)."""
    manifest = os.path.join(entity_dir, '.filtered')
    with open(manifest, 'a', encoding='utf-8') as f:
        f.write(doi.lower() + '\n')


def load_filtered_dois(entity_dir: str) -> Set[str]:
    """Load the set of filtered DOIs from the .filtered manifest."""
    manifest = os.path.join(entity_dir, '.filtered')
    if not os.path.exists(manifest):
        return set()
    try:
        with open(manifest, 'r', encoding='utf-8') as f:
            return {line.strip().lower() for line in f if line.strip()}
    except OSError:
        return set()


# ---------------------------------------------------------------------------
# .notfound manifest (works — append-only, per-ISSN directory)
# ---------------------------------------------------------------------------

def record_not_found_doi(entity_dir: str, doi: str) -> None:
    """Append a DOI to the .notfound manifest in *entity_dir* (thread-safe)."""
    manifest = os.path.join(entity_dir, '.notfound')
    with open(manifest, 'a', encoding='utf-8') as f:
        f.write(doi.lower() + '\n')


def load_not_found_dois(entity_dir: str) -> Set[str]:
    """Load the set of not-found DOIs from the .notfound manifest."""
    manifest = os.path.join(entity_dir, '.notfound')
    if not os.path.exists(manifest):
        return set()
    try:
        with open(manifest, 'r', encoding='utf-8') as f:
            return {line.strip().lower() for line in f if line.strip()}
    except OSError:
        return set()


# ---------------------------------------------------------------------------
# .404 marker files (venues — per-entity empty marker files)
# ---------------------------------------------------------------------------

def write_not_found_marker(output_dir: str, key: str) -> None:
    """Create a .404 marker file indicating *key* was not found."""
    marker = os.path.join(output_dir, f"{key}.404")
    with open(marker, 'w') as f:
        f.write('')


def has_not_found_marker(output_dir: str, key: str) -> bool:
    """Check if a .404 marker file exists for *key*."""
    return os.path.exists(os.path.join(output_dir, f"{key}.404"))


def remove_not_found_marker(output_dir: str, key: str) -> None:
    """Remove a .404 marker file for *key* if it exists."""
    marker = os.path.join(output_dir, f"{key}.404")
    if os.path.exists(marker):
        os.remove(marker)
