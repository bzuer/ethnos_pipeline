
import os
import re
import csv
import configparser
import logging
from typing import Dict, List, Set, Optional


logger = logging.getLogger(__name__)


def read_config(filename: str = 'config.ini') -> configparser.ConfigParser:
    """Read config.ini, searching script dir then repo root."""
    parser = configparser.ConfigParser()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    candidates = [
        filename if os.path.isabs(filename) else None,
        os.path.join(script_dir, filename),
        os.path.join(repo_root, filename),
    ]
    for path in candidates:
        if path and parser.read(path):
            return parser
    raise FileNotFoundError(f"Config '{filename}' not found.")


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


def load_venues_from_csv(filepath: str) -> List[Dict[str, str]]:
    """Read a venues CSV file. Returns list of dicts with stripped values."""
    venues = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            venues.append({k.strip(): (v.strip() if v else '') for k, v in row.items()})
    return venues


def normalize_issn(raw: str) -> Optional[str]:
    """Validate and normalize an ISSN to XXXX-XXXX format. Returns None if invalid."""
    if not raw:
        return None
    cleaned = re.sub(r'[\s\-]', '', raw.strip().upper())
    if not re.match(r'^\d{7}[\dX]$', cleaned):
        return None
    return f"{cleaned[:4]}-{cleaned[4:]}"
