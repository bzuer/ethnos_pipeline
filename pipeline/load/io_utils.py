"""File discovery and cache management for the load pipeline."""

import os
import logging
from typing import Dict, List, Optional


DEFAULT_CACHE_KEYS = (
    "persons_by_orcid",
    "persons_by_norm_name",
    "venues_by_issn",
    "venues_by_eissn",
    "venues_by_name",
    "venues_by_openalex",
    "organizations_by_norm_name",
    "organizations_by_ror",
    "organizations_by_openalex",
    "organizations_by_wikidata",
    "organizations_by_mag",
    "subjects_by_term",
)


def build_cache() -> Dict[str, Dict]:
    return {k: {} for k in DEFAULT_CACHE_KEYS}


def discover_input_folders(root_dir: str) -> List[str]:
    if not os.path.isdir(root_dir):
        logging.critical(f"Root dir '{root_dir}' not found.")
        return []
    subdirectories = sorted([d.path for d in os.scandir(root_dir) if d.is_dir()])
    return subdirectories if subdirectories else [root_dir]


def collect_json_files(root_folder: str, limit: Optional[int]) -> List[str]:
    all_json_files: List[str] = []
    for r, _, fs in os.walk(root_folder):
        for f in fs:
            if not f.endswith(".json"):
                continue
            all_json_files.append(os.path.join(r, f))
            if limit and len(all_json_files) >= limit:
                return all_json_files
    return all_json_files
