"""Backwards-compatibility re-export shim for the load pipeline.

Import from specific modules instead:
  db.py, normalize.py, filtering.py, entities.py, io_utils.py, constants.py
"""

# DB connection
from pipeline.load.db import get_connection, read_db_config, ensure_connection, safe_rollback  # noqa: F401

# Normalization
from pipeline.load.normalize import (  # noqa: F401
    normalize_issn, normalize_doi, normalize_openalex_id, normalize_ror_id,
    normalize_wikidata_id, normalize_mag_id, normalize_language_code,
    normalize_term_key, clean_ingest_text, format_iso_timestamp,
    extract_first_text, _normalize_text_for_filter,
    _normalize_person_name_for_matching, _strip_name_punctuation, _prepare_person_name,
)

# Filtering
from pipeline.load.filtering import (  # noqa: F401
    EXCLUDED_WORK_TYPES, EXCLUDED_TITLE_EXACT, EXCLUDED_TITLE_REGEX,
    REVIEW_SUBTITLE_REGEX, classify_ingest_exclusion,
)

# Entities
from pipeline.load.entities import (  # noqa: F401
    get_or_create_person, get_or_create_organization,
    get_or_create_venue, get_or_create_subject,
    _find_venue_by_issn_cross, _merge_venue_into,
)

# I/O utilities
from pipeline.load.io_utils import (  # noqa: F401
    DEFAULT_CACHE_KEYS, build_cache, discover_input_folders, collect_json_files,
)

# Constants
from pipeline.load.constants import LICENSE_VERSION_RANK  # noqa: F401
