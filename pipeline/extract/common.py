"""Backwards-compatibility re-export shim for the extract pipeline.

Import from specific modules instead:
  config.py, retry.py, http.py, io_utils.py, works_common.py, venues_common.py
"""

import logging
from typing import Optional

from pipeline.extract.config import set_config, get_config, read_config  # noqa: F401
from pipeline.extract.retry import (  # noqa: F401
    MAX_RETRIES, retry_request, run_fail_command,
)
from pipeline.extract.io_utils import (  # noqa: F401
    load_dois_from_file, sanitize_doi_for_filename, parse_entity_from_filename,
    sort_files_by_doi_count, discover_issns,
    record_filtered_doi, load_filtered_dois,
    write_not_found_marker, has_not_found_marker, remove_not_found_marker,
)
from pipeline.extract.works_common import (  # noqa: F401
    EXCLUDED_WORK_TYPES, has_blank_title, save_work,
    get_missing_dois_by_year, load_missing_dois_by_year,
    process_issn_generic,
)
from pipeline.extract.venues_common import (  # noqa: F401
    load_venues_from_csv, load_pending_venues_from_db,
    process_venues_batched,
)

from pipeline.common import normalize_issn  # noqa: F401 — canonical re-export


logger = logging.getLogger(__name__)


def resolve_openalex_source_id(issn: str, eissn: Optional[str] = None,
                               http_client=None, conn=None,
                               config_path: Optional[str] = None) -> Optional[str]:
    """Resolve an ISSN to an OpenAlex source ID.

    Tries the local database first (91% coverage), then the OpenAlex API as fallback.
    If *conn* is provided it is used directly (caller owns it); otherwise a
    temporary connection is opened and closed.
    Returns source ID string (e.g. 'S4210194408') or None.
    """
    # DB lookup
    own_conn = conn is None
    try:
        if own_conn:
            from pipeline.common import get_connection
            conn = get_connection(config_path=config_path)
        cursor = conn.cursor()
        params = [issn]
        clauses = ['issn = ?']
        if eissn:
            clauses.append('eissn = ?')
            params.append(eissn)
        cursor.execute(
            f"SELECT openalex_id FROM venues WHERE ({' OR '.join(clauses)}) "
            "AND openalex_id IS NOT NULL LIMIT 1",
            params,
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row[0]:
            logger.debug(f"[{issn}] OpenAlex source ID from DB: {row[0]}")
            return row[0]
    except Exception as e:
        logger.warning(f"[{issn}] DB lookup failed: {e}")
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # API fallback
    if http_client is None:
        return None

    for candidate in [issn, eissn]:
        if not candidate:
            continue
        try:
            resp = http_client.get(f"https://api.openalex.org/sources/issn:{candidate}")
            if resp.status_code == 200:
                data = resp.json()
                oa_id = data.get('id', '')
                source_id = oa_id.rsplit('/', 1)[-1] if '/' in oa_id else oa_id
                if source_id:
                    logger.debug(f"[{issn}] OpenAlex source ID from API: {source_id}")
                    return source_id
        except Exception as e:
            logger.warning(f"[{issn}] API lookup for {candidate} failed: {e}")

    logger.warning(f"[{issn}] could not resolve OpenAlex source ID")
    return None
