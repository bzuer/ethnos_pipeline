"""Shared helpers for venue loaders (OpenAlex, Scopus, SCImago).

Common data conversion utilities, venue merge constants and logic,
conflict detection, and row-to-dict conversion.
"""

import logging
from typing import Any, Dict, Optional

import mariadb


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Merge constants (shared by OpenAlex and Scopus)
# ---------------------------------------------------------------------------

VENUE_MERGE_UNIQUE_FIELDS = ("issn", "eissn", "scopus_id", "wikidata_id", "openalex_id", "mag_id")
VENUE_MERGE_FILL_IF_NULL_FIELDS = (
    "abbreviated_name",
    "publisher_id",
    "country_code",
    "issn",
    "eissn",
    "homepage_url",
    "aggregation_type",
    "scopus_id",
    "wikidata_id",
    "openalex_id",
    "scielo_id",
    "mag_id",
)
VENUE_MERGE_BOOL_MAX_FIELDS = ("open_access", "is_in_doaj", "is_in_scielo", "is_indexed_in_scopus")


# ---------------------------------------------------------------------------
# Data conversion utilities
# ---------------------------------------------------------------------------

def row_to_dict(cursor: mariadb.Cursor, row: Optional[tuple]) -> Optional[Dict[str, Any]]:
    """Convert a cursor row to a dict using column names from cursor.description."""
    if not row:
        return None
    columns = [d[0] for d in cursor.description]
    return dict(zip(columns, row))


def as_clean_string(value: Any, max_len: Optional[int] = None) -> Optional[str]:
    """Strip and optionally truncate a value to a clean string."""
    if value is None:
        return None
    clean = str(value).strip()
    if not clean:
        return None
    if max_len is not None:
        clean = clean[:max_len]
    return clean


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Conflict detection (shared by OpenAlex and Scopus)
# ---------------------------------------------------------------------------

def find_conflict_id_by_unique_field(
    cursor: mariadb.Cursor,
    field: str,
    value: Any,
    current_venue_id: int,
) -> Optional[int]:
    """Find a conflicting venue ID for a unique field value."""
    if value is None:
        return None
    cursor.execute(f"SELECT id FROM venues WHERE {field} = ? LIMIT 1", (value,))
    result = cursor.fetchone()
    if result and result[0] != current_venue_id:
        return result[0]
    return None


def _get_venue_for_merge(cursor: mariadb.Cursor, venue_id: int) -> Optional[Dict[str, Any]]:
    """Load a venue record with all merge-relevant fields (with FOR UPDATE lock)."""
    merge_cols = (
        "id, name, type, abbreviated_name, publisher_id, country_code, "
        "issn, eissn, homepage_url, aggregation_type, scopus_id, "
        "wikidata_id, openalex_id, scielo_id, mag_id, "
        "open_access, is_in_doaj, is_in_scielo, is_indexed_in_scopus"
    )
    cursor.execute(f"SELECT {merge_cols} FROM venues WHERE id = ? FOR UPDATE", (venue_id,))
    return row_to_dict(cursor, cursor.fetchone())


# ---------------------------------------------------------------------------
# Venue merge (shared by OpenAlex and Scopus)
# ---------------------------------------------------------------------------

def merge_venues_python_fallback(
    cursor: mariadb.Cursor,
    primary_venue_id: int,
    secondary_venue_id: int,
    conflict_field: str,
) -> bool:
    """Merge secondary venue into primary: move FKs, aggregate stats, backfill fields, delete secondary.

    Returns True on success, False if venues are the same ID.
    Raises OperationalError if primary venue not found.
    """
    if primary_venue_id == secondary_venue_id:
        return False

    primary = _get_venue_for_merge(cursor, primary_venue_id)
    if not primary:
        raise mariadb.OperationalError(f"Primary venue {primary_venue_id} not found for merge.")

    secondary = _get_venue_for_merge(cursor, secondary_venue_id)
    if not secondary:
        log.info(
            "  -> Merge fallback (field=%s): secondary venue %s no longer exists; treating as merged.",
            conflict_field,
            secondary_venue_id,
        )
        return True

    # Move publications
    cursor.execute(
        "UPDATE publications SET venue_id = ? WHERE venue_id = ?",
        (primary_venue_id, secondary_venue_id),
    )
    moved_pubs = cursor.rowcount

    # Merge subjects
    cursor.execute(
        "INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source) "
        "SELECT ?, subject_id, score, source FROM venue_subjects WHERE venue_id = ?",
        (primary_venue_id, secondary_venue_id),
    )
    merged_subjects = cursor.rowcount

    # Delete secondary venue
    cursor.execute("DELETE FROM venues WHERE id = ?", (secondary_venue_id,))
    if cursor.rowcount == 0:
        log.warning(
            "  -> Merge fallback (field=%s): secondary venue %s already removed before DELETE.",
            conflict_field,
            secondary_venue_id,
        )

    # Backfill NULL fields from secondary
    update_fields: Dict[str, Any] = {}
    for field in VENUE_MERGE_FILL_IF_NULL_FIELDS:
        primary_val = primary.get(field)
        secondary_val = secondary.get(field)
        if primary_val is not None or secondary_val is None:
            continue

        if field in VENUE_MERGE_UNIQUE_FIELDS:
            cursor.execute(f"SELECT id FROM venues WHERE {field} = ? AND id <> ? LIMIT 1", (secondary_val, primary_venue_id))
            conflict = cursor.fetchone()
            if conflict:
                log.warning(
                    "  -> Merge fallback (field=%s): value '%s' from %s not transferred to venue %s (already used by venue %s).",
                    conflict_field, secondary_val, field, primary_venue_id, conflict[0],
                )
                continue

        update_fields[field] = secondary_val

    # Bool-max fields
    for field in VENUE_MERGE_BOOL_MAX_FIELDS:
        primary_v = int(bool(primary.get(field)))
        secondary_v = int(bool(secondary.get(field)))
        merged_v = max(primary_v, secondary_v)
        if primary.get(field) is None or merged_v != primary_v:
            update_fields[field] = merged_v

    if update_fields:
        clauses = ", ".join([f"`{k}` = ?" for k in update_fields])
        params = list(update_fields.values()) + [primary_venue_id]
        cursor.execute(
            f"UPDATE venues SET {clauses}, `updated_at` = NOW() WHERE id = ?",
            tuple(params),
        )

    log.warning(
        "  -> Duplicate merged via Python fallback by %s: primary=%s <- secondary=%s (pubs:%s, subjects:%s).",
        conflict_field, primary_venue_id, secondary_venue_id, moved_pubs, merged_subjects,
    )
    return True
