"""Entity get-or-create functions for the load pipeline.

Lookup-or-insert logic for persons, organizations, venues, and subjects.
Each function checks cache first, then DB, then INSERTs if needed.
"""

import re
import logging
from typing import Any, Dict, List, Optional

import mariadb

from pipeline.load.normalize import (
    normalize_issn,
    normalize_openalex_id,
    normalize_ror_id,
    normalize_wikidata_id,
    normalize_mag_id,
    normalize_term_key,
    _normalize_person_name_for_matching,
    _prepare_person_name,
)


# ---------------------------------------------------------------------------
# Persons
# ---------------------------------------------------------------------------

def get_or_create_person(cursor: mariadb.Cursor, author_data: Dict, cache: Dict[str, Dict]) -> Optional[int]:
    person_name = _prepare_person_name(author_data)
    surname = person_name["surname"]
    given_names = person_name["given_names"]
    preferred_name = person_name["preferred_name"]

    if not preferred_name:
        return None
    orcid = None
    orcid_raw = author_data.get("orcid") or author_data.get("ORCID")
    if orcid_raw:
        match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", orcid_raw)
        if match:
            orcid = match.group(1).upper()

    match_key = _normalize_person_name_for_matching(preferred_name)
    if not match_key:
        return None

    if orcid and orcid in cache.get("persons_by_orcid", {}):
        return cache["persons_by_orcid"][orcid]
    if not orcid and match_key in cache.get("persons_by_norm_name", {}):
        return cache["persons_by_norm_name"][match_key]

    person_id = None
    existing_orcid = None

    if orcid:
        cursor.execute("SELECT id, orcid FROM persons WHERE orcid = ?", (orcid,))
        if result := cursor.fetchone():
            person_id, existing_orcid = result

    if not person_id:
        cursor.execute("SELECT id, orcid FROM persons WHERE normalized_name = ?", (match_key,))
        if result := cursor.fetchone():
            person_id, existing_orcid = result

    if person_id:
        if orcid and not existing_orcid:
            cursor.execute("SELECT id FROM persons WHERE orcid = ? AND id != ?", (orcid, person_id))
            if not cursor.fetchone():
                try:
                    cursor.execute("UPDATE persons SET orcid = ? WHERE id = ?", (orcid, person_id))
                except mariadb.IntegrityError:
                    pass
        if orcid:
            cache.setdefault("persons_by_orcid", {})[orcid] = person_id
            cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
        return person_id

    try:
        safe_family = surname[:100] if surname else None
        safe_given = given_names[:255] if given_names else None
        cursor.execute(
            "INSERT INTO persons (preferred_name, family_name, given_names, orcid) VALUES (?, ?, ?, ?)",
            (preferred_name, safe_family, safe_given, orcid),
        )
        person_id = cursor.lastrowid
        if person_id:
            if orcid:
                cache.setdefault("persons_by_orcid", {})[orcid] = person_id
            cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
        return person_id
    except mariadb.IntegrityError:
        if orcid:
            cursor.execute("SELECT id FROM persons WHERE orcid = ?", (orcid,))
        else:
            cursor.execute("SELECT id FROM persons WHERE normalized_name = ?", (match_key,))
        if result := cursor.fetchone():
            return result[0]
        return None


# ---------------------------------------------------------------------------
# Organizations
# ---------------------------------------------------------------------------

def get_or_create_organization(
    cursor: mariadb.Cursor,
    name: Optional[str],
    org_type: str,
    cache: Dict[str, Dict],
    ror_id: Optional[str] = None,
    openalex_id: Optional[str] = None,
    wikidata_id: Optional[str] = None,
    mag_id: Optional[str] = None,
    url: Optional[str] = None,
    country_code: Optional[str] = None,
) -> Optional[int]:
    clean_name = name.strip()[:512] if name else None
    norm_name_cache_key = clean_name.lower() if clean_name else None
    ror_id = normalize_ror_id(ror_id)
    openalex_id = normalize_openalex_id(openalex_id)
    wikidata_id = normalize_wikidata_id(wikidata_id)
    mag_id = normalize_mag_id(mag_id)
    url = url.strip()[:512] if url and isinstance(url, str) else None
    country_code = country_code.strip().upper()[:2] if country_code and isinstance(country_code, str) else None

    if ror_id and ror_id in cache.get("organizations_by_ror", {}):
        return cache["organizations_by_ror"][ror_id]
    if openalex_id and openalex_id in cache.get("organizations_by_openalex", {}):
        return cache["organizations_by_openalex"][openalex_id]
    if wikidata_id and wikidata_id in cache.get("organizations_by_wikidata", {}):
        return cache["organizations_by_wikidata"][wikidata_id]
    if mag_id and mag_id in cache.get("organizations_by_mag", {}):
        return cache["organizations_by_mag"][mag_id]
    if norm_name_cache_key:
        cache_key = (norm_name_cache_key, org_type)
        if cache_key in cache.get("organizations_by_norm_name", {}):
            return cache["organizations_by_norm_name"][cache_key]

    org_id = None
    lookup_fields = (
        ("ror_id", ror_id),
        ("openalex_id", openalex_id),
        ("wikidata_id", wikidata_id),
        ("mag_id", mag_id),
    )
    for field, value in lookup_fields:
        if not value:
            continue
        cursor.execute(f"SELECT id FROM organizations WHERE {field} = ?", (value,))
        if result := cursor.fetchone():
            org_id = result[0]
            break

    if not org_id and norm_name_cache_key:
        cursor.execute(
            "SELECT id FROM organizations WHERE standardized_name = ? AND type = ?",
            (norm_name_cache_key, org_type),
        )
        if result := cursor.fetchone():
            org_id = result[0]

    if org_id:
        update_fields = []
        update_params = []
        if ror_id:
            update_fields.append("ror_id = COALESCE(ror_id, ?)")
            update_params.append(ror_id)
        if openalex_id:
            update_fields.append("openalex_id = COALESCE(openalex_id, ?)")
            update_params.append(openalex_id)
        if wikidata_id:
            update_fields.append("wikidata_id = COALESCE(wikidata_id, ?)")
            update_params.append(wikidata_id)
        if mag_id:
            update_fields.append("mag_id = COALESCE(mag_id, ?)")
            update_params.append(mag_id)
        if url:
            update_fields.append("url = COALESCE(url, ?)")
            update_params.append(url)
        if country_code:
            update_fields.append("country_code = COALESCE(country_code, ?)")
            update_params.append(country_code)
        if update_fields:
            update_params.append(org_id)
            try:
                cursor.execute(
                    "UPDATE organizations SET " + ", ".join(update_fields) + " WHERE id = ?",
                    tuple(update_params),
                )
            except mariadb.IntegrityError:
                pass
    else:
        if not clean_name:
            return None
        try:
            cursor.execute(
                "INSERT INTO organizations (name, type, country_code, ror_id, openalex_id, wikidata_id, mag_id, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (clean_name, org_type, country_code, ror_id, openalex_id, wikidata_id, mag_id, url),
            )
            org_id = cursor.lastrowid
        except mariadb.IntegrityError:
            for field, value in lookup_fields:
                if not value:
                    continue
                cursor.execute(f"SELECT id FROM organizations WHERE {field} = ?", (value,))
                if result := cursor.fetchone():
                    org_id = result[0]
                    break
            if not org_id and norm_name_cache_key:
                cursor.execute(
                    "SELECT id FROM organizations WHERE standardized_name = ? AND type = ?",
                    (norm_name_cache_key, org_type),
                )
                if result := cursor.fetchone():
                    org_id = result[0]

    if org_id:
        if norm_name_cache_key:
            cache.setdefault("organizations_by_norm_name", {})[(norm_name_cache_key, org_type)] = org_id
        if ror_id:
            cache.setdefault("organizations_by_ror", {})[ror_id] = org_id
        if openalex_id:
            cache.setdefault("organizations_by_openalex", {})[openalex_id] = org_id
        if wikidata_id:
            cache.setdefault("organizations_by_wikidata", {})[wikidata_id] = org_id
        if mag_id:
            cache.setdefault("organizations_by_mag", {})[mag_id] = org_id
    return org_id


# ---------------------------------------------------------------------------
# Venues
# ---------------------------------------------------------------------------

def _find_venue_by_issn_cross(cursor: mariadb.Cursor, issns: List[str]) -> Optional[tuple]:
    """Search venues by ISSNs across both issn and eissn columns, with format-tolerant matching."""
    if not issns:
        return None
    compact = sorted({v.replace("-", "") for v in issns})
    ph = ",".join(["?"] * len(issns))
    cph = ",".join(["?"] * len(compact))
    params = tuple(issns) * 2 + tuple(compact) * 2
    cursor.execute(
        "SELECT id, issn, eissn, openalex_id FROM venues WHERE "
        f"(issn IN ({ph}) OR eissn IN ({ph}) "
        f"OR REPLACE(issn, '-', '') IN ({cph}) "
        f"OR REPLACE(eissn, '-', '') IN ({cph})) LIMIT 1",
        params,
    )
    return cursor.fetchone()


def _merge_venue_into(cursor: mariadb.Cursor, keep_id: int, drop_id: int) -> None:
    """Merge drop_id venue into keep_id: move FKs, delete secondary."""
    if keep_id == drop_id:
        return
    cursor.execute("UPDATE publications SET venue_id = ? WHERE venue_id = ?", (keep_id, drop_id))
    cursor.execute(
        "INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source) "
        "SELECT ?, subject_id, score, source FROM venue_subjects WHERE venue_id = ?",
        (keep_id, drop_id),
    )
    cursor.execute("DELETE FROM venue_subjects WHERE venue_id = ?", (drop_id,))
    cursor.execute(
        "INSERT INTO venue_yearly_stats (venue_id, year, works_count, oa_works_count, cited_by_count) "
        "SELECT ?, s.year, s.works_count, s.oa_works_count, s.cited_by_count "
        "FROM venue_yearly_stats s LEFT JOIN venue_yearly_stats p "
        "ON p.venue_id = ? AND p.year = s.year "
        "WHERE s.venue_id = ? AND p.venue_id IS NULL",
        (keep_id, keep_id, drop_id),
    )
    cursor.execute("DELETE FROM venue_yearly_stats WHERE venue_id = ?", (drop_id,))
    cursor.execute("DELETE FROM venues WHERE id = ?", (drop_id,))
    logging.info("Venue merge: %s → %s", drop_id, keep_id)


def get_or_create_venue(
    cursor: mariadb.Cursor,
    name: Optional[str],
    print_issn: Optional[str],
    electronic_issn: Optional[str],
    venue_type: str,
    cache: Dict[str, Dict],
    openalex_id: Optional[str] = None,
) -> Optional[int]:
    clean_name = name.strip()[:512] if name else None
    norm_print = normalize_issn(print_issn)
    norm_electronic = normalize_issn(electronic_issn)
    openalex_id = normalize_openalex_id(openalex_id)
    if not clean_name and not norm_print and not norm_electronic and not openalex_id:
        return None
    name_cache_key = (clean_name.lower(), venue_type) if clean_name else None

    # -- Cache lookup --
    all_issns = sorted({v for v in [norm_print, norm_electronic] if v})
    for issn_val in all_issns:
        if issn_val in cache.get("venues_by_issn", {}):
            return cache["venues_by_issn"][issn_val]
        if issn_val in cache.get("venues_by_eissn", {}):
            return cache["venues_by_eissn"][issn_val]
    if name_cache_key and name_cache_key in cache.get("venues_by_name", {}):
        return cache["venues_by_name"][name_cache_key]
    if openalex_id and openalex_id in cache.get("venues_by_openalex", {}):
        return cache["venues_by_openalex"][openalex_id]

    # -- DB lookup: openalex_id → ISSN cross-column → name+type --
    venue_id = None
    db_issn = None
    db_eissn = None
    db_openalex_id = None

    if openalex_id:
        cursor.execute("SELECT id, issn, eissn, openalex_id FROM venues WHERE openalex_id = ? LIMIT 1", (openalex_id,))
        if result := cursor.fetchone():
            venue_id, db_issn, db_eissn, db_openalex_id = result

    if not venue_id and all_issns:
        if result := _find_venue_by_issn_cross(cursor, all_issns):
            venue_id, db_issn, db_eissn, db_openalex_id = result

    if not venue_id and clean_name:
        cursor.execute("SELECT id, issn, eissn, openalex_id FROM venues WHERE name = ? AND type = ? LIMIT 1", (clean_name, venue_type))
        if result := cursor.fetchone():
            venue_id, db_issn, db_eissn, db_openalex_id = result

    # -- Found: backfill missing fields, merge on conflict --
    if venue_id:
        updates = []
        update_params = []
        if norm_print and not db_issn:
            updates.append("issn = ?")
            update_params.append(norm_print)
        if norm_electronic and not db_eissn:
            updates.append("eissn = ?")
            update_params.append(norm_electronic)
        if openalex_id and not db_openalex_id:
            updates.append("openalex_id = ?")
            update_params.append(openalex_id)
        if updates:
            update_params.append(venue_id)
            try:
                cursor.execute("UPDATE venues SET " + ", ".join(updates) + " WHERE id = ?", tuple(update_params))
            except mariadb.IntegrityError:
                for field, val in [("issn", norm_print), ("eissn", norm_electronic), ("openalex_id", openalex_id)]:
                    if not val:
                        continue
                    cursor.execute(f"SELECT id FROM venues WHERE {field} = ? AND id <> ? LIMIT 1", (val, venue_id))
                    conflict_row = cursor.fetchone()
                    if conflict_row:
                        conflict_id = conflict_row[0]
                        try:
                            _merge_venue_into(cursor, venue_id, conflict_id)
                        except mariadb.Error as e:
                            logging.warning("Venue merge %s→%s failed: %s", conflict_id, venue_id, e)
                try:
                    cursor.execute("UPDATE venues SET " + ", ".join(updates) + " WHERE id = ?", tuple(update_params))
                except mariadb.Error:
                    pass
    else:
        # -- Not found: INSERT --
        if not clean_name:
            return None
        try:
            cursor.execute(
                "INSERT INTO venues (name, type, issn, eissn, openalex_id) VALUES (?, ?, ?, ?, ?)",
                (clean_name, venue_type, norm_print, norm_electronic, openalex_id),
            )
            venue_id = cursor.lastrowid
        except mariadb.IntegrityError:
            if openalex_id:
                cursor.execute("SELECT id FROM venues WHERE openalex_id = ? LIMIT 1", (openalex_id,))
                if result := cursor.fetchone():
                    venue_id = result[0]
            if not venue_id and all_issns:
                row = _find_venue_by_issn_cross(cursor, all_issns)
                if row:
                    venue_id = row[0]
            if not venue_id and clean_name:
                cursor.execute("SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1", (clean_name, venue_type))
                if result := cursor.fetchone():
                    venue_id = result[0]

    # -- Update caches --
    if venue_id:
        if norm_print:
            cache.setdefault("venues_by_issn", {})[norm_print] = venue_id
        if norm_electronic:
            cache.setdefault("venues_by_eissn", {})[norm_electronic] = venue_id
        if name_cache_key:
            cache.setdefault("venues_by_name", {})[name_cache_key] = venue_id
        if openalex_id:
            cache.setdefault("venues_by_openalex", {})[openalex_id] = venue_id
    return venue_id


# ---------------------------------------------------------------------------
# Subjects
# ---------------------------------------------------------------------------

def get_or_create_subject(cursor: mariadb.Cursor, term: str, cache: Dict[str, Dict]) -> Optional[int]:
    if not term or not (clean_term := term.strip()):
        return None
    clean_term = clean_term[:255]
    cache_key = clean_term.lower()
    if cache_key in cache.get("subjects_by_term", {}):
        return cache["subjects_by_term"][cache_key]

    term_key_simulated = normalize_term_key(cache_key)[:255]
    if not term_key_simulated:
        return None

    cursor.execute(
        "SELECT id FROM subjects WHERE vocabulary = 'KEYWORD' AND subject_type IS NULL AND term_key = ?",
        (term_key_simulated,),
    )
    if result := cursor.fetchone():
        subject_id = result[0]
    else:
        try:
            cursor.execute(
                "INSERT INTO subjects (term, vocabulary, subject_type, term_key) VALUES (?, 'KEYWORD', NULL, ?)",
                (clean_term, term_key_simulated),
            )
            subject_id = cursor.lastrowid
        except mariadb.IntegrityError:
            cursor.execute(
                "SELECT id FROM subjects WHERE vocabulary = 'KEYWORD' AND subject_type IS NULL AND term_key = ?",
                (term_key_simulated,),
            )
            if result := cursor.fetchone():
                subject_id = result[0]
            else:
                subject_id = None

    if subject_id:
        cache.setdefault("subjects_by_term", {})[cache_key] = subject_id
    return subject_id
