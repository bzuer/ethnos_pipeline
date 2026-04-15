"""
scopus.py

Ingest de venues a partir de JSONs de serial metadata da Scopus.

Prioridade de matching:
1) ISSN/eISSN
2) scopus_id
3) name + type
"""

import os
import sys
import json
import logging
import argparse
import re
from urllib.parse import urlparse, parse_qs
from typing import Any, Dict, List, Optional, Tuple

import mariadb

from pipeline.load.common import (
    build_cache,
    get_connection,
    ensure_connection,
    safe_rollback,
    get_or_create_organization,
    normalize_issn,
    normalize_term_key,
)
from pipeline.load.venues.shared import (
    VENUE_MERGE_UNIQUE_FIELDS, VENUE_MERGE_FILL_IF_NULL_FIELDS,
    VENUE_MERGE_BOOL_MAX_FIELDS,
    row_to_dict, as_clean_string, parse_int,
    find_conflict_id_by_unique_field, _get_venue_for_merge,
    merge_venues_python_fallback,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


ORG_TYPE_PUBLISHER = "PUBLISHER"
VENUE_TYPE_OTHER = "OTHER"
VENUE_TYPE_MAP = {
    "journal": "JOURNAL",
    "tradejournal": "JOURNAL",
    "conferenceproceeding": "CONFERENCE",
    "bookseries": "BOOK_SERIES",
}

SCOPUS_SUBJECT_VOCAB = "Scopus"
SCOPUS_SUBJECT_TYPE = "SubjectArea"
SCOPUS_SUBJECT_SOURCE = "scopus"

# VENUE_MERGE_*, row_to_dict, as_clean_string, parse_int imported from venues.shared


def parse_decimal(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        raw = str(value).strip().replace(",", ".")
        if raw == "":
            return None
        return float(raw)
    except (TypeError, ValueError):
        return None


def parse_bool_to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    raw = str(value).strip().lower()
    if raw in {"1", "true", "t", "yes", "y"}:
        return 1
    if raw in {"0", "false", "f", "no", "n"}:
        return 0
    return None



def normalize_venue_type(aggregation_type: Optional[str]) -> str:
    if not aggregation_type:
        return VENUE_TYPE_OTHER
    return VENUE_TYPE_MAP.get(aggregation_type.strip().lower(), VENUE_TYPE_OTHER)


def collect_json_files(json_dir: str, limit: Optional[int]) -> List[str]:
    files: List[str] = []
    for name in sorted(os.listdir(json_dir)):
        if not name.lower().endswith(".json"):
            continue
        files.append(os.path.join(json_dir, name))
        if limit and len(files) >= limit:
            break
    return files


def extract_homepage_url(entry: Dict[str, Any]) -> Optional[str]:
    prism_url = as_clean_string(entry.get("prism:url"), 512)
    if prism_url:
        return prism_url

    links = entry.get("link")
    if not isinstance(links, list):
        return None
    for ref_name in ("homepage", "scopus-source"):
        for item in links:
            if not isinstance(item, dict):
                continue
            if as_clean_string(item.get("@ref")) != ref_name:
                continue
            href = as_clean_string(item.get("@href"), 512)
            if href:
                return href
    return None


def extract_sjr(entry: Dict[str, Any]) -> Optional[float]:
    lst = (entry.get("SJRList") or {}).get("SJR")
    if isinstance(lst, list) and lst:
        first = lst[0] if isinstance(lst[0], dict) else None
        if first:
            return parse_decimal(first.get("$"))
    return None


def extract_snip(entry: Dict[str, Any]) -> Optional[float]:
    lst = (entry.get("SNIPList") or {}).get("SNIP")
    if isinstance(lst, list) and lst:
        first = lst[0] if isinstance(lst[0], dict) else None
        if first:
            return parse_decimal(first.get("$"))
    return None


def extract_citescore(entry: Dict[str, Any]) -> Optional[float]:
    info = entry.get("citeScoreYearInfoList")
    if not isinstance(info, dict):
        return None
    return parse_decimal(info.get("citeScoreCurrentMetric")) or parse_decimal(info.get("citeScoreTracker"))


def get_subject_area_items(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = entry.get("subject-area")
    if isinstance(data, list):
        return [i for i in data if isinstance(i, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def parse_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    aggregation_type = as_clean_string(entry.get("prism:aggregationType"), 50)
    source_id = as_clean_string(entry.get("source-id"), 50)
    data: Dict[str, Any] = {
        "name": as_clean_string(entry.get("dc:title"), 512),
        "publisher_name": as_clean_string(entry.get("dc:publisher"), 512),
        "venue_type": normalize_venue_type(aggregation_type),
        "aggregation_type": aggregation_type.lower() if aggregation_type else None,
        "scopus_id": source_id,
        "issn": normalize_issn(entry.get("prism:issn")),
        "eissn": normalize_issn(entry.get("prism:eIssn")),
        "homepage_url": extract_homepage_url(entry),
        "open_access": parse_bool_to_int(entry.get("openaccess")),
        "is_in_doaj": parse_bool_to_int(entry.get("openaccessArticle")),
        "citescore": extract_citescore(entry),
        "sjr": extract_sjr(entry),
        "snip": extract_snip(entry),
        "is_indexed_in_scopus": 1,
        "validation_status": "VALIDATED",
        "subject_areas": get_subject_area_items(entry),
    }
    return data


def extract_entry_from_payload(payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    smr = payload.get("serial-metadata-response")
    if not isinstance(smr, dict):
        return None, None
    entries = smr.get("entry")
    if isinstance(entries, list) and entries:
        first = entries[0]
        if isinstance(first, dict):
            return first, smr
    return None, smr


def parse_fallback_venue_id(json_path: str) -> Optional[int]:
    stem = os.path.splitext(os.path.basename(json_path))[0]
    return int(stem) if stem.isdigit() else None


def find_existing_venue(
    cursor: mariadb.Cursor,
    parsed: Dict[str, Any],
    fallback_venue_id: Optional[int] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    cols = (
        "id, name, type, issn, eissn, scopus_id, publisher_id, homepage_url, "
        "aggregation_type, open_access, is_in_doaj, is_indexed_in_scopus, "
        "citescore, sjr, snip, validation_status"
    )
    base = f"SELECT {cols} FROM venues"

    search_issns = sorted({v for v in [parsed.get("issn"), parsed.get("eissn")] if v})
    search_issns_compact = sorted({v.replace("-", "") for v in search_issns})
    if search_issns and search_issns_compact:
        ph = ",".join(["?"] * len(search_issns))
        compact_ph = ",".join(["?"] * len(search_issns_compact))
        params = tuple(search_issns) * 2 + tuple(search_issns_compact) * 2
        cursor.execute(
            (
                f"{base} WHERE ("
                f"issn IN ({ph}) OR eissn IN ({ph}) "
                f"OR REPLACE(issn, '-', '') IN ({compact_ph}) "
                f"OR REPLACE(eissn, '-', '') IN ({compact_ph})"
                f") LIMIT 1"
            ),
            params,
        )
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "issn/eissn"

    if parsed.get("scopus_id"):
        cursor.execute(f"{base} WHERE scopus_id = ? LIMIT 1", (parsed["scopus_id"],))
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "scopus_id"

    if parsed.get("name") and parsed.get("venue_type"):
        cursor.execute(
            f"{base} WHERE name = ? AND type = ? LIMIT 1",
            (parsed["name"], parsed["venue_type"]),
        )
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "name+type"

    if fallback_venue_id:
        cursor.execute(f"{base} WHERE id = ? LIMIT 1", (fallback_venue_id,))
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "filename-id"

    return None, None


def find_conflict_id_by_issn_or_eissn(
    cursor: mariadb.Cursor,
    value: Optional[str],
    current_id: int,
) -> Optional[int]:
    if value is None:
        return None
    cursor.execute(
        "SELECT id FROM venues WHERE (issn = ? OR eissn = ?) AND id <> ? LIMIT 1",
        (value, value, current_id),
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def find_name_type_conflict_id(cursor: mariadb.Cursor, name: str, venue_type: str, current_id: int) -> Optional[int]:
    cursor.execute("SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1", (name, venue_type))
    row = cursor.fetchone()
    if row and row[0] != current_id:
        return row[0]
    return None


def attempt_merge_duplicate(
    cursor: mariadb.Cursor,
    primary_venue_id: int,
    secondary_venue_id: int,
    conflict_field: str,
    merge_duplicates: bool,
    dry_run: bool,
) -> bool:
    if secondary_venue_id == primary_venue_id:
        return False
    if not merge_duplicates:
        return False

    if dry_run:
        log.warning(
            "[DRY-RUN] -> Unificaria por conflito em %s: primary=%s <- secondary=%s.",
            conflict_field,
            primary_venue_id,
            secondary_venue_id,
        )
        return False

    try:
        return merge_venues_python_fallback(cursor, primary_venue_id, secondary_venue_id, conflict_field)
    except mariadb.Error as e_fallback:
        log.error(
            "  -> Failed to merge via Python fallback (primary=%s, secondary=%s, field=%s): %s",
            primary_venue_id,
            secondary_venue_id,
            conflict_field,
            e_fallback,
        )
        return False


def get_or_create_publisher_id(
    cursor: mariadb.Cursor,
    cache: Dict[str, Dict[str, Any]],
    publisher_name: Optional[str],
) -> Optional[int]:
    if not publisher_name:
        return None
    return get_or_create_organization(cursor, publisher_name, ORG_TYPE_PUBLISHER, cache)


def get_or_create_scopus_subject(
    cursor: mariadb.Cursor,
    cache: Dict[str, Dict[str, int]],
    item: Dict[str, Any],
) -> Optional[int]:
    term = as_clean_string(item.get("$"), 255)
    if not term:
        return None

    code = as_clean_string(item.get("@code"), 30)
    external_uri = f"scopus:subject-area:{code}" if code else None
    term_key = normalize_term_key(term)[:255] if term else None
    if not term_key:
        return None

    if external_uri and external_uri in cache["subjects_by_external_uri"]:
        return cache["subjects_by_external_uri"][external_uri]
    if term_key in cache["subjects_by_term_key"]:
        return cache["subjects_by_term_key"][term_key]

    subject_id: Optional[int] = None
    if external_uri:
        cursor.execute("SELECT id FROM subjects WHERE external_uri = ? LIMIT 1", (external_uri,))
        row = cursor.fetchone()
        if row:
            subject_id = row[0]

    if not subject_id:
        cursor.execute(
            "SELECT id FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ? LIMIT 1",
            (SCOPUS_SUBJECT_VOCAB, SCOPUS_SUBJECT_TYPE, term_key),
        )
        row = cursor.fetchone()
        if row:
            subject_id = row[0]

    if not subject_id:
        try:
            cursor.execute(
                """
                INSERT INTO subjects (term, vocabulary, subject_type, lang, external_uri, term_key)
                VALUES (?, ?, ?, 'en', ?, ?)
                """,
                (term, SCOPUS_SUBJECT_VOCAB, SCOPUS_SUBJECT_TYPE, external_uri, term_key),
            )
            subject_id = cursor.lastrowid
        except mariadb.IntegrityError:
            if external_uri:
                cursor.execute("SELECT id FROM subjects WHERE external_uri = ? LIMIT 1", (external_uri,))
                row = cursor.fetchone()
                if row:
                    subject_id = row[0]
            if not subject_id:
                cursor.execute(
                    "SELECT id FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ? LIMIT 1",
                    (SCOPUS_SUBJECT_VOCAB, SCOPUS_SUBJECT_TYPE, term_key),
                )
                row = cursor.fetchone()
                if row:
                    subject_id = row[0]

    if subject_id:
        cache["subjects_by_term_key"][term_key] = subject_id
        if external_uri:
            cache["subjects_by_external_uri"][external_uri] = subject_id
    return subject_id


def sync_venue_subjects(
    cursor: mariadb.Cursor,
    venue_id: int,
    subject_items: List[Dict[str, Any]],
    subject_cache: Dict[str, Dict[str, int]],
    dry_run: bool,
) -> int:
    subject_ids: List[int] = []
    for item in subject_items:
        sid = get_or_create_scopus_subject(cursor, subject_cache, item)
        if sid:
            subject_ids.append(sid)

    if not subject_ids:
        return 0

    unique_ids = sorted(set(subject_ids))
    if dry_run:
        return len(unique_ids)

    placeholders = ", ".join(["(?, ?, NULL, ?)"] * len(unique_ids))
    params: List[Any] = []
    for sid in unique_ids:
        params.extend([venue_id, sid, SCOPUS_SUBJECT_SOURCE])
    cursor.execute(
        f"INSERT IGNORE INTO venue_subjects (venue_id, subject_id, score, source) VALUES {placeholders}",
        tuple(params),
    )
    return cursor.rowcount


def recover_after_integrity_error(
    cursor: mariadb.Cursor,
    parsed: Dict[str, Any],
) -> Tuple[Optional[int], Optional[str]]:
    lookups: List[Tuple[str, str, Tuple[Any, ...]]] = []
    if parsed.get("issn"):
        compact = parsed["issn"].replace("-", "")
        lookups.append(
            (
                "issn",
                "SELECT id FROM venues WHERE issn = ? OR eissn = ? OR REPLACE(issn, '-', '') = ? OR REPLACE(eissn, '-', '') = ? LIMIT 1",
                (parsed["issn"], parsed["issn"], compact, compact),
            )
        )
    if parsed.get("eissn"):
        compact = parsed["eissn"].replace("-", "")
        lookups.append(
            (
                "eissn",
                "SELECT id FROM venues WHERE issn = ? OR eissn = ? OR REPLACE(issn, '-', '') = ? OR REPLACE(eissn, '-', '') = ? LIMIT 1",
                (parsed["eissn"], parsed["eissn"], compact, compact),
            )
        )
    if parsed.get("scopus_id"):
        lookups.append(("scopus_id", "SELECT id FROM venues WHERE scopus_id = ? LIMIT 1", (parsed["scopus_id"],)))
    if parsed.get("name") and parsed.get("venue_type"):
        lookups.append(("name+type", "SELECT id FROM venues WHERE name = ? AND type = ? LIMIT 1", (parsed["name"], parsed["venue_type"])))

    for key_name, query, params in lookups:
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row:
            return row[0], key_name
    return None, None


def build_updates_for_existing(
    cursor: mariadb.Cursor,
    existing: Dict[str, Any],
    parsed: Dict[str, Any],
    publisher_id: Optional[int],
    merge_duplicates: bool,
    dry_run: bool,
) -> Tuple[List[str], List[Any], bool]:
    updates: List[str] = []
    params: List[Any] = []
    venue_id = existing["id"]
    merged_any = False
    merged_secondary_ids = set()

    def add_assign(field: str, value: Any) -> None:
        updates.append(f"{field} = ?")
        params.append(value)

    def add_if_different(field: str, value: Any) -> None:
        if value is None:
            return
        if existing.get(field) != value:
            add_assign(field, value)

    def merge_if_conflict(conflict_id: Optional[int], conflict_field: str) -> bool:
        nonlocal merged_any
        if not conflict_id or conflict_id == venue_id:
            return False
        if conflict_id in merged_secondary_ids:
            return True
        merged_ok = attempt_merge_duplicate(
            cursor=cursor,
            primary_venue_id=venue_id,
            secondary_venue_id=conflict_id,
            conflict_field=conflict_field,
            merge_duplicates=merge_duplicates,
            dry_run=dry_run,
        )
        if merged_ok:
            merged_any = True
            merged_secondary_ids.add(conflict_id)
        return merged_ok

    if parsed.get("name") and not as_clean_string(existing.get("name")):
        conflict_id = find_name_type_conflict_id(cursor, parsed["name"], existing["type"], venue_id)
        if not conflict_id:
            add_assign("name", parsed["name"])
        elif merge_if_conflict(conflict_id, "name+type"):
            pass
        else:
            log.warning("name+type conflict updating venue ID %s. Keeping current name.", venue_id)

    type_source_aggregation = parsed.get("aggregation_type") or as_clean_string(existing.get("aggregation_type"), 50)
    target_type = normalize_venue_type(type_source_aggregation) if type_source_aggregation else None
    if target_type and target_type != VENUE_TYPE_OTHER and existing.get("type") != target_type:
        target_name = as_clean_string(existing.get("name")) or as_clean_string(parsed.get("name"))
        conflict_id = None
        if target_name:
            conflict_id = find_name_type_conflict_id(cursor, target_name, target_type, venue_id)
        if not conflict_id:
            add_assign("type", target_type)
        elif merge_if_conflict(conflict_id, "type(name+type)"):
            pass
        else:
            log.warning(
                "Type conflict updating venue ID %s (target_type=%s). Keeping current type.",
                venue_id,
                target_type,
            )

    if parsed.get("issn") and not as_clean_string(existing.get("issn")):
        conflict_id = find_conflict_id_by_issn_or_eissn(cursor, parsed["issn"], venue_id)
        if not conflict_id:
            add_assign("issn", parsed["issn"])
        elif merge_if_conflict(conflict_id, "issn"):
            pass
        else:
            log.warning("ISSN conflict '%s' updating venue ID %s. Ignoring.", parsed["issn"], venue_id)

    if parsed.get("eissn") and not as_clean_string(existing.get("eissn")):
        conflict_id = find_conflict_id_by_issn_or_eissn(cursor, parsed["eissn"], venue_id)
        if not conflict_id:
            add_assign("eissn", parsed["eissn"])
        elif merge_if_conflict(conflict_id, "eissn"):
            pass
        else:
            log.warning("eISSN conflict '%s' updating venue ID %s. Ignoring.", parsed["eissn"], venue_id)

    if parsed.get("scopus_id") and not as_clean_string(existing.get("scopus_id")):
        conflict_id = find_conflict_id_by_unique_field(cursor, "scopus_id", parsed["scopus_id"], venue_id)
        if not conflict_id:
            add_assign("scopus_id", parsed["scopus_id"])
        elif merge_if_conflict(conflict_id, "scopus_id"):
            pass
        else:
            log.warning("scopus_id conflict '%s' updating venue ID %s. Ignoring.", parsed["scopus_id"], venue_id)

    if publisher_id and existing.get("publisher_id") is None:
        add_assign("publisher_id", publisher_id)

    add_if_different("aggregation_type", parsed.get("aggregation_type"))
    add_if_different("homepage_url", parsed.get("homepage_url"))
    add_if_different("open_access", parsed.get("open_access"))
    add_if_different("is_in_doaj", parsed.get("is_in_doaj"))
    add_if_different("is_indexed_in_scopus", parsed.get("is_indexed_in_scopus"))
    add_if_different("citescore", parsed.get("citescore"))
    add_if_different("sjr", parsed.get("sjr"))
    add_if_different("snip", parsed.get("snip"))
    add_if_different("validation_status", parsed.get("validation_status"))

    updates.append("last_validated_at = CURRENT_TIMESTAMP")
    return updates, params, merged_any


def create_new_venue(
    cursor: mariadb.Cursor,
    parsed: Dict[str, Any],
    publisher_id: Optional[int],
    dry_run: bool,
) -> Tuple[Optional[int], bool]:
    if not parsed.get("name"):
        return None, False

    values: Dict[str, Any] = {
        "name": parsed.get("name"),
        "type": parsed.get("venue_type") or VENUE_TYPE_OTHER,
        "publisher_id": publisher_id,
        "issn": parsed.get("issn"),
        "eissn": parsed.get("eissn"),
        "scopus_id": parsed.get("scopus_id"),
        "aggregation_type": parsed.get("aggregation_type"),
        "homepage_url": parsed.get("homepage_url"),
        "open_access": parsed.get("open_access"),
        "is_in_doaj": parsed.get("is_in_doaj"),
        "is_indexed_in_scopus": parsed.get("is_indexed_in_scopus"),
        "citescore": parsed.get("citescore"),
        "sjr": parsed.get("sjr"),
        "snip": parsed.get("snip"),
        "validation_status": parsed.get("validation_status"),
    }
    cols = []
    params = []
    for k, v in values.items():
        if v is None:
            continue
        cols.append(k)
        params.append(v)
    cols.append("last_validated_at")
    placeholders = ", ".join(["?"] * len(params))

    if dry_run:
        return -1, True

    query = f"INSERT INTO venues ({', '.join(cols)}) VALUES ({placeholders}, CURRENT_TIMESTAMP)"
    existing_id, _ = recover_after_integrity_error(cursor, parsed)
    if existing_id:
        return existing_id, False
    try:
        cursor.execute(query, tuple(params))
        return cursor.lastrowid, True
    except mariadb.IntegrityError:
        recovered_id, _ = recover_after_integrity_error(cursor, parsed)
        if recovered_id:
            return recovered_id, False
        return None, False


def extract_issn_from_request_link(serial_metadata_response: Dict[str, Any]) -> Optional[str]:
    links = serial_metadata_response.get("link")
    if not isinstance(links, list):
        return None
    for item in links:
        if not isinstance(item, dict):
            continue
        href = as_clean_string(item.get("@href"))
        if not href:
            continue
        parsed = urlparse(href)
        query = parse_qs(parsed.query or "")
        values = query.get("issn")
        if not values:
            continue
        return normalize_issn(values[0])
    return None


def resolve_venue_for_status_only(
    cursor: mariadb.Cursor,
    serial_metadata_response: Optional[Dict[str, Any]],
    fallback_venue_id: Optional[int],
) -> Optional[int]:
    if serial_metadata_response:
        query_issn = extract_issn_from_request_link(serial_metadata_response)
        if query_issn:
            compact = query_issn.replace("-", "")
            cursor.execute(
                "SELECT id FROM venues WHERE issn = ? OR eissn = ? OR REPLACE(issn, '-', '') = ? OR REPLACE(eissn, '-', '') = ? LIMIT 1",
                (query_issn, query_issn, compact, compact),
            )
            row = cursor.fetchone()
            if row:
                return row[0]
    if fallback_venue_id:
        cursor.execute("SELECT id FROM venues WHERE id = ? LIMIT 1", (fallback_venue_id,))
        row = cursor.fetchone()
        if row:
            return row[0]
    return None


def process_scopus_venues(
    conn: mariadb.Connection,
    json_files: List[str],
    dry_run: bool,
    commit_batch: int,
    merge_duplicates: bool,
    config_path: Optional[str] = None,
) -> Tuple[mariadb.Connection, Dict[str, int]]:
    counters = {
        "processed": 0,
        "updated": 0,
        "created": 0,
        "not_found": 0,
        "failed": 0,
        "skipped": 0,
        "errors": 0,
        "subjects_linked": 0,
    }

    common_cache = build_cache()
    subject_cache: Dict[str, Dict[str, int]] = {
        "subjects_by_external_uri": {},
        "subjects_by_term_key": {},
    }
    pending_commits = 0

    total = len(json_files)
    for idx, json_path in enumerate(json_files, start=1):
        counters["processed"] += 1
        prefix = f"[{idx}/{total}]"
        venue_label = os.path.basename(json_path)
        outcome = None

        conn = ensure_connection(conn, config_path=config_path)
        cursor = conn.cursor()
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            fallback_venue_id = parse_fallback_venue_id(json_path)
            entry, smr = extract_entry_from_payload(payload)

            if not entry:
                status = "FAILED"
                error_msg = as_clean_string((smr or {}).get("error"))
                if error_msg and error_msg.lower() == "no results found":
                    status = "NOT_FOUND"

                venue_id = resolve_venue_for_status_only(cursor, smr, fallback_venue_id)
                if not venue_id:
                    counters["skipped"] += 1
                    outcome = "Skipped (no entry, no venue)"
                    continue

                if not dry_run:
                    cursor.execute(
                        """
                        UPDATE venues
                        SET validation_status = ?, last_validated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (status, venue_id),
                    )
                    if cursor.rowcount > 0:
                        pending_commits += 1

                venue_label = str(venue_id)
                if status == "NOT_FOUND":
                    counters["not_found"] += 1
                    outcome = "Not found"
                else:
                    counters["failed"] += 1
                    outcome = "Failed"
                continue

            parsed = parse_entry(entry)
            if not parsed.get("name") and not parsed.get("issn") and not parsed.get("eissn") and not parsed.get("scopus_id"):
                counters["skipped"] += 1
                outcome = "Skipped (no identifiers)"
                continue

            venue_label = parsed.get("name") or venue_label
            publisher_id = get_or_create_publisher_id(cursor, common_cache, parsed.get("publisher_name"))
            existing, matched_by = find_existing_venue(cursor, parsed, fallback_venue_id=fallback_venue_id)

            if existing:
                venue_label = f"{existing['id']} {existing.get('name') or parsed.get('name', '')}"
                updates, params, merged_any = build_updates_for_existing(
                    cursor=cursor,
                    existing=existing,
                    parsed=parsed,
                    publisher_id=publisher_id,
                    merge_duplicates=merge_duplicates,
                    dry_run=dry_run,
                )
                # updates list always ends with "last_validated_at = ..." so real field count is len-1
                n_fields = max(0, len(updates) - 1) if updates else 0
                did_update = False
                if not dry_run and updates:
                    params.append(existing["id"])
                    query = "UPDATE venues SET " + ", ".join(updates) + " WHERE id = ?"
                    cursor.execute(query, tuple(params))
                    pending_commits += 1
                    if cursor.rowcount > 0 or merged_any:
                        counters["updated"] += 1
                        did_update = True
                elif not dry_run and merged_any:
                    pending_commits += 1
                    counters["updated"] += 1
                    did_update = True
                elif dry_run and (updates or merged_any):
                    counters["updated"] += 1
                    did_update = True

                linked = 0
                if existing.get("id"):
                    linked = sync_venue_subjects(
                        cursor,
                        existing["id"],
                        parsed.get("subject_areas", []),
                        subject_cache,
                        dry_run=dry_run,
                    )
                    counters["subjects_linked"] += linked

                # Build outcome detail
                parts = [matched_by]
                if n_fields:
                    parts.append(f"{n_fields}f")
                if merged_any:
                    parts.append("merge")
                if linked:
                    parts.append(f"+{linked}s")
                detail = ", ".join(parts)
                if did_update:
                    outcome = f"Updated ({detail})"
                else:
                    outcome = f"OK ({matched_by})"

            else:
                new_venue_id, was_created = create_new_venue(cursor, parsed, publisher_id, dry_run=dry_run)
                if new_venue_id is None:
                    counters["errors"] += 1
                    outcome = "Error (create failed)"
                    continue

                linked = 0
                if not dry_run and was_created:
                    pending_commits += 1
                    counters["created"] += 1
                elif not dry_run and not was_created:
                    counters["updated"] += 1
                elif dry_run and was_created:
                    counters["created"] += 1
                else:
                    counters["updated"] += 1

                if new_venue_id and new_venue_id > 0:
                    linked = sync_venue_subjects(
                        cursor,
                        new_venue_id,
                        parsed.get("subject_areas", []),
                        subject_cache,
                        dry_run=dry_run,
                    )
                    counters["subjects_linked"] += linked

                venue_label = f"{new_venue_id} {parsed.get('name', '')}"
                detail = f" +{linked}s" if linked else ""
                outcome = f"Created{detail}" if was_created else f"Updated (recovered){detail}"

            if not dry_run and pending_commits > 0 and (commit_batch == 0 or pending_commits >= commit_batch):
                conn.commit()
                pending_commits = 0

        except Exception as e:
            counters["errors"] += 1
            if not dry_run:
                if not safe_rollback(conn, f"{prefix} {venue_label}"):
                    conn = ensure_connection(None, config_path=config_path)
                pending_commits = 0
            outcome = f"Error: {e}"
            log.debug("Traceback for %s:", json_path, exc_info=True)
        finally:
            if outcome:
                log.info("%s %s → %s", prefix, venue_label, outcome)
            cursor.close()

    if not dry_run and pending_commits > 0:
        conn.commit()

    return conn, counters


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Scopus venue metadata into the database.")
    parser.add_argument("--json-dir", required=True, help="Directory with Scopus JSON files.")
    parser.add_argument("--limit", type=int, default=None, help="Max JSON files to process.")
    parser.add_argument("--commit-batch", type=int, default=200, help="Commit every N changes (0 = per change).")
    parser.add_argument("--config", type=str, default=None, help="Path to config.ini.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing to DB.")
    parser.add_argument("--no-merge-duplicates", action="store_true", help="Disable auto-merge on unique constraint conflict.")
    args = parser.parse_args()

    if not os.path.isdir(args.json_dir):
        raise FileNotFoundError(f"Directory not found: {args.json_dir}")

    json_files = collect_json_files(args.json_dir, args.limit)
    if not json_files:
        log.warning("No JSON files to process.")
        return
    log.info("--- Scopus venue ingest: %s files from '%s' ---", len(json_files), args.json_dir)

    conn = None
    try:
        conn = get_connection(config_path=args.config)
        conn, counters = process_scopus_venues(
            conn=conn,
            json_files=json_files,
            dry_run=args.dry_run,
            commit_batch=args.commit_batch,
            merge_duplicates=not args.no_merge_duplicates,
            config_path=args.config,
        )
        log.info("=== Finished: %s ===", counters)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
