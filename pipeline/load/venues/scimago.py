"""
Load venue metadata from SCImago CSV exports into the database.

Input:  venues/scimago/*.csv (SCImago Journal & Country Rank exports)
Output: UPDATE/INSERT into venues, venue_subjects, organizations

Matching priority: ISSN/eISSN → scopus_id → name+type → fallback INSERT.
"""

import os
import sys
import csv
import re
import logging
import argparse
from typing import Any, Dict, List, Optional, Tuple

import mariadb
import pycountry

from pipeline.load.common import (
    build_cache,
    get_connection,
    ensure_connection,
    safe_rollback,
    get_or_create_organization,
    normalize_issn,
    normalize_term_key,
)
from pipeline.load.venues.shared import row_to_dict, as_clean_string, parse_int

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


ORG_TYPE_PUBLISHER = "PUBLISHER"
VENUE_TYPE_OTHER = "OTHER"
VENUE_TYPE_MAP = {
    "journal": "JOURNAL",
    "trade journal": "JOURNAL",
    "conference and proceedings": "CONFERENCE",
    "conference proceedings": "CONFERENCE",
    "book series": "BOOK_SERIES",
}

SCIMAGO_SUBJECT_VOCAB = "SCImago"
SCIMAGO_SUBJECT_TYPE = "Category"
SCIMAGO_SUBJECT_SOURCE = "scimago"

# Manual country name → ISO 3166-1 alpha-2 overrides
COUNTRY_OVERRIDES = {
    "United States": "US",
    "United Kingdom": "GB",
    "South Korea": "KR",
    "Czech Republic": "CZ",
    "Russian Federation": "RU",
    "Iran": "IR",
    "Vietnam": "VN",
    "Taiwan": "TW",
    "Moldova": "MD",
    "Syrian Arab Republic": "SY",
    "Venezuela": "VE",
    "Bolivia": "BO",
    "Tanzania": "TZ",
    "Laos": "LA",
    "North Korea": "KP",
    "Brunei Darussalam": "BN",
    "Cote d'Ivoire": "CI",
    "Ivory Coast": "CI",
    "Palestine": "PS",
    "Reunion": "RE",
    "Macau": "MO",
}

CATEGORY_PATTERN = re.compile(r"([^;]+?)(?:\s*\((Q[1-4])\))?(?:;|$)")


def parse_decimal(value: Any) -> Optional[float]:
    """Parse decimal value from SCImago format (comma as decimal separator, dot as thousands)."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        # SCImago uses European format: 8.562 = 8562, 8,562 = 8.562
        # If there's a comma, it's the decimal separator
        if "," in raw:
            raw = raw.replace(".", "").replace(",", ".")
        return float(raw)
    except (TypeError, ValueError):
        return None


# parse_int imported from venues.shared


def get_country_code(country_name: str) -> Optional[str]:
    if not country_name:
        return None
    country_name = country_name.strip()
    if country_name in COUNTRY_OVERRIDES:
        return COUNTRY_OVERRIDES[country_name]
    try:
        return pycountry.countries.search_fuzzy(country_name)[0].alpha_2
    except LookupError:
        return None


def parse_issns(issn_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse SCImago ISSN field (comma-separated, no hyphens) into (issn, eissn)."""
    if not issn_str or not issn_str.strip():
        return None, None
    parts = [x.strip().replace('"', '') for x in issn_str.split(",")]
    normalized = []
    for p in parts:
        n = normalize_issn(p)
        if n:
            normalized.append(n)
    issn = normalized[0] if len(normalized) > 0 else None
    eissn = normalized[1] if len(normalized) > 1 else None
    return issn, eissn


def normalize_venue_type(raw_type: Optional[str]) -> str:
    if not raw_type:
        return VENUE_TYPE_OTHER
    return VENUE_TYPE_MAP.get(raw_type.strip().lower(), VENUE_TYPE_OTHER)


def parse_categories(categories_str: str) -> List[Dict[str, Optional[str]]]:
    """Parse SCImago categories like 'Anthropology (Q1); Cultural Studies (Q2)'."""
    if not categories_str:
        return []
    result = []
    for match in CATEGORY_PATTERN.finditer(categories_str):
        term = match.group(1).strip()
        quartile = match.group(2)  # Q1, Q2, Q3, Q4 or None
        if term:
            result.append({"term": term, "quartile": quartile})
    return result


def collect_csv_files(csv_dir: str, limit: Optional[int]) -> List[str]:
    files = []
    for name in sorted(os.listdir(csv_dir)):
        if not name.lower().endswith(".csv"):
            continue
        files.append(os.path.join(csv_dir, name))
        if limit and len(files) >= limit:
            break
    return files


def find_existing_venue(
    cursor: mariadb.Cursor,
    issn: Optional[str],
    eissn: Optional[str],
    scopus_id: Optional[str],
    name: Optional[str],
    venue_type: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    cols = (
        "id, name, type, issn, eissn, scopus_id, publisher_id, country_code, "
        "open_access, sjr, h_index, is_indexed_in_scopus, validation_status"
    )
    base = f"SELECT {cols} FROM venues"

    # 1. ISSN cross-match
    search_issns = sorted({v for v in [issn, eissn] if v})
    if search_issns:
        compact = sorted({v.replace("-", "") for v in search_issns})
        ph = ",".join(["?"] * len(search_issns))
        cph = ",".join(["?"] * len(compact))
        params = tuple(search_issns) * 2 + tuple(compact) * 2
        cursor.execute(
            f"{base} WHERE ("
            f"issn IN ({ph}) OR eissn IN ({ph}) "
            f"OR REPLACE(issn, '-', '') IN ({cph}) "
            f"OR REPLACE(eissn, '-', '') IN ({cph})"
            f") LIMIT 1",
            params,
        )
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "issn"

    # 2. scopus_id
    if scopus_id:
        cursor.execute(f"{base} WHERE scopus_id = ? LIMIT 1", (scopus_id,))
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "scopus_id"

    # 3. name+type
    if name:
        cursor.execute(f"{base} WHERE name = ? AND type = ? LIMIT 1", (name, venue_type))
        found = row_to_dict(cursor, cursor.fetchone())
        if found:
            return found, "name+type"

    return None, None


def get_or_create_scimago_subject(
    cursor: mariadb.Cursor,
    cache: Dict[str, Dict[str, int]],
    term: str,
) -> Optional[int]:
    term = term.strip()[:255]
    if not term:
        return None

    term_key = normalize_term_key(term)[:255]
    if not term_key:
        return None

    if term_key in cache["subjects_by_term_key"]:
        return cache["subjects_by_term_key"][term_key]

    # Look up existing by vocabulary + type + term_key
    cursor.execute(
        "SELECT id FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ? LIMIT 1",
        (SCIMAGO_SUBJECT_VOCAB, SCIMAGO_SUBJECT_TYPE, term_key),
    )
    row = cursor.fetchone()
    if row:
        subject_id = row[0]
    else:
        # Also check Scopus SubjectArea — SCImago categories are the same taxonomy
        cursor.execute(
            "SELECT id FROM subjects WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea' AND term_key = ? LIMIT 1",
            (term_key,),
        )
        row = cursor.fetchone()
        if row:
            subject_id = row[0]
        else:
            try:
                cursor.execute(
                    "INSERT INTO subjects (term, vocabulary, subject_type, lang, term_key) "
                    "VALUES (?, ?, ?, 'en', ?)",
                    (term, SCIMAGO_SUBJECT_VOCAB, SCIMAGO_SUBJECT_TYPE, term_key),
                )
                subject_id = cursor.lastrowid
            except mariadb.IntegrityError:
                cursor.execute(
                    "SELECT id FROM subjects WHERE vocabulary = ? AND subject_type = ? AND term_key = ? LIMIT 1",
                    (SCIMAGO_SUBJECT_VOCAB, SCIMAGO_SUBJECT_TYPE, term_key),
                )
                row = cursor.fetchone()
                subject_id = row[0] if row else None

    if subject_id:
        cache["subjects_by_term_key"][term_key] = subject_id
    return subject_id


QUARTILE_SCORE = {"Q1": 1.0, "Q2": 0.75, "Q3": 0.5, "Q4": 0.25}


def sync_venue_subjects(
    cursor: mariadb.Cursor,
    venue_id: int,
    categories: List[Dict[str, Optional[str]]],
    subject_cache: Dict[str, Dict[str, int]],
    dry_run: bool,
) -> int:
    if not categories:
        return 0

    pairs: List[Tuple[int, Optional[float]]] = []
    for cat in categories:
        sid = get_or_create_scimago_subject(cursor, subject_cache, cat["term"])
        if sid:
            score = QUARTILE_SCORE.get(cat.get("quartile"))
            pairs.append((sid, score))

    if not pairs:
        return 0

    unique = {}
    for sid, score in pairs:
        if sid not in unique or (score is not None and (unique[sid] is None or score > unique[sid])):
            unique[sid] = score

    if dry_run:
        return len(unique)

    linked = 0
    for sid, score in unique.items():
        try:
            cursor.execute(
                "INSERT INTO venue_subjects (venue_id, subject_id, score, source) VALUES (?, ?, ?, ?) "
                "ON DUPLICATE KEY UPDATE score = GREATEST(COALESCE(score, 0), COALESCE(?, 0)), source = ?",
                (venue_id, sid, score, SCIMAGO_SUBJECT_SOURCE, score, SCIMAGO_SUBJECT_SOURCE),
            )
            linked += 1
        except mariadb.Error:
            pass
    return linked


def build_venue_updates(
    existing: Dict[str, Any],
    parsed: Dict[str, Any],
    publisher_id: Optional[int],
) -> Tuple[List[str], List[Any]]:
    updates: List[str] = []
    params: List[Any] = []

    def add_if_better(field: str, new_val: Any) -> None:
        if new_val is None:
            return
        old_val = existing.get(field)
        if old_val is None or old_val != new_val:
            updates.append(f"{field} = ?")
            params.append(new_val)

    def fill_if_null(field: str, new_val: Any) -> None:
        if new_val is None:
            return
        if existing.get(field) is None:
            updates.append(f"{field} = ?")
            params.append(new_val)

    # Backfill identifiers
    fill_if_null("issn", parsed.get("issn"))
    fill_if_null("eissn", parsed.get("eissn"))
    fill_if_null("scopus_id", parsed.get("scopus_id"))
    fill_if_null("publisher_id", publisher_id)
    fill_if_null("country_code", parsed.get("country_code"))

    # Metrics: update if SCImago has better/newer data
    add_if_better("sjr", parsed.get("sjr"))
    add_if_better("open_access", parsed.get("open_access"))

    # h_index: only increase
    new_h = parsed.get("h_index")
    if new_h is not None:
        old_h = existing.get("h_index")
        if old_h is None or new_h > old_h:
            updates.append("h_index = ?")
            params.append(new_h)

    # Mark as indexed in Scopus (SCImago is a Scopus-derived ranking)
    if not existing.get("is_indexed_in_scopus"):
        updates.append("is_indexed_in_scopus = 1")

    # Validate
    if existing.get("validation_status") in ("PENDING", None):
        updates.append("validation_status = 'VALIDATED'")

    updates.append("last_validated_at = CURRENT_TIMESTAMP")
    return updates, params


def process_csv_row(
    cursor: mariadb.Cursor,
    row: Dict[str, str],
    common_cache: Dict[str, Dict],
    subject_cache: Dict[str, Dict[str, int]],
    dry_run: bool,
) -> Tuple[str, str]:
    """Process a single SCImago CSV row. Returns (outcome_label, venue_label)."""
    sourceid = as_clean_string(row.get("Sourceid"), 50)
    title = as_clean_string(row.get("Title"), 512)
    if not title:
        return "skipped", "(no title)"

    raw_type = row.get("Type", "")
    venue_type = normalize_venue_type(raw_type)
    issn, eissn = parse_issns(row.get("Issn", ""))
    publisher_name = as_clean_string(row.get("Publisher"), 512)
    country_name = as_clean_string(row.get("Country"), 100)
    country_code = get_country_code(country_name) if country_name else None
    categories = parse_categories(row.get("Categories", ""))

    sjr = parse_decimal(row.get("SJR"))
    h_index = parse_int(row.get("H index"))
    is_oa = 1 if (row.get("Open Access") or "").strip().lower() in ("yes", "oa") else 0

    parsed = {
        "name": title,
        "venue_type": venue_type,
        "issn": issn,
        "eissn": eissn,
        "scopus_id": sourceid,
        "country_code": country_code,
        "sjr": sjr,
        "h_index": h_index,
        "open_access": is_oa,
    }

    publisher_id = None
    if publisher_name:
        publisher_id = get_or_create_organization(
            cursor, publisher_name, ORG_TYPE_PUBLISHER, common_cache,
            country_code=country_code,
        )

    existing, matched_by = find_existing_venue(cursor, issn, eissn, sourceid, title, venue_type)
    venue_label = title

    if existing:
        venue_id = existing["id"]
        venue_label = f"{venue_id} {existing.get('name') or title}"

        updates, params = build_venue_updates(existing, parsed, publisher_id)
        n_fields = max(0, len(updates) - 1)  # exclude last_validated_at
        did_update = False

        if not dry_run and updates:
            params.append(venue_id)
            cursor.execute(
                "UPDATE venues SET " + ", ".join(updates) + " WHERE id = ?",
                tuple(params),
            )
            did_update = cursor.rowcount > 0

        linked = sync_venue_subjects(cursor, venue_id, categories, subject_cache, dry_run)

        parts = [matched_by]
        if n_fields:
            parts.append(f"{n_fields}f")
        if linked:
            parts.append(f"+{linked}s")
        detail = ", ".join(parts)

        if did_update or (dry_run and n_fields):
            return f"Updated ({detail})", venue_label
        return f"OK ({detail})", venue_label

    else:
        # Insert new venue
        values = {
            "name": title,
            "type": venue_type,
            "issn": issn,
            "eissn": eissn,
            "scopus_id": sourceid,
            "publisher_id": publisher_id,
            "country_code": country_code,
            "sjr": sjr,
            "h_index": h_index,
            "open_access": is_oa,
            "is_indexed_in_scopus": 1,
            "validation_status": "VALIDATED",
        }
        cols = []
        ins_params = []
        for k, v in values.items():
            if v is not None:
                cols.append(k)
                ins_params.append(v)
        cols.append("last_validated_at")

        if dry_run:
            linked = sync_venue_subjects(cursor, -1, categories, subject_cache, dry_run=True)
            detail = f"+{linked}s" if linked else ""
            return f"Created{detail}", venue_label

        placeholders = ", ".join(["?"] * len(ins_params))
        query = f"INSERT INTO venues ({', '.join(cols)}) VALUES ({placeholders}, CURRENT_TIMESTAMP)"
        try:
            cursor.execute(query, tuple(ins_params))
            venue_id = cursor.lastrowid
        except mariadb.IntegrityError:
            # Race / unique conflict — try to find the venue
            existing, _ = find_existing_venue(cursor, issn, eissn, sourceid, title, venue_type)
            if existing:
                venue_id = existing["id"]
            else:
                return "Error (insert conflict)", venue_label

        linked = sync_venue_subjects(cursor, venue_id, categories, subject_cache, dry_run)
        detail = f" +{linked}s" if linked else ""
        venue_label = f"{venue_id} {title}"
        return f"Created{detail}", venue_label


def process_scimago_csv(
    conn: mariadb.Connection,
    csv_path: str,
    dry_run: bool,
    commit_batch: int,
    common_cache: Dict[str, Dict],
    subject_cache: Dict[str, Dict[str, int]],
) -> Dict[str, int]:
    counters = {
        "processed": 0,
        "updated": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
    }

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        rows = list(reader)

    total = len(rows)
    log.info("--- Processing %s: %d rows ---", os.path.basename(csv_path), total)

    pending_commits = 0
    seen_sourceids = set()

    for idx, row in enumerate(rows, start=1):
        counters["processed"] += 1
        prefix = f"[{idx}/{total}]"

        # Deduplicate within file by Sourceid
        sourceid = (row.get("Sourceid") or "").strip()
        if sourceid and sourceid in seen_sourceids:
            log.info("%s %s → Skipped (duplicate sourceid)", prefix, row.get("Title", ""))
            counters["skipped"] += 1
            continue
        if sourceid:
            seen_sourceids.add(sourceid)

        cursor = conn.cursor()
        try:
            outcome, venue_label = process_csv_row(
                cursor, row, common_cache, subject_cache, dry_run,
            )
            log.info("%s %s → %s", prefix, venue_label, outcome)

            if outcome.startswith("Updated"):
                counters["updated"] += 1
                pending_commits += 1
            elif outcome.startswith("Created"):
                counters["created"] += 1
                pending_commits += 1
            elif outcome.startswith("OK"):
                pending_commits += 1  # last_validated_at still updated
            elif outcome.startswith("Error"):
                counters["errors"] += 1
            else:
                counters["skipped"] += 1

            if not dry_run and pending_commits > 0 and (commit_batch == 0 or pending_commits >= commit_batch):
                conn.commit()
                pending_commits = 0

        except Exception as e:
            counters["errors"] += 1
            if not dry_run:
                safe_rollback(conn, f"scimago {os.path.basename(csv_path)} row={idx}")
                pending_commits = 0
            log.error("%s %s → Error: %s", prefix, row.get("Title", ""), e)
            log.debug("Traceback:", exc_info=True)
        finally:
            cursor.close()

    if not dry_run and pending_commits > 0:
        conn.commit()

    return counters


def main() -> None:
    parser = argparse.ArgumentParser(description="Load SCImago venue data into the database.")
    parser.add_argument("csv_dir", help="Directory containing SCImago CSV files, or a single CSV file path.")
    parser.add_argument("--limit", type=int, default=None, help="Max CSV files to process.")
    parser.add_argument("--commit-batch", type=int, default=200, help="Commit every N changes (0 = per change).")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB.")
    parser.add_argument("--config", default=None, help="Path to config.ini.")
    args = parser.parse_args()

    if os.path.isfile(args.csv_dir):
        csv_files = [args.csv_dir]
    elif os.path.isdir(args.csv_dir):
        csv_files = collect_csv_files(args.csv_dir, args.limit)
    else:
        log.error("Path not found: %s", args.csv_dir)
        sys.exit(1)

    if not csv_files:
        log.warning("No CSV files to process.")
        return

    log.info("=== SCImago venue load: %d file(s) ===", len(csv_files))

    conn = None
    try:
        conn = get_connection(config_path=args.config)
        common_cache = build_cache()
        subject_cache: Dict[str, Dict[str, int]] = {"subjects_by_term_key": {}}

        grand_totals: Dict[str, int] = {}
        for csv_path in csv_files:
            conn = ensure_connection(conn, config_path=args.config)
            counters = process_scimago_csv(
                conn, csv_path, args.dry_run, args.commit_batch,
                common_cache, subject_cache,
            )
            for k, v in counters.items():
                grand_totals[k] = grand_totals.get(k, 0) + v
            log.info("--- File done: %s ---", counters)

        log.info("=== Finished: %s ===", grand_totals)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
