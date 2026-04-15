#!/usr/bin/env python3
"""
Unified database text cleanup and normalization.

Phases (executed in order):
  text            – Text cleanup (HTML, control chars, Unicode, edge trimming).
  sentinel        – Convert placeholders (none, null, n/a) to SQL NULL.
  identifiers     – Normalize DOI, ORCID, ISSN, ISBN.
  title_subtitle  – Split works.title into title + subtitle on first ":".
  capitalization  – Fix ALL CAPS → title case, all lower → sentence case.

Usage:
  python cleanup.py                                         # profile + all phases
  python cleanup.py --mode profile                          # profile only
  python cleanup.py --mode run                              # all phases
  python cleanup.py --phase text --table works              # text on works only
  python cleanup.py --phase text --table organizations --column name
  python cleanup.py --dry-run                               # simulate without writing
"""
import argparse
import html
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import mariadb
from titlecase import titlecase

try:
    from pipeline.transform.common import get_connection
except ModuleNotFoundError:
    from common import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

SENTINEL_TO_NULL = {
    "none", "null", "n/a", "na",
    "[no title available]", "no title available", "title unavailable",
}

CORE_PROFILE_TABLES = [
    "works", "publications", "persons", "organizations",
    "venues", "subjects", "authorships", "files", "work_references",
]

# Tables cleanup must not touch:
# - summary_*: truncated and rebuilt by sp_orchestrate_all_summaries.
# - subject_relevance_tiers / venue_ranking_rules / subject_stoplist:
#   curated reference data — altering strings would corrupt the ranking rules.
EXCLUDED_TABLES = {
    "summary_publications",
    "summary_venues",
    "summary_persons",
    "subject_relevance_tiers",
    "venue_ranking_rules",
    "subject_stoplist",
}

EXCLUDED_COLUMNS = {
    "id", "doi", "isbn", "issn", "eissn", "orcid", "ror_id",
    "scopus_id", "lattes_id", "license_url", "file_hash", "md5",
    "title_normalized", "term_key", "sha1", "sha256",
}

# --- regex ---
HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTISPACE_RE = re.compile(r"\s+")
ABSTRACT_PREFIX_RE = re.compile(
    r"^(abstract|resumo|resumen)\s*[:\-–—]*\s*", re.IGNORECASE,
)
INVISIBLE_OR_CONTROL_RE = re.compile(
    r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F-\u009F"
    r"\u200B-\u200F\u202A-\u202E\u2060\uFEFF\uFFFD]",
)
DOI_PREFIX_RE = re.compile(
    r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", re.IGNORECASE,
)
DOI_SHAPE_RE = re.compile(r"^10\.\S+/\S+$", re.IGNORECASE)
ORCID_PREFIX_RE = re.compile(
    r"^(?:https?://orcid\.org/|orcid:\s*)", re.IGNORECASE,
)
ORCID_SHAPE_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-[\dX]{4}$")

# --- title capitalization ---
_ALLCAPS_THRESHOLD = 0.8  # fraction of alpha chars that must be upper


# ---------------------------------------------------------------------------
#  Dataclasses
# ---------------------------------------------------------------------------

_SHRINK_RATIO_THRESHOLD = 0.01   # reject if cleaned < 1% of original length
_SHRINK_MIN_ORIGINAL_LEN = 20   # only check ratio when original is this long

_SKIP = object()  # sentinel: _check_update returns this to mean "do not update"


@dataclass
class ColumnStats:
    scanned: int = 0
    changed: int = 0
    updated: int = 0
    skipped_integrity: int = 0
    skipped_empty: int = 0
    skipped_shrink: int = 0
    errors: int = 0

    def add(self, other: "ColumnStats") -> None:
        for attr in (
            "scanned", "changed", "updated",
            "skipped_integrity", "skipped_empty", "skipped_shrink", "errors",
        ):
            setattr(self, attr, getattr(self, attr) + getattr(other, attr))


# ---------------------------------------------------------------------------
#  Text helpers
# ---------------------------------------------------------------------------

def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def clean_text_value(
    value: Optional[str], remove_abstract_prefix: bool = False,
) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    # Strip real HTML tags BEFORE unescaping entities — otherwise
    # "&lt;text&gt;" becomes "<text>" which HTML_TAG_RE destroys.
    cleaned = HTML_TAG_RE.sub(" ", value)
    cleaned = html.unescape(cleaned)
    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = INVISIBLE_OR_CONTROL_RE.sub(" ", cleaned)
    cleaned = cleaned.replace("\ufffd", " ")
    cleaned = MULTISPACE_RE.sub(" ", cleaned).strip()
    cleaned = trim_intrusive_edges(cleaned)
    if remove_abstract_prefix and cleaned:
        cleaned = ABSTRACT_PREFIX_RE.sub("", cleaned).strip()
        cleaned = trim_intrusive_edges(cleaned)
    return cleaned if cleaned else ""


def trim_intrusive_edges(value: str) -> str:
    if not value:
        return value
    start, end = 0, len(value)
    while start < end and _is_intrusive_edge(value[start]):
        start += 1
    while end > start and _is_intrusive_edge(value[end - 1]):
        end -= 1
    return value[start:end].strip()


def _is_intrusive_edge(ch: str) -> bool:
    if ch.isspace():
        return True
    cat = unicodedata.category(ch)
    # Strip: control chars, dashes, connectors, "other" punctuation (.,;:!?…)
    # Keep:  Ps/Pe (brackets: ()[]{}), Pi/Pf (quotes: «»‹›) — these are paired
    if cat in {"Cc", "Cf", "Cs"}:
        return True
    if cat in {"Po", "Pd", "Pc"}:
        return True
    return ch in {"_", "|", "·", "•", "‒", "–", "—", "-", "…"}


# --- identifier normalizers ---

def normalize_doi(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    doi = value.strip()
    if not doi:
        return ""
    doi = DOI_PREFIX_RE.sub("", doi)
    doi = doi.replace("\u00A0", " ")
    doi = re.sub(r"\s+", "", doi)
    doi = doi.strip().strip("\"'")
    doi = re.sub(r"[.,;:]+$", "", doi)
    return doi.lower()


def normalize_orcid(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    orcid = ORCID_PREFIX_RE.sub("", value.strip()).upper()
    if not orcid:
        return ""
    compact = re.sub(r"[^0-9X]", "", orcid)
    if len(compact) == 16:
        orcid = f"{compact[0:4]}-{compact[4:8]}-{compact[8:12]}-{compact[12:16]}"
    return orcid


def normalize_issn(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    issn = re.sub(r"[^0-9Xx]", "", value.strip())
    if not issn:
        return ""
    if len(issn) == 8:
        return f"{issn[:4]}-{issn[4:]}".upper()
    return value.strip()


def normalize_isbn(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    isbn = value.strip()
    if not isbn:
        return ""
    return re.sub(r"\s+", "", isbn).upper()


# ---------------------------------------------------------------------------
#  Title capitalization
# ---------------------------------------------------------------------------

def _is_all_upper(text: str) -> bool:
    """Return True if text is effectively ALL CAPS."""
    alpha = [c for c in text if c.isalpha()]
    if len(alpha) < 4:
        return False
    upper_count = sum(1 for c in alpha if c.isupper())
    return upper_count / len(alpha) >= _ALLCAPS_THRESHOLD


def _is_all_lower(text: str) -> bool:
    """Return True if text is effectively all lowercase."""
    alpha = [c for c in text if c.isalpha()]
    if len(alpha) < 4:
        return False
    return all(c.islower() for c in alpha)


def fix_capitalization(value: Optional[str]) -> Optional[str]:
    """Fix ALL CAPS or all-lowercase titles/subtitles using titlecase.

    ALL CAPS  → titlecase.
    all lower → titlecase.
    Mixed     → unchanged (return original).
    """
    if not value or not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value

    if _is_all_upper(text) or _is_all_lower(text):
        return titlecase(text)

    return value


# ---------------------------------------------------------------------------
#  DB introspection (from text.py — uses STATISTICS for precise is_unique)
# ---------------------------------------------------------------------------

def get_database_name(conn: mariadb.Connection) -> str:
    cur = conn.cursor()
    cur.execute("SELECT DATABASE()")
    name = cur.fetchone()[0]
    cur.close()
    if not name:
        raise RuntimeError("No schema selected on connection.")
    return name


def discover_target_columns(
    conn: mariadb.Connection,
    include_tables: Optional[set] = None,
    include_columns: Optional[set] = None,
    skip_column_exclusions: bool = False,
) -> List[Dict]:
    db_name = get_database_name(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          c.TABLE_NAME,
          c.COLUMN_NAME,
          GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS pk_cols,
          MAX(CASE WHEN s.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END) AS is_unique,
          MAX(CASE WHEN c.COLUMN_NAME = kcu.COLUMN_NAME THEN 1 ELSE 0 END) AS is_pk_col
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND tc.TABLE_NAME = c.TABLE_NAME
         AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
         AND kcu.TABLE_NAME = tc.TABLE_NAME
         AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        LEFT JOIN INFORMATION_SCHEMA.STATISTICS s
          ON s.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND s.TABLE_NAME = c.TABLE_NAME
         AND s.COLUMN_NAME = c.COLUMN_NAME
         AND s.NON_UNIQUE = 0
        WHERE c.TABLE_SCHEMA = ?
          AND c.DATA_TYPE IN ('varchar','text','mediumtext','longtext')
          AND c.EXTRA NOT LIKE '%GENERATED%'
        GROUP BY c.TABLE_NAME, c.COLUMN_NAME
        ORDER BY c.TABLE_NAME, c.COLUMN_NAME
        """,
        (db_name,),
    )
    rows = cur.fetchall()
    cur.close()

    targets: List[Dict] = []
    for table, column, pk_columns, is_unique, is_pk_col in rows:
        if table in EXCLUDED_TABLES:
            continue
        if not skip_column_exclusions and column in EXCLUDED_COLUMNS:
            continue
        if include_tables and table not in include_tables:
            continue
        if include_columns and column not in include_columns:
            continue
        if not pk_columns:
            continue
        pk_cols = [p.strip() for p in str(pk_columns).split(",") if p.strip()]
        targets.append({
            "table": table,
            "column": column,
            "pk_cols": pk_cols,
            "is_unique": bool(is_unique),
            "is_pk_column": bool(is_pk_col),
        })
    return targets


# ---------------------------------------------------------------------------
#  Batch read
# ---------------------------------------------------------------------------

def fetch_rows_single_pk(
    cur: mariadb.Cursor, table: str, column: str, pk_col: str,
    batch_size: int, last_pk: Optional[object],
    limit: int, fetched: int,
) -> List[Tuple]:
    tq, cq, pkq = quote_ident(table), quote_ident(column), quote_ident(pk_col)
    if limit > 0:
        remaining = limit - fetched
        if remaining <= 0:
            return []
        batch_size = min(batch_size, remaining)
    if last_pk is None:
        cur.execute(
            f"SELECT {pkq}, {cq} FROM {tq} "
            f"WHERE {cq} IS NOT NULL AND {cq} <> '' ORDER BY {pkq} LIMIT ?",
            (batch_size,),
        )
    else:
        cur.execute(
            f"SELECT {pkq}, {cq} FROM {tq} "
            f"WHERE {cq} IS NOT NULL AND {cq} <> '' AND {pkq} > ? ORDER BY {pkq} LIMIT ?",
            (last_pk, batch_size),
        )
    return cur.fetchall()


def fetch_rows_multi_pk(
    cur: mariadb.Cursor, table: str, column: str, pk_cols: Sequence[str],
    batch_size: int, offset: int, limit: int,
) -> List[Tuple]:
    tq, cq = quote_ident(table), quote_ident(column)
    pk_select = ", ".join(quote_ident(pk) for pk in pk_cols)
    pk_order = ", ".join(quote_ident(pk) for pk in pk_cols)
    if limit > 0:
        remaining = limit - offset
        if remaining <= 0:
            return []
        batch_size = min(batch_size, remaining)
    cur.execute(
        f"SELECT {pk_select}, {cq} FROM {tq} "
        f"WHERE {cq} IS NOT NULL AND {cq} <> '' ORDER BY {pk_order} LIMIT ? OFFSET ?",
        (batch_size, offset),
    )
    return cur.fetchall()


# ---------------------------------------------------------------------------
#  Batch write — always bulk first, row-by-row fallback on conflict
# ---------------------------------------------------------------------------

def flush_updates(
    conn: mariadb.Connection,
    table: str, column: str, pk_cols: Sequence[str],
    updates: List[Tuple],
    dry_run: bool,
    stats: ColumnStats,
) -> None:
    if not updates:
        return
    if dry_run:
        stats.updated += len(updates)
        return

    tq = quote_ident(table)
    cq = quote_ident(column)
    where_clause = " AND ".join(f"{quote_ident(pk)} = ?" for pk in pk_cols)
    sql = f"UPDATE {tq} SET {cq} = ? WHERE {where_clause}"

    cur = conn.cursor()

    # Always try bulk first — handles 99%+ of cases instantly
    try:
        cur.executemany(sql, updates)
        conn.commit()
        stats.updated += len(updates)
        cur.close()
        return
    except mariadb.Error:
        conn.rollback()
        cur.close()
        # Fall through to row-by-row with a fresh cursor

    # Row-by-row with savepoints — fresh cursor after rollback
    cur = conn.cursor()
    for params in updates:
        try:
            cur.execute("SAVEPOINT sp_cleanup")
            cur.execute(sql, params)
            cur.execute("RELEASE SAVEPOINT sp_cleanup")
            stats.updated += 1
        except mariadb.IntegrityError:
            cur.execute("ROLLBACK TO SAVEPOINT sp_cleanup")
            cur.execute("RELEASE SAVEPOINT sp_cleanup")
            stats.skipped_integrity += 1
        except mariadb.Error:
            cur.execute("ROLLBACK TO SAVEPOINT sp_cleanup")
            cur.execute("RELEASE SAVEPOINT sp_cleanup")
            stats.errors += 1

    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
#  Column processor (generic — used by all phases)
# ---------------------------------------------------------------------------

def process_column(
    conn: mariadb.Connection,
    table: str, column: str, pk_cols: Sequence[str],
    transform: Callable[[Optional[str]], Optional[str]],
    batch_size: int, flush_size: int,
    limit: int, dry_run: bool, verbose_every: int,
    shrink_guard: bool = True,
) -> ColumnStats:
    stats = ColumnStats()
    updates: List[Tuple] = []
    read_cur = conn.cursor()

    def _check_update(raw: str, cleaned, stats: ColumnStats):
        """Validate a cleaned value before queuing an update.

        Returns _SKIP to reject, None to set SQL NULL, or a string to write.
        """
        # transform returned "no change"
        if cleaned == raw:
            return _SKIP
        # transform signals "set to NULL" (used by sentinel phase)
        if cleaned is None:
            stats.changed += 1
            return None  # will become SQL NULL
        # cleaned to empty string — reject
        if cleaned == "":
            stats.skipped_empty += 1
            return _SKIP
        # --- shrink guard: reject drastic truncation ---
        if shrink_guard:
            raw_len = len(raw)
            if (raw_len >= _SHRINK_MIN_ORIGINAL_LEN
                    and len(cleaned) / raw_len < _SHRINK_RATIO_THRESHOLD):
                if stats.skipped_shrink == 0:
                    log.warning(
                        "  SHRINK GUARD: %s.%s — original (%d chars) → cleaned "
                        "(%d chars): %r → %r",
                        table, column, raw_len, len(cleaned),
                        raw[:80], cleaned[:80],
                    )
                stats.skipped_shrink += 1
                return _SKIP
        stats.changed += 1
        return cleaned

    if len(pk_cols) == 1:
        last_pk = None
        while True:
            rows = fetch_rows_single_pk(
                read_cur, table, column, pk_cols[0],
                batch_size, last_pk, limit, stats.scanned,
            )
            if not rows:
                break
            for pk_val, raw in rows:
                stats.scanned += 1
                value = _check_update(raw, transform(raw), stats)
                if value is _SKIP:
                    continue
                updates.append((value, pk_val))
                if len(updates) >= flush_size:
                    flush_updates(conn, table, column, pk_cols, updates, dry_run, stats)
                    updates.clear()
            last_pk = rows[-1][0]
            if verbose_every > 0 and stats.scanned % verbose_every == 0:
                log.info(
                    f"  {table}.{column}: scanned={stats.scanned} "
                    f"changed={stats.changed} updated={stats.updated} "
                    f"skip_integrity={stats.skipped_integrity} "
                    f"skip_shrink={stats.skipped_shrink}"
                )
    else:
        offset = 0
        while True:
            rows = fetch_rows_multi_pk(
                read_cur, table, column, pk_cols,
                batch_size, offset, limit,
            )
            if not rows:
                break
            for row in rows:
                raw = row[-1]
                pk_vals = row[:-1]
                stats.scanned += 1
                value = _check_update(raw, transform(raw), stats)
                if value is _SKIP:
                    continue
                updates.append((value, *pk_vals))
                if len(updates) >= flush_size:
                    flush_updates(conn, table, column, pk_cols, updates, dry_run, stats)
                    updates.clear()
            offset += len(rows)
            if verbose_every > 0 and stats.scanned % verbose_every == 0:
                log.info(
                    f"  {table}.{column}: scanned={stats.scanned} "
                    f"changed={stats.changed} updated={stats.updated} "
                    f"skip_integrity={stats.skipped_integrity} "
                    f"skip_shrink={stats.skipped_shrink}"
                )

    read_cur.close()
    if updates:
        flush_updates(conn, table, column, pk_cols, updates, dry_run, stats)
    return stats


# ---------------------------------------------------------------------------
#  Phase runners
# ---------------------------------------------------------------------------

def run_text_phase(
    conn: mariadb.Connection, args: argparse.Namespace,
    include_tables: Optional[set], include_columns: Optional[set],
) -> ColumnStats:
    log.info("=== Fase: text ===")
    targets = discover_target_columns(conn, include_tables, include_columns)
    if not targets:
        log.info("No target columns.")
        return ColumnStats()

    total = ColumnStats()
    for t in targets:
        if t["is_pk_column"]:
            continue
        remove_abs = t["table"] == "works" and t["column"] == "abstract"
        transform = lambda v, r=remove_abs: clean_text_value(v, remove_abstract_prefix=r)
        stats = process_column(
            conn, t["table"], t["column"], t["pk_cols"], transform,
            args.batch_size, args.flush_size,
            args.limit_per_column, args.dry_run, args.verbose_every,
        )
        total.add(stats)
        if stats.changed or stats.errors:
            log.info(
                f"[text] {t['table']}.{t['column']}: scanned={stats.scanned} "
                f"changed={stats.changed} updated={stats.updated} "
                f"skip_integrity={stats.skipped_integrity} "
                f"skip_empty={stats.skipped_empty} skip_shrink={stats.skipped_shrink} "
                f"errors={stats.errors}"
            )
    return total


def _sentinel_transform(value: Optional[str]) -> Optional[str]:
    """Return None to signal 'set column to SQL NULL' for sentinel values."""
    if value is None:
        return value  # already NULL, no change (will match raw → skip)
    if not isinstance(value, str):
        value = str(value)
    trimmed = value.strip()
    if not trimmed:
        return value  # empty but not a sentinel
    return None if trimmed.lower() in SENTINEL_TO_NULL else value


def run_sentinel_phase(
    conn: mariadb.Connection, args: argparse.Namespace,
    include_tables: Optional[set], include_columns: Optional[set],
) -> ColumnStats:
    log.info("=== Fase: sentinel ===")
    targets = [
        t for t in discover_target_columns(conn, include_tables, include_columns)
        if not t["is_pk_column"]
    ]
    if not targets:
        log.info("No target columns.")
        return ColumnStats()

    total = ColumnStats()
    for t in targets:
        stats = process_column(
            conn, t["table"], t["column"], t["pk_cols"], _sentinel_transform,
            args.batch_size, args.flush_size,
            args.limit_per_column, args.dry_run, args.verbose_every,
            shrink_guard=False,
        )
        total.add(stats)
        if stats.changed:
            log.info(
                f"[sentinel] {t['table']}.{t['column']}: scanned={stats.scanned} "
                f"changed={stats.changed} updated={stats.updated} "
                f"skip_integrity={stats.skipped_integrity} errors={stats.errors}"
            )
    return total


def _dedup_work_references_cited_doi(
    conn: mariadb.Connection, dry_run: bool,
) -> int:
    """Delete work_references rows whose cited_doi, once normalized, duplicates
    an existing row for the same citing_work_id.

    This handles two patterns that normalize_doi changes:
      - trailing punctuation  (e.g. '10.1080/xxx.' → '10.1080/xxx')
      - embedded spaces       (e.g. '10.1177/026 123' → '10.1177/026123')

    Because the collation is case-insensitive, lowercasing alone never causes
    a conflict — only structural changes (space/punct removal) do.
    """
    cur = conn.cursor()

    # Find rows where normalizing cited_doi would collide with an existing row.
    # We use a Python-side pass to apply the same normalize_doi logic.
    cur.execute(
        "SELECT wr.id, wr.citing_work_id, wr.cited_doi "
        "FROM work_references wr "
        "WHERE wr.cited_doi REGEXP '[.,;:]+$' "
        "   OR wr.cited_doi LIKE '%% %%'"
    )
    candidates = cur.fetchall()
    cur.close()

    if not candidates:
        return 0

    # For each candidate, check if the normalized form already exists
    ids_to_delete: List[int] = []
    check_cur = conn.cursor()
    for row_id, citing_wid, raw_doi in candidates:
        norm = normalize_doi(raw_doi)
        if norm == raw_doi:
            continue
        # Check if normalized DOI already exists for this citing_work_id
        check_cur.execute(
            "SELECT 1 FROM work_references "
            "WHERE citing_work_id = ? AND cited_doi = ? AND id <> ? LIMIT 1",
            (citing_wid, norm, row_id),
        )
        if check_cur.fetchone():
            ids_to_delete.append(row_id)
    check_cur.close()

    if not ids_to_delete:
        return 0

    log.info(
        f"[id] work_references.cited_doi: {len(ids_to_delete)} duplicate rows "
        f"to delete before normalization"
    )

    if dry_run:
        return len(ids_to_delete)

    # Delete in batches
    del_cur = conn.cursor()
    batch = 1000
    for i in range(0, len(ids_to_delete), batch):
        chunk = ids_to_delete[i : i + batch]
        placeholders = ",".join("?" for _ in chunk)
        del_cur.execute(
            f"DELETE FROM work_references WHERE id IN ({placeholders})",
            chunk,
        )
        conn.commit()
    del_cur.close()

    log.info(f"[id] work_references.cited_doi: deleted {len(ids_to_delete)} duplicates")
    return len(ids_to_delete)


def run_identifier_phase(
    conn: mariadb.Connection, args: argparse.Namespace,
) -> ColumnStats:
    log.info("=== Fase: identifiers ===")
    total = ColumnStats()
    id_targets: List[Tuple[str, str, Callable]] = [
        ("publications", "doi", normalize_doi),
        ("work_references", "cited_doi", normalize_doi),
        ("persons", "orcid", normalize_orcid),
        ("venues", "issn", normalize_issn),
        ("venues", "eissn", normalize_issn),
        ("publications", "isbn", normalize_isbn),
    ]
    for table, column, fn in id_targets:
        if table == "work_references" and column == "cited_doi":
            _dedup_work_references_cited_doi(conn, args.dry_run)
        cols = discover_target_columns(
            conn, {table}, {column}, skip_column_exclusions=True,
        )
        if not cols:
            continue
        t = cols[0]
        stats = process_column(
            conn, t["table"], t["column"], t["pk_cols"], fn,
            args.batch_size, args.flush_size,
            args.limit_per_column, args.dry_run, args.verbose_every,
            shrink_guard=False,
        )
        total.add(stats)
        if stats.changed or stats.errors:
            log.info(
                f"[id] {table}.{column}: scanned={stats.scanned} "
                f"changed={stats.changed} updated={stats.updated} "
                f"skip_integrity={stats.skipped_integrity} errors={stats.errors}"
            )
    return total


# ---------------------------------------------------------------------------
#  Profile
# ---------------------------------------------------------------------------

def print_profile(conn: mariadb.Connection) -> None:
    log.info("=== Database Profile ===")
    log.info(f"schema: {get_database_name(conn)}")

    cur = conn.cursor()
    cur.execute(
        "SELECT table_name, table_rows FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_type='BASE TABLE' ORDER BY table_name"
    )
    table_rows = {t: int(r or 0) for t, r in cur.fetchall()}
    log.info(f"base_tables: {len(table_rows)}")
    for t in CORE_PROFILE_TABLES:
        if t in table_rows:
            log.info(f"  {t}: ~{table_rows[t]} rows")

    profile_queries = [
        ("works.title leading punct",
         "SELECT COUNT(*) FROM works WHERE title REGEXP '^[[:space:][:punct:]_\\\\|]+'"),
        ("works.abstract html",
         "SELECT COUNT(*) FROM works WHERE abstract REGEXP '<[^>]+>'"),
        ("persons.preferred_name leading punct",
         "SELECT COUNT(*) FROM persons WHERE preferred_name REGEXP '^[[:space:][:punct:]_\\\\|]+'"),
        ("organizations.name double spaces",
         "SELECT COUNT(*) FROM organizations WHERE name REGEXP '[[:space:]]{2,}'"),
        ("venues.name leading punct",
         "SELECT COUNT(*) FROM venues WHERE name REGEXP '^[[:space:][:punct:]_\\\\|]+'"),
        ("publications.doi upper",
         "SELECT COUNT(*) FROM publications WHERE doi IS NOT NULL AND BINARY doi <> BINARY LOWER(doi)"),
        ("publications.doi prefixed",
         "SELECT COUNT(*) FROM publications WHERE doi IS NOT NULL "
         "AND LOWER(doi) REGEXP '^(https?://(dx\\\\.)?doi\\\\.org/|doi:)'"),
        ("work_references.cited_doi upper",
         "SELECT COUNT(*) FROM work_references WHERE cited_doi IS NOT NULL "
         "AND BINARY cited_doi <> BINARY LOWER(cited_doi)"),
        ("venues.issn without hyphen",
         "SELECT COUNT(*) FROM venues WHERE issn IS NOT NULL AND issn REGEXP '^[0-9Xx]{8}$'"),
        ("venues.eissn without hyphen",
         "SELECT COUNT(*) FROM venues WHERE eissn IS NOT NULL AND eissn REGEXP '^[0-9Xx]{8}$'"),
        ("works.title with : and no subtitle",
         "SELECT COUNT(*) FROM works WHERE title LIKE '%:%' AND subtitle IS NULL"),
        ("works.title ALL CAPS",
         "SELECT COUNT(*) FROM works WHERE BINARY title = BINARY UPPER(title) "
         "AND title <> '' AND CHAR_LENGTH(title) > 3"),
        ("works.title all lowercase",
         "SELECT COUNT(*) FROM works WHERE BINARY title = BINARY LOWER(title) "
         "AND title <> '' AND CHAR_LENGTH(title) > 10"),
        ("works.subtitle ALL CAPS",
         "SELECT COUNT(*) FROM works WHERE subtitle IS NOT NULL "
         "AND BINARY subtitle = BINARY UPPER(subtitle) AND subtitle <> '' AND CHAR_LENGTH(subtitle) > 3"),
        ("works.subtitle all lowercase",
         "SELECT COUNT(*) FROM works WHERE subtitle IS NOT NULL "
         "AND BINARY subtitle = BINARY LOWER(subtitle) AND subtitle <> '' AND CHAR_LENGTH(subtitle) > 5"),
        ("publications placeholders",
         "SELECT COUNT(*) FROM publications WHERE "
         "LOWER(TRIM(COALESCE(volume,''))) IN ('none','null','n/a','na') OR "
         "LOWER(TRIM(COALESCE(issue,''))) IN ('none','null','n/a','na') OR "
         "LOWER(TRIM(COALESCE(pages,''))) IN ('none','null','n/a','na')"),
    ]
    log.info("quality_signals:")
    for label, q in profile_queries:
        try:
            cur.execute(q)
            log.info(f"  {label}: {cur.fetchone()[0]}")
        except mariadb.Error as e:
            log.warning(f"  {label}: ERRO ({e})")
    cur.close()


# ---------------------------------------------------------------------------
#  Title/subtitle split
# ---------------------------------------------------------------------------

def run_title_subtitle_phase(
    conn: mariadb.Connection, args: argparse.Namespace,
) -> ColumnStats:
    """Split works.title on the first ':' into title + subtitle.

    Only processes rows where subtitle IS NULL and title contains ':'.
    The colon itself is removed; both parts are trimmed.
    """
    log.info("=== Fase: title_subtitle ===")
    stats = ColumnStats()
    cur = conn.cursor()
    batch_size = args.batch_size
    flush_size = args.flush_size
    limit = args.limit_per_column
    dry_run = args.dry_run
    verbose_every = args.verbose_every

    last_id: Optional[int] = None
    updates: List[Tuple] = []

    while True:
        if limit > 0:
            remaining = limit - stats.scanned
            if remaining <= 0:
                break
            effective_batch = min(batch_size, remaining)
        else:
            effective_batch = batch_size

        if last_id is None:
            cur.execute(
                "SELECT id, title FROM works "
                "WHERE title LIKE '%:%' AND subtitle IS NULL "
                "ORDER BY id LIMIT ?",
                (effective_batch,),
            )
        else:
            cur.execute(
                "SELECT id, title FROM works "
                "WHERE title LIKE '%:%' AND subtitle IS NULL AND id > ? "
                "ORDER BY id LIMIT ?",
                (last_id, effective_batch),
            )
        rows = cur.fetchall()
        if not rows:
            break

        for row_id, title in rows:
            stats.scanned += 1
            colon_pos = title.find(":")
            if colon_pos < 0:
                continue
            new_title = title[:colon_pos].strip()
            new_subtitle = title[colon_pos + 1:].strip()
            if not new_title:
                stats.skipped_empty += 1
                continue
            if not new_subtitle:
                stats.skipped_empty += 1
                continue
            # Capitalize first letter of subtitle
            new_subtitle = new_subtitle[0].upper() + new_subtitle[1:]
            stats.changed += 1
            updates.append((new_title, new_subtitle, row_id))
            if len(updates) >= flush_size:
                _flush_title_subtitle(conn, updates, dry_run, stats)
                updates.clear()

        last_id = rows[-1][0]
        if verbose_every > 0 and stats.scanned % verbose_every == 0:
            log.info(
                f"  works.title_subtitle: scanned={stats.scanned} "
                f"changed={stats.changed} updated={stats.updated} "
                f"skip_empty={stats.skipped_empty}"
            )

    cur.close()
    if updates:
        _flush_title_subtitle(conn, updates, dry_run, stats)

    log.info(
        f"[title_subtitle] works: scanned={stats.scanned} "
        f"changed={stats.changed} updated={stats.updated} "
        f"skip_empty={stats.skipped_empty} errors={stats.errors}"
    )
    return stats


def _flush_title_subtitle(
    conn: mariadb.Connection,
    updates: List[Tuple],
    dry_run: bool,
    stats: ColumnStats,
) -> None:
    if not updates or dry_run:
        stats.updated += len(updates)
        return

    sql = "UPDATE `works` SET `title` = ?, `subtitle` = ? WHERE `id` = ?"
    cur = conn.cursor()
    try:
        cur.executemany(sql, updates)
        conn.commit()
        stats.updated += len(updates)
    except mariadb.Error:
        conn.rollback()
        for params in updates:
            try:
                cur.execute("SAVEPOINT sp_ts")
                cur.execute(sql, params)
                cur.execute("RELEASE SAVEPOINT sp_ts")
                stats.updated += 1
            except mariadb.Error:
                cur.execute("ROLLBACK TO SAVEPOINT sp_ts")
                cur.execute("RELEASE SAVEPOINT sp_ts")
                stats.errors += 1
        conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
#  Capitalization fix
# ---------------------------------------------------------------------------

def _capitalize_first(value: Optional[str]) -> Optional[str]:
    """Capitalize the first letter of a string, leaving the rest unchanged."""
    if not value or not isinstance(value, str):
        return value
    if not value[0].islower():
        return value
    return value[0].upper() + value[1:]


def run_capitalization_phase(
    conn: mariadb.Connection, args: argparse.Namespace,
) -> ColumnStats:
    """Fix ALL CAPS, all-lowercase, and lowercase-initial titles/subtitles."""
    log.info("=== Fase: capitalization ===")
    total = ColumnStats()

    for column in ("title", "subtitle"):
        cols = discover_target_columns(conn, {"works"}, {column})
        if not cols:
            continue
        t = cols[0]

        # Pass 1: ALL CAPS / all lowercase → titlecase
        stats = process_column(
            conn, t["table"], t["column"], t["pk_cols"], fix_capitalization,
            args.batch_size, args.flush_size,
            args.limit_per_column, args.dry_run, args.verbose_every,
        )
        total.add(stats)
        if stats.changed or stats.errors:
            log.info(
                f"[capitalization] works.{column}: scanned={stats.scanned} "
                f"changed={stats.changed} updated={stats.updated} "
                f"skip_integrity={stats.skipped_integrity} "
                f"skip_empty={stats.skipped_empty} skip_shrink={stats.skipped_shrink} "
                f"errors={stats.errors}"
            )

        # Pass 2: first letter lowercase → capitalize
        stats2 = process_column(
            conn, t["table"], t["column"], t["pk_cols"], _capitalize_first,
            args.batch_size, args.flush_size,
            args.limit_per_column, args.dry_run, args.verbose_every,
            shrink_guard=False,
        )
        total.add(stats2)
        if stats2.changed or stats2.errors:
            log.info(
                f"[capitalize_first] works.{column}: scanned={stats2.scanned} "
                f"changed={stats2.changed} updated={stats2.updated} "
                f"errors={stats2.errors}"
            )
    return total


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

ALL_PHASES = ["text", "sentinel", "identifiers", "title_subtitle", "capitalization"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified database text cleanup and normalization.",
    )
    p.add_argument("--config", type=str, help="Caminho para config.ini")
    p.add_argument("--mode", choices=["profile", "run", "both"], default="both")
    p.add_argument(
        "--phase", action="append", choices=ALL_PHASES,
        help="Phase(s) to execute (default: all).",
    )
    p.add_argument("--table", action="append", help="Filtrar tabela(s)")
    p.add_argument("--column", action="append", help="Filtrar coluna(s)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--flush-size", type=int, default=1000)
    p.add_argument("--limit-per-column", type=int, default=0, help="0 = sem limite")
    p.add_argument("--verbose-every", type=int, default=50000, help="Progresso a cada N linhas (0 desabilita)")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()

    include_tables = set(args.table) if args.table else None
    include_columns = set(args.column) if args.column else None
    phases = args.phase or ALL_PHASES

    conn = get_connection(args.config)
    cur = conn.cursor()
    cur.execute(
        "SET SESSION net_read_timeout=3600, net_write_timeout=3600, "
        "tmp_table_size=536870912, max_heap_table_size=536870912"
    )
    cur.close()
    try:
        if args.mode in {"profile", "both"}:
            print_profile(conn)

        grand = ColumnStats()
        if args.mode in {"run", "both"}:
            if "text" in phases:
                grand.add(run_text_phase(conn, args, include_tables, include_columns))
            if "sentinel" in phases:
                grand.add(run_sentinel_phase(conn, args, include_tables, include_columns))
            if "identifiers" in phases:
                grand.add(run_identifier_phase(conn, args))
            if "title_subtitle" in phases:
                grand.add(run_title_subtitle_phase(conn, args))
            if "capitalization" in phases:
                grand.add(run_capitalization_phase(conn, args))

            log.info(
                f"=== Summary: scanned={grand.scanned} changed={grand.changed} "
                f"updated={grand.updated} skip_integrity={grand.skipped_integrity} "
                f"skip_empty={grand.skipped_empty} skip_shrink={grand.skipped_shrink} "
                f"errors={grand.errors} elapsed={time.time() - started:.1f}s ==="
            )
            if grand.skipped_shrink > 0:
                log.warning(
                    "WARNING: %d values rejected by shrink guard — "
                    "check if any transformation is truncating text "
                    "inappropriately.", grand.skipped_shrink,
                )
    finally:
        try:
            conn.close()
        except mariadb.Error:
            pass


if __name__ == "__main__":
    main()
