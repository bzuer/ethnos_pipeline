"""Shared helpers for work loaders (Crossref and OpenAlex).

DOI skip/mark functions used identically by both sources.
Fast-mode helpers: filename-based DOI extraction and bulk existence check.
Also contains shared authorship-role parsing/reconciliation helpers used by
loaders and the extraordinary repair script.
"""

import os
import logging
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import mariadb

from pipeline.load.constants import STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE


def should_skip_existing_doi(
    cursor: mariadb.Cursor,
    doi: Optional[str],
    mode: str,
    doi_presence_cache: Dict[str, bool],
) -> bool:
    """Return True if the file should be skipped at the runner level.

    - new/fast: skip when DOI already exists.
    - update:   skip when DOI does not exist (defer no-DOI records to the loader
                so it can still try an openalex_id fallback).
    """
    if mode == "update":
        if not doi:
            return False
        if doi in doi_presence_cache:
            return not doi_presence_cache[doi]
        cursor.execute("SELECT 1 FROM publications WHERE doi = ? LIMIT 1", (doi,))
        exists = cursor.fetchone() is not None
        doi_presence_cache[doi] = exists
        return not exists
    if mode not in ("new", "fast") or not doi:
        return False
    if doi in doi_presence_cache:
        return doi_presence_cache[doi]
    cursor.execute("SELECT 1 FROM publications WHERE doi = ? LIMIT 1", (doi,))
    exists = cursor.fetchone() is not None
    doi_presence_cache[doi] = exists
    return exists


def mark_doi_processed(
    doi: Optional[str],
    mode: str,
    status: str,
    doi_presence_cache: Dict[str, bool],
) -> None:
    """Mark a DOI as processed in the presence cache after successful load."""
    if mode in ("new", "fast") and doi and status in (STATUS_INSERTED, STATUS_UPDATED, STATUS_NO_CHANGE):
        doi_presence_cache[doi] = True


# ---------------------------------------------------------------------------
# Fast-mode helpers
# ---------------------------------------------------------------------------

def doi_from_filename(filepath: str) -> Optional[str]:
    """Extract DOI from a sanitized JSON filename (best-effort reverse of sanitize_doi_for_filename).

    Filename convention: ``10.XXXX_suffix.json`` where ``_`` replaced the
    single ``/`` separating registrant from suffix.  We restore the first
    ``_`` after the ``10.`` prefix back to ``/``.
    """
    stem = os.path.basename(filepath).removesuffix(".json")
    if not stem.startswith("10."):
        return None
    doi = stem.replace("_", "/", 1)
    return doi if "/" in doi else None


def bulk_filter_new_files(
    cursor: mariadb.Cursor,
    json_files: List[str],
    doi_presence_cache: Dict[str, bool],
    batch_size: int = 1000,
) -> Tuple[List[str], int]:
    """Return only files whose DOI is not yet in publications (fast mode).

    1. Derive DOI from each filename (no JSON parsing).
    2. Bulk-check against DB in batches.
    3. Populate doi_presence_cache for downstream mark_doi_processed.

    Returns (new_files, skipped_count).
    """
    file_doi_pairs: List[Tuple[str, str]] = []
    unparseable: List[str] = []

    for fp in json_files:
        doi = doi_from_filename(fp)
        if doi:
            file_doi_pairs.append((fp, doi))
        else:
            unparseable.append(fp)

    existing_dois: set = set()
    all_dois = [d for _, d in file_doi_pairs]

    for i in range(0, len(all_dois), batch_size):
        batch = all_dois[i : i + batch_size]
        placeholders = ",".join(["?"] * len(batch))
        cursor.execute(
            f"SELECT doi FROM publications WHERE doi IN ({placeholders})",
            tuple(batch),
        )
        existing_dois.update(row[0] for row in cursor)

    for doi in existing_dois:
        doi_presence_cache[doi] = True
    for _, doi in file_doi_pairs:
        if doi not in existing_dois:
            doi_presence_cache[doi] = False

    new_files = [fp for fp, doi in file_doi_pairs if doi not in existing_dois]
    # Include unparseable files so they get processed normally (fallback)
    new_files.extend(unparseable)
    skipped = len(json_files) - len(new_files)

    if skipped:
        logging.info(f"FAST skip={skipped} new={len(new_files)} (unparseable={len(unparseable)})")

    return new_files, skipped


def bulk_filter_update_files(
    cursor: mariadb.Cursor,
    json_files: List[str],
    doi_presence_cache: Dict[str, bool],
    batch_size: int = 1000,
) -> Tuple[List[str], int]:
    """Return only files whose DOI is already in publications (update mode).

    Mirrors bulk_filter_new_files but keeps existing DOIs instead of new ones.
    Files whose DOI can't be derived from the filename fall through so the
    loader can still match them via openalex_id.
    """
    file_doi_pairs: List[Tuple[str, str]] = []
    unparseable: List[str] = []

    for fp in json_files:
        doi = doi_from_filename(fp)
        if doi:
            file_doi_pairs.append((fp, doi))
        else:
            unparseable.append(fp)

    existing_dois: set = set()
    all_dois = [d for _, d in file_doi_pairs]

    for i in range(0, len(all_dois), batch_size):
        batch = all_dois[i : i + batch_size]
        placeholders = ",".join(["?"] * len(batch))
        cursor.execute(
            f"SELECT doi FROM publications WHERE doi IN ({placeholders})",
            tuple(batch),
        )
        existing_dois.update(row[0] for row in cursor)

    for doi in existing_dois:
        doi_presence_cache[doi] = True
    for _, doi in file_doi_pairs:
        if doi not in existing_dois:
            doi_presence_cache[doi] = False

    keep_files = [fp for fp, doi in file_doi_pairs if doi in existing_dois]
    keep_files.extend(unparseable)
    skipped = len(json_files) - len(keep_files)

    if skipped:
        logging.info(f"UPDATE skip={skipped} keep={len(keep_files)} (unparseable={len(unparseable)})")

    return keep_files, skipped


# ---------------------------------------------------------------------------
# Authorship-role helpers
# ---------------------------------------------------------------------------

SUPPORTED_AUTHORSHIP_ROLES = ("AUTHOR", "EDITOR", "TRANSLATOR", "REVIEWER")

AUTHORSHIP_ROLE_PRIORITY = {
    "AUTHOR": 1,
    "EDITOR": 2,
    "TRANSLATOR": 3,
    "REVIEWER": 4,
}

_AUTHORSHIP_ROLE_ALIASES = {
    "author": "AUTHOR",
    "editor": "EDITOR",
    "translator": "TRANSLATOR",
    "reviewer": "REVIEWER",
}

_KNOWN_UNMAPPED_CROSSREF_ROLE_KEYS = (
    "chair",
    "chair-translator",
    "reviewed-author",
)


def normalize_authorship_role(raw_role: Optional[str]) -> Optional[str]:
    """Normalize a raw contributor role into the supported enum values."""
    if not raw_role:
        return None
    role = str(raw_role).strip().lower()
    normalized = _AUTHORSHIP_ROLE_ALIASES.get(role)
    if normalized in SUPPORTED_AUTHORSHIP_ROLES:
        return normalized
    return None


def extract_crossref_contributors(message: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Extract supported Crossref contributor lists plus unmapped role counters."""
    contributors: List[Dict[str, Any]] = []
    unmapped_roles: Counter = Counter()

    if not isinstance(message, dict):
        return contributors, {}

    for field, role in _AUTHORSHIP_ROLE_ALIASES.items():
        items = message.get(field) or []
        if not isinstance(items, list):
            continue
        for position, person_data in enumerate(items, 1):
            if not isinstance(person_data, dict):
                continue
            affiliation_name = None
            aff_list = person_data.get("affiliation") or []
            if isinstance(aff_list, list) and aff_list and isinstance(aff_list[0], dict):
                affiliation_name = aff_list[0].get("name")
            contributors.append(
                {
                    "role": role,
                    "position": position,
                    "person_data": person_data,
                    "display_name": " ".join(
                        part.strip()
                        for part in (person_data.get("given"), person_data.get("family"))
                        if isinstance(part, str) and part.strip()
                    ) or None,
                    "affiliation_name": affiliation_name,
                    "affiliation_openalex_id": None,
                    "affiliation_ror_id": None,
                    "affiliation_country": None,
                    # Crossref payloads used here do not reliably expose this flag.
                    "is_corresponding": None,
                }
            )

    for key in _KNOWN_UNMAPPED_CROSSREF_ROLE_KEYS:
        items = message.get(key) or []
        if isinstance(items, list) and items:
            unmapped_roles[key] += len(items)

    return contributors, dict(unmapped_roles)


def reconcile_resolved_authorships(
    cursor: mariadb.Cursor,
    work_id: int,
    resolved_contributors: List[Dict[str, Any]],
    *,
    apply_changes: bool = True,
    allow_reclassification: bool = True,
) -> Dict[str, Any]:
    """Reconcile resolved contributor rows against `authorships` conservatively.

    Expected input rows already have `person_id`, `role`, `position`,
    `affiliation_id`, and optional `is_corresponding`.

    Behavior:
    - insert missing rows
    - update position/affiliation/is_corresponding on exact matches
    - reclassify a single existing row when both the current and desired state
      have exactly one role for that person on the work
    - never delete extra DB rows that are absent from the payload
    """
    result: Dict[str, Any] = {
        "inserted": 0,
        "updated": 0,
        "reclassified": 0,
        "duplicates_skipped": 0,
        "ignored": 0,
        "actions": [],
        "changed": False,
    }
    if not resolved_contributors:
        return result

    cursor.execute(
        """
        SELECT person_id, role, position, affiliation_id, COALESCE(is_corresponding, 0)
        FROM authorships
        WHERE work_id = ?
        """,
        (work_id,),
    )
    current_rows: Dict[Tuple[int, str], Dict[str, Any]] = {}
    current_roles_by_person: Dict[int, set] = defaultdict(set)
    for person_id, role, position, affiliation_id, is_corresponding in cursor.fetchall():
        normalized_role = normalize_authorship_role(role) or str(role)
        key = (person_id, normalized_role)
        current_rows[key] = {
            "position": position,
            "affiliation_id": affiliation_id,
            "is_corresponding": int(is_corresponding or 0),
        }
        current_roles_by_person[person_id].add(normalized_role)

    desired_roles_by_person: Dict[int, set] = defaultdict(set)
    for contributor in resolved_contributors:
        person_id = contributor.get("person_id")
        role = normalize_authorship_role(contributor.get("role"))
        if person_id and role:
            desired_roles_by_person[person_id].add(role)

    processed_keys = set()
    for contributor in resolved_contributors:
        person_id = contributor.get("person_id")
        role = normalize_authorship_role(contributor.get("role"))
        if not person_id or not role:
            result["ignored"] += 1
            continue

        key = (person_id, role)
        if key in processed_keys:
            result["duplicates_skipped"] += 1
            continue
        processed_keys.add(key)

        position = int(contributor.get("position") or 0) or 1
        affiliation_id = contributor.get("affiliation_id")
        is_corresponding = contributor.get("is_corresponding")
        display_name = contributor.get("display_name")

        if key in current_rows:
            current = current_rows[key]
            updates = []
            params: List[Any] = []
            new_state = dict(current)

            if position != current["position"]:
                updates.append("position = ?")
                params.append(position)
                new_state["position"] = position
            if affiliation_id is not None and affiliation_id != current["affiliation_id"]:
                updates.append("affiliation_id = ?")
                params.append(affiliation_id)
                new_state["affiliation_id"] = affiliation_id
            if is_corresponding is not None:
                bool_is_corresponding = int(bool(is_corresponding))
                if bool_is_corresponding != current["is_corresponding"]:
                    updates.append("is_corresponding = ?")
                    params.append(bool_is_corresponding)
                    new_state["is_corresponding"] = bool_is_corresponding

            if updates:
                params.extend((work_id, person_id, role))
                if apply_changes:
                    cursor.execute(
                        f"UPDATE authorships SET {', '.join(updates)} WHERE work_id = ? AND person_id = ? AND role = ?",
                        tuple(params),
                    )
                current_rows[key] = new_state
                result["updated"] += 1
                result["actions"].append(
                    {
                        "action": "update",
                        "work_id": work_id,
                        "person_id": person_id,
                        "role": role,
                        "display_name": display_name,
                    }
                )
            continue

        current_roles = current_roles_by_person.get(person_id, set())
        desired_roles = desired_roles_by_person.get(person_id, set())
        if allow_reclassification and len(current_roles) == 1 and len(desired_roles) == 1:
            previous_role = next(iter(current_roles))
            previous_key = (person_id, previous_role)
            previous_state = current_rows.get(previous_key)
            if previous_state and previous_role != role:
                updates = ["role = ?", "position = ?"]
                params = [role, position]
                new_state = dict(previous_state)
                new_state["position"] = position
                if affiliation_id is not None:
                    updates.append("affiliation_id = ?")
                    params.append(affiliation_id)
                    new_state["affiliation_id"] = affiliation_id
                if is_corresponding is not None:
                    updates.append("is_corresponding = ?")
                    bool_is_corresponding = int(bool(is_corresponding))
                    params.append(bool_is_corresponding)
                    new_state["is_corresponding"] = bool_is_corresponding
                params.extend((work_id, person_id, previous_role))
                if apply_changes:
                    cursor.execute(
                        f"UPDATE authorships SET {', '.join(updates)} WHERE work_id = ? AND person_id = ? AND role = ?",
                        tuple(params),
                    )
                current_rows.pop(previous_key, None)
                current_rows[key] = new_state
                current_roles_by_person[person_id].discard(previous_role)
                current_roles_by_person[person_id].add(role)
                result["reclassified"] += 1
                result["actions"].append(
                    {
                        "action": "reclassify",
                        "work_id": work_id,
                        "person_id": person_id,
                        "from_role": previous_role,
                        "to_role": role,
                        "display_name": display_name,
                    }
                )
                continue

        insert_is_corresponding = int(bool(is_corresponding)) if is_corresponding is not None else 0
        inserted = True
        if apply_changes:
            cursor.execute(
                """
                INSERT IGNORE INTO authorships
                (work_id, person_id, affiliation_id, role, position, is_corresponding)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (work_id, person_id, affiliation_id, role, position, insert_is_corresponding),
            )
            inserted = cursor.rowcount > 0
        if inserted:
            current_rows[key] = {
                "position": position,
                "affiliation_id": affiliation_id,
                "is_corresponding": insert_is_corresponding,
            }
            current_roles_by_person[person_id].add(role)
            result["inserted"] += 1
            result["actions"].append(
                {
                    "action": "insert",
                    "work_id": work_id,
                    "person_id": person_id,
                    "role": role,
                    "display_name": display_name,
                }
            )

    result["changed"] = any(result[key] > 0 for key in ("inserted", "updated", "reclassified"))
    return result
