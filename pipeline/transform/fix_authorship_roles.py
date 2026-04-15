"""
Extraordinary repair script for Crossref-derived authorship roles.

It uses the local Crossref JSON cache as source of truth for supported roles
(`AUTHOR`, `EDITOR`, `TRANSLATOR`, `REVIEWER`) and reconciles the `authorships`
table conservatively:

- insert missing supported roles
- update position/affiliation/corresponding flags when the payload is better
- reclassify a single existing row when the DB and payload both imply exactly
  one role for that person on that work
- never delete extra DB rows silently; instead report them as conflicts
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import subprocess
from collections import Counter
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import mariadb

from pipeline.common import get_connection
from pipeline.extract.io_utils import sanitize_doi_for_filename
from pipeline.load.constants import ORG_TYPE_INSTITUTE
from pipeline.load.db import ensure_connection, safe_rollback
from pipeline.load.entities import get_or_create_organization, get_or_create_person
from pipeline.load.io_utils import build_cache
from pipeline.load.normalize import (
    _normalize_person_name_for_matching,
    _prepare_person_name,
    normalize_doi,
)
from pipeline.load.works.shared import (
    extract_crossref_contributors,
    reconcile_resolved_authorships,
)


log = logging.getLogger(__name__)

ROLE_SEARCH_PATTERN = r'"(editor|translator|reviewer)"\s*:'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extraordinary repair of Crossref-derived authorship roles."
    )
    parser.add_argument(
        "--mode",
        choices=("audit", "apply"),
        default="audit",
        help="`audit` only reports proposed changes; `apply` writes them.",
    )
    parser.add_argument(
        "--source-dir",
        default="works/crossref",
        help="Root directory of Crossref JSON cache.",
    )
    parser.add_argument(
        "--report-dir",
        default="backup",
        help="Directory for JSONL/summary reports and backups.",
    )
    parser.add_argument(
        "--config",
        default="config.ini",
        help="Path to config.ini.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of candidate cache files to process.",
    )
    parser.add_argument(
        "--doi",
        action="append",
        default=[],
        help="Process a specific DOI. Can be repeated.",
    )
    parser.add_argument(
        "--work-id",
        type=int,
        action="append",
        default=[],
        help="Process all Crossref DOIs linked to a specific work_id. Can be repeated.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")


def _extract_orcid(author_data: Dict[str, Any]) -> Optional[str]:
    orcid_raw = author_data.get("orcid") or author_data.get("ORCID")
    if not orcid_raw:
        return None
    match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", str(orcid_raw))
    return match.group(1).upper() if match else None


def find_person_id(cursor: mariadb.Cursor, author_data: Dict[str, Any], cache: Dict[str, Dict]) -> Optional[int]:
    person_name = _prepare_person_name(author_data)
    preferred_name = person_name["preferred_name"]
    if not preferred_name:
        return None

    orcid = _extract_orcid(author_data)
    match_key = _normalize_person_name_for_matching(preferred_name)
    if not match_key:
        return None

    if orcid and orcid in cache.get("persons_by_orcid", {}):
        return cache["persons_by_orcid"][orcid]
    if match_key in cache.get("persons_by_norm_name", {}):
        return cache["persons_by_norm_name"][match_key]

    if orcid:
        cursor.execute("SELECT id FROM persons WHERE orcid = ?", (orcid,))
        if result := cursor.fetchone():
            person_id = result[0]
            cache.setdefault("persons_by_orcid", {})[orcid] = person_id
            cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
            return person_id

    cursor.execute("SELECT id FROM persons WHERE normalized_name = ?", (match_key,))
    if result := cursor.fetchone():
        person_id = result[0]
        if orcid:
            cache.setdefault("persons_by_orcid", {})[orcid] = person_id
        cache.setdefault("persons_by_norm_name", {})[match_key] = person_id
        return person_id
    return None


def find_organization_id(cursor: mariadb.Cursor, name: Optional[str], cache: Dict[str, Dict]) -> Optional[int]:
    clean_name = name.strip()[:512] if name and isinstance(name, str) else None
    if not clean_name:
        return None
    norm_name_cache_key = clean_name.lower()
    cache_key = (norm_name_cache_key, ORG_TYPE_INSTITUTE)
    if cache_key in cache.get("organizations_by_norm_name", {}):
        return cache["organizations_by_norm_name"][cache_key]

    cursor.execute(
        "SELECT id FROM organizations WHERE standardized_name = ? AND type = ?",
        (norm_name_cache_key, ORG_TYPE_INSTITUTE),
    )
    if result := cursor.fetchone():
        org_id = result[0]
        cache.setdefault("organizations_by_norm_name", {})[cache_key] = org_id
        return org_id
    return None


def resolve_contributors(
    cursor: mariadb.Cursor,
    contributors: Sequence[Dict[str, Any]],
    cache: Dict[str, Dict],
    *,
    create_missing: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    resolved: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []

    for contributor in contributors:
        person_data = contributor.get("person_data") or {}
        if create_missing:
            person_id = get_or_create_person(cursor, person_data, cache)
            affiliation_id = get_or_create_organization(
                cursor,
                contributor.get("affiliation_name"),
                ORG_TYPE_INSTITUTE,
                cache,
            )
        else:
            person_id = find_person_id(cursor, person_data, cache)
            affiliation_id = find_organization_id(cursor, contributor.get("affiliation_name"), cache)

        if not person_id:
            unresolved.append(
                {
                    "role": contributor.get("role"),
                    "display_name": contributor.get("display_name"),
                    "reason": "person_not_found" if not create_missing else "person_not_created",
                }
            )
            continue

        resolved.append(
            {
                "person_id": person_id,
                "role": contributor.get("role"),
                "position": contributor.get("position"),
                "affiliation_id": affiliation_id,
                "is_corresponding": contributor.get("is_corresponding"),
                "display_name": contributor.get("display_name"),
            }
        )

    return resolved, unresolved


def fetch_work_lookup(cursor: mariadb.Cursor, doi: str) -> Optional[Tuple[int, int, Optional[str]]]:
    cursor.execute(
        "SELECT work_id, id, source FROM publications WHERE doi = ? LIMIT 1",
        (doi,),
    )
    if result := cursor.fetchone():
        return result[0], result[1], result[2]
    return None


def fetch_authorship_snapshot(cursor: mariadb.Cursor, work_id: int) -> List[Dict[str, Any]]:
    cursor.execute(
        """
        SELECT a.person_id, p.preferred_name, a.role, a.position, a.affiliation_id, COALESCE(a.is_corresponding, 0)
        FROM authorships a
        JOIN persons p ON p.id = a.person_id
        WHERE a.work_id = ?
        ORDER BY a.role, a.position, a.person_id
        """,
        (work_id,),
    )
    return [
        {
            "person_id": person_id,
            "preferred_name": preferred_name,
            "role": role,
            "position": position,
            "affiliation_id": affiliation_id,
            "is_corresponding": int(is_corresponding or 0),
        }
        for person_id, preferred_name, role, position, affiliation_id, is_corresponding in cursor.fetchall()
    ]


def compute_extra_current_rows(
    snapshot: Sequence[Dict[str, Any]],
    resolved: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    desired_keys = {
        (item["person_id"], item["role"])
        for item in resolved
        if item.get("person_id") and item.get("role")
    }
    return [
        row for row in snapshot
        if (row["person_id"], row["role"]) not in desired_keys
    ]


def load_crossref_message(filepath: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        with filepath.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        log.error("FILE %s error=json detail=%s", filepath.name, exc)
        return None, None

    message = data.get("message") or data.get("crossref_response", {}).get("message", {})
    if not isinstance(message, dict) or not message:
        return None, None

    doi = normalize_doi(message.get("DOI"))
    if not doi:
        doi = normalize_doi(filepath.stem.replace("_", "/", 1))
    return message, doi


def discover_role_candidate_files(source_dir: Path) -> List[Path]:
    try:
        result = subprocess.run(
            [
                "rg",
                "-l",
                "--glob",
                "*.json",
                ROLE_SEARCH_PATTERN,
                str(source_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        result = None

    if result and result.returncode in (0, 1):
        paths = [Path(line) for line in result.stdout.splitlines() if line.strip()]
        return sorted(set(paths))

    log.warning("rg unavailable or failed; falling back to Python file scan")
    candidates: List[Path] = []
    markers = ('"editor"', '"translator"', '"reviewer"')
    for filepath in source_dir.rglob("*.json"):
        try:
            with filepath.open("r", encoding="utf-8") as handle:
                text = handle.read()
        except OSError:
            continue
        if any(marker in text for marker in markers):
            candidates.append(filepath)
    return sorted(set(candidates))


def resolve_targeted_paths(
    source_dir: Path,
    dois: Sequence[str],
    work_ids: Sequence[int],
    cursor: mariadb.Cursor,
) -> List[Path]:
    paths: Set[Path] = set()
    for raw_doi in dois:
        doi = normalize_doi(raw_doi)
        if not doi:
            continue
        safe = sanitize_doi_for_filename(doi)
        paths.update(source_dir.rglob(f"{safe}.json"))

    for work_id in work_ids:
        cursor.execute(
            "SELECT doi FROM publications WHERE work_id = ? AND doi IS NOT NULL",
            (work_id,),
        )
        for row in cursor.fetchall():
            doi = normalize_doi(row[0])
            if not doi:
                continue
            safe = sanitize_doi_for_filename(doi)
            paths.update(source_dir.rglob(f"{safe}.json"))
    return sorted(paths)


def ensure_report_dir(report_dir: Path) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)


def write_jsonl(handle, payload: Dict[str, Any]) -> None:
    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def backup_work_snapshot(
    cursor: mariadb.Cursor,
    backup_handle,
    work_id: int,
    doi: str,
    filepath: Path,
) -> None:
    snapshot = fetch_authorship_snapshot(cursor, work_id)
    write_jsonl(
        backup_handle,
        {
            "work_id": work_id,
            "doi": doi,
            "cache_file": str(filepath),
            "authorships": snapshot,
        },
    )


def process_candidate(
    cursor: mariadb.Cursor,
    filepath: Path,
    cache: Dict[str, Dict],
    *,
    mode: str,
    backup_handle,
    backed_up_work_ids: Set[int],
) -> Optional[Dict[str, Any]]:
    message, doi = load_crossref_message(filepath)
    if not message or not doi:
        return None

    contributors, unmapped_roles = extract_crossref_contributors(message)
    if not contributors and not unmapped_roles:
        return None

    lookup = fetch_work_lookup(cursor, doi)
    if not lookup:
        return {
            "doi": doi,
            "cache_file": str(filepath),
            "status": "missing_db_record",
            "supported_roles": dict(Counter(item["role"] for item in contributors)),
            "unmapped_roles": unmapped_roles,
        }

    work_id, publication_id, source = lookup
    snapshot = fetch_authorship_snapshot(cursor, work_id)
    resolved, unresolved = resolve_contributors(
        cursor,
        contributors,
        cache,
        create_missing=False,
    )
    plan = reconcile_resolved_authorships(cursor, work_id, resolved, apply_changes=False)
    effective_resolved = resolved

    applied = None
    if mode == "apply" and (plan["changed"] or unresolved):
        if work_id not in backed_up_work_ids and backup_handle is not None:
            backup_work_snapshot(cursor, backup_handle, work_id, doi, filepath)
            backed_up_work_ids.add(work_id)
        resolved_apply, unresolved_apply = resolve_contributors(
            cursor,
            contributors,
            cache,
            create_missing=True,
        )
        applied = reconcile_resolved_authorships(cursor, work_id, resolved_apply, apply_changes=True)
        effective_resolved = resolved_apply
        unresolved = unresolved_apply

    extra_current_rows = compute_extra_current_rows(snapshot, effective_resolved)

    status = "unchanged"
    effective = applied or plan
    if effective["changed"]:
        status = "changed"
    if extra_current_rows or unresolved or unmapped_roles:
        if status == "unchanged":
            status = "conflict"

    return {
        "doi": doi,
        "work_id": work_id,
        "publication_id": publication_id,
        "publication_source": source,
        "cache_file": str(filepath),
        "status": status,
        "supported_roles": dict(Counter(item["role"] for item in contributors)),
        "unmapped_roles": unmapped_roles,
        "unresolved_contributors": unresolved,
        "extra_current_rows": extra_current_rows,
        "planned": {
            "inserted": plan["inserted"],
            "updated": plan["updated"],
            "reclassified": plan["reclassified"],
            "duplicates_skipped": plan["duplicates_skipped"],
        },
        "applied": None if applied is None else {
            "inserted": applied["inserted"],
            "updated": applied["updated"],
            "reclassified": applied["reclassified"],
            "duplicates_skipped": applied["duplicates_skipped"],
        },
        "actions": effective["actions"],
    }


def build_summary(
    args: argparse.Namespace,
    candidates: Sequence[Path],
    counters: Counter,
    role_counters: Counter,
    unmapped_role_counters: Counter,
    report_paths: Dict[str, str],
) -> Dict[str, Any]:
    return {
        "mode": args.mode,
        "source_dir": args.source_dir,
        "report_dir": args.report_dir,
        "processed_at_utc": datetime.now(UTC).isoformat(),
        "candidate_files": len(candidates),
        "limit": args.limit,
        "target_dois": args.doi,
        "target_work_ids": args.work_id,
        "counts": dict(counters),
        "supported_role_counts": dict(role_counters),
        "unmapped_role_counts": dict(unmapped_role_counters),
        "report_paths": report_paths,
    }


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    source_dir = Path(args.source_dir)
    report_dir = Path(args.report_dir)
    ensure_report_dir(report_dir)

    if not source_dir.is_dir():
        log.critical("Source dir '%s' not found.", source_dir)
        return 1

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_prefix = report_dir / f"fix_authorship_roles_{args.mode}_{timestamp}"
    actions_path = report_prefix.with_suffix(".actions.jsonl")
    summary_path = report_prefix.with_suffix(".summary.json")
    backup_path = report_prefix.with_suffix(".backup.jsonl")

    conn: Optional[mariadb.Connection] = None
    actions_handle = None
    backup_handle = None
    try:
        conn = get_connection(
            args.config,
            read_timeout=300,
            write_timeout=300,
            retries=5,
            retry_base=1.0,
            retry_max=12.0,
        )
        cursor = conn.cursor()
        targeted_mode = bool(args.doi or args.work_id)
        if targeted_mode:
            candidates = resolve_targeted_paths(source_dir, args.doi, args.work_id, cursor)
        else:
            candidates = discover_role_candidate_files(source_dir)
        if args.limit:
            candidates = candidates[:args.limit]

        if not candidates:
            log.warning("No candidate Crossref cache files found.")
            return 0

        actions_handle = actions_path.open("w", encoding="utf-8")
        if args.mode == "apply":
            backup_handle = backup_path.open("w", encoding="utf-8")

        counters: Counter = Counter()
        role_counters: Counter = Counter()
        unmapped_role_counters: Counter = Counter()
        backed_up_work_ids: Set[int] = set()
        cache = build_cache()

        total = len(candidates)
        for index, filepath in enumerate(candidates, 1):
            conn = ensure_connection(
                conn,
                config_path=args.config,
                read_timeout=300,
                write_timeout=300,
                retries=5,
                retry_base=1.0,
                retry_max=12.0,
            )
            cursor = conn.cursor()
            try:
                record = process_candidate(
                    cursor,
                    filepath,
                    cache,
                    mode=args.mode,
                    backup_handle=backup_handle,
                    backed_up_work_ids=backed_up_work_ids,
                )
                if record is None:
                    counters["skipped_no_supported_roles"] += 1
                    continue

                write_jsonl(actions_handle, record)
                counters["processed_records"] += 1
                counters[f"status_{record['status']}"] += 1
                for role, count in (record.get("supported_roles") or {}).items():
                    role_counters[role] += count
                for role, count in (record.get("unmapped_roles") or {}).items():
                    unmapped_role_counters[role] += count

                planned = record.get("planned") or {}
                applied = record.get("applied") or {}
                for field in ("inserted", "updated", "reclassified"):
                    counters[f"planned_{field}"] += int(planned.get(field) or 0)
                    counters[f"applied_{field}"] += int(applied.get(field) or 0)
                counters["unresolved_contributors"] += len(record.get("unresolved_contributors") or [])
                counters["extra_current_rows"] += len(record.get("extra_current_rows") or [])
                if record["status"] == "missing_db_record":
                    counters["missing_db_record"] += 1

                if args.mode == "apply" and record.get("applied"):
                    conn.commit()
                elif args.mode == "apply":
                    conn.rollback()

                if index % 100 == 0 or index == total:
                    log.info("Progress: %d/%d candidate files processed", index, total)
            except Exception as exc:
                if conn is not None:
                    safe_rollback(conn, f"fix_authorship_roles:{filepath.name}")
                log.error("FILE %s error=unexpected detail=%s", filepath, exc)
                counters["errors"] += 1

        if backup_handle is not None:
            backup_handle.flush()
        actions_handle.flush()

        report_paths = {
            "actions": str(actions_path),
            "summary": str(summary_path),
        }
        if args.mode == "apply":
            report_paths["backup"] = str(backup_path)

        summary = build_summary(
            args,
            candidates,
            counters,
            role_counters,
            unmapped_role_counters,
            report_paths,
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Summary written to %s", summary_path)
        return 0
    finally:
        if actions_handle is not None:
            actions_handle.close()
        if backup_handle is not None:
            backup_handle.close()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
