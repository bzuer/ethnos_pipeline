#!/usr/bin/env python3
"""
Definitive metrics calculation and sphinx summary orchestrator.

Recalculates all denormalized counters, h-indexes, venue stats, and
rebuilds sphinx search summary tables.

Modes:
  --full     Complete recalculation of everything.
  --partial  Only update entities with missing/stale data (default).

Entity flags (additive):
  --works          Reference/citation count sync from work_references.
  --persons        Person stats + h_index.
  --organizations  Organization stats.
  --venues         Venue stats + h_index + yearly_stats + impact factors + ranking.
  --sphinx         Rebuild all sphinx/search summary tables.

No entity flag = all entities.

Usage:
  python metrics.py                                # partial, all entities
  python metrics.py --full                         # full recalculation
  python metrics.py --works                        # partial works only
  python metrics.py --full --persons --venues      # full persons + venues
  python metrics.py --sphinx                       # rebuild sphinx summaries
  python metrics.py --dry-run                      # preview steps
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import List

import mariadb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from pipeline.common import read_db_config, _prepare_connection_params
except ModuleNotFoundError:
    from common import read_db_config, _prepare_connection_params

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _get_connection(config_path=None):
    """Like pipeline.common.get_connection but with longer timeouts for bulk ops."""
    db_config = read_db_config(config_path=config_path)
    params = _prepare_connection_params(db_config)
    conn = mariadb.connect(**params, connect_timeout=30,
                           read_timeout=3600, write_timeout=3600)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_uca1400_ai_ci'")
    cur.close()
    return conn


# ---------------------------------------------------------------------------
#  Data model
# ---------------------------------------------------------------------------

@dataclass
class Step:
    name: str
    description: str
    sql: str


# ===================================================================
#  WORKS — reference/citation count sync
# ===================================================================

WORKS_FULL: List[Step] = [

    Step(
        "w1a_sync_reference_count",
        "Sync reference_count from work_references (works with refs)",
        """UPDATE works w
JOIN (
    SELECT citing_work_id, COUNT(*) as cnt
    FROM work_references
    GROUP BY citing_work_id
) rc ON w.id = rc.citing_work_id
SET w.reference_count = rc.cnt
WHERE w.reference_count != rc.cnt""",
    ),

    Step(
        "w1b_zero_reference_count",
        "Zero reference_count for works with no references",
        """UPDATE works w
LEFT JOIN work_references wr ON wr.citing_work_id = w.id
SET w.reference_count = 0
WHERE wr.citing_work_id IS NULL AND w.reference_count != 0""",
    ),

    Step(
        "w2a_sync_citation_count",
        "Sync citation_count from work_references (works with citations)",
        """UPDATE works w
JOIN (
    SELECT cited_work_id, COUNT(*) as cnt
    FROM work_references
    WHERE cited_work_id IS NOT NULL
    GROUP BY cited_work_id
) cc ON w.id = cc.cited_work_id
SET w.citation_count = cc.cnt
WHERE w.citation_count != cc.cnt""",
    ),

    Step(
        "w2b_zero_citation_count",
        "Zero citation_count for works with no citations",
        """UPDATE works w
LEFT JOIN work_references wr ON wr.cited_work_id = w.id
SET w.citation_count = 0
WHERE wr.cited_work_id IS NULL AND w.citation_count != 0""",
    ),
]

# Partial mode uses the same SQL — the WHERE != filter already handles "only mismatched"
WORKS_PARTIAL = WORKS_FULL


# ===================================================================
#  PERSONS — stats + h_index
# ===================================================================

PERSONS_FULL: List[Step] = [

    Step(
        "p1_person_stats",
        "Recalculate total_works, total_citations, corresponding_author_count, years",
        """UPDATE persons p
JOIN (
    SELECT
        a.person_id,
        COUNT(DISTINCT a.work_id) AS total_works,
        COALESCE(SUM(w.citation_count), 0) AS total_citations,
        COALESCE(SUM(a.is_corresponding = 1), 0) AS corresponding_author_count,
        MIN(pub.year) AS first_publication_year,
        MAX(pub.year) AS latest_publication_year
    FROM authorships a
    JOIN works w ON a.work_id = w.id
    LEFT JOIN publications pub ON w.id = pub.work_id
    GROUP BY a.person_id
) calc ON p.id = calc.person_id
SET p.total_works = calc.total_works,
    p.total_citations = calc.total_citations,
    p.corresponding_author_count = calc.corresponding_author_count,
    p.first_publication_year = calc.first_publication_year,
    p.latest_publication_year = calc.latest_publication_year""",
    ),

    Step(
        "p2_person_h_index",
        "Calculate h_index for all persons",
        """UPDATE persons p
JOIN (
    SELECT
        person_id,
        COALESCE(MAX(CASE WHEN rn <= citation_count THEN rn ELSE 0 END), 0) AS h_index
    FROM (
        SELECT
            a.person_id,
            w.citation_count,
            ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY w.citation_count DESC) AS rn
        FROM authorships a
        JOIN works w ON a.work_id = w.id
    ) ranked
    GROUP BY person_id
) calc ON p.id = calc.person_id
SET p.h_index = calc.h_index
WHERE p.h_index IS NULL OR p.h_index != calc.h_index""",
    ),

    Step(
        "p3_persons_summary",
        "Rebuild sphinx_persons_summary",
        "CALL sp_update_persons_summary()",
    ),
]

PERSONS_PARTIAL: List[Step] = [

    Step(
        "p2_person_h_index_missing",
        "Calculate h_index for persons where NULL",
        """UPDATE persons p
JOIN (
    SELECT
        person_id,
        COALESCE(MAX(CASE WHEN rn <= citation_count THEN rn ELSE 0 END), 0) AS h_index
    FROM (
        SELECT
            a.person_id,
            w.citation_count,
            ROW_NUMBER() OVER (PARTITION BY a.person_id ORDER BY w.citation_count DESC) AS rn
        FROM authorships a
        JOIN works w ON a.work_id = w.id
        WHERE a.person_id IN (SELECT id FROM persons WHERE h_index IS NULL)
    ) ranked
    GROUP BY person_id
) calc ON p.id = calc.person_id
SET p.h_index = calc.h_index
WHERE p.h_index IS NULL""",
    ),
]


# ===================================================================
#  ORGANIZATIONS — stats
# ===================================================================

ORGANIZATIONS_FULL: List[Step] = [

    Step(
        "o1_org_stats",
        "Recalculate publication_count, researcher_count, total_citations, oa_works_count",
        """UPDATE organizations o
JOIN (
    SELECT
        a.affiliation_id,
        COUNT(DISTINCT a.work_id) AS publication_count,
        COUNT(DISTINCT a.person_id) AS researcher_count,
        COALESCE(SUM(w.citation_count), 0) AS total_citations,
        COUNT(DISTINCT CASE WHEN pub.open_access = 1 THEN a.work_id END) AS open_access_works_count
    FROM authorships a
    JOIN works w ON a.work_id = w.id
    LEFT JOIN publications pub ON w.id = pub.work_id
    WHERE a.affiliation_id IS NOT NULL
    GROUP BY a.affiliation_id
) calc ON o.id = calc.affiliation_id
SET o.publication_count = calc.publication_count,
    o.researcher_count = calc.researcher_count,
    o.total_citations = calc.total_citations,
    o.open_access_works_count = calc.open_access_works_count""",
    ),
]

ORGANIZATIONS_PARTIAL: List[Step] = [

    Step(
        "o1_org_stats_missing",
        "Recalculate stats for organizations with zero publication_count",
        """UPDATE organizations o
JOIN (
    SELECT
        a.affiliation_id,
        COUNT(DISTINCT a.work_id) AS publication_count,
        COUNT(DISTINCT a.person_id) AS researcher_count,
        COALESCE(SUM(w.citation_count), 0) AS total_citations,
        COUNT(DISTINCT CASE WHEN pub.open_access = 1 THEN a.work_id END) AS open_access_works_count
    FROM authorships a
    JOIN works w ON a.work_id = w.id
    LEFT JOIN publications pub ON w.id = pub.work_id
    WHERE a.affiliation_id IS NOT NULL
      AND a.affiliation_id IN (
          SELECT id FROM organizations WHERE publication_count = 0 OR publication_count IS NULL
      )
    GROUP BY a.affiliation_id
) calc ON o.id = calc.affiliation_id
SET o.publication_count = calc.publication_count,
    o.researcher_count = calc.researcher_count,
    o.total_citations = calc.total_citations,
    o.open_access_works_count = calc.open_access_works_count""",
    ),
]


# ===================================================================
#  VENUES — stats + h_index + yearly_stats + impact + ranking
# ===================================================================

VENUES_FULL: List[Step] = [

    Step(
        "v1_venue_stats",
        "Recalculate works_count, cited_by_count, coverage_start/end_year",
        """UPDATE venues v
JOIN (
    SELECT
        p.venue_id,
        COUNT(DISTINCT p.work_id) AS works_count,
        COALESCE(SUM(w.citation_count), 0) AS cited_by_count,
        MIN(p.year) AS coverage_start_year,
        MAX(p.year) AS coverage_end_year
    FROM publications p
    JOIN works w ON p.work_id = w.id
    WHERE p.venue_id IS NOT NULL
    GROUP BY p.venue_id
) calc ON v.id = calc.venue_id
SET v.works_count = calc.works_count,
    v.cited_by_count = calc.cited_by_count,
    v.coverage_start_year = calc.coverage_start_year,
    v.coverage_end_year = calc.coverage_end_year""",
    ),

    Step(
        "v2_venue_h_index",
        "Calculate h_index for all venues",
        """UPDATE venues v
JOIN (
    SELECT
        venue_id,
        COALESCE(MAX(CASE WHEN rn <= citation_count THEN rn ELSE 0 END), 0) AS h_index
    FROM (
        SELECT
            p.venue_id,
            w.citation_count,
            ROW_NUMBER() OVER (PARTITION BY p.venue_id ORDER BY w.citation_count DESC) AS rn
        FROM publications p
        JOIN works w ON p.work_id = w.id
        WHERE p.venue_id IS NOT NULL
    ) ranked
    GROUP BY venue_id
) calc ON v.id = calc.venue_id
SET v.h_index = calc.h_index
WHERE v.h_index IS NULL OR v.h_index != calc.h_index""",
    ),

    Step(
        "v3_venue_yearly_stats",
        "Populate venue_yearly_stats from publications",
        """INSERT INTO venue_yearly_stats (venue_id, year, works_count, oa_works_count, cited_by_count)
SELECT
    p.venue_id,
    p.year,
    COUNT(DISTINCT p.work_id),
    COUNT(DISTINCT CASE WHEN p.open_access = 1 THEN p.work_id END),
    COALESCE(SUM(w.citation_count), 0)
FROM publications p
JOIN works w ON p.work_id = w.id
WHERE p.venue_id IS NOT NULL AND p.year IS NOT NULL
GROUP BY p.venue_id, p.year
ON DUPLICATE KEY UPDATE
    works_count = VALUES(works_count),
    oa_works_count = VALUES(oa_works_count),
    cited_by_count = VALUES(cited_by_count)""",
    ),

    Step(
        "v4_impact_factors",
        "Calculate 10yr impact factors",
        "CALL sp_update_10yr_impact_factors()",
    ),

    Step(
        "v5_venue_ranking",
        "Calculate venue ranking scores",
        "CALL sp_calculate_venue_ranking()",
    ),

    Step(
        "v6_venues_summary",
        "Rebuild sphinx_venues_summary",
        "CALL sp_populate_sphinx_venues_summary()",
    ),
]

VENUES_PARTIAL: List[Step] = [

    Step(
        "v2_venue_h_index_missing",
        "Calculate h_index for venues where NULL",
        """UPDATE venues v
JOIN (
    SELECT
        venue_id,
        COALESCE(MAX(CASE WHEN rn <= citation_count THEN rn ELSE 0 END), 0) AS h_index
    FROM (
        SELECT
            p.venue_id,
            w.citation_count,
            ROW_NUMBER() OVER (PARTITION BY p.venue_id ORDER BY w.citation_count DESC) AS rn
        FROM publications p
        JOIN works w ON p.work_id = w.id
        WHERE p.venue_id IS NOT NULL
          AND p.venue_id IN (SELECT id FROM venues WHERE h_index IS NULL)
    ) ranked
    GROUP BY venue_id
) calc ON v.id = calc.venue_id
SET v.h_index = calc.h_index
WHERE v.h_index IS NULL""",
    ),

    Step(
        "v3_venue_yearly_stats_missing",
        "Populate venue_yearly_stats for venues with no stats",
        """INSERT INTO venue_yearly_stats (venue_id, year, works_count, oa_works_count, cited_by_count)
SELECT
    p.venue_id,
    p.year,
    COUNT(DISTINCT p.work_id),
    COUNT(DISTINCT CASE WHEN p.open_access = 1 THEN p.work_id END),
    COALESCE(SUM(w.citation_count), 0)
FROM publications p
JOIN works w ON p.work_id = w.id
WHERE p.venue_id IS NOT NULL AND p.year IS NOT NULL
  AND p.venue_id NOT IN (SELECT DISTINCT venue_id FROM venue_yearly_stats)
GROUP BY p.venue_id, p.year
ON DUPLICATE KEY UPDATE
    works_count = VALUES(works_count),
    oa_works_count = VALUES(oa_works_count),
    cited_by_count = VALUES(cited_by_count)""",
    ),
]


# ===================================================================
#  SPHINX — rebuild summary tables
# ===================================================================

SPHINX_FULL: List[Step] = [

    Step(
        "x1_work_author_summary",
        "Rebuild work_author_summary",
        "CALL sp_update_work_author_summary_all()",
    ),

    Step(
        "x2_work_subjects_summary",
        "Rebuild work_subjects_summary",
        "CALL sp_update_work_subjects_summary_all()",
    ),

    Step(
        "x3_works_summary",
        "Rebuild sphinx_works_summary",
        "CALL sp_update_works_summary()",
    ),

    Step(
        "x4_venues_summary",
        "Rebuild sphinx_venues_summary",
        "CALL sp_populate_sphinx_venues_summary()",
    ),

    Step(
        "x5_persons_summary",
        "Rebuild sphinx_persons_summary",
        "CALL sp_update_persons_summary()",
    ),
]

# No partial mode for sphinx — always full rebuild
SPHINX_PARTIAL: List[Step] = []


# ---------------------------------------------------------------------------
#  Execution engine
# ---------------------------------------------------------------------------

def execute_steps(conn: mariadb.Connection, steps: List[Step],
                  section_label: str, dry_run: bool) -> None:
    if not steps:
        log.info("=== %s — skipped (no steps for this mode) ===", section_label)
        return

    total = len(steps)
    log.info("=== %s (%d steps) ===", section_label, total)

    for i, step in enumerate(steps, 1):
        log.info("[%d/%d] %s — %s", i, total, step.name, step.description)

        if dry_run:
            preview = step.sql.replace('\n', ' ')[:120]
            log.info("  [dry-run] %s...", preview)
            continue

        t0 = time.time()
        cur = conn.cursor()
        try:
            cur.execute(step.sql)
            try:
                while cur.nextset():
                    pass
            except mariadb.Error:
                pass
            conn.commit()
            affected = cur.rowcount
            elapsed = time.time() - t0
            log.info("  affected=%d elapsed=%.1fs", affected, elapsed)
        except mariadb.Error as e:
            conn.rollback()
            log.error("  FAILED at step %s: %s", step.name, e)
            raise
        finally:
            cur.close()


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Metrics calculation and sphinx summary orchestrator.",
    )
    p.add_argument("--config", type=str, help="Path to config.ini")

    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true",
                      help="Complete recalculation of all selected entities")
    mode.add_argument("--partial", action="store_true",
                      help="Only update entities with missing/stale data (default)")

    p.add_argument("--works", action="store_true",
                   help="Reference/citation count sync")
    p.add_argument("--persons", action="store_true",
                   help="Person stats + h_index")
    p.add_argument("--organizations", action="store_true",
                   help="Organization stats")
    p.add_argument("--venues", action="store_true",
                   help="Venue stats + h_index + yearly_stats + impact + ranking")
    p.add_argument("--sphinx", action="store_true",
                   help="Rebuild all sphinx summary tables")
    p.add_argument("--dry-run", action="store_true",
                   help="Log steps without executing SQL")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()

    is_full = args.full
    # if no entity flag given, run all
    any_entity = args.works or args.persons or args.organizations or args.venues or args.sphinx
    run_works = args.works or not any_entity
    run_persons = args.persons or not any_entity
    run_orgs = args.organizations or not any_entity
    run_venues = args.venues or not any_entity
    run_sphinx = args.sphinx or (not any_entity and is_full)

    conn = _get_connection(args.config)
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION wait_timeout=28800, "
                    "net_read_timeout=3600, net_write_timeout=3600, "
                    "innodb_lock_wait_timeout=300")
        cur.close()

        mode_label = "full" if is_full else "partial"
        log.info("=== metrics %s ===", mode_label)

        if run_works:
            steps = WORKS_FULL if is_full else WORKS_PARTIAL
            execute_steps(conn, steps, f"works ({mode_label})", args.dry_run)

        if run_persons:
            steps = PERSONS_FULL if is_full else PERSONS_PARTIAL
            execute_steps(conn, steps, f"persons ({mode_label})", args.dry_run)

        if run_orgs:
            steps = ORGANIZATIONS_FULL if is_full else ORGANIZATIONS_PARTIAL
            execute_steps(conn, steps, f"organizations ({mode_label})", args.dry_run)

        if run_venues:
            steps = VENUES_FULL if is_full else VENUES_PARTIAL
            execute_steps(conn, steps, f"venues ({mode_label})", args.dry_run)

        if run_sphinx:
            steps = SPHINX_FULL if is_full else SPHINX_PARTIAL
            execute_steps(conn, steps, f"sphinx ({mode_label})", args.dry_run)

        elapsed = time.time() - started
        log.info("=== metrics complete — elapsed=%.1fs ===", elapsed)
    finally:
        try:
            conn.close()
        except mariadb.Error:
            pass


if __name__ == "__main__":
    main()
