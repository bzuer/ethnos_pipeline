#!/usr/bin/env python3
"""
Post-normalization data cleanup orchestrator.

Replaces manual execution of delete_from_works.sql and persons_full_process.sql
with a Python wrapper that provides granular control, logging, and --dry-run.

Runs AFTER cleanup.py (text normalization). Handles:
  --works    Junk deletion, orphan removal, OA propagation, consistency checks.
  --persons  Person deduplication, name cleanup, signatures, metrics recalculation.
  --full     Both (default).

Usage:
  python postprocess.py                        # full (works + persons)
  python postprocess.py --works                # works only
  python postprocess.py --persons              # persons only
  python postprocess.py --dry-run              # show steps without executing
  python postprocess.py --works --dry-run      # preview works steps
"""

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import List

import mariadb

# allow running from repo root
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


# ---------------------------------------------------------------------------
#  WORKS steps (from delete_from_works.sql)
# ---------------------------------------------------------------------------

WORKS_STEPS: List[Step] = [

    # §0 — Abstract / title prefix cleaning

    Step(
        "s0a_strip_abstract_prefix",
        "Strip leading Abstract:/Resumen:/Resumo:/etc. from abstracts",
        r"""UPDATE works
SET abstract = TRIM(REGEXP_REPLACE(
    abstract,
    '^(abstract|resumen|resumo|résumé|zusammenfassung|sommario|riassunto)[[:space:]]*:?[[:space:]]*',
    ''
))
WHERE abstract REGEXP '^(abstract|resumen|resumo|résumé|zusammenfassung|sommario|riassunto)[[:space:]]*:?'""",
    ),

    Step(
        "s0b_strip_abstract_punct",
        "Strip leading punctuation/whitespace remnants from abstracts",
        r"""UPDATE works
SET abstract = TRIM(REGEXP_REPLACE(abstract, '^[[:space:]:\-–—]+', ''))
WHERE abstract REGEXP '^[[:space:]:\-–—]+'""",
    ),

    Step(
        "s0c_null_empty_abstracts",
        "Null out abstracts that became empty",
        """UPDATE works SET abstract = NULL
WHERE abstract IS NOT NULL AND TRIM(abstract) = ''""",
    ),

    Step(
        "s0d_strip_title_junk",
        "Strip leading non-alpha junk from titles",
        r"""UPDATE works
SET title = TRIM(REGEXP_REPLACE(title, '^[^[:alpha:]]+', ''))
WHERE title REGEXP '^[^[:alpha:]]'""",
    ),

    # §1 — Delete junk / non-scholarly works

    Step(
        "s1_delete_junk_works",
        "Delete junk/non-scholarly works by title patterns",
        r"""DELETE FROM works
WHERE

    -- 1a. Empty / placeholder / garbage
    TRIM(COALESCE(title, '')) = ''
    OR LOWER(TRIM(title)) IN (
        '-', '--', '---', 'n/a', 'na', 'none', 'unknown', 'untitled',
        'no title', 'title unavailable', 'título indisponível',
        'título não disponível', 'sans titre', 'ohne titel', 'senza titolo'
    )
    OR title LIKE '%.%.%.%'
    OR title LIKE '%,%,%.'
    OR title LIKE '%.%:%'
    OR title LIKE '%.%,%.%'
    OR title LIKE '%.%)%.%'
    OR title LIKE '%.%,%:%'

    -- 1b. Exact standalone boilerplate titles
    OR LOWER(TRIM(title)) IN (
        'publications received', 'notes', 'notes and news',
        'notes of the quarter', 'note of the quarter',
        'back numbers of the journal', 'rejoinder', 'discussion',
        'comment', 'comments', 'reply',
        'international meetings', 'annual meeting',
        'front matter', 'back matter', 'front cover', 'back cover',
        'cover and front matter', 'cover and back matter',
        'table of contents', 'masthead', 'impressum', 'colophon',
        'editorial board', 'conselho editorial', 'comitê editorial',
        'equipo editorial', 'comité de rédaction', 'herausgeber',
        'about the authors', 'notes on contributors', 'notes on contributor',
        'contributors', 'contributor', 'contribuidores',
        'colaboradores', 'collaborators',
        'author biographies', 'author biography',
        'biographical note', 'biographical notes',
        'notas sobre os colaboradores', 'notas sobre los colaboradores',
        'miscellaneous', 'recent scholarship', 'recently published',
        'new publications', 'back issues',
        'books received', 'obras recebidas', 'libros recibidos',
        'new books', 'publications reçues', 'eingegangene publikationen',
        'call for papers', 'call for submissions',
        'conference announcement', 'conference report', 'conference program',
        'symposium announcement', 'meeting report', 'minutes of meeting',
        'agenda',
        'abstract', 'abstracts', 'resumo', 'resumos',
        'resumen', 'resúmenes', 'résumé', 'résumés',
        'zusammenfassung', 'zusammenfassungen',
        'toc', 'subject index', 'author index', 'keyword index',
        'contents', 'sumário', 'sumario', 'índice', 'indice',
        'índices', 'indices', 'contents volume', 'annual index',
        'correspondence',
        'preface', 'foreword', 'avant-propos',
        'apresentação', 'presentación',
        'issue information',
        'expediente', 'table des illustrations',
        'the bantu treasury series', 'the vilakazi prize',
        'witwatersrand university press', 'un publications',
        'matéria de capa', 'matéria final',
        'materias preliminares', 'materias finales'
    )

    -- 1c. Anchored prefix patterns
    OR title LIKE 'list of %'
    OR title LIKE 'message from %'
    OR title LIKE 'program for %'
    OR title LIKE 'surveys by %'

    OR title LIKE 'acknowledgment%'
    OR title LIKE 'acknowledgement%'
    OR title LIKE 'agradecimiento%'
    OR title LIKE 'agradecimento%'
    OR title LIKE 'remerciement%'
    OR title LIKE 'danksagung%'
    OR title LIKE 'ringraziament%'

    OR title LIKE 'editorial%'
    OR title LIKE 'note from the editor%'
    OR title LIKE 'from the editor%'
    OR title LIKE 'carta do editor%'
    OR title LIKE 'letter from the editor%'

    OR title LIKE 'announcement%'
    OR title LIKE 'anúncio%'
    OR title LIKE 'anuncio%'
    OR title LIKE 'mitteilung%'
    OR title LIKE 'avviso%'
    OR title LIKE 'avvisi %'

    OR title LIKE 'correction:%'
    OR title LIKE 'correction to %'
    OR title LIKE 'corrections:%'
    OR title LIKE 'corrections to %'
    OR title LIKE 'corrigendum%'
    OR title LIKE 'corregendum%'
    OR title LIKE 'errata%'
    OR title LIKE 'erratum%'
    OR title LIKE 'correção%'
    OR title LIKE 'correções%'
    OR title LIKE 'retraction:%'
    OR title LIKE 'retraction of %'
    OR title LIKE 'retracted:%'
    OR title LIKE 'expression of concern%'

    OR title LIKE 'index to volume%'
    OR title LIKE 'index for volume%'
    OR title LIKE 'contents of volume%'

    OR title LIKE 'issue information%'
    OR title LIKE 'informações do fascículo%'
    OR title LIKE 'información del número%'
    OR title LIKE 'informazioni del fascicolo%'

    OR title LIKE 'call for papers%'
    OR title LIKE 'call for submissions%'

    OR title LIKE 'letter to the editor%'
    OR title LIKE 'letters to the editor%'

    OR title LIKE 'book review:%'
    OR title LIKE 'book reviews:%'
    OR title LIKE 'book review -%'
    OR title LIKE 'resenha:%'
    OR title LIKE 'reseña:%'
    OR title LIKE 'comptes rendus%'
    OR title LIKE 'comptes-rendus%'
    OR title LIKE 'rezension:%'
    OR title LIKE 'rezensionen:%'
    OR title LIKE 'reviewed by %'

    OR title LIKE 'communications, comments%'

    OR LOWER(TRIM(title)) IN ('notice', 'notices')

    OR title LIKE 'teses %'
    OR title LIKE 'dissertações %'

    -- 1d. REGEX patterns — structural debris
    OR title REGEXP ',\\s*eds?\\.?\\s*$'
    OR title REGEXP '\\(eds?\\.?\\)\\s*$'
    OR title REGEXP ',\\s*[0-9]+\\s*pp\\.?\\s*$'
    OR title REGEXP ',\\s*[0-9]+\\s*pages?\\s*$'
    OR title REGEXP '\\bISBN[\\s:\\-]*[0-9Xx\\-]{10,}'
    OR title REGEXP '\\b(USD|GBP|EUR|BRL|AUD|CAD|CHF)\\s*[0-9]+'
    OR title REGEXP '[\\$£€]\\s*[0-9]+\\.[0-9]{2}'""",
    ),

    # §2 — Nullify Python 'None' string literals

    Step(
        "s2a_nullify_none_publications",
        "Nullify Python 'None' string literals in publications",
        """UPDATE publications
SET
    volume         = NULLIF(volume, 'None'),
    pmid           = NULLIF(pmid, 'None'),
    pmcid          = NULLIF(pmcid, 'None'),
    isbn           = NULLIF(isbn, 'None'),
    asin           = NULLIF(asin, 'None'),
    udc            = NULLIF(udc, 'None'),
    lbc            = NULLIF(lbc, 'None'),
    ddc            = NULLIF(ddc, 'None'),
    lcc            = NULLIF(lcc, 'None'),
    google_book_id = NULLIF(google_book_id, 'None')
WHERE
    volume = 'None' OR pmid = 'None' OR pmcid = 'None' OR isbn = 'None'
    OR asin = 'None' OR udc = 'None' OR lbc = 'None'
    OR ddc = 'None' OR lcc = 'None' OR google_book_id = 'None'""",
    ),

    Step(
        "s2b_nullify_none_files",
        "Nullify Python 'None' string literals in files",
        """UPDATE files
SET
    language = NULLIF(language, 'None'),
    sha1     = NULLIF(sha1, 'None'),
    sha256   = NULLIF(sha256, 'None'),
    ipfs_cid = NULLIF(ipfs_cid, 'None')
WHERE
    language = 'None' OR sha1 = 'None' OR sha256 = 'None' OR ipfs_cid = 'None'""",
    ),

    # §3 — Orphan removal

    Step(
        "s3a_orphan_works_no_authorship",
        "Delete works without any authorship",
        """DELETE w FROM works w
LEFT JOIN authorships a ON a.work_id = w.id
WHERE a.work_id IS NULL""",
    ),

    Step(
        "s3b_orphan_files",
        "Delete files pointing to nonexistent publications",
        """DELETE f FROM files f
LEFT JOIN publications p ON f.publication_id = p.id
WHERE p.id IS NULL""",
    ),

    Step(
        "s3c_orphan_sphinx_queue",
        "Delete sphinx_queue entries for deleted works",
        """DELETE sq FROM sphinx_queue sq
LEFT JOIN works w ON sq.work_id = w.id
WHERE w.id IS NULL""",
    ),

    Step(
        "s3d_orphan_venues",
        "Delete venues with zero publications",
        """DELETE v FROM venues v
LEFT JOIN publications p ON p.venue_id = v.id
WHERE p.venue_id IS NULL""",
    ),

    Step(
        "s3e_orphan_subjects",
        "Delete subjects with zero associations",
        """DELETE s FROM subjects s
LEFT JOIN work_subjects  ws ON ws.subject_id = s.id
LEFT JOIN venue_subjects vs ON vs.subject_id = s.id
WHERE ws.subject_id IS NULL AND vs.subject_id IS NULL""",
    ),

    Step(
        "s3f_orphan_organizations",
        "Delete organizations with no references anywhere",
        """DELETE o FROM organizations o
LEFT JOIN authorships  a   ON o.id = a.affiliation_id
LEFT JOIN funding      f   ON o.id = f.funder_id
LEFT JOIN programs     pr  ON o.id = pr.institution_id
LEFT JOIN publications pub ON o.id = pub.publisher_id
LEFT JOIN venues       ve  ON o.id = ve.publisher_id
WHERE a.affiliation_id  IS NULL
  AND f.funder_id       IS NULL
  AND pr.institution_id IS NULL
  AND pub.publisher_id  IS NULL
  AND ve.publisher_id   IS NULL""",
    ),

    # §4 — Open access propagation

    Step(
        "s4_open_access_propagation",
        "Propagate open_access flag from venues/licenses/SciELO",
        """UPDATE publications p
JOIN venues v ON p.venue_id = v.id
SET p.open_access = 1
WHERE p.open_access = 0
  AND (v.open_access = 1 OR p.license_url IS NOT NULL OR p.scielo_pid IS NOT NULL)""",
    ),

    # §5 — Consistency procedures

    Step(
        "s5a_sp_clean_inconsistent",
        "CALL sp_clean_inconsistent_data()",
        "CALL sp_clean_inconsistent_data()",
    ),

    Step(
        "s5b_sp_clean_orphaned_data",
        "CALL sp_clean_orphaned_data()",
        "CALL sp_clean_orphaned_data()",
    ),

    Step(
        "s5c_sp_clean_orphaned_persons",
        "CALL sp_clean_orphaned_persons()",
        "CALL sp_clean_orphaned_persons()",
    ),
]


# ---------------------------------------------------------------------------
#  PERSONS setup (session variable + staging tables)
# ---------------------------------------------------------------------------

PERSONS_SETUP: List[Step] = [

    Step(
        "setup_clean_regex",
        "Set @clean_regex session variable for title prefix patterns",
        r"""SET @clean_regex = '(?i)^(dr\.?|dra\.?|prof\.?|profa\.?|professor\.?|professora\.?|ph\.?d\.?|msc\.?|mr\.?|mrs\.?|ms\.?|rev\.?|eng\.?|engª\.?|me\.?)\s+'""",
    ),

    Step(
        "setup_temp_merge_pairs",
        "Ensure temp_person_merge_pairs table exists",
        """CREATE TABLE IF NOT EXISTS temp_person_merge_pairs (
    primary_person_id INT(11) NOT NULL,
    secondary_person_id INT(11) NOT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (secondary_person_id),
    KEY idx_primary (primary_person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci""",
    ),

    Step(
        "setup_staging_signatures",
        "Ensure staging_person_signatures table exists",
        """CREATE TABLE IF NOT EXISTS staging_person_signatures (
    person_id INT PRIMARY KEY,
    signature_string VARCHAR(255) NOT NULL,
    KEY idx_staging_sig (signature_string)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci""",
    ),
]


# ---------------------------------------------------------------------------
#  PERSONS steps (from persons_full_process.sql)
# ---------------------------------------------------------------------------

PERSONS_STEPS: List[Step] = [

    # Phase 1: cleanup & deduplication

    Step(
        "p1a_truncate",
        "Truncate merge pairs table for regex pre-dedup",
        "TRUNCATE TABLE temp_person_merge_pairs",
    ),

    Step(
        "p1a_prededup_regex",
        "Pre-dedup persons by regex-cleaned preferred_name",
        r"""INSERT INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
WITH CleanedNames AS (
    SELECT
        id,
        LOWER(
            TRIM(REGEXP_REPLACE(
                REPLACE(
                    TRIM(REGEXP_REPLACE(
                        REGEXP_REPLACE(preferred_name, @clean_regex, ''),
                        '^[\\s[:punct:]\\-—''"\'`]+|[\\s[:punct:]\\-—''"\'`]+$', ''
                    )),
                '.', ' '),
            '\\s+', ' '))
        ) AS cleaned_name
    FROM persons
),
RankedNames AS (
    SELECT
        id,
        cleaned_name,
        MIN(id) OVER (PARTITION BY cleaned_name) AS primary_id
    FROM CleanedNames
    WHERE cleaned_name IS NOT NULL AND cleaned_name != '' AND LENGTH(cleaned_name) > 2
)
SELECT primary_id, id
FROM RankedNames
WHERE id != primary_id
ON DUPLICATE KEY UPDATE primary_person_id = LEAST(primary_person_id, VALUES(primary_person_id))""",
    ),

    Step(
        "p1b_prededup_normalized",
        "Pre-dedup persons by normalized_name column",
        """INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, normalized_name
    FROM persons
    WHERE normalized_name IS NOT NULL AND normalized_name != ''
    GROUP BY normalized_name
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.normalized_name = canonical.normalized_name AND duplicate.id != canonical.id""",
    ),

    Step(
        "p1_merge",
        "Merge person duplicates (batch 1: regex + normalized_name)",
        "CALL sp_merge_persons_in_batches()",
    ),

    Step(
        "p2_delete_invalid",
        "Delete persons with syntactically null names (cleaned length <= 2)",
        r"""DELETE FROM persons
WHERE LENGTH(
    TRIM(REGEXP_REPLACE(
        REPLACE(
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE(preferred_name, @clean_regex, ''),
                '^[\\s[:punct:]\\-—''"\'`]+|[\\s[:punct:]\\-—''"\'`]+$', ''
            )),
        '.', ' '),
    '\\s+', ' '))
) <= 2""",
    ),

    Step(
        "p3_clean_names",
        "Remove title prefixes and normalize spacing in preferred_name",
        r"""UPDATE IGNORE persons
SET preferred_name = TRIM(REGEXP_REPLACE(
    REPLACE(
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(preferred_name, @clean_regex, ''),
            '^[\\s[:punct:]\\-—''"\'`]+|[\\s[:punct:]\\-—''"\'`]+$', ''
        )),
    '.', ' '),
'\\s+', ' '))
WHERE preferred_name REGEXP @clean_regex
   OR preferred_name REGEXP '^[\\s[:punct:]\\-—''"\'`]+|[\\s[:punct:]\\-—''"\'`]+$'
   OR preferred_name REGEXP '\\s{2,}'""",
    ),

    # Phase 2: dedup by external IDs

    Step(
        "p4_truncate",
        "Truncate merge pairs table for ID-based dedup",
        "TRUNCATE TABLE temp_person_merge_pairs",
    ),

    Step(
        "p4a_dedup_orcid",
        "Dedup persons by ORCID",
        """INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, orcid
    FROM persons
    WHERE orcid IS NOT NULL AND orcid != ''
    GROUP BY orcid
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.orcid = canonical.orcid AND duplicate.id != canonical.id""",
    ),

    Step(
        "p4b_dedup_scopus",
        "Dedup persons by Scopus ID",
        """INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, scopus_id
    FROM persons
    WHERE scopus_id IS NOT NULL AND scopus_id != ''
    GROUP BY scopus_id
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.scopus_id = canonical.scopus_id AND duplicate.id != canonical.id""",
    ),

    Step(
        "p4c_dedup_lattes",
        "Dedup persons by Lattes ID",
        """INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, lattes_id
    FROM persons
    WHERE lattes_id IS NOT NULL AND lattes_id != ''
    GROUP BY lattes_id
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.lattes_id = canonical.lattes_id AND duplicate.id != canonical.id""",
    ),

    Step(
        "p4_merge",
        "Merge person duplicates (batch 2: ORCID + Scopus + Lattes)",
        "CALL sp_merge_persons_in_batches()",
    ),

    # Phase 3: signatures & name parts

    Step(
        "p5a_signatures",
        "Generate person signatures (sp_generate_person_signatures)",
        "CALL sp_generate_person_signatures(NULL, 1)",
    ),

    Step(
        "p5b_name_parts",
        "Fix given_names/family_name (sp_fix_person_name_parts)",
        "CALL sp_fix_person_name_parts(NULL, 1)",
    ),

    # Phase 4: metrics recalculation

    Step(
        "p6_recalculation",
        "Full recalculation of person/org/venue metrics",
        "CALL sp_run_full_recalculation()",
    ),
]


# ---------------------------------------------------------------------------
#  Execution engine
# ---------------------------------------------------------------------------

def execute_steps(conn: mariadb.Connection, steps: List[Step],
                  section_label: str, dry_run: bool) -> None:
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
            # drain any result sets from stored procedures
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
        description="Post-normalization data cleanup: junk removal, orphans, "
                    "persons dedup, signatures, metrics.",
    )
    p.add_argument("--config", type=str, help="Path to config.ini")
    p.add_argument("--works", action="store_true",
                   help="Run works cleanup (§0-§5)")
    p.add_argument("--persons", action="store_true",
                   help="Run persons processing (phases 1-4)")
    p.add_argument("--full", action="store_true",
                   help="Run both --works and --persons (default)")
    p.add_argument("--dry-run", action="store_true",
                   help="Log steps without executing SQL")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    started = time.time()

    # default: full if no specific flag given
    run_works = args.full or args.works or (not args.works and not args.persons)
    run_persons = args.full or args.persons or (not args.works and not args.persons)

    conn = _get_connection(args.config)
    try:
        # increase server-side timeouts for long-running stored procedures
        cur = conn.cursor()
        cur.execute("SET SESSION wait_timeout=28800, "
                    "net_read_timeout=3600, net_write_timeout=3600, "
                    "innodb_lock_wait_timeout=300")
        cur.close()

        if run_works:
            execute_steps(conn, WORKS_STEPS, "works", args.dry_run)

        if run_persons:
            execute_steps(conn, PERSONS_SETUP, "persons-setup", args.dry_run)
            execute_steps(conn, PERSONS_STEPS, "persons", args.dry_run)

        elapsed = time.time() - started
        log.info("=== postprocess complete — elapsed=%.1fs ===", elapsed)
    finally:
        try:
            conn.close()
        except mariadb.Error:
            pass


if __name__ == "__main__":
    main()
