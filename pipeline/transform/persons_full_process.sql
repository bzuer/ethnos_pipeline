-- ============================================================================
-- Unified person processing script (v4.1)
--
-- Phase 1: Cleanup & deduplication (regex name cleaning, normalized_name, external IDs)
-- Phase 2: Signatures & name parts (sp_generate_person_signatures, sp_fix_person_name_parts)
-- Phase 3: Metrics recalculation — handled by metrics.py --full (removed from this script)
--
-- Run: mariadb data < pipeline/transform/persons_full_process.sql
-- Requires: sp_merge_persons_in_batches, sp_generate_person_signatures, sp_fix_person_name_parts
--           (defined in procedures.sql)
-- ============================================================================

-- Title-prefix cleanup regex (centralized)
SET @clean_regex = '(?i)^(dr\\.?|dra\\.?|prof\\.?|profa\\.?|professor\\.?|professora\\.?|ph\\.?d\\.?|msc\\.?|mr\\.?|mrs\\.?|ms\\.?|rev\\.?|eng\\.?|engª\\.?|me\\.?)\\s+';

-- Ensure staging tables exist
CREATE TABLE IF NOT EXISTS temp_person_merge_pairs (
    primary_person_id INT(11) NOT NULL,
    secondary_person_id INT(11) NOT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (secondary_person_id),
    KEY idx_primary (primary_person_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

CREATE TABLE IF NOT EXISTS staging_person_signatures (
    person_id INT PRIMARY KEY,
    signature_string VARCHAR(255) NOT NULL,
    KEY idx_staging_sig (signature_string)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;


-- =====================================================================
-- PHASE 1: CLEANUP & DEDUPLICATION
-- =====================================================================

-- [STEP 1a] Pre-deduplication (regex-based name cleaning)
SELECT 'STEP 1a: Pre-deduplication (regex)...' AS status;
TRUNCATE TABLE temp_person_merge_pairs;

INSERT INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
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
ON DUPLICATE KEY UPDATE primary_person_id = LEAST(primary_person_id, VALUES(primary_person_id));

-- [STEP 1b] Pre-deduplication (normalized_name column)
SELECT 'STEP 1b: Pre-deduplication (normalized_name)...' AS status;
INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, normalized_name
    FROM persons
    WHERE normalized_name IS NOT NULL AND normalized_name != ''
    GROUP BY normalized_name
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.normalized_name = canonical.normalized_name AND duplicate.id != canonical.id;

CALL sp_merge_persons_in_batches();


-- [STEP 2] Remove invalid records (cleaned name <= 2 chars)
SELECT 'STEP 2: Removing syntactically null records...' AS status;
DELETE FROM persons
WHERE LENGTH(
    TRIM(REGEXP_REPLACE(
        REPLACE(
            TRIM(REGEXP_REPLACE(
                REGEXP_REPLACE(preferred_name, @clean_regex, ''),
                '^[\\s[:punct:]\\-—''"\'`]+|[\\s[:punct:]\\-—''"\'`]+$', ''
            )),
        '.', ' '),
    '\\s+', ' '))
) <= 2;


-- [STEP 3] Advanced cleanup (remove title prefixes, normalize spacing)
SELECT 'STEP 3: Removing title prefixes, normalizing spacing...' AS status;
UPDATE IGNORE persons
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
   OR preferred_name REGEXP '\\s{2,}';


-- [STEP 4] Deduplication by external identifiers (ORCID, Scopus, Lattes)
SELECT 'STEP 4: Deduplication by ORCID, Scopus, Lattes...' AS status;
TRUNCATE TABLE temp_person_merge_pairs;

-- ORCID
INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, orcid
    FROM persons
    WHERE orcid IS NOT NULL AND orcid != ''
    GROUP BY orcid
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.orcid = canonical.orcid AND duplicate.id != canonical.id;

-- Scopus
INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, scopus_id
    FROM persons
    WHERE scopus_id IS NOT NULL AND scopus_id != ''
    GROUP BY scopus_id
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.scopus_id = canonical.scopus_id AND duplicate.id != canonical.id;

-- Lattes
INSERT IGNORE INTO temp_person_merge_pairs (primary_person_id, secondary_person_id)
SELECT canonical.id, duplicate.id
FROM persons AS duplicate
JOIN (
    SELECT MIN(id) AS id, lattes_id
    FROM persons
    WHERE lattes_id IS NOT NULL AND lattes_id != ''
    GROUP BY lattes_id
    HAVING COUNT(id) > 1
) AS canonical
ON duplicate.lattes_id = canonical.lattes_id AND duplicate.id != canonical.id;

CALL sp_merge_persons_in_batches();


-- =====================================================================
-- PHASE 2: SIGNATURES & NAME PARTS
-- =====================================================================

-- [STEP 5a] Generate signatures (cursor-based, preserves compound hyphens, tiered particles)
SELECT 'STEP 5a: Generating signatures (sp_generate_person_signatures)...' AS status;
CALL sp_generate_person_signatures(NULL, 1);
TRUNCATE TABLE staging_person_signatures;


-- PHASE 3: METRICS RECALCULATION
-- Handled by: venv/bin/python pipeline/transform/metrics.py --full
-- (sp_run_full_recalculation is obsolete — removed)

SELECT 'persons_full_process.sql completed.' AS final_status;