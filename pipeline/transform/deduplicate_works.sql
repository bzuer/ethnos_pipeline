-- ============================================================
-- deduplicate_works.sql — Work deduplication (title + author)
-- ============================================================
--
-- Run directly:  mariadb data < pipeline/transform/deduplicate_works.sql
--
-- Requires:  work_author_summary, sphinx_works_summary populated
--            (i.e., run metrics.py --sphinx first)
--
-- Dedup key: full_title_normalized = LOWER(CONCAT(title, subtitle))
--   This prevents false merges of works that share a title but have
--   different subtitles (e.g., "Applied Anthropology" review articles
--   each reviewing a different book).
--
-- Three strategies, applied in sequence:
--   1. full_title_normalized + full author string
--   2. full_title_normalized + first author + year
--   3. full_title_normalized + venue + year
--
-- Merge mechanics (sp_run_pending_work_merges):
--   Redirects all FKs (authorships, publications, work_references,
--   work_subjects, files, funding, course_bibliography) from secondary
--   to primary work_id, then deletes the secondary work.
-- ============================================================


-- ============================================================
-- §0  SCHEMA: ensure full_title_normalized column exists
-- ============================================================

-- Idempotent: MariaDB errors on duplicate column, so we check first.
SET @col_exists = (
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'works'
      AND column_name = 'full_title_normalized'
);

SET @ddl = IF(@col_exists = 0,
    'ALTER TABLE works ADD COLUMN full_title_normalized VARCHAR(255) GENERATED ALWAYS AS (LEFT(TRIM(LOWER(CONCAT(title, COALESCE(CONCAT('' '', subtitle), '''')))), 255)) STORED AFTER title_normalized',
    'SELECT ''full_title_normalized already exists'' AS status'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists = (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'works'
      AND index_name = 'idx_works_full_title_normalized'
);

SET @ddl_idx = IF(@idx_exists = 0,
    'CREATE INDEX idx_works_full_title_normalized ON works (full_title_normalized)',
    'SELECT ''idx_works_full_title_normalized already exists'' AS status'
);
PREPARE stmt FROM @ddl_idx;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- ============================================================
-- §1  PROCEDURE: sp_run_pending_work_merges
-- ============================================================
-- Reads temp_work_merge_pairs (primary_work_id, secondary_work_id).
-- Redirects all FKs to primary, deletes secondary works.

DROP PROCEDURE IF EXISTS sp_run_pending_work_merges;
DELIMITER //
CREATE PROCEDURE sp_run_pending_work_merges()
BEGIN
    -- Redirect all foreign keys to the primary work
    UPDATE IGNORE authorships t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    UPDATE IGNORE course_bibliography t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    UPDATE IGNORE files t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    UPDATE IGNORE funding t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    UPDATE IGNORE publications t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    UPDATE IGNORE work_references t
    JOIN temp_work_merge_pairs p ON t.citing_work_id = p.secondary_work_id
    SET t.citing_work_id = p.primary_work_id;

    UPDATE IGNORE work_references t
    JOIN temp_work_merge_pairs p ON t.cited_work_id = p.secondary_work_id
    SET t.cited_work_id = p.primary_work_id;

    UPDATE IGNORE work_subjects t
    JOIN temp_work_merge_pairs p ON t.work_id = p.secondary_work_id
    SET t.work_id = p.primary_work_id;

    -- Delete the secondary (duplicate) works
    DELETE w FROM works w
    JOIN temp_work_merge_pairs p ON w.id = p.secondary_work_id;
END //
DELIMITER ;


-- ============================================================
-- §2  PROCEDURE: sp_deduplicate_works_advanced
-- ============================================================
-- Three-strategy dedup using full_title_normalized (title+subtitle).
-- Loops until no more pairs are found.

DROP PROCEDURE IF EXISTS sp_deduplicate_works_advanced;
DELIMITER //
CREATE PROCEDURE sp_deduplicate_works_advanced(IN p_batch_size INT)
BEGIN
    DECLARE v_total_pairs INT DEFAULT 1;

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_work_merge_pairs (
        primary_work_id INT NOT NULL,
        secondary_work_id INT NOT NULL,
        PRIMARY KEY (secondary_work_id),
        KEY idx_primary (primary_work_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

    WHILE v_total_pairs > 0 DO
        TRUNCATE TABLE temp_work_merge_pairs;

        -- Strategy 1: full_title_normalized + full author string
        INSERT INTO temp_work_merge_pairs (primary_work_id, secondary_work_id)
        SELECT canonical.id, duplicate.id
        FROM works duplicate
        JOIN work_author_summary was_dup ON duplicate.id = was_dup.work_id
        JOIN (
            SELECT MIN(w.id) as id, w.full_title_normalized, was.author_string
            FROM works w
            JOIN work_author_summary was ON w.id = was.work_id
            WHERE w.full_title_normalized IS NOT NULL
              AND LENGTH(w.full_title_normalized) > 10
              AND was.author_string IS NOT NULL
            GROUP BY w.full_title_normalized, was.author_string
            HAVING COUNT(w.id) > 1
        ) canonical ON duplicate.full_title_normalized = canonical.full_title_normalized
                   AND was_dup.author_string = canonical.author_string
                   AND duplicate.id != canonical.id
        ON DUPLICATE KEY UPDATE primary_work_id = LEAST(primary_work_id, VALUES(primary_work_id));

        -- Strategy 2: full_title_normalized + first author + year
        INSERT INTO temp_work_merge_pairs (primary_work_id, secondary_work_id)
        SELECT canonical.id, duplicate.id
        FROM works duplicate
        JOIN sphinx_works_summary sws_dup ON duplicate.id = sws_dup.id
        JOIN work_author_summary was_dup ON duplicate.id = was_dup.work_id
        JOIN (
            SELECT MIN(w.id) as id, w.full_title_normalized, was.first_author_id, sws.year
            FROM works w
            JOIN work_author_summary was ON w.id = was.work_id
            JOIN sphinx_works_summary sws ON w.id = sws.id
            WHERE w.full_title_normalized IS NOT NULL
              AND LENGTH(w.full_title_normalized) > 10
              AND was.first_author_id IS NOT NULL
              AND sws.year > 0
            GROUP BY w.full_title_normalized, was.first_author_id, sws.year
            HAVING COUNT(w.id) > 1
        ) canonical ON duplicate.full_title_normalized = canonical.full_title_normalized
                   AND was_dup.first_author_id = canonical.first_author_id
                   AND sws_dup.year = canonical.year
                   AND duplicate.id != canonical.id
        ON DUPLICATE KEY UPDATE primary_work_id = LEAST(primary_work_id, VALUES(primary_work_id));

        -- Strategy 3: full_title_normalized + venue + year
        INSERT INTO temp_work_merge_pairs (primary_work_id, secondary_work_id)
        SELECT canonical.id, duplicate.id
        FROM works duplicate
        JOIN sphinx_works_summary sws_dup ON duplicate.id = sws_dup.id
        JOIN (
            SELECT MIN(w.id) as id, w.full_title_normalized, sws.venue_name, sws.year
            FROM sphinx_works_summary sws
            JOIN works w ON sws.id = w.id
            WHERE w.full_title_normalized IS NOT NULL
              AND LENGTH(w.full_title_normalized) > 10
              AND sws.venue_name IS NOT NULL
              AND sws.venue_name != ''
              AND sws.year > 0
            GROUP BY w.full_title_normalized, sws.venue_name, sws.year
            HAVING COUNT(sws.id) > 1
        ) canonical ON duplicate.full_title_normalized = canonical.full_title_normalized
                   AND sws_dup.venue_name = canonical.venue_name
                   AND sws_dup.year = canonical.year
                   AND duplicate.id != canonical.id
        ON DUPLICATE KEY UPDATE primary_work_id = LEAST(primary_work_id, VALUES(primary_work_id));

        -- Transitive closure: if A merges into B and B merges into C,
        -- redirect A directly to C
        UPDATE temp_work_merge_pairs t1
        JOIN temp_work_merge_pairs t2 ON t1.primary_work_id = t2.secondary_work_id
        SET t1.primary_work_id = t2.primary_work_id;

        SELECT COUNT(*) INTO v_total_pairs FROM temp_work_merge_pairs;

        -- Batch cap
        IF v_total_pairs > p_batch_size THEN
            DELETE FROM temp_work_merge_pairs
            WHERE secondary_work_id NOT IN (
                SELECT secondary_work_id FROM (
                    SELECT secondary_work_id
                    FROM temp_work_merge_pairs
                    LIMIT p_batch_size
                ) as subquery
            );
        END IF;

        -- Execute merges
        IF v_total_pairs > 0 THEN
            CALL sp_run_pending_work_merges();
        END IF;

    END WHILE;

    DROP TEMPORARY TABLE IF EXISTS temp_work_merge_pairs;
END //
DELIMITER ;


-- ============================================================
-- §3  PROCEDURE: sp_merge_works_by_title_authors
-- ============================================================
-- Simpler variant: full_title_normalized + author string only.
-- Useful for targeted cleanup without venue/year fallback strategies.

DROP PROCEDURE IF EXISTS sp_merge_works_by_title_authors;
DELIMITER //
CREATE PROCEDURE sp_merge_works_by_title_authors(IN p_batch_size INT)
BEGIN
    DECLARE v_rows_affected INT DEFAULT 1;

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_work_merge_pairs (
        primary_work_id INT NOT NULL,
        secondary_work_id INT NOT NULL,
        PRIMARY KEY (secondary_work_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

    WHILE v_rows_affected > 0 DO
        TRUNCATE TABLE temp_work_merge_pairs;

        INSERT INTO temp_work_merge_pairs (primary_work_id, secondary_work_id)
        WITH WorkGroups AS (
            SELECT MIN(w.id) as primary_id, w.full_title_normalized, was.author_string
            FROM works w
            JOIN work_author_summary was ON w.id = was.work_id
            WHERE w.full_title_normalized IS NOT NULL
              AND LENGTH(w.full_title_normalized) > 10
            GROUP BY w.full_title_normalized, was.author_string
            HAVING COUNT(w.id) > 1
        )
        SELECT wg.primary_id, w.id
        FROM works w
        JOIN work_author_summary was ON w.id = was.work_id
        JOIN WorkGroups wg
          ON w.full_title_normalized = wg.full_title_normalized
         AND was.author_string = wg.author_string
        WHERE w.id != wg.primary_id
        LIMIT p_batch_size;

        SET v_rows_affected = FOUND_ROWS();

        IF v_rows_affected > 0 THEN
            CALL sp_run_pending_work_merges();
        END IF;
    END WHILE;

    DROP TEMPORARY TABLE IF EXISTS temp_work_merge_pairs;
END //
DELIMITER ;


-- ============================================================
-- §4  RUN: execute full dedup
-- ============================================================

SELECT '=== Work deduplication: starting ===' AS status;
SELECT COUNT(*) AS works_before FROM works;

CALL sp_deduplicate_works_advanced(5000);

SELECT COUNT(*) AS works_after FROM works;
SELECT '=== Work deduplication: completed ===' AS status;
