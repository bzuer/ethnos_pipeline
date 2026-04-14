-- ============================================================================
-- pipeline/transform/procedures.sql
--
-- Source-controlled copies of the procedures and functions that drive the
-- transform stage, plus a handful of repair/maintenance helpers. Every
-- definition here is a verbatim dump of the live DB routine (definer clause
-- stripped for portability). Run with:
--
--   mariadb data < pipeline/transform/procedures.sql
--
-- The four summary / ranking procedures live in their own files:
--   sp_build_summary_publications.sql
--   sp_build_summary_venues.sql
--   sp_build_summary_persons.sql
--   venue_ranking_setup.sql (sp_calculate_venue_ranking + tier seed)
-- ============================================================================

DELIMITER $$

-- ---- sp_clean_html_entities ---------------------------------------------------------
-- Phase 1 — HTML entity cleanup on works/orgs/venues/persons text.
DROP PROCEDURE IF EXISTS `sp_clean_html_entities`$$
CREATE PROCEDURE `sp_clean_html_entities`()
BEGIN
    DECLARE total_cleaned INT DEFAULT 0;
    
    START TRANSACTION;
    
    
    UPDATE IGNORE works SET title = TRIM(REPLACE(REPLACE(title, '&amp;', '&'), '&nbsp;', ' ')) WHERE title REGEXP '&[a-zA-Z0-9#]+;';
    SET total_cleaned = total_cleaned + ROW_COUNT();
    
    UPDATE IGNORE works SET abstract = TRIM(REPLACE(REPLACE(abstract, '&amp;', '&'), '&nbsp;', ' ')) WHERE abstract REGEXP '&[a-zA-Z0-9#]+;' AND abstract IS NOT NULL;
    SET total_cleaned = total_cleaned + ROW_COUNT();
    
    UPDATE IGNORE organizations SET name = TRIM(REPLACE(REPLACE(name, '&amp;', '&'), '&nbsp;', ' ')) WHERE name REGEXP '&[a-zA-Z0-9#]+;';
    SET total_cleaned = total_cleaned + ROW_COUNT();
    
    UPDATE IGNORE venues SET name = TRIM(REPLACE(REPLACE(name, '&amp;', '&'), '&nbsp;', ' ')) WHERE name REGEXP '&[a-zA-Z0-9#]+;';
    SET total_cleaned = total_cleaned + ROW_COUNT();
    
    UPDATE IGNORE persons SET preferred_name = TRIM(REPLACE(REPLACE(preferred_name, '&amp;', '&'), '&nbsp;', ' ')) WHERE preferred_name REGEXP '&[a-zA-Z0-9#]+;' AND preferred_name IS NOT NULL;
    SET total_cleaned = total_cleaned + ROW_COUNT();
    
    COMMIT;
    
    SELECT CONCAT('HTML entities cleaned (com IGNORE): ', total_cleaned, ' records afetados') as result;
END $$

-- ---- sp_normalize_publications_data ---------------------------------------------------------
-- Phase 1 — normalize publication metadata, DOIs, etc.
DROP PROCEDURE IF EXISTS `sp_normalize_publications_data`$$
CREATE PROCEDURE `sp_normalize_publications_data`()
BEGIN
    
    
    UPDATE publications
    SET 
        pmid = CASE WHEN LOWER(pmid) IN ('none', 'null', 'nan', '') THEN NULL ELSE pmid END,
        pmcid = CASE WHEN LOWER(pmcid) IN ('none', 'null', 'nan', '') THEN NULL ELSE pmcid END,
        isbn = CASE WHEN LOWER(isbn) IN ('none', 'null', 'nan', '') THEN NULL ELSE isbn END,
        asin = CASE WHEN LOWER(asin) IN ('none', 'null', 'nan', '') THEN NULL ELSE asin END,
        udc = CASE WHEN LOWER(udc) IN ('none', 'null', 'nan', '') THEN NULL ELSE udc END,
        lbc = CASE WHEN LOWER(lbc) IN ('none', 'null', 'nan', '') THEN NULL ELSE lbc END,
        ddc = CASE WHEN LOWER(ddc) IN ('none', 'null', 'nan', '') THEN NULL ELSE ddc END,
        lcc = CASE WHEN LOWER(lcc) IN ('none', 'null', 'nan', '') THEN NULL ELSE lcc END,
        google_book_id = CASE WHEN LOWER(google_book_id) IN ('none', 'null', 'nan', '') THEN NULL ELSE google_book_id END,
        volume = CASE WHEN LOWER(volume) IN ('none', 'null', 'nan', '') THEN NULL ELSE volume END,
        issue = CASE WHEN LOWER(issue) IN ('none', 'null', 'nan', '') THEN NULL ELSE issue END,
        pages = CASE WHEN LOWER(pages) IN ('none', 'null', 'nan', '') THEN NULL ELSE pages END
    WHERE 
        LOWER(pmid) IN ('none', 'null', 'nan', '') OR
        LOWER(pmcid) IN ('none', 'null', 'nan', '') OR
        LOWER(isbn) IN ('none', 'null', 'nan', '') OR
        LOWER(asin) IN ('none', 'null', 'nan', '') OR
        LOWER(udc) IN ('none', 'null', 'nan', '') OR
        LOWER(lbc) IN ('none', 'null', 'nan', '') OR
        LOWER(ddc) IN ('none', 'null', 'nan', '') OR
        LOWER(lcc) IN ('none', 'null', 'nan', '') OR
        LOWER(google_book_id) IN ('none', 'null', 'nan', '') OR
        LOWER(volume) IN ('none', 'null', 'nan', '') OR
        LOWER(issue) IN ('none', 'null', 'nan', '') OR
        LOWER(pages) IN ('none', 'null', 'nan', '');

    
    
    UPDATE publications
    SET volume = NULL
    WHERE volume LIKE '10.%/%';

    UPDATE publications
    SET issue = NULL
    WHERE issue LIKE '10.%/%';

    
    
    UPDATE publications
    SET isbn = LEFT(REPLACE(SUBSTRING_INDEX(isbn, ',', 1), '-', ''), 20)
    WHERE isbn LIKE '%-%' OR isbn LIKE '%,%';

    
    
    UPDATE publications
    SET pages = TRIM(REGEXP_REPLACE(pages, '(?i)\\s+p\\.?$|\\s+pp\\.?$', ''))
    WHERE pages REGEXP '(?i)\\s+p\\.?$|\\s+pp\\.?$';

    
    UPDATE publications
    SET pages = TRIM(pages)
    WHERE pages != TRIM(pages);

    
    SELECT 'Procedimento de normalização dos metadados concluído com sucesso.' AS status;
END $$

-- ---- sp_clean_split_compound_persons ---------------------------------------------------------
-- Phase 1 — split compound person names conservatively.
DROP PROCEDURE IF EXISTS `sp_clean_split_compound_persons`$$
CREATE PROCEDURE `sp_clean_split_compound_persons`()
BEGIN
    
    CREATE TEMPORARY TABLE temp_split_names AS
    WITH RECURSIVE name_splitter AS (
        SELECT 
            id AS original_person_id,
            TRIM(SUBSTRING_INDEX(preferred_name, ',', 1)) AS extracted_name,
            TRIM(SUBSTRING(preferred_name, LENGTH(SUBSTRING_INDEX(preferred_name, ',', 1)) + 2)) AS remainder
        FROM persons
        WHERE preferred_name LIKE '%,%'
        
        UNION ALL
        
        SELECT 
            original_person_id,
            TRIM(SUBSTRING_INDEX(remainder, ',', 1)),
            TRIM(SUBSTRING(remainder, LENGTH(SUBSTRING_INDEX(remainder, ',', 1)) + 2))
        FROM name_splitter
        WHERE remainder != ''
    )
    SELECT original_person_id, extracted_name 
    FROM name_splitter 
    WHERE extracted_name != '';

    
    INSERT IGNORE INTO persons (preferred_name)
    SELECT DISTINCT extracted_name FROM temp_split_names;

    
    INSERT IGNORE INTO authorships (work_id, person_id, role, position)
    SELECT a.work_id, p_new.id, 'AUTHOR', 99
    FROM authorships a
    JOIN temp_split_names tsn ON a.person_id = tsn.original_person_id
    JOIN persons p_new ON p_new.preferred_name = tsn.extracted_name COLLATE utf8mb4_unicode_ci;

    
    DELETE p FROM persons p
    JOIN (SELECT DISTINCT original_person_id FROM temp_split_names) del_list 
      ON p.id = del_list.original_person_id;

    DROP TEMPORARY TABLE temp_split_names;
END $$

-- ---- sp_clean_core_data ---------------------------------------------------------
-- Phase 2 — cascade-delete orphan authorships/publications/refs/persons.
DROP PROCEDURE IF EXISTS `sp_clean_core_data`$$
CREATE PROCEDURE `sp_clean_core_data`()
BEGIN
    DELETE a FROM authorships a LEFT JOIN works w ON a.work_id = w.id WHERE w.id IS NULL;
    DELETE a FROM authorships a LEFT JOIN persons p ON a.person_id = p.id WHERE p.id IS NULL;
    DELETE pub FROM publications pub LEFT JOIN works w ON pub.work_id = w.id WHERE w.id IS NULL;
    DELETE cb FROM course_bibliography cb LEFT JOIN works w ON cb.work_id = w.id WHERE w.id IS NULL;
    DELETE f FROM funding f LEFT JOIN works w ON f.work_id = w.id WHERE w.id IS NULL;
    DELETE wr FROM work_references wr LEFT JOIN works w ON wr.citing_work_id = w.id WHERE w.id IS NULL;
    
    DELETE p FROM persons p 
    LEFT JOIN authorships a ON p.id = a.person_id 
    LEFT JOIN course_instructors ci ON p.id = ci.person_id 
    WHERE a.work_id IS NULL AND ci.course_id IS NULL;
END $$

-- ---- sp_rebuild_signatures ---------------------------------------------------------
-- Phase 2 — cursor-based rebuild of persons.signature_id.
DROP PROCEDURE IF EXISTS `sp_rebuild_signatures`$$
CREATE PROCEDURE `sp_rebuild_signatures`()
BEGIN
    DECLARE v_id INT;
    DECLARE v_preferred_name VARCHAR(255);
    DECLARE v_clean VARCHAR(500);
    DECLARE v_last_name VARCHAR(255);
    DECLARE v_given_upper VARCHAR(500);
    DECLARE v_initials VARCHAR(255);
    DECLARE v_particle VARCHAR(255);
    DECLARE v_signature VARCHAR(255);
    DECLARE v_done INT DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT id, preferred_name
        FROM persons
        WHERE preferred_name IS NOT NULL
          AND TRIM(preferred_name) != '';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    DROP TEMPORARY TABLE IF EXISTS tmp_signatures;
    CREATE TEMPORARY TABLE tmp_signatures (
        person_id INT PRIMARY KEY,
        signature_string VARCHAR(255)
    ) ENGINE=InnoDB;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_id, v_preferred_name;
        IF v_done THEN
            LEAVE read_loop;
        END IF;

        SET v_clean = TRIM(REGEXP_REPLACE(UPPER(v_preferred_name), '[.,()"\'‐−–—/\\[\\]{}#@!?;:°ªº&~`^_+=|<>]', ' '));
        SET v_clean = REGEXP_REPLACE(v_clean, '([^[:alpha:]])-', '\\1 ');
        SET v_clean = REGEXP_REPLACE(v_clean, '-([^[:alpha:]])', ' \\1');
        SET v_clean = REGEXP_REPLACE(v_clean, '^-', '');
        SET v_clean = REGEXP_REPLACE(v_clean, '-$', '');
        SET v_clean = TRIM(REGEXP_REPLACE(v_clean, '\\s+', ' '));
        SET v_clean = TRIM(REGEXP_REPLACE(v_clean, '\\s+(NETO|NETA|SOBRINHO|SOBRINHA|BISNETO|BISNETA|TERCEIRO|TERCEIRA|SR|SRA|II|III|IV|V|VI|VII|VIII|IX|X)$', ''));

        IF v_clean IS NULL OR v_clean = '' THEN
            ITERATE read_loop;
        END IF;

        IF LOCATE(' ', v_clean) = 0 THEN
            IF CHAR_LENGTH(v_clean) > 1 THEN
                SET v_signature = v_clean;
            ELSE
                ITERATE read_loop;
            END IF;
        ELSE
            SET v_particle = REGEXP_SUBSTR(v_clean, '\\b(VAN DER|VAN DEN|VAN DE|VON DER|VON DEM|DE LA|DE LOS|DE LAS|DE LO)\\s+[^\\s]+$');
            
            IF v_particle IS NULL OR v_particle = '' THEN
                SET v_particle = REGEXP_SUBSTR(v_clean, '\\b(DO|DA|DOS|DAS|DE|DEL|DELA|DELLA|DELLE|DELLO|DEGLI|DI|DU|DES|VAN|VON|AL|EL|LA|LE|LES|LO|LOS|LAS|SAINT|SAINTE|SAN|SANTA|SANTO|MC|MAC|BEN|BIN|IBN|AB|AP|AUF|ZU|ZUM|ZUR|TER|TEN)\\s+[^\\s]+$');
            END IF;

            IF v_particle IS NOT NULL AND v_particle != '' THEN
                SET v_last_name = v_particle;
            ELSE
                SET v_last_name = REGEXP_SUBSTR(v_clean, '[^\\s]+$');
            END IF;

            IF v_last_name IS NULL OR v_last_name = '' THEN
                ITERATE read_loop;
            END IF;

            SET v_given_upper = TRIM(SUBSTRING(v_clean, 1, CHAR_LENGTH(v_clean) - CHAR_LENGTH(v_last_name)));

            IF v_given_upper != '' AND v_given_upper IS NOT NULL THEN
                SET v_initials = TRIM(REGEXP_REPLACE(v_given_upper, '\\b([[:alpha:]])[^\\s]*\\s*', '\\1 '));
                SET v_signature = CONCAT(v_last_name, ' ', v_initials);
            ELSE
                SET v_signature = v_last_name;
            END IF;
        END IF;

        IF v_signature IS NOT NULL AND TRIM(v_signature) != '' THEN
            INSERT INTO tmp_signatures (person_id, signature_string) VALUES (v_id, TRIM(v_signature))
            ON DUPLICATE KEY UPDATE signature_string = VALUES(signature_string);
        END IF;

    END LOOP;
    CLOSE cur;

    INSERT IGNORE INTO signatures (signature)
    SELECT DISTINCT signature_string FROM tmp_signatures;

    UPDATE persons p
    JOIN tmp_signatures ts ON p.id = ts.person_id
    JOIN signatures s ON s.signature = ts.signature_string
    SET p.signature_id = s.id;

    DROP TEMPORARY TABLE tmp_signatures;
END $$

-- ---- sp_repair_work_references_consistency ---------------------------------------------------------
-- Phase 3 — reconcile RESOLVED rows missing cited_work_id.
DROP PROCEDURE IF EXISTS `sp_repair_work_references_consistency`$$
CREATE PROCEDURE `sp_repair_work_references_consistency`(IN p_apply TINYINT)
BEGIN
    DECLARE v_apply TINYINT DEFAULT 1;
    DECLARE v_before_invalid BIGINT DEFAULT 0;
    DECLARE v_exact_matches BIGINT DEFAULT 0;
    DECLARE v_set_pending BIGINT DEFAULT 0;
    DECLARE v_after_invalid BIGINT DEFAULT 0;

    SET v_apply = IFNULL(p_apply, 1);

    SELECT COUNT(*)
      INTO v_before_invalid
      FROM work_references wr
     WHERE wr.status = 'RESOLVED'
       AND wr.cited_work_id IS NULL;

    SELECT COUNT(*)
      INTO v_exact_matches
      FROM work_references wr
      JOIN publications p ON p.doi = wr.cited_doi
     WHERE wr.status = 'RESOLVED'
       AND wr.cited_work_id IS NULL;

    IF v_apply = 1 THEN
        UPDATE work_references wr
        JOIN publications p
          ON p.doi = wr.cited_doi
        SET wr.cited_work_id = p.work_id,
            wr.resolved_at = COALESCE(wr.resolved_at, NOW())
        WHERE wr.status = 'RESOLVED'
          AND wr.cited_work_id IS NULL;

        UPDATE work_references wr
        SET wr.status = 'PENDING',
            wr.resolved_at = NULL
        WHERE wr.status = 'RESOLVED'
          AND wr.cited_work_id IS NULL;

        SET v_set_pending = ROW_COUNT();
    END IF;

    SELECT COUNT(*)
      INTO v_after_invalid
      FROM work_references wr
     WHERE wr.status = 'RESOLVED'
       AND wr.cited_work_id IS NULL;

    SELECT
        v_apply AS apply_mode,
        v_before_invalid AS invalid_before,
        v_exact_matches AS exact_matches_available,
        v_set_pending AS rows_set_to_pending,
        v_after_invalid AS invalid_after;
END $$

-- ---- sp_resolve_all_pending_existing ---------------------------------------------------------
-- Phase 3 — resolve PENDING refs whose DOI now exists.
DROP PROCEDURE IF EXISTS `sp_resolve_all_pending_existing`$$
CREATE PROCEDURE `sp_resolve_all_pending_existing`(IN p_batch_size INT)
BEGIN
    DECLARE v_rows_affected INT DEFAULT 1;
    DECLARE v_total_resolved INT DEFAULT 0;

    WHILE v_rows_affected > 0 DO
        UPDATE work_references wr
        JOIN (
            SELECT wr_inner.id, p.work_id
            FROM work_references wr_inner
            JOIN publications p ON wr_inner.cited_doi = p.doi
            WHERE wr_inner.status = 'PENDING'
            LIMIT p_batch_size
        ) batch ON wr.id = batch.id
        SET 
            wr.cited_work_id = batch.work_id,
            wr.status = 'RESOLVED',
            wr.resolved_at = CURRENT_TIMESTAMP;

        SET v_rows_affected = ROW_COUNT();
        SET v_total_resolved = v_total_resolved + v_rows_affected;
    END WHILE;

    SELECT CONCAT('Total de referências retroativas resolvidas: ', v_total_resolved) AS status;
END $$

-- ---- sp_resolve_pending_references ---------------------------------------------------------
-- Phase 3 helper — single-DOI pending resolver.
DROP PROCEDURE IF EXISTS `sp_resolve_pending_references`$$
CREATE PROCEDURE `sp_resolve_pending_references`(IN p_limit INT)
BEGIN
    
    
    
    UPDATE work_references wr
    JOIN publications p ON wr.cited_doi = p.doi
    SET 
        wr.cited_work_id = p.work_id,
        wr.status = 'RESOLVED',
        wr.resolved_at = CURRENT_TIMESTAMP
    WHERE 
        wr.status = 'PENDING' 
        AND wr.cited_doi IS NOT NULL
        AND p.doi IS NOT NULL
    LIMIT p_limit;

END $$

-- ---- sp_review_reference_consistency ---------------------------------------------------------
-- Phase 3 — iterative revert/resolve until stable.
DROP PROCEDURE IF EXISTS `sp_review_reference_consistency`$$
CREATE PROCEDURE `sp_review_reference_consistency`(IN p_batch_size INT)
BEGIN
    DECLARE v_rows_affected INT DEFAULT 1;

    
    WHILE v_rows_affected > 0 DO
        UPDATE work_references wr
        LEFT JOIN publications p ON wr.cited_doi = p.doi
        SET
            wr.status = 'PENDING',
            wr.cited_work_id = NULL,
            wr.resolved_at = NULL
        WHERE
            p.doi IS NULL
            AND (wr.status != 'PENDING' OR wr.cited_work_id IS NOT NULL)
        LIMIT p_batch_size;

        SET v_rows_affected = ROW_COUNT();
        DO SLEEP(0.1);
    END WHILE;

    SET v_rows_affected = 1;

    
    WHILE v_rows_affected > 0 DO
        UPDATE work_references wr
        INNER JOIN publications p ON wr.cited_doi = p.doi
        SET
            wr.status = 'RESOLVED',
            wr.cited_work_id = p.work_id,
            wr.resolved_at = CURRENT_TIMESTAMP
        WHERE
            wr.status = 'PENDING'
            OR wr.cited_work_id IS NULL
            OR wr.cited_work_id != p.work_id
        LIMIT p_batch_size;

        SET v_rows_affected = ROW_COUNT();
        DO SLEEP(0.1);
    END WHILE;

    SELECT 'Reference consistency review completed.' AS execution_status;
END $$

-- ---- sp_update_core_statistics ---------------------------------------------------------
-- Phase 4 — rebuild aggregates on persons/organizations/venues.
DROP PROCEDURE IF EXISTS `sp_update_core_statistics`$$
CREATE PROCEDURE `sp_update_core_statistics`()
BEGIN
    SET FOREIGN_KEY_CHECKS = 0;
    SET SESSION group_concat_max_len = 1000000;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_person_stats;
    CREATE TEMPORARY TABLE tmp_person_stats (
        person_id INT PRIMARY KEY,
        total_works INT DEFAULT 0,
        total_citations INT DEFAULT 0,
        corresponding_count INT DEFAULT 0,
        first_year SMALLINT DEFAULT NULL,
        last_year SMALLINT DEFAULT NULL,
        h_index INT DEFAULT 0
    ) ENGINE=InnoDB;

    INSERT INTO tmp_person_stats (person_id, total_works, corresponding_count, first_year, last_year)
    SELECT 
        a.person_id,
        COUNT(DISTINCT a.work_id),
        SUM(CASE WHEN a.is_corresponding = 1 THEN 1 ELSE 0 END),
        MIN(p.year),
        MAX(p.year)
    FROM authorships a
    LEFT JOIN publications p ON a.work_id = p.work_id
    GROUP BY a.person_id;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_person_hindex;
    CREATE TEMPORARY TABLE tmp_person_hindex (
        person_id INT PRIMARY KEY,
        total_citations INT DEFAULT 0,
        h_index INT DEFAULT 0
    ) ENGINE=InnoDB;

    INSERT INTO tmp_person_hindex (person_id, total_citations, h_index)
    SELECT 
        person_id,
        SUM(citations),
        MAX(CASE WHEN citations >= rn THEN rn ELSE 0 END)
    FROM (
        SELECT 
            a.person_id,
            w.citation_count AS citations,
            ROW_NUMBER() OVER(PARTITION BY a.person_id ORDER BY w.citation_count DESC) as rn
        FROM authorships a
        JOIN works w ON a.work_id = w.id
    ) ranked
    GROUP BY person_id;

    UPDATE tmp_person_stats t
    JOIN tmp_person_hindex h ON t.person_id = h.person_id
    SET t.total_citations = h.total_citations, t.h_index = h.h_index;

    UPDATE persons p JOIN tmp_person_stats t ON p.id = t.person_id
    SET p.total_works = t.total_works, p.total_citations = t.total_citations, 
        p.corresponding_author_count = t.corresponding_count, p.first_publication_year = t.first_year, 
        p.latest_publication_year = t.last_year, p.h_index = t.h_index;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_org_stats;
    CREATE TEMPORARY TABLE tmp_org_stats (
        affiliation_id INT PRIMARY KEY,
        researcher_count INT DEFAULT 0,
        publication_count INT DEFAULT 0,
        total_citations INT DEFAULT 0,
        open_access_count INT DEFAULT 0
    ) ENGINE=InnoDB;

    INSERT INTO tmp_org_stats (affiliation_id, researcher_count, publication_count, total_citations, open_access_count)
    SELECT 
        a.affiliation_id,
        COUNT(DISTINCT a.person_id),
        COUNT(DISTINCT a.work_id),
        SUM(w.citation_count),
        SUM(CASE WHEN pub.open_access = 1 THEN 1 ELSE 0 END)
    FROM authorships a
    JOIN works w ON a.work_id = w.id
    LEFT JOIN publications pub ON a.work_id = pub.work_id
    WHERE a.affiliation_id IS NOT NULL
    GROUP BY a.affiliation_id;

    UPDATE organizations o JOIN tmp_org_stats t ON o.id = t.affiliation_id
    SET o.publication_count = t.publication_count, o.researcher_count = t.researcher_count, 
        o.total_citations = t.total_citations, o.open_access_works_count = t.open_access_count;

    
    DROP TEMPORARY TABLE IF EXISTS tmp_venue_stats;
    CREATE TEMPORARY TABLE tmp_venue_stats (
        venue_id INT PRIMARY KEY,
        works_count INT DEFAULT 0,
        cited_by_count INT DEFAULT 0,
        start_year SMALLINT DEFAULT NULL,
        end_year SMALLINT DEFAULT NULL
    ) ENGINE=InnoDB;

    INSERT INTO tmp_venue_stats (venue_id, works_count, cited_by_count, start_year, end_year)
    SELECT 
        pub.venue_id,
        COUNT(DISTINCT w.id),
        SUM(w.citation_count),
        MIN(pub.year),
        MAX(pub.year)
    FROM works w
    JOIN publications pub ON w.id = pub.work_id
    WHERE pub.venue_id IS NOT NULL
    GROUP BY pub.venue_id;

    UPDATE venues v JOIN tmp_venue_stats t ON v.id = t.venue_id
    SET v.works_count = t.works_count, v.cited_by_count = t.cited_by_count, 
        v.coverage_start_year = t.start_year, v.coverage_end_year = t.end_year;

    
    DROP TEMPORARY TABLE tmp_person_stats;
    DROP TEMPORARY TABLE tmp_person_hindex;
    DROP TEMPORARY TABLE tmp_org_stats;
    DROP TEMPORARY TABLE tmp_venue_stats;
    SET FOREIGN_KEY_CHECKS = 1;
END $$

-- ---- fn_calculate_10yr_impact_factor ---------------------------------------------------------
-- Phase 5 helper — 10-year impact factor for one venue.
DROP FUNCTION IF EXISTS `fn_calculate_10yr_impact_factor`$$
CREATE FUNCTION `fn_calculate_10yr_impact_factor`(p_venue_id INT, p_target_year INT) RETURNS decimal(10,3)
    READS SQL DATA
    DETERMINISTIC
BEGIN
    DECLARE v_numerator INT DEFAULT 0;
    DECLARE v_denominator INT DEFAULT 0;
    DECLARE v_result DECIMAL(10,3) DEFAULT 0.000;

    SELECT COUNT(*) INTO v_denominator
    FROM publications p
    JOIN works w ON p.work_id = w.id
    WHERE p.venue_id = p_venue_id
      AND p.year BETWEEN (p_target_year - 10) AND (p_target_year - 1)
      AND w.work_type IN ('ARTICLE', 'CONFERENCE', 'CHAPTER', 'BOOK');

    IF v_denominator = 0 THEN RETURN 0.000; END IF;

    SELECT COUNT(*) INTO v_numerator
    FROM work_references wr
    JOIN publications citing_pub ON wr.citing_work_id = citing_pub.work_id 
    JOIN publications cited_pub ON wr.cited_work_id = cited_pub.work_id    
    WHERE cited_pub.venue_id = p_venue_id
      AND wr.status = 'RESOLVED'
      AND citing_pub.year = p_target_year 
      AND cited_pub.year BETWEEN (p_target_year - 10) AND (p_target_year - 1);

    SET v_result = v_numerator / v_denominator;
    RETURN v_result;
END $$

-- ---- sp_update_10yr_impact_factors ---------------------------------------------------------
-- Phase 5 — bulk impact_factor update via fn above.
DROP PROCEDURE IF EXISTS `sp_update_10yr_impact_factors`$$
CREATE PROCEDURE `sp_update_10yr_impact_factors`()
BEGIN
    DECLARE v_reference_year INT;
    
    
    SET v_reference_year = YEAR(CURDATE()) - 1;

    
    UPDATE venues v
    SET 
        impact_factor = fn_calculate_10yr_impact_factor(v.id, v_reference_year),
        updated_at = NOW()
    WHERE 
        
        EXISTS (
            SELECT 1 
            FROM publications p 
            WHERE p.venue_id = v.id 
              AND p.year BETWEEN (v_reference_year - 10) AND (v_reference_year - 1)
        );

    SELECT 
        ROW_COUNT() as venues_updated, 
        v_reference_year as calculation_year,
        'Success (10-year window)' as status;
END $$

-- ---- sp_orchestrate_all_summaries ---------------------------------------------------------
-- Phase 6 — alt single-entry summary rebuild.
DROP PROCEDURE IF EXISTS `sp_orchestrate_all_summaries`$$
CREATE PROCEDURE `sp_orchestrate_all_summaries`(IN p_batch_size INT)
BEGIN
    CALL sp_build_summary_publications(p_batch_size);
    CALL sp_build_summary_venues();
    CALL sp_build_summary_persons(p_batch_size);
END $$

-- ---- sp_reindex_database ---------------------------------------------------------
-- Phase 7 — refresh InnoDB table statistics.
DROP PROCEDURE IF EXISTS `sp_reindex_database`$$
CREATE PROCEDURE `sp_reindex_database`()
BEGIN
    
    SET FOREIGN_KEY_CHECKS = 0;
    
    
    ANALYZE TABLE works;
    ANALYZE TABLE persons;
    ANALYZE TABLE organizations;
    ANALYZE TABLE publications;
    ANALYZE TABLE authorships;
    
    
    SET FOREIGN_KEY_CHECKS = 1;
    
    SELECT 'Reindexação concluída' AS status;
END $$

-- ---- sp_disable_all_triggers ---------------------------------------------------------
-- Maintenance — disable data triggers for bulk repairs.
DROP PROCEDURE IF EXISTS `sp_disable_all_triggers`$$
CREATE PROCEDURE `sp_disable_all_triggers`()
BEGIN
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_trigger_definitions (
        trigger_name VARCHAR(64),
        event_manipulation VARCHAR(6),
        event_object_table VARCHAR(64),
        action_timing VARCHAR(6),
        sql_mode TEXT,
        definer TEXT,
        action_statement LONGTEXT
    );
    TRUNCATE TABLE temp_trigger_definitions;

    INSERT INTO temp_trigger_definitions
    SELECT 
        TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE,
        ACTION_TIMING, SQL_MODE, DEFINER, ACTION_STATEMENT
    FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA = DATABASE();

    BLOCK1: BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE v_trigger_name VARCHAR(64);
        DECLARE cur CURSOR FOR SELECT trigger_name FROM temp_trigger_definitions;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        OPEN cur;
        read_loop: LOOP
            FETCH cur INTO v_trigger_name;
            IF done THEN LEAVE read_loop; END IF;
            SET @drop_sql = CONCAT('DROP TRIGGER IF EXISTS `', v_trigger_name, '`');
            PREPARE stmt FROM @drop_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END LOOP;
        CLOSE cur;
    END BLOCK1;
    
END $$

-- ---- sp_enable_all_triggers ---------------------------------------------------------
-- Maintenance — re-enable data triggers after bulk repairs.
DROP PROCEDURE IF EXISTS `sp_enable_all_triggers`$$
CREATE PROCEDURE `sp_enable_all_triggers`()
BEGIN
    BLOCK2: BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE v_trigger_name, v_event, v_table_name, v_timing, v_definer, v_sql_mode TEXT;
        DECLARE v_action LONGTEXT;
        DECLARE cur CURSOR FOR SELECT trigger_name, event_manipulation, event_object_table, action_timing, definer, sql_mode, action_statement FROM temp_trigger_definitions;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        OPEN cur;
        read_loop: LOOP
            FETCH cur INTO v_trigger_name, v_event, v_table_name, v_timing, v_definer, v_sql_mode, v_action;
            IF done THEN LEAVE read_loop; END IF;
            SET @create_sql = CONCAT(
                'CREATE DEFINER=', v_definer,
                ' TRIGGER `', v_trigger_name, '` ',
                v_timing, ' ', v_event,
                ' ON `', v_table_name, '` FOR EACH ROW ',
                v_action
            );
            PREPARE stmt FROM @create_sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END LOOP;
        CLOSE cur;
    END BLOCK2;
    DROP TEMPORARY TABLE IF EXISTS temp_trigger_definitions;
    
END $$

-- ---- sp_merge_single_organization_pair ---------------------------------------------------------
-- Maintenance — merge two organization rows.
DROP PROCEDURE IF EXISTS `sp_merge_single_organization_pair`$$
CREATE PROCEDURE `sp_merge_single_organization_pair`(
    IN p_primary_org_id INT,    
    IN p_secondary_org_id INT   
)
BEGIN
    
    DECLARE v_primary_exists INT DEFAULT 0;
    DECLARE v_secondary_exists INT DEFAULT 0;

    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        
        RESIGNAL;
    END;

    
    IF p_primary_org_id IS NULL OR p_secondary_org_id IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Both primary and secondary IDs must be provided.';
    END IF;

    IF p_primary_org_id = p_secondary_org_id THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Primary and secondary IDs cannot be the same.';
    END IF;

    
    SELECT COUNT(*) INTO v_primary_exists FROM organizations WHERE id = p_primary_org_id;
    SELECT COUNT(*) INTO v_secondary_exists FROM organizations WHERE id = p_secondary_org_id;

    IF v_primary_exists = 0 OR v_secondary_exists = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'One or both organization IDs do not exist.';
    END IF;

    
    START TRANSACTION;

    
    UPDATE organizations o_primary
    JOIN organizations o_secondary ON o_secondary.id = p_secondary_org_id
    SET
        o_primary.ror_id = COALESCE(o_primary.ror_id, o_secondary.ror_id),
        o_primary.wikidata_id = COALESCE(o_primary.wikidata_id, o_secondary.wikidata_id),
        o_primary.openalex_id = COALESCE(o_primary.openalex_id, o_secondary.openalex_id),
        o_primary.mag_id = COALESCE(o_primary.mag_id, o_secondary.mag_id),
        o_primary.url = COALESCE(o_primary.url, o_secondary.url),
        o_primary.updated_at = NOW()
    WHERE o_primary.id = p_primary_org_id;

    
    UPDATE IGNORE authorships SET affiliation_id = p_primary_org_id WHERE affiliation_id = p_secondary_org_id;
    UPDATE IGNORE funding SET funder_id = p_primary_org_id WHERE funder_id = p_secondary_org_id;
    UPDATE IGNORE programs SET institution_id = p_primary_org_id WHERE institution_id = p_secondary_org_id;
    UPDATE IGNORE publications SET publisher_id = p_primary_org_id WHERE publisher_id = p_secondary_org_id;
    UPDATE IGNORE venues SET publisher_id = p_primary_org_id WHERE publisher_id = p_secondary_org_id;

    
    DELETE FROM organizations WHERE id = p_secondary_org_id;

    COMMIT;

    
    SELECT CONCAT('Successfully merged organization ID ', p_secondary_org_id, ' into ID ', p_primary_org_id) as Result;

END $$

-- ---- sp_fix_merged_work ---------------------------------------------------------
-- Maintenance — repair authorships/refs after a work merge.
DROP PROCEDURE IF EXISTS `sp_fix_merged_work`$$
CREATE PROCEDURE `sp_fix_merged_work`(IN p_work_id INT)
BEGIN
    DECLARE v_publication_id_to_move INT;
    DECLARE v_first_publication_id INT;
    DECLARE v_new_work_id INT;
    DECLARE v_unmerged_count INT DEFAULT 0;
    DECLARE v_done INT DEFAULT FALSE;

    DECLARE cur_publications_to_move CURSOR FOR
        SELECT id
        FROM publications
        WHERE work_id = p_work_id AND id != v_first_publication_id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

    START TRANSACTION;

    SELECT MIN(id) INTO v_first_publication_id
    FROM publications
    WHERE work_id = p_work_id;

    OPEN cur_publications_to_move;

    move_loop: LOOP
        FETCH cur_publications_to_move INTO v_publication_id_to_move;
        IF v_done THEN
            LEAVE move_loop;
        END IF;

        
        INSERT INTO works (title, subtitle, abstract, work_type, language, reference_count)
        SELECT title, subtitle, abstract, work_type, language, reference_count
        FROM works WHERE id = p_work_id;

        SET v_new_work_id = LAST_INSERT_ID();

        
        UPDATE publications SET work_id = v_new_work_id WHERE id = v_publication_id_to_move;
        UPDATE files SET work_id = v_new_work_id WHERE publication_id = v_publication_id_to_move;

        
        INSERT INTO authorships (work_id, person_id, affiliation_id, role, `position`, is_corresponding)
        SELECT v_new_work_id, person_id, affiliation_id, role, `position`, is_corresponding
        FROM authorships
        WHERE work_id = p_work_id;

        INSERT INTO funding (work_id, funder_id, grant_number, program_name, amount, currency)
        SELECT v_new_work_id, funder_id, grant_number, program_name, amount, currency
        FROM funding
        WHERE work_id = p_work_id;

        INSERT INTO work_subjects (work_id, subject_id, relevance_score, assigned_by)
        SELECT v_new_work_id, subject_id, relevance_score, assigned_by
        FROM work_subjects
        WHERE work_id = p_work_id;

        INSERT INTO course_bibliography (course_id, work_id, reading_type, week_number, notes)
        SELECT course_id, v_new_work_id, reading_type, week_number, notes
        FROM course_bibliography
        WHERE work_id = p_work_id;

        SET v_unmerged_count = v_unmerged_count + 1;

    END LOOP move_loop;
    CLOSE cur_publications_to_move;

    COMMIT;
    
    IF v_unmerged_count > 0 THEN
       SELECT CONCAT('Sucesso para work_id ', p_work_id, ': ', v_unmerged_count, ' publicações desmembradas com relacionamentos duplicados por segurança.') AS status;
    END IF;

END $$

-- ---- sp_fix_family_name ---------------------------------------------------------
-- Maintenance — rewrite family_name across persons/authorships.
DROP PROCEDURE IF EXISTS `sp_fix_family_name`$$
CREATE PROCEDURE `sp_fix_family_name`(
    IN p_wrong  VARCHAR(255),
    IN p_right  VARCHAR(255)
)
BEGIN
    DECLARE v_affected INT DEFAULT 0;

    
    UPDATE IGNORE persons
    SET family_name = p_right
    WHERE family_name = p_wrong;
    SET v_affected = ROW_COUNT();

    
    UPDATE IGNORE persons
    SET preferred_name = CONCAT(LEFT(preferred_name, CHAR_LENGTH(preferred_name) - CHAR_LENGTH(p_wrong)), p_right)
    WHERE preferred_name LIKE CONCAT('% ', p_wrong)
      AND family_name = p_right;

    SELECT CONCAT(p_wrong, ' → ', p_right, ': ', v_affected, ' family_name(s) corrigido(s), ', ROW_COUNT(), ' preferred_name(s) corrigido(s)') AS resultado;
END $$

-- ---- sp_fix_family_given_names ---------------------------------------------------------
-- Maintenance — patch split family/given names in batches.
DROP PROCEDURE IF EXISTS `sp_fix_family_given_names`$$
CREATE PROCEDURE `sp_fix_family_given_names`(IN p_batch_size INT)
BEGIN
    DECLARE v_max_id INT;
    DECLARE v_offset INT DEFAULT 0;

    SELECT MAX(id) INTO v_max_id FROM persons;

    WHILE v_offset <= v_max_id DO
        UPDATE persons 
        SET 
            
            family_name = CASE 
                WHEN LOCATE(' ', TRIM(preferred_name)) > 0 THEN
                    REGEXP_SUBSTR(TRIM(preferred_name), '(?i)(\\b(do|da|dos|das|de|del|della|di|du|van der|van|von der|von|al|el|la|le|saint|sainte|mc|mac|o)\\s+)*[^\\s]+(\\s+(filho|junior|neto|sobrinho|jr|sr|iii|iv|v))?$')
                ELSE 
                    TRIM(preferred_name)
            END,
            
            given_names = CASE 
                WHEN LOCATE(' ', TRIM(preferred_name)) > 0 THEN
                    NULLIF(TRIM(SUBSTRING(TRIM(preferred_name), 1, 
                        LENGTH(TRIM(preferred_name)) - LENGTH(
                            REGEXP_SUBSTR(TRIM(preferred_name), '(?i)(\\b(do|da|dos|das|de|del|della|di|du|van der|van|von der|von|al|el|la|le|saint|sainte|mc|mac|o)\\s+)*[^\\s]+(\\s+(filho|junior|neto|sobrinho|jr|sr|iii|iv|v))?$')
                        )
                    )), '')
                ELSE 
                    NULL
            END
        WHERE id BETWEEN v_offset AND v_offset + p_batch_size
          AND preferred_name IS NOT NULL;
        
        SET v_offset = v_offset + p_batch_size + 1;
    END WHILE;
END $$

DELIMITER ;
