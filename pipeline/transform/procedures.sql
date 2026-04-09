-- =============================================================================
-- Stored procedures called by transform scripts.
--
-- Callers:
--   delete_from_works.sql  : sp_clean_inconsistent_data, sp_clean_orphaned_data, sp_clean_orphaned_persons,
--                            sp_review_reference_consistency
--   persons_full_process.sql: sp_merge_persons_in_batches, sp_generate_person_signatures, sp_fix_person_name_parts
--   metrics.py             : sp_update_persons_summary, sp_update_10yr_impact_factors,
--                            sp_populate_sphinx_venues_summary, sp_update_work_author_summary_all,
--                            sp_update_work_subjects_summary_all, sp_update_works_summary
--   (sp_run_full_recalculation is obsolete — preserved for reference only)
--
-- Run: mariadb data < pipeline/transform/procedures.sql
-- =============================================================================

DELIMITER $$

-- ===========================================================================
-- Called by: delete_from_works.sql
-- ===========================================================================

DROP PROCEDURE IF EXISTS `sp_clean_inconsistent_data`$$
CREATE PROCEDURE `sp_clean_inconsistent_data`()
BEGIN
    
    DELETE a FROM authorships a
    LEFT JOIN works w ON a.work_id = w.id
    LEFT JOIN persons p ON a.person_id = p.id
    WHERE w.id IS NULL OR p.id IS NULL;
    
    
    DELETE pub FROM publications pub
    LEFT JOIN works w ON pub.work_id = w.id
    WHERE w.id IS NULL;
    
    
    DELETE c FROM courses c
    LEFT JOIN programs p ON c.program_id = p.id
    WHERE p.id IS NULL;
    
    SELECT CONCAT('Dados inconsistentes removidos: ', ROW_COUNT()) AS result;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_clean_orphaned_data`$$
CREATE PROCEDURE `sp_clean_orphaned_data`()
BEGIN
    DELETE was FROM work_author_summary was LEFT JOIN works w ON was.work_id = w.id WHERE w.id IS NULL;
    DELETE a FROM authorships a LEFT JOIN works w ON a.work_id = w.id LEFT JOIN persons p ON a.person_id = p.id WHERE w.id IS NULL OR p.id IS NULL;
    SELECT 'Limpeza de dados órfãos concluída.' AS result;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_clean_orphaned_persons`$$
CREATE PROCEDURE `sp_clean_orphaned_persons`()
BEGIN
    
    
    DELETE p FROM persons p
    LEFT JOIN authorships a ON p.id = a.person_id
    WHERE a.work_id IS NULL;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_review_reference_consistency`$$
CREATE PROCEDURE `sp_review_reference_consistency`(IN p_batch_size INT)
BEGIN
    DECLARE v_rows_affected INT DEFAULT 1;

    -- Pass 1: unresolved references (doi no longer exists) -> PENDING
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

    -- Pass 2: resolvable references -> RESOLVED
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
END ;;$$


-- ===========================================================================
-- Called by: persons_full_process.sql
-- ===========================================================================

DROP PROCEDURE IF EXISTS `sp_merge_persons_in_batches`$$
CREATE PROCEDURE `sp_merge_persons_in_batches`()
BEGIN
    DECLARE v_batch_size INT DEFAULT 1000;
    DECLARE v_rows_affected INT;
    DECLARE v_total_processed INT DEFAULT 0;
    DECLARE v_continue BOOLEAN DEFAULT TRUE;

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_batch (
        primary_person_id INT,
        secondary_person_id INT,
        PRIMARY KEY (secondary_person_id)
    );

    CREATE TEMPORARY TABLE IF NOT EXISTS temp_keys_to_transfer (
        primary_person_id INT,
        secondary_person_id INT,
        orcid VARCHAR(20),
        scopus_id VARCHAR(50),
        lattes_id VARCHAR(20),
        signature_id INT UNSIGNED,
        PRIMARY KEY (secondary_person_id)
    );

    REPEAT
        TRUNCATE TABLE temp_batch;
        TRUNCATE TABLE temp_keys_to_transfer;

        INSERT INTO temp_batch (primary_person_id, secondary_person_id)
        SELECT primary_person_id, secondary_person_id
        FROM temp_person_merge_pairs
        LIMIT v_batch_size;

        SET v_rows_affected = ROW_COUNT();
        SET v_continue = (v_rows_affected > 0);
        SET v_total_processed = v_total_processed + v_rows_affected;

        IF v_continue THEN
            START TRANSACTION;

            INSERT INTO temp_keys_to_transfer (primary_person_id, secondary_person_id, orcid, scopus_id, lattes_id, signature_id)
            SELECT
                p_primary.id,
                p_secondary.id,
                CASE WHEN p_primary.orcid IS NULL THEN p_secondary.orcid ELSE NULL END,
                CASE WHEN p_primary.scopus_id IS NULL THEN p_secondary.scopus_id ELSE NULL END,
                CASE WHEN p_primary.lattes_id IS NULL THEN p_secondary.lattes_id ELSE NULL END,
                CASE WHEN p_primary.signature_id IS NULL THEN p_secondary.signature_id ELSE NULL END
            FROM persons p_primary
            JOIN temp_batch t ON p_primary.id = t.primary_person_id
            JOIN persons p_secondary ON p_secondary.id = t.secondary_person_id;

            UPDATE persons p
            JOIN temp_keys_to_transfer temp ON p.id = temp.secondary_person_id
            SET p.orcid = NULL, p.scopus_id = NULL, p.lattes_id = NULL;

            UPDATE persons p
            JOIN temp_keys_to_transfer temp ON p.id = temp.primary_person_id
            SET
                p.orcid = COALESCE(p.orcid, temp.orcid),
                p.scopus_id = COALESCE(p.scopus_id, temp.scopus_id),
                p.lattes_id = COALESCE(p.lattes_id, temp.lattes_id),
                p.signature_id = COALESCE(p.signature_id, temp.signature_id),
                p.is_verified = GREATEST(p.is_verified, (SELECT is_verified FROM persons WHERE id = temp.secondary_person_id));

            UPDATE IGNORE authorships a JOIN temp_batch t ON a.person_id = t.secondary_person_id SET a.person_id = t.primary_person_id;
            UPDATE IGNORE course_instructors ci JOIN temp_batch t ON ci.person_id = t.secondary_person_id SET ci.person_id = t.primary_person_id;
            UPDATE IGNORE course_instructors ci JOIN temp_batch t ON ci.canonical_person_id = t.secondary_person_id SET ci.canonical_person_id = t.primary_person_id;
            UPDATE IGNORE person_match_log pml JOIN temp_batch t ON pml.matched_person_id = t.secondary_person_id SET pml.matched_person_id = t.primary_person_id;
            UPDATE IGNORE work_author_summary was JOIN temp_batch t ON was.first_author_id = t.secondary_person_id SET was.first_author_id = t.primary_person_id;

            DELETE FROM persons WHERE id IN (SELECT secondary_person_id FROM temp_batch);
            DELETE FROM temp_person_merge_pairs WHERE secondary_person_id IN (SELECT secondary_person_id FROM temp_batch);

            COMMIT;
        END IF;

    UNTIL NOT v_continue END REPEAT;

    DROP TEMPORARY TABLE IF EXISTS temp_batch;
    DROP TEMPORARY TABLE IF EXISTS temp_keys_to_transfer;
    
    SELECT CONCAT('Merge concluído. Total processado: ', v_total_processed) AS status;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_generate_person_signatures`$$
CREATE PROCEDURE `sp_generate_person_signatures`(
    IN p_person_id      INT,
    IN p_force_rebuild  TINYINT
)
BEGIN

    DECLARE v_id             INT;
    DECLARE v_preferred_name VARCHAR(255);
    DECLARE v_clean          VARCHAR(500);
    DECLARE v_last_name      VARCHAR(255);
    DECLARE v_given_upper    VARCHAR(500);
    DECLARE v_initials       VARCHAR(255);
    DECLARE v_particle       VARCHAR(255);
    DECLARE v_signature      VARCHAR(255);
    DECLARE v_done           INT DEFAULT 0;
    DECLARE v_rows_staged    INT DEFAULT 0;
    DECLARE v_rows_linked    INT DEFAULT 0;
    DECLARE v_started_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    DECLARE cur CURSOR FOR
        SELECT id, preferred_name
        FROM persons
        WHERE preferred_name IS NOT NULL
          AND TRIM(preferred_name) != ''
          AND (p_person_id IS NULL OR id = p_person_id)
          AND (p_force_rebuild = 1 OR signature_id IS NULL);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO processing_log (entity_type, entity_id, action, status, error_message, created_at)
        VALUES ('PERSON', COALESCE(p_person_id, 0), 'sp_generate_person_signatures', 'FAILED',
                CONCAT('Exception at ', NOW()), NOW());
        RESIGNAL;
    END;

    TRUNCATE TABLE staging_person_signatures;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_id, v_preferred_name;
        IF v_done THEN
            LEAVE read_loop;
        END IF;

        SET v_clean     = NULL;
        SET v_last_name = NULL;
        SET v_particle  = NULL;
        SET v_signature = NULL;

        
        SET v_clean = TRIM(REGEXP_REPLACE(
            UPPER(v_preferred_name),
            '[.,()"\'‐−–—/\\[\\]{}#@!?;:°ªº&~`^_+=|<>]',
            ' '
        ));
        SET v_clean = REGEXP_REPLACE(v_clean, '([^[:alpha:]])-', '\\1 ');
        SET v_clean = REGEXP_REPLACE(v_clean, '-([^[:alpha:]])', ' \\1');
        SET v_clean = REGEXP_REPLACE(v_clean, '^-', '');
        SET v_clean = REGEXP_REPLACE(v_clean, '-$', '');
        SET v_clean = TRIM(REGEXP_REPLACE(v_clean, '\\s+', ' '));

        
        SET v_clean = TRIM(REGEXP_REPLACE(
            v_clean,
            '\\s+(NETO|NETA|SOBRINHO|SOBRINHA|BISNETO|BISNETA|TERCEIRO|TERCEIRA|SR|SRA|II|III|IV|V|VI|VII|VIII|IX|X)$',
            ''
        ));
        SET v_clean = TRIM(REGEXP_REPLACE(
            v_clean,
            '\\s+(NETO|NETA|SOBRINHO|SOBRINHA|BISNETO|BISNETA|TERCEIRO|TERCEIRA|SR|SRA|II|III|IV|V|VI|VII|VIII|IX|X)$',
            ''
        ));

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
            
            SET v_particle = NULL;

            SET v_particle = REGEXP_SUBSTR(
                v_clean,
                '\\b(VAN DER|VAN DEN|VAN DE|VON DER|VON DEM|DE LA|DE LOS|DE LAS|DE LO)\\s+[^\\s]+$'
            );

            
            IF v_particle IS NULL OR v_particle = '' THEN
                SET v_particle = REGEXP_SUBSTR(
                    v_clean,
                    '\\b(DO|DA|DOS|DAS|DE|DEL|DELA|DELLA|DELLE|DELLO|DEGLI|DI|DU|DES|VAN|VON|AL|EL|LA|LE|LES|LO|LOS|LAS|SAINT|SAINTE|SAN|SANTA|SANTO|MC|MAC|BEN|BIN|IBN|AB|AP|AUF|ZU|ZUM|ZUR|TER|TEN)\\s+[^\\s]+$'
                );
            END IF;

            IF v_particle IS NOT NULL AND v_particle != '' THEN
                SET v_last_name = v_particle;
            ELSE
                SET v_last_name = REGEXP_SUBSTR(v_clean, '[^\\s]+$');
            END IF;

            IF v_last_name IS NULL OR v_last_name = '' THEN
                ITERATE read_loop;
            END IF;

            
            SET v_given_upper = TRIM(SUBSTRING(
                v_clean, 1,
                CHAR_LENGTH(v_clean) - CHAR_LENGTH(v_last_name)
            ));

            IF v_given_upper != '' AND v_given_upper IS NOT NULL THEN
                SET v_initials = TRIM(REGEXP_REPLACE(
                    v_given_upper,
                    '\\b([[:alpha:]])[^\\s]*\\s*',
                    '\\1 '
                ));
                SET v_signature = CONCAT(v_last_name, ' ', v_initials);
            ELSE
                SET v_signature = v_last_name;
            END IF;
        END IF;

        IF v_signature IS NOT NULL AND TRIM(v_signature) != '' THEN
            INSERT IGNORE INTO staging_person_signatures (person_id, signature_string)
            VALUES (v_id, TRIM(v_signature));
            SET v_rows_staged = v_rows_staged + ROW_COUNT();
        END IF;

    END LOOP;

    CLOSE cur;

    START TRANSACTION;

    INSERT IGNORE INTO signatures (signature)
    SELECT DISTINCT signature_string
    FROM staging_person_signatures;

    UPDATE persons p
    INNER JOIN staging_person_signatures st ON st.person_id = p.id
    INNER JOIN signatures s                 ON s.signature  = st.signature_string
    SET p.signature_id = s.id;

    SET v_rows_linked = ROW_COUNT();

    COMMIT;

    INSERT INTO processing_log (entity_type, entity_id, action, status, error_message, created_at)
    VALUES (
        'PERSON',
        COALESCE(p_person_id, 0),
        'sp_generate_person_signatures',
        'SUCCESS',
        CONCAT('staged=', v_rows_staged, ' linked=', v_rows_linked,
               ' elapsed=', TIMESTAMPDIFF(SECOND, v_started_at, NOW()), 's'),
        NOW()
    );

END ;;$$

DROP PROCEDURE IF EXISTS `sp_fix_person_name_parts`$$
CREATE PROCEDURE `sp_fix_person_name_parts`(
    IN p_person_id   INT,
    IN p_force       TINYINT
)
BEGIN

    DECLARE v_id             INT;
    DECLARE v_preferred_name VARCHAR(255);
    DECLARE v_clean          VARCHAR(500);
    DECLARE v_last_name      VARCHAR(255);
    DECLARE v_particle       VARCHAR(255);
    DECLARE v_new_family     VARCHAR(255);
    DECLARE v_new_given      VARCHAR(255);
    DECLARE v_pref_trimmed   VARCHAR(255);
    DECLARE v_pref_upper     VARCHAR(255);
    DECLARE v_family_pos     INT;
    DECLARE v_done           INT DEFAULT 0;
    DECLARE v_rows_updated   INT DEFAULT 0;
    DECLARE v_started_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

    DECLARE cur CURSOR FOR
        SELECT id, preferred_name
        FROM persons
        WHERE preferred_name IS NOT NULL
          AND TRIM(preferred_name) != ''
          AND (p_person_id IS NULL OR id = p_person_id)
          AND (
              p_force = 1
              OR family_name IS NULL
              OR TRIM(COALESCE(family_name, '')) = ''
          );

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO processing_log (entity_type, entity_id, action, status, error_message, created_at)
        VALUES ('PERSON', COALESCE(p_person_id, 0), 'sp_fix_person_name_parts', 'FAILED',
                CONCAT('Exception at ', NOW()), NOW());
        RESIGNAL;
    END;

    OPEN cur;

    read_loop: LOOP
        FETCH cur INTO v_id, v_preferred_name;
        IF v_done THEN
            LEAVE read_loop;
        END IF;

        SET v_pref_trimmed = TRIM(v_preferred_name);
        SET v_pref_upper   = UPPER(v_pref_trimmed);

        
        SET v_clean = TRIM(REGEXP_REPLACE(
            v_pref_upper,
            '[.,()"\'‐−–—/\\[\\]{}#@!?;:°ªº&~`^_+=|<>]',
            ' '
        ));
        SET v_clean = REGEXP_REPLACE(v_clean, '([^[:alpha:]])-', '\\1 ');
        SET v_clean = REGEXP_REPLACE(v_clean, '-([^[:alpha:]])', ' \\1');
        SET v_clean = REGEXP_REPLACE(v_clean, '^-', '');
        SET v_clean = REGEXP_REPLACE(v_clean, '-$', '');
        SET v_clean = TRIM(REGEXP_REPLACE(v_clean, '\\s+', ' '));

        
        SET v_clean = TRIM(REGEXP_REPLACE(
            v_clean,
            '\\s+(NETO|NETA|SOBRINHO|SOBRINHA|BISNETO|BISNETA|TERCEIRO|TERCEIRA|SR|SRA|II|III|IV|V|VI|VII|VIII|IX|X)$',
            ''
        ));
        SET v_clean = TRIM(REGEXP_REPLACE(
            v_clean,
            '\\s+(NETO|NETA|SOBRINHO|SOBRINHA|BISNETO|BISNETA|TERCEIRO|TERCEIRA|SR|SRA|II|III|IV|V|VI|VII|VIII|IX|X)$',
            ''
        ));

        IF v_clean IS NULL OR v_clean = '' THEN
            ITERATE read_loop;
        END IF;

        IF LOCATE(' ', v_clean) = 0 THEN
            IF CHAR_LENGTH(v_clean) > 1 THEN
                UPDATE persons
                SET family_name = v_pref_trimmed,
                    given_names = NULL
                WHERE id = v_id;
                SET v_rows_updated = v_rows_updated + ROW_COUNT();
            END IF;
            ITERATE read_loop;
        END IF;

        
        SET v_particle = NULL;

        SET v_particle = REGEXP_SUBSTR(
            v_clean,
            '\\b(VAN DER|VAN DEN|VAN DE|VON DER|VON DEM|DE LA|DE LOS|DE LAS|DE LO)\\s+[^\\s]+$'
        );

        
        IF v_particle IS NULL OR v_particle = '' THEN
            SET v_particle = REGEXP_SUBSTR(
                v_clean,
                '\\b(DO|DA|DOS|DAS|DE|DEL|DELA|DELLA|DELLE|DELLO|DEGLI|DI|DU|DES|VAN|VON|AL|EL|LA|LE|LES|LO|LOS|LAS|SAINT|SAINTE|SAN|SANTA|SANTO|MC|MAC|BEN|BIN|IBN|AB|AP|AUF|ZU|ZUM|ZUR|TER|TEN)\\s+[^\\s]+$'
            );
        END IF;

        IF v_particle IS NOT NULL AND v_particle != '' THEN
            SET v_last_name = v_particle;
        ELSE
            SET v_last_name = REGEXP_SUBSTR(v_clean, '[^\\s]+$');
        END IF;

        IF v_last_name IS NULL OR v_last_name = '' THEN
            ITERATE read_loop;
        END IF;

        
        SET v_family_pos = 0;

        BEGIN
            DECLARE v_search_from INT DEFAULT 1;
            DECLARE v_found       INT DEFAULT 0;
            DECLARE v_candidate   INT;

            search_loop: LOOP
                SET v_candidate = LOCATE(v_last_name, v_pref_upper, v_search_from);
                IF v_candidate = 0 THEN
                    LEAVE search_loop;
                END IF;
                IF v_candidate + CHAR_LENGTH(v_last_name) - 1 = CHAR_LENGTH(v_pref_upper) THEN
                    IF v_candidate = 1 OR SUBSTRING(v_pref_upper, v_candidate - 1, 1) = ' ' THEN
                        SET v_found = v_candidate;
                    END IF;
                END IF;
                SET v_search_from = v_candidate + 1;
                IF v_search_from > CHAR_LENGTH(v_pref_upper) THEN
                    LEAVE search_loop;
                END IF;
            END LOOP;

            SET v_family_pos = v_found;
        END;

        IF v_family_pos = 0 THEN
            SET v_family_pos = CHAR_LENGTH(v_pref_trimmed) - CHAR_LENGTH(v_last_name) + 1;
        END IF;

        
        SET v_new_family = TRIM(SUBSTRING(v_pref_trimmed, v_family_pos));

        IF v_family_pos > 1 THEN
            SET v_new_given = TRIM(SUBSTRING(v_pref_trimmed, 1, v_family_pos - 1));
            IF v_new_given = '' THEN
                SET v_new_given = NULL;
            END IF;
        ELSE
            SET v_new_given = NULL;
        END IF;

        UPDATE persons
        SET family_name = v_new_family,
            given_names = v_new_given
        WHERE id = v_id
          AND (
              COALESCE(family_name, '') != COALESCE(v_new_family, '')
              OR COALESCE(given_names, '') != COALESCE(v_new_given, '')
          );

        SET v_rows_updated = v_rows_updated + ROW_COUNT();

    END LOOP;

    CLOSE cur;

    INSERT INTO processing_log (entity_type, entity_id, action, status, error_message, created_at)
    VALUES (
        'PERSON',
        COALESCE(p_person_id, 0),
        'sp_fix_person_name_parts',
        'SUCCESS',
        CONCAT('updated=', v_rows_updated,
               ' elapsed=', TIMESTAMPDIFF(SECOND, v_started_at, NOW()), 's'),
        NOW()
    );

END ;;$$

DROP PROCEDURE IF EXISTS `sp_run_full_recalculation`$$
CREATE PROCEDURE `sp_run_full_recalculation`()
BEGIN
    
    SET FOREIGN_KEY_CHECKS = 0;

    
    UPDATE persons p
    JOIN (
        SELECT 
            a.person_id,
            COUNT(DISTINCT a.work_id) as total_works,
            SUM(w.citation_count) as total_citations,
            SUM(CASE WHEN a.is_corresponding = 1 THEN 1 ELSE 0 END) as corresponding_count,
            MIN(pub.year) as first_year,
            MAX(pub.year) as last_year
        FROM authorships a
        JOIN works w ON a.work_id = w.id
        LEFT JOIN publications pub ON a.work_id = pub.work_id
        GROUP BY a.person_id
    ) stats ON p.id = stats.person_id
    SET 
        p.total_works = COALESCE(stats.total_works, 0),
        p.total_citations = COALESCE(stats.total_citations, 0),
        p.corresponding_author_count = COALESCE(stats.corresponding_count, 0),
        p.first_publication_year = stats.first_year,
        p.latest_publication_year = stats.last_year;

    
    UPDATE organizations o
    JOIN (
        SELECT 
            a.affiliation_id,
            COUNT(DISTINCT a.person_id) as researcher_count,
            COUNT(DISTINCT a.work_id) as publication_count,
            SUM(w.citation_count) as total_citations,
            SUM(CASE WHEN p.open_access = 1 THEN 1 ELSE 0 END) as open_access_count
        FROM authorships a
        JOIN works w ON a.work_id = w.id
        LEFT JOIN publications p ON a.work_id = p.work_id
        GROUP BY a.affiliation_id
    ) stats ON o.id = stats.affiliation_id
    SET 
        o.publication_count = COALESCE(stats.publication_count, 0),
        o.researcher_count = COALESCE(stats.researcher_count, 0),
        o.total_citations = COALESCE(stats.total_citations, 0),
        o.open_access_works_count = COALESCE(stats.open_access_count, 0);

    
    UPDATE venues v
    JOIN (
        SELECT 
            p.venue_id,
            COUNT(DISTINCT w.id) as works_count,
            SUM(w.citation_count) as cited_by_count,
            MIN(p.year) as start_year,
            MAX(p.year) as end_year
        FROM works w
        JOIN publications p ON w.id = p.work_id
        WHERE p.venue_id IS NOT NULL
        GROUP BY p.venue_id
    ) stats ON v.id = stats.venue_id
    SET 
        v.works_count = COALESCE(stats.works_count, 0),
        v.cited_by_count = COALESCE(stats.cited_by_count, 0),
        v.coverage_start_year = stats.start_year,
        v.coverage_end_year = stats.end_year;

    SET FOREIGN_KEY_CHECKS = 1;
END ;;$$


-- ===========================================================================
-- Called by: metrics.py
-- ===========================================================================

DROP PROCEDURE IF EXISTS `sp_update_persons_summary`$$
CREATE PROCEDURE `sp_update_persons_summary`()
BEGIN
    INSERT INTO sphinx_persons_summary 
        (id, search_content, preferred_name, is_verified, total_works, latest_publication_year)
    SELECT
        p.id,
        CONCAT_WS(' ', p.preferred_name, s.signature) AS search_content,
        p.preferred_name,
        p.is_verified,
        p.total_works,
        p.latest_publication_year
    FROM persons p
    LEFT JOIN signatures s ON p.signature_id = s.id
    ON DUPLICATE KEY UPDATE
        search_content = VALUES(search_content),
        preferred_name = VALUES(preferred_name),
        is_verified = VALUES(is_verified),
        total_works = VALUES(total_works),
        latest_publication_year = VALUES(latest_publication_year);
END ;;$$

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
END ;;$$

DROP PROCEDURE IF EXISTS `sp_populate_sphinx_venues_summary`$$
CREATE PROCEDURE `sp_populate_sphinx_venues_summary`()
BEGIN
    SET SESSION group_concat_max_len = 1000000;
    TRUNCATE TABLE `sphinx_venues_summary`;

    CREATE TEMPORARY TABLE tmp_venue_oa
    SELECT venue_id, (SUM(open_access=1)*100.0/COUNT(*)) as oa_avg
    FROM publications 
    WHERE venue_id IS NOT NULL
    GROUP BY venue_id;

    CREATE TEMPORARY TABLE tmp_venue_subjects
    SELECT venue_id, GROUP_CONCAT(term ORDER BY score DESC SEPARATOR '; ') as subjects_str
    FROM (
        SELECT vs.venue_id, s.term, vs.score,
               ROW_NUMBER() OVER(PARTITION BY vs.venue_id ORDER BY vs.score DESC) as rn
        FROM venue_subjects vs
        JOIN subjects s ON vs.subject_id = s.id
    ) ranked
    WHERE rn <= 20
    GROUP BY venue_id;

    CREATE TEMPORARY TABLE tmp_venue_works
    SELECT venue_id, GROUP_CONCAT(title ORDER BY citation_count DESC SEPARATOR '"; "') as works_str
    FROM (
        SELECT p.venue_id, w.title, w.citation_count,
               ROW_NUMBER() OVER(PARTITION BY p.venue_id ORDER BY w.citation_count DESC) as rn
        FROM publications p
        JOIN works w ON p.work_id = w.id
        WHERE p.venue_id IS NOT NULL
    ) ranked
    WHERE rn <= 5
    GROUP BY venue_id;

    ALTER TABLE tmp_venue_oa ADD PRIMARY KEY (venue_id);
    ALTER TABLE tmp_venue_subjects ADD PRIMARY KEY (venue_id);
    ALTER TABLE tmp_venue_works ADD PRIMARY KEY (venue_id);

    INSERT INTO `sphinx_venues_summary` (
        id, name, abbreviated_name, type, publisher_name, country_code, issn, eissn, 
        subjects_string, top_works_string, works_count, cited_by_count, 
        impact_factor, h_index, open_access_percentage
    )
    SELECT
        v.id, v.name, v.abbreviated_name, v.type, o.name, v.country_code, v.issn, v.eissn,
        vsa.subjects_str, vwa.works_str, 
        COALESCE(v.works_count, 0), COALESCE(v.cited_by_count, 0),
        v.impact_factor, v.h_index, COALESCE(voa.oa_avg, 0.00)
    FROM venues v
    LEFT JOIN organizations o ON v.publisher_id = o.id
    LEFT JOIN tmp_venue_oa voa ON v.id = voa.venue_id
    LEFT JOIN tmp_venue_subjects vsa ON v.id = vsa.venue_id
    LEFT JOIN tmp_venue_works vwa ON v.id = vwa.venue_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_venue_oa;
    DROP TEMPORARY TABLE IF EXISTS tmp_venue_subjects;
    DROP TEMPORARY TABLE IF EXISTS tmp_venue_works;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_update_work_author_summary_all`$$
CREATE PROCEDURE `sp_update_work_author_summary_all`()
BEGIN
    SET SESSION group_concat_max_len = 1000000;
    
    TRUNCATE TABLE work_author_summary;
    
    INSERT INTO work_author_summary (work_id, author_string, first_author_id)
    SELECT 
        a.work_id,
        GROUP_CONCAT(p.preferred_name ORDER BY a.position ASC SEPARATOR '; '),
        CAST(SUBSTRING_INDEX(GROUP_CONCAT(a.person_id ORDER BY a.position ASC), ',', 1) AS UNSIGNED)
    FROM authorships a
    JOIN persons p ON a.person_id = p.id
    WHERE a.role = 'AUTHOR'
    GROUP BY a.work_id;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_update_work_subjects_summary_all`$$
CREATE PROCEDURE `sp_update_work_subjects_summary_all`()
BEGIN
    TRUNCATE TABLE work_subjects_summary;

    INSERT INTO work_subjects_summary (work_id, subjects_string)
    SELECT
        ws.work_id,
        GROUP_CONCAT(s.term ORDER BY s.term SEPARATOR '; ')
    FROM
        work_subjects ws
    JOIN
        subjects s ON ws.subject_id = s.id
    INNER JOIN 
        works w ON ws.work_id = w.id
    GROUP BY
        ws.work_id;
END ;;$$

DROP PROCEDURE IF EXISTS `sp_update_works_summary`$$
CREATE PROCEDURE `sp_update_works_summary`()
BEGIN
    INSERT INTO sphinx_works_summary (
        id, title, subtitle, abstract, author_string, venue_name, venue_abbrev,
        first_author_name, publisher_name, doi, publication_id, venue_id, publisher_id,
        first_author_id, author_count, institutions_count, citation_count, reference_count,
        resolved_references_count, pending_references_count, cited_by_count,
        has_pending_references, has_files, created_ts, `year`, work_type, `language`,
        open_access, peer_reviewed, subjects_string
    )
    WITH LatestPublication AS (
        SELECT
            p.id as publication_id, p.work_id, p.doi, p.`year`, p.open_access, p.peer_reviewed, p.venue_id, p.publisher_id,
            ROW_NUMBER() OVER(PARTITION BY p.work_id ORDER BY p.`year` DESC, p.id DESC) as rn
        FROM publications p
    ),
    AuthorStats AS (
        SELECT
            a.work_id,
            COUNT(DISTINCT a.person_id) as author_count,
            COUNT(DISTINCT a.affiliation_id) as institutions_count
        FROM authorships a
        WHERE a.role = 'AUTHOR'
        GROUP BY a.work_id
    ),
    ReferenceStats AS (
        SELECT
            citing_work_id,
            COUNT(*) as reference_count,
            SUM(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) as resolved_references_count,
            SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending_references_count,
            MAX(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as has_pending_references
        FROM work_references
        GROUP BY citing_work_id
    ),
    FileStats AS (
        SELECT work_id, MAX(1) as has_files
        FROM files
        GROUP BY work_id
    )
    SELECT
        w.id, w.title, w.subtitle, w.abstract, was.author_string, v.name AS venue_name, v.abbreviated_name AS venue_abbrev,
        p_first.preferred_name AS first_author_name, org.name AS publisher_name, lp.doi, lp.publication_id, lp.venue_id, lp.publisher_id,
        was.first_author_id, COALESCE(ast.author_count, 0), COALESCE(ast.institutions_count, 0), w.citation_count, COALESCE(rs.reference_count, 0),
        COALESCE(rs.resolved_references_count, 0), COALESCE(rs.pending_references_count, 0), w.citation_count AS cited_by_count,
        COALESCE(rs.has_pending_references, 0), COALESCE(fs.has_files, 0), UNIX_TIMESTAMP(w.created_at) AS created_ts, lp.`year`, w.work_type, w.language,
        lp.open_access, lp.peer_reviewed, wss.subjects_string
    FROM works w
    LEFT JOIN work_author_summary was ON was.work_id = w.id
    LEFT JOIN persons p_first ON p_first.id = was.first_author_id
    LEFT JOIN LatestPublication lp ON lp.work_id = w.id AND lp.rn = 1
    LEFT JOIN venues v ON v.id = lp.venue_id
    LEFT JOIN organizations org ON org.id = lp.publisher_id
    LEFT JOIN AuthorStats ast ON ast.work_id = w.id
    LEFT JOIN ReferenceStats rs ON rs.citing_work_id = w.id
    LEFT JOIN FileStats fs ON fs.work_id = w.id
    LEFT JOIN work_subjects_summary wss ON wss.work_id = w.id
    ON DUPLICATE KEY UPDATE
        title = VALUES(title), subtitle = VALUES(subtitle), abstract = VALUES(abstract), author_string = VALUES(author_string),
        venue_name = VALUES(venue_name), venue_abbrev = VALUES(venue_abbrev), first_author_name = VALUES(first_author_name),
        publisher_name = VALUES(publisher_name), doi = VALUES(doi), publication_id = VALUES(publication_id), venue_id = VALUES(venue_id),
        publisher_id = VALUES(publisher_id), first_author_id = VALUES(first_author_id), author_count = VALUES(author_count),
        institutions_count = VALUES(institutions_count), citation_count = VALUES(citation_count), reference_count = VALUES(reference_count),
        resolved_references_count = VALUES(resolved_references_count), pending_references_count = VALUES(pending_references_count),
        cited_by_count = VALUES(cited_by_count), has_pending_references = VALUES(has_pending_references), has_files = VALUES(has_files),
        `year` = VALUES(`year`), work_type = VALUES(work_type), `language` = VALUES(`language`), open_access = VALUES(open_access),
        peer_reviewed = VALUES(peer_reviewed), subjects_string = VALUES(subjects_string);
END ;;$$


DELIMITER ;
