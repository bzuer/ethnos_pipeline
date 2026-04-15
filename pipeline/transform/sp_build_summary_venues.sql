DROP PROCEDURE IF EXISTS `sp_build_summary_venues`;;
CREATE PROCEDURE `sp_build_summary_venues`()
BEGIN
    SET SESSION group_concat_max_len = 1000000;

    DROP TEMPORARY TABLE IF EXISTS tmp_venue_subjects;
    CREATE TEMPORARY TABLE tmp_venue_subjects (
        venue_id INT PRIMARY KEY,
        top_subjects_json LONGTEXT
    ) ENGINE=InnoDB;

    INSERT INTO tmp_venue_subjects (venue_id, top_subjects_json)
    SELECT venue_id, JSON_ARRAYAGG(JSON_OBJECT('id', subject_id, 'term', term, 'score', score))
    FROM (
        SELECT vs.venue_id, s.id AS subject_id, s.term, vs.score,
               ROW_NUMBER() OVER(PARTITION BY vs.venue_id ORDER BY vs.score DESC) as rn
        FROM venue_subjects vs
        JOIN subjects s ON vs.subject_id = s.id
    ) ranked
    WHERE rn <= 10
    GROUP BY venue_id;

    TRUNCATE TABLE summary_venues;

    INSERT INTO summary_venues (
        venue_id, publisher_id, name_search, abbrev_search, publisher_search,
        venue_type, country_code, issn, eissn, scopus_id, open_access_status,
        total_publications_count, total_cited_by_count, coverage_start_year, coverage_end_year,
        global_ranking_score, impact_factor, citescore, h_index, top_subjects_json
    )
    SELECT 
        v.id, v.publisher_id, v.name, v.abbreviated_name, o.name,
        v.type, v.country_code, v.issn, v.eissn, v.scopus_id, v.open_access,
        v.works_count, v.cited_by_count, v.coverage_start_year, v.coverage_end_year,
        v.total_score, v.impact_factor, v.citescore, v.h_index, tvs.top_subjects_json
    FROM venues v
    LEFT JOIN organizations o ON v.publisher_id = o.id
    LEFT JOIN tmp_venue_subjects tvs ON v.id = tvs.venue_id;

    DROP TEMPORARY TABLE IF EXISTS tmp_venue_subjects;
END;;

-- Atualizando o Orquestrador
DROP PROCEDURE IF EXISTS `sp_orchestrate_all_summaries`;;
CREATE PROCEDURE `sp_orchestrate_all_summaries`(IN p_batch_size INT)
BEGIN
    CALL sp_build_summary_publications(p_batch_size);
    CALL sp_build_summary_venues();
    CALL sp_build_summary_persons(p_batch_size);
END;;

DELIMITER ;
