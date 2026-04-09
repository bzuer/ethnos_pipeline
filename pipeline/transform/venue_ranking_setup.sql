-- =============================================================================
--  venue_ranking_setup.sql
--  Venue relevance ranking for anthropology/sociology database.
--
--  Scoring components (all stored in venues table):
--    subject_score     — Scopus SubjectArea MAX or OA Topic weighted avg (0-10)
--    snip_score        — LOG2(SNIP + 1) bibliometric impact (0-~4)
--    oa_score          — open access bonus (0 or 0.2)
--    authorship_score  — fraction of prolific authors (h_index >= 5) (0-2)
--    affiliation_score — fraction of authors from core institutions (0-2)
--    citation_score    — fraction of outgoing refs to core venues (0-2)
--    llm_score         — LLM-assessed relevance for anthropology (0-5)
--    total_score       — sum of all components
--
--  Run: mariadb data < pipeline/transform/venue_ranking_setup.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- §0  Table: subject_relevance_tiers
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS subject_relevance_tiers;

CREATE TABLE subject_relevance_tiers (
    subject_id  INT          NOT NULL,
    vocabulary  VARCHAR(50)  NOT NULL COMMENT 'Scopus | OpenAlex',
    tier        CHAR(1)      NOT NULL COMMENT 'A=core, B=strong, C=related, D=peripheral, E=generic',
    weight      DECIMAL(5,2) NOT NULL,
    PRIMARY KEY (subject_id),
    KEY idx_tier (tier),
    KEY idx_vocabulary (vocabulary),
    CONSTRAINT fk_srt_subject FOREIGN KEY (subject_id) REFERENCES subjects (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;


-- -----------------------------------------------------------------------------
-- §1  Populate: Scopus SubjectAreas (52 subjects)
-- -----------------------------------------------------------------------------

-- Tier A — core anthropology/sociology (weight 10)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'A', 10.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN ('Anthropology', 'Sociology and Political Science');

-- Tier B — strong affinity (weight 8)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'B', 8.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN (
    'Cultural Studies',
    'History',
    'Gender Studies',
    'Demography',
    'Political Science and International Relations',
    'Archeology',
    'Archeology (arts and humanities'
);

-- Tier C — related humanities / social science (weight 5)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'C', 5.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN (
    'Education',
    'Literature and Literary Theory',
    'Law',
    'Religious Studies',
    'Communication',
    'Philosophy',
    'Geography, Planning and Development',
    'Linguistics and Language',
    'Language and Linguistics',
    'Visual Arts and Performing Arts',
    'Urban Studies',
    'Social Sciences (miscellaneous)',
    'Social Sciences (all',
    'Public Administration',
    'Development',
    'Arts and Humanities (miscellaneous)',
    'Arts and Humanities (all',
    'Music',
    'Classics',
    'Museology',
    'Industrial Relations'
);

-- Tier D — peripheral (weight 2)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'D', 2.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN (
    'Economics and Econometrics',
    'Psychiatry and Mental Health',
    'Social Psychology',
    'Clinical Psychology',
    'Applied Psychology',
    'Developmental and Educational Psychology',
    'Experimental and Cognitive Psychology',
    'Psychology (all',
    'Psychology (miscellaneous)',
    'Health (social science',
    'Health Policy',
    'History and Philosophy of Science',
    'Library and Information Sciences',
    'Architecture',
    'Tourism, Leisure and Hospitality Management',
    'Media Technology',
    'Public Health, Environmental and Occupational Health',
    'Life-span and Life-course Studies',
    'Geriatrics and Gerontology',
    'Human Factors and Ergonomics',
    'Cognitive Neuroscience'
);

-- Tier E — generic catch-all (weight 0.5)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'E', 0.50 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN ('Multidisciplinary');


-- -----------------------------------------------------------------------------
-- §2  Populate: OpenAlex Subfields (17 subjects)
--     OA Topics inherit their parent Subfield's weight.
-- -----------------------------------------------------------------------------

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'A', 10.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term IN ('Sociology and Political Science');

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'B', 8.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term IN ('Political Science and International Relations', 'Archeology');

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'C', 5.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term IN (
    'Literature and Literary Theory', 'Linguistics and Language',
    'Geography, Planning and Development', 'Visual Arts and Performing Arts',
    'Education', 'Development'
);

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'D', 2.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term IN (
    'Economics and Econometrics', 'Developmental and Educational Psychology',
    'Experimental and Cognitive Psychology', 'Psychiatry and Mental health',
    'Public Health, Environmental and Occupational Health',
    'Ecology, Evolution, Behavior and Systematics',
    'Complementary and alternative medicine', 'Speech and Hearing'
);

SELECT vocabulary, tier, weight, COUNT(*) AS subjects
FROM subject_relevance_tiers
GROUP BY vocabulary, tier, weight
ORDER BY vocabulary, tier;


-- -----------------------------------------------------------------------------
-- §3  sp_refresh_rule_map — populate cache_rule_subject_map
-- -----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_refresh_rule_map;

DELIMITER //
CREATE PROCEDURE sp_refresh_rule_map()
BEGIN
    TRUNCATE TABLE cache_rule_subject_map;

    -- Scopus SubjectAreas: direct mapping
    INSERT INTO cache_rule_subject_map (rule_id, subject_id)
    SELECT srt.subject_id, srt.subject_id
    FROM subject_relevance_tiers srt
    WHERE srt.vocabulary = 'Scopus';

    -- OA Topics → parent Subfield
    INSERT IGNORE INTO cache_rule_subject_map (rule_id, subject_id)
    SELECT srt.subject_id, s.id
    FROM subject_relevance_tiers srt
    JOIN subjects s ON s.parent_id = srt.subject_id
    WHERE srt.vocabulary = 'OpenAlex'
      AND s.vocabulary = 'OpenAlex'
      AND s.subject_type = 'Topic';

    -- Subfields themselves (some venues have direct Subfield links)
    INSERT IGNORE INTO cache_rule_subject_map (rule_id, subject_id)
    SELECT srt.subject_id, srt.subject_id
    FROM subject_relevance_tiers srt
    WHERE srt.vocabulary = 'OpenAlex';

    SELECT CONCAT('Rule map refreshed. Entries: ', ROW_COUNT()) AS status;
END //
DELIMITER ;

CALL sp_refresh_rule_map();


-- -----------------------------------------------------------------------------
-- §4  sp_calculate_venue_ranking — full multi-component scoring
--
--  Components:
--    subject_score (0-10)     — topic relevance (dominant signal)
--    snip_score (0-~4)        — bibliometric impact
--    oa_score (0 or 0.2)      — open access bonus
--    authorship_score (0-2)   — prolific author concentration
--    affiliation_score (0-2)  — core institution affiliation
--    citation_score (0-2)     — citation network linkage to core venues
--    llm_score (0-5)          — LLM-assessed relevance for anthropology
-- -----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_calculate_venue_ranking;

DELIMITER //
CREATE PROCEDURE sp_calculate_venue_ranking()
BEGIN
    DECLARE scored_count INT DEFAULT 0;

    -- =================================================================
    -- Reset all scores
    -- =================================================================
    UPDATE venues SET
        subject_score     = 0.000,
        snip_score        = 0.000,
        oa_score          = 0.000,
        authorship_score  = 0.000,
        affiliation_score = 0.000,
        citation_score    = 0.000,
        llm_score         = 0.000,
        total_score       = 0.000;

    -- =================================================================
    -- SUBJECT SCORE (0-10)
    -- =================================================================

    -- Step 1a: Scopus-based — MAX(weight)
    UPDATE venues v
    INNER JOIN (
        SELECT vs.venue_id AS id, MAX(srt.weight) AS sub_score
        FROM venue_subjects vs
        INNER JOIN subject_relevance_tiers srt
            ON vs.subject_id = srt.subject_id AND srt.vocabulary = 'Scopus'
        GROUP BY vs.venue_id
    ) scopus ON v.id = scopus.id
    SET v.subject_score = scopus.sub_score;

    -- Step 1b: OA Topics weighted average — complement or standalone
    UPDATE venues v
    INNER JOIN (
        SELECT vs.venue_id AS id,
               SUM(COALESCE(srt.weight, 0)) / COUNT(*) AS sub_score
        FROM venue_subjects vs
        INNER JOIN subjects s ON vs.subject_id = s.id
        LEFT JOIN subject_relevance_tiers srt
            ON s.parent_id = srt.subject_id AND srt.vocabulary = 'OpenAlex'
        WHERE s.vocabulary = 'OpenAlex' AND s.subject_type = 'Topic'
        GROUP BY vs.venue_id
    ) oa ON v.id = oa.id
    SET v.subject_score = GREATEST(v.subject_score, oa.sub_score)
    WHERE oa.sub_score > v.subject_score;

    -- =================================================================
    -- SNIP SCORE (0-~4)
    -- =================================================================
    UPDATE venues
    SET snip_score = LOG2(COALESCE(snip, 0) + 1)
    WHERE subject_score > 0;

    -- =================================================================
    -- OA SCORE (0 or 0.2)
    -- =================================================================
    UPDATE venues
    SET oa_score = CASE WHEN COALESCE(open_access, 0) = 1 THEN 0.2 ELSE 0.0 END
    WHERE subject_score > 0;

    -- =================================================================
    -- AUTHORSHIP SCORE (0-2)
    -- Fraction of venue's authors with h_index >= 5, scaled to 0-2.
    -- Formula: MIN(prolific_pct / 10, 2.0)
    --   27% prolific → 2.0 (cap)
    --   10% prolific → 1.0
    --   1% prolific  → 0.1
    -- =================================================================
    UPDATE venues v
    INNER JOIN (
        SELECT p.venue_id AS id,
               LEAST(
                   COUNT(DISTINCT CASE WHEN pe.h_index >= 5 THEN a.person_id END)
                   * 100.0
                   / NULLIF(COUNT(DISTINCT a.person_id), 0)
                   / 10.0,
                   2.0
               ) AS a_score
        FROM publications p
        INNER JOIN authorships a ON a.work_id = p.work_id
        INNER JOIN persons pe ON a.person_id = pe.id
        WHERE p.venue_id IS NOT NULL
        GROUP BY p.venue_id
    ) auth ON v.id = auth.id
    SET v.authorship_score = ROUND(auth.a_score, 3)
    WHERE v.subject_score > 0 AND auth.a_score > 0;

    -- =================================================================
    -- AFFILIATION SCORE (0-2)
    -- Fraction of authors from core institutions (top 500 orgs by author
    -- count in subject_score >= 8 venues), scaled to 0-2.
    -- Formula: MIN(core_inst_pct / 15, 2.0)
    --   30%+ → 2.0 (cap)
    --   15%  → 1.0
    --   3%   → 0.2
    --
    -- Uses a temp table for the core institution list.
    -- =================================================================
    DROP TEMPORARY TABLE IF EXISTS _tmp_core_institutions;
    CREATE TEMPORARY TABLE _tmp_core_institutions (
        org_id INT NOT NULL PRIMARY KEY
    );

    INSERT INTO _tmp_core_institutions (org_id)
    SELECT a.affiliation_id
    FROM authorships a
    INNER JOIN publications p ON a.work_id = p.work_id
    INNER JOIN venues v ON p.venue_id = v.id
    WHERE v.subject_score >= 8 AND a.affiliation_id IS NOT NULL
    GROUP BY a.affiliation_id
    ORDER BY COUNT(DISTINCT a.person_id) DESC
    LIMIT 500;

    UPDATE venues v
    INNER JOIN (
        SELECT p.venue_id AS id,
               LEAST(
                   COUNT(DISTINCT CASE WHEN ci.org_id IS NOT NULL THEN a.person_id END)
                   * 100.0
                   / NULLIF(COUNT(DISTINCT a.person_id), 0)
                   / 15.0,
                   2.0
               ) AS af_score
        FROM publications p
        INNER JOIN authorships a ON a.work_id = p.work_id
        LEFT JOIN _tmp_core_institutions ci ON a.affiliation_id = ci.org_id
        WHERE p.venue_id IS NOT NULL
        GROUP BY p.venue_id
    ) aff ON v.id = aff.id
    SET v.affiliation_score = ROUND(aff.af_score, 3)
    WHERE v.subject_score > 0 AND aff.af_score > 0;

    DROP TEMPORARY TABLE IF EXISTS _tmp_core_institutions;

    -- =================================================================
    -- CITATION SCORE (0-2)
    -- Fraction of resolved outgoing references that cite venues with
    -- subject_score >= 5 (core + related), scaled to 0-2.
    -- Formula: MIN(core_ref_pct / 40, 2.0)
    --   80%+ → 2.0 (cap)
    --   40%  → 1.0
    --   10%  → 0.25
    -- =================================================================
    UPDATE venues v
    INNER JOIN (
        SELECT p_src.venue_id AS id,
               LEAST(
                   COUNT(CASE WHEN v_cited.subject_score >= 5 THEN 1 END)
                   * 100.0
                   / NULLIF(COUNT(*), 0)
                   / 40.0,
                   2.0
               ) AS c_score
        FROM work_references wr
        INNER JOIN publications p_src ON wr.citing_work_id = p_src.work_id
        INNER JOIN publications p_tgt ON wr.cited_work_id = p_tgt.work_id
        LEFT JOIN venues v_cited ON p_tgt.venue_id = v_cited.id
        WHERE p_src.venue_id IS NOT NULL
          AND wr.cited_work_id IS NOT NULL
        GROUP BY p_src.venue_id
    ) cit ON v.id = cit.id
    SET v.citation_score = ROUND(cit.c_score, 3)
    WHERE v.subject_score > 0 AND cit.c_score > 0;

    -- =================================================================
    -- LLM SCORE (0-5)
    -- Direct mapping from llm_relevance (0=none … 5=core anthropology).
    -- NULL → 0 (not evaluated, no bonus, no penalty).
    -- =================================================================
    UPDATE venues
    SET llm_score = COALESCE(llm_relevance, 0)
    WHERE subject_score > 0;

    -- =================================================================
    -- TOTAL SCORE
    -- =================================================================
    UPDATE venues
    SET total_score = subject_score + snip_score + oa_score
                    + authorship_score + affiliation_score + citation_score
                    + llm_score
    WHERE subject_score > 0;

    SET scored_count = (SELECT COUNT(*) FROM venues WHERE subject_score > 0);
    SELECT CONCAT('Ranking updated. Venues scored: ', scored_count) AS status;
END //
DELIMITER ;


-- -----------------------------------------------------------------------------
-- §5  v_venue_ranking_final — view with all score components
-- -----------------------------------------------------------------------------

DROP VIEW IF EXISTS v_venue_ranking_final;

CREATE VIEW v_venue_ranking_final AS
SELECT
    RANK() OVER (ORDER BY v.total_score DESC, v.snip_score DESC) AS rank_position,
    v.id             AS venue_id,
    v.name           AS venue_name,
    v.type           AS type,
    v.total_score,
    v.subject_score  AS pts_tematicos,
    v.snip_score     AS pts_impacto,
    v.oa_score       AS pts_open_access,
    v.authorship_score  AS pts_autoria,
    v.affiliation_score AS pts_afiliacao,
    v.citation_score    AS pts_citacao,
    v.llm_score         AS pts_llm,
    (
        SELECT GROUP_CONCAT(DISTINCT CONCAT(srt.tier, ':', s.term) ORDER BY srt.weight DESC SEPARATOR ', ')
        FROM venue_subjects vsub
        JOIN subjects s ON vsub.subject_id = s.id
        JOIN subject_relevance_tiers srt ON s.id = srt.subject_id
        WHERE vsub.venue_id = v.id
        LIMIT 1
    ) AS matched_scopus,
    (
        SELECT GROUP_CONCAT(DISTINCT CONCAT(srt.tier, ':', p.term) ORDER BY srt.weight DESC SEPARATOR ', ')
        FROM venue_subjects vsub
        JOIN subjects s ON vsub.subject_id = s.id
        JOIN subjects p ON s.parent_id = p.id
        JOIN subject_relevance_tiers srt ON p.id = srt.subject_id AND srt.vocabulary = 'OpenAlex'
        WHERE vsub.venue_id = v.id
          AND s.vocabulary = 'OpenAlex' AND s.subject_type = 'Topic'
        LIMIT 1
    ) AS matched_subfields
FROM venues v
WHERE v.subject_score > 0
ORDER BY v.total_score DESC;


-- -----------------------------------------------------------------------------
-- §6  Run the ranking
-- -----------------------------------------------------------------------------
CALL sp_calculate_venue_ranking();
