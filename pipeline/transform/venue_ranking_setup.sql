-- =============================================================================
--  venue_ranking_setup.sql
--  Venue relevance ranking for anthropology/sociology database.
--
--  Scoring components (all stored in venues table):
--    subject_score     — positive tier weight (MAX Scopus or OA Topic avg),
--                        reduced by negative tier penalty, floored at 0 (0-10)
--    oa_score          — open access bonus (0 or 0.2)
--    authorship_score  — fraction of prolific authors (h_index >= 5) (0-2)
--    affiliation_score — fraction of authors from core institutions (0-2)
--    citation_score    — fraction of outgoing refs to core venues (0-2)
--    llm_score         — LLM-assessed relevance, doubled (0-10)
--    total_score       — sum of all components (0-26.2 theoretical max)
--
--  Tier weights (subject_relevance_tiers):
--    A  (10) core anthropology
--    B  (8)  strong affinity
--    C  (5)  related humanities / social science
--    D  (2)  peripheral
--    E  (0.5) generic catch-all
--    S  (6)  broad socio-political (Sociology and Political Science — too
--            generic for Tier A, catches all of political science)
--    N  (-)  negative / penalty for biological-anthropology markers
--
--  authorship_score and affiliation_score are dampened by a confidence
--  factor MIN(1, LN(1+N)/LN(51)) so venues with few distinct authors do
--  not inflate the ranking from small samples. The "core institutions"
--  list is built from venues with llm_relevance >= 4 (not subject_score),
--  breaking the feedback loop where broad Tier A tags auto-validated
--  political-science institutions.
--
--  Run: mariadb data < pipeline/transform/venue_ranking_setup.sql
-- =============================================================================


-- -----------------------------------------------------------------------------
-- §1  Table: subject_relevance_tiers
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS subject_relevance_tiers;

CREATE TABLE subject_relevance_tiers (
    subject_id  INT          NOT NULL,
    vocabulary  VARCHAR(50)  NOT NULL COMMENT 'Scopus | OpenAlex',
    tier        CHAR(1)      NOT NULL COMMENT 'A=core, B=strong, C=related, D=peripheral, E=generic, S=socio-political broad, N=negative penalty',
    weight      DECIMAL(5,2) NOT NULL,
    PRIMARY KEY (subject_id),
    KEY idx_tier (tier),
    KEY idx_vocabulary (vocabulary),
    CONSTRAINT fk_srt_subject FOREIGN KEY (subject_id) REFERENCES subjects (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;


-- -----------------------------------------------------------------------------
-- §2  Populate: Scopus SubjectAreas
-- -----------------------------------------------------------------------------

-- Tier A — core anthropology (weight 10). Note: Scopus "Anthropology" is a
-- single SubjectArea that groups social, cultural, biological, physical and
-- forensic anthropology together — the negative tier (§2b) compensates for
-- the biological side when it appears jointly with biological markers.
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'A', 10.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term = 'Anthropology';

-- Tier S — broad socio-political catch-all (weight 6). "Sociology and
-- Political Science" tags ~1400 venues in Scopus, only ~25% of which are
-- LLM-relevant to anthropology (it catches all of political science). It
-- would dominate subject_score if treated as Tier A.
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'S', 6.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term = 'Sociology and Political Science';

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

-- Tier N — biological / non-social penalty (Scopus). Stored as positive
-- weight; the procedure subtracts it from subject_score. These SubjectAreas
-- mark venues that mix biological anthropology with social anthropology
-- tags, inflating them to Tier A via the MAX aggregation.
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'N', 5.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term = 'Paleontology';

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'N', 3.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term IN (
    'Anatomy',
    'Genetics',
    'Animal Science and Zoology',
    'Pathology and Forensic Medicine',
    'Developmental Biology',
    'Ecology'
);

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'Scopus', 'N', 2.00 FROM subjects
WHERE vocabulary = 'Scopus' AND subject_type = 'SubjectArea'
  AND term = 'Ecology, Evolution, Behavior and Systematics';


-- -----------------------------------------------------------------------------
-- §3  Populate: OpenAlex — synthetic "Anthropology" Subfield + topic reparenting
--
--  OA taxonomy places 4 anthropology Topics under a "Anthropology" Subfield,
--  but the venue loader only creates Subfield rows when the upstream payload
--  actually includes them. Historical data left 3 of 4 Topics orphaned
--  (parent_id NULL) and 1 (Forensic Anthropology and Bioarchaeology Studies)
--  pointing to Archeology, which caused biological-anthropology venues to
--  inherit Archeology's Tier B weight.
--
--  This section creates a synthetic Subfield "Anthropology" if missing, then
--  reparents the three orphan Topics. The loader's get_or_create_subject
--  matches on (vocabulary, subject_type, term_key), so a future OA load will
--  adopt this row in place (updating external_uri) rather than duplicating.
--  Forensic Anthropology and Bioarchaeology Studies is handled separately by
--  tier N below (topic-level direct match).
-- -----------------------------------------------------------------------------

INSERT IGNORE INTO subjects (term, vocabulary, subject_type, parent_id, term_key)
VALUES ('Anthropology', 'OpenAlex', 'Subfield', NULL, 'anthropology');

SET @oa_anthro_subfield := (
    SELECT id FROM subjects
    WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
      AND term = 'Anthropology'
    LIMIT 1
);

UPDATE subjects
SET parent_id = @oa_anthro_subfield
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Topic'
  AND term IN (
    'Anthropological Studies and Insights',
    'Anthropology: Ethics, History, Culture',
    'European Linguistics and Anthropology'
  )
  AND (parent_id IS NULL OR parent_id = @oa_anthro_subfield);


-- -----------------------------------------------------------------------------
-- §3b  Populate: OpenAlex Subfield tiers
--     OA Topics inherit their parent Subfield's weight at scoring time.
-- -----------------------------------------------------------------------------

-- Tier A — core anthropology (the synthetic Anthropology Subfield from §3)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'A', 10.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term = 'Anthropology';

-- Tier S — broad socio-political (same rationale as Scopus Tier S)
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'S', 6.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Subfield'
  AND term = 'Sociology and Political Science';

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

-- Tier N — biological / non-social penalty (OA Topics, direct match).
-- OpenAlex Topics are too granular for parent-based classification, so
-- penalties are applied directly on topic IDs (unlike positive tiers,
-- which propagate via Topic→Subfield parent). Weights are lighter than
-- Scopus NEG because a single biological Topic on a broad generalist
-- journal (e.g. Current Anthropology) is weak evidence of biological
-- focus — it typically indicates editorial breadth, not dominance.
INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'N', 1.50 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Topic'
  AND term IN (
    'Forensic Anthropology and Bioarchaeology Studies',
    'Paleopathology and ancient diseases',
    'Primate Behavior and Ecology'
);

INSERT INTO subject_relevance_tiers (subject_id, vocabulary, tier, weight)
SELECT id, 'OpenAlex', 'N', 1.00 FROM subjects
WHERE vocabulary = 'OpenAlex' AND subject_type = 'Topic'
  AND term IN (
    'Pleistocene-Era Hominins and Archaeology',
    'Evolution and Paleontology Studies',
    'Paleontology and Evolutionary Biology'
);

SELECT vocabulary, tier, weight, COUNT(*) AS subjects
FROM subject_relevance_tiers
GROUP BY vocabulary, tier, weight
ORDER BY vocabulary, tier;


-- -----------------------------------------------------------------------------
-- §4  sp_calculate_venue_ranking — full multi-component scoring
--
--  Components:
--    subject_score (0-10)     — positive tier weight minus negative tier
--                               penalty, floored at 0 (dominant signal)
--    oa_score (0 or 0.2)      — open access bonus
--    authorship_score (0-2)   — prolific author concentration × confidence
--    affiliation_score (0-2)  — core institution affiliation × confidence
--    citation_score (0-2)     — citation network linkage to core venues
--    llm_score (0-10)         — LLM-assessed relevance × 2
--
--  Confidence factor = MIN(1, LN(1 + N_distinct_authors) / LN(51)) — reaches
--  full weight at ~50 authors; venues with 1-5 authors get 10-40% of raw score.
--
--  Core institutions list (for affiliation_score) is built from venues with
--  llm_relevance >= 4 rather than subject_score >= 8, breaking the feedback
--  loop where broad Tier A Scopus tags auto-validated political science
--  departments.
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

    -- Step 1b: OA Topics weighted average — complement or standalone.
    -- Only positive tiers propagate via parent Subfield; negative tiers are
    -- handled directly in Step 1c so they don't contaminate the average.
    UPDATE venues v
    INNER JOIN (
        SELECT vs.venue_id AS id,
               SUM(COALESCE(srt.weight, 0)) / COUNT(*) AS sub_score
        FROM venue_subjects vs
        INNER JOIN subjects s ON vs.subject_id = s.id
        LEFT JOIN subject_relevance_tiers srt
            ON s.parent_id = srt.subject_id AND srt.vocabulary = 'OpenAlex'
                AND srt.tier <> 'N'
        WHERE s.vocabulary = 'OpenAlex' AND s.subject_type = 'Topic'
        GROUP BY vs.venue_id
    ) oa ON v.id = oa.id
    SET v.subject_score = GREATEST(v.subject_score, oa.sub_score)
    WHERE oa.sub_score > v.subject_score;

    -- Step 1c: Apply negative tier penalty. Subtracts the sum of tier N
    -- weights across both vocabularies from subject_score, floored at 0.
    -- Matches directly on subject_id (no parent propagation), so Scopus
    -- SubjectAreas and OA Topics can both be penalised.
    UPDATE venues v
    INNER JOIN (
        SELECT vs.venue_id AS id, SUM(srt.weight) AS penalty
        FROM venue_subjects vs
        INNER JOIN subject_relevance_tiers srt
            ON vs.subject_id = srt.subject_id AND srt.tier = 'N'
        GROUP BY vs.venue_id
    ) neg ON v.id = neg.id
    SET v.subject_score = GREATEST(0, v.subject_score - neg.penalty)
    WHERE v.subject_score > 0;

    -- =================================================================
    -- OA SCORE (0 or 0.2)
    -- =================================================================
    UPDATE venues
    SET oa_score = CASE WHEN COALESCE(open_access, 0) = 1 THEN 0.2 ELSE 0.0 END
    WHERE subject_score > 0;

    -- =================================================================
    -- AUTHORSHIP SCORE (0-2)
    -- Fraction of venue's authors with h_index >= 5, scaled to 0-2,
    -- dampened by a confidence factor based on total distinct authors.
    -- Formula: MIN(prolific_pct / 10, 2.0) × MIN(1, LN(1+N)/LN(51))
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
               )
               * LEAST(
                   1.0,
                   LN(1 + COUNT(DISTINCT a.person_id)) / LN(51)
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
    -- count in venues with llm_relevance >= 4), scaled to 0-2, dampened by
    -- confidence factor. Using LLM relevance instead of subject_score
    -- breaks the feedback loop where generic "Sociology and Political
    -- Science" venues auto-validated political science departments as
    -- core institutions. Denominator excludes authors without affiliation
    -- data so venues with sparse org coverage aren't penalized.
    -- Formula: MIN(core_inst_pct / 15, 2.0) × MIN(1, LN(1+N)/LN(51))
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
    WHERE v.llm_relevance >= 4 AND a.affiliation_id IS NOT NULL
    GROUP BY a.affiliation_id
    ORDER BY COUNT(DISTINCT a.person_id) DESC
    LIMIT 500;

    UPDATE venues v
    INNER JOIN (
        SELECT p.venue_id AS id,
               LEAST(
                   COUNT(DISTINCT CASE WHEN ci.org_id IS NOT NULL THEN a.person_id END)
                   * 100.0
                   / NULLIF(COUNT(DISTINCT CASE WHEN a.affiliation_id IS NOT NULL THEN a.person_id END), 0)
                   / 15.0,
                   2.0
               )
               * LEAST(
                   1.0,
                   LN(1 + COUNT(DISTINCT a.person_id)) / LN(51)
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
    -- LLM SCORE (0-10)
    -- Mapping from llm_relevance × 2 (0=none … 5=core anthropology → 10).
    -- NULL → 0 (not evaluated, no bonus, no penalty). Applied regardless
    -- of subject_score so venues with LLM coverage but no Scopus/OA tags
    -- (e.g. curated eBooks, repositories) still get ranked.
    -- =================================================================
    UPDATE venues
    SET llm_score = COALESCE(llm_relevance, 0) * 2
    WHERE llm_relevance IS NOT NULL OR subject_score > 0;

    -- =================================================================
    -- TOTAL SCORE (0-26.2 theoretical max: 10 + 0.2 + 2 + 2 + 2 + 10)
    -- Computed whenever any component is positive, so LLM-only venues
    -- (no Scopus/OA classification) still receive a score.
    -- =================================================================
    UPDATE venues
    SET total_score = subject_score + oa_score
                    + authorship_score + affiliation_score + citation_score
                    + llm_score
    WHERE subject_score > 0 OR llm_score > 0;

    SET scored_count = (SELECT COUNT(*) FROM venues WHERE total_score > 0);
    SELECT CONCAT('Ranking updated. Venues scored: ', scored_count) AS status;
END //
DELIMITER ;


-- -----------------------------------------------------------------------------
-- §5  Run the ranking
-- -----------------------------------------------------------------------------
CALL sp_calculate_venue_ranking();
