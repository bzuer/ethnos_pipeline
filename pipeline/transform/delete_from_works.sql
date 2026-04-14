-- ============================================================
-- delete_from_works.sql — optional junk / boilerplate cleanup
--
-- NOT part of pipeline_processor.sql. Run on demand only after a back-up.
-- Target: MariaDB 11.8+ (collation uca1400_ai_ci).
--
-- Scope:
--   §0 — strip leading "Abstract:"/"Resumen:" and leading punctuation noise
--        from works.abstract/title; normalise ellipsis unicode.
--   §1 — delete works whose title is editorial boilerplate (front/back matter,
--        TOC, "Editorial", "Book Review: ...", corrigenda, etc.).
--   §1b — delete works whose subtitle matches a bibliographic review citation
--         (ARTICLE/CHAPTER only).
--   §2 — orphan cleanup for rows that sp_clean_core_data does NOT cover
--        (works without authorship, venues/organizations/subjects without any
--        references to them).
--   §3 — propagate open_access = 1 from venues / licenses / SciELO PIDs.
--
-- Redundant content removed: the "None"-literal nullification block (handled
-- by sp_normalize_publications_data) and calls to dropped procedures
-- (sp_clean_inconsistent_data / sp_clean_orphaned_data / sp_clean_orphaned_persons).
-- The former sphinx_queue orphan clean-up is gone because the table no
-- longer exists.
-- ============================================================

-- ============================================================
-- §0  ABSTRACT / TITLE PREFIX CLEANING
-- ============================================================

-- Strip leading "Abstract:", "Resumen:", "Resumo:" etc. (case-insensitive via collation)
UPDATE works
SET abstract = TRIM(REGEXP_REPLACE(
    abstract,
    '^(abstract|resumen|resumo|résumé|zusammenfassung|sommario|riassunto)[[:space:]]*:?[[:space:]]*',
    ''
))
WHERE abstract REGEXP '^(abstract|resumen|resumo|résumé|zusammenfassung|sommario|riassunto)[[:space:]]*:?';

-- Strip leading punctuation/whitespace remnants from abstract
UPDATE works
SET abstract = TRIM(REGEXP_REPLACE(abstract, '^[[:space:]:\\-–—]+', ''))
WHERE abstract REGEXP '^[[:space:]:\\-–—]+';

-- Null out abstracts that became empty
UPDATE works SET abstract = NULL
WHERE abstract IS NOT NULL AND TRIM(abstract) = '';

-- Strip leading non-alpha junk from titles
UPDATE works
SET title = TRIM(REGEXP_REPLACE(title, '^[^[:alpha:]]+', ''))
WHERE title REGEXP '^[^[:alpha:]]';

-- Unicode adjustments
UPDATE `works` 
SET 
    `title` = REPLACE(`title`, '...', '…'),
    `subtitle` = REPLACE(`subtitle`, '...', '…')
WHERE 
    `title` LIKE '%...%' 
    OR `subtitle` LIKE '%...%';


-- ============================================================
-- §1  DELETE JUNK / NON-SCHOLARLY WORKS
--
--     DESIGN PRINCIPLES:
--     - Exact match (IN list) for standalone boilerplate titles.
--     - Anchored prefix (LIKE 'x%') for editorial/admin patterns.
--     - REGEX only for structural patterns (ed./eds., pagination, ISBN, currency).
--     - NO unanchored '%keyword%' on ambiguous terms (review, index,
--       cover, contents, notes, varia, appendix, correspondence, etc.)
--       to avoid deleting legitimate research works.
-- ============================================================

DELETE FROM works
WHERE

    -- --------------------------------------------------------
    -- 1a. Empty / placeholder / garbage
    -- --------------------------------------------------------
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

    -- --------------------------------------------------------
    -- 1b. Exact standalone boilerplate titles
    -- --------------------------------------------------------
    OR LOWER(TRIM(title)) IN (
        -- English
        'publications received', 'notes', 'notes and news',
        'notes of the quarter', 'note of the quarter',
        'back numbers of the journal', 'rejoinder', 'discussion',
        'comment', 'comments', 'reply',
        'international meetings', 'annual meeting',
        -- Front/back matter
        'front matter', 'back matter', 'front cover', 'back cover',
        'cover and front matter', 'cover and back matter',
        'table of contents', 'masthead', 'impressum', 'colophon',
        -- Editorial board
        'editorial board', 'conselho editorial', 'comitê editorial',
        'equipo editorial', 'comité de rédaction', 'herausgeber',
        -- Contributors
        'about the authors', 'notes on contributors', 'notes on contributor',
        'contributors', 'contributor', 'contribuidores',
        'colaboradores', 'collaborators',
        'author biographies', 'author biography',
        'biographical note', 'biographical notes',
        'notas sobre os colaboradores', 'notas sobre los colaboradores',
        -- Misc boilerplate
        'miscellaneous', 'recent scholarship', 'recently published',
        'new publications', 'back issues',
        'books received', 'obras recebidas', 'libros recibidos',
        'new books', 'publications reçues', 'eingegangene publikationen',
        -- Events
        'call for papers', 'call for submissions',
        'conference announcement', 'conference report', 'conference program',
        'symposium announcement', 'meeting report', 'minutes of meeting',
        'agenda',
        -- Abstracts / summaries as title
        'abstract', 'abstracts', 'resumo', 'resumos',
        'resumen', 'resúmenes', 'résumé', 'résumés',
        'zusammenfassung', 'zusammenfassungen',
        -- TOC / index
        'toc', 'subject index', 'author index', 'keyword index',
        'contents', 'sumário', 'sumario', 'índice', 'indice',
        'índices', 'indices', 'contents volume', 'annual index',
        -- Correspondence (standalone)
        'correspondence',
        -- Presentation / foreword
        'preface', 'foreword', 'avant-propos',
        'apresentação', 'presentación',
        -- Issue info
        'issue information',
        -- Regional
        'expediente', 'table des illustrations',
        'the bantu treasury series', 'the vilakazi prize',
        'witwatersrand university press', 'un publications',
        -- Matéria capa
        'matéria de capa', 'matéria final',
        'materias preliminares', 'materias finales'
    )

    -- --------------------------------------------------------
    -- 1c. Anchored prefix patterns (LIKE 'x%')
    -- --------------------------------------------------------

    -- Administrative / lists
    OR title LIKE 'list of %'
    OR title LIKE 'message from %'
    OR title LIKE 'program for %'
    OR title LIKE 'surveys by %'

    -- Acknowledgments (all languages)
    OR title LIKE 'acknowledgment%'
    OR title LIKE 'acknowledgement%'
    OR title LIKE 'agradecimiento%'
    OR title LIKE 'agradecimento%'
    OR title LIKE 'remerciement%'
    OR title LIKE 'danksagung%'
    OR title LIKE 'ringraziament%'

    -- Editorials (anchored start)
    OR title LIKE 'editorial%'
    OR title LIKE 'note from the editor%'
    OR title LIKE 'from the editor%'
    OR title LIKE 'carta do editor%'
    OR title LIKE 'letter from the editor%'

    -- Announcements (anchored)
    OR title LIKE 'announcement%'
    OR title LIKE 'anúncio%'
    OR title LIKE 'anuncio%'
    OR title LIKE 'mitteilung%'
    OR title LIKE 'avviso%'
    OR title LIKE 'avvisi %'

    -- Corrections / errata / retractions (anchored with delimiter)
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

    -- TOC / index (anchored)
    OR title LIKE 'index to volume%'
    OR title LIKE 'index for volume%'
    OR title LIKE 'contents of volume%'

    -- Issue info (anchored)
    OR title LIKE 'issue information%'
    OR title LIKE 'informações do fascículo%'
    OR title LIKE 'información del número%'
    OR title LIKE 'informazioni del fascicolo%'

    -- Call for papers (anchored)
    OR title LIKE 'call for papers%'
    OR title LIKE 'call for submissions%'

    -- Letters to editor (anchored)
    OR title LIKE 'letter to the editor%'
    OR title LIKE 'letters to the editor%'

    -- Book reviews (anchored with delimiter — "Book Review: ..." is junk;
    -- "A Review of Particle Physics" is NOT matched)
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
    OR title LIKE 'review of%'

    -- Communications (anchored)
    OR title LIKE 'communications, comments%'

    -- Notices (exact-ish, anchored)
    OR LOWER(TRIM(title)) IN ('notice', 'notices')

    -- Teses / dissertações (anchored)
    OR title LIKE 'teses %'
    OR title LIKE 'dissertações %'

    -- --------------------------------------------------------
    -- 1d. REGEX patterns — structural debris
    -- --------------------------------------------------------

    -- Editor attribution at end of title: "..., ed." / "..., eds." / "... (ed.)" / "... (eds.)"
    OR title REGEXP ',\\s*eds?\\.?\\s*$'
    OR title REGEXP '\\(eds?\\.?\\)\\s*$'

    -- Pagination at end: "..., 320 pp." / "..., 320 pp"
    OR title REGEXP ',\\s*[0-9]+\\s*pp\\.?\\s*$'
    OR title REGEXP ',\\s*[0-9]+\\s*pages?\\s*$'

    -- ISBN embedded in title
    OR title REGEXP '\\bISBN[\\s:\\-]*[0-9Xx\\-]{10,}'

    -- Price with currency code: "USD 29.95", "EUR 35", "GBP 20"
    OR title REGEXP '\\b(USD|GBP|EUR|BRL|AUD|CAD|CHF)\\s*[0-9]+'

    -- Price with currency symbol followed by amount (bibliographic debris)
    -- Anchored: symbol + digits + dot + digits to avoid false positives
    OR title REGEXP '[\\$£€]\\s*[0-9]+\\.[0-9]{2}'
;

-- §1b  DELETE BOOK REVIEWS IDENTIFIABLE BY SUBTITLE CONTENT
--
-- After cleanup.py splits "Title: Subtitle", many book reviews end up as:
--   title = "Book Title"
--   subtitle = "By Author Name. Publisher, Year. 320 pp. $29.95"
-- These are review citations, not genuine subtitles.

-- Conservative: subtitle has BOTH publisher/pages AND "By/Edited by Author"
DELETE w FROM works w
WHERE w.subtitle IS NOT NULL
  AND w.subtitle REGEXP '(University.*Press|Press|Publishers?|Verlag|\\. Pp\\.|, pp\\.|ISBN|\\$[0-9]+|£[0-9]+|\\(cloth\\)|\\(paper\\))'
  AND w.subtitle REGEXP '\\. By [A-Z]|\\. Edited by [A-Z]|\\. Ed\\. by| By [A-Z].*Press'
  AND w.work_type IN ('ARTICLE', 'CHAPTER');

-- Broader: subtitle has publisher + pages/ISBN, long enough to be bibliographic citation
DELETE w FROM works w
WHERE w.subtitle IS NOT NULL
  AND w.subtitle REGEXP '(University.*Press|\\. Pp\\.|, pp\\.|[0-9]+ pp|ISBN)'
  AND LENGTH(w.subtitle) > 50
  AND w.work_type IN ('ARTICLE', 'CHAPTER');

-- Author-first format: "Author Name: Publisher, Year, NNN pp"
DELETE w FROM works w
WHERE w.subtitle IS NOT NULL
  AND w.subtitle REGEXP '(Press|Publishers?|Books?).*[0-9]{4}'
  AND w.subtitle REGEXP '[0-9]+\\s*pp'
  AND LENGTH(w.subtitle) > 40
  AND w.work_type IN ('ARTICLE', 'CHAPTER');

-- Price in subtitle (bibliographic debris): "$29.95" / "£19.95"
DELETE w FROM works w
WHERE w.subtitle IS NOT NULL
  AND w.subtitle REGEXP '[\\$£€]\\s*[0-9]+\\.[0-9]{2}'
  AND w.subtitle REGEXP '(Press|Publishers?|pp\\.|ISBN)'
  AND w.work_type IN ('ARTICLE', 'CHAPTER');


-- ============================================================
-- §2  ORPHAN REMOVAL (LEFT JOIN for performance)
--     Complements sp_clean_core_data, which does not touch works,
--     venues, organizations, subjects or files.
-- ============================================================

-- 2a. Works without any authorship
DELETE w FROM works w
LEFT JOIN authorships a ON a.work_id = w.id
WHERE a.work_id IS NULL;

-- 2b. Files pointing to nonexistent publications
DELETE f FROM files f
LEFT JOIN publications p ON f.publication_id = p.id
WHERE p.id IS NULL;

-- 2c. Venues with zero publications
DELETE v FROM venues v
LEFT JOIN publications p ON p.venue_id = v.id
WHERE p.venue_id IS NULL;

-- 2d. Subjects with zero associations
DELETE s FROM subjects s
LEFT JOIN work_subjects  ws ON ws.subject_id = s.id
LEFT JOIN venue_subjects vs ON vs.subject_id = s.id
WHERE ws.subject_id IS NULL AND vs.subject_id IS NULL;

-- 2e. Organizations with no references anywhere (single pass)
DELETE o FROM organizations o
LEFT JOIN authorships  a   ON o.id = a.affiliation_id
LEFT JOIN funding      f   ON o.id = f.funder_id
LEFT JOIN programs     pr  ON o.id = pr.institution_id
LEFT JOIN publications pub ON o.id = pub.publisher_id
LEFT JOIN venues       ve  ON o.id = ve.publisher_id
WHERE a.affiliation_id  IS NULL
  AND f.funder_id       IS NULL
  AND pr.institution_id IS NULL
  AND pub.publisher_id  IS NULL
  AND ve.publisher_id   IS NULL;


-- ============================================================
-- §3  OPEN ACCESS PROPAGATION
-- ============================================================

UPDATE publications p
JOIN venues v ON p.venue_id = v.id
SET p.open_access = 1
WHERE p.open_access = 0
  AND (v.open_access = 1 OR p.license_url IS NOT NULL OR p.scielo_pid IS NOT NULL);
