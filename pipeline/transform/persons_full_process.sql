-- ============================================================================
-- SCRIPT DE PROCESSAMENTO UNIFICADO PARA A ENTIDADE 'PERSONS' (VERSÃO 4.0)
--
-- MUDANÇAS (v4.0):
--   - Etapa 5 substituída por sp_generate_person_signatures (cursor-based,
--     CHAR_LENGTH, hifens compostos preservados, partículas tiered).
--   - Adicionada Etapa 5b: sp_fix_person_name_parts (given_names/family_name
--     com reverse-locate para recuperação de casing original).
--   - CTE inline de assinaturas removido (lógica agora encapsulada nas
--     procedures, evitando divergência de regras).
--   - sp_populate_signatures substituída por lógica de upsert já embutida
--     em sp_generate_person_signatures.
-- ============================================================================

-- Configuração da RegEx de Limpeza (Centralizada)
SET @clean_regex = '(?i)^(dr\\.?|dra\\.?|prof\\.?|profa\\.?|professor\\.?|professora\\.?|ph\\.?d\\.?|msc\\.?|mr\\.?|mrs\\.?|ms\\.?|rev\\.?|eng\\.?|engª\\.?|me\\.?)\\s+';

-- Garantir existência das tabelas de staging
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
-- FASE 1: LIMPEZA E DEDUPLICAÇÃO
-- =====================================================================

-- [ETAPA 1a] Pré-Deduplicação (lógica Regex)
SELECT 'ETAPA 1a: Pré-Deduplicação (lógica Regex)...' AS status;
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

-- [ETAPA 1b] Pré-Deduplicação (coluna normalized_name)
SELECT 'ETAPA 1b: Pré-Deduplicação (coluna normalized_name)...' AS status;
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


-- [ETAPA 2] Remoção de Registros Inválidos
SELECT 'ETAPA 2: Removendo registros sintaticamente nulos...' AS status;
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


-- [ETAPA 3] Limpeza Avançada (remoção de títulos, normalização)
SELECT 'ETAPA 3: Remoção de títulos e normalização de espaçamento...' AS status;
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


-- [ETAPA 4] Deduplicação Final por Identificadores Externos
SELECT 'ETAPA 4: Deduplicação por ORCID, Scopus, Lattes...' AS status;
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
-- FASE 2: ASSINATURAS E PARTES DO NOME
-- =====================================================================

-- [ETAPA 5a] Gerar assinaturas (cursor-based, CHAR_LENGTH, hifens
--            compostos preservados, partículas tiered, upsert em
--            signatures, vinculação de persons.signature_id)
SELECT 'ETAPA 5a: Gerando assinaturas (sp_generate_person_signatures)...' AS status;
CALL sp_generate_person_signatures(NULL, 1);

-- [ETAPA 5b] Corrigir given_names / family_name (reverse-locate,
--            CHAR_LENGTH, mesma lógica de partículas)
SELECT 'ETAPA 5b: Corrigindo given_names/family_name (sp_fix_person_name_parts)...' AS status;
CALL sp_fix_person_name_parts(NULL, 1);


-- =====================================================================
-- FASE 3: RECÁLCULO DE MÉTRICAS
-- =====================================================================

SELECT 'ETAPA 6: Executando recálculo completo de métricas...' AS status;
CALL sp_run_full_recalculation();


SELECT 'PROCESSO UNIFICADO CONCLUÍDO (V4.0).' AS final_status;