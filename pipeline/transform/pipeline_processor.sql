-- ============================================================================
-- PIPELINE GLOBAL DE PROCESSAMENTO DE DADOS (ETL/ELT)
-- Ordem estrita de execução para garantia de integridade relacional e matemática
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FASE 1: Sanitização e Normalização Básica
-- Objetivo: Padronizar codificação, metadados (DOIs/ISBNs) e anomalias de string.
-- ----------------------------------------------------------------------------
CALL sp_clean_html_entities();
CALL sp_normalize_publications_data();
CALL sp_clean_split_compound_persons();

-- ----------------------------------------------------------------------------
-- FASE 2: Integridade Estrutural e Resolução de Identidades
-- Objetivo: Purgar registros órfãos e construir hashes de assinatura de autores.
-- ----------------------------------------------------------------------------
CALL sp_clean_core_data();
CALL sp_rebuild_signatures();

-- ----------------------------------------------------------------------------
-- FASE 3: Reconstrução do Grafo de Citações
-- Objetivo: Conectar a malha de referências utilizando os DOIs normalizados na Fase 1.
-- ----------------------------------------------------------------------------
CALL sp_repair_work_references_consistency(1);
CALL sp_resolve_all_pending_existing(10000);
CALL sp_review_reference_consistency(10000);

-- ----------------------------------------------------------------------------
-- FASE 4: Agregação Estatística Base (PRÉ-REQUISITO MATEMÁTICO CRÍTICO)
-- Objetivo: Computar volumetria, citações e calcular o H-Index dos autores.
-- Nota: DEVE rodar antes da Fase 5, pois o ranking de veículos usa o H-Index.
-- ----------------------------------------------------------------------------
CALL sp_update_core_statistics();

-- ----------------------------------------------------------------------------
-- FASE 5: Cômputo de Escores Qualitativos e Ranking
-- Objetivo: Avaliar veículos e calcular métricas de impacto de longo prazo.
-- ----------------------------------------------------------------------------
CALL sp_calculate_venue_ranking();
CALL sp_update_10yr_impact_factors();

-- ----------------------------------------------------------------------------
-- FASE 6: Sumarização e Indexação para API (Processamento em Lotes)
-- Objetivo: Construir as tabelas de leitura rápida e payloads JSON de forma contínua.
-- Lote definido para 50.000 registros visando estabilidade de I/O.
-- ----------------------------------------------------------------------------
CALL sp_build_summary_publications(50000);
CALL sp_build_summary_venues(); -- (Processamento em lote único, dados menores)
CALL sp_build_summary_persons(50000);

-- Alternativamente, se quiser rodar via orquestrador:
-- CALL sp_orchestrate_all_summaries(50000);

-- ----------------------------------------------------------------------------
-- FASE 7: Otimização do Motor Relacional
-- Objetivo: Atualizar as estatísticas do Query Planner (Árvores B+) após as movimentações.
-- ----------------------------------------------------------------------------
CALL sp_reindex_database();
