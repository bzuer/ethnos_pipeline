-- ============================================================================
-- pipeline/transform/pipeline_processor.sql
--
-- Canonical ordered transform runner. Every step is a CALL into a procedure
-- that is source-controlled in:
--   pipeline/transform/procedures.sql                  (phases 1-5, 7 + helpers)
--   pipeline/transform/sp_build_summary_publications.sql (phase 6)
--   pipeline/transform/sp_build_summary_venues.sql       (phase 6)
--   pipeline/transform/sp_build_summary_persons.sql      (phase 6)
--   pipeline/transform/venue_ranking_setup.sql           (phase 5 — ranking)
--
-- Always back up the database before running this script. sp_clean_core_data
-- issues unconditional cascade deletes on orphans and the sp_build_summary_*
-- procedures TRUNCATE the summary tables before rebuilding them.
--
-- Run:
--   mariadb data < pipeline/transform/pipeline_processor.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Phase 1 — Sanitization and metadata normalization
-- ----------------------------------------------------------------------------
CALL sp_clean_html_entities();
CALL sp_normalize_publications_data();
CALL sp_clean_split_compound_persons();

-- ----------------------------------------------------------------------------
-- Phase 2 — Structural integrity and identity
-- ----------------------------------------------------------------------------
CALL sp_clean_core_data();
CALL sp_rebuild_signatures();

-- ----------------------------------------------------------------------------
-- Phase 3 — Citation graph repair and resolution
-- ----------------------------------------------------------------------------
CALL sp_repair_work_references_consistency(1);
CALL sp_resolve_all_pending_existing(10000);
CALL sp_review_reference_consistency(10000);

-- ----------------------------------------------------------------------------
-- Phase 4 — Core aggregate statistics (must run before phase 5)
-- ----------------------------------------------------------------------------
CALL sp_update_core_statistics();

-- ----------------------------------------------------------------------------
-- Phase 5 — Venue scoring and impact factor
-- ----------------------------------------------------------------------------
CALL sp_calculate_venue_ranking();
CALL sp_update_10yr_impact_factors();

-- ----------------------------------------------------------------------------
-- Phase 6 — Summary rebuild (50k batch size for stability on large datasets)
-- ----------------------------------------------------------------------------
CALL sp_build_summary_publications(50000);
CALL sp_build_summary_venues();
CALL sp_build_summary_persons(50000);
-- Alternative single-entry orchestration:
-- CALL sp_orchestrate_all_summaries(50000);

-- ----------------------------------------------------------------------------
-- Phase 7 — Engine optimization
-- ----------------------------------------------------------------------------
CALL sp_reindex_database();
