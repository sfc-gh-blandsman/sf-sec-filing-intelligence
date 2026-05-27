-- =============================================================================
-- 01: Drop Legacy Utility Objects
-- =============================================================================
-- Removes temporary/debug objects created during development that are no longer
-- needed in the production pipeline.
--
-- Safe to run at any time — these objects are not referenced by any active
-- tasks, stored procedures, or the Streamlit app.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Debug/development stored procedures (replaced by production SPs)
-- =============================================================================

DROP PROCEDURE IF EXISTS _DQ_FETCH(VARCHAR);
DROP PROCEDURE IF EXISTS CHECK_EDGAR_QUARTER(INT, INT);
DROP PROCEDURE IF EXISTS DEBUG_FEED_FORMS(VARCHAR);

-- =============================================================================
-- Temporary tables that may have been left behind
-- =============================================================================

DROP TABLE IF EXISTS _FEED_INDEX_TMP;
DROP TABLE IF EXISTS _FEED_CONTENT_TMP;
DROP TABLE IF EXISTS _METRICS_EXCERPTS;
DROP TABLE IF EXISTS _METRICS_BATCH;
DROP TABLE IF EXISTS _GUIDANCE_EXCERPTS;
DROP TABLE IF EXISTS _GUIDANCE_BATCH;
DROP TABLE IF EXISTS _DEDUP_KEEP;
DROP TABLE IF EXISTS _CONTENT_DEDUP_KEEP;

-- =============================================================================
-- Verify
-- =============================================================================

SELECT 'Cleanup complete. Legacy objects dropped.' AS status;
