-- =============================================================================
-- 01: Cortex Search Service (Initial Setup)
-- =============================================================================
-- Creates a Cortex Search Service over FILING_CHUNKS for semantic retrieval.
-- Uses multi-index syntax (TEXT + VECTOR) with Arctic embedding model.
--
-- IMPORTANT: This script creates the service ONCE during initial setup.
-- After creation, the service uses INCREMENTAL refresh (the default):
--   - TARGET_LAG = '1 day' automatically detects and indexes new/changed rows
--   - PRIMARY KEY (CHUNK_ID) enables optimized delta-only embedding
--   - Only new chunks are embedded on each refresh cycle (not all 8M+ rows)
--   - The serving DAG triggers ALTER ... REFRESH for immediate updates
--
-- IMMUTABLE PROPERTIES (require recreation by re-running this script):
--   - EMBEDDING_MODEL (snowflake-arctic-embed-m-v1.5)
--   - REFRESH_MODE (INCREMENTAL)
--   - TEXT INDEXES columns (CHUNK_TEXT, CHUNK_ID, ACCESSION_NO)
--   - VECTOR INDEXES columns (CHUNK_TEXT with model)
--
-- MUTABLE PROPERTIES (can be changed with ALTER without recreation):
--   - TARGET_LAG, WAREHOUSE, ATTRIBUTES, PRIMARY KEY, AUTO_SUSPEND, COMMENT
--   - Scoring profiles (ADD/DROP via ALTER)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Search Service: config-driven name from $config_search_service
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE IDENTIFIER($config_search_service)
    TEXT INDEXES CHUNK_TEXT, CHUNK_ID, ACCESSION_NO
    VECTOR INDEXES CHUNK_TEXT (model='snowflake-arctic-embed-m-v1.5')
    PRIMARY KEY (CHUNK_ID)
    ATTRIBUTES COMPANY_NAME, TICKER, FORM_TYPE, SECTION_NAME, FILED_AT, PERIOD_OF_REPORT, INDUSTRY_SECTOR, INDUSTRY_TITLE, CHUNK_ID, ACCESSION_NO
    WAREHOUSE = IDENTIFIER($config_warehouse)
    TARGET_LAG = '1 day'
    COMMENT = 'SEC filing semantic search (multi-index, Arctic M-v1.5 embeddings, PK optimized)'
AS (
    SELECT
        CHUNK_ID, CHUNK_TEXT, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE, SECTION_NAME,
        TO_VARCHAR(FILED_AT, 'YYYY-MM-DD') AS FILED_AT,
        TO_VARCHAR(PERIOD_OF_REPORT, 'YYYY-MM-DD') AS PERIOD_OF_REPORT,
        COALESCE(INDUSTRY_SECTOR, 'Other') AS INDUSTRY_SECTOR,
        INDUSTRY_TITLE
    FROM FILING_CHUNKS
    WHERE CHUNK_TEXT IS NOT NULL AND LENGTH(CHUNK_TEXT) > 100
);


-- =============================================================================
-- Scoring Profiles (query-time only — no rebuild required)
-- =============================================================================

ALTER CORTEX SEARCH SERVICE IDENTIFIER($config_search_service)
  ADD SCORING PROFILE no_rerank
  WITH WEIGHTS (reranking_weight => 0);


-- =============================================================================
-- Build Monitor SP
-- =============================================================================
-- Polls until the search service is ACTIVE. Useful for automation.

CREATE OR REPLACE PROCEDURE MONITOR_SEARCH_SERVICE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    serving_state VARCHAR; source_rows INT; iteration INT DEFAULT 0;
    svc_name VARCHAR;
BEGIN
    svc_name := _CFG('search_service');
    LOOP
        iteration := iteration + 1;
        SHOW CORTEX SEARCH SERVICES LIKE :svc_name IN SCHEMA;
        SELECT "serving_state", "source_data_num_rows"
            INTO :serving_state, :source_rows
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
        IF (:serving_state = 'ACTIVE') THEN BREAK; END IF;
        IF (:iteration >= 720) THEN RETURN 'TIMEOUT (' || :iteration || ' iterations)'; END IF;
        CALL SYSTEM$WAIT(300);
    END LOOP;
    RETURN 'ACTIVE: ' || :svc_name || ', rows=' || :source_rows::VARCHAR;
END;
$$;


-- =============================================================================
-- MONITORING
-- =============================================================================

-- SHOW CORTEX SEARCH SERVICES IN SCHEMA;
-- SHOW SCORING PROFILES IN CORTEX SEARCH SERVICE IDENTIFIER($config_search_service);
