-- =============================================================================
-- 01: FULL PROJECT TEARDOWN
-- =============================================================================
-- Removes ALL objects created by the SEC Filing Intelligence project.
-- Run this to completely clean up and start fresh.
--
-- WARNING: This script is DESTRUCTIVE and IRREVERSIBLE.
-- All data, tasks, services, and infrastructure will be permanently deleted.
--
-- Usage:
--   1. Review the script to confirm you want to delete everything
--   2. Run in a Snowsight worksheet as ACCOUNTADMIN
--   3. The database drop at the end cascades to all schema objects
--
-- Note: Some objects (EAI, notification integration) are account-level and
-- must be dropped separately from the database.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- Step 1: Suspend all task DAGs (required before dropping tasks)
-- =============================================================================

ALTER TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_INGEST_ROOT SUSPEND;
ALTER TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_ENRICH_ROOT SUSPEND;
ALTER TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_PROCESSING_ROOT SUSPEND;
ALTER TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SERVING_ROOT SUSPEND;
ALTER TASK IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_DAG_ROOT SUSPEND;

-- =============================================================================
-- Step 2: Drop all tasks (children first, then roots)
-- =============================================================================

-- Feed DAG
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_INGEST_FINALIZER;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_VALIDATE;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_JAN;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_FEB;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_MAR;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_APR;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_MAY;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_JUN;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_JUL;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_AUG;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_SEP;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_OCT;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_NOV;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_DEC;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_FEED_INGEST_ROOT;

-- Enrichment DAG
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_ENRICH_FINALIZER;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_ENRICH_BACKFILL;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_ENRICH_TICKERS;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_ENRICH_ROOT;

-- Processing DAG
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_PROCESSING_FINALIZER;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_WAIT_SEARCH_ACTIVE;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_REFRESH_SEARCH;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_PROPAGATE_INDUSTRY;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_NORMALIZE_SIGNALS;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_GUIDANCE_EXTRACT;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_METRICS_EXTRACT;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SIGNAL_10K;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SIGNAL_10Q;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SIGNAL_8K;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_CHUNK_10K;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_CHUNK_10Q;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_CHUNK_8K;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_PROCESSING_ROOT;

-- Serving DAG
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SERVING_FINALIZER;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.T_SERVING_ROOT;

-- Eval DAG
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_DAG_FINALIZER;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_DAG_BENCHMARK;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_DAG_MATERIALIZE;
DROP TASK IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_DAG_ROOT;

-- =============================================================================
-- Step 3: Drop Streamlit app
-- =============================================================================

DROP STREAMLIT IF EXISTS SEC_FILINGS.FILING_DATA.SEC_FILING_DASHBOARD;

-- =============================================================================
-- Step 4: Drop Cortex Agent
-- =============================================================================

DROP AGENT IF EXISTS SEC_FILINGS.FILING_DATA.SEC_FILING_AGENT;

-- =============================================================================
-- Step 5: Drop Cortex Search Service
-- =============================================================================

DROP CORTEX SEARCH SERVICE IF EXISTS SEC_FILINGS.FILING_DATA.SEC_FILING_SEARCH;

-- =============================================================================
-- Step 6: Drop Semantic View
-- =============================================================================

DROP SEMANTIC VIEW IF EXISTS SEC_FILINGS.FILING_DATA.SEC_FILING_ANALYTICS;

-- =============================================================================
-- Step 7: Drop stored procedures
-- =============================================================================

DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.LOAD_FEED_ARCHIVE(VARCHAR, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.LOAD_FEED_DATE_RANGE(VARCHAR, VARCHAR, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.LOAD_EDGAR_METADATA(NUMBER, NUMBER, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.DOWNLOAD_FILING_BATCH(NUMBER, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.ENRICH_TICKERS(NUMBER, VARCHAR, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.ENRICH_TICKERS_BULK(NUMBER, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.SIGNAL_EXTRACT_10K();
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.SIGNAL_EXTRACT_10Q();
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.SIGNAL_EXTRACT_8K();
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.EXTRACT_KEY_METRICS_BATCH(NUMBER);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.EXTRACT_FORWARD_GUIDANCE_BATCH(NUMBER);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.MONITOR_SEARCH_SERVICE(VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.SEARCH_LATENCY_BENCHMARK(VARCHAR, NUMBER, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA._RUN_EVAL();
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA._CREATE_EVAL_DATASET();
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA._WRITE_EVAL_CONFIG(VARCHAR, VARCHAR);
DROP PROCEDURE IF EXISTS SEC_FILINGS.FILING_DATA.DEBUG_FEED_FORMS(VARCHAR);

-- =============================================================================
-- Step 8: Drop functions (UDFs)
-- =============================================================================

DROP FUNCTION IF EXISTS SEC_FILINGS.FILING_DATA.CLEAN_TEXT(VARCHAR);
DROP FUNCTION IF EXISTS SEC_FILINGS.FILING_DATA.CHUNK_FILING(VARCHAR, VARCHAR, NUMBER, NUMBER);
DROP FUNCTION IF EXISTS SEC_FILINGS.FILING_DATA._CFG(VARCHAR);
DROP FUNCTION IF EXISTS SEC_FILINGS.FILING_DATA._DQ_FETCH(VARCHAR);

-- =============================================================================
-- Step 9: Drop stages
-- =============================================================================

DROP STAGE IF EXISTS SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE;
DROP STAGE IF EXISTS SEC_FILINGS.FILING_DATA.EVAL_CONFIGS;

-- =============================================================================
-- Step 10: Drop network rule + external access integration
-- =============================================================================

DROP NETWORK RULE IF EXISTS SEC_FILINGS.FILING_DATA.EDGAR_NETWORK_RULE;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS SEC_EDGAR_EAI;

-- =============================================================================
-- Step 11: Drop notification integration (email)
-- =============================================================================

DROP NOTIFICATION INTEGRATION IF EXISTS SEC_FILING_INTELLIGENCE_EMAIL_INT;

-- =============================================================================
-- Step 12: Drop warehouses
-- =============================================================================

DROP WAREHOUSE IF EXISTS FILING_WH;
DROP WAREHOUSE IF EXISTS FILING_BUILD_WH;
DROP WAREHOUSE IF EXISTS FILING_INGEST_WH;

-- =============================================================================
-- Step 13: Drop database (cascades to schema + any remaining objects)
-- =============================================================================
-- This is the nuclear option. Everything above is redundant if you drop the DB,
-- but the explicit drops above ensure clean teardown even if you want to keep
-- the database for other purposes.

DROP DATABASE IF EXISTS SEC_FILINGS;

-- =============================================================================
-- Verify: nothing remains
-- =============================================================================
-- SHOW DATABASES LIKE 'SEC_FILINGS';              -- Should return 0 rows
-- SHOW WAREHOUSES LIKE 'FILING%';                 -- Should return 0 rows
-- SHOW NOTIFICATION INTEGRATIONS LIKE 'SEC%';     -- Should return 0 rows
-- SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'SEC%';  -- Should return 0 rows
