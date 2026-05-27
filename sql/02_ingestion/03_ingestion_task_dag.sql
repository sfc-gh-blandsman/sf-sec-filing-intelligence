-- =============================================================================
-- 03: Ingestion Task DAG
-- =============================================================================
-- Automated Task DAG for large-batch SEC EDGAR ingestion.
-- Loads metadata for all quarters, then downloads filings concurrently by form type.
--
-- DAG Structure:
--   T_INGEST_ROOT (manual trigger)
--   ├── T_INGEST_METADATA     — Loops all quarters for configured year range
--   ├── T_DOWNLOAD_10K        — AFTER METADATA: downloads 10-K filings in batches
--   ├── T_DOWNLOAD_10Q        — AFTER METADATA: downloads 10-Q concurrently
--   ├── T_DOWNLOAD_8K         — AFTER METADATA: downloads 8-K concurrently
--   └── T_INGEST_FINALIZER    — FINALIZE: emails summary with counts
--
-- Usage:
--   1. Run 00_config.sql to set session variables
--   2. Run this script to create the DAG
--   3. EXECUTE TASK T_INGEST_ROOT;
--   4. Monitor: SELECT * FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
--              WHERE ROOT_TASK_NAME = 'T_INGEST_ROOT';
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Root Task (manual trigger only — CRON set to never-fire date)
-- =============================================================================

CREATE OR REPLACE TASK T_INGEST_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse)
    SCHEDULE = 'USING CRON 0 0 1 1 * America/New_York'
    TASK_AUTO_RETRY_ATTEMPTS = 2
AS SELECT 1;

-- =============================================================================
-- Step 1: Load Metadata (all quarters in year range)
-- =============================================================================

CREATE OR REPLACE TASK T_INGEST_METADATA
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    AFTER T_INGEST_ROOT
AS
BEGIN
    LET start_year INT := _CFG('ingest_start_year')::INT;
    LET end_year INT := _CFG('ingest_end_year')::INT;
    LET y INT;
    LET q INT;
    LET result VARCHAR;
    LET ua VARCHAR := _CFG('user_agent');

    FOR y IN :start_year TO :end_year DO
        FOR q IN 1 TO 4 DO
            CALL LOAD_EDGAR_METADATA(:y, :q, :ua);
        END FOR;
    END FOR;

    RETURN 'Metadata loaded for ' || :start_year || ' to ' || :end_year;
END;

-- =============================================================================
-- Step 2: Download filings (3 concurrent tasks by form type)
-- =============================================================================

CREATE OR REPLACE TASK T_DOWNLOAD_10K
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    TASK_AUTO_RETRY_ATTEMPTS = 2
    AFTER T_INGEST_METADATA
AS
BEGIN
    LET batch_size INT := 5000;
    LET start_date VARCHAR := _CFG('ingest_start_year') || '-01-01';
    LET end_date VARCHAR := _CFG('ingest_end_year') || '-12-31';
    LET result VARCHAR := '';
    LET iteration INT := 0;
    LET ua VARCHAR := _CFG('user_agent');

    LOOP
        iteration := iteration + 1;
        CALL DOWNLOAD_FILING_BATCH(:batch_size, '10-K', :start_date, :end_date, :ua);
        result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        IF (CONTAINS(:result, 'No pending')) THEN BREAK; END IF;
        IF (:iteration >= 200) THEN BREAK; END IF;
    END LOOP;

    RETURN '10-K download complete after ' || :iteration || ' batches: ' || :result;
END;

CREATE OR REPLACE TASK T_DOWNLOAD_10Q
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    TASK_AUTO_RETRY_ATTEMPTS = 2
    AFTER T_INGEST_METADATA
AS
BEGIN
    LET batch_size INT := 5000;
    LET start_date VARCHAR := _CFG('ingest_start_year') || '-01-01';
    LET end_date VARCHAR := _CFG('ingest_end_year') || '-12-31';
    LET result VARCHAR := '';
    LET iteration INT := 0;
    LET ua VARCHAR := _CFG('user_agent');

    LOOP
        iteration := iteration + 1;
        CALL DOWNLOAD_FILING_BATCH(:batch_size, '10-Q', :start_date, :end_date, :ua);
        result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        IF (CONTAINS(:result, 'No pending')) THEN BREAK; END IF;
        IF (:iteration >= 200) THEN BREAK; END IF;
    END LOOP;

    RETURN '10-Q download complete after ' || :iteration || ' batches: ' || :result;
END;

CREATE OR REPLACE TASK T_DOWNLOAD_8K
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    TASK_AUTO_RETRY_ATTEMPTS = 2
    AFTER T_INGEST_METADATA
AS
BEGIN
    LET batch_size INT := 5000;
    LET start_date VARCHAR := _CFG('ingest_start_year') || '-01-01';
    LET end_date VARCHAR := _CFG('ingest_end_year') || '-12-31';
    LET result VARCHAR := '';
    LET iteration INT := 0;
    LET ua VARCHAR := _CFG('user_agent');

    LOOP
        iteration := iteration + 1;
        CALL DOWNLOAD_FILING_BATCH(:batch_size, '8-K', :start_date, :end_date, :ua);
        result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        IF (CONTAINS(:result, 'No pending')) THEN BREAK; END IF;
        IF (:iteration >= 200) THEN BREAK; END IF;
    END LOOP;

    RETURN '8-K download complete after ' || :iteration || ' batches: ' || :result;
END;

-- =============================================================================
-- Step 3: Finalizer (email summary)
-- =============================================================================

CREATE OR REPLACE TASK T_INGEST_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = T_INGEST_ROOT
AS
BEGIN
    LET index_count INT := 0;
    LET content_count INT := 0;

    SELECT COUNT(*) INTO :index_count FROM FILING_INDEX;
    SELECT COUNT(*) INTO :content_count FROM FILING_CONTENT;

    CALL SYSTEM$SEND_EMAIL(_CFG('email_integration'), _CFG('email_recipient'),
        'SEC Filing Ingestion Complete',
        'INGESTION DAG COMPLETE\n================================\n' ||
        'FILING_INDEX rows: ' || :index_count::VARCHAR || '\n' ||
        'FILING_CONTENT rows: ' || :content_count::VARCHAR || '\n' ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR);
END;

-- =============================================================================
-- Resume all child tasks (bottom-up order)
-- =============================================================================

ALTER TASK T_INGEST_FINALIZER RESUME;
ALTER TASK T_DOWNLOAD_8K RESUME;
ALTER TASK T_DOWNLOAD_10Q RESUME;
ALTER TASK T_DOWNLOAD_10K RESUME;
ALTER TASK T_INGEST_METADATA RESUME;
ALTER TASK T_INGEST_ROOT RESUME;

-- =============================================================================
-- EXECUTION
-- =============================================================================
-- EXECUTE TASK T_INGEST_ROOT;

-- MONITORING:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
-- WHERE ROOT_TASK_NAME = 'T_INGEST_ROOT';
