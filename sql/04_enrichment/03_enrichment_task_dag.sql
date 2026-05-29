-- =============================================================================
-- 03: Enrichment Task DAG (Tickers + Industry Mapping)
-- =============================================================================
-- Server-side task DAG for post-ingestion enrichment.
-- Runs ticker enrichment (SEC API lookups) then metadata backfill
-- (SIC → INDUSTRY_SECTOR + INDUSTRY_TITLE + PERIOD_OF_REPORT propagation).
--
-- Architecture:
--   T_ENRICH_ROOT (triggered by feed ingestion finalizer or manual)
--   ├── T_ENRICH_TICKERS (loops ENRICH_TICKERS SP until DONE)
--   ├── T_ENRICH_BACKFILL (AFTER TICKERS — maps SIC, propagates to downstream)
--   └── T_ENRICH_FINALIZER (emails summary → triggers T_PROCESSING_ROOT)
--
-- Chain: T_FEED_INGEST_FINALIZER → T_ENRICH_ROOT → T_PROCESSING_ROOT
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Root Task (triggered by feed finalizer or manual EXECUTE TASK)
-- =============================================================================

CREATE OR REPLACE TASK T_ENRICH_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse)
    SCHEDULE = 'USING CRON 0 0 29 2 * UTC'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    COMMENT = 'Root: enrichment DAG (tickers + industry mapping). Triggered by feed finalizer.'
AS
    SELECT 'ENRICHMENT_DAG_STARTED' AS status;


-- =============================================================================
-- Step 1: Ticker Enrichment (loops until DONE)
-- =============================================================================

CREATE OR REPLACE TASK T_ENRICH_TICKERS
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Bulk + per-CIK ticker enrichment (loops until DONE)'
    AFTER T_ENRICH_ROOT
AS
BEGIN
    LET result VARCHAR := '';
    LET last_cik VARCHAR := '0000000000';
    LET iteration INT := 0;

    -- Phase 1: Bulk enrichment (one HTTP call, ~10K CIK->ticker mappings)
    CALL ENRICH_TICKERS_BULK(_CFG('user_agent'));

    -- Phase 2: Per-CIK for remaining (catches CIKs not in bulk file)
    LOOP
        iteration := iteration + 1;
        CALL ENRICH_TICKERS(500, :last_cik, _CFG('user_agent'));
        result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

        IF (CONTAINS(:result, 'DONE')) THEN BREAK; END IF;
        IF (:iteration >= 100) THEN BREAK; END IF;

        -- Extract last_cik from result for cursor resumption
        last_cik := REGEXP_SUBSTR(:result, 'last_cik=([0-9]+)', 1, 1, 'e');
        IF (:last_cik IS NULL) THEN BREAK; END IF;
    END LOOP;

    RETURN 'Ticker enrichment: bulk + ' || :iteration || ' per-CIK batches. Final: ' || :result;
END;


-- =============================================================================
-- Step 2: Metadata Backfill (SIC → Industry + Period propagation)
-- =============================================================================

CREATE OR REPLACE TASK T_ENRICH_BACKFILL
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Maps SIC codes to INDUSTRY_SECTOR + INDUSTRY_TITLE, propagates to downstream tables'
    AFTER T_ENRICH_TICKERS
AS
BEGIN
    -- Step 2a: Extract PERIOD_OF_REPORT from content text
    UPDATE FILING_INDEX fi
    SET PERIOD_OF_REPORT = TRY_TO_DATE(
        REGEXP_SUBSTR(fc.CONTENT_TEXT, 'CONFORMED PERIOD OF REPORT:\\s*(\\d{8})', 1, 1, 'e'),
        'YYYYMMDD'
    )
    FROM FILING_CONTENT fc
    WHERE fi.ACCESSION_NO = fc.ACCESSION_NO
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.PERIOD_OF_REPORT IS NULL;

    -- Step 2b: Extract SIC code from content text and map to industry
    UPDATE FILING_INDEX fi
    SET SIC_CODE = sic_raw.sic_code,
        INDUSTRY_SECTOR = COALESCE(ref.SECTOR, 'Other'),
        INDUSTRY_TITLE = COALESCE(ref.INDUSTRY_TITLE, 'Other')
    FROM (
        SELECT fc.ACCESSION_NO,
               LPAD(COALESCE(
                   REGEXP_SUBSTR(fc.CONTENT_TEXT, 'STANDARD INDUSTRIAL CLASSIFICATION:.*\\[(\\d+)\\]', 1, 1, 'e'),
                   REGEXP_SUBSTR(fc.CONTENT_TEXT, '<ASSIGNED-SIC>(\\d+)', 1, 1, 'e')
               ), 4, '0') AS sic_code
        FROM FILING_CONTENT fc
        WHERE fc.CONTENT_TEXT IS NOT NULL
    ) sic_raw
    LEFT JOIN SIC_CODES ref ON ref.SIC_CODE = sic_raw.sic_code
    WHERE fi.ACCESSION_NO = sic_raw.ACCESSION_NO
      AND sic_raw.sic_code IS NOT NULL
      AND fi.INDUSTRY_SECTOR IS NULL;

    -- Step 2c: Map INDUSTRY_SECTOR for filings where SIC_CODE was set at ingestion
    UPDATE FILING_INDEX fi
    SET INDUSTRY_SECTOR = COALESCE(ref.SECTOR, 'Other'),
        INDUSTRY_TITLE = COALESCE(ref.INDUSTRY_TITLE, 'Other')
    FROM SIC_CODES ref
    WHERE fi.SIC_CODE = ref.SIC_CODE
      AND fi.SIC_CODE IS NOT NULL
      AND fi.INDUSTRY_SECTOR IS NULL;

    -- Step 2d: Set 'Other' for any remaining unmapped filings
    UPDATE FILING_INDEX
    SET INDUSTRY_SECTOR = 'Other', INDUSTRY_TITLE = 'Other'
    WHERE INDUSTRY_SECTOR IS NULL;

    -- Step 2e: Propagate PERIOD_OF_REPORT to FILING_SIGNALS
    UPDATE FILING_SIGNALS fs
    SET PERIOD_OF_REPORT = fi.PERIOD_OF_REPORT
    FROM FILING_INDEX fi
    WHERE fs.ACCESSION_NO = fi.ACCESSION_NO
      AND fi.PERIOD_OF_REPORT IS NOT NULL
      AND fs.PERIOD_OF_REPORT IS NULL;

    RETURN 'Backfill complete';
END;


-- =============================================================================
-- Finalizer (emails summary → triggers processing DAG)
-- =============================================================================

CREATE OR REPLACE TASK T_ENRICH_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = T_ENRICH_ROOT
    COMMENT = 'Finalizer: emails enrichment summary, conditionally triggers processing DAG'
AS
BEGIN
    LET total INT;
    LET with_ticker INT;
    LET with_sector INT;
    LET with_period INT;

    SELECT COUNT(*) INTO :total FROM FILING_INDEX;
    SELECT COUNT(TICKER) INTO :with_ticker FROM FILING_INDEX;
    SELECT COUNT(INDUSTRY_SECTOR) INTO :with_sector FROM FILING_INDEX;
    SELECT COUNT(PERIOD_OF_REPORT) INTO :with_period FROM FILING_INDEX;

    LET msg VARCHAR := 'ENRICHMENT DAG COMPLETE' || CHR(10) || CHR(10) ||
        'Total filings: ' || :total::VARCHAR || CHR(10) ||
        'With ticker: ' || :with_ticker::VARCHAR || ' (' || ROUND(100.0 * :with_ticker / NULLIF(:total, 0), 1)::VARCHAR || '%)' || CHR(10) ||
        'With INDUSTRY_SECTOR: ' || :with_sector::VARCHAR || ' (' || ROUND(100.0 * :with_sector / NULLIF(:total, 0), 1)::VARCHAR || '%)' || CHR(10) ||
        'With PERIOD_OF_REPORT: ' || :with_period::VARCHAR || ' (' || ROUND(100.0 * :with_period / NULLIF(:total, 0), 1)::VARCHAR || '%)' || CHR(10) ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR;

    -- Only chain to processing if enrichment produced results
    IF (:with_sector > 0 AND :with_period > 0) THEN
        IF (_CFG('enable_dag_emails') = 'TRUE') THEN
            CALL SYSTEM$SEND_EMAIL(
                _CFG('email_integration'),
                _CFG('email_recipient'),
                'SEC Filing Enrichment: COMPLETE -> Processing',
                :msg
            );
        END IF;
        EXECUTE TASK T_PROCESSING_ROOT;
    ELSE
        IF (_CFG('enable_dag_emails') = 'TRUE') THEN
            CALL SYSTEM$SEND_EMAIL(
                _CFG('email_integration'),
                _CFG('email_recipient'),
                'SEC Filing Enrichment: FAILED (no sector/period data)',
                :msg
            );
        END IF;
    END IF;
END;


-- =============================================================================
-- Resume all tasks
-- =============================================================================

ALTER TASK T_ENRICH_TICKERS RESUME;
ALTER TASK T_ENRICH_BACKFILL RESUME;
ALTER TASK T_ENRICH_FINALIZER RESUME;
ALTER TASK T_ENRICH_ROOT RESUME;


-- =============================================================================
-- EXECUTION + MONITORING
-- =============================================================================
-- Manual trigger:
--   EXECUTE TASK T_ENRICH_ROOT;
--
-- Monitor:
--   SELECT COUNT(*) AS total, COUNT(TICKER) AS with_ticker,
--          COUNT(INDUSTRY_SECTOR) AS with_sector
--   FROM FILING_INDEX;
