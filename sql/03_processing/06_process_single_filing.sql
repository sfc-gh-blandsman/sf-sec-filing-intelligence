-- =============================================================================
-- 06: Process Single Filing (On-Demand Spot Processing)
-- =============================================================================
-- Processes a single filing through the full pipeline:
--   1. SIC/Industry enrichment (if missing)
--   2. Chunking (CHUNK_FILING UDF — same as DAG)
--   3. Signal extraction (AI_EXTRACT — same as DAG)
--   4. Status updates
--   5. Industry propagation to chunks/signals
--   6. Cortex Search refresh (forced)
--
-- Use case: Spot-process a newly gap-filled filing immediately instead of
-- waiting for the next full processing DAG run.
--
-- Usage:
--   CALL PROCESS_SINGLE_FILING('0000320193-25-000079');
--
-- Dependencies:
--   - CLEAN_TEXT UDF, CHUNK_FILING UDF (from 01/02 scripts)
--   - SIC_CODES reference table (for industry mapping)
--   - Cortex Search service (for forced refresh)
--   - _CFG() function
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

CREATE OR REPLACE PROCEDURE PROCESS_SINGLE_FILING(P_ACCESSION_NO VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_form_type VARCHAR;
    v_company VARCHAR;
    v_ticker VARCHAR;
    v_sector VARCHAR;
    v_sic VARCHAR;
    v_chunk_count INT := 0;
    v_signal_count INT := 0;
    v_search_svc VARCHAR;
    v_result VARCHAR;
BEGIN
    -- Validate filing exists
    SELECT FORM_TYPE, COMPANY_NAME, TICKER, INDUSTRY_SECTOR, SIC_CODE
    INTO :v_form_type, :v_company, :v_ticker, :v_sector, :v_sic
    FROM FILING_INDEX WHERE ACCESSION_NO = :P_ACCESSION_NO;

    IF (:v_form_type IS NULL) THEN
        RETURN 'ERROR: Filing ' || :P_ACCESSION_NO || ' not found in FILING_INDEX.';
    END IF;

    -- Validate content exists
    IF (NOT EXISTS (SELECT 1 FROM FILING_CONTENT WHERE ACCESSION_NO = :P_ACCESSION_NO)) THEN
        RETURN 'ERROR: No content found for ' || :P_ACCESSION_NO || ' in FILING_CONTENT.';
    END IF;

    -- Step 1: Fix SIC/Industry if missing
    IF (:v_sic IS NULL) THEN
        -- Try to get SIC from content header (already in index for most filings)
        -- If not available, skip — will remain NULL
        NULL;
    END IF;

    IF (:v_sector IS NULL OR :v_sector = 'Other') THEN
        -- Map SIC to industry sector via reference table
        BEGIN
            SELECT INDUSTRY_SECTOR, INDUSTRY_TITLE INTO :v_sector, :v_result
            FROM SIC_CODES WHERE SIC_CODE = :v_sic;
            IF (:v_sector IS NOT NULL) THEN
                UPDATE FILING_INDEX
                SET INDUSTRY_SECTOR = :v_sector, INDUSTRY_TITLE = :v_result
                WHERE ACCESSION_NO = :P_ACCESSION_NO;
            END IF;
        EXCEPTION WHEN OTHER THEN NULL; END;
    END IF;

    -- Step 2: Chunk the filing (same logic as T_CHUNK_10K/10Q/8K)
    -- Only if not already chunked
    IF (NOT EXISTS (SELECT 1 FROM FILING_CHUNKS WHERE ACCESSION_NO = :P_ACCESSION_NO)) THEN
        INSERT INTO FILING_CHUNKS
            (CHUNK_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
             FILED_AT, PERIOD_OF_REPORT, SECTION_NAME, CHUNK_INDEX,
             CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR, INDUSTRY_TITLE)
        SELECT
            fi.ACCESSION_NO || '_' || c.VALUE:chunk_index::VARCHAR,
            fi.ACCESSION_NO, fi.COMPANY_NAME, fi.TICKER, fi.FORM_TYPE,
            fi.FILED_AT, fi.PERIOD_OF_REPORT,
            c.VALUE:section_name::VARCHAR, c.VALUE:chunk_index::INT,
            c.VALUE:chunk_text::VARCHAR,
            LENGTH(c.VALUE:chunk_text::VARCHAR) / 4,
            fi.INDUSTRY_SECTOR, fi.INDUSTRY_TITLE
        FROM FILING_CONTENT fc
        JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
        LATERAL FLATTEN(INPUT => CHUNK_FILING(CLEAN_TEXT(fc.CONTENT_TEXT), fi.FORM_TYPE, 1500, 200)) c
        WHERE fc.ACCESSION_NO = :P_ACCESSION_NO
          AND fc.CONTENT_TEXT IS NOT NULL
          AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

        SELECT COUNT(*) INTO :v_chunk_count FROM FILING_CHUNKS WHERE ACCESSION_NO = :P_ACCESSION_NO;

        -- Update parse status
        UPDATE FILING_CONTENT SET PARSE_STATUS = 'CHUNKED', PROCESSED_AT = CURRENT_TIMESTAMP()
        WHERE ACCESSION_NO = :P_ACCESSION_NO;
    ELSE
        SELECT COUNT(*) INTO :v_chunk_count FROM FILING_CHUNKS WHERE ACCESSION_NO = :P_ACCESSION_NO;
    END IF;

    -- Step 3: Signal extraction (same logic as SIGNAL_EXTRACT_10K/10Q/8K)
    -- Only if not already extracted
    IF (NOT EXISTS (SELECT 1 FROM FILING_SIGNALS WHERE ACCESSION_NO = :P_ACCESSION_NO)) THEN
        INSERT INTO FILING_SIGNALS
            (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
             SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
             KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
             EXTRACTION_MODEL, IS_AMENDMENT)
        WITH source AS (
            SELECT fc.ACCESSION_NO, fi.COMPANY_NAME, fi.TICKER, fi.FORM_TYPE,
                   fi.FILED_AT, fi.PERIOD_OF_REPORT, fi.IS_AMENDMENT,
                   fi.INDUSTRY_SECTOR, fi.INDUSTRY_TITLE,
                   LEFT(CLEAN_TEXT(fc.CONTENT_TEXT), 16000) AS excerpt
            FROM FILING_CONTENT fc
            JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
            WHERE fc.ACCESSION_NO = :P_ACCESSION_NO AND fc.CONTENT_TEXT IS NOT NULL
        ),
        extracted AS (
            SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
                text => s.excerpt,
                responseFormat => {
                    'event_type': 'string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other',
                    'sentiment': 'string - one of: POSITIVE, NEGATIVE, NEUTRAL, MIXED',
                    'summary': 'string - 2-3 sentence summary of the most material information',
                    'key_metrics': 'object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change',
                    'risk_flags': 'array of strings - specific risk categories mentioned',
                    'material_items': 'array of strings - for 8-Ks: Item numbers reported'
                }
            ) AS ai_result FROM source s
        )
        SELECT e.ACCESSION_NO || '_sig', e.ACCESSION_NO, e.COMPANY_NAME, e.TICKER, e.FORM_TYPE,
            e.FILED_AT, e.PERIOD_OF_REPORT,
            COALESCE(NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'), 'Annual Report'),
            COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL'),
            NULLIF(e.ai_result:response:summary::TEXT, 'None'),
            NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None'),
            CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
            CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
            e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, 'arctic-extract', e.IS_AMENDMENT
        FROM extracted e WHERE e.ai_result IS NOT NULL;

        SELECT COUNT(*) INTO :v_signal_count FROM FILING_SIGNALS WHERE ACCESSION_NO = :P_ACCESSION_NO;

        -- Update signal status
        UPDATE FILING_CONTENT SET SIGNAL_STATUS = 'EXTRACTED'
        WHERE ACCESSION_NO = :P_ACCESSION_NO;
    ELSE
        SELECT COUNT(*) INTO :v_signal_count FROM FILING_SIGNALS WHERE ACCESSION_NO = :P_ACCESSION_NO;
    END IF;

    -- Step 4: Force Cortex Search refresh
    v_search_svc := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('search_service');
    EXECUTE IMMEDIATE 'ALTER CORTEX SEARCH SERVICE ' || :v_search_svc || ' RESUME';

    -- Return summary
    RETURN 'Processed ' || :v_company || ' ' || :v_form_type || ' (' || :P_ACCESSION_NO || '): ' ||
           :v_chunk_count::VARCHAR || ' chunks, ' || :v_signal_count::VARCHAR || ' signal(s). ' ||
           'Search refresh triggered.';
END;
$$;


-- =============================================================================
-- SP: TRIGGER_PROCESS_FILING (Async wrapper via dynamic task)
-- =============================================================================
-- Creates and executes a one-shot task to process a single filing
-- asynchronously. Emails the result when complete.
--
-- Usage:
--   CALL TRIGGER_PROCESS_FILING('0000320193-25-000079');
--   -- Returns immediately with task name for monitoring
-- =============================================================================

CREATE OR REPLACE PROCEDURE TRIGGER_PROCESS_FILING(P_ACCESSION_NO VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    task_name VARCHAR;
    task_ddl VARCHAR;
    task_body VARCHAR;
    email_int VARCHAR;
    email_to VARCHAR;
BEGIN
    task_name := 'PROCESS_FILING_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    email_int := _CFG('email_integration');
    email_to := _CFG('email_recipient');

    task_body := 'BEGIN' ||
        ' LET result VARCHAR;' ||
        ' CALL PROCESS_SINGLE_FILING(''' || :P_ACCESSION_NO || ''');' ||
        ' result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));' ||
        ' CALL SYSTEM$SEND_EMAIL(''' || :email_int || ''', ''' || :email_to || ''',' ||
        ' ''SEC Filing Processed: ' || :P_ACCESSION_NO || ''',' ||
        ' :result);' ||
        ' END';

    task_ddl := 'CREATE OR REPLACE TASK ' || :task_name ||
                ' WAREHOUSE = FILING_BUILD_WH' ||
                ' USER_TASK_TIMEOUT_MS = 3600000' ||
                ' SCHEDULE = ''USING CRON 0 0 29 2 * UTC''' ||
                ' COMMENT = ''One-shot: process filing ' || :P_ACCESSION_NO || '''' ||
                ' AS ' || :task_body;

    EXECUTE IMMEDIATE :task_ddl;
    EXECUTE IMMEDIATE 'EXECUTE TASK ' || :task_name;

    RETURN 'Processing triggered. Task: ' || :task_name || '. You will receive an email when complete.';
END;
$$;
