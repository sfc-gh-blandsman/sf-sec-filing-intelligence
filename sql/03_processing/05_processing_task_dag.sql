-- =============================================================================
-- 05: Processing Task DAG (Chunking + Signal Extraction)
-- =============================================================================
-- Server-side task DAG that runs all processing in parallel.
-- No Snowsight session dependency — runs to completion autonomously.
--
-- Architecture:
--   T_PROCESSING_ROOT (root — manual trigger via EXECUTE TASK)
--   ├── T_CHUNK_10K          (parallel — chunks 10-K filings)
--   ├── T_CHUNK_10Q          (parallel — chunks 10-Q filings)
--   ├── T_CHUNK_8K           (parallel — chunks 8-K + other form types)
--   ├── T_SIGNAL_10K         (parallel — CALL SIGNAL_EXTRACT_10K())
--   ├── T_SIGNAL_10Q         (parallel — CALL SIGNAL_EXTRACT_10Q())
--   ├── T_SIGNAL_8K          (parallel — CALL SIGNAL_EXTRACT_8K())
--   ├── T_NORMALIZE_SIGNALS  (after signals — event type normalization)
--   ├── T_METRICS_EXTRACT    (after signals — revenue/EPS extraction)
--   ├── T_GUIDANCE_EXTRACT   (after signals — forward guidance extraction)
--   ├── T_PROPAGATE_INDUSTRY (after chunks + signals + metrics + normalize)
--   ├── T_REFRESH_SEARCH     (after propagation — incremental Cortex Search refresh)
--   ├── T_WAIT_SEARCH_ACTIVE (after refresh — polls until ACTIVE)
--   └── T_PROCESSING_FINALIZER (finalizer — emails summary + eval instructions)
--
-- Key properties:
--   - Idempotent: WHERE PARSE_STATUS/SIGNAL_STATUS = 'PENDING' (safe to re-run)
--   - 48-hour timeout per child task
--   - TASK_AUTO_RETRY_ATTEMPTS = 2 on root (applies to entire DAG)
--   - Chunking and signal extraction run in PARALLEL (different status columns)
--   - Signal tasks use SPs because AI_EXTRACT JSON responseFormat causes parse
--     errors in inline task BEGIN...END blocks
--   - Finalizer emails completion counts
--
-- How to run:
--   1. Run 00_config.sql (sets session variables for IDENTIFIER refs)
--   2. Execute this script (creates SPs + tasks)
--   3. EXECUTE TASK T_PROCESSING_ROOT;
--   4. Monitor: SELECT * FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
--              WHERE ROOT_TASK_NAME = 'T_PROCESSING_ROOT';
--
-- Prerequisites:
--   - CLEAN_TEXT and CHUNK_FILING UDFs deployed
--   - FILING_CONTENT populated with raw filing text
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- SIGNAL EXTRACTION STORED PROCEDURES
-- =============================================================================
-- These wrap the AI_EXTRACT logic in SPs because the JSON responseFormat
-- literal causes SQL parse errors inside task BEGIN...END blocks.

CREATE OR REPLACE PROCEDURE SIGNAL_EXTRACT_10K()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO FILING_SIGNALS
        (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
         SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
         KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
         EXTRACTION_MODEL, IS_AMENDMENT, EXTRACTION_METHOD, SIGNAL_EXTRACTED_AT)
    WITH source AS (
        SELECT v.ACCESSION_NO, v.COMPANY_NAME, v.TICKER, v.FORM_TYPE,
               v.FILED_AT, v.PERIOD_OF_REPORT, v.IS_AMENDMENT,
               v.INDUSTRY_SECTOR, v.INDUSTRY_TITLE, v.EXCERPT
        FROM V_SIGNAL_EXCERPT v
        JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = v.ACCESSION_NO
        WHERE fc.SIGNAL_STATUS = 'PENDING' AND v.FORM_TYPE = '10-K'
    ),
    extracted AS (
        SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
            text => s.EXCERPT,
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
        e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, 'arctic-extract', e.IS_AMENDMENT,
        'section_targeted', CURRENT_TIMESTAMP()
    FROM extracted e WHERE e.ai_result IS NOT NULL;

    UPDATE FILING_CONTENT fc SET SIGNAL_STATUS = 'EXTRACTED'
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-K')
      AND EXISTS (SELECT 1 FROM FILING_SIGNALS sg WHERE sg.ACCESSION_NO = fc.ACCESSION_NO);

    RETURN 'SIGNAL_EXTRACT_10K complete';
END;
$$;

CREATE OR REPLACE PROCEDURE SIGNAL_EXTRACT_10Q()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO FILING_SIGNALS
        (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
         SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
         KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
         EXTRACTION_MODEL, IS_AMENDMENT, EXTRACTION_METHOD, SIGNAL_EXTRACTED_AT)
    WITH source AS (
        SELECT v.ACCESSION_NO, v.COMPANY_NAME, v.TICKER, v.FORM_TYPE,
               v.FILED_AT, v.PERIOD_OF_REPORT, v.IS_AMENDMENT,
               v.INDUSTRY_SECTOR, v.INDUSTRY_TITLE, v.EXCERPT
        FROM V_SIGNAL_EXCERPT v
        JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = v.ACCESSION_NO
        WHERE fc.SIGNAL_STATUS = 'PENDING' AND v.FORM_TYPE = '10-Q'
    ),
    extracted AS (
        SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
            text => s.EXCERPT,
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
        COALESCE(NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'), 'Quarterly Report'),
        COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL'),
        NULLIF(e.ai_result:response:summary::TEXT, 'None'),
        NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None'),
        CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
        CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
        e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, 'arctic-extract', e.IS_AMENDMENT,
        'section_targeted', CURRENT_TIMESTAMP()
    FROM extracted e WHERE e.ai_result IS NOT NULL;

    UPDATE FILING_CONTENT fc SET SIGNAL_STATUS = 'EXTRACTED'
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-Q')
      AND EXISTS (SELECT 1 FROM FILING_SIGNALS sg WHERE sg.ACCESSION_NO = fc.ACCESSION_NO);

    RETURN 'SIGNAL_EXTRACT_10Q complete';
END;
$$;

CREATE OR REPLACE PROCEDURE SIGNAL_EXTRACT_8K()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO FILING_SIGNALS
        (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
         SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
         KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
         EXTRACTION_MODEL, IS_AMENDMENT, EXTRACTION_METHOD, SIGNAL_EXTRACTED_AT)
    WITH source AS (
        SELECT v.ACCESSION_NO, v.COMPANY_NAME, v.TICKER, v.FORM_TYPE,
               v.FILED_AT, v.PERIOD_OF_REPORT, v.IS_AMENDMENT,
               v.INDUSTRY_SECTOR, v.INDUSTRY_TITLE, v.EXCERPT
        FROM V_SIGNAL_EXCERPT v
        JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = v.ACCESSION_NO
        WHERE fc.SIGNAL_STATUS = 'PENDING' AND v.FORM_TYPE NOT IN ('10-K', '10-Q')
    ),
    extracted AS (
        SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
            text => s.EXCERPT,
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
        COALESCE(NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'), 'Current Report'),
        COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL'),
        NULLIF(e.ai_result:response:summary::TEXT, 'None'),
        NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None'),
        CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
        CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
        e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, 'arctic-extract', e.IS_AMENDMENT,
        'raw_first_16k', CURRENT_TIMESTAMP()
    FROM extracted e WHERE e.ai_result IS NOT NULL;

    UPDATE FILING_CONTENT fc SET SIGNAL_STATUS = 'EXTRACTED'
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE NOT IN ('10-K', '10-Q'))
      AND EXISTS (SELECT 1 FROM FILING_SIGNALS sg WHERE sg.ACCESSION_NO = fc.ACCESSION_NO);

    RETURN 'SIGNAL_EXTRACT_8K complete';
END;
$$;


-- =============================================================================
-- ROOT TASK (manual trigger — CRON set to never-fire)
-- =============================================================================

CREATE OR REPLACE TASK T_PROCESSING_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    SCHEDULE = 'USING CRON 0 0 29 2 * UTC'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    COMMENT = 'Root: bulk processing DAG. Trigger via EXECUTE TASK T_PROCESSING_ROOT.'
AS
    SELECT 'PROCESSING_DAG_STARTED' AS status;


-- =============================================================================
-- CHUNKING CHILD TASKS (3 parallel)
-- =============================================================================

CREATE OR REPLACE TASK T_CHUNK_10K
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Bulk chunk 10-K filings (1500 chars, 200 overlap)'
    AFTER T_PROCESSING_ROOT
AS
BEGIN
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
        LENGTH(c.VALUE:chunk_text::VARCHAR) / 4, NULL, NULL
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
    LATERAL FLATTEN(INPUT => CHUNK_FILING(CLEAN_TEXT(fc.CONTENT_TEXT), fi.FORM_TYPE, 1500, 200)) c
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE = '10-K'
      AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

    UPDATE FILING_CONTENT fc
    SET PARSE_STATUS = 'CHUNKED', PROCESSED_AT = CURRENT_TIMESTAMP()
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-K')
      AND EXISTS (SELECT 1 FROM FILING_CHUNKS ck WHERE ck.ACCESSION_NO = fc.ACCESSION_NO);
END;

CREATE OR REPLACE TASK T_CHUNK_10Q
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Bulk chunk 10-Q filings (1500 chars, 200 overlap)'
    AFTER T_PROCESSING_ROOT
AS
BEGIN
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
        LENGTH(c.VALUE:chunk_text::VARCHAR) / 4, NULL, NULL
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
    LATERAL FLATTEN(INPUT => CHUNK_FILING(CLEAN_TEXT(fc.CONTENT_TEXT), fi.FORM_TYPE, 1500, 200)) c
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE = '10-Q'
      AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

    UPDATE FILING_CONTENT fc
    SET PARSE_STATUS = 'CHUNKED', PROCESSED_AT = CURRENT_TIMESTAMP()
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-Q')
      AND EXISTS (SELECT 1 FROM FILING_CHUNKS ck WHERE ck.ACCESSION_NO = fc.ACCESSION_NO);
END;

CREATE OR REPLACE TASK T_CHUNK_8K
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Bulk chunk 8-K and other form types (1500 chars, 200 overlap)'
    AFTER T_PROCESSING_ROOT
AS
BEGIN
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
        LENGTH(c.VALUE:chunk_text::VARCHAR) / 4, NULL, NULL
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
    LATERAL FLATTEN(INPUT => CHUNK_FILING(CLEAN_TEXT(fc.CONTENT_TEXT), fi.FORM_TYPE, 1500, 200)) c
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE NOT IN ('10-K', '10-Q')
      AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

    UPDATE FILING_CONTENT fc
    SET PARSE_STATUS = 'CHUNKED', PROCESSED_AT = CURRENT_TIMESTAMP()
    WHERE fc.PARSE_STATUS = 'PENDING'
      AND EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE NOT IN ('10-K', '10-Q'))
      AND EXISTS (SELECT 1 FROM FILING_CHUNKS ck WHERE ck.ACCESSION_NO = fc.ACCESSION_NO);
END;


-- =============================================================================
-- SIGNAL EXTRACTION CHILD TASKS (3 parallel, CALL SPs)
-- =============================================================================

CREATE OR REPLACE TASK T_SIGNAL_10K
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Signal extraction for 10-K filings (calls SIGNAL_EXTRACT_10K SP)'
    AFTER T_PROCESSING_ROOT
AS
    CALL SIGNAL_EXTRACT_10K();

CREATE OR REPLACE TASK T_SIGNAL_10Q
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Signal extraction for 10-Q filings (calls SIGNAL_EXTRACT_10Q SP)'
    AFTER T_PROCESSING_ROOT
AS
    CALL SIGNAL_EXTRACT_10Q();

CREATE OR REPLACE TASK T_SIGNAL_8K
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Signal extraction for 8-K and other types (calls SIGNAL_EXTRACT_8K SP)'
    AFTER T_PROCESSING_ROOT
AS
    CALL SIGNAL_EXTRACT_8K();


-- =============================================================================
-- KEY METRICS EXTRACTION SP (batched CTAS + UPDATE pattern)
-- =============================================================================
-- AI_COMPLETE with TYPE OBJECT response_format cannot be used inside
-- UPDATE subqueries (Snowflake engine limitation). Workaround: CTAS to temp
-- table, then UPDATE from temp. Batches of 500 with progress saved per batch.
--
-- Default model: llama3.3-70b (44% coverage, ~$17/8K filings)
-- Alternative: change model to 'claude-opus-4-7' for 52% coverage (~$170/8K filings)

CREATE OR REPLACE PROCEDURE EXTRACT_KEY_METRICS_BATCH(BATCH_SIZE INT DEFAULT 500)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_updated INT DEFAULT 0;
    batch_rows INT;
    iteration INT DEFAULT 0;
BEGIN
    LOOP
        iteration := iteration + 1;

        -- Step 1a: Materialize keyword-targeted excerpts (no AI call)
        -- Split from AI_COMPLETE to avoid Snowflake internal error 300010:391167117
        -- which occurs when AI_COMPLETE is nested inside a complex LISTAGG subquery CTAS.
        CREATE OR REPLACE TEMPORARY TABLE _METRICS_EXCERPTS AS
        SELECT fi.ACCESSION_NO,
            LEFT(LISTAGG(
                CASE WHEN LOWER(ck.CHUNK_TEXT) LIKE '%revenue%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%net income%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%net loss%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%earnings per share%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%diluted%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%total net sales%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%in thousands%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%in millions%'
                THEN ck.CHUNK_TEXT END, ' '
            ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 16000) AS excerpt
        FROM FILING_INDEX fi
        JOIN FILING_CHUNKS ck ON ck.ACCESSION_NO = fi.ACCESSION_NO
        JOIN FILING_SIGNALS fs ON fs.ACCESSION_NO = fi.ACCESSION_NO
        WHERE ck.SECTION_NAME IN ('Financial Statements', 'MD&A', 'Results of Operations', 'Financial Statements and Exhibits')
          AND fi.FORM_TYPE IN ('10-K', '10-Q', '8-K')
          AND fs.METRICS_EXTRACTED_AT IS NULL
        GROUP BY fi.ACCESSION_NO
        HAVING excerpt IS NOT NULL AND LENGTH(excerpt) > 200
        LIMIT :BATCH_SIZE;

        -- Check if any rows produced (break before expensive AI call)
        SELECT COUNT(*) INTO :batch_rows FROM _METRICS_EXCERPTS;
        IF (:batch_rows = 0) THEN BREAK; END IF;

        -- Step 1b: Run AI_COMPLETE from flat materialized table
        CREATE OR REPLACE TEMPORARY TABLE _METRICS_BATCH AS
        SELECT e.ACCESSION_NO,
            SNOWFLAKE.CORTEX.AI_COMPLETE(
                model => 'llama3.3-70b',
                prompt => 'Extract key financial metrics from this SEC filing excerpt. Only extract explicitly stated numbers. If a metric is not found, return null.

IMPORTANT: SEC filings state their reporting unit in the financial statement header (e.g., "in thousands", "in millions", or raw dollars if no unit is stated). Include this unit in the revenue and net_income values. Examples:
- If header says "in thousands" and revenue line shows $178,882 → return "$178,882 (in thousands)"
- If header says "in millions" and revenue shows $2,142.3 → return "$2,142.3 million"
- If no unit is stated and revenue shows $10,083,472 → return "$10,083,472"

CRITICAL: A single filing may use DIFFERENT units in different sections (e.g., "in thousands" in Financial Statements tables vs "in millions" in MD&A narrative). When you find a revenue number, use the unit declared in the SAME TABLE or PARAGRAPH as that number. Do NOT mix a number from one section with a unit from a different section. If the filing shows BOTH a raw tabular number (e.g., "1,409,281" in a table headed "in thousands") AND a narrative number (e.g., "$1,409.3 million" in text), prefer the narrative version because it is self-contained with its own unit.

Filing text:
' || e.excerpt,
                response_format => TYPE OBJECT(
                    revenue VARCHAR,
                    net_income VARCHAR,
                    eps VARCHAR,
                    yoy_change VARCHAR
                )
            ) AS metrics_raw
        FROM _METRICS_EXCERPTS e;

        -- Step 2: Update flattened columns + raw KEY_METRICS backup
        UPDATE FILING_SIGNALS fs
        SET KEY_METRICS = m.metrics_raw::VARCHAR,
            REVENUE = NULLIF(m.metrics_raw:revenue::VARCHAR, 'null'),
            NET_INCOME = NULLIF(m.metrics_raw:net_income::VARCHAR, 'null'),
            EPS = NULLIF(m.metrics_raw:eps::VARCHAR, 'null'),
            YOY_CHANGE = NULLIF(m.metrics_raw:yoy_change::VARCHAR, 'null')
        FROM _METRICS_BATCH m
        WHERE fs.ACCESSION_NO = m.ACCESSION_NO
          AND m.metrics_raw IS NOT NULL;

        -- Normalize EPS: extract single per-share value from various formats.
        -- Handles: $X.XX, $(X.XX), $ (X.XX), -$X.XX, ($X.XX), and all with "(in thousands/millions)" suffix.
        -- Parentheses = negative (accounting convention). Ranges and complex structures → NULL.
        -- Output format: "$X.XX" or "$-X.XX" (maintains $ prefix for consistency).
        UPDATE FILING_SIGNALS
        SET EPS_NORMALIZED = CASE
            -- Range pattern (e.g., "$1.05 - $2.12") → NULL
            WHEN EPS LIKE '%$%-%$%' THEN NULL
            -- Zero/dash → NULL
            WHEN REGEXP_REPLACE(REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', ''), '[\\$\\s\\-\\.]', '') = '' THEN NULL
            -- Parenthetical negative: $(0.33), $ (3.52), ($1.23)
            WHEN REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', '') LIKE '%(%)%' THEN
                '$-' || REGEXP_REPLACE(REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', ''), '[^0-9.]', '')
            -- Negative with dash: -$0.50, $-1.25, -0.15
            WHEN REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', '') LIKE '%-%' THEN
                '$-' || REGEXP_REPLACE(REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', ''), '[^0-9.]', '')
            -- Positive: $2.39, 1.50, $0.08 (in thousands)
            ELSE
                '$' || REGEXP_REPLACE(REGEXP_REPLACE(EPS, '\\s*\\(in (thousands|millions)\\)$', ''), '[^0-9.]', '')
        END
        WHERE EPS IS NOT NULL AND EPS_NORMALIZED IS NULL
          AND LENGTH(EPS) <= 30;

        -- EPS validation: NULL out non-parseable values (multi-value lists, text like "N/A", "*")
        UPDATE FILING_SIGNALS
        SET EPS_NORMALIZED = NULL
        WHERE EPS_NORMALIZED IS NOT NULL
          AND TRY_TO_DOUBLE(REPLACE(EPS_NORMALIZED, '$', '')) IS NULL;

        -- Normalize REVENUE: convert to millions USD using the unit stated in the value.
        -- billion → ×1000, million → as-is, thousand → ÷1000, raw dollars → ÷1,000,000
        UPDATE FILING_SIGNALS
        SET REVENUE_NORMALIZED = CASE
            WHEN LOWER(REVENUE) LIKE '%billion%' THEN
                TRY_TO_DOUBLE(REGEXP_REPLACE(REGEXP_SUBSTR(REVENUE, '[-]?[0-9][0-9,.]*'), ',', '')) * 1000
            WHEN LOWER(REVENUE) LIKE '%million%' THEN
                TRY_TO_DOUBLE(REGEXP_REPLACE(REGEXP_SUBSTR(REVENUE, '[-]?[0-9][0-9,.]*'), ',', ''))
            WHEN LOWER(REVENUE) LIKE '%thousand%' THEN
                TRY_TO_DOUBLE(REGEXP_REPLACE(REGEXP_SUBSTR(REVENUE, '[-]?[0-9][0-9,.]*'), ',', '')) / 1000
            ELSE
                TRY_TO_DOUBLE(REGEXP_REPLACE(REGEXP_SUBSTR(REVENUE, '[-]?[0-9][0-9,.]*'), ',', '')) / 1000000
        END
        WHERE REVENUE IS NOT NULL AND REVENUE_NORMALIZED IS NULL;

        -- Validation cap: no company exceeds $750B annual revenue. Values above this
        -- are unit misclassifications (AI confused "in thousands" with "in millions").
        UPDATE FILING_SIGNALS
        SET REVENUE_NORMALIZED = NULL
        WHERE REVENUE_NORMALIZED > 750000;

        -- Mark these filings as metrics-extracted (prevents re-processing)
        UPDATE FILING_SIGNALS
        SET METRICS_EXTRACTED_AT = CURRENT_TIMESTAMP()
        WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM _METRICS_BATCH)
          AND METRICS_EXTRACTED_AT IS NULL;

        rows_updated := rows_updated + SQLROWCOUNT;

        -- Safety: max 50 iterations (50 × 500 = 25,000 filings max)
        IF (:iteration >= 50) THEN BREAK; END IF;
    END LOOP;

    DROP TABLE IF EXISTS _METRICS_EXCERPTS;
    DROP TABLE IF EXISTS _METRICS_BATCH;
    RETURN 'KEY_METRICS extraction complete: ' || :rows_updated || ' rows updated in ' || :iteration || ' batches';
END;
$$;


-- =============================================================================
-- KEY METRICS EXTRACTION (runs after chunking + signal tasks complete)
-- =============================================================================
-- Uses AI_COMPLETE with structured output on section-targeted input (MD&A +
-- Financial Statements chunks) for higher-quality financial metric extraction.
--
-- Default model: llama3.3-70b (44% coverage, $17/8K filings)
-- Alternative: claude-opus-4-7 gives richer context with 52% coverage
--   but costs 10x more (~$170/8K filings). Change model below if desired.

CREATE OR REPLACE TASK T_METRICS_EXTRACT
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Extracts key financial metrics via AI_COMPLETE (llama3.3-70b, structured output)'
    AFTER T_CHUNK_10K, T_CHUNK_10Q, T_CHUNK_8K, T_SIGNAL_10K, T_SIGNAL_10Q, T_SIGNAL_8K
AS CALL EXTRACT_KEY_METRICS_BATCH(500);


-- =============================================================================
-- FORWARD GUIDANCE EXTRACTION (parallel with metrics, after signal tasks)
-- =============================================================================
-- Extracts forward-looking financial guidance from MD&A/Business sections.
-- Targets guidance-specific language (outlook, forecasts, expectations).
-- Separate from metrics extraction because guidance lives in different sections
-- and requires a different AI prompt. Owns FORWARD_GUIDANCE + GUIDANCE_NORMALIZED.

CREATE OR REPLACE PROCEDURE EXTRACT_FORWARD_GUIDANCE_BATCH(BATCH_SIZE INT DEFAULT 500)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_updated INT DEFAULT 0;
    batch_rows INT;
    iteration INT DEFAULT 0;
BEGIN
    LOOP
        iteration := iteration + 1;

        CREATE OR REPLACE TEMPORARY TABLE _GUIDANCE_EXCERPTS AS
        SELECT fi.ACCESSION_NO,
            LEFT(LISTAGG(
                CASE WHEN LOWER(ck.CHUNK_TEXT) LIKE '%we expect%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%outlook%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%we anticipate%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%forecast%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%full year%expect%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%we now expect%'
                     OR LOWER(ck.CHUNK_TEXT) LIKE '%guidance%range%'
                THEN ck.CHUNK_TEXT END, ' '
            ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 16000) AS excerpt
        FROM FILING_INDEX fi
        JOIN FILING_CHUNKS ck ON ck.ACCESSION_NO = fi.ACCESSION_NO
        JOIN FILING_SIGNALS fs ON fs.ACCESSION_NO = fi.ACCESSION_NO
        WHERE ck.SECTION_NAME IN ('MD&A', 'Results of Operations', 'Business')
          AND fi.FORM_TYPE IN ('10-K', '10-Q')
          AND fs.GUIDANCE_EXTRACTED_AT IS NULL
        GROUP BY fi.ACCESSION_NO
        HAVING excerpt IS NOT NULL AND LENGTH(excerpt) > 200
        LIMIT :BATCH_SIZE;

        SELECT COUNT(*) INTO :batch_rows FROM _GUIDANCE_EXCERPTS;
        IF (:batch_rows = 0) THEN BREAK; END IF;

        CREATE OR REPLACE TEMPORARY TABLE _GUIDANCE_BATCH AS
        SELECT e.ACCESSION_NO,
            SNOWFLAKE.CORTEX.AI_COMPLETE(
                model => 'llama3.3-70b',
                prompt => 'Extract forward-looking financial guidance from this SEC filing excerpt. Look for: specific revenue/earnings targets, growth rate expectations, margin guidance, or outlook statements for future periods. Return the exact forward guidance statement as a brief summary. Return null if no forward-looking financial guidance is stated. Do NOT return accounting standards (ASC, GAAP, FASB) as guidance. Do NOT return historical results as guidance.

Filing text:
' || e.excerpt,
                response_format => TYPE OBJECT(guidance VARCHAR)
            ) AS guidance_raw
        FROM _GUIDANCE_EXCERPTS e;

        UPDATE FILING_SIGNALS fs
        SET FORWARD_GUIDANCE = NULLIF(m.guidance_raw:guidance::VARCHAR, 'null'),
            GUIDANCE_NORMALIZED = NULLIF(m.guidance_raw:guidance::VARCHAR, 'null')
        FROM _GUIDANCE_BATCH m
        WHERE fs.ACCESSION_NO = m.ACCESSION_NO
          AND m.guidance_raw IS NOT NULL;

        -- Mark these filings as guidance-extracted (prevents re-processing)
        UPDATE FILING_SIGNALS
        SET GUIDANCE_EXTRACTED_AT = CURRENT_TIMESTAMP()
        WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM _GUIDANCE_BATCH)
          AND GUIDANCE_EXTRACTED_AT IS NULL;

        rows_updated := rows_updated + SQLROWCOUNT;

        IF (:iteration >= 50) THEN BREAK; END IF;
    END LOOP;

    DROP TABLE IF EXISTS _GUIDANCE_EXCERPTS;
    DROP TABLE IF EXISTS _GUIDANCE_BATCH;
    RETURN 'FORWARD_GUIDANCE extraction complete: ' || :rows_updated || ' rows updated in ' || :iteration || ' batches';
END;
$$;

CREATE OR REPLACE TASK T_GUIDANCE_EXTRACT
    WAREHOUSE = IDENTIFIER($config_warehouse_build)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Extracts forward guidance from MD&A sections via AI_COMPLETE'
    AFTER T_CHUNK_10K, T_CHUNK_10Q, T_CHUNK_8K, T_SIGNAL_10K, T_SIGNAL_10Q, T_SIGNAL_8K
AS CALL EXTRACT_FORWARD_GUIDANCE_BATCH(500);


-- =============================================================================
-- EVENT TYPE NORMALIZATION (runs after signal extraction, parallel with metrics/guidance)
-- =============================================================================
-- Maps 97+ hallucinated EVENT_TYPE values to 12 canonical categories.
-- Idempotent: only updates rows where EVENT_TYPE_NORMALIZED IS NULL.

CREATE OR REPLACE TASK T_NORMALIZE_SIGNALS
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 600000
    COMMENT = 'Normalizes EVENT_TYPE to 12 canonical categories'
    AFTER T_SIGNAL_10K, T_SIGNAL_10Q, T_SIGNAL_8K
AS
BEGIN
    UPDATE FILING_SIGNALS
    SET EVENT_TYPE_NORMALIZED = CASE
        WHEN EVENT_TYPE IN ('Earnings', 'M&A', 'Leadership Change', 'Risk Disclosure', 
                            'Guidance Update', 'Regulatory', 'Capital Markets', 'Bankruptcy',
                            'Annual Report', 'Quarterly Report', 'Current Report', 'Other') 
            THEN EVENT_TYPE
        WHEN EVENT_TYPE ILIKE '%acqui%' OR EVENT_TYPE ILIKE '%merger%' OR EVENT_TYPE ILIKE '%disposition%'
            THEN 'M&A'
        WHEN EVENT_TYPE ILIKE '%change in control%' OR EVENT_TYPE ILIKE '%change of control%'
            THEN 'M&A'
        WHEN EVENT_TYPE ILIKE '%leadership%' OR EVENT_TYPE ILIKE '%chief%' OR EVENT_TYPE ILIKE '%officer%'
            THEN 'Leadership Change'
        WHEN EVENT_TYPE ILIKE '%regulation%' OR EVENT_TYPE ILIKE '%compliance%' OR EVENT_TYPE ILIKE '%sanction%'
            OR EVENT_TYPE ILIKE '%mine safety%' OR EVENT_TYPE ILIKE '%ESG%' OR EVENT_TYPE ILIKE '%audit%'
            OR EVENT_TYPE ILIKE '%accountant%' OR EVENT_TYPE ILIKE '%accounting%'
            THEN 'Regulatory'
        WHEN EVENT_TYPE ILIKE '%dividend%' OR EVENT_TYPE ILIKE '%issuance%' OR EVENT_TYPE ILIKE '%notes%'
            OR EVENT_TYPE ILIKE '%repurchase%' OR EVENT_TYPE ILIKE '%capital%' OR EVENT_TYPE ILIKE '%credit%'
            OR EVENT_TYPE ILIKE '%loan%' OR EVENT_TYPE ILIKE '%euro%'
            THEN 'Capital Markets'
        WHEN EVENT_TYPE ILIKE '%guidance%' OR EVENT_TYPE ILIKE '%forward%look%' OR EVENT_TYPE ILIKE '%outlook%'
            OR EVENT_TYPE ILIKE '%update%'
            THEN 'Guidance Update'
        WHEN EVENT_TYPE ILIKE '%risk%'
            THEN 'Risk Disclosure'
        WHEN EVENT_TYPE ILIKE '%bankrupt%' OR EVENT_TYPE ILIKE '%shell%'
            THEN 'Bankruptcy'
        ELSE 'Other'
    END
    WHERE EVENT_TYPE_NORMALIZED IS NULL;
END;


-- =============================================================================
-- INDUSTRY PROPAGATION (runs after chunking + signal + metrics + normalization)
-- =============================================================================
-- Propagates INDUSTRY_SECTOR + INDUSTRY_TITLE from FILING_INDEX to downstream
-- tables (FILING_CHUNKS and FILING_SIGNALS) after processing completes.

CREATE OR REPLACE TASK T_PROPAGATE_INDUSTRY
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Propagates INDUSTRY_SECTOR/TITLE + TICKER from FILING_INDEX to chunks and signals'
    AFTER T_CHUNK_10K, T_CHUNK_10Q, T_CHUNK_8K, T_SIGNAL_10K, T_SIGNAL_10Q, T_SIGNAL_8K, T_METRICS_EXTRACT, T_NORMALIZE_SIGNALS
AS
BEGIN
    -- Propagate industry to chunks
    UPDATE FILING_CHUNKS fc
    SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR,
        INDUSTRY_TITLE = fi.INDUSTRY_TITLE
    FROM FILING_INDEX fi
    WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
      AND fi.INDUSTRY_SECTOR IS NOT NULL
      AND fc.INDUSTRY_SECTOR IS NULL;

    -- Propagate industry to signals
    UPDATE FILING_SIGNALS fs
    SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR,
        INDUSTRY_TITLE = fi.INDUSTRY_TITLE
    FROM FILING_INDEX fi
    WHERE fs.ACCESSION_NO = fi.ACCESSION_NO
      AND fi.INDUSTRY_SECTOR IS NOT NULL
      AND fs.INDUSTRY_SECTOR IS NULL;

    -- Propagate ticker to chunks
    UPDATE FILING_CHUNKS fc
    SET TICKER = fi.TICKER
    FROM FILING_INDEX fi
    WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
      AND fi.TICKER IS NOT NULL
      AND fc.TICKER IS NULL;

    -- Propagate ticker to signals
    UPDATE FILING_SIGNALS fs
    SET TICKER = fi.TICKER
    FROM FILING_INDEX fi
    WHERE fs.ACCESSION_NO = fi.ACCESSION_NO
      AND fi.TICKER IS NOT NULL
      AND fs.TICKER IS NULL;

    RETURN 'Industry + ticker propagation complete';
END;


-- =============================================================================
-- CORTEX SEARCH REFRESH (after chunking + industry propagation)
-- =============================================================================
-- Triggers incremental refresh of the search service. Only new/changed chunks
-- are embedded. If service doesn't exist (first run), creates it.
-- Fires after chunking + propagation — does NOT wait for signal extraction.

CREATE OR REPLACE TASK T_REFRESH_SEARCH
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Incremental search refresh after chunking. Creates service on first run.'
    AFTER T_PROPAGATE_INDUSTRY
AS
BEGIN
    LET svc_name VARCHAR := _CFG('search_service');
    LET svc_exists INT := 0;

    SHOW CORTEX SEARCH SERVICES LIKE :svc_name IN SCHEMA;
    SELECT COUNT(*) INTO :svc_exists FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    IF (:svc_exists = 0) THEN
        EXECUTE IMMEDIATE '
            CREATE CORTEX SEARCH SERVICE ' || :svc_name || '
                TEXT INDEXES CHUNK_TEXT, CHUNK_ID, ACCESSION_NO
                VECTOR INDEXES CHUNK_TEXT (model=''snowflake-arctic-embed-m-v1.5'')
                PRIMARY KEY (CHUNK_ID)
                ATTRIBUTES COMPANY_NAME, TICKER, FORM_TYPE, SECTION_NAME, FILED_AT, PERIOD_OF_REPORT, INDUSTRY_SECTOR, INDUSTRY_TITLE, CHUNK_ID, ACCESSION_NO
                WAREHOUSE = ' || _CFG('warehouse') || '
                TARGET_LAG = ''1 day''
                COMMENT = ''SEC filing semantic search (multi-index, Arctic M-v1.5, incremental refresh)''
            AS (
                SELECT
                    CHUNK_ID, CHUNK_TEXT, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE, SECTION_NAME,
                    TO_VARCHAR(FILED_AT, ''YYYY-MM-DD'') AS FILED_AT,
                    TO_VARCHAR(PERIOD_OF_REPORT, ''YYYY-MM-DD'') AS PERIOD_OF_REPORT,
                    COALESCE(INDUSTRY_SECTOR, ''Other'') AS INDUSTRY_SECTOR,
                    INDUSTRY_TITLE
                FROM FILING_CHUNKS
                WHERE CHUNK_TEXT IS NOT NULL AND LENGTH(CHUNK_TEXT) > 100
            )';
        RETURN 'Search service CREATED (first time): ' || :svc_name;
    ELSE
        EXECUTE IMMEDIATE 'ALTER CORTEX SEARCH SERVICE ' || :svc_name || ' RESUME INDEXING';
        EXECUTE IMMEDIATE 'ALTER CORTEX SEARCH SERVICE ' || :svc_name || ' REFRESH';
        RETURN 'Search service REFRESHED (incremental): ' || :svc_name;
    END IF;
END;


CREATE OR REPLACE TASK T_WAIT_SEARCH_ACTIVE
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Polls until Cortex Search serving_state = ACTIVE after refresh'
    AFTER T_REFRESH_SEARCH
AS
BEGIN
    LET serving_state VARCHAR;
    LET iteration INT DEFAULT 0;
    LET svc_name VARCHAR := _CFG('search_service');

    LOOP
        iteration := iteration + 1;
        SHOW CORTEX SEARCH SERVICES LIKE :svc_name IN SCHEMA;
        SELECT "serving_state" INTO :serving_state
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

        IF (:serving_state = 'ACTIVE') THEN BREAK; END IF;
        IF (:iteration >= 720) THEN
            RETURN 'TIMEOUT waiting for search service after ' || :iteration || ' iterations';
        END IF;
        CALL SYSTEM$WAIT(300);
    END LOOP;

    RETURN 'Search service ACTIVE after ' || :iteration || ' polls';
END;


-- =============================================================================
-- FINALIZER (runs after ALL children complete or fail)
-- =============================================================================

CREATE OR REPLACE TASK T_PROCESSING_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = T_PROCESSING_ROOT
    COMMENT = 'Finalizer: emails pipeline summary with eval instructions'
AS
BEGIN
    LET total_chunks INT;
    LET total_signals INT;
    LET pending_chunk INT;
    LET pending_signal INT;
    LET with_revenue INT;

    SELECT COUNT(*) INTO :total_chunks FROM FILING_CHUNKS;
    SELECT COUNT(*) INTO :total_signals FROM FILING_SIGNALS;
    SELECT COUNT(*) INTO :pending_chunk FROM FILING_CONTENT WHERE PARSE_STATUS = 'PENDING';
    SELECT COUNT(*) INTO :pending_signal FROM FILING_CONTENT WHERE SIGNAL_STATUS = 'PENDING';
    SELECT COUNT(REVENUE) INTO :with_revenue FROM FILING_SIGNALS;

    LET status VARCHAR := IFF(:pending_chunk = 0 AND :pending_signal = 0, 'COMPLETE', 'PARTIAL');

    LET msg VARCHAR := 'PROCESSING + SEARCH REFRESH: ' || :status || CHR(10) || CHR(10) ||
        'Chunks: ' || :total_chunks::VARCHAR || CHR(10) ||
        'Signals: ' || :total_signals::VARCHAR || CHR(10) ||
        'With Revenue: ' || :with_revenue::VARCHAR || CHR(10) ||
        'Pending (chunk): ' || :pending_chunk::VARCHAR || CHR(10) ||
        'Pending (signal): ' || :pending_signal::VARCHAR || CHR(10) ||
        'Search: refreshed (incremental)' || CHR(10) ||
        'Analyst: ready (semantic view queries live data)' || CHR(10) || CHR(10) ||
        'RUN EVAL (interactive session required):' || CHR(10) ||
        '  1. CALL _RUN_EVAL();' || CHR(10) ||
        '  2. EXECUTE TASK EVAL_DAG_ROOT;' || CHR(10) ||
        '  3. Results: SELECT * FROM EVAL_RESULTS ORDER BY CAPTURED_AT DESC;' || CHR(10) ||
        '  Reference: sql/06_agent/02_eval_framework.sql' || CHR(10) || CHR(10) ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR;

    CALL SYSTEM$SEND_EMAIL(
        _CFG('email_integration'),
        _CFG('email_recipient'),
        'SEC Filing Pipeline: ' || :status || ' — Run Eval Next',
        :msg
    );
END;


-- =============================================================================
-- ENABLE THE DAG (Resume all tasks — required before EXECUTE TASK)
-- =============================================================================

ALTER TASK T_CHUNK_10K RESUME;
ALTER TASK T_CHUNK_10Q RESUME;
ALTER TASK T_CHUNK_8K RESUME;
ALTER TASK T_SIGNAL_10K RESUME;
ALTER TASK T_SIGNAL_10Q RESUME;
ALTER TASK T_SIGNAL_8K RESUME;
ALTER TASK T_METRICS_EXTRACT RESUME;
ALTER TASK T_GUIDANCE_EXTRACT RESUME;
ALTER TASK T_PROPAGATE_INDUSTRY RESUME;
ALTER TASK T_REFRESH_SEARCH RESUME;
ALTER TASK T_WAIT_SEARCH_ACTIVE RESUME;
ALTER TASK T_PROCESSING_FINALIZER RESUME;
ALTER TASK T_PROCESSING_ROOT RESUME;


-- =============================================================================
-- TRIGGER EXECUTION
-- =============================================================================
-- Uncomment to fire the DAG immediately:
-- EXECUTE TASK T_PROCESSING_ROOT;


-- =============================================================================
-- MONITORING
-- =============================================================================

-- Task execution history:
-- SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, RETURN_VALUE, ERROR_MESSAGE
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
--     RESULT_LIMIT => 20
-- ))
-- WHERE ROOT_TASK_ID = (SELECT SYSTEM$TASK_FIND_ROOT_ID('T_PROCESSING_ROOT'))
-- ORDER BY SCHEDULED_TIME DESC;

-- Pipeline progress (chunking):
-- SELECT PARSE_STATUS, COUNT(*) AS filing_count FROM FILING_CONTENT GROUP BY 1;

-- Pipeline progress (signals):
-- SELECT SIGNAL_STATUS, COUNT(*) AS filing_count FROM FILING_CONTENT GROUP BY 1;

-- Chunk stats:
-- SELECT FORM_TYPE, COUNT(DISTINCT ACCESSION_NO) AS filings, COUNT(*) AS chunks,
--        ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT ACCESSION_NO), 0), 1) AS avg_per_filing
-- FROM FILING_CHUNKS GROUP BY 1 ORDER BY 1;
