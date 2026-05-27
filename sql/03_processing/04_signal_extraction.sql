-- =============================================================================
-- 04: Bulk Signal Extraction (REFERENCE — manual execution)
-- =============================================================================
-- NOTE: For production use, use 05_processing_task_dag.sql instead.
-- This file contains the raw SQL for manual execution in separate sessions.
-- The task DAG provides: auto-retry, 48h timeout, email notification, and
-- parallel chunking + signal extraction without Snowsight session dependency.
-- =============================================================================
-- Bulk set-based INSERT for AI signal extraction across all filings.
-- Decoupled from chunking — reads CONTENT_TEXT directly, uses SIGNAL_STATUS.
--
-- Strategy:
--   - Run 3 concurrent sessions, partitioned by FORM_TYPE
--   - Runs in PARALLEL with chunking (03_chunking_pipeline.sql)
--   - Uses SIGNAL_STATUS column on FILING_CONTENT for tracking
--   - Uses build warehouse for maximum parallelism
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- BULK PROCESSING: Run these 3 queries in SEPARATE SESSIONS concurrently
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Session A: 10-K signals
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_SIGNALS
    (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
     KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
     EXTRACTION_MODEL, IS_AMENDMENT)
WITH source AS (
    SELECT
        fc.ACCESSION_NO,
        fi.COMPANY_NAME,
        fi.TICKER,
        fi.FORM_TYPE,
        fi.FILED_AT,
        fi.PERIOD_OF_REPORT,
        fi.IS_AMENDMENT,
        LEFT(CLEAN_TEXT(fc.CONTENT_TEXT), 16000) AS excerpt
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE = '10-K'
),
extracted AS (
    SELECT
        s.*,
        SNOWFLAKE.CORTEX.AI_EXTRACT(
            text          => s.excerpt,
            responseFormat => {
                'event_type':    'string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other',
                'sentiment':     'string - one of: POSITIVE, NEGATIVE, NEUTRAL, MIXED',
                'summary':       'string - 2-3 sentence plain-English summary of the most material information in this filing',
                'key_metrics':   'object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change',
                'risk_flags':    'array of strings - specific risk categories explicitly mentioned',
                'material_items':'array of strings - for 8-Ks only: list of Item numbers reported'
            }
        ) AS ai_result
    FROM source s
)
SELECT
    e.ACCESSION_NO || '_sig'                        AS SIGNAL_ID,
    e.ACCESSION_NO,
    e.COMPANY_NAME,
    e.TICKER,
    e.FORM_TYPE,
    e.FILED_AT                                      AS SIGNAL_DATE,
    e.PERIOD_OF_REPORT,
    COALESCE(
        NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'),
        CASE
            WHEN e.FORM_TYPE = '10-K' THEN 'Annual Report'
            WHEN e.FORM_TYPE = '10-Q' THEN 'Quarterly Report'
            WHEN e.FORM_TYPE = '8-K'  THEN 'Current Report'
            ELSE 'Other'
        END
    )                                               AS EVENT_TYPE,
    COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL') AS SENTIMENT,
    NULLIF(e.ai_result:response:summary::TEXT, 'None')      AS SUMMARY,
    NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None') AS KEY_METRICS,
    CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL
         ELSE e.ai_result:response:risk_flags::ARRAY END AS RISK_FLAGS,
    CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL
         ELSE e.ai_result:response:material_items::ARRAY END AS MATERIAL_ITEMS,
    NULL                                            AS INDUSTRY_SECTOR,
    NULL                                            AS INDUSTRY_TITLE,
    'arctic-extract'                                AS EXTRACTION_MODEL,
    e.IS_AMENDMENT
FROM extracted e
WHERE e.ai_result IS NOT NULL;

-- Mark 10-K filings as EXTRACTED
UPDATE FILING_CONTENT fc
SET    SIGNAL_STATUS = 'EXTRACTED'
WHERE  fc.SIGNAL_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-K'
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_SIGNALS sg
           WHERE sg.ACCESSION_NO = fc.ACCESSION_NO
       );


-- ---------------------------------------------------------------------------
-- Session B: 10-Q signals
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_SIGNALS
    (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
     KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
     EXTRACTION_MODEL, IS_AMENDMENT)
WITH source AS (
    SELECT
        fc.ACCESSION_NO,
        fi.COMPANY_NAME,
        fi.TICKER,
        fi.FORM_TYPE,
        fi.FILED_AT,
        fi.PERIOD_OF_REPORT,
        fi.IS_AMENDMENT,
        LEFT(CLEAN_TEXT(fc.CONTENT_TEXT), 16000) AS excerpt
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE = '10-Q'
),
extracted AS (
    SELECT
        s.*,
        SNOWFLAKE.CORTEX.AI_EXTRACT(
            text          => s.excerpt,
            responseFormat => {
                'event_type':    'string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other',
                'sentiment':     'string - one of: POSITIVE, NEGATIVE, NEUTRAL, MIXED',
                'summary':       'string - 2-3 sentence plain-English summary of the most material information in this filing',
                'key_metrics':   'object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change',
                'risk_flags':    'array of strings - specific risk categories explicitly mentioned',
                'material_items':'array of strings - for 8-Ks only: list of Item numbers reported'
            }
        ) AS ai_result
    FROM source s
)
SELECT
    e.ACCESSION_NO || '_sig',
    e.ACCESSION_NO,
    e.COMPANY_NAME,
    e.TICKER,
    e.FORM_TYPE,
    e.FILED_AT,
    e.PERIOD_OF_REPORT,
    COALESCE(
        NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'),
        CASE
            WHEN e.FORM_TYPE = '10-K' THEN 'Annual Report'
            WHEN e.FORM_TYPE = '10-Q' THEN 'Quarterly Report'
            WHEN e.FORM_TYPE = '8-K'  THEN 'Current Report'
            ELSE 'Other'
        END
    ),
    COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL'),
    NULLIF(e.ai_result:response:summary::TEXT, 'None'),
    NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None'),
    CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
    CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
    NULL,
    NULL,
    'arctic-extract',
    e.IS_AMENDMENT
FROM extracted e
WHERE e.ai_result IS NOT NULL;

-- Mark 10-Q filings as EXTRACTED
UPDATE FILING_CONTENT fc
SET    SIGNAL_STATUS = 'EXTRACTED'
WHERE  fc.SIGNAL_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-Q'
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_SIGNALS sg
           WHERE sg.ACCESSION_NO = fc.ACCESSION_NO
       );


-- ---------------------------------------------------------------------------
-- Session C: 8-K and other form types
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_SIGNALS
    (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
     KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
     EXTRACTION_MODEL, IS_AMENDMENT)
WITH source AS (
    SELECT
        fc.ACCESSION_NO,
        fi.COMPANY_NAME,
        fi.TICKER,
        fi.FORM_TYPE,
        fi.FILED_AT,
        fi.PERIOD_OF_REPORT,
        fi.IS_AMENDMENT,
        LEFT(CLEAN_TEXT(fc.CONTENT_TEXT), 16000) AS excerpt
    FROM FILING_CONTENT fc
    JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
    WHERE fc.SIGNAL_STATUS = 'PENDING'
      AND fc.CONTENT_TEXT IS NOT NULL
      AND fi.FORM_TYPE NOT IN ('10-K', '10-Q')
),
extracted AS (
    SELECT
        s.*,
        SNOWFLAKE.CORTEX.AI_EXTRACT(
            text          => s.excerpt,
            responseFormat => {
                'event_type':    'string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other',
                'sentiment':     'string - one of: POSITIVE, NEGATIVE, NEUTRAL, MIXED',
                'summary':       'string - 2-3 sentence plain-English summary of the most material information in this filing',
                'key_metrics':   'object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change',
                'risk_flags':    'array of strings - specific risk categories explicitly mentioned',
                'material_items':'array of strings - for 8-Ks only: list of Item numbers reported'
            }
        ) AS ai_result
    FROM source s
)
SELECT
    e.ACCESSION_NO || '_sig',
    e.ACCESSION_NO,
    e.COMPANY_NAME,
    e.TICKER,
    e.FORM_TYPE,
    e.FILED_AT,
    e.PERIOD_OF_REPORT,
    COALESCE(
        NULLIF(e.ai_result:response:event_type::VARCHAR, 'None'),
        CASE
            WHEN e.FORM_TYPE = '10-K' THEN 'Annual Report'
            WHEN e.FORM_TYPE = '10-Q' THEN 'Quarterly Report'
            WHEN e.FORM_TYPE = '8-K'  THEN 'Current Report'
            ELSE 'Other'
        END
    ),
    COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, 'None'), 'NEUTRAL'),
    NULLIF(e.ai_result:response:summary::TEXT, 'None'),
    NULLIF(e.ai_result:response:key_metrics::VARCHAR, 'None'),
    CASE WHEN e.ai_result:response:risk_flags::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
    CASE WHEN e.ai_result:response:material_items::VARCHAR = 'None' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
    NULL,
    NULL,
    'arctic-extract',
    e.IS_AMENDMENT
FROM extracted e
WHERE e.ai_result IS NOT NULL;

-- Mark remaining filings as EXTRACTED
UPDATE FILING_CONTENT fc
SET    SIGNAL_STATUS = 'EXTRACTED'
WHERE  fc.SIGNAL_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE NOT IN ('10-K', '10-Q')
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_SIGNALS sg
           WHERE sg.ACCESSION_NO = fc.ACCESSION_NO
       );


-- =============================================================================
-- MONITORING: Check progress from any session
-- =============================================================================

-- SELECT SIGNAL_STATUS, COUNT(*) AS filing_count FROM FILING_CONTENT GROUP BY 1 ORDER BY 1;
-- SELECT FORM_TYPE, EVENT_TYPE, COUNT(*) AS signal_count
-- FROM FILING_SIGNALS GROUP BY 1, 2 ORDER BY 1, 3 DESC;
