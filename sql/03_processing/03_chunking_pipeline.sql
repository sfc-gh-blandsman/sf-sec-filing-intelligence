-- =============================================================================
-- 03: Bulk Chunking Pipeline (REFERENCE — manual execution)
-- =============================================================================
-- NOTE: For production use, use 05_processing_task_dag.sql instead.
-- This file contains the raw SQL for manual execution in separate sessions.
-- The task DAG provides: auto-retry, 48h timeout, email notification, and
-- parallel chunking + signal extraction without Snowsight session dependency.
-- =============================================================================
-- Bulk set-based INSERT for chunking all filings in parallel sessions.
-- No stored embeddings — Cortex Search auto-embeds at index time.
--
-- Strategy:
--   - Run 3 concurrent sessions, partitioned by FORM_TYPE
--   - Single INSERT...SELECT per session (no SP loop)
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
-- Session 1: 10-K filings
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_CHUNKS
    (CHUNK_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     FILED_AT, PERIOD_OF_REPORT, SECTION_NAME, CHUNK_INDEX,
     CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR, INDUSTRY_TITLE)
SELECT
    fi.ACCESSION_NO || '_' || c.VALUE:chunk_index::VARCHAR   AS CHUNK_ID,
    fi.ACCESSION_NO,
    fi.COMPANY_NAME,
    fi.TICKER,
    fi.FORM_TYPE,
    fi.FILED_AT,
    fi.PERIOD_OF_REPORT,
    c.VALUE:section_name::VARCHAR                             AS SECTION_NAME,
    c.VALUE:chunk_index::INT                                  AS CHUNK_INDEX,
    c.VALUE:chunk_text::VARCHAR                               AS CHUNK_TEXT,
    LENGTH(c.VALUE:chunk_text::VARCHAR) / 4                  AS TOKEN_COUNT,
    NULL                                                      AS INDUSTRY_SECTOR,
    NULL                                                      AS INDUSTRY_TITLE
FROM FILING_CONTENT fc
JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
LATERAL FLATTEN(
    INPUT => CHUNK_FILING(
        CLEAN_TEXT(fc.CONTENT_TEXT),
        fi.FORM_TYPE,
        1500,
        200
    )
) c
WHERE fc.PARSE_STATUS = 'PENDING'
  AND fc.CONTENT_TEXT IS NOT NULL
  AND fi.FORM_TYPE = '10-K'
  AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

-- Mark 10-K filings as CHUNKED
UPDATE FILING_CONTENT fc
SET    PARSE_STATUS = 'CHUNKED',
       PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE  fc.PARSE_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-K'
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_CHUNKS ck
           WHERE ck.ACCESSION_NO = fc.ACCESSION_NO
       );


-- ---------------------------------------------------------------------------
-- Session 2: 10-Q filings
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_CHUNKS
    (CHUNK_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     FILED_AT, PERIOD_OF_REPORT, SECTION_NAME, CHUNK_INDEX,
     CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR, INDUSTRY_TITLE)
SELECT
    fi.ACCESSION_NO || '_' || c.VALUE:chunk_index::VARCHAR   AS CHUNK_ID,
    fi.ACCESSION_NO,
    fi.COMPANY_NAME,
    fi.TICKER,
    fi.FORM_TYPE,
    fi.FILED_AT,
    fi.PERIOD_OF_REPORT,
    c.VALUE:section_name::VARCHAR                             AS SECTION_NAME,
    c.VALUE:chunk_index::INT                                  AS CHUNK_INDEX,
    c.VALUE:chunk_text::VARCHAR                               AS CHUNK_TEXT,
    LENGTH(c.VALUE:chunk_text::VARCHAR) / 4                  AS TOKEN_COUNT,
    NULL                                                      AS INDUSTRY_SECTOR,
    NULL                                                      AS INDUSTRY_TITLE
FROM FILING_CONTENT fc
JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
LATERAL FLATTEN(
    INPUT => CHUNK_FILING(
        CLEAN_TEXT(fc.CONTENT_TEXT),
        fi.FORM_TYPE,
        1500,
        200
    )
) c
WHERE fc.PARSE_STATUS = 'PENDING'
  AND fc.CONTENT_TEXT IS NOT NULL
  AND fi.FORM_TYPE = '10-Q'
  AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

-- Mark 10-Q filings as CHUNKED
UPDATE FILING_CONTENT fc
SET    PARSE_STATUS = 'CHUNKED',
       PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE  fc.PARSE_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE = '10-Q'
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_CHUNKS ck
           WHERE ck.ACCESSION_NO = fc.ACCESSION_NO
       );


-- ---------------------------------------------------------------------------
-- Session 3: 8-K and other form types
-- ---------------------------------------------------------------------------
USE WAREHOUSE IDENTIFIER($config_warehouse_build);

INSERT INTO FILING_CHUNKS
    (CHUNK_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
     FILED_AT, PERIOD_OF_REPORT, SECTION_NAME, CHUNK_INDEX,
     CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR, INDUSTRY_TITLE)
SELECT
    fi.ACCESSION_NO || '_' || c.VALUE:chunk_index::VARCHAR   AS CHUNK_ID,
    fi.ACCESSION_NO,
    fi.COMPANY_NAME,
    fi.TICKER,
    fi.FORM_TYPE,
    fi.FILED_AT,
    fi.PERIOD_OF_REPORT,
    c.VALUE:section_name::VARCHAR                             AS SECTION_NAME,
    c.VALUE:chunk_index::INT                                  AS CHUNK_INDEX,
    c.VALUE:chunk_text::VARCHAR                               AS CHUNK_TEXT,
    LENGTH(c.VALUE:chunk_text::VARCHAR) / 4                  AS TOKEN_COUNT,
    NULL                                                      AS INDUSTRY_SECTOR,
    NULL                                                      AS INDUSTRY_TITLE
FROM FILING_CONTENT fc
JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
LATERAL FLATTEN(
    INPUT => CHUNK_FILING(
        CLEAN_TEXT(fc.CONTENT_TEXT),
        fi.FORM_TYPE,
        1500,
        200
    )
) c
WHERE fc.PARSE_STATUS = 'PENDING'
  AND fc.CONTENT_TEXT IS NOT NULL
  AND fi.FORM_TYPE NOT IN ('10-K', '10-Q')
  AND c.VALUE:chunk_text::VARCHAR IS NOT NULL;

-- Mark remaining filings as CHUNKED
UPDATE FILING_CONTENT fc
SET    PARSE_STATUS = 'CHUNKED',
       PROCESSED_AT = CURRENT_TIMESTAMP()
WHERE  fc.PARSE_STATUS = 'PENDING'
  AND  EXISTS (
           SELECT 1 FROM FILING_INDEX fi
           WHERE fi.ACCESSION_NO = fc.ACCESSION_NO AND fi.FORM_TYPE NOT IN ('10-K', '10-Q')
       )
  AND  EXISTS (
           SELECT 1 FROM FILING_CHUNKS ck
           WHERE ck.ACCESSION_NO = fc.ACCESSION_NO
       );


-- =============================================================================
-- MONITORING: Check progress from any session
-- =============================================================================

-- SELECT PARSE_STATUS, COUNT(*) AS filing_count
-- FROM FILING_CONTENT GROUP BY 1 ORDER BY 1;

-- SELECT FORM_TYPE, COUNT(DISTINCT ACCESSION_NO) AS filings_chunked,
--        COUNT(*) AS total_chunks
-- FROM FILING_CHUNKS GROUP BY 1 ORDER BY 1;
