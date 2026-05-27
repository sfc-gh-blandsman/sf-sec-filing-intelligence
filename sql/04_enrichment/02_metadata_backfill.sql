-- =============================================================================
-- 02: Metadata Backfill (Period of Report + Industry)
-- =============================================================================
-- Extracts PERIOD_OF_REPORT and Industry (SIC code) from the <SEC-HEADER>
-- section of each filing, then propagates to downstream tables.
--
-- Coverage:
--   - PERIOD_OF_REPORT: ~100% of filings (from CONFORMED PERIOD OF REPORT)
--   - INDUSTRY_SECTOR + INDUSTRY_TITLE (SIC -> sector): ~98% of filings (from STANDARD INDUSTRIAL CLASSIFICATION)
--
-- Cost: $0 (pure SQL regex extraction, no AI functions)
-- Idempotent: Safe to re-run (overwrites existing values)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Step 1: Extract PERIOD_OF_REPORT into FILING_INDEX
-- =============================================================================

UPDATE FILING_INDEX fi
SET PERIOD_OF_REPORT = TRY_TO_DATE(
    REGEXP_SUBSTR(fc.CONTENT_TEXT, 'CONFORMED PERIOD OF REPORT:\\s*(\\d{8})', 1, 1, 'e'),
    'YYYYMMDD'
)
FROM FILING_CONTENT fc
WHERE fi.ACCESSION_NO = fc.ACCESSION_NO
  AND fc.CONTENT_TEXT IS NOT NULL;


-- =============================================================================
-- Step 2: Extract SIC code and map to Industry sector via SIC_CODES table
-- =============================================================================
-- Requires: 00_sic_reference_data.sql has been run (SIC_CODES table exists)
-- Handles both TXT format (plain-text headers) and FEED format (SGML tags)

UPDATE FILING_INDEX fi
SET SIC_CODE = sic_raw.sic_code,
    INDUSTRY_SECTOR = COALESCE(ref.SECTOR, 'Other'),
    INDUSTRY_TITLE = COALESCE(ref.INDUSTRY_TITLE, 'Other')
FROM (
    SELECT
        fc.ACCESSION_NO,
        LPAD(COALESCE(
            -- TXT format: "STANDARD INDUSTRIAL CLASSIFICATION: DESCRIPTION [1234]"
            REGEXP_SUBSTR(fc.CONTENT_TEXT, 'STANDARD INDUSTRIAL CLASSIFICATION:.*\\[(\\d+)\\]', 1, 1, 'e'),
            -- FEED/SGML format: "<ASSIGNED-SIC>1234"
            REGEXP_SUBSTR(fc.CONTENT_TEXT, '<ASSIGNED-SIC>(\\d+)', 1, 1, 'e')
        ), 4, '0') AS sic_code
    FROM FILING_CONTENT fc
    WHERE fc.CONTENT_TEXT IS NOT NULL
) sic_raw
LEFT JOIN SIC_CODES ref ON ref.SIC_CODE = sic_raw.sic_code
WHERE fi.ACCESSION_NO = sic_raw.ACCESSION_NO
  AND sic_raw.sic_code IS NOT NULL;

-- Step 2b: Map INDUSTRY_SECTOR for filings where SIC_CODE was set at ingestion
-- (Feed-loaded filings have SIC_CODE already populated but no INDUSTRY_SECTOR)
UPDATE FILING_INDEX fi
SET INDUSTRY_SECTOR = COALESCE(ref.SECTOR, 'Other'),
    INDUSTRY_TITLE = COALESCE(ref.INDUSTRY_TITLE, 'Other')
FROM SIC_CODES ref
WHERE fi.SIC_CODE = ref.SIC_CODE
  AND fi.SIC_CODE IS NOT NULL
  AND fi.INDUSTRY_SECTOR IS NULL;


-- =============================================================================
-- Step 3: Propagate PERIOD_OF_REPORT to FILING_SIGNALS
-- =============================================================================

UPDATE FILING_SIGNALS fs
SET PERIOD_OF_REPORT = fi.PERIOD_OF_REPORT
FROM FILING_INDEX fi
WHERE fs.ACCESSION_NO = fi.ACCESSION_NO
  AND fi.PERIOD_OF_REPORT IS NOT NULL;


-- =============================================================================
-- Step 4: Propagate INDUSTRY_SECTOR + INDUSTRY_TITLE to FILING_SIGNALS
-- =============================================================================

UPDATE FILING_SIGNALS fs
SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR,
    INDUSTRY_TITLE = fi.INDUSTRY_TITLE
FROM FILING_INDEX fi
WHERE fs.ACCESSION_NO = fi.ACCESSION_NO
  AND fi.INDUSTRY_SECTOR IS NOT NULL;


-- =============================================================================
-- Step 5: Propagate INDUSTRY_SECTOR + INDUSTRY_TITLE + PERIOD_OF_REPORT to FILING_CHUNKS
-- =============================================================================

UPDATE FILING_CHUNKS fc
SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR,
    INDUSTRY_TITLE = fi.INDUSTRY_TITLE
FROM FILING_INDEX fi
WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
  AND fi.INDUSTRY_SECTOR IS NOT NULL;

UPDATE FILING_CHUNKS fc
SET PERIOD_OF_REPORT = fi.PERIOD_OF_REPORT
FROM FILING_INDEX fi
WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
  AND fi.PERIOD_OF_REPORT IS NOT NULL
  AND fc.PERIOD_OF_REPORT IS NULL;


-- =============================================================================
-- Verification
-- =============================================================================

-- SELECT COUNT(*) AS total, COUNT(PERIOD_OF_REPORT) AS has_period,
--        COUNT(INDUSTRY_SECTOR) AS has_sector, COUNT(INDUSTRY_TITLE) AS has_title
-- FROM FILING_INDEX;

-- SELECT INDUSTRY_SECTOR, COUNT(*) AS cnt
-- FROM FILING_INDEX WHERE INDUSTRY_SECTOR IS NOT NULL
-- GROUP BY 1 ORDER BY cnt DESC;

-- SELECT INDUSTRY_TITLE, COUNT(*) AS cnt
-- FROM FILING_INDEX WHERE INDUSTRY_TITLE IS NOT NULL
-- GROUP BY 1 ORDER BY cnt DESC LIMIT 20;
