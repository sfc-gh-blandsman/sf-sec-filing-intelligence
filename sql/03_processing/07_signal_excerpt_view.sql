-- =============================================================================
-- 07: Signal Excerpt View (Shared Section-Targeted Extraction Source)
-- =============================================================================
-- Provides the optimized excerpt for AI_EXTRACT signal extraction.
-- Used by both the bulk DAG SPs (SIGNAL_EXTRACT_10K/10Q/8K) and the
-- spot-processing SP (PROCESS_FILINGS).
--
-- For 10-K/10-Q filings: builds a section-targeted excerpt from FILING_CHUNKS
--   - Risk Factors: 3K chars
--   - MD&A: 5K chars
--   - Financial Statements (first 3 chunks): 3K chars
--   - Business: 3K chars
--   - Market Risk: 2K chars
--   Total: up to 16K chars of focused, signal-rich content
--
-- For 8-K and other forms: uses first 16K of cleaned raw content (no sections)
--
-- Fallback: if chunks don't exist for a filing, uses raw content
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

CREATE OR REPLACE VIEW V_SIGNAL_EXCERPT AS
SELECT
    fc.ACCESSION_NO,
    fi.COMPANY_NAME,
    fi.TICKER,
    fi.FORM_TYPE,
    fi.FILED_AT,
    fi.PERIOD_OF_REPORT,
    fi.IS_AMENDMENT,
    fi.INDUSTRY_SECTOR,
    fi.INDUSTRY_TITLE,
    CASE
        WHEN fi.FORM_TYPE IN ('10-K','10-Q','10-K/A','10-Q/A','10-KT')
             AND ce.targeted_excerpt IS NOT NULL
             AND LENGTH(ce.targeted_excerpt) > 500
        THEN LEFT(ce.targeted_excerpt, 16000)
        ELSE LEFT(CLEAN_TEXT(fc.CONTENT_TEXT), 16000)
    END AS EXCERPT
FROM FILING_CONTENT fc
JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
LEFT JOIN (
    SELECT ck.ACCESSION_NO,
        COALESCE(LEFT(LISTAGG(
            CASE WHEN ck.SECTION_NAME = 'Risk Factors' THEN ck.CHUNK_TEXT END, ' '
        ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 3000), '') ||
        COALESCE(LEFT(LISTAGG(
            CASE WHEN ck.SECTION_NAME = 'MD&A' THEN ck.CHUNK_TEXT END, ' '
        ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 5000), '') ||
        COALESCE(LEFT(LISTAGG(
            CASE WHEN ck.SECTION_NAME = 'Financial Statements' AND ck.CHUNK_INDEX <= 3 THEN ck.CHUNK_TEXT END, ' '
        ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 3000), '') ||
        COALESCE(LEFT(LISTAGG(
            CASE WHEN ck.SECTION_NAME = 'Business' THEN ck.CHUNK_TEXT END, ' '
        ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 3000), '') ||
        COALESCE(LEFT(LISTAGG(
            CASE WHEN ck.SECTION_NAME = 'Market Risk' THEN ck.CHUNK_TEXT END, ' '
        ) WITHIN GROUP (ORDER BY ck.CHUNK_INDEX), 2000), '')
        AS targeted_excerpt
    FROM FILING_CHUNKS ck
    GROUP BY ck.ACCESSION_NO
) ce ON ce.ACCESSION_NO = fc.ACCESSION_NO
WHERE fc.CONTENT_TEXT IS NOT NULL;
