-- =============================================================================
-- 04: Event Type Normalization
-- =============================================================================
-- Normalizes the 97+ hallucinated EVENT_TYPE values into 12 canonical categories.
--
-- AI_EXTRACT sometimes invents specific event descriptions (e.g.,
-- "Change in Registrant's Certifying Accountant", "Issuance of $18 Billion Notes")
-- instead of using the canonical categories from the extraction prompt.
--
-- Canonical 12 event types:
--   Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update,
--   Regulatory, Capital Markets, Bankruptcy, Annual Report, Quarterly Report,
--   Current Report, Other
--
-- This script populates EVENT_TYPE_NORMALIZED for all rows where it's NULL.
-- Safe to re-run (WHERE EVENT_TYPE_NORMALIZED IS NULL).
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Normalize EVENT_TYPE → EVENT_TYPE_NORMALIZED (idempotent)
-- =============================================================================

UPDATE FILING_SIGNALS
SET EVENT_TYPE_NORMALIZED = CASE
    -- Already canonical (12 types from the extraction prompt + form-type defaults)
    WHEN EVENT_TYPE IN ('Earnings', 'M&A', 'Leadership Change', 'Risk Disclosure', 
                        'Guidance Update', 'Regulatory', 'Capital Markets', 'Bankruptcy',
                        'Annual Report', 'Quarterly Report', 'Current Report', 'Other') 
        THEN EVENT_TYPE

    -- M&A variants
    WHEN EVENT_TYPE ILIKE '%acqui%' OR EVENT_TYPE ILIKE '%merger%' OR EVENT_TYPE ILIKE '%disposition%'
        THEN 'M&A'
    WHEN EVENT_TYPE ILIKE '%change in control%' OR EVENT_TYPE ILIKE '%change of control%'
        THEN 'M&A'

    -- Leadership variants
    WHEN EVENT_TYPE ILIKE '%leadership%' OR EVENT_TYPE ILIKE '%chief%' OR EVENT_TYPE ILIKE '%officer%'
        THEN 'Leadership Change'

    -- Regulatory variants (accountant changes, ESG, compliance, mine safety)
    WHEN EVENT_TYPE ILIKE '%regulation%' OR EVENT_TYPE ILIKE '%compliance%' OR EVENT_TYPE ILIKE '%sanction%'
        OR EVENT_TYPE ILIKE '%mine safety%' OR EVENT_TYPE ILIKE '%ESG%' OR EVENT_TYPE ILIKE '%audit%'
        OR EVENT_TYPE ILIKE '%accountant%' OR EVENT_TYPE ILIKE '%accounting%'
        THEN 'Regulatory'

    -- Capital Markets variants (dividends, issuances, notes, repurchases, credit)
    WHEN EVENT_TYPE ILIKE '%dividend%' OR EVENT_TYPE ILIKE '%issuance%' OR EVENT_TYPE ILIKE '%notes%'
        OR EVENT_TYPE ILIKE '%repurchase%' OR EVENT_TYPE ILIKE '%capital%' OR EVENT_TYPE ILIKE '%credit%'
        OR EVENT_TYPE ILIKE '%loan%' OR EVENT_TYPE ILIKE '%euro%'
        THEN 'Capital Markets'

    -- Guidance variants (forward-looking, outlook, updates)
    WHEN EVENT_TYPE ILIKE '%guidance%' OR EVENT_TYPE ILIKE '%forward%look%' OR EVENT_TYPE ILIKE '%outlook%'
        OR EVENT_TYPE ILIKE '%update%'
        THEN 'Guidance Update'

    -- Risk variants
    WHEN EVENT_TYPE ILIKE '%risk%'
        THEN 'Risk Disclosure'

    -- Bankruptcy variants
    WHEN EVENT_TYPE ILIKE '%bankrupt%' OR EVENT_TYPE ILIKE '%shell%'
        THEN 'Bankruptcy'

    -- Everything else
    ELSE 'Other'
END
WHERE EVENT_TYPE_NORMALIZED IS NULL;

-- =============================================================================
-- Verify
-- =============================================================================

SELECT EVENT_TYPE_NORMALIZED, COUNT(*) AS cnt
FROM FILING_SIGNALS
GROUP BY 1
ORDER BY 2 DESC;
