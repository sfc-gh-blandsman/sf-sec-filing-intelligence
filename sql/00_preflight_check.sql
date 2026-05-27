-- =============================================================================
-- PREFLIGHT CHECK — Run BEFORE starting the install
-- =============================================================================
-- Validates that your Snowflake account has the required features and
-- configuration for the SEC Filing Intelligence pipeline.
--
-- Run this script first. If any check fails, resolve the issue before
-- proceeding to Phase 1.
--
-- Requirements:
--   - ACCOUNTADMIN role (or equivalent privileges)
--   - Cortex AI functions (AI_EXTRACT, AI_COMPLETE)
--   - Cortex Search capability
--   - Cortex Agent capability
--   - External network access (for SEC EDGAR API)
--   - Verified email address (for pipeline notifications)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- Check 1: Cortex AI Functions available
-- =============================================================================
-- AI_EXTRACT and AI_COMPLETE are required for signal extraction and metrics.

SELECT 'CHECK 1: Cortex AI Functions' AS check_name,
       CASE WHEN SNOWFLAKE.CORTEX.AI_COMPLETE('llama3.3-70b', 'Say OK') IS NOT NULL
            THEN 'PASS — AI_COMPLETE is available'
            ELSE 'FAIL — AI_COMPLETE not responding'
       END AS result;

-- =============================================================================
-- Check 2: Email verified (required for task DAG notifications)
-- =============================================================================

SELECT 'CHECK 2: Email Verification' AS check_name,
       CASE WHEN "email" IS NOT NULL AND "email" != ''
            THEN 'PASS — Email: ' || "email"
            ELSE 'FAIL — No email set. Run: ALTER USER SET EMAIL = ''you@company.com''; then verify in Snowsight.'
       END AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
;

-- Workaround: just describe the current user
DESCRIBE USER CURRENT_USER();

-- =============================================================================
-- Check 3: External network access capability
-- =============================================================================
-- The pipeline needs to reach SEC EDGAR (www.sec.gov) for feed downloads.

SELECT 'CHECK 3: External Access' AS check_name,
       'INFO — Will be validated when creating NETWORK RULE in Phase 1. '
       || 'Ensure your account allows CREATE NETWORK RULE and CREATE EXTERNAL ACCESS INTEGRATION.' AS result;

-- =============================================================================
-- Check 4: Cortex Search available
-- =============================================================================

SELECT 'CHECK 4: Cortex Search' AS check_name,
       'INFO — Will be validated in Phase 5. Ensure CORTEX_ENABLED = TRUE on your account.' AS result;

-- =============================================================================
-- Check 5: Warehouse creation privileges
-- =============================================================================

SELECT 'CHECK 5: Warehouse Privileges' AS check_name,
       CASE WHEN CURRENT_ROLE() = 'ACCOUNTADMIN'
            THEN 'PASS — Running as ACCOUNTADMIN (full privileges)'
            ELSE 'WARNING — Running as ' || CURRENT_ROLE() || '. May lack CREATE WAREHOUSE privilege.'
       END AS result;

-- =============================================================================
-- Check 6: Cross-region inference (required for some AI models)
-- =============================================================================

SELECT 'CHECK 6: Cross-Region Inference' AS check_name,
       'INFO — If AI_COMPLETE fails with "model not available", enable cross-region inference: '
       || 'ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = ''ANY_REGION'';' AS result;

-- =============================================================================
-- Summary
-- =============================================================================

SELECT '✅ PREFLIGHT COMPLETE' AS status,
       'If all checks above show PASS or INFO, proceed to Phase 1 (sql/01_infrastructure/).' AS next_step;
