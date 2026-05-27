-- =============================================================================
-- 05: Streamlit App Deployment
-- =============================================================================
-- Deploys the SEC Filing Intelligence Dashboard to Streamlit in Snowflake.
-- Creates a stage, uploads app files, and creates the Streamlit object.
--
-- Prerequisites:
--   - Run 00_config.sql first
--   - streamlit/ directory contains: SEC_Filing_Explorer.py, environment.yml, pyproject.toml
--
-- Alternative: Upload files manually via Snowsight (Data > Databases > Stage > Upload)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Stage for Streamlit app files
-- =============================================================================

CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Streamlit app source files for SEC Filing Intelligence Dashboard';

-- =============================================================================
-- Upload files programmatically
-- =============================================================================
-- Option 1: Via PUT (works from any SQL client — replace <PROJECT_DIR> with your path)
--
-- PUT file://<PROJECT_DIR>/streamlit/SEC_Filing_Explorer.py @STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://<PROJECT_DIR>/streamlit/environment.yml @STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://<PROJECT_DIR>/streamlit/pyproject.toml @STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- Option 2: Via snow CLI (from project root):
--
--   snow stage copy streamlit/SEC_Filing_Explorer.py @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE --overwrite
--   snow stage copy streamlit/environment.yml @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE --overwrite
--   snow stage copy streamlit/pyproject.toml @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE --overwrite
--
-- Option 3: Via Snowsight UI — navigate to STREAMLIT_STAGE, click "+ Files"
-- =============================================================================


-- =============================================================================
-- Create (or replace) the Streamlit app
-- =============================================================================

CREATE OR REPLACE STREAMLIT SEC_FILING_DASHBOARD
    ROOT_LOCATION = '@STREAMLIT_STAGE'
    MAIN_FILE = 'SEC_Filing_Explorer.py'
    QUERY_WAREHOUSE = IDENTIFIER($config_warehouse)
    COMMENT = 'SEC Filing Intelligence Dashboard — Pipeline, Eval, Explorer, Cost';

-- Grant access to other roles if needed:
-- GRANT USAGE ON STREAMLIT SEC_FILING_DASHBOARD TO ROLE <role_name>;

-- =============================================================================
-- Verify deployment
-- =============================================================================
-- SHOW STREAMLITS IN SCHEMA;
-- DESCRIBE STREAMLIT SEC_FILING_DASHBOARD;
