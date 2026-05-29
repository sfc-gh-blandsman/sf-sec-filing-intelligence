-- =============================================================================
-- 05: Streamlit App Deployment (Container Runtime)
-- =============================================================================
-- Deploys the SEC Filing Intelligence Dashboard using container runtime.
-- Container runtime provides: persistent server (no tab resets), faster loads,
-- shared caching, latest Streamlit features (st.dialog, etc.), lower cost.
--
-- Prerequisites:
--   - Run 00_config.sql first
--   - Compute pool created (STREAMLIT_COMPUTE_POOL)
--   - streamlit/ directory contains: SEC_Filing_Explorer.py, pyproject.toml
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
-- Option 1: Via PUT (from SnowSQL or any SQL client — replace <PROJECT_DIR>)
--
-- PUT file://<PROJECT_DIR>/streamlit/SEC_Filing_Explorer.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://<PROJECT_DIR>/streamlit/pyproject.toml @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- Option 2: Via Snowsight UI — navigate to STREAMLIT_STAGE, click "+ Files"
-- =============================================================================


-- =============================================================================
-- Create (or replace) the Streamlit app — CONTAINER RUNTIME
-- =============================================================================
-- Container runtime benefits:
--   - Persistent shared server (no per-viewer startup, no tab resets on reload)
--   - Streamlit 1.50+ (st.dialog, latest features)
--   - Shared caching across all viewers
--   - Lower cost than warehouse runtime for frequent access
--   - Faster load times after initial container startup

CREATE OR REPLACE STREAMLIT SEC_FILING_DASHBOARD
    FROM '@STREAMLIT_STAGE'
    MAIN_FILE = 'SEC_Filing_Explorer.py'
    RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
    COMPUTE_POOL = STREAMLIT_COMPUTE_POOL
    EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_EAI)
    QUERY_WAREHOUSE = IDENTIFIER($config_warehouse)
    COMMENT = 'SEC Filing Intelligence Dashboard — 7 tabs (container runtime)';

-- Activate the live version
ALTER STREAMLIT SEC_FILING_DASHBOARD ADD LIVE VERSION FROM LAST;

-- Grant access to other roles if needed:
-- GRANT USAGE ON STREAMLIT SEC_FILING_DASHBOARD TO ROLE <role_name>;


-- =============================================================================
-- Alternative: Warehouse Runtime (legacy, for accounts without compute pools)
-- =============================================================================
-- CREATE OR REPLACE STREAMLIT SEC_FILING_DASHBOARD
--     ROOT_LOCATION = '@STREAMLIT_STAGE'
--     MAIN_FILE = 'SEC_Filing_Explorer.py'
--     QUERY_WAREHOUSE = IDENTIFIER($config_warehouse)
--     COMMENT = 'SEC Filing Intelligence Dashboard — 7 tabs (warehouse runtime)';


-- =============================================================================
-- Verify deployment
-- =============================================================================
-- SHOW STREAMLITS IN SCHEMA;
-- DESCRIBE STREAMLIT SEC_FILING_DASHBOARD;
