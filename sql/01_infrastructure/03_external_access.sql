-- =============================================================================
-- 03: External Access Integration
-- =============================================================================
-- Creates network rules and external access integrations for:
--   1. SEC EDGAR API (filing ingestion + ticker enrichment)
--   2. PyPI (Streamlit container runtime package installation)
-- Run 00_config.sql first to set session variables.
--
-- IMPORTANT: Statements in this file MUST be executed in strict top-to-bottom order.
-- Network rules must exist BEFORE their referencing External Access Integration.
-- Do NOT parallelize or reorder these statements.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- SEC EDGAR: Network rule + EAI
-- =============================================================================

-- Network rule allowing HTTPS access to SEC EDGAR
CREATE OR REPLACE NETWORK RULE IDENTIFIER($config_database || '.' || $config_schema || '.' || $config_network_rule)
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('www.sec.gov:443', 'efts.sec.gov:443', 'data.sec.gov:443')
    COMMENT = 'Allow outbound HTTPS to SEC EDGAR for filing downloads and company lookups';

-- External access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION IDENTIFIER($config_eai_name)
    ALLOWED_NETWORK_RULES = (IDENTIFIER($config_database || '.' || $config_schema || '.' || $config_network_rule))
    ENABLED = TRUE
    COMMENT = 'External access for SEC EDGAR filing ingestion and ticker enrichment';

-- =============================================================================
-- PyPI: Network rule + EAI (required for Streamlit container runtime)
-- =============================================================================
-- The container runtime needs to install Python packages (plotly, etc.) from PyPI
-- at startup. Without this, the Streamlit app fails with DNS resolution errors.

CREATE OR REPLACE NETWORK RULE PYPI_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('pypi.org:443', 'files.pythonhosted.org:443')
    COMMENT = 'Allow outbound HTTPS to PyPI for Streamlit container runtime package installation';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION PYPI_EAI
    ALLOWED_NETWORK_RULES = (IDENTIFIER($config_database || '.' || $config_schema || '.PYPI_NETWORK_RULE'))
    ENABLED = TRUE
    COMMENT = 'External access for PyPI package downloads (Streamlit container runtime)';
