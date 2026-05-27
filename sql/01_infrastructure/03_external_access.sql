-- =============================================================================
-- 03: External Access Integration
-- =============================================================================
-- Creates network rule and external access integration for SEC EDGAR API.
-- Required for ingestion SPs to download filings from sec.gov.
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

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
