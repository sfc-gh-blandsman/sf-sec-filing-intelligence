-- =============================================================================
-- 01: Database and Schema
-- =============================================================================
-- Creates the database and schema for the SEC Filing Intelligence pipeline.
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Create database
CREATE DATABASE IF NOT EXISTS IDENTIFIER($config_database)
    COMMENT = 'SEC EDGAR filing intelligence — ingestion, AI signal extraction, search, and agent';

-- Create schema
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($config_database || '.' || $config_schema)
    COMMENT = 'Filing pipeline: metadata, content, chunks, signals, search, and agent';

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Core Tables
-- =============================================================================

-- Filing metadata from EDGAR full-index
CREATE TABLE IF NOT EXISTS FILING_INDEX (
    ACCESSION_NO        VARCHAR(25)    NOT NULL PRIMARY KEY,
    CIK                 VARCHAR(10)    NOT NULL,
    COMPANY_NAME        VARCHAR(500),
    FORM_TYPE           VARCHAR(20),
    FILED_AT            TIMESTAMP_TZ,
    PRIMARY_DOC_URL     VARCHAR(1000),
    FILING_INDEX_URL    VARCHAR(1000),
    IS_AMENDMENT        BOOLEAN        DEFAULT FALSE,
    TICKER              VARCHAR(20),
    TICKER_CHECKED_AT   TIMESTAMP_TZ,
    PERIOD_OF_REPORT    DATE,
    SIC_CODE            VARCHAR(4),
    INDUSTRY_SECTOR     VARCHAR(100),
    INDUSTRY_TITLE      VARCHAR(200),
    DOWNLOADED_AT       TIMESTAMP_TZ
);

-- Raw filing text content
CREATE TABLE IF NOT EXISTS FILING_CONTENT (
    ACCESSION_NO        VARCHAR(25)    NOT NULL PRIMARY KEY,
    CONTENT_TEXT        TEXT,
    STAGE_FILE_PATH     VARCHAR(500),
    FILE_SIZE_BYTES     NUMBER,
    FILE_FORMAT         VARCHAR(10),
    PARSE_STATUS        VARCHAR(20)    DEFAULT 'PENDING',
    PARSE_ERROR         VARCHAR(500),
    SIGNAL_STATUS       VARCHAR(20)    DEFAULT 'PENDING',
    PROCESSED_AT        TIMESTAMP_TZ
);

-- Section-aware text chunks for Cortex Search
CREATE TABLE IF NOT EXISTS FILING_CHUNKS (
    CHUNK_ID            VARCHAR(80)    NOT NULL PRIMARY KEY,
    ACCESSION_NO        VARCHAR(25)    NOT NULL,
    COMPANY_NAME        VARCHAR(500),
    TICKER              VARCHAR(20),
    FORM_TYPE           VARCHAR(20),
    FILED_AT            TIMESTAMP_TZ,
    PERIOD_OF_REPORT    DATE,
    SECTION_NAME        VARCHAR(100),
    CHUNK_INDEX         INT,
    CHUNK_TEXT          TEXT           NOT NULL,
    TOKEN_COUNT         INT,
    INDUSTRY_SECTOR     VARCHAR(100),
    INDUSTRY_TITLE      VARCHAR(200),
    CREATED_AT          TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
);

-- AI-extracted structured investment signals
CREATE TABLE IF NOT EXISTS FILING_SIGNALS (
    SIGNAL_ID           VARCHAR(50)    NOT NULL PRIMARY KEY,
    ACCESSION_NO        VARCHAR(25)    NOT NULL,
    COMPANY_NAME        VARCHAR(500),
    TICKER              VARCHAR(20),
    FORM_TYPE           VARCHAR(20),
    SIGNAL_DATE         TIMESTAMP_TZ   NOT NULL,
    PERIOD_OF_REPORT    DATE,
    EVENT_TYPE          VARCHAR(500),
    EVENT_TYPE_NORMALIZED VARCHAR(50),
    SENTIMENT           VARCHAR(20),
    SUMMARY             TEXT,
    KEY_METRICS         TEXT,
    REVENUE             VARCHAR(500),
    REVENUE_NORMALIZED  FLOAT,
    NET_INCOME          VARCHAR(500),
    EPS                 VARCHAR(1000),
    EPS_NORMALIZED      VARCHAR(50),
    YOY_CHANGE          VARCHAR(200),
    FORWARD_GUIDANCE    TEXT,
    GUIDANCE_NORMALIZED TEXT,
    METRICS_EXTRACTED_AT TIMESTAMP_TZ,
    GUIDANCE_EXTRACTED_AT TIMESTAMP_TZ,
    RISK_FLAGS          ARRAY,
    MATERIAL_ITEMS      ARRAY,
    INDUSTRY_SECTOR     VARCHAR(100),
    INDUSTRY_TITLE      VARCHAR(200),
    EXTRACTION_MODEL    VARCHAR(100),
    EXTRACTION_METHOD   VARCHAR(50)    DEFAULT 'raw_first_16k',
    SIGNAL_EXTRACTED_AT TIMESTAMP_TZ,
    IS_AMENDMENT        BOOLEAN,
    CREATED_AT          TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
);

-- High-value exhibits extracted from SEC filings (press releases, contracts, M&A)
CREATE TABLE IF NOT EXISTS FILING_EXHIBITS (
    EXHIBIT_ID          VARCHAR(100)   NOT NULL PRIMARY KEY,
    ACCESSION_NO        VARCHAR(25)    NOT NULL,
    DOC_SEQUENCE        INT,
    EXHIBIT_TYPE        VARCHAR(20),
    FILENAME            VARCHAR(200),
    DESCRIPTION         VARCHAR(500),
    CONTENT_TEXT        TEXT,
    FILE_SIZE_CHARS     INT,
    CREATED_AT          TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'High-value exhibits from SEC filings: EX-99 (press releases), EX-10 (contracts), EX-2 (M&A).';

-- =============================================================================
-- Evaluation results (populated by eval framework)
-- =============================================================================

CREATE TABLE IF NOT EXISTS EVAL_RESULTS (
    RUN_NAME            VARCHAR        NOT NULL,
    AGENT_NAME          VARCHAR        NOT NULL,
    EVAL_CONFIG         VARCHAR,
    RECORD_ID           VARCHAR,
    INPUT_ID            VARCHAR,
    REQUEST_ID          VARCHAR,
    EVAL_TIMESTAMP      TIMESTAMP_TZ,
    DURATION_MS         INT,
    INPUT               VARCHAR,
    OUTPUT              VARCHAR,
    ERROR               VARCHAR,
    GROUND_TRUTH        VARCHAR,
    METRIC_NAME         VARCHAR,
    EVAL_AGG_SCORE      FLOAT,
    METRIC_TYPE         VARCHAR,
    METRIC_STATUS       VARIANT,
    METRIC_CALLS        VARIANT,
    TOTAL_INPUT_TOKENS  INT,
    TOTAL_OUTPUT_TOKENS INT,
    LLM_CALL_COUNT      INT,
    CAPTURED_AT         TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Materialized eval results from EXECUTE_AI_EVALUATION runs.';

-- Search latency benchmark results
CREATE TABLE IF NOT EXISTS SEARCH_LATENCY_RESULTS (
    SERVICE_NAME       VARCHAR(100)   NOT NULL,
    SCORING_PROFILE    VARCHAR(50)    NOT NULL,
    QUERY_TYPE         VARCHAR(20),
    QUERY_ID           INT,
    QUERY_TEXT         VARCHAR,
    LATENCY_MS         FLOAT,
    RESULT_COUNT       INT,
    RUN_TIMESTAMP      TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Per-query search latency results for benchmarking.';

-- Feed ingestion progress tracking
CREATE TABLE IF NOT EXISTS _FEED_INGEST_LOG (
    FEED_DATE       VARCHAR(10)    NOT NULL,
    LOADED          INT            DEFAULT 0,
    STATUS          VARCHAR(20)    DEFAULT 'STARTED',
    STARTED_AT      TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Feed ingestion progress tracking. Query for % complete during long runs.';

-- Runtime configuration for task DAGs (tasks cannot access session variables)
CREATE TABLE IF NOT EXISTS _PIPELINE_CONFIG (
    KEY    VARCHAR(100) NOT NULL PRIMARY KEY,
    VALUE  VARCHAR(500) NOT NULL
)
COMMENT = 'Runtime config for task DAGs. Populated by 00_config.sql, read by _CFG() helper.';

-- Helper function for clean config lookups inside task bodies
CREATE OR REPLACE FUNCTION _CFG(KEY_NAME VARCHAR)
RETURNS VARCHAR
AS 'SELECT VALUE FROM _PIPELINE_CONFIG WHERE KEY = KEY_NAME';

-- =============================================================================
-- Clustering for query performance
-- =============================================================================

ALTER TABLE FILING_CHUNKS CLUSTER BY (FORM_TYPE, FILED_AT);
ALTER TABLE FILING_SIGNALS CLUSTER BY (FORM_TYPE, SIGNAL_DATE);
