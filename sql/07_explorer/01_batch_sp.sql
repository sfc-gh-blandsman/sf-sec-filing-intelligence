-- =============================================================================
-- 01: Explorer Batch SP — EXPLORER_SECTOR_ANALYSIS
-- =============================================================================
-- Batch stored procedure for automated sector-level analysis.
-- Two modes:
--   1. LIST mode: Returns all available sectors with filing counts
--   2. SECTOR mode: Runs agent queries for a specific sector
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- Results table for batch analysis output
CREATE TABLE IF NOT EXISTS EXPLORER_RESULTS (
    RUN_ID              VARCHAR(50)    NOT NULL,
    SECTOR              VARCHAR(200),
    QUERY_TYPE          VARCHAR(50),
    QUERY_TEXT          TEXT,
    AGENT_RESPONSE      TEXT,
    FILING_COUNT        INT,
    RUN_TIMESTAMP       TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Batch explorer results from EXPLORER_SECTOR_ANALYSIS SP.';

-- =============================================================================
-- SP: EXPLORER_SECTOR_ANALYSIS
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXPLORER_SECTOR_ANALYSIS(
    P_MODE VARCHAR DEFAULT 'LIST',
    P_SECTOR VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    run_id VARCHAR;
    sector_count INT DEFAULT 0;
    agent_fqn VARCHAR;
    query_text VARCHAR;
    response VARCHAR;
BEGIN
    run_id := 'explore-' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS');
    agent_fqn := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('agent_name');

    IF (UPPER(:P_MODE) = 'LIST') THEN
        -- List all sectors with filing counts
        INSERT INTO EXPLORER_RESULTS (RUN_ID, SECTOR, QUERY_TYPE, FILING_COUNT)
        SELECT :run_id, INDUSTRY_SECTOR, 'sector_list', COUNT(*)
        FROM FILING_INDEX
        WHERE INDUSTRY_SECTOR IS NOT NULL
        GROUP BY INDUSTRY_SECTOR
        ORDER BY COUNT(*) DESC;

        SELECT COUNT(DISTINCT INDUSTRY_SECTOR) INTO :sector_count
        FROM FILING_INDEX WHERE INDUSTRY_SECTOR IS NOT NULL;

        RETURN 'Listed ' || :sector_count || ' sectors. Run ID: ' || :run_id;
    END IF;

    IF (UPPER(:P_MODE) = 'SECTOR' AND :P_SECTOR IS NOT NULL) THEN
        -- Run standardized queries for this sector

        -- Query 1: Sentiment overview
        query_text := 'What is the overall sentiment distribution for ' || :P_SECTOR || ' filings? Show counts by sentiment.';
        BEGIN
            EXECUTE IMMEDIATE 'SELECT SNOWFLAKE.CORTEX.AGENT_RUN(''' || :agent_fqn || ''', ''' || REPLACE(:query_text, '''', '''''') || ''')';
            response := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        EXCEPTION WHEN OTHER THEN response := 'ERROR: ' || SQLERRM; END;
        INSERT INTO EXPLORER_RESULTS (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE)
        VALUES (:run_id, :P_SECTOR, 'sentiment_overview', :query_text, :response);

        -- Query 2: Key events
        query_text := 'What are the most common event types in ' || :P_SECTOR || ' 8-K filings?';
        BEGIN
            EXECUTE IMMEDIATE 'SELECT SNOWFLAKE.CORTEX.AGENT_RUN(''' || :agent_fqn || ''', ''' || REPLACE(:query_text, '''', '''''') || ''')';
            response := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        EXCEPTION WHEN OTHER THEN response := 'ERROR: ' || SQLERRM; END;
        INSERT INTO EXPLORER_RESULTS (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE)
        VALUES (:run_id, :P_SECTOR, 'key_events', :query_text, :response);

        -- Query 3: Risk themes
        query_text := 'Find common risk factors in ' || :P_SECTOR || ' 10-K filings. What themes appear across multiple companies?';
        BEGIN
            EXECUTE IMMEDIATE 'SELECT SNOWFLAKE.CORTEX.AGENT_RUN(''' || :agent_fqn || ''', ''' || REPLACE(:query_text, '''', '''''') || ''')';
            response := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        EXCEPTION WHEN OTHER THEN response := 'ERROR: ' || SQLERRM; END;
        INSERT INTO EXPLORER_RESULTS (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE)
        VALUES (:run_id, :P_SECTOR, 'risk_themes', :query_text, :response);

        RETURN 'Sector analysis complete for ' || :P_SECTOR || '. Run ID: ' || :run_id;
    END IF;

    RETURN 'ERROR: Invalid mode. Use LIST or SECTOR with P_SECTOR parameter.';
END;
$$;
