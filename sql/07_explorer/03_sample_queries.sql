-- =============================================================================
-- 03: Sample Queries — Interactive + Batch Examples
-- =============================================================================
-- Ready-to-run queries for testing the agent and exploring results.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Interactive Agent Queries (run manually)
-- =============================================================================

-- Basic search query
-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'What risk factors did Apple disclose in their most recent 10-K?'
-- );

-- Analyst query
-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'Show the top 5 industries by total filing count.'
-- );

-- Cross-tool query (analyst → search)
-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'Which Technology companies had M&A events in 2022? Show details from their 8-K filings.'
-- );


-- =============================================================================
-- Batch Explorer Usage
-- =============================================================================

-- List all available sectors:
-- CALL EXPLORER_SECTOR_ANALYSIS('LIST');

-- Analyze a specific sector:
-- CALL EXPLORER_SECTOR_ANALYSIS('SECTOR', 'Technology');
-- CALL EXPLORER_SECTOR_ANALYSIS('SECTOR', 'Financial Services');
-- CALL EXPLORER_SECTOR_ANALYSIS('SECTOR', 'Healthcare & Biotech');

-- Run all sectors (takes ~30 min depending on corpus size):
-- CALL EXPLORER_BATCH_ALL_SECTORS();


-- =============================================================================
-- Review Explorer Results
-- =============================================================================

-- Latest run results:
-- SELECT SECTOR, QUERY_TYPE, LEFT(AGENT_RESPONSE, 200) AS response_preview
-- FROM EXPLORER_RESULTS
-- WHERE RUN_TIMESTAMP >= DATEADD('day', -1, CURRENT_TIMESTAMP())
-- ORDER BY RUN_TIMESTAMP DESC;

-- Sector filing counts:
-- SELECT SECTOR, FILING_COUNT
-- FROM EXPLORER_RESULTS
-- WHERE QUERY_TYPE = 'sector_list'
--   AND RUN_TIMESTAMP = (SELECT MAX(RUN_TIMESTAMP) FROM EXPLORER_RESULTS WHERE QUERY_TYPE = 'sector_list')
-- ORDER BY FILING_COUNT DESC;


-- =============================================================================
-- Pipeline Status Queries
-- =============================================================================

-- Ingestion status:
-- SELECT * FROM V_INGESTION_STATUS;

-- Processing progress:
-- SELECT PARSE_STATUS, SIGNAL_STATUS, COUNT(*) AS cnt
-- FROM FILING_CONTENT GROUP BY 1, 2 ORDER BY 1, 2;

-- Chunk statistics:
-- SELECT FORM_TYPE, COUNT(DISTINCT ACCESSION_NO) AS filings,
--        COUNT(*) AS chunks, ROUND(AVG(TOKEN_COUNT)) AS avg_tokens
-- FROM FILING_CHUNKS GROUP BY 1 ORDER BY 1;

-- Signal distribution:
-- SELECT EVENT_TYPE, SENTIMENT, COUNT(*) AS cnt
-- FROM FILING_SIGNALS GROUP BY 1, 2 ORDER BY 1, 3 DESC;
