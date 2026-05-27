-- =============================================================================
-- 03: Search Latency Benchmark
-- =============================================================================
-- Benchmarks Cortex Search latency with and without reranking.
-- Uses 20 representative SEC filing questions as test queries.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Benchmark SP (takes fully-qualified service name)
-- =============================================================================

CREATE OR REPLACE PROCEDURE SEARCH_LATENCY_BENCHMARK(P_SERVICE_FQN VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    start_ts TIMESTAMP_TZ;
    end_ts TIMESTAMP_TZ;
    latency_ms FLOAT;
    total_queries INT DEFAULT 0;
    cur_qid INT;
    cur_qtext VARCHAR;
    search_sql VARCHAR;
    service_short VARCHAR;
    c1 CURSOR FOR SELECT QUERY_ID, QUERY_TEXT FROM _BENCH_QUERIES ORDER BY QUERY_ID;
    c2 CURSOR FOR SELECT QUERY_ID, QUERY_TEXT FROM _BENCH_QUERIES ORDER BY QUERY_ID;
BEGIN
    service_short := SPLIT_PART(:P_SERVICE_FQN, '.', -1);

    CREATE OR REPLACE TEMPORARY TABLE _BENCH_QUERIES (
        QUERY_ID INT, QUERY_TEXT VARCHAR
    );
    INSERT INTO _BENCH_QUERIES (QUERY_ID, QUERY_TEXT) VALUES
        (1, 'What risk factors did pharmaceutical companies disclose in 2025 10-K filings?'),
        (2, 'Find 8-K filings from Technology companies reporting leadership changes'),
        (3, 'What did banks discuss about credit losses in recent 10-K filings?'),
        (4, 'Find 10-K Risk Factors discussing cybersecurity threats or data breaches'),
        (5, 'What did Energy & Transportation companies discuss about regulatory compliance?'),
        (6, 'Find Life Sciences 8-K filings about acquisitions or mergers'),
        (7, 'Find a Finance company 10-K discussing interest rate risk exposure'),
        (8, 'What common risk factors appeared across multiple 2025 10-K filings?'),
        (9, 'Find Manufacturing companies discussing supply chain resilience in MD&A'),
        (10, 'Find filings discussing artificial intelligence risks or opportunities'),
        (11, 'Find a Real Estate & Construction company filing about property valuations'),
        (12, 'What did Trade & Services companies report in 8-K current reports?'),
        (13, 'Find 10-K filings discussing inflation impact on operating costs'),
        (14, 'Find 8-K filings about dividend declarations or capital returns'),
        (15, 'What geopolitical risks did companies disclose in 2025 10-K filings?'),
        (16, 'Find Technology company 10-K sections about competition and market position'),
        (17, 'climate change environmental sustainability risk factors in SEC filings'),
        (18, 'Find filings discussing debt covenants or refinancing risk'),
        (19, 'What did Life Sciences companies report about FDA approvals or clinical trials?'),
        (20, 'supply chain disruption semiconductor shortage risk factors');

    -- Profile 1: default (reranking enabled)
    OPEN c1;
    FOR rec IN c1 DO
        cur_qid := rec.QUERY_ID;
        cur_qtext := rec.QUERY_TEXT;
        start_ts := CURRENT_TIMESTAMP();

        search_sql := 'SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(''' || :P_SERVICE_FQN || ''', ''{' ||
            '"query": "' || REPLACE(REPLACE(:cur_qtext, '"', '\\"'), '''', '''''') || '", ' ||
            '"columns": ["CHUNK_ID", "COMPANY_NAME", "FORM_TYPE"], ' ||
            '"limit": 10' ||
            '}'')) AS r';
        EXECUTE IMMEDIATE :search_sql;

        end_ts := CURRENT_TIMESTAMP();
        latency_ms := DATEDIFF('millisecond', :start_ts, :end_ts);

        INSERT INTO SEARCH_LATENCY_RESULTS
            (SERVICE_NAME, SCORING_PROFILE, QUERY_TYPE, QUERY_ID, QUERY_TEXT, LATENCY_MS, RESULT_COUNT)
        VALUES (:service_short, 'default', 'semantic', :cur_qid, :cur_qtext, :latency_ms, 10);
        total_queries := total_queries + 1;
    END FOR;
    CLOSE c1;

    -- Profile 2: no_rerank (graceful skip if unavailable)
    OPEN c2;
    FOR rec IN c2 DO
        cur_qid := rec.QUERY_ID;
        cur_qtext := rec.QUERY_TEXT;
        start_ts := CURRENT_TIMESTAMP();

        BEGIN
            search_sql := 'SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(''' || :P_SERVICE_FQN || ''', ''{' ||
                '"query": "' || REPLACE(REPLACE(:cur_qtext, '"', '\\"'), '''', '''''') || '", ' ||
                '"columns": ["CHUNK_ID", "COMPANY_NAME", "FORM_TYPE"], ' ||
                '"scoring_profile": "no_rerank", ' ||
                '"limit": 10' ||
                '}'')) AS r';
            EXECUTE IMMEDIATE :search_sql;
        EXCEPTION
            WHEN OTHER THEN
                CLOSE c2;
                DROP TABLE IF EXISTS _BENCH_QUERIES;
                RETURN 'Benchmark partial: ' || :total_queries::VARCHAR || ' queries (default only — no_rerank unavailable on ' || :P_SERVICE_FQN || ')';
        END;

        end_ts := CURRENT_TIMESTAMP();
        latency_ms := DATEDIFF('millisecond', :start_ts, :end_ts);

        INSERT INTO SEARCH_LATENCY_RESULTS
            (SERVICE_NAME, SCORING_PROFILE, QUERY_TYPE, QUERY_ID, QUERY_TEXT, LATENCY_MS, RESULT_COUNT)
        VALUES (:service_short, 'no_rerank', 'semantic', :cur_qid, :cur_qtext, :latency_ms, 10);
        total_queries := total_queries + 1;
    END FOR;
    CLOSE c2;

    DROP TABLE IF EXISTS _BENCH_QUERIES;
    RETURN 'Benchmark complete: ' || :total_queries::VARCHAR || ' queries (' || :P_SERVICE_FQN || ')';
END;
$$;


-- =============================================================================
-- EXECUTION
-- =============================================================================
-- After search service is ACTIVE:
-- CALL SEARCH_LATENCY_BENCHMARK('<db>.<schema>.<search_service_name>');

-- Comparison report:
-- SELECT SERVICE_NAME, SCORING_PROFILE,
--        ROUND(AVG(LATENCY_MS), 0) AS avg_ms,
--        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LATENCY_MS), 0) AS p50_ms,
--        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LATENCY_MS), 0) AS p95_ms
-- FROM SEARCH_LATENCY_RESULTS
-- WHERE RUN_TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
-- GROUP BY 1, 2 ORDER BY 1, 2;
