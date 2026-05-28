-- =============================================================================
-- 05: Trigger Sector Analysis (Async via Dynamic Task)
-- =============================================================================
-- Creates and executes a one-shot task to run EXPLORER_CUSTOM_ANALYSIS
-- asynchronously. The Streamlit app calls this instead of the SP directly
-- so it doesn't block the UI for long-running full-sector analyses.
--
-- The task:
--   - Named EXPLORER_RUN_<YYYYMMDDHHMMSS> (unique per invocation)
--   - Runs on FILING_WH
--   - Executes EXPLORER_CUSTOM_ANALYSIS with the given parameters
--   - Can be monitored via INFORMATION_SCHEMA.TASK_HISTORY
--   - Results appear in EXPLORER_RESULTS as they're inserted
--
-- Dependencies:
--   - EXPLORER_CUSTOM_ANALYSIS SP (04_custom_analysis.sql)
--   - FILING_WH warehouse
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: TRIGGER_SECTOR_ANALYSIS
-- =============================================================================

CREATE OR REPLACE PROCEDURE TRIGGER_SECTOR_ANALYSIS(
    P_SECTOR VARCHAR,
    P_QUERY TEXT DEFAULT NULL,
    P_SECTION VARCHAR DEFAULT NULL,
    P_FORM_TYPE VARCHAR DEFAULT NULL,
    P_OUTPUT_MODE VARCHAR DEFAULT 'excerpts',
    P_LIMIT INT DEFAULT NULL,
    P_MODEL VARCHAR DEFAULT 'llama3.3-70b'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    task_name VARCHAR;
    task_ddl VARCHAR;
    sp_call VARCHAR;
    query_arg VARCHAR;
    section_arg VARCHAR;
    form_arg VARCHAR;
    limit_arg VARCHAR;
    model_arg VARCHAR;
BEGIN
    -- Generate unique task name
    task_name := 'EXPLORER_RUN_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- Build SP argument strings (handle NULLs)
    query_arg := CASE WHEN :P_QUERY IS NOT NULL THEN '''' || REPLACE(:P_QUERY, '''', '''''') || '''' ELSE 'NULL' END;
    section_arg := CASE WHEN :P_SECTION IS NOT NULL THEN '''' || :P_SECTION || '''' ELSE 'NULL' END;
    form_arg := CASE WHEN :P_FORM_TYPE IS NOT NULL THEN '''' || :P_FORM_TYPE || '''' ELSE 'NULL' END;
    limit_arg := CASE WHEN :P_LIMIT IS NOT NULL THEN :P_LIMIT::VARCHAR ELSE 'NULL' END;
    model_arg := '''' || :P_MODEL || '''';

    -- Build the CALL statement
    sp_call := 'CALL EXPLORER_CUSTOM_ANALYSIS(''' || :P_SECTOR || ''', ' ||
               :query_arg || ', ' || :section_arg || ', ' || :form_arg || ', ''' ||
               :P_OUTPUT_MODE || ''', ' || :limit_arg || ', ' || :model_arg || ')';

    -- Create the task
    task_ddl := 'CREATE OR REPLACE TASK ' || :task_name ||
                ' WAREHOUSE = FILING_WH' ||
                ' SCHEDULE = ''USING CRON 0 0 29 2 * UTC''' ||
                ' COMMENT = ''Async sector analysis: ' || :P_SECTOR || ' (' || :P_OUTPUT_MODE || ')''' ||
                ' AS ' || :sp_call;

    EXECUTE IMMEDIATE :task_ddl;

    -- Execute the task (runs immediately regardless of schedule)
    EXECUTE IMMEDIATE 'EXECUTE TASK ' || :task_name;

    RETURN 'Sector analysis triggered. Task: ' || :task_name || '. Results will appear in EXPLORER_RESULTS as companies are processed.';
END;
$$;
