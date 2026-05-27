-- =============================================================================
-- 02: Scheduled Task for Overnight Batch Analysis
-- =============================================================================
-- Runs EXPLORER_SECTOR_ANALYSIS across all sectors overnight.
-- Emails summary when complete.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Batch task: runs nightly, processes one sector per execution
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXPLORER_BATCH_ALL_SECTORS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    cur_sector VARCHAR;
    processed INT DEFAULT 0;
    c1 CURSOR FOR
        SELECT DISTINCT INDUSTRY_SECTOR
        FROM FILING_INDEX
        WHERE INDUSTRY_SECTOR IS NOT NULL
        ORDER BY INDUSTRY_SECTOR;
BEGIN
    OPEN c1;
    FOR rec IN c1 DO
        cur_sector := rec.INDUSTRY_SECTOR;
        CALL EXPLORER_SECTOR_ANALYSIS('SECTOR', :cur_sector);
        processed := processed + 1;
    END FOR;
    CLOSE c1;

    RETURN 'Batch complete: ' || :processed || ' sectors analyzed';
END;
$$;

-- Scheduled task (runs weekly, suspended by default)
CREATE OR REPLACE TASK EXPLORER_WEEKLY_BATCH
    WAREHOUSE = IDENTIFIER($config_warehouse)
    SCHEDULE = 'USING CRON 0 2 * * 0 America/New_York'
    COMMENT = 'Weekly overnight sector analysis batch. Suspend when not needed.'
AS CALL EXPLORER_BATCH_ALL_SECTORS();

-- Task remains SUSPENDED by default — resume when ready:
-- ALTER TASK EXPLORER_WEEKLY_BATCH RESUME;
