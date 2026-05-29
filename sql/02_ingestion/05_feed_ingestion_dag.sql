-- =============================================================================
-- 05: Feed Ingestion Task DAG (Parallel Monthly, Multi-Year)
-- =============================================================================
-- Server-side task DAG for bulk feed archive ingestion.
-- 12 monthly tasks run in PARALLEL (one per month), each calling
-- LOAD_FEED_DATE_RANGE for its month. Multi-cluster warehouse auto-scales.
--
-- Multi-year support: The finalizer loops through years from ingest_start_year
-- to ingest_end_year via _PIPELINE_CONFIG['current_ingestion_year']. After each
-- year completes successfully, it advances to the next. On failure, it retries
-- up to 2 times before stopping.
--
-- Architecture:
--   T_FEED_INGEST_ROOT (ensures config, clears partial log entries)
--   ├── T_FEED_JAN through T_FEED_DEC (12 parallel, read current_ingestion_year)
--   ├── T_FEED_VALIDATE (cleanup: orphans, dupes, content orphans)
--   └── T_FEED_INGEST_FINALIZER:
--         IF orphans > 0 AND retries >= 2: STOP
--         ELIF orphans > 0: RETRY (re-trigger ROOT)
--         ELIF current_year < end_year: ADVANCE (increment year, re-trigger ROOT)
--         ELSE: COMPLETE (chain to T_ENRICH_ROOT)
--
-- Rate limit safety: Each task makes ~1 HTTP request per 1-3 minutes.
-- 12 parallel = max 0.36 req/sec (SEC limit is 10 req/sec = 3.6% utilization).
--
-- Progress monitoring (from any session):
--   SELECT status, COUNT(*) AS days, SUM(loaded) AS filings_loaded
--   FROM _FEED_INGEST_LOG GROUP BY 1;
--
-- How to run:
--   1. Run 00_config.sql
--   2. Set _PIPELINE_CONFIG: ingest_start_year, ingest_end_year, current_ingestion_year
--   3. Run this script (creates tasks)
--   4. EXECUTE TASK T_FEED_INGEST_ROOT;
--   5. Pipeline auto-loops years until complete, then chains to enrichment
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Progress table (if not already created by schema script)
-- =============================================================================

CREATE TABLE IF NOT EXISTS _FEED_INGEST_LOG (
    FEED_DATE       VARCHAR(10)    NOT NULL,
    LOADED          INT            DEFAULT 0,
    STATUS          VARCHAR(20)    DEFAULT 'STARTED',
    STARTED_AT      TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Feed ingestion progress tracking. Query for % complete during long runs.';

-- =============================================================================
-- Root Task (manual trigger — CRON set to never-fire)
-- =============================================================================

CREATE OR REPLACE TASK T_FEED_INGEST_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    SCHEDULE = 'USING CRON 0 0 29 2 * UTC'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    COMMENT = 'Root: multi-year feed ingestion DAG. Loops years via finalizer.'
AS
BEGIN
    -- Ensure current_ingestion_year is set in config
    LET cur_year VARCHAR;
    BEGIN
        SELECT VALUE INTO :cur_year FROM _PIPELINE_CONFIG WHERE KEY = 'current_ingestion_year';
    EXCEPTION WHEN OTHER THEN cur_year := NULL; END;
    IF (:cur_year IS NULL) THEN
        INSERT INTO _PIPELINE_CONFIG VALUES ('current_ingestion_year', _CFG('ingest_start_year'));
    END IF;
    -- Ensure feed_retry_count exists
    LET retry_val VARCHAR;
    BEGIN
        SELECT VALUE INTO :retry_val FROM _PIPELINE_CONFIG WHERE KEY = 'feed_retry_count';
    EXCEPTION WHEN OTHER THEN
        INSERT INTO _PIPELINE_CONFIG VALUES ('feed_retry_count', '0');
    END;
    -- Clear partial/error log entries from previous failed runs (preserves DONE + SKIPPED)
    DELETE FROM _FEED_INGEST_LOG WHERE STATUS NOT IN ('DONE', 'SKIPPED_404', 'SKIPPED_403');
END;


-- =============================================================================
-- Monthly Tasks (12 parallel — all AFTER root)
-- =============================================================================

CREATE OR REPLACE TASK T_FEED_JAN
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: January'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-01-01',
    _CFG('current_ingestion_year') || '-01-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_FEB
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: February (ends 28th — for leap years, manually load Feb 29)'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-02-01',
    _CFG('current_ingestion_year') || '-02-28',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_MAR
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: March'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-03-01',
    _CFG('current_ingestion_year') || '-03-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_APR
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: April'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-04-01',
    _CFG('current_ingestion_year') || '-04-30',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_MAY
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: May'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-05-01',
    _CFG('current_ingestion_year') || '-05-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_JUN
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: June'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-06-01',
    _CFG('current_ingestion_year') || '-06-30',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_JUL
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: July'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-07-01',
    _CFG('current_ingestion_year') || '-07-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_AUG
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: August'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-08-01',
    _CFG('current_ingestion_year') || '-08-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_SEP
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: September'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-09-01',
    _CFG('current_ingestion_year') || '-09-30',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_OCT
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: October'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-10-01',
    _CFG('current_ingestion_year') || '-10-31',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_NOV
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: November'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-11-01',
    _CFG('current_ingestion_year') || '-11-30',
    _CFG('user_agent')
);

CREATE OR REPLACE TASK T_FEED_DEC
    WAREHOUSE = IDENTIFIER($config_warehouse_ingest)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Feed ingestion: December'
    AFTER T_FEED_INGEST_ROOT
AS CALL LOAD_FEED_DATE_RANGE(
    _CFG('current_ingestion_year') || '-12-01',
    _CFG('current_ingestion_year') || '-12-31',
    _CFG('user_agent')
);


-- =============================================================================
-- Validation (runs after ALL monthly tasks, before finalizer)
-- =============================================================================
-- Cleans orphaned INDEX rows (no CONTENT) and corrupt CONTENT rows.
-- Ensures data integrity before downstream DAGs process the data.
-- Runs even if some monthly tasks failed (by design — cleans up after failures).

CREATE OR REPLACE TASK T_FEED_VALIDATE
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 1200000
    AFTER T_FEED_JAN, T_FEED_FEB, T_FEED_MAR, T_FEED_APR, T_FEED_MAY, T_FEED_JUN,
          T_FEED_JUL, T_FEED_AUG, T_FEED_SEP, T_FEED_OCT, T_FEED_NOV, T_FEED_DEC
AS
BEGIN
    LET orphan_count INT := 0;
    LET empty_count INT := 0;

    -- 1. Remove orphaned INDEX rows (no matching CONTENT or corrupt content)
    DELETE FROM FILING_INDEX fi
    USING (
        SELECT fi2.ACCESSION_NO FROM FILING_INDEX fi2
        LEFT JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = fi2.ACCESSION_NO
        WHERE fc.ACCESSION_NO IS NULL
           OR fc.CONTENT_TEXT IS NULL
           OR LENGTH(fc.CONTENT_TEXT) < 100
    ) orphans
    WHERE fi.ACCESSION_NO = orphans.ACCESSION_NO;
    orphan_count := SQLROWCOUNT;

    -- 2. Remove empty/corrupt content rows
    DELETE FROM FILING_CONTENT
    WHERE CONTENT_TEXT IS NULL OR LENGTH(CONTENT_TEXT) < 100;
    empty_count := SQLROWCOUNT;

    -- 3. De-duplicate FILING_INDEX (Snowflake PKs are not enforced;
    --    parallel tasks can insert the same ACCESSION_NO at quarter boundaries)
    LET dup_count INT := 0;
    CREATE TEMPORARY TABLE _DEDUP_KEEP AS
        SELECT * FROM FILING_INDEX
        WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM FILING_INDEX GROUP BY 1 HAVING COUNT(*) > 1)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCESSION_NO ORDER BY FILED_AT) = 1;
    SELECT COUNT(*) INTO :dup_count FROM _DEDUP_KEEP;
    IF (:dup_count > 0) THEN
        DELETE FROM FILING_INDEX WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM _DEDUP_KEEP);
        INSERT INTO FILING_INDEX SELECT * FROM _DEDUP_KEEP;
    END IF;
    DROP TABLE IF EXISTS _DEDUP_KEEP;

    -- 3b. De-duplicate FILING_CONTENT (same race condition as INDEX)
    LET content_dup_count INT := 0;
    CREATE TEMPORARY TABLE _CONTENT_DEDUP_KEEP AS
        SELECT * FROM FILING_CONTENT
        WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM FILING_CONTENT GROUP BY 1 HAVING COUNT(*) > 1)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCESSION_NO ORDER BY ACCESSION_NO) = 1;
    SELECT COUNT(*) INTO :content_dup_count FROM _CONTENT_DEDUP_KEEP;
    IF (:content_dup_count > 0) THEN
        DELETE FROM FILING_CONTENT WHERE ACCESSION_NO IN (SELECT ACCESSION_NO FROM _CONTENT_DEDUP_KEEP);
        INSERT INTO FILING_CONTENT SELECT * FROM _CONTENT_DEDUP_KEEP;
    END IF;
    DROP TABLE IF EXISTS _CONTENT_DEDUP_KEEP;

    -- 4. Remove orphaned CONTENT rows (CONTENT without matching INDEX)
    LET content_orphan_count INT := 0;
    DELETE FROM FILING_CONTENT fc
    WHERE NOT EXISTS (SELECT 1 FROM FILING_INDEX fi WHERE fi.ACCESSION_NO = fc.ACCESSION_NO);
    content_orphan_count := SQLROWCOUNT;

    -- 5. Completeness check: gap-fill for current ingestion year
    --    Calls FILL_FEED_GAPS for each day marked DONE/PARTIAL in current year
    --    that may have gaps vs the EDGAR daily index.
    LET current_year VARCHAR;
    LET gap_fill_result VARCHAR;
    LET gaps_filled INT := 0;
    SELECT VALUE INTO :current_year FROM _PIPELINE_CONFIG WHERE KEY = 'current_ingestion_year';

    -- Call the standalone audit SP for the current year (auto-fill enabled)
    CALL VALIDATE_FEED_COMPLETENESS(:current_year::INT, :current_year::INT, TRUE);
    gap_fill_result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

    RETURN 'Validation: ' || orphan_count || ' orphans, '
           || empty_count || ' empty content, '
           || dup_count || ' index dupes, '
           || content_dup_count || ' content dupes, '
           || content_orphan_count || ' content orphans removed. '
           || 'Gap fill: ' || :gap_fill_result;
END;


-- =============================================================================
-- Finalizer (year loop: retry on failure, advance on success, chain on final year)
-- =============================================================================

CREATE OR REPLACE TASK T_FEED_INGEST_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = T_FEED_INGEST_ROOT
    COMMENT = 'Finalizer: year loop — retries on failure, advances on success, chains to enrichment on final year'
AS
BEGIN
    LET current_year INT := _CFG('current_ingestion_year')::INT;
    LET end_year INT := _CFG('ingest_end_year')::INT;
    LET orphan_count INT := 0;
    LET done_days INT; LET total_filings INT;
    LET index_count INT; LET content_count INT;
    LET retry_count INT := 0;

    SELECT COUNT(*) INTO :done_days FROM _FEED_INGEST_LOG WHERE STATUS = 'DONE';
    SELECT COALESCE(SUM(LOADED), 0) INTO :total_filings FROM _FEED_INGEST_LOG WHERE STATUS = 'DONE';
    SELECT COUNT(*) INTO :index_count FROM FILING_INDEX;
    SELECT COUNT(*) INTO :content_count FROM FILING_CONTENT;

    SELECT COUNT(*) INTO :orphan_count
    FROM FILING_INDEX fi
    LEFT JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = fi.ACCESSION_NO
    WHERE fc.ACCESSION_NO IS NULL;

    BEGIN
        SELECT VALUE::INT INTO :retry_count FROM _PIPELINE_CONFIG WHERE KEY = 'feed_retry_count';
    EXCEPTION WHEN OTHER THEN retry_count := 0; END;

    -- Safety cap: defense against unforeseen infinite loop bugs
    IF (:current_year > :end_year + 1) THEN
        CALL SYSTEM$SEND_EMAIL(_CFG('email_integration'), _CFG('email_recipient'),
            'SEC Filing Feed: SAFETY STOP',
            'current_year=' || :current_year::VARCHAR || ' exceeds end_year=' || :end_year::VARCHAR || '. Manual intervention required.');
        RETURN 'SAFETY STOP';
    END IF;

    -- Determine next action
    LET next_action VARCHAR;

    -- Calculate percentage complete for this year (weekdays only)
    LET total_weekdays INT := 0;
    SELECT COUNT(*) INTO :total_weekdays
    FROM (SELECT DATEADD(day, seq4(), :current_year::VARCHAR || '-01-01')::DATE AS d
          FROM TABLE(GENERATOR(ROWCOUNT => 366))
          WHERE d <= :current_year::VARCHAR || '-12-31'
            AND DAYOFWEEK(d) NOT IN (0, 6));
    -- Count DONE + SKIPPED + INCOMPLETE as "complete" for advancement purposes
    -- (INCOMPLETE = gap-filler already ran, remaining gaps are permanently unfillable)
    LET year_done_days INT := 0;
    SELECT COUNT(*) INTO :year_done_days FROM _FEED_INGEST_LOG
    WHERE STATUS IN ('DONE', 'INCOMPLETE', 'SKIPPED_404', 'SKIPPED_403')
      AND FEED_DATE >= :current_year::VARCHAR || '-01-01'
      AND FEED_DATE <= :current_year::VARCHAR || '-12-31';
    LET pct_done FLOAT := ROUND(:year_done_days::FLOAT / NULLIF(:total_weekdays, 0) * 100, 1);

    IF (:orphan_count > 0 AND :pct_done >= 90.0) THEN
        -- >= 90% complete — advance despite orphans (remaining days are too large or holidays)
        UPDATE _PIPELINE_CONFIG SET VALUE = '0' WHERE KEY = 'feed_retry_count';
        IF (:current_year < :end_year) THEN
            UPDATE _PIPELINE_CONFIG SET VALUE = (:current_year + 1)::VARCHAR WHERE KEY = 'current_ingestion_year';
            next_action := 'ADVANCING (90%+) — Year ' || :current_year::VARCHAR || ' is ' || :pct_done::VARCHAR || '% complete (' || :year_done_days::VARCHAR || '/' || :total_weekdays::VARCHAR || ' days). Skipping ' || :orphan_count::VARCHAR || ' orphans. Moving to ' || (:current_year + 1)::VARCHAR || '.';
            EXECUTE TASK T_FEED_INGEST_ROOT;
        ELSE
            next_action := 'COMPLETE (90%+) — Final year ' || :current_year::VARCHAR || ' is ' || :pct_done::VARCHAR || '% complete. Triggering T_ENRICH_ROOT.';
            EXECUTE TASK T_ENRICH_ROOT;
        END IF;
    ELSEIF (:orphan_count > 0 AND :retry_count >= 2) THEN
        -- Too many retries for this year — stop
        next_action := 'STOPPED — Year ' || :current_year::VARCHAR || ' failed after ' || :retry_count::VARCHAR || ' retries (' || :pct_done::VARCHAR || '% complete). Manual intervention required.';
    ELSEIF (:orphan_count > 0) THEN
        -- Retry this year (increment counter, re-trigger ROOT)
        UPDATE _PIPELINE_CONFIG SET VALUE = (:retry_count + 1)::VARCHAR WHERE KEY = 'feed_retry_count';
        next_action := 'RETRYING — Year ' || :current_year::VARCHAR || ' has ' || :orphan_count::VARCHAR || ' orphans (' || :pct_done::VARCHAR || '% complete, attempt ' || (:retry_count + 1)::VARCHAR || '/2). Re-triggering T_FEED_INGEST_ROOT.';
        EXECUTE TASK T_FEED_INGEST_ROOT;
    ELSEIF (:pct_done >= 90.0 AND :current_year < :end_year) THEN
        -- Success (>=90%): reset retry counter and advance to next year
        UPDATE _PIPELINE_CONFIG SET VALUE = '0' WHERE KEY = 'feed_retry_count';
        UPDATE _PIPELINE_CONFIG SET VALUE = (:current_year + 1)::VARCHAR WHERE KEY = 'current_ingestion_year';
        next_action := 'ADVANCING — Year ' || :current_year::VARCHAR || ' is ' || :pct_done::VARCHAR || '% complete (' || :year_done_days::VARCHAR || '/' || :total_weekdays::VARCHAR || ' days). Moving to ' || (:current_year + 1)::VARCHAR || '.';
        EXECUTE TASK T_FEED_INGEST_ROOT;
    ELSEIF (:pct_done >= 90.0) THEN
        -- Final year complete (>=90%): chain to enrichment
        UPDATE _PIPELINE_CONFIG SET VALUE = '0' WHERE KEY = 'feed_retry_count';
        next_action := 'COMPLETE — All years (' || _CFG('ingest_start_year') || '-' || :end_year::VARCHAR || ') done (' || :pct_done::VARCHAR || '%). Triggering T_ENRICH_ROOT.';
        EXECUTE TASK T_ENRICH_ROOT;
    ELSEIF (:retry_count >= 2) THEN
        -- Under 90% after max retries — stop
        next_action := 'STOPPED — Year ' || :current_year::VARCHAR || ' at ' || :pct_done::VARCHAR || '% (' || :year_done_days::VARCHAR || '/' || :total_weekdays::VARCHAR || ' days) after ' || :retry_count::VARCHAR || ' retries. Manual intervention required.';
    ELSE
        -- Under 90%, retry (increment counter, re-trigger ROOT)
        UPDATE _PIPELINE_CONFIG SET VALUE = (:retry_count + 1)::VARCHAR WHERE KEY = 'feed_retry_count';
        next_action := 'RETRYING (low coverage) — Year ' || :current_year::VARCHAR || ' at ' || :pct_done::VARCHAR || '% (' || :year_done_days::VARCHAR || '/' || :total_weekdays::VARCHAR || ' days, attempt ' || (:retry_count + 1)::VARCHAR || '/2). Re-triggering T_FEED_INGEST_ROOT.';
        EXECUTE TASK T_FEED_INGEST_ROOT;
    END IF;

    -- Email always includes what happened + what's being triggered
    LET msg VARCHAR := 'FEED INGESTION: Year ' || :current_year::VARCHAR || CHR(10) || CHR(10) ||
        'Days processed (DONE): ' || :done_days::VARCHAR || CHR(10) ||
        'Year progress: ' || :year_done_days::VARCHAR || '/' || :total_weekdays::VARCHAR || ' weekdays (' || :pct_done::VARCHAR || '%)' || CHR(10) ||
        'Filings loaded: ' || :total_filings::VARCHAR || CHR(10) ||
        'Total INDEX: ' || :index_count::VARCHAR || CHR(10) ||
        'Total CONTENT: ' || :content_count::VARCHAR || CHR(10) ||
        'Orphans: ' || :orphan_count::VARCHAR || CHR(10) ||
        'Retry count: ' || :retry_count::VARCHAR || '/2' || CHR(10) ||
        'Next: ' || :next_action || CHR(10) ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR;

    CALL SYSTEM$SEND_EMAIL(
        _CFG('email_integration'),
        _CFG('email_recipient'),
        'SEC Filing Feed: Year ' || :current_year::VARCHAR || ' — ' ||
            IFF(:orphan_count > 0 AND :retry_count >= 2, 'STOPPED',
            IFF(:orphan_count > 0, 'RETRYING',
            IFF(:current_year < :end_year, 'ADVANCING', 'COMPLETE'))),
        :msg
    );
END;


-- =============================================================================
-- Resume all tasks (enable the DAG)
-- =============================================================================

ALTER TASK T_FEED_JAN RESUME;
ALTER TASK T_FEED_FEB RESUME;
ALTER TASK T_FEED_MAR RESUME;
ALTER TASK T_FEED_APR RESUME;
ALTER TASK T_FEED_MAY RESUME;
ALTER TASK T_FEED_JUN RESUME;
ALTER TASK T_FEED_JUL RESUME;
ALTER TASK T_FEED_AUG RESUME;
ALTER TASK T_FEED_SEP RESUME;
ALTER TASK T_FEED_OCT RESUME;
ALTER TASK T_FEED_NOV RESUME;
ALTER TASK T_FEED_DEC RESUME;
ALTER TASK T_FEED_VALIDATE RESUME;
ALTER TASK T_FEED_INGEST_FINALIZER RESUME;
ALTER TASK T_FEED_INGEST_ROOT RESUME;


-- =============================================================================
-- EXECUTION + MONITORING
-- =============================================================================
-- Trigger:
--   EXECUTE TASK T_FEED_INGEST_ROOT;
--
-- Task graph status:
--   SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, RETURN_VALUE, ERROR_MESSAGE
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--       SCHEDULED_TIME_RANGE_START => DATEADD('hour', -48, CURRENT_TIMESTAMP()),
--       RESULT_LIMIT => 50
--   ))
--   WHERE ROOT_TASK_ID = (SELECT SYSTEM$TASK_FIND_ROOT_ID('T_FEED_INGEST_ROOT'))
--   ORDER BY SCHEDULED_TIME DESC;
--
-- Progress (% complete):
--   SELECT
--       COUNT(CASE WHEN STATUS = 'DONE' THEN 1 END) AS days_done,
--       COUNT(CASE WHEN STATUS = 'LOADING' THEN 1 END) AS days_in_progress,
--       COUNT(*) AS total_days_attempted,
--       SUM(CASE WHEN STATUS = 'DONE' THEN LOADED ELSE 0 END) AS filings_loaded,
--       ROUND(100.0 * COUNT(CASE WHEN STATUS = 'DONE' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS pct_complete
--   FROM _FEED_INGEST_LOG;
