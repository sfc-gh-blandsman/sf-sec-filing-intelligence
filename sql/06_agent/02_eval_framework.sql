-- =============================================================================
-- 02: Evaluation Framework
-- =============================================================================
-- Sets up eval dataset + manual eval workflow + materialize DAG.
--
-- Components:
--   1. YAML file format + stage for eval config
--   2. Python SP to generate eval config YAML (no run_params, no dataset block)
--   3. _CREATE_EVAL_DATASET() SP for one-time dataset registration
--   4. Eval dataset table (28 questions, VARIANT ground truth)
--   5. _RUN_EVAL() SP for manual eval execution (interactive session)
--   6. Task DAG: ROOT → MATERIALIZE → BENCHMARK → FINALIZER (results collection only)
--
-- Workflow:
--   1. CALL _RUN_EVAL();           -- starts eval (manual, interactive session)
--   2. Poll STATUS until COMPLETED  -- both metrics score in interactive context
--   3. EXECUTE TASK EVAL_DAG_ROOT;  -- materializes results + runs benchmark + emails
--
-- Why manual: SNOW-3490805 causes logical_consistency to fail when started from
-- within a Task. It works correctly in interactive sessions (proven at 71.4%).
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- Stage + File Format for Eval Config YAML
-- =============================================================================
-- Snowflake requires a specific file format for eval YAML files.

CREATE OR REPLACE FILE FORMAT YAML_FILE_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE
  ESCAPE_UNENCLOSED_FIELD = NONE;

CREATE OR REPLACE STAGE EVAL_CONFIGS
  FILE_FORMAT = YAML_FILE_FORMAT
  COMMENT = 'Evaluation configuration YAML files';

-- =============================================================================
-- Eval Config YAML Generator (minimal — matches POC V2 pattern)
-- =============================================================================
-- Generates a minimal YAML with only evaluation: + metrics: blocks.
-- No dataset: block (dataset pre-created via _CREATE_EVAL_DATASET).
-- No run_params: block (run_name is passed via EXECUTE_AI_EVALUATION call).
-- This matches the EDGAR_V2_AGENT POC's eval config that successfully scored
-- both answer_correctness and logical_consistency on 28 questions.

CREATE OR REPLACE PROCEDURE _WRITE_EVAL_CONFIG()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'write_config'
EXECUTE AS CALLER
AS '
def write_config(session):
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    agent_name = session.sql("SELECT VALUE FROM _PIPELINE_CONFIG WHERE KEY = ''agent_name''").collect()[0][0]
    agent_fqn = f"{db}.{schema}.{agent_name}"
    ds_name = session.sql("SELECT VALUE FROM _PIPELINE_CONFIG WHERE KEY = ''eval_dataset_name''").collect()
    dataset_name = ds_name[0][0] if ds_name else "SEC_FILING_EVAL_DATASET_V8"
    dataset_fqn = f"{db}.{schema}.{dataset_name}"

    # Minimal YAML — matches POC pattern that scored logical_consistency
    # Uses FQN for both agent_name and dataset_name (required for task context per SNOW-3349334)
    yaml_content = f"""evaluation:
  agent_params:
    agent_name: "{agent_fqn}"
    agent_type: "CORTEX AGENT"
  source_metadata:
    type: "dataset"
    dataset_name: "{dataset_fqn}"

metrics:
  - "answer_correctness"
  - "logical_consistency"
"""
    import tempfile, os
    stage_path = f"@{db}.{schema}.EVAL_CONFIGS"
    tmp_dir = tempfile.mkdtemp()
    filepath = os.path.join(tmp_dir, ''eval_config.yaml'')
    with open(filepath, ''w'') as f:
        f.write(yaml_content)
    session.file.put(filepath, stage_path, auto_compress=False, overwrite=True)
    os.unlink(filepath)
    os.rmdir(tmp_dir)
    return f''eval_config.yaml uploaded (dataset={dataset_name}, agent={agent_fqn})''
';

-- =============================================================================
-- Eval Dataset Registration (call once after adding/changing questions)
-- =============================================================================
-- Creates a stable Snowflake Dataset from the SEC_FILING_EVAL_DATASET table.
-- Call this ONCE after modifying questions, not on every eval run.
-- Per docs: use SYSTEM$CREATE_EVALUATION_DATASET for SQL-based dataset creation.

CREATE OR REPLACE PROCEDURE _CREATE_EVAL_DATASET()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    LET db VARCHAR := _CFG('database');
    LET sch VARCHAR := _CFG('schema');
    LET table_fqn VARCHAR := :db || '.' || :sch || '.SEC_FILING_EVAL_DATASET';
    LET ds_fqn VARCHAR := :db || '.' || :sch || '.SEC_FILING_EVAL_DATASET_V8';
    CALL SYSTEM$CREATE_EVALUATION_DATASET(
        'Cortex Agent',
        :table_fqn,
        :ds_fqn,
        OBJECT_CONSTRUCT('query_text', 'INPUT_QUERY', 'expected_tools', 'GROUND_TRUTH')
    );
    RETURN 'Dataset ' || :ds_fqn || ' created from ' || :table_fqn;
END;

-- =============================================================================
-- Eval Dataset: 28 questions for 2025 SEC filing data
-- =============================================================================
-- Ground truth uses VARIANT type with {"ground_truth_output": "..."} format
-- per Snowflake Agent Evaluation specification.

CREATE OR REPLACE TABLE SEC_FILING_EVAL_DATASET (
    INPUT_QUERY   VARCHAR NOT NULL,
    GROUND_TRUTH  VARIANT
);

-- 28 questions: 16 general + 3 risk-focused + 9 financial metrics
-- Note: Kept at 28 to stay within logical_consistency trace limits (SNOW-3490805).
-- If logical_consistency fails, reduce to 25 by removing financial metric questions.
INSERT INTO SEC_FILING_EVAL_DATASET
SELECT column1, PARSE_JSON(column2) FROM VALUES
-- General questions (analyst + search)
('How many 10-K filings had negative sentiment in Q1 2025?', '{"ground_truth_output": "Count of 10-K filings with SENTIMENT = NEGATIVE and SIGNAL_DATE between January and March 2025."}'),
('Show the monthly trend of M&A events in 2025.', '{"ground_truth_output": "Monthly counts of filings with event_type = M&A, grouped by month using SIGNAL_DATE."}'),
('Find 8-K filings from Technology companies reporting leadership changes.', '{"ground_truth_output": "8-K filings from Technology industry_sector with leadership change content, showing company names and filing dates."}'),
('What is the negative sentiment rate by industry sector?', '{"ground_truth_output": "Percentage of filings with NEGATIVE sentiment per INDUSTRY_SECTOR."}'),
('Quote the language about cybersecurity risks in a recent 10-K filing.', '{"ground_truth_output": "Direct quoted text from a 10-K Risk Factors section about cybersecurity, with company name and filing date."}'),
('How many filings were submitted by Finance companies in Q1 2025?', '{"ground_truth_output": "Total filing count where INDUSTRY_SECTOR = Finance and SIGNAL_DATE in Q1 2025."}'),
('Find 10-K Risk Factors discussing climate-related risks in Energy & Transportation filings.', '{"ground_truth_output": "Text from Energy & Transportation sector 10-K Risk Factors about climate change, carbon emissions, or environmental regulations."}'),
('Compare the number of positive vs negative filings across form types.', '{"ground_truth_output": "Comparison table showing positive and negative sentiment counts by form_type."}'),
('What did manufacturers discuss about supply chain resilience in their 10-K MD&A section?', '{"ground_truth_output": "Text from Manufacturing sector MD&A discussing supply chain strategies, inventory management, or supplier diversification."}'),
('Which companies had the most 8-K filings in 2025?', '{"ground_truth_output": "Top companies ranked by 8-K filing count in 2025."}'),
('Find a Real Estate & Construction company filing discussing interest rate impact.', '{"ground_truth_output": "Text from Real Estate & Construction sector filing about interest rate exposure or financing costs, with company and date."}'),
('What is the breakdown of event types for 8-K filings?', '{"ground_truth_output": "Distribution of event_type values for 8-K filings with counts per category."}'),
('Find Life Sciences 8-K filings about acquisitions.', '{"ground_truth_output": "8-K filings from Life Sciences sector with M&A event type or acquisition content."}'),
('How many guidance update events occurred in each month of Q1 2025?', '{"ground_truth_output": "Monthly counts of event_type = Guidance Update for Jan, Feb, Mar 2025."}'),
('What common risk factors appeared across multiple 2025 10-K filings?', '{"ground_truth_output": "Common themes across 2025 10-K Risk Factors: macroeconomic uncertainty, AI disruption, cybersecurity, regulatory changes."}'),
('Find filings discussing artificial intelligence risks or opportunities.', '{"ground_truth_output": "Text from filings mentioning AI, machine learning, or AI-related risks/opportunities with company names."}'),
-- Risk-focused questions (tests company-specific and sector-specific risk retrieval)
('What new risk factors did Microsoft disclose in their most recent 10-K filing?', '{"ground_truth_output": "Risk factor text from Microsoft Corp 10-K filing, showing company-specific risks such as competition, cybersecurity, AI regulation, cloud infrastructure, and product adoption. Should retrieve actual quoted language from the filing."}'),
('In the Finance sector, what are the most commonly discussed risk themes across 10-K filings?', '{"ground_truth_output": "Common risk themes across Finance sector 10-K filings synthesized from multiple filings. Should include themes like credit risk, interest rate exposure, regulatory compliance, cybersecurity, and market volatility."}'),
('What are the top risk factors discussed by Technology companies in their 10-K filings?', '{"ground_truth_output": "Common risk themes from Technology sector 10-K Risk Factors sections. Should include themes like cybersecurity, competition, AI disruption, regulatory changes, intellectual property, and talent retention, with cited examples from specific companies."}'),
-- Financial metrics questions (tests REVENUE, NET_INCOME, EPS, YOY_CHANGE, FORWARD_GUIDANCE columns)
('What was the revenue for the largest Technology companies that filed 10-Ks?', '{"ground_truth_output": "Revenue figures from Technology sector 10-K filings, showing company names and their reported revenue amounts from the REVENUE column."}'),
('How many companies reported negative net income in 2025 10-K filings, and which 5 had the largest losses?', '{"ground_truth_output": "A count of companies with negative NET_INCOME in 10-K filings from 2025, plus the top 5 by magnitude of loss with company name and net income figure."}'),
('Show the top companies by EPS from 10-K filings.', '{"ground_truth_output": "Companies ranked by EPS (earnings per share) from 10-K filings, showing company name and EPS value from the EPS column."}'),
('Which sectors had the most filings with year-over-year revenue growth?', '{"ground_truth_output": "Count of filings with non-null YOY_CHANGE by INDUSTRY_SECTOR, indicating which sectors had the most filings reporting year-over-year changes."}'),
('How many 10-K filings reported forward guidance?', '{"ground_truth_output": "Count of 10-K filings where FORWARD_GUIDANCE is not null, indicating how many companies provided forward-looking financial guidance."}'),
('What was AbbVie revenue and net income in their 2025 10-K?', '{"ground_truth_output": "AbbVie Inc. reported revenue of approximately $56 billion and net income of approximately $4-5 billion in their 2025 10-K filing. The exact figures should come from the REVENUE and NET_INCOME columns."}'),
('Compare revenue figures between Finance and Technology sector 10-K filings.', '{"ground_truth_output": "Revenue figures grouped by INDUSTRY_SECTOR for Finance vs Technology 10-K filings, showing representative companies and their revenue from the REVENUE column."}'),
('What did companies with declining revenue discuss in their risk factors?', '{"ground_truth_output": "Risk factor text from companies that have negative YOY_CHANGE values, showing what declining-revenue companies cited as risks. Should combine analyst (filter by YOY_CHANGE) then search (retrieve risk factor text)."}'),
('Find the MD&A section from a company that reported EPS growth.', '{"ground_truth_output": "MD&A text from a filing where EPS is positive and YOY_CHANGE indicates growth. Should use analyst to identify a company with positive metrics, then search for their MD&A content."}');


-- =============================================================================
-- Eval Results Table (enriched with per-question breakdown)
-- =============================================================================

CREATE TABLE IF NOT EXISTS EVAL_RESULTS (
    RUN_NAME            VARCHAR        NOT NULL,
    AGENT_NAME          VARCHAR        NOT NULL,
    INPUT               TEXT,
    OUTPUT              TEXT,
    GROUND_TRUTH        VARCHAR,
    METRIC_NAME         VARCHAR,
    EVAL_AGG_SCORE      FLOAT,
    CRITERIA            TEXT,
    EXPLANATION         TEXT,
    ORIGINAL_SCORE      INT,
    DURATION_MS         INT,
    TOTAL_INPUT_TOKENS  INT,
    TOTAL_OUTPUT_TOKENS INT,
    ERROR               VARCHAR,
    CAPTURED_AT         TIMESTAMP_TZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Materialized eval results with per-question score breakdown and reasoning.';


-- =============================================================================
-- Eval Run State (shared between _RUN_EVAL and MATERIALIZE DAG)
-- =============================================================================

CREATE TABLE IF NOT EXISTS _EVAL_LAST_RUN (
    RUN_NAME    VARCHAR,
    STARTED_AT  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- Manual Eval Runner (call interactively — both metrics score reliably)
-- =============================================================================
-- SNOW-3490805: logical_consistency fails to persist when eval is started from
-- within a Snowflake Task (trace size exceeds judge model limits in task context).
-- It works correctly in interactive sessions. This SP wraps the manual workflow.
--
-- Usage:
--   CALL _RUN_EVAL();                      -- starts eval, writes run_name
--   -- Wait ~5 min, then poll:
--   CALL EXECUTE_AI_EVALUATION('STATUS', OBJECT_CONSTRUCT('run_name',
--       (SELECT RUN_NAME FROM _EVAL_LAST_RUN)),
--       '@SEC_FILINGS.FILING_DATA.EVAL_CONFIGS/eval_config.yaml');
--   -- Once COMPLETED: materialize results
--   EXECUTE TASK EVAL_DAG_ROOT;

CREATE OR REPLACE PROCEDURE _RUN_EVAL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    -- Generate YAML
    CALL _WRITE_EVAL_CONFIG();

    LET db VARCHAR := _CFG('database');
    LET sch VARCHAR := _CFG('schema');
    LET stage_path VARCHAR := '@' || :db || '.' || :sch || '.EVAL_CONFIGS/eval_config.yaml';
    LET run_ts VARCHAR;
    SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD-HH24MISS') INTO :run_ts;
    LET run_name VARCHAR := 'eval-' || :run_ts;

    -- Write run_name to shared state (MATERIALIZE DAG reads this)
    INSERT OVERWRITE INTO _EVAL_LAST_RUN VALUES (:run_name, CURRENT_TIMESTAMP());

    -- Start eval
    CALL EXECUTE_AI_EVALUATION('START',
        OBJECT_CONSTRUCT('run_name', :run_name),
        :stage_path);

    RETURN 'Eval started: ' || :run_name || '. Poll STATUS until COMPLETED, then EXECUTE TASK EVAL_DAG_ROOT;';
END;

-- =============================================================================
-- Eval Task DAG
-- =============================================================================
-- DAG structure: ROOT → MATERIALIZE → BENCHMARK → FINALIZER
-- The eval itself is run MANUALLY via _RUN_EVAL() (both metrics score reliably
-- in interactive sessions). The DAG only materializes results + benchmarks.
-- This avoids SNOW-3490805 (logical_consistency persistence fails in task context).

-- Drop legacy tasks from older DAG design (if they exist)
DROP TASK IF EXISTS EVAL_DAG_MONITOR;
DROP TASK IF EXISTS EVAL_DAG_RUN_EVAL;

CREATE OR REPLACE TASK EVAL_DAG_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse)
    SCHEDULE = 'USING CRON 0 0 29 2 * UTC'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    COMMENT = 'Root: eval DAG. Trigger immediately after _RUN_EVAL() — MATERIALIZE task polls until eval completes.'
AS SELECT 1;

-- Step 1: Materialize eval results (polls GET_AI_EVALUATION_DATA for scored data)
CREATE OR REPLACE TASK EVAL_DAG_MATERIALIZE
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    AFTER EVAL_DAG_ROOT
AS
BEGIN
    LET db VARCHAR := _CFG('database');
    LET sch VARCHAR := _CFG('schema');
    LET agent VARCHAR := _CFG('agent_name');

    -- Read run_name from shared state table (written by _RUN_EVAL)
    LET run_name VARCHAR := '';
    SELECT RUN_NAME INTO :run_name FROM _EVAL_LAST_RUN LIMIT 1;
    IF (:run_name = '' OR :run_name IS NULL) THEN
        RETURN 'ERROR: No run_name found in _EVAL_LAST_RUN. Run _RUN_EVAL() first.';
    END IF;

    -- Poll until SCORED data appears (rows with METRIC_NAME IS NOT NULL)
    -- Exit early if all invocations complete but scoring failed
    LET row_count INT := 0;
    LET total_rows INT := 0;
    LET poll INT := 0;
    LOOP
        poll := poll + 1;
        BEGIN
            SELECT COUNT(*), COUNT(CASE WHEN METRIC_NAME IS NOT NULL THEN 1 END)
            INTO :total_rows, :row_count
            FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(
                :db, :sch, :agent, 'CORTEX AGENT', :run_name));
        EXCEPTION WHEN OTHER THEN total_rows := 0; row_count := 0; END;
        IF (:row_count > 0) THEN BREAK; END IF;
        -- Early exit: if all invocations done but 0 scored after 10 min, scoring failed
        IF (:total_rows >= 20 AND :row_count = 0 AND :poll >= 10) THEN
            RETURN 'EVAL SCORING FAILED (invocations=' || :total_rows::VARCHAR || ' but 0 scored). Run: ' || :run_name;
        END IF;
        IF (:poll >= 120) THEN RETURN 'TIMEOUT waiting for scored eval data. Run: ' || :run_name; END IF;
        CALL SYSTEM$WAIT(60);
    END LOOP;

    -- Materialize all scored rows
    INSERT INTO EVAL_RESULTS (RUN_NAME, AGENT_NAME, INPUT, OUTPUT, GROUND_TRUTH,
        METRIC_NAME, EVAL_AGG_SCORE, CRITERIA, EXPLANATION, ORIGINAL_SCORE,
        DURATION_MS, TOTAL_INPUT_TOKENS, TOTAL_OUTPUT_TOKENS, ERROR, CAPTURED_AT)
    SELECT :run_name, :db || '.' || :sch || '.' || :agent,
        r.INPUT, r.OUTPUT, r.GROUND_TRUTH::VARCHAR,
        r.METRIC_NAME, r.EVAL_AGG_SCORE,
        mc.VALUE:criteria::VARCHAR,
        mc.VALUE:explanation::VARCHAR,
        mc.VALUE:full_metadata:original_score::INT,
        r.DURATION_MS, r.TOTAL_INPUT_TOKENS, r.TOTAL_OUTPUT_TOKENS, r.ERROR,
        CURRENT_TIMESTAMP()
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_EVALUATION_DATA(:db, :sch, :agent, 'CORTEX AGENT', :run_name)) r,
    LATERAL FLATTEN(INPUT => r.METRIC_CALLS) mc
    WHERE r.METRIC_NAME IS NOT NULL;

    LET materialized INT;
    SELECT COUNT(*) INTO :materialized FROM EVAL_RESULTS WHERE RUN_NAME = :run_name;
    RETURN 'Eval ' || :run_name || ' materialized=' || :materialized::VARCHAR || ' rows';
END;

-- Step 2: Search latency benchmark
CREATE OR REPLACE TASK EVAL_DAG_BENCHMARK
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Runs search latency benchmark after eval completes'
    AFTER EVAL_DAG_MATERIALIZE
AS
BEGIN
    LET svc_fqn VARCHAR := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('search_service');
    CALL SEARCH_LATENCY_BENCHMARK(:svc_fqn);
END;

-- Step 3: Finalizer (email notification)
CREATE OR REPLACE TASK EVAL_DAG_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = EVAL_DAG_ROOT
    COMMENT = 'Finalizer: emails eval completion notice'
AS
BEGIN
    LET row_count INT := 0;
    LET avg_score FLOAT := 0;
    BEGIN
        SELECT COUNT(*), ROUND(AVG(EVAL_AGG_SCORE), 2)
        INTO :row_count, :avg_score
        FROM EVAL_RESULTS
        WHERE CAPTURED_AT >= DATEADD('hour', -2, CURRENT_TIMESTAMP());
    EXCEPTION WHEN OTHER THEN row_count := 0; END;

    LET msg VARCHAR := 'EVAL DAG COMPLETE' || CHR(10) || CHR(10) ||
        'Results materialized: ' || :row_count::VARCHAR || ' rows' || CHR(10) ||
        'Average score: ' || :avg_score::VARCHAR || CHR(10) ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR || CHR(10) || CHR(10) ||
        'Query results: SELECT INPUT, METRIC_NAME, EVAL_AGG_SCORE, EXPLANATION FROM EVAL_RESULTS ORDER BY CAPTURED_AT DESC';

    IF (_CFG('enable_dag_emails') = 'TRUE') THEN
        CALL SYSTEM$SEND_EMAIL(
            _CFG('email_integration'),
            _CFG('email_recipient'),
            'SEC Filing Agent Eval: COMPLETE (avg=' || :avg_score::VARCHAR || ')',
            :msg
        );
    END IF;
END;

-- Resume all child tasks
ALTER TASK EVAL_DAG_MATERIALIZE RESUME;
ALTER TASK EVAL_DAG_BENCHMARK RESUME;
ALTER TASK EVAL_DAG_FINALIZER RESUME;
ALTER TASK EVAL_DAG_ROOT RESUME;


-- =============================================================================
-- EXECUTION: First time setup (or after changing questions)
-- =============================================================================
-- CALL _CREATE_EVAL_DATASET();  -- registers dataset from table (run once)

-- =============================================================================
-- EXECUTION: Run evaluation (manual — interactive session required)
-- =============================================================================
-- CALL _RUN_EVAL();
-- EXECUTE TASK EVAL_DAG_ROOT;  -- trigger immediately; MATERIALIZE polls until eval completes
--
-- The MATERIALIZE task polls GET_AI_EVALUATION_DATA every 60s (up to 2 hours)
-- waiting for scored results. No need to manually poll STATUS first.

-- =============================================================================
-- USEFUL QUERIES
-- =============================================================================
-- Per-question scores with reasoning:
-- SELECT INPUT, METRIC_NAME, EVAL_AGG_SCORE, LEFT(EXPLANATION, 200)
-- FROM EVAL_RESULTS ORDER BY EVAL_AGG_SCORE ASC;
--
-- Average by metric:
-- SELECT METRIC_NAME, ROUND(AVG(EVAL_AGG_SCORE), 2) AS avg_score
-- FROM EVAL_RESULTS WHERE RUN_NAME = (SELECT MAX(RUN_NAME) FROM EVAL_RESULTS)
-- GROUP BY 1;
--
-- Worst performers:
-- SELECT INPUT, METRIC_NAME, EVAL_AGG_SCORE, EXPLANATION
-- FROM EVAL_RESULTS WHERE EVAL_AGG_SCORE < 0.5
-- ORDER BY EVAL_AGG_SCORE;
