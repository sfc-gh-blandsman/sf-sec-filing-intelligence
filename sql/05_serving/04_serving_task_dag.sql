-- =============================================================================
-- 04: Serving Layer Schema-Change DAG (Manual Trigger Only)
-- =============================================================================
-- Manual-trigger DAG for recreating the semantic view and redeploying the agent.
-- Run ONLY when schema definitions change — NOT needed for routine data refreshes.
--
-- Cortex Search uses incremental refresh (handled by the Processing DAG).
-- The Semantic View queries FILING_SIGNALS + FILING_INDEX live at runtime —
-- new data appears automatically without recreation.
-- The Agent references both services by name and doesn't need redeployment
-- unless its YAML configuration changes.
--
-- WHEN TO RUN THIS DAG:
--   - Semantic view definition changes (new columns, metrics, VQRs, synonyms)
--   - Agent YAML changes (new tools, updated instructions, model change)
--   - After changing FILING_SIGNALS or FILING_INDEX schema (new columns)
--
-- NOT NEEDED FOR:
--   - New data loaded into existing tables
--   - Routine pipeline runs (processing DAG handles search refresh)
--   - Changes to FILING_CHUNKS (search service handles via incremental refresh)
--
-- Architecture:
--   T_SERVING_ROOT (manual trigger — CRON never-fire)
--   ├── T_RECREATE_SEMANTIC_VIEW (recreates semantic view DDL)
--   ├── T_REDEPLOY_AGENT (AFTER SEMANTIC_VIEW — redeploys agent with current config)
--   └── T_SERVING_FINALIZER (emails summary)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

-- =============================================================================
-- Root Task (triggered by processing finalizer or manual EXECUTE TASK)
-- =============================================================================

CREATE OR REPLACE TASK T_SERVING_ROOT
    WAREHOUSE = IDENTIFIER($config_warehouse)
    SCHEDULE = 'USING CRON 0 0 29 2 * UTC'
    TASK_AUTO_RETRY_ATTEMPTS = 2
    COMMENT = 'Root: schema-change DAG (semantic view + agent). Manual trigger only.'
AS
    SELECT 'SERVING_SCHEMA_CHANGE_STARTED' AS status;


-- =============================================================================
-- Step 1: Recreate Semantic View
-- =============================================================================

CREATE OR REPLACE TASK T_RECREATE_SEMANTIC_VIEW
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Recreates semantic view for Cortex Analyst (run only for schema changes)'
    AFTER T_SERVING_ROOT
AS
BEGIN
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE SEMANTIC VIEW ' || _CFG('semantic_view') || '
      TABLES (
        signals AS FILING_SIGNALS
          PRIMARY KEY (SIGNAL_ID)
          WITH SYNONYMS = (''investment signals'', ''filing signals'', ''EDGAR signals'', ''SEC filings'')
          COMMENT = ''AI-extracted investment signals from SEC EDGAR filings.'',
        meta AS FILING_INDEX
          PRIMARY KEY (ACCESSION_NO)
          WITH SYNONYMS = (''filing metadata'', ''EDGAR index'')
          COMMENT = ''SEC EDGAR filing metadata''
      )
      RELATIONSHIPS (
        signals_to_meta AS signals(ACCESSION_NO) REFERENCES meta(ACCESSION_NO)
      )
      FACTS (
        signals.accession_no AS signals.ACCESSION_NO
          WITH SYNONYMS = (''accession number'', ''filing id'')
          COMMENT = ''EDGAR accession number'',
        signals.revenue AS signals.REVENUE_NORMALIZED WITH SYNONYMS = (''total revenue'', ''net revenue'', ''sales'', ''revenue'') COMMENT = ''Revenue in millions USD (FLOAT). NULL if not extractable.'',
        signals.net_income AS signals.NET_INCOME WITH SYNONYMS = (''net income'', ''net loss'', ''profit'') COMMENT = ''Net income/loss figure (text).'',
        signals.eps AS signals.EPS_NORMALIZED WITH SYNONYMS = (''earnings per share'', ''diluted EPS'', ''EPS'') COMMENT = ''Normalized single EPS value. NULL for multi-class structures.'',
        signals.yoy_change AS signals.YOY_CHANGE WITH SYNONYMS = (''year over year'', ''YoY growth'') COMMENT = ''Year-over-year change (text).'',
        signals.guidance_normalized AS signals.GUIDANCE_NORMALIZED WITH SYNONYMS = (''guidance'', ''outlook'', ''forecast'') COMMENT = ''Forward guidance from MD&A. NULL if not stated.''
      )
      DIMENSIONS (
        signals.company_name AS signals.COMPANY_NAME WITH SYNONYMS = (''company'', ''filer'', ''issuer'') COMMENT = ''Company name'',
        signals.ticker AS signals.TICKER WITH SYNONYMS = (''stock ticker'', ''symbol'') COMMENT = ''Stock ticker symbol'',
        signals.form_type AS signals.FORM_TYPE WITH SYNONYMS = (''filing type'', ''SEC form'') COMMENT = ''SEC filing type: 10-K, 10-Q, 8-K'',
        signals.event_type AS signals.EVENT_TYPE WITH SYNONYMS = (''event'', ''signal type'') COMMENT = ''AI-classified event type'',
        signals.sentiment AS signals.SENTIMENT WITH SYNONYMS = (''tone'', ''filing tone'') COMMENT = ''AI-assessed sentiment: POSITIVE, NEGATIVE, NEUTRAL, MIXED'',
        signals.industry_sector AS signals.INDUSTRY_SECTOR WITH SYNONYMS = (''sector'', ''industry'', ''Healthcare'', ''healthcare'') COMMENT = ''SEC Office-based industry sector. Values: Technology, Life Sciences, Finance, Real Estate & Construction, Energy & Transportation, Manufacturing, Trade & Services, Crypto Assets, Other.'',
        signals.industry_title AS signals.INDUSTRY_TITLE WITH SYNONYMS = (''specific industry'', ''SIC description'', ''sub-sector'') COMMENT = ''Specific SEC industry title for drill-down'',
        signals.is_amendment AS signals.IS_AMENDMENT WITH SYNONYMS = (''amendment'', ''amended filing'') COMMENT = ''TRUE if amended filing'',
        meta.cik AS meta.CIK WITH SYNONYMS = (''SEC CIK'', ''central index key'') COMMENT = ''SEC Central Index Key'',
        signals.signal_date AS signals.SIGNAL_DATE WITH SYNONYMS = (''filing date'', ''date filed'', ''when filed'') COMMENT = ''Date the SEC received the filing'',
        signals.period_of_report AS signals.PERIOD_OF_REPORT WITH SYNONYMS = (''fiscal period'', ''report period'', ''period end'') COMMENT = ''Fiscal period end date the filing covers''
      )
      METRICS (
        signals.filing_count AS COUNT(signals.SIGNAL_ID) WITH SYNONYMS = (''number of filings'', ''total filings'') COMMENT = ''Total filing count'',
        signals.positive_signals AS COUNT(CASE WHEN signals.SENTIMENT = ''POSITIVE'' THEN 1 END) WITH SYNONYMS = (''positive filings'') COMMENT = ''Positive sentiment count'',
        signals.negative_signals AS COUNT(CASE WHEN signals.SENTIMENT = ''NEGATIVE'' THEN 1 END) WITH SYNONYMS = (''negative filings'') COMMENT = ''Negative sentiment count'',
        signals.neutral_signals AS COUNT(CASE WHEN signals.SENTIMENT = ''NEUTRAL'' THEN 1 END) WITH SYNONYMS = (''neutral filings'') COMMENT = ''Neutral sentiment count'',
        signals.earnings_count AS COUNT(CASE WHEN signals.EVENT_TYPE = ''Earnings'' THEN 1 END) WITH SYNONYMS = (''earnings events'') COMMENT = ''Earnings event count'',
        signals.ma_count AS COUNT(CASE WHEN signals.EVENT_TYPE = ''M&A'' THEN 1 END) WITH SYNONYMS = (''merger filings'', ''M&A events'') COMMENT = ''M&A event count'',
        signals.risk_disclosure_count AS COUNT(CASE WHEN signals.EVENT_TYPE = ''Risk Disclosure'' THEN 1 END) WITH SYNONYMS = (''risk disclosures'') COMMENT = ''Risk disclosure count'',
        signals.leadership_change_count AS COUNT(CASE WHEN signals.EVENT_TYPE = ''Leadership Change'' THEN 1 END) WITH SYNONYMS = (''leadership events'') COMMENT = ''Leadership change count'',
        signals.guidance_count AS COUNT(CASE WHEN signals.EVENT_TYPE = ''Guidance Update'' THEN 1 END) WITH SYNONYMS = (''guidance updates'') COMMENT = ''Guidance update count'',
        signals.amendment_count AS COUNT(CASE WHEN signals.IS_AMENDMENT = TRUE THEN 1 END) WITH SYNONYMS = (''amended filings'') COMMENT = ''Amendment count'',
        signals.negative_sentiment_pct AS ROUND(100.0 * COUNT(CASE WHEN signals.SENTIMENT = ''NEGATIVE'' THEN 1 END) / NULLIF(COUNT(signals.SIGNAL_ID), 0), 2) WITH SYNONYMS = (''negative rate'', ''percent negative'') COMMENT = ''Percentage negative sentiment''
      )
      COMMENT = ''Investment signal analytics over SEC EDGAR filing corpus.''
      AI_SQL_GENERATION ''Use SIGNAL_DATE for date filters unless user asks about fiscal period. PERIOD_OF_REPORT is fiscal end date. Account for NULLs in aggregations.''
    ';

    RETURN 'Semantic view recreated: ' || _CFG('semantic_view');
END;


-- =============================================================================
-- Step 4: Redeploy Agent (dynamic SQL injects config into YAML)
-- =============================================================================

CREATE OR REPLACE TASK T_REDEPLOY_AGENT
    WAREHOUSE = IDENTIFIER($config_warehouse)
    USER_TASK_TIMEOUT_MS = 172800000
    COMMENT = 'Redeploys Cortex Agent with current config values'
    AFTER T_RECREATE_SEMANTIC_VIEW
AS
BEGIN
    LET search_fqn VARCHAR := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('search_service');
    LET sv_fqn VARCHAR := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('semantic_view');
    LET wh_name VARCHAR := _CFG('warehouse');
    LET agent_name VARCHAR := _CFG('agent_name');

    -- Use the same dynamic SQL pattern as 01_agent_deployment.sql
    -- (abbreviated spec for task context — full spec in 06_agent/01_agent_deployment.sql)
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE AGENT ' || :agent_name || '
      COMMENT = ''SEC EDGAR filing research agent — search + analyst.''
      FROM SPECIFICATION
    $$
    models:
      orchestration: claude-opus-4-7
    orchestration:
      budget:
        seconds: 1800
        tokens: 400000
    instructions:
      orchestration: |
        You are SEC Filing Analyst. Use filing_semantic_search for text/quotes, filing_analyst for counts/trends.
        Filter columns: filed_at (YYYY-MM-DD), form_type, company_name, ticker, section_name, period_of_report, industry_sector, industry_title
        Industry sectors: Technology, Life Sciences, Finance, Real Estate & Construction, Energy & Transportation, Manufacturing, Trade & Services, Crypto Assets, Other
        "Healthcare" = "Life Sciences". "Financial Services" = "Finance".
      response: |
        Cite every claim: [Company] [Form] filed [date]. Use markdown tables for aggregates.
    tools:
      - tool_spec:
          type: cortex_search
          name: filing_semantic_search
          description: Semantic search over SEC filing text. Filter by filed_at, form_type, company_name, ticker, section_name, period_of_report, industry_sector, industry_title.
      - tool_spec:
          type: cortex_analyst_text_to_sql
          name: filing_analyst
          description: |
            Structured analytics over filing signals.
            Dimensions: company_name, ticker, form_type, event_type, sentiment, industry_sector, industry_title, is_amendment, signal_date, period_of_report.
            Metrics: filing_count, positive_signals, negative_signals, earnings_count, ma_count, risk_disclosure_count, leadership_change_count, guidance_count, amendment_count, negative_sentiment_pct.
            Facts: revenue, net_income, eps_normalized, yoy_change, guidance_normalized.
    tool_resources:
      filing_semantic_search:
        search_service: ' || :search_fqn || '
        max_results: 20
        id_column: CHUNK_ID
        title_column: COMPANY_NAME
      filing_analyst:
        semantic_view: ' || :sv_fqn || '
        execution_environment:
          type: warehouse
          warehouse: ' || :wh_name || '
    $$';

    RETURN 'Agent redeployed: ' || :agent_name;
END;


-- =============================================================================
-- Finalizer (emails summary → triggers eval DAG)
-- =============================================================================

CREATE OR REPLACE TASK T_SERVING_FINALIZER
    WAREHOUSE = IDENTIFIER($config_warehouse)
    FINALIZE = T_SERVING_ROOT
    COMMENT = 'Finalizer: emails serving layer summary with task status'
AS
BEGIN
    LET chunk_count INT;
    LET signal_count INT;
    LET succeeded INT := 0;
    LET failed INT := 0;
    LET failed_names VARCHAR := '';

    SELECT COUNT(*) INTO :chunk_count FROM FILING_CHUNKS;
    SELECT COUNT(*) INTO :signal_count FROM FILING_SIGNALS;

    -- Task status from this DAG run
    BEGIN
        SELECT COUNT(CASE WHEN STATE = 'SUCCEEDED' THEN 1 END),
               COUNT(CASE WHEN STATE = 'FAILED' THEN 1 END),
               COALESCE(LISTAGG(CASE WHEN STATE = 'FAILED' THEN NAME END, ', '), '')
        INTO :succeeded, :failed, :failed_names
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 20
        ))
        WHERE (NAME LIKE 'T_REFRESH_%' OR NAME LIKE 'T_RECREATE_%' OR NAME LIKE 'T_WAIT_%' OR NAME LIKE 'T_REDEPLOY_%')
          AND SCHEDULED_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
    EXCEPTION WHEN OTHER THEN NULL; END;

    LET status_line VARCHAR := :succeeded::VARCHAR || ' succeeded, ' || :failed::VARCHAR || ' failed';
    IF (:failed > 0) THEN
        status_line := :status_line || ' [' || :failed_names || ']';
    END IF;

    LET msg VARCHAR := 'SERVING LAYER DAG COMPLETE' || CHR(10) || CHR(10) ||
        'Status: ' || :status_line || CHR(10) ||
        'Cortex Search: refreshed incrementally (' || :chunk_count::VARCHAR || ' total chunks)' || CHR(10) ||
        'Semantic View: recreated (' || :signal_count::VARCHAR || ' signals)' || CHR(10) ||
        'Agent: redeployed' || CHR(10) || CHR(10) ||
        'NEXT STEP: Run agent evaluation manually:' || CHR(10) ||
        '  1. Execute: sql/06_agent/02_eval_framework.sql (MANUAL EVAL section)' || CHR(10) ||
        '     CALL _RUN_EVAL();' || CHR(10) ||
        '  2. Poll: CALL EXECUTE_AI_EVALUATION(''STATUS'', ...) until COMPLETED' || CHR(10) ||
        '  3. Materialize: EXECUTE TASK EVAL_DAG_ROOT;' || CHR(10) ||
        'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR;

    CALL SYSTEM$SEND_EMAIL(
        _CFG('email_integration'),
        _CFG('email_recipient'),
        'SEC Filing Serving: ' || IFF(:failed > 0, 'PARTIAL (' || :failed::VARCHAR || ' failed)', 'COMPLETE') || ' — Run Eval Next',
        :msg
    );
END;


-- =============================================================================
-- Resume all tasks
-- =============================================================================

ALTER TASK T_RECREATE_SEMANTIC_VIEW RESUME;
ALTER TASK T_REDEPLOY_AGENT RESUME;
ALTER TASK T_SERVING_FINALIZER RESUME;
ALTER TASK T_SERVING_ROOT RESUME;


-- =============================================================================
-- EXECUTION + MONITORING
-- =============================================================================
-- Manual trigger (schema changes only):
--   EXECUTE TASK T_SERVING_ROOT;
--
-- Monitor:
--   SHOW AGENTS IN SCHEMA;
--   DESCRIBE SEMANTIC VIEW SEC_FILING_ANALYTICS;
