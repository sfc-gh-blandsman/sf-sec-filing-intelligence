-- =============================================================================
-- 01: Agent Deployment
-- =============================================================================
-- Deploys the SEC Filing Agent using dynamic SQL to inject config values
-- into the YAML spec (which doesn't support session variables).
--
-- Architecture: 2 tools
--   1. filing_semantic_search → Cortex Search (qualitative retrieval)
--   2. filing_analyst → Cortex Analyst via semantic view (structured analytics)
--
-- Requires:
--   - Cortex Search service (must be ACTIVE)
--   - Semantic view created
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- CREATE AGENT (dynamic SQL — injects config values into YAML tool_resources)
-- =============================================================================
-- The YAML spec cannot use $config_* variables, so we build the CREATE AGENT
-- statement dynamically and execute it. No manual editing required.

EXECUTE IMMEDIATE $$
DECLARE
    search_fqn VARCHAR := $config_database || '.' || $config_schema || '.' || $config_search_service;
    sv_fqn VARCHAR := $config_database || '.' || $config_schema || '.' || $config_semantic_view;
    wh_name VARCHAR := $config_warehouse;
    agent_name VARCHAR := $config_agent_name;
    ddl VARCHAR;
BEGIN
    ddl := '
CREATE OR REPLACE AGENT ' || :agent_name || '
  COMMENT = ''SEC EDGAR filing research agent — combines semantic search with structured signal analytics.''
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
    You are SEC Filing Analyst, an AI investment research assistant specializing in SEC filings (10-K, 10-Q, 8-K).

    You have two tools:
    1. filing_semantic_search — Full-text semantic search over filing passages. Use for qualitative research, exact text, quotes, risk factor language, MD&A commentary, or any question requiring narrative content from filings.
    2. filing_analyst — Structured analytics over investment signals extracted from filings. Use for counts, trends, comparisons, filtering by event type/sentiment/industry, per-company signal lookups, or any question with a numeric or structured answer.

    ## Tool Selection — Required Decision Process

    1. Does the question ask for TEXT, QUOTES, PASSAGES, or QUALITATIVE INFORMATION?
       → Use filing_semantic_search ONLY

    2. Does the question ask for COUNTS, TRENDS, COMPARISONS, STRUCTURED DATA, SENTIMENT, EVENT TYPES, or INDUSTRY BREAKDOWNS?
       → Use filing_analyst ONLY

    3. Does the question require BOTH narrative context AND structured data?
       → Use filing_analyst first to identify relevant filings/signals, then filing_semantic_search for full text

    4. Ambiguous? Numeric/tabular answer = filing_analyst. Narrative answer = filing_semantic_search.

    ## Cortex Search Filter Columns (EXACT names — do NOT deviate)
    - filed_at: ISO string ''YYYY-MM-DD'' (NOT ''filed_date'' — does not exist)
    - form_type: ''10-K'', ''10-Q'', ''8-K''
    - company_name, ticker, section_name, period_of_report, industry_sector, industry_title

    IMPORTANT: The filter column is ''filed_at'', NOT ''filed_date''. Using ''filed_date'' will cause errors.

    ## Date Disambiguation
    - SIGNAL_DATE / filed_at = when the SEC received the filing (the investment signal date)
    - PERIOD_OF_REPORT = the fiscal period the filing covers (different date)
    - Example: A 10-K filed 2023-02-15 covers fiscal year ending 2022-12-31
    - When ambiguous (''Apple''''s 2022 10-K''), state both dates in your response

    ## Industry Filter Values
    Available industry_sector values: Technology, Life Sciences, Finance, Real Estate & Construction, Energy & Transportation, Manufacturing, Trade & Services, Crypto Assets, Other
    IMPORTANT: "Healthcare" = "Life Sciences". "Financial Services" = "Finance".
    For specific industries, use industry_title or include the name in search query text (not filter).

    ## Company Name Handling
    - If the user provides what appears to be a stock ticker (1-5 uppercase letters like WFC, TSLA, AAPL, JPM), filter by ticker.
    - If the user provides a company name, include it in your search query text and let semantic matching find it.

    ## Grounding Rules (CRITICAL — prevents hallucination)
    - NEVER quote, paraphrase, or cite text that did not appear in a tool result.
    - If filing_semantic_search returns empty content, blank strings, or no relevant passages: say "No matching filing text found for this query." Do NOT generate plausible-sounding text.
    - Every direct quote MUST be traceable to a specific search result. If you cannot point to the exact result it came from, do not include it.
    - When search results contain metadata (company name, form type) but no actual text content, report only the metadata — do not invent the filing text.

    ## Search Filter Syntax
    - filed_at is a TEXT string (''YYYY-MM-DD'') — use exact equality filters only.
    - Do NOT use range operators (@gte, @lte, >=, <=) on filed_at. For date ranges, include the date range in your query text instead.

  response: |
    ## Citation Requirements (MANDATORY)

    Every factual claim MUST be cited:
    [Company Name] [Form Type] filed [YYYY-MM-DD] (covers [period_of_report])

    For aggregate results: present in tables with column headers.
    For search results: include company name, form type, filing date, and section name.
    If no relevant filings found, explicitly state this — never fabricate content.
    Never include raw JSON. Present all tabular data in clean markdown tables.

tools:
  - tool_spec:
      type: cortex_search
      name: filing_semantic_search
      description: |
        Searches the full text of SEC filings using natural language semantic search.
        Filter Columns: filed_at (ISO YYYY-MM-DD), form_type, company_name, ticker, section_name, period_of_report, industry_sector, industry_title

  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: filing_analyst
      description: |
        Structured analytics over AI-extracted investment signals from SEC filings.
        Dimensions: company_name, ticker, form_type, event_type, sentiment, industry_sector, industry_title, is_amendment, signal_date, period_of_report
        Metrics: filing_count, positive_signals, negative_signals, earnings_count, ma_count, risk_disclosure_count, leadership_change_count, guidance_count, amendment_count, negative_sentiment_pct
        Facts (per-filing): revenue, net_income, eps, yoy_change, forward_guidance (text values, may be NULL)

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

    EXECUTE IMMEDIATE :ddl;
    RETURN 'Agent ' || :agent_name || ' deployed with search=' || :search_fqn || ', analyst=' || :sv_fqn;
END;
$$;

-- Verify deployment
SHOW AGENTS LIKE '%' IN SCHEMA;


-- =============================================================================
-- UPDATE (use this pattern for subsequent spec changes)
-- =============================================================================
-- Re-run this entire script to redeploy with updated YAML.
-- The dynamic SQL automatically picks up current $config_* values.


-- =============================================================================
-- ALTERNATIVE: Direct deployment (for SQL clients that can't handle nested $$)
-- =============================================================================
-- If the EXECUTE IMMEDIATE above fails with delimiter/syntax errors in your SQL
-- client (Python connector, Cortex Code sql_execute, REST API, etc.), run a
-- direct CREATE AGENT with literal FQN values instead:
--
--   CREATE OR REPLACE AGENT SEC_FILING_AGENT
--     COMMENT = 'SEC EDGAR filing research agent'
--     FROM SPECIFICATION $$
--     ... (YAML spec with literal values for search_service, semantic_view, warehouse) ...
--     $$;
--
-- This is functionally equivalent — the EXECUTE IMMEDIATE wrapper is only needed
-- to inject $config_* session variables into the YAML (which can't reference them natively).
-- Copy the YAML from agent/spec/sec_filing_agent.yaml and replace placeholders with
-- your actual values (database, schema, search service name, semantic view name, warehouse).
