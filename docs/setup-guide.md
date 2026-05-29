# Setup Guide — Snowsight Worksheet Execution

Complete step-by-step guide to deploy the SEC Filing Intelligence pipeline by copying SQL scripts into Snowsight worksheets.

---

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role access
- Cortex AI functions enabled (AI_EXTRACT)
- Cortex Search, Semantic Views, and Cortex Agent features enabled
- External network access capability (for SEC EDGAR API)
- A verified email address for notifications (check: `DESCRIBE USER <your_username>` → `IS_EMAIL_VERIFIED = true`)

---

## Important: Session Variables

Every worksheet must start by pasting and running `sql/00_config.sql`. Session variables (`$config_*`) only persist within a single Snowsight worksheet session. If you open a new worksheet, you must paste the config again.

**Workflow for each worksheet:**
1. Open a new Snowsight worksheet
2. Paste the contents of `sql/00_config.sql` at the top
3. Run the config (select all config lines → Run)
4. Paste the next script below
5. Run the script

### Programmatic Execution (Python, CI/CD, AI agents like Cortex Code)

If executing scripts programmatically (not in Snowsight worksheets):
- Session variables (`$config_*`) and `USE DATABASE/SCHEMA` do NOT persist across separate SQL calls
- Either execute each phase file as a single multi-statement script, or prefix every statement with the USE context
- Alternatively, use fully-qualified object names (e.g., `SEC_FILINGS.FILING_DATA.FILING_INDEX`)
- File uploads to stages require `snow stage copy` (SnowCLI) or the Snowsight UI — there is no pure-SQL equivalent for uploading local files to a stage
- See the **PROGRAMMATIC DEPLOYMENT GUIDE** in `agents.md` for error patterns, file difficulty ratings, and the `EXECUTE IMMEDIATE $$` workaround

**Snowsight-only requirement:** `sql/06_agent/02_eval_framework.sql` (eval framework) has complex nested SPs that require Snowsight's statement parser. All other scripts are deployable programmatically. The eval framework is optional — the core pipeline works without it.

---

## Phase 1: Infrastructure (2 minutes)

### Preflight Check (recommended)

Before starting, run the preflight validation to catch account-specific incompatibilities early:

1. Open a Snowsight worksheet
2. Paste and run `sql/00_preflight_check.sql`
3. Review the output — all checks should show PASS or INFO
4. If any check shows FAIL, resolve before proceeding

### Worksheet 1: Config + Database + Schema

Paste and run these in order in ONE worksheet:

1. `sql/00_config.sql` — Sets all session variables (the INSERT at the bottom will silently skip on first run — this is expected)
2. `sql/01_infrastructure/01_database_and_schema.sql` — Creates database, schema, and all tables
3. `sql/01_infrastructure/02_warehouses.sql` — Creates 3 warehouses (steady-state, build, ingest)
4. `sql/01_infrastructure/03_external_access.sql` — Network rule + EAI for SEC EDGAR API
5. `sql/01_infrastructure/04_email_integration.sql` — Email notifications for task DAGs

After Phase 1 completes, **re-run `sql/00_config.sql`** to persist config values into `_PIPELINE_CONFIG` (the table now exists).

**Verify:** Run `SHOW TABLES IN SCHEMA;` — you should see FILING_INDEX, FILING_CONTENT, FILING_CHUNKS, FILING_SIGNALS, SIC_CODES, and other tables.

---

## Phase 1b: Dashboard (Optional — recommended, 1 minute)

Deploy the Streamlit monitoring dashboard early so you can track pipeline progress from the start. The dashboard has 7 tabs: Pipeline, Data Quality, Filing Explorer (RAG), Research Explorer, Cost Monitor, Pipeline Control, and Agent Eval.

**Prerequisites:** Phase 1 must be complete (PYPI_EAI is created by `03_external_access.sql` and is required for the container runtime to install packages like plotly).

### Worksheet 1b: Streamlit Dashboard

Paste and run:

1. `sql/00_config.sql`
2. `sql/05_serving/05_streamlit_deploy.sql` — Creates stage + Streamlit app object (with `EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_EAI)` for package installs)

Then upload the app files using ONE of these methods:

**Option A: SnowCLI** (recommended — works for automation and AI-assisted installs):

```bash
snow stage copy streamlit/SEC_Filing_Explorer.py @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE/ --overwrite
snow stage copy streamlit/pyproject.toml @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE/ --overwrite
snow stage copy streamlit/environment.yml @SEC_FILINGS.FILING_DATA.STREAMLIT_STAGE/ --overwrite
```

**Option B: Snowsight UI** — Navigate to Data → SEC_FILINGS → FILING_DATA → Stages → STREAMLIT_STAGE → click "+ Files" and upload all files from the `streamlit/` directory.

**Important for AI-assisted / programmatic installs:** There is no pure-SQL mechanism to upload local files to a stage. SnowCLI (`snow stage copy`) is required. The main Streamlit app file (~116KB) is too large to reliably pass as a SQL string literal.

**Verify:**

```sql
SHOW STREAMLITS IN SCHEMA;
-- Should show SEC_FILING_DASHBOARD
```

Open the app from Snowsight: Projects → Streamlit → SEC_FILING_DASHBOARD

**What works immediately (after Phase 1):**
- Pipeline Dashboard — row counts, DAG status diagrams (empty until ingestion starts)
- Pipeline Control — trigger ingestion runs, edit config, emergency stop
- Cost Monitor — warehouse credits, AI usage

**What requires later phases:**
- Filing Explorer (RAG) — needs Cortex Search service (Phase 5)
- Agent Eval Results — needs eval framework (optional)

**Dependencies:** `snowflake-snowpark-python`, `streamlit`, `plotly` (all from Snowflake Anaconda channel, no version pins needed).

**Runtime dependencies (created progressively during install):**
- Phase 1: `_PIPELINE_CONFIG` table, `PYPI_EAI`, `STREAMLIT_COMPUTE_POOL`, `FILING_WH`
- Phase 4: `TRIGGER_PROCESS_FILINGS_FULL` SP (for custom date range in Pipeline Control)
- Phase 5: `SEC_FILING_SEARCH` service (for Filing Explorer RAG tab)
- Phase 6b: Task DAGs (for Pipeline Control trigger buttons + DAG status display)

The app handles missing dependencies gracefully (try/except). Tabs that depend on later phases show informational messages until those phases complete.

---

## Phase 2: Deploy SPs + Quick Start Pipeline (~15 minutes)

### Worksheet 2: All SPs + Quick Start

Paste and run each script in order in ONE worksheet:

1. `sql/00_config.sql` — (always first)
2. `sql/02_ingestion/04_feed_archive_loader.sql` — Feed ingestion SPs
3. `sql/04_enrichment/00_sic_reference_data.sql` — SIC reference data (494 codes)
4. `sql/04_enrichment/01_ticker_enrichment.sql` — Ticker enrichment SPs
5. `sql/03_processing/01_text_cleaning_udf.sql` — CLEAN_TEXT UDF
6. `sql/03_processing/02_chunking_udf.sql` — CHUNK_FILING UDF
7. `sql/03_processing/07_signal_excerpt_view.sql` — V_SIGNAL_EXCERPT view
8. `sql/03_processing/05_processing_task_dag.sql` — Signal/metrics/guidance SPs + Processing DAG
9. `sql/03_processing/06_process_single_filing.sql` — PREPARE_FILINGS, PROCESS_FILINGS, TRIGGER_PROCESS_FILINGS_FULL, QUICK_START SPs

Then run the Quick Start (one call does everything):

```sql
CALL QUICK_START($config_quick_start_date);
```

This single call:
- Ingests 376 filings from the configured date (2025-02-21) via feed archive
- Creates a dynamic task DAG that automatically runs:
  - Enrichment (ticker + SIC/industry mapping)
  - Chunking (section-aware, 1500 chars, 200 overlap)
  - Signal extraction (section-targeted excerpts for 10-K/10-Q)
  - Key metrics extraction (revenue, EPS, net income, YoY)
  - Forward guidance extraction
  - Event type normalization (60+ raw → 12 canonical categories)
  - Industry/ticker propagation to downstream tables
  - Cortex Search service refresh
  - Serving layer update (semantic view + agent redeployment)
- Returns immediately with a DAG name for monitoring
- Dynamic tasks self-clean after completion

**Monitor progress:**
```sql
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
WHERE NAME LIKE 'PROCESS_FULL_%'
ORDER BY SCHEDULED_TIME DESC;
```

**Verify (after DAG completes, ~15 minutes):**
```sql
SELECT COUNT(*) AS index_rows FROM FILING_INDEX;     -- Should be ~376
SELECT COUNT(*) AS content_rows FROM FILING_CONTENT; -- Should be ~376

SELECT COUNT(*) AS total_signals, COUNT(REVENUE) AS has_revenue,
       COUNT(FORWARD_GUIDANCE) AS has_guidance,
       COUNT(EVENT_TYPE_NORMALIZED) AS has_normalized
FROM FILING_SIGNALS;
-- Should show: 376 signals, ~44% with revenue, guidance varies, 100% normalized

SELECT INDUSTRY_SECTOR, COUNT(*) FROM FILING_SIGNALS GROUP BY 1 ORDER BY 2 DESC;
-- Should show 8 sectors: Finance, Technology, Life Sciences, Manufacturing, etc.
```

---

## Phase 5: Serving Layer (~5 minutes)

### Worksheet 5: Search + Semantic View + Agent

Paste and run in order:

1. `sql/00_config.sql`
2. `sql/05_serving/01_cortex_search.sql` — Creates Cortex Search service (builds index)
3. `sql/05_serving/02_semantic_view.sql` — Creates semantic view for Cortex Analyst

Wait for search service to become ACTIVE:
```sql
SHOW CORTEX SEARCH SERVICES IN SCHEMA;
-- Check "serving_state" = 'ACTIVE' (usually takes 1-5 minutes for Quick Start data)
```

Then deploy the agent:

4. `sql/06_agent/01_agent_deployment.sql` — Deploys agent using dynamic SQL (no manual editing needed)
5. `sql/05_serving/04_serving_task_dag.sql` — Creates the Serving DAG (T_SERVING_ROOT) for automated semantic view + agent redeployment. Required by TRIGGER_PROCESS_FILINGS_FULL.

**Verify:**
```sql
SHOW AGENTS IN SCHEMA;
-- Should show SEC_FILING_AGENT

SHOW TASKS LIKE 'T_SERVING%' IN SCHEMA;
-- Should show T_SERVING_ROOT, T_RECREATE_SEMANTIC_VIEW, T_REDEPLOY_AGENT, T_SERVING_FINALIZER
```

---

## Phase 6: Test the Agent

```sql
-- Test search (qualitative)
SELECT SNOWFLAKE.CORTEX.AGENT_RUN(
    $config_database || '.' || $config_schema || '.' || $config_agent_name,
    'What risk factors did pharmaceutical companies disclose in recent 10-K filings?'
);

-- Test analyst (structured)
SELECT SNOWFLAKE.CORTEX.AGENT_RUN(
    $config_database || '.' || $config_schema || '.' || $config_agent_name,
    'What is the filing count by industry sector?'
);
```

---

## Done! Quick Start Complete (~15 minutes total)

You now have a working SEC Filing Intelligence agent with:
- 376 filings from Feb 21, 2025
- ~54,000 searchable text chunks
- ~376 AI-extracted investment signals
- Semantic search over filing text
- Structured analytics over filing signals
- A 2-tool Cortex Agent combining both

---

## Phase 6b: Deploy Task DAGs (required for Pipeline Control tab)

The Streamlit Pipeline Control tab triggers pipeline runs via Snowflake Task DAGs. Deploy these if you want to use automated multi-year ingestion, the emergency stop, or restart buttons.

### Worksheet 6b: Task DAGs

Paste and run each script in a **separate worksheet** (each contains BEGIN...END blocks with semicolons that require Snowsight's statement splitter). Each worksheet must have `sql/00_config.sql` pasted and run at the top:

1. **Worksheet A:** `sql/00_config.sql` + `sql/02_ingestion/05_feed_ingestion_dag.sql` — Feed ingestion (12 parallel monthly tasks, multi-year loop)
2. **Worksheet B:** `sql/00_config.sql` + `sql/04_enrichment/03_enrichment_task_dag.sql` — Ticker enrichment + industry backfill
3. **Worksheet C:** `sql/00_config.sql` + `sql/03_processing/07_signal_excerpt_view.sql` — **Deploy FIRST** (V_SIGNAL_EXCERPT view, required by signal extraction SPs)
4. **Worksheet D:** `sql/00_config.sql` + `sql/03_processing/05_processing_task_dag.sql` — Chunking, signals, metrics, normalization, search refresh

**Additional SPs (run after DAGs):**

5. **Worksheet E:** `sql/00_config.sql` + `sql/02_ingestion/06_feed_gap_filler.sql` — Gap-filler SPs (FILL_FEED_GAPS, VALIDATE_FEED_COMPLETENESS)
6. **Worksheet F:** `sql/00_config.sql` + `sql/03_processing/06_process_single_filing.sql` — Spot-processing SPs (PREPARE/PROCESS/TRIGGER_PROCESS_FILINGS, TRIGGER_PROCESS_FILINGS_FULL, REEXTRACT_SIGNALS)

**Important:** `07_signal_excerpt_view.sql` (Worksheet C) MUST be deployed before `05_processing_task_dag.sql` (Worksheet D) — the signal extraction SPs reference the `V_SIGNAL_EXCERPT` view.

**Verify:**
```sql
SHOW TASKS IN SCHEMA;
-- Should show T_FEED_INGEST_ROOT, T_ENRICH_ROOT, T_PROCESSING_ROOT and their children
-- (T_SERVING_ROOT was deployed in Phase 5)
SHOW VIEWS LIKE 'V_SIGNAL_EXCERPT';
-- Should show the view
SHOW PROCEDURES LIKE 'FILL_FEED_GAPS';
SHOW PROCEDURES LIKE 'PROCESS_FILINGS';
SHOW PROCEDURES LIKE 'TRIGGER_PROCESS_FILINGS_FULL';
SHOW PROCEDURES LIKE 'REEXTRACT_SIGNALS';
```

After deploying, the Pipeline Control tab in the Streamlit app can trigger and manage pipeline runs.

---

## Scaling to Full Year (Optional)

### Option A: Feed Ingestion Task DAG (recommended — parallel, ~2.5 hours/year)

```sql
-- In a new worksheet:
-- 1. Paste sql/00_config.sql
-- 2. Paste sql/02_ingestion/05_feed_ingestion_dag.sql
-- 3. Run (creates 12 parallel monthly tasks)
-- 4. Trigger:
EXECUTE TASK T_FEED_INGEST_ROOT;

-- Monitor progress:
SELECT status, COUNT(*) AS days, SUM(loaded) AS filings
FROM _FEED_INGEST_LOG GROUP BY 1;
```

### Option B: Manual feed range (sequential, ~8 hours)

```sql
CALL LOAD_FEED_DATE_RANGE(
    $config_ingest_start_year || '-01-01',
    $config_ingest_end_year || '-12-31',
    $config_user_agent
);
```

### After full ingestion

If you deployed the Task DAGs (Phase 6b), the pipeline **auto-chains**: Feed finalizer triggers Enrichment, which triggers Processing (including search refresh). No manual re-runs needed.

If you did NOT deploy the DAGs, manually run:
```sql
-- Re-run enrichment
CALL ENRICH_TICKERS(500, '0000000000', $config_user_agent);
-- Re-run metadata backfill (paste and run sql/04_enrichment/02_metadata_backfill.sql)
-- Re-run processing (paste and run sql/03_processing/03_chunking_pipeline.sql + 04_signal_extraction.sql)
-- Refresh search service:
ALTER CORTEX SEARCH SERVICE SEC_FILING_SEARCH REFRESH;
```

---

## Alternative: Individual Filing Download (slow — per-filing HTTP)

If you need targeted downloads (e.g., specific companies or date ranges) instead of bulk feed ingestion:

```sql
-- In a new worksheet with sql/00_config.sql:
-- 1. Paste sql/02_ingestion/02_download_filings.sql (creates DOWNLOAD_FILING_BATCH SP)
-- 2. Run targeted downloads:
CALL DOWNLOAD_FILING_BATCH(100, '10-K', '2025-01-01', '2025-12-31', $config_user_agent);
CALL DOWNLOAD_FILING_BATCH(100, '8-K', '2025-01-01', '2025-12-31', $config_user_agent);
-- Repeat with larger batch sizes until "No pending..." returned
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Object does not exist` | You forgot to paste `sql/00_config.sql` at the top of this worksheet |
| `HTTP 403` from SEC EDGAR | Check `config_user_agent` includes your company name + email |
| Search service stuck in BUILDING | Wait (large indexes take hours); check with `SHOW CORTEX SEARCH SERVICES` |
| Agent returns empty results | Verify search service shows `serving_state = 'ACTIVE'` |
| Signal extraction slow | Use FILING_BUILD_WH; for production use the Processing Task DAG |
| `No target filings` from feed SP | Likely a weekend/holiday — SEC doesn't publish feeds on non-business days |
| Session variable not found | You opened a new worksheet without pasting config — paste and re-run `00_config.sql` |
| Streamlit "Packages not found" | Remove all version pins from environment.yml; use only package names without `==X.Y` |
| Streamlit takes long to load | First load builds the environment (~30s); subsequent loads are fast |
| Streamlit "invalid identifier" | Column names in ACCOUNT_USAGE views differ from regular tables — check actual schema |
| `SNOWFLAKE.CORTEX.AGENT_RUN()` not found | Use the JSON invocation: `SELECT SNOWFLAKE.CORTEX.AGENT_RUN('<agent_name>', {'query': '...'})` as a fallback. Check that Cortex Agent is enabled on your account. |
