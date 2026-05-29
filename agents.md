# agents.md — SEC Filing Intelligence

**Read this file at the start of every session before doing anything else.**

---

## PROJECT

A **SEC EDGAR filing intelligence pipeline** on Snowflake. The system ingests public SEC filings (10-K, 10-Q, 8-K), processes them through an AI pipeline (chunking, signal extraction, metrics, guidance), and serves them via:

1. **Cortex Search** — semantic retrieval over filing text chunks
2. **Cortex Analyst** — aggregate analytics via a semantic view over filing signals
3. **Cortex Agent** — investment research Q&A combining both tools
4. **Streamlit Dashboard** — 7-tab monitoring, control, and research app

The pipeline is fully parameterized via `sql/00_config.sql` — zero hardcoded database names, schema names, emails, or account identifiers anywhere in the codebase.

---

## SNOWFLAKE CONFIGURATION

All account-specific values live in `sql/00_config.sql` (gitignored). Copy from `sql/00_config.sql.example` and fill in your values. Key settings:

- Database and schema names
- 3 warehouses (steady-state, build, ingest)
- External access integration name
- Email integration + recipient
- SEC EDGAR user-agent string
- Service names (search, semantic view, agent)

---

## KEY OBJECTS (once deployed)

| Object | Type | Purpose |
|---|---|---|
| `FILING_INDEX` | Table | EDGAR filing metadata (accession numbers, CIKs, dates, URLs, tickers, industry) |
| `FILING_CONTENT` | Table | Raw filing text content |
| `FILING_CHUNKS` | Table | Section-aware text chunks (1500 chars, 200 overlap) |
| `FILING_SIGNALS` | Table | AI-extracted structured investment signals |
| `SEC_FILING_SEARCH` | Cortex Search | Semantic search over filing chunks (Arctic M-v1.5, incremental refresh) |
| `SEC_FILING_ANALYTICS` | Semantic View | Aggregate analytics for Cortex Analyst (live query, no materialization) |
| `SEC_FILING_AGENT` | Cortex Agent | 2-tool agent (search + analyst), claude-opus-4-7 orchestrator |
| `SEC_FILING_DASHBOARD` | Streamlit | 7-tab monitoring dashboard |
| `V_SIGNAL_EXCERPT` | View | Section-targeted excerpt builder for AI_EXTRACT (shared by DAG + spot-processing) |
| `EXPLORER_RESULTS` | Table | Batch research explorer results |
| `_FEED_INGEST_LOG` | Table | Feed ingestion progress tracking (statuses: DONE, PARTIAL, INCOMPLETE, SKIPPED_403, SKIPPED_404) |
| `_PIPELINE_CONFIG` | Table | Runtime configuration for task DAGs |
| `EVAL_RESULTS` | Table | Materialized agent evaluation results |
| `FILL_FEED_GAPS` | SP | Tier-2 gap filler: downloads missing filings individually from EDGAR daily index |
| `VALIDATE_FEED_COMPLETENESS` | SP | Audit feed completeness across year ranges (compares vs EDGAR daily index) |
| `PREPARE_FILINGS` | SP (Python) | Downloads content + enriches ticker + fixes industry for array of accessions |
| `PROCESS_FILINGS` | SP (SQL) | Chunks + signal-extracts + search refresh for array of accessions |
| `TRIGGER_PROCESS_FILINGS` | SP (SQL) | Async wrapper: creates dynamic task, emails on completion |
| `REEXTRACT_SIGNALS` | SP (SQL) | Force re-extract signals using section-targeted method |

---

## FOLDER LAYOUT

```
sf-sec-filing-intelligence/
├── agents.md                          ← THIS FILE (read at session start)
├── README.md                          ← Project overview + quick start
├── .gitignore                         ← Excludes sql/00_config.sql, project-diary.md
├── sql/
│   ├── 00_config.sql.example         ← Template config (committed)
│   ├── 00_config.sql                 ← YOUR values (gitignored, never commit)
│   ├── 00_preflight_check.sql        ← Account validation before install
│   ├── 01_infrastructure/
│   │   ├── 01_database_and_schema.sql ← Database, schema, all tables
│   │   ├── 02_warehouses.sql          ← 3 warehouses (steady-state, build, ingest)
│   │   ├── 03_external_access.sql     ← Network rule + EAI for SEC EDGAR
│   │   └── 04_email_integration.sql   ← Email notifications for task DAGs
│   ├── 02_ingestion/
│   │   ├── 01_load_metadata.sql       ← LOAD_EDGAR_METADATA SP (legacy, master.gz)
│   │   ├── 02_download_filings.sql    ← DOWNLOAD_FILING_BATCH SP (legacy, per-filing)
│   │   ├── 04_feed_archive_loader.sql ← LOAD_FEED_ARCHIVE + LOAD_FEED_DATE_RANGE SPs (primary)
│   │   ├── 05_feed_ingestion_dag.sql  ← Feed DAG (12 parallel months, multi-year loop)
│   │   └── 06_feed_gap_filler.sql     ← FILL_FEED_GAPS + VALIDATE_FEED_COMPLETENESS (Tier-2 gap filling)
│   ├── 03_processing/
│   │   ├── 01_text_cleaning_udf.sql   ← CLEAN_TEXT UDF
│   │   ├── 02_chunking_udf.sql        ← CHUNK_FILING UDF (section-aware, 1500/200)
│   │   ├── 03_chunking_pipeline.sql   ← Reference: manual bulk chunking
│   │   ├── 04_signal_extraction.sql   ← Reference: manual bulk extraction
│   │   ├── 05_processing_task_dag.sql ← Processing DAG (14 tasks, production)
│   │   ├── 06_process_single_filing.sql ← PREPARE/PROCESS/TRIGGER_PROCESS_FILINGS + REEXTRACT_SIGNALS
│   │   └── 07_signal_excerpt_view.sql ← V_SIGNAL_EXCERPT (shared section-targeted excerpt logic)
│   ├── 04_enrichment/
│   │   ├── 00_sic_reference_data.sql  ← SIC_CODES reference table
│   │   ├── 01_ticker_enrichment.sql   ← ENRICH_TICKERS SP (SEC API → ticker)
│   │   ├── 02_metadata_backfill.sql   ← SIC→industry + period of report
│   │   ├── 03_enrichment_task_dag.sql ← Enrichment DAG
│   │   └── 04_event_type_normalization.sql ← 97 types → 12 canonical categories
│   ├── 05_serving/
│   │   ├── 01_cortex_search.sql       ← CREATE CORTEX SEARCH SERVICE (initial setup)
│   │   ├── 02_semantic_view.sql       ← CREATE SEMANTIC VIEW (COALESCE normalized event types)
│   │   ├── 03_latency_benchmark.sql   ← SEARCH_LATENCY_BENCHMARK SP
│   │   ├── 04_serving_task_dag.sql    ← Serving DAG (manual trigger, schema changes only)
│   │   └── 05_streamlit_deploy.sql    ← Streamlit stage + app creation
│   ├── 06_agent/
│   │   ├── 01_agent_deployment.sql    ← CREATE AGENT (dynamic SQL, injects config)
│   │   └── 02_eval_framework.sql      ← Eval dataset + 4-task DAG
│   ├── 07_explorer/
│   │   ├── 01_batch_sp.sql            ← EXPLORER_SECTOR_ANALYSIS SP + EXPLORER_RESULTS table
│   │   ├── 02_task_schedule.sql       ← Nightly batch task
│   │   ├── 03_sample_queries.sql      ← Interactive + batch examples
│   │   └── 04_custom_analysis.sql     ← EXPLORER_CUSTOM_ANALYSIS SP (Research Explorer backend)
│   └── 99_teardown/
│       ├── 01_teardown.sql            ← Full project teardown (drops everything)
│       └── 02_drop_legacy_utilities.sql ← Drops dev/debug objects
├── agent/
│   ├── spec/
│   │   └── sec_filing_agent.yaml      ← Agent YAML spec (reference)
│   └── eval/
│       ├── eval_config.yaml           ← Metric definitions
│       └── sample_questions.sql       ← 20 generic eval questions
├── streamlit/
│   ├── SEC_Filing_Explorer.py         ← 7-tab SiS dashboard
│   └── environment.yml               ← Dependencies (snowpark, streamlit, plotly)
└── docs/
    ├── architecture.md                ← System design + data flow
    ├── setup-guide.md                 ← Step-by-step installation
    ├── lessons-learned.md             ← 10 architectural insights
    └── diagrams/                      ← draw.io diagrams (LucidChart-importable)
```

---

## HOW TO RUN

Configure your connection in `sql/00_config.sql`, then execute scripts in phase order via Snowsight worksheets. See `docs/setup-guide.md` for full instructions.

### Phase 1: Infrastructure
```
sql/00_config.sql                          ← Run first in EVERY worksheet
sql/01_infrastructure/01_database_and_schema.sql
sql/01_infrastructure/02_warehouses.sql
sql/01_infrastructure/03_external_access.sql
sql/01_infrastructure/04_email_integration.sql
```

### Phase 1b: Dashboard (deploy early for monitoring)
```
sql/05_serving/05_streamlit_deploy.sql     ← Creates stage + app
PUT streamlit/SEC_Filing_Explorer.py @STREAMLIT_STAGE
PUT streamlit/environment.yml @STREAMLIT_STAGE
```

### Phase 2: Ingestion (Feed Method — primary)
```
sql/02_ingestion/04_feed_archive_loader.sql ← Creates SPs
sql/02_ingestion/05_feed_ingestion_dag.sql  ← Creates DAG tasks
EXECUTE TASK T_FEED_INGEST_ROOT;            ← Triggers multi-year ingestion
```

### Phase 3: Enrichment
```
sql/04_enrichment/00_sic_reference_data.sql
sql/04_enrichment/01_ticker_enrichment.sql
sql/04_enrichment/02_metadata_backfill.sql
sql/04_enrichment/03_enrichment_task_dag.sql
```

### Phase 4: Processing
```
sql/03_processing/01_text_cleaning_udf.sql
sql/03_processing/02_chunking_udf.sql
sql/03_processing/05_processing_task_dag.sql
EXECUTE TASK T_PROCESSING_ROOT;
```

### Phase 5: Serving + Agent
```
sql/05_serving/01_cortex_search.sql         ← Creates search service ONCE
sql/05_serving/02_semantic_view.sql         ← Creates semantic view
sql/06_agent/01_agent_deployment.sql        ← Deploys agent
```

> **Routine pipeline:** Feed → Enrich → Processing (includes search refresh) → Email
>
> **Schema changes only:** `EXECUTE TASK T_SERVING_ROOT` to recreate semantic view + agent.

---

## KEY RULES

1. **Code-in-project-first.** All SQL must exist in the project directory before execution. No ad-hoc DDL/DML without first writing it to a project file.

2. **Never guess column names.** Run `DESCRIBE TABLE` before writing SQL against any table.

3. **The date filter column in Cortex Search is `filed_at`, NOT `filed_date`.** Using `filed_date` causes errors.

4. **`SIGNAL_DATE` != `PERIOD_OF_REPORT`.** SIGNAL_DATE is when filed with SEC; PERIOD_OF_REPORT is the fiscal period covered.

5. **Rate limit SEC.gov requests** — maximum 10 req/sec per EDGAR fair use policy.

6. **All SQL is idempotent** — uses `CREATE OR REPLACE` and `INSERT ... WHERE NOT EXISTS` patterns.

7. **Never commit without explicit request.** Do not `git add`, `git commit`, or stage files unless the user explicitly asks.

8. **Use `$config_*` variables everywhere.** Never hardcode database names, schema names, warehouse names, email addresses, or account references in SQL files.

9. **Use the build warehouse only for bulk operations.** The 4XL warehouse is for chunking/extraction. Use the Large warehouse for everything else.

10. **Suspend Cortex Search when not in use.** Prevents 24/7 credit burn. Resume serving before demos.

11. **Write to the project diary.** After every session that makes changes, append an entry to `project-diary.md` (local only, gitignored).

---

## PROGRAMMATIC DEPLOYMENT GUIDE

For AI agents (Cortex Code, Claude Code, Cursor) or SQL connectors (Python connector, REST API) deploying this project. The core challenge: SQL connectors split multi-statement SQL on semicolons, which breaks `CREATE TASK` and `CREATE PROCEDURE` with `BEGIN...END` scripting blocks.

### Error Patterns and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `unexpected '<EOF>'` | Connector split `BEGIN...END` on internal `;` | Wrap in `EXECUTE IMMEDIATE $$ ... $$` |
| `statement count N did not match desired 1` | Same — multiple semicolons detected | Use single-statement execution or `EXECUTE IMMEDIATE $$` |
| `Session variable '$config_X' does not exist` | Session variables don't persist across calls | Replace `$config_*` with literal values before execution |
| `Property 'handler' must be specified` | Python SP missing explicit HANDLER | Add `HANDLER = 'function_name'` (Snowsight infers it; connectors don't) |
| `Cannot perform CREATE TASK. No current database.` | Context lost after failed statement | Use fully-qualified names: `DB.SCHEMA.OBJECT` |
| `Invalid finalized root task` | Creating finalizer while root is started | Suspend root → create finalizer → resume root |
| `Unable to update graph with root task not suspended` | Modifying child while root is active | Suspend root → modify child → resume root |

### File Deployment Difficulty

| File | Difficulty | Notes |
|------|-----------|-------|
| `01_database_and_schema.sql` | Easy | Pure DDL |
| `02_warehouses.sql` | Easy | Pure DDL + compute pool |
| `03_external_access.sql` | Easy | Must execute in order (rule before EAI) |
| `04_email_integration.sql` | Easy | Single DDL |
| `04_feed_archive_loader.sql` | Easy | Python SPs with `$$` — works as-is |
| `05_feed_ingestion_dag.sql` | Easy | Tasks have simple CALL bodies |
| `00_sic_reference_data.sql` | Easy | DDL + INSERT VALUES |
| `01_ticker_enrichment.sql` | Easy | Python SPs with `$$` |
| `01_text_cleaning_udf.sql` | Easy | Python UDF |
| `02_chunking_udf.sql` | Easy | Python UDF |
| `07_signal_excerpt_view.sql` | Easy | Single CREATE VIEW |
| `05_processing_task_dag.sql` | Hard | Tasks with `BEGIN...END` bodies — use `EXECUTE IMMEDIATE $$` per task |
| `06_process_single_filing.sql` | Medium | Python SP + SQL SPs — works with `$$` |
| `04_serving_task_dag.sql` | Easy | `REDEPLOY_AGENT()` SP eliminates nested `$$` issue |
| `01_cortex_search.sql` | Easy | Single DDL |
| `02_semantic_view.sql` | Easy | Single DDL |
| `01_agent_deployment.sql` | Easy | Direct `CREATE AGENT ... FROM SPECIFICATION $$` works |
| `02_eval_framework.sql` | Hard | Complex nested SPs — deploy from Snowsight |

### The EXECUTE IMMEDIATE $$ Pattern

For any `CREATE TASK` or `CREATE PROCEDURE` with `BEGIN...END`:

```sql
EXECUTE IMMEDIATE
$$
CREATE OR REPLACE TASK FULLY.QUALIFIED.TASK_NAME
    WAREHOUSE = MY_WH
    AFTER FULLY.QUALIFIED.PARENT
AS
BEGIN
    UPDATE table SET col = val WHERE cond;
    RETURN 'done';
END;
$$
```

Rules:
- Fully qualify ALL object names (tasks, predecessors, FINALIZE references)
- No `$$` inside the body (use string concatenation for dynamic SQL)
- Suspend root task before creating finalizer tasks
- Resume all child tasks before resuming root (leaf-to-root order)

### Quick Start for Programmatic Deployment

After infrastructure (Phase 1), deploy all SPs then call `QUICK_START`:

```sql
CALL QUICK_START('2025-02-21');
```

This creates a monitorable dynamic task DAG that handles: ingestion → enrichment → chunking → signal extraction → metrics → guidance → normalization → search refresh → serving update. Returns immediately.

---

## AVAILABLE SKILLS

Use the `skill` tool to invoke these when working on this project:

| Skill | When to use |
|---|---|
| `cortex-agent` | Creating, editing, testing, or evaluating the SEC_FILING_AGENT |
| `cortex-agent` → `evaluate-cortex-agent` | Running formal agent evaluations with metrics |
| `cortex-agent` → `optimize-cortex-agent` | Improving agent performance based on eval results |
| `semantic-view` | Creating or modifying the semantic view |
| `semantic-view-optimization` | Auditing semantic view quality, adding VQRs |
| `sql-author` | Writing and verifying SQL queries against project tables |
| `cortex-ai-function-studio` | Testing AI function outputs (extraction prompts) |
| `search-optimization` | Tuning Cortex Search (chunk size, embedding model, attributes) |
| `data-quality` | Validating extraction quality, checking data distributions |
| `lineage` | Understanding data flow between tables in the pipeline |
| `cost-intelligence` | Monitoring credit usage and optimizing spend |
| `drawio-diagrams` | Generating architectural diagrams for docs/diagrams/ |
