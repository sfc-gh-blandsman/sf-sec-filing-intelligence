# agents.md — SEC Filing Intelligence

**Read this file at the start of every session before doing anything else.**

---

## PROJECT

A **SEC EDGAR filing intelligence pipeline** on Snowflake. The system ingests public SEC filings (10-K, 10-Q, 8-K), processes them through an AI pipeline (chunking, signal extraction, metrics, guidance), and serves them via:

1. **Cortex Search** — semantic retrieval over filing text chunks
2. **Cortex Analyst** — aggregate analytics via a semantic view over filing signals
3. **Cortex Agent** — investment research Q&A combining both tools
4. **Streamlit Dashboard** — 6-tab monitoring and control app

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
| `SEC_FILING_DASHBOARD` | Streamlit | 6-tab monitoring dashboard |
| `_FEED_INGEST_LOG` | Table | Feed ingestion progress tracking |
| `_PIPELINE_CONFIG` | Table | Runtime configuration for task DAGs |
| `EVAL_RESULTS` | Table | Materialized agent evaluation results |

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
│   │   └── 05_feed_ingestion_dag.sql  ← Feed DAG (12 parallel months, multi-year loop)
│   ├── 03_processing/
│   │   ├── 01_text_cleaning_udf.sql   ← CLEAN_TEXT UDF
│   │   ├── 02_chunking_udf.sql        ← CHUNK_FILING UDF (section-aware, 1500/200)
│   │   ├── 03_chunking_pipeline.sql   ← Reference: manual bulk chunking
│   │   ├── 04_signal_extraction.sql   ← Reference: manual bulk extraction
│   │   └── 05_processing_task_dag.sql ← Processing DAG (14 tasks, production)
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
│   │   ├── 01_batch_sp.sql            ← EXPLORER_SECTOR_ANALYSIS SP
│   │   ├── 02_task_schedule.sql       ← Weekly batch task
│   │   └── 03_sample_queries.sql      ← Interactive + batch examples
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
│   ├── SEC_Filing_Explorer.py         ← 6-tab SiS dashboard
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
