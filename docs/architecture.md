# Architecture — SEC Filing Intelligence

## System Overview

An end-to-end pipeline that ingests SEC EDGAR filings (10-K, 10-Q, 8-K), processes them through AI extraction, and serves structured investment signals via semantic search, a Cortex Agent, and a Streamlit monitoring dashboard.

---

## Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     SEC EDGAR Public Feed Archives                            │
│         Daily tar.gz archives containing all filings for that day            │
│         https://www.sec.gov/Archives/edgar/Feed/{YYYY}/QTR{Q}/               │
└──────────────────────────┬───────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    FEED INGESTION DAG (12 parallel monthly tasks)             │
│  Downloads tar.gz → parses SEC headers → extracts metadata + content         │
│  Outputs: FILING_INDEX (metadata) + FILING_CONTENT (raw text)                │
└──────────────────────────┬───────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         ENRICHMENT DAG                                        │
│  ┌─────────────────┐    ┌────────────────────────────┐                       │
│  │ ENRICH_TICKERS  │    │ ENRICH_BACKFILL            │                       │
│  │ (SEC API by CIK)│───▶│ (SIC→industry, period)     │                       │
│  └─────────────────┘    └────────────────────────────┘                       │
└──────────────────────────┬───────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING DAG (14 tasks)                              │
│                                                                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐  ┌──────────┐ ┌──────────┐ ┌────────┐│
│  │CHUNK_10K│ │CHUNK_10Q│ │CHUNK_8K │  │SIGNAL_10K│ │SIGNAL_10Q│ │SIG_8K  ││
│  └────┬────┘ └────┬────┘ └────┬────┘  └─────┬────┘ └─────┬────┘ └───┬────┘│
│       │            │           │              │             │          │     │
│       │            │           │              ▼             ▼          ▼     │
│       │            │           │     ┌────────────────────────────────────┐  │
│       │            │           │     │  NORMALIZE_SIGNALS (event types)   │  │
│       │            │           │     │  METRICS_EXTRACT (revenue/EPS)     │  │
│       │            │           │     │  GUIDANCE_EXTRACT (forward outlook)│  │
│       │            │           │     └──────────────────┬─────────────────┘  │
│       │            │           │                        │                    │
│       ▼            ▼           ▼                        ▼                    │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │              PROPAGATE_INDUSTRY (industry + ticker to all tables)     │    │
│  └──────────────────────────────┬───────────────────────────────────────┘    │
│                                 │                                            │
│                                 ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │          REFRESH_SEARCH (incremental Cortex Search update)            │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────┬───────────────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                        │
│                                                                              │
│  ┌─────────────────────┐  ┌────────────────────┐  ┌──────────────────────┐  │
│  │  Cortex Search      │  │  Semantic View     │  │  Cortex Agent        │  │
│  │  (Arctic M-v1.5)    │  │  (live query)      │  │  (search + analyst)  │  │
│  │  FILING_CHUNKS →    │  │  FILING_SIGNALS →  │  │  2-tool orchestrator │  │
│  │  semantic retrieval  │  │  structured SQL    │  │  claude-opus-4-7     │  │
│  └─────────────────────┘  └────────────────────┘  └──────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │              Streamlit Dashboard (6 tabs)                              │    │
│  │  Pipeline | Data Quality | Explorer (RAG) | Cost | Control | Eval     │    │
│  └──────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Chain

The full automated pipeline chains via task DAG finalizers:

```
Feed DAG → Enrich DAG → Processing DAG → Email Notification
                                              ↑
              Serving DAG (manual trigger, schema changes only)
```

Each DAG is triggered by the previous DAG's finalizer via `EXECUTE TASK`. The pipeline is fully idempotent — safe to re-run at any point.

---

## Table Schema

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| FILING_INDEX | SEC filing metadata | ACCESSION_NO (PK), CIK, COMPANY_NAME, FORM_TYPE, FILED_AT, TICKER, INDUSTRY_SECTOR, SIC_CODE, PERIOD_OF_REPORT |
| FILING_CONTENT | Raw filing text | ACCESSION_NO, CONTENT_TEXT, PARSE_STATUS, SIGNAL_STATUS |
| FILING_CHUNKS | Section-aware text chunks | CHUNK_ID (PK), ACCESSION_NO, SECTION_NAME, CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR |
| FILING_SIGNALS | AI-extracted investment signals | SIGNAL_ID (PK), ACCESSION_NO, EVENT_TYPE, EVENT_TYPE_NORMALIZED, SENTIMENT, SUMMARY, REVENUE, EPS, FORWARD_GUIDANCE |
| _FEED_INGEST_LOG | Feed ingestion progress tracking | FEED_DATE, STATUS (DONE/SKIPPED_404/SKIPPED_403/ERROR), LOADED |
| _PIPELINE_CONFIG | Runtime config for task DAGs | KEY, VALUE |
| SIC_CODES | SIC → industry sector reference | SIC_CODE, INDUSTRY_SECTOR, INDUSTRY_TITLE |

---

## Key Design Decisions

1. **Feed archives over per-filing download** — A single daily tar.gz contains all filings (~100-1000x fewer HTTP requests). Ingests a full year in ~2.5 hours vs days.

2. **Section-aware chunking (1500 chars, 200 overlap)** — Identifies 10-K/10-Q/8-K sections (Risk Factors, MD&A, Financial Statements, etc.) before splitting. Chunks respect section boundaries.

3. **No stored embeddings** — Cortex Search auto-embeds at index time using Arctic M-v1.5 with incremental refresh. Saves storage and simplifies the pipeline.

4. **Two-pass AI extraction** — Base signals via AI_EXTRACT (fast, cheap: event type, sentiment, summary). Key metrics via AI_COMPLETE with structured output (targeted at financial keyword chunks only — avoids hallucinating numbers from non-financial filings).

5. **Event type normalization** — AI_EXTRACT produces 97+ hallucinated event types; a post-extraction normalization step maps these to 12 canonical categories via CASE WHEN rules.

6. **COALESCE pattern for semantic view** — `COALESCE(EVENT_TYPE_NORMALIZED, EVENT_TYPE)` provides graceful fallback for signals not yet normalized.

7. **Percentage-based finalizer guard** — Feed DAG won't advance to the next year until ≥90% of weekdays are covered. Prevents partial ingestion from being treated as complete.

8. **TICKER_CHECKED_AT optimization** — Prevents re-checking CIKs that have no ticker (non-public filers) on every enrichment run. Saves ~90 minutes of SEC API calls per run.

9. **_PIPELINE_CONFIG + _CFG() pattern** — Session variables are inaccessible inside task DAG bodies. A config table + helper function provides runtime configuration.

10. **All tasks idempotent** — Status columns (PARSE_STATUS, SIGNAL_STATUS, METRICS_EXTRACTED_AT, etc.) gate processing. Safe to re-run any task.

---

## Compute Strategy

| Phase | Warehouse | Type | Rationale |
|-------|-----------|------|-----------|
| Feed ingestion | FILING_INGEST_WH | Snowpark-optimized Medium | High-memory for streaming 800MB+ archives in Python |
| Chunking + Signal extraction | FILING_BUILD_WH | Standard 4XL | CPU/GPU-intensive AI functions over large datasets |
| Metrics + Guidance extraction | FILING_BUILD_WH | Standard 4XL | AI_COMPLETE with structured output (batched) |
| Enrichment + Propagation | FILING_WH | Standard Large | Lightweight SQL updates |
| Search refresh | FILING_WH | Standard Large | Incremental embedding updates |
| Dashboard + Agent queries | FILING_WH | Standard Large | Real-time serving |

---

## Streamlit Dashboard

6-tab monitoring and control app deployed as Streamlit in Snowflake:

| Tab | Purpose |
|-----|---------|
| Pipeline | DAG diagrams (color-coded by live status), row counts, ingestion progress + ETA |
| Data Quality | Completeness scorecard, event type distribution, extraction methodology |
| Filing Explorer (RAG) | Chat-based semantic search with LLM-generated answers |
| Cost Monitor | Warehouse credits, AI token usage, search service stats |
| Pipeline Control | Trigger runs, edit config, emergency stop + recovery |
| Agent Eval | Run evaluations, view per-question scores, detailed explanations |

---

## Evaluation Framework

The agent is evaluated against 20 test questions across two metrics:
- **Answer Correctness** — factual accuracy of responses
- **Logical Consistency** — quality of reasoning and tool selection

Eval runs are triggered via `_RUN_EVAL()` (interactive session) + `EVAL_DAG_ROOT` (materialization).
