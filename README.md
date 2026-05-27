# SEC Filing Intelligence

End-to-end pipeline for ingesting SEC EDGAR filings (10-K, 10-Q, 8-K), extracting structured investment signals with Cortex AI, and serving them via semantic search, a Cortex Agent, and a Streamlit monitoring dashboard.

## Architecture

```
SEC EDGAR Feed Archives
        │
        ▼
Feed Ingestion DAG (12 parallel monthly tasks, multi-year loop)
        │
        ▼
Enrichment DAG (ticker lookup + industry mapping)
        │
        ▼
Processing DAG (chunking + signals + metrics + guidance + normalization + search refresh)
        │
        ├──── Cortex Search (semantic retrieval over filing chunks)
        │           │
        │           ▼
        │     Cortex Agent (2-tool: search + analyst)
        │           ▲
        ├──── Semantic View (structured analytics over signals)
        │
        └──── Streamlit Dashboard (6-tab monitoring + control)
```

See `docs/diagrams/` for detailed draw.io diagrams (importable to LucidChart).

## Quick Start (~15 minutes)

1. Copy config and fill in your values:
   ```bash
   cp sql/00_config.sql.example sql/00_config.sql
   # Edit with your database, warehouse, email, user-agent, etc.
   ```

2. Run the preflight check (optional but recommended):
   ```sql
   -- Paste and run sql/00_preflight_check.sql
   -- Validates: AI functions, email, privileges, cross-region inference
   ```

3. Follow `docs/setup-guide.md` — 6 phases, all copy-paste in Snowsight worksheets:
   - Phase 1: Infrastructure (database, tables, warehouses, external access)
   - Phase 1b: Dashboard (deploy Streamlit for monitoring — works immediately)
   - Phase 2: Ingestion (feed archive SP + single day test)
   - Phase 3: Enrichment (SIC codes, tickers, industry mapping)
   - Phase 4: Processing (chunking + signal extraction)
   - Phase 5: Serving (Cortex Search + Semantic View + Agent)
   - Phase 6b: Task DAGs (enables automated pipeline + Pipeline Control tab)

4. Test the agent:
   ```sql
   SELECT SNOWFLAKE.CORTEX.AGENT(
       '<database>.<schema>.SEC_FILING_AGENT',
       'What risk factors did pharmaceutical companies disclose in recent 10-K filings?'
   );
   ```

## Build Time Estimates

| Phase | Quick Start | Full Year | Multi-Year (3-4 years) |
|-------|:-----------:|:---------:|:----------------------:|
| Infrastructure | 2 min | 2 min | 2 min |
| Feed ingestion | 2 min (1 day) | ~2.5 hrs | ~2.5 hrs/year |
| Enrichment | 1 min | ~15 min | ~15 min |
| Processing (chunk + signal + metrics) | 3 min | ~4 hrs | ~4 hrs |
| Search service build | 5 min | 30 min | 30 min |
| **Total** | **~15 min** | **~7 hrs** | **~7 + 2.5/year hrs** |

## Project Structure

```
sf-sec-filing-intelligence/
├── sql/
│   ├── 00_config.sql.example          ← Template config (committed)
│   ├── 00_preflight_check.sql         ← Account validation before install
│   ├── 01_infrastructure/             ← Database, warehouses, external access, email
│   ├── 02_ingestion/                  ← Feed archive loader + ingestion DAG
│   ├── 03_processing/                 ← Chunking, signals, metrics, guidance, processing DAG
│   ├── 04_enrichment/                 ← Ticker lookup, industry mapping, event normalization
│   ├── 05_serving/                    ← Cortex Search, semantic view, Streamlit deploy
│   ├── 06_agent/                      ← Agent deployment + evaluation framework
│   ├── 07_explorer/                   ← Batch analysis SP + scheduled tasks
│   └── 99_teardown/                   ← Full project teardown (uninstall)
├── agent/
│   ├── spec/                          ← Agent YAML specification (reference)
│   └── eval/                          ← Evaluation config + sample questions
├── streamlit/
│   ├── SEC_Filing_Explorer.py         ← 6-tab dashboard (Pipeline, Quality, Explorer, Cost, Control, Eval)
│   └── environment.yml               ← SiS dependencies (snowpark, streamlit, plotly)
└── docs/
    ├── architecture.md                ← System design + data flow
    ├── setup-guide.md                 ← Step-by-step installation
    ├── lessons-learned.md             ← Architectural insights + gotchas
    └── diagrams/                      ← draw.io diagrams (importable to LucidChart)
```

## Monitoring Dashboard

A 6-tab Streamlit in Snowflake app provides real-time monitoring and control:

| Tab | Purpose |
|-----|---------|
| Pipeline | DAG diagrams with live status, ingestion progress + ETA |
| Data Quality | Completeness scorecard, event type distribution, extraction methodology |
| Filing Explorer (RAG) | Chat interface with semantic search + LLM-generated answers |
| Cost Monitor | Warehouse credits, AI token usage, search service stats |
| Pipeline Control | Trigger ingestion runs, edit config, emergency stop + recovery |
| Agent Eval | Run evaluations, per-question scores, detailed explanations |

Deploy early (Phase 1b) to monitor pipeline progress from the start.

## Task DAGs

Four automated DAGs chain sequentially via finalizers:

| DAG | Tasks | Duration | Trigger |
|-----|-------|----------|---------|
| Feed Ingestion | 15 (12 months + root + validate + finalizer) | ~2.5 hrs/year | Manual or Pipeline Control tab |
| Enrichment | 4 (root + tickers + backfill + finalizer) | ~15 min | Auto-chained from feed |
| Processing | 15 (chunks + signals + normalize + metrics + guidance + propagate + search) | ~4 hrs | Auto-chained from enrichment |
| Serving | 3 (root + semantic view + finalizer) | <1 min | Manual (schema changes only) |

Plus an optional Eval DAG (4 tasks) for agent evaluation.

## Teardown / Uninstall

To completely remove all project objects and start fresh:
```sql
-- Run sql/99_teardown/01_teardown.sql in Snowsight as ACCOUNTADMIN
-- Drops: all tasks, streamlit, agent, search service, semantic view, SPs, tables, warehouses, database
```

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- Cortex AI functions enabled (AI_EXTRACT, COMPLETE)
- Cortex Search and Cortex Agent features enabled
- External network access capability (for SEC EDGAR API)
- Verified email address (for pipeline notifications)

## Configuration

All values are parameterized via session variables in `sql/00_config.sql`. No hardcoded database names, schema names, emails, or account references exist in the codebase.

Key config variables:
- `config_database` / `config_schema` — where all objects are created
- `config_warehouse` / `config_warehouse_build` / `config_warehouse_ingest` — compute resources
- `config_eai_name` — external access integration for SEC EDGAR
- `config_user_agent` — SEC EDGAR API user agent string (required by fair use policy)
- `config_search_service` / `config_semantic_view` / `config_agent_name` — service names

## Signal Extraction

The pipeline uses a multi-pass approach:

1. **Base signals** (AI_EXTRACT, arctic-extract): event_type, sentiment, summary, risk_flags — fast and cheap
2. **Event normalization** (rule-based): Maps 97+ AI-generated event types to 12 canonical categories
3. **Key metrics** (AI_COMPLETE, llama3.3-70b): revenue, net_income, eps — keyword-targeted from Financial Statements/MD&A chunks
4. **Forward guidance** (AI_COMPLETE, llama3.3-70b): outlook statements — targeted from 10-K/10-Q filings with forward-looking language

## Data Sources

All data comes from the SEC EDGAR public API:
- **Feed archives**: `https://www.sec.gov/Archives/edgar/Feed/{YYYY}/QTR{Q}/{YYYYMMDD}.nc.tar.gz`
- **Ticker lookup**: `https://data.sec.gov/submissions/CIK{cik}.json`

## License

Internal use. SEC EDGAR data is public domain.
