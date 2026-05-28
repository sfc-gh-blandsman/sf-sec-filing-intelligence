-- =============================================================================
-- 04: Custom Analysis SP — EXPLORER_CUSTOM_ANALYSIS
-- =============================================================================
-- Flexible batch analysis SP for the Research Explorer tab.
-- Runs filtered Cortex Search queries across all companies in a sector,
-- optionally with LLM synthesis (summarized/compared modes).
--
-- Parameters:
--   P_SECTOR      — Industry sector to analyze (from FILING_INDEX.INDUSTRY_SECTOR)
--   P_QUERY       — Optional semantic search query (NULL = retrieve by metadata only)
--   P_SECTION     — Section filter (NULL = all sections)
--   P_FORM_TYPE   — Form type filter (NULL = all form types)
--   P_OUTPUT_MODE — 'excerpts' (no LLM), 'summarized', or 'compared'
--   P_LIMIT       — Max companies to process (NULL = all companies in sector)
--
-- Results stored in EXPLORER_RESULTS with QUERY_PARAMS for audit trail.
--
-- Dependencies:
--   - EXPLORER_RESULTS table (created by 01_batch_sp.sql)
--   - Cortex Search service (SEC_FILING_SEARCH)
--   - _CFG() function
--   - FILING_INDEX table (for company lookup)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: EXPLORER_CUSTOM_ANALYSIS
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXPLORER_CUSTOM_ANALYSIS(
    P_SECTOR VARCHAR,
    P_QUERY TEXT DEFAULT NULL,
    P_SECTION VARCHAR DEFAULT NULL,
    P_FORM_TYPE VARCHAR DEFAULT NULL,
    P_OUTPUT_MODE VARCHAR DEFAULT 'excerpts',
    P_LIMIT INT DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_custom_analysis'
EXECUTE AS CALLER
AS $$
import json
from datetime import datetime

def run_custom_analysis(session, p_sector, p_query, p_section, p_form_type, p_output_mode, p_limit):
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    def esc(s):
        """Escape single quotes for SQL string literals."""
        if s is None:
            return ''
        return str(s).replace("'", "''")

    # Get search service name from config
    search_svc = session.sql(f"SELECT {fqn}._CFG('search_service')").collect()[0][0]
    search_fqn = f"{db}.{schema}.{search_svc}"

    run_id = f"custom-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    query_params = json.dumps({
        "sector": p_sector,
        "query": p_query,
        "section": p_section,
        "form_type": p_form_type,
        "output_mode": p_output_mode,
        "limit": p_limit
    })

    # Get companies in sector, ordered by filing count (most active first)
    limit_clause = f"LIMIT {p_limit}" if p_limit else ""
    tickers_df = session.sql(f"""
        SELECT TICKER, COMPANY_NAME, COUNT(*) AS cnt
        FROM {fqn}.FILING_INDEX
        WHERE INDUSTRY_SECTOR = '{esc(p_sector)}'
          AND TICKER IS NOT NULL
        GROUP BY TICKER, COMPANY_NAME
        ORDER BY cnt DESC
        {limit_clause}
    """).collect()

    if not tickers_df:
        return f"No companies with tickers found in sector: {p_sector}"

    all_results = []
    search_columns = ["CHUNK_TEXT", "CHUNK_ID", "ACCESSION_NO", "COMPANY_NAME",
                      "TICKER", "FORM_TYPE", "SECTION_NAME", "FILED_AT"]

    for row in tickers_df:
        ticker = row["TICKER"]
        company = row["COMPANY_NAME"]

        # Build search request with optional filters
        search_query = p_query if p_query else f"{p_section or 'filing'} {company}"

        # Build filter - only include non-NULL filter conditions
        filter_parts = [{"@eq": {"TICKER": ticker}}]
        if p_form_type:
            filter_parts.append({"@eq": {"FORM_TYPE": p_form_type}})
        if p_section:
            filter_parts.append({"@eq": {"SECTION_NAME": p_section}})

        filters = {"@and": filter_parts} if len(filter_parts) > 1 else filter_parts[0]

        request = {"query": search_query, "columns": search_columns, "limit": 5, "filter": filters}
        request_json = json.dumps(request)

        try:
            result = session.sql(f"""
                SELECT PARSE_JSON(
                    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                        '{search_fqn}',
                        '{request_json.replace("'", "''")}'
                    )
                )['results'] AS results
            """).collect()

            if result and result[0]["RESULTS"]:
                chunks = json.loads(result[0]["RESULTS"])
                for chunk in chunks:
                    all_results.append({
                        "ticker": ticker,
                        "company": company,
                        "chunk_text": chunk.get("CHUNK_TEXT", "")[:4000],
                        "filed_at": chunk.get("FILED_AT", ""),
                        "section": chunk.get("SECTION_NAME", ""),
                        "form_type": chunk.get("FORM_TYPE", "")
                    })
        except Exception as e:
            all_results.append({
                "ticker": ticker,
                "company": company,
                "chunk_text": f"ERROR: {str(e)[:200]}",
                "filed_at": "",
                "section": p_section or "",
                "form_type": p_form_type or ""
            })

    if not all_results:
        return f"No results found for sector {p_sector} with given filters."

    # For excerpts mode: store raw results
    if p_output_mode == 'excerpts':
        for r in all_results:
            session.sql(f"""
                INSERT INTO {fqn}.EXPLORER_RESULTS
                    (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE, QUERY_PARAMS, RUN_TIMESTAMP)
                VALUES ('{run_id}', '{esc(p_sector)}', 'custom_excerpt',
                        '{esc(r["ticker"])} | {esc(r["filed_at"])} | {esc(r["section"])}',
                        '{esc(r["chunk_text"])}',
                        '{esc(query_params)}',
                        CURRENT_TIMESTAMP())
            """).collect()

    elif p_output_mode in ('summarized', 'compared'):
        # Group chunks by company for LLM synthesis
        by_company = {}
        for r in all_results:
            key = r["ticker"]
            if key not in by_company:
                by_company[key] = {"company": r["company"], "chunks": []}
            by_company[key]["chunks"].append(r["chunk_text"][:2000])

        if p_output_mode == 'summarized':
            for ticker, data in by_company.items():
                context = "\n\n".join(data["chunks"][:3])
                section_label = p_section or "filing"
                prompt = f"Summarize the key points from these {section_label} excerpts from {data['company']} ({ticker}) SEC filings. Be concise (3-5 bullet points):\n\n{context}"
                prompt_escaped = prompt.replace("'", "''")
                try:
                    resp = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.3-70b', '{prompt_escaped}') AS r").collect()
                    summary = resp[0]["R"] if resp else "No summary generated"
                except Exception as e:
                    summary = f"ERROR: {str(e)[:200]}"

                session.sql(f"""
                    INSERT INTO {fqn}.EXPLORER_RESULTS
                        (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE, QUERY_PARAMS, RUN_TIMESTAMP)
                    VALUES ('{run_id}', '{esc(p_sector)}', 'custom_summary',
                            '{esc(ticker)} - {esc(data["company"])}',
                            '{esc(summary)}',
                            '{esc(query_params)}',
                            CURRENT_TIMESTAMP())
                """).collect()

        elif p_output_mode == 'compared':
            # Build comparison context from all companies (top 10 by chunk count)
            comparison_parts = []
            for ticker, data in list(by_company.items())[:10]:
                excerpt = data["chunks"][0][:1500] if data["chunks"] else ""
                comparison_parts.append(f"[{data['company']} ({ticker})]:\n{excerpt}")

            context = "\n\n---\n\n".join(comparison_parts)
            section_label = p_section or "filings"
            prompt = f"Compare the {section_label} across these companies. Identify 3-5 common themes and note how each company addresses them. Format as a markdown table with themes as rows and companies as columns:\n\n{context}"
            prompt_escaped = prompt.replace("'", "''")
            try:
                resp = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.3-70b', '{prompt_escaped}') AS r").collect()
                comparison = resp[0]["R"] if resp else "No comparison generated"
            except Exception as e:
                comparison = f"ERROR: {str(e)[:200]}"

            session.sql(f"""
                INSERT INTO {fqn}.EXPLORER_RESULTS
                    (RUN_ID, SECTOR, QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE, QUERY_PARAMS, RUN_TIMESTAMP)
                VALUES ('{run_id}', '{esc(p_sector)}', 'custom_comparison',
                        'Cross-company {esc(section_label)} comparison',
                        '{esc(comparison)}',
                        '{esc(query_params)}',
                        CURRENT_TIMESTAMP())
            """).collect()

    return f"Custom analysis complete. Sector: {p_sector}, Companies: {len(tickers_df)}, Results: {len(all_results)}, Mode: {p_output_mode}. Run ID: {run_id}"
$$;
