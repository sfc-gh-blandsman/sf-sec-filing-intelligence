"""
SEC Filing Intelligence Dashboard
==================================
4-tab Streamlit in Snowflake app:
  1. Pipeline Dashboard — data health, DAG status, row counts
  2. Eval Results — agent evaluation scores and breakdown
  3. SEC Filing Explorer — RAG search + document viewer
  4. Cost Monitor — credit usage and AI function costs
"""
import json
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# =============================================================================
# Configuration
# =============================================================================

st.set_page_config(page_title="SEC Filing Intelligence", layout="wide")

# Session initialization — supports both container and warehouse runtimes
try:
    conn = st.connection("snowflake")
    session = conn.session()
except Exception:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()

# Read config from _PIPELINE_CONFIG
@st.cache_data(ttl=300)
def get_config():
    rows = session.sql("SELECT KEY, VALUE FROM _PIPELINE_CONFIG").collect()
    return {r["KEY"]: r["VALUE"] for r in rows}

CONFIG = get_config()
SEARCH_SERVICE = f"{CONFIG.get('database', 'SEC_FILINGS')}.{CONFIG.get('schema', 'FILING_DATA')}.{CONFIG.get('search_service', 'SEC_FILING_SEARCH')}"

# =============================================================================
# Tab 1: Pipeline Dashboard
# =============================================================================

# DAG definitions: (task_name, [dependencies])
FEED_DAG = [
    ("T_FEED_INGEST_ROOT", []),
    ("T_FEED_JAN", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_FEB", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_MAR", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_APR", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_MAY", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_JUN", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_JUL", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_AUG", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_SEP", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_OCT", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_NOV", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_DEC", ["T_FEED_INGEST_ROOT"]),
    ("T_FEED_VALIDATE", ["T_FEED_JAN", "T_FEED_FEB", "T_FEED_MAR", "T_FEED_APR",
                          "T_FEED_MAY", "T_FEED_JUN", "T_FEED_JUL", "T_FEED_AUG",
                          "T_FEED_SEP", "T_FEED_OCT", "T_FEED_NOV", "T_FEED_DEC"]),
    ("T_FEED_INGEST_FINALIZER", []),
]

ENRICH_DAG = [
    ("T_ENRICH_ROOT", []),
    ("T_ENRICH_TICKERS", ["T_ENRICH_ROOT"]),
    ("T_ENRICH_BACKFILL", ["T_ENRICH_TICKERS"]),
    ("T_ENRICH_FINALIZER", []),
]

PROCESSING_DAG = [
    ("T_PROCESSING_ROOT", []),
    ("T_CHUNK_10K", ["T_PROCESSING_ROOT"]),
    ("T_CHUNK_10Q", ["T_PROCESSING_ROOT"]),
    ("T_CHUNK_8K", ["T_PROCESSING_ROOT"]),
    ("T_SIGNAL_10K", ["T_PROCESSING_ROOT"]),
    ("T_SIGNAL_10Q", ["T_PROCESSING_ROOT"]),
    ("T_SIGNAL_8K", ["T_PROCESSING_ROOT"]),
    ("T_NORMALIZE_SIGNALS", ["T_SIGNAL_10K", "T_SIGNAL_10Q", "T_SIGNAL_8K"]),
    ("T_METRICS_EXTRACT", ["T_CHUNK_10K", "T_CHUNK_10Q", "T_CHUNK_8K",
                            "T_SIGNAL_10K", "T_SIGNAL_10Q", "T_SIGNAL_8K"]),
    ("T_GUIDANCE_EXTRACT", ["T_CHUNK_10K", "T_CHUNK_10Q", "T_CHUNK_8K",
                             "T_SIGNAL_10K", "T_SIGNAL_10Q", "T_SIGNAL_8K"]),
    ("T_PROPAGATE_INDUSTRY", ["T_CHUNK_10K", "T_CHUNK_10Q", "T_CHUNK_8K",
                               "T_SIGNAL_10K", "T_SIGNAL_10Q", "T_SIGNAL_8K",
                               "T_METRICS_EXTRACT", "T_NORMALIZE_SIGNALS"]),
    ("T_REFRESH_SEARCH", ["T_PROPAGATE_INDUSTRY"]),
    ("T_WAIT_SEARCH_ACTIVE", ["T_REFRESH_SEARCH"]),
    ("T_PROCESSING_FINALIZER", []),
]

EVAL_DAG = [
    ("EVAL_DAG_ROOT", []),
    ("EVAL_DAG_MATERIALIZE", ["EVAL_DAG_ROOT"]),
    ("EVAL_DAG_BENCHMARK", ["EVAL_DAG_MATERIALIZE"]),
    ("EVAL_DAG_FINALIZER", []),
]

# Color scheme for node states
STATE_COLORS = {
    "SUCCEEDED": "#2ECC40",
    "EXECUTING": "#FFDC00",
    "FAILED": "#FF4136",
    "SCHEDULED": "#AAAAAA",
    "SKIPPED": "#B10DC9",
}


def build_dag_dot(dag_def, task_states):
    """Generate graphviz DOT source for a DAG with color-coded nodes."""
    lines = ["digraph {", "  rankdir=LR;", "  node [shape=box, style=filled, fontsize=10];"]

    for task_name, deps in dag_def:
        state = task_states.get(task_name, "PENDING")
        color = STATE_COLORS.get(state, "#DDDDDD")
        fontcolor = "black" if state != "FAILED" else "white"
        # Mark executing nodes with bold border
        peripheries = "2" if state == "EXECUTING" else "1"
        label = task_name.replace("T_FEED_INGEST_", "").replace("T_FEED_", "").replace("T_ENRICH_", "").replace("T_PROCESSING_", "").replace("T_", "")
        if state == "EXECUTING":
            label = f">>> {label} <<<"
        lines.append(f'  "{task_name}" [label="{label}", fillcolor="{color}", fontcolor="{fontcolor}", peripheries={peripheries}];')

    for task_name, deps in dag_def:
        for dep in deps:
            lines.append(f'  "{dep}" -> "{task_name}";')

    lines.append("}")
    return "\n".join(lines)


def render_pipeline_dashboard():
    st.header("Pipeline Dashboard")

    # Key metrics
    @st.cache_data(ttl=60)
    def get_row_counts():
        return session.sql("""
            SELECT
                (SELECT COUNT(*) FROM FILING_INDEX) AS index_count,
                (SELECT COUNT(*) FROM FILING_CONTENT) AS content_count,
                (SELECT COUNT(*) FROM FILING_CHUNKS) AS chunk_count,
                (SELECT COUNT(*) FROM FILING_SIGNALS) AS signal_count
        """).collect()[0]

    counts = get_row_counts()
    with st.container(border=True):
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Filing Index", f"{counts['INDEX_COUNT']:,}")
        col2.metric("Filing Content", f"{counts['CONTENT_COUNT']:,}")
        col3.metric("Chunks", f"{counts['CHUNK_COUNT']:,}")
        col4.metric("Signals", f"{counts['SIGNAL_COUNT']:,}")

    st.divider()

    # -------------------------------------------------------------------------
    # DAG Diagram with live status
    # -------------------------------------------------------------------------
    st.subheader("Pipeline DAG Status")

    @st.cache_data(ttl=15)
    def get_task_states():
        """Get most recent state for each task from the last 24h."""
        rows = session.sql("""
            SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
                   GRAPH_RUN_GROUP_ID,
                   DATEDIFF('second', SCHEDULED_TIME, COALESCE(COMPLETED_TIME, CURRENT_TIMESTAMP())) AS elapsed_sec
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
                RESULT_LIMIT => 200
            ))
            WHERE NAME LIKE 'T_%'
            ORDER BY SCHEDULED_TIME DESC
        """).collect()
        # Keep only the most recent run per task
        states = {}
        for r in rows:
            name = r["NAME"]
            if name not in states:
                states[name] = {
                    "state": r["STATE"],
                    "elapsed_sec": r["ELAPSED_SEC"],
                    "scheduled": r["SCHEDULED_TIME"],
                    "completed": r["COMPLETED_TIME"],
                    "group_id": r["GRAPH_RUN_GROUP_ID"],
                }
        return states

    try:
        task_states_raw = get_task_states()
    except Exception:
        task_states_raw = {}

    # Extract state string only for the DOT builder
    task_state_map = {k: v["state"] for k, v in task_states_raw.items()}

    # Identify which DAG is currently active
    executing_tasks = [k for k, v in task_state_map.items() if v == "EXECUTING"]
    active_dag_name = None
    if any(t.startswith("T_FEED") for t in executing_tasks):
        active_dag_name = "Feed Ingestion"
    elif any(t.startswith("T_ENRICH") for t in executing_tasks):
        active_dag_name = "Enrichment"
    elif any(t.startswith("T_PROCESSING") or t.startswith("T_CHUNK") or t.startswith("T_SIGNAL") or t.startswith("T_METRICS") or t.startswith("T_GUIDANCE") or t.startswith("T_PROPAGATE") or t.startswith("T_REFRESH") or t.startswith("T_WAIT") for t in executing_tasks):
        active_dag_name = "Processing"

    if active_dag_name:
        st.success(f"Active DAG: **{active_dag_name}** — {len(executing_tasks)} task(s) executing")
    else:
        st.info("No DAG currently executing.")

    # Show DAG tabs
    dag_tab1, dag_tab2, dag_tab3, dag_tab4 = st.tabs(["Feed Ingestion", "Enrichment", "Processing", "Agent Eval"])

    with dag_tab1:
        dot = build_dag_dot(FEED_DAG, task_state_map)
        st.graphviz_chart(dot)
    with dag_tab2:
        dot = build_dag_dot(ENRICH_DAG, task_state_map)
        st.graphviz_chart(dot)
    with dag_tab3:
        dot = build_dag_dot(PROCESSING_DAG, task_state_map)
        st.graphviz_chart(dot)
    with dag_tab4:
        dot = build_dag_dot(EVAL_DAG, task_state_map)
        st.graphviz_chart(dot)

    # Legend
    legend_cols = st.columns(5)
    for col, (state, color) in zip(legend_cols, STATE_COLORS.items()):
        col.markdown(f'<span style="background-color:{color};padding:2px 8px;border-radius:3px;font-size:12px">{state}</span>', unsafe_allow_html=True)

    st.divider()

    # -------------------------------------------------------------------------
    # Progress & ETA
    # -------------------------------------------------------------------------
    st.subheader("Ingestion Progress & ETA")

    @st.cache_data(ttl=30)
    def get_ingest_progress():
        return session.sql("""
            WITH years AS (
                SELECT DISTINCT LEFT(FEED_DATE, 4) AS YEAR FROM _FEED_INGEST_LOG
            ),
            all_days AS (
                SELECT y.YEAR,
                       DATEADD('day', g.seq, y.YEAR || '-01-01') AS d
                FROM years y
                CROSS JOIN (SELECT ROW_NUMBER() OVER(ORDER BY SEQ4()) - 1 AS seq
                            FROM TABLE(GENERATOR(ROWCOUNT => 366))) g
            ),
            weekdays AS (
                SELECT YEAR, COUNT(*) AS total_days
                FROM all_days
                WHERE DAYOFWEEK(d) NOT IN (0, 6)
                  AND YEAR(d) = YEAR::INT
                GROUP BY YEAR
            ),
            completed AS (
                SELECT LEFT(FEED_DATE, 4) AS YEAR,
                       COUNT(*) AS completed_days
                FROM _FEED_INGEST_LOG
                WHERE STATUS IN ('DONE', 'SKIPPED_404', 'SKIPPED_403')
                GROUP BY 1
            )
            SELECT w.YEAR, w.total_days, COALESCE(c.completed_days, 0) AS completed_days
            FROM weekdays w
            LEFT JOIN completed c ON w.YEAR = c.YEAR
            ORDER BY 1
        """).to_pandas()

    df_progress = get_ingest_progress()
    if not df_progress.empty:
        for _, row in df_progress.iterrows():
            year = int(row["YEAR"])
            total = int(row["TOTAL_DAYS"])
            done = int(row["COMPLETED_DAYS"])
            pct = done / total if total > 0 else 0
            st.markdown(f"**{year}**: {done}/{total} days ({pct*100:.0f}%)")
            st.progress(pct)

        # Overall progress
        total_all = int(df_progress["TOTAL_DAYS"].sum())
        done_all = int(df_progress["COMPLETED_DAYS"].sum())
        remaining = total_all - done_all

        if remaining > 0:
            years_incomplete = len(df_progress[df_progress["COMPLETED_DAYS"] < df_progress["TOTAL_DAYS"]])
            st.caption(f"{remaining} weekdays remaining across {years_incomplete} year(s)")
        elif remaining == 0:
            st.success("All feed ingestion complete!")
    else:
        st.info("No feed ingest log data.")

    st.divider()

    # -------------------------------------------------------------------------
    # Processing progress
    # -------------------------------------------------------------------------
    st.subheader("Processing Progress")
    @st.cache_data(ttl=30)
    def get_processing_progress():
        return session.sql("""
            SELECT
                COUNT(*) AS total,
                COUNT(CASE WHEN PARSE_STATUS = 'CHUNKED' THEN 1 END) AS chunked,
                COUNT(CASE WHEN SIGNAL_STATUS = 'EXTRACTED' THEN 1 END) AS extracted,
                COUNT(CASE WHEN PARSE_STATUS = 'PENDING' THEN 1 END) AS pending_parse,
                COUNT(CASE WHEN SIGNAL_STATUS = 'PENDING' THEN 1 END) AS pending_signal
            FROM FILING_CONTENT
        """).collect()[0]

    proc = get_processing_progress()
    total_content = int(proc["TOTAL"]) if proc["TOTAL"] else 0
    if total_content > 0:
        chunk_pct = int(proc["CHUNKED"]) / total_content
        signal_pct = int(proc["EXTRACTED"]) / total_content
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Chunking**: {int(proc['CHUNKED']):,}/{total_content:,} ({chunk_pct*100:.0f}%)")
            st.progress(chunk_pct)
        with c2:
            st.markdown(f"**Signal Extraction**: {int(proc['EXTRACTED']):,}/{total_content:,} ({signal_pct*100:.0f}%)")
            st.progress(signal_pct)
    else:
        st.info("No filing content to process yet.")

    st.divider()

    # Year breakdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Filings by Year")
        @st.cache_data(ttl=60)
        def get_year_breakdown():
            rows = session.sql("""
                SELECT YEAR(FILED_AT) AS year,
                       COUNT(*) AS filings,
                       COUNT(TICKER) AS with_ticker,
                       COUNT(INDUSTRY_SECTOR) AS with_industry
                FROM FILING_INDEX
                WHERE YEAR(FILED_AT) >= 2020
                GROUP BY 1 ORDER BY 1
            """).to_pandas()
            return rows

        df_years = get_year_breakdown()
        if not df_years.empty:
            fig = px.bar(df_years, x="YEAR", y="FILINGS", text_auto=True,
                        color_discrete_sequence=["#29B5E8"])
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Feed Ingest Status")
        @st.cache_data(ttl=60)
        def get_feed_status():
            return session.sql("""
                SELECT status, COUNT(*) AS days, SUM(loaded) AS filings
                FROM _FEED_INGEST_LOG
                GROUP BY 1 ORDER BY 1
            """).to_pandas()

        df_feed = get_feed_status()
        if not df_feed.empty:
            fig = px.pie(df_feed, values="DAYS", names="STATUS",
                        color_discrete_sequence=["#29B5E8", "#90EE90", "#FFD699", "#FF6B6B"])
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # Pipeline config
    with st.expander("Pipeline Configuration"):
        st.json(CONFIG)


# =============================================================================
# Tab 2: Eval Results
# =============================================================================

def render_eval_results():
    st.header("Agent Eval Results")

    st.markdown("""
    Evaluates the **SEC Filing Agent** against 20 test questions across two metrics:
    **Answer Correctness** (factual accuracy) and **Logical Consistency** (reasoning quality).

    **To generate a new eval run:**
    1. In a Snowsight worksheet: `CALL _RUN_EVAL();` — starts the evaluation
    2. Wait for scoring to complete (~5-10 min)
    3. `EXECUTE TASK EVAL_DAG_ROOT;` — materializes results into this dashboard

    Or use the button below to trigger the full process.
    """)

    # Check if eval is currently running
    @st.cache_data(ttl=15)
    def is_eval_running():
        try:
            rows = session.sql("""
                SELECT NAME FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()),
                    RESULT_LIMIT => 20
                )) WHERE NAME LIKE 'EVAL_DAG%' AND STATE = 'EXECUTING'
            """).collect()
            return len(rows) > 0
        except Exception:
            return False

    eval_running = is_eval_running()

    if eval_running:
        st.warning("An evaluation is currently in progress. Wait for it to complete before starting a new one.")
        st.button("Run New Evaluation", disabled=True, key="run_eval_btn")
    else:
        st.caption("This will invoke the SEC Filing Agent 20 times (once per test question), score the responses, and materialize results. Takes ~5-10 minutes and consumes AI credits.")
        if st.button("Run New Evaluation", type="primary", key="run_eval_btn"):
            with st.spinner("Starting evaluation — calling _RUN_EVAL() and triggering EVAL_DAG_ROOT..."):
                try:
                    session.sql("CALL _RUN_EVAL()").collect()
                    session.sql("EXECUTE TASK EVAL_DAG_ROOT").collect()
                    st.success("Evaluation started. Results will appear here in ~10 minutes. Refresh to check.")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Failed to start eval: {str(e)[:200]}. Ensure `sql/06_agent/02_eval_framework.sql` has been deployed.")

    st.divider()

    @st.cache_data(ttl=60)
    def get_eval_runs():
        return session.sql("SELECT DISTINCT RUN_NAME FROM EVAL_RESULTS ORDER BY RUN_NAME DESC").to_pandas()

    runs = get_eval_runs()
    if runs.empty:
        st.info("No evaluation results yet. Run: `CALL _RUN_EVAL();` then `EXECUTE TASK EVAL_DAG_ROOT;`")
        return

    selected_run = st.selectbox("Select Eval Run", runs["RUN_NAME"].tolist())

    @st.cache_data(ttl=60)
    def get_eval_data(run_name):
        return session.sql(f"""
            SELECT * FROM EVAL_RESULTS
            WHERE RUN_NAME = '{run_name}'
            ORDER BY METRIC_NAME, EVAL_AGG_SCORE
        """).to_pandas()

    df = get_eval_data(selected_run)

    # Aggregate scores
    st.subheader("Score Summary")
    metrics = df.groupby("METRIC_NAME")["EVAL_AGG_SCORE"].agg(["mean", "min", "max", "count"]).reset_index()
    cols = st.columns(len(metrics))
    for i, (_, row) in enumerate(metrics.iterrows()):
        cols[i].metric(
            row["METRIC_NAME"],
            f"{row['mean']:.2f}",
            help=f"Min: {row['min']:.2f} | Max: {row['max']:.2f} | N: {int(row['count'])}"
        )

    st.divider()

    # Per-question scores — compact dataframe
    st.subheader("Per-Question Scores")

    # Pivot: one row per question, columns for each metric
    questions = df["INPUT"].unique()
    rows = []
    for question in questions:
        q_data = df[df["INPUT"] == question]
        scores = {row["METRIC_NAME"]: row["EVAL_AGG_SCORE"] for _, row in q_data.iterrows()}
        correctness = scores.get("answer_correctness", None)
        consistency = scores.get("logical_consistency", None)
        c_icon = "✅" if correctness is not None and correctness >= 0.7 else "⚠️" if correctness is not None and correctness >= 0.4 else "❌" if correctness is not None else "—"
        l_icon = "✅" if consistency is not None and consistency >= 0.7 else "⚠️" if consistency is not None and consistency >= 0.4 else "❌" if consistency is not None else "—"
        rows.append({
            "Question": question,
            "Correctness": f"{c_icon} {correctness:.2f}" if correctness is not None else "— not computed",
            "Consistency": f"{l_icon} {consistency:.2f}" if consistency is not None else "— not computed",
        })

    df_scores = pd.DataFrame(rows)
    st.dataframe(df_scores, use_container_width=True, hide_index=True, height=min(400, 35 * len(rows) + 38))
    st.caption("✅ ≥ 0.7 | ⚠️ ≥ 0.4 | ❌ < 0.4 | — not computed")

    # Detail table
    st.subheader("Detailed Results")
    for _, row in df.iterrows():
        score = row["EVAL_AGG_SCORE"]
        icon = "✅" if score >= 0.7 else "⚠️" if score >= 0.4 else "❌"
        with st.expander(f"{icon} [{row['METRIC_NAME']}] {row['INPUT']} — Score: {score:.2f}"):
            st.write(f"**Score:** {score:.2f}")
            explanation = row["EXPLANATION"] if "EXPLANATION" in row.index and str(row["EXPLANATION"]) != "None" else None
            if explanation:
                st.write(f"**Explanation:** {explanation}")
            duration = row["DURATION_MS"] if "DURATION_MS" in row.index and str(row["DURATION_MS"]) != "None" else None
            if duration:
                st.write(f"**Duration:** {duration}ms")

    # Latency benchmark
    st.divider()
    st.subheader("Search Latency Benchmark")
    @st.cache_data(ttl=300)
    def get_latency():
        return session.sql("""
            SELECT SCORING_PROFILE, QUERY_TYPE,
                   ROUND(AVG(LATENCY_MS), 0) AS avg_ms,
                   ROUND(MIN(LATENCY_MS), 0) AS min_ms,
                   ROUND(MAX(LATENCY_MS), 0) AS max_ms,
                   COUNT(*) AS queries
            FROM SEARCH_LATENCY_RESULTS
            GROUP BY 1, 2 ORDER BY 1, 2
        """).to_pandas()

    df_latency = get_latency()
    if not df_latency.empty:
        st.dataframe(df_latency, use_container_width=True)
    else:
        st.caption("No latency benchmark data. Run: `CALL SEARCH_LATENCY_BENCHMARK(...);`")


# =============================================================================
# Tab 3: SEC Filing Explorer
# =============================================================================

SEARCH_COLUMNS = [
    "CHUNK_TEXT", "CHUNK_ID", "ACCESSION_NO", "COMPANY_NAME",
    "TICKER", "FORM_TYPE", "SECTION_NAME", "FILED_AT",
    "PERIOD_OF_REPORT", "INDUSTRY_SECTOR"
]

MODELS = [
    "claude-4-sonnet",
    "claude-3-5-sonnet",
    "openai-gpt-4.1",
    "llama4-maverick",
    "llama3.3-70b",
    "mistral-large2",
]


def search_filings(query, filters=None, limit=10):
    request = {"query": query, "columns": SEARCH_COLUMNS, "limit": limit}
    if filters:
        request["filter"] = filters
    request_json = json.dumps(request)
    sql = f"""
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{SEARCH_SERVICE}',
                $${request_json}$$
            )
        )['results'] AS results
    """
    result = session.sql(sql).collect()
    if result:
        return json.loads(result[0]["RESULTS"])
    return []


def cortex_complete(model, prompt):
    escaped = prompt.replace("\\", "\\\\").replace("'", "\\'")
    sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped}') AS response"
    result = session.sql(sql).collect()
    if result:
        return result[0]["RESPONSE"]
    return ""


def render_filing_explorer():
    st.header("SEC Filing Explorer")

    st.caption("""
    A Retrieval Augmented Generation (RAG) interface over SEC filings. Ask a question and the app
    searches filing chunks via Cortex Search, retrieves relevant passages, then passes them to an LLM
    to synthesize a grounded answer. Settings (left panel — click > to open): LLM Model, result count,
    and filters (Ticker, Form Type, Industry Sector). For advanced multi-tool research, use the
    SEC Filing Agent in Snowsight: AI & ML > Cortex Agents > SEC_FILING_AGENT.
    """)

    st.divider()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Read filter values from sidebar (stored in session_state keys)
    model = st.session_state.get("explorer_model", MODELS[0])
    num_results = st.session_state.get("num_results", 10)
    ticker_filter = st.session_state.get("ticker_filter", "")
    form_type_filter = st.session_state.get("form_type_filter", "")
    industry_filter = st.session_state.get("industry_filter", "")

    # Chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # Search input
    if question := st.chat_input("Ask about SEC filings..."):
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        # Build filters
        filters = None
        filter_parts = []
        if ticker_filter:
            filter_parts.append({"@eq": {"TICKER": ticker_filter.upper()}})
        if form_type_filter:
            filter_parts.append({"@eq": {"FORM_TYPE": form_type_filter}})
        if industry_filter:
            filter_parts.append({"@eq": {"INDUSTRY_SECTOR": industry_filter}})
        if filter_parts:
            filters = {"@and": filter_parts} if len(filter_parts) > 1 else filter_parts[0]

        with st.chat_message("assistant"):
            with st.spinner("Searching SEC filings..."):
                try:
                    t0 = time.time()
                    results = search_filings(question, filters=filters, limit=num_results)
                    elapsed_ms = (time.time() - t0) * 1000
                except Exception as e:
                    st.error(f"Search error: {str(e)[:200]}")
                    results = []

            if not results:
                response = "No results found for your query."
                st.warning(response)
            else:
                st.caption(f"Retrieved {len(results)} chunks in {elapsed_ms:.0f}ms")

                # Generate response
                context = "\n\n".join([
                    f"[{r.get('COMPANY_NAME', '')} | {r.get('FORM_TYPE', '')} | {r.get('FILED_AT', '')}]\n{r.get('CHUNK_TEXT', '')[:1000]}"
                    for r in results[:5]
                ])
                prompt = f"""Answer the question using ONLY the SEC filing excerpts below. Be concise and cite companies/dates.

<context>
{context}
</context>

<question>{question}</question>"""

                response = cortex_complete(model, prompt)
                st.markdown(response)

                # Show sources
                with st.expander(f"Sources ({len(results)} chunks)"):
                    for i, r in enumerate(results):
                        st.markdown(f"**{i+1}.** {r.get('COMPANY_NAME', 'Unknown')} | {r.get('TICKER', '')} | {r.get('FORM_TYPE', '')} | {r.get('FILED_AT', '')} | {r.get('SECTION_NAME', '')}")
                        st.caption(r.get("CHUNK_TEXT", "")[:200] + "...")
                        st.divider()

            st.session_state.messages.append({"role": "assistant", "content": response})


# =============================================================================
# Tab 4: Cost Monitor
# =============================================================================

def render_cost_monitor():
    st.header("Cost Monitor")

    WAREHOUSE_PURPOSES = {
        "FILING_WH": "Steady-state queries, search, dashboard (Large)",
        "FILING_INGEST_WH": "Feed archive download & parsing (Snowpark-optimized Medium)",
        "FILING_BUILD_WH": "Bulk chunking & signal extraction (4XL)",
    }

    # Warehouse credits
    st.subheader("Warehouse Credit Usage (Last 30 Days)")
    @st.cache_data(ttl=300)
    def get_warehouse_credits():
        try:
            return session.sql("""
                SELECT WAREHOUSE_NAME,
                       DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
                       SUM(CREDITS_USED) AS credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
                  AND WAREHOUSE_NAME LIKE 'FILING%'
                GROUP BY 1, 2
                ORDER BY 2, 1
            """).to_pandas()
        except Exception:
            return None

    df_credits = get_warehouse_credits()
    if df_credits is not None and not df_credits.empty:
        # Summary metrics
        total = df_credits["CREDITS"].sum()
        by_wh = df_credits.groupby("WAREHOUSE_NAME")["CREDITS"].sum().reset_index()

        cols = st.columns(len(by_wh) + 1)
        cols[0].metric("Total Credits", f"{total:.1f}")
        for i, (_, row) in enumerate(by_wh.iterrows()):
            purpose = WAREHOUSE_PURPOSES.get(row["WAREHOUSE_NAME"], "")
            cols[i+1].metric(row["WAREHOUSE_NAME"], f"{row['CREDITS']:.1f}", help=purpose)

        # Warehouse purpose legend
        st.caption(" | ".join([f"**{k}**: {v}" for k, v in WAREHOUSE_PURPOSES.items()]))

        # Time series
        fig = px.line(df_credits, x="USAGE_DATE", y="CREDITS", color="WAREHOUSE_NAME",
                     color_discrete_sequence=["#29B5E8", "#11567F", "#90EE90"])
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No warehouse metering data available.")

    st.divider()

    # Cortex AI usage
    st.subheader("Cortex AI Usage")
    @st.cache_data(ttl=300)
    def get_ai_usage():
        try:
            return session.sql("""
                SELECT FUNCTION_NAME, MODEL_NAME,
                       ROUND(SUM(CREDITS), 4) AS total_credits,
                       COUNT(*) AS query_hours
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
                GROUP BY 1, 2
                ORDER BY 3 DESC
            """).to_pandas()
        except Exception:
            return None

    df_ai = get_ai_usage()
    if df_ai is not None and not df_ai.empty:
        fig = px.bar(df_ai, x="MODEL_NAME", y="TOTAL_CREDITS", color="FUNCTION_NAME",
                    text_auto=".2f",
                    color_discrete_sequence=["#29B5E8", "#11567F", "#90EE90"])
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_ai, use_container_width=True)
    else:
        st.info("No Cortex AI usage data available (or insufficient permissions).")

    st.divider()

    # Search service stats
    st.subheader("Cortex Search Service")
    @st.cache_data(ttl=60)
    def get_search_stats():
        try:
            svc_name = CONFIG.get("search_service", "SEC_FILING_SEARCH")
            rows = session.sql(f"SHOW CORTEX SEARCH SERVICES LIKE '{svc_name}' IN SCHEMA").collect()
            if rows:
                return rows[0]
        except Exception:
            pass
        return None

    stats = get_search_stats()
    if stats:
        try:
            s1, s2, s3 = st.columns(3)
            s1.metric("Indexed Rows", f"{stats['source_data_num_rows']:,}")
            s2.metric("Indexing State", stats["indexing_state"])
            s3.metric("Serving State", stats["serving_state"])
        except Exception:
            st.info("Could not read search service stats.")


# =============================================================================
# Tab 5: Pipeline Control
# =============================================================================

# All task names by DAG (for suspend/resume operations)
ALL_FEED_TASKS = [
    "T_FEED_INGEST_ROOT", "T_FEED_JAN", "T_FEED_FEB", "T_FEED_MAR",
    "T_FEED_APR", "T_FEED_MAY", "T_FEED_JUN", "T_FEED_JUL",
    "T_FEED_AUG", "T_FEED_SEP", "T_FEED_OCT", "T_FEED_NOV",
    "T_FEED_DEC", "T_FEED_VALIDATE", "T_FEED_INGEST_FINALIZER"
]
ALL_ENRICH_TASKS = [
    "T_ENRICH_ROOT", "T_ENRICH_TICKERS", "T_ENRICH_BACKFILL", "T_ENRICH_FINALIZER"
]
ALL_PROCESSING_TASKS = [
    "T_PROCESSING_ROOT", "T_CHUNK_10K", "T_CHUNK_10Q", "T_CHUNK_8K",
    "T_SIGNAL_10K", "T_SIGNAL_10Q", "T_SIGNAL_8K", "T_NORMALIZE_SIGNALS",
    "T_METRICS_EXTRACT", "T_GUIDANCE_EXTRACT", "T_PROPAGATE_INDUSTRY",
    "T_REFRESH_SEARCH", "T_WAIT_SEARCH_ACTIVE", "T_PROCESSING_FINALIZER"
]
ALL_SERVING_TASKS = [
    "T_SERVING_ROOT", "T_SERVING_FINALIZER"
]
ALL_DAG_ROOTS = ["T_FEED_INGEST_ROOT", "T_ENRICH_ROOT", "T_PROCESSING_ROOT", "T_SERVING_ROOT"]


def render_pipeline_control():
    st.header("Pipeline Control")

    # Check if pipeline is halted (any root suspended)
    @st.cache_data(ttl=10)
    def get_root_task_states():
        try:
            rows = session.sql("""
                SELECT "name", "state" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            """).collect()
        except Exception:
            rows = []
        # Fallback: query each root directly
        states = {}
        for root in ALL_DAG_ROOTS:
            try:
                r = session.sql(f"SHOW TASKS LIKE '{root}' IN SCHEMA").collect()
                if r:
                    states[root] = r[0]["state"]
            except Exception:
                states[root] = "unknown"
        return states

    root_states = get_root_task_states()
    is_halted = any(s == "suspended" for s in root_states.values())

    # Active DAG indicator
    @st.cache_data(ttl=15)
    def get_active_tasks():
        try:
            rows = session.sql("""
                SELECT NAME, STATE FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()),
                    RESULT_LIMIT => 100
                )) WHERE NAME LIKE 'T_%' AND STATE = 'EXECUTING'
            """).collect()
            return [r["NAME"] for r in rows]
        except Exception:
            return []

    active = get_active_tasks()
    if active:
        st.success(f"Pipeline active: {len(active)} task(s) executing — {', '.join(active[:5])}")
    elif is_halted:
        st.error("Pipeline is HALTED. All tasks are suspended.")
    else:
        st.info("Pipeline idle. No tasks currently executing.")

    st.divider()

    # -------------------------------------------------------------------------
    # Section 1: Trigger Ingestion Run
    # -------------------------------------------------------------------------
    st.subheader("Trigger Ingestion Run")

    if is_halted:
        st.warning("Pipeline is halted. Resume tasks before triggering a new run.")
    else:
        mode = st.radio("Ingestion mode", ["Full Year Range", "Custom Date Range"], horizontal=True)

        if mode == "Full Year Range":
            col1, col2 = st.columns(2)
            start_yr = col1.number_input("Start Year", min_value=2006, max_value=2026, value=2023)
            end_yr = col2.number_input("End Year", min_value=int(start_yr), max_value=2026, value=2026)
            years = int(end_yr) - int(start_yr) + 1
            feed_hrs = years * 2.5
            st.markdown(f"""
            **Estimated time per stage:**
            - Feed Ingestion: ~{feed_hrs:.0f} hrs ({years} year{'s' if years > 1 else ''} × ~2.5 hrs/year)
            - Enrichment: ~15 min (ticker lookup + industry backfill)
            - Processing: ~4 hrs (chunking + signals + metrics extraction)
            - **Total: ~{feed_hrs + 0.25 + 4:.0f} hrs end-to-end**
            """)
            st.info("After ingestion completes, the pipeline automatically chains: **Feed → Enrich → Processing → Search Refresh**")

            confirm = st.checkbox("I confirm I want to start this ingestion run", key="confirm_year")
            if st.button("Start Ingestion", disabled=not confirm, type="primary"):
                # Verify task DAG is deployed
                try:
                    chk = session.sql("SHOW TASKS LIKE 'T_FEED_INGEST_ROOT' IN SCHEMA").collect()
                    if not chk:
                        raise Exception("not found")
                except Exception:
                    st.error("Feed Ingestion DAG not deployed. Run `sql/02_ingestion/05_feed_ingestion_dag.sql` in Snowsight first.")
                    st.stop()
                session.sql(f"UPDATE _PIPELINE_CONFIG SET VALUE = '{int(start_yr)}' WHERE KEY = 'ingest_start_year'").collect()
                session.sql(f"UPDATE _PIPELINE_CONFIG SET VALUE = '{int(end_yr)}' WHERE KEY = 'ingest_end_year'").collect()
                session.sql(f"UPDATE _PIPELINE_CONFIG SET VALUE = '{int(start_yr)}' WHERE KEY = 'current_ingestion_year'").collect()
                session.sql("UPDATE _PIPELINE_CONFIG SET VALUE = '0' WHERE KEY = 'feed_retry_count'").collect()
                session.sql("EXECUTE TASK T_FEED_INGEST_ROOT").collect()
                st.success(f"Feed DAG triggered for {int(start_yr)}-{int(end_yr)}. Monitor progress in the Pipeline tab.")
                st.cache_data.clear()

        else:  # Custom Date Range
            col1, col2 = st.columns(2)
            from datetime import date
            start_dt = col1.date_input("Start Date", value=date(2023, 1, 1))
            end_dt = col2.date_input("End Date", value=date(2023, 1, 31))
            st.warning("Custom date range calls `LOAD_FEED_DATE_RANGE` directly (no DAG). After completion, you must manually trigger downstream DAGs.")
            st.markdown("""
            **After this completes, run in order:**
            1. `EXECUTE TASK T_ENRICH_ROOT` — ticker enrichment + industry backfill
            2. Processing DAG chains automatically from enrichment
            """)

            confirm = st.checkbox("I confirm I want to start this date range ingestion", key="confirm_date")
            if st.button("Start Date Range Ingestion", disabled=not confirm, type="primary"):
                ua = CONFIG.get("user_agent", "Snowflake SEC-Filing-Project admin@company.com")
                session.sql(f"CALL LOAD_FEED_DATE_RANGE('{start_dt}', '{end_dt}', '{ua}')").collect()
                st.success(f"Feed date range {start_dt} to {end_dt} triggered.")
                st.cache_data.clear()

    st.divider()

    # -------------------------------------------------------------------------
    # Section 2: Pipeline Configuration
    # -------------------------------------------------------------------------
    st.subheader("Pipeline Configuration")

    CONFIG_DESCRIPTIONS = {
        "ingest_start_year": "First year to ingest when Feed DAG is triggered",
        "ingest_end_year": "Last year to ingest (DAG loops through years sequentially)",
        "current_ingestion_year": "Year currently being processed (auto-advances after each year completes)",
        "feed_retry_count": "Current retry attempt counter (0 = fresh start, resets on year advance)",
        "warehouse": "Steady-state warehouse for queries and lightweight tasks (Large)",
        "warehouse_build": "Build warehouse for chunking + signal extraction (4XL, high cost)",
        "warehouse_ingest": "Ingest warehouse for feed archive downloads (Snowpark-optimized Medium)",
        "database": "Target database for all pipeline objects",
        "schema": "Target schema for all pipeline objects",
        "search_service": "Cortex Search service name (used by search refresh task)",
        "semantic_view": "Semantic view name (used by serving DAG)",
        "agent_name": "Cortex Agent name (used by serving DAG)",
        "email_integration": "Email notification integration for pipeline alerts",
        "email_recipient": "Email recipient for pipeline completion/failure notifications",
        "user_agent": "SEC EDGAR user-agent string (required by SEC fair-use policy)",
    }

    @st.cache_data(ttl=30)
    def get_pipeline_config():
        return session.sql("SELECT KEY, VALUE FROM _PIPELINE_CONFIG ORDER BY KEY").to_pandas()

    df_config = get_pipeline_config()

    EDITABLE_KEYS = {"ingest_start_year", "ingest_end_year", "current_ingestion_year",
                     "feed_retry_count", "warehouse", "warehouse_build", "warehouse_ingest"}

    with st.form("config_form"):
        updated = {}
        for _, row in df_config.iterrows():
            key = row["KEY"]
            val = row["VALUE"]
            desc = CONFIG_DESCRIPTIONS.get(key, "")
            if key in EDITABLE_KEYS:
                new_val = st.text_input(key, value=val, key=f"cfg_{key}", help=desc)
                st.caption(desc)
                if new_val != val:
                    updated[key] = new_val
            else:
                st.text_input(key, value=val, disabled=True, key=f"cfg_{key}", help=desc)

        st.divider()
        save_confirm = st.checkbox(
            "I understand that changing these values may affect running or future pipeline executions",
            key="save_confirm"
        )
        if st.form_submit_button("Save Configuration", disabled=not save_confirm):
            if updated:
                for k, v in updated.items():
                    session.sql(f"UPDATE _PIPELINE_CONFIG SET VALUE = '{v}' WHERE KEY = '{k}'").collect()
                st.success(f"Updated {len(updated)} config value(s): {', '.join(updated.keys())}")
                st.cache_data.clear()
            else:
                st.info("No changes to save.")

    st.divider()

    # -------------------------------------------------------------------------
    # Section 3: Emergency Stop & Recovery
    # -------------------------------------------------------------------------
    st.subheader("Emergency Stop & Recovery")

    if not is_halted:
        # Show HALT UI
        st.markdown("""
        <div style="border: 2px solid #FF4136; border-radius: 8px; padding: 16px; background-color: #FFF5F5;">
        <strong>Halting the pipeline</strong> will allow any currently-executing task to finish its current
        operation (typically under 5 minutes for enrichment/processing, up to 30 minutes for a single feed
        download). No further tasks will start after the current one completes.<br><br>
        The pipeline is <strong>idempotent</strong> — it will safely resume from where it left off.
        Use only when necessary, for example: incorrect configuration detected mid-run, unexpected cost
        accumulation, or data quality issues that need investigation before continuing.
        </div>
        """, unsafe_allow_html=True)

        st.write("")  # spacer
        halt_confirm = st.checkbox("I understand this will halt all active pipeline tasks", key="halt_confirm")
        if st.button("HALT ALL PIPELINE TASKS", disabled=not halt_confirm, type="primary"):
            # Suspend all tasks in all DAGs
            all_tasks = ALL_FEED_TASKS + ALL_ENRICH_TASKS + ALL_PROCESSING_TASKS + ALL_SERVING_TASKS
            errors = []
            for task in all_tasks:
                try:
                    session.sql(f"ALTER TASK {task} SUSPEND").collect()
                except Exception as e:
                    errors.append(f"{task}: {str(e)[:80]}")
            if errors:
                st.warning(f"Some tasks could not be suspended: {len(errors)} errors")
                with st.expander("Details"):
                    for e in errors:
                        st.text(e)
            else:
                st.success("All pipeline tasks suspended. Currently-executing tasks will finish their current operation.")
            st.cache_data.clear()
            st.rerun()

    else:
        # Show RECOVERY UI
        st.success("Pipeline is HALTED. All root tasks are suspended.")
        st.caption("Currently-executing tasks (if any) will finish their current operation but no new tasks will start.")

        st.write("")
        if st.button("RESUME ALL TASKS", type="primary"):
            # Resume all DAGs using SYSTEM$TASK_DEPENDENTS_ENABLE
            for root in ALL_DAG_ROOTS:
                try:
                    session.sql(f"SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('{root}')").collect()
                except Exception:
                    # Fallback: resume individually
                    pass
            st.success("All tasks resumed. Pipeline is ready to accept triggers.")
            st.session_state["pipeline_resumed"] = True
            st.cache_data.clear()
            st.rerun()

        # Restart buttons (enabled after resume)
        st.write("")
        st.subheader("Restart Pipeline")
        resumed = st.session_state.get("pipeline_resumed", False)

        if not resumed:
            st.caption("Resume tasks first, then restart buttons will become active.")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Restart Feed DAG**")
            st.caption("Re-attempts incomplete year ingestion from where it left off")
            if st.button("Restart Feed", disabled=not resumed, key="restart_feed"):
                try:
                    session.sql("EXECUTE TASK T_FEED_INGEST_ROOT").collect()
                    st.success("Feed DAG triggered.")
                except Exception:
                    st.error("Feed DAG not deployed. Run `sql/02_ingestion/05_feed_ingestion_dag.sql` first.")

            st.markdown("**Restart Enrich DAG**")
            st.caption("Re-runs ticker enrichment and industry backfill on un-enriched filings")
            if st.button("Restart Enrich", disabled=not resumed, key="restart_enrich"):
                try:
                    session.sql("EXECUTE TASK T_ENRICH_ROOT").collect()
                    st.success("Enrich DAG triggered.")
                except Exception:
                    st.error("Enrich DAG not deployed. Run `sql/04_enrichment/03_enrichment_task_dag.sql` first.")

        with col2:
            st.markdown("**Restart Processing DAG**")
            st.caption("Re-runs chunking, signals, metrics on un-processed filings")
            if st.button("Restart Processing", disabled=not resumed, key="restart_processing"):
                try:
                    session.sql("EXECUTE TASK T_PROCESSING_ROOT").collect()
                    st.success("Processing DAG triggered.")
                except Exception:
                    st.error("Processing DAG not deployed. Run `sql/03_processing/05_processing_task_dag.sql` first.")

            st.markdown("**Restart Full Pipeline**")
            st.caption("Starts from feed ingestion and chains through the full pipeline")
            if st.button("Restart Full Chain", disabled=not resumed, key="restart_full"):
                try:
                    session.sql("UPDATE _PIPELINE_CONFIG SET VALUE = '0' WHERE KEY = 'feed_retry_count'").collect()
                    session.sql("EXECUTE TASK T_FEED_INGEST_ROOT").collect()
                    st.success("Full pipeline triggered (Feed → Enrich → Processing).")
                except Exception:
                    st.error("Feed DAG not deployed. Run `sql/02_ingestion/05_feed_ingestion_dag.sql` first.")

    # -------------------------------------------------------------------------
    # Section 4: Pipeline Hygiene
    # -------------------------------------------------------------------------
    st.divider()
    st.subheader("Pipeline Hygiene")
    st.caption("Identify and process individual filings that are missing from Cortex Search or Analyst.")

    # Summary metrics
    @st.cache_data(ttl=60)
    def get_hygiene_metrics():
        return session.sql("""
            SELECT
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE PARSE_STATUS = 'PENDING') AS pending_chunk,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE SIGNAL_STATUS = 'PENDING') AS pending_signal,
                (SELECT COUNT(*) FROM FILING_INDEX fi WHERE NOT EXISTS (
                    SELECT 1 FROM FILING_CONTENT fc WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
                )) AS no_content
        """).collect()[0]

    try:
        hm = get_hygiene_metrics()
        pending_chunk = int(hm['PENDING_CHUNK'])
        pending_signal = int(hm['PENDING_SIGNAL'])
        no_content = int(hm['NO_CONTENT'])

        def _backlog_color(val):
            if val == 0: return "#2ECC40"
            elif val < 100: return "#FFDC00"
            else: return "#FF4136"

        col1, col2, col3 = st.columns(3)
        col1.markdown(
            f'<div style="text-align:center; padding:12px; border-radius:8px; border:2px solid {_backlog_color(pending_chunk)}">'
            f'<div style="font-size:2em; font-weight:bold; color:{_backlog_color(pending_chunk)}">{pending_chunk:,}</div>'
            f'<div style="color:#666">Pending Chunking</div></div>', unsafe_allow_html=True)
        col2.markdown(
            f'<div style="text-align:center; padding:12px; border-radius:8px; border:2px solid {_backlog_color(pending_signal)}">'
            f'<div style="font-size:2em; font-weight:bold; color:{_backlog_color(pending_signal)}">{pending_signal:,}</div>'
            f'<div style="color:#666">Pending Signals</div></div>', unsafe_allow_html=True)
        col3.markdown(
            f'<div style="text-align:center; padding:12px; border-radius:8px; border:2px solid {_backlog_color(no_content)}">'
            f'<div style="font-size:2em; font-weight:bold; color:{_backlog_color(no_content)}">{no_content:,}</div>'
            f'<div style="color:#666">No Content</div></div>', unsafe_allow_html=True)
    except Exception:
        pass

    # Filings missing processing
    st.markdown("**Filings Awaiting Processing**")

    @st.cache_data(ttl=60)
    def get_pending_filings():
        return session.sql("""
            SELECT fi.ACCESSION_NO, fi.COMPANY_NAME, fi.TICKER, fi.FORM_TYPE,
                   LEFT(fi.FILED_AT::VARCHAR, 10) AS FILED_DATE,
                   fc.PARSE_STATUS, fc.SIGNAL_STATUS
            FROM FILING_CONTENT fc
            JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO
            WHERE fc.PARSE_STATUS = 'PENDING' OR fc.SIGNAL_STATUS = 'PENDING'
            ORDER BY fi.FILED_AT DESC
            LIMIT 50
        """).to_pandas()

    df_pending = get_pending_filings()
    if not df_pending.empty:
        st.dataframe(df_pending, use_container_width=True, hide_index=True, height=250)

        # Multi-select filings to process
        filing_options = df_pending["ACCESSION_NO"].tolist()
        selected_filings = st.multiselect(
            "Select filing(s) to process:",
            filing_options,
            key="hygiene_select",
            format_func=lambda a: f"{a} — {df_pending[df_pending['ACCESSION_NO']==a].iloc[0]['COMPANY_NAME']} ({df_pending[df_pending['ACCESSION_NO']==a].iloc[0]['FORM_TYPE']})"
        )
        if selected_filings:
            st.caption(f"{len(selected_filings)} filing(s) selected")
            if st.button(f"Process {len(selected_filings)} Filing(s) Now", type="primary", key="hygiene_process"):
                try:
                    acc_list = ",".join(f"'{a}'" for a in selected_filings)
                    result = session.sql(f"CALL TRIGGER_PROCESS_FILINGS(ARRAY_CONSTRUCT({acc_list}))").collect()
                    if result:
                        st.success(result[0][0])
                except Exception as e:
                    st.error(f"Failed: {str(e)[:200]}")
    else:
        st.success("All filings are fully processed.")

    # Recent processing tasks
    st.markdown("**Recent Spot-Processing Tasks**")

    @st.cache_data(ttl=30)
    def get_recent_process_tasks():
        return session.sql("""
            SELECT NAME, STATE,
                   QUERY_START_TIME::VARCHAR AS STARTED,
                   DATEDIFF('second', QUERY_START_TIME, COALESCE(COMPLETED_TIME, CURRENT_TIMESTAMP())) AS ELAPSED_SEC
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
                RESULT_LIMIT => 10
            ))
            WHERE NAME LIKE 'PROCESS_FILING_%'
            ORDER BY SCHEDULED_TIME DESC
        """).to_pandas()

    try:
        df_tasks = get_recent_process_tasks()
        if not df_tasks.empty:
            st.dataframe(df_tasks, use_container_width=True, hide_index=True)
        else:
            st.caption("No recent spot-processing tasks.")
    except Exception:
        st.caption("Unable to query task history.")


# =============================================================================
# Tab 6: Data Quality
# =============================================================================

def render_data_quality():
    st.header("Data Quality")

    # Health Score Badge with transparent breakdown
    @st.cache_data(ttl=300)
    def compute_health_score():
        row = session.sql("""
            SELECT
                (SELECT COUNT(*) FROM FILING_INDEX) AS idx,
                (SELECT COUNT(*) FROM FILING_CONTENT) AS content,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE PARSE_STATUS = 'CHUNKED') AS chunked,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE SIGNAL_STATUS = 'EXTRACTED') AS extracted,
                (SELECT COUNT(TICKER) FROM FILING_INDEX) AS has_ticker,
                (SELECT COUNT(INDUSTRY_SECTOR) FROM FILING_INDEX) AS has_sector
        """).collect()[0]
        idx = max(int(row["IDX"]), 1)
        content = max(int(row["CONTENT"]), 1)
        components = [
            ("Content Coverage", int(row["CONTENT"]), idx),
            ("Chunking", int(row["CHUNKED"]), content),
            ("Signal Extraction", int(row["EXTRACTED"]), content),
            ("Ticker Enrichment", int(row["HAS_TICKER"]), idx),
            ("Industry Sector", int(row["HAS_SECTOR"]), idx),
        ]
        avg = sum(n / d for _, n, d in components) / len(components) * 100
        return round(avg, 0), components

    try:
        health, components = compute_health_score()
        if health >= 90:
            color = "#2ECC40"
        elif health >= 70:
            color = "#FFDC00"
        else:
            color = "#FF4136"
        st.markdown(
            f'<div style="display:inline-block; background:{color}; color:#fff; padding:6px 16px; '
            f'border-radius:20px; font-weight:bold; font-size:1.1em; margin-bottom:12px;">'
            f'Health Score: {health:.0f}/100</div>',
            unsafe_allow_html=True
        )
        with st.expander("Score Breakdown"):
            for name, numerator, denominator in components:
                pct = numerator / denominator if denominator > 0 else 0
                col_name, col_bar, col_val = st.columns([3, 5, 3])
                col_name.markdown(f"**{name}**")
                col_bar.progress(pct)
                col_val.caption(f"{pct*100:.0f}% ({numerator:,} / {denominator:,})")
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Section 1: Pipeline Data Flow
    # -------------------------------------------------------------------------
    st.subheader("Pipeline Data Flow")
    st.markdown("""
    **How data moves through the pipeline and what each step produces:**

    | Stage | Source | Output | Method |
    |-------|--------|--------|--------|
    | **Feed Ingestion** | SEC EDGAR daily tar.gz archives | FILING_INDEX (metadata) + FILING_CONTENT (raw text) | SEC headers provide: CIK, company name, form type, filed date, SIC code, period of report |
    | **Ticker Enrichment** | SEC EDGAR company tickers API | FILING_INDEX.TICKER | CIK → ticker lookup via `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=...` |
    | **Industry Classification** | SIC code (from feed headers) | FILING_INDEX.INDUSTRY_SECTOR | 4-digit SIC code mapped to 9 sectors via SIC_CODES reference table |
    | **Chunking** | FILING_CONTENT.CONTENT_TEXT | FILING_CHUNKS | Section-aware splitting: 1500 chars, 200 overlap. Identifies sections (MD&A, Risk Factors, Financial Statements, etc.) |
    | **Signal Extraction** | First 16K chars of filing content | FILING_SIGNALS | `AI_EXTRACT` (arctic-extract) → event_type, sentiment, summary, risk_flags, material_items |
    | **Key Metrics** | Keyword-targeted chunks (Financial Statements + MD&A) | FILING_SIGNALS.REVENUE, NET_INCOME, EPS | `AI_COMPLETE` (llama3.3-70b) with structured output on chunks containing revenue/earnings keywords |
    | **Forward Guidance** | Outlook-keyword chunks (MD&A + Business) | FILING_SIGNALS.FORWARD_GUIDANCE | `AI_COMPLETE` (llama3.3-70b) on chunks containing "expect", "outlook", "forecast" language |
    | **Normalization** | AI text output (e.g., "$2.1 billion") | REVENUE_NORMALIZED, EPS_NORMALIZED | Rule-based parsing: billion→×1000M, million→as-is, thousands→÷1000, with validation caps |
    | **Industry Propagation** | FILING_INDEX.INDUSTRY_SECTOR | FILING_CHUNKS + FILING_SIGNALS | Copies sector/ticker from index to downstream tables after processing |
    """)

    st.divider()

    # -------------------------------------------------------------------------
    # Section 2: Completeness Scorecard
    # -------------------------------------------------------------------------
    st.subheader("Completeness Scorecard")

    @st.cache_data(ttl=60)
    def get_quality_metrics():
        return session.sql("""
            SELECT
                (SELECT COUNT(*) FROM FILING_INDEX) AS index_total,
                (SELECT COUNT(*) FROM FILING_CONTENT) AS content_total,
                (SELECT COUNT(*) FROM FILING_CHUNKS) AS chunks_total,
                (SELECT COUNT(*) FROM FILING_SIGNALS) AS signals_total,
                (SELECT COUNT(CASE WHEN PARSE_STATUS = 'CHUNKED' THEN 1 END) FROM FILING_CONTENT) AS content_chunked,
                (SELECT COUNT(CASE WHEN SIGNAL_STATUS = 'EXTRACTED' THEN 1 END) FROM FILING_CONTENT) AS content_extracted,
                (SELECT COUNT(TICKER) FROM FILING_INDEX) AS idx_ticker,
                (SELECT COUNT(INDUSTRY_SECTOR) FROM FILING_INDEX) AS idx_industry,
                (SELECT COUNT(SIC_CODE) FROM FILING_INDEX) AS idx_sic,
                (SELECT COUNT(PERIOD_OF_REPORT) FROM FILING_INDEX) AS idx_period,
                (SELECT COUNT(REVENUE) FROM FILING_SIGNALS) AS sig_revenue,
                (SELECT COUNT(NET_INCOME) FROM FILING_SIGNALS) AS sig_net_income,
                (SELECT COUNT(EPS) FROM FILING_SIGNALS) AS sig_eps,
                (SELECT COUNT(REVENUE_NORMALIZED) FROM FILING_SIGNALS) AS sig_rev_norm,
                (SELECT COUNT(EPS_NORMALIZED) FROM FILING_SIGNALS) AS sig_eps_norm,
                (SELECT COUNT(FORWARD_GUIDANCE) FROM FILING_SIGNALS) AS sig_guidance,
                (SELECT COUNT(METRICS_EXTRACTED_AT) FROM FILING_SIGNALS) AS sig_metrics_processed,
                (SELECT COUNT(GUIDANCE_EXTRACTED_AT) FROM FILING_SIGNALS) AS sig_guidance_processed
        """).collect()[0]

    try:
        m = get_quality_metrics()
    except Exception:
        st.info("Unable to load quality metrics.")
        return

    idx_total = int(m["INDEX_TOTAL"]) or 1
    sig_total = int(m["SIGNALS_TOTAL"]) or 1

    scorecard = [
        ("Index → Content", int(m["CONTENT_TOTAL"]), idx_total, "Feed archive extraction"),
        ("Content → Chunked", int(m["CONTENT_CHUNKED"]), int(m["CONTENT_TOTAL"]) or 1, "Section-aware chunking"),
        ("Content → Signals", int(m["CONTENT_EXTRACTED"]), int(m["CONTENT_TOTAL"]) or 1, "AI_EXTRACT (arctic-extract)"),
        ("Ticker", int(m["IDX_TICKER"]), idx_total, "SEC EDGAR API by CIK"),
        ("Industry Sector", int(m["IDX_INDUSTRY"]), idx_total, "SIC code → sector mapping"),
        ("SIC Code", int(m["IDX_SIC"]), idx_total, "SEC feed archive headers"),
        ("Period of Report", int(m["IDX_PERIOD"]), idx_total, "SEC feed archive headers"),
        ("Revenue (raw)", int(m["SIG_REVENUE"]), sig_total, "AI_COMPLETE on financial chunks"),
        ("Revenue (normalized)", int(m["SIG_REV_NORM"]), sig_total, "Rule-based unit parsing"),
        ("EPS (raw)", int(m["SIG_EPS"]), sig_total, "AI_COMPLETE on financial chunks"),
        ("EPS (normalized)", int(m["SIG_EPS_NORM"]), sig_total, "Rule-based parsing + validation"),
        ("Forward Guidance", int(m["SIG_GUIDANCE"]), sig_total, "AI_COMPLETE on outlook chunks"),
    ]

    for field, count, total, source in scorecard:
        pct = count / total if total > 0 else 0
        col1, col2, col3 = st.columns([3, 1, 4])
        col1.markdown(f"**{field}**")
        col2.markdown(f"{pct*100:.0f}%")
        col3.progress(pct)
    st.caption(f"Based on {idx_total:,} filings in FILING_INDEX, {sig_total:,} signals in FILING_SIGNALS")

    st.divider()

    # -------------------------------------------------------------------------
    # Section 2b: Data Completeness
    # -------------------------------------------------------------------------
    st.subheader("Data Completeness")

    # --- Filings Over Time ---
    st.markdown("**Filings by Year and Form Type**")

    @st.cache_data(ttl=300)
    def get_filings_by_year_type():
        return session.sql("""
            SELECT YEAR(FILED_AT) AS YEAR, FORM_TYPE, COUNT(*) AS CNT
            FROM FILING_INDEX
            WHERE YEAR(FILED_AT) >= 2020
            GROUP BY 1, 2
            ORDER BY 1, 2
        """).to_pandas()

    df_year_type = get_filings_by_year_type()
    if not df_year_type.empty:
        fig = px.bar(df_year_type, x="YEAR", y="CNT", color="FORM_TYPE",
                     barmode="stack", text_auto=True,
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                         xaxis_title="Year", yaxis_title="Filings",
                         legend_title="Form Type")
        st.plotly_chart(fig, use_container_width=True)

    # Recent 90-day daily volume
    st.markdown("**Daily Ingestion Volume (Last 90 Days)**")

    @st.cache_data(ttl=300)
    def get_daily_volume():
        return session.sql("""
            SELECT LEFT(FILED_AT::VARCHAR, 10) AS FILED_DATE, COUNT(*) AS CNT
            FROM FILING_INDEX
            WHERE FILED_AT >= DATEADD('day', -90, CURRENT_TIMESTAMP())
            GROUP BY 1
            ORDER BY 1
        """).to_pandas()

    df_daily = get_daily_volume()
    if not df_daily.empty:
        fig = px.line(df_daily, x="FILED_DATE", y="CNT",
                     color_discrete_sequence=["#11567F"])
        fig.update_layout(height=250, margin=dict(l=20, r=20, t=10, b=20),
                         xaxis_title="Date", yaxis_title="Filings/Day")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- Processing Pipeline Status ---
    st.markdown("**Processing Pipeline Status**")

    @st.cache_data(ttl=300)
    def get_processing_status():
        return session.sql("""
            SELECT
                (SELECT COUNT(*) FROM FILING_INDEX) AS idx,
                (SELECT COUNT(*) FROM FILING_CONTENT) AS content,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE PARSE_STATUS = 'CHUNKED') AS chunked,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE SIGNAL_STATUS = 'EXTRACTED') AS extracted,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE PARSE_STATUS = 'PENDING') AS pending_chunk,
                (SELECT COUNT(*) FROM FILING_CONTENT WHERE SIGNAL_STATUS = 'PENDING') AS pending_signal,
                (SELECT COUNT(*) FROM FILING_INDEX fi WHERE NOT EXISTS (
                    SELECT 1 FROM FILING_CONTENT fc WHERE fc.ACCESSION_NO = fi.ACCESSION_NO
                )) AS idx_no_content
        """).collect()[0]

    try:
        ps = get_processing_status()
        with st.container(border=True):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Index", f"{int(ps['IDX']):,}")
            col2.metric("Content", f"{int(ps['CONTENT']):,}")
            col3.metric("Chunked", f"{int(ps['CHUNKED']):,}")
            col4.metric("Signals Extracted", f"{int(ps['EXTRACTED']):,}")

            # Pending backlog
            pending_chunk = int(ps["PENDING_CHUNK"])
            pending_signal = int(ps["PENDING_SIGNAL"])
            idx_no_content = int(ps["IDX_NO_CONTENT"])
            if pending_chunk > 0 or pending_signal > 0 or idx_no_content > 0:
                st.warning(f"**Backlog:** {idx_no_content:,} filings without content | "
                          f"{pending_chunk:,} awaiting chunking | {pending_signal:,} awaiting signal extraction")
    except Exception:
        pass

    st.divider()

    # --- Enrichment Coverage by Year ---
    st.markdown("**Enrichment Coverage by Year**")

    @st.cache_data(ttl=300)
    def get_enrichment_by_year():
        return session.sql("""
            SELECT YEAR(FILED_AT) AS YEAR,
                   COUNT(*) AS TOTAL,
                   ROUND(COUNT(TICKER) * 100.0 / COUNT(*), 1) AS PCT_TICKER,
                   ROUND(COUNT(INDUSTRY_SECTOR) * 100.0 / COUNT(*), 1) AS PCT_SECTOR,
                   ROUND(COUNT(PERIOD_OF_REPORT) * 100.0 / COUNT(*), 1) AS PCT_PERIOD,
                   ROUND(COUNT(SIC_CODE) * 100.0 / COUNT(*), 1) AS PCT_SIC
            FROM FILING_INDEX
            WHERE YEAR(FILED_AT) >= 2020
            GROUP BY 1 ORDER BY 1
        """).to_pandas()

    df_enrich = get_enrichment_by_year()
    if not df_enrich.empty:
        # Melt for grouped bar
        df_melt = df_enrich.melt(id_vars=["YEAR", "TOTAL"],
                                  value_vars=["PCT_TICKER", "PCT_SECTOR", "PCT_PERIOD", "PCT_SIC"],
                                  var_name="Metric", value_name="Percent")
        df_melt["Metric"] = df_melt["Metric"].map({
            "PCT_TICKER": "Ticker", "PCT_SECTOR": "Industry Sector",
            "PCT_PERIOD": "Period of Report", "PCT_SIC": "SIC Code"
        })
        fig = px.bar(df_melt, x="YEAR", y="Percent", color="Metric",
                     barmode="group", text_auto=".0f",
                     color_discrete_sequence=px.colors.qualitative.Set1)
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                         yaxis_range=[0, 105], yaxis_title="% Coverage")
        st.plotly_chart(fig, use_container_width=True)

    # --- Extraction Coverage by Year (10-K/10-Q only) ---
    st.markdown("**Extraction Coverage by Year** (10-K and 10-Q only)")

    @st.cache_data(ttl=300)
    def get_extraction_by_year():
        return session.sql("""
            SELECT YEAR(SIGNAL_DATE) AS YEAR,
                   COUNT(*) AS TOTAL,
                   ROUND(COUNT(REVENUE) * 100.0 / NULLIF(COUNT(*), 0), 1) AS PCT_REVENUE,
                   ROUND(COUNT(EPS) * 100.0 / NULLIF(COUNT(*), 0), 1) AS PCT_EPS,
                   ROUND(COUNT(FORWARD_GUIDANCE) * 100.0 / NULLIF(COUNT(*), 0), 1) AS PCT_GUIDANCE,
                   ROUND(COUNT(REVENUE_NORMALIZED) * 100.0 / NULLIF(COUNT(*), 0), 1) AS PCT_REV_NORM
            FROM FILING_SIGNALS
            WHERE FORM_TYPE IN ('10-K', '10-Q')
              AND YEAR(SIGNAL_DATE) >= 2020
            GROUP BY 1 ORDER BY 1
        """).to_pandas()

    df_extract = get_extraction_by_year()
    if not df_extract.empty:
        df_ext_melt = df_extract.melt(id_vars=["YEAR", "TOTAL"],
                                       value_vars=["PCT_REVENUE", "PCT_EPS", "PCT_GUIDANCE", "PCT_REV_NORM"],
                                       var_name="Metric", value_name="Percent")
        df_ext_melt["Metric"] = df_ext_melt["Metric"].map({
            "PCT_REVENUE": "Revenue (raw)", "PCT_EPS": "EPS (raw)",
            "PCT_GUIDANCE": "Forward Guidance", "PCT_REV_NORM": "Revenue (normalized)"
        })
        fig = px.bar(df_ext_melt, x="YEAR", y="Percent", color="Metric",
                     barmode="group", text_auto=".0f",
                     color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20),
                         yaxis_range=[0, 55], yaxis_title="% Coverage")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- Outlier Detection ---
    st.subheader("Outlier Detection")

    # Outlier 1: Low-volume ingestion days
    st.markdown("**Low-Volume Ingestion Days** (< 50% of 5-day rolling average)")

    @st.cache_data(ttl=300)
    def get_low_volume_days():
        return session.sql("""
            WITH daily AS (
                SELECT FEED_DATE, LOADED,
                       AVG(LOADED) OVER (ORDER BY FEED_DATE ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS avg_5day
                FROM _FEED_INGEST_LOG
                WHERE STATUS = 'DONE' AND LOADED > 0
            )
            SELECT FEED_DATE, LOADED, ROUND(avg_5day) AS AVG_NEARBY,
                   ROUND(LOADED * 100.0 / NULLIF(avg_5day, 0), 0) AS PCT_OF_AVG
            FROM daily
            WHERE LOADED < avg_5day * 0.5 AND avg_5day > 50
            ORDER BY PCT_OF_AVG ASC
            LIMIT 20
        """).to_pandas()

    df_low_vol = get_low_volume_days()
    if not df_low_vol.empty:
        st.dataframe(df_low_vol, use_container_width=True, hide_index=True)
        drill_date = st.selectbox("Investigate day:", df_low_vol["FEED_DATE"].tolist(), key="dq_lowvol_select")
        if drill_date:
            detail = session.sql(f"""
                SELECT FORM_TYPE, COUNT(*) AS cnt
                FROM FILING_INDEX WHERE LEFT(FILED_AT::VARCHAR, 10) = '{drill_date}'
                GROUP BY 1 ORDER BY 2 DESC
            """).to_pandas()
            st.caption(f"Filing breakdown for {drill_date}:")
            st.dataframe(detail, hide_index=True)
    else:
        st.success("No low-volume outlier days detected.")

    # Outlier 2: Revenue outliers
    st.markdown("**Revenue Outliers** (> $500B or negative)")

    @st.cache_data(ttl=300)
    def get_revenue_outliers():
        return session.sql("""
            SELECT ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE, SIGNAL_DATE::VARCHAR AS SIGNAL_DATE,
                   REVENUE_NORMALIZED AS REV_M, REVENUE AS REV_RAW
            FROM FILING_SIGNALS
            WHERE REVENUE_NORMALIZED IS NOT NULL
              AND (REVENUE_NORMALIZED > 500000 OR REVENUE_NORMALIZED < 0)
            ORDER BY ABS(REVENUE_NORMALIZED) DESC
            LIMIT 20
        """).to_pandas()

    df_rev_outliers = get_revenue_outliers()
    if not df_rev_outliers.empty:
        st.dataframe(df_rev_outliers, use_container_width=True, hide_index=True)
        if st.button("Show Raw Revenue Text", key="dq_drill_rev"):
            st.session_state["dq_drill"] = "revenue"
    else:
        st.success("No revenue outliers detected.")

    if st.session_state.get("dq_drill") == "revenue" and not df_rev_outliers.empty:
        acc = df_rev_outliers.iloc[0]["ACCESSION_NO"]
        detail = session.sql(f"""
            SELECT REVENUE, REVENUE_NORMALIZED, NET_INCOME, EPS, COMPANY_NAME
            FROM FILING_SIGNALS WHERE ACCESSION_NO = '{acc}'
        """).collect()
        if detail:
            st.json({k: str(v) for k, v in detail[0].asDict().items()})

    # Outlier 3: Chunk size outliers
    st.markdown("**Chunk Size Outliers**")

    @st.cache_data(ttl=300)
    def get_chunk_outliers():
        return session.sql("""
            SELECT
                COUNT(CASE WHEN TOKEN_COUNT < 10 THEN 1 END) AS tiny_chunks,
                COUNT(CASE WHEN TOKEN_COUNT > 450 THEN 1 END) AS oversized_chunks,
                COUNT(CASE WHEN SECTION_NAME IS NULL OR SECTION_NAME = '' THEN 1 END) AS no_section,
                COUNT(*) AS total_chunks
            FROM FILING_CHUNKS
        """).collect()[0]

    try:
        co = get_chunk_outliers()
        tiny = int(co["TINY_CHUNKS"])
        oversized = int(co["OVERSIZED_CHUNKS"])
        no_section = int(co["NO_SECTION"])
        total = int(co["TOTAL_CHUNKS"])
        col1, col2, col3 = st.columns(3)
        col1.metric("Tiny (< 10 tokens)", f"{tiny:,}", f"{tiny*100/total:.2f}%")
        col2.metric("Oversized (> 450 tokens)", f"{oversized:,}", f"{oversized*100/total:.2f}%")
        col3.metric("No Section Detected", f"{no_section:,}", f"{no_section*100/total:.2f}%")

        if st.button("Show Sample Tiny Chunks", key="dq_drill_chunks"):
            st.session_state["dq_drill"] = "chunks"
    except Exception:
        pass

    if st.session_state.get("dq_drill") == "chunks":
        samples = session.sql("""
            SELECT ACCESSION_NO, TICKER, SECTION_NAME, TOKEN_COUNT,
                   LEFT(CHUNK_TEXT, 200) AS PREVIEW
            FROM FILING_CHUNKS
            WHERE TOKEN_COUNT < 10
            LIMIT 10
        """).to_pandas()
        st.dataframe(samples, use_container_width=True, hide_index=True)

    # Outlier 4: Companies with excessive filings
    st.markdown("**Top Companies by Filing Count** (potential duplicates)")

    @st.cache_data(ttl=300)
    def get_top_filers():
        return session.sql("""
            SELECT CIK, MAX(COMPANY_NAME) AS COMPANY, MAX(TICKER) AS TICKER,
                   COUNT(*) AS FILINGS, COUNT(DISTINCT FORM_TYPE) AS FORM_TYPES
            FROM FILING_INDEX
            GROUP BY CIK
            ORDER BY FILINGS DESC
            LIMIT 15
        """).to_pandas()

    df_top = get_top_filers()
    if not df_top.empty:
        st.dataframe(df_top, use_container_width=True, hide_index=True)

    # Clear drill-down state
    if st.button("Clear Investigation", key="dq_clear_drill"):
        if "dq_drill" in st.session_state:
            del st.session_state["dq_drill"]
        st.rerun()

    st.divider()

    # -------------------------------------------------------------------------
    # Section 2c: Signal Extraction Method
    # -------------------------------------------------------------------------
    st.subheader("Signal Extraction Method")
    st.caption("Tracks which extraction strategy was used on each signal and migration progress.")

    @st.cache_data(ttl=300)
    def get_extraction_method_stats():
        return session.sql("""
            SELECT
                COALESCE(EXTRACTION_METHOD, 'raw_first_16k') AS METHOD,
                COUNT(*) AS CNT,
                COUNT(CASE WHEN RISK_FLAGS IS NOT NULL THEN 1 END) AS HAS_RISK_FLAGS,
                COUNT(CASE WHEN KEY_METRICS IS NOT NULL THEN 1 END) AS HAS_KEY_METRICS
            FROM FILING_SIGNALS
            GROUP BY 1
        """).to_pandas()

    try:
        df_method = get_extraction_method_stats()
        if not df_method.empty:
            col_pie, col_quality = st.columns(2)

            with col_pie:
                st.markdown("**Extraction Method Distribution**")
                colors = {"section_targeted": "#2ECC40", "raw_first_16k": "#AAAAAA"}
                fig = px.pie(df_method, values="CNT", names="METHOD",
                            color="METHOD", color_discrete_map=colors,
                            hole=0.4)
                fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig, use_container_width=True)

            with col_quality:
                st.markdown("**Quality: Fill Rate by Method**")
                df_method["PCT_RISK_FLAGS"] = (df_method["HAS_RISK_FLAGS"] * 100.0 / df_method["CNT"]).round(1)
                df_method["PCT_KEY_METRICS"] = (df_method["HAS_KEY_METRICS"] * 100.0 / df_method["CNT"]).round(1)
                df_qual = df_method.melt(id_vars=["METHOD"], value_vars=["PCT_RISK_FLAGS", "PCT_KEY_METRICS"],
                                         var_name="Field", value_name="Fill Rate %")
                df_qual["Field"] = df_qual["Field"].map({"PCT_RISK_FLAGS": "Risk Flags", "PCT_KEY_METRICS": "Key Metrics"})
                fig = px.bar(df_qual, x="METHOD", y="Fill Rate %", color="Field",
                            barmode="group", text_auto=".1f",
                            color_discrete_sequence=["#FF6B35", "#11567F"])
                fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10),
                                 yaxis_range=[0, 100])
                st.plotly_chart(fig, use_container_width=True)

        # Section budget visualization
        st.markdown("**Section Budget Allocation** (16K char excerpt for 10-K/10-Q)")
        budget_data = [
            {"Section": "Risk Factors", "Budget": 3000, "Purpose": "risk_flags"},
            {"Section": "MD&A", "Budget": 5000, "Purpose": "sentiment, summary, metrics"},
            {"Section": "Financial Statements", "Budget": 3000, "Purpose": "key_metrics"},
            {"Section": "Business", "Budget": 3000, "Purpose": "event_type, summary"},
            {"Section": "Market Risk", "Budget": 2000, "Purpose": "risk_flags"},
        ]
        df_budget = pd.DataFrame(budget_data)
        fig = px.bar(df_budget, x="Budget", y="Section", orientation="h",
                    text="Budget", color="Purpose",
                    color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=200, margin=dict(l=10, r=10, t=10, b=10),
                         xaxis_title="Characters", showlegend=True,
                         legend=dict(orientation="h", yanchor="bottom", y=-0.4))
        fig.update_traces(textposition="inside")
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        pass

    st.divider()

    # -------------------------------------------------------------------------
    # Section 3: Quality Indicators (charts)
    # -------------------------------------------------------------------------
    st.subheader("Quality Indicators")

    col_left, col_right = st.columns(2)

    with col_left:
        # Event type distribution
        st.markdown("**Event Type Distribution**")
        @st.cache_data(ttl=120)
        def get_event_types():
            return session.sql("""
                SELECT COALESCE(EVENT_TYPE_NORMALIZED, EVENT_TYPE) AS EVENT_TYPE, COUNT(*) AS cnt
                FROM FILING_SIGNALS
                GROUP BY 1 ORDER BY 2 DESC
                LIMIT 12
            """).to_pandas()

        df_events = get_event_types()
        if not df_events.empty:
            fig = px.pie(df_events, values="CNT", names="EVENT_TYPE",
                        color_discrete_sequence=px.colors.qualitative.Set3)
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        # Sentiment distribution
        st.markdown("**Sentiment Distribution**")
        @st.cache_data(ttl=120)
        def get_sentiments():
            return session.sql("""
                SELECT SENTIMENT, COUNT(*) AS cnt
                FROM FILING_SIGNALS
                WHERE SENTIMENT IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC
            """).to_pandas()

        df_sent = get_sentiments()
        if not df_sent.empty:
            colors = {"POSITIVE": "#2ECC40", "NEGATIVE": "#FF4136", "NEUTRAL": "#AAAAAA", "MIXED": "#FFDC00"}
            fig = px.bar(df_sent, x="SENTIMENT", y="CNT", text_auto=True,
                        color="SENTIMENT", color_discrete_map=colors)
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    col_left2, col_right2 = st.columns(2)

    with col_left2:
        # Chunk token distribution
        st.markdown("**Chunk Size Distribution (tokens)**")
        @st.cache_data(ttl=300)
        def get_chunk_dist():
            return session.sql("""
                SELECT WIDTH_BUCKET(TOKEN_COUNT, 0, 500, 10) * 50 AS bucket, COUNT(*) AS cnt
                FROM FILING_CHUNKS
                WHERE TOKEN_COUNT > 0
                GROUP BY 1 ORDER BY 1
            """).to_pandas()

        df_chunks = get_chunk_dist()
        if not df_chunks.empty:
            fig = px.bar(df_chunks, x="BUCKET", y="CNT", text_auto=True,
                        color_discrete_sequence=["#29B5E8"])
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20),
                             xaxis_title="Tokens", yaxis_title="Chunks")
            st.plotly_chart(fig, use_container_width=True)

    with col_right2:
        # Enrichment coverage by year
        st.markdown("**Ticker Coverage by Year**")
        @st.cache_data(ttl=120)
        def get_ticker_by_year():
            return session.sql("""
                SELECT YEAR(FILED_AT) AS year,
                       COUNT(*) AS total,
                       COUNT(TICKER) AS with_ticker,
                       ROUND(COUNT(TICKER) * 100.0 / COUNT(*), 1) AS pct
                FROM FILING_INDEX
                WHERE YEAR(FILED_AT) >= 2020
                GROUP BY 1 ORDER BY 1
            """).to_pandas()

        df_ticker = get_ticker_by_year()
        if not df_ticker.empty:
            fig = px.bar(df_ticker, x="YEAR", y="PCT", text_auto=".0f",
                        color_discrete_sequence=["#11567F"])
            fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20),
                             yaxis_title="% with Ticker", yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # -------------------------------------------------------------------------
    # Section 4: Extraction Methodology
    # -------------------------------------------------------------------------
    st.subheader("Extraction Methodology")
    st.markdown("""
    **Why some fields have less than 100% coverage:**

    - **Ticker** (~80%): Only publicly-traded companies have tickers. Private filers, SPACs pre-merger,
      and foreign private issuers often have no ticker in SEC's database. The enrichment process checks
      every CIK against SEC's company tickers API — CIKs without matches are marked as checked to avoid
      redundant API calls on subsequent runs.

    - **Key Metrics (Revenue/EPS)** (~18-24%): Metrics extraction is *keyword-targeted* — it only runs
      on filings whose chunks contain financial keywords (revenue, net income, EPS, diluted, etc.) in
      Financial Statements or MD&A sections. An 8-K announcing a leadership change won't have revenue
      data. This is by design to avoid hallucinating numbers from non-financial filings.

    - **Forward Guidance** (~12%): Guidance extraction targets 10-K and 10-Q filings with forward-looking
      language ("we expect", "outlook", "forecast", "guidance range"). 8-K filings and annual reports
      without outlook statements won't have guidance. This is intentional — extracting "guidance" from
      a filing that doesn't contain any would produce fabricated data.

    - **Normalization** (Revenue: ~97% of raw, EPS: ~97% of raw): Rule-based parsing converts AI text
      output to numeric values. ~3% fail parsing due to unusual formats (ranges like "$1.05 - $2.12",
      complex multi-value responses, or AI hallucinations). Values exceeding $750B revenue are capped
      to NULL as likely unit misclassifications.

    - **Sentiment/Event Type** (~100%): AI_EXTRACT always produces these fields with fallback defaults
      (NEUTRAL sentiment, form-type-based event type) when the model can't determine a value.
    """)


# =============================================================================
# Tab 7: Research Explorer
# =============================================================================

SECTION_OPTIONS = [
    "Risk Factors", "MD&A", "Business", "Financial Statements",
    "Legal Proceedings", "Market Risk", "Controls and Procedures",
    "Results of Operations", "Other Events", "Director/Officer Changes"
]

RESEARCH_MODELS = [
    "claude-4-sonnet",
    "claude-opus-4-7",
    "openai-gpt-4.1",
    "llama4-maverick",
    "llama3.3-70b",
    "mistral-large2",
    "gemini-3.1-pro",
]


def render_research_explorer():
    st.header("Research Explorer")
    st.caption("Filter-first, excerpt-retrieval workflow for technical business users. Prioritizes pre-filtering and raw document access over LLM synthesis.")

    # -------------------------------------------------------------------------
    # Inputs
    # -------------------------------------------------------------------------
    col_left, col_right = st.columns([2, 1])

    with col_left:
        tickers_input = st.text_area(
            "Company Tickers (comma-separated, e.g. AAPL, MSFT, TSLA)",
            height=68, key="re_tickers",
            help="Leave empty to skip company filter. Used with 'Run for Entire Sector' button below."
        )
        search_query = st.text_input(
            "Search Query (optional semantic refinement)",
            placeholder="e.g. supply chain disruption, AI-related risks",
            key="re_query"
        )

    with col_right:
        form_types = st.multiselect("Filing Type", ["10-K", "10-Q", "8-K"], default=["10-K"], key="re_form",
                                    help="Leave empty to search all form types.")
        sections = st.multiselect("Section Filter", SECTION_OPTIONS, default=["Risk Factors"], key="re_sections",
                                  help="Leave empty to search all sections.")
        from datetime import date, timedelta
        date_start = st.date_input("Start Date", value=date.today() - timedelta(days=730), key="re_start")
        date_end = st.date_input("End Date", value=date.today(), key="re_end")

    output_mode = st.radio("Output Mode", ["Excerpts", "Summarized", "Compared"], horizontal=True, key="re_mode",
                           help="Excerpts: raw text, no LLM. Summarized: per-company LLM summary. Compared: cross-company theme grid.")
    research_model = st.selectbox("LLM Model (for Summarized/Compared modes)", RESEARCH_MODELS, index=4, key="re_llm_model",
                                  help="Model used for synthesis in Summarized and Compared modes. Not used in Excerpts mode.")

    st.divider()

    # -------------------------------------------------------------------------
    # Execute search
    # -------------------------------------------------------------------------
    if st.button("Search", type="primary", key="re_search"):
        # Parse tickers
        tickers = [t.strip().upper() for t in tickers_input.split(",") if t.strip()] if tickers_input.strip() else []

        if not tickers:
            st.warning("Enter at least one ticker to search, or use 'Run for Entire Sector' below.")
            return

        all_results = []
        progress = st.progress(0)

        for i, ticker in enumerate(tickers):
            progress.progress((i + 1) / len(tickers))

            # Build section/form combinations (empty = search all)
            section_list = sections if sections else [None]
            form_list = form_types if form_types else [None]

            for section in section_list:
                for form_type in form_list:
                    # Build filter - only include non-None conditions
                    filter_parts = [{"@eq": {"TICKER": ticker}}]
                    if form_type:
                        filter_parts.append({"@eq": {"FORM_TYPE": form_type}})
                    if section:
                        filter_parts.append({"@eq": {"SECTION_NAME": section}})
                    filters = {"@and": filter_parts} if len(filter_parts) > 1 else filter_parts[0]

                    query = search_query if search_query else (section or "SEC filing disclosure")
                    try:
                        results = search_filings(query, filters=filters, limit=5)
                        for r in results:
                            filed = r.get("FILED_AT", "")
                            # Date range filter (filed_at is text 'YYYY-MM-DD')
                            if filed and filed >= str(date_start) and filed <= str(date_end):
                                accession = r.get("ACCESSION_NO", "")
                                all_results.append({
                                    "Ticker": ticker,
                                    "Company": r.get("COMPANY_NAME", ""),
                                    "Form Type": r.get("FORM_TYPE", ""),
                                    "Filed Date": filed,
                                    "Section": r.get("SECTION_NAME", ""),
                                    "Accession No": accession,
                                    "EDGAR Link": "",  # populated below via batch lookup
                                    "Excerpt": r.get("CHUNK_TEXT", "")
                                })
                    except Exception:
                        pass

        progress.empty()

        if not all_results:
            st.warning("No results found for the given filters.")
            return

        # Batch-lookup EDGAR filing URLs from FILING_INDEX
        unique_accessions = list(set(r["Accession No"] for r in all_results if r["Accession No"]))
        if unique_accessions:
            accession_list = ",".join(f"'{a}'" for a in unique_accessions)
            try:
                url_rows = session.sql(f"""
                    SELECT ACCESSION_NO, PRIMARY_DOC_URL
                    FROM FILING_INDEX
                    WHERE ACCESSION_NO IN ({accession_list})
                """).collect()
                url_map = {r["ACCESSION_NO"]: r["PRIMARY_DOC_URL"] for r in url_rows if r["PRIMARY_DOC_URL"]}
                for r in all_results:
                    r["EDGAR Link"] = url_map.get(r["Accession No"], "")
            except Exception:
                pass  # URLs remain empty if lookup fails

        st.success(f"Retrieved {len(all_results)} excerpts across {len(tickers)} ticker(s).")

        # Store in session state for display
        st.session_state["re_results"] = all_results
        st.session_state["re_output_mode"] = output_mode

    # -------------------------------------------------------------------------
    # Display results
    # -------------------------------------------------------------------------
    results = st.session_state.get("re_results", [])
    mode = st.session_state.get("re_output_mode", "Excerpts")

    if results:
        if mode == "Excerpts":
            # Group by company
            df_results = pd.DataFrame(results)

            # Show Evolution toggle
            years_span = len(df_results["Filed Date"].str[:4].unique()) if not df_results.empty else 0
            tickers_count = df_results["Ticker"].nunique()
            if years_span >= 2 and tickers_count > 1:
                show_evolution = st.toggle("Show Evolution (group by company + year)", key="re_evolution")
                if show_evolution:
                    df_results["Year"] = df_results["Filed Date"].str[:4]
                    df_results = df_results.sort_values(["Ticker", "Year", "Filed Date"], ascending=[True, False, False])

            st.dataframe(
                df_results[["Ticker", "Company", "Form Type", "Filed Date", "Section", "Accession No", "EDGAR Link", "Excerpt"]],
                use_container_width=True, hide_index=True, height=400,
                column_config={
                    "EDGAR Link": st.column_config.LinkColumn("Source", display_text="View on EDGAR"),
                }
            )

            # CSV download
            csv = df_results.to_csv(index=False)
            st.download_button("Download as CSV", csv, "research_explorer_results.csv", "text/csv", key="re_ticker_csv")

            # Expandable full text with source citations
            with st.expander("Full Excerpt Text (expandable)"):
                for i, r in enumerate(results[:50]):
                    accession = r.get("Accession No", "")
                    link_md = f" | [EDGAR]({r['EDGAR Link']})" if r.get("EDGAR Link") else ""
                    st.markdown(f"**{r['Ticker']}** | {r['Form Type']} | {r['Filed Date']} | {r['Section']} | `{accession}`{link_md}")
                    st.text(r["Excerpt"][:2000])
                    st.divider()

        elif mode == "Summarized":
            st.subheader("Per-Company Summaries")
            # Group by ticker (include source metadata)
            by_ticker = {}
            for r in results:
                key = r["Ticker"]
                if key not in by_ticker:
                    by_ticker[key] = {"company": r["Company"], "excerpts": [], "sources": []}
                by_ticker[key]["excerpts"].append(r["Excerpt"][:2000])
                by_ticker[key]["sources"].append({
                    "accession": r.get("Accession No", ""),
                    "filed": r.get("Filed Date", ""),
                    "form": r.get("Form Type", ""),
                    "section": r.get("Section", ""),
                    "link": r.get("EDGAR Link", ""),
                })

            for ticker, data in by_ticker.items():
                context = "\n\n".join(data["excerpts"][:5])
                prompt = f"Summarize the key points from these SEC filing excerpts for {data['company']} ({ticker}). Be concise (3-5 bullet points):\n\n{context}"
                with st.spinner(f"Summarizing {ticker}..."):
                    summary = cortex_complete(research_model, prompt)
                st.markdown(f"### {data['company']} ({ticker})")
                st.markdown(summary)
                with st.expander(f"Sources ({len(data['sources'])} filings)"):
                    for s in data["sources"]:
                        link_md = f" — [View on EDGAR]({s['link']})" if s["link"] else ""
                        st.caption(f"{s['form']} | Filed {s['filed']} | {s['section']} | `{s['accession']}`{link_md}")
                st.divider()

        elif mode == "Compared":
            st.subheader("Cross-Company Comparison")
            # Build comparison (include source metadata)
            by_ticker = {}
            for r in results:
                key = r["Ticker"]
                if key not in by_ticker:
                    by_ticker[key] = {"company": r["Company"], "excerpts": [], "sources": []}
                by_ticker[key]["excerpts"].append(r["Excerpt"][:1500])
                by_ticker[key]["sources"].append({
                    "accession": r.get("Accession No", ""),
                    "filed": r.get("Filed Date", ""),
                    "form": r.get("Form Type", ""),
                    "link": r.get("EDGAR Link", ""),
                })

            if len(by_ticker) < 2:
                st.warning(f"Comparison requires at least 2 companies with results. Only found {len(by_ticker)}: {', '.join(by_ticker.keys())}. Try broadening your date range or section filters.")
            else:
                comparison_parts = []
                for ticker, data in list(by_ticker.items())[:10]:
                    excerpt = data["excerpts"][0] if data["excerpts"] else ""
                    comparison_parts.append(f"[{data['company']} ({ticker})]:\n{excerpt}")

                context = "\n\n---\n\n".join(comparison_parts)
                sections_str = ", ".join(sections) if sections else "filings"
                companies_str = ", ".join(f"{data['company']} ({t})" for t, data in list(by_ticker.items())[:10])
                prompt = (
                    f"Compare the {sections_str} across these {len(comparison_parts)} companies: {companies_str}. "
                    f"Identify 3-5 common themes. Present as a markdown table with themes as rows and ALL {len(comparison_parts)} companies as columns. "
                    f"If a company does not address a theme, write 'Not discussed'.\n\nExcerpts:\n\n{context}"
                )

                with st.spinner("Generating comparison..."):
                    comparison = cortex_complete(research_model, prompt)
                st.markdown(comparison)

                # Source citations for comparison
                with st.expander("Source Filings Used in Comparison"):
                    for ticker, data in list(by_ticker.items())[:10]:
                        st.markdown(f"**{data['company']} ({ticker})**")
                        for s in data["sources"][:3]:
                            link_md = f" — [View on EDGAR]({s['link']})" if s["link"] else ""
                            st.caption(f"{s['form']} | Filed {s['filed']} | `{s['accession']}`{link_md}")

    # -------------------------------------------------------------------------
    # Run for Entire Sector (isolated fragment — no full-page rerun on interaction)
    # -------------------------------------------------------------------------
    @st.fragment
    def sector_analysis_fragment():
        st.divider()
        st.subheader("Run for Entire Sector")
        st.caption("Apply current filters to ALL companies in a sector. Runs asynchronously — results appear as companies are processed.")

        sector_options = ["Technology", "Life Sciences", "Finance", "Real Estate & Construction",
                          "Energy & Transportation", "Manufacturing", "Trade & Services", "Crypto Assets", "Other"]
        col_s1, col_s2 = st.columns([2, 1])
        with col_s1:
            selected_sector = st.selectbox("Target Sector", sector_options, key="re_sector")
        with col_s2:
            company_limit = st.number_input("Company Limit (0 = all)", min_value=0, value=0, key="re_limit",
                                            help="Limit how many companies to process. 0 means all companies in the sector.")

        # Show company count for selected sector (pre-fetched, no per-switch SQL)
        @st.cache_data(ttl=300)
        def get_all_sector_counts():
            return session.sql("""
                SELECT INDUSTRY_SECTOR, COUNT(DISTINCT TICKER) AS cnt
                FROM FILING_INDEX WHERE TICKER IS NOT NULL
                GROUP BY 1
            """).to_pandas()

        try:
            sector_counts = get_all_sector_counts()
            match = sector_counts[sector_counts["INDUSTRY_SECTOR"] == selected_sector]
            cnt = int(match["CNT"].iloc[0]) if not match.empty else 0
            st.caption(f"{cnt} companies with tickers in {selected_sector} sector")
        except Exception:
            cnt = 0

        sector_mode_map = {"Excerpts": "excerpts", "Summarized": "summarized", "Compared": "compared"}
        sector_output = sector_mode_map.get(output_mode, "excerpts")
        section_str = sections[0] if sections else None
        form_str = form_types[0] if form_types else None
        limit_val = company_limit if company_limit > 0 else None

        if st.button("Run for Entire Sector", key="re_sector_btn"):
            @st.dialog("Run for Entire Sector")
            def show_sector_confirm():
                st.write(f"**Sector:** {selected_sector} ({cnt} companies)")
                st.write(f"**Model:** {research_model} | **Mode:** {sector_output}")

                # Run name input
                run_name = st.text_input("Run Name", value=f"{selected_sector} - {section_str or 'All Sections'} ({sector_output})",
                                         key="dialog_run_name", max_chars=200,
                                         help="Give this run a descriptive name for easy identification later.")

                # Build params (cheap, no SQL)
                query_param = f"'{search_query}'" if search_query else "NULL"
                section_param = f"'{section_str}'" if section_str else "NULL"
                form_param = f"'{form_str}'" if form_str else "NULL"
                limit_param_str = str(limit_val) if limit_val else "NULL"
                total_companies = limit_val if limit_val else cnt
                run_name_param = f"'{run_name.replace(chr(39), chr(39)+chr(39))}'" if run_name.strip() else "NULL"

                # Run timing probe with spinner — uses SEARCH_PREVIEW only (no write to EXPLORER_RESULTS)
                with st.spinner("Estimating runtime (probing 1 search call)..."):
                    t0 = time.time()
                    try:
                        import json as _json
                        probe_request = _json.dumps({
                            "query": section_str or "SEC filing disclosure",
                            "columns": ["CHUNK_TEXT"],
                            "limit": 5,
                            "filter": {"@eq": {"INDUSTRY_SECTOR": selected_sector}}
                        })
                        session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                                '{SEARCH_SERVICE}',
                                $${probe_request}$$
                            )
                        """).collect()
                        probe_sec = time.time() - t0
                    except Exception as e:
                        st.error(f"Probe failed: {str(e)[:200]}")
                        return

                # Show estimate
                est_seconds = probe_sec * total_companies
                if est_seconds < 120:
                    est_str = f"{est_seconds:.0f} seconds"
                else:
                    est_str = f"{est_seconds / 60:.0f} minutes"

                st.success(f"**Estimated runtime:** ~{est_str} for {total_companies} companies ({probe_sec:.1f}s per company)")

                # Confirm button
                if st.button("Confirm & Execute", type="primary", key="dialog_confirm",
                             disabled=st.session_state.get("re_executing", False)):
                    st.session_state["re_executing"] = True
                    with st.spinner("Triggering async analysis..."):
                        try:
                            date_start_param = f"'{date_start}'" if date_start else "NULL"
                            date_end_param = f"'{date_end}'" if date_end else "NULL"
                            result = session.sql(f"""
                                CALL TRIGGER_SECTOR_ANALYSIS(
                                    '{selected_sector}', {query_param}, {section_param}, {form_param}, '{sector_output}', {limit_param_str}, '{research_model}', {run_name_param}, {date_start_param}, {date_end_param}
                                )
                            """).collect()
                            if result:
                                st.success(result[0][0])
                        except Exception as e:
                            st.error(f"Failed: {str(e)[:200]}")
                    st.session_state["re_executing"] = False
                    time.sleep(2)
                    st.rerun()

            show_sector_confirm()

        # View full-sector runs
        with st.expander("View Full-Sector Runs"):
            if st.button("Refresh", key="re_refresh"):
                st.cache_data.clear()

            # Cached: running tasks
            @st.cache_data(ttl=30)
            def get_running_explorer_tasks():
                return session.sql("""
                    SELECT NAME, STATE FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                        SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
                        RESULT_LIMIT => 20
                    )) WHERE NAME LIKE 'EXPLORER_RUN_%' AND STATE = 'EXECUTING'
                """).collect()

            try:
                running = get_running_explorer_tasks()
                if running:
                    st.info(f"{len(running)} task(s) running: {', '.join(r['NAME'] for r in running)}")
            except Exception:
                pass

            # Cached: previous runs list
            @st.cache_data(ttl=60)
            def get_explorer_runs():
                return session.sql("""
                    SELECT RUN_ID, MAX(RUN_NAME) AS RUN_NAME, MAX(SECTOR) AS SECTOR, MAX(QUERY_TYPE) AS QUERY_TYPE,
                           COUNT(*) AS RESULT_COUNT,
                           CONVERT_TIMEZONE('America/New_York', MAX(RUN_TIMESTAMP)) AS LAST_RUN_ET
                    FROM EXPLORER_RESULTS
                    GROUP BY RUN_ID
                    ORDER BY MAX(RUN_TIMESTAMP) DESC
                    LIMIT 20
                """).to_pandas()

            try:
                runs = get_explorer_runs()
                if not runs.empty:
                    # Build display labels: "Run Name (timestamp)" or "RUN_ID (timestamp)" if no name
                    run_labels = []
                    for _, row in runs.iterrows():
                        name = row["RUN_NAME"] if row["RUN_NAME"] else row["RUN_ID"]
                        ts = row["LAST_RUN_ET"]
                        ts_str = ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, 'strftime') else str(ts)[:16]
                        run_labels.append(f"{name} ({ts_str} ET)")
                    
                    selected_idx = st.selectbox("Select a run:", range(len(run_labels)), format_func=lambda i: run_labels[i], key="re_prev_run")
                    selected_run = runs.iloc[selected_idx]["RUN_ID"]

                    # Gate results behind a button click
                    if st.button("View Results", key="re_view_results"):
                        st.session_state["re_loaded_run"] = selected_run

                    if st.session_state.get("re_loaded_run") == selected_run:
                        @st.cache_data(ttl=60)
                        def get_run_results(run_id):
                            return session.sql(f"""
                                SELECT QUERY_TYPE, QUERY_TEXT, AGENT_RESPONSE, QUERY_PARAMS, RUN_TIMESTAMP
                                FROM EXPLORER_RESULTS
                                WHERE RUN_ID = '{run_id}'
                                ORDER BY RUN_TIMESTAMP
                            """).to_pandas()

                        run_df = get_run_results(selected_run)

                        # Show search parameters header (with model)
                        if not run_df.empty and run_df.iloc[0]["QUERY_PARAMS"]:
                            try:
                                params = json.loads(run_df.iloc[0]["QUERY_PARAMS"])
                                param_parts = [
                                    f"Sector: `{params.get('sector', 'N/A')}`",
                                    f"Query: `{params.get('query', 'None')}`",
                                    f"Section: `{params.get('section', 'N/A')}`",
                                    f"Form: `{params.get('form_type', 'N/A')}`",
                                    f"Mode: `{params.get('output_mode', 'N/A')}`",
                                    f"Model: `{params.get('model', 'N/A')}`",
                                    f"Limit: `{params.get('limit') or 'all'}`",
                                ]
                                # Date range (if present)
                                ds = params.get('date_start')
                                de = params.get('date_end')
                                if ds or de:
                                    param_parts.append(f"Dates: `{ds or '...'} to {de or '...'}`")
                                # Stats (if present)
                                cs = params.get('companies_searched')
                                cw = params.get('companies_with_results')
                                if cs is not None:
                                    param_parts.append(f"Companies: `{cw}/{cs} with results`")
                                st.markdown(f"**Search Parameters:** {' | '.join(param_parts)}")
                            except Exception:
                                pass

                        # --- Tabbed visualization ---
                        run_tab1, run_tab2, run_tab3 = st.tabs(["Summary", "Details", "Raw Data"])

                        with run_tab1:
                            st.caption(f"{len(run_df)} result(s) in this run")
                            # Results breakdown by query_type
                            if not run_df.empty:
                                type_counts = run_df["QUERY_TYPE"].value_counts()
                                for qtype, count in type_counts.items():
                                    st.write(f"- **{qtype}**: {count} results")

                        with run_tab2:
                            for _, row in run_df.iterrows():
                                qtype = row["QUERY_TYPE"]
                                if qtype == "custom_comparison":
                                    st.markdown("**Cross-Company Comparison:**")
                                    st.markdown(row["AGENT_RESPONSE"])
                                elif qtype == "custom_summary":
                                    st.markdown(f"**{row['QUERY_TEXT']}:**")
                                    st.markdown(row["AGENT_RESPONSE"])
                                elif qtype == "custom_excerpt":
                                    # QUERY_TEXT format: "TICKER | FILED_AT | SECTION | ACCESSION_NO | URL"
                                    parts = [p.strip() for p in (row["QUERY_TEXT"] or "").split("|")]
                                    meta = " | ".join(parts[:3])
                                    url = parts[4] if len(parts) > 4 and parts[4].startswith("http") else ""
                                    link_md = f" — [View on EDGAR]({url})" if url else ""
                                    st.caption(f"{meta}{link_md}")
                                    st.text(row["AGENT_RESPONSE"][:1000] if row["AGENT_RESPONSE"] else "")
                                else:
                                    st.markdown(f"**{row['QUERY_TYPE']}:** {row['QUERY_TEXT']}")
                                    st.markdown(row["AGENT_RESPONSE"] or "")
                                st.divider()

                        with run_tab3:
                            st.dataframe(run_df, use_container_width=True, hide_index=True, height=400)
                            # SQL query for reproducibility
                            st.code(f"SELECT * FROM SEC_FILINGS.FILING_DATA.EXPLORER_RESULTS WHERE RUN_ID = '{selected_run}' ORDER BY RUN_TIMESTAMP;", language="sql")
                            # CSV download
                            csv_data = run_df.to_csv(index=False)
                            st.download_button(
                                "Download Results as CSV",
                                csv_data,
                                f"explorer_run_{selected_run}.csv",
                                "text/csv",
                                key="re_download_csv"
                            )
                else:
                    st.info("No previous runs yet.")
            except Exception as e:
                err = str(e)
                if "does not exist" in err:
                    st.info("EXPLORER_RESULTS table not found. Deploy `sql/07_explorer/01_batch_sp.sql` first.")
                else:
                    st.error(f"Error loading previous runs: {err[:200]}")

    sector_analysis_fragment()


# =============================================================================
# Main App
# =============================================================================

def main():
    st.title("SEC Filing Intelligence")

    # Custom CSS for modern look
    st.markdown("""
    <style>
    /* Snowflake blue accent on headers */
    h2 { color: #11567F; border-bottom: 2px solid #29B5E8; padding-bottom: 4px; }
    h3 { color: #1a1a2e; }

    /* Metric styling */
    [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 700; }
    [data-testid="stMetricLabel"] { font-size: 0.85rem; color: #666; }

    /* Dataframe header styling */
    [data-testid="stDataFrame"] th { background-color: #11567F !important; color: white !important; }

    /* Subtle shadows on containers */
    [data-testid="stVerticalBlock"] > div[data-testid="stExpander"] {
        border-radius: 8px;
        border: 1px solid #e0e0e0;
    }

    /* Tab styling */
    button[data-baseweb="tab"] { font-weight: 600; }

    /* Progress bars */
    [data-testid="stProgress"] > div > div { border-radius: 4px; }
    </style>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Pipeline", "🔬 Data Quality", "🔍 Filing Explorer (RAG)", "🔎 Research Explorer", "💰 Cost Monitor", "⚙️ Pipeline Control", "📈 Agent Eval"
    ])

    with tab1:
        render_pipeline_dashboard()
    with tab2:
        render_data_quality()
    with tab3:
        render_filing_explorer()
    with tab4:
        render_research_explorer()
    with tab5:
        render_cost_monitor()
    with tab6:
        render_pipeline_control()
    with tab7:
        render_eval_results()

    # Sidebar only shows Filing Explorer controls (rendered after tabs so it's always present)
    # but labeled clearly as belonging to the Explorer tab
    with st.sidebar:
        st.header("🔍 Filing Explorer Settings")
        st.caption("These controls apply to the Filing Explorer tab.")
        st.selectbox("LLM Model", MODELS, key="explorer_model")
        st.number_input("Results", min_value=1, max_value=20, value=10, key="num_results")

        st.subheader("Search Filters")
        st.text_input("Ticker (e.g. AAPL)", key="ticker_filter")
        st.selectbox("Form Type", ["", "10-K", "10-Q", "8-K"], key="form_type_filter")
        st.selectbox("Industry Sector", [
            "", "Technology", "Life Sciences", "Finance",
            "Real Estate & Construction", "Energy & Transportation",
            "Manufacturing", "Trade & Services", "Crypto Assets", "Other"
        ], key="industry_filter")

        if st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()


if __name__ == "__main__":
    main()
