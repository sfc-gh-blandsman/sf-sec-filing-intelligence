-- =============================================================================
-- 06: Modular Filing Processing (On-Demand, Batch-Capable)
-- =============================================================================
-- Three-layer architecture for processing 1-N filings on demand:
--
--   PREPARE_FILINGS(accessions[])  — Python SP (has EAI)
--     Downloads content if missing, enriches ticker, fixes SIC/industry.
--
--   PROCESS_FILINGS(accessions[])  — SQL SP
--     Chunks (CHUNK_FILING UDF), signal-extracts (AI_EXTRACT), refreshes search.
--     Same logic as DAG tasks but scoped to specific accessions.
--
--   TRIGGER_PROCESS_FILINGS(accessions[])  — SQL SP (async wrapper)
--     Creates a dynamic task that calls PREPARE then PROCESS, emails on done.
--     Returns immediately with task name.
--
--   TRIGGER_PROCESS_FILINGS_FULL(accessions[] | NULL)  — SQL SP (full pipeline)
--     Creates a dynamic task DAG: prepare → process → metrics → guidance →
--     normalize → propagate → search → serving → email. Self-cleans on completion.
--     Pass NULL to process all PENDING filings.
--
-- Backward-compatible wrappers:
--   PROCESS_SINGLE_FILING(accession)  — calls PROCESS_FILINGS([accession])
--   TRIGGER_PROCESS_FILING(accession) — calls TRIGGER_PROCESS_FILINGS([accession])
--
-- Usage:
--   -- Single filing (async with email):
--   CALL TRIGGER_PROCESS_FILINGS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--
--   -- Multiple filings (async with email):
--   CALL TRIGGER_PROCESS_FILINGS(ARRAY_CONSTRUCT('acc1', 'acc2', 'acc3'));
--
--   -- Full pipeline for all pending (async, self-cleaning DAG):
--   CALL TRIGGER_PROCESS_FILINGS_FULL(NULL);
--
--   -- Synchronous (blocks until done):
--   CALL PREPARE_FILINGS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--   CALL PROCESS_FILINGS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--
-- Dependencies:
--   - SEC_EDGAR_EAI (for content download + ticker enrichment)
--   - CLEAN_TEXT UDF, CHUNK_FILING UDF
--   - SIC_CODES reference table
--   - Cortex Search service
--   - EXTRACT_KEY_METRICS_BATCH, EXTRACT_FORWARD_GUIDANCE_BATCH SPs
--   - T_SERVING_ROOT task (optional, for serving layer update)
--   - _CFG() function, email integration
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse_build);


-- =============================================================================
-- SP: PREPARE_FILINGS (Python — download content + enrich ticker + fix industry)
-- =============================================================================

CREATE OR REPLACE PROCEDURE PREPARE_FILINGS(P_ACCESSIONS ARRAY)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
HANDLER = 'prepare_filings'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import re
import time

def prepare_filings(session, p_accessions):
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    if not p_accessions:
        return "No accessions provided."

    accessions = list(p_accessions)
    results = {"downloaded": 0, "enriched": 0, "industry_fixed": 0, "errors": []}

    for acc in accessions:
        # --- Step 1: Download content if missing ---
        has_content = session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {fqn}.FILING_CONTENT WHERE ACCESSION_NO = '{acc}'
        """).collect()[0]["CNT"]

        if has_content == 0:
            # Get filing URL from index
            idx_row = session.sql(f"""
                SELECT CIK, PRIMARY_DOC_URL FROM {fqn}.FILING_INDEX WHERE ACCESSION_NO = '{acc}'
            """).collect()
            if not idx_row:
                results["errors"].append(f"{acc}: not in FILING_INDEX")
                continue

            url = idx_row[0]["PRIMARY_DOC_URL"]
            if not url:
                cik = idx_row[0]["CIK"].lstrip('0') or '0'
                acc_nodashes = acc.replace('-', '')
                url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodashes}/{acc}.txt"

            headers = {'User-Agent': 'Snowflake SEC-Filing-Project admin@company.com'}
            time.sleep(0.5)
            try:
                resp = requests.get(url, headers=headers, timeout=60)
                if resp.status_code != 200:
                    results["errors"].append(f"{acc}: HTTP {resp.status_code}")
                    continue
                raw_text = resp.text
                if len(raw_text) < 100:
                    results["errors"].append(f"{acc}: empty content")
                    continue

                # Extract document content
                text_match = re.search(r'<TEXT>(.*?)</TEXT>', raw_text, re.DOTALL)
                doc_content = text_match.group(1) if text_match else raw_text

                import pandas as pd
                df = pd.DataFrame([{
                    'ACCESSION_NO': acc,
                    'CONTENT_TEXT': doc_content[:16_000_000],
                    'STAGE_FILE_PATH': None,
                    'FILE_SIZE_BYTES': len(raw_text),
                    'FILE_FORMAT': 'SPOT_PROCESS',
                    'PARSE_STATUS': 'PENDING',
                    'PARSE_ERROR': None,
                    'SIGNAL_STATUS': 'PENDING'
                }])
                tmp = f"{fqn}._SPOT_CONTENT_TMP"
                session.create_dataframe(df).write.mode("overwrite").save_as_table(tmp, table_type="temporary")
                session.sql(f"""
                    INSERT INTO {fqn}.FILING_CONTENT
                        (ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH, FILE_SIZE_BYTES, FILE_FORMAT, PARSE_STATUS, PARSE_ERROR, SIGNAL_STATUS)
                    SELECT ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH, FILE_SIZE_BYTES::NUMBER, FILE_FORMAT, PARSE_STATUS, PARSE_ERROR, SIGNAL_STATUS
                    FROM {tmp}
                    WHERE NOT EXISTS (SELECT 1 FROM {fqn}.FILING_CONTENT WHERE ACCESSION_NO = '{acc}')
                """).collect()
                session.sql(f"DROP TABLE IF EXISTS {tmp}").collect()
                results["downloaded"] += 1
            except Exception as e:
                results["errors"].append(f"{acc}: {str(e)[:80]}")
                continue

        # --- Step 2: Enrich ticker if missing ---
        ticker_row = session.sql(f"""
            SELECT TICKER, TICKER_CHECKED_AT, CIK FROM {fqn}.FILING_INDEX WHERE ACCESSION_NO = '{acc}'
        """).collect()
        if ticker_row and ticker_row[0]["TICKER"] is None and ticker_row[0]["TICKER_CHECKED_AT"] is None:
            cik = ticker_row[0]["CIK"]
            headers = {'User-Agent': 'Snowflake SEC-Filing-Project admin@company.com'}
            time.sleep(0.5)
            try:
                ticker_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=1&search_text=&output=atom"
                resp = requests.get(ticker_url, headers=headers, timeout=15)
                ticker = None
                if resp.status_code == 200:
                    m = re.search(r'<ticker-symbol>([^<]+)</ticker-symbol>', resp.text)
                    if m:
                        ticker = m.group(1).strip().upper()
                if ticker:
                    session.sql(f"""
                        UPDATE {fqn}.FILING_INDEX SET TICKER = '{ticker}', TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
                        WHERE ACCESSION_NO = '{acc}'
                    """).collect()
                    results["enriched"] += 1
                else:
                    session.sql(f"""
                        UPDATE {fqn}.FILING_INDEX SET TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
                        WHERE ACCESSION_NO = '{acc}'
                    """).collect()
            except Exception:
                pass

        # --- Step 3: Fix SIC/Industry if missing ---
        idx_info = session.sql(f"""
            SELECT SIC_CODE, INDUSTRY_SECTOR FROM {fqn}.FILING_INDEX WHERE ACCESSION_NO = '{acc}'
        """).collect()
        if idx_info and (idx_info[0]["INDUSTRY_SECTOR"] is None or idx_info[0]["INDUSTRY_SECTOR"] == 'Other'):
            sic = idx_info[0]["SIC_CODE"]
            if sic:
                try:
                    sic_row = session.sql(f"""
                        SELECT INDUSTRY_SECTOR, INDUSTRY_TITLE FROM {fqn}.SIC_CODES WHERE SIC_CODE = '{sic}'
                    """).collect()
                    if sic_row and sic_row[0]["INDUSTRY_SECTOR"]:
                        session.sql(f"""
                            UPDATE {fqn}.FILING_INDEX
                            SET INDUSTRY_SECTOR = '{sic_row[0]["INDUSTRY_SECTOR"]}',
                                INDUSTRY_TITLE = '{sic_row[0]["INDUSTRY_TITLE"].replace("'", "''")}'
                            WHERE ACCESSION_NO = '{acc}'
                        """).collect()
                        results["industry_fixed"] += 1
                except Exception:
                    pass

    return (f"Prepared {len(accessions)} filing(s): "
            f"{results['downloaded']} downloaded, {results['enriched']} tickers enriched, "
            f"{results['industry_fixed']} industries fixed, {len(results['errors'])} errors."
            + (f" Errors: {', '.join(results['errors'][:5])}" if results['errors'] else ""))
$$;


-- =============================================================================
-- SP: PROCESS_FILINGS (SQL — chunk + signal extract + search refresh)
-- =============================================================================

CREATE OR REPLACE PROCEDURE PROCESS_FILINGS(P_ACCESSIONS ARRAY)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_chunk_count INT := 0;
    v_signal_count INT := 0;
    v_search_svc VARCHAR;
    v_acc_list VARCHAR;
BEGIN
    -- Build comma-separated list for IN clause
    SELECT LISTAGG('''' || VALUE::VARCHAR || '''', ',') INTO :v_acc_list
    FROM TABLE(FLATTEN(INPUT => :P_ACCESSIONS));

    IF (:v_acc_list IS NULL OR :v_acc_list = '') THEN
        RETURN 'No accessions provided.';
    END IF;

    -- Chunk all pending filings in the list
    EXECUTE IMMEDIATE '
        INSERT INTO FILING_CHUNKS
            (CHUNK_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
             FILED_AT, PERIOD_OF_REPORT, SECTION_NAME, CHUNK_INDEX,
             CHUNK_TEXT, TOKEN_COUNT, INDUSTRY_SECTOR, INDUSTRY_TITLE)
        SELECT
            fi.ACCESSION_NO || ''_'' || c.VALUE:chunk_index::VARCHAR,
            fi.ACCESSION_NO, fi.COMPANY_NAME, fi.TICKER, fi.FORM_TYPE,
            fi.FILED_AT, fi.PERIOD_OF_REPORT,
            c.VALUE:section_name::VARCHAR, c.VALUE:chunk_index::INT,
            c.VALUE:chunk_text::VARCHAR,
            LENGTH(c.VALUE:chunk_text::VARCHAR) / 4,
            fi.INDUSTRY_SECTOR, fi.INDUSTRY_TITLE
        FROM FILING_CONTENT fc
        JOIN FILING_INDEX fi ON fi.ACCESSION_NO = fc.ACCESSION_NO,
        LATERAL FLATTEN(INPUT => CHUNK_FILING(CLEAN_TEXT(fc.CONTENT_TEXT), fi.FORM_TYPE, 1500, 200)) c
        WHERE fc.ACCESSION_NO IN (' || :v_acc_list || ')
          AND fc.PARSE_STATUS = ''PENDING''
          AND fc.CONTENT_TEXT IS NOT NULL
          AND c.VALUE:chunk_text::VARCHAR IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM FILING_CHUNKS ck WHERE ck.ACCESSION_NO = fc.ACCESSION_NO)
    ';

    -- Update parse status
    EXECUTE IMMEDIATE '
        UPDATE FILING_CONTENT SET PARSE_STATUS = ''CHUNKED'', PROCESSED_AT = CURRENT_TIMESTAMP()
        WHERE ACCESSION_NO IN (' || :v_acc_list || ')
          AND PARSE_STATUS = ''PENDING''
          AND EXISTS (SELECT 1 FROM FILING_CHUNKS WHERE FILING_CHUNKS.ACCESSION_NO = FILING_CONTENT.ACCESSION_NO)
    ';

    -- Count chunks created
    EXECUTE IMMEDIATE '
        SELECT COUNT(*) FROM FILING_CHUNKS WHERE ACCESSION_NO IN (' || :v_acc_list || ')
    ' INTO :v_chunk_count;

    -- Signal extraction using shared V_SIGNAL_EXCERPT view
    EXECUTE IMMEDIATE '
        INSERT INTO FILING_SIGNALS
            (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
             SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
             KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
             EXTRACTION_MODEL, IS_AMENDMENT, EXTRACTION_METHOD, SIGNAL_EXTRACTED_AT)
        WITH source AS (
            SELECT v.ACCESSION_NO, v.COMPANY_NAME, v.TICKER, v.FORM_TYPE,
                   v.FILED_AT, v.PERIOD_OF_REPORT, v.IS_AMENDMENT,
                   v.INDUSTRY_SECTOR, v.INDUSTRY_TITLE, v.EXCERPT
            FROM V_SIGNAL_EXCERPT v
            JOIN FILING_CONTENT fc ON fc.ACCESSION_NO = v.ACCESSION_NO
            WHERE fc.ACCESSION_NO IN (' || :v_acc_list || ')
              AND fc.SIGNAL_STATUS = ''PENDING''
              AND NOT EXISTS (SELECT 1 FROM FILING_SIGNALS WHERE FILING_SIGNALS.ACCESSION_NO = fc.ACCESSION_NO)
        ),
        extracted AS (
            SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
                text => s.EXCERPT,
                responseFormat => {
                    ''event_type'': ''string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other'',
                    ''sentiment'': ''string - strictly one of: POSITIVE, NEGATIVE, NEUTRAL'',
                    ''summary'': ''string - 2-3 sentence summary of the most material information'',
                    ''key_metrics'': ''object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change'',
                    ''risk_flags'': ''array of strings - specific risk categories mentioned'',
                    ''material_items'': ''array of strings - for 8-Ks: Item numbers reported''
                }
            ) AS ai_result FROM source s
        )
        SELECT e.ACCESSION_NO || ''_sig'', e.ACCESSION_NO, e.COMPANY_NAME, e.TICKER, e.FORM_TYPE,
            e.FILED_AT, e.PERIOD_OF_REPORT,
            COALESCE(NULLIF(e.ai_result:response:event_type::VARCHAR, ''None''), ''Other''),
            COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, ''None''), ''NEUTRAL''),
            NULLIF(e.ai_result:response:summary::TEXT, ''None''),
            NULLIF(e.ai_result:response:key_metrics::VARCHAR, ''None''),
            CASE WHEN e.ai_result:response:risk_flags::VARCHAR = ''None'' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
            CASE WHEN e.ai_result:response:material_items::VARCHAR = ''None'' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
            e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, ''arctic-extract'', e.IS_AMENDMENT,
            ''section_targeted'', CURRENT_TIMESTAMP()
        FROM extracted e WHERE e.ai_result IS NOT NULL
    ';

    -- Update signal status
    EXECUTE IMMEDIATE '
        UPDATE FILING_CONTENT SET SIGNAL_STATUS = ''EXTRACTED''
        WHERE ACCESSION_NO IN (' || :v_acc_list || ')
          AND SIGNAL_STATUS = ''PENDING''
          AND EXISTS (SELECT 1 FROM FILING_SIGNALS WHERE FILING_SIGNALS.ACCESSION_NO = FILING_CONTENT.ACCESSION_NO)
    ';

    -- Count signals
    EXECUTE IMMEDIATE '
        SELECT COUNT(*) FROM FILING_SIGNALS WHERE ACCESSION_NO IN (' || :v_acc_list || ')
    ' INTO :v_signal_count;

    -- Force Cortex Search refresh
    v_search_svc := _CFG('database') || '.' || _CFG('schema') || '.' || _CFG('search_service');
    EXECUTE IMMEDIATE 'ALTER CORTEX SEARCH SERVICE ' || :v_search_svc || ' RESUME';

    RETURN 'Processed ' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filing(s): ' ||
           :v_chunk_count::VARCHAR || ' chunks, ' || :v_signal_count::VARCHAR || ' signal(s). Search refresh triggered.';
END;
$$;


-- =============================================================================
-- SP: TRIGGER_PROCESS_FILINGS (Async wrapper — dynamic task + email)
-- =============================================================================

CREATE OR REPLACE PROCEDURE TRIGGER_PROCESS_FILINGS(P_ACCESSIONS ARRAY)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    task_name VARCHAR;
    task_ddl VARCHAR;
    task_body VARCHAR;
    email_int VARCHAR;
    email_to VARCHAR;
    acc_str VARCHAR;
BEGIN
    task_name := 'PROCESS_FILING_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    email_int := _CFG('email_integration');
    email_to := _CFG('email_recipient');

    -- Build ARRAY_CONSTRUCT string from the input array
    SELECT LISTAGG('''' || VALUE::VARCHAR || '''', ',') INTO :acc_str
    FROM TABLE(FLATTEN(INPUT => :P_ACCESSIONS));

    task_body := 'BEGIN' ||
        ' LET prep_result VARCHAR; LET proc_result VARCHAR;' ||
        ' CALL PREPARE_FILINGS(ARRAY_CONSTRUCT(' || :acc_str || '));' ||
        ' prep_result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));' ||
        ' CALL PROCESS_FILINGS(ARRAY_CONSTRUCT(' || :acc_str || '));' ||
        ' proc_result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));' ||
        ' IF ((SELECT VALUE FROM _PIPELINE_CONFIG WHERE KEY = ''enable_dag_emails'') = ''TRUE'') THEN' ||
        '   CALL SYSTEM$SEND_EMAIL(''' || :email_int || ''', ''' || :email_to || ''',' ||
        '   ''SEC Filing Processing Complete (' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filings)'',' ||
        '   :prep_result || CHR(10) || :proc_result);' ||
        ' END IF;' ||
        ' END';

    task_ddl := 'CREATE OR REPLACE TASK ' || :task_name ||
                ' WAREHOUSE = FILING_BUILD_WH' ||
                ' USER_TASK_TIMEOUT_MS = 7200000' ||
                ' SCHEDULE = ''USING CRON 0 0 29 2 * UTC''' ||
                ' COMMENT = ''Spot-process ' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filing(s)''' ||
                ' AS ' || :task_body;

    EXECUTE IMMEDIATE :task_ddl;
    EXECUTE IMMEDIATE 'EXECUTE TASK ' || :task_name;

    RETURN 'Processing ' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filing(s) async. Task: ' || :task_name || '. Email on completion.';
END;
$$;


-- =============================================================================
-- Backward-compatible wrappers (single-filing convenience)
-- =============================================================================

CREATE OR REPLACE PROCEDURE PROCESS_SINGLE_FILING(P_ACCESSION_NO VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL PREPARE_FILINGS(ARRAY_CONSTRUCT(:P_ACCESSION_NO));
    LET prep_result VARCHAR := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    CALL PROCESS_FILINGS(ARRAY_CONSTRUCT(:P_ACCESSION_NO));
    LET proc_result VARCHAR := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN :prep_result || ' | ' || :proc_result;
END;
$$;

CREATE OR REPLACE PROCEDURE TRIGGER_PROCESS_FILING(P_ACCESSION_NO VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL TRIGGER_PROCESS_FILINGS(ARRAY_CONSTRUCT(:P_ACCESSION_NO));
    LET result VARCHAR := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN :result;
END;
$$;


-- =============================================================================
-- SP: REEXTRACT_SIGNALS (Force re-extraction with section-targeted method)
-- =============================================================================
-- Deletes existing signals for the given accessions and re-extracts using
-- V_SIGNAL_EXCERPT (section-targeted for 10-K/10-Q). Logs the new method.
--
-- Usage:
--   -- Re-extract a single filing:
--   CALL REEXTRACT_SIGNALS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--
--   -- Re-extract a batch:
--   CALL REEXTRACT_SIGNALS(ARRAY_CONSTRUCT('acc1', 'acc2', 'acc3'));
--
--   -- Re-extract all filings still using old method:
--   -- (build array from: SELECT ARRAY_AGG(ACCESSION_NO) FROM FILING_SIGNALS
--   --  WHERE EXTRACTION_METHOD = 'raw_first_16k' AND FORM_TYPE = '10-K' LIMIT 100)
-- =============================================================================

CREATE OR REPLACE PROCEDURE REEXTRACT_SIGNALS(P_ACCESSIONS ARRAY)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_deleted INT := 0;
    v_extracted INT := 0;
    v_acc_list VARCHAR;
BEGIN
    SELECT LISTAGG('''' || VALUE::VARCHAR || '''', ',') INTO :v_acc_list
    FROM TABLE(FLATTEN(INPUT => :P_ACCESSIONS));

    IF (:v_acc_list IS NULL OR :v_acc_list = '') THEN
        RETURN 'No accessions provided.';
    END IF;

    -- Delete existing signals for these filings
    EXECUTE IMMEDIATE '
        DELETE FROM FILING_SIGNALS WHERE ACCESSION_NO IN (' || :v_acc_list || ')
    ';
    v_deleted := SQLROWCOUNT;

    -- Reset signal status to PENDING
    EXECUTE IMMEDIATE '
        UPDATE FILING_CONTENT SET SIGNAL_STATUS = ''PENDING''
        WHERE ACCESSION_NO IN (' || :v_acc_list || ')
    ';

    -- Re-extract using V_SIGNAL_EXCERPT (section-targeted for 10-K/10-Q)
    EXECUTE IMMEDIATE '
        INSERT INTO FILING_SIGNALS
            (SIGNAL_ID, ACCESSION_NO, COMPANY_NAME, TICKER, FORM_TYPE,
             SIGNAL_DATE, PERIOD_OF_REPORT, EVENT_TYPE, SENTIMENT, SUMMARY,
             KEY_METRICS, RISK_FLAGS, MATERIAL_ITEMS, INDUSTRY_SECTOR, INDUSTRY_TITLE,
             EXTRACTION_MODEL, IS_AMENDMENT, EXTRACTION_METHOD, SIGNAL_EXTRACTED_AT)
        WITH source AS (
            SELECT v.ACCESSION_NO, v.COMPANY_NAME, v.TICKER, v.FORM_TYPE,
                   v.FILED_AT, v.PERIOD_OF_REPORT, v.IS_AMENDMENT,
                   v.INDUSTRY_SECTOR, v.INDUSTRY_TITLE, v.EXCERPT
            FROM V_SIGNAL_EXCERPT v
            WHERE v.ACCESSION_NO IN (' || :v_acc_list || ')
        ),
        extracted AS (
            SELECT s.*, SNOWFLAKE.CORTEX.AI_EXTRACT(
                text => s.EXCERPT,
                responseFormat => {
                    ''event_type'': ''string - one of: Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Other'',
                    ''sentiment'': ''string - strictly one of: POSITIVE, NEGATIVE, NEUTRAL'',
                    ''summary'': ''string - 2-3 sentence summary of the most material information'',
                    ''key_metrics'': ''object - any financial figures mentioned: revenue, net_income, eps, guidance, yoy_change'',
                    ''risk_flags'': ''array of strings - specific risk categories mentioned'',
                    ''material_items'': ''array of strings - for 8-Ks: Item numbers reported''
                }
            ) AS ai_result FROM source s
        )
        SELECT e.ACCESSION_NO || ''_sig'', e.ACCESSION_NO, e.COMPANY_NAME, e.TICKER, e.FORM_TYPE,
            e.FILED_AT, e.PERIOD_OF_REPORT,
            COALESCE(NULLIF(e.ai_result:response:event_type::VARCHAR, ''None''), ''Other''),
            COALESCE(NULLIF(e.ai_result:response:sentiment::VARCHAR, ''None''), ''NEUTRAL''),
            NULLIF(e.ai_result:response:summary::TEXT, ''None''),
            NULLIF(e.ai_result:response:key_metrics::VARCHAR, ''None''),
            CASE WHEN e.ai_result:response:risk_flags::VARCHAR = ''None'' THEN NULL ELSE e.ai_result:response:risk_flags::ARRAY END,
            CASE WHEN e.ai_result:response:material_items::VARCHAR = ''None'' THEN NULL ELSE e.ai_result:response:material_items::ARRAY END,
            e.INDUSTRY_SECTOR, e.INDUSTRY_TITLE, ''arctic-extract'', e.IS_AMENDMENT,
            ''section_targeted'', CURRENT_TIMESTAMP()
        FROM extracted e WHERE e.ai_result IS NOT NULL
    ';

    -- Update signal status
    EXECUTE IMMEDIATE '
        UPDATE FILING_CONTENT SET SIGNAL_STATUS = ''EXTRACTED''
        WHERE ACCESSION_NO IN (' || :v_acc_list || ')
          AND EXISTS (SELECT 1 FROM FILING_SIGNALS WHERE FILING_SIGNALS.ACCESSION_NO = FILING_CONTENT.ACCESSION_NO)
    ';

    SELECT COUNT(*) INTO :v_extracted FROM FILING_SIGNALS
    WHERE ACCESSION_NO IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(INPUT => :P_ACCESSIONS)));

    RETURN 'Re-extracted ' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filing(s): ' ||
           :v_deleted::VARCHAR || ' old signals deleted, ' ||
           :v_extracted::VARCHAR || ' new signals created (section_targeted method).';
END;
$$;


-- =============================================================================
-- SP: TRIGGER_PROCESS_FILINGS_FULL (Full Pipeline — Dynamic Task DAG)
-- =============================================================================
-- Creates a self-cleaning dynamic task DAG that runs the ENTIRE pipeline:
--   prepare → process → metrics → guidance → normalize → propagate → search → serving
--
-- Unlike TRIGGER_PROCESS_FILINGS (which only runs prepare + process), this SP
-- runs all enrichment, extraction, normalization, propagation, and serving steps.
--
-- The DAG self-cleans: the finalizer task drops all dynamic tasks on completion.
--
-- Usage:
--   -- Process specific filings (full pipeline):
--   CALL TRIGGER_PROCESS_FILINGS_FULL(ARRAY_CONSTRUCT('acc1', 'acc2'));
--
--   -- Process ALL pending filings (full pipeline):
--   CALL TRIGGER_PROCESS_FILINGS_FULL(NULL);
--
-- DAG Structure:
--   <dag_name>_ROOT (root — never-fire schedule)
--   ├── <dag_name>_PREPARE     (CALL PREPARE_FILINGS, warehouse = ingest)
--   ├── <dag_name>_PROCESS     (CALL PROCESS_FILINGS, after PREPARE, warehouse = build)
--   ├── <dag_name>_METRICS     (CALL EXTRACT_KEY_METRICS_BATCH, after PROCESS, warehouse = build)
--   ├── <dag_name>_GUIDANCE    (CALL EXTRACT_FORWARD_GUIDANCE_BATCH, after PROCESS, warehouse = build)
--   ├── <dag_name>_NORMALIZE   (inline UPDATE, after PROCESS, warehouse = steady-state)
--   ├── <dag_name>_PROPAGATE   (inline UPDATEs, after METRICS+NORMALIZE, warehouse = steady-state)
--   ├── <dag_name>_SEARCH      (ALTER CORTEX SEARCH REFRESH, after PROPAGATE, warehouse = steady-state)
--   ├── <dag_name>_SERVING     (EXECUTE TASK T_SERVING_ROOT, after SEARCH, warehouse = steady-state)
--   └── <dag_name>_FINALIZER   (FINALIZE = ROOT: emails + self-cleans)
--
-- Dependencies:
--   - PREPARE_FILINGS, PROCESS_FILINGS SPs (this file)
--   - EXTRACT_KEY_METRICS_BATCH, EXTRACT_FORWARD_GUIDANCE_BATCH SPs (05_processing_task_dag.sql)
--   - T_SERVING_ROOT task (04_serving_task_dag.sql, optional — skipped if not found)
--   - _CFG() function, email integration
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

CREATE OR REPLACE PROCEDURE TRIGGER_PROCESS_FILINGS_FULL(
    P_ACCESSIONS ARRAY DEFAULT NULL,
    P_FEED_DATE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    dag_name VARCHAR;
    acc_str VARCHAR;
    acc_array_sql VARCHAR;
    wh_build VARCHAR;
    wh_ingest VARCHAR;
    wh_steady VARCHAR;
    db_name VARCHAR;
    schema_name VARCHAR;
    svc_name VARCHAR;
    email_int VARCHAR;
    email_to VARCHAR;
    user_agent VARCHAR;
    prepare_after VARCHAR;
    filing_count INT;
BEGIN
    -- Resolve config values
    wh_build := _CFG('warehouse_build');
    wh_ingest := _CFG('warehouse_ingest');
    wh_steady := _CFG('warehouse');
    db_name := _CFG('database');
    schema_name := _CFG('schema');
    svc_name := _CFG('search_service');
    email_int := _CFG('email_integration');
    email_to := _CFG('email_recipient');
    user_agent := _CFG('user_agent');

    -- Generate unique DAG name
    dag_name := 'PROCESS_FULL_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    -- =========================================================================
    -- ROOT TASK (never-fire schedule)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_ROOT
            WAREHOUSE = ' || :wh_steady || '
            USER_TASK_TIMEOUT_MS = 172800000
            SCHEDULE = ''USING CRON 0 0 29 2 * UTC''
            COMMENT = ''Full processing DAG''
        AS SELECT 1';

    -- =========================================================================
    -- INGEST (optional — only if P_FEED_DATE provided)
    -- =========================================================================
    IF (:P_FEED_DATE IS NOT NULL) THEN
        EXECUTE IMMEDIATE '
            CREATE TASK ' || :dag_name || '_INGEST
                WAREHOUSE = ' || :wh_ingest || '
                USER_TASK_TIMEOUT_MS = 172800000
                COMMENT = ''Ingest feed archive for ' || :P_FEED_DATE || '''
                AFTER ' || :dag_name || '_ROOT
            AS CALL LOAD_FEED_ARCHIVE(''' || :P_FEED_DATE || ''', ''' || :user_agent || ''')';
        prepare_after := :dag_name || '_INGEST';
    ELSE
        prepare_after := :dag_name || '_ROOT';
    END IF;

    -- Default to all PENDING filings if NULL/empty
    IF (:P_ACCESSIONS IS NULL OR ARRAY_SIZE(:P_ACCESSIONS) = 0) THEN
        -- If we're ingesting, accessions won't exist yet — use NULL to process all PENDING at runtime
        acc_array_sql := 'NULL';
    ELSE
        -- Build ARRAY_CONSTRUCT string from input array
        SELECT LISTAGG('''' || VALUE::VARCHAR || '''', ',') INTO :acc_str
        FROM TABLE(FLATTEN(INPUT => :P_ACCESSIONS));
        acc_array_sql := 'ARRAY_CONSTRUCT(' || :acc_str || ')';
    END IF;

    -- =========================================================================
    -- PREPARE (download content + enrich ticker + fix industry)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_PREPARE
            WAREHOUSE = ' || :wh_ingest || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Prepare filings: download + ticker + industry''
            AFTER ' || :prepare_after || '
        AS CALL PREPARE_FILINGS(' || :acc_array_sql || ')';

    -- =========================================================================
    -- PROCESS (chunk + signal extract + search refresh)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_PROCESS
            WAREHOUSE = ' || :wh_build || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Process filings: chunk + signal extract''
            AFTER ' || :dag_name || '_PREPARE
        AS CALL PROCESS_FILINGS(' || :acc_array_sql || ')';

    -- =========================================================================
    -- METRICS (extract revenue, EPS, net income, YoY)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_METRICS
            WAREHOUSE = ' || :wh_build || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Extract key financial metrics''
            AFTER ' || :dag_name || '_PROCESS
        AS CALL EXTRACT_KEY_METRICS_BATCH(500)';

    -- =========================================================================
    -- GUIDANCE (extract forward guidance statements)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_GUIDANCE
            WAREHOUSE = ' || :wh_build || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Extract forward guidance''
            AFTER ' || :dag_name || '_PROCESS
        AS CALL EXTRACT_FORWARD_GUIDANCE_BATCH(500)';

    -- =========================================================================
    -- NORMALIZE (map EVENT_TYPE to 12 canonical categories)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_NORMALIZE
            WAREHOUSE = ' || :wh_steady || '
            USER_TASK_TIMEOUT_MS = 600000
            COMMENT = ''Normalize event types to 12 categories''
            AFTER ' || :dag_name || '_PROCESS
        AS
        BEGIN
            UPDATE FILING_SIGNALS SET EVENT_TYPE_NORMALIZED = CASE
                WHEN EVENT_TYPE IN (''Earnings'',''M&A'',''Leadership Change'',''Risk Disclosure'',''Guidance Update'',''Regulatory'',''Capital Markets'',''Bankruptcy'',''Annual Report'',''Quarterly Report'',''Current Report'',''Other'') THEN EVENT_TYPE
                WHEN EVENT_TYPE ILIKE ''%acqui%'' OR EVENT_TYPE ILIKE ''%merger%'' OR EVENT_TYPE ILIKE ''%disposition%'' THEN ''M&A''
                WHEN EVENT_TYPE ILIKE ''%change in control%'' OR EVENT_TYPE ILIKE ''%change of control%'' THEN ''M&A''
                WHEN EVENT_TYPE ILIKE ''%leadership%'' OR EVENT_TYPE ILIKE ''%chief%'' OR EVENT_TYPE ILIKE ''%officer%'' THEN ''Leadership Change''
                WHEN EVENT_TYPE ILIKE ''%regulation%'' OR EVENT_TYPE ILIKE ''%compliance%'' OR EVENT_TYPE ILIKE ''%sanction%'' OR EVENT_TYPE ILIKE ''%mine safety%'' OR EVENT_TYPE ILIKE ''%ESG%'' OR EVENT_TYPE ILIKE ''%audit%'' OR EVENT_TYPE ILIKE ''%accountant%'' OR EVENT_TYPE ILIKE ''%accounting%'' THEN ''Regulatory''
                WHEN EVENT_TYPE ILIKE ''%dividend%'' OR EVENT_TYPE ILIKE ''%issuance%'' OR EVENT_TYPE ILIKE ''%notes%'' OR EVENT_TYPE ILIKE ''%repurchase%'' OR EVENT_TYPE ILIKE ''%capital%'' OR EVENT_TYPE ILIKE ''%credit%'' OR EVENT_TYPE ILIKE ''%loan%'' OR EVENT_TYPE ILIKE ''%euro%'' THEN ''Capital Markets''
                WHEN EVENT_TYPE ILIKE ''%guidance%'' OR EVENT_TYPE ILIKE ''%forward%look%'' OR EVENT_TYPE ILIKE ''%outlook%'' OR EVENT_TYPE ILIKE ''%update%'' THEN ''Guidance Update''
                WHEN EVENT_TYPE ILIKE ''%risk%'' THEN ''Risk Disclosure''
                WHEN EVENT_TYPE ILIKE ''%bankrupt%'' OR EVENT_TYPE ILIKE ''%shell%'' THEN ''Bankruptcy''
                ELSE ''Other''
            END WHERE EVENT_TYPE_NORMALIZED IS NULL;
        END';

    -- =========================================================================
    -- PROPAGATE (industry + ticker from FILING_INDEX to chunks + signals)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_PROPAGATE
            WAREHOUSE = ' || :wh_steady || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Propagate industry + ticker to downstream tables''
            AFTER ' || :dag_name || '_METRICS, ' || :dag_name || '_NORMALIZE
        AS
        BEGIN
            UPDATE FILING_CHUNKS fc SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR, INDUSTRY_TITLE = fi.INDUSTRY_TITLE FROM FILING_INDEX fi WHERE fc.ACCESSION_NO = fi.ACCESSION_NO AND fi.INDUSTRY_SECTOR IS NOT NULL AND fc.INDUSTRY_SECTOR IS NULL;
            UPDATE FILING_SIGNALS fs SET INDUSTRY_SECTOR = fi.INDUSTRY_SECTOR, INDUSTRY_TITLE = fi.INDUSTRY_TITLE FROM FILING_INDEX fi WHERE fs.ACCESSION_NO = fi.ACCESSION_NO AND fi.INDUSTRY_SECTOR IS NOT NULL AND fs.INDUSTRY_SECTOR IS NULL;
            UPDATE FILING_CHUNKS fc SET TICKER = fi.TICKER FROM FILING_INDEX fi WHERE fc.ACCESSION_NO = fi.ACCESSION_NO AND fi.TICKER IS NOT NULL AND fc.TICKER IS NULL;
            UPDATE FILING_SIGNALS fs SET TICKER = fi.TICKER FROM FILING_INDEX fi WHERE fs.ACCESSION_NO = fi.ACCESSION_NO AND fi.TICKER IS NOT NULL AND fs.TICKER IS NULL;
        END';

    -- =========================================================================
    -- SEARCH (refresh Cortex Search service)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_SEARCH
            WAREHOUSE = ' || :wh_steady || '
            USER_TASK_TIMEOUT_MS = 3600000
            COMMENT = ''Refresh Cortex Search service''
            AFTER ' || :dag_name || '_PROPAGATE
        AS
        BEGIN
            ALTER CORTEX SEARCH SERVICE ' || :db_name || '.' || :schema_name || '.' || :svc_name || ' RESUME INDEXING;
            ALTER CORTEX SEARCH SERVICE ' || :db_name || '.' || :schema_name || '.' || :svc_name || ' REFRESH;
        END';

    -- =========================================================================
    -- SERVING (trigger serving DAG if it exists)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_SERVING
            WAREHOUSE = ' || :wh_steady || '
            USER_TASK_TIMEOUT_MS = 172800000
            COMMENT = ''Trigger serving layer update (semantic view + agent)''
            AFTER ' || :dag_name || '_SEARCH
        AS
        BEGIN
            LET serving_exists INT := 0;
            SHOW TASKS LIKE ''T_SERVING_ROOT'' IN SCHEMA;
            SELECT COUNT(*) INTO :serving_exists FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            IF (:serving_exists > 0) THEN
                EXECUTE TASK T_SERVING_ROOT;
            END IF;
        END';

    -- =========================================================================
    -- FINALIZER (email summary + self-clean)
    -- =========================================================================
    EXECUTE IMMEDIATE '
        CREATE TASK ' || :dag_name || '_FINALIZER
            WAREHOUSE = ' || :wh_steady || '
            FINALIZE = ' || :dag_name || '_ROOT
            COMMENT = ''Finalizer: email summary + drop dynamic tasks''
        AS
        BEGIN
            LET total_signals INT;
            LET with_revenue INT;
            LET with_guidance INT;
            LET with_normalized INT;

            SELECT COUNT(*) INTO :total_signals FROM FILING_SIGNALS;
            SELECT COUNT(REVENUE) INTO :with_revenue FROM FILING_SIGNALS;
            SELECT COUNT(FORWARD_GUIDANCE) INTO :with_guidance FROM FILING_SIGNALS;
            SELECT COUNT(EVENT_TYPE_NORMALIZED) INTO :with_normalized FROM FILING_SIGNALS;

            LET msg VARCHAR := ''FULL PROCESSING PIPELINE COMPLETE'' || CHR(10) || CHR(10) ||
                ''Filings processed: ' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ''' || CHR(10) ||
                ''Total signals: '' || :total_signals::VARCHAR || CHR(10) ||
                ''With revenue: '' || :with_revenue::VARCHAR || CHR(10) ||
                ''With guidance: '' || :with_guidance::VARCHAR || CHR(10) ||
                ''With normalized type: '' || :with_normalized::VARCHAR || CHR(10) ||
                ''Search: refreshed'' || CHR(10) ||
                ''Serving: updated'' || CHR(10) || CHR(10) ||
                ''Timestamp: '' || CURRENT_TIMESTAMP()::VARCHAR;

            IF ((SELECT VALUE FROM _PIPELINE_CONFIG WHERE KEY = ''enable_dag_emails'') = ''TRUE'') THEN
                CALL SYSTEM$SEND_EMAIL(
                    ''' || :email_int || ''',
                    ''' || :email_to || ''',
                    ''SEC Full Pipeline Complete (' || ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filings)'',
                    :msg
                );
            END IF;

            -- Self-clean: drop all dynamic tasks
            DROP TASK IF EXISTS ' || :dag_name || '_FINALIZER;
            DROP TASK IF EXISTS ' || :dag_name || '_SERVING;
            DROP TASK IF EXISTS ' || :dag_name || '_SEARCH;
            DROP TASK IF EXISTS ' || :dag_name || '_PROPAGATE;
            DROP TASK IF EXISTS ' || :dag_name || '_NORMALIZE;
            DROP TASK IF EXISTS ' || :dag_name || '_GUIDANCE;
            DROP TASK IF EXISTS ' || :dag_name || '_METRICS;
            DROP TASK IF EXISTS ' || :dag_name || '_PROCESS;
            DROP TASK IF EXISTS ' || :dag_name || '_PREPARE;
            DROP TASK IF EXISTS ' || :dag_name || '_INGEST;
            DROP TASK IF EXISTS ' || :dag_name || '_ROOT;
        END';

    -- =========================================================================
    -- RESUME all child tasks, then root, then execute
    -- =========================================================================
    IF (:P_FEED_DATE IS NOT NULL) THEN
        EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_INGEST RESUME';
    END IF;
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_PREPARE RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_PROCESS RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_METRICS RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_GUIDANCE RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_NORMALIZE RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_PROPAGATE RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_SEARCH RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_SERVING RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_FINALIZER RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK ' || :dag_name || '_ROOT RESUME';
    EXECUTE IMMEDIATE 'EXECUTE TASK ' || :dag_name || '_ROOT';

    LET desc_str VARCHAR := CASE
        WHEN :P_FEED_DATE IS NOT NULL THEN 'feed_date=' || :P_FEED_DATE
        WHEN :P_ACCESSIONS IS NOT NULL THEN ARRAY_SIZE(:P_ACCESSIONS)::VARCHAR || ' filings'
        ELSE 'all PENDING filings'
    END;

    RETURN 'Full pipeline DAG created and triggered: ' || :dag_name ||
           ' (' || :desc_str || '). ' ||
           'Monitor: SELECT NAME, STATE FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(' ||
           'SCHEDULED_TIME_RANGE_START => DATEADD(''hour'', -24, CURRENT_TIMESTAMP()), RESULT_LIMIT => 20' ||
           ')) WHERE NAME LIKE ''' || :dag_name || '%'' ORDER BY SCHEDULED_TIME DESC;';
END;
$$;


-- =============================================================================
-- SP: QUICK_START (One-call setup: ingest single day + full processing DAG)
-- =============================================================================
-- Convenience wrapper for first-time setup. Ingests one day of filings from
-- SEC EDGAR feed archive, then triggers the full processing pipeline as a
-- monitorable dynamic task DAG.
--
-- Usage:
--   CALL QUICK_START();                    -- Uses default date (2025-02-21)
--   CALL QUICK_START('2025-03-05');        -- Custom date
--
-- The SP returns immediately with a DAG name for monitoring.
-- =============================================================================

CREATE OR REPLACE PROCEDURE QUICK_START(P_DATE VARCHAR DEFAULT '2025-02-21')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CALL TRIGGER_PROCESS_FILINGS_FULL(NULL, :P_DATE);
    LET result VARCHAR := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    RETURN 'Quick Start initiated for ' || :P_DATE || '. ' || :result;
END;
$$;

