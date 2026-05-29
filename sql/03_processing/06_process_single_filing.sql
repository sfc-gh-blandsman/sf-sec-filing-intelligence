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
--   -- Synchronous (blocks until done):
--   CALL PREPARE_FILINGS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--   CALL PROCESS_FILINGS(ARRAY_CONSTRUCT('0000320193-25-000079'));
--
-- Dependencies:
--   - SEC_EDGAR_EAI (for content download + ticker enrichment)
--   - CLEAN_TEXT UDF, CHUNK_FILING UDF
--   - SIC_CODES reference table
--   - Cortex Search service
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
