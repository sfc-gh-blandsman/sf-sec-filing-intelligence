-- =============================================================================
-- 02: Download Filing Content
-- =============================================================================
-- Stored procedure to download actual filing text from SEC EDGAR.
-- Reads URLs from FILING_INDEX, downloads HTML/TXT, strips tags,
-- and stores cleaned text in FILING_CONTENT.
--
-- Rate limited to ~9 requests/sec per SEC fair use policy.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: DOWNLOAD_FILING_BATCH
-- =============================================================================
-- Downloads a batch of filings from SEC EDGAR and stores content.
--
-- Parameters:
--   BATCH_LIMIT: Number of filings to download in this batch
--   FORM_TYPE:   Filter by form type ('10-K', '10-Q', '8-K')
--   START_DATE:  Start of filing date range (YYYY-MM-DD)
--   END_DATE:    End of filing date range (YYYY-MM-DD)
--
-- Usage:
--   CALL DOWNLOAD_FILING_BATCH(100, '10-K', '2025-01-01', '2025-12-31');
--
-- For quick start mode, use 100 as BATCH_LIMIT (adjustable).
-- =============================================================================

CREATE OR REPLACE PROCEDURE DOWNLOAD_FILING_BATCH(
    BATCH_LIMIT NUMBER,
    FORM_TYPE VARCHAR,
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','requests','pandas')
HANDLER = 'download_filing_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS
$$
import requests
import time
import pandas as pd

RATE_LIMIT_SLEEP = 0.11
MAX_TEXT_CHARS = 16_000_000  # 16MB Snowflake TEXT column limit

def download_filing_batch(session, batch_limit: int, form_type: str, start_date: str, end_date: str, user_agent: str) -> str:
    # Derive database/schema from current context (works in both interactive and Task)
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate'
    }

    pending_df = session.sql(f"""
        SELECT fi.ACCESSION_NO, fi.PRIMARY_DOC_URL
        FROM   {fqn}.FILING_INDEX fi
        LEFT JOIN {fqn}.FILING_CONTENT fc ON fi.ACCESSION_NO = fc.ACCESSION_NO
        WHERE  fi.DOWNLOADED_AT IS NULL
          AND  fi.FORM_TYPE = '{form_type}'
          AND  TO_DATE(fi.FILED_AT) BETWEEN '{start_date}' AND '{end_date}'
          AND  fc.ACCESSION_NO IS NULL
        ORDER BY fi.FILED_AT DESC
        LIMIT  {batch_limit}
    """).to_pandas()

    if pending_df.empty:
        return f"No pending {form_type} filings in {start_date} to {end_date}"

    downloaded, errors = 0, 0
    content_rows = []
    downloaded_accessions = []

    for _, row in pending_df.iterrows():
        accession_no = str(row['ACCESSION_NO'])
        url          = str(row['PRIMARY_DOC_URL'])
        try:
            resp = requests.get(url, headers=headers, timeout=45)
            time.sleep(RATE_LIMIT_SLEEP)
            if resp.status_code != 200:
                errors += 1
                content_rows.append({
                    'ACCESSION_NO': accession_no, 'CONTENT_TEXT': None,
                    'STAGE_FILE_PATH': None, 'FILE_SIZE_BYTES': 0,
                    'FILE_FORMAT': 'ERROR', 'PARSE_STATUS': 'ERROR',
                    'PARSE_ERROR': f"HTTP {resp.status_code}"
                })
                continue
            raw_content = resp.text
            file_format = 'HTML' if '<html' in raw_content[:500].lower() else 'TXT'
            # Store raw content — CLEAN_TEXT UDF handles cleaning at chunk time
            content_text = raw_content[:MAX_TEXT_CHARS]
            parse_error = 'TRUNCATED_AT_16MB' if len(raw_content) > MAX_TEXT_CHARS else None
            content_rows.append({
                'ACCESSION_NO': accession_no, 'CONTENT_TEXT': content_text,
                'STAGE_FILE_PATH': None, 'FILE_SIZE_BYTES': len(raw_content),
                'FILE_FORMAT': file_format, 'PARSE_STATUS': 'PENDING',
                'PARSE_ERROR': parse_error
            })
            downloaded_accessions.append(accession_no)
            downloaded += 1
        except Exception as exc:
            errors += 1
            content_rows.append({
                'ACCESSION_NO': accession_no, 'CONTENT_TEXT': None,
                'STAGE_FILE_PATH': None, 'FILE_SIZE_BYTES': 0,
                'FILE_FORMAT': 'ERROR', 'PARSE_STATUS': 'ERROR',
                'PARSE_ERROR': str(exc)[:500]
            })

    if content_rows:
        df = pd.DataFrame(content_rows)
        tmp = f"{fqn}.FILING_CONTENT_TMP_{abs(hash(form_type + start_date)) % 100000}"
        session.create_dataframe(df).write.mode("overwrite").save_as_table(tmp)
        session.sql(f"""
            INSERT INTO {fqn}.FILING_CONTENT
                (ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH, FILE_SIZE_BYTES,
                 FILE_FORMAT, PARSE_STATUS, PARSE_ERROR)
            SELECT t.ACCESSION_NO, t.CONTENT_TEXT, t.STAGE_FILE_PATH,
                   t.FILE_SIZE_BYTES::NUMBER, t.FILE_FORMAT, t.PARSE_STATUS, t.PARSE_ERROR
            FROM   {tmp} t
            WHERE  NOT EXISTS (
                SELECT 1 FROM {fqn}.FILING_CONTENT fc
                WHERE  fc.ACCESSION_NO = t.ACCESSION_NO
            )
        """).collect()
        session.sql(f"DROP TABLE IF EXISTS {tmp}").collect()

    if downloaded_accessions:
        ids_sql = "', '".join(downloaded_accessions)
        session.sql(f"""
            UPDATE {fqn}.FILING_INDEX
            SET    DOWNLOADED_AT = CURRENT_TIMESTAMP()
            WHERE  ACCESSION_NO IN ('{ids_sql}')
        """).collect()

    return (f"Batch complete: {downloaded} downloaded, {errors} errors "
            f"(form={form_type}, range={start_date} to {end_date})")
$$;


-- NOTE: This is the ALTERNATIVE ingestion method (per-filing HTTP download).
-- For bulk ingestion, use 04_feed_archive_loader.sql instead (100-1000x faster).

-- =============================================================================
-- EXECUTION: Quick Start (100 filings, single form type)
-- =============================================================================
-- CALL DOWNLOAD_FILING_BATCH(100, '10-K', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');
-- CALL DOWNLOAD_FILING_BATCH(100, '10-Q', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');
-- CALL DOWNLOAD_FILING_BATCH(100, '8-K', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');

-- =============================================================================
-- EXECUTION: Full download (all pending filings for a form type)
-- =============================================================================
-- CALL DOWNLOAD_FILING_BATCH(5000, '10-K', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');
-- CALL DOWNLOAD_FILING_BATCH(5000, '10-Q', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');
-- CALL DOWNLOAD_FILING_BATCH(5000, '8-K', $config_ingest_start_year || '-01-01', $config_ingest_end_year || '-12-31');
-- Repeat until SP returns "No pending..." for each form type.
