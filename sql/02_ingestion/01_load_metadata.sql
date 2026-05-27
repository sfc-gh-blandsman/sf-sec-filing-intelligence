-- =============================================================================
-- 01: Load EDGAR Metadata
-- =============================================================================
-- Stored procedure to download SEC EDGAR full-index metadata for a given
-- year/quarter and load into FILING_INDEX table.
--
-- Source: https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz
-- Target forms: 10-K, 10-K/A, 10-KT, 10-KSB, 10-Q, 10-Q/A, 10-QSB, 8-K, 8-K/A
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: LOAD_EDGAR_METADATA
-- =============================================================================
-- Downloads master.gz for a given year/quarter, parses target form types,
-- and inserts into FILING_INDEX (deduplicates on ACCESSION_NO).
--
-- Usage:
--   CALL LOAD_EDGAR_METADATA(2023, 1);  -- Load Q1 2023
--
-- For quick start, call for a single quarter then limit downloads.
-- For full corpus, loop across all quarters in your date range.
-- =============================================================================

CREATE OR REPLACE PROCEDURE LOAD_EDGAR_METADATA(
    "YEAR" NUMBER(38,0),
    "QUARTER" NUMBER(38,0),
    "USER_AGENT" VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','requests','pandas')
HANDLER = 'load_edgar_metadata'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS
$$
import requests
import gzip
import pandas as pd
from datetime import datetime, timezone

TARGET_FORMS = {
    '10-K', '10-K/A', '10-KT', '10-KSB',
    '10-Q', '10-Q/A', '10-QSB',
    '8-K',  '8-K/A'
}

def load_edgar_metadata(session, year: int, quarter: int, user_agent: str) -> str:
    # Derive database/schema from current context (works in both interactive and Task)
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]

    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz"
    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate'
    }

    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code != 200:
        return f"ERROR: HTTP {resp.status_code} fetching {url}"

    raw = gzip.decompress(resp.content).decode('latin-1')
    lines = raw.splitlines()

    # Skip header block — find the dashed separator line
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith('---') or line.startswith('CIK|'):
            data_start = i + 1
            break

    rows = []
    skipped_bad = 0

    for line in lines[data_start:]:
        if not line.strip():
            continue
        parts = line.split('|')
        if len(parts) < 5:
            skipped_bad += 1
            continue

        cik          = parts[0].strip().zfill(10)
        company_name = parts[1].strip()[:500]
        form_type    = parts[2].strip()
        date_filed   = parts[3].strip()
        filename     = parts[4].strip()

        if form_type not in TARGET_FORMS:
            continue
        if not filename or not date_filed:
            continue

        accession_no = filename.split('/')[-1].replace('.txt', '')
        if not accession_no:
            continue

        primary_url = f"https://www.sec.gov/Archives/{filename}"
        index_url   = f"https://www.sec.gov/Archives/{filename.replace('.txt', '-index.htm')}"

        try:
            dt = datetime.strptime(date_filed, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            filed_at_str = dt.strftime('%Y-%m-%d %H:%M:%S +0000')
        except ValueError:
            skipped_bad += 1
            continue

        rows.append({
            'ACCESSION_NO':     accession_no,
            'CIK':              cik,
            'COMPANY_NAME':     company_name,
            'FORM_TYPE':        form_type,
            'FILED_AT':         filed_at_str,
            'PRIMARY_DOC_URL':  primary_url[:1000],
            'FILING_INDEX_URL': index_url[:1000],
            'IS_AMENDMENT':     '/' in form_type
        })

    if not rows:
        return f"No target filings found in {year} Q{quarter}. Bad lines: {skipped_bad}"

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=['ACCESSION_NO'])

    fqn = f"{db}.{schema}"
    tmp_table = f"{fqn}.FILING_INDEX_TMP_{year}_Q{quarter}"
    session.create_dataframe(df).write.mode("overwrite").save_as_table(tmp_table)

    session.sql(f"""
        INSERT INTO {fqn}.FILING_INDEX
            (ACCESSION_NO, CIK, COMPANY_NAME, FORM_TYPE, FILED_AT,
             PRIMARY_DOC_URL, FILING_INDEX_URL, IS_AMENDMENT)
        SELECT t.ACCESSION_NO, t.CIK, t.COMPANY_NAME, t.FORM_TYPE, t.FILED_AT::TIMESTAMP_TZ,
               t.PRIMARY_DOC_URL, t.FILING_INDEX_URL, t.IS_AMENDMENT::BOOLEAN
        FROM   {tmp_table} t
        WHERE  NOT EXISTS (
            SELECT 1 FROM {fqn}.FILING_INDEX fi
            WHERE  fi.ACCESSION_NO = t.ACCESSION_NO
        )
    """).collect()

    session.sql(f"DROP TABLE IF EXISTS {tmp_table}").collect()

    form_counts = df['FORM_TYPE'].value_counts().to_dict()
    summary = ", ".join(f"{v} {k}" for k, v in sorted(form_counts.items()))
    return f"Loaded {len(rows)} filings for {year} Q{quarter}: {summary}"
$$;


-- =============================================================================
-- EXECUTION: Quick Start (single quarter)
-- =============================================================================
-- CALL LOAD_EDGAR_METADATA($config_ingest_start_year, 1);

-- =============================================================================
-- EXECUTION: Full year range
-- =============================================================================
-- Run this block to load all quarters in your configured range:
--
-- DECLARE
--     y INT; q INT;
-- BEGIN
--     FOR y IN $config_ingest_start_year TO $config_ingest_end_year DO
--         FOR q IN 1 TO 4 DO
--             CALL LOAD_EDGAR_METADATA(:y, :q);
--         END FOR;
--     END FOR;
-- END;
